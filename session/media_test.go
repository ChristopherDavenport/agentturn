package session

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentsession/remote"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// screenshots are the images the shot tool returns in turn: two
// distinct ones, each returned twice.
var screenshots = [][]byte{[]byte("\x89PNG first screen"), []byte("\x89PNG second screen")}

func pngURL(data []byte) string {
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(data)
}

// shooter calls shot until four outputs are in, then answers, and
// records what each request carried: how many images, and how many of
// those were not data URLs.
type shooter struct {
	mu             sync.Mutex
	images, notURL int
}

func (m *shooter) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	outputs := 0
	m.mu.Lock()
	for _, item := range req.Input {
		if _, ok := item.(*openresponses.FunctionCallOutput); ok {
			outputs++
		}
		for _, p := range contentOf(item) {
			if img, ok := p.(*openresponses.InputImage); ok {
				m.images++
				if !strings.HasPrefix(img.ImageURL, "data:") {
					m.notURL++
				}
			}
		}
	}
	m.mu.Unlock()
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if outputs >= 4 {
		msg, err := em.Message(openresponses.PhaseFinalAnswer)
		if err != nil {
			return err
		}
		if err := msg.Text("seen"); err != nil {
			return err
		}
		return em.Complete()
	}
	call, err := em.FunctionCall(fmt.Sprintf("shot_%d", outputs), "shot")
	if err != nil {
		return err
	}
	if err := call.Arguments(`{}`); err != nil {
		return err
	}
	if err := call.Close(); err != nil {
		return err
	}
	return em.Complete()
}

// countingBlobs is a memory store that counts the blobs put into it.
type countingBlobs struct {
	*agentsession.MemoryStore
	puts int
}

func (c *countingBlobs) PutBlob(ctx context.Context, data []byte) (string, error) {
	c.puts++
	return c.MemoryStore.PutBlob(ctx, data)
}

func (c *countingBlobs) PutTypedBlob(ctx context.Context, data []byte, mediaType string) (string, error) {
	c.puts++
	return c.MemoryStore.PutTypedBlob(ctx, data, mediaType)
}

// TestTheRecorderConvertsASidecarSessionsMedia pins #284: in a sidecar
// session the recorder stores each inline image it records, in item and
// queued entries alike, as a blob before the entry, writes the entry
// with the blob's sidecar: reference, and hashes the request with the
// same substitution, while the loop and the model keep the data URLs.
// The shape is the round 11 media probe's s2 without the host's
// AfterToolCall conversion and its resolving model wrapper: every hash
// verifies, the provider sees only data URLs, and each distinct image
// is stored once.
func TestTheRecorderConvertsASidecarSessionsMedia(t *testing.T) {
	ctx := context.Background()
	store := &countingBlobs{MemoryStore: agentsession.NewMemoryStore()}
	blobs := store
	rec, s, err := Start(ctx, store, agentsession.Header{Media: agentsession.MediaSidecar})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	shot := agenttool.NewFunc("shot", "takes a screenshot", []byte(`{"type":"object"}`),
		func(context.Context, agenttool.Call) (agenttool.Result, error) {
			img := screenshots[n%2]
			n++
			return agenttool.Result{Output: openresponses.FunctionCallOutputData{Parts: openresponses.Contents{
				&openresponses.InputText{Text: fmt.Sprintf("screen %d", n)},
				&openresponses.InputImage{ImageURL: pngURL(img)},
			}}}, nil
		})
	model := &shooter{}
	a := agentturn.New(agentturn.Config{Model: model, ModelName: "m", Tools: []agenttool.Tool{shot}})
	defer rec.Attach(a)()
	// An input queued before the run, holding the first screenshot too.
	if err := rec.Queue(ctx, a, agentturn.QueueFollowUp, openresponses.UserMessage(
		&openresponses.InputText{Text: "and this one"}, &openresponses.InputImage{ImageURL: pngURL(screenshots[0])})); err != nil {
		t.Fatal(err)
	}
	if end, err := a.Prompt(ctx, openresponses.UserText("look")); err != nil || end.Reason != agentturn.ReasonDone {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	if model.images == 0 || model.notURL != 0 {
		t.Errorf("the model was sent %d images, %d of them not a data URL", model.images, model.notURL)
	}
	// The loop keeps the inline form.
	for _, item := range a.State().Transcript {
		for _, p := range contentOf(item) {
			if img, ok := p.(*openresponses.InputImage); ok && !strings.HasPrefix(img.ImageURL, "data:") {
				t.Errorf("the agent's transcript holds %q", img.ImageURL)
			}
		}
	}
	if n := verifyAll(t, s); n != 6 {
		t.Errorf("responses = %d, want 6", n)
	}
	refs := 0
	for _, e := range s.Entries() {
		line, err := agentsession.MarshalEntry(e)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(line, []byte("data:image")) {
			t.Errorf("%s entry %s holds an inline image", e.EntryType(), e.Base().ID)
		}
		if _, ok := e.(*agentsession.QueuedEntry); ok && !bytes.Contains(line, []byte("sidecar:sha256:")) {
			t.Errorf("the queued entry names no blob: %s", line)
		}
		refs += bytes.Count(line, []byte("sidecar:sha256:"))
	}
	if refs < 5 {
		t.Errorf("entries name %d sidecar references, want one per image recorded", refs)
	}
	held, err := agentsession.SessionBlobs(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(held) != 2 || blobs.puts != 2 {
		t.Errorf("the session names %d blobs and %d were put, want each distinct screenshot once", len(held), blobs.puts)
	}
	for _, h := range held {
		if _, err := store.Blob(ctx, h); err != nil {
			t.Errorf("blob %s: %v", h, err)
		}
	}
}

// putCounter counts the blobs put through it into another writer.
type putCounter struct {
	agentsession.BlobWriter
	puts int
}

func (c *putCounter) PutBlob(ctx context.Context, data []byte) (string, error) {
	c.puts++
	return c.BlobWriter.PutBlob(ctx, data)
}

// TestAChildOfASidecarSessionStoresItsMediaThroughWithMedia pins the two
// other routes of #284: a child session takes its parent's sidecar form
// (#283), so the recorder converts the child's media as it does the
// parent's, and WithMedia names the writer the blobs go through in place
// of the store's.
func TestAChildOfASidecarSessionStoresItsMediaThroughWithMedia(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	blobs := &putCounter{BlobWriter: store}
	rec, s, err := Start(ctx, store, agentsession.Header{Media: agentsession.MediaSidecar}, WithMedia(blobs))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	shot := agenttool.NewFunc("shot", "takes a screenshot", []byte(`{"type":"object"}`),
		func(context.Context, agenttool.Call) (agenttool.Result, error) {
			n++
			return agenttool.Result{Output: openresponses.FunctionCallOutputData{Parts: openresponses.Contents{
				&openresponses.InputImage{ImageURL: pngURL(screenshots[n%2])},
			}}}, nil
		})
	childModel := &shooter{}
	browse := agent.New(agentturn.Config{Name: "task", Description: "browses", Model: childModel, ModelName: "m", Tools: []agenttool.Tool{shot}},
		agent.WithObserver(rec.Observe), agent.WithRunContext(rec.ChildContext))
	a := agentturn.New(agentturn.Config{Model: &twoCalls{calls: []string{`{"input":"browse"}`}}, Tools: []agenttool.Tool{browse}})
	defer rec.Attach(a)()
	if end, err := a.Prompt(ctx, openresponses.UserText("go")); err != nil || end.Reason != agentturn.ReasonDone {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	if childModel.images == 0 || childModel.notURL != 0 {
		t.Errorf("the child's model was sent %d images, %d of them not a data URL", childModel.images, childModel.notURL)
	}
	l := links(s)
	if len(l) != 1 {
		t.Fatalf("links = %+v", l)
	}
	child, err := store.Open(ctx, l[0].Session)
	if err != nil {
		t.Fatal(err)
	}
	verifyAll(t, child)
	held, err := agentsession.SessionBlobs(child)
	if err != nil {
		t.Fatal(err)
	}
	if len(held) != 2 || blobs.puts != 2 {
		t.Errorf("the child names %d blobs and %d were put through WithMedia's writer, want 2 and 2", len(held), blobs.puts)
	}
	for _, e := range child.Entries() {
		if line, _ := agentsession.MarshalEntry(e); bytes.Contains(line, []byte("data:image")) {
			t.Errorf("child %s entry holds an inline image", e.EntryType())
		}
	}
}

// TestEveryInlineMediaFormIsStoredAsABlob pins the rest of #284's rule:
// every inline medium an item can carry is stored as a blob in a sidecar
// session, a video and a file URL as well as an image and file data, a
// data URL that is not base64 as well as one that is, and media in an
// item type the library does not model, a computer_call_output's
// screenshot, as well as in a message.
func TestEveryInlineMediaFormIsStoredAsABlob(t *testing.T) {
	ctx := context.Background()
	store := &countingBlobs{MemoryStore: agentsession.NewMemoryStore()}
	rec, s, err := Start(ctx, store, agentsession.Header{Media: agentsession.MediaSidecar})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: answers{}, ModelName: "m"})
	defer rec.Attach(a)()
	screenshot := &openresponses.UnknownItem{Type: "computer_call_output", Raw: []byte(`{"type":"computer_call_output","call_id":"cu_1","output":{"type":"computer_screenshot","image_url":"` + pngURL([]byte("\x89PNG desktop")) + `"}}`)}
	prompt := openresponses.UserMessage(
		&openresponses.InputText{Text: "all of these"},
		&openresponses.InputVideo{VideoURL: "data:video/mp4;base64," + base64.StdEncoding.EncodeToString([]byte("a video"))},
		&openresponses.InputFile{Filename: "a.pdf", FileURL: "data:application/pdf;base64," + base64.StdEncoding.EncodeToString([]byte("%PDF a file"))},
		&openresponses.InputImage{ImageURL: "data:image/svg+xml,%3Csvg%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%2F%3E"},
	)
	if end, err := a.Prompt(ctx, screenshot, prompt); err != nil || end.Reason != agentturn.ReasonDone {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	verifyAll(t, s)
	for _, e := range s.Entries() {
		line, err := agentsession.MarshalEntry(e)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(line, []byte(`"data:`)) {
			t.Errorf("%s entry holds inline media: %s", e.EntryType(), line)
		}
	}
	if store.puts != 4 {
		t.Errorf("%d blobs were put, want the screenshot, the video, the file and the image", store.puts)
	}
}

// capturedInputs is a model that answers every request with a message
// and keeps each request's input as JSON, served over HTTP as an
// upstream provider's endpoint.
type capturedInputs struct {
	echo.Adapter
	mu   sync.Mutex
	sent []string
}

func (m *capturedInputs) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	data, err := json.Marshal(req.Input)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.sent = append(m.sent, string(data))
	m.mu.Unlock()
	return answers{}.CreateStream(ctx, req, sink)
}

// TestAResumedSidecarSessionSendsItsMediaAsDataURLs is the round 11
// media probe's resume: a sidecar session recorded with a PNG and a
// JPEG in its prompt and a follow-up image queued and left owed, then
// resumed in a new process through AgentOptions and Requeue and
// prompted through openresponses' ClientAdapter against an upstream
// served over HTTP. The recorder stores each blob with its data URL's
// media type and resolves every sidecar: reference it hands the agent
// back to a data URL of that type, so the adapter, which refuses a
// sidecar: URL, sends the request, the upstream receives each image as
// it was given, and every response verifies. Over RFC 0003 the
// resuming client reads each blob for the session, as a new principal
// must.
func TestAResumedSidecarSessionSendsItsMediaAsDataURLs(t *testing.T) {
	png, jpeg, owed := []byte("\x89PNG the screen"), []byte("\xff\xd8\xff a photo"), []byte("\x89PNG queued later")
	jpegURL := "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(jpeg)
	stores := map[string]func(t *testing.T) (first, second agentsession.Store){
		"memory": func(*testing.T) (agentsession.Store, agentsession.Store) {
			st := agentsession.NewMemoryStore()
			return st, st
		},
		"remote": func(t *testing.T) (agentsession.Store, agentsession.Store) {
			h := remote.NewHandler(agentsession.NewMemoryStore(), remote.WithInsecureNoAuth())
			srv := httptest.NewServer(h)
			t.Cleanup(func() {
				srv.CloseClientConnections()
				srv.Close()
				h.Close()
			})
			dial := func() agentsession.Store {
				c, err := remote.Dial(srv.URL)
				if err != nil {
					t.Fatal(err)
				}
				return c
			}
			return dial(), dial()
		},
	}
	for name, open := range stores {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			first, second := open(t)
			rec, s, err := Start(ctx, first, agentsession.Header{Media: agentsession.MediaSidecar})
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(agentturn.Config{Model: answers{}, ModelName: "m"})
			detach := rec.Attach(a)
			prompt := openresponses.UserMessage(&openresponses.InputText{Text: "compare these"},
				&openresponses.InputImage{ImageURL: pngURL(png)}, &openresponses.InputImage{ImageURL: jpegURL})
			if end, err := a.Prompt(ctx, prompt); err != nil || end.Reason != agentturn.ReasonDone {
				t.Fatalf("prompt: err=%v end=%+v", err, end)
			}
			// An input queued while the agent is idle, which the path owes
			// when the process ends.
			if err := rec.Queue(ctx, a, agentturn.QueueFollowUp, openresponses.UserMessage(
				&openresponses.InputText{Text: "and this"}, &openresponses.InputImage{ImageURL: pngURL(owed)})); err != nil {
				t.Fatal(err)
			}
			detach()
			// The process ends, and with it the first client's lease.
			if c, ok := first.(io.Closer); ok && first != second {
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
			}

			// A new process.
			rec2, s2, err := Resume(ctx, second, s.ID())
			if err != nil {
				t.Fatal(err)
			}
			opts, err := AgentOptions(s2, rec2.ReadOptions()...)
			if err != nil {
				t.Fatal(err)
			}
			model := &capturedInputs{}
			upstream := httptest.NewServer(openresponses.NewHandler(model))
			defer upstream.Close()
			b := agentturn.New(agentturn.Config{Model: openresponses.NewClient(upstream.URL).AsAdapter(), ModelName: "m"}, opts...)
			defer rec2.Attach(b)()
			if n, _ := rec2.Requeue(ctx, b); n != 1 {
				t.Errorf("Requeue handed back %d inputs, want the owed follow-up", n)
			}
			if end, err := b.Prompt(ctx, openresponses.UserText("again")); err != nil || end.Reason != agentturn.ReasonDone {
				t.Fatalf("prompt after the resume: err=%v end=%+v", err, end)
			}
			if len(model.sent) == 0 {
				t.Fatal("the upstream received no request")
			}
			for _, want := range []string{pngURL(png), jpegURL, pngURL(owed)} {
				if !strings.Contains(strings.Join(model.sent, "\n"), want) {
					t.Errorf("the upstream never received %.40s…", want)
				}
			}
			for k, sent := range model.sent {
				if strings.Contains(sent, "sidecar:") {
					t.Errorf("request %d sent a sidecar: reference: %s", k, sent)
				}
			}
			s3, err := second.Open(ctx, s.ID())
			if err != nil {
				t.Fatal(err)
			}
			if n := verifyAll(t, s3); n < 3 {
				t.Errorf("responses = %d, want the first run's and the resumed run's", n)
			}
			for _, e := range s3.Entries() {
				if line, _ := agentsession.MarshalEntry(e); bytes.Contains(line, []byte(`"data:`)) {
					t.Errorf("%s entry holds inline media: %s", e.EntryType(), line)
				}
			}
		})
	}
}

// TestASidecarBlobWithNoTypeResolvesAsOctetStream pins the rest of the
// resume rule: a blob held with no media type, as one a writer that
// keeps none stored, comes back as an application/octet-stream data
// URL, and a sidecar session's items are not handed back at all with
// no blob reader to resolve them.
func TestASidecarBlobWithNoTypeResolvesAsOctetStream(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	// putCounter is no TypedBlobWriter, so the type is left behind.
	rec, s, err := Start(ctx, store, agentsession.Header{Media: agentsession.MediaSidecar}, WithMedia(&putCounter{BlobWriter: store}))
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: answers{}, ModelName: "m"})
	detach := rec.Attach(a)
	shot := []byte("\x89PNG untyped")
	if _, err := a.Prompt(ctx, openresponses.UserMessage(&openresponses.InputImage{ImageURL: pngURL(shot)})); err != nil {
		t.Fatal(err)
	}
	detach()
	rec2, s2, err := Resume(ctx, store, s.ID(), WithMedia(&putCounter{BlobWriter: store}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Transcript(s2); !errors.Is(err, ErrNoBlobReader) {
		t.Errorf("Transcript with no blob reader: err = %v, want ErrNoBlobReader", err)
	}
	items, err := Transcript(s2, rec2.ReadOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	want := "data:application/octet-stream;base64," + base64.StdEncoding.EncodeToString(shot)
	if got, _ := json.Marshal(items[0]); !strings.Contains(string(got), want) {
		t.Errorf("the prompt came back as %s, want its image as %s", got, want)
	}
}
