package session

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
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
