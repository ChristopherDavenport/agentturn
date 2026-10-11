package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	fronta2a "github.com/ChristopherDavenport/agentturn/front/a2a"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
	"github.com/a2aproject/a2a-go/a2asrv"
	"github.com/a2aproject/a2a-go/a2asrv/eventqueue"
)

// records collects what a call writes through agenttool.WriteRecord,
// with the call each was filed under.
type records struct {
	mu   sync.Mutex
	got  []*agenttool.Record
	call []string
}

func (r *records) fn(ctx context.Context, rec *agenttool.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, rec)
	c, _ := agenttool.CallFrom(ctx)
	r.call = append(r.call, c.ID)
	return nil
}

func (r *records) of(ns string) []json.RawMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []json.RawMessage
	for _, rec := range r.got {
		if rec.NS == ns {
			out = append(out, rec.Data)
		}
	}
	return out
}

// TestACallRecordsItsCallee pins step 6's report: a call writes a
// Callee record naming the URL it called, the task and its context, and
// the relation, subsession by default and peer without the cancel, under
// the call it belongs to, streaming and blocking alike.
func TestACallRecordsItsCallee(t *testing.T) {
	remoteCfg := agentturn.Config{Name: "Echo", Model: &echo.Adapter{}, ModelName: "remote"}
	client, card := serve(t, fronta2a.New(remoteCfg), nil)
	for _, tc := range []struct {
		name string
		opts []Option
		rel  string
	}{
		{"streaming", nil, RelationSubsession},
		{"blocking", []Option{WithBlocking()}, RelationSubsession},
		{"peer", []Option{WithoutCancelOnAbort()}, RelationPeer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var recs records
			remote := New(client, card, tc.opts...)
			local := agentturn.Config{Model: callsOnce{remote.Name()}, ModelName: "local", Tools: []agenttool.Tool{remote}, ToolRecorder: recs.fn}
			var callID string
			for ev := range agentturn.Run(context.Background(), nil, openresponses.Items{openresponses.UserText("hello")}, local) {
				if e, ok := ev.(*agentturn.ToolEnd); ok {
					callID = e.CallID
					if e.Err != nil {
						t.Fatalf("tool_end err = %v", e.Err)
					}
				}
			}
			got := recs.of(CalleeNS)
			if len(got) != 1 {
				t.Fatalf("callee records = %d, want 1: %s", len(got), got)
			}
			var c Callee
			if err := json.Unmarshal(got[0], &c); err != nil {
				t.Fatal(err)
			}
			if c.URL != card.URL || c.Task == "" || c.Context == "" || c.Relation != tc.rel {
				t.Errorf("callee = %+v, want url %q relation %q", c, card.URL, tc.rel)
			}
			if recs.call[0] != callID || callID == "" {
				t.Errorf("record filed under %q, want the call %q", recs.call[0], callID)
			}
			if n := len(recs.of(CancelNS)); n != 0 {
				t.Errorf("cancel records = %d for a call that completed", n)
			}
		})
	}
}

// callsOnce calls name once and then echoes.
type callsOnce struct{ name string }

func (m callsOnce) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	for _, item := range req.Input {
		if _, ok := item.(*openresponses.FunctionCallOutput); ok {
			return (&echo.Adapter{}).CreateStream(ctx, req, sink)
		}
	}
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	w, err := em.FunctionCall("", m.name)
	if err != nil {
		return err
	}
	if err := w.Arguments(`{"input":"do it"}`); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Complete()
}

// stalling is a remote agent whose task works until it is cancelled,
// and whose cancel takes as long as the test says.
type stalling struct {
	cancelled chan a2a.TaskID
	release   chan struct{}
	done      chan struct{}
	once      sync.Once
}

func newStalling() *stalling {
	return &stalling{cancelled: make(chan a2a.TaskID, 4), release: make(chan struct{}), done: make(chan struct{})}
}

func (s *stalling) Execute(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue) error {
	if err := q.Write(ctx, a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateWorking, nil)); err != nil {
		return err
	}
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
	}
	return nil
}

func (s *stalling) Cancel(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue) error {
	s.cancelled <- reqCtx.TaskID
	<-s.release
	s.once.Do(func() { close(s.done) })
	ev := a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateCanceled, nil)
	ev.Final = true
	return q.Write(ctx, ev)
}

// TestAnAbortedCallCancelsItsTaskWithoutWaiting pins the cancel: a call
// whose context ends sends tasks/cancel for its task and returns at
// once, while the remote agent has not answered the cancel, and records
// that the cancel was sent; a peer's task gets no cancel and no record.
func TestAnAbortedCallCancelsItsTaskWithoutWaiting(t *testing.T) {
	for _, peer := range []bool{false, true} {
		name := "subsession"
		var opts []Option
		if peer {
			name = "peer"
			opts = append(opts, WithoutCancelOnAbort())
		}
		t.Run(name, func(t *testing.T) {
			remote := newStalling()
			defer func() {
				close(remote.release)
				remote.once.Do(func() { close(remote.done) })
			}()
			client, card := serve(t, remote, nil)
			tool := New(client, card, opts...)
			var recs records
			ctx, cancel := context.WithCancel(agenttool.ContextWithRecorder(context.Background(), recs.fn))
			ret := make(chan error, 1)
			go func() {
				_, err := tool.Execute(ctx, agenttool.Call{ID: "c1", Args: json.RawMessage(`{"input":"work"}`)})
				ret <- err
			}()
			deadline := time.After(5 * time.Second)
			for len(recs.of(CalleeNS)) == 0 {
				select {
				case <-deadline:
					t.Fatal("no callee record before the abort")
				case <-time.After(10 * time.Millisecond):
				}
			}
			var c Callee
			_ = json.Unmarshal(recs.of(CalleeNS)[0], &c)
			cancel()
			select {
			case err := <-ret:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("err = %v, want context.Canceled", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("the call waited on the remote agent's cancel")
			}
			if peer {
				select {
				case id := <-remote.cancelled:
					t.Errorf("a peer's task %q was cancelled", id)
				case <-time.After(300 * time.Millisecond):
				}
				if n := len(recs.of(CancelNS)); n != 0 {
					t.Errorf("cancel records = %d for a peer", n)
				}
				return
			}
			select {
			case id := <-remote.cancelled:
				if string(id) != c.Task {
					t.Errorf("cancelled %q, want the call's task %q", id, c.Task)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the remote agent saw no tasks/cancel")
			}
			sent := recs.of(CancelNS)
			if len(sent) != 1 {
				t.Fatalf("cancel records = %d, want 1", len(sent))
			}
			var cs CancelSent
			_ = json.Unmarshal(sent[0], &cs)
			if cs.Task != c.Task || cs.URL != card.URL {
				t.Errorf("cancel record = %+v, want task %q url %q", cs, c.Task, card.URL)
			}
		})
	}
}

// asking is a remote agent that asks for a colour on the first message
// of a task and answers with it on the next.
type asking struct{}

func (asking) Execute(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue) error {
	if reqCtx.StoredTask == nil {
		ev := a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateInputRequired, a2a.NewMessageForTask(a2a.MessageRoleAgent, reqCtx, a2a.TextPart{Text: "which colour?"}))
		ev.Final = true
		return q.Write(ctx, ev)
	}
	ev := a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateCompleted, a2a.NewMessageForTask(a2a.MessageRoleAgent, reqCtx, a2a.TextPart{Text: "painted " + partsText(reqCtx.Message.Parts)}))
	ev.Final = true
	return q.Write(ctx, ev)
}

func (asking) Cancel(context.Context, *a2asrv.RequestContext, eventqueue.Queue) error { return nil }

// TestTheInputRequiredHookAnswersOnTheSameTask pins WithInputRequired:
// the remote agent's question goes to the hook, its answer is sent on the
// same task, and the call completes in one; without the hook, or when the
// hook cannot answer, the call ends input-required as before.
func TestTheInputRequiredHookAnswersOnTheSameTask(t *testing.T) {
	client, card := serve(t, asking{}, nil)
	for _, mode := range []string{"streaming", "blocking"} {
		t.Run(mode, func(t *testing.T) {
			var opts []Option
			if mode == "blocking" {
				opts = append(opts, WithBlocking())
			}
			var asked []string
			var tasks []a2a.TaskID
			hook := func(_ context.Context, info TaskInfo, q *a2a.Message) ([]a2a.Part, error) {
				asked = append(asked, partsText(q.Parts))
				tasks = append(tasks, info.TaskID)
				return []a2a.Part{a2a.TextPart{Text: "blue"}}, nil
			}
			var recs records
			ctx := agenttool.ContextWithRecorder(context.Background(), recs.fn)
			res, err := New(client, card, append(opts, WithInputRequired(hook))...).Execute(ctx, agenttool.Call{ID: "c1", Args: json.RawMessage(`{"input":"paint"}`)})
			if err != nil || res.Output.Text != "painted blue" {
				t.Fatalf("result = %+v err = %v", res.Output, err)
			}
			if len(asked) != 1 || asked[0] != "which colour?" || tasks[0] == "" {
				t.Errorf("hook asked %q about %v", asked, tasks)
			}
			if info := res.Details.(TaskInfo); info.TaskID != tasks[0] {
				t.Errorf("answered on task %q, asked on %q", info.TaskID, tasks[0])
			}
			if n := len(recs.of(CalleeNS)); n != 1 {
				t.Errorf("callee records = %d, want 1 for one task", n)
			}

			_, err = New(client, card, opts...).Execute(context.Background(), agenttool.Call{ID: "c2", Args: json.RawMessage(`{"input":"paint"}`)})
			var ir *InputRequiredError
			if !errors.As(err, &ir) || ir.Message != "which colour?" {
				t.Errorf("without a hook err = %v", err)
			}
			declining := func(context.Context, TaskInfo, *a2a.Message) ([]a2a.Part, error) {
				return nil, agentturn.ErrInputRequired
			}
			_, err = New(client, card, append(opts, WithInputRequired(declining))...).Execute(context.Background(), agenttool.Call{ID: "c3", Args: json.RawMessage(`{"input":"paint"}`)})
			if !errors.As(err, &ir) {
				t.Errorf("with a hook that cannot answer err = %v", err)
			}
		})
	}
}

// working is a remote agent whose task works until it is cancelled.
type working struct{ cancelled chan a2a.TaskID }

func (working) Execute(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue) error {
	return q.Write(ctx, a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateWorking, nil))
}

func (s working) Cancel(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue) error {
	s.cancelled <- reqCtx.TaskID
	ev := a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateCanceled, nil)
	ev.Final = true
	return q.Write(ctx, ev)
}

// TestACallThatEndsBeforeItsTaskCancelsIt pins the early end: a call
// that returns while its task still works, here a blocking call over a
// client that polls, which answers with the task as it stands, ends and
// stops the task as an abort does, the Callee recorded before the
// CancelSent.
func TestACallThatEndsBeforeItsTaskCancelsIt(t *testing.T) {
	remote := working{cancelled: make(chan a2a.TaskID, 4)}
	_, card := serve(t, remote, nil)
	client, err := a2aclient.NewFromCard(context.Background(), card, a2aclient.WithConfig(a2aclient.Config{Polling: true}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Destroy() })
	var recs records
	ctx := agenttool.ContextWithRecorder(context.Background(), recs.fn)
	if _, err := New(client, card, WithBlocking()).Execute(ctx, agenttool.Call{ID: "c1", Args: json.RawMessage(`{"input":"work"}`)}); err == nil {
		t.Fatal("a call whose task never ended succeeded")
	}
	select {
	case <-remote.cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("the remote agent saw no tasks/cancel")
	}
	recs.mu.Lock()
	var order []string
	for _, r := range recs.got {
		order = append(order, r.NS)
	}
	recs.mu.Unlock()
	if len(order) != 2 || order[0] != CalleeNS || order[1] != CancelNS {
		t.Errorf("records = %v, want the callee then the cancel", order)
	}
}

// TestTheEndpointIsTheOneCalled pins the URL: with ObserveEndpoint on
// the client the record names the endpoint the client sent to, and New
// leaves the client as it was; without it the record names no URL
// rather than one the client may not have called.
func TestTheEndpointIsTheOneCalled(t *testing.T) {
	remoteCfg := agentturn.Config{Name: "Echo", Model: &echo.Adapter{}, ModelName: "remote"}
	observed, card := serve(t, fronta2a.New(remoteCfg), nil)
	plain, err := a2aclient.NewFromCard(context.Background(), card)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plain.Destroy() })
	for _, tc := range []struct {
		name   string
		client *a2aclient.Client
		url    string
	}{
		{"observed", observed, card.URL},
		{"unobserved", plain, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var recs records
			ctx := agenttool.ContextWithRecorder(context.Background(), recs.fn)
			if _, err := New(tc.client, card).Execute(ctx, agenttool.Call{ID: "c1", Args: json.RawMessage(`{"input":"hi"}`)}); err != nil {
				t.Fatal(err)
			}
			var c Callee
			_ = json.Unmarshal(recs.of(CalleeNS)[0], &c)
			if c.URL != tc.url {
				t.Errorf("url = %q, want %q", c.URL, tc.url)
			}
		})
	}
}

// pestering asks on every message, with no status message after the
// first, so a hook answers until the bound.
type pestering struct{}

func (pestering) Execute(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue) error {
	var msg *a2a.Message
	if reqCtx.StoredTask == nil {
		msg = a2a.NewMessageForTask(a2a.MessageRoleAgent, reqCtx, a2a.TextPart{Text: "more?"})
	}
	ev := a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateInputRequired, msg)
	ev.Final = true
	return q.Write(ctx, ev)
}

func (pestering) Cancel(context.Context, *a2asrv.RequestContext, eventqueue.Queue) error { return nil }

// TestInputRoundsAreBounded pins the bound and the empty question: a
// hook is asked at most WithInputRounds times, then the call ends
// input-required, and a question with no message reaches it as an empty
// one, never nil.
func TestInputRoundsAreBounded(t *testing.T) {
	client, card := serve(t, pestering{}, nil)
	var asked int
	var nilQuestion bool
	hook := func(_ context.Context, _ TaskInfo, q *a2a.Message) ([]a2a.Part, error) {
		asked++
		nilQuestion = nilQuestion || q == nil
		return []a2a.Part{a2a.TextPart{Text: "yes"}}, nil
	}
	_, err := New(client, card, WithInputRequired(hook), WithInputRounds(3)).Execute(context.Background(), agenttool.Call{ID: "c1", Args: json.RawMessage(`{"input":"go"}`)})
	var ir *InputRequiredError
	if !errors.As(err, &ir) || asked != 3 || nilQuestion {
		t.Errorf("err = %v, asked %d times, a nil question %v", err, asked, nilQuestion)
	}
}

// failing is a recorder that cannot write.
func failing(context.Context, *agenttool.Record) error { return errors.New("disk full") }

// TestARecordThatCannotBeWritten pins the record's failure: while the
// task runs it fails the call, which gives the task up; once the remote
// agent has answered, the answer stands and the failure is on the
// result's TaskInfo.
func TestARecordThatCannotBeWritten(t *testing.T) {
	remoteCfg := agentturn.Config{Name: "Echo", Model: &echo.Adapter{}, ModelName: "remote"}
	client, card := serve(t, fronta2a.New(remoteCfg), nil)
	ctx := agenttool.ContextWithRecorder(context.Background(), failing)
	res, err := New(client, card, WithBlocking()).Execute(ctx, agenttool.Call{ID: "c1", Args: json.RawMessage(`{"input":"hi"}`)})
	if err != nil || res.Output.Text != "hi" || res.Details.(TaskInfo).RecordErr == nil {
		t.Errorf("blocking: %+v %v", res, err)
	}
	remote := newStalling()
	defer func() {
		close(remote.release)
		remote.once.Do(func() { close(remote.done) })
	}()
	stalled, stalledCard := serve(t, remote, nil)
	if _, err := New(stalled, stalledCard).Execute(ctx, agenttool.Call{ID: "c2", Args: json.RawMessage(`{"input":"work"}`)}); err == nil {
		t.Error("streaming: a call whose record failed while the task ran succeeded")
	}
	select {
	case <-remote.cancelled:
	case <-time.After(5 * time.Second):
		t.Error("streaming: the task was not given up")
	}
}

// messaging asks on the first message of a task and answers the next
// with a plain message, not a task.
type messaging struct{ cancelled chan a2a.TaskID }

func (messaging) Execute(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue) error {
	if reqCtx.StoredTask == nil {
		ev := a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateInputRequired, a2a.NewMessageForTask(a2a.MessageRoleAgent, reqCtx, a2a.TextPart{Text: "which?"}))
		ev.Final = true
		return q.Write(ctx, ev)
	}
	return q.Write(ctx, a2a.NewMessageForTask(a2a.MessageRoleAgent, reqCtx, a2a.TextPart{Text: "done"}))
}

func (s messaging) Cancel(_ context.Context, reqCtx *a2asrv.RequestContext, _ eventqueue.Queue) error {
	s.cancelled <- reqCtx.TaskID
	return nil
}

// TestAMessageAnswerAfterAnInputRoundCancelsNothing pins that a call
// whose answered round ends in a message has nothing left running to
// cancel.
func TestAMessageAnswerAfterAnInputRoundCancelsNothing(t *testing.T) {
	remote := messaging{cancelled: make(chan a2a.TaskID, 4)}
	client, card := serve(t, remote, nil)
	hook := func(context.Context, TaskInfo, *a2a.Message) ([]a2a.Part, error) {
		return []a2a.Part{a2a.TextPart{Text: "that one"}}, nil
	}
	var recs records
	ctx := agenttool.ContextWithRecorder(context.Background(), recs.fn)
	res, err := New(client, card, WithBlocking(), WithInputRequired(hook)).Execute(ctx, agenttool.Call{ID: "c1", Args: json.RawMessage(`{"input":"go"}`)})
	if err != nil || res.Output.Text != "done" {
		t.Fatalf("result = %+v err = %v", res.Output, err)
	}
	select {
	case id := <-remote.cancelled:
		t.Errorf("task %q was cancelled after a message answer", id)
	case <-time.After(300 * time.Millisecond):
	}
	if n := len(recs.of(CancelNS)); n != 0 {
		t.Errorf("cancel records = %d", n)
	}
}

// flaky fails its first write and takes the rest.
type flaky struct {
	records
	failed bool
}

func (f *flaky) fn(ctx context.Context, rec *agenttool.Record) error {
	f.mu.Lock()
	first := !f.failed
	f.failed = true
	f.mu.Unlock()
	if first {
		return errors.New("busy")
	}
	return f.records.fn(ctx, rec)
}

// TestACalleeRecordThatFailedIsWrittenAgain pins that a Callee whose
// write failed is not marked written: the cancel that follows writes it
// before the CancelSent.
func TestACalleeRecordThatFailedIsWrittenAgain(t *testing.T) {
	remote := newStalling()
	defer func() {
		close(remote.release)
		remote.once.Do(func() { close(remote.done) })
	}()
	client, card := serve(t, remote, nil)
	var f flaky
	ctx := agenttool.ContextWithRecorder(context.Background(), f.fn)
	if _, err := New(client, card).Execute(ctx, agenttool.Call{ID: "c1", Args: json.RawMessage(`{"input":"work"}`)}); err == nil {
		t.Fatal("a call whose record failed while the task ran succeeded")
	}
	f.mu.Lock()
	var order []string
	for _, r := range f.got {
		order = append(order, r.NS)
	}
	f.mu.Unlock()
	if len(order) != 2 || order[0] != CalleeNS || order[1] != CancelNS {
		t.Errorf("records = %v, want the callee written again, then the cancel", order)
	}
}
