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
