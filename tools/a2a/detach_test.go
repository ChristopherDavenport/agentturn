package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	fronta2a "github.com/ChristopherDavenport/agentturn/front/a2a"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
	"github.com/a2aproject/a2a-go/a2asrv"
	"github.com/a2aproject/a2a-go/a2asrv/eventqueue"
)

// gated is a remote agent whose task works until the gate named by its
// message's text opens, then completes with an artifact saying so, or
// is cancelled.
type gated struct {
	mu        sync.Mutex
	gates     map[string]chan struct{}
	cancelled chan a2a.TaskID
}

func newGated(names ...string) *gated {
	g := &gated{gates: map[string]chan struct{}{}, cancelled: make(chan a2a.TaskID, 4)}
	for _, n := range names {
		g.gates[n] = make(chan struct{})
	}
	return g
}

func (g *gated) open(name string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	close(g.gates[name])
}

func (g *gated) Execute(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue) error {
	name := partsText(reqCtx.Message.Parts)
	g.mu.Lock()
	gate := g.gates[name]
	g.mu.Unlock()
	if err := q.Write(ctx, a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateWorking, nil)); err != nil {
		return err
	}
	select {
	case <-gate:
	case <-ctx.Done():
		return nil
	case <-time.After(8 * time.Second):
		// A test that failed before opening the gate: the task ends,
		// so its stream closes.
		ev := a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateFailed, nil)
		ev.Final = true
		return q.Write(ctx, ev)
	}
	if err := q.Write(ctx, a2a.NewArtifactEvent(reqCtx, a2a.TextPart{Text: name + " finished"})); err != nil {
		return err
	}
	ev := a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateCompleted, nil)
	ev.Final = true
	return q.Write(ctx, ev)
}

func (g *gated) Cancel(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue) error {
	g.cancelled <- reqCtx.TaskID
	ev := a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateCanceled, nil)
	ev.Final = true
	return q.Write(ctx, ev)
}

// callsAll calls every tool it is offered once, each with its own name
// as the input, and answers without a call once any output is in.
type callsAll struct{}

func (callsAll) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	for _, item := range req.Input {
		if _, ok := item.(*openresponses.FunctionCallOutput); ok {
			w, err := em.Message(openresponses.PhaseFinalAnswer)
			if err != nil {
				return err
			}
			if err := w.Text("noted"); err != nil {
				return err
			}
			if err := w.Close(); err != nil {
				return err
			}
			return em.Complete()
		}
	}
	for _, tl := range req.Tools {
		name := tl.(*openresponses.FunctionTool).Name
		w, err := em.FunctionCall("", name)
		if err != nil {
			return err
		}
		args, _ := json.Marshal(Args{Input: name})
		if err := w.Arguments(string(args)); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
	}
	return em.Complete()
}

// TestDetachedCallsLeaveTheBatch pins #290: two detached remote calls
// in one batch return once their tasks run, the batch and the run end
// before either task does, the callee is recorded as a peer, and each
// task's result reaches the parent through the agent's delivery, under
// the callee trigger naming its call and task, streaming and blocking
// alike.
func TestDetachedCallsLeaveTheBatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
	}{
		{name: "streaming"},
		{name: "blocking", opts: []Option{WithBlocking(), WithPollInterval(MinPollInterval)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remote := newGated("lint", "test")
			client, card := serve(t, remote, nil)
			var parent *agentturn.Agent
			type delivered struct {
				callID string
				info   TaskInfo
				err    error
			}
			got := make(chan delivered, 2)
			detach := func(callID string, info TaskInfo, final *a2a.Task, err error) {
				text := Answer(final).Text
				if err != nil {
					text = err.Error()
				}
				ctx := agentturn.ContextWithTrigger(context.Background(), agentturn.CalleeTrigger(callID, string(info.TaskID)))
				_, _, derr := parent.Deliver(ctx, openresponses.UserText("from "+callID+": "+text))
				got <- delivered{callID, info, errors.Join(err, derr)}
			}
			var recs records
			tools := []agenttool.Tool{
				New(client, card, append(tc.opts, WithName("lint"), WithDetach(detach))...),
				New(client, card, append(tc.opts, WithName("test"), WithDetach(detach))...),
			}
			parent = agentturn.New(agentturn.Config{Model: callsAll{}, ModelName: "local", Tools: tools, ToolRecorder: recs.fn})
			var triggers []agentturn.Trigger
			var mu sync.Mutex
			parent.Subscribe(func(_ context.Context, ev agentturn.Event) error {
				if e, ok := ev.(*agentturn.TurnStart); ok {
					mu.Lock()
					for _, in := range e.Arrived {
						if in.Mode == agentturn.InputDeliver {
							triggers = append(triggers, in.Trigger)
						}
					}
					mu.Unlock()
				}
				return nil
			})
			type prompted struct {
				end *agentturn.RunEnd
				err error
			}
			first := make(chan prompted, 1)
			go func() {
				end, err := parent.Prompt(context.Background(), openresponses.UserText("lint and test"))
				first <- prompted{end, err}
			}()
			var end *agentturn.RunEnd
			select {
			case p := <-first:
				if p.err != nil || p.end.Reason != agentturn.ReasonDone {
					t.Fatalf("first run: %v, %+v", p.err, p.end)
				}
				end = p.end
			case <-time.After(5 * time.Second):
				remote.open("lint")
				remote.open("test")
				t.Fatal("the batch waited on the remote tasks")
			}
			calls := map[string]string{}
			for _, item := range end.Items {
				switch v := item.(type) {
				case *openresponses.FunctionCall:
					calls[v.CallID] = v.Name
				case *openresponses.FunctionCallOutput:
					if !strings.Contains(v.Output.Text, "working on it in the background") {
						t.Errorf("output of %s = %q, want it running", v.CallID, v.Output.Text)
					}
				}
			}
			if len(calls) != 2 {
				t.Fatalf("calls = %v", calls)
			}
			for _, raw := range recs.of(CalleeNS) {
				var c Callee
				_ = json.Unmarshal(raw, &c)
				if c.Relation != RelationPeer || c.Task == "" {
					t.Errorf("callee = %+v, want a peer's task", c)
				}
			}
			for _, name := range []string{"lint", "test"} {
				remote.open(name)
				select {
				case d := <-got:
					if d.err != nil {
						t.Errorf("%s: %v", calls[d.callID], d.err)
					}
					if calls[d.callID] != name || d.info.State != a2a.TaskStateCompleted {
						t.Errorf("delivered %s %+v, want %s completed", calls[d.callID], d.info, name)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("%s's result never arrived", name)
				}
			}
			var texts []string
			for _, item := range parent.State().Transcript {
				if m, ok := item.(*openresponses.Message); ok && m.Role == openresponses.RoleUser {
					texts = append(texts, m.Text())
				}
			}
			for id, name := range calls {
				if !strings.Contains(strings.Join(texts, "\n"), "from "+id+": "+name+" finished") {
					t.Errorf("the parent never read %s's result: %q", name, texts)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if len(triggers) != 2 {
				t.Fatalf("delivered inputs = %+v", triggers)
			}
			for _, tr := range triggers {
				if tr.Kind != agentturn.TriggerCallee || calls[tr.Ref] == "" || tr.Extra["task"] == "" {
					t.Errorf("delivery trigger = %+v, want the callee of a call", tr)
				}
			}
		})
	}
}

// TestAnAbortCutsADetachedCall pins the cut: an abort of the parent's
// run while it still runs cancels the detached call's task, records the
// cancel after the peer's callee, and hands fn the abort; the run
// ending by itself, as above, does not.
func TestAnAbortCutsADetachedCall(t *testing.T) {
	remote := newGated("lint")
	client, card := serve(t, remote, nil)
	ends := make(chan error, 1)
	tool := New(client, card, WithName("lint"), WithDetach(func(_ string, _ TaskInfo, _ *a2a.Task, err error) { ends <- err }))
	started := make(chan struct{})
	hold := agenttool.New("hold", "", func(ctx context.Context, _ struct{}) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	})
	var recs records
	parent := agentturn.New(agentturn.Config{Model: callsAll{}, ModelName: "local", Tools: []agenttool.Tool{tool, hold}, ToolExecution: agentturn.ExecSequential, ToolRecorder: recs.fn})
	stop := errors.New("stop everything")
	go func() {
		<-started
		parent.AbortCause(stop)
	}()
	if _, err := parent.Prompt(context.Background(), openresponses.UserText("lint")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-ends:
		if !errors.Is(err, context.Canceled) && !errors.Is(err, stop) {
			t.Errorf("fn got %v, want the abort", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the abort did not reach the detached call")
	}
	select {
	case <-remote.cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("the remote agent saw no tasks/cancel")
	}
	if n := len(recs.of(CancelNS)); n != 1 {
		t.Errorf("cancel records = %d, want 1", n)
	}
}

// worksThenReads calls work until an output is in, then answers with
// the user's messages it was sent.
type worksThenReads struct{}

func (worksThenReads) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	var users []string
	answered := false
	for _, item := range req.Input {
		switch v := item.(type) {
		case *openresponses.Message:
			if v.Role == openresponses.RoleUser {
				users = append(users, v.Text())
			}
		case *openresponses.FunctionCallOutput:
			answered = true
		}
	}
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if !answered {
		w, err := em.FunctionCall("", "work")
		if err != nil {
			return err
		}
		if err := w.Arguments(`{}`); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		return em.Complete()
	}
	w, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if err := w.Text("read: " + strings.Join(users, " | ")); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Complete()
}

// TestSteerAWorkingTask pins the consume side of #287 with #290's
// handle: Steer sends to the task a detached call follows, a front
// under WithDeliverWhileBusy hands it to the working agent, and the
// task's answer, which reaches fn, read it.
func TestSteerAWorkingTask(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	work := agenttool.New("work", "works until released", func(ctx context.Context, _ struct{}) (string, error) {
		close(entered)
		<-release
		return "worked", nil
	})
	exec := fronta2a.New(agentturn.Config{Name: "Worker", Model: worksThenReads{}, Tools: []agenttool.Tool{work}}, fronta2a.WithDeliverWhileBusy())
	srv := httptest.NewServer(a2asrv.NewJSONRPCHandler(exec.RequestHandler()))
	t.Cleanup(srv.Close)
	card := &a2a.AgentCard{Name: "Worker", URL: srv.URL, PreferredTransport: a2a.TransportProtocolJSONRPC, Capabilities: a2a.AgentCapabilities{Streaming: true}}
	client, err := a2aclient.NewFromCard(context.Background(), card, a2aclient.WithInterceptors(ObserveEndpoint()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Destroy() })
	// A test that fails before releasing the tool releases it as it
	// ends, so the server can close.
	var releaseOnce sync.Once
	releaseWork := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseWork)
	ends := make(chan *a2a.Task, 1)
	tool := New(client, card, WithDetach(func(_ string, _ TaskInfo, final *a2a.Task, err error) {
		if err != nil {
			t.Errorf("the task ended with %v", err)
		}
		ends <- final
	}))
	res, err := tool.Execute(context.Background(), agenttool.Call{ID: "c1", Args: json.RawMessage(`{"input":"lint the repo"}`)})
	if err != nil {
		t.Fatal(err)
	}
	info := res.Details.(TaskInfo)
	<-entered
	task, err := Steer(context.Background(), client, info, a2a.TextPart{Text: "also check the cache"})
	if err != nil {
		t.Fatalf("steer: %v", err)
	}
	if task.ID != info.TaskID || task.Status.State.Terminal() {
		t.Errorf("the steer was answered with %s %s, want the working task %s", task.ID, task.Status.State, info.TaskID)
	}
	releaseWork()
	select {
	case final := <-ends:
		if got := Answer(final).Text; got != "read: lint the repo | also check the cache" {
			t.Errorf("answer = %q, want it to read the steer", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the task never ended")
	}
}

// countingGets counts the tasks/get requests a client sends.
type countingGets struct {
	a2aclient.PassthroughInterceptor
	mu sync.Mutex
	n  int
}

func (c *countingGets) Before(ctx context.Context, req *a2aclient.Request) (context.Context, error) {
	if _, ok := req.Payload.(*a2a.TaskQueryParams); ok {
		c.mu.Lock()
		c.n++
		c.mu.Unlock()
	}
	return ctx, nil
}

func (c *countingGets) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// TestThePollIntervalHasALowerBound pins the review of #290: an
// interval below MinPollInterval is raised to it, so a detached call
// does not ask for its task in a tight loop.
func TestThePollIntervalHasALowerBound(t *testing.T) {
	remote := newGated("lint")
	_, card := serve(t, remote, nil)
	gets := &countingGets{}
	client, err := a2aclient.NewFromCard(context.Background(), card, a2aclient.WithInterceptors(gets))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Destroy() })
	ends := make(chan struct{})
	tool := New(client, card, WithName("lint"), WithBlocking(), WithPollInterval(time.Nanosecond), WithDetach(func(string, TaskInfo, *a2a.Task, error) { close(ends) }))
	if _, err := tool.Execute(context.Background(), agenttool.Call{ID: "c1", Args: json.RawMessage(`{"input":"lint"}`)}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * MinPollInterval)
	n := gets.count()
	remote.open("lint")
	<-ends
	if n > 5 {
		t.Errorf("tasks/get sent %d times in 4 minimum intervals", n)
	}
}

// authLater is a remote agent whose task works until its gate opens,
// then waits on credentials.
type authLater struct{ gate chan struct{} }

func (s authLater) Execute(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue) error {
	if err := q.Write(ctx, a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateWorking, nil)); err != nil {
		return err
	}
	select {
	case <-s.gate:
	case <-ctx.Done():
		return nil
	case <-time.After(8 * time.Second):
	}
	ev := a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateAuthRequired, a2a.NewMessageForTask(a2a.MessageRoleAgent, reqCtx, a2a.TextPart{Text: "sign in"}))
	ev.Final = true
	return q.Write(ctx, ev)
}

func (authLater) Cancel(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue) error {
	ev := a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateCanceled, nil)
	ev.Final = true
	return q.Write(ctx, ev)
}

// TestADetachedCallEndsOnAuthRequired pins the review of #290: a task
// a detached call follows by tasks/get ends the follow once it waits on
// credentials, as a call that is not detached ends there, and fn gets
// the input-required error.
func TestADetachedCallEndsOnAuthRequired(t *testing.T) {
	remote := authLater{gate: make(chan struct{})}
	client, card := serve(t, remote, nil)
	ends := make(chan error, 1)
	tool := New(client, card, WithBlocking(), WithPollInterval(MinPollInterval), WithDetach(func(_ string, info TaskInfo, _ *a2a.Task, err error) {
		if info.State != a2a.TaskStateAuthRequired {
			t.Errorf("fn's task state = %s", info.State)
		}
		ends <- err
	}))
	if _, err := tool.Execute(context.Background(), agenttool.Call{ID: "c1", Args: json.RawMessage(`{"input":"work"}`)}); err != nil {
		t.Fatal(err)
	}
	close(remote.gate)
	select {
	case err := <-ends:
		var ir *InputRequiredError
		if !errors.As(err, &ir) {
			t.Errorf("fn got %v, want the input-required error", err)
		}
	case <-time.After(10 * MinPollInterval):
		t.Fatal("the follow never ended on auth-required")
	}
}
