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
	"github.com/ChristopherDavenport/agentturn/handoff"
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
// cancel after the peer's callee, and hands fn the abort, whether the
// task is followed by its stream or by tasks/get; the run ending by
// itself, as above, does not.
func TestAnAbortCutsADetachedCall(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
	}{
		{name: "streaming"},
		{name: "blocking", opts: []Option{WithBlocking(), WithPollInterval(MinPollInterval)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remote := newGated("lint")
			client, card := serve(t, remote, nil)
			ends := make(chan error, 1)
			tool := New(client, card, append(tc.opts, WithName("lint"), WithDetach(func(_ string, _ TaskInfo, _ *a2a.Task, err error) { ends <- err }))...)
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
		})
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

// TestADetachedCallThatNeverDetachesIsAnOrdinaryCall pins the review of
// #290: a detached call that returns before its task runs on returns
// as a call that is not detached does. An answer final at once is
// recorded as the call's own callee, a subsession, not a peer, and a
// call that fails before it returns, here on a callee record it cannot
// write, stops the task it leaves rather than leaving it to run with
// nobody following it.
func TestADetachedCallThatNeverDetachesIsAnOrdinaryCall(t *testing.T) {
	never := WithDetach(func(string, TaskInfo, *a2a.Task, error) { t.Error("fn was called for a call that never detached") })
	t.Run("final at once", func(t *testing.T) {
		client, card := serve(t, scripted{reply: true, message: "hi"}, nil)
		var recs records
		ctx := agenttool.ContextWithRecorder(context.Background(), recs.fn)
		res, err := New(client, card, never).Execute(ctx, agenttool.Call{ID: "c1", Args: json.RawMessage(`{"input":"hello"}`)})
		if err != nil || res.Output.Text != "hi" {
			t.Fatalf("call = %+v, %v", res.Output, err)
		}
		var c Callee
		_ = json.Unmarshal(recs.of(CalleeNS)[0], &c)
		if c.Relation != RelationSubsession {
			t.Errorf("callee = %+v, want the call's own subsession", c)
		}
	})
	t.Run("failed before it returned", func(t *testing.T) {
		remote := working{cancelled: make(chan a2a.TaskID, 4)}
		client, card := serve(t, remote, nil)
		ctx := agenttool.ContextWithRecorder(context.Background(), failing)
		if _, err := New(client, card, never).Execute(ctx, agenttool.Call{ID: "c1", Args: json.RawMessage(`{"input":"work"}`)}); err == nil {
			t.Fatal("the call succeeded with no callee record")
		}
		select {
		case <-remote.cancelled:
		case <-time.After(5 * time.Second):
			t.Fatal("the task the call left was not cancelled")
		}
	})
}

// slowStart is a remote agent that names its task only once its gate
// opens.
type slowStart struct{ gate chan struct{} }

func (s slowStart) Execute(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue) error {
	select {
	case <-s.gate:
	case <-ctx.Done():
		return nil
	case <-time.After(8 * time.Second):
	}
	ev := a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateCompleted, nil)
	ev.Final = true
	return q.Write(ctx, ev)
}

func (slowStart) Cancel(context.Context, *a2asrv.RequestContext, eventqueue.Queue) error { return nil }

// TestADetachedCallHonoursItsContext pins the review of #290: until the
// call returns on a running task it is the call's, so its own context
// ending, a per-call timeout inside a run say, ends it, though the task
// would be followed on the run's context, and fn is not called.
func TestADetachedCallHonoursItsContext(t *testing.T) {
	remote := slowStart{gate: make(chan struct{})}
	defer close(remote.gate)
	client, card := serve(t, remote, nil)
	inner := New(client, card, WithDetach(func(string, TaskInfo, *a2a.Task, error) { t.Error("fn was called for a call that never detached") }))
	timed := agenttool.NewFunc("work", "works", inner.Parameters(), func(ctx context.Context, call agenttool.Call) (agenttool.Result, error) {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		return inner.Execute(ctx, call)
	})
	parent := agentturn.New(agentturn.Config{Model: callsAll{}, ModelName: "local", Tools: []agenttool.Tool{timed}})
	ret := make(chan *agentturn.RunEnd, 1)
	go func() {
		end, _ := parent.Prompt(context.Background(), openresponses.UserText("work"))
		ret <- end
	}()
	select {
	case end := <-ret:
		var out *openresponses.FunctionCallOutput
		for _, item := range end.Items {
			if o, ok := item.(*openresponses.FunctionCallOutput); ok {
				out = o
			}
		}
		if out == nil || !strings.Contains(out.Output.String(), "deadline") {
			t.Errorf("the call's output = %+v, want the call's deadline", out)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the call outlived its context")
	}
}

// lintsThenHandsOff calls lint and transfer_to_b in one batch when the
// user asks it to, and otherwise answers; it keeps the user texts each
// request carried.
type lintsThenHandsOff struct {
	mu   sync.Mutex
	seen []string
}

func (m *lintsThenHandsOff) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	var texts []string
	for _, item := range req.Input {
		if msg, ok := item.(*openresponses.Message); ok && msg.Role == openresponses.RoleUser {
			texts = append(texts, msg.Text())
		}
	}
	m.mu.Lock()
	m.seen = append(m.seen, strings.Join(texts, "|"))
	m.mu.Unlock()
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if msg, ok := req.Input[len(req.Input)-1].(*openresponses.Message); ok && msg.Text() == "lint, then hand off" {
		for _, name := range []string{"lint", "transfer_to_b"} {
			w, err := em.FunctionCall("", name)
			if err != nil {
				return err
			}
			args, _ := json.Marshal(Args{Input: "lint"})
			if err := w.Arguments(string(args)); err != nil {
				return err
			}
			if err := w.Close(); err != nil {
				return err
			}
		}
		return em.Complete()
	}
	if err := em.Item(openresponses.AssistantText("ok")); err != nil {
		return err
	}
	return em.Complete()
}

func (m *lintsThenHandsOff) saw(text string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return strings.Contains(strings.Join(m.seen, "\n"), text)
}

// TestADetachedResultDuringATransferGoesToTheReceiver pins the
// integration review of #290 × #274: a detached call's task finishing
// while the sender's run stops on transfer_to_b delivers its result to
// the receiver. The delivery, queued while the sender's run ends,
// starts no run under the sender's configuration, which would answer
// it as the sender and leave handoff.Team's switch refused; it waits,
// and the receiver's run the team starts takes it.
func TestADetachedResultDuringATransferGoesToTheReceiver(t *testing.T) {
	remote := newGated("lint")
	client, card := serve(t, remote, nil)
	sender, receiver := &lintsThenHandsOff{}, &lintsThenHandsOff{}
	var parent *agentturn.Agent
	delivered := make(chan error, 1)
	returned := make(chan struct{})
	detach := func(callID string, info TaskInfo, final *a2a.Task, err error) {
		ctx := agentturn.ContextWithTrigger(context.Background(), agentturn.CalleeTrigger(callID, string(info.TaskID)))
		_, _, derr := parent.Deliver(ctx, openresponses.UserText("lint says: "+Answer(final).Text))
		delivered <- errors.Join(err, derr)
		close(returned)
	}
	// The team's switch waits, in the handoff's filter, until the
	// delivery has returned or started a run of its own, so the
	// window between the sender's run and the receiver's is the
	// delivery's to take.
	settled := func(t agentturn.Transcript) agentturn.Transcript {
		deadline := time.Now().Add(5 * time.Second)
		for !parent.State().Running && time.Now().Before(deadline) {
			select {
			case <-returned:
				return t
			case <-time.After(time.Millisecond):
			}
		}
		return t
	}
	team, err := handoff.New(
		handoff.Agent{Config: agentturn.Config{Name: "a", Model: sender, ModelName: "m", Tools: []agenttool.Tool{New(client, card, WithName("lint"), WithDetach(detach))}}, Handoffs: []handoff.To{{Agent: "b", Filter: settled}}},
		handoff.Agent{Config: agentturn.Config{Name: "b", Model: receiver, ModelName: "m"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	parent = agentturn.New(team.Config("a"))
	parent.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		switch e := ev.(type) {
		case *agentturn.ToolEnd:
			if e.Name == "transfer_to_b" {
				// The task finishes as the sender's run stops on the
				// transfer.
				remote.open("lint")
			}
		case *agentturn.RunEnd:
			if parent.Config().Name != "a" {
				return nil
			}
			// The result is delivered before the sender's run has
			// ended, so it waits on that run.
			deadline := time.Now().Add(5 * time.Second)
			for parent.State().Steering == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
		}
		return nil
	})
	end, path, err := team.Run(context.Background(), parent, openresponses.UserText("lint, then hand off"))
	if err != nil {
		t.Fatalf("Team.Run: %v (path %v)", err, path)
	}
	if strings.Join(path, ",") != "a,b" || end.Reason != agentturn.ReasonDone {
		t.Errorf("Team.Run = %+v, path %v", end, path)
	}
	select {
	case err := <-delivered:
		if err != nil && !errors.Is(err, agentturn.ErrTerminated) {
			t.Errorf("delivery: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the result was never delivered")
	}
	if err := parent.WaitForIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sender.saw("lint says") {
		t.Error("the sender's configuration answered the detached result")
	}
	if !receiver.saw("lint says: lint finished") {
		t.Errorf("the receiver never read the detached result: %q", receiver.seen)
	}
}
