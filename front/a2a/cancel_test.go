package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
	"github.com/a2aproject/a2a-go/a2asrv"
	"github.com/a2aproject/a2a-go/a2asrv/eventqueue"
)

// loggedRun is what a record saw of one run: its trigger, the outputs
// appended with who decided each and why, and how it ended.
type loggedRun struct {
	trigger agentturn.Trigger
	outputs []loggedOutput
	reason  agentturn.Reason
	cause   agentturn.StopCause
}

type loggedOutput struct{ callID, text, by, why string }

// runLog is a RecorderFor that logs every run of every conversation.
type runLog struct {
	mu   sync.Mutex
	runs []*loggedRun
}

func (l *runLog) attach(ctx context.Context, _ string, a *agentturn.Agent) (context.Context, func(), error) {
	run := &loggedRun{}
	l.mu.Lock()
	l.runs = append(l.runs, run)
	l.mu.Unlock()
	return ctx, a.Subscribe(func(ctx context.Context, ev agentturn.Event) error {
		l.mu.Lock()
		defer l.mu.Unlock()
		switch ev := ev.(type) {
		case *agentturn.RunStart:
			run.trigger = ev.Trigger
		case *agentturn.ItemEnd:
			if out, ok := ev.Item.(*openresponses.FunctionCallOutput); ok {
				run.outputs = append(run.outputs, loggedOutput{out.CallID, out.Output.Text, agentturn.DeciderFromContext(ctx, out.CallID), agentturn.ReasonFromContext(ctx, out.CallID)})
			}
		case *agentturn.RunEnd:
			run.reason, run.cause = ev.Reason, ev.Cause
		}
		return nil
	}), nil
}

func (l *runLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.runs)
}

func (l *runLog) last() loggedRun {
	l.mu.Lock()
	defer l.mu.Unlock()
	return *l.runs[len(l.runs)-1]
}

// taskStore is an SDK task store kept as JSON, as one that outlives the
// process keeps it, so two handlers can share it across a restart.
type taskStore struct {
	mu    sync.Mutex
	tasks map[a2a.TaskID][]byte
	vers  map[a2a.TaskID]a2a.TaskVersion
}

func (s *taskStore) Save(_ context.Context, task *a2a.Task, _ a2a.Event, _ *a2a.Task, _ a2a.TaskVersion) (a2a.TaskVersion, error) {
	raw, err := json.Marshal(task)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tasks == nil {
		s.tasks, s.vers = map[a2a.TaskID][]byte{}, map[a2a.TaskID]a2a.TaskVersion{}
	}
	s.tasks[task.ID] = raw
	s.vers[task.ID]++
	return s.vers[task.ID], nil
}

func (s *taskStore) Get(_ context.Context, id a2a.TaskID) (*a2a.Task, a2a.TaskVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.tasks[id]
	if !ok {
		return nil, 0, a2a.ErrTaskNotFound
	}
	var task a2a.Task
	if err := json.Unmarshal(raw, &task); err != nil {
		return nil, 0, err
	}
	return &task, s.vers[id], nil
}

func (s *taskStore) List(context.Context, *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	return nil, a2a.ErrUnsupportedOperation
}

// peer serves exec over JSON-RPC under httptest, its tasks in tasks, and
// returns a client sending token's bearer.
func peer(t *testing.T, exec *Executor, tasks a2asrv.TaskStore) func(token string) *a2aclient.Client {
	t.Helper()
	srv := httptest.NewServer(a2asrv.NewJSONRPCHandler(exec.RequestHandler(a2asrv.WithTaskStore(tasks))))
	t.Cleanup(srv.Close)
	card := exec.Secure(AgentCard(context.Background(), agentturn.Config{Name: "callee"}, srv.URL, "1"))
	return func(token string) *a2aclient.Client {
		client, err := a2aclient.NewFromCard(context.Background(), card, a2aclient.WithInterceptors(bearer{scheme: "Bearer", token: token}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Destroy() })
		return client
	}
}

// send sends msg and returns the task it answered with.
func send(t *testing.T, client *a2aclient.Client, msg *a2a.Message) *a2a.Task {
	t.Helper()
	res, err := client.SendMessage(context.Background(), &a2a.MessageSendParams{Message: msg})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	task, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("send answered %T", res)
	}
	return task
}

// pendingIDs returns the IDs of the function_call data parts on an
// input-required task's status message.
func pendingIDs(t *testing.T, task *a2a.Task) []string {
	t.Helper()
	if task.Status.State != a2a.TaskStateInputRequired {
		t.Fatalf("task = %s (%s), want input-required", task.Status.State, taskText(task))
	}
	var ids []string
	for id := range listedCalls(task) {
		ids = append(ids, id)
	}
	return ids
}

var remoteTool = openresponses.NewFunctionTool("remote", "the caller runs it", json.RawMessage(`{"type":"object"}`))

// settledAs checks that the conversation holds no unanswered call and
// that each of ids is answered by the refusal naming who canceled, and
// that the record's last run refused exactly ids, under who's trigger,
// naming nobody as the decider, and ended without calling the model.
func settledAs(t *testing.T, store *MemoryStore, conv string, log *runLog, who string, ids ...string) {
	t.Helper()
	want := "the caller " + who + " canceled the task, so this call was not run"
	tr, _ := store.Load(context.Background(), conv)
	if left := unanswered(tr); len(left) != 0 {
		t.Errorf("the conversation still holds %d unanswered call(s)", len(left))
	}
	outputs := map[string]string{}
	for _, item := range tr {
		if out, ok := item.(*openresponses.FunctionCallOutput); ok {
			outputs[out.CallID] = out.Output.Text
		}
	}
	for _, id := range ids {
		if outputs[id] != want {
			t.Errorf("stored output of %s = %q, want %q", id, outputs[id], want)
		}
	}
	run := log.last()
	if run.trigger.Kind != "a2a" || run.trigger.Ref != who {
		t.Errorf("the settling run's trigger = %+v, want a2a/%s", run.trigger, who)
	}
	if run.reason != agentturn.ReasonStopped || run.cause != agentturn.StopRefused {
		t.Errorf("the settling run ended %s/%s, want stopped/refused", run.reason, run.cause)
	}
	if len(run.outputs) != len(ids) {
		t.Fatalf("the settling run recorded %d output(s), want %d: %+v", len(run.outputs), len(ids), run.outputs)
	}
	for _, out := range run.outputs {
		if out.text != want || out.why != want || out.by != "" {
			t.Errorf("recorded refusal = %+v, want %q by nobody", out, want)
		}
	}
}

// TestCancelSettlesWhatTheTaskLeftPending pins #255 for the caller's own
// calls: a cancel of a task left input-required, with no run active,
// refuses its pending call as canceled by the caller, on a run the
// record observes under the caller's trigger, so the conversation holds
// no unanswered call and the next message starts clean. Another
// principal's cancel is refused as not found and writes nothing, a
// second cancel settles nothing again, and a late answer to the settled
// call is refused rather than stored as a second output.
func TestCancelSettlesWhatTheTaskLeftPending(t *testing.T) {
	store, log := &MemoryStore{}, &runLog{}
	exec := New(agentturn.Config{Model: callsThenDone{"remote"}},
		WithAuthenticator(BearerTokens(map[string]string{"a-token": "alice", "b-token": "bob"})),
		WithConversationStore(store), WithCallerTools(remoteTool), WithRecorderFor(log.attach))
	client := peer(t, exec, &taskStore{})
	alice, bob := client("a-token"), client("b-token")

	task := send(t, alice, userMessage("go"))
	ids := pendingIDs(t, task)
	if len(ids) != 1 {
		t.Fatalf("pending = %v", ids)
	}

	if _, err := bob.CancelTask(context.Background(), &a2a.TaskIDParams{ID: task.ID}); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Errorf("bob's cancel: err = %v, want not found", err)
	}
	if tr, _ := store.Load(context.Background(), task.ContextID); len(unanswered(tr)) != 1 || log.count() != 1 {
		t.Errorf("bob's cancel wrote: %d unanswered, %d runs", len(unanswered(tr)), log.count())
	}

	got, err := alice.CancelTask(context.Background(), &a2a.TaskIDParams{ID: task.ID})
	if err != nil || got.Status.State != a2a.TaskStateCanceled {
		t.Fatalf("alice's cancel = %+v, %v", got, err)
	}
	settledAs(t, store, task.ContextID, log, "alice", ids...)

	before, _ := store.Load(context.Background(), task.ContextID)
	got, err = alice.CancelTask(context.Background(), &a2a.TaskIDParams{ID: task.ID})
	if err != nil || got.Status.State != a2a.TaskStateCanceled {
		t.Errorf("a second cancel = %+v, %v", got, err)
	}
	if after, _ := store.Load(context.Background(), task.ContextID); len(after) != len(before) || log.count() != 2 {
		t.Errorf("a second cancel settled again: %d items, was %d; %d runs", len(after), len(before), log.count())
	}

	late := a2a.NewMessage(a2a.MessageRoleUser, a2a.DataPart{Data: map[string]any{"type": "function_call_output", "call_id": ids[0], "output": "late"}})
	late.ContextID = task.ContextID
	if _, err := alice.SendMessage(context.Background(), &a2a.MessageSendParams{Message: late}); !errors.Is(err, a2a.ErrInvalidParams) {
		t.Errorf("a late answer to the settled call: err = %v, want invalid params", err)
	}
	if after, _ := store.Load(context.Background(), task.ContextID); len(after) != len(before) {
		t.Errorf("the late answer was stored: %d items, was %d", len(after), len(before))
	}

	next := userMessage("start over")
	next.ContextID = task.ContextID
	if task := send(t, alice, next); task.Status.State != a2a.TaskStateCompleted || taskText(task) != "done" {
		t.Errorf("the next message = %s %q, want completed", task.Status.State, taskText(task))
	}
}

// TestCancelSettlesAnEscalatedQuestion pins #255 under WithEscalation:
// the question the task put to the caller, and the call the policy held
// beside it, are refused as canceled, neither runs, and the calls the
// task put to its caller are forgotten.
func TestCancelSettlesAnEscalatedQuestion(t *testing.T) {
	var mu sync.Mutex
	ran := map[string]int{}
	tool := func(name string) agenttool.Tool {
		return agenttool.NewFunc(name, "", json.RawMessage(`{"type":"object"}`), func(context.Context, agenttool.Call) (agenttool.Result, error) {
			mu.Lock()
			ran[name]++
			mu.Unlock()
			return agenttool.Text(name + " ran"), nil
		})
	}
	hook := func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
		switch info.Call.Name {
		case "deploy":
			return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "deploys are asked"}, nil
		case "read":
			return &agentturn.ToolDecision{Action: agentturn.Defer, Held: true, Reason: "allowed by read"}, nil
		}
		return nil, nil
	}
	store, log := &MemoryStore{}, &runLog{}
	exec := New(agentturn.Config{Model: callsBoth{"deploy", "read"}, Tools: []agenttool.Tool{tool("deploy"), tool("read")}, BeforeToolCall: hook},
		WithAuthenticator(BearerTokens(map[string]string{"a-token": "alice"})), WithEscalation(),
		WithConversationStore(store), WithRecorderFor(log.attach))
	alice := peer(t, exec, &taskStore{})("a-token")

	task := send(t, alice, userMessage("ship it"))
	ids := pendingIDs(t, task)
	if esc, _ := task.Status.Message.Metadata[MetaEscalatedCalls].([]any); len(esc) != 1 || len(ids) != 2 {
		t.Fatalf("escalated = %v, pending = %v", esc, ids)
	}
	got, err := alice.CancelTask(context.Background(), &a2a.TaskIDParams{ID: task.ID})
	if err != nil || got.Status.State != a2a.TaskStateCanceled {
		t.Fatalf("cancel = %+v, %v", got, err)
	}
	settledAs(t, store, task.ContextID, log, "alice", ids...)
	mu.Lock()
	if len(ran) != 0 {
		t.Errorf("ran %v after the cancel", ran)
	}
	mu.Unlock()
	if left, _ := store.LoadEscalated(context.Background(), task.ContextID); len(left) != 0 {
		t.Errorf("escalated calls kept: %+v", left)
	}

	next := userMessage("never mind")
	next.ContextID = task.ContextID
	if task := send(t, alice, next); task.Status.State != a2a.TaskStateCompleted {
		t.Errorf("the next message = %s %q, want completed", task.Status.State, taskText(task))
	}
}

// forgetsWaiting is a conversation store that keeps the owners and the
// escalated calls but not the task each conversation waits on.
type forgetsWaiting struct{ s *MemoryStore }

func (f forgetsWaiting) Load(ctx context.Context, id string) (agentturn.Transcript, error) {
	return f.s.Load(ctx, id)
}

func (f forgetsWaiting) Save(ctx context.Context, id string, t agentturn.Transcript) error {
	return f.s.Save(ctx, id, t)
}

func (f forgetsWaiting) Owner(ctx context.Context, key string) (string, bool, error) {
	return f.s.Owner(ctx, key)
}

func (f forgetsWaiting) Claim(ctx context.Context, key, principal string) (string, error) {
	return f.s.Claim(ctx, key, principal)
}

func (f forgetsWaiting) LoadEscalated(ctx context.Context, id string) ([]Escalated, error) {
	return f.s.LoadEscalated(ctx, id)
}

func (f forgetsWaiting) SaveEscalated(ctx context.Context, id string, calls []Escalated) error {
	return f.s.SaveEscalated(ctx, id, calls)
}

// TestCancelSettlesAfterARestart pins that the settlement needs nothing
// the process held: a new executor over the same conversation and task
// stores settles the task the old one left input-required, from the
// task it waits on where the store keeps it, and from the task's status
// alone where the store does not.
func TestCancelSettlesAfterARestart(t *testing.T) {
	for _, keeps := range []bool{true, false} {
		t.Run(map[bool]string{true: "store keeps the waiting task", false: "store forgets it"}[keeps], func(t *testing.T) {
			testCancelAfterRestart(t, keeps)
		})
	}
}

func testCancelAfterRestart(t *testing.T, keeps bool) {
	store, tasks := &MemoryStore{}, &taskStore{}
	var conversations ConversationStore = store
	if !keeps {
		conversations = forgetsWaiting{store}
	}
	tokens := map[string]string{"a-token": "alice"}
	cfg := agentturn.Config{Model: callsThenDone{"remote"}}
	first := peer(t, New(cfg, WithAuthenticator(BearerTokens(tokens)), WithConversationStore(conversations), WithCallerTools(remoteTool)), tasks)
	task := send(t, first("a-token"), userMessage("go"))
	ids := pendingIDs(t, task)
	if waiting, _ := store.LoadWaiting(context.Background(), task.ContextID); (waiting != "") != keeps {
		t.Fatalf("the store names %q as waited on", waiting)
	}

	log := &runLog{}
	restarted := peer(t, New(cfg, WithAuthenticator(BearerTokens(tokens)), WithConversationStore(conversations), WithCallerTools(remoteTool), WithRecorderFor(log.attach)), tasks)
	got, err := restarted("a-token").CancelTask(context.Background(), &a2a.TaskIDParams{ID: task.ID})
	if err != nil || got.Status.State != a2a.TaskStateCanceled {
		t.Fatalf("cancel after the restart = %+v, %v", got, err)
	}
	if log.count() != 1 {
		t.Fatalf("runs recorded after the restart = %d, want the settling one", log.count())
	}
	settledAs(t, store, task.ContextID, log, "alice", ids...)
}

// capture is a queue that keeps what is written to it.
type capture struct {
	eventqueue.Queue
	events []a2a.Event
}

func (c *capture) Write(_ context.Context, ev a2a.Event) error {
	c.events = append(c.events, ev)
	return nil
}

// countingModel counts its requests and blocks the ones after the first
// n until release is closed, signalling entered when it does.
type countingModel struct {
	inner   agentturn.Model
	n       int
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	calls   int
}

func (m *countingModel) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.mu.Lock()
	m.calls++
	block := m.calls > m.n && m.release != nil
	m.mu.Unlock()
	if block {
		m.once.Do(func() { close(m.entered) })
		<-m.release
	}
	return m.inner.CreateStream(ctx, req, sink)
}

func (m *countingModel) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// TestCancelRacingAMessage pins the cancel against a message on the same
// conversation. A message on the task that the SDK let through before
// the cancel settled it does not run: the task stays canceled and the
// conversation as the cancel left it. A cancel arriving while another
// task runs on the conversation, which answers the canceled task's calls,
// settles nothing, so each call keeps one output.
func TestCancelRacingAMessage(t *testing.T) {
	t.Run("a message on the canceled task", func(t *testing.T) {
		model := &countingModel{inner: callsThenDone{"remote"}}
		store := &MemoryStore{}
		exec := New(agentturn.Config{Model: model}, WithConversationStore(store), WithCallerTools(remoteTool))
		h := a2asrv.NewHandler(exec)
		res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: userMessage("go")})
		if err != nil {
			t.Fatal(err)
		}
		task := res.(*a2a.Task)
		ids := pendingIDs(t, task)
		if _, err := h.OnCancelTask(context.Background(), &a2a.TaskIDParams{ID: task.ID}); err != nil {
			t.Fatal(err)
		}
		before, _ := store.Load(context.Background(), task.ContextID)
		if len(unanswered(before)) != 0 {
			t.Fatalf("the cancel left %d unanswered", len(unanswered(before)))
		}
		answer := a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.DataPart{Data: map[string]any{"type": "function_call_output", "call_id": ids[0], "output": "late"}})
		q := &capture{}
		reqCtx := &a2asrv.RequestContext{Message: answer, TaskID: task.ID, ContextID: task.ContextID, StoredTask: task}
		if err := exec.Execute(context.Background(), reqCtx, q); err != nil {
			t.Fatalf("Execute = %v", err)
		}
		if len(q.events) != 1 || q.events[0].(*a2a.TaskStatusUpdateEvent).Status.State != a2a.TaskStateCanceled {
			t.Errorf("events = %+v, want the task canceled", q.events)
		}
		if after, _ := store.Load(context.Background(), task.ContextID); len(after) != len(before) || model.count() != 1 {
			t.Errorf("the late message ran: %d items, was %d; %d model calls", len(after), len(before), model.count())
		}
		exec.mu.Lock()
		if _, kept := exec.canceled[task.ContextID]; kept {
			t.Error("the cancel's mark outlived the message it served")
		}
		exec.mu.Unlock()
		next := userMessage("again")
		next.ContextID = task.ContextID
		if res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: next}); err != nil || res.(*a2a.Task).Status.State != a2a.TaskStateCompleted {
			t.Errorf("a new task on the conversation = %+v, %v", res, err)
		}
	})
	t.Run("another task in flight", func(t *testing.T) {
		model := &countingModel{inner: callsThenDone{"remote"}, n: 1, entered: make(chan struct{}), release: make(chan struct{})}
		store := &MemoryStore{}
		exec := New(agentturn.Config{Model: model}, WithConversationStore(store), WithCallerTools(remoteTool))
		h := a2asrv.NewHandler(exec)
		res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: userMessage("go")})
		if err != nil {
			t.Fatal(err)
		}
		task := res.(*a2a.Task)
		ids := pendingIDs(t, task)
		answer := a2a.NewMessage(a2a.MessageRoleUser, a2a.DataPart{Data: map[string]any{"type": "function_call_output", "call_id": ids[0], "output": "answered"}})
		answer.ContextID = task.ContextID
		done := make(chan *a2a.Task, 1)
		go func() {
			res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: answer})
			if err != nil {
				t.Errorf("the other task: %v", err)
			}
			got, _ := res.(*a2a.Task)
			done <- got
		}()
		<-model.entered
		if got, err := h.OnCancelTask(context.Background(), &a2a.TaskIDParams{ID: task.ID}); err != nil || got.Status.State != a2a.TaskStateCanceled {
			t.Errorf("cancel = %+v, %v", got, err)
		}
		close(model.release)
		if other := <-done; other == nil || other.Status.State != a2a.TaskStateCompleted {
			t.Fatalf("the other task = %+v", other)
		}
		tr, _ := store.Load(context.Background(), task.ContextID)
		var outputs []string
		for _, item := range tr {
			if out, ok := item.(*openresponses.FunctionCallOutput); ok {
				outputs = append(outputs, out.Output.Text)
			}
		}
		if len(outputs) != 1 || outputs[0] != "answered" || len(unanswered(tr)) != 0 {
			t.Errorf("outputs = %q, unanswered = %d; want the other task's answer alone", outputs, len(unanswered(tr)))
		}
	})
}

// TestCancelSeesTheConversationFree pins the order at a run's end: the
// record has let go and the conversation is free before the task's
// input-required status is written, so a cancel sent the moment a
// blocking send returns that status settles the calls rather than
// finding the run still registered. The record's detach is slow, which
// is when the old order lost the race.
func TestCancelSeesTheConversationFree(t *testing.T) {
	t.Run("at the status", func(t *testing.T) {
		var detached bool
		exec := New(agentturn.Config{Model: callsThenDone{"remote"}}, WithCallerTools(remoteTool),
			WithRecorderFor(func(ctx context.Context, _ string, _ *agentturn.Agent) (context.Context, func(), error) {
				return ctx, func() { detached = true }, nil
			}))
		var checked bool
		q := &probe{at: func(ev a2a.Event) {
			st, ok := ev.(*a2a.TaskStatusUpdateEvent)
			if !ok || st.Status.State != a2a.TaskStateInputRequired {
				return
			}
			checked = true
			exec.mu.Lock()
			defer exec.mu.Unlock()
			if exec.busy["c1"] || exec.cancels["t1"] != nil || !detached {
				t.Errorf("at the input-required status: busy %v, run registered %v, detached %v", exec.busy["c1"], exec.cancels["t1"] != nil, detached)
			}
		}}
		reqCtx := &a2asrv.RequestContext{Message: userMessage("go"), TaskID: "t1", ContextID: "c1"}
		if err := exec.Execute(context.Background(), reqCtx, q); err != nil || !checked {
			t.Fatalf("Execute = %v, status seen %v", err, checked)
		}
		// The follow-up that answers nothing re-lists the calls the same way.
		checked = false
		reqCtx = &a2asrv.RequestContext{Message: userMessage("well?"), TaskID: "t1", ContextID: "c1"}
		if err := exec.Execute(context.Background(), reqCtx, q); err != nil || !checked {
			t.Fatalf("the follow-up's Execute = %v, status seen %v", err, checked)
		}
	})
	t.Run("over the wire", func(t *testing.T) {
		store, log := &MemoryStore{}, &runLog{}
		slow := func(ctx context.Context, id string, a *agentturn.Agent) (context.Context, func(), error) {
			ctx, detach, err := log.attach(ctx, id, a)
			return ctx, func() {
				time.Sleep(100 * time.Millisecond)
				detach()
			}, err
		}
		exec := New(agentturn.Config{Model: callsThenDone{"remote"}},
			WithAuthenticator(BearerTokens(map[string]string{"a-token": "alice"})),
			WithConversationStore(store), WithCallerTools(remoteTool), WithRecorderFor(slow))
		alice := peer(t, exec, &taskStore{})("a-token")
		task := send(t, alice, userMessage("go"))
		ids := pendingIDs(t, task)
		got, err := alice.CancelTask(context.Background(), &a2a.TaskIDParams{ID: task.ID})
		if err != nil || got.Status.State != a2a.TaskStateCanceled {
			t.Fatalf("cancel = %+v, %v", got, err)
		}
		settledAs(t, store, task.ContextID, log, "alice", ids...)
	})
}

// probe is a queue that shows every event to at before keeping it.
type probe struct {
	capture
	at func(a2a.Event)
}

func (p *probe) Write(ctx context.Context, ev a2a.Event) error {
	p.at(ev)
	return p.capture.Write(ctx, ev)
}

// ctxStore is a conversation store that, as a durable one does, fails a
// write whose context is done.
type ctxStore struct{ *MemoryStore }

func (c ctxStore) Save(ctx context.Context, id string, t agentturn.Transcript) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.MemoryStore.Save(ctx, id, t)
}

// TestCancelOutlivesItsRequest pins that the settlement does not hang
// on the cancel's request: a caller that hung up, its context canceled
// before the executor looked, still has the calls refused, recorded and
// stored, by a record and a store that fail a write whose context is
// done.
func TestCancelOutlivesItsRequest(t *testing.T) {
	store, log := &MemoryStore{}, &runLog{}
	record := func(ctx context.Context, id string, a *agentturn.Agent) (context.Context, func(), error) {
		ctx, detach, err := log.attach(ctx, id, a)
		stop := a.Subscribe(func(ctx context.Context, _ agentturn.Event) error { return ctx.Err() })
		return ctx, func() { stop(); detach() }, err
	}
	exec := New(agentturn.Config{Model: callsThenDone{"remote"}},
		WithAuthenticator(BearerTokens(map[string]string{"a-token": "alice"})),
		WithConversationStore(ctxStore{store}), WithCallerTools(remoteTool), WithRecorderFor(record))
	task := send(t, peer(t, exec, &taskStore{})("a-token"), userMessage("go"))
	ids := pendingIDs(t, task)
	ctx, hangUp := context.WithCancel(context.WithValue(ContextWithPrincipal(context.Background(), "alice"), guardedKey{}, true))
	hangUp()
	q := &capture{}
	if err := exec.Cancel(ctx, &a2asrv.RequestContext{TaskID: task.ID, ContextID: task.ContextID, StoredTask: task}, q); err != nil {
		t.Fatalf("Cancel = %v", err)
	}
	if len(q.events) != 1 || q.events[0].(*a2a.TaskStatusUpdateEvent).Status.State != a2a.TaskStateCanceled {
		t.Errorf("events = %+v, want the task canceled", q.events)
	}
	settledAs(t, store, task.ContextID, log, "alice", ids...)
}

// alwaysCalls calls name on every request.
type alwaysCalls struct{ name string }

func (m alwaysCalls) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	w, err := em.FunctionCall("", m.name)
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

// TestCancelLeavesALaterTasksCalls pins that a cancel settles only what
// the conversation waits on its task for. When a later task listed the
// same calls again, a message that answered none of them, or answered
// them and left calls of its own, the cancel of the earlier task settles
// nothing, and the later task's calls stay its to answer or cancel.
func TestCancelLeavesALaterTasksCalls(t *testing.T) {
	sendOn := func(t *testing.T, h a2asrv.RequestHandler, msg *a2a.Message) *a2a.Task {
		t.Helper()
		res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: msg})
		if err != nil {
			t.Fatal(err)
		}
		return res.(*a2a.Task)
	}
	output := func(id, text string) a2a.DataPart {
		return a2a.DataPart{Data: map[string]any{"type": "function_call_output", "call_id": id, "output": text}}
	}
	t.Run("listed again", func(t *testing.T) {
		store := &MemoryStore{}
		h := a2asrv.NewHandler(New(agentturn.Config{Model: callsThenDone{"remote"}}, WithConversationStore(store), WithCallerTools(remoteTool)))
		first := sendOn(t, h, userMessage("go"))
		ids := pendingIDs(t, first)
		again := userMessage("hello?")
		again.ContextID = first.ContextID
		second := sendOn(t, h, again)
		if relisted := pendingIDs(t, second); len(relisted) != 1 || relisted[0] != ids[0] {
			t.Fatalf("the second task lists %v, want %v", relisted, ids)
		}
		if got, err := h.OnCancelTask(context.Background(), &a2a.TaskIDParams{ID: first.ID}); err != nil || got.Status.State != a2a.TaskStateCanceled {
			t.Fatalf("cancel = %+v, %v", got, err)
		}
		if tr, _ := store.Load(context.Background(), first.ContextID); len(unanswered(tr)) != 1 {
			t.Errorf("the cancel settled the second task's call: %d unanswered", len(unanswered(tr)))
		}
		if done := sendOn(t, h, a2a.NewMessageForTask(a2a.MessageRoleUser, second, output(ids[0], "ok"))); done.Status.State != a2a.TaskStateCompleted {
			t.Errorf("the second task's answer = %s %q", done.Status.State, taskText(done))
		}
	})
	t.Run("moved on", func(t *testing.T) {
		store := &MemoryStore{}
		h := a2asrv.NewHandler(New(agentturn.Config{Model: alwaysCalls{"remote"}}, WithConversationStore(store), WithCallerTools(remoteTool)))
		first := sendOn(t, h, userMessage("go"))
		ids := pendingIDs(t, first)
		answer := a2a.NewMessage(a2a.MessageRoleUser, output(ids[0], "ok"))
		answer.ContextID = first.ContextID
		second := sendOn(t, h, answer)
		next := pendingIDs(t, second)
		if _, err := h.OnCancelTask(context.Background(), &a2a.TaskIDParams{ID: first.ID}); err != nil {
			t.Fatal(err)
		}
		tr, _ := store.Load(context.Background(), first.ContextID)
		if left := unanswered(tr); len(left) != 1 || left[0].CallID != next[0] {
			t.Errorf("after the first task's cancel, unanswered = %v, want the second task's %v", left, next)
		}
		if _, err := h.OnCancelTask(context.Background(), &a2a.TaskIDParams{ID: second.ID}); err != nil {
			t.Fatal(err)
		}
		if tr, _ := store.Load(context.Background(), first.ContextID); len(unanswered(tr)) != 0 {
			t.Errorf("the second task's cancel left %d unanswered", len(unanswered(tr)))
		}
	})
}

// TestCanceledMarksAgeOut pins that a cancel's mark does not outlive
// the window it serves when its conversation sees no further message.
func TestCanceledMarksAgeOut(t *testing.T) {
	var exec Executor
	now := time.Now()
	exec.markCanceled("old", "t1", now.Add(-2*canceledMarkAge))
	exec.markCanceled("new", "t2", now)
	if _, ok := exec.canceled["old"]; ok || len(exec.canceled) != 1 {
		t.Errorf("marks = %v, want the old one dropped", exec.canceled)
	}
}

// TestCancelBeforeTheStatusReachesTheSDK pins a cancel that comes after
// the run let go of the conversation and before its input-required
// status was written, a caller aborting as the run ends: the SDK's task
// is still working, and the calls are settled all the same, since the
// conversation waits on the task.
func TestCancelBeforeTheStatusReachesTheSDK(t *testing.T) {
	store := &MemoryStore{}
	exec := New(agentturn.Config{Model: callsThenDone{"remote"}}, WithConversationStore(store), WithCallerTools(remoteTool))
	working := &a2a.Task{ID: "t1", ContextID: "c1", Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}
	var canceled error
	q := &probe{at: func(ev a2a.Event) {
		if st, ok := ev.(*a2a.TaskStatusUpdateEvent); ok && st.Status.State == a2a.TaskStateInputRequired {
			canceled = exec.Cancel(context.Background(), &a2asrv.RequestContext{TaskID: "t1", ContextID: "c1", StoredTask: working}, &capture{})
		}
	}}
	if err := exec.Execute(context.Background(), &a2asrv.RequestContext{Message: userMessage("go"), TaskID: "t1", ContextID: "c1"}, q); err != nil {
		t.Fatal(err)
	}
	if canceled != nil {
		t.Fatalf("Cancel = %v", canceled)
	}
	tr, _ := store.Load(context.Background(), "c1")
	var outputs []string
	for _, item := range tr {
		if out, ok := item.(*openresponses.FunctionCallOutput); ok {
			outputs = append(outputs, out.Output.Text)
		}
	}
	if len(unanswered(tr)) != 0 || len(outputs) != 1 || outputs[0] != "the caller canceled the task, so this call was not run" {
		t.Errorf("unanswered = %d, outputs = %q; want the call refused", len(unanswered(tr)), outputs)
	}
	if waiting, _ := store.LoadWaiting(context.Background(), "c1"); waiting != "" {
		t.Errorf("the conversation still waits on %q", waiting)
	}
}
