package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
)

// noteRecord is what the note tool writes through agenttool.WriteRecord.
type noteRecord struct {
	Q string `json:"q"`
}

func (noteRecord) RecordNS() string { return "test:note" }

type conversationKey struct{}

// fakeRecorder stands in for a session recorder: per conversation, the
// events of every run in the order they were delivered, and the tool
// records with what had been delivered when each was written.
type fakeRecorder struct {
	mu         sync.Mutex
	contexts   []string
	seeded     []int
	offered    [][]string
	runs       [][]string
	records    []string
	dispatched map[string]bool
	detached   int
}

func (f *fakeRecorder) attach(ctx context.Context, contextID string, a *agentturn.Agent) (context.Context, func(), error) {
	f.mu.Lock()
	f.contexts = append(f.contexts, contextID)
	f.seeded = append(f.seeded, len(a.State().Transcript))
	var names []string
	offered := a.Config().Tools
	if provider := a.Config().ToolProvider; provider != nil {
		offered = provider(ctx)
	}
	for _, tl := range offered {
		names = append(names, tl.Name())
	}
	f.offered = append(f.offered, names)
	f.runs = append(f.runs, nil)
	run := len(f.runs) - 1
	if f.dispatched == nil {
		f.dispatched = map[string]bool{}
	}
	f.mu.Unlock()

	cfg := a.Config()
	cfg.ToolRecorder = func(ctx context.Context, rec *agenttool.Record) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		call, _ := agenttool.CallFrom(ctx)
		conv, _ := ctx.Value(conversationKey{}).(string)
		// A recorder in lockstep with the run has seen the call's
		// dispatch before the tool writes under it.
		state := "undispatched"
		if f.dispatched[call.ID] {
			state = "dispatched"
		}
		f.records = append(f.records, rec.NS+" "+state+" "+conv)
		return nil
	}
	if err := a.SetConfig(cfg); err != nil {
		return nil, nil, err
	}
	unsubscribe := a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.runs[run] = append(f.runs[run], ev.EventType())
		if d, ok := ev.(*agentturn.ToolDispatch); ok {
			f.dispatched[d.CallID] = true
		}
		return nil
	})
	return context.WithValue(ctx, conversationKey{}, contextID), func() {
		unsubscribe()
		f.mu.Lock()
		f.detached++
		f.mu.Unlock()
	}, nil
}

// TestRecorderForObservesEveryRun pins #141: every run the executor
// drives reaches what RecorderFor attached, in lockstep, from run_start
// to run_end, under the configuration the run used, with the context
// it returned on the tools' calls, and its tool recorder in force.
func TestRecorderForObservesEveryRun(t *testing.T) {
	note := agenttool.New("note", "writes a record", func(ctx context.Context, in struct {
		Q string `json:"q"`
	}) (string, error) {
		return "noted", agenttool.WriteRecord(ctx, noteRecord(in))
	})
	rec := &fakeRecorder{}
	store := &MemoryStore{}
	h := a2asrv.NewHandler(New(agentturn.Config{Model: callsEveryTool{}, Tools: []agenttool.Tool{note}},
		WithConversationStore(store),
		WithCallerTools(openresponses.NewFunctionTool("remote", "caller side", json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`))),
		WithRecorderFor(rec.attach)))

	task := sendTask(t, h, userMessage("go"))
	if task.Status.State != a2a.TaskStateInputRequired {
		t.Fatalf("state = %s (%s)", task.Status.State, taskText(task))
	}
	dp := task.Status.Message.Parts[0].(a2a.DataPart)
	answer := a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.DataPart{Data: map[string]any{
		"type": "function_call_output", "call_id": dp.Data["call_id"], "output": "remote-ran",
	}})
	if done := sendTask(t, h, answer); done.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("resumed = %s %q", done.Status.State, taskText(done))
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.contexts) != 2 || rec.contexts[0] != task.ContextID || rec.contexts[1] != task.ContextID {
		t.Fatalf("attached for %v, want the task's context twice", rec.contexts)
	}
	stored, _ := store.Load(context.Background(), task.ContextID)
	if rec.seeded[0] != 0 || rec.seeded[1] == 0 || rec.seeded[1] >= len(stored) {
		t.Errorf("seeded with %v items; stored %d", rec.seeded, len(stored))
	}
	for i, names := range rec.offered {
		if !slices.Equal(names, []string{"note", "remote"}) {
			t.Errorf("run %d offered %v, want the agent's tools and the caller's", i, names)
		}
	}
	for i, run := range rec.runs {
		if len(run) == 0 || run[0] != agentturn.EventRunStart || run[len(run)-1] != agentturn.EventRunEnd {
			t.Errorf("run %d events = %v, want run_start through run_end", i, run)
		}
	}
	if !slices.Contains(rec.runs[0], agentturn.EventToolDispatch) {
		t.Errorf("first run events = %v, want the note call's dispatch", rec.runs[0])
	}
	if want := []string{"test:note dispatched " + task.ContextID}; !slices.Equal(rec.records, want) {
		t.Errorf("records = %v, want %v", rec.records, want)
	}
	if rec.detached != 2 {
		t.Errorf("detached %d times, want 2", rec.detached)
	}
}

// TestRecorderForFailures pins that a recorder that cannot open, or
// returns no context, fails the send before any task exists or the
// model is called, and one that fails an event fails the task through
// the run.
func TestRecorderForFailures(t *testing.T) {
	boom := errors.New("store down")
	tests := []struct {
		name string
		fn   RecorderFor
		// calls is how many model calls the task makes; sendErr, when
		// set, is in the error the send fails with, and otherwise the
		// send returns a task failed with boom.
		calls   int
		sendErr string
	}{
		{
			name: "open",
			fn: func(context.Context, string, *agentturn.Agent) (context.Context, func(), error) {
				return nil, nil, boom
			},
			sendErr: boom.Error(),
		},
		{
			name: "nil context",
			fn: func(context.Context, string, *agentturn.Agent) (context.Context, func(), error) {
				return nil, nil, nil
			},
			sendErr: "nil context",
		},
		{
			name: "event",
			fn: func(ctx context.Context, _ string, a *agentturn.Agent) (context.Context, func(), error) {
				return ctx, a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
					if _, ok := ev.(*agentturn.TurnStart); ok {
						return boom
					}
					return nil
				}), nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			model := &recordingModel{Adapter: &echo.Adapter{}, onRequest: func(openresponses.Request) { calls++ }}
			store := &MemoryStore{}
			exec := New(agentturn.Config{Model: model}, WithConversationStore(store), WithRecorderFor(tt.fn))
			res, err := a2asrv.NewHandler(exec).OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: userMessage("hello")})
			task, _ := res.(*a2a.Task)
			switch {
			case tt.sendErr != "" && (err == nil || !strings.Contains(err.Error(), tt.sendErr) || res != nil):
				t.Errorf("res = %+v, err = %v; want no result and an error with %q", res, err, tt.sendErr)
			case tt.sendErr == "" && (err != nil || task.Status.State != a2a.TaskStateFailed || !strings.Contains(taskText(task), boom.Error())):
				t.Errorf("task = %+v, err = %v; want failed with %v", task, err, boom)
			}
			if calls != tt.calls {
				t.Errorf("model calls = %d, want %d", calls, tt.calls)
			}
			if tt.sendErr != "" && len(store.conversations) != 0 {
				t.Errorf("stored %v after a recorder that did not open", store.conversations)
			}
		})
	}
}

// blockingModel echoes, holding its first call until release closes.
type blockingModel struct {
	*echo.Adapter
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	inputs  []int
}

func (m *blockingModel) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.mu.Lock()
	m.inputs = append(m.inputs, len(req.Input))
	m.mu.Unlock()
	first := false
	m.once.Do(func() { first = true })
	if first {
		close(m.entered)
		<-m.release
	}
	return m.Adapter.CreateStream(ctx, req, sink)
}

// TestBusyConversationRefusesASecondTask pins that a message on a
// context ID with a task in flight is refused at once with
// ErrConversationBusy, and the task in flight completes and saves its
// turn.
func TestBusyConversationRefusesASecondTask(t *testing.T) {
	model := &blockingModel{Adapter: &echo.Adapter{}, entered: make(chan struct{}), release: make(chan struct{})}
	store := &MemoryStore{}
	exec := New(agentturn.Config{Model: model}, WithConversationStore(store))
	h := a2asrv.NewHandler(exec)

	first := make(chan *a2a.Task, 1)
	go func() {
		msg := userMessage("first")
		msg.ContextID = "c1"
		res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: msg})
		if err != nil {
			t.Errorf("first send: %v", err)
		}
		task, _ := res.(*a2a.Task)
		first <- task
	}()
	<-model.entered
	second := userMessage("second")
	second.ContextID = "c1"
	res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: second})
	if !errors.Is(err, ErrConversationBusy) || !errors.Is(err, a2a.ErrInvalidRequest) || res != nil {
		t.Errorf("second send = %+v, %v; want refused as busy", res, err)
	}
	close(model.release)
	if task := <-first; task == nil || task.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("first task = %+v", task)
	}
	if tr, _ := store.Load(context.Background(), "c1"); len(tr) != 2 {
		t.Errorf("stored %d items, want the first turn", len(tr))
	}
	exec.mu.Lock()
	defer exec.mu.Unlock()
	if len(exec.busy) != 0 {
		t.Errorf("busy conversations left: %v", exec.busy)
	}
}

// TestSendToOwnConversationFailsFast pins that a served agent whose
// tool sends a message to the conversation it is running in gets the
// busy error back as the tool's failure, rather than waiting on itself.
func TestSendToOwnConversationFailsFast(t *testing.T) {
	var h a2asrv.RequestHandler
	var sendErr error
	self := agenttool.New("self", "sends to its own conversation", func(ctx context.Context, in struct {
		Q string `json:"q"`
	}) (string, error) {
		msg := userMessage(in.Q)
		msg.ContextID = "c1"
		_, sendErr = h.OnSendMessage(ctx, &a2a.MessageSendParams{Message: msg})
		return "", sendErr
	})
	h = a2asrv.NewHandler(New(agentturn.Config{Model: callsEveryTool{}, Tools: []agenttool.Tool{self}}))

	done := make(chan *a2a.Task, 1)
	go func() {
		msg := userMessage("go")
		msg.ContextID = "c1"
		res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: msg})
		if err != nil {
			t.Errorf("send: %v", err)
		}
		task, _ := res.(*a2a.Task)
		done <- task
	}()
	select {
	case task := <-done:
		if task == nil || task.Status.State != a2a.TaskStateCompleted {
			t.Errorf("task = %+v", task)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the agent waited on its own conversation")
	}
	if !errors.Is(sendErr, ErrConversationBusy) {
		t.Errorf("the tool's send err = %v, want %v", sendErr, ErrConversationBusy)
	}
}
