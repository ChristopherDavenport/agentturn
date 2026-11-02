package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
)

// worksOnce calls work until an output is in, then answers with the
// user's messages it was sent, and keeps every request's user texts.
type worksOnce struct {
	mu       sync.Mutex
	requests [][]string
}

func (m *worksOnce) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
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
	m.mu.Lock()
	m.requests = append(m.requests, users)
	m.mu.Unlock()
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

func (m *worksOnce) seen() [][]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([][]string(nil), m.requests...)
}

var nonBlocking = &a2a.MessageSendConfig{Blocking: new(bool)}

// finalTask polls h until task is final.
func finalTask(t *testing.T, h a2asrv.RequestHandler, id a2a.TaskID) *a2a.Task {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		task, err := h.OnGetTask(context.Background(), &a2a.TaskQueryParams{ID: id})
		if err == nil && task.Status.State.Terminal() {
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %s never ended", id)
	return nil
}

// TestASteerReachesTheWorkingTask pins #287: under WithDeliverWhileBusy
// a message naming a working task is delivered to its agent, and the
// send is answered with that task; a steer during a blocked tool
// reaches the next model call within the same task, and one that
// arrives as the run ends starts a run that belongs to the task. A
// data part is refused while the task runs, a message naming only the
// context is still refused as busy, and without the option the steer
// is refused.
func TestASteerReachesTheWorkingTask(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
		// steer is the message sent while the task works; atEnd sends
		// it as the first run's end is delivered rather than during
		// the tool.
		steer func(task *a2a.Task) *a2a.Message
		atEnd bool
		// refused is whether the steer is refused, and refusedAs the
		// error it is refused with, when the test names one.
		refused   bool
		refusedAs error
	}{
		{
			name: "during a blocked tool",
			opts: []Option{WithDeliverWhileBusy()},
			steer: func(task *a2a.Task) *a2a.Message {
				return a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.TextPart{Text: "also check the cache"})
			},
		},
		{
			name: "as the run ends",
			opts: []Option{WithDeliverWhileBusy()},
			steer: func(task *a2a.Task) *a2a.Message {
				return a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.TextPart{Text: "also check the cache"})
			},
			atEnd: true,
		},
		{
			name: "a data part",
			opts: []Option{WithDeliverWhileBusy()},
			steer: func(task *a2a.Task) *a2a.Message {
				return a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.DataPart{Data: map[string]any{"type": "function_call_output", "call_id": "x", "output": "y"}})
			},
			refused:   true,
			refusedAs: a2a.ErrInvalidParams,
		},
		{
			name: "naming only the context",
			opts: []Option{WithDeliverWhileBusy()},
			steer: func(task *a2a.Task) *a2a.Message {
				m := userMessage("also check the cache")
				m.ContextID = task.ContextID
				return m
			},
			refused:   true,
			refusedAs: ErrConversationBusy,
		},
		{
			name: "without the option",
			steer: func(task *a2a.Task) *a2a.Message {
				return a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.TextPart{Text: "also check the cache"})
			},
			refused: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			model := &worksOnce{}
			entered, release := make(chan struct{}), make(chan struct{})
			work := agenttool.New("work", "works until released", func(ctx context.Context, _ struct{}) (string, error) {
				close(entered)
				if !tc.atEnd {
					<-release
				}
				return "worked", nil
			})
			store := &MemoryStore{}
			var h a2asrv.RequestHandler
			var steered *a2a.Task
			var steerErr error
			var once sync.Once
			opts := append([]Option{WithConversationStore(store)}, tc.opts...)
			var started *a2a.Task
			if tc.atEnd {
				opts = append(opts, WithRecorderFor(func(ctx context.Context, _ string, a *agentturn.Agent) (context.Context, func(), error) {
					return ctx, a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
						if _, ok := ev.(*agentturn.RunEnd); ok {
							once.Do(func() {
								res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: tc.steer(started), Config: nonBlocking})
								steered, _ = res.(*a2a.Task)
								steerErr = err
							})
						}
						return nil
					}), nil
				}))
			}
			h = New(agentturn.Config{Model: model, Tools: []agenttool.Tool{work}}, opts...).RequestHandler()
			first := userMessage("lint the repo")
			first.ContextID = "c1"
			res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: first, Config: nonBlocking})
			if err != nil {
				t.Fatal(err)
			}
			started = res.(*a2a.Task)
			<-entered
			if !tc.atEnd {
				// A refusal comes at once, a blocking send's included.
				cfg := nonBlocking
				if tc.refused {
					cfg = nil
				}
				res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: tc.steer(started), Config: cfg})
				steered, _ = res.(*a2a.Task)
				steerErr = err
				close(release)
			}
			final := finalTask(t, h, started.ID)
			if final.Status.State != a2a.TaskStateCompleted {
				t.Fatalf("task ended %s", final.Status.State)
			}
			requests := model.seen()
			if tc.refused {
				if steerErr == nil {
					t.Fatalf("the steer was accepted: %+v", steered)
				}
				if tc.refusedAs != nil && !errors.Is(steerErr, tc.refusedAs) {
					t.Errorf("steer err = %v, want %v", steerErr, tc.refusedAs)
				}
				for _, users := range requests {
					if strings.Contains(strings.Join(users, "|"), "also check the cache") {
						t.Errorf("a refused steer reached the model: %q", users)
					}
				}
				return
			}
			if steerErr != nil {
				t.Fatalf("steer: %v", steerErr)
			}
			if steered == nil || steered.ID != started.ID || steered.Status.State.Terminal() {
				t.Errorf("the steer was answered with %+v, want the working task %s", steered, started.ID)
			}
			last := requests[len(requests)-1]
			if len(requests) < 2 || len(last) != 2 || last[1] != "also check the cache" {
				t.Errorf("requests = %q, want the steer in the task's next model call", requests)
			}
			if !strings.Contains(partsText(final.Status.Message.Parts), "also check the cache") {
				t.Errorf("the task's answer = %q, want it to read the steer", partsText(final.Status.Message.Parts))
			}
			stored, _ := store.Load(context.Background(), "c1")
			var texts []string
			for _, item := range stored {
				if m, ok := item.(*openresponses.Message); ok && m.Role == openresponses.RoleUser {
					texts = append(texts, m.Text())
				}
			}
			if strings.Join(texts, "|") != "lint the repo|also check the cache" {
				t.Errorf("stored user messages = %q", texts)
			}
		})
	}
}

// asksTheCaller calls the caller's tool remote once released, then
// answers with the user's messages it was sent.
type asksTheCaller struct {
	worksOnce
	entered, release chan struct{}
	once             sync.Once
}

func (m *asksTheCaller) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	for _, item := range req.Input {
		if _, ok := item.(*openresponses.FunctionCallOutput); ok {
			return m.worksOnce.CreateStream(ctx, req, sink)
		}
	}
	m.once.Do(func() { close(m.entered) })
	<-m.release
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	w, err := em.FunctionCall("", "remote")
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

// TestASteerWaitsForTheAnswerToPendingCalls pins the delivery's rule
// for a task that stops for input (#287): a steer the run did not take
// before it stopped waits for the message answering the calls, and the
// run that message resumes takes it after the answered batch.
func TestASteerWaitsForTheAnswerToPendingCalls(t *testing.T) {
	model := &asksTheCaller{entered: make(chan struct{}), release: make(chan struct{})}
	h := New(agentturn.Config{Model: model}, WithDeliverWhileBusy(),
		WithCallerTools(openresponses.NewFunctionTool("remote", "caller side", json.RawMessage(`{"type":"object"}`)))).RequestHandler()
	first := userMessage("lint the repo")
	first.ContextID = "c1"
	res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: first, Config: nonBlocking})
	if err != nil {
		t.Fatal(err)
	}
	task := res.(*a2a.Task)
	<-model.entered
	steer := a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.TextPart{Text: "also check the cache"})
	if _, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: steer, Config: nonBlocking}); err != nil {
		t.Fatalf("steer: %v", err)
	}
	close(model.release)
	asking := finalOrAsking(t, h, task.ID)
	if asking.Status.State != a2a.TaskStateInputRequired {
		t.Fatalf("task = %s, want input-required", asking.Status.State)
	}
	ids := pendingIDs(t, asking)
	answer := a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.DataPart{Data: map[string]any{"type": "function_call_output", "call_id": ids[0], "output": "found"}})
	done := sendTask(t, h, answer)
	if done.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("answered task = %s", done.Status.State)
	}
	requests := model.seen()
	if last := requests[len(requests)-1]; len(last) != 2 || last[1] != "also check the cache" {
		t.Errorf("requests = %q, want the steer after the answered call", requests)
	}
}

// finalOrAsking polls h until task is final or waits on input.
func finalOrAsking(t *testing.T, h a2asrv.RequestHandler, id a2a.TaskID) *a2a.Task {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		task, err := h.OnGetTask(context.Background(), &a2a.TaskQueryParams{ID: id})
		if err == nil && (task.Status.State.Terminal() || task.Status.State == a2a.TaskStateInputRequired) {
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %s never stopped", id)
	return nil
}
