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
				<-release
				return "worked", nil
			})
			store := &MemoryStore{}
			var h a2asrv.RequestHandler
			var steered *a2a.Task
			var steerErr error
			var once sync.Once
			// atEnd's steer is sent from the run's goroutine, and handed
			// back here.
			type sent struct {
				task *a2a.Task
				err  error
			}
			atEnd := make(chan sent, 1)
			opts := append([]Option{WithConversationStore(store)}, tc.opts...)
			var started *a2a.Task
			if tc.atEnd {
				opts = append(opts, WithRecorderFor(func(ctx context.Context, _ string, a *agentturn.Agent) (context.Context, func(), error) {
					return ctx, a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
						if _, ok := ev.(*agentturn.RunEnd); ok {
							once.Do(func() {
								res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: tc.steer(started), Config: nonBlocking})
								task, _ := res.(*a2a.Task)
								atEnd <- sent{task, err}
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
			if tc.atEnd {
				// The tool returns once the task is known here, so the
				// run's end, which steers, reads it.
				close(release)
			} else {
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
			if tc.atEnd {
				s := <-atEnd
				steered, steerErr = s.task, s.err
			}
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

// TestACancelDropsAnOwedSteer pins the review of #287: a steer left for
// the message answering a task's calls is dropped with the task when
// the caller cancels it, so the conversation's next task never reads
// it.
func TestACancelDropsAnOwedSteer(t *testing.T) {
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
	if asking := finalOrAsking(t, h, task.ID); asking.Status.State != a2a.TaskStateInputRequired {
		t.Fatalf("task = %s, want input-required", asking.Status.State)
	}
	if _, err := h.OnCancelTask(context.Background(), &a2a.TaskIDParams{ID: task.ID}); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	next := userMessage("start over")
	next.ContextID = "c1"
	if done := sendTask(t, h, next); done.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("next task = %s", done.Status.State)
	}
	for _, users := range model.seen() {
		if strings.Contains(strings.Join(users, "|"), "also check the cache") {
			t.Errorf("the next task read the canceled task's steer: %q", users)
		}
	}
}

// TestSteersAreBounded pins the review of #287: a working task takes at
// most MaxSteers steers not yet taken, and refuses the next.
func TestSteersAreBounded(t *testing.T) {
	model := &worksOnce{}
	entered, release := make(chan struct{}), make(chan struct{})
	work := agenttool.New("work", "works until released", func(ctx context.Context, _ struct{}) (string, error) {
		close(entered)
		<-release
		return "worked", nil
	})
	h := New(agentturn.Config{Model: model, Tools: []agenttool.Tool{work}}, WithDeliverWhileBusy()).RequestHandler()
	first := userMessage("lint the repo")
	first.ContextID = "c1"
	res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: first, Config: nonBlocking})
	if err != nil {
		t.Fatal(err)
	}
	task := res.(*a2a.Task)
	<-entered
	defer func() { finalTask(t, h, task.ID) }()
	defer close(release)
	for i := range MaxSteers + 1 {
		steer := a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.TextPart{Text: "more"})
		_, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: steer, Config: nonBlocking})
		if i < MaxSteers && err != nil {
			t.Fatalf("steer %d: %v", i, err)
		}
		if i == MaxSteers && !errors.Is(err, a2a.ErrInvalidRequest) {
			t.Errorf("steer past the bound: %v, want refused", err)
		}
	}
}

// redelivering is a Redeliverer standing in for a session recorder's:
// it notes each steer it is handed, then hands it to the agent.
type redelivering struct {
	mu    sync.Mutex
	texts []string
}

func (r *redelivering) Redeliver(ctx context.Context, a *agentturn.Agent, items ...openresponses.Item) error {
	r.mu.Lock()
	for _, item := range items {
		if m, ok := item.(*openresponses.Message); ok {
			r.texts = append(r.texts, m.Text())
		}
	}
	r.mu.Unlock()
	return a.Redeliver(ctx, items...)
}

// TestASteerGoesThroughTheRecorder pins the integration review of
// #287 × #244 (RFC 0001 Steering's SHOULD): a steer into a working
// task, and the same steer the task left for the message answering
// its calls, are handed to the Redeliverer the task's RecorderFor put
// on its context, which writes them before the agent holds them, and
// the agent still takes the steer after the answered batch.
func TestASteerGoesThroughTheRecorder(t *testing.T) {
	model := &asksTheCaller{entered: make(chan struct{}), release: make(chan struct{})}
	via := &redelivering{}
	recorded := func(ctx context.Context, _ string, _ *agentturn.Agent) (context.Context, func(), error) {
		return ContextWithRedeliverer(ctx, via), nil, nil
	}
	h := New(agentturn.Config{Model: model}, WithDeliverWhileBusy(), WithRecorderFor(recorded),
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
	if done := sendTask(t, h, answer); done.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("answered task = %s", done.Status.State)
	}
	via.mu.Lock()
	texts := strings.Join(via.texts, "|")
	via.mu.Unlock()
	if texts != "also check the cache|also check the cache" {
		t.Errorf("the recorder was handed %q, want the steer and the owed steer", texts)
	}
	requests := model.seen()
	if last := requests[len(requests)-1]; len(last) != 2 || last[1] != "also check the cache" {
		t.Errorf("requests = %q, want the steer after the answered call", requests)
	}
}

// worksTwice calls work until two outputs are in, then answers.
type worksTwice struct{ worksOnce }

func (m *worksTwice) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	outputs := 0
	for _, item := range req.Input {
		if _, ok := item.(*openresponses.FunctionCallOutput); ok {
			outputs++
		}
	}
	if outputs != 1 {
		return m.worksOnce.CreateStream(ctx, req, sink)
	}
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
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

// TestATakenSteerNoLongerCounts pins the integration review of the
// steer bound: MaxSteers counts the steers a task holds untaken, so
// once the agent has taken a full bound of them, as it does after a
// batch, the task takes as many again during its next call.
func TestATakenSteerNoLongerCounts(t *testing.T) {
	model := &worksTwice{}
	entered := make(chan struct{}, 2)
	release := make(chan struct{}, 2)
	work := agenttool.New("work", "works until released", func(ctx context.Context, _ struct{}) (string, error) {
		entered <- struct{}{}
		<-release
		return "worked", nil
	})
	h := New(agentturn.Config{Model: model, Tools: []agenttool.Tool{work}}, WithDeliverWhileBusy()).RequestHandler()
	first := userMessage("lint the repo")
	first.ContextID = "c1"
	res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: first, Config: nonBlocking})
	if err != nil {
		t.Fatal(err)
	}
	task := res.(*a2a.Task)
	for round := range 2 {
		<-entered
		for i := range MaxSteers {
			steer := a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.TextPart{Text: "more"})
			if _, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: steer, Config: nonBlocking}); err != nil {
				release <- struct{}{}
				release <- struct{}{}
				t.Fatalf("round %d, steer %d: %v", round, i, err)
			}
		}
		release <- struct{}{}
	}
	if done := finalTask(t, h, task.ID); done.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("task = %s", done.Status.State)
	}
}

// triggering is a Redeliverer that notes the trigger each steer was
// handed under, then hands it to the agent.
type triggering struct {
	mu       sync.Mutex
	triggers []agentturn.Trigger
}

func (r *triggering) Redeliver(ctx context.Context, a *agentturn.Agent, items ...openresponses.Item) error {
	r.mu.Lock()
	r.triggers = append(r.triggers, agentturn.TriggerFromContext(ctx))
	r.mu.Unlock()
	return a.Redeliver(ctx, items...)
}

// TestTwoSteersWithOneTextAreTwo pins the round-two review of #244 ×
// #287: a steer's trigger names the A2A message that carried it, so a
// recorder that takes a steer it already owes under its entry rather
// than twice still takes two real steers with the same text as two.
func TestTwoSteersWithOneTextAreTwo(t *testing.T) {
	model := &worksOnce{}
	entered, release := make(chan struct{}), make(chan struct{})
	work := agenttool.New("work", "works until released", func(ctx context.Context, _ struct{}) (string, error) {
		close(entered)
		<-release
		return "worked", nil
	})
	via := &triggering{}
	recorded := func(ctx context.Context, _ string, _ *agentturn.Agent) (context.Context, func(), error) {
		return ContextWithRedeliverer(ctx, via), nil, nil
	}
	h := New(agentturn.Config{Model: model, Tools: []agenttool.Tool{work}}, WithDeliverWhileBusy(), WithRecorderFor(recorded)).RequestHandler()
	first := userMessage("lint the repo")
	first.ContextID = "c1"
	res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: first, Config: nonBlocking})
	if err != nil {
		t.Fatal(err)
	}
	task := res.(*a2a.Task)
	<-entered
	var ids []string
	for range 2 {
		steer := a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.TextPart{Text: "again"})
		ids = append(ids, steer.ID)
		if _, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: steer, Config: nonBlocking}); err != nil {
			t.Fatalf("steer: %v", err)
		}
	}
	close(release)
	finalTask(t, h, task.ID)
	via.mu.Lock()
	defer via.mu.Unlock()
	if len(via.triggers) != 2 {
		t.Fatalf("the recorder was handed %d steers, want 2", len(via.triggers))
	}
	for i, tr := range via.triggers {
		if tr.Extra["message"] != ids[i] {
			t.Errorf("steer %d's trigger = %+v, want it to name message %s", i, tr, ids[i])
		}
	}
}
