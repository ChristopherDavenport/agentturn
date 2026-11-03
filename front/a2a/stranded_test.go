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

// finishes calls finish when the user says "go" and otherwise answers,
// keeping the user texts it was sent.
type finishes struct {
	mu   sync.Mutex
	seen []string
}

func (m *finishes) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	msg, user := req.Input[len(req.Input)-1].(*openresponses.Message)
	if user && msg.Role == openresponses.RoleUser {
		m.mu.Lock()
		m.seen = append(m.seen, msg.Text())
		m.mu.Unlock()
		if msg.Text() == "go" {
			w, err := em.FunctionCall("", "finish")
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
	}
	if err := em.Item(openresponses.AssistantText("ok")); err != nil {
		return err
	}
	return em.Complete()
}

// TestADeliveryAfterATerminatingEndIsTakenInTheTask pins the round-two
// review of #274 × #290 for front/a2a: a delivery left queued by a run
// that ended on a terminating result no handoff follows is taken by a
// run of the same task, rather than lost with the task's agent.
func TestADeliveryAfterATerminatingEndIsTakenInTheTask(t *testing.T) {
	finish := agenttool.NewFunc("finish", "answers for the model", json.RawMessage(`{"type":"object"}`),
		func(context.Context, agenttool.Call) (agenttool.Result, error) {
			return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "finished"}, Terminate: true}, nil
		})
	model := &finishes{}
	done := make(chan error, 1)
	atEnd := func(ctx context.Context, _ string, a *agentturn.Agent) (context.Context, func(), error) {
		delivered := false
		unsub := a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
			if _, ok := ev.(*agentturn.RunEnd); !ok || delivered {
				return nil
			}
			delivered = true
			go func() {
				_, _, err := a.Deliver(context.Background(), openresponses.UserText("the build passed"))
				done <- err
			}()
			for a.State().Steering == 0 {
				time.Sleep(time.Millisecond)
			}
			return nil
		})
		return ctx, unsub, nil
	}
	h := a2asrv.NewHandler(New(agentturn.Config{Model: model, Tools: []agenttool.Tool{finish}}, WithRecorderFor(atEnd)))
	task := sendTask(t, h, userMessage("go"))
	if task.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("task = %s", task.Status.State)
	}
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, agentturn.ErrTerminated) {
			t.Fatalf("Deliver: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Deliver never returned")
	}
	model.mu.Lock()
	defer model.mu.Unlock()
	if strings.Join(model.seen, "|") != "go|the build passed" {
		t.Errorf("the model was sent %q, want the delivery taken in the task", model.seen)
	}
}
