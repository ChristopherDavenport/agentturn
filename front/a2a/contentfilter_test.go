package a2a

import (
	"context"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
)

// filtered answers as a provider's content filter does: a call to the
// first tool it is offered when call is set, then a message with a
// preamble and a refusal, ended incomplete with content_filter.
type filtered struct{ call bool }

func (m filtered) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if m.call {
		w, err := em.FunctionCall("c1", req.Tools[0].(*openresponses.FunctionTool).Name)
		if err != nil {
			return err
		}
		if err := w.Arguments(`{}`); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
	}
	w, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if err := w.Text("Sure, "); err != nil {
		return err
	}
	if err := w.Refusal("I can't help with that."); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Incomplete(openresponses.IncompleteReasonContentFilter)
}

// TestContentFilterStopRejectsTask pins #314: a run the provider's
// content filter stopped rejects the task with FilteredText, as a
// guard's stop does with RefusedText, rather than completing it with
// the text before the refusal, and the agent runs none of the refused
// response's calls.
func TestContentFilterStopRejectsTask(t *testing.T) {
	for _, tc := range []struct {
		name string
		call bool
	}{
		{"a message", false},
		{"a call and a message", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ran := 0
			probe := agenttool.New("probe", "", func(context.Context, agenttool.NoArgs) (string, error) {
				ran++
				return "open", nil
			})
			cfg := agentturn.Config{Model: filtered{call: tc.call}, ModelName: "m", Tools: []agenttool.Tool{probe}}
			task := sendTask(t, a2asrv.NewHandler(New(cfg)), userMessage("probe a"))
			if task.Status.State != a2a.TaskStateRejected || task.Status.Message == nil || partsText(task.Status.Message.Parts) != FilteredText {
				t.Errorf("task = %s %q, want rejected %q", task.Status.State, taskText(task), FilteredText)
			}
			if ran != 0 {
				t.Errorf("the agent's tool ran %d times", ran)
			}
		})
	}
}
