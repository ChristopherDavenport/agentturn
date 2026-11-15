package a2a

import (
	"context"
	"strings"
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
// the text before the refusal, the agent runs none of the refused
// response's calls, and the refusal text is in the task's artifacts,
// with or without an OutputGuard.
func TestContentFilterStopRejectsTask(t *testing.T) {
	for _, tc := range []struct {
		name  string
		call  bool
		guard bool
	}{
		{"a message", false, false},
		{"a call and a message", true, false},
		// Under an OutputGuard a message's artifact is written whole.
		{"a message, an output guard", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ran := 0
			probe := agenttool.New("probe", "", func(context.Context, agenttool.NoArgs) (string, error) {
				ran++
				return "open", nil
			})
			cfg := agentturn.Config{Model: filtered{call: tc.call}, ModelName: "m", Tools: []agenttool.Tool{probe}}
			if tc.guard {
				cfg.OutputGuard = func(context.Context, agentturn.OutputInfo) (*openresponses.Message, error) { return nil, nil }
			}
			task := sendTask(t, a2asrv.NewHandler(New(cfg)), userMessage("probe a"))
			if task.Status.State != a2a.TaskStateRejected || task.Status.Message == nil || partsText(task.Status.Message.Parts) != FilteredText {
				t.Errorf("task = %s %q, want rejected %q", task.Status.State, taskText(task), FilteredText)
			}
			if ran != 0 {
				t.Errorf("the agent's tool ran %d times", ran)
			}
			var artifacts strings.Builder
			for _, a := range task.Artifacts {
				artifacts.WriteString(partsText(a.Parts))
			}
			if !strings.Contains(artifacts.String(), "I can't help with that.") {
				t.Errorf("artifacts = %q, want the refusal text the caller reads", artifacts.String())
			}
		})
	}
}

// cutShort answers with a message cut at the output limit, holding no
// text when empty.
type cutShort struct{ empty bool }

func (m cutShort) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	w, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if m.empty {
		if err := w.Close(); err != nil {
			return err
		}
		return em.Incomplete(openresponses.IncompleteReasonMaxOutputTokens)
	}
	if err := w.Text("Once upon"); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Incomplete(openresponses.IncompleteReasonMaxOutputTokens)
}

// TestACutAnswerCompletesMarked pins #314's max_output_tokens decision
// here: the task completes with the cut text, which is the agent's
// answer, and its status message says it was cut under MetaIncomplete,
// the response's incomplete_details.
func TestACutAnswerCompletesMarked(t *testing.T) {
	task := sendTask(t, a2asrv.NewHandler(New(agentturn.Config{Model: cutShort{}, ModelName: "m"})), userMessage("write at length"))
	if task.Status.State != a2a.TaskStateCompleted || task.Status.Message == nil || partsText(task.Status.Message.Parts) != "Once upon" {
		t.Fatalf("task = %s %q, want completed with the cut text", task.Status.State, taskText(task))
	}
	meta, ok := task.Status.Message.Metadata[MetaIncomplete].(map[string]any)
	if !ok || meta["reason"] != "max_output_tokens" {
		t.Errorf("status metadata = %v, want %s with the reason", task.Status.Message.Metadata, MetaIncomplete)
	}
}

// TestACutAnswerWithNoTextIsMarked pins that a task whose answer the
// output limit cut before any text still says it was cut: its status
// message carries MetaIncomplete and no text.
func TestACutAnswerWithNoTextIsMarked(t *testing.T) {
	task := sendTask(t, a2asrv.NewHandler(New(agentturn.Config{Model: cutShort{empty: true}, ModelName: "m"})), userMessage("write at length"))
	if task.Status.State != a2a.TaskStateCompleted || task.Status.Message == nil {
		t.Fatalf("task = %s %+v, want completed with a status message", task.Status.State, task.Status.Message)
	}
	meta, ok := task.Status.Message.Metadata[MetaIncomplete].(map[string]any)
	if !ok || meta["reason"] != "max_output_tokens" {
		t.Errorf("status metadata = %v, want %s with the reason", task.Status.Message.Metadata, MetaIncomplete)
	}
}

// TestACutAnswerUnderTheStopHookIsMarked pins MetaIncomplete on a cut
// answer the stop hook ended.
func TestACutAnswerUnderTheStopHookIsMarked(t *testing.T) {
	cfg := agentturn.Config{Model: cutShort{}, ModelName: "m", ShouldStopAfterTurn: func(context.Context, agentturn.TurnInfo) (bool, error) { return true, nil }}
	task := sendTask(t, a2asrv.NewHandler(New(cfg)), userMessage("write at length"))
	if task.Status.State != a2a.TaskStateCompleted || task.Status.Message == nil {
		t.Fatalf("task = %s, want completed with a status message", task.Status.State)
	}
	if meta, ok := task.Status.Message.Metadata[MetaIncomplete].(map[string]any); !ok || meta["reason"] != "max_output_tokens" {
		t.Errorf("status metadata = %v", task.Status.Message.Metadata)
	}
}
