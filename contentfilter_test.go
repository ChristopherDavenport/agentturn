package agentturn

import (
	"context"
	"fmt"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// filtering answers its first request as a provider's content filter
// does: optionally a function call, then a message holding a refusal
// part, and response.incomplete with content_filter and an extension
// member in its details. Every later request is answered with a
// message.
type filtering struct {
	call  bool
	calls int
}

func (m *filtering) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.calls++
	if m.calls == 1 {
		next := sink
		sink = openresponses.EventSinkFunc(func(ev openresponses.StreamEvent) error {
			if e, ok := ev.(*openresponses.ResponseIncompleteEvent); ok {
				e.Response.IncompleteDetails.Extra = map[string]any{"anthropic.refusal_category": "cyber"}
			}
			return next.Send(ev)
		})
	}
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if m.calls == 1 && m.call {
		fc, err := em.FunctionCall("c1", "upper")
		if err != nil {
			return err
		}
		if err := fc.Arguments(`{"text":"x"}`); err != nil {
			return err
		}
		if err := fc.Close(); err != nil {
			return err
		}
	}
	w, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if m.calls > 1 {
		if err := w.Text(fmt.Sprintf("reply %d", m.calls)); err != nil {
			return err
		}
		return em.Complete()
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

// TestContentFilterStops pins #314: a response the provider ended
// incomplete with content_filter stops the run with StopContentFilter.
// Its calls are answered with RefusedCallOutput and never run, the end
// carries the details and the refusal text and is no answer, no
// turn_end is raised, a steer taken during the turn stays queued, and
// the next prompt goes ahead.
func TestContentFilterStops(t *testing.T) {
	for _, tc := range []struct {
		name  string
		call  bool
		items string
	}{
		{name: "with a call", call: true, items: "user function_call assistant function_call_output"},
		{name: "without calls", items: "user assistant"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ran := 0
			tool := agenttool.New("upper", "", func(context.Context, struct {
				Text string `json:"text"`
			}) (string, error) {
				ran++
				return "", nil
			})
			model := &filtering{call: tc.call}
			a := New(Config{Model: model, Tools: []agenttool.Tool{tool}})
			var toolEvents, turnEnds int
			steered := false
			a.Subscribe(func(_ context.Context, ev Event) error {
				switch ev.(type) {
				case *ToolStart, *ToolDispatch, *ToolEnd:
					toolEvents++
				case *TurnEnd:
					turnEnds++
				case *ResponseEnd:
					if !steered {
						steered = true
						a.Steer(openresponses.UserText("and also"))
					}
				}
				return nil
			})
			end, err := a.Prompt(context.Background(), openresponses.UserText("go"))
			if err != nil {
				t.Fatal(err)
			}
			if end.Reason != ReasonStopped || end.Cause != StopContentFilter || end.Err != nil || end.Withheld {
				t.Fatalf("end = %s %q %v withheld=%v", end.Reason, end.Cause, end.Err, end.Withheld)
			}
			if end.Incomplete == nil || end.Incomplete.Reason != openresponses.IncompleteReasonContentFilter || end.Incomplete.Extra["anthropic.refusal_category"] != "cyber" {
				t.Errorf("incomplete = %+v", end.Incomplete)
			}
			if end.Refusal != "I can't help with that." {
				t.Errorf("refusal = %q", end.Refusal)
			}
			if text, ok := end.Answer(); ok {
				t.Errorf("answer = %q", text)
			}
			if got := itemTypes(end.Items); got != tc.items {
				t.Errorf("run items = %s", got)
			}
			if tc.call {
				out := end.Items[3].(*openresponses.FunctionCallOutput)
				if out.CallID != "c1" || out.Output.Text != RefusedCallOutput {
					t.Errorf("output = %+v", out)
				}
			}
			if len(end.Pending) != 0 || ran != 0 || toolEvents != 0 || turnEnds != 0 {
				t.Errorf("pending = %v, tool ran %d times, tool events %d, turn_end %d", end.Pending, ran, toolEvents, turnEnds)
			}
			if st := a.State(); st.Steering != 1 {
				t.Errorf("steering = %d, want the steer left queued", st.Steering)
			}
			end, err = a.Prompt(context.Background(), openresponses.UserText("again"))
			if err != nil || end.Reason != ReasonDone {
				t.Fatalf("next prompt: err = %v, end = %+v", err, end)
			}
			if answer, _ := end.Answer(); answer != "reply 2" {
				t.Errorf("answer = %q", answer)
			}
		})
	}
}

// cutOff answers its first request with a call when call is set, then
// a message cut at the output limit, its details dropped when bare;
// every later request is answered with a message that completes.
type cutOff struct {
	call, bare bool
	calls      int
}

func (m *cutOff) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.calls++
	if m.bare {
		next := sink
		sink = openresponses.EventSinkFunc(func(ev openresponses.StreamEvent) error {
			if e, ok := ev.(*openresponses.ResponseIncompleteEvent); ok {
				e.Response.IncompleteDetails = nil
			}
			return next.Send(ev)
		})
	}
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if m.calls == 1 && m.call {
		fc, err := em.FunctionCall("c1", "upper")
		if err != nil {
			return err
		}
		if err := fc.Arguments(`{"text":"x"}`); err != nil {
			return err
		}
		if err := fc.Close(); err != nil {
			return err
		}
	}
	w, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if err := w.Text(fmt.Sprintf("Once upon %d", m.calls)); err != nil {
		return err
	}
	if m.calls > 1 && m.call {
		return em.Complete()
	}
	return em.Incomplete(openresponses.IncompleteReasonMaxOutputTokens)
}

// TestOutputLimitStops pins the decision on #314's max_output_tokens
// case: a final answer cut at the output limit ends the run stopped
// with StopMaxOutputTokens and the response's details, its text still
// the answer; one whose details name no reason reads the same; a
// response cut after a call runs the call and goes on.
func TestOutputLimitStops(t *testing.T) {
	for _, tc := range []struct {
		name   string
		model  *cutOff
		reason Reason
		cause  StopCause
		answer string
	}{
		{"an answer", &cutOff{}, ReasonStopped, StopMaxOutputTokens, "Once upon 1"},
		{"an answer, no details", &cutOff{bare: true}, ReasonStopped, StopMaxOutputTokens, "Once upon 1"},
		{"a call, then an answer", &cutOff{call: true}, ReasonDone, "", "Once upon 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tool := agenttool.New("upper", "", func(context.Context, struct {
				Text string `json:"text"`
			}) (string, error) {
				return "X", nil
			})
			a := New(Config{Model: tc.model, Tools: []agenttool.Tool{tool}})
			end, err := a.Prompt(context.Background(), openresponses.UserText("go"))
			if err != nil {
				t.Fatal(err)
			}
			if end.Reason != tc.reason || end.Cause != tc.cause || end.Err != nil {
				t.Fatalf("end = %s %q %v", end.Reason, end.Cause, end.Err)
			}
			if text, ok := end.Answer(); !ok || text != tc.answer {
				t.Errorf("answer = %q, %v, want %q", text, ok, tc.answer)
			}
			cut := tc.cause == StopMaxOutputTokens
			if got := end.Incomplete != nil; got != cut || cut && end.Incomplete.Reason != openresponses.IncompleteReasonMaxOutputTokens {
				t.Errorf("incomplete = %+v", end.Incomplete)
			}
		})
	}
}

// TestContentFilterRefusalIsWhatWasKept pins #314's RunEnd.Refusal under
// an OutputGuard: it is the refusal of the messages the transcript
// kept, the guard's replacement where it made one, as Answer reads the
// replacement, never the text the guard replaced.
func TestContentFilterRefusalIsWhatWasKept(t *testing.T) {
	for _, tc := range []struct {
		name        string
		replacement *openresponses.Message
		want        string
	}{
		{"kept", nil, "I can't help with that."},
		{"replaced with text", openresponses.AssistantText("[withheld]"), ""},
		{"replaced with a refusal", &openresponses.Message{Role: openresponses.RoleAssistant, Content: openresponses.Contents{&openresponses.Refusal{Refusal: "[redacted]"}}}, "[redacted]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := New(Config{Model: &filtering{}, OutputGuard: func(context.Context, OutputInfo) (*openresponses.Message, error) {
				return tc.replacement, nil
			}})
			end, err := a.Prompt(context.Background(), openresponses.UserText("go"))
			if err != nil || end.Cause != StopContentFilter {
				t.Fatalf("end = %+v err = %v", end, err)
			}
			if end.Refusal != tc.want {
				t.Errorf("refusal = %q, want %q", end.Refusal, tc.want)
			}
		})
	}
}
