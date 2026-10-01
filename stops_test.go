package agentturn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// counting is the echo model, counting its calls.
type counting struct{ calls int }

func (m *counting) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.calls++
	return (&echo.Adapter{}).CreateStream(ctx, req, sink)
}

// TestGuardBeforeTheCallStops pins #116: an error wrapping ErrGuard from
// the hooks that run before the model call stops the run as a policy
// stop, with no model call, a model_blocked carrying the request from
// BeforeModelCall; any other error is still a failure.
func TestGuardBeforeTheCallStops(t *testing.T) {
	spent := fmt.Errorf("%w: cost limit reached", ErrGuard)
	boom := errors.New("boom")
	for _, tc := range []struct {
		name        string
		turn        func(context.Context, TurnStartInfo) (openresponses.Items, error)
		call        func(context.Context, *openresponses.Request) error
		reason      Reason
		wantErr     error
		wantBlocked bool
	}{
		{name: "before turn, guard", turn: func(context.Context, TurnStartInfo) (openresponses.Items, error) { return nil, spent }, reason: ReasonStopped, wantErr: spent},
		{name: "before model call, guard", call: func(context.Context, *openresponses.Request) error { return spent }, reason: ReasonStopped, wantErr: spent, wantBlocked: true},
		{name: "before turn, failure", turn: func(context.Context, TurnStartInfo) (openresponses.Items, error) { return nil, boom }, reason: ReasonError, wantErr: boom},
		{name: "before model call, failure", call: func(context.Context, *openresponses.Request) error { return boom }, reason: ReasonError, wantErr: boom, wantBlocked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			model := &counting{}
			a := New(Config{Model: model, BeforeTurn: tc.turn, BeforeModelCall: tc.call})
			var types []string
			a.Subscribe(func(_ context.Context, ev Event) error {
				types = append(types, ev.EventType())
				return nil
			})
			end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
			if end.Reason != tc.reason || !errors.Is(end.Err, tc.wantErr) {
				t.Fatalf("end = %s %v", end.Reason, end.Err)
			}
			if tc.reason == ReasonStopped && end.Cause != StopGuard {
				t.Errorf("cause = %q", end.Cause)
			}
			if model.calls != 0 {
				t.Errorf("the model was called %d times", model.calls)
			}
			blocked := false
			for _, typ := range types {
				blocked = blocked || typ == EventModelBlocked
				if typ == EventTurnStart {
					t.Errorf("a turn_start for a turn that made no call: %v", types)
				}
			}
			if blocked != tc.wantBlocked {
				t.Errorf("model_blocked raised = %v in %v", blocked, types)
			}
		})
	}
}

// TestSteerWhileIdleJoinsTheFirstCall pins #123: an item steered while
// the agent is idle is in the first turn's request and inputs, after
// the prompt, rather than one model call late; a refusal on Resume
// leaves it for the run that follows.
func TestSteerWhileIdleJoinsTheFirstCall(t *testing.T) {
	a := New(Config{Model: &echo.Adapter{}})
	var first *TurnStart
	a.Subscribe(func(_ context.Context, ev Event) error {
		if e, ok := ev.(*TurnStart); ok && first == nil {
			first = e
		}
		return nil
	})
	a.Steer(openresponses.UserText("also this"))
	if _, err := a.Prompt(context.Background(), openresponses.UserText("do that")); err != nil {
		t.Fatal(err)
	}
	if first == nil || len(first.Inputs) != 2 || first.Inputs[1].(*openresponses.Message).Text() != "also this" {
		t.Fatalf("first turn's inputs = %+v", first)
	}
	in := first.Request.Input
	if len(in) != 2 || in[0].(*openresponses.Message).Text() != "do that" {
		t.Errorf("first request input = %+v", in)
	}
}

// TestQueueCarriesItsTrigger checks that Queue reports the trigger on
// the context with each item, and that Steer and FollowUp report none.
func TestQueueCarriesItsTrigger(t *testing.T) {
	a := New(Config{Model: &echo.Adapter{}})
	var got []*Queued
	a.Subscribe(func(_ context.Context, ev Event) error {
		if q, ok := ev.(*Queued); ok {
			got = append(got, q)
		}
		return nil
	})
	cron := Trigger{Kind: "cron", Ref: "03:00", Source: "scheduler"}
	a.Queue(ContextWithTrigger(context.Background(), cron), QueueFollowUp, openresponses.UserText("nightly"))
	a.Steer(openresponses.UserText("plain"))
	a.Queue(context.Background(), "bogus", openresponses.UserText("unknown mode"))
	if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || !reflect.DeepEqual(got[0].Trigger, cron) || got[0].Mode != QueueFollowUp || !got[1].Trigger.IsZero() || got[1].Mode != QueueSteer || got[2].Mode != QueueFollowUp {
		t.Errorf("queued = %+v", got)
	}
}

// TestTriggerExtraRefusedWhereItEnters checks that a trigger whose
// Extra cannot be written is refused by the call that brings it in, so
// nothing is queued and nothing runs, and the next run on a plain
// context is unaffected.
func TestTriggerExtraRefusedWhereItEnters(t *testing.T) {
	enter := map[string]func(ctx context.Context, a *Agent) error{
		"Prompt": func(ctx context.Context, a *Agent) error {
			_, err := a.Prompt(ctx, openresponses.UserText("go"))
			return err
		},
		"Queue": func(ctx context.Context, a *Agent) error {
			return a.Queue(ctx, QueueFollowUp, openresponses.UserText("queued"))
		},
		"Deliver": func(ctx context.Context, a *Agent) error {
			_, _, err := a.Deliver(ctx, openresponses.UserText("delivered"))
			return err
		},
		"Run": func(ctx context.Context, _ *Agent) error {
			var end *RunEnd
			for ev := range Run(ctx, nil, openresponses.Items{openresponses.UserText("go")}, Config{Model: &echo.Adapter{}}) {
				if e, ok := ev.(*RunEnd); ok {
					end = e
				}
			}
			return end.Err
		},
	}
	extras := map[string]map[string]any{
		"a trigger member": {"kind": "forged"},
		"not JSON":         {"due_at": func() {}},
	}
	for name, fn := range enter {
		for bad, extra := range extras {
			t.Run(name+", "+bad, func(t *testing.T) {
				a := New(Config{Model: &echo.Adapter{}})
				var events int
				a.Subscribe(func(context.Context, Event) error { events++; return nil })
				ctx := ContextWithTrigger(context.Background(), Trigger{Kind: "cron", Extra: extra})
				if err := fn(ctx, a); !errors.Is(err, ErrTriggerExtra) {
					t.Fatalf("err = %v, want ErrTriggerExtra", err)
				}
				if st := a.State(); st.Steering != 0 || st.FollowUps != 0 || len(st.Transcript) != 0 || events != 0 {
					t.Fatalf("state after a refusal = %+v, %d events", st, events)
				}
				end, err := a.Prompt(context.Background(), openresponses.UserText("next"))
				if err != nil || end.Reason != ReasonDone || len(a.State().Transcript) != 2 {
					t.Errorf("the next prompt: err=%v end=%+v", err, end)
				}
			})
		}
	}
}

// TestRunEndAnswer pins the answer #138's consumers share: the assistant
// message that ends the run's items, when it has text.
func TestRunEndAnswer(t *testing.T) {
	call := &openresponses.FunctionCall{CallID: "c1", Name: "lookup", Arguments: `{}`}
	raw := &openresponses.UnknownItem{Type: "hermes:raw", Raw: json.RawMessage(`{"type":"hermes:raw"}`)}
	for _, tc := range []struct {
		name  string
		items Transcript
		want  string
		ok    bool
		// withheld marks the run's message withheld by OutputGuard.
		withheld bool
	}{
		{"nothing", nil, "", false, false},
		{"an answer", Transcript{openresponses.UserText("q"), openresponses.AssistantText("a")}, "a", true, false},
		{"an empty message", Transcript{openresponses.UserText("q"), openresponses.AssistantText("")}, "", false, false},
		{"a preamble and a call", Transcript{openresponses.UserText("q"), openresponses.AssistantText("Let me check."), call, openresponses.NewFunctionCallOutput("c1", "found")}, "", false, false},
		{"a user message last", Transcript{openresponses.AssistantText("a"), openresponses.UserText("and?")}, "", false, false},
		{"an extension item after the answer", Transcript{openresponses.UserText("q"), openresponses.AssistantText("a"), raw}, "a", true, false},
		{"extension items after the answer", Transcript{openresponses.UserText("q"), openresponses.AssistantText("a"), raw, nil, raw}, "a", true, false},
		{"an extension item after a call's output", Transcript{openresponses.UserText("q"), call, openresponses.NewFunctionCallOutput("c1", "found"), raw}, "", false, false},
		{"only extension items", Transcript{raw}, "", false, false},
		{"an extension item after a withheld answer", Transcript{openresponses.UserText("q"), openresponses.AssistantText("a"), raw}, "", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := (&RunEnd{Items: tc.items, Withheld: tc.withheld}).Answer()
			if got != tc.want || ok != tc.ok {
				t.Errorf("Answer() = %q, %v, want %q, %v", got, ok, tc.want, tc.ok)
			}
		})
	}
}

// TestOutputGuardStops pins #181: an OutputGuard error wrapping ErrGuard
// stops the run as a policy stop, with the message it ruled on kept out
// of the transcript and no item_end for it; any other error still fails
// the run.
func TestOutputGuardStops(t *testing.T) {
	rule := fmt.Errorf("%w: pii rule", ErrGuard)
	boom := errors.New("boom")
	for _, tc := range []struct {
		name   string
		err    error
		reason Reason
		cause  StopCause
	}{
		{name: "guard", err: rule, reason: ReasonStopped, cause: StopGuard},
		{name: "failure", err: boom, reason: ReasonError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := New(Config{Model: &echo.Adapter{}, OutputGuard: func(context.Context, OutputInfo) (*openresponses.Message, error) {
				return nil, tc.err
			}})
			var assistantEnds, turnEnds int
			a.Subscribe(func(_ context.Context, ev Event) error {
				switch e := ev.(type) {
				case *ItemEnd:
					if m, ok := e.Item.(*openresponses.Message); ok && m.Role == openresponses.RoleAssistant {
						assistantEnds++
					}
				case *TurnEnd:
					turnEnds++
				}
				return nil
			})
			end, _ := a.Prompt(context.Background(), openresponses.UserText("call 555-0100"))
			if end.Reason != tc.reason || end.Cause != tc.cause || !errors.Is(end.Err, tc.err) {
				t.Fatalf("end = %s %q %v", end.Reason, end.Cause, end.Err)
			}
			if got := itemTypes(a.State().Transcript); got != "user" {
				t.Errorf("transcript = %s", got)
			}
			if _, ok := end.Answer(); ok || assistantEnds != 0 || turnEnds != 0 {
				t.Errorf("answer=%v assistant item_end=%d turn_end=%d", ok, assistantEnds, turnEnds)
			}
		})
	}
}

// callThenSpeak answers its first request with a function call and then
// a message in one response, and every later one with a message.
type callThenSpeak struct{ calls int }

func (m *callThenSpeak) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.calls++
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if m.calls == 1 {
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
	if err := w.Text(fmt.Sprintf("reply %d", m.calls)); err != nil {
		return err
	}
	return em.Complete()
}

// TestOutputGuardStopClosesTheResponsesCalls pins #181: a call the
// withheld response completed before the message is answered with
// WithheldCallOutput, never run and never carrying the guard's text, so
// the next prompt goes ahead.
func TestOutputGuardStopClosesTheResponsesCalls(t *testing.T) {
	rule := fmt.Errorf("%w: secret rule", ErrGuard)
	ran := 0
	tool := agenttool.New("upper", "", func(context.Context, struct {
		Text string `json:"text"`
	}) (string, error) {
		ran++
		return "", nil
	})
	model := &callThenSpeak{}
	a := New(Config{Model: model, Tools: []agenttool.Tool{tool}, OutputGuard: func(_ context.Context, info OutputInfo) (*openresponses.Message, error) {
		if info.Message.Text() == "reply 1" {
			return nil, rule
		}
		return nil, nil
	}})
	var toolEvents int
	a.Subscribe(func(_ context.Context, ev Event) error {
		switch ev.(type) {
		case *ToolStart, *ToolDispatch, *ToolEnd:
			toolEvents++
		}
		return nil
	})
	end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
	if end.Reason != ReasonStopped || end.Cause != StopGuard || !errors.Is(end.Err, rule) {
		t.Fatalf("end = %s %q %v", end.Reason, end.Cause, end.Err)
	}
	if got := itemTypes(a.State().Transcript); got != "user function_call function_call_output" {
		t.Fatalf("transcript = %s", got)
	}
	out := a.State().Transcript[2].(*openresponses.FunctionCallOutput)
	if out.CallID != "c1" || out.Output.Text != WithheldCallOutput {
		t.Errorf("output = %+v", out)
	}
	if len(end.Pending) != 0 || ran != 0 || toolEvents != 0 {
		t.Errorf("pending = %v, tool ran %d times, tool events %d", end.Pending, ran, toolEvents)
	}
	if got := itemTypes(end.Items); got != "user function_call function_call_output" {
		t.Errorf("run items = %s", got)
	}
	end, err := a.Prompt(context.Background(), openresponses.UserText("again"))
	if err != nil || end.Reason != ReasonDone {
		t.Fatalf("next prompt: err = %v, end = %+v", err, end)
	}
	if answer, _ := end.Answer(); answer != "reply 2" {
		t.Errorf("answer = %q", answer)
	}
}

// withholding answers with a function call, a message, or both, before
// a message an OutputGuard withholds, all in one response with usage.
// doneOnly sends output_item.done alone for each item, as some
// servers do.
type withholding struct {
	call, preamble, doneOnly bool
}

func (m withholding) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	if m.doneOnly {
		next := sink
		sink = openresponses.EventSinkFunc(func(ev openresponses.StreamEvent) error {
			switch ev.(type) {
			case *openresponses.ResponseCreatedEvent, *openresponses.ResponseInProgressEvent,
				*openresponses.OutputItemDoneEvent, *openresponses.ResponseCompletedEvent:
				return next.Send(ev)
			}
			return nil
		})
	}
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if m.call {
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
	if m.preamble {
		w, err := em.Message(openresponses.PhaseCommentary)
		if err != nil {
			return err
		}
		if err := w.Text("Let me tell you."); err != nil {
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
	if err := w.Text("SECRET"); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	em.Response().Usage = &openresponses.Usage{InputTokens: 7, OutputTokens: 5, TotalTokens: 12}
	return em.Complete()
}

// TestOutputGuardWithheldResponse pins the record of a withheld
// response: a response_end with Withheld set, incomplete with
// content_filter, carrying the usage of the whole response and as
// output what the transcript took from it, before the outputs closing
// its calls; a run end with Withheld set and no answer, whatever
// message the response spoke before the withheld one; and on a stream
// that sends output_item.done alone, the calls before the message
// dropped with it.
func TestOutputGuardWithheldResponse(t *testing.T) {
	tool := agenttool.New("upper", "", func(context.Context, struct {
		Text string `json:"text"`
	}) (string, error) {
		return "", nil
	})
	for _, tc := range []struct {
		name       string
		model      withholding
		output     string
		transcript string
		events     string
	}{
		{name: "alone", model: withholding{}, output: "", transcript: "user", events: "response_end run_end"},
		{name: "after a preamble", model: withholding{preamble: true}, output: "assistant", transcript: "user assistant", events: "response_end run_end"},
		{name: "after a call", model: withholding{call: true}, output: "function_call", transcript: "user function_call function_call_output", events: "response_end item_start item_end run_end"},
		{name: "after a call and a preamble", model: withholding{call: true, preamble: true}, output: "function_call assistant", transcript: "user function_call assistant function_call_output", events: "response_end item_start item_end run_end"},
		{name: "done only, after a call", model: withholding{call: true, doneOnly: true}, output: "", transcript: "user", events: "response_end run_end"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := New(Config{Model: tc.model, Tools: []agenttool.Tool{tool}, OutputGuard: func(_ context.Context, info OutputInfo) (*openresponses.Message, error) {
				if info.Message.Text() == "SECRET" {
					return nil, fmt.Errorf("%w: secret", ErrGuard)
				}
				return nil, nil
			}})
			var withheld *ResponseEnd
			var after []string
			a.Subscribe(func(_ context.Context, ev Event) error {
				if e, ok := ev.(*ResponseEnd); ok && e.Withheld {
					withheld = e
				}
				if withheld != nil {
					after = append(after, ev.EventType())
				}
				return nil
			})
			end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
			if end.Reason != ReasonStopped || end.Cause != StopGuard || !end.Withheld {
				t.Fatalf("end = %s %q withheld=%v", end.Reason, end.Cause, end.Withheld)
			}
			if text, ok := end.Answer(); ok {
				t.Errorf("Answer() = %q, true, want none", text)
			}
			if got := itemTypes(a.State().Transcript); got != tc.transcript {
				t.Errorf("transcript = %s, want %s", got, tc.transcript)
			}
			if withheld == nil {
				t.Fatal("no withheld response_end")
			}
			resp := withheld.Response
			if resp.Status != openresponses.ResponseStatusIncomplete || resp.IncompleteDetails == nil || resp.IncompleteDetails.Reason != openresponses.IncompleteReasonContentFilter || resp.Error != nil {
				t.Errorf("response = %s %+v %+v", resp.Status, resp.IncompleteDetails, resp.Error)
			}
			if resp.Usage == nil || resp.Usage.TotalTokens != 12 {
				t.Errorf("usage = %+v, want the whole response's", resp.Usage)
			}
			if got := itemTypes(resp.Output); got != tc.output {
				t.Errorf("response output = %q, want %q", got, tc.output)
			}
			if resp.ID == "" {
				t.Error("response has no ID")
			}
			if got := fmt.Sprint(after); got != "["+tc.events+"]" {
				t.Errorf("events from response_end = %s, want [%s]", got, tc.events)
			}
		})
	}
}
