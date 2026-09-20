package agentturn

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

func TestBeforeToolCallSeesTheBatch(t *testing.T) {
	var seen []string
	cfg := Config{Model: &twoCalls{}, Tools: []agenttool.Tool{agenttool.New("a", "", upper), agenttool.New("b", "", upper), agenttool.New("c", "", upper)}, MaxTurns: 1,
		BeforeToolCall: func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
			names := make([]string, len(info.Batch))
			for i, c := range info.Batch {
				names[i] = c.Name
			}
			seen = append(seen, fmt.Sprintf("%d/%d:%s@%s", info.Index, len(info.Batch), info.Call.Name, strings.Join(names, "")))
			// A policy that asks about b holds the rest of the batch.
			if info.Index >= 1 {
				return &ToolDecision{Action: Defer}, nil
			}
			return nil, nil
		}}
	_, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err != nil || end.Reason != ReasonInputRequired || len(end.Pending) != 2 {
		t.Fatalf("end = %+v err=%v", end, err)
	}
	if strings.Join(seen, " ") != "0/3:a@abc 1/3:b@abc 2/3:c@abc" {
		t.Errorf("hook saw %v", seen)
	}
	if got := itemTypes(end.Items); got != "user function_call function_call function_call function_call_output" {
		t.Errorf("items = %q", got)
	}
}

func TestRefuseEndsTheRunWithoutAModelCall(t *testing.T) {
	deferAll := func(context.Context, ToolCallInfo) (*ToolDecision, error) { return &ToolDecision{Action: Defer}, nil }
	a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)}, BeforeToolCall: deferAll})
	if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	rec.subscribe(a)
	call := a.State().Pending[0].Call
	end, err := a.Resume(context.Background(), Refuse(openresponses.NewFunctionCallOutput(call.CallID, "no, stop")))
	if err != nil || end.Reason != ReasonStopped || end.Cause != StopRefused {
		t.Fatalf("resume: err=%v end=%+v", err, end)
	}
	// The output is in the transcript, no model call was made, and the
	// transcript is a valid input.
	if got := rec.types(); strings.Join(got, " ") != "run_start item_start item_end run_end" {
		t.Errorf("events = %v", got)
	}
	st := a.State()
	if len(st.Pending) != 0 || itemTypes(st.Transcript) != "user function_call function_call_output" || !CanContinue(st.Transcript) {
		t.Errorf("state = %q pending=%v", itemTypes(st.Transcript), st.Pending)
	}
	// A plain Output still calls the model.
	if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	end, err = a.Resume(context.Background(), Output(openresponses.NewFunctionCallOutput(a.State().Pending[0].Call.CallID, "no")))
	if err != nil || end.Reason != ReasonDone {
		t.Errorf("plain output: err=%v end=%+v", err, end)
	}
	// A refusal beside an approval runs the approved call, appends the
	// refusal, then ends.
	if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	pending := a.State().Pending
	if len(pending) != 1 {
		t.Fatalf("pending = %v", pending)
	}
	end, err = a.Resume(context.Background(), Refuse(openresponses.NewFunctionCallOutput(pending[0].Call.CallID, "no")))
	if err != nil || end.Cause != StopRefused {
		t.Errorf("refuse again: err=%v end=%+v", err, end)
	}
}

func TestNotesFollowTheOutputs(t *testing.T) {
	// A hook's note is a developer message after the batch's outputs.
	cfg := Config{Model: &twoCalls{}, Tools: []agenttool.Tool{agenttool.New("a", "", upper), agenttool.New("b", "", upper)}, MaxTurns: 1,
		BeforeToolCall: func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
			if info.Call.Name == "a" {
				return &ToolDecision{Note: "a ran in the sandbox"}, nil
			}
			return nil, nil
		}}
	_, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	if got := itemTypes(end.Items); got != "user function_call function_call function_call_output function_call_output developer" {
		t.Errorf("items = %q", got)
	}
	if m := end.Items[5].(*openresponses.Message); m.Text() != "a ran in the sandbox" {
		t.Errorf("note = %q", m.Text())
	}
	// A blocked call's note is dropped: its reason is what the model
	// sees.
	cfg.BeforeToolCall = func(context.Context, ToolCallInfo) (*ToolDecision, error) {
		return &ToolDecision{Action: Block, Reason: "refused", Note: "ignored"}, nil
	}
	_, end, _ = collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if got := itemTypes(end.Items); got != "user function_call function_call function_call_output function_call_output" {
		t.Errorf("blocked items = %q", got)
	}

	// On Resume, an approval's note is a user message after the
	// approved outputs; an output answer's note follows the outputs; and
	// what was steered in while the user decided goes with them.
	deferAll := func(context.Context, ToolCallInfo) (*ToolDecision, error) { return &ToolDecision{Action: Defer}, nil }
	a := New(Config{Model: &twoCalls{}, Tools: []agenttool.Tool{agenttool.New("a", "", upper), agenttool.New("b", "", upper)}, BeforeToolCall: deferAll, MaxTurns: 1})
	if _, err := a.Prompt(context.Background(), openresponses.UserText("x")); err != nil {
		t.Fatal(err)
	}
	pending := a.State().Pending
	a.Steer(openresponses.UserText("steered while deciding"))
	end, err = a.Resume(context.Background(),
		Output(openresponses.NewFunctionCallOutput(pending[1].Call.CallID, "did it myself")).WithNote("that one I handled"),
		Approve(pending[0].Call.CallID).WithNote("but never delete build/ again"))
	if err != nil {
		t.Fatal(err)
	}
	// The output answer and its note first, then the approved call's
	// output and its note, then what was steered in; the next turn's
	// call follows.
	want := "user function_call function_call function_call_output user function_call_output user user"
	if got := itemTypes(a.State().Transcript[:8]); got != want {
		t.Errorf("transcript = %q\nwant         %q", got, want)
	}
	texts := []string{}
	for _, it := range a.State().Transcript[:8] {
		if m, ok := it.(*openresponses.Message); ok && m.Role == openresponses.RoleUser {
			texts = append(texts, m.Text())
		}
	}
	if strings.Join(texts, "|") != "x|that one I handled|but never delete build/ again|steered while deciding" {
		t.Errorf("user texts = %v", texts)
	}
	_ = end
}

func TestToolProviderListIsValidated(t *testing.T) {
	upperTool := agenttool.New("upper", "", upper)
	turn := 0
	cfg := Config{Model: &echo.Adapter{}, ToolProvider: func(context.Context) []agenttool.Tool {
		turn++
		if turn == 2 {
			return []agenttool.Tool{upperTool, upperTool}
		}
		return []agenttool.Tool{upperTool}
	}}
	_, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("abc")}, cfg))
	if end.Reason != ReasonError || err == nil || !strings.Contains(err.Error(), "turn 2") || !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("err=%v end=%+v", err, end)
	}
	// The first turn ran: its items are on the run end.
	if got := itemTypes(end.Items); got != "user function_call function_call_output" {
		t.Errorf("items = %q", got)
	}
}

func TestGuardStopAndFinal(t *testing.T) {
	var finals []bool
	cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)},
		ShouldStopAfterTurn: func(_ context.Context, info TurnInfo) (bool, error) {
			finals = append(finals, info.Final)
			if info.Final && strings.Contains(info.Response.OutputText(), "ABC") {
				return false, fmt.Errorf("%w: output names the secret", ErrGuard)
			}
			return false, nil
		}}
	// The low-level loop puts the guard's error on the RunEnd; an Agent
	// does not return it, since a guard stop is not a failure.
	_, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("abc")}, cfg))
	if !errors.Is(err, ErrGuard) || end.Reason != ReasonStopped || end.Cause != StopGuard || !errors.Is(end.Err, ErrGuard) {
		t.Errorf("guard stop: err=%v end=%+v", err, end)
	}
	if fmt.Sprint(finals) != "[false true]" {
		t.Errorf("finals = %v", finals)
	}
	// Through an Agent, a guard stop is not an error.
	a := New(cfg)
	end, err = a.Prompt(context.Background(), openresponses.UserText("abc"))
	if err != nil || end.Cause != StopGuard {
		t.Errorf("agent guard stop: err=%v end=%+v", err, end)
	}
	// Any other error is still a failure.
	cfg.ShouldStopAfterTurn = func(context.Context, TurnInfo) (bool, error) { return false, errors.New("boom") }
	_, end, err = collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("abc")}, cfg))
	if err == nil || end.Reason != ReasonError {
		t.Errorf("hook failure: err=%v end=%+v", err, end)
	}
	cfg.ShouldStopAfterTurn = func(context.Context, TurnInfo) (bool, error) { return true, nil }
	_, end, _ = collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("abc")}, cfg))
	if end.Reason != ReasonStopped || end.Cause != StopHook {
		t.Errorf("hook halt: end=%+v", end)
	}
}

func TestOutputGuardReplacesTheMessage(t *testing.T) {
	var guarded []string
	cfg := Config{Model: &echo.Adapter{}, ModelName: "m",
		OutputGuard: func(_ context.Context, info OutputInfo) (*openresponses.Message, error) {
			guarded = append(guarded, info.Message.Text())
			if strings.Contains(info.Message.Text(), "555") {
				return openresponses.AssistantText("[withheld]"), nil
			}
			return nil, nil
		}}
	events, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("call 555-0100")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	// The transcript and the item_end carry the placeholder; the deltas
	// carried the original.
	if m := end.Items[1].(*openresponses.Message); m.Text() != "[withheld]" || m.Role != openresponses.RoleAssistant {
		t.Errorf("appended = %+v", m)
	}
	sawDelta := false
	for _, ev := range events {
		switch e := ev.(type) {
		case *ItemEnd:
			if m, ok := e.Item.(*openresponses.Message); ok && m.Role == openresponses.RoleAssistant && m.Text() != "[withheld]" {
				t.Errorf("item_end carried %q", m.Text())
			}
		case *ItemUpdate:
			sawDelta = true
		}
	}
	if !sawDelta || len(guarded) != 1 || !strings.Contains(guarded[0], "555") {
		t.Errorf("deltas=%v guarded=%v", sawDelta, guarded)
	}
	// A kept message passes unchanged; a guard error fails the run.
	_, end, _ = collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("hello")}, cfg))
	if m := end.Items[1].(*openresponses.Message); !strings.Contains(m.Text(), "hello") {
		t.Errorf("kept = %q", m.Text())
	}
	cfg.OutputGuard = func(context.Context, OutputInfo) (*openresponses.Message, error) { return nil, errors.New("no") }
	_, end, err = collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("hello")}, cfg))
	if err == nil || end.Reason != ReasonError {
		t.Errorf("guard error: err=%v end=%+v", err, end)
	}
}

func TestBeforeTurnAppendsItems(t *testing.T) {
	turns := 0
	// Two turns: the echo adapter answers the developer message with a
	// tool call while tools are offered, so the budget ends the run.
	cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)}, MaxTurns: 2,
		BeforeTurn: func(_ context.Context, info TurnStartInfo) (openresponses.Items, error) {
			turns++
			if info.Turn != turns || len(info.Transcript) == 0 {
				t.Errorf("turn start info = %+v", info)
			}
			return openresponses.Items{openresponses.DeveloperText(fmt.Sprintf("the time is now, turn %d", info.Turn))}, nil
		}}
	events, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("abc")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	if got := itemTypes(end.Items); got != "user developer function_call function_call_output developer function_call function_call_output" || end.Cause != StopMaxTurns {
		t.Errorf("items = %q cause=%s", got, end.Cause)
	}
	// The injected item has its item events and is in the request.
	var req openresponses.Request
	for _, ev := range events {
		if e, ok := ev.(*TurnStart); ok && e.Turn == 1 {
			req = e.Request
		}
	}
	if len(req.Input) != 2 || req.Input[1].(*openresponses.Message).Role != openresponses.RoleDeveloper {
		t.Errorf("request input = %v", itemTypes(req.Input))
	}
	cfg.BeforeTurn = func(context.Context, TurnStartInfo) (openresponses.Items, error) {
		return nil, errors.New("clock down")
	}
	_, end, err = collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("abc")}, cfg))
	if err == nil || end.Reason != ReasonError || !strings.Contains(err.Error(), "before-turn") {
		t.Errorf("hook error: err=%v end=%+v", err, end)
	}
}

func TestRunRefusesDanglingCalls(t *testing.T) {
	cfg := Config{Model: &echo.Adapter{}}
	dangling := Transcript{openresponses.UserText("x"), &openresponses.FunctionCall{CallID: "c1", Name: "f", Arguments: "{}"}}
	_, end, err := collect(t, Run(context.Background(), dangling, openresponses.Items{openresponses.UserText("y")}, cfg))
	if !errors.Is(err, ErrInputRequired) || end.Reason != ReasonError {
		t.Errorf("run with a dangling call: err=%v", err)
	}
	// Prompts that open with the outputs answer it.
	events, end, err := collect(t, Run(context.Background(), dangling, openresponses.Items{openresponses.NewFunctionCallOutput("c1", "ok"), openresponses.UserText("y")}, cfg))
	if err != nil || end.Reason != ReasonDone || runStartOf(events).Source != SourceResume {
		t.Errorf("run answering the call: err=%v end=%+v", err, end)
	}
	_, _, err = collect(t, Continue(context.Background(), append(dangling, openresponses.UserText("y")), cfg))
	if !errors.Is(err, ErrInputRequired) {
		t.Errorf("continue with a dangling call: err=%v", err)
	}
}
