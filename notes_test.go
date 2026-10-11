package agentturn

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// TestEveryNoteFollowsItsBatch pins the one rule for a note: whatever
// the user said reaches the model after every output of the batch it
// belongs to, never between the calls and their outputs. An accept's
// note on a nested call (#245), at any depth, and a note on a tool's own
// question each follow the invoking call's batch, as a model call's note
// does.
func TestEveryNoteFollowsItsBatch(t *testing.T) {
	accept := func(note string) agenttool.Elicitor {
		return func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) {
			return agenttool.Answer{Action: agenttool.ActionAccept, Note: note}, nil
		}
	}
	read := agenttool.New("read", "reads", func(context.Context, echoArgs) (string, error) { return "contents", nil })
	other := agenttool.New("other", "", upper)
	invokes := func(name, target string) agenttool.Tool {
		return agenttool.New(name, "", func(ctx context.Context, _ echoArgs) (string, error) {
			if _, err := Invoke(ctx, target, json.RawMessage(`{"text":"t"}`)); err != nil {
				return "", err
			}
			return "done", nil
		})
	}
	confirm := agenttool.New("confirm", "", func(ctx context.Context, _ echoArgs) (string, error) {
		elicit, _ := agenttool.ElicitorFrom(ctx)
		if _, err := elicit(ctx, agenttool.Elicitation{Message: "go ahead?"}); err != nil {
			return "", err
		}
		return "confirmed", nil
	})
	defersRead := func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
		if info.Call.Name == "read" {
			return &ToolDecision{Action: Defer, Reason: "approval required by read"}, nil
		}
		return nil, nil
	}
	cases := []struct {
		name  string
		tools []agenttool.Tool
		calls []string
		note  string
	}{
		{name: "an accept on a nested call", tools: []agenttool.Tool{invokes("eval", "read"), read, other}, calls: []string{"eval", "other"}, note: "The user said: only the unit tests"},
		{name: "an accept two calls deep", tools: []agenttool.Tool{invokes("eval", "inner"), invokes("inner", "read"), read, other}, calls: []string{"eval", "other"}, note: "The user said: only the unit tests"},
		{name: "a tool's own question", tools: []agenttool.Tool{confirm, other}, calls: []string{"confirm", "other"}, note: "The user said: only the unit tests"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Model: callsNamed{names: tc.calls}, Tools: tc.tools, BeforeToolCall: defersRead, ToolElicitor: accept("only the unit tests"), MaxTurns: 1}
			_, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
			if err != nil {
				t.Fatal(err)
			}
			if got := itemTypes(end.Items); got != "user function_call function_call function_call_output function_call_output developer" {
				t.Fatalf("items = %q", got)
			}
			if out := end.Items[3].(*openresponses.FunctionCallOutput); strings.HasPrefix(out.Output.Text, "error") {
				t.Errorf("the first call failed: %q", out.Output.Text)
			}
			if m := end.Items[5].(*openresponses.Message); m.Text() != tc.note {
				t.Errorf("note = %q, want %q", m.Text(), tc.note)
			}
		})
	}
}

// TestANoteWaitsForADeferredCall pins that a batch's notes wait while a
// call of it is deferred: the deferred call's output comes from the
// Resume, and the notes follow it, the batch's own and then the
// Resume's, so no message splits the batch's calls from their outputs.
func TestANoteWaitsForADeferredCall(t *testing.T) {
	hook := func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
		if info.Call.Name == "a" {
			return &ToolDecision{Note: "a ran in the sandbox"}, nil
		}
		return &ToolDecision{Action: Defer, Reason: "ask"}, nil
	}
	a := New(Config{Model: &twoCalls{}, Tools: []agenttool.Tool{agenttool.New("a", "", upper), agenttool.New("b", "", upper)}, BeforeToolCall: hook, MaxTurns: 1})
	end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
	if err != nil || end.Reason != ReasonInputRequired {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	if got := itemTypes(a.State().Transcript); got != "user function_call function_call function_call_output" {
		t.Fatalf("transcript while b waits = %q", got)
	}
	b := end.Pending[0].Call.CallID
	if _, err := a.Resume(context.Background(), Output(openresponses.NewFunctionCallOutput(b, "did it myself")).WithNote("that one I handled")); err != nil {
		t.Fatal(err)
	}
	tr := a.State().Transcript
	if got := itemTypes(tr[:6]); got != "user function_call function_call function_call_output function_call_output developer" {
		t.Fatalf("transcript = %q", itemTypes(tr))
	}
	if m := tr[5].(*openresponses.Message); m.Text() != "a ran in the sandbox" {
		t.Errorf("the batch's note = %q", m.Text())
	}
	if m, ok := tr[6].(*openresponses.Message); !ok || m.Role != openresponses.RoleUser || m.Text() != "that one I handled" {
		t.Errorf("the Resume's note = %#v", tr[6])
	}
}
