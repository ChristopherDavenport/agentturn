package agentturn

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// TestAnAnswerIsByTheDeciderItNames pins #267: the decision an answer
// to an asked call makes is by the decider the answer names
// (agenttool.Answer.By), or "human" when it names none, and the
// answer's note reaches the model as a user message only for "human",
// as a developer message for a rule or another model. An answer naming
// a decider nobody knows is refused, never read as the user's, so the
// deferral it was asked about refuses the call. A note on a tool's own
// question follows the same voice.
func TestAnAnswerIsByTheDeciderItNames(t *testing.T) {
	read := agenttool.New("read", "reads", func(context.Context, echoArgs) (string, error) { return "contents", nil })
	eval := agenttool.New("eval", "", func(ctx context.Context, _ echoArgs) (string, error) {
		if _, err := Invoke(ctx, "read", json.RawMessage(`{"text":"t"}`)); err != nil {
			return "", err
		}
		return "done", nil
	})
	confirm := agenttool.New("confirm", "", func(ctx context.Context, _ echoArgs) (string, error) {
		elicit, _ := agenttool.ElicitorFrom(ctx)
		if _, err := elicit(ctx, agenttool.Elicitation{Message: "go ahead?"}); err != nil {
			return "", err
		}
		return "confirmed", nil
	})
	hook := func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
		if info.Call.Name == "read" {
			return &ToolDecision{Action: Defer, Reason: "approval required by read"}, nil
		}
		return nil, nil
	}
	cases := []struct {
		name   string
		call   string
		by     string
		wantBy string
		// want is the run's messages after the prompt.
		want string
		// refused says the nested call was refused, not run.
		refused bool
	}{
		{name: "an answer naming nobody", call: "eval", wantBy: "human", want: "user: only go.mod"},
		{name: "a person", call: "eval", by: agenttool.ByHuman, wantBy: "human", want: "user: only go.mod"},
		{name: "a rule", call: "eval", by: agenttool.ByPolicy, wantBy: "policy", want: "developer: only go.mod"},
		{name: "another model", call: "eval", by: agenttool.ByAgent, wantBy: "agent", want: "developer: only go.mod"},
		{name: "an unknown decider", call: "eval", by: "system", refused: true},
		{name: "a rule on a tool's own question", call: "confirm", by: agenttool.ByPolicy, want: "developer: only go.mod"},
		{name: "a person on a tool's own question", call: "confirm", want: "user: only go.mod"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			accept := func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) {
				return agenttool.Answer{Action: agenttool.ActionAccept, Note: "only go.mod", By: tc.by}, nil
			}
			cfg := Config{Model: callsNamed{names: []string{tc.call}}, Tools: []agenttool.Tool{eval, read, confirm}, BeforeToolCall: hook, ToolElicitor: accept, MaxTurns: 1}
			events, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
			if err != nil {
				t.Fatal(err)
			}
			var nested *ToolStart
			for _, ev := range events {
				if e, ok := ev.(*ToolStart); ok && e.Parent != "" {
					nested = e
				}
			}
			if tc.refused {
				// The deferral stands and refuses the call.
				if nested == nil || nested.Decision == nil || nested.Decision.Action != Defer {
					t.Fatalf("nested start = %+v, want the deferral to stand", nested)
				}
				if out := end.Items[2].(*openresponses.FunctionCallOutput); !strings.Contains(out.Output.Text, "a nested call cannot be deferred") {
					t.Errorf("output = %q, want the call refused", out.Output.Text)
				}
				if got := strings.Join(texts(end.Items), "|"); got != "user: x" {
					t.Errorf("messages = %q, want no note from a refused answer", got)
				}
				return
			}
			if tc.wantBy != "" {
				if nested == nil || nested.Decision == nil || nested.Decision.Action != Allow || nested.Decision.By != tc.wantBy {
					t.Fatalf("nested start = %+v, want allowed by %q", nested, tc.wantBy)
				}
			}
			if got := strings.Join(texts(end.Items), "|"); got != "user: x|"+tc.want {
				t.Errorf("messages = %q, want the note as %q", got, tc.want)
			}
		})
	}
}
