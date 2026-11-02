package agent

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestAChildsDeferralAsksTheParentsElicitor pins #84: under
// WithAskParent a call the child's own hook defers is put to the
// elicitor of the call that started the child, as a nested call's
// deferral is, and the child goes on with the answer. The call fails
// with input-required only when there is nobody to ask, and then names
// what it waits on through agentturn.InputRequired: each call's ID,
// name and arguments, and the child's run as the callee.
func TestAChildsDeferralAsksTheParentsElicitor(t *testing.T) {
	lookup := agenttool.New("lookup", "", func(context.Context, struct {
		Text string `json:"text"`
	}) (string, error) {
		return "FOUND", nil
	})
	for _, tc := range []struct {
		name string
		// ask installs WithAskParent; answer is the parent's
		// elicitor, nil for none.
		ask    bool
		answer *agenttool.Answer
		held   bool
		// want is the parent's call output; input says it failed
		// with input-required.
		want  string
		input bool
		asked bool
	}{
		{name: "accepted", ask: true, answer: &agenttool.Answer{Action: agenttool.ActionAccept}, want: "Tool result: FOUND", asked: true},
		{name: "declined", ask: true, answer: &agenttool.Answer{Action: agenttool.ActionDecline}, want: "Tool result: Error: declined when asked: a person approves lookups", asked: true},
		{name: "nobody to ask", ask: true, input: true},
		{name: "a cancel", ask: true, answer: &agenttool.Answer{Action: agenttool.ActionCancel}, input: true, asked: true},
		{name: "a held call is released", ask: true, held: true, answer: &agenttool.Answer{Action: agenttool.ActionAccept}, want: "Tool result: FOUND"},
		{name: "without the option", answer: &agenttool.Answer{Action: agenttool.ActionAccept}, input: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			childCfg := agentturn.Config{Name: "child", Description: "looks up", Model: &echo.Adapter{}, Tools: []agenttool.Tool{lookup},
				BeforeToolCall: func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
					return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "a person approves lookups", By: "policy", Held: tc.held}, nil
				}}
			var opts []Option
			if tc.ask {
				opts = append(opts, WithAskParent())
			}
			child := New(childCfg, opts...)
			var asked []agentturn.AskedCall
			var under []string
			parent := agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{child}, MaxTurns: 1}
			if tc.answer != nil {
				parent.ToolElicitor = func(ctx context.Context, q agenttool.Elicitation) (agenttool.Answer, error) {
					c, _ := agentturn.AskedCallFrom(ctx)
					asked = append(asked, c)
					call, _ := agenttool.CallFrom(ctx)
					under = append(under, call.ID)
					return *tc.answer, nil
				}
			}
			var end *agentturn.ToolEnd
			var start *agentturn.ToolStart
			for ev := range agentturn.Run(context.Background(), nil, openresponses.Items{openresponses.UserText("find")}, parent) {
				switch e := ev.(type) {
				case *agentturn.ToolStart:
					start = e
				case *agentturn.ToolEnd:
					end = e
				}
			}
			if end == nil || start == nil {
				t.Fatal("the parent never called the child")
			}
			if (len(asked) > 0) != tc.asked {
				t.Fatalf("asked %+v, want asked %v", asked, tc.asked)
			}
			if tc.asked && (asked[0].Name != "lookup" || string(asked[0].Args) != `{"text":"find"}` || under[0] != start.CallID) {
				t.Errorf("asked %+v under %v, want lookup filed under the parent's call %s", asked[0], under, start.CallID)
			}
			if !tc.input {
				if end.Err != nil || end.Result.Output.Text != tc.want {
					t.Errorf("parent's call = %q, %v; want %q", end.Result.Output.Text, end.Err, tc.want)
				}
				return
			}
			if !errors.Is(end.Err, agentturn.ErrInputRequired) {
				t.Fatalf("parent's call err = %v, want input required", end.Err)
			}
			var waiting agentturn.InputRequired
			if !errors.As(end.Err, &waiting) {
				t.Fatalf("err %T is no agentturn.InputRequired", end.Err)
			}
			info, _ := end.Result.Details.(ChildInfo)
			items := waiting.PendingCalls()
			if len(items) != 1 || items[0].Name != "lookup" || string(items[0].Args) != `{"text":"find"}` ||
				items[0].ID != info.Pending[0].Call.CallID || items[0].Callee != info.RunID || info.RunID == "" {
				t.Errorf("waiting = %+v, want the child's lookup on run %s", items, info.RunID)
			}
			if !strings.Contains(end.Err.Error(), "needs input") {
				t.Errorf("err = %v", end.Err)
			}
		})
	}
}
