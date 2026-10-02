package agentturn

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestAskedCallOnTheElicitorsContext pins #212: the elicitor asked
// about a nested call the hook deferred finds the call on its context
// as data, whole, where the question's text quotes the first 500 bytes
// of the arguments. The question stays filed under the invoking call.
func TestAskedCallOnTheElicitorsContext(t *testing.T) {
	long := strings.Repeat("x", 2*maxAskedArgs)
	args := json.RawMessage(`{"text":"` + long + `"}`)
	read := agenttool.New("read", "reads", func(context.Context, echoArgs) (string, error) { return "contents", nil })
	eval := agenttool.New("eval", "runs code", func(ctx context.Context, _ echoArgs) (string, error) {
		_, err := Invoke(ctx, "read", args)
		return "ran", err
	})
	defers := func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
		if info.Call.Name == "read" {
			return &ToolDecision{Action: Defer, Reason: "approval required by read", By: "policy"}, nil
		}
		return nil, nil
	}
	var asked AskedCall
	var found bool
	var under string
	var question string
	cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{eval, read}, BeforeToolCall: defers, MaxTurns: 1,
		ToolElicitor: func(ctx context.Context, q agenttool.Elicitation) (agenttool.Answer, error) {
			asked, found = AskedCallFrom(ctx)
			call, _ := agenttool.CallFrom(ctx)
			under = call.ID
			question = q.Message
			return agenttool.Answer{Action: agenttool.ActionAccept}, nil
		}}
	events, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err != nil || end.Reason == ReasonError {
		t.Fatalf("err=%v end=%+v", err, end)
	}
	evalCall, readCall := "", ""
	for _, ev := range events {
		if e, ok := ev.(*ToolStart); ok {
			if e.Parent == "" {
				evalCall = e.CallID
			} else {
				readCall = e.CallID
			}
		}
	}
	if !found {
		t.Fatal("the elicitor's context carries no AskedCall")
	}
	if asked.Parent != evalCall || asked.CallID != readCall || asked.Name != "read" || string(asked.Args) != string(args) {
		t.Errorf("asked = %+v, want parent %s call %s", asked, evalCall, readCall)
	}
	if asked.Decision == nil || asked.Decision.Action != Defer || asked.Decision.Reason != "approval required by read" {
		t.Errorf("asked.Decision = %+v", asked.Decision)
	}
	if under != evalCall {
		t.Errorf("asked under %q, want the invoking call %q", under, evalCall)
	}
	if strings.Contains(question, long) || !strings.Contains(question, "…") || !strings.Contains(question, "approval required by read") {
		t.Errorf("question = %q, want the arguments clipped and the reason named", question)
	}
}

// TestAsk pins the one rule a question about a call follows: the text,
// the decision each answer makes, by "human", and the cases that leave
// the caller with the deferral it started with.
func TestAsk(t *testing.T) {
	cases := []struct {
		name string
		// elicit is installed on the context; nil installs none.
		elicit     agenttool.Elicitor
		reason     string
		wantOK     bool
		wantAction ToolAction
		wantReason string
	}{
		{name: "no elicitor", reason: "rule r"},
		{name: "accept", elicit: answers(agenttool.Answer{Action: agenttool.ActionAccept}), reason: "rule r", wantOK: true, wantAction: Allow, wantReason: "rule r"},
		{name: "accept, no reason", elicit: answers(agenttool.Answer{Action: agenttool.ActionAccept}), wantOK: true, wantAction: Allow, wantReason: "allowed when asked"},
		{name: "decline", elicit: answers(agenttool.Answer{Action: agenttool.ActionDecline}), reason: "rule r", wantOK: true, wantAction: Block, wantReason: "declined when asked: rule r"},
		{name: "decline, no reason", elicit: answers(agenttool.Answer{Action: agenttool.ActionDecline}), wantOK: true, wantAction: Block, wantReason: "declined when asked"},
		{name: "cancel", elicit: answers(agenttool.Answer{Action: agenttool.ActionCancel}), reason: "rule r"},
		{name: "elicitor error", elicit: func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) {
			return agenttool.Answer{}, errors.New("no terminal")
		}, reason: "rule r"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var got []agenttool.Elicitation
			var onCtx AskedCall
			var found bool
			if tc.elicit != nil {
				ctx = agenttool.ContextWithElicitor(ctx, func(ctx context.Context, q agenttool.Elicitation) (agenttool.Answer, error) {
					got = append(got, q)
					onCtx, found = AskedCallFrom(ctx)
					return tc.elicit(ctx, q)
				})
			}
			deferral := &ToolDecision{Action: Defer, Reason: tc.reason, By: "policy"}
			call := AskedCall{Parent: "call_p", CallID: "call_n", Name: "read", Args: json.RawMessage(`{"text":"go.mod"}`), Decision: deferral}
			d, ok := Ask(ctx, call)
			if ok != tc.wantOK {
				t.Fatalf("Ask = %+v, %v; want ok %v", d, ok, tc.wantOK)
			}
			// The deferral is the caller's to keep: Ask never writes
			// to it.
			if deferral.Action != Defer || deferral.Reason != tc.reason || deferral.By != "policy" {
				t.Errorf("the deferral was changed: %+v", deferral)
			}
			if tc.elicit != nil {
				want := `Allow read with arguments {"text":"go.mod"}?`
				if tc.reason != "" {
					want += " " + tc.reason
				}
				if len(got) != 1 || got[0].Message != want {
					t.Errorf("asked %+v, want one question %q", got, want)
				}
				if !found || onCtx.CallID != "call_n" || onCtx.Parent != "call_p" || onCtx.Decision != deferral {
					t.Errorf("the call on the elicitor's context = %+v, %v", onCtx, found)
				}
			}
			if !tc.wantOK {
				if d != nil {
					t.Errorf("decision = %+v, want none", d)
				}
				return
			}
			if d == nil || d.Action != tc.wantAction || d.Reason != tc.wantReason || d.By != "human" {
				t.Errorf("decision = %+v, want %v %q by human", d, tc.wantAction, tc.wantReason)
			}
		})
	}
}

// TestAskedCallFromAnEmptyContext checks the accessor's miss and that
// ContextWithAskedCall is what Ask relies on.
func TestAskedCallFromAnEmptyContext(t *testing.T) {
	if call, ok := AskedCallFrom(context.Background()); ok || call.CallID != "" {
		t.Errorf("AskedCallFrom(empty) = %+v, %v", call, ok)
	}
	want := AskedCall{CallID: "call_1", Name: "n"}
	if got, ok := AskedCallFrom(ContextWithAskedCall(context.Background(), want)); !ok || got.CallID != want.CallID || got.Name != want.Name {
		t.Errorf("AskedCallFrom = %+v, %v", got, ok)
	}
}

// answers is an elicitor that gives one answer to every question.
func answers(a agenttool.Answer) agenttool.Elicitor {
	return func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) { return a, nil }
}
