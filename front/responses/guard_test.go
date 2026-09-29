package responses

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	"github.com/ChristopherDavenport/openresponses/streamtest"
)

// TestOutputGuardReachesTheCaller pins #108: a message the output guard
// replaced reaches the caller as the replacement, in the response and
// in the done events, in a full run and in a single turn with the
// caller's tools; a message the guard kept streams as before.
func TestOutputGuardReachesTheCaller(t *testing.T) {
	const withheld = "[withheld]"
	replace := func(context.Context, agentturn.OutputInfo) (*openresponses.Message, error) {
		return openresponses.AssistantText(withheld), nil
	}
	keep := func(context.Context, agentturn.OutputInfo) (*openresponses.Message, error) { return nil, nil }
	callerTool := openresponses.NewFunctionTool("lookup", "caller owned", json.RawMessage(`{"type":"object"}`))
	fullRun := request(openresponses.UserText("abc"))
	oneTurn := request(openresponses.UserText("abc"), &openresponses.FunctionCall{CallID: "c1", Name: "lookup", Arguments: `{}`}, openresponses.NewFunctionCallOutput("c1", "found"))
	oneTurn.Tools = openresponses.Tools{callerTool}
	for _, tc := range []struct {
		name  string
		req   openresponses.Request
		guard func(context.Context, agentturn.OutputInfo) (*openresponses.Message, error)
		want  string
	}{
		{"full run, replaced", fullRun, replace, withheld},
		{"single turn, replaced", oneTurn, replace, withheld},
		{"full run, kept", fullRun, keep, "abc"},
		{"single turn, kept", oneTurn, keep, "found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seen []agentturn.OutputInfo
			guard := func(ctx context.Context, info agentturn.OutputInfo) (*openresponses.Message, error) {
				seen = append(seen, info)
				return tc.guard(ctx, info)
			}
			a := New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", OutputGuard: guard})
			sink, err := streamtest.Run(context.Background(), a, tc.req)
			if err != nil {
				t.Fatal(err)
			}
			if len(seen) != 1 {
				t.Fatalf("the guard saw %d messages", len(seen))
			}
			resp := sink.Response()
			if got := resp.OutputText(); !strings.Contains(got, tc.want) || (tc.want == withheld && got != withheld) {
				t.Errorf("response text = %q, want %q", got, tc.want)
			}
			var deltas strings.Builder
			for _, ev := range sink.Events() {
				switch e := ev.(type) {
				case *openresponses.OutputTextDeltaEvent:
					deltas.WriteString(e.Delta)
				case *openresponses.OutputTextDoneEvent:
					if e.Text != resp.OutputText() {
						t.Errorf("output_text.done = %q, response %q", e.Text, resp.OutputText())
					}
				case *openresponses.OutputItemDoneEvent:
					if m, ok := e.Item.(*openresponses.Message); ok && m.Content.Text() != resp.OutputText() {
						t.Errorf("output_item.done = %q, response %q", m.Content.Text(), resp.OutputText())
					}
				}
			}
			// A kept message streams its text whole, as before.
			if tc.want != withheld && deltas.String() != resp.OutputText() {
				t.Errorf("deltas %q, text %q", deltas.String(), resp.OutputText())
			}
		})
	}
}

// TestOutputGuardSeesWhatPrecedes pins #111: the guard is given the
// items of the response that precede the message.
func TestOutputGuardSeesWhatPrecedes(t *testing.T) {
	var info agentturn.OutputInfo
	a := agentturn.New(agentturn.Config{Model: whole{}, OutputGuard: func(_ context.Context, i agentturn.OutputInfo) (*openresponses.Message, error) {
		info = i
		return nil, nil
	}})
	if _, err := a.Prompt(context.Background(), openresponses.UserText("x")); err != nil {
		t.Fatal(err)
	}
	if got := itemTypes(info.Output); got != "reasoning" || info.ResponseID == "" {
		t.Errorf("output before the message = %q, response %q", got, info.ResponseID)
	}
}
