package responses

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	"github.com/ChristopherDavenport/openresponses/streamtest"
)

// transfer is a handoff tool: it answers on the model's behalf and ends
// the run.
var transfer = agenttool.NewFunc("transfer_to_billing", "hands the conversation to billing", json.RawMessage(`{"type":"object"}`),
	func(context.Context, agenttool.Call) (agenttool.Result, error) {
		return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "transferred"}, Terminate: true}, nil
	})

// TestTerminatingStop pins #163: an agent whose only tool terminates
// completes with the tool's output when nothing takes the handoff, and
// with the receiver's answer, in the same response, when WithHandoff
// hands the transcript to another configuration.
func TestTerminatingStop(t *testing.T) {
	var billed []string
	billing := agentturn.Config{Model: &echo.Adapter{}, Instructions: "You are billing.", BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
		billed = append(billed, req.Instructions+" "+itemTypes(req.Input))
		return nil
	}}
	for _, tc := range []struct {
		name    string
		handoff func(context.Context, *agentturn.RunEnd) (agentturn.Config, bool)
		want    string
		asked   int
	}{
		{"no handoff", nil, "transferred", 0},
		{"declined", func(context.Context, *agentturn.RunEnd) (agentturn.Config, bool) { return agentturn.Config{}, false }, "transferred", 1},
		{"handed off", func(context.Context, *agentturn.RunEnd) (agentturn.Config, bool) { return billing, true }, "Tool result: transferred", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ends []*agentturn.RunEnd
			billed = nil
			var opts []Option
			if tc.handoff != nil {
				opts = append(opts, WithHandoff(func(ctx context.Context, end *agentturn.RunEnd) (agentturn.Config, bool) {
					ends = append(ends, end)
					return tc.handoff(ctx, end)
				}))
			}
			a := New(agentturn.Config{Model: preamble{}, ModelName: "m", Instructions: "You are triage.", Tools: []agenttool.Tool{transfer}}, opts...)
			sink, err := streamtest.Run(context.Background(), a, request(openresponses.UserText("I was double charged")))
			if err != nil {
				t.Fatal(err)
			}
			if err := streamtest.Validate(sink.Events()); err != nil {
				t.Fatalf("stream: %v", err)
			}
			resp := sink.Response()
			if resp.Status != openresponses.ResponseStatusCompleted || itemTypes(resp.Output) != "assistant assistant" {
				t.Fatalf("response = %s %s", resp.Status, itemTypes(resp.Output))
			}
			if got := resp.Output[1].(*openresponses.Message).Text(); got != tc.want {
				t.Errorf("answer = %q, want %q", got, tc.want)
			}
			if len(ends) != tc.asked {
				t.Fatalf("handoff asked %d times, want %d", len(ends), tc.asked)
			}
			for _, end := range ends {
				if end.Cause != agentturn.StopTerminate {
					t.Errorf("handoff asked about %s/%s", end.Reason, end.Cause)
				}
			}
		})
	}
}
