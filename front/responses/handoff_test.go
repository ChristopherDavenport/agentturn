package responses

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	"github.com/ChristopherDavenport/openresponses/streamtest"
)

// transfer is a handoff tool: it answers on the model's behalf, names
// the destination in Details, and ends the run.
var transfer = agenttool.NewFunc("transfer_to_billing", "hands the conversation to billing", json.RawMessage(`{"type":"object"}`),
	func(context.Context, agenttool.Call) (agenttool.Result, error) {
		return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "transferred"}, Terminate: true, Details: "billing"}, nil
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
		handoff func(context.Context, *agentturn.RunEnd, []*agentturn.ToolEnd) (agentturn.Config, bool)
		want    string
		asked   int
	}{
		{"no handoff", nil, "transferred", 0},
		{"declined", func(context.Context, *agentturn.RunEnd, []*agentturn.ToolEnd) (agentturn.Config, bool) {
			return agentturn.Config{}, false
		}, "transferred", 1},
		{"handed off", func(context.Context, *agentturn.RunEnd, []*agentturn.ToolEnd) (agentturn.Config, bool) {
			return billing, true
		}, "Tool result: transferred", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ends []*agentturn.RunEnd
			billed = nil
			var opts []Option
			if tc.handoff != nil {
				opts = append(opts, WithHandoff(func(ctx context.Context, end *agentturn.RunEnd, results []*agentturn.ToolEnd) (agentturn.Config, bool) {
					ends = append(ends, end)
					if len(results) != 1 || results[0].Result.Details != "billing" {
						t.Errorf("handoff given results %v, want the transfer's with its details", results)
					}
					return tc.handoff(ctx, end, results)
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

// TestHandoffTrigger pins #182: the receiver's run after WithHandoff is
// continued under a handoff trigger naming the sender, which its
// BeforeTurn context carries; the sender's run carries none.
func TestHandoffTrigger(t *testing.T) {
	var triggers []agentturn.Trigger
	seen := func(ctx context.Context, _ agentturn.TurnStartInfo) (openresponses.Items, error) {
		triggers = append(triggers, agentturn.TriggerFromContext(ctx))
		return nil, nil
	}
	billing := agentturn.Config{Name: "billing", Model: &echo.Adapter{}, BeforeTurn: seen}
	a := New(agentturn.Config{Name: "triage", Model: preamble{}, ModelName: "m", Tools: []agenttool.Tool{transfer}, BeforeTurn: seen},
		WithHandoff(func(context.Context, *agentturn.RunEnd, []*agentturn.ToolEnd) (agentturn.Config, bool) {
			return billing, true
		}))
	if _, err := streamtest.Run(context.Background(), a, request(openresponses.UserText("I was double charged"))); err != nil {
		t.Fatal(err)
	}
	want := []agentturn.Trigger{{}, {Kind: "handoff", Ref: "triage"}}
	if !reflect.DeepEqual(triggers, want) {
		t.Errorf("BeforeTurn triggers = %+v, want %+v", triggers, want)
	}
}

// TestStartAfterHandoff pins #180: the conversation's next request
// after a handoff starts under the configuration WithStart finds in the
// input, and under the adapter's own without the option or when it
// declines.
func TestStartAfterHandoff(t *testing.T) {
	var served []string
	serves := func(name string) func(context.Context, *openresponses.Request) error {
		return func(context.Context, *openresponses.Request) error {
			served = append(served, name)
			return nil
		}
	}
	billing := agentturn.Config{Name: "billing", Model: &echo.Adapter{}, BeforeModelCall: serves("billing")}
	lastTransfer := func(_ context.Context, t agentturn.Transcript) (agentturn.Config, bool) {
		for i := len(t) - 1; i >= 0; i-- {
			if call, ok := t[i].(*openresponses.FunctionCall); ok && call.Name == "transfer_to_billing" {
				return billing, true
			}
		}
		return agentturn.Config{}, false
	}
	for _, tc := range []struct {
		name   string
		start  func(context.Context, agentturn.Transcript) (agentturn.Config, bool)
		want   []string
		answer string
	}{
		{"no start", nil, []string{"triage", "billing"}, "Tool result: transferred"},
		{"declined", func(context.Context, agentturn.Transcript) (agentturn.Config, bool) { return agentturn.Config{}, false }, []string{"triage", "billing"}, "Tool result: transferred"},
		{"last transfer", lastTransfer, []string{"billing"}, "when is the refund"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := []Option{WithToolItems(), WithHandoff(func(context.Context, *agentturn.RunEnd, []*agentturn.ToolEnd) (agentturn.Config, bool) {
				return billing, true
			})}
			if tc.start != nil {
				opts = append(opts, WithStart(tc.start))
			}
			a := New(agentturn.Config{Name: "triage", Model: preamble{}, ModelName: "m", Tools: []agenttool.Tool{transfer}, BeforeModelCall: serves("triage")}, opts...)
			first := request(openresponses.UserText("I was double charged"))
			sink, err := streamtest.Run(context.Background(), a, first)
			if err != nil {
				t.Fatal(err)
			}
			served = nil
			next := request(append(append(first.Input, sink.Response().Output...), openresponses.UserText("when is the refund"))...)
			sink, err = streamtest.Run(context.Background(), a, next)
			if err != nil {
				t.Fatal(err)
			}
			if err := streamtest.Validate(sink.Events()); err != nil {
				t.Fatalf("stream: %v", err)
			}
			if !reflect.DeepEqual(served, tc.want) {
				t.Errorf("second request served by %v, want %v", served, tc.want)
			}
			if got := sink.Response().OutputText(); !strings.HasSuffix(got, tc.answer) {
				t.Errorf("answer = %q", got)
			}
		})
	}
}
