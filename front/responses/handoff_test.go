package responses

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
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
		return HandedTo(t, func(call *openresponses.FunctionCall) (agentturn.Config, string, bool) {
			return billing, "transferred", call.Name == "transfer_to_billing"
		})
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

// TestHandoffs pins #190: a transfer call is a handoff only when its
// output is the transfer tool's own text and the route takes it; a call
// a guard withheld, a hook blocked, a tool failed on, or the route does
// not reach is not, and the last handoff is the conversation's.
func TestHandoffs(t *testing.T) {
	billing := agentturn.Config{Name: "billing"}
	refunds := agentturn.Config{Name: "refunds"}
	agents := map[string]agentturn.Config{"billing": billing, "refunds": refunds}
	route := func(reachable ...string) Route {
		return func(call *openresponses.FunctionCall) (agentturn.Config, string, bool) {
			name := strings.TrimPrefix(call.Name, "transfer_to_")
			if !slices.Contains(reachable, name) {
				return agentturn.Config{}, "", false
			}
			return agents[name], `{"assistant":"` + name + `"}`, true
		}
	}
	call := func(id, to string) *openresponses.FunctionCall {
		return &openresponses.FunctionCall{CallID: id, Name: "transfer_to_" + to, Arguments: "{}"}
	}
	out := func(id, text string) *openresponses.FunctionCallOutput {
		return &openresponses.FunctionCallOutput{CallID: id, Output: openresponses.FunctionCallOutputData{Text: text}}
	}
	user := openresponses.UserText("I was double charged")
	for _, tc := range []struct {
		name  string
		t     agentturn.Transcript
		route Route
		want  []string
		at    []int
	}{
		{"transferred", agentturn.Transcript{user, call("c1", "billing"), out("c1", `{"assistant":"billing"}`)}, route("billing"), []string{"billing"}, []int{2}},
		{"withheld", agentturn.Transcript{user, call("c1", "billing"), out("c1", agentturn.WithheldCallOutput)}, route("billing"), nil, nil},
		{"blocked", agentturn.Transcript{user, call("c1", "billing"), out("c1", "Error: blocked by policy")}, route("billing"), nil, nil},
		{"unanswered", agentturn.Transcript{user, call("c1", "billing")}, route("billing"), nil, nil},
		{"parts", agentturn.Transcript{user, call("c1", "billing"), &openresponses.FunctionCallOutput{CallID: "c1", Output: openresponses.FunctionCallOutputData{Text: `{"assistant":"billing"}`, Parts: openresponses.Contents{&openresponses.InputText{Text: "x"}}}}}, route("billing"), nil, nil},
		{"output before the call", agentturn.Transcript{user, out("c1", `{"assistant":"billing"}`), call("c1", "billing")}, route("billing"), nil, nil},
		{"unreachable", agentturn.Transcript{user, call("c1", "refunds"), out("c1", `{"assistant":"refunds"}`)}, route("billing"), nil, nil},
		{"not a transfer", agentturn.Transcript{user, &openresponses.FunctionCall{CallID: "c1", Name: "lookup"}, out("c1", `{"assistant":"billing"}`)}, route("billing"), nil, nil},
		{"two handoffs", agentturn.Transcript{user, call("c1", "billing"), out("c1", `{"assistant":"billing"}`), call("c2", "refunds"), out("c2", `{"assistant":"refunds"}`)}, route("billing", "refunds"), []string{"billing", "refunds"}, []int{2, 4}},
		{"a later one withheld", agentturn.Transcript{user, call("c1", "billing"), out("c1", `{"assistant":"billing"}`), call("c2", "refunds"), out("c2", agentturn.WithheldCallOutput)}, route("billing", "refunds"), []string{"billing"}, []int{2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var names []string
			var at []int
			for _, h := range Handoffs(tc.t, tc.route) {
				names = append(names, h.To.Name)
				at = append(at, h.Output)
			}
			if !reflect.DeepEqual(names, tc.want) || !reflect.DeepEqual(at, tc.at) {
				t.Errorf("Handoffs = %v at %v, want %v at %v", names, at, tc.want, tc.at)
			}
			cfg, ok := HandedTo(tc.t, tc.route)
			if want := len(tc.want) > 0; ok != want || (ok && cfg.Name != tc.want[len(tc.want)-1]) {
				t.Errorf("HandedTo = %q %v", cfg.Name, ok)
			}
		})
	}
}
