package responses

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

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

// thinksThenTransfers answers a user message with a reasoning item
// signed for the requested model and a call to its first tool, and
// echoes a function_call_output.
type thinksThenTransfers struct{}

func (thinksThenTransfers) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	if _, ok := req.Input[len(req.Input)-1].(*openresponses.FunctionCallOutput); ok {
		return (&echo.Adapter{}).CreateStream(ctx, req, sink)
	}
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if err := em.Item(&openresponses.ReasoningItem{Summary: openresponses.Contents{}, EncryptedContent: "sig:" + req.Model}); err != nil {
		return err
	}
	w, err := em.FunctionCall("", req.Tools[0].(*openresponses.FunctionTool).Name)
	if err != nil {
		return err
	}
	if err := w.Arguments(`{}`); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Complete()
}

// reasoningSigs lists the encrypted content of the reasoning items in
// items.
func reasoningSigs(items openresponses.Items) []string {
	sigs := []string{}
	for _, item := range items {
		if r, ok := item.(*openresponses.ReasoningItem); ok {
			sigs = append(sigs, r.EncryptedContent)
		}
	}
	return sigs
}

// TestReasoningAcrossHandoff pins #91: the receiver of a handoff on
// another model is sent none of the sender's reasoning items, within
// the response that handed off and, under WithTransfers, in the
// conversation's next request, in both modes; a receiver on the same
// model is sent them, as is one WithStart alone starts, which cannot
// tell where the input's reasoning came from.
func TestReasoningAcrossHandoff(t *testing.T) {
	route := func(cfg agentturn.Config) Route {
		return func(call *openresponses.FunctionCall) (agentturn.Config, string, bool) {
			return cfg, "transferred", call.Name == "transfer_to_billing"
		}
	}
	for _, tc := range []struct {
		name      string
		billing   string // billing's ModelName
		start     string // "transfers", "start", "both" or ""
		caller    bool   // the next request carries a caller tool: one turn
		handedOff []string
		next      []string
	}{
		{"another model", "other-2", "transfers", false, []string{}, []string{}},
		{"another model, one turn", "other-2", "transfers", true, []string{}, []string{}},
		{"same model", "reasoner-1", "transfers", false, []string{"sig:reasoner-1"}, []string{"sig:reasoner-1"}},
		{"WithStart alone", "other-2", "start", false, []string{}, []string{"sig:reasoner-1"}},
		{"WithStart before WithTransfers", "other-2", "both", false, []string{}, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seen [][]string
			billing := agentturn.Config{Name: "billing", Model: &echo.Adapter{}, ModelName: tc.billing, BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
				seen = append(seen, reasoningSigs(req.Input))
				return nil
			}}
			opts := []Option{WithToolItems(), WithHandoff(func(context.Context, *agentturn.RunEnd, []*agentturn.ToolEnd) (agentturn.Config, bool) {
				return billing, true
			})}
			switch tc.start {
			case "transfers":
				opts = append(opts, WithTransfers(route(billing)))
			case "start", "both":
				opts = append(opts, WithStart(func(_ context.Context, t agentturn.Transcript) (agentturn.Config, bool) {
					return HandedTo(t, route(billing))
				}))
				if tc.start == "both" {
					opts = append(opts, WithTransfers(route(billing)))
				}
			}
			a := New(agentturn.Config{Name: "triage", Model: thinksThenTransfers{}, ModelName: "reasoner-1", Tools: []agenttool.Tool{transfer}}, opts...)
			first := request(openresponses.UserText("I was double charged"))
			sink, err := streamtest.Run(context.Background(), a, first)
			if err != nil {
				t.Fatal(err)
			}
			if len(seen) != 1 || !reflect.DeepEqual(seen[0], tc.handedOff) {
				t.Fatalf("billing's request in the response that handed off carried %v, want %v", seen, tc.handedOff)
			}
			if got := reasoningSigs(sink.Response().Output); !reflect.DeepEqual(got, []string{"sig:reasoner-1"}) {
				t.Errorf("response output's reasoning = %v, want the sender's", got)
			}
			seen = nil
			next := request(append(append(first.Input, sink.Response().Output...), openresponses.UserText("when is the refund"))...)
			if tc.caller {
				next.Tools = openresponses.Tools{openresponses.NewFunctionTool("remote", "caller side", json.RawMessage(`{"type":"object"}`))}
			}
			if _, err := streamtest.Run(context.Background(), a, next); err != nil {
				t.Fatal(err)
			}
			if len(seen) != 1 || !reflect.DeepEqual(seen[0], tc.next) {
				t.Errorf("billing's request in the next request carried %v, want %v", seen, tc.next)
			}
		})
	}
}

// TestAReceiverInheritsTheQuestionChannel pins #234 over this front: a
// receiver without a ToolElicitor or ToolRecorder of its own, whether a
// WithHandoff switch within the request or the configuration a later
// request starts under by WithTransfers, puts its tools' questions to
// the adapter's own elicitor, and one with its own keeps it.
func TestAReceiverInheritsTheQuestionChannel(t *testing.T) {
	confirm := agenttool.NewFunc("confirm", "confirms the refund", json.RawMessage(`{"type":"object"}`),
		func(ctx context.Context, _ agenttool.Call) (agenttool.Result, error) {
			elicit, ok := agenttool.ElicitorFrom(ctx)
			if !ok {
				return agenttool.Text("nobody asked"), nil
			}
			if _, err := elicit(ctx, agenttool.Elicitation{Message: "refund?"}); err != nil {
				return agenttool.Result{}, err
			}
			return agenttool.Text("confirmed"), nil
		})
	route := func(billing agentturn.Config) Route {
		return func(call *openresponses.FunctionCall) (agentturn.Config, string, bool) {
			return billing, "transferred", call.Name == "transfer_to_billing"
		}
	}
	for _, tc := range []struct {
		name     string
		next     bool
		ownAsker bool
	}{
		{name: "a handoff within the request"},
		{name: "the next request's start", next: true},
		{name: "a receiver with its own", next: true, ownAsker: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var asked, askedOwn int
			billing := agentturn.Config{Name: "billing", Model: callsOnce{"confirm"}, Tools: []agenttool.Tool{confirm}}
			if tc.ownAsker {
				billing.ToolElicitor = func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) {
					askedOwn++
					return agenttool.Answer{Action: agenttool.ActionAccept}, nil
				}
			}
			triage := agentturn.Config{Name: "triage", Model: preamble{}, ModelName: "m", Tools: []agenttool.Tool{transfer},
				ToolElicitor: func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) {
					asked++
					return agenttool.Answer{Action: agenttool.ActionAccept}, nil
				}}
			req := request(openresponses.UserText("I was double charged"))
			opts := []Option{WithHandoff(func(context.Context, *agentturn.RunEnd, []*agentturn.ToolEnd) (agentturn.Config, bool) {
				return billing, true
			})}
			if tc.next {
				opts = []Option{WithTransfers(route(billing))}
				call := &openresponses.FunctionCall{CallID: "call_t", Name: "transfer_to_billing", Arguments: "{}"}
				req = request(openresponses.UserText("I was double charged"), call, openresponses.NewFunctionCallOutput("call_t", "transferred"), openresponses.UserText("refund it"))
			}
			sink, err := streamtest.Run(context.Background(), New(triage, opts...), req)
			if err != nil {
				t.Fatal(err)
			}
			if got := sink.Response().OutputText(); !strings.HasSuffix(got, "Tool result: confirmed") {
				t.Errorf("answer = %q, want the receiver's tool to have asked", got)
			}
			wantOwn, wantServing := 0, 1
			if tc.ownAsker {
				wantOwn, wantServing = 1, 0
			}
			if asked != wantServing || askedOwn != wantOwn {
				t.Errorf("asked the adapter's elicitor %d times and the receiver's %d, want %d and %d", asked, askedOwn, wantServing, wantOwn)
			}
		})
	}
}

// callsOnce calls name once, with no arguments, and echoes once the
// call has its output.
type callsOnce struct{ name string }

func (m callsOnce) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	calls := map[string]bool{}
	for _, item := range req.Input {
		switch it := item.(type) {
		case *openresponses.FunctionCall:
			if it.Name == m.name {
				calls[it.CallID] = true
			}
		case *openresponses.FunctionCallOutput:
			if calls[it.CallID] {
				return (&echo.Adapter{}).CreateStream(ctx, req, sink)
			}
		}
	}
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	w, err := em.FunctionCall("", m.name)
	if err != nil {
		return err
	}
	if err := w.Arguments(`{}`); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Complete()
}

// TestHandoffsWithinARequestAreBounded pins RFC 0001's MUST: a pair of
// agents that hand a request back and forth stop after the adapter's
// bound, WithMaxHandoffs or the default, and the response completes
// rather than running until the request's context ends.
func TestHandoffsWithinARequestAreBounded(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
		want int
	}{
		{"the default", nil, 10},
		{"WithMaxHandoffs", []Option{WithMaxHandoffs(3)}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := agentturn.Config{Name: "a", Model: callsAlways{"transfer_to_billing"}, Tools: []agenttool.Tool{transfer}}
			b := agentturn.Config{Name: "b", Model: callsAlways{"transfer_to_billing"}, Tools: []agenttool.Tool{transfer}}
			asked := 0
			opts := append([]Option{WithHandoff(func(context.Context, *agentturn.RunEnd, []*agentturn.ToolEnd) (agentturn.Config, bool) {
				asked++
				if asked%2 == 1 {
					return b, true
				}
				return a, true
			})}, tc.opts...)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			sink, err := streamtest.Run(ctx, New(a, opts...), request(openresponses.UserText("go")))
			if err != nil {
				t.Fatal(err)
			}
			if st := sink.Response().Status; st != openresponses.ResponseStatusCompleted {
				t.Errorf("response = %s", st)
			}
			if asked != tc.want {
				t.Errorf("handoffs taken = %d, want %d", asked, tc.want)
			}
		})
	}
}

// callsAlways calls name on every request.
type callsAlways struct{ name string }

func (m callsAlways) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	w, err := em.FunctionCall("", m.name)
	if err != nil {
		return err
	}
	if err := w.Arguments(`{}`); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Complete()
}
