package handoff

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/front/responses"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/streamtest"
)

// transfers is a model that hands off: on a user message it says a
// preamble, when it has one, and calls the transfer tool to its
// destination; on anything else, or when it hands to nobody, it answers
// with its name.
type transfers struct {
	name, to string
	preamble bool
	// always hands off whatever the input ends with.
	always bool
}

func (m transfers) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	last := req.Input[len(req.Input)-1]
	user := false
	if msg, ok := last.(*openresponses.Message); ok && msg.Role == openresponses.RoleUser {
		user = true
	}
	if m.to == "" || !user && !m.always {
		if err := em.Item(openresponses.AssistantText(m.name + " here")); err != nil {
			return err
		}
		return em.Complete()
	}
	if m.preamble {
		if err := em.Item(openresponses.AssistantText("Let me get you to " + m.to + ".")); err != nil {
			return err
		}
	}
	w, err := em.FunctionCall("", ToolName(m.to))
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

// requests records each request a configuration's model is sent.
type requests struct{ seen []*openresponses.Request }

func (r *requests) hook(_ context.Context, req *openresponses.Request) error {
	c := *req
	r.seen = append(r.seen, &c)
	return nil
}

func toolNames(req *openresponses.Request) []string {
	var out []string
	for _, t := range req.Tools {
		if ft, ok := t.(*openresponses.FunctionTool); ok {
			out = append(out, ft.Name)
		}
	}
	return out
}

var issueRefund = agenttool.NewFunc("issue_refund", "refunds a charge", json.RawMessage(`{"type":"object"}`),
	func(context.Context, agenttool.Call) (agenttool.Result, error) {
		return agenttool.Text("refunded"), nil
	})

// TestATeamHandsOff pins #274's rules for the switch loop: the receiver
// continues under a trigger of kind "handoff" naming the sender, with
// its own instructions and tools, and the path names who had the
// conversation; a receiver with no question or record channel of its
// own inherits the sender's.
func TestATeamHandsOff(t *testing.T) {
	var refundReqs requests
	var triggers []agentturn.Trigger
	elicit := func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) {
		return agenttool.Answer{Action: agenttool.ActionAccept}, nil
	}
	team, err := New(
		Agent{Config: agentturn.Config{Name: "Triage agent", Description: "routes", Model: transfers{name: "triage", to: "Refund agent", preamble: true}, Instructions: "You triage.", ToolElicitor: elicit},
			Handoffs: []To{{Agent: "Refund agent"}}},
		Agent{Config: agentturn.Config{Name: "Refund agent", Description: "refunds charges", Model: transfers{name: "refunds"}, Instructions: "You refund.", Tools: []agenttool.Tool{issueRefund},
			BeforeModelCall: refundReqs.hook,
			BeforeTurn: func(ctx context.Context, _ agentturn.TurnStartInfo) (openresponses.Items, error) {
				triggers = append(triggers, agentturn.TriggerFromContext(ctx))
				return nil, nil
			}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(team.Config("Triage agent"))
	end, path, err := team.Run(context.Background(), a, openresponses.UserText("I was double charged"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(path, ",") != "Triage agent,Refund agent" {
		t.Errorf("path = %v", path)
	}
	if text, _ := end.Answer(); text != "refunds here" {
		t.Errorf("answer = %q", text)
	}
	if len(triggers) != 1 || triggers[0].Kind != "handoff" || triggers[0].Ref != "Triage agent" {
		t.Errorf("receiver's triggers = %+v, want one of kind handoff naming the sender", triggers)
	}
	if len(refundReqs.seen) != 1 {
		t.Fatalf("refund agent's requests = %d", len(refundReqs.seen))
	}
	req := refundReqs.seen[0]
	if req.Instructions != "You refund." || strings.Join(toolNames(req), ",") != "issue_refund" {
		t.Errorf("receiver's request: instructions %q, tools %v", req.Instructions, toolNames(req))
	}
	// The history is kept: the transfer call and its output are there.
	if !strings.Contains(itemTypes(req.Input), "function_call function_call_output") {
		t.Errorf("receiver's input = %s, want the history kept", itemTypes(req.Input))
	}
	if a.Config().Name != "Refund agent" || a.Config().ToolElicitor == nil {
		t.Errorf("agent's configuration = %s with elicitor %v, want the receiver inheriting the sender's", a.Config().Name, a.Config().ToolElicitor != nil)
	}
}

// TestAFilteredHandoffLeavesAContinuableTranscript pins #276: a trim
// between runs that would leave a model output last gets a developer
// message naming the transfer, the sender and the receiver, so the
// receiver's first request ends in input the model can answer; a trim
// that leaves an input last gets none.
func TestAFilteredHandoffLeavesAContinuableTranscript(t *testing.T) {
	for _, tc := range []struct {
		name     string
		preamble bool
		want     string
	}{
		{"a preamble left last", true, "user assistant developer"},
		{"the user's message left last", false, "user"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var refundReqs requests
			team, err := New(
				Agent{Config: agentturn.Config{Name: "Triage agent", Model: transfers{name: "triage", to: "Refund agent", preamble: tc.preamble}},
					Handoffs: []To{{Agent: "Refund agent", Filter: RemoveAllTools}}},
				Agent{Config: agentturn.Config{Name: "Refund agent", Model: transfers{name: "refunds"}, BeforeModelCall: refundReqs.hook}},
			)
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(team.Config("Triage agent"))
			end, path, err := team.Run(context.Background(), a, openresponses.UserText("I was double charged"))
			if err != nil {
				t.Fatalf("run: %v (path %v)", err, path)
			}
			if text, _ := end.Answer(); text != "refunds here" || len(refundReqs.seen) != 1 {
				t.Fatalf("answer = %q after %d receiver requests", text, len(refundReqs.seen))
			}
			in := refundReqs.seen[0].Input
			if got := itemTypes(in); got != tc.want {
				t.Fatalf("receiver's input = %s, want %s", got, tc.want)
			}
			if tc.preamble {
				note := in[len(in)-1].(*openresponses.Message).Text()
				for _, s := range []string{ToolName("Refund agent"), "Triage agent", "Refund agent"} {
					if !strings.Contains(note, s) {
						t.Errorf("note %q does not name %q", note, s)
					}
				}
			}
		})
	}
}

// TestHandoffsAreBounded pins #274's MUST: a ping-pong stops after
// MaxHandoffs switches for one input, with ErrTooManyHandoffs.
func TestHandoffsAreBounded(t *testing.T) {
	team, err := New(
		Agent{Config: agentturn.Config{Name: "A", Model: transfers{name: "a", to: "B", always: true}}, Handoffs: []To{{Agent: "B"}}},
		Agent{Config: agentturn.Config{Name: "B", Model: transfers{name: "b", to: "A", always: true}}, Handoffs: []To{{Agent: "A"}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	team.MaxHandoffs = 3
	_, path, err := team.Run(context.Background(), agentturn.New(team.Config("A")), openresponses.UserText("go"))
	if !errors.Is(err, ErrTooManyHandoffs) {
		t.Fatalf("err = %v, want ErrTooManyHandoffs", err)
	}
	if strings.Join(path, ",") != "A,B,A,B" {
		t.Errorf("path = %v, want three switches", path)
	}
}

// TestADisabledHandoffIsNotOffered pins To.Enabled: a handoff it says
// no to is not among the tools of the request.
func TestADisabledHandoffIsNotOffered(t *testing.T) {
	type key struct{}
	for _, on := range []bool{true, false} {
		var reqs requests
		team, err := New(
			Agent{Config: agentturn.Config{Name: "Triage agent", Model: transfers{name: "triage"}, Tools: []agenttool.Tool{issueRefund}, BeforeModelCall: reqs.hook},
				Handoffs: []To{{Agent: "Refund agent", Enabled: func(ctx context.Context) bool { return ctx.Value(key{}) == true }}}},
			Agent{Config: agentturn.Config{Name: "Refund agent", Model: transfers{name: "refunds"}}},
		)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.WithValue(context.Background(), key{}, on)
		if _, _, err := team.Run(ctx, agentturn.New(team.Config("Triage agent")), openresponses.UserText("hi")); err != nil {
			t.Fatal(err)
		}
		want := "issue_refund"
		if on {
			want += "," + ToolName("Refund agent")
		}
		if got := strings.Join(toolNames(reqs.seen[0]), ","); got != want {
			t.Errorf("enabled %v: tools %s, want %s", on, got, want)
		}
	}
}

// TestTheRouteServesTheTeamOverAFront pins Route: the same team over
// front/responses' WithTransfers starts the conversation's next request
// under the receiver, the transfer found by the tool's own output.
func TestTheRouteServesTheTeamOverAFront(t *testing.T) {
	team, err := New(
		Agent{Config: agentturn.Config{Name: "Triage agent", Model: transfers{name: "triage", to: "Refund agent"}}, Handoffs: []To{{Agent: "Refund agent"}}},
		Agent{Config: agentturn.Config{Name: "Refund agent", Model: transfers{name: "refunds"}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	front := responses.New(team.Config("Triage agent"), responses.WithTransfers(team.Route()), responses.WithToolItems())
	first := openresponses.Request{Model: "m", Input: openresponses.Items{openresponses.UserText("I was double charged")}}
	sink, err := streamtest.Run(context.Background(), front, first)
	if err != nil {
		t.Fatal(err)
	}
	next := openresponses.Request{Model: "m", Input: append(append(first.Input, sink.Response().Output...), openresponses.UserText("well?"))}
	sink, err = streamtest.Run(context.Background(), front, next)
	if err != nil {
		t.Fatal(err)
	}
	if got := sink.Response().OutputText(); got != "refunds here" {
		t.Errorf("next request answered %q, want the receiver", got)
	}
	cfg, output, ok := team.Route()(&openresponses.FunctionCall{Name: ToolName("Refund agent")})
	if !ok || cfg.Name != "Refund agent" || output == "" {
		t.Errorf("route = %s %q %v", cfg.Name, output, ok)
	}
	if _, _, ok := team.Route()(&openresponses.FunctionCall{Name: "issue_refund"}); ok {
		t.Error("route takes a call that is no transfer")
	}
}

// TestNewRefusesABadTeam checks the team is checked when it is built.
func TestNewRefusesABadTeam(t *testing.T) {
	m := transfers{name: "x"}
	for _, tc := range []struct {
		name   string
		agents []Agent
	}{
		{"an unknown receiver", []Agent{{Config: agentturn.Config{Name: "A", Model: m}, Handoffs: []To{{Agent: "B"}}}}},
		{"two agents of one name", []Agent{{Config: agentturn.Config{Name: "A", Model: m}}, {Config: agentturn.Config{Name: "A", Model: m}}}},
		{"an agent with no name", []Agent{{Config: agentturn.Config{Model: m}}}},
		{"a handoff to itself", []Agent{{Config: agentturn.Config{Name: "A", Model: m}, Handoffs: []To{{Agent: "A"}}}}},
		{"the same handoff twice", []Agent{{Config: agentturn.Config{Name: "A", Model: m}, Handoffs: []To{{Agent: "B"}, {Agent: "B"}}}, {Config: agentturn.Config{Name: "B", Model: m}}}},
		{"two names one tool", []Agent{{Config: agentturn.Config{Name: "Refund agent", Model: m}}, {Config: agentturn.Config{Name: "refund_agent", Model: m}}}},
		{"a tool of the transfer's name", []Agent{{Config: agentturn.Config{Name: "A", Model: m, Tools: []agenttool.Tool{agenttool.NewFunc("transfer_to_b", "", nil, issueRefund.Execute)}}, Handoffs: []To{{Agent: "B"}}}, {Config: agentturn.Config{Name: "B", Model: m}}}},
	} {
		if _, err := New(tc.agents...); err == nil {
			t.Errorf("%s: New took it", tc.name)
		}
	}
}

// TestToolName pins the transfer tool's name, the Agents SDK's: the
// agent's name lowered, each character that is not a letter or a digit
// an underscore.
func TestToolName(t *testing.T) {
	for in, want := range map[string]string{
		"Billing agent": "transfer_to_billing_agent",
		"refunds":       "transfer_to_refunds",
		"Agent-2 (EU)":  "transfer_to_agent_2__eu_",
		"Ünïcode desk":  "transfer_to__n_code_desk",
	} {
		if got := ToolName(in); got != want {
			t.Errorf("ToolName(%q) = %q, want %q", in, got, want)
		}
	}
}

func itemTypes(items openresponses.Items) string {
	parts := make([]string, 0, len(items))
	for _, it := range items {
		typ := it.ItemType()
		if m, ok := it.(*openresponses.Message); ok {
			typ = string(m.Role)
		}
		parts = append(parts, typ)
	}
	return strings.Join(parts, " ")
}
