package handoff

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

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
	a := agentturn.New(team.Config("A"))
	_, path, err := team.Run(context.Background(), a, openresponses.UserText("go"))
	if !errors.Is(err, ErrTooManyHandoffs) {
		t.Fatalf("err = %v, want ErrTooManyHandoffs", err)
	}
	// The agent agrees with a front's WithTransfers: the last transfer
	// handed the conversation on, so its receiver has it, unrun.
	if strings.Join(path, ",") != "A,B,A,B,A" {
		t.Errorf("path = %v, want three switches followed and the fourth taken", path)
	}
	to, _ := responses.HandedTo(a.State().Transcript, team.Route())
	if a.Config().Name != to.Name {
		t.Errorf("the agent is %s, the transcript hands to %s", a.Config().Name, to.Name)
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

// script calls names in turn, one per output the input holds, then
// answers with its name.
type script struct {
	name  string
	names []string
}

func (m script) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	n := 0
	for _, item := range req.Input {
		if _, ok := item.(*openresponses.FunctionCallOutput); ok {
			n++
		}
	}
	if n >= len(m.names) {
		if err := em.Item(openresponses.AssistantText(m.name + " here")); err != nil {
			return err
		}
		return em.Complete()
	}
	w, err := em.FunctionCall("", m.names[n])
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

// TestATeamFollowsATransferAfterAResume pins Team.Resume: a run that
// paused on a deferred call is resumed through the team, and a
// transfer the resumed run ends on is followed as Run follows one.
func TestATeamFollowsATransferAfterAResume(t *testing.T) {
	lookup := agenttool.NewFunc("lookup", "", nil, func(context.Context, agenttool.Call) (agenttool.Result, error) {
		return agenttool.Text("found"), nil
	})
	team, err := New(
		Agent{Config: agentturn.Config{Name: "Triage agent", Model: script{name: "triage", names: []string{"lookup", ToolName("Refund agent")}}, Tools: []agenttool.Tool{lookup},
			BeforeToolCall: func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
				if info.Call.Name == "lookup" {
					return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "ask"}, nil
				}
				return nil, nil
			}}, Handoffs: []To{{Agent: "Refund agent"}}},
		Agent{Config: agentturn.Config{Name: "Refund agent", Model: transfers{name: "refunds"}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(team.Config("Triage agent"))
	end, _, err := team.Run(context.Background(), a, openresponses.UserText("I was double charged"))
	if err != nil || end.Reason != agentturn.ReasonInputRequired {
		t.Fatalf("first run = %+v, %v; want it paused on lookup", end, err)
	}
	end, path, err := team.Resume(context.Background(), a, agentturn.Approve(end.Pending[0].Call.CallID))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(path, ",") != "Triage agent,Refund agent" {
		t.Errorf("path = %v", path)
	}
	if text, _ := end.Answer(); text != "refunds here" || a.Config().Name != "Refund agent" {
		t.Errorf("answer = %q under %s, want the receiver's", text, a.Config().Name)
	}
}

// TestATeamTrimsOverAFront pins Team.Handoff and Team.Trim: a team
// served through front/responses with WithHandoff and WithHandoffTrim
// applies the handoff's filter, so the receiver's first request holds
// no tool call and ends in the note naming the handoff.
func TestATeamTrimsOverAFront(t *testing.T) {
	var refundReqs requests
	team, err := New(
		Agent{Config: agentturn.Config{Name: "Triage agent", Model: transfers{name: "triage", to: "Refund agent", preamble: true}},
			Handoffs: []To{{Agent: "Refund agent", Filter: RemoveAllTools}}},
		Agent{Config: agentturn.Config{Name: "Refund agent", Model: transfers{name: "refunds"}, BeforeModelCall: refundReqs.hook}},
	)
	if err != nil {
		t.Fatal(err)
	}
	front := responses.New(team.Config("Triage agent"), responses.WithTransfers(team.Route()), responses.WithHandoff(team.Handoff), responses.WithHandoffTrim(team.Trim), responses.WithToolItems())
	first := openresponses.Request{Model: "m", Input: openresponses.Items{openresponses.UserText("I was double charged")}}
	sink, err := streamtest.Run(context.Background(), front, first)
	if err != nil {
		t.Fatal(err)
	}
	if got := sink.Response().OutputText(); !strings.HasSuffix(got, "refunds here") {
		t.Errorf("answer = %q", got)
	}
	if len(refundReqs.seen) != 1 || itemTypes(refundReqs.seen[0].Input) != "user assistant developer" {
		t.Fatalf("receiver's requests = %d, first input %v", len(refundReqs.seen), refundReqs.seen)
	}
	// The trim holds for the receiver's later requests (#276).
	next := openresponses.Request{Model: "m", Input: append(append(first.Input, sink.Response().Output...), openresponses.UserText("well?"))}
	if _, err := streamtest.Run(context.Background(), front, next); err != nil {
		t.Fatal(err)
	}
	if len(refundReqs.seen) != 2 || itemTypes(refundReqs.seen[1].Input) != "user assistant developer assistant user" {
		t.Errorf("receiver's next input = %v, want it trimmed as the first", refundReqs.seen)
	}
}

// handsOn calls its tool when the last input is the user saying "go",
// or, with always, whenever the last input is not a user message, and
// otherwise answers; it keeps the user texts it was sent.
type handsOn struct {
	tool   string
	always bool
	seen   *[]string
}

func (m handsOn) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	msg, user := req.Input[len(req.Input)-1].(*openresponses.Message)
	user = user && msg.Role == openresponses.RoleUser
	if user {
		*m.seen = append(*m.seen, msg.Text())
	}
	if user && msg.Text() == "go" || !user && m.always {
		w, err := em.FunctionCall("", m.tool)
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
	if err := em.Item(openresponses.AssistantText("ok")); err != nil {
		return err
	}
	return em.Complete()
}

// deliverAtEnd delivers text to a when the run of the configuration
// named from ends, and waits until the item is queued.
func deliverAtEnd(a *agentturn.Agent, from, text string) <-chan error {
	done := make(chan error, 1)
	delivered := false
	a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		if _, ok := ev.(*agentturn.RunEnd); !ok || delivered || a.Config().Name != from {
			return nil
		}
		delivered = true
		go func() {
			_, _, err := a.Deliver(context.Background(), openresponses.UserText(text))
			done <- err
		}()
		for a.State().Steering == 0 {
			time.Sleep(time.Millisecond)
		}
		return nil
	})
	return done
}

// TestADeliveryAfterATerminatingEndIsTaken pins the round-two review
// of #274 × #290: a delivery left queued by a run that ended on a
// terminating result the team does not hand off on, a tool's own
// answer or a transfer past the bound, is taken by a run the team
// starts for it, rather than waiting for the next input.
func TestADeliveryAfterATerminatingEndIsTaken(t *testing.T) {
	finish := agenttool.NewFunc("finish", "answers for the model", json.RawMessage(`{"type":"object"}`),
		func(context.Context, agenttool.Call) (agenttool.Result, error) {
			return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "finished"}, Terminate: true}, nil
		})
	t.Run("declined", func(t *testing.T) {
		var seen []string
		team, err := New(Agent{Config: agentturn.Config{Name: "A", Model: handsOn{tool: "finish", seen: &seen}, Tools: []agenttool.Tool{finish}}})
		if err != nil {
			t.Fatal(err)
		}
		a := agentturn.New(team.Config("A"))
		done := deliverAtEnd(a, "A", "the build passed")
		if _, _, err := team.Run(context.Background(), a, openresponses.UserText("go")); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil && !errors.Is(err, agentturn.ErrTerminated) {
			t.Fatalf("Deliver: %v", err)
		}
		if strings.Join(seen, "|") != "go|the build passed" {
			t.Errorf("the model was sent %q, want the delivery taken", seen)
		}
	})
	t.Run("at the bound", func(t *testing.T) {
		var seenA, seenB []string
		team, err := New(
			Agent{Config: agentturn.Config{Name: "A", Model: handsOn{tool: ToolName("B"), seen: &seenA}}, Handoffs: []To{{Agent: "B"}}},
			Agent{Config: agentturn.Config{Name: "B", Model: handsOn{tool: ToolName("A"), always: true, seen: &seenB}}, Handoffs: []To{{Agent: "A"}}},
		)
		if err != nil {
			t.Fatal(err)
		}
		team.MaxHandoffs = 1
		a := agentturn.New(team.Config("A"))
		done := deliverAtEnd(a, "B", "the build passed")
		if _, _, err := team.Run(context.Background(), a, openresponses.UserText("go")); !errors.Is(err, ErrTooManyHandoffs) {
			t.Fatalf("err = %v, want ErrTooManyHandoffs", err)
		}
		if err := <-done; err != nil && !errors.Is(err, agentturn.ErrTerminated) {
			t.Fatalf("Deliver: %v", err)
		}
		if strings.Join(seenA, "|") != "go|the build passed" {
			t.Errorf("A, which the last transfer handed to, was sent %q, want the delivery taken", seenA)
		}
	})
}

// TestTheHostsOfferCoversTheTransfers is #307: a team's Offer sees each
// agent's whole list, its own tools and the transfers, on every
// request, so a filter that removes transfer_to_refund_agent leaves it
// out of the request, a wrapper wraps it, and the receiver's own tools
// pass through the same offer.
func TestTheHostsOfferCoversTheTransfers(t *testing.T) {
	for _, deny := range []bool{true, false} {
		var triage, refund requests
		team, err := New(
			Agent{Config: agentturn.Config{Name: "Triage agent", Model: transfers{name: "triage", to: "Refund agent"}, Tools: []agenttool.Tool{issueRefund}, BeforeModelCall: triage.hook},
				Handoffs: []To{{Agent: "Refund agent"}}},
			Agent{Config: agentturn.Config{Name: "Refund agent", Model: transfers{name: "refunds"}, Tools: []agenttool.Tool{issueRefund}, BeforeModelCall: refund.hook}},
		)
		if err != nil {
			t.Fatal(err)
		}
		offered := map[string][]string{}
		team.Offer = func(_ context.Context, agent string, tools []agenttool.Tool) []agenttool.Tool {
			var out []agenttool.Tool
			for _, tool := range tools {
				offered[agent] = append(offered[agent], tool.Name())
				if deny && tool.Name() == ToolName("Refund agent") {
					continue
				}
				out = append(out, tool)
			}
			return out
		}
		end, path, err := team.Run(context.Background(), agentturn.New(team.Config("Triage agent")), openresponses.UserText("I was double charged"))
		if err != nil {
			t.Fatal(err)
		}
		if got, want := strings.Join(toolNames(triage.seen[0]), ","), "issue_refund"; deny && got != want {
			t.Errorf("denied: the triage request offers %s, want %s", got, want)
		}
		if want := "issue_refund," + ToolName("Refund agent"); !strings.HasPrefix(strings.Join(offered["Triage agent"], ","), want) {
			t.Errorf("deny %v: the offer saw %v for the triage agent, want %s", deny, offered["Triage agent"], want)
		}
		if deny {
			// The model called a transfer it was not offered: the loop
			// answers it as unknown, and nothing hands off.
			if strings.Join(path, ",") != "Triage agent" || len(refund.seen) != 0 {
				t.Errorf("denied: path %v, refund requests %d; end %+v", path, len(refund.seen), end)
			}
			continue
		}
		if strings.Join(path, ",") != "Triage agent,Refund agent" {
			t.Fatalf("allowed: path = %v", path)
		}
		if got := offered["Refund agent"]; len(got) == 0 || got[0] != "issue_refund" {
			t.Errorf("allowed: the offer saw %v for the receiver", got)
		}
	}
}

// TestAnOfferWrapperKeepsTheTransfer is the l6-3c review's L1 and L6: a
// wrapper [Team.Offer] puts around every tool, a policy's or an
// audit's, reaches the transfer and runs for it, and the handoff is
// taken while the wrapper keeps the transfer's name and result; one
// that changes the result makes the call no handoff, as a blocked
// call is not, so the team stays with the sender, as Offer's doc
// says.
func TestAnOfferWrapperKeepsTheTransfer(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(agenttool.Result) agenttool.Result
		path   string
	}{
		{name: "kept", change: func(r agenttool.Result) agenttool.Result { return r }, path: "Triage agent,Refund agent"},
		{name: "changed", change: func(r agenttool.Result) agenttool.Result {
			r.Output.Text += " (audited)"
			return r
		}, path: "Triage agent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			team, err := New(
				Agent{Config: agentturn.Config{Name: "Triage agent", Model: transfers{name: "triage", to: "Refund agent"}, Tools: []agenttool.Tool{issueRefund}},
					Handoffs: []To{{Agent: "Refund agent"}}},
				Agent{Config: agentturn.Config{Name: "Refund agent", Model: transfers{name: "refunds"}, Tools: []agenttool.Tool{issueRefund}}},
			)
			if err != nil {
				t.Fatal(err)
			}
			var ran []string
			team.Offer = func(_ context.Context, _ string, tools []agenttool.Tool) []agenttool.Tool {
				out := make([]agenttool.Tool, len(tools))
				for i, tool := range tools {
					out[i] = agenttool.Wrap(tool, func(ctx context.Context, call agenttool.Call) (agenttool.Result, error) {
						ran = append(ran, tool.Name())
						r, err := tool.Execute(ctx, call)
						return tc.change(r), err
					})
				}
				return out
			}
			_, path, err := team.Run(context.Background(), agentturn.New(team.Config("Triage agent")), openresponses.UserText("I was double charged"))
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(ran, ToolName("Refund agent")) {
				t.Errorf("the wrapper ran for %v, not the transfer", ran)
			}
			if got := strings.Join(path, ","); got != tc.path {
				t.Errorf("path = %s, want %s", got, tc.path)
			}
		})
	}
}

// TestATransferNameAmongTheToolsOfTheMoment is the l6-3c review's L2:
// a tool of the moment that takes a transfer's name, an MCP server's
// transfer_to_refund_agent, which [New] cannot see in Config.Tools,
// fails the turn by the loop's check that the tools of a request have
// distinct names, with [Team.Offer] set or not.
func TestATransferNameAmongTheToolsOfTheMoment(t *testing.T) {
	for _, offer := range []bool{false, true} {
		clash := agenttool.NewFunc(ToolName("Refund agent"), "a remote server's tool", json.RawMessage(`{"type":"object"}`),
			func(context.Context, agenttool.Call) (agenttool.Result, error) { return agenttool.Text("remote"), nil })
		team, err := New(
			Agent{Config: agentturn.Config{Name: "Triage agent", Model: transfers{name: "triage", to: "Refund agent"},
				ToolProvider: func(context.Context) []agenttool.Tool { return []agenttool.Tool{issueRefund, clash} }},
				Handoffs: []To{{Agent: "Refund agent"}}},
			Agent{Config: agentturn.Config{Name: "Refund agent", Model: transfers{name: "refunds"}, Tools: []agenttool.Tool{issueRefund}}},
		)
		if err != nil {
			t.Fatal(err)
		}
		if offer {
			team.Offer = func(_ context.Context, _ string, tools []agenttool.Tool) []agenttool.Tool { return tools }
		}
		end, path, err := team.Run(context.Background(), agentturn.New(team.Config("Triage agent")), openresponses.UserText("I was double charged"))
		if err == nil && (end == nil || end.Err == nil) {
			t.Errorf("offer %v: the run took the clash, path %v, end %+v", offer, path, end)
			continue
		}
		if err == nil {
			err = end.Err
		}
		if !strings.Contains(err.Error(), "duplicate name") {
			t.Errorf("offer %v: err = %v, want a duplicate name", offer, err)
		}
	}
}
