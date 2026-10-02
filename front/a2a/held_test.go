package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
)

// holds is a BeforeToolCall that defers every call to name with
// reason, as a policy that asks a person does.
func holds(name, reason string) func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
	return func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
		if info.Call.Name == name {
			return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: reason}, nil
		}
		return nil, nil
	}
}

// answering returns an elicitor that answers every question with action
// and appends the call it was asked about to asked.
func answering(action agenttool.Action, asked *[]agentturn.AskedCall) agenttool.Elicitor {
	return func(ctx context.Context, _ agenttool.Elicitation) (agenttool.Answer, error) {
		call, _ := agentturn.AskedCallFrom(ctx)
		*asked = append(*asked, call)
		return agenttool.Answer{Action: action}, nil
	}
}

// decisionsOf subscribes to the agent's tool_start events and appends
// each decision to out, for a WithRecorderFor.
func decisionsOf(out *[]*agentturn.ToolDecision) RecorderFor {
	return func(ctx context.Context, _ string, a *agentturn.Agent) (context.Context, func(), error) {
		return ctx, a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
			if s, ok := ev.(*agentturn.ToolStart); ok {
				*out = append(*out, s.Decision)
			}
			return nil
		}), nil
	}
}

// TestHeldOwnCallIsAskedNotHandedToCaller pins #209: a call to one of
// the agent's own tools that its BeforeToolCall held is a question for
// the serving side, put to Config.ToolElicitor, and never shown to the
// caller as input-required, whose answer would stand in for the tool's
// output. The person accepts and the tool runs, by "human" with the
// hook's reason; declines and the model sees the refusal; and with no
// elicitor the call is refused with a stated reason. With and without
// caller-owned tools in play, since the hook that splits the two is
// installed either way.
func TestHeldOwnCallIsAskedNotHandedToCaller(t *testing.T) {
	const reason = "a person approves refunds"
	for _, tc := range []struct {
		name     string
		action   agenttool.Action // "" for no elicitor
		ran      int
		asked    int
		text     string
		decision agentturn.ToolDecision
	}{
		{"accepted", agenttool.ActionAccept, 1, 1, "Tool result: refunded",
			agentturn.ToolDecision{Action: agentturn.Allow, Reason: reason, By: "human"}},
		{"declined", agenttool.ActionDecline, 0, 1, "Tool result: Error: declined when asked: " + reason,
			agentturn.ToolDecision{Action: agentturn.Block, Reason: "declined when asked: " + reason, By: "human"}},
		{"no elicitor", "", 0, 0, "Tool result: Error: a call to the agent's own tool cannot be handed to the caller: " + reason,
			agentturn.ToolDecision{Action: agentturn.Block, Reason: "a call to the agent's own tool cannot be handed to the caller: " + reason}},
	} {
		for _, callers := range []bool{false, true} {
			name := tc.name
			if callers {
				name += " with caller tools"
			}
			t.Run(name, func(t *testing.T) {
				var ran int
				refund := agenttool.NewFunc("issue_refund", "refunds a charge", json.RawMessage(`{"type":"object"}`),
					func(context.Context, agenttool.Call) (agenttool.Result, error) {
						ran++
						return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "refunded"}}, nil
					})
				var asked []agentturn.AskedCall
				cfg := agentturn.Config{Model: callsOnce{"issue_refund"}, Tools: []agenttool.Tool{refund}, BeforeToolCall: holds("issue_refund", reason)}
				if tc.action != "" {
					cfg.ToolElicitor = answering(tc.action, &asked)
				}
				var decisions []*agentturn.ToolDecision
				store := &MemoryStore{}
				opts := []Option{WithConversationStore(store), WithRecorderFor(decisionsOf(&decisions))}
				if callers {
					opts = append(opts, WithCallerTools(openresponses.NewFunctionTool("remote", "caller side", json.RawMessage(`{"type":"object"}`))))
				}
				task := sendTask(t, a2asrv.NewHandler(New(cfg, opts...)), userMessage("refund me"))
				if task.Status.State != a2a.TaskStateCompleted || taskText(task) != tc.text {
					t.Fatalf("task = %s %q, want completed %q", task.Status.State, taskText(task), tc.text)
				}
				if ran != tc.ran {
					t.Errorf("issue_refund ran %d times, want %d", ran, tc.ran)
				}
				if task.Status.Message != nil {
					if _, ok := task.Status.Message.Metadata[MetaPendingCalls]; ok {
						t.Errorf("status message lists pending calls: %v", task.Status.Message.Metadata[MetaPendingCalls])
					}
					for _, part := range task.Status.Message.Parts {
						if _, ok := part.(a2a.DataPart); ok {
							t.Errorf("status message shows the caller a call: %+v", part)
						}
					}
				}
				if len(asked) != tc.asked {
					t.Fatalf("elicitor asked %d times, want %d: %+v", len(asked), tc.asked, asked)
				}
				if tc.asked == 1 {
					want := agentturn.AskedCall{CallID: asked[0].CallID, Name: "issue_refund", Args: json.RawMessage(`{}`),
						Decision: &agentturn.ToolDecision{Action: agentturn.Defer, Reason: reason}}
					if asked[0].CallID == "" || !reflect.DeepEqual(asked[0], want) {
						t.Errorf("asked about %+v (decision %+v), want %+v", asked[0], asked[0].Decision, want)
					}
				}
				if len(decisions) != 1 || decisions[0] == nil || !reflect.DeepEqual(*decisions[0], tc.decision) {
					t.Errorf("tool_start decisions = %+v, want one %+v", decisions, tc.decision)
				}
				stored, _ := store.Load(context.Background(), task.ContextID)
				if len(unanswered(stored)) != 0 {
					t.Errorf("stored conversation holds %d unanswered call(s): %s", len(unanswered(stored)), itemTypes(stored))
				}
			})
		}
	}
}

// serving returns a BeforeModelCall that appends name to served, so a
// test knows which agent answered.
func serving(name string, served *[]string) func(context.Context, *openresponses.Request) error {
	return func(context.Context, *openresponses.Request) error {
		*served = append(*served, name)
		return nil
	}
}

// TestHeldTransferIsNotAHandoff pins #209's (b'): a transfer the
// sender's own hook held, under WithTransfers and WithHandoff, is put
// to the elicitor. Declined, no handoff happens and the conversation's
// next task is still the sender's; accepted, the handoff proceeds as it
// would unheld and the next task starts at the receiver. The caller is
// never asked to answer the transfer, so its text cannot name the next
// agent.
func TestHeldTransferIsNotAHandoff(t *testing.T) {
	for _, tc := range []struct {
		name     string
		action   agenttool.Action
		handoffs int
		first    []string
		text     string
		next     []string
	}{
		// Declined, the sender keeps the conversation: its model, echo
		// once the transfer has an output, calls the transfer again on
		// the next message and is declined again, so triage serves the
		// next task twice and refunds never does.
		{"declined", agenttool.ActionDecline, 0, []string{"triage", "triage"},
			"Tool result: Error: declined when asked: a person approves an escalation to refunds", []string{"triage", "triage"}},
		{"accepted", agenttool.ActionAccept, 1, []string{"triage", "refunds"}, "Tool result: transferred", []string{"refunds"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var served []string
			refunds := agentturn.Config{Name: "refunds", Model: &echo.Adapter{}, BeforeModelCall: serving("refunds", &served)}
			route := func(call *openresponses.FunctionCall) (agentturn.Config, string, bool) {
				return refunds, "transferred", call.Name == "transfer_to_refunds_agent"
			}
			transfer := agenttool.NewFunc("transfer_to_refunds_agent", "hands the conversation to refunds", json.RawMessage(`{"type":"object"}`),
				func(context.Context, agenttool.Call) (agenttool.Result, error) {
					return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "transferred"}, Terminate: true}, nil
				})
			var asked []agentturn.AskedCall
			triage := agentturn.Config{Name: "triage", Model: callsOnce{"transfer_to_refunds_agent"}, Tools: []agenttool.Tool{transfer},
				BeforeToolCall:  holds("transfer_to_refunds_agent", "a person approves an escalation to refunds"),
				BeforeModelCall: serving("triage", &served),
				ToolElicitor:    answering(tc.action, &asked)}
			handoffs := 0
			h := a2asrv.NewHandler(New(triage, WithTransfers(route),
				WithHandoff(func(context.Context, *agentturn.RunEnd, []*agentturn.ToolEnd) (agentturn.Config, bool) {
					handoffs++
					return refunds, true
				})))
			first := sendTask(t, h, userMessage("refund my last charge"))
			if first.Status.State != a2a.TaskStateCompleted || taskText(first) != tc.text {
				t.Fatalf("first task = %s %q, want completed %q", first.Status.State, taskText(first), tc.text)
			}
			if handoffs != tc.handoffs || !reflect.DeepEqual(served, tc.first) {
				t.Errorf("first task: %d handoff(s), served by %v; want %d, %v", handoffs, served, tc.handoffs, tc.first)
			}
			if len(asked) != 1 || asked[0].Name != "transfer_to_refunds_agent" {
				t.Errorf("elicitor asked about %+v, want the transfer once", asked)
			}
			served = nil
			next := userMessage("go ahead")
			next.ContextID = first.ContextID
			task := sendTask(t, h, next)
			if task.Status.State != a2a.TaskStateCompleted || !reflect.DeepEqual(served, tc.next) {
				t.Errorf("next task = %s served by %v, want completed by %v", task.Status.State, served, tc.next)
			}
		})
	}
}

// TestAnswerToOwnCallRefused pins #209's guard in checkAnswers: a stored
// conversation that holds a pending call to one of the agent's own
// tools, which only an older release or a seeded store can leave, is
// not the caller's to answer. A function_call_output for it is refused
// as invalid params and the tool never runs; one for a pending call to
// a caller-owned tool is taken as before.
func TestAnswerToOwnCallRefused(t *testing.T) {
	var ran int
	refund := agenttool.NewFunc("issue_refund", "refunds a charge", json.RawMessage(`{"type":"object"}`),
		func(context.Context, agenttool.Call) (agenttool.Result, error) {
			ran++
			return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "refunded"}}, nil
		})
	refunds := agentturn.Config{Name: "refunds", Model: &echo.Adapter{}, Tools: []agenttool.Tool{refund}}
	route := func(call *openresponses.FunctionCall) (agentturn.Config, string, bool) {
		return refunds, "transferred", call.Name == "transfer_to_refunds_agent"
	}
	for _, tc := range []struct {
		name    string
		tools   []agenttool.Tool
		opts    []Option
		pending string
		declare string
		refused bool
	}{
		{"the agent's own tool", []agenttool.Tool{refund}, nil, "issue_refund", "", true},
		{"a tool of the configuration the task starts under", nil, []Option{WithStart(func(context.Context, agentturn.Transcript) (agentturn.Config, bool) {
			return refunds, true
		})}, "issue_refund", "", true},
		{"a transfer the route takes", nil, []Option{WithTransfers(route)}, "transfer_to_refunds_agent", "", true},
		{"a caller-owned tool", nil, nil, "remote", "remote", false},
		// The executor's configuration owns the name too, and a receiver
		// of a handoff without it offered the caller's stub: the pending
		// call is the caller's (review of #209).
		{"a caller tool the agent's configuration shadows", []agenttool.Tool{refund}, []Option{WithCallerTools(&openresponses.FunctionTool{Name: "issue_refund", Parameters: json.RawMessage(`{"type":"object"}`)})}, "issue_refund", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ran = 0
			store := &MemoryStore{}
			if err := store.Save(context.Background(), "c1", openresponses.Items{
				openresponses.UserText("refund me"),
				&openresponses.FunctionCall{CallID: "call_1", Name: tc.pending, Arguments: "{}"},
			}); err != nil {
				t.Fatal(err)
			}
			h := a2asrv.NewHandler(New(agentturn.Config{Name: "triage", Model: &echo.Adapter{}, Tools: tc.tools},
				append([]Option{WithConversationStore(store)}, tc.opts...)...))
			msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.DataPart{Data: map[string]any{"type": "function_call_output", "call_id": "call_1", "output": "approved by the caller"}})
			if tc.declare != "" {
				msg.SetMeta(MetaCallerTools, []any{map[string]any{"type": "function", "name": tc.declare, "parameters": map[string]any{"type": "object"}}})
			}
			msg.ContextID = "c1"
			res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: msg})
			if !tc.refused {
				if err != nil {
					t.Fatalf("send: %v", err)
				}
				if task, _ := res.(*a2a.Task); task == nil || task.Status.State != a2a.TaskStateCompleted {
					t.Errorf("task = %+v, want completed", res)
				}
				return
			}
			if !errors.Is(err, a2a.ErrInvalidParams) || res != nil || !strings.Contains(err.Error(), "agent's own") {
				t.Fatalf("send = %+v, %v; want refused as invalid params naming the agent's own call", res, err)
			}
			if ran != 0 {
				t.Errorf("issue_refund ran %d times", ran)
			}
			if stored, _ := store.Load(context.Background(), "c1"); len(stored) != 2 {
				t.Errorf("stored = %s, want the conversation as it was", itemTypes(stored))
			}
		})
	}
}

// TestDeferredStubSurvivesAReceiverOwningItsName pins that ownership of
// a pending call is judged by the configuration that made it: triage
// offered the caller's declared lookup stub and deferred the model's
// call to it in the batch that also ran the transfer to refunds, which
// owns a lookup of its own. The next message starts under refunds, yet
// the pending lookup is the caller's: a message that does not answer it
// is shown it again, and the caller's answer is taken (review of #209).
func TestDeferredStubSurvivesAReceiverOwningItsName(t *testing.T) {
	var ran []string
	lookup := agenttool.NewFunc("lookup", "looks up", json.RawMessage(`{"type":"object"}`),
		func(_ context.Context, call agenttool.Call) (agenttool.Result, error) {
			ran = append(ran, call.ID)
			return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "found"}}, nil
		})
	refunds := agentturn.Config{Name: "refunds", Model: &echo.Adapter{}, Tools: []agenttool.Tool{lookup}}
	route := func(call *openresponses.FunctionCall) (agentturn.Config, string, bool) {
		return refunds, "transferred", call.Name == "transfer_to_refunds_agent"
	}
	for _, tc := range []struct {
		name    string
		answer  bool
		want    a2a.TaskState
		pending int
	}{
		{"a message that does not answer it", false, a2a.TaskStateInputRequired, 1},
		{"the caller's answer", true, a2a.TaskStateCompleted, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ran = nil
			store := &MemoryStore{}
			if err := store.Save(context.Background(), "c1", openresponses.Items{
				openresponses.UserText("look up and transfer me"),
				&openresponses.FunctionCall{CallID: "call_1", Name: "lookup", Arguments: "{}"},
				&openresponses.FunctionCall{CallID: "call_2", Name: "transfer_to_refunds_agent", Arguments: "{}"},
				openresponses.NewFunctionCallOutput("call_2", "transferred"),
			}); err != nil {
				t.Fatal(err)
			}
			h := a2asrv.NewHandler(New(agentturn.Config{Name: "triage", Model: &echo.Adapter{}}, WithConversationStore(store), WithTransfers(route)))
			var msg *a2a.Message
			if tc.answer {
				msg = a2a.NewMessage(a2a.MessageRoleUser, a2a.DataPart{Data: map[string]any{"type": "function_call_output", "call_id": "call_1", "output": "INV-42"}})
			} else {
				msg = a2a.NewMessage(a2a.MessageRoleUser, a2a.TextPart{Text: "any news?"})
			}
			msg.ContextID = "c1"
			res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: msg})
			if err != nil {
				t.Fatalf("send: %v", err)
			}
			task, _ := res.(*a2a.Task)
			if task == nil || task.Status.State != tc.want {
				t.Fatalf("task = %+v, want %s", res, tc.want)
			}
			stored, _ := store.Load(context.Background(), "c1")
			if got := unanswered(stored); len(got) != tc.pending {
				t.Errorf("stored conversation waits on %d call(s), want %d: %s", len(got), tc.pending, itemTypes(stored))
			}
			for _, id := range ran {
				if id == "call_1" {
					t.Errorf("the caller's call ran here: %v", ran)
				}
			}
		})
	}
}

// TestStaleOwnCallIsClosed pins the other half of #209's guard: a
// stored conversation holding a pending call to one of the agent's own
// tools, left by an older release, is not wedged. A message that does
// not answer it goes on, the call is dropped from the conversation, the
// caller is shown no pending call, and the tool never runs.
func TestStaleOwnCallIsClosed(t *testing.T) {
	// The echo model calls the first tool it is offered, so the new run
	// makes a call of its own; the stale call_1 must not be the one run.
	var ran []string
	refund := agenttool.NewFunc("issue_refund", "refunds a charge", json.RawMessage(`{"type":"object"}`),
		func(_ context.Context, call agenttool.Call) (agenttool.Result, error) {
			ran = append(ran, call.ID)
			return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "refunded"}}, nil
		})
	store := &MemoryStore{}
	if err := store.Save(context.Background(), "c1", openresponses.Items{
		openresponses.UserText("refund me"),
		&openresponses.FunctionCall{CallID: "call_1", Name: "issue_refund", Arguments: "{}"},
	}); err != nil {
		t.Fatal(err)
	}
	h := a2asrv.NewHandler(New(agentturn.Config{Name: "triage", Model: &echo.Adapter{}, Tools: []agenttool.Tool{refund}}, WithConversationStore(store)))
	msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.TextPart{Text: "never mind"})
	msg.ContextID = "c1"
	res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: msg})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	task, _ := res.(*a2a.Task)
	if task == nil || task.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("task = %+v, want completed", res)
	}
	if task.Status.Message != nil {
		if meta := task.Status.Message.Meta(); meta[MetaPendingCalls] != nil {
			t.Errorf("the caller was shown pending calls: %v", meta[MetaPendingCalls])
		}
	}
	for _, id := range ran {
		if id == "call_1" {
			t.Errorf("the stale call ran: issue_refund ran as %v", ran)
		}
	}
	stored, _ := store.Load(context.Background(), "c1")
	for _, item := range stored {
		if fc, ok := item.(*openresponses.FunctionCall); ok && fc.CallID == "call_1" {
			t.Errorf("stored = %s, want the stale call dropped", itemTypes(stored))
		}
	}
	if len(unanswered(stored)) != 0 {
		t.Errorf("stored conversation still waits on %d call(s)", len(unanswered(stored)))
	}
}

// TestHeldOwnCallAfterHandoffAsksSendersElicitor pins #209's
// inheritance: after a WithHandoff switch to a configuration with no
// ToolElicitor, a call the receiver's hook holds to the receiver's own
// tool is put to the elicitor the sender's configuration carried, as a
// nested call would be.
func TestHeldOwnCallAfterHandoffAsksSendersElicitor(t *testing.T) {
	var ran int
	refund := agenttool.NewFunc("issue_refund", "refunds a charge", json.RawMessage(`{"type":"object"}`),
		func(context.Context, agenttool.Call) (agenttool.Result, error) {
			ran++
			return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "refunded"}}, nil
		})
	billing := agentturn.Config{Name: "billing", Model: callsOnce{"issue_refund"}, Tools: []agenttool.Tool{refund},
		BeforeToolCall: holds("issue_refund", "a person approves refunds")}
	var asked []agentturn.AskedCall
	triage := agentturn.Config{Name: "triage", Model: preamble{}, Tools: []agenttool.Tool{transferToBilling}, ToolElicitor: answering(agenttool.ActionAccept, &asked)}
	h := a2asrv.NewHandler(New(triage, WithHandoff(func(context.Context, *agentturn.RunEnd, []*agentturn.ToolEnd) (agentturn.Config, bool) {
		return billing, true
	})))
	task := sendTask(t, h, userMessage("I was double charged"))
	if task.Status.State != a2a.TaskStateCompleted || taskText(task) != "Tool result: refunded" {
		t.Fatalf("task = %s %q, want completed with the refund's output", task.Status.State, taskText(task))
	}
	if ran != 1 || len(asked) != 1 || asked[0].Name != "issue_refund" {
		t.Errorf("issue_refund ran %d times, elicitor asked about %+v; want once each", ran, asked)
	}
}
