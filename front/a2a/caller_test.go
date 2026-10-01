package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
)

// declaring returns a user message that declares a caller-owned tool
// named name.
func declaring(name, text string) *a2a.Message {
	msg := userMessage(text)
	msg.SetMeta(MetaCallerTools, []any{map[string]any{"type": "function", "name": name, "parameters": map[string]any{"type": "object"}}})
	return msg
}

// TestDeclaredToolCannotStandForTheAgents pins #190: a message may not
// declare a caller-owned tool named as one the agent offers, under its
// own configuration or the one the task starts under, nor one the
// route takes for a handoff, so the caller never answers a transfer.
// The bypass this closes: a caller declared transfer_to_refunds_agent,
// answered the deferred call with the route's text, and its next task
// started at refunds.
func TestDeclaredToolCannotStandForTheAgents(t *testing.T) {
	refunds := agentturn.Config{Name: "refunds", Model: &echo.Adapter{}}
	lookup := agenttool.NewFunc("lookup", "looks up", json.RawMessage(`{"type":"object"}`),
		func(context.Context, agenttool.Call) (agenttool.Result, error) {
			return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "found"}}, nil
		})
	billing := agentturn.Config{Name: "billing", Model: &echo.Adapter{}, Tools: []agenttool.Tool{lookup}}
	route := func(call *openresponses.FunctionCall) (agentturn.Config, string, bool) {
		return refunds, "transferred", call.Name == "transfer_to_refunds_agent"
	}
	for _, tc := range []struct {
		name    string
		declare string
		opts    []Option
		refused bool
	}{
		{"a transfer the route takes", "transfer_to_refunds_agent", []Option{WithTransfers(route)}, true},
		{"the agent's own tool", "transfer_to_billing", nil, true},
		{"a tool of the configuration WithStart picks", "lookup", []Option{WithStart(func(context.Context, agentturn.Transcript) (agentturn.Config, bool) {
			return billing, true
		})}, true},
		{"a tool of the agent's alone", "remote", []Option{WithTransfers(route)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &MemoryStore{}
			h := a2asrv.NewHandler(New(agentturn.Config{Name: "triage", Model: &echo.Adapter{}, Tools: []agenttool.Tool{transferToBilling}},
				append([]Option{WithConversationStore(store)}, tc.opts...)...))
			msg := declaring(tc.declare, "please")
			msg.ContextID = "c1"
			res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: msg})
			if tc.refused {
				if !errors.Is(err, a2a.ErrInvalidParams) || res != nil {
					t.Fatalf("send = %+v, %v; want refused as invalid params", res, err)
				}
				if stored, _ := store.Load(context.Background(), "c1"); len(stored) != 0 {
					t.Errorf("stored %s, want nothing", itemTypes(stored))
				}
				return
			}
			if err != nil {
				t.Fatalf("send: %v", err)
			}
		})
	}
}

// callsOnce calls the tool named name until the input holds an output
// for it, then echoes.
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

// TestReceiverToolWinsOverDeclared pins #190: a handoff's receiver that
// offers a tool named as one the message declared runs its own, rather
// than deferring the call to the caller.
func TestReceiverToolWinsOverDeclared(t *testing.T) {
	var ran int
	remote := agenttool.NewFunc("remote", "billing's own", json.RawMessage(`{"type":"object"}`),
		func(context.Context, agenttool.Call) (agenttool.Result, error) {
			ran++
			return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "billing ran it"}}, nil
		})
	billing := agentturn.Config{Name: "billing", Model: callsOnce{"remote"}, Tools: []agenttool.Tool{remote}}
	h := a2asrv.NewHandler(New(agentturn.Config{Name: "triage", Model: preamble{}, Tools: []agenttool.Tool{transferToBilling}},
		WithHandoff(func(context.Context, *agentturn.RunEnd, []*agentturn.ToolEnd) (agentturn.Config, bool) {
			return billing, true
		})))
	task := sendTask(t, h, declaring("remote", "I was double charged"))
	if task.Status.State != a2a.TaskStateCompleted || ran != 1 {
		t.Errorf("task = %s %q, billing's tool ran %d times; want completed with it run once", task.Status.State, taskText(task), ran)
	}
}
