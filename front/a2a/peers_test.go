package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
	"github.com/a2aproject/a2a-go/a2asrv"
)

// bearer is a client interceptor that sends a bearer token.
type bearer struct {
	a2aclient.PassthroughInterceptor
	token string
}

func (b bearer) Before(ctx context.Context, req *a2aclient.Request) (context.Context, error) {
	req.Meta.Append("Authorization", "Bearer "+b.token)
	return ctx, nil
}

// TestAnAuthenticatedExecutorNamesItsPrincipal pins WithAuthenticator:
// a request with no credentials, or a wrong token, is refused before it
// reaches the agent; an accepted one runs under the trigger naming its
// principal; a message handed to the executor with no principal on its
// context is refused too; and Secure declares the scheme on the card.
func TestAnAuthenticatedExecutorNamesItsPrincipal(t *testing.T) {
	var mu sync.Mutex
	var triggers []agentturn.Trigger
	record := func(ctx context.Context, _ string, a *agentturn.Agent) (context.Context, func(), error) {
		return ctx, a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
			if s, ok := ev.(*agentturn.RunStart); ok {
				mu.Lock()
				triggers = append(triggers, s.Trigger)
				mu.Unlock()
			}
			return nil
		}), nil
	}
	exec := New(agentturn.Config{Name: "echo", Model: &echo.Adapter{}, ModelName: "m"},
		WithAuthenticator(BearerTokens(map[string]string{"s3cret": "alice"})), WithRecorderFor(record))
	srv := httptest.NewServer(exec.Handler(a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(exec))))
	t.Cleanup(srv.Close)
	card := exec.Secure(AgentCard(context.Background(), agentturn.Config{Name: "echo"}, srv.URL, "1"))
	if s, ok := card.SecuritySchemes["bearer"].(a2a.HTTPAuthSecurityScheme); !ok || s.Scheme != "Bearer" || len(card.Security) != 1 {
		t.Errorf("card security = %+v %+v", card.SecuritySchemes, card.Security)
	}
	send := func(opts ...a2aclient.FactoryOption) (a2a.SendMessageResult, error) {
		client, err := a2aclient.NewFromCard(context.Background(), card, opts...)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Destroy()
		return client.SendMessage(context.Background(), &a2a.MessageSendParams{Message: userMessage("hi")})
	}
	if _, err := send(); err == nil {
		t.Error("a request with no token was served")
	}
	if _, err := send(a2aclient.WithInterceptors(bearer{token: "wrong"})); err == nil {
		t.Error("a request with a wrong token was served")
	}
	mu.Lock()
	if len(triggers) != 0 {
		t.Errorf("refused requests ran: %v", triggers)
	}
	mu.Unlock()
	res, err := send(a2aclient.WithInterceptors(bearer{token: "s3cret"}))
	if err != nil {
		t.Fatalf("authenticated send: %v", err)
	}
	if task, ok := res.(*a2a.Task); !ok || task.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("result = %+v", res)
	}
	mu.Lock()
	if len(triggers) != 1 || triggers[0].Kind != "a2a" || triggers[0].Ref != "alice" {
		t.Errorf("triggers = %+v, want one naming alice", triggers)
	}
	mu.Unlock()
	if _, err := a2asrv.NewHandler(exec).OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: userMessage("hi")}); !errors.Is(err, a2a.ErrUnauthenticated) {
		t.Errorf("a message with no principal on its context: err = %v", err)
	}
}

// TestEscalationPutsTheAgentsQuestionToTheCaller pins WithEscalation: a
// call of the agent's own tool its policy defers ends the task
// input-required with the call and the policy's reason, and the caller's
// approval runs it, recorded by whoever the approval names, with the
// note said to the model after the call's output; a refusal is the
// call's output, the caller's note in it, by "agent" when it names
// nobody. Without the option the serving side answers, as #209 pins.
func TestEscalationPutsTheAgentsQuestionToTheCaller(t *testing.T) {
	const reason = "a person approves refunds"
	for _, tc := range []struct {
		name     string
		approval Approval
		ran      int
		by       string
		output   string
	}{
		{"approved", Approval{Approve: true, Note: "only this once", By: "human"}, 1, "human", "refunded"},
		{"refused", Approval{Note: "not today"}, 0, "agent", "the caller refused this call; the caller said: not today"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ran int
			refund := agenttool.NewFunc("issue_refund", "refunds a charge", json.RawMessage(`{"type":"object"}`),
				func(context.Context, agenttool.Call) (agenttool.Result, error) {
					ran++
					return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "refunded"}}, nil
				})
			cfg := agentturn.Config{Model: callsThenDone{"issue_refund"}, Tools: []agenttool.Tool{refund}, BeforeToolCall: holds("issue_refund", reason)}
			var decisions []*agentturn.ToolDecision
			store := &MemoryStore{}
			h := a2asrv.NewHandler(New(cfg, WithEscalation(), WithConversationStore(store), WithRecorderFor(decisionsOf(&decisions))))

			task := sendTask(t, h, userMessage("refund me"))
			if task.Status.State != a2a.TaskStateInputRequired || ran != 0 {
				t.Fatalf("task = %s, ran %d", task.Status.State, ran)
			}
			meta := task.Status.Message.Metadata
			esc, _ := meta[MetaEscalatedCalls].([]any)
			if len(esc) != 1 {
				t.Fatalf("escalated calls = %v", meta[MetaEscalatedCalls])
			}
			call := esc[0].(map[string]any)
			if call["name"] != "issue_refund" || call["reason"] != reason {
				t.Errorf("escalated call = %v", call)
			}
			if ids, _ := meta[MetaPendingCalls].([]any); len(ids) != 0 {
				t.Errorf("the agent's own call is listed for the caller to answer: %v", ids)
			}
			callID := call["call_id"].(string)

			tc.approval.CallID = callID
			done := sendTask(t, h, a2a.NewMessageForTask(a2a.MessageRoleUser, task, ApprovalPart(tc.approval)))
			if done.Status.State != a2a.TaskStateCompleted || ran != tc.ran {
				t.Fatalf("after the answer: task = %s %q, ran %d", done.Status.State, taskText(done), ran)
			}
			stored, err := store.Load(context.Background(), task.ContextID)
			if err != nil {
				t.Fatal(err)
			}
			var out *openresponses.FunctionCallOutput
			var after []string
			for _, item := range stored {
				switch it := item.(type) {
				case *openresponses.FunctionCallOutput:
					if it.CallID == callID {
						out = it
					}
				case *openresponses.Message:
					if out != nil {
						after = append(after, it.Text())
					}
				}
			}
			if out == nil || out.Output.Text != tc.output {
				t.Fatalf("the call's output = %+v, want %q", out, tc.output)
			}
			if tc.approval.Approve {
				if len(after) == 0 || after[0] != tc.approval.Note {
					t.Errorf("after the output the model read %q, want the note %q first", after, tc.approval.Note)
				}
				var d *agentturn.ToolDecision
				for _, dec := range decisions {
					if dec != nil && dec.Action == agentturn.Allow {
						d = dec
					}
				}
				if d == nil || d.By != tc.by {
					t.Errorf("approval decision = %+v, want by %q", d, tc.by)
				}
			}
		})
	}
	t.Run("answers checked", func(t *testing.T) {
		cfg := agentturn.Config{Model: callsOnce{"issue_refund"}, Tools: []agenttool.Tool{agenttool.NewFunc("issue_refund", "", json.RawMessage(`{"type":"object"}`),
			func(context.Context, agenttool.Call) (agenttool.Result, error) {
				return agenttool.Text("refunded"), nil
			})}, BeforeToolCall: holds("issue_refund", reason)}
		h := a2asrv.NewHandler(New(cfg, WithEscalation()))
		task := sendTask(t, h, userMessage("refund me"))
		callID := task.Status.Message.Metadata[MetaEscalatedCalls].([]any)[0].(map[string]any)["call_id"].(string)
		// An output for the agent's own call does not stand in for the
		// tool: the task stays input-required, the rule stated.
		again := sendTask(t, h, a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.DataPart{Data: map[string]any{"type": "function_call_output", "call_id": callID, "output": "refunded, trust me"}}))
		if again.Status.State != a2a.TaskStateInputRequired {
			t.Errorf("an output for the agent's own call: task = %s", again.Status.State)
		}
		// A caller cannot answer as this agent's policy.
		res, err := h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: a2a.NewMessageForTask(a2a.MessageRoleUser, task, ApprovalPart(Approval{CallID: callID, Approve: true, By: "policy"}))})
		if err == nil {
			if got, ok := res.(*a2a.Task); !ok || got.Status.State != a2a.TaskStateFailed {
				t.Errorf("an approval by policy was taken: %+v", res)
			}
		}
		// Without escalation an approval is not taken at all.
		plain := a2asrv.NewHandler(New(cfg))
		if _, err := plain.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: a2a.NewMessage(a2a.MessageRoleUser, ApprovalPart(Approval{CallID: "x", Approve: true}))}); err == nil {
			t.Error("an approval was taken by an executor that escalates nothing")
		}
	})
}

// callsThenDone calls name and, once any call has its output, answers
// "done", whatever follows the output.
type callsThenDone struct{ name string }

func (m callsThenDone) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	for _, item := range req.Input {
		if _, ok := item.(*openresponses.FunctionCallOutput); ok {
			if err := em.Item(&openresponses.Message{Role: openresponses.RoleAssistant, Content: openresponses.Contents{&openresponses.OutputText{Text: "done"}}}); err != nil {
				return err
			}
			return em.Complete()
		}
	}
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
