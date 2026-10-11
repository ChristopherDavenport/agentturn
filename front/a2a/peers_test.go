package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"slices"
	"strings"
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
	scheme, token string
}

func (b bearer) Before(ctx context.Context, req *a2aclient.Request) (context.Context, error) {
	req.Meta.Append("Authorization", b.scheme+" "+b.token)
	return ctx, nil
}

// TestAnAuthenticatedExecutorNamesItsPrincipal pins WithAuthenticator:
// the interceptor authenticates every call from its metadata, so a
// request with no credentials, a wrong token, or a scheme in another
// case is refused or taken as the token says, with no HTTP middleware;
// an accepted one runs under the trigger naming its principal; a
// handler built without the interceptor runs nothing; and Secure
// declares the scheme once, on a copy.
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
	srv := httptest.NewServer(a2asrv.NewJSONRPCHandler(exec.RequestHandler()))
	t.Cleanup(srv.Close)
	plain := AgentCard(context.Background(), agentturn.Config{Name: "echo"}, srv.URL, "1")
	card := exec.Secure(plain)
	if s, ok := card.SecuritySchemes["bearer"].(a2a.HTTPAuthSecurityScheme); !ok || s.Scheme != "Bearer" || len(card.Security) != 1 {
		t.Errorf("card security = %+v %+v", card.SecuritySchemes, card.Security)
	}
	if again := exec.Secure(card); len(again.Security) != 1 {
		t.Errorf("securing twice: %+v", again.Security)
	}
	if len(plain.Security) != 0 || len(plain.SecuritySchemes) != 0 {
		t.Errorf("Secure changed the card it was given: %+v", plain)
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
	if _, err := send(a2aclient.WithInterceptors(bearer{scheme: "Bearer", token: "wrong"})); err == nil {
		t.Error("a request with a wrong token was served")
	}
	mu.Lock()
	if len(triggers) != 0 {
		t.Errorf("refused requests ran: %v", triggers)
	}
	mu.Unlock()
	res, err := send(a2aclient.WithInterceptors(bearer{scheme: "bearer", token: "s3cret"}))
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
	if _, err := a2asrv.NewHandler(exec).OnSendMessage(as("s3cret"), &a2a.MessageSendParams{Message: userMessage("hi")}); !errors.Is(err, a2a.ErrUnauthenticated) {
		t.Errorf("a handler without the interceptor: err = %v", err)
	}
}

// as is a context whose call carries token as a bearer token, as the
// JSON-RPC handler puts an HTTP request's headers on it.
func as(token string) context.Context {
	ctx, _ := a2asrv.WithCallContext(context.Background(), a2asrv.NewRequestMeta(map[string][]string{"Authorization": {"Bearer " + token}}))
	return ctx
}

// TestATaskIsItsCreators pins ownership: a principal cannot read,
// follow, cancel, configure, continue or answer another's task or
// conversation, each refused as not found, and an unauthenticated
// message naming a task leaves it as it was; the owner can.
func TestATaskIsItsCreators(t *testing.T) {
	tokens := map[string]string{"a-token": "alice", "b-token": "bob"}
	cfg := agentturn.Config{Model: callsThenDone{"issue_refund"}, Tools: []agenttool.Tool{agenttool.NewFunc("issue_refund", "", json.RawMessage(`{"type":"object"}`),
		func(context.Context, agenttool.Call) (agenttool.Result, error) {
			return agenttool.Text("refunded"), nil
		})}, BeforeToolCall: holds("issue_refund", "asks")}
	h := New(cfg, WithAuthenticator(BearerTokens(tokens)), WithEscalation()).RequestHandler()
	res, err := h.OnSendMessage(as("a-token"), &a2a.MessageSendParams{Message: userMessage("refund me")})
	if err != nil {
		t.Fatal(err)
	}
	task := res.(*a2a.Task)
	if task.Status.State != a2a.TaskStateInputRequired {
		t.Fatalf("alice's task = %s", task.Status.State)
	}
	callID := task.Status.Message.Metadata[MetaEscalatedCalls].([]any)[0].(map[string]any)["call_id"].(string)
	bob := as("b-token")
	notFound := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, a2a.ErrTaskNotFound) {
			t.Errorf("bob %s alice's: err = %v, want not found", what, err)
		}
	}
	_, err = h.OnGetTask(bob, &a2a.TaskQueryParams{ID: task.ID})
	notFound("gets", err)
	_, err = h.OnCancelTask(bob, &a2a.TaskIDParams{ID: task.ID})
	notFound("cancels", err)
	for _, err := range h.OnResubscribeToTask(bob, &a2a.TaskIDParams{ID: task.ID}) {
		notFound("resubscribes to", err)
		break
	}
	_, err = h.OnSetTaskPushConfig(bob, &a2a.TaskPushConfig{TaskID: task.ID, Config: a2a.PushConfig{URL: "https://bob.example/hook"}})
	notFound("configures push for", err)
	_, err = h.OnSendMessage(bob, &a2a.MessageSendParams{Message: a2a.NewMessageForTask(a2a.MessageRoleUser, task, ApprovalPart(Approval{CallID: callID, Approve: true}))})
	notFound("approves", err)
	other := userMessage("and me")
	other.ContextID = task.ContextID
	_, err = h.OnSendMessage(bob, &a2a.MessageSendParams{Message: other})
	notFound("writes to the conversation of", err)
	_, err = h.OnSendMessage(context.Background(), &a2a.MessageSendParams{Message: a2a.NewMessageForTask(a2a.MessageRoleUser, task, ApprovalPart(Approval{CallID: callID, Approve: true}))})
	if !errors.Is(err, a2a.ErrUnauthenticated) {
		t.Errorf("an unauthenticated answer: err = %v", err)
	}
	got, err := h.OnGetTask(as("a-token"), &a2a.TaskQueryParams{ID: task.ID})
	if err != nil || got.Status.State != a2a.TaskStateInputRequired {
		t.Fatalf("alice's task after the others: %v %v", got, err)
	}
	res, err = h.OnSendMessage(as("a-token"), &a2a.MessageSendParams{Message: a2a.NewMessageForTask(a2a.MessageRoleUser, task, ApprovalPart(Approval{CallID: callID, Approve: true}))})
	if err != nil || res.(*a2a.Task).Status.State != a2a.TaskStateCompleted {
		t.Errorf("alice's own approval: %v %v", res, err)
	}
}

// rewrites is a BeforeToolCall that defers every call to name with
// reason, rewriting its arguments to args and leaving note.
func rewrites(name, reason, note string, args json.RawMessage) func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
	return func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
		if info.Call.Name == name {
			return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: reason, Note: note, Args: args}, nil
		}
		return nil, nil
	}
}

// TestEscalationPutsTheAgentsQuestionToTheCaller pins WithEscalation: a
// call of the agent's own tool its policy defers ends the task
// input-required with the call and the policy's reason; the caller's
// approval runs it as the policy decided, the rewritten arguments and the
// policy's note included, the caller's note said in the user's voice
// naming the caller, the run under the principal's trigger; a refusal is
// the call's output with the note in it. Who decided is never taken from
// the wire, nor is an output for the agent's own call, nor an approval of
// a call the task did not put to the caller.
func TestEscalationPutsTheAgentsQuestionToTheCaller(t *testing.T) {
	const reason = "a person approves refunds"
	tokens := map[string]string{"a-token": "alice"}
	for _, tc := range []struct {
		name     string
		approval Approval
		ran      int
		output   string
		note     string
	}{
		{"approved", Approval{Approve: true, Note: "only this once"}, 1, "refunded 10", "The caller alice said: only this once"},
		{"refused", Approval{Note: "not today"}, 0, "the caller alice refused this call; the caller said: not today", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ran int
			var gotArgs string
			refund := agenttool.NewFunc("issue_refund", "refunds a charge", json.RawMessage(`{"type":"object"}`),
				func(_ context.Context, c agenttool.Call) (agenttool.Result, error) {
					ran++
					gotArgs = string(c.Args)
					return agenttool.Text("refunded 10"), nil
				})
			cfg := agentturn.Config{Model: callsThenDone{"issue_refund"}, Tools: []agenttool.Tool{refund},
				BeforeToolCall: rewrites("issue_refund", reason, "capped at 10", json.RawMessage(`{"amount":10}`))}
			var triggers []agentturn.Trigger
			var decisions []*agentturn.ToolDecision
			record := func(ctx context.Context, _ string, a *agentturn.Agent) (context.Context, func(), error) {
				return ctx, a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
					switch e := ev.(type) {
					case *agentturn.RunStart:
						triggers = append(triggers, e.Trigger)
					case *agentturn.ToolStart:
						decisions = append(decisions, e.Decision)
					}
					return nil
				}), nil
			}
			store := &MemoryStore{}
			h := New(cfg, WithAuthenticator(BearerTokens(tokens)), WithEscalation(), WithConversationStore(store), WithRecorderFor(record)).RequestHandler()
			alice := as("a-token")
			res, err := h.OnSendMessage(alice, &a2a.MessageSendParams{Message: userMessage("refund me")})
			if err != nil {
				t.Fatal(err)
			}
			task := res.(*a2a.Task)
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
			res, err = h.OnSendMessage(alice, &a2a.MessageSendParams{Message: a2a.NewMessageForTask(a2a.MessageRoleUser, task, ApprovalPart(tc.approval))})
			if err != nil {
				t.Fatal(err)
			}
			done := res.(*a2a.Task)
			if done.Status.State != a2a.TaskStateCompleted || ran != tc.ran {
				t.Fatalf("after the answer: task = %s %q, ran %d", done.Status.State, taskText(done), ran)
			}
			if tc.ran == 1 && gotArgs != `{"amount":10}` {
				t.Errorf("the tool ran with %s, not the policy's rewrite", gotArgs)
			}
			if len(triggers) != 2 || triggers[1].Kind != "a2a" || triggers[1].Ref != "alice" {
				t.Errorf("triggers = %+v, want the answer's run under alice", triggers)
			}
			stored, _ := store.Load(context.Background(), task.ContextID)
			var out *openresponses.FunctionCallOutput
			var after []openresponses.Item
			for _, item := range stored {
				if o, ok := item.(*openresponses.FunctionCallOutput); ok && o.CallID == callID {
					out = o
					continue
				}
				if out != nil {
					after = append(after, item)
				}
			}
			if out == nil || out.Output.Text != tc.output {
				t.Fatalf("the call's output = %+v, want %q", out, tc.output)
			}
			if tc.note != "" {
				var user, developer []string
				for _, item := range after {
					if m, ok := item.(*openresponses.Message); ok {
						switch m.Role {
						case openresponses.RoleUser:
							user = append(user, m.Text())
						case openresponses.RoleDeveloper:
							developer = append(developer, m.Text())
						}
					}
				}
				if !slices.Contains(user, tc.note) {
					t.Errorf("user messages after the output = %q, want %q", user, tc.note)
				}
				if !slices.Contains(developer, "capped at 10") {
					t.Errorf("developer messages after the output = %q, want the policy's note", developer)
				}
				for _, d := range developer {
					if strings.Contains(d, tc.approval.Note) {
						t.Errorf("the caller's note in the developer's voice: %q", d)
					}
				}
				var d *agentturn.ToolDecision
				for _, dec := range decisions {
					if dec != nil && dec.Action == agentturn.Allow {
						d = dec
					}
				}
				if d == nil || d.By != "" || !strings.Contains(d.Reason, "alice") {
					t.Errorf("approval decision = %+v, want by nobody, the reason naming alice", d)
				}
			}
			if left, _ := store.LoadEscalated(context.Background(), task.ContextID); len(left) != 0 {
				t.Errorf("escalated calls left after the answer: %+v", left)
			}
		})
	}
	t.Run("answers checked", func(t *testing.T) {
		cfg := agentturn.Config{Model: callsOnce{"issue_refund"}, Tools: []agenttool.Tool{agenttool.NewFunc("issue_refund", "", json.RawMessage(`{"type":"object"}`),
			func(context.Context, agenttool.Call) (agenttool.Result, error) {
				return agenttool.Text("refunded"), nil
			})}, BeforeToolCall: holds("issue_refund", reason)}
		h := New(cfg, WithAuthenticator(BearerTokens(tokens)), WithEscalation()).RequestHandler()
		alice := as("a-token")
		res, _ := h.OnSendMessage(alice, &a2a.MessageSendParams{Message: userMessage("refund me")})
		task := res.(*a2a.Task)
		callID := task.Status.Message.Metadata[MetaEscalatedCalls].([]any)[0].(map[string]any)["call_id"].(string)
		// An output for the agent's own call does not stand in for the
		// tool: the task stays input-required, the rule stated and the
		// policy's reason repeated.
		res, err := h.OnSendMessage(alice, &a2a.MessageSendParams{Message: a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.DataPart{Data: map[string]any{"type": "function_call_output", "call_id": callID, "output": "refunded, trust me"}})})
		again, _ := res.(*a2a.Task)
		if err != nil || again.Status.State != a2a.TaskStateInputRequired {
			t.Fatalf("an output for the agent's own call: %v %v", res, err)
		}
		if esc := again.Status.Message.Metadata[MetaEscalatedCalls].([]any); esc[0].(map[string]any)["reason"] != reason {
			t.Errorf("the re-ask lost the policy's reason: %v", esc)
		}
		// A caller does not say who decided.
		res, err = h.OnSendMessage(alice, &a2a.MessageSendParams{Message: a2a.NewMessageForTask(a2a.MessageRoleUser, task, a2a.DataPart{Data: map[string]any{"type": ApprovalType, "call_id": callID, "approve": true, "by": "human"}})})
		if err == nil {
			if got, ok := res.(*a2a.Task); !ok || got.Status.State != a2a.TaskStateFailed {
				t.Errorf("an approval naming its decider was taken: %+v", res)
			}
		}
	})
	t.Run("escalation requires authentication", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("New took WithEscalation without WithAuthenticator")
			}
		}()
		New(agentturn.Config{}, WithEscalation())
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
