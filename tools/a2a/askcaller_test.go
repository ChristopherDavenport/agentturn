package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	fronta2a "github.com/ChristopherDavenport/agentturn/front/a2a"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
	"github.com/a2aproject/a2a-go/a2asrv"
)

// callsThenSays calls each tool it names, with {"amount":5}, and once
// the calls' outputs are in its input says them, so the caller reads
// what the remote's model read.
type callsThenSays []string

func (c callsThenSays) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	var read []string
	for _, item := range req.Input {
		if out, ok := item.(*openresponses.FunctionCallOutput); ok {
			read = append(read, out.Output.Text)
		}
	}
	if len(read) > 0 {
		w, err := em.Message(openresponses.PhaseFinalAnswer)
		if err != nil {
			return err
		}
		if err := w.Text("remote read: " + strings.Join(read, " | ")); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		return em.Complete()
	}
	for _, name := range c {
		w, err := em.FunctionCall("", name)
		if err != nil {
			return err
		}
		if err := w.Arguments(`{"amount":5}`); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
	}
	return em.Complete()
}

// escalated is a remote agent served through front/a2a under
// WithEscalation to the bearer token "a-token" as alice: the counts of
// its tools' runs and the decisions its runs started calls with.
type escalated struct {
	mu        sync.Mutex
	ran       map[string]int
	decisions []*agentturn.ToolDecision
}

// tool is a remote tool that counts its runs and says "<name> ran".
func (e *escalated) tool(name string) agenttool.Tool {
	return agenttool.NewFunc(name, "", json.RawMessage(`{"type":"object"}`),
		func(context.Context, agenttool.Call) (agenttool.Result, error) {
			e.mu.Lock()
			e.ran[name]++
			e.mu.Unlock()
			if name == "issue_refund" {
				return agenttool.Text("refunded 5"), nil
			}
			return agenttool.Text(name + " ran"), nil
		})
}

func (e *escalated) runs(name string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ran[name]
}

// serveEscalating serves a remote agent whose model calls each of
// names, with tools that count their runs, under hook, through
// front/a2a's WithEscalation; chosen lets alice name who decided. It
// returns a client for it and its card, named Refunds.
func serveEscalating(t *testing.T, names []string, hook func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error), chosen bool) (*escalated, *a2aclient.Client, *a2a.AgentCard) {
	t.Helper()
	e := &escalated{ran: map[string]int{}}
	var tools []agenttool.Tool
	for _, n := range names {
		tools = append(tools, e.tool(n))
	}
	remoteCfg := agentturn.Config{Name: "Refunds", Model: callsThenSays(names), Tools: tools, BeforeToolCall: hook}
	auth := fronta2a.Authenticator{Name: "bearer", Scheme: a2a.HTTPAuthSecurityScheme{Scheme: "Bearer"},
		Principal: func(r *http.Request) (string, error) {
			if r.Header.Get("Authorization") != "Bearer a-token" {
				return "", errors.New("no token")
			}
			return "alice", nil
		}}
	record := func(ctx context.Context, _ string, a *agentturn.Agent) (context.Context, func(), error) {
		return ctx, a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
			if s, ok := ev.(*agentturn.ToolStart); ok {
				e.mu.Lock()
				e.decisions = append(e.decisions, s.Decision)
				e.mu.Unlock()
			}
			return nil
		}), nil
	}
	fopts := []fronta2a.Option{fronta2a.WithAuthenticator(auth), fronta2a.WithEscalation(), fronta2a.WithRecorderFor(record)}
	if chosen {
		fopts = append(fopts, fronta2a.WithChosenBy(func(context.Context, string) bool { return true }))
	}
	exec := fronta2a.New(remoteCfg, fopts...)
	srv := httptest.NewServer(a2asrv.NewJSONRPCHandler(exec.RequestHandler()))
	t.Cleanup(srv.Close)
	card := &a2a.AgentCard{Name: "Refunds", URL: srv.URL, PreferredTransport: a2a.TransportProtocolJSONRPC, Capabilities: a2a.AgentCapabilities{Streaming: true}}
	client, err := a2aclient.NewFromCard(context.Background(), card, a2aclient.WithInterceptors(ObserveEndpoint(), bearer{token: "a-token"}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Destroy() })
	return e, client, card
}

// asks defers every call to name with reason.
func asks(name, reason string) func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
	return func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
		if info.Call.Name == name {
			return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: reason}, nil
		}
		return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "held beside " + name, Held: true}, nil
	}
}

// bearer is a client interceptor that sends a bearer token.
type bearer struct {
	a2aclient.PassthroughInterceptor
	token string
}

func (b bearer) Before(ctx context.Context, req *a2aclient.Request) (context.Context, error) {
	if req.Meta == nil {
		req.Meta = a2aclient.CallMeta{}
	}
	req.Meta["authorization"] = []string{"Bearer " + b.token}
	return ctx, nil
}

// TestAskCallerAnswersARemotesEscalation pins #296: under
// WithAskCaller a remote task waiting only on calls its policy
// escalated to its caller is put to the elicitor of the call that
// started it, the question naming the call, the remote's reason and
// the remote agent and filed under the a2a call, and answered on the
// same task with one approval per call. An accept runs the remote's
// call once; a refusal reaches the remote's model with the answerer's
// note. Who decided is sent only under WithSendBy, to a remote that
// accepts it. A call the remote held beside the asked one is not asked
// about and follows the answer. With nobody to ask, WithInputRequired
// answers the task when it is set, and otherwise the call fails
// input-required and names the escalated call, as it does without the
// option.
func TestAskCallerAnswersARemotesEscalation(t *testing.T) {
	const reason = "a person approves refunds"
	for _, tc := range []struct {
		name string
		// answer is the caller's elicitor's answer; nil installs none.
		answer *agenttool.Answer
		opts   []Option
		// chosen lets the caller name who decided on the remote.
		chosen bool
		ran    int
		want   string
		// by is the decider the remote records the approval with.
		by    string
		input bool
		// sibling adds a call the remote holds beside issue_refund;
		// siblingRan is how often it runs.
		sibling    bool
		siblingRan int
		// hooked is whether WithInputRequired answers the task.
		hooked bool
	}{
		{name: "allowed", answer: &agenttool.Answer{Action: agenttool.ActionAccept}, ran: 1, want: "remote read: refunded 5"},
		{name: "refused with a note", answer: &agenttool.Answer{Action: agenttool.ActionDecline, Note: "not this month"}, want: "remote read: the caller alice refused this call; the caller said: not this month"},
		{name: "allowed with a note", answer: &agenttool.Answer{Action: agenttool.ActionAccept, Note: "only this once"}, ran: 1, want: "remote read: refunded 5"},
		{name: "allowed, who decided sent", answer: &agenttool.Answer{Action: agenttool.ActionAccept}, opts: []Option{WithSendBy()}, chosen: true, ran: 1, want: "remote read: refunded 5", by: agenttool.ByHuman},
		{name: "nobody to ask", input: true},
		{name: "a held sibling runs with the accept", answer: &agenttool.Answer{Action: agenttool.ActionAccept}, sibling: true, ran: 1, siblingRan: 1, want: "remote read: refunded 5 | read_ledger ran"},
		{name: "a held sibling is refused with the decline", answer: &agenttool.Answer{Action: agenttool.ActionDecline}, sibling: true, want: "remote read: the caller alice refused this call | not run: the caller alice refused the call this one was held beside"},
		{name: "nobody to ask, the hook answers", hooked: true, want: "remote read: the caller alice refused this call; the caller said: the hook says no"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			names := []string{"issue_refund"}
			if tc.sibling {
				names = append(names, "read_ledger")
			}
			remote, client, card := serveEscalating(t, names, asks("issue_refund", reason), tc.chosen)

			opts := append([]Option{WithAskCaller()}, tc.opts...)
			hooked := 0
			if tc.hooked {
				opts = append(opts, WithInputRequired(func(_ context.Context, _ TaskInfo, q *a2a.Message) ([]a2a.Part, error) {
					hooked++
					var parts []a2a.Part
					escalated, _ := q.Metadata[fronta2a.MetaEscalatedCalls].([]any)
					for _, e := range escalated {
						id, _ := e.(map[string]any)["call_id"].(string)
						parts = append(parts, fronta2a.ApprovalPart(fronta2a.Approval{CallID: id, Note: "the hook says no"}))
					}
					return parts, nil
				}))
			}
			tool := New(client, card, opts...)
			var asked []agentturn.AskedCall
			var questions []string
			var under []string
			local := agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{tool}, MaxTurns: 1}
			if tc.answer != nil {
				local.ToolElicitor = func(ctx context.Context, q agenttool.Elicitation) (agenttool.Answer, error) {
					c, _ := agentturn.AskedCallFrom(ctx)
					asked = append(asked, c)
					questions = append(questions, q.Message)
					call, _ := agenttool.CallFrom(ctx)
					under = append(under, call.ID)
					return *tc.answer, nil
				}
			}
			var start *agentturn.ToolStart
			var end *agentturn.ToolEnd
			var said []string
			for ev := range agentturn.Run(context.Background(), nil, openresponses.Items{openresponses.UserText("refund the charge")}, local) {
				switch e := ev.(type) {
				case *agentturn.ToolStart:
					start = e
				case *agentturn.ToolEnd:
					end = e
				case *agentturn.ItemEnd:
					if m, ok := e.Item.(*openresponses.Message); ok {
						said = append(said, m.Text())
					}
				}
			}
			if start == nil || end == nil {
				t.Fatal("the remote agent was never called")
			}
			// The answerer's note is the remote's to read, sent on the
			// approval; this run does not say it again.
			for _, text := range said {
				if tc.answer != nil && tc.answer.Note != "" && strings.Contains(text, tc.answer.Note) && !strings.HasPrefix(text, "Tool result:") {
					t.Errorf("the caller's run says the note itself: %q", text)
				}
			}
			if ran := remote.runs("issue_refund"); ran != tc.ran {
				t.Errorf("the remote's call ran %d times, want %d", ran, tc.ran)
			}
			if ran := remote.runs("read_ledger"); ran != tc.siblingRan {
				t.Errorf("the held call ran %d times, want %d", ran, tc.siblingRan)
			}
			if tc.input {
				var waiting agentturn.InputRequired
				if !errors.As(end.Err, &waiting) {
					t.Fatalf("err = %v, want input required", end.Err)
				}
				if calls := waiting.PendingCalls(); len(calls) != 1 || calls[0].Name != "issue_refund" {
					t.Errorf("waiting on %+v, want the escalated issue_refund", calls)
				}
				return
			}
			if end.Err != nil || end.Result.Output.Text != tc.want {
				t.Errorf("the a2a call = %q, %v; want %q", end.Result.Output.Text, end.Err, tc.want)
			}
			if tc.hooked {
				if hooked != 1 || len(asked) != 0 {
					t.Errorf("the hook answered %d times and the elicitor %d; want the hook once", hooked, len(asked))
				}
				return
			}
			if len(asked) != 1 {
				t.Fatalf("asked %d times, want once, about the asked call alone", len(asked))
			}
			if a := asked[0]; a.Name != "issue_refund" || string(a.Args) != `{"amount":5}` || a.Callee != "Refunds" || a.Decision == nil || a.Decision.Reason != reason || a.CallID == "" {
				t.Errorf("asked about %+v, want the remote's issue_refund, its reason and the remote agent", a)
			}
			if under[0] != start.CallID {
				t.Errorf("the question is filed under %q, want the a2a call %q", under[0], start.CallID)
			}
			if !strings.Contains(questions[0], "Refunds") || !strings.Contains(questions[0], reason) {
				t.Errorf("question %q does not name the remote agent and its reason", questions[0])
			}
			var approval *agentturn.ToolDecision
			remote.mu.Lock()
			defer remote.mu.Unlock()
			for _, d := range remote.decisions {
				if d != nil && strings.HasPrefix(d.Reason, "approved by the caller") {
					approval = d
				}
			}
			if tc.ran > 0 && (approval == nil || approval.By != tc.by) {
				t.Errorf("the remote recorded the approval as %+v, want by %q", approval, tc.by)
			}
		})
	}
}

// TestADetachedCallAsksNobody pins WithAskCaller under WithDetach: the
// task is followed on the run's context once the call has returned,
// where the call's elicitor is not, so an elicitor the host put on the
// prompt's context is not asked from the background either; the task
// keeps WithInputRequired.
func TestADetachedCallAsksNobody(t *testing.T) {
	remote, client, card := serveEscalating(t, []string{"issue_refund"}, asks("issue_refund", "a person approves refunds"), false)
	var mu sync.Mutex
	asked, hooked := 0, 0
	hook := func(context.Context, TaskInfo, *a2a.Message) ([]a2a.Part, error) {
		mu.Lock()
		hooked++
		mu.Unlock()
		return nil, agentturn.ErrInputRequired
	}
	ended := make(chan error, 1)
	detach := func(_ string, _ TaskInfo, _ *a2a.Task, err error) { ended <- err }
	tool := New(client, card, WithAskCaller(), WithInputRequired(hook), WithDetach(detach))
	elicit := func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) {
		mu.Lock()
		asked++
		mu.Unlock()
		return agenttool.Answer{Action: agenttool.ActionAccept}, nil
	}
	parent := agentturn.New(agentturn.Config{Model: callsAll{}, Tools: []agenttool.Tool{tool}})
	ctx := agenttool.ContextWithElicitor(context.Background(), elicit)
	if _, err := parent.Prompt(ctx, openresponses.UserText("refund the charge")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-ended:
		var waiting agentturn.InputRequired
		if !errors.As(err, &waiting) {
			t.Errorf("the detached task ended with %v, want input required", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the detached task never ended")
	}
	mu.Lock()
	defer mu.Unlock()
	if asked != 0 || hooked != 1 {
		t.Errorf("asked the prompt's elicitor %d times and the hook %d; want the hook once and nobody asked", asked, hooked)
	}
	if ran := remote.runs("issue_refund"); ran != 0 {
		t.Errorf("the remote's call ran %d times, want none", ran)
	}
}

// TestAskCallerLeavesATaskWaitingOnOtherInput pins that a task whose
// status also lists calls to the caller's own tools is not answered by
// asking about the escalated ones: one message answers both, so it is
// left to WithInputRequired.
func TestAskCallerLeavesATaskWaitingOnOtherInput(t *testing.T) {
	asked := 0
	ctx := agenttool.ContextWithElicitor(context.Background(), func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) {
		asked++
		return agenttool.Answer{Action: agenttool.ActionAccept}, nil
	})
	status := &a2a.Message{Metadata: map[string]any{
		fronta2a.MetaEscalatedCalls: []any{map[string]any{"call_id": "c1", "name": "issue_refund", "arguments": `{"amount":5}`, "reason": "a person approves refunds"}},
		fronta2a.MetaPendingCalls:   []any{"c2"},
	}}
	r := &remote{name: "Refunds", askCaller: true}
	if parts, ok := r.askEscalated(ctx, status); ok || asked != 0 {
		t.Errorf("answered %v (%v) after asking %d times; want it left to WithInputRequired", parts, ok, asked)
	}
}
