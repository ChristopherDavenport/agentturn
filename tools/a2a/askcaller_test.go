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

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	fronta2a "github.com/ChristopherDavenport/agentturn/front/a2a"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
	"github.com/a2aproject/a2a-go/a2asrv"
)

// refundsThenSays calls issue_refund, and once a call's output is in
// its input says the output, so the caller reads what the remote's
// model read.
type refundsThenSays struct{}

func (refundsThenSays) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	for _, item := range req.Input {
		if out, ok := item.(*openresponses.FunctionCallOutput); ok {
			w, err := em.Message(openresponses.PhaseFinalAnswer)
			if err != nil {
				return err
			}
			if err := w.Text("remote read: " + out.Output.Text); err != nil {
				return err
			}
			if err := w.Close(); err != nil {
				return err
			}
			return em.Complete()
		}
	}
	w, err := em.FunctionCall("", "issue_refund")
	if err != nil {
		return err
	}
	if err := w.Arguments(`{"amount":5}`); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Complete()
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
// accepts it. With nobody to ask, the call fails input-required and
// names the escalated call, as it does without the option.
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
	}{
		{name: "allowed", answer: &agenttool.Answer{Action: agenttool.ActionAccept}, ran: 1, want: "remote read: refunded 5"},
		{name: "refused with a note", answer: &agenttool.Answer{Action: agenttool.ActionDecline, Note: "not this month"}, want: "remote read: the caller alice refused this call; the caller said: not this month"},
		{name: "allowed with a note", answer: &agenttool.Answer{Action: agenttool.ActionAccept, Note: "only this once"}, ran: 1, want: "remote read: refunded 5"},
		{name: "allowed, who decided sent", answer: &agenttool.Answer{Action: agenttool.ActionAccept}, opts: []Option{WithSendBy()}, chosen: true, ran: 1, want: "remote read: refunded 5", by: agenttool.ByHuman},
		{name: "nobody to ask", input: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			ran := 0
			refund := agenttool.NewFunc("issue_refund", "refunds a charge", json.RawMessage(`{"type":"object"}`),
				func(context.Context, agenttool.Call) (agenttool.Result, error) {
					mu.Lock()
					ran++
					mu.Unlock()
					return agenttool.Text("refunded 5"), nil
				})
			remoteCfg := agentturn.Config{Name: "Refunds", Model: refundsThenSays{}, Tools: []agenttool.Tool{refund},
				BeforeToolCall: func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
					return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: reason}, nil
				}}
			auth := fronta2a.Authenticator{Name: "bearer", Scheme: a2a.HTTPAuthSecurityScheme{Scheme: "Bearer"},
				Principal: func(r *http.Request) (string, error) {
					if r.Header.Get("Authorization") != "Bearer a-token" {
						return "", errors.New("no token")
					}
					return "alice", nil
				}}
			var decisions []*agentturn.ToolDecision
			record := func(ctx context.Context, _ string, a *agentturn.Agent) (context.Context, func(), error) {
				return ctx, a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
					if e, ok := ev.(*agentturn.ToolStart); ok {
						mu.Lock()
						decisions = append(decisions, e.Decision)
						mu.Unlock()
					}
					return nil
				}), nil
			}
			fopts := []fronta2a.Option{fronta2a.WithAuthenticator(auth), fronta2a.WithEscalation(), fronta2a.WithRecorderFor(record)}
			if tc.chosen {
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

			tool := New(client, card, append([]Option{WithAskCaller()}, tc.opts...)...)
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
			mu.Lock()
			defer mu.Unlock()
			if ran != tc.ran {
				t.Errorf("the remote's call ran %d times, want %d", ran, tc.ran)
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
			if len(asked) != 1 {
				t.Fatalf("asked %d times, want once", len(asked))
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
			for _, d := range decisions {
				if d != nil {
					approval = d
				}
			}
			if tc.ran > 0 && (approval == nil || approval.By != tc.by) {
				t.Errorf("the remote recorded the approval as %+v, want by %q", approval, tc.by)
			}
		})
	}
}
