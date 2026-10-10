package control

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

type textArgs struct {
	Text string `json:"text"`
}

func upper(_ context.Context, a textArgs) (string, error) { return strings.ToUpper(a.Text), nil }

// serve serves c over httptest and dials it, with every scope.
func serve(t *testing.T, c agentturn.Control, opts ...Option) *Client {
	t.Helper()
	if len(opts) == 0 {
		opts = []Option{WithInsecureNoAuth()}
	}
	srv := httptest.NewServer(Handler(c, opts...))
	t.Cleanup(srv.Close)
	client, err := Dial(t.Context(), srv.URL, WithReconnectDelay(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// questionAgent is an agent over cfg with its QuestionElicitor
// installed, as a host turns questions on.
func questionAgent(t *testing.T, cfg agentturn.Config) *agentturn.Agent {
	t.Helper()
	a := agentturn.New(cfg)
	cfg.ToolElicitor = a.QuestionElicitor()
	if err := a.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	return a
}

// asker asks the user and returns the action and note it was given.
func asker() agenttool.Tool {
	return agenttool.New("ask", "asks", func(ctx context.Context, _ textArgs) (string, error) {
		elicit, ok := agenttool.ElicitorFrom(ctx)
		if !ok {
			return "", errors.New("no elicitor")
		}
		ans, err := elicit(ctx, agenttool.Elicitation{Message: "delete the branch?"})
		if err != nil {
			return "", err
		}
		return string(ans.Action) + ":" + ans.Note, nil
	})
}

func lastOutput(items openresponses.Items) string {
	for _, it := range slices.Backward(items) {
		if out, ok := it.(*openresponses.FunctionCallOutput); ok {
			return out.Output.Text
		}
	}
	return ""
}

func TestAPromptRunsOverTheWire(t *testing.T) {
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "uppercases", upper)}})
	c := serve(t, a)
	end, err := c.Prompt(t.Context(), openresponses.UserText("abc"))
	if err != nil {
		t.Fatal(err)
	}
	if end.Reason != agentturn.ReasonDone || lastOutput(end.Items) != "ABC" {
		t.Fatalf("end = %+v", end)
	}
	if got := c.State(); len(got.Transcript) != len(a.State().Transcript) || got.Running {
		t.Errorf("state over the wire = %+v", got)
	}
}

// TestTheEventsAreTheAgents checks a remote subscriber gets what one in
// process gets, in the same order.
func TestTheEventsAreTheAgents(t *testing.T) {
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "uppercases", upper)}})
	c := serve(t, a)
	var local []string
	a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		local = append(local, ev.EventType())
		return nil
	})
	var mu sync.Mutex
	var remote []string
	done := make(chan struct{})
	unsub := c.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		mu.Lock()
		defer mu.Unlock()
		remote = append(remote, ev.EventType())
		if _, ok := ev.(*agentturn.RunEnd); ok {
			close(done)
		}
		return nil
	})
	defer unsub()
	if _, err := c.Prompt(t.Context(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("no run_end over the stream")
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(local, remote) {
		t.Errorf("remote events differ:\nlocal  %v\nremote %v", local, remote)
	}
}

func TestResumeAfterADeferral(t *testing.T) {
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "uppercases", upper, agenttool.WithAnnotations(agenttool.Annotations{ReadOnly: true}))},
		BeforeToolCall: func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "rule r", By: "policy"}, nil
		}})
	c := serve(t, a)
	end, err := c.Prompt(t.Context(), openresponses.UserText("abc"))
	if err != nil {
		t.Fatal(err)
	}
	if end.Reason != agentturn.ReasonInputRequired || len(end.Pending) != 1 {
		t.Fatalf("end = %+v", end)
	}
	p := end.Pending[0]
	if p.Decision == nil || p.Decision.Reason != "rule r" || p.Tool == nil || p.Tool.Name() != "upper" || !agenttool.AnnotationsOf(p.Tool).ReadOnly {
		t.Errorf("pending = %+v, tool %#v", p, p.Tool)
	}
	// An answer for no pending call is the loop's error, with its identity.
	if _, err := c.Resume(t.Context(), agentturn.Approve("call_nope")); !errors.Is(err, agentturn.ErrNotPending) {
		t.Errorf("Resume of an unknown call = %v", err)
	}
	end, err = c.Resume(t.Context(), agentturn.Approve(p.Call.CallID).WithBy("human"))
	if err != nil {
		t.Fatal(err)
	}
	if end.Reason != agentturn.ReasonDone || lastOutput(end.Items) != "ABC" {
		t.Fatalf("end after resume = %+v", end)
	}
}

// blocker is a tool that signals it runs and waits for release or its
// context.
func blocker(running chan<- struct{}, release <-chan struct{}) agenttool.Tool {
	return agenttool.New("wait", "waits", func(ctx context.Context, _ textArgs) (string, error) {
		running <- struct{}{}
		select {
		case <-release:
			return "released", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
}

func TestQueueSteersAndFollowsUpARun(t *testing.T) {
	running, release := make(chan struct{}, 1), make(chan struct{})
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, MaxTurns: 3, Tools: []agenttool.Tool{blocker(running, release)}})
	c := serve(t, a)
	type result struct {
		end *agentturn.RunEnd
		err error
	}
	done := make(chan result, 1)
	go func() {
		end, err := c.Prompt(context.Background(), openresponses.UserText("go"))
		done <- result{end, err}
	}()
	<-running
	if err := c.Queue(t.Context(), agentturn.QueueSteer, openresponses.UserText("steered")); err != nil {
		t.Fatal(err)
	}
	if err := c.Queue(t.Context(), agentturn.QueueFollowUp, openresponses.UserText("followed")); err != nil {
		t.Fatal(err)
	}
	if s := c.State(); !s.Running || s.Steering != 1 || s.FollowUps != 1 {
		t.Errorf("state while queued = running %v steering %d follow-ups %d", s.Running, s.Steering, s.FollowUps)
	}
	close(release)
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	// The steer went into the run; the follow-up into it, or, when the
	// run stopped at MaxTurns first, it waits for the next.
	var texts []string
	for _, it := range r.end.Items {
		if m, ok := it.(*openresponses.Message); ok && m.Role == openresponses.RoleUser {
			texts = append(texts, m.Text())
		}
	}
	for _, it := range c.State().Queued {
		if m, ok := it.(*openresponses.Message); ok {
			texts = append(texts, "queued "+m.Text())
		}
	}
	if !slices.Equal(texts, []string{"go", "steered", "followed"}) && !slices.Equal(texts, []string{"go", "steered", "queued followed"}) {
		t.Errorf("user messages = %q", texts)
	}
	if err := c.Queue(t.Context(), "sideways", openresponses.UserText("x")); err == nil {
		t.Error("an unknown queue mode was taken")
	}
}

func TestAbortMidBatch(t *testing.T) {
	running := make(chan struct{}, 1)
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{blocker(running, nil)}})
	c := serve(t, a)
	done := make(chan *agentturn.RunEnd, 1)
	go func() {
		end, _ := c.Prompt(context.Background(), openresponses.UserText("go"))
		done <- end
	}()
	<-running
	// A second prompt while one runs is the loop's ErrRunning.
	if _, err := c.Prompt(t.Context(), openresponses.UserText("again")); !errors.Is(err, agentturn.ErrRunning) {
		t.Errorf("a second prompt = %v", err)
	}
	c.Abort()
	select {
	case end := <-done:
		if end == nil || end.Reason != agentturn.ReasonAborted || len(end.Pending) != 1 || end.Pending[0].Reason != agentturn.PendingAborted {
			t.Fatalf("end = %+v", end)
		}
		if !errors.Is(end.Err, context.Canceled) {
			t.Errorf("end.Err = %v", end.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the run did not abort")
	}
}

// TestQuestionsCrossTheWire answers a tool's question from a remote
// subscriber, a note included, and checks a subscriber that comes back
// while a question waits is sent it again.
func TestQuestionsCrossTheWire(t *testing.T) {
	cases := []struct {
		name   string
		answer agenttool.Answer
		want   string
	}{
		{name: "accept", answer: agenttool.Answer{Action: agenttool.ActionAccept}, want: "accept:"},
		{name: "decline with a note", answer: agenttool.Answer{Action: agenttool.ActionDecline, Note: "use the fixture"}, want: "decline:use the fixture"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := questionAgent(t, agentturn.Config{Model: &echo.Adapter{}, MaxTurns: 1, Tools: []agenttool.Tool{asker()}})
			c := serve(t, a)
			// The first subscriber sees the question and leaves without
			// answering it.
			first := make(chan *agentturn.Question, 1)
			unsub := c.Subscribe(func(_ context.Context, ev agentturn.Event) error {
				if q, ok := ev.(*agentturn.Question); ok {
					select {
					case first <- q:
					default:
					}
				}
				return nil
			})
			done := make(chan *agentturn.RunEnd, 1)
			go func() {
				end, _ := c.Prompt(context.Background(), openresponses.UserText("x"))
				done <- end
			}()
			q := <-first
			unsub()
			if q.Elicitation.Message != "delete the branch?" || q.CallID == "" {
				t.Errorf("question = %+v", q)
			}
			// One that comes later is sent it, and answers.
			var mu sync.Mutex
			var closed *agentturn.QuestionClosed
			gotClose := make(chan struct{})
			unsub2 := c.Subscribe(func(_ context.Context, ev agentturn.Event) error {
				switch e := ev.(type) {
				case *agentturn.Question:
					if e.ID != q.ID {
						t.Errorf("the late subscriber got question %s, want %s", e.ID, q.ID)
					}
					if err := c.Reply(e.ID, tc.answer); err != nil {
						t.Errorf("Reply: %v", err)
					}
				case *agentturn.QuestionClosed:
					mu.Lock()
					closed = e
					mu.Unlock()
					close(gotClose)
				}
				return nil
			})
			defer unsub2()
			end := <-done
			if end == nil || lastOutput(end.Items) != tc.want {
				t.Fatalf("end = %+v", end)
			}
			<-gotClose
			mu.Lock()
			defer mu.Unlock()
			if closed.ID != q.ID || closed.Answer == nil || closed.Answer.Note != tc.answer.Note {
				t.Errorf("closed = %+v", closed)
			}
			// The question is gone: replying again is ErrNoQuestion.
			if err := c.Reply(q.ID, tc.answer); !errors.Is(err, agentturn.ErrNoQuestion) {
				t.Errorf("a second reply = %v", err)
			}
		})
	}
}

// recorder is a Control that records which methods reached it.
type recorder struct {
	mu    sync.Mutex
	calls []string
	ctx   context.Context
	emit  []agentturn.Event
}

// noteCtx records a call and the context it came with; note one that
// takes none.
func (r *recorder) noteCtx(ctx context.Context, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, name)
	r.ctx = ctx
}

func (r *recorder) note(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, name)
}

func (r *recorder) Prompt(ctx context.Context, _ ...openresponses.Item) (*agentturn.RunEnd, error) {
	r.noteCtx(ctx, "Prompt")
	return &agentturn.RunEnd{Reason: agentturn.ReasonDone}, nil
}

func (r *recorder) Resume(ctx context.Context, _ ...agentturn.Answer) (*agentturn.RunEnd, error) {
	r.noteCtx(ctx, "Resume")
	return &agentturn.RunEnd{Reason: agentturn.ReasonDone}, nil
}

func (r *recorder) Queue(ctx context.Context, _ agentturn.QueueMode, _ ...openresponses.Item) error {
	r.noteCtx(ctx, "Queue")
	return nil
}

func (r *recorder) Abort() { r.note("Abort") }

func (r *recorder) State() agentturn.State {
	r.note("State")
	return agentturn.State{}
}

func (r *recorder) Subscribe(fn func(context.Context, agentturn.Event) error) func() {
	r.note("Subscribe")
	for _, ev := range r.emit {
		_ = fn(context.Background(), ev)
	}
	return func() {}
}

func (r *recorder) Reply(string, agenttool.Answer) error {
	r.note("Reply")
	return nil
}

func (r *recorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

// TestScopes pins that each method needs its scope, that read implies
// nothing else, and that a handler with no authenticator refuses
// everyone.
func TestScopes(t *testing.T) {
	tokens := map[string]Principal{
		"reader":  {Name: "watcher", Scopes: []Scope{ScopeRead}},
		"driver":  {Name: "driver", Scopes: []Scope{ScopeRead, ScopeControl}},
		"answers": {Name: "alice", Scopes: []Scope{ScopeAnswer}},
		"all":     {Name: "root", Scopes: AllScopes},
	}
	calls := []struct {
		name  string
		scope Scope
		do    func(*Client) error
	}{
		{"State", ScopeRead, func(c *Client) error { _, err := c.FetchState(context.Background()); return err }},
		{"Prompt", ScopeControl, func(c *Client) error {
			_, err := c.Prompt(context.Background(), openresponses.UserText("x"))
			return err
		}},
		{"Queue", ScopeControl, func(c *Client) error {
			return c.Queue(context.Background(), agentturn.QueueSteer, openresponses.UserText("x"))
		}},
		{"Resume", ScopeAnswer, func(c *Client) error { _, err := c.Resume(context.Background(), agentturn.Approve("c")); return err }},
		{"Reply", ScopeAnswer, func(c *Client) error { return c.Reply("q", agenttool.Answer{Action: agenttool.ActionAccept}) }},
		{"Command", ScopeCommand, func(c *Client) error { _, err := c.Command(context.Background(), "info", nil); return err }},
	}
	for token, p := range tokens {
		t.Run(token, func(t *testing.T) {
			r := &recorder{}
			srv := httptest.NewServer(Handler(r, WithAuthenticator(BearerTokens(tokens)), WithCommands(map[string]Command{
				"info": {Run: func(context.Context, json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{}`), nil }},
			})))
			defer srv.Close()
			c, err := Dial(t.Context(), srv.URL, WithBearerToken(token))
			if err != nil {
				t.Fatal(err)
			}
			for _, call := range calls {
				before := len(r.seen())
				err := call.do(c)
				reached := len(r.seen()) > before || call.name == "Command" && err == nil
				if p.Has(call.scope) {
					if err != nil || !reached {
						t.Errorf("%s with %v: err %v, reached %v", call.name, p.Scopes, err, reached)
					}
				} else if !errors.Is(err, ErrForbidden) || len(r.seen()) != before {
					t.Errorf("%s with %v: err %v, calls %v; want it forbidden before the agent", call.name, p.Scopes, err, r.seen())
				}
			}
		})
	}
	t.Run("the principal is on the run's context", func(t *testing.T) {
		r := &recorder{}
		srv := httptest.NewServer(Handler(r, WithAuthenticator(BearerTokens(tokens))))
		defer srv.Close()
		c, err := Dial(t.Context(), srv.URL, WithBearerToken("answers"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Resume(t.Context(), agentturn.Approve("c")); err != nil {
			t.Fatal(err)
		}
		r.mu.Lock()
		ctx := r.ctx
		r.mu.Unlock()
		if p, ok := PrincipalFrom(ctx); !ok || p.Name != "alice" {
			t.Errorf("principal = %+v, %v", p, ok)
		}
		if tr := agentturn.TriggerFromContext(ctx); tr.Kind != "control" || tr.Ref != "alice" {
			t.Errorf("trigger = %+v", tr)
		}
	})
	t.Run("a wrong token", func(t *testing.T) {
		srv := httptest.NewServer(Handler(&recorder{}, WithAuthenticator(BearerTokens(tokens))))
		defer srv.Close()
		for _, tok := range []string{"", "nope", "reader2", "Reader"} {
			if _, err := Dial(t.Context(), srv.URL, WithBearerToken(tok)); !errors.Is(err, ErrUnauthenticated) {
				t.Errorf("token %q: %v", tok, err)
			}
		}
	})
	t.Run("no authenticator refuses everyone", func(t *testing.T) {
		r := &recorder{}
		srv := httptest.NewServer(Handler(r))
		defer srv.Close()
		if _, err := Dial(t.Context(), srv.URL); !errors.Is(err, ErrUnauthenticated) {
			t.Errorf("Dial = %v", err)
		}
		resp, err := http.Post(srv.URL+"/resume", "application/json", strings.NewReader(`{"answers":[{"call_id":"c"}]}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized || len(r.seen()) != 0 {
			t.Errorf("status %d, calls %v", resp.StatusCode, r.seen())
		}
	})
	t.Run("a read-only stream", func(t *testing.T) {
		srv := httptest.NewServer(Handler(&recorder{}, WithAuthenticator(BearerTokens(tokens))))
		defer srv.Close()
		var errs []error
		var mu sync.Mutex
		c, err := Dial(t.Context(), srv.URL, WithBearerToken("answers"), WithStreamErrors(func(err error) {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}))
		if err != nil {
			t.Fatal(err)
		}
		c.Subscribe(func(context.Context, agentturn.Event) error { return nil })()
		mu.Lock()
		defer mu.Unlock()
		if len(errs) == 0 || !errors.Is(errs[0], ErrForbidden) || !errors.Is(errs[0], ErrStreamBroken) {
			t.Errorf("stream errors = %v", errs)
		}
	})
}

func TestCommands(t *testing.T) {
	var gotArgs json.RawMessage
	c := serve(t, &recorder{}, WithInsecureNoAuth(), WithCommands(map[string]Command{
		"model": {Description: "switch the model", Schema: json.RawMessage(`{"type":"object"}`), Run: func(_ context.Context, args json.RawMessage) (json.RawMessage, error) {
			gotArgs = args
			return json.RawMessage(`{"model":"b"}`), nil
		}},
		"fail": {Description: "fails", Run: func(context.Context, json.RawMessage) (json.RawMessage, error) { return nil, agentturn.ErrRunning }},
	}))
	list, err := c.Commands(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].Name != "fail" || list[1].Name != "model" || list[1].Description != "switch the model" || string(list[1].Schema) != `{"type":"object"}` {
		t.Errorf("commands = %+v", list)
	}
	res, err := c.Command(t.Context(), "model", json.RawMessage(`{"name":"b"}`))
	if err != nil || string(res) != `{"model":"b"}` || string(gotArgs) != `{"name":"b"}` {
		t.Errorf("model = %s, %v (args %s)", res, err, gotArgs)
	}
	if _, err := c.Command(t.Context(), "fail", nil); !errors.Is(err, agentturn.ErrRunning) {
		t.Errorf("fail = %v", err)
	}
	if _, err := c.Command(t.Context(), "nope", nil); !errors.Is(err, ErrUnknownCommand) {
		t.Errorf("nope = %v", err)
	}
}

func TestBadRequestsAreRefused(t *testing.T) {
	r := &recorder{}
	srv := httptest.NewServer(Handler(r, WithInsecureNoAuth(), WithMaxRequestBytes(64)))
	defer srv.Close()
	cases := []struct {
		path, body string
		want       int
	}{
		{"/prompt", `{"items":[{"type":"message","role":"user","content":"` + strings.Repeat("x", 100) + `"}]}`, http.StatusRequestEntityTooLarge},
		{"/prompt", `{"items":`, http.StatusBadRequest},
		{"/prompt", `{"items":[7]}`, http.StatusBadRequest},
		{"/resume", `{"answers":[{"call_id":"c","output":{"type":"message"}}]}`, http.StatusBadRequest},
		{"/reply", `{"id":"q","answer":{"action":"maybe"}}`, http.StatusBadRequest},
		{"/state", ``, http.StatusMethodNotAllowed},
	}
	for _, tc := range cases {
		resp, err := http.Post(srv.URL+tc.path, "application/json", strings.NewReader(tc.body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("%s %s: status %d, want %d", tc.path, tc.body, resp.StatusCode, tc.want)
		}
	}
	if calls := r.seen(); len(calls) != 0 {
		t.Errorf("bad requests reached the agent: %v", calls)
	}
}

// TestASlowStreamIsCut checks a stream that falls behind is ended with
// an overflow, which the client reports as a break.
func TestASlowStreamIsCut(t *testing.T) {
	r := &recorder{}
	for range 5 {
		r.emit = append(r.emit, &agentturn.Queued{Item: openresponses.UserText("x"), Mode: agentturn.QueueSteer})
	}
	srv := httptest.NewServer(Handler(r, WithInsecureNoAuth(), WithEventBuffer(1)))
	defer srv.Close()
	broke := make(chan error, 8)
	c, err := Dial(t.Context(), srv.URL, WithReconnectDelay(time.Hour), WithStreamErrors(func(err error) {
		broke <- err
	}))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	got := 0
	unsub := c.Subscribe(func(context.Context, agentturn.Event) error {
		mu.Lock()
		got++
		mu.Unlock()
		return nil
	})
	defer unsub()
	select {
	case err := <-broke:
		if !errors.Is(err, ErrStreamBroken) || !strings.Contains(err.Error(), "behind") {
			t.Errorf("break = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stream was not cut")
	}
	mu.Lock()
	defer mu.Unlock()
	if got != 1 {
		t.Errorf("%d events before the cut, want the 1 the buffer held", got)
	}
}

func TestDialRefusesWhatIsNotTheProtocol(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"protocol":"something-else"}`))
	}))
	defer srv.Close()
	if _, err := Dial(t.Context(), srv.URL); err == nil {
		t.Error("dialled a server of another protocol")
	}
	if _, err := Dial(t.Context(), "ftp://example.test"); err == nil {
		t.Error("dialled ftp")
	}
}
