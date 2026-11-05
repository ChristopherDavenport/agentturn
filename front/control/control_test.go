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

// TestDeliverOverTheWire pins RFC 0001's rule for a front that lets a
// remote party queue input (#288): it offers the delivery, which says
// whether the items joined the run in flight, and otherwise starts a
// run that takes them and returns its end. A remote party cannot tell
// from Queue and State whether a run will still take a steer.
func TestDeliverOverTheWire(t *testing.T) {
	t.Run("to an idle agent, a run starts", func(t *testing.T) {
		a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, MaxTurns: 1})
		c := serve(t, a)
		joined, end, err := c.Deliver(t.Context(), openresponses.UserText("the build passed"))
		if err != nil {
			t.Fatal(err)
		}
		if joined || end == nil || end.Reason != agentturn.ReasonDone {
			t.Fatalf("joined %v, end %+v; want a run started and done", joined, end)
		}
		if m, ok := end.Items[0].(*openresponses.Message); !ok || m.Text() != "the build passed" {
			t.Errorf("the run's first item = %+v, want the delivery", end.Items[0])
		}
	})
	t.Run("during a run, it joins", func(t *testing.T) {
		running, release := make(chan struct{}, 1), make(chan struct{})
		a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, MaxTurns: 3, Tools: []agenttool.Tool{blocker(running, release)}})
		c := serve(t, a)
		done := make(chan *agentturn.RunEnd, 1)
		go func() {
			end, _ := c.Prompt(context.Background(), openresponses.UserText("go"))
			done <- end
		}()
		<-running
		type delivered struct {
			joined bool
			end    *agentturn.RunEnd
			err    error
		}
		got := make(chan delivered, 1)
		go func() {
			joined, end, err := c.Deliver(context.Background(), openresponses.UserText("the build passed"))
			got <- delivered{joined, end, err}
		}()
		// The queued report waits for the run's next event, so the
		// state says when the server's agent holds the delivery.
		var d *delivered
		for deadline := time.Now().Add(10 * time.Second); d == nil && a.State().Steering == 0 && time.Now().Before(deadline); {
			select {
			case r := <-got:
				d = &r // it came back before anything was queued
			case <-time.After(5 * time.Millisecond):
			}
		}
		close(release)
		if d == nil {
			r := <-got
			d = &r
		}
		if d.err != nil || !d.joined || d.end != nil {
			t.Errorf("joined %v, end %+v, err %v; want joined", d.joined, d.end, d.err)
		}
		<-done
	})
	t.Run("a Control without one", func(t *testing.T) {
		c := serve(t, struct{ agentturn.Control }{agentturn.New(agentturn.Config{Model: &echo.Adapter{}})})
		if _, _, err := c.Deliver(t.Context(), openresponses.UserText("x")); !errors.Is(err, ErrNoDelivery) {
			t.Errorf("err = %v, want ErrNoDelivery", err)
		}
	})
}

// TestAcceptOverTheWire pins #302 across the wire: the control front
// carries a delivery's two steps, so a view forwarding a person's
// steers answers each once the server's agent has it, while a quiet
// tool still runs, and learns the outcome after.
func TestAcceptOverTheWire(t *testing.T) {
	t.Run("during a run, both are accepted before the tool ends", func(t *testing.T) {
		running, release := make(chan struct{}, 1), make(chan struct{})
		a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, MaxTurns: 3, Tools: []agenttool.Tool{blocker(running, release)}})
		c := serve(t, a)
		done := make(chan *agentturn.RunEnd, 1)
		go func() {
			end, _ := c.Prompt(context.Background(), openresponses.UserText("go"))
			done <- end
		}()
		<-running
		var released sync.Once
		defer released.Do(func() { close(release) })
		accepted := make(chan *agentturn.Delivery, 2)
		go func() {
			for _, text := range []string{"one", "two"} {
				d, err := c.Accept(context.Background(), openresponses.UserText(text))
				if err != nil {
					t.Error(err)
					return
				}
				accepted <- d
			}
		}()
		var ds []*agentturn.Delivery
		for len(ds) < 2 {
			select {
			case d := <-accepted:
				ds = append(ds, d)
			case <-time.After(5 * time.Second):
				t.Fatalf("%d of two accepted while the tool ran", len(ds))
			}
		}
		if n := a.State().Steering; n != 2 {
			t.Errorf("steering = %d, want both", n)
		}
		released.Do(func() { close(release) })
		for i, d := range ds {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			joined, end, err := d.Wait(ctx)
			cancel()
			if err != nil || !joined || end != nil {
				t.Errorf("delivery %d: joined %v, end %+v, err %v; want joined", i, joined, end, err)
			}
		}
		<-done
	})
	t.Run("to an idle agent, the agent starts the run", func(t *testing.T) {
		a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, MaxTurns: 1})
		c := serve(t, a)
		ctx, cancel := context.WithCancel(context.Background())
		d, err := c.Accept(ctx, openresponses.UserText("the build passed"))
		// The caller goes away at once: the agent settles it anyway.
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		wctx, wcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer wcancel()
		joined, end, err := d.Wait(wctx)
		if err != nil || joined || end == nil || end.Reason != agentturn.ReasonDone {
			t.Fatalf("joined %v, end %+v, err %v; want a run started and done", joined, end, err)
		}
	})
	t.Run("a refusal queues nothing", func(t *testing.T) {
		a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, MaxTurns: 1})
		c := serve(t, a)
		var err error
		if herr := a.WhileIdle(func() error {
			_, err = c.Accept(t.Context(), openresponses.UserText("x"))
			return nil
		}); herr != nil {
			t.Fatal(herr)
		}
		if !errors.Is(err, agentturn.ErrHeld) {
			t.Errorf("err = %v, want ErrHeld", err)
		}
		if n := a.State().Steering; n != 0 {
			t.Errorf("steering = %d, want nothing queued", n)
		}
	})
	t.Run("a Control without one", func(t *testing.T) {
		c := serve(t, struct{ agentturn.Control }{agentturn.New(agentturn.Config{Model: &echo.Adapter{}})})
		if _, err := c.Accept(t.Context(), openresponses.UserText("x")); !errors.Is(err, ErrNoDelivery) {
			t.Errorf("err = %v, want ErrNoDelivery", err)
		}
	})
	t.Run("behind a wrapper that cannot flush itself", func(t *testing.T) {
		// A logging middleware's writer that unwraps to the server's
		// but implements no Flush.
		h := Handler(agentturn.New(agentturn.Config{Model: &echo.Adapter{}, MaxTurns: 1}), WithInsecureNoAuth())
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h.ServeHTTP(unflushed{w}, r)
		}))
		t.Cleanup(srv.Close)
		c, err := Dial(t.Context(), srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		d, err := c.Accept(t.Context(), openresponses.UserText("x"))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, end, err := d.Wait(ctx); err != nil || end == nil {
			t.Fatalf("end %+v, err %v; want a run", end, err)
		}
	})
	t.Run("a client gone after the accept, the agent settles it", func(t *testing.T) {
		running, release := make(chan struct{}, 2), make(chan struct{})
		// One turn: the run stops after its batch without draining the
		// delivery, which then needs a run of its own.
		a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, MaxTurns: 1, Tools: []agenttool.Tool{blocker(running, release)}})
		ends := make(chan *agentturn.RunEnd, 4)
		a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
			if e, ok := ev.(*agentturn.RunEnd); ok {
				ends <- e
			}
			return nil
		})
		srv := httptest.NewServer(Handler(a, WithInsecureNoAuth()))
		t.Cleanup(srv.Close)
		go func() { _, _ = a.Prompt(context.Background(), openresponses.UserText("go")) }()
		<-running
		var released sync.Once
		defer released.Do(func() { close(release) })
		raw, err := itemsOut(openresponses.Items{openresponses.UserText("later")})
		if err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(struct {
			Items json.RawMessage `json:"items"`
		}{raw})
		ctx, cancel := context.WithCancel(t.Context())
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/accept", strings.NewReader(string(body)))
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var first acceptReply
		if err := json.NewDecoder(resp.Body).Decode(&first); err != nil || !first.Accepted {
			t.Fatalf("first = %+v, err %v", first, err)
		}
		// The client goes away before the outcome.
		cancel()
		resp.Body.Close()
		released.Do(func() { close(release) })
		for i := range 2 {
			select {
			case <-ends:
			case <-time.After(10 * time.Second):
				t.Fatalf("%d runs ended; the delivery was not settled once its client went away", i)
			}
		}
		var texts []string
		for _, item := range a.State().Transcript {
			if m, ok := item.(*openresponses.Message); ok && m.Role == openresponses.RoleUser {
				texts = append(texts, m.Text())
			}
		}
		if !slices.Contains(texts, "later") {
			t.Errorf("user messages = %q, want the delivery taken", texts)
		}
	})
	t.Run("a server without the route", func(t *testing.T) {
		h := Handler(agentturn.New(agentturn.Config{Model: &echo.Adapter{}}), WithInsecureNoAuth())
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/accept" {
				http.NotFound(w, r)
				return
			}
			h.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)
		c, err := Dial(t.Context(), srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Accept(t.Context(), openresponses.UserText("x")); !errors.Is(err, ErrNoDelivery) {
			t.Errorf("err = %v, want ErrNoDelivery", err)
		}
	})
}

// unflushed is a middleware's response writer that unwraps to the one
// it wraps but does not implement http.Flusher itself.
type unflushed struct{ w http.ResponseWriter }

func (u unflushed) Header() http.Header         { return u.w.Header() }
func (u unflushed) Write(b []byte) (int, error) { return u.w.Write(b) }
func (u unflushed) WriteHeader(code int)        { u.w.WriteHeader(code) }
func (u unflushed) Unwrap() http.ResponseWriter { return u.w }

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
					if err := c.Reply(context.Background(), e.ID, tc.answer); err != nil {
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
			if err := c.Reply(context.Background(), q.ID, tc.answer); !errors.Is(err, agentturn.ErrNoQuestion) {
				t.Errorf("a second reply = %v", err)
			}
		})
	}
}

// recorder is a Control that records which methods reached it.
type recorder struct {
	mu      sync.Mutex
	calls   []string
	ctx     context.Context
	emit    []agentturn.Event
	answers []agentturn.Answer
	replies []agenttool.Answer
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

func (r *recorder) Resume(ctx context.Context, answers ...agentturn.Answer) (*agentturn.RunEnd, error) {
	r.noteCtx(ctx, "Resume")
	r.mu.Lock()
	r.answers = answers
	r.mu.Unlock()
	return &agentturn.RunEnd{Reason: agentturn.ReasonDone}, nil
}

func (r *recorder) Queue(ctx context.Context, _ agentturn.QueueMode, _ ...openresponses.Item) error {
	r.noteCtx(ctx, "Queue")
	return nil
}

func (r *recorder) Deliver(ctx context.Context, _ ...openresponses.Item) (bool, *agentturn.RunEnd, error) {
	r.noteCtx(ctx, "Deliver")
	return true, nil, nil
}

func (r *recorder) Accept(ctx context.Context, _ ...openresponses.Item) (*agentturn.Delivery, error) {
	r.noteCtx(ctx, "Accept")
	d, settle := agentturn.NewDelivery()
	settle(true, nil, nil)
	return d, nil
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

func (r *recorder) Reply(ctx context.Context, _ string, a agenttool.Answer) error {
	r.noteCtx(ctx, "Reply")
	r.mu.Lock()
	r.replies = append(r.replies, a)
	r.mu.Unlock()
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
		{"Deliver", ScopeControl, func(c *Client) error {
			_, _, err := c.Deliver(context.Background(), openresponses.UserText("x"))
			return err
		}},
		{"Resume", ScopeAnswer, func(c *Client) error { _, err := c.Resume(context.Background(), agentturn.Approve("c")); return err }},
		{"Reply", ScopeAnswer, func(c *Client) error {
			return c.Reply(context.Background(), "q", agenttool.Answer{Action: agenttool.ActionAccept})
		}},
		{"Command", ScopeCommand, func(c *Client) error { _, err := c.Command(context.Background(), "info", nil); return err }},
	}
	for token, p := range tokens {
		t.Run(token, func(t *testing.T) {
			r := &recorder{}
			srv := httptest.NewServer(Handler(r, WithAuthenticator(tokenAuth(tokens)), WithCommands(map[string]Command{
				"info": {Run: func(context.Context, json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{}`), nil }},
			})))
			defer srv.Close()
			c, err := Dial(t.Context(), srv.URL, withToken(token))
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
		srv := httptest.NewServer(Handler(r, WithAuthenticator(tokenAuth(tokens))))
		defer srv.Close()
		c, err := Dial(t.Context(), srv.URL, withToken("answers"))
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
	t.Run("the principal is on an answer's context", func(t *testing.T) {
		r := &recorder{}
		srv := httptest.NewServer(Handler(r, WithAuthenticator(tokenAuth(tokens))))
		defer srv.Close()
		c, err := Dial(t.Context(), srv.URL, withToken("answers"))
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Reply(t.Context(), "q", agenttool.Answer{Action: agenttool.ActionAccept}); err != nil {
			t.Fatal(err)
		}
		r.mu.Lock()
		ctx := r.ctx
		r.mu.Unlock()
		if by, ok := agentturn.AnswererFrom(ctx); !ok || by.Kind != "control" || by.Ref != "alice" {
			t.Errorf("answerer = %+v, %v", by, ok)
		}
	})
	// #240: a client holding only answer cannot record its approval as
	// a rule's; a host lets a principal it names choose.
	t.Run("who decided a resume", func(t *testing.T) {
		chooser := map[string]Principal{
			"answers": {Name: "alice", Scopes: []Scope{ScopeAnswer}},
			"engine":  {Name: "engine", Scopes: []Scope{ScopeAnswer, "decide"}},
		}
		for _, tc := range []struct {
			token, sent, want string
		}{
			{"answers", "policy", "human"},
			{"answers", "", "human"},
			{"engine", "policy", "policy"},
			{"engine", "", ""},
		} {
			r := &recorder{}
			srv := httptest.NewServer(Handler(r, WithAuthenticator(tokenAuth(chooser)), WithChosenBy("decide")))
			c, err := Dial(t.Context(), srv.URL, withToken(tc.token))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Resume(t.Context(), agentturn.Approve("c").WithBy(tc.sent)); err != nil {
				t.Fatal(err)
			}
			srv.Close()
			r.mu.Lock()
			got := r.answers[0].By
			r.mu.Unlock()
			if got != tc.want {
				t.Errorf("%s sending by %q: recorded %q, want %q", tc.token, tc.sent, got, tc.want)
			}
		}
	})
	// #267: who answered a question is on the answer, and a client
	// names it only under the same host check as a resume's.
	t.Run("who decided a reply", func(t *testing.T) {
		chooser := map[string]Principal{
			"answers": {Name: "alice", Scopes: []Scope{ScopeAnswer}},
			"engine":  {Name: "engine", Scopes: []Scope{ScopeAnswer, "decide"}},
		}
		for _, tc := range []struct {
			token, sent, want string
			refused           bool
		}{
			{token: "answers", sent: "policy", want: "human"},
			{token: "answers", sent: "", want: "human"},
			{token: "answers", sent: "system", want: "human"},
			{token: "engine", sent: "policy", want: "policy"},
			{token: "engine", sent: "agent", want: "agent"},
			{token: "engine", sent: "", want: ""},
			{token: "engine", sent: "system", refused: true},
		} {
			r := &recorder{}
			srv := httptest.NewServer(Handler(r, WithAuthenticator(tokenAuth(chooser)), WithChosenBy("decide")))
			c, err := Dial(t.Context(), srv.URL, withToken(tc.token))
			if err != nil {
				t.Fatal(err)
			}
			err = c.Reply(t.Context(), "q", agenttool.Answer{Action: agenttool.ActionAccept, By: tc.sent, Note: "n"})
			srv.Close()
			r.mu.Lock()
			replies := slices.Clone(r.replies)
			r.mu.Unlock()
			if tc.refused {
				if err == nil || len(replies) != 0 {
					t.Errorf("%s sending by %q: err %v, replies %+v; want it refused", tc.token, tc.sent, err, replies)
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(replies) != 1 || replies[0].By != tc.want || replies[0].Note != "n" {
				t.Errorf("%s sending by %q: replied %+v, want by %q", tc.token, tc.sent, replies, tc.want)
			}
		}
	})
	t.Run("a wrong token", func(t *testing.T) {
		srv := httptest.NewServer(Handler(&recorder{}, WithAuthenticator(tokenAuth(tokens))))
		defer srv.Close()
		for _, tok := range []string{"", "nope", "reader2", "Reader"} {
			if _, err := Dial(t.Context(), srv.URL, withToken(tok)); !errors.Is(err, ErrUnauthenticated) {
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
		srv := httptest.NewServer(Handler(&recorder{}, WithAuthenticator(tokenAuth(tokens))))
		defer srv.Close()
		// The stream's refusal arrives on its own goroutine, after
		// Subscribe returns: wait for it.
		first := make(chan error, 1)
		c, err := Dial(t.Context(), srv.URL, withToken("answers"), WithStreamErrors(func(err error) {
			select {
			case first <- err:
			default:
			}
		}))
		if err != nil {
			t.Fatal(err)
		}
		unsubscribe := c.Subscribe(func(context.Context, agentturn.Event) error { return nil })
		defer unsubscribe()
		select {
		case err := <-first:
			if !errors.Is(err, ErrForbidden) || !errors.Is(err, ErrStreamBroken) {
				t.Errorf("stream error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("no stream error within 5s")
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

// endWatcher subscribes to a in process and hands over its run ends.
func endWatcher(a *agentturn.Agent) <-chan *agentturn.RunEnd {
	ends := make(chan *agentturn.RunEnd, 4)
	a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		if e, ok := ev.(*agentturn.RunEnd); ok {
			ends <- e
		}
		return nil
	})
	return ends
}

// TestADroppedConnectionDoesNotAbortTheRun cuts the connection under a
// Prompt: the client says the connection was lost, and the run goes on
// to its end on the agent, where another subscriber sees it.
func TestADroppedConnectionDoesNotAbortTheRun(t *testing.T) {
	running, release := make(chan struct{}, 1), make(chan struct{})
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, MaxTurns: 1, Tools: []agenttool.Tool{blocker(running, release)}})
	ends := endWatcher(a)
	srv := httptest.NewServer(Handler(a, WithInsecureNoAuth()))
	defer srv.Close()
	c, err := Dial(t.Context(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 1)
	go func() {
		_, err := c.Prompt(context.Background(), openresponses.UserText("go"))
		errs <- err
	}()
	<-running
	srv.CloseClientConnections()
	select {
	case err := <-errs:
		if !errors.Is(err, ErrConnectionLost) {
			t.Fatalf("Prompt over a cut connection = %v, want ErrConnectionLost", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Prompt did not return when its connection was cut")
	}
	if s := a.State(); !s.Running {
		t.Fatal("the run stopped with its connection")
	}
	close(release)
	select {
	case end := <-ends:
		if end.Reason == agentturn.ReasonAborted || end.Err != nil {
			t.Errorf("the run ended %s (%v), want it to finish", end.Reason, end.Err)
		}
		if lastOutput(end.Items) != "released" {
			t.Errorf("end = %+v", end)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the run did not end")
	}
}

// TestACancelledCallerAbortsTheRun keeps the in-process meaning of the
// caller's context: cancelling it aborts the run, through /abort.
func TestACancelledCallerAbortsTheRun(t *testing.T) {
	running, release := make(chan struct{}, 1), make(chan struct{})
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{blocker(running, release)}})
	ends := endWatcher(a)
	c := serve(t, a)
	// Registered after serve, so it runs first: a run the cancel failed
	// to abort is let go, and the server closes, rather than the test
	// hanging on it.
	t.Cleanup(func() { close(release) })
	ctx, cancel := context.WithCancel(t.Context())
	errs := make(chan error, 1)
	go func() {
		_, err := c.Prompt(ctx, openresponses.UserText("go"))
		errs <- err
	}()
	<-running
	cancel()
	if err := <-errs; !errors.Is(err, context.Canceled) || errors.Is(err, ErrConnectionLost) {
		t.Errorf("Prompt = %v, want the context's error", err)
	}
	select {
	case end := <-ends:
		if end.Reason != agentturn.ReasonAborted {
			t.Errorf("the run ended %s, want aborted", end.Reason)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelling the caller did not abort the run")
	}
}

// TestATransportFailureIsALostConnection checks a server that is gone
// is ErrConnectionLost, not an answer.
func TestATransportFailureIsALostConnection(t *testing.T) {
	srv := httptest.NewServer(Handler(&recorder{}, WithInsecureNoAuth()))
	c, err := Dial(t.Context(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	srv.Close()
	if _, err := c.Prompt(t.Context(), openresponses.UserText("x")); !errors.Is(err, ErrConnectionLost) {
		t.Errorf("Prompt = %v", err)
	}
	if _, err := c.Resume(t.Context(), agentturn.Approve("c")); !errors.Is(err, ErrConnectionLost) {
		t.Errorf("Resume = %v", err)
	}
}

// Every 401 carries the challenge WithChallenge names, as RFC 9110
// §15.5.2 requires: the authenticator's refusal and no authenticator.
// No other answer carries it, and without the option none does.
func TestChallenge(t *testing.T) {
	const challenge = `Bearer realm="test"`
	auth := WithAuthenticator(tokenAuth(map[string]Principal{
		"reader": {Name: "watcher", Scopes: []Scope{ScopeRead}},
		"all":    {Name: "root", Scopes: AllScopes},
	}))
	for _, sv := range []struct {
		name      string
		challenge string
		opts      []Option
	}{
		{"with the option", challenge, []Option{WithChallenge(challenge)}},
		{"without the option", "", nil},
	} {
		t.Run(sv.name, func(t *testing.T) {
			url := func(opts ...Option) string {
				srv := httptest.NewServer(Handler(&recorder{}, append(slices.Clone(sv.opts), opts...)...))
				t.Cleanup(srv.Close)
				return srv.URL
			}
			unauthed, authed := url(), url(auth)
			tests := []struct {
				name, method, url, token, body string
				status                         int
			}{
				{"no authenticator", http.MethodGet, unauthed + "/", "", "", http.StatusUnauthorized},
				{"no token", http.MethodGet, authed + "/state", "", "", http.StatusUnauthorized},
				{"wrong token", http.MethodGet, authed + "/state", "guess", "", http.StatusUnauthorized},
				{"wrong token, events", http.MethodGet, authed + "/events", "guess", "", http.StatusUnauthorized},
				{"wrong token, prompt", http.MethodPost, authed + "/prompt", "guess", `{}`, http.StatusUnauthorized},
				{"no scope", http.MethodPost, authed + "/prompt", "reader", `{}`, http.StatusForbidden},
				{"bad request", http.MethodPost, authed + "/queue", "all", `{"mode":"sideways"}`, http.StatusBadRequest},
				{"no route", http.MethodGet, authed + "/nothing", "all", "", http.StatusNotFound},
				{"state", http.MethodGet, authed + "/state", "reader", "", http.StatusOK},
			}
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					req, err := http.NewRequestWithContext(t.Context(), tc.method, tc.url, strings.NewReader(tc.body))
					if err != nil {
						t.Fatal(err)
					}
					if tc.token != "" {
						req.Header.Set("Authorization", "Bearer "+tc.token)
					}
					resp, err := http.DefaultClient.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					resp.Body.Close()
					if resp.StatusCode != tc.status {
						t.Fatalf("status %d, want %d", resp.StatusCode, tc.status)
					}
					want := ""
					if tc.status == http.StatusUnauthorized {
						want = sv.challenge
					}
					if got := resp.Header.Get("WWW-Authenticate"); got != want {
						t.Errorf("WWW-Authenticate %q, want %q", got, want)
					}
				})
			}
		})
	}
}

// TestTheCallersClaimIsTheHostsToTrust pins the control front's caller
// claim (the part A review's M3 of #303): a client sends the trigger
// its caller's context holds, so a person at a terminal sends kind
// "human", and the handler runs the input under it only for a
// principal the host lets claim one (WithTriggerClaims); anyone else's
// input runs under kind "control" naming the principal, the claim
// unrecorded. A claim that names no ref names the principal.
func TestTheCallersClaimIsTheHostsToTrust(t *testing.T) {
	const claim Scope = "claim"
	tokens := map[string]Principal{
		"person": {Name: "chris", Scopes: append(slices.Clone(AllScopes), claim)},
		"driver": {Name: "driver", Scopes: AllScopes},
	}
	calls := []struct {
		name string
		do   func(context.Context, *Client) error
	}{
		{"Prompt", func(ctx context.Context, c *Client) error {
			_, err := c.Prompt(ctx, openresponses.UserText("hi"))
			return err
		}},
		{"Queue", func(ctx context.Context, c *Client) error {
			return c.Queue(ctx, agentturn.QueueSteer, openresponses.UserText("hi"))
		}},
		{"Deliver", func(ctx context.Context, c *Client) error {
			_, _, err := c.Deliver(ctx, openresponses.UserText("hi"))
			return err
		}},
		{"Accept", func(ctx context.Context, c *Client) error {
			d, err := c.Accept(ctx, openresponses.UserText("hi"))
			if err != nil {
				return err
			}
			_, _, err = d.Wait(ctx)
			return err
		}},
		{"Resume", func(ctx context.Context, c *Client) error {
			_, err := c.Resume(ctx, agentturn.Approve("c"))
			return err
		}},
	}
	for _, tc := range []struct {
		name  string
		token string
		claim agentturn.Trigger
		want  agentturn.Trigger
	}{
		{"a person's claim, trusted", "person", agentturn.Trigger{Kind: agentturn.TriggerHuman}, agentturn.Trigger{Kind: agentturn.TriggerHuman, Ref: "chris"}},
		{"a claim with its own ref, trusted", "person", agentturn.Trigger{Kind: agentturn.TriggerHuman, Ref: "chris@tty1", Source: "console"}, agentturn.Trigger{Kind: agentturn.TriggerHuman, Ref: "chris@tty1", Source: "console"}},
		{"no claim", "person", agentturn.Trigger{}, agentturn.Trigger{Kind: "control", Ref: "chris"}},
		{"an untrusted claim", "driver", agentturn.Trigger{Kind: agentturn.TriggerHuman}, agentturn.Trigger{Kind: "control", Ref: "driver"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &recorder{}
			srv := httptest.NewServer(Handler(r, WithAuthenticator(tokenAuth(tokens)), WithTriggerClaims(claim)))
			defer srv.Close()
			c, err := Dial(t.Context(), srv.URL, withToken(tc.token))
			if err != nil {
				t.Fatal(err)
			}
			ctx := t.Context()
			if !tc.claim.IsZero() {
				ctx = agentturn.ContextWithTrigger(ctx, tc.claim)
			}
			for _, call := range calls {
				if err := call.do(ctx, c); err != nil {
					t.Fatalf("%s: %v", call.name, err)
				}
				r.mu.Lock()
				got := agentturn.TriggerFromContext(r.ctx)
				r.mu.Unlock()
				if got.Kind != tc.want.Kind || got.Ref != tc.want.Ref || got.Source != tc.want.Source {
					t.Errorf("%s ran under %+v, want %+v", call.name, got, tc.want)
				}
			}
		})
	}
	// A reply carries the answerer's claim the same way: a person
	// answering at a terminal claims human.
	for _, tc := range []struct {
		name  string
		token string
		claim agentturn.Trigger
		want  agentturn.Trigger
	}{
		{"a person's answer, trusted", "person", agentturn.Trigger{Kind: agentturn.TriggerHuman}, agentturn.Trigger{Kind: agentturn.TriggerHuman, Ref: "chris"}},
		{"an answer with no claim", "person", agentturn.Trigger{}, agentturn.Trigger{Kind: "control", Ref: "chris"}},
		{"an untrusted answer's claim", "driver", agentturn.Trigger{Kind: agentturn.TriggerHuman}, agentturn.Trigger{Kind: "control", Ref: "driver"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &recorder{}
			srv := httptest.NewServer(Handler(r, WithAuthenticator(tokenAuth(tokens)), WithTriggerClaims(claim)))
			defer srv.Close()
			c, err := Dial(t.Context(), srv.URL, withToken(tc.token))
			if err != nil {
				t.Fatal(err)
			}
			ctx := t.Context()
			if !tc.claim.IsZero() {
				ctx = agentturn.ContextWithAnswerer(ctx, tc.claim)
			}
			if err := c.Reply(ctx, "q", agenttool.Answer{Action: agenttool.ActionAccept}); err != nil {
				t.Fatal(err)
			}
			r.mu.Lock()
			got, ok := agentturn.AnswererFrom(r.ctx)
			r.mu.Unlock()
			if !ok || got.Kind != tc.want.Kind || got.Ref != tc.want.Ref {
				t.Errorf("answered by %+v, want %+v", got, tc.want)
			}
		})
	}
	t.Run("a trusted claim the format cannot write", func(t *testing.T) {
		r := &recorder{}
		srv := httptest.NewServer(Handler(r, WithAuthenticator(tokenAuth(tokens)), WithTriggerClaims(claim)))
		defer srv.Close()
		c, err := Dial(t.Context(), srv.URL, withToken("person"))
		if err != nil {
			t.Fatal(err)
		}
		ctx := agentturn.ContextWithTrigger(t.Context(), agentturn.Trigger{Kind: agentturn.TriggerHuman, Extra: map[string]any{"kind": "x"}})
		if _, err := c.Prompt(ctx, openresponses.UserText("hi")); !errors.Is(err, agentturn.ErrTriggerExtra) {
			t.Errorf("err = %v, want ErrTriggerExtra", err)
		}
		if seen := r.seen(); slices.Contains(seen, "Prompt") {
			t.Errorf("the agent was prompted: %v", seen)
		}
	})
}
