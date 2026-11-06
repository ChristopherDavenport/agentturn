package session

import (
	"context"
	"errors"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/front/control"
	"github.com/ChristopherDavenport/openresponses"
)

// deliveries returns the queued entries of s holding a message, by its
// text.
func deliveries(s *agentsession.Session) map[string]*agentsession.QueuedEntry {
	out := map[string]*agentsession.QueuedEntry{}
	for _, e := range s.Entries() {
		if q, ok := e.(*agentsession.QueuedEntry); ok {
			if m, ok := q.Item.(*openresponses.Message); ok {
				out[m.Text()] = q
			}
		}
	}
	return out
}

// TestADeliveryIsRecordedAsOne pins RFC 0001's queued row (#244): a
// queued event says how the input arrived, and the recorder writes a
// delivery's entry with format 0.15's deliver. Recorder.Deliver writes
// it before the agent takes the input, so one made while the agent
// waits on an approval survives a quit, and a recorder hands an owed
// delivery back as a delivery, which the turn that takes it reads as
// one rather than as a steer.
func TestADeliveryIsRecordedAsOne(t *testing.T) {
	t.Run("from the event", func(t *testing.T) {
		ctx := context.Background()
		rec, s, err := Start(ctx, agentsession.NewMemoryStore(), agentsession.Header{})
		if err != nil {
			t.Fatal(err)
		}
		a := agentturn.New(agentturn.Config{Model: allCalls{}, ModelName: "m", MaxTurns: 1})
		defer rec.Attach(a)()
		if _, _, err := a.Deliver(ctx, openresponses.UserText("the build passed")); err != nil {
			t.Fatal(err)
		}
		a.Steer(openresponses.UserText("steered"))
		if _, err := a.Prompt(ctx, openresponses.UserText("next")); err != nil {
			t.Fatal(err)
		}
		q := deliveries(s)
		if d := q["the build passed"]; d == nil || !d.Deliver {
			t.Errorf("delivery's entry = %+v, want deliver", d)
		}
		if d := q["steered"]; d == nil || d.Deliver {
			t.Errorf("steer's entry = %+v, want no deliver", d)
		}
		verifyAll(t, s)
	})
	t.Run("at an approval prompt, across a quit", func(t *testing.T) {
		ctx := context.Background()
		store := agentsession.NewMemoryStore()
		rec, s, err := Start(ctx, store, agentsession.Header{})
		if err != nil {
			t.Fatal(err)
		}
		hold := func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "ask"}, nil
		}
		tools := []agenttool.Tool{agenttool.New("deploy", "", func(context.Context, echoArgs) (string, error) { return "deployed", nil })}
		cfg := agentturn.Config{Model: allCalls{}, ModelName: "m", MaxTurns: 1, Tools: tools, BeforeToolCall: hold}
		a := agentturn.New(cfg)
		unsub := rec.Attach(a)
		end, err := a.Prompt(ctx, openresponses.UserText("deploy"))
		if err != nil || end.Reason != agentturn.ReasonInputRequired {
			t.Fatalf("first run: %v, %+v", err, end)
		}
		// The build's result arrives while the person decides; then the
		// process quits.
		if _, _, err := rec.Deliver(ctx, a, openresponses.UserText("the build passed")); !errors.Is(err, agentturn.ErrInputRequired) {
			t.Errorf("delivery at the prompt: err = %v, want ErrInputRequired", err)
		}
		unsub()

		rec, s, err = Resume(ctx, store, s.ID())
		if err != nil {
			t.Fatal(err)
		}
		if d := deliveries(s)["the build passed"]; d == nil || !d.Deliver {
			t.Errorf("the delivery's entry = %+v, want it written with deliver before the quit", d)
		}
		opts, err := AgentOptions(s)
		if err != nil {
			t.Fatal(err)
		}
		cfg.BeforeToolCall = nil
		b := agentturn.New(cfg, opts...)
		defer rec.Attach(b)()
		if n, deliver := rec.Requeue(ctx, b); n != 1 || !deliver {
			t.Fatalf("requeued %d, want the delivery", n)
		}
		var arrived []agentturn.TurnInput
		b.Subscribe(func(_ context.Context, ev agentturn.Event) error {
			if e, ok := ev.(*agentturn.TurnStart); ok && e.Turn == 1 {
				arrived = e.Arrived
			}
			return nil
		})
		pending := b.State().Pending
		if len(pending) != 1 {
			t.Fatalf("pending = %+v", pending)
		}
		if _, err := b.Resume(ctx, agentturn.Approve(pending[0].Call.CallID)); err != nil {
			t.Fatal(err)
		}
		i := slices.IndexFunc(arrived, func(in agentturn.TurnInput) bool {
			m, ok := in.Item.(*openresponses.Message)
			return ok && m.Text() == "the build passed"
		})
		if i < 0 || arrived[i].Mode != agentturn.InputDeliver {
			t.Errorf("the first turn's inputs = %+v, want the delivery as deliver", arrived)
		}
		verifyAll(t, s)
	})
}

// TestAnOwedDeliveryStartsTheRunThatTakesIt pins the host's half of
// the queued row (#244): Requeue says that what it handed back holds a
// delivery, and Continue then runs although the transcript ends with
// the model's answer, its first turn taking the delivery as one.
func TestAnOwedDeliveryStartsTheRunThatTakesIt(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := agentturn.Config{Model: allCalls{}, ModelName: "m", MaxTurns: 1}
	a := agentturn.New(cfg)
	unsub := rec.Attach(a)
	if _, err := a.Prompt(ctx, openresponses.UserText("hello")); err != nil {
		t.Fatal(err)
	}
	unsub()
	// A delivery the process accepted and wrote, then quit before any
	// run took it.
	if _, err := store.Append(ctx, s.ID(), agentsession.NewDelivery(openresponses.UserText("the build passed"))); err != nil {
		t.Fatal(err)
	}

	rec, s, err = Resume(ctx, store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	opts, err := AgentOptions(s)
	if err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(cfg, opts...)
	defer rec.Attach(b)()
	n, deliver := rec.Requeue(ctx, b)
	if n != 1 || !deliver {
		t.Errorf("Requeue = %d, %v; want one input, a delivery", n, deliver)
	}
	var arrived []agentturn.TurnInput
	b.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		if e, ok := ev.(*agentturn.TurnStart); ok && e.Turn == 1 {
			arrived = e.Arrived
		}
		return nil
	})
	end, err := b.Continue(ctx)
	if err != nil {
		t.Fatalf("continue: %v", err)
	}
	if end.Reason != agentturn.ReasonDone {
		t.Errorf("run ended %s", end.Reason)
	}
	if len(arrived) != 1 || arrived[0].Mode != agentturn.InputDeliver {
		t.Errorf("the first turn's inputs = %+v, want the delivery as deliver", arrived)
	}
	verifyAll(t, s)
}

// TestARefusedDeliveryLeavesNothingOwed pins Recorder.Deliver's side of
// #244: a delivery the agent refuses, held idle by WhileIdle, writes no
// queued entry, so nothing is owed for it: not after the next run's end,
// and not after a restart, where a retry the caller made already
// delivered it once.
func TestARefusedDeliveryLeavesNothingOwed(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := agentturn.Config{Model: allCalls{}, ModelName: "m", MaxTurns: 1}
	a := agentturn.New(cfg)
	unsub := rec.Attach(a)
	if err := a.WhileIdle(func() error {
		_, _, err := rec.Deliver(ctx, a, openresponses.UserText("the build passed"))
		if !errors.Is(err, agentturn.ErrRunning) {
			t.Errorf("a delivery during the hold: err = %v, want ErrRunning", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// The caller retries once the hold is over, and the run takes it.
	if _, _, err := rec.Deliver(ctx, a, openresponses.UserText("the build passed")); err != nil {
		t.Fatal(err)
	}
	unsub()
	n := 0
	for _, e := range s.Entries() {
		if _, ok := e.(*agentsession.QueuedEntry); ok {
			n++
		}
	}
	if n != 1 {
		t.Errorf("%d queued entries, want the retry's alone", n)
	}

	rec, s, err = Resume(ctx, store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	opts, err := AgentOptions(s)
	if err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(cfg, opts...)
	defer rec.Attach(b)()
	if n, _ := rec.Requeue(ctx, b); n != 0 {
		t.Errorf("Requeue handed back %d inputs after the restart, want none", n)
	}
	verifyAll(t, s)
}

// TestARemoteDeliveryAtAnApprovalPromptIsWrittenFirst pins #244 over
// the wire: a front serving the recorder's Control writes a delivery's
// queued entry before the agent takes it, as Recorder.Deliver does, so
// one a remote party makes while the agent waits on an approval is on
// the record before any run takes it, and survives a quit.
func TestARemoteDeliveryAtAnApprovalPromptIsWrittenFirst(t *testing.T) {
	ctx := context.Background()
	rec, s, err := Start(ctx, agentsession.NewMemoryStore(), agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	hold := func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
		return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "ask"}, nil
	}
	tools := []agenttool.Tool{agenttool.New("deploy", "", func(context.Context, echoArgs) (string, error) { return "deployed", nil })}
	a := agentturn.New(agentturn.Config{Model: allCalls{}, ModelName: "m", MaxTurns: 1, Tools: tools, BeforeToolCall: hold})
	defer rec.Attach(a)()
	if end, err := a.Prompt(ctx, openresponses.UserText("deploy")); err != nil || end.Reason != agentturn.ReasonInputRequired {
		t.Fatalf("first run: %v, %+v", err, end)
	}
	srv := httptest.NewServer(control.Handler(rec.Control(a), control.WithInsecureNoAuth()))
	defer srv.Close()
	c, err := control.Dial(ctx, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Deliver(ctx, openresponses.UserText("the build passed")); !errors.Is(err, agentturn.ErrInputRequired) {
		t.Errorf("remote delivery at the prompt: err = %v, want ErrInputRequired", err)
	}
	if d := deliveries(s)["the build passed"]; d == nil || !d.Deliver {
		t.Errorf("the delivery's entry = %+v, want it written with deliver before any run", d)
	}
}

// queuedCount counts the queued entries of s holding a message with
// text.
func queuedCount(s *agentsession.Session, text string) int {
	n := 0
	for _, e := range s.Entries() {
		if q, ok := e.(*agentsession.QueuedEntry); ok {
			if m, ok := q.Item.(*openresponses.Message); ok && m.Text() == text {
				n++
			}
		}
	}
	return n
}

// TestARedeliveredSteerIsWrittenFirst pins the integration review of
// #287 × #244: Recorder.Redeliver, which front/a2a's steer goes
// through, writes the steer's queued entry, with deliver, before the
// agent holds it, so it is on the record across a quit; and one the
// path already owes, handed over again by a later task's recorder, is
// taken under its entry rather than written twice.
func TestARedeliveredSteerIsWrittenFirst(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := agentturn.Config{Model: allCalls{}, ModelName: "m", MaxTurns: 1}
	a := agentturn.New(cfg)
	unsub := rec.Attach(a)
	if _, err := a.Prompt(ctx, openresponses.UserText("hello")); err != nil {
		t.Fatal(err)
	}
	steer := openresponses.UserText("also check the cache")
	if err := rec.Redeliver(ctx, a, steer); err != nil {
		t.Fatal(err)
	}
	if d := deliveries(s)["also check the cache"]; d == nil || !d.Deliver {
		t.Fatalf("the steer's entry = %+v, want it written with deliver before the agent took it", d)
	}
	if st := a.State(); st.Steering != 1 {
		t.Fatalf("the agent holds %d steers", st.Steering)
	}
	// The process quits before a run takes it.
	unsub()

	rec, s, err = Resume(ctx, store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	opts, err := AgentOptions(s)
	if err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(cfg, opts...)
	defer rec.Attach(b)()
	// The front hands its owed steer over again, a copy of it.
	if err := rec.Redeliver(ctx, b, openresponses.UserText("also check the cache")); err != nil {
		t.Fatal(err)
	}
	var arrived []agentturn.TurnInput
	b.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		if e, ok := ev.(*agentturn.TurnStart); ok && e.Turn == 1 {
			arrived = e.Arrived
		}
		return nil
	})
	if _, err := b.Continue(ctx); err != nil {
		t.Fatal(err)
	}
	if len(arrived) != 1 || arrived[0].Mode != agentturn.InputDeliver {
		t.Errorf("the first turn's inputs = %+v, want the steer as a delivery", arrived)
	}
	s, err = store.Open(ctx, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	if n := queuedCount(s, "also check the cache"); n != 1 {
		t.Errorf("the steer has %d queued entries, want the one written first", n)
	}
	verifyAll(t, s)
}

// TestARequeuedSteerIsNotRedeliveredTwice pins the round-two review of
// Recorder.Redeliver: an owed steer the host has already handed back
// with Requeue, which the agent holds, is not written or queued again
// when the front hands the same steer over; the agent holds it once.
func TestARequeuedSteerIsNotRedeliveredTwice(t *testing.T) {
	ctx := agentturn.ContextWithTrigger(context.Background(), agentturn.Trigger{Kind: "a2a", Ref: "alice", Extra: map[string]any{"message": "m1"}})
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := agentturn.Config{Model: allCalls{}, ModelName: "m", MaxTurns: 1}
	a := agentturn.New(cfg)
	unsub := rec.Attach(a)
	if _, err := a.Prompt(context.Background(), openresponses.UserText("hello")); err != nil {
		t.Fatal(err)
	}
	if err := rec.Redeliver(ctx, a, openresponses.UserText("also check the cache")); err != nil {
		t.Fatal(err)
	}
	unsub()

	rec, s, err = Resume(context.Background(), store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	opts, err := AgentOptions(s)
	if err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(cfg, opts...)
	defer rec.Attach(b)()
	if n, _ := rec.Requeue(context.Background(), b); n != 1 {
		t.Fatalf("requeued %d, want the steer", n)
	}
	if err := rec.Redeliver(ctx, b, openresponses.UserText("also check the cache")); err != nil {
		t.Fatal(err)
	}
	if st := b.State(); st.Steering != 1 {
		t.Errorf("the agent holds %d steers, want the one Requeue handed back", st.Steering)
	}
	if _, err := b.Continue(context.Background()); err != nil {
		t.Fatal(err)
	}
	s, err = store.Open(context.Background(), s.ID())
	if err != nil {
		t.Fatal(err)
	}
	if n := queuedCount(s, "also check the cache"); n != 1 {
		t.Errorf("the steer has %d queued entries, want one", n)
	}
	verifyAll(t, s)
}

// TestARequeuedSteerIsSkippedOnlyForItsHolder pins the round-three
// review of Recorder.Redeliver: the skip for an owed steer Requeue
// handed back holds only for the agent it was handed to, so a host
// that reuses one recorder across tasks hands the steer to another
// task's agent rather than skip it.
func TestARequeuedSteerIsSkippedOnlyForItsHolder(t *testing.T) {
	ctx := agentturn.ContextWithTrigger(context.Background(), agentturn.Trigger{Kind: "a2a", Ref: "alice", Extra: map[string]any{"message": "m1"}})
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := agentturn.Config{Model: allCalls{}, ModelName: "m", MaxTurns: 1}
	a := agentturn.New(cfg)
	unsub := rec.Attach(a)
	if _, err := a.Prompt(context.Background(), openresponses.UserText("hello")); err != nil {
		t.Fatal(err)
	}
	if err := rec.Redeliver(ctx, a, openresponses.UserText("also check the cache")); err != nil {
		t.Fatal(err)
	}
	unsub()

	rec, s, err = Resume(context.Background(), store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	opts, err := AgentOptions(s)
	if err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(cfg, opts...)
	if n, _ := rec.Requeue(context.Background(), b); n != 1 {
		t.Fatalf("requeued %d, want the steer", n)
	}
	c := agentturn.New(cfg, opts...)
	if err := rec.Redeliver(ctx, c, openresponses.UserText("also check the cache")); err != nil {
		t.Fatal(err)
	}
	if st := c.State(); st.Steering != 1 {
		t.Errorf("the other agent holds %d steers, want the steer handed to it", st.Steering)
	}
	if st := b.State(); st.Steering != 1 {
		t.Errorf("the agent Requeue handed it to holds %d steers, want 1", st.Steering)
	}
}

// TestAnAcceptedDeliveryIsWrittenFirst pins #302 beside #244: the
// recorder's two-step delivery writes each item's queued entry, with
// deliver, before the agent accepts it, so the accept a caller takes
// at once is on the record, at an approval prompt and over the wire
// alike; and one the agent refuses leaves nothing owed.
func TestAnAcceptedDeliveryIsWrittenFirst(t *testing.T) {
	setup := func(t *testing.T) (*Recorder, *agentsession.Session, *agentturn.Agent) {
		t.Helper()
		ctx := context.Background()
		rec, s, err := Start(ctx, agentsession.NewMemoryStore(), agentsession.Header{})
		if err != nil {
			t.Fatal(err)
		}
		hold := func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "ask"}, nil
		}
		tools := []agenttool.Tool{agenttool.New("deploy", "", func(context.Context, echoArgs) (string, error) { return "deployed", nil })}
		a := agentturn.New(agentturn.Config{Model: allCalls{}, ModelName: "m", MaxTurns: 1, Tools: tools, BeforeToolCall: hold})
		t.Cleanup(rec.Attach(a))
		if end, err := a.Prompt(ctx, openresponses.UserText("deploy")); err != nil || end.Reason != agentturn.ReasonInputRequired {
			t.Fatalf("first run: %v, %+v", err, end)
		}
		return rec, s, a
	}
	for _, via := range []string{"recorder", "wire"} {
		t.Run(via, func(t *testing.T) {
			ctx := context.Background()
			rec, s, a := setup(t)
			var acc agentturn.Accepter = rec.Control(a)
			if via == "wire" {
				srv := httptest.NewServer(control.Handler(rec.Control(a), control.WithInsecureNoAuth()))
				defer srv.Close()
				c, err := control.Dial(ctx, srv.URL)
				if err != nil {
					t.Fatal(err)
				}
				acc = c
			}
			d, err := acc.Accept(ctx, openresponses.UserText("the build passed"))
			if err != nil {
				t.Fatal(err)
			}
			if q := deliveries(s)["the build passed"]; q == nil || !q.Deliver {
				t.Errorf("the delivery's entry = %+v, want it written with deliver when the accept returns", q)
			}
			if _, _, err := d.Wait(ctx); !errors.Is(err, agentturn.ErrInputRequired) {
				t.Errorf("the outcome: err = %v, want ErrInputRequired", err)
			}
		})
	}
	t.Run("refused while held", func(t *testing.T) {
		ctx := context.Background()
		rec, s, a := setup(t)
		if err := a.WhileIdle(func() error {
			if _, err := rec.Accept(ctx, a, openresponses.UserText("the build passed")); !errors.Is(err, agentturn.ErrHeld) {
				t.Errorf("an accept during the hold: err = %v, want ErrHeld", err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if n := queuedCount(s, "the build passed"); n != 0 {
			t.Errorf("%d queued entries for a refused accept, want none", n)
		}
	})
}
