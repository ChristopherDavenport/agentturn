package session

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
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
		if n := rec.Requeue(ctx, b); n != 1 {
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
