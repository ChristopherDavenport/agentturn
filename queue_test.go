package agentturn

import (
	"context"
	"errors"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestQueuedReportsWhatWasAccepted is the gateway's case: an item is
// accepted from a sender that is answered 202 and appended later, and
// the host has to be able to keep it in between. The event carries what
// it needs to: the item, which queue, and what the caller said caused
// it.
func TestQueuedReportsWhatWasAccepted(t *testing.T) {
	blocking := agenttool.New("wait", "", func(ctx context.Context, _ echoArgs) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{blocking}})
	var queued []*Queued
	a.Subscribe(func(_ context.Context, ev Event) error {
		if e, ok := ev.(*Queued); ok {
			queued = append(queued, e)
		}
		return nil
	})
	// Accepted while the agent is idle.
	ctx := ContextWithTrigger(context.Background(), Trigger{Kind: "slack", Ref: "C123"})
	if err := a.FollowUp(ctx, openresponses.UserText("and then this")); err != nil {
		t.Fatal(err)
	}
	if len(queued) != 1 {
		t.Fatalf("queued events = %d", len(queued))
	}
	if e := queued[0]; e.Mode != QueueFollowUp || e.RunID != "" || e.Trigger.String() != "slack:C123" {
		t.Errorf("event = %+v", e)
	}
	if m, ok := queued[0].Item.(*openresponses.Message); !ok || m.Text() != "and then this" {
		t.Errorf("item = %v", queued[0].Item)
	}
	// Accepted while a run is in flight: the event names the run.
	started := make(chan struct{})
	a.Subscribe(func(_ context.Context, ev Event) error {
		if _, ok := ev.(*ToolStart); ok {
			close(started)
		}
		return nil
	})
	go func() {
		<-started
		_ = a.Steer(context.Background(), Hidden(openresponses.DeveloperText("<notice>be brief</notice>")))
		a.Abort()
	}()
	end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
	if end == nil || end.Reason != ReasonAborted {
		t.Fatalf("end = %+v", end)
	}
	if len(queued) != 2 {
		t.Fatalf("queued events = %d", len(queued))
	}
	if e := queued[1]; e.Mode != QueueSteer || e.RunID != end.RunID || !e.Hidden {
		t.Errorf("event = %+v, run %q", e, end.RunID)
	}
	// The queues still hold what was accepted, for a host that persists
	// them itself.
	st := a.State()
	if len(st.Steered) != 1 || len(st.Queued) != 1 {
		t.Errorf("state = %d steered, %d queued", len(st.Steered), len(st.Queued))
	}
}

// TestQueuedSubscriberRefusesTheItem checks that a host that cannot
// make an accepted item durable can refuse the accept.
func TestQueuedSubscriberRefusesTheItem(t *testing.T) {
	full := errors.New("inbox full")
	a := New(Config{Model: &echo.Adapter{}})
	a.Subscribe(func(_ context.Context, ev Event) error {
		if _, ok := ev.(*Queued); ok {
			return full
		}
		return nil
	})
	err := a.Steer(context.Background(), openresponses.UserText("one"), openresponses.UserText("two"))
	if !errors.Is(err, full) {
		t.Fatalf("err = %v", err)
	}
	if st := a.State(); len(st.Steered) != 0 || st.Steering != 0 {
		t.Errorf("a refused item was queued: %+v", st.Steered)
	}
}
