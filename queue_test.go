package agentturn

import (
	"context"
	"errors"
	"sync"
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

// TestQueuedIsDeliveredUnderTheSameBarrier steers a running agent from
// another goroutine while its tool reports progress, so the queued
// events and the run's own events reach the subscribers at the same
// moment. A subscriber is documented as never being entered twice at
// once, so a front may keep state without a lock of its own; the slice
// here is unguarded on purpose, and under -race it is the assertion.
func TestQueuedIsDeliveredUnderTheSameBarrier(t *testing.T) {
	const steps = 40
	started := make(chan struct{})
	var once sync.Once
	reporting := agenttool.New("work", "works", func(ctx context.Context, _ echoArgs) (string, error) {
		once.Do(func() { close(started) })
		for i := 0; i < steps; i++ {
			agenttool.Progress(ctx, agenttool.Text("working"))
		}
		return "done", nil
	})
	a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{reporting}, MaxTurns: 1})
	var seen []string
	a.Subscribe(func(_ context.Context, ev Event) error {
		seen = append(seen, ev.EventType())
		return nil
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-started
		for i := 0; i < steps; i++ {
			if err := a.Steer(context.Background(), openresponses.UserText("more")); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	<-done
	queued := 0
	for _, typ := range seen {
		if typ == EventQueued {
			queued++
		}
	}
	if queued != steps {
		t.Errorf("queued events = %d, want %d", queued, steps)
	}
	if st := a.State(); len(st.Steered)+len(st.Transcript) == 0 {
		t.Error("nothing was queued or appended")
	}
}
