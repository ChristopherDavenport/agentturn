package agentturn

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestQueuedReportsWhatWasAccepted is the gateway's case: an item is
// accepted from a sender that is answered 202 and appended later, and a
// host writing what it accepted needs to see it as an accept rather
// than as an item a run produced.
func TestQueuedReportsWhatWasAccepted(t *testing.T) {
	a := New(Config{Model: &echo.Adapter{}, MaxTurns: 1})
	var queued []*Queued
	var order []string
	a.Subscribe(func(_ context.Context, ev Event) error {
		if e, ok := ev.(*Queued); ok {
			queued = append(queued, e)
		}
		order = append(order, ev.EventType())
		return nil
	})
	// Accepted while the agent is idle: the report follows at the
	// start of the next run, before anything that run does.
	a.FollowUp(openresponses.UserText("and then this"))
	if len(queued) != 0 {
		t.Fatalf("an idle agent delivered %d events with no goroutine to deliver them", len(queued))
	}
	if st := a.State(); len(st.Queued) != 1 {
		t.Fatalf("the item was not queued: %+v", st.Queued)
	}
	end, err := a.Prompt(context.Background(), openresponses.UserText("go"))
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 1 {
		t.Fatalf("queued events = %d", len(queued))
	}
	if e := queued[0]; e.Mode != QueueFollowUp || e.RunID != "" || e.Hidden {
		t.Errorf("event = %+v", e)
	}
	if m, ok := queued[0].Item.(*openresponses.Message); !ok || m.Text() != "and then this" {
		t.Errorf("item = %v", queued[0].Item)
	}
	if len(order) == 0 || order[0] != EventQueued {
		t.Errorf("the accept was reported after the run began: %v", order)
	}
	if end.Reason == ReasonError {
		t.Fatalf("end = %+v", end)
	}

	// Accepted while a run is in flight: the event names that run, and
	// the item it carries is the item itself.
	notice := openresponses.DeveloperText("<notice>be brief</notice>")
	b := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)}, MaxTurns: 1})
	var inFlight []*Queued
	b.Subscribe(func(_ context.Context, ev Event) error {
		switch e := ev.(type) {
		case *ToolStart:
			b.Steer(Hidden(notice))
		case *Queued:
			inFlight = append(inFlight, e)
		}
		return nil
	})
	end, err = b.Prompt(context.Background(), openresponses.UserText("go"))
	if err != nil {
		t.Fatal(err)
	}
	if len(inFlight) != 1 {
		t.Fatalf("queued events = %d", len(inFlight))
	}
	if e := inFlight[0]; e.Mode != QueueSteer || e.RunID != end.RunID || !e.Hidden {
		t.Errorf("event = %+v, run %q", e, end.RunID)
	}
	if m, ok := inFlight[0].Item.(*openresponses.Message); !ok || m.Text() != notice.Text() {
		t.Errorf("item = %v", inFlight[0].Item)
	}
}

// TestSteerFromASubscriberDoesNotBlock is the pattern the docs allow
// with care: a subscriber that steers on an event it can tell apart
// from its own items. Accepting an item never waits on delivery, so it
// returns and the report follows on the next event.
func TestSteerFromASubscriberDoesNotBlock(t *testing.T) {
	a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)}, MaxTurns: 2})
	steered := false
	var order []string
	a.Subscribe(func(_ context.Context, ev Event) error {
		order = append(order, ev.EventType())
		if _, ok := ev.(*ToolEnd); ok && !steered {
			steered = true
			a.Steer(openresponses.UserText("and the duplicates"))
			// The item is queued by the time this returns, and the
			// report has not been delivered yet.
			if n := a.State().Steering; n != 1 {
				t.Errorf("steering = %d", n)
			}
		}
		return nil
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
			t.Error(err)
		}
	}()
	<-done
	if !steered {
		t.Fatal("the subscriber never steered")
	}
	// The accept was reported before the item reached the transcript.
	queued, item := -1, -1
	for i, typ := range order {
		if typ == EventQueued && queued < 0 {
			queued = i
		}
	}
	for _, it := range a.State().Transcript {
		if m, ok := it.(*openresponses.Message); ok && m.Text() == "and the duplicates" {
			item = 1
		}
	}
	if queued < 0 || item < 0 {
		t.Errorf("queued at %d, steered item in the transcript: %v (%v)", queued, item >= 0, order)
	}
}

// TestQueuedIsDeliveredUnderTheSameBarrier steers a running agent from
// another goroutine while its tool reports progress. A subscriber is
// documented as never being entered twice at once, so a front may keep
// state without a lock of its own; the slice here is unguarded on
// purpose, and under -race it is the assertion.
func TestQueuedIsDeliveredUnderTheSameBarrier(t *testing.T) {
	const steps = 40
	started := make(chan struct{})
	accepted := make(chan struct{})
	var once sync.Once
	reporting := agenttool.New("work", "works", func(ctx context.Context, _ echoArgs) (string, error) {
		once.Do(func() { close(started) })
		for i := 0; i < steps; i++ {
			agenttool.Progress(ctx, agenttool.Text("working"))
		}
		// The call ends once everything has been accepted, so the run
		// is still there to report all of it.
		<-accepted
		return "done", nil
	})
	a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{reporting}, MaxTurns: 1})
	var seen []string
	a.Subscribe(func(_ context.Context, ev Event) error {
		seen = append(seen, ev.EventType())
		return nil
	})
	go func() {
		defer close(accepted)
		<-started
		for i := 0; i < steps; i++ {
			a.Steer(openresponses.UserText("more"))
		}
	}()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	queued := 0
	for _, typ := range seen {
		if typ == EventQueued {
			queued++
		}
	}
	if queued != steps {
		t.Errorf("queued events = %d, want %d in %s", queued, steps, strings.Join(seen[:min(len(seen), 8)], " "))
	}
}

// TestDeliverReachesARunThatIsEnding pins #148: an item delivered while
// a run is in its final turn_end joins it, one delivered during its
// run_end, when State still reads Running and the run takes no more
// steers, starts a run of its own rather than waiting for the next
// prompt, and one delivered to an idle agent starts a run too. The
// run_end case delivers from a goroutine, as a detached child's end
// does, and holds the subscriber until the item is queued, so the
// window is hit every time.
func TestDeliverReachesARunThatIsEnding(t *testing.T) {
	type outcome struct {
		joined bool
		end    *RunEnd
		err    error
	}
	for _, tc := range []struct {
		name       string
		at         string
		wantJoined bool
		wantCalls  int
		wantRuns   int
		// wantQueuedRun says the queued report names the run in flight.
		wantQueuedRun bool
	}{
		{name: "final turn_end", at: EventTurnEnd, wantJoined: true, wantCalls: 2, wantRuns: 1, wantQueuedRun: true},
		{name: "run_end", at: EventRunEnd, wantCalls: 2, wantRuns: 2},
		{name: "idle", wantCalls: 2, wantRuns: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			model := &counting{}
			a := New(Config{Model: model})
			result := make(chan outcome, 1)
			deliver := func() {
				joined, end, err := a.Deliver(context.Background(), openresponses.UserText("the task is done"))
				result <- outcome{joined, end, err}
			}
			var once sync.Once
			var runs []string
			var queued []*Queued
			a.Subscribe(func(_ context.Context, ev Event) error {
				switch e := ev.(type) {
				case *RunEnd:
					runs = append(runs, e.RunID)
				case *Queued:
					queued = append(queued, e)
				}
				if ev.EventType() != tc.at {
					return nil
				}
				once.Do(func() {
					if tc.at == EventTurnEnd {
						// Joining does not wait, so a subscriber may.
						deliver()
						return
					}
					go deliver()
					for a.State().Steering == 0 {
						runtime.Gosched()
					}
					if !a.State().Running {
						t.Error("the run reads idle during its run_end")
					}
				})
				return nil
			})
			first, err := a.Prompt(context.Background(), openresponses.UserText("start the task"))
			if err != nil {
				t.Fatal(err)
			}
			if tc.at == "" {
				deliver()
			}
			got := <-result
			if got.err != nil || got.joined != tc.wantJoined || (got.end == nil) != tc.wantJoined {
				t.Fatalf("Deliver = %+v", got)
			}
			if err := a.WaitForIdle(context.Background()); err != nil {
				t.Fatal(err)
			}
			st := a.State()
			if model.calls != tc.wantCalls || len(runs) != tc.wantRuns || st.Steering != 0 {
				t.Errorf("model calls %d, runs %d, still steering %d", model.calls, len(runs), st.Steering)
			}
			if last, ok := st.Transcript[len(st.Transcript)-1].(*openresponses.Message); !ok || !strings.Contains(last.Text(), "the task is done") {
				t.Errorf("the model did not answer the delivered item: %+v", st.Transcript)
			}
			wantRun := ""
			if tc.wantQueuedRun {
				wantRun = first.RunID
			}
			if len(queued) != 1 || queued[0].RunID != wantRun {
				t.Errorf("queued = %+v, want run %q", queued, wantRun)
			}
		})
	}
}

// TestDeliverWhenNoRunCanStart checks that an item Deliver cannot start
// a run for stays queued, with the reason.
func TestDeliverWhenNoRunCanStart(t *testing.T) {
	call := &openresponses.FunctionCall{CallID: "c1", Name: "upper", Arguments: `{}`}
	for _, tc := range []struct {
		name    string
		agent   *Agent
		items   openresponses.Items
		want    error
		steered int
	}{
		{"pending calls", New(Config{Model: &echo.Adapter{}}, WithTranscript(Transcript{openresponses.UserText("go"), call})), openresponses.Items{openresponses.UserText("done")}, ErrInputRequired, 1},
		{"nothing a model answers", New(Config{Model: &echo.Adapter{}}), openresponses.Items{openresponses.AssistantText("done")}, ErrCannotContinue, 1},
		{"no items", New(Config{Model: &echo.Adapter{}}), nil, ErrNoPrompt, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			joined, end, err := tc.agent.Deliver(context.Background(), tc.items...)
			if !errors.Is(err, tc.want) || joined || end != nil {
				t.Errorf("Deliver = %v %v %v, want %v", joined, end, err, tc.want)
			}
			if st := tc.agent.State(); st.Steering != tc.steered || st.Running {
				t.Errorf("state = %+v", st)
			}
		})
	}
}
