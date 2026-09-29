package agentturn

import (
	"context"
	"errors"
	"fmt"
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

// TestDeliverReachesARunThatIsEnding pins #148: a delivered item joins
// the run in flight only when a model call of that run then sees it. An
// item delivered in the final turn_end joins; one delivered during
// run_end, when State still reads Running and the run takes no more
// steers, starts a run of its own rather than waiting for the next
// prompt, as one delivered to an idle agent does; and one delivered
// during a turn that then stops, on its turn budget or a stop hook,
// stays queued past that stop and starts a run too, rather than being
// appended unanswered. Each delivers from a goroutine, as a detached
// child's end does, and holds the event or the tool until the item is
// queued, so the window is hit every time.
func TestDeliverReachesARunThatIsEnding(t *testing.T) {
	type outcome struct {
		joined bool
		end    *RunEnd
		err    error
	}
	for _, tc := range []struct {
		name string
		// at is the event the item is delivered during, "tool" for
		// inside the turn's tool call and "" for once the agent is idle.
		at         string
		tool       bool
		maxTurns   int
		stopHook   bool
		wantJoined bool
		wantRuns   int
		// wantQueuedRun says the queued report names the first run.
		wantQueuedRun bool
	}{
		{name: "final turn_end", at: EventTurnEnd, wantJoined: true, wantRuns: 1, wantQueuedRun: true},
		{name: "run_end", at: EventRunEnd, wantRuns: 2},
		{name: "idle", wantRuns: 2},
		{name: "a turn the budget stops", at: "tool", tool: true, maxTurns: 1, wantRuns: 2, wantQueuedRun: true},
		{name: "a turn a hook stops", at: "tool", tool: true, stopHook: true, wantRuns: 2, wantQueuedRun: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delivered := openresponses.UserText("the task is done")
			result := make(chan outcome, 1)
			var a *Agent
			var once sync.Once
			// hold delivers from a goroutine and waits until the item is
			// queued, from inside an event or a tool.
			hold := func() {
				once.Do(func() {
					go func() {
						joined, end, err := a.Deliver(context.Background(), delivered)
						result <- outcome{joined, end, err}
					}()
					for a.State().Steering == 0 {
						runtime.Gosched()
					}
				})
			}
			cfg := Config{Model: &echo.Adapter{}, MaxTurns: tc.maxTurns}
			if tc.tool {
				cfg.Tools = []agenttool.Tool{agenttool.New("work", "", func(context.Context, echoArgs) (string, error) {
					hold()
					return "started", nil
				})}
			}
			if tc.stopHook {
				cfg.ShouldStopAfterTurn = func(context.Context, TurnInfo) (bool, error) { return true, nil }
			}
			a = New(cfg)
			var runs []*RunEnd
			var queued []*Queued
			a.Subscribe(func(_ context.Context, ev Event) error {
				switch e := ev.(type) {
				case *RunEnd:
					runs = append(runs, e)
				case *Queued:
					queued = append(queued, e)
				}
				if ev.EventType() == tc.at {
					hold()
					if tc.at == EventRunEnd && !a.State().Running {
						t.Error("the run reads idle during its run_end")
					}
				}
				return nil
			})
			first, err := a.Prompt(context.Background(), openresponses.UserText("start the task"))
			if err != nil {
				t.Fatal(err)
			}
			if tc.at == "" {
				// An idle agent's run takes the item at once, so there is
				// no window to hold: deliver and wait for the run.
				joined, end, err := a.Deliver(context.Background(), delivered)
				result <- outcome{joined, end, err}
			}
			got := <-result
			if got.err != nil || got.joined != tc.wantJoined || (got.end == nil) != tc.wantJoined {
				t.Fatalf("Deliver = %+v", got)
			}
			if err := a.WaitForIdle(context.Background()); err != nil {
				t.Fatal(err)
			}
			st := a.State()
			if len(runs) != tc.wantRuns || st.Steering != 0 {
				t.Errorf("runs %d, still steering %d", len(runs), st.Steering)
			}
			// The model answered the item: something it produced follows.
			at := -1
			for i, item := range st.Transcript {
				if item == openresponses.Item(delivered) {
					at = i
				}
			}
			if at < 0 || at == len(st.Transcript)-1 {
				t.Errorf("the model did not answer the delivered item (at %d of %d)", at, len(st.Transcript))
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

// TestDeliverToARunThatStopsAfterTakingIt checks the stop that cannot be
// decided before the drain: a guard before the next model call. The run
// took the item and ended without a model call seeing it, and Deliver
// says so with that run's end rather than reporting that it joined.
func TestDeliverToARunThatStopsAfterTakingIt(t *testing.T) {
	spent := fmt.Errorf("%w: cost limit reached", ErrGuard)
	result := make(chan error, 1)
	var a *Agent
	var endOf *RunEnd
	work := agenttool.New("work", "", func(context.Context, echoArgs) (string, error) {
		go func() {
			joined, end, err := a.Deliver(context.Background(), openresponses.UserText("the task is done"))
			endOf = end
			if err == nil && joined {
				err = errors.New("joined a run whose model never saw the item")
			}
			result <- err
		}()
		for a.State().Steering == 0 {
			runtime.Gosched()
		}
		return "started", nil
	})
	a = New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{work},
		BeforeTurn: func(_ context.Context, info TurnStartInfo) (openresponses.Items, error) {
			if info.Turn > 1 {
				return nil, spent
			}
			return nil, nil
		}})
	first, err := a.Prompt(context.Background(), openresponses.UserText("start the task"))
	if err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if endOf == nil || endOf.RunID != first.RunID || endOf.Cause != StopGuard {
		t.Errorf("Deliver's end = %+v, want the guarded run %s", endOf, first.RunID)
	}
}
