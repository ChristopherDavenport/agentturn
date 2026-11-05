package agentturn

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// TestDeliverFromInsideTheRun checks that Deliver called with the
// context of the run's own tool, of a child run one of its tools made,
// or of a subscriber, which the run waits on, does not wait for the
// run: it joins a run that will still drain the items, which the model
// then answers in that run, and reports ErrRunning from a run past its
// last drain, the items left for the next run.
func TestDeliverFromInsideTheRun(t *testing.T) {
	type outcome struct {
		joined bool
		err    error
	}
	for _, tc := range []struct {
		name       string
		from       string
		wantJoined bool
		wantErr    error
		wantRuns   int
	}{
		{"the run's tool", "tool", true, nil, 1},
		{"a child run's tool", "child", true, nil, 1},
		{"a subscriber in run_end", "run_end", false, ErrRunning, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delivered := openresponses.UserText("the task is done")
			var a *Agent
			var once sync.Once
			var got outcome
			deliver := func(ctx context.Context) {
				once.Do(func() {
					joined, _, err := a.Deliver(ctx, delivered)
					got = outcome{joined, err}
				})
			}
			var tools []agenttool.Tool
			switch tc.from {
			case "tool":
				tools = []agenttool.Tool{agenttool.New("work", "", func(ctx context.Context, _ echoArgs) (string, error) {
					deliver(ctx)
					return "started", nil
				})}
			case "child":
				child := New(Config{Model: &echo.Adapter{}, MaxTurns: 1, Tools: []agenttool.Tool{agenttool.New("inner", "", func(ctx context.Context, _ echoArgs) (string, error) {
					deliver(ctx)
					return "started", nil
				})}})
				tools = []agenttool.Tool{agenttool.New("work", "", func(ctx context.Context, _ echoArgs) (string, error) {
					_, err := child.Prompt(ctx, openresponses.UserText("go"))
					return "started", err
				})}
			}
			a = New(Config{Model: &echo.Adapter{}, Tools: tools})
			runs := 0
			a.Subscribe(func(ctx context.Context, ev Event) error {
				if _, ok := ev.(*RunEnd); ok {
					runs++
					if tc.from == "run_end" {
						deliver(ctx)
					}
				}
				return nil
			})
			done := make(chan error, 1)
			go func() {
				_, err := a.Prompt(context.Background(), openresponses.UserText("start the task"))
				done <- err
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Deliver from inside the run waited on the run")
			}
			if got.joined != tc.wantJoined || !errors.Is(got.err, tc.wantErr) {
				t.Fatalf("Deliver = %+v", got)
			}
			st := a.State()
			if runs != tc.wantRuns {
				t.Errorf("runs = %d", runs)
			}
			at := -1
			for i, item := range st.Transcript {
				if item == openresponses.Item(delivered) {
					at = i
				}
			}
			if tc.wantJoined && (at < 0 || at == len(st.Transcript)-1) {
				t.Errorf("the run did not answer the delivered item (at %d of %d)", at, len(st.Transcript))
			}
			if !tc.wantJoined && (at >= 0 || st.Steering != 1) {
				t.Errorf("the item was not left for the next run: at %d, steering %d", at, st.Steering)
			}
		})
	}
}

// transferring calls transfer_to_b when its last input is the user
// saying "go", and otherwise answers; it keeps the user texts each
// request carried.
type transferring struct {
	mu   sync.Mutex
	seen []string
}

func (m *transferring) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	var texts []string
	for _, item := range req.Input {
		if msg, ok := item.(*openresponses.Message); ok && msg.Role == openresponses.RoleUser {
			texts = append(texts, msg.Text())
		}
	}
	m.mu.Lock()
	m.seen = append(m.seen, strings.Join(texts, "|"))
	m.mu.Unlock()
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if msg, ok := req.Input[len(req.Input)-1].(*openresponses.Message); ok && msg.Text() == "go" {
		w, err := em.FunctionCall("", "transfer_to_b")
		if err != nil {
			return err
		}
		if err := w.Arguments(`{}`); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		return em.Complete()
	}
	if err := em.Item(openresponses.AssistantText("ok")); err != nil {
		return err
	}
	return em.Complete()
}

func (m *transferring) saw(text string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.seen {
		if strings.Contains(s, text) {
			return true
		}
	}
	return false
}

// TestADeliveryAfterATransferWaitsForTheReceiver pins the review of
// #244 × #274: a delivery that finds the last run ended on a
// terminating result, a handoff's transfer, starts no run under the
// sender's configuration, which would race the host's switch to the
// receiver; it stays queued, Deliver says so with ErrTerminated and
// that run's end, and the receiver's run the host starts takes it.
// That holds for a delivery made while the run ends and for one made
// once the agent is idle.
func TestADeliveryAfterATransferWaitsForTheReceiver(t *testing.T) {
	for _, at := range []string{EventRunEnd, "idle"} {
		t.Run(at, func(t *testing.T) {
			delivered := "the background task is done"
			transfer := agenttool.NewFunc("transfer_to_b", "hands off to b", []byte(`{"type":"object"}`), func(context.Context, agenttool.Call) (agenttool.Result, error) {
				return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: `{"assistant":"b"}`}, Terminate: true}, nil
			})
			sender, receiver := &transferring{}, &transferring{}
			a := New(Config{Name: "a", Model: sender, ModelName: "m", Tools: []agenttool.Tool{transfer}})
			type outcome struct {
				end *RunEnd
				err error
			}
			result := make(chan outcome, 1)
			deliver := func() {
				_, end, err := a.Deliver(context.Background(), openresponses.UserText(delivered))
				result <- outcome{end, err}
			}
			var once sync.Once
			a.Subscribe(func(_ context.Context, ev Event) error {
				if ev.EventType() == at {
					once.Do(func() {
						go deliver()
						for a.State().Steering == 0 {
							runtime.Gosched()
						}
					})
				}
				return nil
			})
			end, err := a.Prompt(context.Background(), openresponses.UserText("go"))
			if err != nil || end.Cause != StopTerminate {
				t.Fatalf("sender's run = %+v, %v", end, err)
			}
			if at == "idle" {
				deliver()
			}
			var got outcome
			select {
			case got = <-result:
			case <-time.After(5 * time.Second):
				t.Fatal("Deliver never returned")
			}
			if !errors.Is(got.err, ErrTerminated) || got.end == nil || got.end.RunID != end.RunID {
				t.Errorf("Deliver = %+v, want ErrTerminated with the sender's run's end", got)
			}
			// The host follows the transfer: the receiver's run takes it.
			if err := a.SetConfig(Config{Name: "b", Model: receiver, ModelName: "m"}); err != nil {
				t.Fatalf("the switch to the receiver: %v", err)
			}
			if _, err := a.Continue(ContextWithTrigger(context.Background(), Trigger{Kind: "handoff", Ref: "a"})); err != nil {
				t.Fatal(err)
			}
			if sender.saw(delivered) {
				t.Error("the sender's configuration answered the delivery")
			}
			if !receiver.saw(delivered) {
				t.Errorf("the receiver never saw the delivery: %q", receiver.seen)
			}
		})
	}
}

// heldTool is a tool that says when it runs and returns once released,
// a quiet call that raises no event while it waits.
func heldTool(running chan<- struct{}, release <-chan struct{}) agenttool.Tool {
	return agenttool.New("wait", "waits", func(ctx context.Context, _ echoArgs) (string, error) {
		running <- struct{}{}
		select {
		case <-release:
			return "released", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
}

// TestADeliveryIsAcceptedBeforeItsOutcome pins #302: accepting a
// delivery returns once its items are queued, and its outcome follows
// on the handle, so a view forwarding two steers typed while a quiet
// tool runs answers both before the tool ends. The agent settles both:
// they join the run in flight in the order given.
func TestADeliveryIsAcceptedBeforeItsOutcome(t *testing.T) {
	running, release := make(chan struct{}, 1), make(chan struct{})
	a := New(Config{Model: &echo.Adapter{}, MaxTurns: 3, Tools: []agenttool.Tool{heldTool(running, release)}})
	done := make(chan *RunEnd, 1)
	go func() {
		end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
		done <- end
	}()
	<-running
	var released sync.Once
	defer released.Do(func() { close(release) })
	accepted := make(chan *Delivery, 2)
	go func() {
		for _, text := range []string{"one", "two"} {
			d, err := a.Accept(context.Background(), openresponses.UserText(text))
			if err != nil {
				t.Error(err)
				return
			}
			accepted <- d
		}
	}()
	var ds []*Delivery
	for len(ds) < 2 {
		select {
		case d := <-accepted:
			ds = append(ds, d)
		case <-time.After(2 * time.Second):
			t.Fatalf("%d of two deliveries accepted while the tool ran; Accept waited for the outcome", len(ds))
		}
	}
	if st := a.State(); st.Steering != 2 {
		t.Errorf("steering = %d, want both queued", st.Steering)
	}
	released.Do(func() { close(release) })
	for i, d := range ds {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		joined, end, err := d.Wait(ctx)
		cancel()
		if err != nil || !joined || end != nil {
			t.Errorf("delivery %d: joined %v, end %+v, err %v; want joined", i, joined, end, err)
		}
	}
	<-done
	var texts []string
	for _, item := range a.State().Transcript {
		if m, ok := item.(*openresponses.Message); ok && m.Role == openresponses.RoleUser {
			texts = append(texts, m.Text())
		}
	}
	if want := []string{"go", "one", "two"}; !slices.Equal(texts, want) {
		t.Errorf("user messages = %q, want %q", texts, want)
	}
}

// TestAQueuedReportDoesNotWaitOutAQuietTool pins #302's second half: a
// run in flight reports an accepted item without waiting for its next
// event, so a host showing what it accepted shows a steer typed while a
// tool runs before the tool ends.
func TestAQueuedReportDoesNotWaitOutAQuietTool(t *testing.T) {
	running, release := make(chan struct{}, 1), make(chan struct{})
	a := New(Config{Model: &echo.Adapter{}, MaxTurns: 3, Tools: []agenttool.Tool{heldTool(running, release)}})
	reported := make(chan *Queued, 4)
	a.Subscribe(func(_ context.Context, ev Event) error {
		if e, ok := ev.(*Queued); ok {
			reported <- e
		}
		return nil
	})
	done := make(chan *RunEnd, 1)
	go func() {
		end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
		done <- end
	}()
	<-running
	var released sync.Once
	defer released.Do(func() { close(release) })
	if err := a.Queue(ContextWithTrigger(context.Background(), Trigger{Kind: TriggerHuman}), QueueSteer, openresponses.UserText("and this")); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-reported:
		if e.RunID == "" || e.Trigger.Kind != TriggerHuman {
			t.Errorf("queued = %+v, want the run in flight and the trigger", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the queued report waited for the tool to end")
	}
	released.Do(func() { close(release) })
	end := <-done
	if end == nil || end.Reason != ReasonDone {
		t.Fatalf("end = %+v", end)
	}
	select {
	case e := <-reported:
		t.Errorf("a second report: %+v", e)
	default:
	}
}

// TestAQueuedReportDoesNotSpinASubscriber pins the bound on #302's
// reports between events: a subscriber that queues on every queued
// report gets one report more right after the one it reacted to, and
// the next waits for the run's next event, rather than reports and
// accepts chasing each other while the tool runs.
func TestAQueuedReportDoesNotSpinASubscriber(t *testing.T) {
	running, release := make(chan struct{}, 1), make(chan struct{})
	a := New(Config{Model: &echo.Adapter{}, MaxTurns: 3, Tools: []agenttool.Tool{heldTool(running, release)}})
	var holding atomic.Bool
	holding.Store(true)
	first := make(chan struct{})
	var once sync.Once
	a.Subscribe(func(_ context.Context, ev Event) error {
		if _, ok := ev.(*Queued); ok && holding.Load() {
			once.Do(func() { close(first) })
			a.Steer(openresponses.UserText("again"))
		}
		return nil
	})
	done := make(chan *RunEnd, 1)
	go func() {
		end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
		done <- end
	}()
	<-running
	var released sync.Once
	defer released.Do(func() { close(release) })
	a.Steer(openresponses.UserText("and this"))
	select {
	case <-first:
	case <-time.After(2 * time.Second):
		t.Fatal("the queued report waited for the tool to end")
	}
	time.Sleep(100 * time.Millisecond)
	if n := a.State().Steering; n > 3 {
		t.Errorf("steering = %d while the tool ran, want at most 3: the reports spun", n)
	}
	holding.Store(false)
	released.Do(func() { close(release) })
	if end := <-done; end == nil || end.Reason == ReasonError {
		t.Fatalf("end = %+v", end)
	}
}

// TestOutsideAcceptsDuringAReportAreReported pins the bound on #302's
// reports to what a subscriber itself queues: an outside caller's
// accept that arrives while a queued report is being delivered, a
// second typed steer, is reported before a quiet tool ends, however
// many arrive one after another during the reports before it.
func TestOutsideAcceptsDuringAReportAreReported(t *testing.T) {
	running, release := make(chan struct{}, 1), make(chan struct{})
	a := New(Config{Model: &echo.Adapter{}, MaxTurns: 3, Tools: []agenttool.Tool{heldTool(running, release)}})
	var holding atomic.Bool
	holding.Store(true)
	reported, proceed := make(chan string, 4), make(chan struct{})
	a.Subscribe(func(_ context.Context, ev Event) error {
		e, ok := ev.(*Queued)
		if !ok || !holding.Load() {
			return nil
		}
		m, _ := e.Item.(*openresponses.Message)
		reported <- m.Text()
		// Held inside the report until the test has accepted the next
		// item from outside.
		<-proceed
		return nil
	})
	done := make(chan *RunEnd, 1)
	go func() {
		end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
		done <- end
	}()
	<-running
	var released sync.Once
	defer released.Do(func() { close(release) })
	person := ContextWithTrigger(context.Background(), Trigger{Kind: TriggerHuman})
	steer := func(text string) {
		t.Helper()
		if err := a.Queue(person, QueueSteer, openresponses.UserText(text)); err != nil {
			t.Fatal(err)
		}
	}
	steer("one")
	for _, next := range []string{"two", "three"} {
		select {
		case <-reported:
		case <-time.After(2 * time.Second):
			holding.Store(false)
			close(proceed)
			t.Fatalf("the report before %q waited for the tool to end", next)
		}
		// The report under way is inside the subscriber: this accept
		// cannot take the barrier.
		steer(next)
		proceed <- struct{}{}
	}
	select {
	case text := <-reported:
		if text != "three" {
			t.Errorf("reported %q, want three", text)
		}
	case <-time.After(2 * time.Second):
		holding.Store(false)
		close(proceed)
		t.Fatal("the second outside accept during a report waited for the tool to end")
	}
	holding.Store(false)
	close(proceed)
	released.Do(func() { close(release) })
	if end := <-done; end == nil || end.Reason != ReasonDone {
		t.Fatalf("end = %+v", end)
	}
}
