package agentturn

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ChristopherDavenport/openresponses"
)

// Errors returned by [Agent].
var (
	// ErrRunning is returned when Prompt, Continue, Resume, SetConfig or
	// SetTranscript is called while a run is active.
	ErrRunning = errors.New("agentturn: agent is already running")
	// ErrInputRequired is returned when the last run left calls
	// unanswered, deferred to the caller or cut off by an abort or a
	// failure; [Agent.Resume] with their outputs first. The typed
	// errors of tools/agent and tools/a2a match it with errors.Is, so a
	// host can ask "does any sub-agent need input" once.
	ErrInputRequired = errors.New("agentturn: pending tool calls must be resumed before continuing")
	// ErrNotPending is returned when Resume was given an output for a
	// call that is not pending, left a pending call unanswered, or was
	// called with nothing pending.
	ErrNotPending = errors.New("agentturn: output does not answer a pending call")
)

// Agent is the stateful loop: a transcript, queues, subscribers and run
// control over [Run]. One run at a time; a second Prompt while one is
// active returns [ErrRunning]. The zero Agent has no model and is idle;
// use [New].
//
// Subscribers are called synchronously, in registration order, for every
// event, so every event is a barrier: the loop does not move to the next
// phase until each subscriber has returned. A subscriber that returns an
// error ends the run with ReasonError. The run_end is delivered with a
// context that is not cancelled, even after [Agent.Abort], so a
// subscriber that writes durable state on it can.
type Agent struct {
	cfg Config

	mu         sync.Mutex
	transcript Transcript
	subs       []subscription
	nextSub    int
	steer      openresponses.Items
	followUp   openresponses.Items
	running    bool
	runID      string
	turn       int
	cancel     context.CancelFunc
	idle       chan struct{}
	pending    []*openresponses.FunctionCall
}

type subscription struct {
	id int
	fn func(context.Context, Event) error
}

// Option configures a new Agent.
type Option func(*Agent)

// WithTranscript starts the agent from an existing transcript, as when
// resuming a session. Function calls after the last user message that
// have no output are pending, as they would be after the run that made
// them: Prompt and Continue return [ErrInputRequired] until
// [Agent.Resume] has answered them.
func WithTranscript(t Transcript) Option {
	return func(a *Agent) {
		a.transcript = append(Transcript(nil), t...)
		a.pending = unansweredCalls(a.transcript)
	}
}

// New builds an agent.
func New(cfg Config, opts ...Option) *Agent {
	a := &Agent{cfg: cfg, idle: closedChan()}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// Config returns the agent's configuration.
func (a *Agent) Config() Config { return a.cfg }

// SetConfig replaces the configuration for the next run: model,
// instructions, reasoning, tools, hooks, all of it. It returns
// [ErrRunning] while a run is active. Subscribers and queues are kept;
// a session recorder attached to the agent sees the change as a config
// delta on the next turn.
func (a *Agent) SetConfig(cfg Config) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.running {
		return ErrRunning
	}
	a.cfg = cfg
	return nil
}

// SetTranscript replaces the transcript, as when switching to another
// branch of a session. It returns [ErrRunning] while a run is active.
// The pending calls are derived from the new transcript as
// [WithTranscript] derives them, so whatever the old transcript was
// waiting on is forgotten and whatever the new one is waiting on must
// be answered through [Agent.Resume]. Queued Steer and FollowUp items
// are kept.
func (a *Agent) SetTranscript(t Transcript) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.running {
		return ErrRunning
	}
	a.transcript = append(Transcript(nil), t...)
	a.pending = unansweredCalls(a.transcript)
	return nil
}

// State is a snapshot of the agent.
type State struct {
	// Transcript is a copy of the slice; items are shared.
	Transcript Transcript
	Running    bool
	RunID      string
	Turn       int
	// Steering and FollowUps count the queued items.
	Steering  int
	FollowUps int
	// Pending lists the calls awaiting outputs, deferred or cut off by
	// an abort or a failure; Prompt and Continue refuse until Resume has
	// answered them.
	Pending []*openresponses.FunctionCall
}

// State returns a snapshot.
func (a *Agent) State() State {
	a.mu.Lock()
	defer a.mu.Unlock()
	return State{
		Transcript: append(Transcript(nil), a.transcript...),
		Running:    a.running,
		RunID:      a.runID,
		Turn:       a.turn,
		Steering:   len(a.steer),
		FollowUps:  len(a.followUp),
		Pending:    append([]*openresponses.FunctionCall(nil), a.pending...),
	}
}

// Prompt appends items and runs until idle. It returns when the run_end
// subscribers have returned, with the [RunEnd] that says how the run
// ended: done, stopped, input_required with the pending calls, or
// aborted with the context error on RunEnd.Err. The error is set only
// when the run could not start ([ErrNoPrompt], [ErrRunning],
// [ErrInputRequired], [ErrNoModel]) or ended with ReasonError, in which
// case it is RunEnd.Err and the RunEnd is returned alongside.
func (a *Agent) Prompt(ctx context.Context, items ...openresponses.Item) (*RunEnd, error) {
	if len(items) == 0 {
		return nil, ErrNoPrompt
	}
	return a.run(ctx, items, false)
}

// Continue runs from the transcript as it stands, which must satisfy
// [CanContinue], and returns as [Agent.Prompt] does.
func (a *Agent) Continue(ctx context.Context) (*RunEnd, error) {
	a.mu.Lock()
	pending := len(a.pending) > 0
	ok := CanContinue(a.transcript)
	a.mu.Unlock()
	if pending {
		return nil, ErrInputRequired
	}
	if !ok {
		return nil, ErrCannotContinue
	}
	return a.run(ctx, nil, false)
}

// Resume answers the calls the last run left pending and continues,
// whether they were deferred to the caller or cut off by an abort or a
// failure. Every pending call must have exactly one output, and no
// output may answer a call that is not pending; a caller that refuses
// a call answers it with the refusal as text, which the model then
// sees. The outputs are appended with their item events before the
// model is called, and the run returns as [Agent.Prompt] does. With
// nothing pending, Resume returns [ErrNotPending].
func (a *Agent) Resume(ctx context.Context, outputs ...*openresponses.FunctionCallOutput) (*RunEnd, error) {
	a.mu.Lock()
	want := make(map[string]bool, len(a.pending))
	for _, call := range a.pending {
		want[call.CallID] = true
	}
	a.mu.Unlock()
	if len(want) == 0 {
		return nil, fmt.Errorf("%w: nothing is pending", ErrNotPending)
	}
	items := make(openresponses.Items, 0, len(outputs))
	for _, out := range outputs {
		if out == nil || !want[out.CallID] {
			return nil, fmt.Errorf("%w: %q", ErrNotPending, callID(out))
		}
		delete(want, out.CallID)
		items = append(items, out)
	}
	if len(want) > 0 {
		return nil, fmt.Errorf("%w: %d pending call(s) unanswered", ErrNotPending, len(want))
	}
	return a.run(ctx, items, true)
}

func callID(out *openresponses.FunctionCallOutput) string {
	if out == nil {
		return ""
	}
	return out.CallID
}

func (a *Agent) run(ctx context.Context, prompts openresponses.Items, resuming bool) (*RunEnd, error) {
	a.mu.Lock()
	if err := a.cfg.validate(); err != nil {
		a.mu.Unlock()
		return nil, err
	}
	if a.running {
		a.mu.Unlock()
		return nil, ErrRunning
	}
	if len(a.pending) > 0 && !resuming {
		a.mu.Unlock()
		return nil, ErrInputRequired
	}
	a.pending = nil
	ctx, cancel := context.WithCancel(ctx)
	a.running = true
	a.cancel = cancel
	a.turn = 0
	a.idle = make(chan struct{})
	transcript := append(Transcript(nil), a.transcript...)
	cfg := a.cfg
	a.mu.Unlock()

	r := &runner{
		cfg:        cfg,
		transcript: transcript,
		emit: func(ev Event) error {
			if _, ok := ev.(*RunEnd); ok {
				// The run is over; a subscriber writing durable state
				// on run_end must not see the abort's cancellation.
				return a.deliver(context.WithoutCancel(ctx), ev)
			}
			return a.deliver(ctx, ev)
		},
		steer:    a.drainSteer,
		followUp: a.drainFollowUp,
	}
	end := r.run(ctx, prompts)
	cancel()

	a.mu.Lock()
	a.running = false
	a.cancel = nil
	close(a.idle)
	a.mu.Unlock()

	if end.Reason == ReasonError {
		return end, end.Err
	}
	return end, nil
}

// deliver updates state from the event and calls every subscriber in
// registration order.
func (a *Agent) deliver(ctx context.Context, ev Event) error {
	a.mu.Lock()
	switch e := ev.(type) {
	case *RunStart:
		a.runID = e.RunID
	case *TurnStart:
		a.turn = e.Turn
	case *ItemEnd:
		a.transcript = append(a.transcript, e.Item)
	case *RunEnd:
		a.pending = e.Pending
	}
	// Snapshot the subscriber list and release the lock before calling
	// out: a subscriber may Subscribe, unsubscribe or read State.
	subs := append([]subscription(nil), a.subs...)
	a.mu.Unlock()
	for _, s := range subs {
		if err := s.fn(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

// Subscribe registers fn for every event and returns a function that
// removes it. Subscribing during a run takes effect from the next event.
func (a *Agent) Subscribe(fn func(context.Context, Event) error) (unsubscribe func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	id := a.nextSub
	a.nextSub++
	a.subs = append(a.subs, subscription{id: id, fn: fn})
	return func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		for i, s := range a.subs {
			if s.id == id {
				// The three-index slice forces a fresh array, so a
				// snapshot taken by deliver is never written through.
				a.subs = append(a.subs[:i:i], a.subs[i+1:]...)
				return
			}
		}
	}
}

// Steer queues items to be injected after the current tool batch,
// before the next model call. When the agent is idle they are consumed
// by the next run at the same point.
func (a *Agent) Steer(items ...openresponses.Item) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.steer = append(a.steer, items...)
}

// FollowUp queues items to be injected when the run would otherwise
// end, so the agent keeps going instead of going idle.
func (a *Agent) FollowUp(items ...openresponses.Item) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.followUp = append(a.followUp, items...)
}

func (a *Agent) drainSteer() openresponses.Items {
	a.mu.Lock()
	defer a.mu.Unlock()
	items := a.steer
	a.steer = nil
	return items
}

func (a *Agent) drainFollowUp() openresponses.Items {
	a.mu.Lock()
	defer a.mu.Unlock()
	items := a.followUp
	a.followUp = nil
	return items
}

// Abort cancels the active run, if any. The model stream and running
// tools see the cancellation through their context and the run ends
// with ReasonAborted.
func (a *Agent) Abort() {
	a.mu.Lock()
	cancel := a.cancel
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// WaitForIdle blocks until no run is active or ctx is done. An agent
// that has never run is idle.
func (a *Agent) WaitForIdle(ctx context.Context) error {
	a.mu.Lock()
	idle := a.idle
	a.mu.Unlock()
	if idle == nil {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
