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
	// ErrRunning: Prompt or Continue was called while a run is active.
	ErrRunning = errors.New("agentturn: agent is already running")
	// ErrAborted: the run was aborted with [Agent.Abort] or its context.
	ErrAborted = errors.New("agentturn: run aborted")
	// ErrInputRequired: the last run deferred calls that are still
	// unanswered; [Agent.Resume] with their outputs first.
	ErrInputRequired = errors.New("agentturn: pending tool calls must be resumed before continuing")
	// ErrNotPending: Resume was given an output for a call that is not
	// pending, or left a pending call unanswered.
	ErrNotPending = errors.New("agentturn: output does not answer a pending call")
)

// Agent is the stateful loop: a transcript, queues, subscribers and run
// control over [Run]. One run at a time; a second Prompt while one is
// active returns [ErrRunning].
//
// Subscribers are called synchronously, in registration order, for every
// event, so every event is a barrier: the loop does not move to the next
// phase until each subscriber has returned. A subscriber that returns an
// error ends the run with ReasonError.
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
// resuming a session.
func WithTranscript(t Transcript) Option {
	return func(a *Agent) { a.transcript = append(Transcript(nil), t...) }
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
	// Pending lists the deferred calls awaiting outputs; Prompt and
	// Continue refuse until Resume has answered them.
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
// subscribers have returned: nil when the run finished or was stopped,
// [ErrAborted] when it was aborted, and the failure otherwise.
func (a *Agent) Prompt(ctx context.Context, items ...openresponses.Item) error {
	if len(items) == 0 {
		return ErrNoPrompt
	}
	return a.run(ctx, items, false)
}

// Continue runs from the transcript as it stands, which must end with a
// user message or a function call output.
func (a *Agent) Continue(ctx context.Context) error {
	a.mu.Lock()
	pending := len(a.pending) > 0
	ok := canContinue(a.transcript)
	a.mu.Unlock()
	if pending {
		return ErrInputRequired
	}
	if !ok {
		return ErrCannotContinue
	}
	return a.run(ctx, nil, false)
}

// Resume answers the calls the last run deferred and continues. Every
// pending call must have exactly one output, and no output may answer
// a call that is not pending; a caller that refuses a call answers it
// with the refusal as text, which the model then sees. The outputs are
// appended with their item events before the model is called.
func (a *Agent) Resume(ctx context.Context, outputs ...*openresponses.FunctionCallOutput) error {
	a.mu.Lock()
	pending := make(map[string]bool, len(a.pending))
	for _, call := range a.pending {
		pending[call.CallID] = true
	}
	a.mu.Unlock()
	items := make(openresponses.Items, 0, len(outputs))
	for _, out := range outputs {
		if out == nil || !pending[out.CallID] {
			return fmt.Errorf("%w: %q", ErrNotPending, callID(out))
		}
		delete(pending, out.CallID)
		items = append(items, out)
	}
	if len(pending) > 0 {
		return fmt.Errorf("%w: %d pending call(s) unanswered", ErrNotPending, len(pending))
	}
	if len(items) == 0 {
		return ErrNoPrompt
	}
	return a.run(ctx, items, true)
}

func callID(out *openresponses.FunctionCallOutput) string {
	if out == nil {
		return ""
	}
	return out.CallID
}

func (a *Agent) run(ctx context.Context, prompts openresponses.Items, resuming bool) error {
	if err := a.cfg.validate(); err != nil {
		return err
	}
	a.mu.Lock()
	if a.running {
		a.mu.Unlock()
		return ErrRunning
	}
	if len(a.pending) > 0 && !resuming {
		a.mu.Unlock()
		return ErrInputRequired
	}
	a.pending = nil
	ctx, cancel := context.WithCancel(ctx)
	a.running = true
	a.cancel = cancel
	a.turn = 0
	a.idle = make(chan struct{})
	transcript := append(Transcript(nil), a.transcript...)
	a.mu.Unlock()

	r := &runner{
		cfg:        a.cfg,
		transcript: transcript,
		emit:       func(ev Event) error { return a.deliver(ctx, ev) },
		steer:      a.drainSteer,
		followUp:   a.drainFollowUp,
	}
	end := r.run(ctx, prompts)
	cancel()

	a.mu.Lock()
	a.running = false
	a.cancel = nil
	close(a.idle)
	a.mu.Unlock()

	switch end.Reason {
	case ReasonAborted:
		return ErrAborted
	case ReasonError:
		return end.Err
	}
	return nil
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
				a.subs = append(a.subs[:i:i], a.subs[i+1:]...)
				return
			}
		}
	}
}

// Steer queues an item to be injected after the current tool batch,
// before the next model call. When the agent is idle it is consumed by
// the next run at the same point.
func (a *Agent) Steer(item openresponses.Item) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.steer = append(a.steer, item)
}

// FollowUp queues an item to be injected when the run would otherwise
// end, so the agent keeps going instead of going idle.
func (a *Agent) FollowUp(item openresponses.Item) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.followUp = append(a.followUp, item)
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

// WaitForIdle blocks until no run is active or ctx is done.
func (a *Agent) WaitForIdle(ctx context.Context) error {
	a.mu.Lock()
	idle := a.idle
	a.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
