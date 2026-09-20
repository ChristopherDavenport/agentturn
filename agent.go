package agentturn

import (
	"context"
	"encoding/json"
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
	// failure; answer them through [Agent.Resume], or open the next
	// [Agent.Prompt] with their outputs. The typed errors of tools/agent
	// and tools/a2a match it with errors.Is, so a host can ask "does any
	// sub-agent need input" once.
	ErrInputRequired = errors.New("agentturn: pending tool calls must be resumed before continuing")
	// ErrNotPending is returned when Resume was given an answer for a
	// call that is not pending, left a pending call unanswered, or was
	// called with nothing pending, and when a Prompt opens with an
	// output for a call that is not pending.
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
// error ends the run with ReasonError. Every event is delivered with a
// context whose cancellation is lifted: [Agent.Abort] reaches the model
// stream and the running tools, and the events that follow it, the
// tool_end of a cut-off call and the run_end among them, still reach a
// subscriber that writes durable state with a context it can use. A
// subscriber that fails while the run is being aborted ends it with
// ReasonAborted and its error joined to the context error on RunEnd.Err.
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
	// an abort or a failure; Continue refuses until Resume has answered
	// them, and Prompt unless it opens with their outputs.
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
// [ErrInputRequired], [ErrNotPending], [ErrNoModel]) or ended with
// ReasonError, in which case it is RunEnd.Err and the RunEnd is
// returned alongside.
//
// While calls are pending, a prompt that opens with a
// function_call_output for each of them is accepted: the outputs are
// appended with their item events ahead of the message, so the next
// model call sees the answers and the message together, which is what
// a front wants after an abort when the user's next line is the next
// prompt. A leading output for a call that is not pending returns
// [ErrNotPending]; a prompt that leaves a pending call unanswered
// returns [ErrInputRequired].
func (a *Agent) Prompt(ctx context.Context, items ...openresponses.Item) (*RunEnd, error) {
	if len(items) == 0 {
		return nil, ErrNoPrompt
	}
	return a.run(ctx, items, nil, false)
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
	return a.run(ctx, nil, nil, false)
}

// Answer resolves one pending call for [Agent.Resume]: an output the
// caller produced, or an approval that runs the call inside the loop.
// Build one with [Output], [Approve] or [ApproveWith].
type Answer struct {
	// CallID names the pending call.
	CallID string
	// Output, when set, answers the call without running it: a refusal
	// as text, or the result of a call the caller ran itself.
	Output *openresponses.FunctionCallOutput
	// Args, for an approval, replaces the arguments the tool receives,
	// as ToolDecision.Args does. nil keeps the call's own.
	Args json.RawMessage
}

// Output answers a pending call with out.
func Output(out *openresponses.FunctionCallOutput) Answer {
	if out == nil {
		return Answer{}
	}
	return Answer{CallID: out.CallID, Output: out}
}

// Approve runs the pending call with the arguments the model gave.
func Approve(callID string) Answer { return Answer{CallID: callID} }

// ApproveWith runs the pending call with args in place of the model's.
func ApproveWith(callID string, args json.RawMessage) Answer {
	return Answer{CallID: callID, Args: args}
}

// Resume answers the calls the last run left pending and continues,
// whether they were deferred to the caller or cut off by an abort or a
// failure. Every pending call must have exactly one answer, and no
// answer may name a call that is not pending. An answer is an output
// or an approval: a caller that refuses a call answers it with the
// refusal as text, which the model then sees; a caller that approves a
// deferred call lets the loop run it.
//
// The outputs are appended with their item events first. The approved
// calls then run as one batch as the loop runs any batch, with
// BeforeToolCall skipped because the decision has been made: tool_start,
// tool_update and tool_end are emitted with Turn 0, Sequential and
// MaxParallelTools apply, AfterToolCall runs, and the outputs are
// appended in the calls' transcript order. A batch whose every result
// sets Terminate ends the run with ReasonStopped without calling the
// model. The model is then called and the run returns as [Agent.Prompt]
// does. With nothing pending, Resume returns [ErrNotPending].
func (a *Agent) Resume(ctx context.Context, answers ...Answer) (*RunEnd, error) {
	a.mu.Lock()
	pending := append([]*openresponses.FunctionCall(nil), a.pending...)
	a.mu.Unlock()
	if len(pending) == 0 {
		return nil, fmt.Errorf("%w: nothing is pending", ErrNotPending)
	}
	byID := make(map[string]*openresponses.FunctionCall, len(pending))
	for _, call := range pending {
		byID[call.CallID] = call
	}
	var outputs openresponses.Items
	var approved []approval
	for _, ans := range answers {
		call, ok := byID[ans.CallID]
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrNotPending, ans.CallID)
		}
		delete(byID, ans.CallID)
		if ans.Output != nil {
			outputs = append(outputs, ans.Output)
			continue
		}
		approved = append(approved, approval{call: call, args: ans.Args})
	}
	if len(byID) > 0 {
		return nil, fmt.Errorf("%w: %d pending call(s) unanswered", ErrNotPending, len(byID))
	}
	return a.run(ctx, outputs, approved, true)
}

// answersPending checks the outputs that open a prompt against the
// pending calls: every pending call must be answered exactly once by a
// leading function_call_output, and no leading output may name a call
// that is not pending.
func answersPending(pending []*openresponses.FunctionCall, prompts openresponses.Items) error {
	want := make(map[string]bool, len(pending))
	for _, call := range pending {
		want[call.CallID] = true
	}
	for _, item := range prompts {
		out, ok := item.(*openresponses.FunctionCallOutput)
		if !ok {
			break
		}
		if !want[out.CallID] {
			return fmt.Errorf("%w: %q", ErrNotPending, out.CallID)
		}
		delete(want, out.CallID)
	}
	if len(want) > 0 {
		return fmt.Errorf("%w: %d pending call(s) unanswered", ErrInputRequired, len(want))
	}
	return nil
}

func (a *Agent) run(ctx context.Context, prompts openresponses.Items, approved []approval, resuming bool) (*RunEnd, error) {
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
		// A prompt that opens with the outputs of every pending call
		// answers them on the way to the next message.
		if err := answersPending(a.pending, prompts); err != nil {
			a.mu.Unlock()
			return nil, err
		}
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

	// Subscribers get the values of ctx but never its cancellation: an
	// abort cuts the model and the tools, and what they leave behind
	// still has to be written.
	subCtx := context.WithoutCancel(ctx)
	r := &runner{
		cfg:        cfg,
		transcript: transcript,
		emit:       func(ev Event) error { return a.deliver(subCtx, ev) },
		steer:      a.drainSteer,
		followUp:   a.drainFollowUp,
	}
	end := r.run(ctx, prompts, approved)
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
