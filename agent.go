package agentturn

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/ChristopherDavenport/agenttool"
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
	// ErrAmbiguousCall is returned when Resume was asked to approve a
	// call that may already have run, one [PendingCall.MayHaveRun]
	// reports, whose tool does not say it can run again: agenttool's
	// replay is unknown, or keyed and the key of the dispatch it would
	// repeat is not known, or keyed and the approval changes the
	// arguments under that key, or keeps them under another. Such a
	// call must not run again; answer
	// it with [OutcomeUnknown] so the model can check before it asks
	// again, or, when the host accepts the risk, approve it with
	// [Answer.WithRunAgain].
	ErrAmbiguousCall = errors.New("agentturn: a call that may have run cannot run again")
	// ErrCallAnswered is returned when Resume was asked to approve a
	// call pending as [PendingAnswered] or [PendingRejected]: a record
	// answered it without running it again, or refused it, and only its
	// output may follow.
	ErrCallAnswered = errors.New("agentturn: an answered call is owed its output and nothing else")
)

// Agent is the stateful loop: a transcript, queues, subscribers and run
// control over [Run]. One run at a time; a second Prompt while one is
// active returns [ErrRunning]. The zero Agent has no model and is idle;
// use [New].
//
// Subscribers are called synchronously, in registration order, for every
// event, so every event is a barrier: the loop does not move to the next
// phase until each subscriber has returned, a tool that raises an event
// with [Invoke] waits for them, and so does a caller queueing an item
// with [Agent.Steer]. One event is delivered at a time, whichever
// goroutine raised it, so a subscriber is never entered from two
// goroutines at once. A subscriber that steers or prompts the agent
// from inside an event does so with the context it was handed, which
// carries the delivery it already holds, or from another goroutine. A subscriber that returns an
// error ends the run with ReasonError. Every event is delivered with a
// context whose cancellation is lifted: [Agent.Abort] reaches the model
// stream and the running tools, and the events that follow it, the
// tool_end of a cut-off call and the run_end among them, still reach a
// subscriber that writes durable state with a context it can use. A
// subscriber that fails while the run is being aborted ends it with
// ReasonAborted and its error joined to the context error on RunEnd.Err.
type Agent struct {
	cfg Config

	// emitMu is the delivery barrier: one event reaches the subscribers
	// at a time, whichever goroutine raised it.
	emitMu sync.Mutex

	mu         sync.Mutex
	transcript Transcript
	subs       []subscription
	nextSub    int
	steer      []queuedItem
	followUp   []queuedItem
	running    bool
	// closing is set once the run in flight is past its last drain of
	// the queues, before its run_end: an item steered after it waits
	// for the next run, so Deliver starts one rather than joining.
	closing bool
	// drains counts the drains of the steer queue that took something,
	// and seen is what it was when the last request was sent, so
	// Deliver can tell a model call has seen the items it queued.
	drains int
	seen   int
	// progress is closed and replaced when seen moves, a run marks
	// itself past its last drain, or a run ends: what Deliver waits on.
	progress chan struct{}
	// lastEnd is the end of the last run, for a Deliver whose items that
	// run took and ended without a model call seeing them.
	lastEnd *RunEnd
	// queued holds the accepts not yet reported: Steer and FollowUp
	// append, and the goroutine that owns delivery reports them at its
	// next event.
	queued []*Queued
	// steered is closed by a steer and replaced when the steer queue is
	// drained, so a tool's batch hears a steer made during it.
	steered chan struct{}
	runID   string
	turn    int
	cancel  context.CancelCauseFunc
	// runCancel cancels the context RunContext hands the run's tools,
	// which outlives the run's own.
	runCancel context.CancelCauseFunc
	idle      chan struct{}
	pending   []PendingCall
	// seeded is what WithPending gave, applied to the pending calls
	// once every option has run.
	seeded []PendingCall
	// reserved holds the call IDs a record names beyond the transcript,
	// which a call the model makes must not take.
	reserved map[string]bool
	// reasoning says which model produced the reasoning items of the
	// transcript it knows, so a run leaves another model's out of its
	// requests.
	reasoning ReasoningModels
}

type subscription struct {
	id int
	fn func(context.Context, Event) error
}

// Option configures a new Agent.
type Option func(*Agent)

// WithTranscript starts the agent from an existing transcript, as when
// resuming a session. Function calls anywhere in it that have no
// output are pending, with [PendingUnknown] as their reason since the
// loop cannot say whether they ran, as they would be after the run
// that made them: Prompt and Continue return [ErrInputRequired] until
// [Agent.Resume] has answered them, or a Prompt opens with their
// outputs. Resume holds an approval of such a call to the replay rule,
// since it may have run; [WithPending] says which ones did not.
func WithTranscript(t Transcript) Option {
	return func(a *Agent) {
		a.transcript = append(Transcript(nil), t...)
		a.pending = pendingCalls(unansweredCalls(a.transcript), PendingUnknown)
	}
}

// WithPending says why the calls the seeded transcript leaves without
// an output are pending, and with what key they were handed to their
// tools, from a record that knows more than the transcript does: a
// session's dispatch entries tell a call that never started from one
// that may have run. Each is matched to a pending call by its call ID,
// with the same name and arguments; one that matches none is ignored,
// and a pending call it does not list stays [PendingUnknown]. A call
// it lists as never started, or as deferred before any dispatch, is
// approved without the replay rule [Agent.Resume] applies to the
// others, and one it lists as answered takes only an output. It applies to the
// transcript [WithTranscript] gives, whichever option comes first; for
// one [Agent.SetTranscript] sets later, [Agent.SetPending] does the
// same. The session package's AgentOptions gives both options for a
// stored session.
func WithPending(pending []PendingCall) Option {
	return func(a *Agent) {
		a.seeded = append([]PendingCall(nil), pending...)
	}
}

// WithReservedCallIDs names call IDs a call the model makes must not
// take although the transcript does not hold them: the calls a
// compaction folded out of a session's context, or a trim dropped, are
// still on its path, those of a branch a rewind or a fork left are
// still in the session, and a provider that numbers its calls per
// response repeats their IDs. A call that repeats one runs under an ID
// of the loop's own, as a call repeating one in the transcript does.
// The session package's AgentOptions gives every call ID in a stored
// session.
func WithReservedCallIDs(ids []string) Option {
	return func(a *Agent) { a.reserve(ids) }
}

// New builds an agent.
func New(cfg Config, opts ...Option) *Agent {
	a := &Agent{cfg: cfg, idle: closedChan(), steered: make(chan struct{}), progress: make(chan struct{})}
	for _, opt := range opts {
		opt(a)
	}
	a.seedPending()
	return a
}

// seedPending applies what WithPending gave to the pending calls.
func (a *Agent) seedPending() {
	mergePending(a.pending, a.seeded)
	a.seeded = nil
}

// mergePending replaces each of pending with what known says of the
// same call, matched by call ID and, where known has the call, its name
// and arguments, keeping the transcript's own call item.
func mergePending(pending, known []PendingCall) {
	byID := make(map[string]PendingCall, len(known))
	for _, p := range known {
		if p.Call != nil {
			byID[p.Call.CallID] = p
		}
	}
	for i, p := range pending {
		q, ok := byID[p.Call.CallID]
		if !ok || q.Call.Name != p.Call.Name || q.Call.Arguments != p.Call.Arguments {
			continue
		}
		q.Call = p.Call
		pending[i] = q
	}
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
// [ErrRunning] while a run is active. Subscribers and the queues are
// kept, so anything steered or queued under the old configuration
// goes to the next run under the new one; a session recorder attached
// to the agent sees the change as a config delta on the next turn.
//
// The transcript is kept too, reasoning items included, and the loop
// owns what a change of model means for them: a reasoning item carries
// a signature only its own provider accepts, so each request leaves
// out the ones a model with another [Config.ModelName] produced, which
// the agent attributed as its runs went (see [ReasoningModels]). The
// transcript and a record of it keep them, and a configuration that
// switches back to that model is sent them again. A configuration with
// an empty ModelName names no model, so nothing is attributed to it
// and nothing is left out of its requests. A session recorder
// cannot write a request that leaves items out of the middle of its
// history, so the responses after such a change carry no request hash.
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
// branch of a session, or when a handoff trims what the receiving
// configuration sees, between runs and once. It returns [ErrRunning]
// while a run is active. A session recorder writing the agent is moved
// to match with its Rebase: to the entry of the last item kept, for a
// trim that keeps a prefix, whose responses then carry hashes; a trim
// from the middle runs as well and leaves the next responses without
// one. A handoff to another model need not trim the previous model's
// reasoning items: the loop leaves them out of each request, as
// [Agent.SetConfig] says, for every item it attributed, which the new
// transcript keeps when it holds the same item. A transcript rebuilt
// from elsewhere has no attribution until [WithReasoningModels] or
// [ContextWithReasoningModels] gives it.
// The pending calls are derived from the new transcript as
// [WithTranscript] derives them, except that a call the agent already
// had pending, the same call under the same ID, keeps its reason, key
// and arguments: a held call stays held. Any other is [PendingUnknown],
// so an approval of it is held to the replay rule as for any call that
// may have run, until [Agent.SetPending] says what a record knows of
// it. Whatever the old transcript was waiting on and the new one does
// not hold is forgotten, and whatever the new one is waiting on must be
// answered through [Agent.Resume]. Queued Steer and FollowUp items are
// kept and go to the next run on the new transcript; a host that does
// not want them there reads them from [Agent.State] first. The call IDs
// the old transcript holds stay reserved, as [Agent.ReserveCallIDs]
// reserves them, since a session that recorded them holds them on the
// branch the agent left.
func (a *Agent) SetTranscript(t Transcript) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.running {
		return ErrRunning
	}
	for _, item := range a.transcript {
		if call, ok := item.(*openresponses.FunctionCall); ok && call.CallID != "" {
			a.reserve([]string{call.CallID})
		}
	}
	known := a.pending
	a.transcript = append(Transcript(nil), t...)
	a.reasoning = a.reasoning.retain(a.transcript)
	a.pending = pendingCalls(unansweredCalls(a.transcript), PendingUnknown)
	mergePending(a.pending, known)
	return nil
}

// SetPending says why the calls the transcript leaves without an output
// are pending, and with what key and arguments they were handed to
// their tools, as [WithPending] does for a new agent: after
// [Agent.SetTranscript] to a branch of a session, with what the
// session package's Pending reads there. Each is matched to a pending
// call by its call ID, with the same name and arguments, and ignored
// when it matches none. It returns
// [ErrRunning] while a run is active.
func (a *Agent) SetPending(pending []PendingCall) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.running {
		return ErrRunning
	}
	mergePending(a.pending, pending)
	return nil
}

// ReserveCallIDs adds to the call IDs a call the model makes must not
// take, as [WithReservedCallIDs] does for a new agent: after
// [Agent.SetTranscript] to a session's context, with every call ID in
// the session. The IDs reserved earlier stay reserved. It returns
// [ErrRunning] while a run is active.
func (a *Agent) ReserveCallIDs(ids ...string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.running {
		return ErrRunning
	}
	a.reserve(ids)
	return nil
}

func (a *Agent) reserve(ids []string) {
	for _, id := range ids {
		if a.reserved == nil {
			a.reserved = map[string]bool{}
		}
		a.reserved[id] = true
	}
}

// State is a snapshot of the agent.
type State struct {
	// Transcript is a copy of the slice; items are shared.
	Transcript Transcript
	// Running is true from the run's start until its run_end has been
	// delivered, which is after the run's last drain of the queues: it
	// does not say that a steer will be taken; [Agent.Deliver] does.
	Running bool
	RunID   string
	Turn    int
	// Steering and FollowUps count the queued items; Steered and Queued
	// are the items themselves, copies of the queues, so a host can
	// persist what it accepted and queue it again after a restart.
	Steering  int
	FollowUps int
	Steered   openresponses.Items
	Queued    openresponses.Items
	// Pending lists the calls awaiting outputs, each with why: deferred,
	// cut off by an abort or a failure, or found unanswered in a seeded
	// transcript. Continue refuses until Resume has answered them, and
	// Prompt unless it opens with their outputs.
	Pending []PendingCall
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
		Steered:    queuedItems(a.steer),
		Queued:     queuedItems(a.followUp),
		Pending:    append([]PendingCall(nil), a.pending...),
	}
}

// Prompt appends items and runs until idle. It returns when the run_end
// subscribers have returned, with the [RunEnd] that says how the run
// ended: done, stopped, input_required with the pending calls, or
// aborted with the context error on RunEnd.Err. The error is set only
// when the run could not start ([ErrNoPrompt], [ErrRunning],
// [ErrInputRequired], [ErrNotPending], [ErrNoModel]) or ended with
// ReasonError, in which case it is RunEnd.Err and the RunEnd is
// returned alongside. A [Trigger] attached to ctx with
// [ContextWithTrigger] is carried on the run's RunStart.
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
	return a.run(ctx, items, nil, false, false)
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
	return a.run(ctx, nil, nil, false, false)
}

// Answer resolves one pending call for [Agent.Resume]: an output the
// caller produced, or an approval that runs the call inside the loop.
// Build one with [Output], [Approve], [ApproveWith], [Refuse] or
// [OutcomeUnknown], attach what the user said with [Answer.WithNote]
// and who said it with [Answer.WithBy].
type Answer struct {
	// CallID names the pending call.
	CallID string
	// Output, when set, answers the call without running it: a refusal
	// as text, or the result of a call the caller ran itself.
	Output *openresponses.FunctionCallOutput
	// Args, for an approval, replaces the arguments the tool receives,
	// as ToolDecision.Args does. nil keeps the call's own.
	Args json.RawMessage
	// Note is what the user said when answering: it is appended as a
	// user message after the outputs of every answer of the Resume, so
	// the model reads the result and the note together, in that order,
	// in the same turn.
	Note string
	// Terminate ends the run with ReasonStopped and StopRefused once
	// every answer is in, without calling the model, for a refusal that
	// should end the turn so the user can say what to do instead. The
	// outputs are still appended, so the transcript stays a valid input.
	Terminate bool
	// By names who decided this answer, for the record: the session
	// format knows "human" for a person at a prompt, "policy" for a
	// rule that answered on its own and "agent" for another model. It
	// rides on the ToolDecision the loop synthesises for an approval,
	// and a session recorder writes it on the decision for an answer of
	// either kind. There is no default: a policy engine answers through
	// Resume as often as a person does, so an answer that names nobody
	// is recorded as an anonymous decision rather than guessed at.
	By string
	// Reason says why the call was answered so. For an approval it
	// rides on the ToolDecision as By does, and a session recorder
	// writes it on the proceed decision. An approval of a call that may
	// have run that gives none carries the rule that let it run again:
	// [RunAgainSafeReason], [RunAgainKeyedReason] or [RunAgainReason]. For an output answering a call
	// that may have run, it rides on the run's context with
	// [ContextWithReasons], and a session recorder writes it on the
	// answer decision before the output: "not run again: replay
	// unknown", say. An output for a call that did not run is its own
	// reason, since it is what the model sees, and this is not recorded.
	Reason string
	// Origin, for an output taken from a record rather than produced
	// now, names where it was taken from, in the terms of the library
	// that read it: agentturn/session names the entry of the output the
	// answer repeats, on a branch a rebase left or in the session this
	// one forks. The loop carries it unread, on the run's context with
	// [ContextWithOrigins] as it carries Reason, so a session recorder
	// can tie the output it writes to the one it repeats and to the
	// child session that produced it. Empty for any other answer.
	Origin string
	// IdempotencyKey, for an approval, is the key the tool receives in
	// place of the one the loop would give it: the pending call's, for
	// a call that may have run, and a new one otherwise. A key names one
	// operation, so a key other than that of the dispatch the approval
	// repeats makes the call a new operation: for a keyed call that may
	// have run, Resume takes one only with new arguments, or with
	// [Answer.WithRunAgain]. A host that resumes after a restart and
	// wants the operation repeated leaves it empty, or sets the key of
	// that dispatch, which a session recorder wrote on it.
	IdempotencyKey string
	// RunAgain, for an approval, says the host accepts running a call
	// that may have run although its tool does not say that is safe.
	// It is the only way past the replay rule [Agent.Resume] applies,
	// and the proceed is recorded with [RunAgainReason] when the
	// answer gives no reason of its own.
	RunAgain bool
}

// The reasons an approval of a call that may have run carries when it
// gives none, naming the rule that let it run again.
const (
	// RunAgainReason is the reason of an approval built with
	// [Answer.WithRunAgain].
	RunAgainReason = "run again: accepted by the caller"
	// RunAgainSafeReason is the reason of an approval whose tool says
	// replay is safe.
	RunAgainSafeReason = "run again: safe"
	// RunAgainKeyedReason is the reason of an approval whose tool says
	// replay is keyed, run under the key of the dispatch it repeats or
	// with new arguments under a new one.
	RunAgainKeyedReason = "run again: keyed"
)

// Output answers a pending call with out.
func Output(out *openresponses.FunctionCallOutput) Answer {
	if out == nil {
		return Answer{}
	}
	return Answer{CallID: out.CallID, Output: out}
}

// Refuse answers a pending call with out and ends the run instead of
// calling the model: [Output] with Terminate set.
func Refuse(out *openresponses.FunctionCallOutput) Answer {
	a := Output(out)
	a.Terminate = true
	return a
}

// OutcomeUnknownReason is the reason an answer built with
// [OutcomeUnknown] carries until [Answer.WithReason] gives another.
const OutcomeUnknownReason = "not run again: outcome unknown"

// OutcomeUnknown answers a pending call that may have run and must not
// run again with the error agenttool's replay rule asks for, so the
// model sees that the call may or may not have taken effect and can
// check before it asks again. It carries [OutcomeUnknownReason].
func OutcomeUnknown(callID string) Answer {
	res := agenttool.ErrorResult(errors.New("outcome unknown: the call was cut off after it was handed to its tool, and it may or may not have taken effect; check before calling it again"))
	a := Output(&openresponses.FunctionCallOutput{CallID: callID, Output: res.Output})
	a.Reason = OutcomeUnknownReason
	return a
}

// Approve runs the pending call with the arguments it is pending with,
// [PendingCall.Args], which a decision that held it or a hand-off it
// may have run in gave it, else those the model gave.
func Approve(callID string) Answer { return Answer{CallID: callID} }

// ApproveWith runs the pending call with args in place of the model's
// and of any [PendingCall.Args].
func ApproveWith(callID string, args json.RawMessage) Answer {
	return Answer{CallID: callID, Args: args}
}

// WithNote returns the answer with note attached.
func (a Answer) WithNote(note string) Answer {
	a.Note = note
	return a
}

// WithBy returns the answer with by attached: who decided it, in the
// session format's terms ("human", "policy", "agent").
func (a Answer) WithBy(by string) Answer {
	a.By = by
	return a
}

// WithReason returns the answer with reason attached: why an approval
// runs the call, or why an output answers a call that may have run
// without running it again.
func (a Answer) WithReason(reason string) Answer {
	a.Reason = reason
	return a
}

// WithOrigin returns the output answer with origin attached: where the
// output was taken from, in the terms of the library that read it,
// for an output a record held rather than one produced now.
func (a Answer) WithOrigin(origin string) Answer {
	a.Origin = origin
	return a
}

// WithIdempotencyKey returns the approval with key as the key its tool
// receives.
func (a Answer) WithIdempotencyKey(key string) Answer {
	a.IdempotencyKey = key
	return a
}

// WithRunAgain returns the approval with [Answer.RunAgain] set: the
// call runs even if it may have run and its tool does not say it can
// run again, whatever its side effect.
func (a Answer) WithRunAgain() Answer {
	a.RunAgain = true
	return a
}

// Resume answers the calls the last run left pending and continues,
// whether they were deferred to the caller or cut off by an abort or a
// failure. Every pending call must have exactly one answer, and no
// answer may name a call that is not pending. An answer is an output
// or an approval: a caller that refuses a call answers it with the
// refusal as text, which the model then sees; a caller that approves a
// deferred call lets the loop run it. [Answer.By] says who decided,
// for the record.
//
// An approved call runs with the idempotency key the answer carries,
// else that of the dispatch it repeats, the last time it was handed to
// its tool, else a new one, and with the arguments the answer carries,
// else the ones that dispatch ran with, else its own. A call that may
// already have run, pending as [PendingAborted], as [PendingUnknown]
// since the loop cannot say, or held after its dispatch
// ([PendingCall.MayHaveRun]), is ambiguous, and Resume applies
// agenttool's rule for running it again: it approves the call when
// its tool's replay for the arguments it would run with is safe, or
// keyed and either run with the arguments and under the key of the
// dispatch it repeats, which must be known, or with new arguments
// under a key the answer chose, a new operation; otherwise it returns
// [ErrAmbiguousCall] and runs nothing. [Answer.WithRunAgain] is the
// only way past it. An approval that passes the rule without a reason
// of its own carries the rule's, [RunAgainSafeReason] or
// [RunAgainKeyedReason], which a session recorder writes on the
// proceed it records for the call. An
// agent seeded with [WithPending] from a record knows which calls
// never started, and those are not checked, and which were answered
// or refused without running: those take an output, and an approval
// of one returns [ErrCallAnswered].
//
// The outputs are appended with their item events first, then the
// notes of the answers that carry one, as user messages. The approved
// calls then run as one batch as the loop runs any batch, with
// BeforeToolCall skipped for a call the approval decides, one held or
// one that may have run. A call pending as [PendingUndispatched] was
// decided by nothing, so its approval is put to BeforeToolCall, with
// the approved batch as the batch: a Block or a Defer applies as it
// would in a run, and a call it defers leaves the run ending with
// [ReasonInputRequired] once the batch is in. tool_start, tool_update
// and tool_end are emitted with Turn 0, Sequential and
// MaxParallelTools apply, AfterToolCall runs, the outputs are appended
// in the calls' transcript order and their notes after them, and
// anything steered in meanwhile follows. A batch whose results set
// Terminate ends the run with ReasonStopped without calling the model,
// and so does any answer built with [Refuse]. Otherwise the model is
// called and the run returns as [Agent.Prompt] does. With nothing
// pending, Resume returns [ErrNotPending].
func (a *Agent) Resume(ctx context.Context, answers ...Answer) (*RunEnd, error) {
	a.mu.Lock()
	pending := append([]PendingCall(nil), a.pending...)
	cfg := a.cfg
	a.mu.Unlock()
	if len(pending) == 0 {
		return nil, fmt.Errorf("%w: nothing is pending", ErrNotPending)
	}
	byID := make(map[string]PendingCall, len(pending))
	for _, p := range pending {
		byID[p.Call.CallID] = p
	}
	var outputs, notes openresponses.Items
	var approved []approval
	deciders, reasons, origins := map[string]string{}, map[string]string{}, map[string]string{}
	terminate := false
	for _, ans := range answers {
		p, ok := byID[ans.CallID]
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrNotPending, ans.CallID)
		}
		delete(byID, ans.CallID)
		terminate = terminate || ans.Terminate
		if ans.By != "" {
			deciders[ans.CallID] = ans.By
		}
		if ans.Output != nil {
			outputs = append(outputs, ans.Output)
			if ans.Reason != "" {
				reasons[ans.CallID] = ans.Reason
			}
			if ans.Origin != "" {
				origins[ans.CallID] = ans.Origin
			}
			if ans.Note != "" {
				notes = append(notes, openresponses.UserText(ans.Note))
			}
			continue
		}
		if p.Reason == PendingAnswered || p.Reason == PendingRejected {
			return nil, fmt.Errorf("%w: %q", ErrCallAnswered, ans.CallID)
		}
		key := ans.IdempotencyKey
		if key == "" {
			key = p.IdempotencyKey
		}
		// A call run again runs as it was handed over, with the
		// arguments a decision gave it, unless the answer says
		// otherwise.
		args := ans.Args
		if args == nil {
			args = p.Args
		}
		reason := ans.Reason
		if p.MayHaveRun() {
			rule := RunAgainReason
			if !ans.RunAgain {
				var err error
				if rule, err = mayRunAgain(ctx, cfg, p, args, key); err != nil {
					return nil, err
				}
			}
			if reason == "" {
				reason = rule
			}
		}
		approved = append(approved, approval{call: p.Call, args: args, note: ans.Note, by: ans.By, reason: reason, key: key, decide: p.Reason == PendingUndispatched})
	}
	if len(byID) > 0 {
		return nil, fmt.Errorf("%w: %d pending call(s) unanswered", ErrNotPending, len(byID))
	}
	// An output the caller wrote raises no tool_start, so the decider
	// of an answer of that kind, its reason and where its output was
	// taken from ride on the run's context, where a recorder writing
	// the decision and the output for it finds them.
	ctx = ContextWithOrigins(ContextWithReasons(ContextWithDeciders(ctx, deciders), reasons), origins)
	return a.run(ctx, append(outputs, notes...), approved, true, terminate)
}

// mayRunAgain applies agenttool's rule for running a call again to an
// approval of p, which may have run, and returns the reason it passed:
// its tool's replay for args, the arguments it would run with, asked
// under key, the key it would run under, must be safe, or keyed. A
// keyed call may run again only under the key of the dispatch it
// repeats, which must be known: another key is another operation, and
// the one that may have happened is not deduplicated. It may run with
// other arguments than that dispatch ran with only under another key,
// since those arguments are another operation too. A call no tool has
// the name of runs nothing, and the loop refuses it.
func mayRunAgain(ctx context.Context, cfg Config, p PendingCall, args json.RawMessage, key string) (string, error) {
	call := p.Call
	tool, ok := cfg.tools(ctx).Lookup(call.Name)
	if !ok {
		return "", nil
	}
	args = orEmpty(args, call.Arguments)
	ctx = agenttool.WithCall(ctx, agenttool.Call{ID: call.CallID, Args: args, IdempotencyKey: key})
	switch agenttool.ReplayOf(ctx, tool, args) {
	case agenttool.ReplaySafe:
		return RunAgainSafeReason, nil
	case agenttool.ReplayKeyed:
		same := sameArgs(args, orEmpty(p.Args, call.Arguments))
		switch {
		case key == "":
			return "", fmt.Errorf("%w: %q is keyed and the key of the dispatch it repeats is not known", ErrAmbiguousCall, call.CallID)
		case same && p.IdempotencyKey == "":
			return "", fmt.Errorf("%w: %q is keyed and the key of the dispatch it repeats is not known, so another key would run it as a new operation", ErrAmbiguousCall, call.CallID)
		case same && key != p.IdempotencyKey:
			return "", fmt.Errorf("%w: %q is keyed and would run again under another key than the dispatch it repeats, as a new operation", ErrAmbiguousCall, call.CallID)
		case !same && key == p.IdempotencyKey:
			return "", fmt.Errorf("%w: %q is keyed and would run again with other arguments under the key of the dispatch it repeats", ErrAmbiguousCall, call.CallID)
		}
		return RunAgainKeyedReason, nil
	}
	return "", fmt.Errorf("%w: %q", ErrAmbiguousCall, call.CallID)
}

// orEmpty is args, else the call's own arguments, an empty object
// standing in for none.
func orEmpty(args json.RawMessage, own string) json.RawMessage {
	if args == nil {
		args = json.RawMessage(own)
	}
	if len(args) == 0 {
		args = json.RawMessage("{}")
	}
	return args
}

// sameArgs reports whether two argument objects are the same JSON
// value, whatever their spelling.
func sameArgs(a, b json.RawMessage) bool {
	x, errX := decodeNumbers(a)
	y, errY := decodeNumbers(b)
	if errX != nil || errY != nil {
		return bytes.Equal(a, b)
	}
	return reflect.DeepEqual(x, y)
}

// decodeNumbers decodes raw keeping numbers as written, so two integers
// past float64's precision are not taken for the same one.
func decodeNumbers(raw json.RawMessage) (any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	err := d.Decode(&v)
	return v, err
}

// answersPending checks the outputs that open a prompt against the
// pending calls: every pending call must be answered exactly once by a
// leading function_call_output, and no leading output may name a call
// that is not pending.
func answersPending(pending []PendingCall, prompts openresponses.Items) error {
	want := make(map[string]bool, len(pending))
	for _, p := range pending {
		want[p.Call.CallID] = true
	}
	for _, item := range unhideAll(prompts) {
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

func (a *Agent) run(ctx context.Context, prompts openresponses.Items, approved []approval, resuming, terminate bool) (*RunEnd, error) {
	a.mu.Lock()
	run, err := a.startUnlock(ctx, prompts, approved, resuming, terminate)
	if err != nil {
		return nil, err
	}
	return run()
}

// startUnlock calls start, with a.mu held by the caller, and releases
// it however start returns: a panic there, from a tool the
// configuration holds, leaves the agent usable by a caller that
// recovers it.
func (a *Agent) startUnlock(ctx context.Context, prompts openresponses.Items, approved []approval, resuming, terminate bool) (func() (*RunEnd, error), error) {
	defer a.mu.Unlock()
	return a.start(ctx, prompts, approved, resuming, terminate)
}

// start begins a run with a.mu held, so the check that the agent is
// idle and the mark that it is running are one step, and returns the
// function that drives the run once the lock is released.
func (a *Agent) start(ctx context.Context, prompts openresponses.Items, approved []approval, resuming, terminate bool) (func() (*RunEnd, error), error) {
	if err := a.cfg.validate(); err != nil {
		return nil, err
	}
	if a.running {
		return nil, ErrRunning
	}
	if err := TriggerFromContext(ctx).Validate(); err != nil {
		return nil, err
	}
	if len(a.pending) > 0 && !resuming {
		// A prompt that opens with the outputs of every pending call
		// answers them on the way to the next message.
		if err := answersPending(a.pending, prompts); err != nil {
			return nil, err
		}
	}
	prior := a.pending
	a.pending = nil
	// The context a tool's background work derives from: the prompt's
	// values, cancelled by an abort of this run or by the prompt's own
	// cancellation, and not by the run ending.
	runCtx, runCancel := linkRunContext(ctx)
	ctx, cancel := context.WithCancelCause(ctx)
	a.running = true
	a.closing = false
	// The run's ID is known from here, so an item queued before its
	// run_start names it rather than the run before.
	a.runID = openresponses.NewID("run")
	a.cancel = cancel
	a.runCancel = runCancel
	a.turn = 0
	a.idle = make(chan struct{})
	transcript := append(Transcript(nil), a.transcript...)
	cfg := a.cfg
	runID := a.runID

	return func() (*RunEnd, error) {
		// Subscribers get the values of ctx but never its cancellation:
		// an abort cuts the model and the tools, and what they leave
		// behind still has to be written.
		ctx := context.WithValue(ctx, inRunKey{a}, runID)
		subCtx := context.WithoutCancel(ctx)
		r := &runner{
			cfg:        cfg,
			transcript: transcript,
			send:       func(ev Event) error { return a.deliver(subCtx, ev) },
			steer:      a.drainSteer,
			followUp:   a.drainFollowUp,
			last:       a.drainLast,
			closing:    a.close,
			steered:    a.steerSignal,
			runCtx:     runCtx,
			prior:      prior,
			reserved:   a.reserved,
			reasoning:  a.reasoning.merged(reasoningFromContext(ctx)),
			runID:      runID,
			resuming:   resuming,
		}
		end := r.run(ctx, prompts, approved, terminate)
		cancel(nil)

		a.mu.Lock()
		a.reasoning = r.reasoning.retain(a.transcript)
		a.running = false
		a.closing = false
		a.cancel, a.runCancel = nil, nil
		close(a.idle)
		a.signal()
		a.mu.Unlock()

		if end.Reason == ReasonError {
			return end, end.Err
		}
		return end, nil
	}, nil
}

// deliver calls every subscriber for one event, one event at a time.
// Delivery is the barrier: the run's events and the events a nested
// call raises from a tool's goroutine pass through it, so a subscriber
// is never entered from two goroutines at once. The items [Agent.Steer]
// and [Agent.FollowUp] accepted since the last event are reported
// first, so an item is always announced before anything it produces.
//
// Nothing outside a run delivers, so a subscriber never waits on a
// barrier it is itself holding: steering from inside an event queues
// the item and returns.
func (a *Agent) deliver(ctx context.Context, ev Event) error {
	a.emitMu.Lock()
	defer a.emitMu.Unlock()
	if err := a.drainQueued(ctx); err != nil {
		return err
	}
	if err := a.dispatch(ctx, ev); err != nil {
		return err
	}
	if _, last := ev.(*RunEnd); last {
		// Nothing follows the run's end, so what a subscriber accepted
		// while it was being delivered is reported now rather than
		// waiting for a run that may never come.
		return a.drainQueued(ctx)
	}
	return nil
}

// drainQueued reports the items accepted since the last event, in the
// order they were accepted. It is one pass: an item a subscriber
// accepts while these are being delivered waits for the next event,
// which is what keeps a subscriber that steers on every queued event
// from spinning here.
func (a *Agent) drainQueued(ctx context.Context) error {
	a.mu.Lock()
	evs := a.queued
	a.queued = nil
	a.mu.Unlock()
	for _, ev := range evs {
		if err := a.dispatch(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

// dispatch updates state from the event and calls every subscriber in
// registration order.
func (a *Agent) dispatch(ctx context.Context, ev Event) error {
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
		a.lastEnd = e
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
	if _, ok := ev.(*TurnStart); ok {
		// Every subscriber took the turn_start, so the request goes out
		// carrying everything drained so far.
		a.mu.Lock()
		a.seen = a.drains
		a.signal()
		a.mu.Unlock()
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
// by the next run before its first model call, after its prompt, or
// after the batch of calls a Resume approved.
//
// Each item is reported to the subscribers as a [Queued] event, so a
// host writing what it accepted has it before the run appends it and
// can tell an item it accepted from one a run produced. The report is
// not the accept: the item is queued when this returns, and the event
// is delivered by whichever goroutine owns delivery, at its next event.
// A run in flight reports it before its next event; an idle agent
// reports it at the start of the next run, so a host that must not
// lose an input writes it before calling here rather than from the
// event. A run in flight takes it unless it is past its last drain,
// after its final turn_end or once it has decided to stop, when it
// waits for the next run like an item steered into an idle agent; a
// host whose input arrives on its own time and must reach the model
// calls [Agent.Deliver] instead. A run that stops after a turn, on its
// turn limit, a stop hook or a terminating result, decides so before
// it drains, so what was steered during that turn is not appended to
// it: the next run takes it, after its prompt, so a host that wants it
// answered first calls [Agent.Continue] rather than Prompt.
//
// It never blocks on the delivery barrier, so steering from inside a
// subscriber is safe: the item is queued and the event follows on the
// next one.
//
// The queues live in memory: an item accepted here is in no record
// until a run appends it or a subscriber writes it, and it survives
// [Agent.Abort], [SetConfig] and [SetTranscript] but not the process. A
// session recorder writes each [Queued] report as a queued entry and
// hands the inputs a resumed session owes back with its Requeue; a
// host without one reads the queues back from [Agent.State] and queues
// them again after a restart.
//
// A steered item is appended with its own item events, which reach
// every subscriber. A subscriber that steers in reaction to an event
// the steered item itself produces, an item_end during a run for
// instance, feeds the run forever, and nothing reports it: the loop
// cannot tell a reaction from a fresh input. Steer from a front, a
// monitor or another goroutine, or from a subscriber only on events it
// can tell apart from its own items, such as a tool_end or a specific
// item type it never steers.
func (a *Agent) Steer(items ...openresponses.Item) {
	// No trigger, so nothing to refuse.
	_ = a.Queue(context.Background(), QueueSteer, items...)
}

// FollowUp queues items to be injected when the run would otherwise
// end, so the agent keeps going instead of going idle. The queue has
// the same life, the same [Queued] event and the same caveat about
// subscribers as [Agent.Steer].
func (a *Agent) FollowUp(items ...openresponses.Item) {
	_ = a.Queue(context.Background(), QueueFollowUp, items...)
}

// Queue is [Agent.Steer] or [Agent.FollowUp], as mode says, for an
// input with a provenance of its own: the [Trigger] attached to ctx
// with [ContextWithTrigger] rides on each item's [Queued] report, so a
// recorder writes what brought the input in, a second person steering
// or a scheduled firing that overlapped a run, rather than only what
// started the run it joins. Nothing else is read from ctx, and it
// never blocks on delivery. A mode other than QueueSteer queues a
// follow-up. A trigger whose Extra cannot be written is refused with
// [ErrTriggerExtra], and nothing is queued.
func (a *Agent) Queue(ctx context.Context, mode QueueMode, items ...openresponses.Item) error {
	if mode != QueueSteer {
		mode = QueueFollowUp
	}
	trigger := TriggerFromContext(ctx)
	if err := trigger.Validate(); err != nil {
		return err
	}
	how := InputSteer
	if mode == QueueFollowUp {
		how = InputFollowUp
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.queue(trigger, mode, how, items)
	return nil
}

// Deliver hands items to the model, for an input that arrives on its
// own time: a background task's result, a detached child's answer. The
// items are queued as [Agent.Queue] queues a steer, with the [Trigger]
// on ctx, and Deliver waits until a model call has seen them or no run
// will take them. They reach the turn that takes them as [InputDeliver]
// among [TurnStartInfo.Inputs], where a steered item reads
// [InputSteer], so a [Config.BeforeTurn] hook that acts on a user's
// message tells a delivery from a steer, which the transcript's tail,
// where an output delivered after a steer looks like a resume's, does
// not say.
//
// A run in flight drains them after its batch, or when it would
// otherwise end, and Deliver returns joined true once the request that
// follows the drain has gone out. A run that decides to stop after its
// turn, on its turn limit, a stop hook, a guard in ShouldStopAfterTurn
// or a terminating result, decides before it drains and leaves them
// queued, as does a run past its last drain, after its final turn_end,
// while [State].Running still reads true. Deliver then waits for that
// run to end and, as with an idle agent, starts a run on ctx that takes
// the items before its first model call, as [Agent.Continue] would
// after a steer, and returns that run's end as Prompt returns it,
// joined false. The stop is not repeated for the delivered items: a
// host that wants it to stick reads the reason on the end returned and
// on the run it asked for, or aborts the run Deliver started.
//
// A run that drained the items and then ended before a model call, on
// a guard before the call, an abort or a failure, has them in its
// transcript unanswered: Deliver returns joined false with the end of
// the last run, which is that one unless another began and ended as
// well before Deliver looked, and starts nothing, since what stopped
// the run is the host's to look at. When no run can be started the
// items stay queued, and the error says why: [ErrInputRequired] while
// calls are pending, the run having stopped for input, whose Resume
// then takes them after its batch; [ErrCannotContinue] when the items
// do not end in something a model can answer; or ctx's error when it
// ended while Deliver waited.
//
// Called from inside the agent's run, from a tool, a hook, a
// subscriber or a child run one of its tools made, with the context it
// was handed, Deliver does not wait, since the run waits on the caller:
// it returns joined true when the run will still drain the items, whose
// stop decisions then apply as they do to any steer, and [ErrRunning]
// when the run is past its last drain, when they wait for the next run.
// A job running on [RunContext], and anything called with a context not
// derived from the run's, waits as any caller does.
func (a *Agent) Deliver(ctx context.Context, items ...openresponses.Item) (joined bool, end *RunEnd, err error) {
	if err := TriggerFromContext(ctx).Validate(); err != nil {
		return false, nil, err
	}
	a.mu.Lock()
	drains, before := a.drains, len(a.steer)
	a.queue(TriggerFromContext(ctx), QueueSteer, InputDeliver, items)
	if len(a.steer) == before {
		a.mu.Unlock()
		return false, nil, ErrNoPrompt
	}
	if a.running && !a.closing && ctx.Value(inRunKey{a}) == a.runID {
		// Called from inside the run, by a tool, a hook, a subscriber or
		// a child run a tool made, which the run waits on: the items
		// join it as a steer, and waiting for its next request would
		// wait on the caller.
		a.mu.Unlock()
		return true, nil, nil
	}
	if a.running && ctx.Value(inRunKey{a}) == a.runID {
		// Inside a run past its last drain, which ends only once the
		// caller returns: the items wait for the next run.
		a.mu.Unlock()
		return false, nil, ErrRunning
	}
	for {
		if a.seen > drains {
			// A request went out after the drain that took the items.
			a.mu.Unlock()
			return true, nil, nil
		}
		if !a.running {
			break
		}
		if a.progress == nil {
			a.progress = make(chan struct{})
		}
		progress := a.progress
		a.mu.Unlock()
		select {
		case <-progress:
		case <-ctx.Done():
			return false, nil, ctx.Err()
		}
		a.mu.Lock()
	}
	if a.drains != drains {
		// A run took the items and ended before a model call saw them.
		end := a.lastEnd
		a.mu.Unlock()
		if end != nil && end.Reason == ReasonError {
			return false, end, end.Err
		}
		return false, end, nil
	}
	if len(a.pending) > 0 {
		a.mu.Unlock()
		return false, nil, ErrInputRequired
	}
	if !CanContinue(append(append(Transcript(nil), a.transcript...), unhideAll(queuedItems(a.steer))...)) {
		a.mu.Unlock()
		return false, nil, ErrCannotContinue
	}
	run, err := a.startUnlock(ctx, nil, nil, false, false)
	if err != nil {
		return false, nil, err
	}
	end, err = run()
	return false, end, err
}

// inRunKey marks the context of everything an agent's run calls, its
// tools, hooks and subscribers and what they call in turn, with the
// run's ID, so Deliver can tell a caller the run is waiting on. The
// run context of background work does not carry it.
type inRunKey struct{ a *Agent }

// queuedItem is one item in a queue: the item as it was given, hidden
// mark included, how it arrived, a steer, a follow-up or a delivery,
// which the turn that takes it reports among its inputs, and the
// trigger it was queued with.
type queuedItem struct {
	item    openresponses.Item
	mode    InputMode
	trigger Trigger
}

// queuedItems returns the items of q, as [State] hands a queue back.
func queuedItems(q []queuedItem) openresponses.Items {
	if len(q) == 0 {
		return nil
	}
	items := make(openresponses.Items, len(q))
	for i, e := range q {
		items[i] = e.item
	}
	return items
}

// inputsOf returns q as the turn that takes it reports it.
func inputsOf(q []queuedItem) []TurnInput {
	if len(q) == 0 {
		return nil
	}
	inputs := make([]TurnInput, len(q))
	for i, e := range q {
		inputs[i] = TurnInput{Item: e.item, Mode: e.mode, Trigger: e.trigger}
	}
	return inputs
}

// queue is Queue with a.mu held; how says how the items arrived, which
// mode alone does not, since Deliver queues a steer.
func (a *Agent) queue(trigger Trigger, mode QueueMode, how InputMode, items openresponses.Items) {
	runID := ""
	if a.running && !a.closing {
		runID = a.runID
	}
	for _, item := range items {
		if item == nil {
			continue
		}
		base, hidden := Unhide(item)
		entry := queuedItem{item: item, mode: how, trigger: trigger}
		if mode == QueueSteer {
			a.steer = append(a.steer, entry)
			select {
			case <-a.steered:
			default:
				close(a.steered)
			}
		} else {
			a.followUp = append(a.followUp, entry)
		}
		a.queued = append(a.queued, &Queued{RunID: runID, Item: base, Mode: mode, Hidden: hidden, Trigger: trigger})
	}
}

func (a *Agent) drainSteer() []TurnInput {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.takeSteer()
}

// takeSteer empties the steer queue with a.mu held.
func (a *Agent) takeSteer() []TurnInput {
	items := a.steer
	a.steer = nil
	if len(items) > 0 {
		// What was heard has been taken; the next batch listens afresh.
		a.steered = make(chan struct{})
		a.drains++
	}
	return inputsOf(items)
}

// drainLast drains both queues for a run that would otherwise end,
// steered items first, and when both are empty marks the run past its
// last drain in the same step, so nothing steered can fall between the
// drain and the mark.
func (a *Agent) drainLast() []TurnInput {
	a.mu.Lock()
	defer a.mu.Unlock()
	items := append(a.takeSteer(), inputsOf(a.followUp)...)
	a.followUp = nil
	if len(items) == 0 {
		a.closing = true
		a.signal()
	}
	return items
}

// close marks the run past its last drain, as it decides to stop, and
// reports whether an item is still queued, which the next run takes.
func (a *Agent) close() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.closing {
		a.closing = true
		a.signal()
	}
	return len(a.steer) > 0 || len(a.followUp) > 0
}

// signal wakes whatever waits on progress, with a.mu held.
func (a *Agent) signal() {
	if a.progress != nil {
		close(a.progress)
	}
	a.progress = make(chan struct{})
}

// steerSignal returns the channel the next steer closes, already
// closed when a steered item is waiting to be drained.
func (a *Agent) steerSignal() <-chan struct{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.steered
}

func (a *Agent) drainFollowUp() []TurnInput {
	a.mu.Lock()
	defer a.mu.Unlock()
	items := a.followUp
	a.followUp = nil
	return inputsOf(items)
}

// Abort cancels the active run, if any. The model stream and running
// tools see the cancellation through their context and the run ends
// with ReasonAborted and context.Canceled on RunEnd.Err. The queues are
// untouched: anything steered or queued and not yet appended goes to
// the next run.
func (a *Agent) Abort() { a.AbortCause(nil) }

// AbortCause is [Agent.Abort] with a reason: cause is what
// context.Cause reports to everything the run called, and what the run
// ends with on RunEnd.Err, so a recorder writes it as the run's end and
// a product can count why its runs were cut. A stream rule that
// matched, an advisor that raised a blocker, a coordinator that
// cancelled a job and a user pressing Esc are four things a session
// otherwise records identically as "context canceled". A nil cause is
// [Agent.Abort].
//
// A cause that a caller wants errors.Is(err, context.Canceled) to keep
// matching should wrap it; the tools of the run see context.Canceled
// from their own context either way, since that is what ctx.Err
// reports.
func (a *Agent) AbortCause(cause error) {
	a.mu.Lock()
	cancel, runCancel := a.cancel, a.runCancel
	a.mu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
	if runCancel != nil {
		if cause == nil {
			cause = context.Canceled
		}
		runCancel(cause)
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
