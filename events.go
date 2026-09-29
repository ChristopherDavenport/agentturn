package agentturn

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// Event is one step of a run. Concrete types are [RunStart], [TurnStart],
// [ModelRetry], [ModelBlocked], [ItemStart], [ItemUpdate], [ItemEnd],
// [ResponseEnd], [ToolStart], [ToolDispatch], [ToolUpdate], [ToolEnd],
// [TurnEnd] and [RunEnd], and [Queued], which belongs to no run. Decoded values are
// pointers, so switch on *ItemUpdate and so on.
type Event interface {
	EventType() string
}

// Event type names.
const (
	EventRunStart     = "run_start"
	EventTurnStart    = "turn_start"
	EventModelRetry   = "model_retry"
	EventModelBlocked = "model_blocked"
	EventResponseEnd  = "response_end"
	EventItemStart    = "item_start"
	EventItemUpdate   = "item_update"
	EventItemEnd      = "item_end"
	EventToolStart    = "tool_start"
	EventToolDispatch = "tool_dispatch"
	EventToolUpdate   = "tool_update"
	EventToolEnd      = "tool_end"
	EventTurnEnd      = "turn_end"
	EventRunEnd       = "run_end"
	EventQueued       = "queued"
)

// Reason says why a run ended.
type Reason string

// Run end reasons.
const (
	// ReasonDone means the model produced a final answer with no tool
	// calls and no queued follow-ups.
	ReasonDone Reason = "done"
	// ReasonStopped means the loop chose not to call the model again:
	// ShouldStopAfterTurn, a terminating tool result, MaxTurns or a
	// refusal on Resume. RunEnd.Cause says which.
	ReasonStopped Reason = "stopped"
	// ReasonInputRequired means BeforeToolCall deferred one or more
	// calls to the caller; RunEnd.Pending lists them and the run
	// continues once their outputs are appended (see Agent.Resume).
	ReasonInputRequired Reason = "input_required"
	// ReasonAborted means the context was cancelled. RunEnd.Err is
	// context.Cause, the reason [Agent.AbortCause] or the host's own
	// context gave, and context.Canceled when there was none. A tool
	// batch cut off by the abort leaves its calls on RunEnd.Pending.
	ReasonAborted Reason = "aborted"
	// ReasonError means the model, a hook or a subscriber failed;
	// RunEnd.Err says which.
	ReasonError Reason = "error"
)

// Source says what a run began from, in the terms the session format
// records: an input, or the answers to calls an earlier run left
// pending.
type Source string

// Run sources.
const (
	// SourceInput is a run that begins with new input, or with nothing:
	// Prompt, Continue, and the low-level Run and Continue.
	SourceInput Source = "input"
	// SourceResume is a run that begins by answering a call that was
	// pending when it started: Resume, and a Prompt that opens with the
	// outputs of the pending calls.
	SourceResume Source = "resume"
)

// Trigger names what caused a run, in the caller's own terms: a cron
// job, a channel message, a user's turn. The loop learns nothing from
// it; it carries the value from [ContextWithTrigger] to [RunStart] so a
// recorder can write it. Kind is the category and Ref the instance; a
// recorder joins them as "kind:ref" when both are set, and writes the
// three apart as well. Source names the layer that took the input, a
// gateway or a scheduler, and is not part of the joined string.
//
// Extra holds the caller's richer facts about the firing, such as when
// it was due or which attempt it is, each a JSON value under a name of
// the caller's. A session recorder writes them as members of the
// trigger object beside kind, ref and source, which is where the
// format puts them, wherever it writes the trigger: on the run start,
// on a queued input's entry and as the source of the item that drains
// it, so a firing queued behind a busy run keeps its slot. The loop
// reads nothing from Extra, but it refuses, with [ErrTriggerExtra], a
// trigger whose Extra names kind, ref or source, the members the
// format defines beside it, or holds a value that does not encode as
// JSON, where the trigger enters: a run started with it and an item
// queued with it are refused at the call, whether or not a recorder is
// attached, rather than failing a later run.
type Trigger struct {
	Kind   string
	Ref    string
	Source string
	Extra  map[string]any
}

// ErrTriggerExtra is returned for a [Trigger] whose Extra cannot be
// written beside kind, ref and source: it names one of the three, or a
// value does not encode as JSON. [Agent.Prompt], [Agent.Continue],
// [Agent.Resume], [Agent.Queue] and [Agent.Deliver] refuse such a
// trigger on their context before anything happens, and [Run] and
// [Continue] end with it before any other event.
var ErrTriggerExtra = errors.New("agentturn: trigger extra cannot be written")

// Validate returns [ErrTriggerExtra], naming the member, when Extra
// cannot be written beside kind, ref and source, and nil otherwise.
func (t Trigger) Validate() error {
	for name, v := range t.Extra {
		switch name {
		case "kind", "ref", "source":
			return fmt.Errorf("%w: %q is a member of the trigger", ErrTriggerExtra, name)
		}
		if _, err := json.Marshal(v); err != nil {
			return fmt.Errorf("%w: %q: %w", ErrTriggerExtra, name, err)
		}
	}
	return nil
}

// IsZero reports whether the trigger names nothing.
func (t Trigger) IsZero() bool {
	return t.Kind == "" && t.Ref == "" && t.Source == "" && len(t.Extra) == 0
}

// String returns "kind:ref", or whichever of the two is set.
func (t Trigger) String() string {
	switch {
	case t.Kind != "" && t.Ref != "":
		return t.Kind + ":" + t.Ref
	case t.Kind != "":
		return t.Kind
	}
	return t.Ref
}

// RunStart opens a run. Source says whether the run answers pending
// calls or begins from input; Trigger is what the caller attached to the
// context with [ContextWithTrigger], zero when nothing was.
type RunStart struct {
	RunID   string
	Source  Source
	Trigger Trigger
}

// EventType returns "run_start".
func (*RunStart) EventType() string { return EventRunStart }

// TurnStart opens a turn and carries the exact request sent to the
// model. Inputs are the items appended to the transcript since the
// previous turn's response, or since the run started for the first
// turn: the tool outputs, the steered and queued messages, the items
// BeforeTurn added. They are what this turn's response answers, so a
// front that routes replies to the message that caused them reads them
// here rather than counting item events between turns.
type TurnStart struct {
	RunID   string
	Turn    int
	Request openresponses.Request
	Inputs  openresponses.Items
}

// EventType returns "turn_start".
func (*TurnStart) EventType() string { return EventTurnStart }

// ModelRetry reports that a model call failed and will be attempted
// again after Delay, under [Config.Retry]. It follows the turn_start
// of the turn; the failed attempt never began an answer, and anything
// it completed on the way was dropped with it.
type ModelRetry struct {
	RunID string
	Turn  int
	// Attempt is the number of the attempt that failed, from 1; the
	// next attempt is Attempt+1.
	Attempt int
	Err     error
	Delay   time.Duration
	// Request is what the next attempt will send: the turn's request,
	// or what [Retry.Revise] made of it. A recorder settles on it as it
	// does on turn_start, so a fallback to another model reaches the
	// path as a config delta.
	Request openresponses.Request
}

// EventType returns "model_retry".
func (*ModelRetry) EventType() string { return EventModelRetry }

// ModelBlocked reports that [Config.BeforeModelCall] refused the turn's
// request, so no call was made: Request is the request as built when
// the hook ran and Err is the hook's error. It is the last event before
// the run ends, in place of the turn_start the call would have had, so
// a recorder can write the call that was refused as distinct from one
// that was made and failed. The run ends with ReasonError, or with
// ReasonStopped and StopGuard when Err wraps [ErrGuard].
type ModelBlocked struct {
	RunID   string
	Turn    int
	Request openresponses.Request
	Err     error
}

// EventType returns "model_blocked".
func (*ModelBlocked) EventType() string { return EventModelBlocked }

// ItemStart announces an item entering the transcript: a prompt or
// queued message, an assistant item as the stream opens it, or a
// function call output. For an assistant item the Item is the live
// accumulated value and fills in as updates arrive.
//
// An item the model completes before its attempt commits, a reasoning
// summary before the first token, is announced here and reaches the
// transcript when the answer begins or the response arrives (see
// [Retry]). An attempt that ends without its response drops what it
// held, so a front that renders from item_start drops what it was
// rendering when a [ModelRetry], or a run_end with an error or an
// abort, follows with no item_end for it.
type ItemStart struct {
	RunID string
	Turn  int
	Item  openresponses.Item
	// ResponseID is the ID of the response streaming the item, and empty
	// for an item the loop appended itself.
	ResponseID string
	// Hidden is set for an item the caller marked with [Hidden]: it is
	// in the model's context and a renderer should not show it. The
	// Item is the item itself, unwrapped.
	Hidden bool
}

// EventType returns "item_start".
func (*ItemStart) EventType() string { return EventItemStart }

// ItemUpdate carries one wire event for an assistant item. Stream is the
// openresponses event verbatim, so a front switches on the same concrete
// types it would use against a remote server. Item is the accumulated
// item so far.
type ItemUpdate struct {
	RunID      string
	Turn       int
	Item       openresponses.Item
	Stream     openresponses.StreamEvent
	ResponseID string
}

// EventType returns "item_update".
func (*ItemUpdate) EventType() string { return EventItemUpdate }

// ItemEnd carries a completed item. For an assistant item it is emitted
// only after output_item.done; partial items never arrive here. The item
// is in the transcript when this event is delivered.
type ItemEnd struct {
	RunID string
	Turn  int
	Item  openresponses.Item
	// ResponseID is the ID of the response that produced the item, and
	// empty for an item the loop appended itself.
	ResponseID string
	// Hidden is set for an item the caller marked with [Hidden]: it is
	// in the model's context and a renderer should not show it. A
	// session recorder writes its entry with visible false.
	Hidden bool
	// Trigger, for an item the run was prompted with, is the run's
	// [Trigger], so a recorder can write how the input arrived; it is
	// zero for every other item, a queued input included, whose own
	// trigger rode on its [Queued] report.
	Trigger Trigger
}

// EventType returns "item_end".
func (*ItemEnd) EventType() string { return EventItemEnd }

// ResponseEnd carries the folded response of a turn, usage included, as
// soon as the stream ends and before any tool of the turn runs. Its
// output items have all been delivered with item_end. A response that
// failed is delivered here too, before the run ends with the error, so
// a recorder can write it.
type ResponseEnd struct {
	RunID    string
	Turn     int
	Response *openresponses.Response
}

// EventType returns "response_end".
func (*ResponseEnd) EventType() string { return EventResponseEnd }

// ToolStart announces a tool call after preflight, in the model's order.
// Turn is the turn whose response made the call, and 0 for a call
// approved through Agent.Resume, whose batch runs before the run's
// first model call; its tool_dispatch, tool_update and tool_end carry
// 0 too.
// Args are the arguments the tool receives, which a decision may have
// rewritten; the function_call item in the transcript keeps the model's.
// Decision is what BeforeToolCall returned for the call, nil when there
// was no hook or it returned nil; for a call approved through
// Agent.Resume it carries the caller's arguments, when they gave any,
// and nothing else.
type ToolStart struct {
	RunID    string
	Turn     int
	CallID   string
	Name     string
	Args     json.RawMessage
	Decision *ToolDecision
	// Parent is the ID of the call whose tool made this one with
	// [Invoke], and empty for a call the model made. A nested call has
	// no function_call item in the transcript: it is the work of the
	// call that made it.
	Parent string
}

// EventType returns "tool_start".
func (*ToolStart) EventType() string { return EventToolStart }

// ToolDispatch reports that a call has been handed to its tool: it has
// taken a slot in the batch's bound and its turn in its chain, and the
// tool is about to run. tool_start says the call was decided; this
// says it started, which for a batch wider than the bound or serial
// by a tool's request is later, and for a call cut off in between
// never. It is where a session recorder writes the dispatch entry, so
// a call cut off before it reads as never started and one cut off
// after as possibly run. It is raised on the call's own goroutine,
// serialised with the run's events, and a subscriber that fails on it
// stops the call: it ends with that error and the tool does not run,
// so a dispatch that could not be made durable is never followed by a
// side effect the record cannot see. The call is then pending as
// [PendingUndispatched], or still as [PendingAborted] when an earlier
// run dispatched it. Subscribers are called in registration order,
// so a subscriber that vetoes a dispatch is registered before the
// recorder: one registered after it refuses a call whose dispatch is
// already durable. Parent is set for a nested call.
//
// IdempotencyKey is the key the tool receives on agenttool.Call: the
// loop mints one for every call it dispatches and keeps it when the
// call runs again, from the pending call or the [Answer] that approved
// it. A recorder writes it with the dispatch, so a host resuming after
// a restart can hand a keyed tool the key of the dispatch it repeats.
type ToolDispatch struct {
	RunID          string
	Turn           int
	CallID         string
	Name           string
	Parent         string
	IdempotencyKey string
}

// EventType returns "tool_dispatch".
func (*ToolDispatch) EventType() string { return EventToolDispatch }

// ToolUpdate carries progress from a running tool.
type ToolUpdate struct {
	RunID   string
	Turn    int
	CallID  string
	Name    string
	Partial agenttool.Result
}

// EventType returns "tool_update".
func (*ToolUpdate) EventType() string { return EventToolUpdate }

// ToolEnd carries a finished call, in completion order. Err is set when
// the tool failed, the arguments were invalid or no tool had the name;
// Blocked is set when BeforeToolCall refused the call. In both cases
// Result holds the error output the model sees. Deferred is set when
// BeforeToolCall handed the call to the caller: nothing ran, Result is
// empty and no output is appended. A call cancelled by an abort ends
// with the context error as Err; its output is not appended either, and
// the call is listed on RunEnd.Pending.
type ToolEnd struct {
	RunID    string
	Turn     int
	CallID   string
	Name     string
	Result   agenttool.Result
	Err      error
	Blocked  bool
	Deferred bool
	// Reason is the decision's reason for a blocked or a deferred call:
	// the rule that refused it, or the one that raised the question a
	// front now asks the user. Empty otherwise.
	Reason string
	// Parent is the ID of the call whose tool made this one with
	// [Invoke], and empty for a call the model made.
	Parent string
}

// EventType returns "tool_end".
func (*ToolEnd) EventType() string { return EventToolEnd }

// TurnEnd closes a turn with the folded response, usage included, and
// the tool results in the model's order.
type TurnEnd struct {
	RunID       string
	Turn        int
	Response    *openresponses.Response
	ToolResults []agenttool.Result
}

// EventType returns "turn_end".
func (*TurnEnd) EventType() string { return EventTurnEnd }

// StopCause says what ended a run with ReasonStopped.
type StopCause string

// Stop causes.
const (
	// StopMaxTurns: Config.MaxTurns was reached with tools still being
	// called or items still queued, which the next run takes.
	StopMaxTurns StopCause = "max_turns"
	// StopHook: ShouldStopAfterTurn returned true.
	StopHook StopCause = "hook"
	// StopGuard: ShouldStopAfterTurn, BeforeTurn or BeforeModelCall
	// returned an error wrapping [ErrGuard]; the error is on RunEnd.Err.
	// Stopped before the model call, the turn has no turn_start; from
	// BeforeModelCall, the request it refused is on a model_blocked.
	StopGuard StopCause = "guard"
	// StopTerminate: every result of the batch set Terminate, so the
	// tools answered on the model's behalf.
	StopTerminate StopCause = "terminate"
	// StopPartialTerminate: some results of the batch set Terminate and
	// others did not. The run ends so the host can act on the call that
	// asked to end it; the other calls ran and their outputs are in the
	// transcript.
	StopPartialTerminate StopCause = "partial_terminate"
	// StopRefused: an answer built with [Refuse] ended the run instead
	// of calling the model.
	StopRefused StopCause = "refused"
)

// RunEnd closes a run. Exactly one is emitted per run and nothing
// follows it. Items are the items the run appended to the transcript.
// A run refused before it started ([ErrNoPrompt], [ErrCannotContinue],
// [ErrNoModel]) is one RunEnd with ReasonError, an empty RunID and no
// other event.
type RunEnd struct {
	RunID  string
	Items  Transcript
	Reason Reason
	// Cause says what stopped the run when Reason is ReasonStopped, and
	// is empty otherwise.
	Cause StopCause
	// Err is set when Reason is ReasonError; to context.Cause of the
	// run's context when Reason is ReasonAborted, which is the cause
	// [Agent.AbortCause] or the host's own context carried and
	// context.Canceled when there was none, wrapping the failure of a
	// subscriber or a hook when one failed for a reason of its own
	// while the run was being aborted; and to the guard's error when
	// Reason is ReasonStopped with Cause StopGuard.
	Err error
	// Pending lists the function calls in the transcript with no
	// function_call_output, in transcript order, each with why: the
	// calls a deferred decision handed to the caller when Reason is
	// ReasonInputRequired, and the calls an abort or a failure cut off
	// before their outputs were appended. It is empty for ReasonDone
	// and ReasonStopped. The transcript is a valid input again once
	// each has an output, which Agent.Resume appends.
	Pending []PendingCall
}

// EventType returns "run_end".
func (*RunEnd) EventType() string { return EventRunEnd }

// Answer returns the run's answer: the text of the assistant message
// that ends Items, and whether there is one with text. A run whose
// last item is anything else did not answer, whatever it said along
// the way: text before a call is a preamble, and a guard that refuses
// the next turn leaves the preamble last among the messages but not
// last among the items. A message that OutputGuard replaced is the
// replacement, and an empty one is no answer.
func (e *RunEnd) Answer() (string, bool) {
	if len(e.Items) == 0 {
		return "", false
	}
	m, ok := e.Items[len(e.Items)-1].(*openresponses.Message)
	if !ok || m.Role != openresponses.RoleAssistant {
		return "", false
	}
	text := m.Text()
	return text, text != ""
}

// PendingReason says why a call has no output.
type PendingReason string

// Pending reasons.
const (
	// PendingDeferred: BeforeToolCall handed the call to the caller and
	// nothing has answered it. The tool did not run.
	PendingDeferred PendingReason = "deferred"
	// PendingAborted: the run was aborted or failed after the call was
	// handed to its tool, in this run or an earlier one. The tool may
	// have run to completion, so its side effect may have happened, and
	// the call is ambiguous: [Agent.Resume] runs it again only as
	// agenttool's replay rule allows.
	PendingAborted PendingReason = "aborted"
	// PendingUndispatched: the loop did not hand the call to its tool,
	// so the tool did not run: a subscriber refused its tool_dispatch,
	// or the run was aborted or failed before the call's turn. A
	// recorder registered before a subscriber that refused has already
	// written the dispatch; see [ToolDispatch]. Nothing has decided to
	// run it, so an approval of it through [Agent.Resume] is put to
	// BeforeToolCall as the call would be in a run.
	PendingUndispatched PendingReason = "undispatched"
	// PendingUnknown: the call was found without an output in a
	// transcript the agent was seeded with, so the loop cannot say
	// whether it ran. A session recorded with dispatch entries can, and
	// [WithPending] seeds the agent with what it says.
	PendingUnknown PendingReason = "unknown"
	// PendingAnswered: a record says the call was answered without
	// being run again, and stopped before the answer's output: a crash
	// between the two. It is owed its output and nothing else, so
	// [Agent.Resume] answers it only with an output and refuses an
	// approval with [ErrCallAnswered]. Only [WithPending] and
	// [Agent.SetPending] give it; a live run never leaves one.
	PendingAnswered PendingReason = "answered"
	// PendingRejected: a record says a decision refused the call before
	// it reached its tool, and stopped before the refusal's output: a
	// crash between the two. The tool did not run, and the call is owed
	// that refusal as its output and nothing else, so [Agent.Resume]
	// answers it only with an output and refuses an approval with
	// [ErrCallAnswered]. Only [WithPending] and [Agent.SetPending] give
	// it; a live run never leaves one.
	PendingRejected PendingReason = "rejected"
)

// PendingCall is a function call with no output and the reason it has
// none.
type PendingCall struct {
	Call   *openresponses.FunctionCall
	Reason PendingReason
	// Tool is the tool the call resolved to in the run that made it, so
	// a prompt can show what it asks about, its annotations and where
	// it runs; nil for a call no tool has the name of and for a call the
	// run did not make, one found in a seeded transcript.
	Tool agenttool.Tool
	// Dispatched, for a deferred call, says it was handed to its tool
	// before it was held: a record holds a hold after the call's
	// dispatch, so the call may have run and waits on someone to say
	// whether it runs again. Its approval is held to the replay rule as
	// an aborted call's is, with IdempotencyKey and Args. The loop's
	// own BeforeToolCall defers a call before it is handed over, so
	// only [WithPending] and [Agent.SetPending] set it.
	Dispatched bool
	// IdempotencyKey is the key the call carried the last time it was
	// handed to its tool, for a call that may have run, and empty
	// otherwise: the key of the dispatch an approval repeats. An
	// approval of the call through [Agent.Resume] runs it with this
	// key unless the answer carries its own.
	IdempotencyKey string
	// Args are the arguments that hand-off gave the tool, for a call
	// that may have run, which a decision may have rewritten; nil when
	// they are the call's own or it was not handed over. An approval
	// runs it again with them unless the answer carries its own.
	Args json.RawMessage
}

// MayHaveRun reports whether the call may have run and an approval of
// it is held to agenttool's replay rule: it is pending as
// [PendingAborted] or [PendingUnknown], or deferred after it was
// dispatched. A call pending as [PendingAnswered] may have run too,
// but it is owed an output and no approval, as one pending as
// [PendingRejected] is.
func (p PendingCall) MayHaveRun() bool {
	return p.Reason == PendingAborted || p.Reason == PendingUnknown || p.Reason == PendingDeferred && p.Dispatched
}

// PendingCalls returns the calls of pending, in order.
func PendingCalls(pending []PendingCall) []*openresponses.FunctionCall {
	if len(pending) == 0 {
		return nil
	}
	out := make([]*openresponses.FunctionCall, len(pending))
	for i, p := range pending {
		out[i] = p.Call
	}
	return out
}

// QueueMode says which queue an item was accepted into.
type QueueMode string

// Queue modes.
const (
	// QueueSteer is [Agent.Steer]: the item joins the run after the
	// current tool batch, before the next model call.
	QueueSteer QueueMode = "steer"
	// QueueFollowUp is [Agent.FollowUp]: the item joins the run when it
	// would otherwise end.
	QueueFollowUp QueueMode = "follow_up"
)

// Queued reports an item [Agent.Steer] or [Agent.FollowUp] accepted
// into a queue, before any run appends it, so a host writing what it
// accepted can tell an item it was given from one a run produced. It
// belongs to no run: the goroutine that owns delivery reports it at its
// next event, which is why it is not the accept itself and a
// subscriber's error cannot refuse the item.
//
// RunID names the run that was in flight when the item was accepted,
// from the moment it was started, and is empty when the agent was idle
// or the run in flight was past its last drain, after its final
// turn_end or once it had decided to stop, so the item waits for the
// next run. A run named here may still stop before it drains the item,
// which then waits as well.
type Queued struct {
	RunID string
	Item  openresponses.Item
	Mode  QueueMode
	// Hidden is set for an item the caller marked with [Hidden].
	Hidden bool
	// Trigger is what brought the item in, from the context given to
	// [Agent.Queue], zero for [Agent.Steer] and [Agent.FollowUp]: the
	// run start names what started the run, and an input that joins it
	// has a provenance of its own.
	Trigger Trigger
}

// EventType returns "queued".
func (*Queued) EventType() string { return EventQueued }

var (
	_ Event = (*Queued)(nil)
	_ Event = (*RunStart)(nil)
	_ Event = (*TurnStart)(nil)
	_ Event = (*ModelRetry)(nil)
	_ Event = (*ModelBlocked)(nil)
	_ Event = (*ItemStart)(nil)
	_ Event = (*ItemUpdate)(nil)
	_ Event = (*ItemEnd)(nil)
	_ Event = (*ResponseEnd)(nil)
	_ Event = (*ToolStart)(nil)
	_ Event = (*ToolUpdate)(nil)
	_ Event = (*ToolEnd)(nil)
	_ Event = (*TurnEnd)(nil)
	_ Event = (*RunEnd)(nil)
)
