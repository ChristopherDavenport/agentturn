package agentturn

import (
	"encoding/json"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// Event is one step of a run. Concrete types are [RunStart], [TurnStart],
// [ModelRetry], [ModelBlocked], [ItemStart], [ItemUpdate], [ItemEnd],
// [ResponseEnd], [ToolStart], [ToolUpdate], [ToolEnd], [TurnEnd] and
// [RunEnd]. Decoded values are pointers, so switch on *ItemUpdate and so
// on.
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
	EventToolUpdate   = "tool_update"
	EventToolEnd      = "tool_end"
	EventTurnEnd      = "turn_end"
	EventRunEnd       = "run_end"
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
	// ReasonAborted means the context was cancelled. A tool batch cut
	// off by the abort leaves its calls on RunEnd.Pending.
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
// recorder joins them as "kind:ref" when both are set.
type Trigger struct {
	Kind string
	Ref  string
}

// IsZero reports whether the trigger names nothing.
func (t Trigger) IsZero() bool { return t.Kind == "" && t.Ref == "" }

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
// of the turn; no item of the failed attempt reached subscribers.
type ModelRetry struct {
	RunID string
	Turn  int
	// Attempt is the number of the attempt that failed, from 1; the
	// next attempt is Attempt+1.
	Attempt int
	Err     error
	Delay   time.Duration
}

// EventType returns "model_retry".
func (*ModelRetry) EventType() string { return EventModelRetry }

// ModelBlocked reports that [Config.BeforeModelCall] refused the turn's
// request, so no call was made: Request is the request as built when
// the hook ran and Err is the hook's error. It is the last event before
// the run ends with ReasonError, in place of the turn_start the call
// would have had, so a recorder can write the call that was refused as
// distinct from one that was made and failed.
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
type ItemStart struct {
	RunID string
	Turn  int
	Item  openresponses.Item
	// ResponseID is the ID of the response streaming the item, and empty
	// for an item the loop appended itself.
	ResponseID string
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
}

// EventType returns "tool_start".
func (*ToolStart) EventType() string { return EventToolStart }

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
	// called.
	StopMaxTurns StopCause = "max_turns"
	// StopHook: ShouldStopAfterTurn returned true.
	StopHook StopCause = "hook"
	// StopGuard: ShouldStopAfterTurn returned an error wrapping
	// [ErrGuard]; the error is on RunEnd.Err.
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
	// Err is set when Reason is ReasonError; to the context error when
	// Reason is ReasonAborted, wrapping the failure of a subscriber or
	// a hook when one failed for a reason of its own while the run was
	// being aborted, so errors.Is finds the context error either way;
	// and to the guard's error when Reason is ReasonStopped with Cause
	// StopGuard.
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

// PendingReason says why a call has no output.
type PendingReason string

// Pending reasons.
const (
	// PendingDeferred: BeforeToolCall handed the call to the caller and
	// nothing has answered it. The tool did not run.
	PendingDeferred PendingReason = "deferred"
	// PendingAborted: the run was aborted or failed while the call was
	// in flight, after its tool_start. The tool may have run to
	// completion, so its side effect may have happened.
	PendingAborted PendingReason = "aborted"
	// PendingUnknown: the call was found without an output in a
	// transcript the agent was seeded with, so the loop cannot say
	// whether it ran. A session recorded with dispatch entries can.
	PendingUnknown PendingReason = "unknown"
)

// PendingCall is a function call with no output and the reason it has
// none.
type PendingCall struct {
	Call   *openresponses.FunctionCall
	Reason PendingReason
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

var (
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
