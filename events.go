package agentturn

import (
	"encoding/json"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// Event is one step of a run. Concrete types are [RunStart], [TurnStart],
// [ItemStart], [ItemUpdate], [ItemEnd], [ResponseEnd], [ToolStart],
// [ToolUpdate], [ToolEnd], [TurnEnd] and [RunEnd]. Decoded values are pointers, so
// switch on *ItemUpdate and so on.
type Event interface {
	EventType() string
}

// Event type names.
const (
	EventRunStart    = "run_start"
	EventTurnStart   = "turn_start"
	EventResponseEnd = "response_end"
	EventItemStart   = "item_start"
	EventItemUpdate  = "item_update"
	EventItemEnd     = "item_end"
	EventToolStart   = "tool_start"
	EventToolUpdate  = "tool_update"
	EventToolEnd     = "tool_end"
	EventTurnEnd     = "turn_end"
	EventRunEnd      = "run_end"
)

// Reason says why a run ended.
type Reason string

// Run end reasons.
const (
	// ReasonDone: the model produced a final answer with no tool calls
	// and no queued follow-ups.
	ReasonDone Reason = "done"
	// ReasonStopped: ShouldStopAfterTurn, a terminating tool batch or
	// MaxTurns ended the run.
	ReasonStopped Reason = "stopped"
	// ReasonInputRequired: BeforeToolCall deferred one or more calls to
	// the caller; RunEnd.Pending lists them and the run continues once
	// their outputs are appended (see Agent.Resume).
	ReasonInputRequired Reason = "input_required"
	// ReasonAborted: the context was cancelled. A tool batch cut off
	// by the abort leaves its calls on RunEnd.Pending.
	ReasonAborted Reason = "aborted"
	// ReasonError: the model, a hook or a subscriber failed; RunEnd.Err
	// says which.
	ReasonError Reason = "error"
)

// RunStart opens a run.
type RunStart struct {
	RunID string
}

// EventType returns "run_start".
func (*RunStart) EventType() string { return EventRunStart }

// TurnStart opens a turn and carries the exact request sent to the
// model.
type TurnStart struct {
	RunID   string
	Turn    int
	Request openresponses.Request
}

// EventType returns "turn_start".
func (*TurnStart) EventType() string { return EventTurnStart }

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
type ToolStart struct {
	RunID  string
	Turn   int
	CallID string
	Name   string
	Args   json.RawMessage
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

// RunEnd closes a run. Exactly one is emitted per run and nothing
// follows it. Items are the items the run appended to the transcript.
type RunEnd struct {
	RunID  string
	Items  Transcript
	Reason Reason
	// Err is set when Reason is ReasonError, and to the context error
	// when Reason is ReasonAborted.
	Err error
	// Pending lists the function calls of the run with no
	// function_call_output, in transcript order: the calls a deferred
	// decision handed to the caller when Reason is ReasonInputRequired,
	// and the calls an abort or a failure cut off before their outputs
	// were appended. It is empty for ReasonDone and ReasonStopped. The
	// transcript is a valid input again once each has an output, which
	// Agent.Resume appends.
	Pending []*openresponses.FunctionCall
}

// EventType returns "run_end".
func (*RunEnd) EventType() string { return EventRunEnd }

var (
	_ Event = (*RunStart)(nil)
	_ Event = (*TurnStart)(nil)
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
