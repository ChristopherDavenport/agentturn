package agentturn

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// Model is anything that can stream an Open Responses response.
type Model = openresponses.Streamer

// Transcript is the conversation as the model sees it, plus app-only
// items that the [Config.Filter] removes before each call.
type Transcript = openresponses.Items

// ExecutionMode selects how a batch of tool calls runs.
type ExecutionMode int

const (
	// ExecParallel runs the calls of a batch concurrently, bounded by
	// [Config.MaxParallelTools], unless a tool in the batch is
	// agenttool.Sequential.
	ExecParallel ExecutionMode = iota
	// ExecSequential runs every batch one call at a time in the model's
	// order.
	ExecSequential
)

// Config describes an agent. The zero value is not usable: Model is
// required.
type Config struct {
	// Name is how the agent names itself when composed: the tool name in
	// tools/agent, the server name in front/mcp, the agent card in
	// front/a2a.
	Name string
	// Description is one paragraph about the agent, used the same way.
	Description string

	// Model streams responses. Required.
	Model Model
	// ModelName is the model field of every request.
	ModelName string
	// Instructions is the instructions field of every request.
	Instructions string

	// Tools the model may call. The loop reads it at the start of each
	// turn.
	Tools []agenttool.Tool
	// ToolProvider, when set, supplies the tools for each turn in place
	// of Tools, so a source whose tool list changes, such as a remote
	// MCP server, is picked up on the next turn.
	ToolProvider func(ctx context.Context) []agenttool.Tool

	Reasoning openresponses.ReasoningConfig
	Text      openresponses.TextConfig

	// Request is the base of every request the loop sends: tool_choice,
	// parallel_tool_calls, max_output_tokens, temperature, truncation,
	// include, safety_identifier, prompt_cache_key, service_tier and
	// every other member are copied from it. The loop owns input, tools,
	// store, stream and previous_response_id, and ModelName,
	// Instructions, Reasoning and Text take precedence over the same
	// members here when set. The loop adds nothing else: the run and
	// turn IDs are on every event, not on the request, so the settings
	// the model sees change only when this config does.
	Request openresponses.Request

	// BeforeModelCall runs on the fully built request of each turn, just
	// before it is sent and before turn_start reports it. It may change
	// anything; a change to Input has the same effect on session
	// verification as a Transform.
	BeforeModelCall func(context.Context, *openresponses.Request) error

	// ToolExecution selects parallel (default) or sequential batches.
	ToolExecution ExecutionMode
	// MaxParallelTools bounds a parallel batch; zero means
	// agenttool.DefaultMaxParallel.
	MaxParallelTools int
	// MaxTurns stops a run after this many turns with ReasonStopped;
	// zero means no limit.
	MaxTurns int

	// Filter drops app-only items before each model call and returns
	// the conversation the model sees. nil means [DefaultFilter].
	Filter func(Transcript) Transcript
	// Transform runs before each model call on the whole transcript and
	// may return a shorter or otherwise edited one for that call only:
	// prune, compact, inject context. It receives a copy of the slice
	// and must not mutate the items. The working transcript is not
	// replaced.
	Transform func(context.Context, Transcript) (Transcript, error)

	// BeforeToolCall runs once per call, in the model's order, before any
	// call of the batch executes. A nil decision allows the call.
	BeforeToolCall func(context.Context, ToolCallInfo) (*ToolDecision, error)
	// AfterToolCall runs when a call completes and may replace its
	// result. A nil override keeps the result.
	AfterToolCall func(context.Context, ToolResultInfo) (*ToolOverride, error)
	// ShouldStopAfterTurn ends the run after a turn even when the model
	// requested tools.
	ShouldStopAfterTurn func(context.Context, TurnInfo) (bool, error)

	// RequestExtra is passed through as Request.Extra on every call.
	RequestExtra map[string]any
}

// ToolCallInfo describes a call before it runs.
type ToolCallInfo struct {
	RunID string
	Turn  int
	// Call is the function_call item as the model produced it.
	Call *openresponses.FunctionCall
	// Tool is the tool that will run, or nil when no tool has that
	// name; the call then fails with an error output unless the hook
	// blocks it first.
	Tool agenttool.Tool
	// Args are the arguments as raw JSON.
	Args json.RawMessage
}

// ToolAction is what BeforeToolCall decides for a call.
type ToolAction int

const (
	// Allow runs the call. It is the zero value, so a decision that only
	// rewrites Args or sets Terminate allows the call.
	Allow ToolAction = iota
	// Block refuses the call. The model sees ToolDecision.Reason as the
	// error output.
	Block
	// Defer hands the call to the caller instead of running it: the
	// other calls of the batch proceed, no output is appended for this
	// one, and the run ends with ReasonInputRequired listing it. The
	// caller appends the output later and continues.
	Defer
)

// ToolDecision is a hook's verdict on a call.
type ToolDecision struct {
	// Action allows, blocks or defers the call.
	Action ToolAction
	// Reason is the message the model sees when Action is Block.
	Reason string
	// Terminate hints the loop to stop after the batch, as a tool result
	// would. It composes with Allow and Block.
	Terminate bool
	// Args, when non-nil, replaces the arguments the tool receives.
	Args json.RawMessage
}

// ToolResultInfo describes a completed call.
type ToolResultInfo struct {
	RunID  string
	Turn   int
	Call   *openresponses.FunctionCall
	Tool   agenttool.Tool
	Args   json.RawMessage
	Result agenttool.Result
	Err    error
}

// ToolOverride replaces the result of a call.
type ToolOverride struct {
	Result agenttool.Result
	Err    error
}

// TurnInfo describes a finished turn.
type TurnInfo struct {
	RunID    string
	Turn     int
	Response *openresponses.Response
	// ToolResults are the results of this turn's calls in the model's
	// order, empty when the model called no tools.
	ToolResults []agenttool.Result
	// Transcript is the working transcript after the turn. It is the
	// loop's live slice; do not mutate it or keep it past the hook.
	Transcript Transcript
}

// DefaultFilter drops every item whose type carries a slug prefix such
// as "agentturn:note", which marks an app-only extension item, and nil
// items. It is the Filter when Config.Filter is nil.
func DefaultFilter(t Transcript) Transcript {
	return VisibleFilter()(t)
}

// VisibleFilter returns a filter like [DefaultFilter] that keeps the
// listed extension item types.
func VisibleFilter(visible ...string) func(Transcript) Transcript {
	keep := make(map[string]bool, len(visible))
	for _, v := range visible {
		keep[v] = true
	}
	return func(t Transcript) Transcript {
		out := make(Transcript, 0, len(t))
		for _, item := range t {
			if item == nil {
				continue
			}
			typ := item.ItemType()
			if strings.Contains(typ, ":") && !keep[typ] {
				continue
			}
			out = append(out, item)
		}
		return out
	}
}

// tools resolves the tool list for a turn.
func (c Config) tools(ctx context.Context) agenttool.Set {
	return agenttool.Set(c.ResolveTools(ctx))
}

// ResolveTools returns the tools of the moment: ToolProvider's answer
// when it is set, Tools otherwise. The loop consults it once per turn;
// a front that builds its own request uses it so the provider fallback
// lives in one place.
func (c Config) ResolveTools(ctx context.Context) []agenttool.Tool {
	if c.ToolProvider != nil {
		return c.ToolProvider(ctx)
	}
	return c.Tools
}

func (c Config) filter() func(Transcript) Transcript {
	if c.Filter != nil {
		return c.Filter
	}
	return DefaultFilter
}
