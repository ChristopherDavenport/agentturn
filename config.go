package agentturn

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"syscall"
	"time"

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
	// MCP server, is picked up on the next turn. It is called once per
	// turn, before the model call, and the same list serves the turn's
	// tool batch, so a call resolves against the tools the model was
	// offered.
	//
	// A provider that returns a snapshot offers a change one turn late
	// when the change is still in flight as the turn starts: a tool
	// whose result announces a new tool returns before the refresh that
	// fetches it has completed. A provider that must offer the change
	// on the very next call waits for it here, bounded by ctx, as
	// mcpclient's Await does:
	//
	//	cfg.ToolProvider = func(ctx context.Context) []agenttool.Tool {
	//		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	//		defer cancel()
	//		_ = remote.Await(ctx)
	//		return remote.Tools()
	//	}
	ToolProvider func(ctx context.Context) []agenttool.Tool

	// Reasoning is the reasoning field of every request: effort and
	// summary. The zero value leaves the request's own.
	Reasoning openresponses.ReasoningConfig
	// Text is the text field of every request: output format and
	// verbosity. The zero value leaves the request's own.
	Text openresponses.TextConfig

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

	// Retry is the policy for transient model failures. The zero value
	// retries nothing; see [Retry].
	Retry Retry

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

// Retry says when a failed model call is attempted again. A retry
// happens inside the turn: the same request is sent again after a
// delay, a [ModelRetry] event tells subscribers, and the turn_start
// and response_end of the turn are delivered once. Only an attempt
// that delivered nothing is retried: once an item of the attempt has
// reached subscribers, or the server has answered with a failed
// response, the failure is final, because the transcript or a
// recorder may already hold part of it. Abort cuts a delay short.
type Retry struct {
	// MaxAttempts is the number of attempts per turn, the first
	// included. Zero or one means no retry.
	MaxAttempts int
	// Backoff returns how long to wait before the next attempt, given
	// the number of the attempt that failed and its error. nil means
	// [DefaultBackoff].
	Backoff func(attempt int, err error) time.Duration
	// Retryable reports whether err is worth another attempt. nil means
	// [DefaultRetryable].
	Retryable func(error) bool
}

func (r Retry) retryable(err error) bool {
	if r.Retryable != nil {
		return r.Retryable(err)
	}
	return DefaultRetryable(err)
}

func (r Retry) backoff(attempt int, err error) time.Duration {
	if r.Backoff != nil {
		return r.Backoff(attempt, err)
	}
	return DefaultBackoff(attempt, err)
}

// DefaultRetryable is the [Retry.Retryable] used when none is set: an
// openresponses error with status 408, 409, 429 or 5xx, a stream that
// ended before its terminal event, and transport failures (net.Error,
// an unexpected EOF, a reset or refused connection). Everything else,
// a 4xx in particular, is final.
func DefaultRetryable(err error) bool {
	var oe *openresponses.Error
	if errors.As(err, &oe) {
		switch status := oe.HTTPStatus(); {
		case status == 408, status == 409, status == 429, status >= 500:
			return true
		}
		return false
	}
	if errors.Is(err, openresponses.ErrTruncatedStream) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return true
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne)
}

// DefaultBackoff is the [Retry.Backoff] used when none is set: the
// Retry-After header of an openresponses error when it names a number
// of seconds, otherwise 500ms doubled per retry and capped at 30s,
// without jitter.
func DefaultBackoff(attempt int, err error) time.Duration {
	var oe *openresponses.Error
	if errors.As(err, &oe) {
		if secs, perr := strconv.Atoi(strings.TrimSpace(oe.Headers.Get("Retry-After"))); perr == nil && secs >= 0 {
			return time.Duration(secs) * time.Second
		}
	}
	d := 500 * time.Millisecond
	for i := 1; i < attempt && d < 30*time.Second; i++ {
		d *= 2
	}
	return min(d, 30*time.Second)
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
	// By names who decided, for the record: the session format knows
	// "human" for a person the hook waited on, "policy" for a rule it
	// evaluated on its own and "agent" for another model. Empty is read
	// as policy. The loop does not use it.
	By string
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
// when it is set, Tools otherwise. The loop consults it once per turn,
// before the model call; a front that builds its own request uses it
// so the provider fallback lives in one place.
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
