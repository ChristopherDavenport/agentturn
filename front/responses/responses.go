// Package responses serves an agent as an Open Responses server: the
// loop as an openresponses.Adapter. Behind openresponses.NewHandler it is
// an endpoint; behind openresponses.Client.AsAdapter on the other side
// it is the Model of another loop. That is the "as a model" role of the
// composition story: an agent composes into another agent over the
// network with no second protocol.
//
// The request's input is the conversation. History is always inlined;
// previous_response_id is rejected because the loop keeps no store.
//
// Two modes, decided per request:
//
//   - The request carries function tools. The caller owns them, so the
//     adapter runs exactly one turn: the model sees the agent's tools
//     and the caller's, and any function call it makes is emitted as an
//     output item for the caller to run. Nothing is executed, not even a
//     call to one of the agent's own tools, because a single response
//     cannot interleave execution with the caller's turn.
//   - The request carries no tools. The agent owns the run: it executes
//     its own tools through the loop until the model answers. The
//     response output carries the assistant messages and reasoning the
//     run produced, with the final message last, and usage sums every
//     turn. The function calls the agent ran and their outputs are left
//     out by default, because a caller that is itself a loop would take
//     them for calls it has to answer; [WithToolItems] includes them for
//     callers that want the whole trace.
//
// When a full run stops because BeforeToolCall deferred calls to the
// caller (agentturn.ReasonInputRequired), the response completes with
// those function_call items last in its output, which is how Open
// Responses expresses input required: the caller runs them and sends
// the outputs back with the conversation on its next request, and the
// run continues from there. Only the deferred calls appear, never the
// ones the agent executed itself, unless [WithToolItems] is on.
package responses

import (
	"context"
	"errors"
	"fmt"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// Adapter serves an agent configuration as an openresponses.Adapter.
type Adapter struct {
	cfg                 agentturn.Config
	requestInstructions bool
	toolItems           bool
}

var _ openresponses.Adapter = (*Adapter)(nil)

// Option configures an Adapter.
type Option func(*Adapter)

// WithRequestInstructions appends the request's instructions after the
// agent's own. By default the request's instructions are ignored: the
// agent is described once, by its Config.
func WithRequestInstructions() Option {
	return func(a *Adapter) { a.requestInstructions = true }
}

// WithToolItems includes the function calls the agent executed and
// their outputs in the response output of a full run, in order. Callers
// that feed the output back into a loop should leave this off.
func WithToolItems() Option {
	return func(a *Adapter) { a.toolItems = true }
}

// New builds an adapter over cfg.
func New(cfg agentturn.Config, opts ...Option) *Adapter {
	a := &Adapter{cfg: cfg}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Config returns the agent configuration.
func (a *Adapter) Config() agentturn.Config { return a.cfg }

// Create runs the request and returns the folded response.
func (a *Adapter) Create(ctx context.Context, req openresponses.Request) (*openresponses.Response, error) {
	return openresponses.CollectStream(ctx, a, req)
}

// Compact delegates to the model when it can compact, and reports
// compaction_not_supported otherwise.
func (a *Adapter) Compact(ctx context.Context, req openresponses.CompactRequest) (*openresponses.CompactResponse, error) {
	c, ok := a.cfg.Model.(interface {
		Compact(context.Context, openresponses.CompactRequest) (*openresponses.CompactResponse, error)
	})
	if !ok {
		return nil, openresponses.InvalidRequest(openresponses.CodeCompactionNotSupported, "the model behind this agent does not compact", "")
	}
	if req.Model == "" || a.cfg.ModelName != "" {
		req.Model = a.model(req.Model)
	}
	return c.Compact(ctx, req)
}

// CreateStream streams the response for req into sink.
func (a *Adapter) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	if a.cfg.Model == nil {
		return openresponses.ServerError("no_model", "agent has no model")
	}
	if req.PreviousResponseID != "" {
		return openresponses.PreviousResponseNotFound(req.PreviousResponseID)
	}
	transcript := agentturn.Transcript(append(openresponses.Items(nil), req.Input...))
	if !endsWithPrompt(transcript) {
		return openresponses.InvalidRequest(openresponses.CodeInvalidValue, "input must end with a user message or a function_call_output", "input")
	}
	callerTools := functionTools(req.Tools)
	resp := openresponses.NewResponse(req)
	resp.Model = a.model(req.Model)
	if a.cfg.Instructions != "" {
		instructions := a.instructions(req.Instructions)
		resp.Instructions = &instructions
	}
	rl := newRelay(sink, resp)
	if len(callerTools) > 0 {
		return a.oneTurn(ctx, req, transcript, callerTools, rl)
	}
	return a.fullRun(ctx, req, transcript, rl)
}

// fullRun lets the agent execute its own tools until it answers.
func (a *Adapter) fullRun(ctx context.Context, req openresponses.Request, transcript agentturn.Transcript, rl *relay) error {
	cfg := a.cfg
	cfg.ModelName = a.model(req.Model)
	cfg.Instructions = a.instructions(req.Instructions)
	var usage openresponses.Usage
	var end *agentturn.RunEnd
	for ev, err := range agentturn.Continue(ctx, transcript, cfg) {
		if err != nil && end == nil {
			// A misuse yields no events; a failed run yields its run_end
			// alongside the error and is handled below.
			if ev == nil {
				return err
			}
		}
		switch e := ev.(type) {
		case *agentturn.ItemStart:
			if _, ok := e.Item.(*openresponses.FunctionCallOutput); ok || !a.emits(e.Item) {
				continue
			}
			if err := rl.start(e.Item); err != nil {
				return err
			}
		case *agentturn.ItemUpdate:
			if !a.emits(e.Item) {
				continue
			}
			if err := rl.update(e.Stream); err != nil {
				return err
			}
		case *agentturn.ItemEnd:
			if !a.emits(e.Item) {
				continue
			}
			if err := rl.end(e.Item); err != nil {
				return err
			}
		case *agentturn.TurnEnd:
			if e.Response != nil && e.Response.Usage != nil {
				addUsage(&usage, *e.Response.Usage)
			}
		case *agentturn.RunEnd:
			end = e
		}
	}
	if end == nil {
		return errors.New("responses: run produced no run_end")
	}
	switch end.Reason {
	case agentturn.ReasonError:
		return end.Err
	case agentturn.ReasonAborted:
		if end.Err != nil {
			return end.Err
		}
		return context.Canceled
	case agentturn.ReasonInputRequired:
		// Open Responses has no interrupted state: a response whose
		// output ends with function_call items is the caller's cue to
		// run them and send the outputs back. The deferred calls are
		// emitted here unless the trace already carried them.
		if !a.toolItems {
			for _, call := range end.Pending {
				if err := rl.end(call); err != nil {
					return err
				}
			}
		}
	}
	rl.em.Response().Usage = &usage
	return rl.em.Complete()
}

// oneTurn calls the model once with the agent's and the caller's tools
// and re-emits the response; calls are the caller's to run. The request
// is the same one the loop would send, Config.BaseRequest over the
// per-request model and instructions, so every Config.Request member
// reaches the model in both modes.
func (a *Adapter) oneTurn(ctx context.Context, req openresponses.Request, transcript agentturn.Transcript, callerTools []*openresponses.FunctionTool, rl *relay) error {
	cfg := a.cfg
	cfg.ModelName = a.model(req.Model)
	cfg.Instructions = a.instructions(req.Instructions)
	if cfg.Reasoning.IsZero() {
		cfg.Reasoning = req.Reasoning
	}
	if cfg.Text.IsZero() {
		cfg.Text = req.Text
	}
	// Resolve the tools once: the collision check and the request see
	// the same list.
	cfg.Tools = cfg.ResolveTools(ctx)
	cfg.ToolProvider = nil
	tools := agenttool.Set(cfg.Tools)
	for _, ct := range callerTools {
		if _, ok := tools.Lookup(ct.Name); ok {
			return openresponses.InvalidRequest(openresponses.CodeInvalidValue, fmt.Sprintf("tool %q is owned by the agent", ct.Name), "tools")
		}
	}
	input := transcript
	if cfg.Transform != nil {
		var err error
		input, err = cfg.Transform(ctx, append(agentturn.Transcript(nil), transcript...))
		if err != nil {
			return fmt.Errorf("transform: %w", err)
		}
	}
	filter := cfg.Filter
	if filter == nil {
		filter = agentturn.DefaultFilter
	}
	upstream := cfg.BaseRequest(ctx)
	upstream.Input = filter(input)
	for _, ct := range callerTools {
		upstream.Tools = append(upstream.Tools, ct)
	}
	if req.ToolChoice != (openresponses.ToolChoice{}) {
		upstream.ToolChoice = req.ToolChoice
	}
	if cfg.BeforeModelCall != nil {
		if err := cfg.BeforeModelCall(ctx, &upstream); err != nil {
			return fmt.Errorf("before-model-call hook: %w", err)
		}
	}
	var acc openresponses.Accumulator
	var final *openresponses.Response
	for ev, err := range openresponses.Events(ctx, a.cfg.Model, upstream) {
		if err != nil {
			return err
		}
		acc.Add(ev)
		switch e := ev.(type) {
		case *openresponses.OutputItemAddedEvent:
			if err := rl.start(acc.Response().Output[e.OutputIndex]); err != nil {
				return err
			}
		case *openresponses.OutputItemDoneEvent:
			if err := rl.end(e.Item); err != nil {
				return err
			}
		case *openresponses.ErrorEvent:
			return e.Err()
		default:
			if err := rl.update(ev); err != nil {
				return err
			}
		}
		if r, ok := openresponses.TerminalResponse(ev); ok {
			final = r
		}
	}
	if final == nil {
		return errors.New("responses: model stream ended without a terminal event")
	}
	switch final.Status {
	case openresponses.ResponseStatusFailed:
		if final.Error != nil {
			return final.Error.Err(0)
		}
		return errors.New("responses: model response failed")
	case openresponses.ResponseStatusIncomplete:
		rl.em.Response().Usage = final.Usage
		reason := openresponses.IncompleteReasonMaxOutputTokens
		if final.IncompleteDetails != nil {
			reason = final.IncompleteDetails.Reason
		}
		return rl.em.Incomplete(reason)
	}
	rl.em.Response().Usage = final.Usage
	return rl.em.Complete()
}

// emits reports whether an item of a full run is re-emitted to the
// caller.
func (a *Adapter) emits(item openresponses.Item) bool {
	switch item.(type) {
	case *openresponses.FunctionCall, *openresponses.FunctionCallOutput:
		return a.toolItems
	}
	return true
}

func (a *Adapter) model(requested string) string {
	if a.cfg.ModelName != "" {
		return a.cfg.ModelName
	}
	return requested
}

func (a *Adapter) instructions(requested string) string {
	switch {
	case !a.requestInstructions || requested == "":
		return a.cfg.Instructions
	case a.cfg.Instructions == "":
		return requested
	default:
		return a.cfg.Instructions + "\n\n" + requested
	}
}

func endsWithPrompt(t agentturn.Transcript) bool {
	if len(t) == 0 {
		return false
	}
	switch v := t[len(t)-1].(type) {
	case *openresponses.Message:
		return v.Role != openresponses.RoleAssistant
	case *openresponses.FunctionCallOutput:
		return true
	}
	return false
}

func functionTools(tools openresponses.Tools) []*openresponses.FunctionTool {
	var out []*openresponses.FunctionTool
	for _, t := range tools {
		if ft, ok := t.(*openresponses.FunctionTool); ok {
			out = append(out, ft)
		}
	}
	return out
}

func addUsage(sum *openresponses.Usage, u openresponses.Usage) {
	sum.InputTokens += u.InputTokens
	sum.OutputTokens += u.OutputTokens
	sum.TotalTokens += u.TotalTokens
	sum.InputTokensDetails.CachedTokens += u.InputTokensDetails.CachedTokens
	sum.OutputTokensDetails.ReasoningTokens += u.OutputTokensDetails.ReasoningTokens
}
