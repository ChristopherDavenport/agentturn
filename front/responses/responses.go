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
//     cannot interleave execution with the caller's turn. There is no
//     run: Transform, BeforeModelCall and OutputGuard run as the loop
//     runs them, the guard with no run ID and turn 1, and no other
//     hook does, ShouldStopAfterTurn among them, so a guard that needs
//     to know whether a message is final has nothing to ask here.
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
//
// A full run that a guard stopped (agentturn.StopGuard) with no answer
// (agentturn.RunEnd.Answer) is a refusal: the response ends incomplete
// with incomplete_details.reason content_filter, collected and
// streamed, as a single turn does when BeforeModelCall refuses it with
// an error wrapping agentturn.ErrGuard. That holds whichever hook the
// guard is on: BeforeTurn or BeforeModelCall, on the first turn or on a
// later one after text that preceded a call, and ShouldStopAfterTurn
// after a turn that only called tools. The refused turn adds nothing
// to the output; what was streamed before the guard stopped the run
// stays, since a stream cannot take it back. The guard's error reaches
// the caller in no form, since its text may carry the rule a caller
// could phrase around; the host has it on RunEnd.Err and in the
// record. A run a guard stopped after an answer completes with that
// answer, the OutputGuard's replacement when it made one.
//
// A full run that a terminating tool result stopped
// (agentturn.StopTerminate or agentturn.StopPartialTerminate) is how a
// handoff ends the sender's part. [WithHandoff] is asked for the
// receiver's configuration, and the transcript continues under it
// within the same response: the receiver's items are relayed as the
// sender's were, usage sums both, and the receiver's answer is the
// response's. A receiver that hands off again is asked about in turn,
// with nothing counting the handoffs but the function. Without the
// option, or when it declines, a terminating stop with no answer
// completes with the text of the last function_call_output as an
// assistant message, the answer the tools gave on the model's behalf,
// as tools/agent reports the same stop.
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
	handoff             func(context.Context, *agentturn.RunEnd) (agentturn.Config, bool)
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

// WithHandoff is asked when a full run stops on StopTerminate or
// StopPartialTerminate. A configuration it returns continues the same
// transcript within the same response, under the request's model and
// instructions as the adapter's own configuration is; false ends the
// response as it would without the option. fn finds the destination
// in end.Items, the terminating call and its output; a host that
// routes on a result's Details, which the model never sees, reads them
// from a ToolEnd event its tool or a subscriber kept. It is asked again each time a run it started stops the same way, so
// a pair of agents that hand back and forth runs until fn declines or
// the request's context ends; a host that wants a bound counts on a
// value it puts on that context.
func WithHandoff(fn func(ctx context.Context, end *agentturn.RunEnd) (agentturn.Config, bool)) Option {
	return func(a *Adapter) { a.handoff = fn }
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
	req.Model = a.model(req.Model)
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
	if !agentturn.CanContinue(transcript) {
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
	cfg := a.perRequest(a.cfg, req)
	var usage openresponses.Usage
	var end *agentturn.RunEnd
	for {
		var err error
		if end, err = a.relayRun(ctx, transcript, cfg, rl, &usage); err != nil {
			return err
		}
		next, ok := a.handsOff(ctx, end)
		if !ok {
			break
		}
		transcript = append(transcript, end.Items...)
		cfg = a.perRequest(next, req)
	}
	switch end.Reason {
	case agentturn.ReasonError:
		return end.Err
	case agentturn.ReasonAborted:
		if end.Err != nil {
			return end.Err
		}
		return context.Canceled
	case agentturn.ReasonStopped:
		// A guard that stopped the run before the model answered
		// refused the request, at whichever hook it stopped it: before
		// the first call, on a tool's output before the next, or after
		// a turn that only called tools. A completed response holding
		// no answer, or only a preamble, would read as a success, and a
		// failure as a server error a caller retries. One that stopped
		// it after an answer, which OutputGuard may have replaced,
		// completes with what the caller is to see.
		if _, ok := end.Answer(); end.Cause == agentturn.StopGuard && !ok {
			rl.em.Response().Usage = &usage
			return rl.em.Incomplete(openresponses.IncompleteReasonContentFilter)
		}
		// A terminating result nobody handed off from answered on the
		// model's behalf. Its text is the answer, as tools/agent has
		// it; without it the response would complete empty.
		if _, ok := end.Answer(); terminated(end) && !ok {
			if text, ok := lastOutputText(end.Items); ok {
				if err := rl.end(openresponses.AssistantText(text)); err != nil {
					return err
				}
			}
		}
	case agentturn.ReasonInputRequired:
		// Open Responses has no interrupted state: a response whose
		// output ends with function_call items is the caller's cue to
		// run them and send the outputs back. The deferred calls are
		// emitted here unless the trace already carried them.
		if !a.toolItems {
			for _, call := range end.Pending {
				if err := rl.end(call.Call); err != nil {
					return err
				}
			}
		}
	}
	rl.em.Response().Usage = &usage
	return rl.em.Complete()
}

// relayRun runs cfg over transcript, relaying its items and adding its
// usage, and returns its end.
func (a *Adapter) relayRun(ctx context.Context, transcript agentturn.Transcript, cfg agentturn.Config, rl *relay, usage *openresponses.Usage) (*agentturn.RunEnd, error) {
	var end *agentturn.RunEnd
	for ev := range agentturn.Continue(ctx, transcript, cfg) {
		switch e := ev.(type) {
		case *agentturn.ItemStart:
			if _, ok := e.Item.(*openresponses.FunctionCallOutput); ok || !a.emits(e.Item) {
				continue
			}
			if err := rl.start(e.Item); err != nil {
				return nil, err
			}
		case *agentturn.ItemUpdate:
			if !a.emits(e.Item) {
				continue
			}
			if err := rl.update(e.Stream); err != nil {
				return nil, err
			}
		case *agentturn.ItemEnd:
			if !a.emits(e.Item) {
				continue
			}
			if err := rl.end(e.Item); err != nil {
				return nil, err
			}
		case *agentturn.TurnEnd:
			if e.Response != nil && e.Response.Usage != nil {
				addUsage(usage, *e.Response.Usage)
			}
		case *agentturn.RunEnd:
			end = e
		}
	}
	if end == nil {
		return nil, errors.New("responses: run produced no run_end")
	}
	return end, nil
}

// handsOff asks WithHandoff for the next configuration when end is a
// terminating stop.
func (a *Adapter) handsOff(ctx context.Context, end *agentturn.RunEnd) (agentturn.Config, bool) {
	if a.handoff == nil || !terminated(end) {
		return agentturn.Config{}, false
	}
	return a.handoff(ctx, end)
}

// perRequest returns cfg under the request's model and instructions.
func (a *Adapter) perRequest(cfg agentturn.Config, req openresponses.Request) agentturn.Config {
	cfg.ModelName = modelName(cfg, req.Model)
	cfg.Instructions = a.instructionsFor(cfg, req.Instructions)
	return cfg
}

// terminated reports whether a terminating tool result stopped the run.
func terminated(end *agentturn.RunEnd) bool {
	return end.Reason == agentturn.ReasonStopped && (end.Cause == agentturn.StopTerminate || end.Cause == agentturn.StopPartialTerminate)
}

// lastOutputText returns the text of the last function call output.
func lastOutputText(items agentturn.Transcript) (string, bool) {
	for i := len(items) - 1; i >= 0; i-- {
		if out, ok := items[i].(*openresponses.FunctionCallOutput); ok {
			return out.Output.Text, out.Output.Text != ""
		}
	}
	return "", false
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
			if errors.Is(err, agentturn.ErrGuard) {
				return rl.em.Incomplete(openresponses.IncompleteReasonContentFilter)
			}
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
			item := e.Item
			if m, ok := item.(*openresponses.Message); ok && m.Role == openresponses.RoleAssistant && cfg.OutputGuard != nil {
				// The guard sees the message before the caller does, as
				// the loop's own turns have it.
				out := acc.Response()
				replacement, err := cfg.OutputGuard(ctx, agentturn.OutputInfo{Turn: 1, ResponseID: out.ID, Message: m, Output: append(openresponses.Items(nil), out.Output[:e.OutputIndex]...)})
				if err != nil {
					return fmt.Errorf("output guard: %w", err)
				}
				if replacement != nil {
					item = replacement
				}
			}
			if err := rl.end(item); err != nil {
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

func (a *Adapter) model(requested string) string { return modelName(a.cfg, requested) }

func modelName(cfg agentturn.Config, requested string) string {
	if cfg.ModelName != "" {
		return cfg.ModelName
	}
	return requested
}

func (a *Adapter) instructions(requested string) string {
	return a.instructionsFor(a.cfg, requested)
}

func (a *Adapter) instructionsFor(cfg agentturn.Config, requested string) string {
	switch {
	case !a.requestInstructions || requested == "":
		return cfg.Instructions
	case cfg.Instructions == "":
		return requested
	default:
		return cfg.Instructions + "\n\n" + requested
	}
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
