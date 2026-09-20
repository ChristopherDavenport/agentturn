package agentturn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// Errors returned before a run starts.
var (
	// ErrNoModel: Config.Model is nil.
	ErrNoModel = errors.New("agentturn: config has no model")
	// ErrCannotContinue: the transcript does not end with a user message
	// or a function call output, so there is nothing for the model to
	// answer.
	ErrCannotContinue = errors.New("agentturn: transcript must end with a user message or a function call output to continue")
	// ErrNoPrompt: Run was called with no prompt items.
	ErrNoPrompt = errors.New("agentturn: no prompt items")
)

// Run appends prompts to the transcript and drives the loop until the
// agent goes idle. Events are yielded in order; the loop runs ahead of
// the consumer and never waits between phases. A fatal failure arrives
// once, as the final pair: a [RunEnd] with ReasonError together with
// its error. A misuse before the run starts is yielded as (nil, err)
// with no events at all. Breaking out of the loop cancels the run.
//
// The transcript is not modified; the items the run appended are on the
// RunEnd.
func Run(ctx context.Context, t Transcript, prompts openresponses.Items, cfg Config) iter.Seq2[Event, error] {
	if len(prompts) == 0 {
		return failNow(ErrNoPrompt)
	}
	return observe(ctx, t, prompts, cfg)
}

// Continue drives the loop from the transcript as it stands. The last
// item must be a user message or a function call output.
func Continue(ctx context.Context, t Transcript, cfg Config) iter.Seq2[Event, error] {
	if !canContinue(t) {
		return failNow(ErrCannotContinue)
	}
	return observe(ctx, t, nil, cfg)
}

func failNow(err error) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) { yield(nil, err) }
}

// canContinue reports whether the model has something to answer.
func canContinue(t Transcript) bool {
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

// unansweredCalls returns the function calls after the last user
// message that have no function_call_output, in transcript order. A
// transcript is a valid input only when this is empty.
func unansweredCalls(t Transcript) []*openresponses.FunctionCall {
	start := 0
	for i := len(t) - 1; i >= 0; i-- {
		if m, ok := t[i].(*openresponses.Message); ok && m.Role != openresponses.RoleAssistant {
			start = i + 1
			break
		}
	}
	answered := map[string]bool{}
	for _, item := range t[start:] {
		if out, ok := item.(*openresponses.FunctionCallOutput); ok {
			answered[out.CallID] = true
		}
	}
	var calls []*openresponses.FunctionCall
	for _, item := range t[start:] {
		if call, ok := item.(*openresponses.FunctionCall); ok && !answered[call.CallID] {
			calls = append(calls, call)
		}
	}
	return calls
}

type observed struct {
	ev  Event
	err error
}

// observe runs the loop in a goroutine and yields its events from the
// caller's.
func observe(ctx context.Context, t Transcript, prompts openresponses.Items, cfg Config) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		if err := cfg.validate(); err != nil {
			yield(nil, err)
			return
		}
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		events := make(chan observed, 256)
		go func() {
			defer close(events)
			r := &runner{
				cfg:        cfg,
				transcript: append(Transcript(nil), t...),
				emit: func(ev Event) error {
					o := observed{ev: ev}
					if end, ok := ev.(*RunEnd); ok {
						o.err = end.Err
						// The run_end is delivered even after the consumer
						// left, without blocking forever.
						select {
						case events <- o:
						default:
							select {
							case events <- o:
							case <-ctx.Done():
							}
						}
						return nil
					}
					select {
					case events <- o:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				},
			}
			r.run(ctx, prompts)
		}()
		stopped := false
		for o := range events {
			if stopped {
				continue
			}
			if _, isEnd := o.ev.(*RunEnd); isEnd {
				yield(o.ev, o.err)
				stopped = true
				continue
			}
			if !yield(o.ev, nil) {
				stopped = true
				cancel()
			}
		}
	}
}

func (c Config) validate() error {
	if c.Model == nil {
		return ErrNoModel
	}
	if c.ToolProvider == nil {
		if err := agenttool.Set(c.Tools).Validate(); err != nil {
			return fmt.Errorf("agentturn: %w", err)
		}
	}
	return nil
}

// BaseRequest returns the request every turn starts from: Config.Request
// with the loop-owned transport members set (store false, stream true,
// no previous_response_id), ModelName, Instructions, Reasoning and Text
// applied over it, RequestExtra merged over its Extra, and the tool
// definitions of the moment. Only Input is added by the loop; a
// recorder uses this to write the initial settings.
func (c Config) BaseRequest(ctx context.Context) openresponses.Request {
	return c.baseRequest(c.tools(ctx))
}

// baseRequest is BaseRequest over tools already resolved, so the loop
// consults ToolProvider once per turn.
func (c Config) baseRequest(tools agenttool.Set) openresponses.Request {
	store := false
	req := c.Request
	req.Input = nil
	req.Tools = tools.Definitions()
	req.Store = &store
	req.Stream = true
	req.StreamOptions = nil
	req.PreviousResponseID = ""
	if c.ModelName != "" {
		req.Model = c.ModelName
	}
	if c.Instructions != "" {
		req.Instructions = c.Instructions
	}
	if !c.Reasoning.IsZero() {
		req.Reasoning = c.Reasoning
	}
	if !c.Text.IsZero() {
		req.Text = c.Text
	}
	if len(c.Request.Metadata) > 0 {
		req.Metadata = make(map[string]string, len(c.Request.Metadata))
		for k, v := range c.Request.Metadata {
			req.Metadata[k] = v
		}
	}
	if len(c.Request.Extra) > 0 || len(c.RequestExtra) > 0 {
		req.Extra = make(map[string]any, len(c.Request.Extra)+len(c.RequestExtra))
		for k, v := range c.Request.Extra {
			req.Extra[k] = v
		}
		for k, v := range c.RequestExtra {
			req.Extra[k] = v
		}
	}
	if len(c.Request.Include) > 0 {
		req.Include = append([]openresponses.Include(nil), c.Request.Include...)
	}
	return req
}

// runner is one run of the loop. emit delivers an event and returns an
// error to abort the run; steer and followUp drain the queues when set.
type runner struct {
	cfg        Config
	transcript Transcript
	emit       func(Event) error
	steer      func() openresponses.Items
	followUp   func() openresponses.Items

	runID string
	turn  int
	added Transcript
}

// errStop carries a run end reason out of a phase.
type errStop struct {
	reason Reason
	err    error
}

func (e *errStop) Error() string {
	if e.err != nil {
		return string(e.reason) + ": " + e.err.Error()
	}
	return string(e.reason)
}

func (r *runner) stop(reason Reason, err error) error { return &errStop{reason: reason, err: err} }

// run drives the loop and returns the RunEnd; it never panics out of a
// phase without one.
func (r *runner) run(ctx context.Context, prompts openresponses.Items) *RunEnd {
	r.runID = openresponses.NewID("run")
	end := &RunEnd{RunID: r.runID, Reason: ReasonDone}
	err := r.loop(ctx, prompts)
	var stop *errStop
	switch {
	case err == nil:
	case errors.As(err, &stop):
		end.Reason = stop.reason
		end.Err = stop.err
	case ctx.Err() != nil:
		end.Reason = ReasonAborted
		end.Err = ctx.Err()
	default:
		end.Reason = ReasonError
		end.Err = err
	}
	end.Items = r.added
	// Whatever ended the run, the calls without an output are the
	// caller's to answer: the deferred ones on input_required, and the
	// ones an abort or a failure cut off before their outputs were
	// appended.
	end.Pending = unansweredCalls(r.transcript)
	// A subscriber that fails on run_end cannot change the outcome; the
	// run has already ended.
	_ = r.emit(end)
	return end
}

func (r *runner) loop(ctx context.Context, prompts openresponses.Items) error {
	if err := r.emit(&RunStart{RunID: r.runID}); err != nil {
		return err
	}
	if err := r.appendItems(prompts); err != nil {
		return err
	}
	for {
		if r.cfg.MaxTurns > 0 && r.turn >= r.cfg.MaxTurns {
			return r.stop(ReasonStopped, nil)
		}
		if err := ctx.Err(); err != nil {
			return r.stop(ReasonAborted, err)
		}
		r.turn++
		tools := r.cfg.tools(ctx)
		resp, err := r.modelTurn(ctx, tools)
		if err != nil {
			return err
		}
		calls := resp.FunctionCalls()
		results, pending, err := r.toolBatch(ctx, tools, calls)
		if err != nil {
			return err
		}
		if err := r.emit(&TurnEnd{RunID: r.runID, Turn: r.turn, Response: resp, ToolResults: results}); err != nil {
			return err
		}
		if len(pending) > 0 {
			return r.stop(ReasonInputRequired, nil)
		}
		if r.cfg.ShouldStopAfterTurn != nil {
			stop, err := r.cfg.ShouldStopAfterTurn(ctx, TurnInfo{RunID: r.runID, Turn: r.turn, Response: resp, ToolResults: results, Transcript: r.transcript})
			if err != nil {
				return fmt.Errorf("should-stop-after-turn hook: %w", err)
			}
			if stop {
				return r.stop(ReasonStopped, nil)
			}
		}
		if len(calls) > 0 && allTerminate(results) {
			return r.stop(ReasonStopped, nil)
		}
		queued := r.drain(r.steer)
		if len(calls) == 0 {
			queued = append(queued, r.drain(r.followUp)...)
			if len(queued) == 0 {
				return nil
			}
		}
		if err := r.appendItems(queued); err != nil {
			return err
		}
	}
}

func (r *runner) drain(q func() openresponses.Items) openresponses.Items {
	if q == nil {
		return nil
	}
	return q()
}

func allTerminate(results []agenttool.Result) bool {
	for _, res := range results {
		if !res.Terminate {
			return false
		}
	}
	return len(results) > 0
}

// appendItems adds items the loop did not stream (prompts, queued
// messages, tool outputs) to the transcript with their item events.
func (r *runner) appendItems(items openresponses.Items) error {
	for _, item := range items {
		if item == nil {
			continue
		}
		if err := r.emit(&ItemStart{RunID: r.runID, Turn: r.turn, Item: item}); err != nil {
			return err
		}
		r.transcript = append(r.transcript, item)
		r.added = append(r.added, item)
		if err := r.emit(&ItemEnd{RunID: r.runID, Turn: r.turn, Item: item}); err != nil {
			return err
		}
	}
	return nil
}

// request builds the request for this turn from config and the
// filtered, transformed transcript.
func (r *runner) request(ctx context.Context, tools agenttool.Set) (openresponses.Request, error) {
	input := Transcript(append(openresponses.Items(nil), r.transcript...))
	if r.cfg.Transform != nil {
		var err error
		input, err = r.cfg.Transform(ctx, input)
		if err != nil {
			return openresponses.Request{}, fmt.Errorf("transform: %w", err)
		}
	}
	req := r.cfg.baseRequest(tools)
	req.Input = r.cfg.filter()(input)
	if r.cfg.BeforeModelCall != nil {
		if err := r.cfg.BeforeModelCall(ctx, &req); err != nil {
			return openresponses.Request{}, fmt.Errorf("before-model-call hook: %w", err)
		}
	}
	return req, nil
}

// modelTurn streams one response, emitting item events, and appends the
// completed items to the transcript.
func (r *runner) modelTurn(ctx context.Context, tools agenttool.Set) (*openresponses.Response, error) {
	req, err := r.request(ctx, tools)
	if err != nil {
		return nil, err
	}
	if err := r.emit(&TurnStart{RunID: r.runID, Turn: r.turn, Request: req}); err != nil {
		return nil, err
	}
	var acc openresponses.Accumulator
	var resp *openresponses.Response
	for ev, err := range openresponses.Events(ctx, r.cfg.Model, req) {
		if err != nil {
			if ctx.Err() != nil {
				return nil, r.stop(ReasonAborted, ctx.Err())
			}
			return nil, fmt.Errorf("model: %w", err)
		}
		acc.Add(ev)
		responseID := ""
		if cur := acc.Response(); cur != nil {
			responseID = cur.ID
		}
		switch e := ev.(type) {
		case *openresponses.OutputItemAddedEvent:
			if err := r.emit(&ItemStart{RunID: r.runID, Turn: r.turn, Item: acc.Response().Output[e.OutputIndex], ResponseID: responseID}); err != nil {
				return nil, err
			}
		case *openresponses.OutputItemDoneEvent:
			r.transcript = append(r.transcript, e.Item)
			r.added = append(r.added, e.Item)
			if err := r.emit(&ItemEnd{RunID: r.runID, Turn: r.turn, Item: e.Item, ResponseID: responseID}); err != nil {
				return nil, err
			}
		case *openresponses.ErrorEvent:
			return nil, fmt.Errorf("model: %w", e.Err())
		default:
			if idx, ok := outputIndex(ev); ok {
				if err := r.emit(&ItemUpdate{RunID: r.runID, Turn: r.turn, Item: acc.Response().Output[idx], Stream: ev, ResponseID: responseID}); err != nil {
					return nil, err
				}
			}
		}
		if final, ok := openresponses.TerminalResponse(ev); ok {
			resp = final
		}
	}
	if resp == nil {
		if ctx.Err() != nil {
			return nil, r.stop(ReasonAborted, ctx.Err())
		}
		return nil, errors.New("model: stream ended without a terminal event")
	}
	if err := r.emit(&ResponseEnd{RunID: r.runID, Turn: r.turn, Response: resp}); err != nil {
		return nil, err
	}
	if resp.Status == openresponses.ResponseStatusFailed {
		if resp.Error != nil {
			return nil, fmt.Errorf("model: %w", resp.Error.Err(0))
		}
		return nil, errors.New("model: response failed")
	}
	return resp, nil
}

// outputIndex returns the output index an item-scoped event refers to.
func outputIndex(ev openresponses.StreamEvent) (int, bool) {
	switch e := ev.(type) {
	case *openresponses.ContentPartAddedEvent:
		return e.OutputIndex, true
	case *openresponses.ContentPartDoneEvent:
		return e.OutputIndex, true
	case *openresponses.OutputTextDeltaEvent:
		return e.OutputIndex, true
	case *openresponses.OutputTextDoneEvent:
		return e.OutputIndex, true
	case *openresponses.RefusalDeltaEvent:
		return e.OutputIndex, true
	case *openresponses.RefusalDoneEvent:
		return e.OutputIndex, true
	case *openresponses.FunctionCallArgumentsDeltaEvent:
		return e.OutputIndex, true
	case *openresponses.FunctionCallArgumentsDoneEvent:
		return e.OutputIndex, true
	case *openresponses.ReasoningSummaryPartAddedEvent:
		return e.OutputIndex, true
	case *openresponses.ReasoningSummaryPartDoneEvent:
		return e.OutputIndex, true
	case *openresponses.ReasoningSummaryTextDeltaEvent:
		return e.OutputIndex, true
	case *openresponses.ReasoningSummaryTextDoneEvent:
		return e.OutputIndex, true
	case *openresponses.ReasoningDeltaEvent:
		return e.OutputIndex, true
	case *openresponses.ReasoningDoneEvent:
		return e.OutputIndex, true
	case *openresponses.OutputTextAnnotationAddedEvent:
		return e.OutputIndex, true
	}
	return 0, false
}

// pending is one call of a batch through preflight and execution.
type pending struct {
	call    *openresponses.FunctionCall
	tool    agenttool.Tool
	args    json.RawMessage
	result  agenttool.Result
	err     error
	blocked bool
	// settled is set when preflight produced the result and the call
	// does not execute.
	settled bool
	// deferred is set when the caller owns the call; no output is
	// appended.
	deferred bool
}

// toolBatch runs the calls of a turn: preflight in order, execute,
// then append the outputs in the model's order. It returns the results
// in that order and the calls a hook deferred to the caller.
func (r *runner) toolBatch(ctx context.Context, tools agenttool.Set, calls []*openresponses.FunctionCall) ([]agenttool.Result, []*openresponses.FunctionCall, error) {
	if len(calls) == 0 {
		return nil, nil, nil
	}
	batch := make([]*pending, len(calls))
	for i, call := range calls {
		p, err := r.preflight(ctx, tools, call)
		if err != nil {
			return nil, nil, err
		}
		batch[i] = p
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, r.stop(ReasonAborted, err)
	}

	var jobs []agenttool.Job
	var jobIndex []int
	for i, p := range batch {
		if p.settled {
			continue
		}
		jobs = append(jobs, agenttool.Job{Tool: p.tool, Call: agenttool.Call{ID: p.call.CallID, Args: p.args}})
		jobIndex = append(jobIndex, i)
	}
	exec := agenttool.Executor{MaxParallel: r.cfg.MaxParallelTools, Sequential: r.cfg.ToolExecution == Sequential}
	var emitErr error
	for ev := range exec.Execute(ctx, jobs) {
		p := batch[jobIndex[ev.Index]]
		if !ev.Final {
			if emitErr = r.emit(&ToolUpdate{RunID: r.runID, Turn: r.turn, CallID: p.call.CallID, Name: p.call.Name, Partial: ev.Result}); emitErr != nil {
				break
			}
			continue
		}
		p.result, p.err = ev.Result, ev.Err
		if emitErr = r.settle(ctx, p); emitErr != nil {
			break
		}
	}
	if emitErr != nil {
		return nil, nil, emitErr
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, r.stop(ReasonAborted, err)
	}

	results := make([]agenttool.Result, len(batch))
	outputs := make(openresponses.Items, 0, len(batch))
	var deferred []*openresponses.FunctionCall
	for i, p := range batch {
		results[i] = p.result
		if p.deferred {
			deferred = append(deferred, p.call)
			continue
		}
		outputs = append(outputs, &openresponses.FunctionCallOutput{CallID: p.call.CallID, Output: p.result.Output})
	}
	if err := r.appendItems(outputs); err != nil {
		return nil, nil, err
	}
	return results, deferred, nil
}

// preflight resolves the tool, checks the arguments and runs
// BeforeToolCall. It emits tool_start and, for a call that will not
// execute, tool_end.
func (r *runner) preflight(ctx context.Context, tools agenttool.Set, call *openresponses.FunctionCall) (*pending, error) {
	p := &pending{call: call, args: json.RawMessage(call.Arguments)}
	if len(p.args) == 0 {
		p.args = json.RawMessage("{}")
	}
	p.tool, _ = tools.Lookup(call.Name)

	var terminate bool
	if r.cfg.BeforeToolCall != nil {
		decision, err := r.cfg.BeforeToolCall(ctx, ToolCallInfo{RunID: r.runID, Turn: r.turn, Call: call, Tool: p.tool, Args: p.args})
		if err != nil {
			return nil, fmt.Errorf("before-tool-call hook: %w", err)
		}
		if decision != nil {
			if decision.Args != nil {
				p.args = decision.Args
			}
			terminate = decision.Terminate
			switch {
			case decision.Block:
				reason := decision.Reason
				if reason == "" {
					reason = "call blocked"
				}
				p.blocked = true
				p.err = errors.New(reason)
			case decision.Defer:
				p.deferred = true
			}
		}
	}
	if err := r.emit(&ToolStart{RunID: r.runID, Turn: r.turn, CallID: call.CallID, Name: call.Name, Args: p.args}); err != nil {
		return nil, err
	}
	if p.deferred {
		p.settled = true
		return p, r.emit(&ToolEnd{RunID: r.runID, Turn: r.turn, CallID: call.CallID, Name: call.Name, Deferred: true})
	}
	switch {
	case p.blocked:
	case p.tool == nil:
		p.err = fmt.Errorf("unknown tool %q", call.Name)
	case !validObject(p.args):
		p.err = errors.New("invalid arguments: expected a JSON object")
	}
	if p.err != nil {
		p.settled = true
		p.result = agenttool.ErrorResult(p.err)
		p.result.Terminate = terminate
		return p, r.settle(ctx, p)
	}
	return p, nil
}

// settle applies AfterToolCall, normalises an error into the output the
// model sees and emits tool_end.
func (r *runner) settle(ctx context.Context, p *pending) error {
	if r.cfg.AfterToolCall != nil && !p.blocked {
		override, err := r.cfg.AfterToolCall(ctx, ToolResultInfo{RunID: r.runID, Turn: r.turn, Call: p.call, Tool: p.tool, Args: p.args, Result: p.result, Err: p.err})
		if err != nil {
			return fmt.Errorf("after-tool-call hook: %w", err)
		}
		if override != nil {
			p.result, p.err = override.Result, override.Err
		}
	}
	if p.err != nil {
		terminate := p.result.Terminate
		details := p.result.Details
		p.result = agenttool.ErrorResult(p.err)
		p.result.Terminate = terminate
		p.result.Details = details
	}
	return r.emit(&ToolEnd{RunID: r.runID, Turn: r.turn, CallID: p.call.CallID, Name: p.call.Name, Result: p.result, Err: p.err, Blocked: p.blocked})
}

func validObject(raw json.RawMessage) bool {
	var v map[string]json.RawMessage
	return json.Unmarshal(raw, &v) == nil && v != nil
}
