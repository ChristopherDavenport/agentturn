package agentturn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// Errors returned before a run starts. Every error the package
// produces, sentinel or wrapped, begins with "agentturn:".
var (
	// ErrNoModel is returned when Config.Model is nil.
	ErrNoModel = errors.New("agentturn: config has no model")
	// ErrCannotContinue is returned when the transcript does not end
	// with a user message or a function call output, so there is
	// nothing for the model to answer.
	ErrCannotContinue = errors.New("agentturn: transcript must end with a user message or a function call output to continue")
	// ErrNoPrompt is returned when Run was called with no prompt items.
	ErrNoPrompt = errors.New("agentturn: no prompt items")
)

// EventBuffer is how many events the loop can run ahead of the consumer
// of [Run] or [Continue] before it blocks; [Agent] delivers every event
// synchronously and has no buffer.
const EventBuffer = 256

// Run appends prompts to the transcript and drives the loop until the
// agent goes idle. Events are yielded in order and the [RunEnd] is the
// single terminal: it is always the last event, its Reason says how the
// run ended and its Err carries the failure when Reason is ReasonError,
// the context error when Reason is ReasonAborted, and nil otherwise. A
// misuse before the run starts ([ErrNoPrompt], [ErrCannotContinue],
// [ErrNoModel], a duplicate tool name) yields one RunEnd with
// ReasonError and no other event.
//
// The loop runs ahead of the consumer by up to [EventBuffer] events and
// applies backpressure after that. Breaking out of the loop cancels the
// run and blocks until it has wound down.
//
// The transcript is not modified; the items the run appended are on the
// RunEnd.
func Run(ctx context.Context, t Transcript, prompts openresponses.Items, cfg Config) iter.Seq[Event] {
	if len(prompts) == 0 {
		return failNow(ErrNoPrompt)
	}
	return observe(ctx, t, prompts, cfg)
}

// Continue drives the loop from the transcript as it stands, which must
// satisfy [CanContinue]. Events arrive as for [Run].
func Continue(ctx context.Context, t Transcript, cfg Config) iter.Seq[Event] {
	if !CanContinue(t) {
		return failNow(ErrCannotContinue)
	}
	return observe(ctx, t, nil, cfg)
}

// failNow yields the RunEnd of a run that never started.
func failNow(err error) iter.Seq[Event] {
	return func(yield func(Event) bool) { yield(&RunEnd{Reason: ReasonError, Err: err}) }
}

// CanContinue reports whether the model has something to answer: the
// transcript ends with a user message or a function call output.
func CanContinue(t Transcript) bool {
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

// unansweredCalls returns the function calls anywhere in the transcript
// that have no function_call_output after them, in transcript order. A
// transcript is a valid input only when this is empty; a message
// appended after a dangling call does not settle it, which is the rule
// a strict server applies.
func unansweredCalls(t Transcript) []*openresponses.FunctionCall {
	answered := map[string]bool{}
	for _, item := range t {
		if out, ok := item.(*openresponses.FunctionCallOutput); ok {
			answered[out.CallID] = true
		}
	}
	var calls []*openresponses.FunctionCall
	for _, item := range t {
		if call, ok := item.(*openresponses.FunctionCall); ok && !answered[call.CallID] {
			calls = append(calls, call)
		}
	}
	return calls
}

// pendingCalls pairs each call with reason.
func pendingCalls(calls []*openresponses.FunctionCall, reason PendingReason) []PendingCall {
	if len(calls) == 0 {
		return nil
	}
	out := make([]PendingCall, len(calls))
	for i, call := range calls {
		out[i] = PendingCall{Call: call, Reason: reason}
	}
	return out
}

// observe runs the loop in a goroutine and yields its events from the
// caller's.
func observe(ctx context.Context, t Transcript, prompts openresponses.Items, cfg Config) iter.Seq[Event] {
	return func(yield func(Event) bool) {
		if err := cfg.validate(); err != nil {
			yield(&RunEnd{Reason: ReasonError, Err: err})
			return
		}
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		events := make(chan Event, EventBuffer)
		go func() {
			defer close(events)
			r := &runner{
				cfg:        cfg,
				transcript: append(Transcript(nil), t...),
				// Every event is sent, cancelled or not: the consumer
				// below drains the channel until the run returns, so a
				// send never blocks for good, and the events an abort
				// leaves behind, the tool_end of a cut-off call and the
				// run_end, reach a consumer that is still there. The
				// loop notices the cancellation through the model, the
				// tools and its own checks.
				emit: func(ev Event) error {
					events <- ev
					return nil
				},
			}
			r.run(ctx, prompts, nil)
		}()
		// After the consumer breaks out, the loop is drained so the
		// goroutine can wind down; nothing more is yielded.
		stopped := false
		for ev := range events {
			if stopped {
				continue
			}
			if !yield(ev) {
				stopped = true
				cancel()
				continue
			}
			if _, isEnd := ev.(*RunEnd); isEnd {
				stopped = true
			}
		}
	}
}

type transcriptKey struct{}
type runIDKey struct{}
type triggerKey struct{}

// ContextWithTrigger attaches a [Trigger] to ctx. A run started with
// that context, through [Run], [Continue] or an [Agent], carries it on
// its [RunStart], so a recorder can write what caused the run without
// the loop learning what a cron job or a channel is.
func ContextWithTrigger(ctx context.Context, t Trigger) context.Context {
	return context.WithValue(ctx, triggerKey{}, t)
}

// TriggerFromContext returns the trigger attached to ctx, or the zero
// Trigger.
func TriggerFromContext(ctx context.Context) Trigger {
	t, _ := ctx.Value(triggerKey{}).(Trigger)
	return t
}

// ContextWithTranscript attaches a transcript to ctx. The loop does this
// with its working transcript before running a turn's hooks and tools,
// so a tool that composes another agent can seed it from the
// conversation without the host threading anything through.
func ContextWithTranscript(ctx context.Context, t Transcript) context.Context {
	return context.WithValue(ctx, transcriptKey{}, t)
}

// TranscriptFromContext returns the transcript the loop attached to the
// context of a hook or a tool call: the working transcript as it stood
// when the batch started, the calls of the batch included. It is a
// snapshot; do not mutate it.
func TranscriptFromContext(ctx context.Context) (Transcript, bool) {
	t, ok := ctx.Value(transcriptKey{}).(Transcript)
	return t, ok
}

// ContextWithRunID attaches a run ID to ctx. The loop does this with
// its own for everything a run calls, the transform, the hooks, the
// model and the tools, so a tool that composes another agent, and
// whatever observes that child, can tell which run it was called from.
func ContextWithRunID(ctx context.Context, runID string) context.Context {
	return context.WithValue(ctx, runIDKey{}, runID)
}

// RunIDFromContext returns the ID of the run whose hook or tool call
// the context belongs to, or "" outside a loop.
func RunIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(runIDKey{}).(string)
	return id
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
	// deferred holds the IDs of the calls a hook handed to the caller
	// during this run, so the run end can say why they are pending.
	deferred map[string]bool
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

// stop carries reason and err out of a phase.
func stop(reason Reason, err error) error { return &errStop{reason: reason, err: err} }

// approval is a pending call the caller approved through Agent.Resume:
// it runs before the first model call of the run, with args in place of
// the call's own when set.
type approval struct {
	call *openresponses.FunctionCall
	args json.RawMessage
}

// run drives the loop and returns the RunEnd. A panic in a tool is
// recovered by agenttool's executor into a PanicError the model sees as
// an error output; a panic in a hook or a subscriber is not recovered
// and unwinds without a RunEnd.
func (r *runner) run(ctx context.Context, prompts openresponses.Items, approved []approval) *RunEnd {
	r.runID = openresponses.NewID("run")
	// Everything the run calls, transform, hooks, model and tools, can
	// tell which run it serves.
	ctx = ContextWithRunID(ctx, r.runID)
	end := &RunEnd{RunID: r.runID, Reason: ReasonDone}
	err := r.loop(ctx, prompts, approved)
	var stop *errStop
	switch {
	case err == nil:
	case errors.As(err, &stop):
		end.Reason = stop.reason
		end.Err = stop.err
	case ctx.Err() != nil:
		end.Reason = ReasonAborted
		end.Err = ctx.Err()
		if !errors.Is(err, ctx.Err()) {
			// A subscriber or a hook failed for a reason of its own
			// while the run was being aborted: the abort is the reason
			// the run ended, and the failure rides on it rather than
			// vanishing.
			end.Err = fmt.Errorf("%w: %w", ctx.Err(), err)
		}
	default:
		end.Reason = ReasonError
		end.Err = err
	}
	end.Items = r.added
	// Whatever ended the run, the calls without an output are the
	// caller's to answer: the deferred ones on input_required, and the
	// ones an abort or a failure cut off before their outputs were
	// appended.
	end.Pending = r.pending()
	// A subscriber that fails on run_end cannot change the outcome; the
	// run has already ended.
	_ = r.emit(end)
	return end
}

// pending lists the calls of the transcript without an output, each
// with why: deferred by a hook in this run, cut off in this run, or
// found in the transcript the run started from.
func (r *runner) pending() []PendingCall {
	calls := unansweredCalls(r.transcript)
	if len(calls) == 0 {
		return nil
	}
	mine := make(map[*openresponses.FunctionCall]bool, len(r.added))
	for _, item := range r.added {
		if call, ok := item.(*openresponses.FunctionCall); ok {
			mine[call] = true
		}
	}
	out := make([]PendingCall, len(calls))
	for i, call := range calls {
		reason := PendingUnknown
		switch {
		case r.deferred[call.CallID]:
			reason = PendingDeferred
		case mine[call]:
			reason = PendingAborted
		}
		out[i] = PendingCall{Call: call, Reason: reason}
	}
	return out
}

// source says what the run begins from: a resume when a prompt answers
// a call the transcript left unanswered or the caller approved one,
// an input otherwise.
func (r *runner) source(prompts openresponses.Items, approved []approval) Source {
	if len(approved) > 0 {
		return SourceResume
	}
	open := map[string]bool{}
	for _, call := range unansweredCalls(r.transcript) {
		open[call.CallID] = true
	}
	for _, item := range prompts {
		if out, ok := item.(*openresponses.FunctionCallOutput); ok && open[out.CallID] {
			return SourceResume
		}
	}
	return SourceInput
}

func (r *runner) loop(ctx context.Context, prompts openresponses.Items, approved []approval) error {
	if err := r.emit(&RunStart{RunID: r.runID, Source: r.source(prompts, approved), Trigger: TriggerFromContext(ctx)}); err != nil {
		return err
	}
	if err := r.appendItems(prompts); err != nil {
		return err
	}
	if len(approved) > 0 {
		results, err := r.approvedBatch(ctx, approved)
		if err != nil {
			return err
		}
		if allTerminate(results) {
			return stop(ReasonStopped, nil)
		}
	}
	for {
		if r.cfg.MaxTurns > 0 && r.turn >= r.cfg.MaxTurns {
			return stop(ReasonStopped, nil)
		}
		if err := ctx.Err(); err != nil {
			return stop(ReasonAborted, err)
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
			return stop(ReasonInputRequired, nil)
		}
		if r.cfg.ShouldStopAfterTurn != nil {
			halt, err := r.cfg.ShouldStopAfterTurn(ctx, TurnInfo{RunID: r.runID, Turn: r.turn, Response: resp, ToolResults: results, Transcript: r.transcript})
			if err != nil {
				return fmt.Errorf("agentturn: should-stop-after-turn hook: %w", err)
			}
			if halt {
				return stop(ReasonStopped, nil)
			}
		}
		if len(calls) > 0 && allTerminate(results) {
			return stop(ReasonStopped, nil)
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
			return openresponses.Request{}, fmt.Errorf("agentturn: transform: %w", err)
		}
	}
	req := r.cfg.baseRequest(tools)
	req.Input = r.cfg.filter()(input)
	if r.cfg.BeforeModelCall != nil {
		if err := r.cfg.BeforeModelCall(ctx, &req); err != nil {
			// The call was never made; the request as built is the
			// record of what was refused.
			if eerr := r.emit(&ModelBlocked{RunID: r.runID, Turn: r.turn, Request: req, Err: err}); eerr != nil {
				return openresponses.Request{}, eerr
			}
			return openresponses.Request{}, fmt.Errorf("agentturn: before-model-call hook: %w", err)
		}
	}
	return req, nil
}

// modelTurn sends the turn's request, retrying a transient failure
// under Config.Retry, emitting item events, and appends the completed
// items to the transcript.
func (r *runner) modelTurn(ctx context.Context, tools agenttool.Set) (*openresponses.Response, error) {
	req, err := r.request(ctx, tools)
	if err != nil {
		return nil, err
	}
	if err := r.emit(&TurnStart{RunID: r.runID, Turn: r.turn, Request: req}); err != nil {
		return nil, err
	}
	for attempt := 1; ; attempt++ {
		resp, committed, err := r.stream(ctx, req)
		if err == nil {
			return resp, nil
		}
		if ctx.Err() != nil {
			return nil, stop(ReasonAborted, ctx.Err())
		}
		if committed || attempt >= r.cfg.Retry.MaxAttempts || !r.cfg.Retry.retryable(err) {
			return nil, fmt.Errorf("agentturn: model: %w", err)
		}
		delay := r.cfg.Retry.backoff(attempt, err)
		if err := r.emit(&ModelRetry{RunID: r.runID, Turn: r.turn, Attempt: attempt, Err: err, Delay: delay}); err != nil {
			return nil, err
		}
		if err := sleep(ctx, delay); err != nil {
			return nil, stop(ReasonAborted, err)
		}
	}
}

// sleep waits for d or until ctx is done.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// stream runs one attempt at the request. committed reports that the
// attempt cannot be retried: an event of it reached subscribers, or
// the server answered with a terminal response. The error is returned
// unwrapped so a retry policy sees the transport or wire error itself.
func (r *runner) stream(ctx context.Context, req openresponses.Request) (resp *openresponses.Response, committed bool, err error) {
	var acc openresponses.Accumulator
	for ev, err := range openresponses.Events(ctx, r.cfg.Model, req) {
		if err != nil {
			return nil, committed, err
		}
		acc.Add(ev)
		emitted, err := r.streamEvent(ev, &acc)
		committed = committed || emitted
		if err != nil {
			return nil, true, err
		}
		if final, ok := openresponses.TerminalResponse(ev); ok {
			resp = final
		}
	}
	if resp == nil {
		return nil, committed, openresponses.ErrTruncatedStream
	}
	if err := r.emit(&ResponseEnd{RunID: r.runID, Turn: r.turn, Response: resp}); err != nil {
		return nil, true, err
	}
	if resp.Status == openresponses.ResponseStatusFailed {
		if resp.Error != nil {
			return nil, true, resp.Error.Err(0)
		}
		return nil, true, errors.New("response failed")
	}
	return resp, true, nil
}

// streamEvent turns one wire event, already added to acc, into the item
// events of the turn: item_start when an output item opens, item_end
// with the transcript append when it is done, item_update for the
// events in between. It reports whether an event was emitted. An error
// event fails the attempt with the wire error, unwrapped.
func (r *runner) streamEvent(ev openresponses.StreamEvent, acc *openresponses.Accumulator) (bool, error) {
	responseID := ""
	if cur := acc.Response(); cur != nil {
		responseID = cur.ID
	}
	switch e := ev.(type) {
	case *openresponses.OutputItemAddedEvent:
		return true, r.emit(&ItemStart{RunID: r.runID, Turn: r.turn, Item: acc.Response().Output[e.OutputIndex], ResponseID: responseID})
	case *openresponses.OutputItemDoneEvent:
		r.transcript = append(r.transcript, e.Item)
		r.added = append(r.added, e.Item)
		return true, r.emit(&ItemEnd{RunID: r.runID, Turn: r.turn, Item: e.Item, ResponseID: responseID})
	case *openresponses.ErrorEvent:
		return false, e.Err()
	}
	if idx, ok := outputIndex(ev); ok {
		return true, r.emit(&ItemUpdate{RunID: r.runID, Turn: r.turn, Item: acc.Response().Output[idx], Stream: ev, ResponseID: responseID})
	}
	return false, nil
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

// callState is one call of a batch through preflight and execution.
type callState struct {
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
	// appended is set once the call's output is in the transcript.
	appended bool
}

// toolBatch runs the calls of a turn: preflight in order, execute,
// then append the outputs in the model's order. It returns the results
// in that order and the calls a hook deferred to the caller.
func (r *runner) toolBatch(ctx context.Context, tools agenttool.Set, calls []*openresponses.FunctionCall) ([]agenttool.Result, []*openresponses.FunctionCall, error) {
	if len(calls) == 0 {
		return nil, nil, nil
	}
	// Hooks and tools see the conversation that produced the calls.
	ctx = r.toolContext(ctx)
	batch, err := r.preflightAll(ctx, tools, calls)
	if err != nil {
		return nil, nil, err
	}
	if err := r.execute(ctx, batch); err != nil {
		return nil, nil, err
	}
	return r.collect(batch)
}

// toolContext is the context hooks and tools run under: ctx with a
// snapshot of the working transcript attached.
func (r *runner) toolContext(ctx context.Context) context.Context {
	return ContextWithTranscript(ctx, append(Transcript(nil), r.transcript...))
}

// preflightAll runs preflight for every call in the model's order and
// checks for an abort before anything executes.
func (r *runner) preflightAll(ctx context.Context, tools agenttool.Set, calls []*openresponses.FunctionCall) ([]*callState, error) {
	batch := make([]*callState, len(calls))
	for i, call := range calls {
		p, err := r.preflight(ctx, tools, call)
		if err != nil {
			return nil, err
		}
		batch[i] = p
	}
	if err := ctx.Err(); err != nil {
		return nil, r.abortBatch(ctx, batch, err)
	}
	return batch, nil
}

// abortBatch ends a batch cut off before it executed: every call that
// had its tool_start and is not settled gets its tool_end with the
// context error, as a call cut off while running does, so the two are
// always paired, and the outputs of the calls preflight settled are
// appended. It returns the stop for the aborted run.
func (r *runner) abortBatch(ctx context.Context, batch []*callState, err error) error {
	for _, p := range batch {
		if p.settled {
			continue
		}
		p.settled = true
		p.err = err
		if serr := r.settle(ctx, p); serr != nil {
			return serr
		}
	}
	if aerr := r.appendFinished(ctx, batch); aerr != nil {
		return aerr
	}
	return stop(ReasonAborted, err)
}

// appendFinished appends, in the batch's order, the outputs of the
// calls that finished in a batch that was cut off: the ones settled
// with a result of their own rather than the context error. A call that
// ran to completion before the abort is then answered in the
// transcript, and only the calls the abort actually cut off are left
// pending. It is a no-op when nothing finished.
func (r *runner) appendFinished(ctx context.Context, batch []*callState) error {
	var outputs openresponses.Items
	for _, p := range batch {
		if p == nil || !p.settled || p.deferred || p.appended {
			continue
		}
		if p.err != nil && ctx.Err() != nil && errors.Is(p.err, ctx.Err()) {
			continue
		}
		p.appended = true
		outputs = append(outputs, &openresponses.FunctionCallOutput{CallID: p.call.CallID, Output: p.result.Output})
	}
	return r.appendItems(outputs)
}

// execute runs the calls preflight did not settle and settles each as
// it finishes, in completion order.
func (r *runner) execute(ctx context.Context, batch []*callState) error {
	var jobs []agenttool.Job
	var jobIndex []int
	for i, p := range batch {
		if p.settled {
			continue
		}
		jobs = append(jobs, agenttool.Job{Tool: p.tool, Call: agenttool.Call{ID: p.call.CallID, Args: p.args}})
		jobIndex = append(jobIndex, i)
	}
	exec := agenttool.Executor{MaxParallel: r.cfg.MaxParallelTools, Sequential: r.cfg.ToolExecution == ExecSequential}
	for ev := range exec.Execute(ctx, jobs) {
		p := batch[jobIndex[ev.Index]]
		var err error
		if ev.Final {
			p.result, p.err = ev.Result, ev.Err
			p.settled = true
			err = r.settle(ctx, p)
		} else {
			err = r.emit(&ToolUpdate{RunID: r.runID, Turn: r.turn, CallID: p.call.CallID, Name: p.call.Name, Partial: ev.Result})
		}
		if err != nil {
			// Leaving the executor's range cancels the batch and waits
			// for the running tools before this returns.
			_ = r.appendFinished(ctx, batch)
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		if aerr := r.appendFinished(ctx, batch); aerr != nil {
			return aerr
		}
		return stop(ReasonAborted, err)
	}
	return nil
}

// collect appends the outputs in the model's order and returns the
// results with the deferred calls.
func (r *runner) collect(batch []*callState) ([]agenttool.Result, []*openresponses.FunctionCall, error) {
	results := make([]agenttool.Result, len(batch))
	outputs := make(openresponses.Items, 0, len(batch))
	var deferred []*openresponses.FunctionCall
	for i, p := range batch {
		results[i] = p.result
		if p.deferred {
			deferred = append(deferred, p.call)
			continue
		}
		p.appended = true
		outputs = append(outputs, &openresponses.FunctionCallOutput{CallID: p.call.CallID, Output: p.result.Output})
	}
	if err := r.appendItems(outputs); err != nil {
		return nil, nil, err
	}
	return results, deferred, nil
}

// approvedBatch runs the calls a caller approved on Resume as one
// batch, before the first turn: preflight without BeforeToolCall, since
// the caller has decided, then execute and collect as for any batch.
// The tool events carry Turn 0.
func (r *runner) approvedBatch(ctx context.Context, approved []approval) ([]agenttool.Result, error) {
	tools := r.cfg.tools(ctx)
	ctx = r.toolContext(ctx)
	batch := make([]*callState, len(approved))
	for i, ap := range approved {
		p := r.prepare(tools, ap.call, ap.args)
		if err := r.emit(&ToolStart{RunID: r.runID, Turn: r.turn, CallID: p.call.CallID, Name: p.call.Name, Args: p.args, Decision: &ToolDecision{Args: ap.args}}); err != nil {
			return nil, err
		}
		if err := r.check(ctx, p, false); err != nil {
			return nil, err
		}
		batch[i] = p
	}
	if err := ctx.Err(); err != nil {
		return nil, r.abortBatch(ctx, batch, err)
	}
	if err := r.execute(ctx, batch); err != nil {
		return nil, err
	}
	results, _, err := r.collect(batch)
	return results, err
}

// preflight resolves the tool, checks the arguments and runs
// BeforeToolCall. It emits tool_start and, for a call that will not
// execute, tool_end.
func (r *runner) preflight(ctx context.Context, tools agenttool.Set, call *openresponses.FunctionCall) (*callState, error) {
	p := r.prepare(tools, call, nil)
	var terminate bool
	var decision *ToolDecision
	if r.cfg.BeforeToolCall != nil {
		var err error
		decision, err = r.cfg.BeforeToolCall(ctx, ToolCallInfo{RunID: r.runID, Turn: r.turn, Call: call, Tool: p.tool, Args: p.args})
		if err != nil {
			return nil, fmt.Errorf("agentturn: before-tool-call hook: %w", err)
		}
		if decision != nil {
			if decision.Args != nil {
				p.args = decision.Args
			}
			terminate = decision.Terminate
			switch decision.Action {
			case Block:
				reason := decision.Reason
				if reason == "" {
					reason = "call blocked"
				}
				p.blocked = true
				p.err = errors.New(reason)
			case Defer:
				p.deferred = true
				if r.deferred == nil {
					r.deferred = map[string]bool{}
				}
				r.deferred[call.CallID] = true
			}
		}
	}
	if err := r.emit(&ToolStart{RunID: r.runID, Turn: r.turn, CallID: call.CallID, Name: call.Name, Args: p.args, Decision: decision}); err != nil {
		return nil, err
	}
	if p.deferred {
		p.settled = true
		return p, r.emit(&ToolEnd{RunID: r.runID, Turn: r.turn, CallID: call.CallID, Name: call.Name, Deferred: true})
	}
	return p, r.check(ctx, p, terminate)
}

// prepare starts the state of a call: the tool with its name, if any,
// and its arguments, args when given and the call's own otherwise, an
// empty object standing in for none.
func (r *runner) prepare(tools agenttool.Set, call *openresponses.FunctionCall, args json.RawMessage) *callState {
	p := &callState{call: call, args: args}
	if p.args == nil {
		p.args = json.RawMessage(call.Arguments)
	}
	if len(p.args) == 0 {
		p.args = json.RawMessage("{}")
	}
	p.tool, _ = tools.Lookup(call.Name)
	return p
}

// check settles a call that will not execute: one a hook blocked, one
// no tool has the name of, or one whose arguments are not an object.
// It returns nil, with the call unsettled, when the call may run.
func (r *runner) check(ctx context.Context, p *callState, terminate bool) error {
	switch {
	case p.blocked:
	case p.tool == nil:
		p.err = fmt.Errorf("unknown tool %q", p.call.Name)
	case !validObject(p.args):
		p.err = errors.New("invalid arguments: expected a JSON object")
	}
	if p.err == nil {
		return nil
	}
	p.settled = true
	p.result = agenttool.ErrorResult(p.err)
	p.result.Terminate = terminate
	return r.settle(ctx, p)
}

// settle applies AfterToolCall, normalises an error into the output the
// model sees and emits tool_end.
func (r *runner) settle(ctx context.Context, p *callState) error {
	if r.cfg.AfterToolCall != nil && !p.blocked {
		override, err := r.cfg.AfterToolCall(ctx, ToolResultInfo{RunID: r.runID, Turn: r.turn, Call: p.call, Tool: p.tool, Args: p.args, Result: p.result, Err: p.err})
		if err != nil {
			return fmt.Errorf("agentturn: after-tool-call hook: %w", err)
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
