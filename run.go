package agentturn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"sync"
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
	// ErrGuard is what a ShouldStopAfterTurn hook wraps to end the run
	// as a policy stop rather than a failure: ReasonStopped with
	// StopGuard, the error on RunEnd.Err.
	ErrGuard = errors.New("agentturn: guard stopped the run")
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
// [ErrNoModel], a duplicate tool name, or [ErrInputRequired] for a
// transcript with a function call that neither the transcript nor the
// prompts' leading outputs answer) yields one RunEnd with ReasonError
// and no other event.
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
	if err := answersPending(pendingCalls(unansweredCalls(t), PendingUnknown), prompts); err != nil {
		return failNow(err)
	}
	return observe(ctx, t, prompts, cfg, false)
}

// Continue drives the loop from the transcript as it stands, which must
// satisfy [CanContinue] and answer every function call it holds. Events
// arrive as for [Run].
func Continue(ctx context.Context, t Transcript, cfg Config) iter.Seq[Event] {
	if calls := unansweredCalls(t); len(calls) > 0 {
		return failNow(fmt.Errorf("%w: %d call(s) unanswered", ErrInputRequired, len(calls)))
	}
	if !CanContinue(t) {
		return failNow(ErrCannotContinue)
	}
	return observe(ctx, t, nil, cfg, false)
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
func observe(ctx context.Context, t Transcript, prompts openresponses.Items, cfg Config, terminate bool) iter.Seq[Event] {
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
				send: func(ev Event) error {
					events <- ev
					return nil
				},
			}
			r.run(ctx, prompts, nil, terminate)
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
type decidersKey struct{}

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

// ContextWithDeciders attaches who decided the answer to each pending
// call, by call ID, in the session format's terms ("human", "policy",
// "agent"). [Agent.Resume] does it from the [Answer.By] of the answers
// it was given, so a subscriber writing the record of an output the
// caller supplied, which raises no tool_start to carry a decision, can
// say who wrote it. A host driving the low-level [Run] with the
// outputs as prompts attaches it itself. The loop reads nothing from
// it.
func ContextWithDeciders(ctx context.Context, by map[string]string) context.Context {
	if len(by) == 0 {
		return ctx
	}
	out := make(map[string]string, len(by))
	for k, v := range by {
		out[k] = v
	}
	return context.WithValue(ctx, decidersKey{}, out)
}

// DeciderFromContext returns who the caller named as the decider of the
// answer for callID, or "" when nobody was named.
func DeciderFromContext(ctx context.Context, callID string) string {
	by, _ := ctx.Value(decidersKey{}).(map[string]string)
	return by[callID]
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

// runner is one run of the loop. send delivers an event and returns an
// error to abort the run; steer and followUp drain the queues when set.
type runner struct {
	cfg        Config
	transcript Transcript
	send       func(Event) error
	steer      func() openresponses.Items
	followUp   func() openresponses.Items

	// hookMu serialises the tool hooks. A nested call runs on the
	// goroutine of the tool that made it, so without it two tools of
	// one batch would enter BeforeToolCall at once, and a nested
	// call's AfterToolCall would run beside the batch's. It is held
	// across the hook alone, never across an emit or a tool, so the
	// only way to block on it is to call Invoke from inside a hook.
	hookMu sync.Mutex

	runID string
	turn  int
	added Transcript
	// ctx is the run's context, for the hooks the stream calls.
	ctx context.Context
	// mark is the length of the transcript after the previous turn's
	// response, so the next turn_start can name what was appended since.
	mark int
	// deferred holds the IDs of the calls a hook handed to the caller
	// during this run, so the run end can say why they are pending.
	deferred map[string]bool
	// held are the completed items of the attempt in flight that the
	// transcript does not have yet, because nothing has committed the
	// attempt: a reasoning item a model opens before its answer. They
	// are appended when the attempt commits and dropped when it ends
	// without committing.
	held []heldItem
}

// heldItem is a completed item waiting for its attempt to commit.
type heldItem struct {
	item       openresponses.Item
	responseID string
}

// emit delivers one event to the consumer. An Agent serialises them
// behind its own barrier, whichever goroutine raised them; the
// low-level loop's channel takes one send at a time.
func (r *runner) emit(ev Event) error { return r.send(ev) }

// beforeToolCall runs the hook for one call, one goroutine at a time.
func (r *runner) beforeToolCall(ctx context.Context, info ToolCallInfo) (*ToolDecision, error) {
	r.hookMu.Lock()
	defer r.hookMu.Unlock()
	return r.cfg.BeforeToolCall(ctx, info)
}

// afterToolCall runs the hook for one result, one goroutine at a time.
func (r *runner) afterToolCall(ctx context.Context, info ToolResultInfo) (*ToolOverride, error) {
	r.hookMu.Lock()
	defer r.hookMu.Unlock()
	return r.cfg.AfterToolCall(ctx, info)
}

// errStop carries a run end reason out of a phase.
type errStop struct {
	reason Reason
	cause  StopCause
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

// stopped carries a stop with its cause out of a phase.
func stopped(cause StopCause, err error) error {
	return &errStop{reason: ReasonStopped, cause: cause, err: err}
}

// approval is a pending call the caller approved through Agent.Resume:
// it runs before the first model call of the run, with args in place of
// the call's own when set, note, when set, appended after the batch's
// outputs, and by naming who approved it, for the record.
type approval struct {
	call *openresponses.FunctionCall
	args json.RawMessage
	note string
	by   string
}

// run drives the loop and returns the RunEnd. terminate says the caller
// asked the run to end once the answers are in, without a model call.
// A panic in a tool is recovered by agenttool's executor into a
// PanicError the model sees as an error output; a panic in a hook or a
// subscriber is not recovered and unwinds without a RunEnd.
func (r *runner) run(ctx context.Context, prompts openresponses.Items, approved []approval, terminate bool) *RunEnd {
	r.runID = openresponses.NewID("run")
	r.mark = len(r.transcript)
	// Everything the run calls, transform, hooks, model and tools, can
	// tell which run it serves.
	ctx = ContextWithRunID(ctx, r.runID)
	r.ctx = ctx
	end := &RunEnd{RunID: r.runID, Reason: ReasonDone}
	err := r.loop(ctx, prompts, approved, terminate)
	var stop *errStop
	switch {
	case err == nil:
	case errors.As(err, &stop):
		end.Reason = stop.reason
		end.Cause = stop.cause
		end.Err = stop.err
	case ctx.Err() != nil:
		end.Reason = ReasonAborted
		end.Err = context.Cause(ctx)
		if !errors.Is(err, context.Cause(ctx)) {
			// A subscriber or a hook failed for a reason of its own
			// while the run was being aborted: the abort is the reason
			// the run ended, and the failure rides on it rather than
			// vanishing.
			end.Err = fmt.Errorf("%w: %w", context.Cause(ctx), err)
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
	for _, item := range unhideAll(prompts) {
		if out, ok := item.(*openresponses.FunctionCallOutput); ok && open[out.CallID] {
			return SourceResume
		}
	}
	return SourceInput
}

func (r *runner) loop(ctx context.Context, prompts openresponses.Items, approved []approval, terminate bool) error {
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
		if cause, ok := terminates(results); ok && !terminate {
			return stopped(cause, nil)
		}
		// What was steered in while the caller was deciding goes to the
		// model with the answers, as after any batch.
		if err := r.appendItems(r.drain(r.steer)); err != nil {
			return err
		}
	}
	if terminate {
		return stopped(StopRefused, nil)
	}
	for {
		if r.cfg.MaxTurns > 0 && r.turn >= r.cfg.MaxTurns {
			return stopped(StopMaxTurns, nil)
		}
		if ctx.Err() != nil {
			return stop(ReasonAborted, context.Cause(ctx))
		}
		r.turn++
		if r.cfg.BeforeTurn != nil {
			items, err := r.cfg.BeforeTurn(ctx, TurnStartInfo{RunID: r.runID, Turn: r.turn, Transcript: r.transcript})
			if err != nil {
				return fmt.Errorf("agentturn: before-turn hook: %w", err)
			}
			if err := r.appendItems(items); err != nil {
				return err
			}
		}
		tools := r.cfg.tools(ctx)
		if r.cfg.ToolProvider != nil {
			// A provided list is checked as Config.Tools was before the
			// run, once per turn, since it may change between turns.
			if err := tools.Validate(); err != nil {
				return fmt.Errorf("agentturn: turn %d: tool provider: %w", r.turn, err)
			}
		}
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
			halt, err := r.cfg.ShouldStopAfterTurn(ctx, TurnInfo{RunID: r.runID, Turn: r.turn, Response: resp, ToolResults: results, Final: len(calls) == 0, Transcript: r.transcript})
			if err != nil {
				if errors.Is(err, ErrGuard) {
					return stopped(StopGuard, err)
				}
				return fmt.Errorf("agentturn: should-stop-after-turn hook: %w", err)
			}
			if halt {
				return stopped(StopHook, nil)
			}
		}
		if cause, ok := terminates(results); ok {
			return stopped(cause, nil)
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

// terminates says whether a batch's results end the run, and how: every
// result set Terminate, or some did. A result's Terminate means the
// tool answered on the model's behalf; the loop stops either way, and
// the cause tells a host whether the whole batch agreed.
func terminates(results []agenttool.Result) (StopCause, bool) {
	some, all := false, len(results) > 0
	for _, res := range results {
		if res.Terminate {
			some = true
		} else {
			all = false
		}
	}
	switch {
	case all:
		return StopTerminate, true
	case some:
		return StopPartialTerminate, true
	}
	return "", false
}

// appendItems adds items the loop did not stream (prompts, queued
// messages, tool outputs) to the transcript with their item events. An
// item the caller marked with [Hidden] is unwrapped here, so the
// transcript and the request hold the item itself and only its events
// say it is hidden.
func (r *runner) appendItems(items openresponses.Items) error {
	for _, item := range items {
		if item == nil {
			continue
		}
		item, hidden := Unhide(item)
		if err := r.emit(&ItemStart{RunID: r.runID, Turn: r.turn, Item: item, Hidden: hidden}); err != nil {
			return err
		}
		r.transcript = append(r.transcript, item)
		r.added = append(r.added, item)
		if err := r.emit(&ItemEnd{RunID: r.runID, Turn: r.turn, Item: item, Hidden: hidden}); err != nil {
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
	inputs := append(openresponses.Items(nil), r.transcript[min(r.mark, len(r.transcript)):]...)
	if err := r.emit(&TurnStart{RunID: r.runID, Turn: r.turn, Request: req, Inputs: inputs}); err != nil {
		return nil, err
	}
	for attempt := 1; ; attempt++ {
		resp, committed, err := r.stream(ctx, req)
		if err == nil {
			r.mark = len(r.transcript)
			return resp, nil
		}
		if ctx.Err() != nil {
			return nil, stop(ReasonAborted, context.Cause(ctx))
		}
		if committed || attempt >= r.cfg.Retry.MaxAttempts || !r.cfg.Retry.retryable(err) {
			return nil, fmt.Errorf("agentturn: model: %w", err)
		}
		delay := r.cfg.Retry.backoff(attempt, err)
		if revise := r.cfg.Retry.Revise; revise != nil {
			// The policy may move the turn to another model or another
			// setting; the record follows the event.
			next := req
			if out := revise(attempt, &next, err); out != nil {
				next = *out
			}
			req = next
		}
		if err := r.emit(&ModelRetry{RunID: r.runID, Turn: r.turn, Attempt: attempt, Err: err, Delay: delay, Request: req}); err != nil {
			return nil, err
		}
		if err := sleep(ctx, delay); err != nil {
			return nil, stop(ReasonAborted, context.Cause(ctx))
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
// attempt cannot be retried: a message or a function call opened, a
// subscriber failed, or the server answered with a terminal response.
// An item the attempt completed before it committed is held until the
// answer begins or the response arrives, and dropped when the attempt
// ends without one, so a failure between a reasoning summary and the
// first token leaves nothing behind and a response that arrives keeps
// everything it carried. The error is returned unwrapped so a retry
// policy sees the transport or wire error itself.
func (r *runner) stream(ctx context.Context, req openresponses.Request) (resp *openresponses.Response, committed bool, err error) {
	var acc openresponses.Accumulator
	r.held = nil
	for ev, err := range openresponses.Events(ctx, r.cfg.Model, req) {
		if err != nil {
			return nil, committed, err
		}
		acc.Add(ev)
		commits, err := r.streamEvent(ev, &acc, committed)
		committed = committed || commits
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
	if resp.Status != openresponses.ResponseStatusFailed {
		// The response arrived, so what the attempt completed on the
		// way to it is the model's output and belongs in the
		// transcript, whether or not a message or a function call ever
		// opened: a reasoning item the model answered with alone, an
		// item cut short by the token limit, an item type this loop
		// does not know. Only an attempt that ended without its
		// response drops what it held.
		if err := r.flushHeld(); err != nil {
			return nil, true, err
		}
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
// events in between. committed says whether the attempt has committed;
// while it has not, a completed item is held rather than appended, and
// the return reports whether this event commits the attempt, which a
// message or a function call opening does. An error event fails the
// attempt with the wire error, unwrapped.
func (r *runner) streamEvent(ev openresponses.StreamEvent, acc *openresponses.Accumulator, committed bool) (bool, error) {
	responseID := ""
	if cur := acc.Response(); cur != nil {
		responseID = cur.ID
	}
	switch e := ev.(type) {
	case *openresponses.OutputItemAddedEvent:
		item := acc.Response().Output[e.OutputIndex]
		commits := !committed && commitsAttempt(item)
		if commits {
			// The answer has started: what the attempt produced on the
			// way to it belongs in the transcript, before it.
			if err := r.flushHeld(); err != nil {
				return true, err
			}
		}
		return commits, r.emit(&ItemStart{RunID: r.runID, Turn: r.turn, Item: item, ResponseID: responseID})
	case *openresponses.OutputItemDoneEvent:
		item := e.Item
		if m, ok := item.(*openresponses.Message); ok && r.cfg.OutputGuard != nil {
			// The guard sees the message before anything keeps it.
			replacement, err := r.cfg.OutputGuard(r.ctx, OutputInfo{RunID: r.runID, Turn: r.turn, ResponseID: responseID, Message: m})
			if err != nil {
				return true, fmt.Errorf("agentturn: output guard: %w", err)
			}
			if replacement != nil {
				item = replacement
			}
		}
		if !committed {
			// Nothing commits the attempt yet, so the item waits: a
			// failure now is retried and leaves no trace.
			r.held = append(r.held, heldItem{item: item, responseID: responseID})
			return false, nil
		}
		r.transcript = append(r.transcript, item)
		r.added = append(r.added, item)
		return false, r.emit(&ItemEnd{RunID: r.runID, Turn: r.turn, Item: item, ResponseID: responseID})
	case *openresponses.ErrorEvent:
		return false, e.Err()
	}
	if idx, ok := outputIndex(ev); ok {
		return false, r.emit(&ItemUpdate{RunID: r.runID, Turn: r.turn, Item: acc.Response().Output[idx], Stream: ev, ResponseID: responseID})
	}
	return false, nil
}

// commitsAttempt reports whether an item opening commits the attempt:
// the model has begun its answer, so a failure after it cannot be
// retried without sending part of the answer twice.
func commitsAttempt(item openresponses.Item) bool {
	switch item.(type) {
	case *openresponses.Message, *openresponses.FunctionCall:
		return true
	}
	return false
}

// flushHeld appends the items the attempt held, in order, with their
// item_end events.
func (r *runner) flushHeld() error {
	held := r.held
	r.held = nil
	for _, h := range held {
		r.transcript = append(r.transcript, h.item)
		r.added = append(r.added, h.item)
		if err := r.emit(&ItemEnd{RunID: r.runID, Turn: r.turn, Item: h.item, ResponseID: h.responseID}); err != nil {
			return err
		}
	}
	return nil
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
	// cut is set when the abort settled the call rather than the tool,
	// so no output is appended for it whatever its error says.
	cut bool
	// parent is the call whose tool made this one with Invoke, empty
	// for a call of the model's batch.
	parent string
	// turn is the turn the call belongs to, taken when the call was
	// prepared: a nested call is settled on the goroutine of the tool
	// that made it, which must not read the loop's own field.
	turn int
	// appended is set once the call's output is in the transcript.
	appended bool
	// note is text appended after the batch's outputs, from the
	// decision or the approval, as a developer or a user message.
	note openresponses.Item
}

// toolBatch runs the calls of a turn: preflight in order, execute,
// then append the outputs in the model's order. It returns the results
// in that order and the calls a hook deferred to the caller.
func (r *runner) toolBatch(ctx context.Context, tools agenttool.Set, calls []*openresponses.FunctionCall) ([]agenttool.Result, []*openresponses.FunctionCall, error) {
	if len(calls) == 0 {
		return nil, nil, nil
	}
	// Hooks and tools see the conversation that produced the calls.
	ctx = r.toolContext(ctx, tools)
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
// snapshot of the working transcript attached, and the invoker that
// runs another of the turn's tools through the loop.
func (r *runner) toolContext(ctx context.Context, tools agenttool.Set) context.Context {
	ctx = ContextWithTranscript(ctx, append(Transcript(nil), r.transcript...))
	// The turn is taken here, on the loop's goroutine: a tool that
	// keeps its context past its batch and invokes from a goroutine of
	// its own must not read a turn the loop is writing.
	turn := r.turn
	return context.WithValue(ctx, invokerKey{}, invoker(func(ctx context.Context, name string, args json.RawMessage) (agenttool.Result, error) {
		return r.invoke(ctx, tools, turn, name, args)
	}))
}

// preflightAll runs preflight for every call in the model's order and
// checks for an abort before anything executes.
func (r *runner) preflightAll(ctx context.Context, tools agenttool.Set, calls []*openresponses.FunctionCall) ([]*callState, error) {
	batch := make([]*callState, len(calls))
	for i, call := range calls {
		p, err := r.preflight(ctx, tools, call, calls, i)
		if err != nil {
			return nil, err
		}
		batch[i] = p
	}
	if ctx.Err() != nil {
		return nil, r.abortBatch(ctx, batch, context.Cause(ctx))
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
		p.settled, p.cut = true, true
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
		if p.cut {
			continue
		}
		if p.err != nil && ctx.Err() != nil && (errors.Is(p.err, ctx.Err()) || errors.Is(p.err, context.Cause(ctx))) {
			// The tool was cut off in flight: it returned the
			// cancellation, or the cause a host gave it.
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
	if ctx.Err() != nil {
		if aerr := r.appendFinished(ctx, batch); aerr != nil {
			return aerr
		}
		return stop(ReasonAborted, context.Cause(ctx))
	}
	return nil
}

// collect appends the outputs in the model's order, then the notes the
// decisions attached, and returns the results with the deferred calls.
func (r *runner) collect(batch []*callState) ([]agenttool.Result, []*openresponses.FunctionCall, error) {
	results := make([]agenttool.Result, len(batch))
	outputs := make(openresponses.Items, 0, len(batch))
	var notes openresponses.Items
	var deferred []*openresponses.FunctionCall
	for i, p := range batch {
		results[i] = p.result
		if p.deferred {
			deferred = append(deferred, p.call)
			continue
		}
		p.appended = true
		outputs = append(outputs, &openresponses.FunctionCallOutput{CallID: p.call.CallID, Output: p.result.Output})
		if p.note != nil {
			notes = append(notes, p.note)
		}
	}
	if err := r.appendItems(append(outputs, notes...)); err != nil {
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
	ctx = r.toolContext(ctx, tools)
	batch := make([]*callState, len(approved))
	for i, ap := range approved {
		p := r.prepare(tools, r.turn, ap.call, ap.args)
		if ap.note != "" {
			p.note = openresponses.UserText(ap.note)
		}
		if err := r.emit(&ToolStart{RunID: r.runID, Turn: r.turn, CallID: p.call.CallID, Name: p.call.Name, Args: p.args, Decision: &ToolDecision{Args: ap.args, Note: ap.note, By: ap.by}}); err != nil {
			return nil, err
		}
		if err := r.check(ctx, p, false); err != nil {
			return nil, err
		}
		batch[i] = p
	}
	if ctx.Err() != nil {
		return nil, r.abortBatch(ctx, batch, context.Cause(ctx))
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
func (r *runner) preflight(ctx context.Context, tools agenttool.Set, call *openresponses.FunctionCall, batch []*openresponses.FunctionCall, index int) (*callState, error) {
	p := r.prepare(tools, r.turn, call, nil)
	var terminate bool
	var decision *ToolDecision
	if r.cfg.BeforeToolCall != nil {
		var err error
		decision, err = r.beforeToolCall(ctx, ToolCallInfo{RunID: r.runID, Turn: r.turn, Call: call, Tool: p.tool, Args: p.args, Batch: batch, Index: index})
		if err != nil {
			return nil, fmt.Errorf("agentturn: before-tool-call hook: %w", err)
		}
		if decision != nil {
			if decision.Args != nil {
				p.args = decision.Args
			}
			if decision.Note != "" && decision.Action == Allow {
				p.note = openresponses.DeveloperText(decision.Note)
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
func (r *runner) prepare(tools agenttool.Set, turn int, call *openresponses.FunctionCall, args json.RawMessage) *callState {
	p := &callState{call: call, args: args, turn: turn}
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
		override, err := r.afterToolCall(ctx, ToolResultInfo{RunID: r.runID, Turn: p.turn, Call: p.call, Tool: p.tool, Args: p.args, Result: p.result, Err: p.err})
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
	return r.emit(&ToolEnd{RunID: r.runID, Turn: p.turn, CallID: p.call.CallID, Name: p.call.Name, Result: p.result, Err: p.err, Blocked: p.blocked, Parent: p.parent})
}

func validObject(raw json.RawMessage) bool {
	var v map[string]json.RawMessage
	return json.Unmarshal(raw, &v) == nil && v != nil
}

type invokerKey struct{}

// invoker runs one nested call under the turn that attached it.
type invoker func(ctx context.Context, name string, args json.RawMessage) (agenttool.Result, error)

// ErrNoInvoker is returned by [Invoke] outside a tool call of a loop.
var ErrNoInvoker = errors.New("agentturn: no loop on the context to invoke a tool through")

// Invoke runs one of the turn's tools as if the model had asked for it
// under the call in flight: [Config.BeforeToolCall] decides, tool_start
// and tool_end are emitted with Parent naming the call that made it,
// [Config.AfterToolCall] may override the result, and a session
// recorder writes it. It is what a tool that lets its code reach the
// agent's other tools, a code-execution kernel over a loopback bridge,
// calls instead of holding an agenttool.Set of its own, where the
// policy, the events and the record would all be absent.
//
// The result is the one the model would have seen, with the error
// beside it: a tool that failed, a name no tool has, arguments that are
// not an object, or a call the hook refused, whose Reason is the error.
// A hook that defers the call refuses it instead, since a nested call
// cannot be handed to the caller: it belongs to a tool that is running.
// Nothing is appended to the transcript, so a nested call costs no
// items and a Terminate on its result means nothing to the loop.
//
// The call it is made under comes from agenttool.CallFrom, which
// agenttool.New puts on every typed tool's context; a tool that
// implements the interface itself and wants the parent named passes
// agenttool.WithCall. Outside a loop, Invoke returns [ErrNoInvoker].
//
// Call it from a tool, with the context the tool was given, and not
// from a hook or a subscriber: BeforeToolCall and AfterToolCall take
// one call at a time, and a subscriber is called while the agent holds
// delivery, so either would wait for something it is itself holding.
func Invoke(ctx context.Context, name string, args json.RawMessage) (agenttool.Result, error) {
	fn, _ := ctx.Value(invokerKey{}).(invoker)
	if fn == nil {
		return agenttool.Result{}, ErrNoInvoker
	}
	return fn(ctx, name, args)
}

// invoke runs a nested call through the turn's hooks, events and
// executor. It never touches the transcript: the call is the work of
// the call that made it.
func (r *runner) invoke(ctx context.Context, tools agenttool.Set, turn int, name string, args json.RawMessage) (agenttool.Result, error) {
	parent := ""
	if call, ok := agenttool.CallFrom(ctx); ok {
		parent = call.ID
	}
	call := &openresponses.FunctionCall{CallID: openresponses.NewID("call"), Name: name, Arguments: string(args)}
	p := r.prepare(tools, turn, call, args)
	p.parent = parent
	var decision *ToolDecision
	if r.cfg.BeforeToolCall != nil {
		var err error
		decision, err = r.beforeToolCall(ctx, ToolCallInfo{RunID: r.runID, Turn: turn, Call: call, Tool: p.tool, Args: p.args, Batch: []*openresponses.FunctionCall{call}, Index: 0})
		if err != nil {
			return agenttool.Result{}, fmt.Errorf("agentturn: before-tool-call hook: %w", err)
		}
		if decision != nil {
			if decision.Args != nil {
				p.args = decision.Args
			}
			switch decision.Action {
			case Block, Defer:
				reason := decision.Reason
				if reason == "" {
					reason = "call blocked"
				}
				if decision.Action == Defer {
					reason = "a nested call cannot be deferred to the caller: " + reason
				}
				p.blocked = true
				p.err = errors.New(reason)
			}
		}
	}
	if err := r.emit(&ToolStart{RunID: r.runID, Turn: turn, CallID: call.CallID, Name: name, Args: p.args, Decision: decision, Parent: parent}); err != nil {
		return agenttool.Result{}, err
	}
	if err := r.check(ctx, p, false); err != nil {
		return agenttool.Result{}, err
	}
	if p.settled {
		return p.result, p.err
	}
	job := agenttool.Job{Tool: p.tool, Call: agenttool.Call{ID: call.CallID, Args: p.args}}
	results, errs := agenttool.Executor{}.Results(ctx, []agenttool.Job{job})
	p.result, p.err = results[0], errs[0]
	p.settled = true
	if err := r.settle(ctx, p); err != nil {
		return agenttool.Result{}, err
	}
	return p.result, p.err
}
