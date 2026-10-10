package agentturn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"
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
	// ErrGuard is what a guard wraps to end the run as a policy stop
	// rather than a failure: ReasonStopped with StopGuard, the error on
	// RunEnd.Err. BeforeTurn, BeforeModelCall, OutputGuard and
	// ShouldStopAfterTurn each read it so.
	ErrGuard = errors.New("agentturn: guard stopped the run")
)

// WithheldCallOutput is the text of the output the loop appends for a
// function call of a response an [Config.OutputGuard] withheld with an
// error wrapping [ErrGuard]: the call completed in the stream before
// the message the guard stopped on, and the loop closes it, never
// having dispatched it, so the transcript holds no call without an
// output and the next prompt goes ahead. The text is fixed and never
// the guard's error, which may say what the guard kept from the model.
const WithheldCallOutput = "Error: not run: the response that made this call was withheld"

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
// [ErrNoModel], [ErrTriggerExtra], a duplicate tool name, or
// [ErrInputRequired] for a
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
	if err := TriggerFromContext(ctx).Validate(); err != nil {
		return failNow(err)
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
	if err := TriggerFromContext(ctx).Validate(); err != nil {
		return failNow(err)
	}
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
		// The context RunContext hands the tools: the caller's values,
		// cut by the caller's cancellation or a consumer that breaks
		// out, and not by the run ending.
		runCtx, runCancel := linkRunContext(ctx)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		events := make(chan Event, EventBuffer)
		go func() {
			defer close(events)
			r := &runner{
				cfg:        cfg,
				transcript: append(Transcript(nil), t...),
				runCtx:     runCtx,
				reasoning:  reasoningFromContext(ctx).merged(nil),
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
			_, isEnd := ev.(*RunEnd)
			if !yield(ev) {
				stopped = true
				cancel()
				if !isEnd {
					// Breaking out before the end is an abort; breaking
					// out on it is not, and work the run started goes on.
					runCancel(context.Canceled)
				}
				continue
			}
			if isEnd {
				stopped = true
			}
		}
	}
}

type transcriptKey struct{}
type runIDKey struct{}
type runContextKey struct{}
type steeredKey struct{}
type triggerKey struct{}
type decidersKey struct{}
type reasonsKey struct{}
type originsKey struct{}
type reservedKey struct{}

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

// ContextWithReservedCallIDs attaches call IDs a call the model makes
// in a run started with ctx must not take, beside those the agent
// reserved with [WithReservedCallIDs] and those an outer context
// attached: the IDs a record holds that the run's transcript does not,
// such as a child session reopened under the call that made it, whose
// new agent knows none of the calls its earlier runs made. The session
// package's Recorder.ChildContext attaches them for a child run.
func ContextWithReservedCallIDs(ctx context.Context, ids ...string) context.Context {
	if len(ids) == 0 {
		return ctx
	}
	reserved := map[string]bool{}
	for id := range reservedFromContext(ctx) {
		reserved[id] = true
	}
	for _, id := range ids {
		reserved[id] = true
	}
	return context.WithValue(ctx, reservedKey{}, reserved)
}

// reservedFromContext returns the call IDs attached to ctx by
// [ContextWithReservedCallIDs], or nil.
func reservedFromContext(ctx context.Context) map[string]bool {
	reserved, _ := ctx.Value(reservedKey{}).(map[string]bool)
	return reserved
}

// ContextWithDeciders attaches who decided the answer to each pending
// call, by call ID, in the session format's terms ("human", "policy",
// "agent"), in place of any an outer context attached; an empty map
// clears them. [Agent.Resume] does it from the [Answer.By] of the answers
// it was given, so a subscriber writing the record of an output the
// caller supplied, which raises no tool_start to carry a decision, can
// say who wrote it. A host driving the low-level [Run] with the
// outputs as prompts attaches it itself. The loop reads nothing from
// it.
func ContextWithDeciders(ctx context.Context, by map[string]string) context.Context {
	if len(by) == 0 {
		if ctx.Value(decidersKey{}) == nil {
			return ctx
		}
		return context.WithValue(ctx, decidersKey{}, map[string]string(nil))
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

// ContextWithReasons attaches why each pending call was answered as it
// was, by call ID, as [ContextWithDeciders] attaches who decided, in
// place of any an outer context attached; an empty map clears them.
// [Agent.Resume] does it from the [Answer.Reason] of the answers it was
// given as outputs, so a subscriber writing the record of an output
// for a call that may have run can say why the call was not run again.
// The loop reads nothing from it.
func ContextWithReasons(ctx context.Context, reasons map[string]string) context.Context {
	if len(reasons) == 0 {
		if ctx.Value(reasonsKey{}) == nil {
			return ctx
		}
		return context.WithValue(ctx, reasonsKey{}, map[string]string(nil))
	}
	out := make(map[string]string, len(reasons))
	for k, v := range reasons {
		out[k] = v
	}
	return context.WithValue(ctx, reasonsKey{}, out)
}

// ReasonFromContext returns the reason the caller gave for the answer
// to callID, or "" when none was given.
func ReasonFromContext(ctx context.Context, callID string) string {
	reasons, _ := ctx.Value(reasonsKey{}).(map[string]string)
	return reasons[callID]
}

// ContextWithOrigins attaches where the output answering each pending
// call was taken from, by call ID, as [ContextWithReasons] attaches
// why, in place of any an outer context attached; an empty map clears
// them. [Agent.Resume] does it from the [Answer.Origin] of the answers
// it was given as outputs, so a subscriber writing the record of an
// output a record held rather than one produced now can tie it to the
// entry it repeats. The loop reads nothing from it.
func ContextWithOrigins(ctx context.Context, origins map[string]string) context.Context {
	if len(origins) == 0 {
		if ctx.Value(originsKey{}) == nil {
			return ctx
		}
		return context.WithValue(ctx, originsKey{}, map[string]string(nil))
	}
	out := make(map[string]string, len(origins))
	for k, v := range origins {
		out[k] = v
	}
	return context.WithValue(ctx, originsKey{}, out)
}

// OriginFromContext returns where the caller said the output answering
// callID was taken from, or "" when it said nothing.
func OriginFromContext(ctx context.Context, callID string) string {
	origins, _ := ctx.Value(originsKey{}).(map[string]string)
	return origins[callID]
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

// RunContext returns the context of the run a tool call, a hook, the
// transform or the model call serves: the values of the context the
// run was started with, the run ID among them, cancelled when the run
// is aborted, through [Agent.Abort], [Agent.AbortCause] or the
// cancellation of the context given to Prompt, and not when the batch
// ends or the run ends by itself. It is what a tool derives background work from that must
// outlive its call and not the run's abort: a task that returns at
// once and reports later, a watcher, a detached child. Once the run
// has ended, Agent.Abort reaches the agent's next run and not this
// one, so work that must be stoppable after that keeps a cancel of its
// own. [Config.ToolRecorder] is on it, so a job that writes the handle
// of what it started with agenttool.WriteRecord reaches the record. The
// call it serves is not: a root run's context carries no call, and a
// child run's, one a tool started on the context of its own call as
// tools/agent does, carries the call that started the child. So a job
// names its call with agenttool.WithCall(rc, call), as tools/agent does
// for a detached child, and one in a child that does not is taken for
// the work of the parent's call. Where a record written after the run
// has ended is filed is the recorder's to say; a session recorder files
// it at the leaf of the session of the run the job came from, the
// child's when the child was given its session ID with
// session.Recorder.ChildContext and the recorder's own otherwise, and
// names the call when that session holds it. The transcript, the invoker, the tool
// elicitor and the steer signal of the call belong to its batch and
// are not on it; a job that needs one carries it from the call's
// context. It reports false outside a loop. Each run registers it with the context
// it was started with until that context ends or the run is aborted,
// so a host that prompts every run with one long-lived cancellable
// context keeps one small registration per finished run until then.
func RunContext(ctx context.Context) (context.Context, bool) {
	rc, ok := ctx.Value(runContextKey{}).(context.Context)
	return rc, ok
}

// Steered returns a channel that is closed when an item is steered into
// the run the tool serves, with [Agent.Steer] or [Agent.Queue], after
// its batch began or before it and not yet drained, so a tool that only
// waits, a sleep, a poll, a wait on another agent, selects on it and
// returns early with what it has; the steered item then joins the run
// after the batch, as ever. The loop does not cut the tool: hearing the
// steer is the tool's choice. It returns nil outside an agent's run,
// and a nil channel never closes.
func Steered(ctx context.Context) <-chan struct{} {
	ch, _ := ctx.Value(steeredKey{}).(<-chan struct{})
	return ch
}

// linkRunContext returns the context RunContext hands a run's tools:
// the values of ctx, cancelled with ctx's cause when ctx is, or by the
// cancel returned, and released from ctx once it is cancelled either
// way.
func linkRunContext(ctx context.Context) (context.Context, context.CancelCauseFunc) {
	runCtx, runCancel := context.WithCancelCause(context.WithoutCancel(ctx))
	stop := context.AfterFunc(ctx, func() { runCancel(context.Cause(ctx)) })
	context.AfterFunc(runCtx, func() { stop() })
	return runCtx, runCancel
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
// error to abort the run; steer and followUp drain the queues when set,
// each item with how it was queued and the trigger it was queued with.
type runner struct {
	cfg        Config
	transcript Transcript
	send       func(Event) error
	steer      func() []TurnInput
	followUp   func() []TurnInput
	// last drains both queues for a run that would otherwise end and,
	// when they are empty, marks it past its last drain in the same
	// step; closing marks it so when the run decides to stop, and
	// reports whether anything is still queued. Both are nil for the
	// low-level loop, which has no queues to deliver into.
	last    func() []TurnInput
	closing func() bool
	// runID, when set before the run, is the ID the run takes, minted
	// where the run was started so the agent names it from then on.
	runID string
	// steered returns the channel a steer closes, for the batch about
	// to run; nil when nothing can steer the run.
	steered func() <-chan struct{}
	// runCtx is the context RunContext hands a tool: the run's values,
	// cancelled on abort and not when the run ends by itself.
	runCtx context.Context
	// resuming is set for a run Agent.Resume started, whose leading
	// outputs answer pending calls rather than carry what the trigger
	// sent.
	resuming bool

	// hookMu serialises the tool hooks. A nested call runs on the
	// goroutine of the tool that made it, so without it two tools of
	// one batch would enter BeforeToolCall at once, and a nested
	// call's AfterToolCall would run beside the batch's. It is held
	// across the hook alone, never across an emit or a tool, so the
	// only way to block on it is to call Invoke from inside a hook.
	hookMu sync.Mutex

	turn  int
	added Transcript
	// ctx is the run's context, for the hooks the stream calls.
	ctx context.Context
	// mark is the length of the transcript after the previous turn's
	// response, so the next turn_start can name what was appended since.
	mark int
	// arrived is what joined the transcript since the previous turn's
	// response, each item with how it came, for the next turn's
	// BeforeTurn and turn_start; it opens with the items the transcript
	// already ended with when the run started that the model did not
	// produce, which the first turn answers.
	arrived []TurnInput
	// deferred holds the IDs of the calls a hook handed to the caller
	// during this run, so the run end can say why they are pending,
	// each with the arguments it was held with, which its decision may
	// have rewritten, and the decision that held it.
	deferred map[string]heldCall
	// callTools holds the tool each call of this run's batches resolved
	// to, so a pending call carries the tool a prompt asks about.
	callTools map[string]agenttool.Tool
	// undispatched holds the IDs of the calls whose tool_dispatch a
	// subscriber refused, so the run end can say their tools never ran.
	// It is written on the loop's goroutine as the batch is drained.
	undispatched map[string]bool
	// dispatched holds each call this run handed to its tool, with the
	// key and the arguments it was handed over with, so the run end can
	// say which calls may have run and how. It is written on the loop's
	// goroutine once a batch is drained.
	dispatched map[string]handOff
	// approved holds the IDs of the calls a Resume approved, so one cut
	// before its dispatch reads as never handed over rather than as
	// waiting for an answer it has had.
	approved map[string]bool
	// prior is what the agent knew of the calls pending when the run
	// started, so a call this run did not dispatch keeps the reason and
	// the key an earlier run gave it.
	prior []PendingCall
	// reserved holds the call IDs the agent reserved, which a call the
	// model makes must not take. The agent does not change it while the
	// run is active. ctxReserved holds those the run's context names.
	reserved, ctxReserved map[string]bool
	// reasoning says which model produced the reasoning items of the
	// transcript it knows, so a request leaves another model's out.
	reasoning ReasoningModels

	// held are the completed items of the attempt in flight that the
	// transcript does not have yet, because nothing has committed the
	// attempt: a reasoning item a model opens before its answer. They
	// are appended when the attempt commits and dropped when it ends
	// without committing.
	held []heldItem
	// callAt maps the output index of every function call the attempt
	// in flight opened or completed, the held ones included, to the
	// last call there and the call ID the loop decided for it: the
	// model's when it names no other call, one of the loop's own when
	// it is empty or taken. callItems maps the item ID of each such
	// call that has one to the same ID, and names the call when it has
	// one: a stream that opens a second call at the index of the first,
	// as Ollama's does, gives each its own. decided holds every ID
	// decided in the attempt, which a later call may not take.
	callAt    map[int]callSlot
	callItems map[string]string
	decided   map[string]bool
	// opened are the items of the attempt in flight in the order they
	// opened, each with its output index and, once it is done, the item
	// as completed, so the guard sees an earlier item as the loop
	// completed it, a replacement included.
	opened []openedItem
}

// callSlot is the last function call opened at an output index: the
// call ID decided for it and the item ID it carried, "" for none.
type callSlot struct{ id, itemID string }

// openedItem is an item of the attempt in flight: its output index,
// the item as completed once it is done, and, for a function call, the
// item ID the stream gave it and the call ID the model gave it, the
// last each event carried, by which renameCalls pairs the completed
// response's calls with the ones the transcript took.
type openedItem struct {
	index int
	done  openresponses.Item
	// itemID and modelCallID are the function call's as the model sent
	// them, "" for none; done carries the call ID the loop decided.
	itemID, modelCallID string
}

// heldItem is a completed item waiting for its attempt to commit.
type heldItem struct {
	item        openresponses.Item
	responseID  string
	modelCallID string
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
	// withheld says OutputGuard withheld a message of the response in
	// flight, which is what stopped the run.
	withheld bool
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
// outputs, by and reason saying who approved it and why, for the
// record, and key, when set, as the idempotency key in place of a new
// one. decide is set for a call nothing has decided, one that never
// started, whose approval BeforeToolCall still rules on.
type approval struct {
	call   *openresponses.FunctionCall
	args   json.RawMessage
	note   string
	by     string
	reason string
	key    string
	decide bool
}

// run drives the loop and returns the RunEnd. terminate says the caller
// asked the run to end once the answers are in, without a model call.
// A panic in a tool is recovered by agenttool's executor into a
// PanicError the model sees as an error output; a panic in a hook or a
// subscriber is not recovered and unwinds without a RunEnd.
func (r *runner) run(ctx context.Context, prompts openresponses.Items, approved []approval, terminate bool) *RunEnd {
	if r.runID == "" {
		r.runID = openresponses.NewID("run")
	}
	r.mark = len(r.transcript)
	r.arrived = continuedInputs(r.transcript)
	r.ctxReserved = reservedFromContext(ctx)
	// Everything the run calls, transform, hooks, model and tools, can
	// tell which run it serves.
	ctx = ContextWithRunID(ctx, r.runID)
	r.runCtx = ContextWithRunID(r.runCtx, r.runID)
	if r.cfg.ToolRecorder != nil {
		// A record a background job writes goes where its call's would.
		r.runCtx = agenttool.ContextWithRecorder(r.runCtx, r.cfg.ToolRecorder)
	}
	ctx = context.WithValue(ctx, runContextKey{}, r.runCtx)
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
		end.Withheld = stop.withheld
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
	// Nothing is drained from here on, whatever ended the run, so an
	// item steered during run_end waits for the next run, and the agent
	// says so.
	r.ending()
	// A subscriber that fails on run_end cannot change the outcome; the
	// run has already ended.
	_ = r.emit(end)
	return end
}

// pending lists the calls of the transcript without an output, each
// with why: deferred by a hook in this run, cut off after or before
// its dispatch in this run, or as the agent knew it when the run
// started. A call an earlier run dispatched that this one did not
// dispatch again is still the ambiguous call it was, with its key.
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
	prior := make(map[string]PendingCall, len(r.prior))
	for _, p := range r.prior {
		prior[p.Call.CallID] = p
	}
	out := make([]PendingCall, len(calls))
	for i, call := range calls {
		before, known := prior[call.CallID]
		h, dispatched := r.dispatched[call.CallID]
		held, deferred := r.deferred[call.CallID]
		p := PendingCall{Call: call, Reason: PendingUnknown, Tool: r.callTools[call.CallID]}
		switch {
		case deferred:
			// A decision that rewrote the call's arguments and held it
			// decided on those, and an approval runs them.
			p.Reason, p.Decision = PendingDeferred, held.decision
			if held.args != nil && !sameArgs(held.args, orEmpty(nil, call.Arguments)) {
				p.Args = held.args
			}
		case dispatched:
			p.Reason, p.IdempotencyKey = PendingAborted, h.key
			if !sameArgs(h.args, orEmpty(nil, call.Arguments)) {
				p.Args = h.args
			}
		case known && before.MayHaveRun():
			// It may have run before this run, which did not run it
			// again, so it is as ambiguous as it was. A call held
			// after its dispatch that this run approved has its hold
			// answered and is left as a cut leaves one; a resume that
			// failed before its batch leaves it held.
			p.Reason, p.Dispatched, p.IdempotencyKey, p.Args = before.Reason, before.Dispatched, before.IdempotencyKey, before.Args
			p.Ran, p.RanWhere, p.Decision = before.Ran, before.RanWhere, before.Decision
			if before.Reason == PendingDeferred && r.approved[call.CallID] {
				p.Reason, p.Dispatched, p.Decision = PendingAborted, false, nil
			}
		case r.undispatched[call.CallID] || mine[call] || r.approved[call.CallID]:
			p.Reason = PendingUndispatched
		case known:
			// As the agent knew it, the arguments a decision held it
			// with included: a resume that failed before its batch
			// decided nothing new.
			p.Reason, p.Dispatched, p.IdempotencyKey, p.Args = before.Reason, before.Dispatched, before.IdempotencyKey, before.Args
			p.Ran, p.RanWhere, p.Decision = before.Ran, before.RanWhere, before.Decision
		}
		if p.Tool == nil {
			p.Tool = before.Tool
		}
		out[i] = p
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
	trigger := TriggerFromContext(ctx)
	if err := r.emit(&RunStart{RunID: r.runID, Source: r.source(prompts, approved), Trigger: trigger}); err != nil {
		return err
	}
	// The outputs a prompt opens with answer pending calls, and arrive
	// as a resume's whether Resume or Prompt brought them; what follows
	// them is what the run was prompted with, or on a Resume the notes
	// the caller supplied. On a Resume the decisions say who gave the
	// outputs, so the trigger rides only on the notes; a Prompt's item
	// events carry it on every item, as before.
	answers := 0
	for _, item := range unhideAll(prompts) {
		if _, ok := item.(*openresponses.FunctionCallOutput); !ok {
			break
		}
		answers++
	}
	onAnswers, rest, onRest := trigger, InputPrompt, trigger
	if r.resuming {
		onAnswers, rest, onRest = Trigger{}, InputResume, Trigger{}
	}
	if err := r.appendWith(prompts[:answers], InputResume, Trigger{}, onAnswers); err != nil {
		return err
	}
	if err := r.appendWith(prompts[answers:], rest, onRest, trigger); err != nil {
		return err
	}
	if len(approved) == 0 && !terminate {
		// What was steered in while the agent was idle joins the run
		// before its first model call, as it would have joined a run in
		// flight before its next one. A run that begins with approved
		// calls drains after their batch, and a refusal leaves the
		// queue for the run that follows it.
		if err := r.appendArrived(r.drain(r.steer)); err != nil {
			return err
		}
	}
	if len(approved) > 0 {
		results, pending, err := r.approvedBatch(ctx, approved)
		if err != nil {
			return err
		}
		if len(pending) > 0 {
			// BeforeToolCall deferred a call that never started, as it
			// would have in the run that made it.
			r.ending()
			return stop(ReasonInputRequired, nil)
		}
		if cause, ok := terminates(results); ok && !terminate {
			return stopped(cause, nil)
		}
		// What was steered in while the caller was deciding goes to the
		// model with the answers, as after any batch.
		if err := r.appendArrived(r.drain(r.steer)); err != nil {
			return err
		}
	}
	if terminate {
		return stopped(StopRefused, nil)
	}
	for {
		if ctx.Err() != nil {
			return stop(ReasonAborted, context.Cause(ctx))
		}
		r.turn++
		if r.cfg.BeforeTurn != nil {
			items, err := r.cfg.BeforeTurn(ctx, TurnStartInfo{RunID: r.runID, Turn: r.turn, Transcript: r.transcript, Inputs: append([]TurnInput(nil), r.arrived...)})
			if err != nil {
				if errors.Is(err, ErrGuard) {
					return stopped(StopGuard, err)
				}
				return fmt.Errorf("agentturn: before-turn hook: %w", err)
			}
			if err := r.appendItems(items, InputHook); err != nil {
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
		// Every stop after the turn is decided before the queues are
		// drained, and marks the run past its last drain as it is
		// decided, so an item steered during the turn stays queued for
		// the run that follows rather than being appended to a run that
		// will not call the model again.
		if len(pending) > 0 {
			r.ending()
			return stop(ReasonInputRequired, nil)
		}
		if r.cfg.ShouldStopAfterTurn != nil {
			halt, err := r.cfg.ShouldStopAfterTurn(ctx, TurnInfo{RunID: r.runID, Turn: r.turn, Response: resp, ToolResults: results, Final: len(calls) == 0, Transcript: r.transcript})
			if err != nil {
				r.ending()
				if errors.Is(err, ErrGuard) {
					return stopped(StopGuard, err)
				}
				return fmt.Errorf("agentturn: should-stop-after-turn hook: %w", err)
			}
			if halt {
				r.ending()
				return stopped(StopHook, nil)
			}
		}
		if cause, ok := terminates(results); ok {
			r.ending()
			return stopped(cause, nil)
		}
		if r.cfg.MaxTurns > 0 && r.turn >= r.cfg.MaxTurns {
			// The budget is spent: a turn with calls stops, and so does a
			// final turn with anything queued, which the run can no
			// longer answer; a final turn with nothing queued is done.
			if waiting := r.ending(); len(calls) > 0 || waiting {
				return stopped(StopMaxTurns, nil)
			}
			return nil
		}
		var queued []TurnInput
		if len(calls) > 0 {
			queued = r.drain(r.steer)
		} else {
			queued = r.drainLast()
			if len(queued) == 0 {
				return nil
			}
		}
		if err := r.appendArrived(queued); err != nil {
			return err
		}
	}
}

func (r *runner) drain(q func() []TurnInput) []TurnInput {
	if q == nil {
		return nil
	}
	return q()
}

// ending marks the run past its last drain and reports whether an item
// is still queued; the low-level loop has no queues.
func (r *runner) ending() bool {
	if r.closing == nil {
		return false
	}
	return r.closing()
}

// drainLast drains the steered items and then the follow-ups for a run
// that would otherwise end.
func (r *runner) drainLast() []TurnInput {
	if r.last != nil {
		return r.last()
	}
	return append(r.drain(r.steer), r.drain(r.followUp)...)
}

// continuedInputs returns the items t ends with that the model did not
// produce, after its last output, each as [InputContinued]: the
// function call outputs, the user and developer messages and the
// namespaced custom items a run started on t answers before anything
// it appends. Any other item, an assistant message, a function call, a
// reasoning item, ends the tail.
func continuedInputs(t Transcript) []TurnInput {
	start := len(t)
	for start > 0 && !modelProduced(t[start-1]) {
		start--
	}
	if start == len(t) {
		return nil
	}
	out := make([]TurnInput, 0, len(t)-start)
	for _, item := range t[start:] {
		out = append(out, TurnInput{Item: item, Mode: InputContinued})
	}
	return out
}

// modelProduced reports whether item is one the model produces rather
// than one a caller, a tool or the loop appended for it to answer.
func modelProduced(item openresponses.Item) bool {
	switch v := item.(type) {
	case nil:
		return true
	case *openresponses.FunctionCallOutput:
		return false
	case *openresponses.Message:
		return v.Role == openresponses.RoleAssistant
	}
	return !strings.Contains(item.ItemType(), ":")
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

// appendItems adds items the loop itself appends, a hook's items, a
// batch's outputs and notes, the outputs closing a withheld response,
// to the transcript with their item events, each arriving as mode with
// no trigger.
func (r *runner) appendItems(items openresponses.Items, mode InputMode) error {
	return r.appendWith(items, mode, Trigger{}, Trigger{})
}

// appendWith appends items as arriving by mode: trigger is what the
// next turn's inputs carry for each, and endTrigger what each item_end
// carries, the run's for an item the run was prompted with. An item the
// caller marked with [Hidden] is unwrapped here, so the transcript and
// the request hold the item itself and only its events say it is
// hidden.
func (r *runner) appendWith(items openresponses.Items, mode InputMode, trigger, endTrigger Trigger) error {
	for _, item := range items {
		if err := r.appendOne(item, mode, trigger, endTrigger); err != nil {
			return err
		}
	}
	return nil
}

// appendArrived appends the items a drain took from the queues, each
// as it was queued: steered, followed up or delivered, with the trigger
// its Queued report carried. Their item_end carries no trigger, as
// before; the run's trigger names what started the run, not them.
func (r *runner) appendArrived(inputs []TurnInput) error {
	for _, in := range inputs {
		if err := r.appendOne(in.Item, in.Mode, in.Trigger, Trigger{}); err != nil {
			return err
		}
	}
	return nil
}

// appendOne appends one item the loop did not stream with its item
// events, and remembers how it arrived for the next turn's inputs.
func (r *runner) appendOne(item openresponses.Item, mode InputMode, trigger, endTrigger Trigger) error {
	if item == nil {
		return nil
	}
	item, hidden := Unhide(item)
	if err := r.emit(&ItemStart{RunID: r.runID, Turn: r.turn, Item: item, Hidden: hidden}); err != nil {
		return err
	}
	r.transcript = append(r.transcript, item)
	r.added = append(r.added, item)
	r.arrived = append(r.arrived, TurnInput{Item: item, Mode: mode, Trigger: trigger})
	if err := r.emit(&ItemEnd{RunID: r.runID, Turn: r.turn, Item: item, Hidden: hidden, Trigger: endTrigger}); err != nil {
		return err
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
	req.Input = r.reasoning.For(r.cfg.ModelName, r.cfg.filter()(input))
	if r.cfg.BeforeModelCall != nil {
		if err := r.cfg.BeforeModelCall(ctx, &req); err != nil {
			// The call was never made; the request as built is the
			// record of what was refused, whether a policy stopped the
			// run or the hook failed.
			if eerr := r.emit(&ModelBlocked{RunID: r.runID, Turn: r.turn, Request: req, Err: err}); eerr != nil {
				return openresponses.Request{}, eerr
			}
			if errors.Is(err, ErrGuard) {
				return openresponses.Request{}, stopped(StopGuard, err)
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
	if err := r.emit(&TurnStart{RunID: r.runID, Turn: r.turn, Request: req, Inputs: inputs, Arrived: append([]TurnInput(nil), r.arrived...)}); err != nil {
		return nil, err
	}
	start := len(r.transcript)
	for attempt := 1; ; attempt++ {
		resp, committed, err := r.stream(ctx, req)
		// What the attempt kept is this model's, however it ended.
		r.attribute(r.transcript[start:])
		if err == nil {
			// The next turn's inputs are what joins from here.
			r.mark, r.arrived = len(r.transcript), nil
			return resp, nil
		}
		var halt *errStop
		if errors.As(err, &halt) {
			// OutputGuard stopped the run: a policy, not a failure to
			// retry or wrap. The withheld response goes on record
			// with what the transcript took from it, and the calls it
			// made before the message are closed, so the transcript is
			// a valid input for the next run.
			if halt.withheld && resp != nil {
				resp.Output = openresponses.Items(r.transcript[start:]).Clone()
				if err := r.emit(&ResponseEnd{RunID: r.runID, Turn: r.turn, Response: resp, Withheld: true}); err != nil {
					return nil, err
				}
			}
			if err := r.closeWithheld(r.transcript[start:]); err != nil {
				return nil, err
			}
			return nil, err
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

// closeWithheld appends a [WithheldCallOutput] for each function call
// among items, the part of the transcript a withheld response added.
// Those calls were never dispatched; nothing else answers them.
func (r *runner) closeWithheld(items Transcript) error {
	var outputs openresponses.Items
	for _, call := range unansweredCalls(items) {
		outputs = append(outputs, &openresponses.FunctionCallOutput{CallID: call.CallID, Output: openresponses.FunctionCallOutputData{Text: WithheldCallOutput}})
	}
	return r.appendItems(outputs, InputTool)
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
	var halt *errStop
	r.held, r.callAt, r.callItems, r.decided, r.opened = nil, nil, nil, nil, nil
	for ev, err := range openresponses.Events(ctx, r.cfg.Model, req) {
		if halt != nil {
			// OutputGuard withheld a message: the rest of the response
			// is read for its usage alone, and nothing of it reaches
			// the transcript or an event.
			if err != nil {
				break
			}
			acc.Add(ev)
			if final, ok := openresponses.TerminalResponse(ev); ok {
				resp = final
			}
			continue
		}
		if err != nil {
			return nil, committed, err
		}
		acc.Add(ev)
		commits, err := r.streamEvent(ev, &acc, committed)
		committed = committed || commits
		if stop := (*errStop)(nil); errors.As(err, &stop) && stop.withheld {
			halt = stop
			continue
		}
		if err != nil {
			var wire *wireError
			if errors.As(err, &wire) {
				// The server reported the failure as an event. Before
				// the answer opened it is a failed attempt like a cut
				// stream, and retried as one; after, it is final.
				return nil, committed, wire.err
			}
			return nil, true, err
		}
		if final, ok := openresponses.TerminalResponse(ev); ok {
			resp = final
		}
	}
	if halt != nil {
		return withheldResponse(&acc, resp), true, halt
	}
	if resp == nil {
		return nil, committed, openresponses.ErrTruncatedStream
	}
	resp = r.renameCalls(resp)
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

// withheldResponse is the response an OutputGuard withheld a message
// of: the terminal response when the stream sent one, the response as
// accumulated otherwise, incomplete with content_filter, with its usage
// and no output, which modelTurn fills with the items the transcript
// took from it. The withheld message is not on it.
func withheldResponse(acc *openresponses.Accumulator, final *openresponses.Response) *openresponses.Response {
	base := final
	if base == nil {
		base = acc.Response()
	}
	var out openresponses.Response
	if base != nil {
		out = *base
	}
	out.Status = openresponses.ResponseStatusIncomplete
	out.IncompleteDetails = &openresponses.IncompleteDetails{Reason: openresponses.IncompleteReasonContentFilter}
	out.Error = nil
	out.Output = nil
	return &out
}

// streamEvent turns one wire event, already added to acc, into the item
// events of the turn: item_start when an output item opens, item_end
// with the transcript append when it is done, item_update for the
// events in between. committed says whether the attempt has committed;
// while it has not, a completed item is held rather than appended, and
// the return reports whether this event commits the attempt, which a
// message or a function call opening does. An error event fails the
// attempt with the wire error, wrapped as a wireError so stream can
// tell it from a delivery failure and hand the policy the error
// itself.
func (r *runner) streamEvent(ev openresponses.StreamEvent, acc *openresponses.Accumulator, committed bool) (bool, error) {
	responseID := ""
	if cur := acc.Response(); cur != nil {
		responseID = cur.ID
	}
	switch e := ev.(type) {
	case *openresponses.OutputItemAddedEvent:
		item, _ := acc.ItemAt(e.OutputIndex)
		r.opened = append(r.opened, openedItem{index: e.OutputIndex})
		if call, ok := item.(*openresponses.FunctionCall); ok {
			// The call's ID is decided as it opens, so its item_start,
			// every item_update and its item_end carry the same one.
			r.decideCallID(call, e.OutputIndex)
			r.opened[len(r.opened)-1].itemID, r.opened[len(r.opened)-1].modelCallID = call.ID, call.CallID
			item = r.withCallID(item, e.OutputIndex)
		}
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
		modelCallID := ""
		if call, ok := item.(*openresponses.FunctionCall); ok {
			if id, opened := r.decidedCallID(call, e.OutputIndex); !opened {
				// A call first seen as it completes, on a stream that
				// sends no output_item.added for it.
				r.decideCallID(call, e.OutputIndex)
			} else if call.ID != "" && r.callAt[e.OutputIndex].itemID == "" {
				// Opened with no item ID and done with one: the ID
				// names it from here, in the response the turn acts on
				// among the rest.
				r.callAt[e.OutputIndex] = callSlot{id: id, itemID: call.ID}
				r.callItems[call.ID] = id
			}
			if item = r.withCallID(call, e.OutputIndex); item != openresponses.Item(call) {
				modelCallID = call.CallID
			}
		}
		self := r.openedAt(e.OutputIndex)
		if call, ok := e.Item.(*openresponses.FunctionCall); ok {
			// What the model called the call as it completed, which is
			// what the response it completes with carries.
			r.opened[self].modelCallID = call.CallID
			if call.ID != "" {
				r.opened[self].itemID = call.ID
			}
		}
		if m, ok := item.(*openresponses.Message); ok && r.cfg.OutputGuard != nil {
			// The guard sees the message before anything keeps it,
			// after the items that opened before it.
			var before openresponses.Items
			cur := acc.Response()
			for _, prior := range r.opened[:self] {
				switch {
				case prior.done != nil:
					before = append(before, prior.done)
				case cur != nil:
					if open, ok := acc.ItemAt(prior.index); ok {
						before = append(before, r.withCallID(open, prior.index))
					}
				}
			}
			replacement, err := r.cfg.OutputGuard(r.ctx, OutputInfo{RunID: r.runID, Turn: r.turn, ResponseID: responseID, Message: m, Output: before})
			if err != nil {
				if errors.Is(err, ErrGuard) {
					// A policy stop: the message is kept from the
					// transcript and the run ends with the turn.
					return true, &errStop{reason: ReasonStopped, cause: StopGuard, err: err, withheld: true}
				}
				return true, fmt.Errorf("agentturn: output guard: %w", err)
			}
			if replacement != nil {
				item = replacement
			}
		}
		r.opened[self].done = item
		if !committed {
			// Nothing commits the attempt yet, so the item waits: a
			// failure now is retried and leaves no trace.
			r.held = append(r.held, heldItem{item: item, responseID: responseID, modelCallID: modelCallID})
			return false, nil
		}
		r.transcript = append(r.transcript, item)
		r.added = append(r.added, item)
		return false, r.emit(&ItemEnd{RunID: r.runID, Turn: r.turn, Item: item, ResponseID: responseID, ModelCallID: modelCallID})
	case *openresponses.ErrorEvent:
		return false, &wireError{err: e.Err()}
	}
	if idx, ok := outputIndex(ev); ok {
		item, _ := acc.ItemAt(idx)
		return false, r.emit(&ItemUpdate{RunID: r.runID, Turn: r.turn, Item: r.withCallID(item, idx), Stream: ev, ResponseID: responseID})
	}
	return false, nil
}

// decideCallID decides the call ID of the function call at index of
// the attempt in flight, given the ID the model sent, and records it in
// callAt, decided and, when the call has an item ID, callItems: the
// model's, or one of the loop's own when the model gave none, or gave
// one a call in the transcript already has, one another
// call of the attempt took, held or not, or one the agent or the run's
// context reserved. A call ID names one call, since an output, a
// pending list and the session record name the call by it alone. The
// new ID is the model's, in the alphabet every provider takes, with a
// random suffix, so it names no call a fold took out of the transcript
// or another branch of the session holds either. A call is decided when
// it opens, so its item_start, item_update and item_end events, the
// transcript, the response the turn acts on and every later request
// carry one ID; a call the stream never opened is decided when it
// completes.
func (r *runner) decideCallID(call *openresponses.FunctionCall, index int) {
	if r.callAt == nil {
		r.callAt, r.callItems, r.decided = map[int]callSlot{}, map[string]string{}, map[string]bool{}
	}
	id := call.CallID
	if r.callIDTaken(id) {
		id = openresponses.NewID(callIDPrefix(id))
	}
	r.callAt[index] = callSlot{id: id, itemID: call.ID}
	r.decided[id] = true
	if call.ID != "" {
		r.callItems[call.ID] = id
	}
}

// decidedCallID returns the call ID decided for call: by its item ID
// when that names a call of the attempt, and otherwise by index, its
// output index, when the last call opened there carried no item ID or
// call carries none, which is the same call on a stream that sends the
// item ID on one of its events alone.
func (r *runner) decidedCallID(call *openresponses.FunctionCall, index int) (string, bool) {
	if id, ok := r.callItems[call.ID]; ok && call.ID != "" {
		return id, true
	}
	slot, ok := r.callAt[index]
	if !ok || (call.ID != "" && slot.itemID != "") {
		return "", false
	}
	return slot.id, true
}

// callIDTaken reports whether a call the model makes may not keep id.
// The calls the attempt held are in decided, not yet in the transcript.
func (r *runner) callIDTaken(id string) bool {
	if id == "" || r.reserved[id] || r.ctxReserved[id] || r.decided[id] {
		return true
	}
	for _, item := range r.transcript {
		if c, ok := item.(*openresponses.FunctionCall); ok && c.CallID == id {
			return true
		}
	}
	return false
}

// withCallID returns item, or a copy of it with the call ID decided
// for the function call at index when that is not the one it carries.
func (r *runner) withCallID(item openresponses.Item, index int) openresponses.Item {
	call, ok := item.(*openresponses.FunctionCall)
	if !ok {
		return item
	}
	id, ok := r.decidedCallID(call, index)
	if !ok || id == call.CallID {
		return item
	}
	renamed := *call
	renamed.CallID = id
	return &renamed
}

// maxCallIDPrefix is the longest prefix callIDPrefix keeps, so the ID
// with openresponses.NewID's '_' and 24 hex digits is at most 64
// characters, which OpenAI and Anthropic both take.
const maxCallIDPrefix = 64 - 1 - 24

// callIDPrefix is the model's call ID with every character outside
// letters, digits, '_' and '-' replaced by '_', the alphabet every
// provider takes for a call ID, cut to maxCallIDPrefix, or "call" when
// it is empty. The ID the loop gives a call is sent to the provider in
// the next request, which may not be the one that made it. Two IDs
// that give one prefix still give two IDs, since the suffix is random.
func callIDPrefix(id string) string {
	if id == "" {
		return "call"
	}
	prefix := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			return r
		}
		return '_'
	}, id)
	return prefix[:min(len(prefix), maxCallIDPrefix)]
}

// openedAt returns the position in opened of the item that last opened
// at index, adding one for an item the stream never opened.
func (r *runner) openedAt(index int) int {
	for i := len(r.opened) - 1; i >= 0; i-- {
		if r.opened[i].index == index && r.opened[i].done == nil {
			return i
		}
	}
	r.opened = append(r.opened, openedItem{index: index})
	return len(r.opened) - 1
}

// renameCalls returns the response the turn acts on: resp with each of
// its function calls replaced by the one the attempt completed for it,
// which carries the call ID decideCallID gave it and the arguments it
// completed with, so the calls the batch runs are the ones the
// transcript holds. The completed calls are in the order they opened,
// which is the order the response lists them in, and each call of the response is
// paired with one of them: by item ID when both carry one, else by the
// call ID the model gave it among the completed calls not yet paired,
// else by order. When a completed call is left unpaired, which a
// terminal response built one item per output index leaves out when a
// later call took the index, the response's calls give way, as a group,
// to the completed calls in the order they opened, so every call the
// transcript holds is run and answered, in the transcript's order. Renaming by output index alone, as before #210, named
// the last call opened at an index for every call the response listed
// there, so a stream that opened every call at index 0 with no item
// IDs ran one call twice, or each with the other's output. An item that
// is not a function call, and a function call the stream never
// completed, which the response alone lists, keep today's name: the
// call ID decided at their output index, when one was.
func (r *runner) renameCalls(resp *openresponses.Response) *openresponses.Response {
	var completed []*openedItem
	for i := range r.opened {
		if _, ok := r.opened[i].done.(*openresponses.FunctionCall); ok {
			completed = append(completed, &r.opened[i])
		}
	}
	paired := make([]int, len(resp.Output))
	for i := range paired {
		paired[i] = -1
	}
	taken := make([]bool, len(completed))
	pair := func(index int, match func(*openedItem) bool) {
		for j, c := range completed {
			if !taken[j] && match(c) {
				paired[index], taken[j] = j, true
				return
			}
		}
	}
	for index, item := range resp.Output {
		if call, ok := item.(*openresponses.FunctionCall); ok && call.ID != "" {
			pair(index, func(c *openedItem) bool { return c.itemID == call.ID })
		}
	}
	for index, item := range resp.Output {
		if call, ok := item.(*openresponses.FunctionCall); ok && paired[index] < 0 && call.CallID != "" {
			pair(index, func(c *openedItem) bool { return c.modelCallID == call.CallID })
		}
	}
	for index, item := range resp.Output {
		if _, ok := item.(*openresponses.FunctionCall); ok && paired[index] < 0 {
			pair(index, func(*openedItem) bool { return true })
		}
	}
	if slices.Contains(taken, false) {
		// A completed call the response does not list, because the
		// server built its terminal response one item per output index
		// and a later call took the index, is in the transcript all the
		// same and owed an output. Every call the response lists is
		// paired by now, so the listed calls, as a group, give way to
		// the completed calls in the order they opened, which is the
		// transcript's order, where the first of them stood; an item
		// that is not a call keeps its place.
		copied := *resp
		copied.Output = make(openresponses.Items, 0, len(resp.Output)+len(completed))
		placed := false
		for _, item := range resp.Output {
			if _, ok := item.(*openresponses.FunctionCall); !ok {
				copied.Output = append(copied.Output, item)
				continue
			}
			if !placed {
				for _, c := range completed {
					copied.Output = append(copied.Output, c.done)
				}
				placed = true
			}
		}
		if !placed {
			for _, c := range completed {
				copied.Output = append(copied.Output, c.done)
			}
		}
		return &copied
	}
	var out *openresponses.Response
	for index, item := range resp.Output {
		renamed := item
		if j := paired[index]; j >= 0 {
			renamed = completed[j].done
		} else {
			renamed = r.withCallID(item, index)
		}
		if renamed == item {
			continue
		}
		if out == nil {
			copied := *resp
			copied.Output = append(openresponses.Items(nil), resp.Output...)
			out = &copied
		}
		out.Output[index] = renamed
	}
	if out == nil {
		return resp
	}
	return out
}

// wireError carries an error event off the stream, so the attempt can
// tell a failure the server reported from one raised while an event
// was delivered, which commits the attempt.
type wireError struct{ err error }

func (e *wireError) Error() string { return e.err.Error() }
func (e *wireError) Unwrap() error { return e.err }

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
		if err := r.emit(&ItemEnd{RunID: r.runID, Turn: r.turn, Item: h.item, ResponseID: h.responseID, ModelCallID: h.modelCallID}); err != nil {
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
	// cut is set when an abort or a failure settled the call rather
	// than the tool, so no output is appended for it whatever its error
	// says.
	cut bool
	// ended is set once the call's tool_end has been delivered, so a
	// batch that ends early can give one to every call that lacks it.
	ended bool
	// terminate is the decision's hint, which rides on the result
	// whether the call is settled in preflight or runs.
	terminate bool
	// dispatchErr is the failure to deliver the call's tool_dispatch,
	// when a consumer refused it: the tool did not run, and the call
	// is cut rather than settled with the failure as its result.
	dispatchErr error
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
	// reason is the decision's reason for a blocked call.
	reason string
	// key is the idempotency key the tool receives, and dispatched is
	// set on the call's goroutine once its tool_dispatch was delivered,
	// read by the loop after the executor has drained.
	key        string
	dispatched bool
	// after is the call before this one in its chain, whose settling
	// this one's dispatch waits for, and done is closed once the loop
	// has settled this call's result, when a later call waits for it.
	after *callState
	done  chan struct{}
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
	if r.cfg.ToolElicitor != nil {
		ctx = agenttool.ContextWithElicitor(ctx, r.cfg.ToolElicitor)
	}
	if r.steered != nil {
		ctx = context.WithValue(ctx, steeredKey{}, r.steered())
	}
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
		batch[i] = p
		if err != nil {
			return nil, r.failBatch(ctx, batch, err)
		}
	}
	if ctx.Err() != nil {
		return nil, r.abortBatch(ctx, batch, context.Cause(ctx))
	}
	return batch, nil
}

// failBatch ends a batch a hook or a consumer failed inside, so the
// run ends with the failure and no tool_start is left without its
// tool_end: every call that has not ended gets one carrying the
// failure, the calls that finished before it have their outputs
// appended, and the rest are pending. The after-call hook does not
// run for the cut calls, since the run has already failed; a delivery
// that fails here is not reported over the failure that ended the run.
// It returns cause.
func (r *runner) failBatch(ctx context.Context, batch []*callState, cause error) error {
	for _, p := range batch {
		if p == nil || p.ended {
			continue
		}
		p.settled, p.cut = true, true
		p.err = cause
		p.result = agenttool.Result{}
		_ = r.finish(p)
	}
	_ = r.appendFinished(ctx, batch)
	return cause
}

// abortBatch ends a batch cut off before it executed: every call that
// had its tool_start and is not settled gets its tool_end with the
// context error, as a call cut off while running does, so the two are
// always paired, and the outputs of the calls preflight settled are
// appended. It returns the stop for the aborted run.
func (r *runner) abortBatch(ctx context.Context, batch []*callState, err error) error {
	var first error
	for _, p := range batch {
		if p == nil || p.ended {
			continue
		}
		p.settled, p.cut = true, true
		p.err = err
		if first != nil {
			// A hook or a consumer has already failed on this abort;
			// the rest are ended without the hook, so every call has
			// its tool_end and the failure is reported once.
			_ = r.finish(p)
			continue
		}
		if serr := r.settle(ctx, p); serr != nil {
			// The failure is reported once every call has its
			// tool_end, so a consumer that fails on one of them does
			// not leave the others without theirs; a hook that
			// failed before the call was ended leaves it to end here.
			first = serr
			if !p.ended {
				_ = r.finish(p)
			}
		}
	}
	if aerr := r.appendFinished(ctx, batch); aerr != nil && first == nil {
		first = aerr
	}
	if first != nil {
		return first
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
	return r.appendItems(outputs, InputTool)
}

// execute runs the calls preflight did not settle and settles each as
// it finishes, in completion order.
func (r *runner) execute(ctx context.Context, batch []*callState) error {
	var jobs []agenttool.Job
	var jobIndex []int
	byID := make(map[string]*callState, len(batch))
	for i, p := range batch {
		if p.settled {
			continue
		}
		jobs = append(jobs, agenttool.Job{Tool: p.tool, Call: agenttool.Call{ID: p.call.CallID, Args: p.args, IdempotencyKey: p.key}})
		jobIndex = append(jobIndex, i)
		byID[p.call.CallID] = p
	}
	exec := r.executor(func(job agenttool.Job) *callState { return byID[job.Call.ID] })
	// A call waits for the one before it in its chain, as the executor
	// orders them: the same Executor value that runs the batch below
	// says which that is, so the two cannot disagree (agenttool#66).
	for _, chain := range exec.Chains(jobs) {
		for k := 1; k < len(chain); k++ {
			prev := batch[jobIndex[chain[k-1]]]
			prev.done = make(chan struct{})
			batch[jobIndex[chain[k]]].after = prev
		}
	}
	// The batch has a context of its own, so a failure can stop the
	// tools, with the failure as the cause a tool reads, and the
	// executor still be drained: every result it holds is received,
	// as an abort receives them, before the batch is settled. Leaving
	// the range would cancel and wait too, but would lose the results
	// that arrive while it waits.
	bctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var failed error
	// hookFailed says the failure was the after-call hook's own, so a
	// result arriving during the drain has nowhere to be judged and is
	// cut; a consumer's failure leaves the hook standing, and such a
	// result goes through it as any other, since the hook is the point
	// past which nobody sees what a tool returned.
	hookFailed := false
	// step handles one event of the batch on the loop's goroutine.
	step := func(p *callState, ev agenttool.Event) {
		if failed != nil {
			if ev.Final && p.dispatchErr != nil {
				r.markUndispatched(p)
			}
			if !ev.Final || isCancellation(ev.Err, bctx) || hookFailed || p.dispatchErr != nil {
				// Cut, or nothing to end yet: failBatch ends what
				// has not ended once the drain is over.
				return
			}
			// A result of the call's own, before the cancellation
			// reached it: settled through the hook, and cut if the
			// hook refuses it too.
			p.result, p.err = ev.Result, ev.Err
			p.settled = true
			if serr := r.settle(ctx, p); serr != nil && !p.ended {
				p.cut, p.err, p.result = true, serr, agenttool.Result{}
				_ = r.finish(p)
			}
			return
		}
		var err error
		if ev.Final {
			if p.dispatchErr != nil {
				// The consumer refused the call's dispatch, so the tool
				// did not run; that is the run's failure, not the
				// call's result, and the batch is drained and cut as
				// for any delivery failure.
				r.markUndispatched(p)
				failed = p.dispatchErr
				cancel(failed)
				return
			}
			p.result, p.err = ev.Result, ev.Err
			p.settled = true
			err = r.settle(ctx, p)
			// settle ends the call before it delivers, so a call left
			// unended is one the hook refused.
			hookFailed = err != nil && !p.ended
		} else {
			err = r.emit(&ToolUpdate{RunID: r.runID, Turn: r.turn, CallID: p.call.CallID, Name: p.call.Name, Partial: ev.Result})
		}
		if err != nil {
			failed = err
			cancel(err)
		}
	}
	for ev := range exec.Execute(bctx, jobs) {
		p := batch[jobIndex[ev.Index]]
		step(p, ev)
		if ev.Final && p.done != nil {
			// Settled, or cut: the next call of its chain may be
			// dispatched.
			close(p.done)
		}
	}
	// The executor has drained, so every dispatch it made has been
	// delivered and its mark is safe to read here.
	for _, p := range batch {
		if p.dispatched {
			r.markDispatched(p)
		}
	}
	if failed != nil {
		// The tools have returned; the calls that did not finish are
		// ended with the failure, as an abort ends them.
		return r.failBatch(ctx, batch, failed)
	}
	if ctx.Err() != nil {
		if aerr := r.appendFinished(ctx, batch); aerr != nil {
			return aerr
		}
		return stop(ReasonAborted, context.Cause(ctx))
	}
	return nil
}

// markUndispatched remembers that p's tool_dispatch was refused, so its
// tool never ran.
func (r *runner) markUndispatched(p *callState) {
	if r.undispatched == nil {
		r.undispatched = map[string]bool{}
	}
	r.undispatched[p.call.CallID] = true
}

// handOff is how a call was handed to its tool.
type handOff struct {
	key  string
	args json.RawMessage
}

// markDispatched remembers that p was handed to its tool, with what
// key and what arguments.
func (r *runner) markDispatched(p *callState) {
	if r.dispatched == nil {
		r.dispatched = map[string]handOff{}
	}
	r.dispatched[p.call.CallID] = handOff{key: p.key, args: p.args}
}

// isCancellation reports whether err is the cancellation of ctx, or its
// cause: what a tool returns when it was cut off rather than a result
// of its own.
func isCancellation(err error, ctx context.Context) bool {
	if err == nil || ctx.Err() == nil {
		return false
	}
	return errors.Is(err, ctx.Err()) || errors.Is(err, context.Cause(ctx))
}

// executor builds the batch executor: the configuration's mode, bound
// and recorder, and a start callback that raises tool_dispatch for the
// call a job belongs to, found by find. The callback runs on the job's
// goroutine; emit serialises it with the run's events. A consumer that
// fails on the event stops the job, which the executor completes with
// that error, so the tool does not run after a dispatch nobody could
// record.
//
// The executor starts a chain's next job once the loop has received
// the last one's result, before the loop has settled it, so the
// callback first waits for that: AfterToolCall, and whatever a hook
// writes there, lands before the next call of the chain is dispatched.
// The loop's goroutine does the settling and waits on nothing of the
// job's, so the wait ends; a batch cut off meanwhile ends it with the
// cancellation, as for a job whose turn never came.
func (r *runner) executor(find func(agenttool.Job) *callState) agenttool.Executor {
	return agenttool.Executor{
		MaxParallel: r.cfg.MaxParallelTools,
		Sequential:  r.cfg.ToolExecution == ExecSequential,
		Recorder:    r.cfg.ToolRecorder,
		OnStart: func(ctx context.Context, job agenttool.Job) error {
			p := find(job)
			if p == nil {
				return nil
			}
			if p.after != nil {
				select {
				case <-p.after.done:
				case <-ctx.Done():
				}
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			err := r.emit(&ToolDispatch{RunID: r.runID, Turn: p.turn, CallID: p.call.CallID, Name: p.call.Name, Parent: p.parent, IdempotencyKey: p.key})
			// Remembered on the call itself: the executor hands the
			// failure back as the job's error, and the loop reads
			// the state, not the error, to tell a refused dispatch
			// from a tool's own failure. The final event crosses a
			// channel, so the loop goroutine reads it after this.
			p.dispatchErr = err
			p.dispatched = err == nil
			return err
		},
	}
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
	if err := r.appendItems(append(outputs, notes...), InputTool); err != nil {
		return nil, nil, err
	}
	return results, deferred, nil
}

// approvedBatch runs the calls a caller approved on Resume as one
// batch, before the first turn: preflight without BeforeToolCall for a
// call the caller's approval decides, and with it for one that never
// started, which nothing has decided, then execute and collect as for
// any batch. The tool events carry Turn 0, the turn of a resume's
// batch, which runs before the run's first model call. It returns the
// results and the calls the hook deferred.
func (r *runner) approvedBatch(ctx context.Context, approved []approval) ([]agenttool.Result, []*openresponses.FunctionCall, error) {
	tools := r.cfg.tools(ctx)
	ctx = r.toolContext(ctx, tools)
	calls := make([]*openresponses.FunctionCall, len(approved))
	for i, ap := range approved {
		calls[i] = ap.call
	}
	batch := make([]*callState, len(approved))
	for i, ap := range approved {
		p := r.prepare(tools, r.turn, ap.call, ap.args)
		r.resolved(p)
		if r.approved == nil {
			r.approved = map[string]bool{}
		}
		r.approved[ap.call.CallID] = true
		if ap.key != "" {
			p.key = ap.key
		}
		if ap.note != "" {
			p.note = openresponses.UserText(ap.note)
		}
		decision := &ToolDecision{Args: ap.args, Note: ap.note, By: ap.by, Reason: ap.reason}
		if ap.decide && r.cfg.BeforeToolCall != nil {
			d, err := r.beforeToolCall(ctx, ToolCallInfo{RunID: r.runID, Turn: r.turn, Call: ap.call, Tool: p.tool, Args: p.args, Batch: calls, Index: i})
			if err != nil {
				return nil, nil, r.failBatch(ctx, batch, fmt.Errorf("agentturn: before-tool-call hook: %w", err))
			}
			if d != nil {
				r.decide(p, d)
				decision = d
			}
		}
		batch[i] = p
		if err := r.emit(&ToolStart{RunID: r.runID, Turn: r.turn, CallID: p.call.CallID, Name: p.call.Name, Args: p.args, Decision: decision}); err != nil {
			return nil, nil, r.failBatch(ctx, batch, err)
		}
		if p.deferred {
			p.settled, p.ended = true, true
			if err := r.emit(&ToolEnd{RunID: r.runID, Turn: r.turn, CallID: p.call.CallID, Name: p.call.Name, Deferred: true, Reason: decision.Reason}); err != nil {
				return nil, nil, r.failBatch(ctx, batch, err)
			}
			continue
		}
		if err := r.check(ctx, p); err != nil {
			return nil, nil, r.failBatch(ctx, batch, err)
		}
	}
	if ctx.Err() != nil {
		return nil, nil, r.abortBatch(ctx, batch, context.Cause(ctx))
	}
	if err := r.execute(ctx, batch); err != nil {
		return nil, nil, err
	}
	return r.collect(batch)
}

// preflight resolves the tool, checks the arguments and runs
// BeforeToolCall. It emits tool_start and, for a call that will not
// execute, tool_end. The state is returned beside an error once
// tool_start has been raised, so the batch can give the call its
// tool_end; before that there is nothing to end and the state is nil.
func (r *runner) preflight(ctx context.Context, tools agenttool.Set, call *openresponses.FunctionCall, batch []*openresponses.FunctionCall, index int) (*callState, error) {
	p := r.prepare(tools, r.turn, call, nil)
	r.resolved(p)
	var decision *ToolDecision
	if r.cfg.BeforeToolCall != nil {
		var err error
		decision, err = r.beforeToolCall(ctx, ToolCallInfo{RunID: r.runID, Turn: r.turn, Call: call, Tool: p.tool, Args: p.args, Batch: batch, Index: index})
		if err != nil {
			return nil, fmt.Errorf("agentturn: before-tool-call hook: %w", err)
		}
		if decision != nil {
			r.decide(p, decision)
		}
	}
	if err := r.emit(&ToolStart{RunID: r.runID, Turn: r.turn, CallID: call.CallID, Name: call.Name, Args: p.args, Decision: decision}); err != nil {
		return p, err
	}
	if p.deferred {
		p.settled, p.ended = true, true
		return p, r.emit(&ToolEnd{RunID: r.runID, Turn: r.turn, CallID: call.CallID, Name: call.Name, Deferred: true, Reason: decision.Reason})
	}
	return p, r.check(ctx, p)
}

// decide applies BeforeToolCall's decision to a call: its arguments,
// its note for an allowed call, its terminate hint, and a Block or a
// Defer.
func (r *runner) decide(p *callState, decision *ToolDecision) {
	if decision.Args != nil {
		p.args = decision.Args
	}
	if decision.Note != "" && decision.Action == Allow {
		p.note = openresponses.DeveloperText(decision.Note)
	}
	p.terminate = decision.Terminate
	switch decision.Action {
	case Block:
		reason := decision.Reason
		if reason == "" {
			reason = "call blocked"
		}
		p.blocked = true
		p.err = errors.New(reason)
		p.reason = reason
	case Defer:
		p.deferred = true
		if r.deferred == nil {
			r.deferred = map[string]heldCall{}
		}
		held := *decision
		r.deferred[p.call.CallID] = heldCall{args: p.args, decision: &held}
	}
}

// heldCall is a call a hook deferred in this run: the arguments it was
// held with and a copy of the decision that held it.
type heldCall struct {
	args     json.RawMessage
	decision *ToolDecision
}

// prepare starts the state of a call: the tool with its name, if any,
// its arguments, args when given and the call's own otherwise, an
// empty object standing in for none, and a new idempotency key: the
// run's ID and the call's. A run ID is random, so no other run's key
// shares it, and within the run a call ID names one call.
func (r *runner) prepare(tools agenttool.Set, turn int, call *openresponses.FunctionCall, args json.RawMessage) *callState {
	p := &callState{call: call, args: args, turn: turn, key: r.runID + "/" + call.CallID}
	if p.args == nil {
		p.args = json.RawMessage(call.Arguments)
	}
	if len(p.args) == 0 {
		p.args = json.RawMessage("{}")
	}
	p.tool, _ = tools.Lookup(call.Name)
	return p
}

// resolved remembers the tool a call of the model's resolved to, for
// the pending list. It runs on the loop's goroutine; a nested call,
// prepared on its tool's, is never pending.
func (r *runner) resolved(p *callState) {
	if p.tool == nil {
		return
	}
	if r.callTools == nil {
		r.callTools = map[string]agenttool.Tool{}
	}
	r.callTools[p.call.CallID] = p.tool
}

// check settles a call that will not execute: one a hook blocked, one
// no tool has the name of, or one whose arguments are not an object.
// It returns nil, with the call unsettled, when the call may run.
func (r *runner) check(ctx context.Context, p *callState) error {
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
	return r.finish(p)
}

// finish ends a call whose result and error are decided: the error
// becomes the output the model sees, the decision's hint is composed
// in, and tool_end is raised. Every call that ran or was cut ends
// here, so none is ended twice, and it runs no hook: settle runs the
// after-call hook and then this; a cut call comes here directly. A
// deferred call is the one exception, ended in preflight with an
// empty result, since nothing ran and nothing was decided about a
// result.
func (r *runner) finish(p *callState) error {
	if p.err != nil {
		terminate := p.result.Terminate
		details := p.result.Details
		p.result = agenttool.ErrorResult(p.err)
		p.result.Terminate = terminate
		p.result.Details = details
	}
	// The decision's hint composes with the tool's own, whether the
	// call ran or was settled in preflight, and an override does not
	// clear it: the policy asked the loop to stop after this batch.
	p.result.Terminate = p.result.Terminate || p.terminate
	// The event is raised whether or not every consumer takes it: a
	// delivery that fails is the consumer's failure, and the call is
	// not ended a second time for it.
	p.ended = true
	return r.emit(&ToolEnd{RunID: r.runID, Turn: p.turn, CallID: p.call.CallID, Name: p.call.Name, Result: p.result, Err: p.err, Blocked: p.blocked, Reason: p.reason, Parent: p.parent})
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
// A nested call cannot be handed to the caller, since it belongs to a
// tool that is running, so one the hook defers is put to the user
// through the agenttool.Elicitor on ctx, when there is one, with [Ask]:
// the question names the call, and the elicitor's context carries it
// as an [AskedCall], its ID, name, whole arguments and the deferral,
// for [AskedCallFrom]. An accept runs it and a decline refuses it, and
// tool_start carries that answer as the decision, by "human". Without
// an elicitor, or on a cancel or a failure to ask, the deferral
// refuses the call. Nothing is appended
// to the transcript, so a nested call costs no items and a Terminate
// on its result means nothing to the loop.
//
// The call it is made under comes from agenttool.CallFrom, which
// agenttool.New puts on every typed tool's context; a tool that
// implements the interface itself and wants the parent named passes
// agenttool.WithCall. Outside a loop, Invoke returns [ErrNoInvoker].
//
// A nested call gets an idempotency key of its own, fresh on every
// Invoke: nothing derives it from the key of the call that made it, so
// a parent run again after a restart invokes with new keys, and a
// keyed tool reached this way deduplicates nothing across the parent's
// attempts; see the open question in RFC 0001.
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
		decision, err = r.beforeToolCall(ctx, ToolCallInfo{RunID: r.runID, Turn: turn, Call: call, Tool: p.tool, Args: p.args, Batch: []*openresponses.FunctionCall{call}, Index: 0, Parent: parent})
		if err != nil {
			return agenttool.Result{}, fmt.Errorf("agentturn: before-tool-call hook: %w", err)
		}
		if decision != nil {
			if decision.Args != nil {
				p.args = decision.Args
			}
			if decision.Action == Defer {
				// A nested call cannot be handed to the caller, so the
				// user is asked through the invoking tool's elicitor;
				// without one, or with no answer, the deferral stands
				// and refuses the call below.
				if d, ok := Ask(ctx, AskedCall{Parent: parent, CallID: call.CallID, Name: name, Args: p.args, Decision: decision}); ok {
					decision = d
				}
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
				p.reason = reason
			}
		}
	}
	if err := r.emit(&ToolStart{RunID: r.runID, Turn: turn, CallID: call.CallID, Name: name, Args: p.args, Decision: decision, Parent: parent}); err != nil {
		return agenttool.Result{}, r.endNested(p, err)
	}
	if err := r.check(ctx, p); err != nil {
		return agenttool.Result{}, r.endNested(p, err)
	}
	if p.settled {
		return p.result, p.err
	}
	job := agenttool.Job{Tool: p.tool, Call: agenttool.Call{ID: call.CallID, Args: p.args, IdempotencyKey: p.key}}
	results, errs := r.executor(func(agenttool.Job) *callState { return p }).Results(ctx, []agenttool.Job{job})
	if p.dispatchErr != nil {
		// The consumer refused the nested call's dispatch, so its tool
		// did not run; the tool that made it decides, as for any
		// failure of the loop's own around a nested call.
		return agenttool.Result{}, r.endNested(p, p.dispatchErr)
	}
	p.result, p.err = results[0], errs[0]
	p.settled = true
	if err := r.settle(ctx, p); err != nil {
		return agenttool.Result{}, r.endNested(p, err)
	}
	return p.result, p.err
}

// endNested gives a nested call whose loop-side handling failed, a
// hook or a consumer, the tool_end its tool_start promised, carrying
// the failure, and returns the failure to the invoking tool. The run
// goes on: the failure is the tool's to answer.
func (r *runner) endNested(p *callState, cause error) error {
	if !p.ended {
		p.settled, p.cut = true, true
		p.err = cause
		p.result = agenttool.Result{}
		_ = r.finish(p)
	}
	return cause
}
