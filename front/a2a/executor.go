package a2a

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/front/responses"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
	"github.com/a2aproject/a2a-go/a2asrv/eventqueue"
)

// Executor serves an agentturn.Config as an A2A agent. Create one with
// [New] and hand it to a2asrv.NewHandler.
type Executor struct {
	cfg         agentturn.Config
	store       ConversationStore
	chunkSize   int
	callerTools []*openresponses.FunctionTool
	recorderFor RecorderFor
	handoff     func(context.Context, *agentturn.RunEnd, []*agentturn.ToolEnd) (agentturn.Config, bool)
	start       func(context.Context, agentturn.Transcript) (agentturn.Config, bool)
	route       Route

	mu      sync.Mutex
	cancels map[a2a.TaskID]context.CancelFunc
	// busy holds the context IDs with a task in flight, so one
	// conversation runs one task at a time.
	busy map[string]bool
}

// ErrConversationBusy is the cause of the error a message gets when its
// context ID already has a task in flight. It is wrapped with
// a2a.ErrInvalidRequest, which is what a caller across the wire sees.
var ErrConversationBusy = errors.New("conversation has a task in flight")

// RefusedText is the status message of a task a guard refused. It is
// fixed, so a guard's reason never reaches the caller.
const RefusedText = "the agent's guard refused this request"

var _ a2asrv.AgentExecutor = (*Executor)(nil)

// Option configures an [Executor].
type Option func(*Executor)

// WithConversationStore replaces the in-memory conversation store.
func WithConversationStore(s ConversationStore) Option {
	return func(e *Executor) { e.store = s }
}

// WithChunkSize sets how many bytes of assistant text are coalesced into
// one artifact chunk. The default is [DefaultChunkSize].
func WithChunkSize(n int) Option {
	return func(e *Executor) { e.chunkSize = n }
}

// WithCallerTools declares function tools every caller executes itself,
// in addition to any a message declares under [MetaCallerTools].
func WithCallerTools(tools ...*openresponses.FunctionTool) Option {
	return func(e *Executor) { e.callerTools = append(e.callerTools, tools...) }
}

// RecorderFor attaches what records one conversation's runs to the
// agent that drives a task in it, before the task's run starts. The
// executor calls it for every task with the task's context ID and a
// fresh agent seeded with the stored transcript and configured with
// the run's configuration, the caller-owned tools and the hook that
// defers them included. What it subscribes to that agent is called
// with every event of the run, synchronously and before the executor
// relays the event, so a recorder writes a call's dispatch before the
// tool runs, and the loop goes at the pace of the recorder's durable
// writes and the relay together. A subscriber error ends the run with
// ReasonError, which fails the task, or with ReasonAborted when the
// run was being aborted, which cancels it. The function may replace
// the agent's configuration with SetConfig, to route
// Config.ToolRecorder to the conversation's record, and it returns the
// context the run is prompted with, derived from ctx, which carries
// whatever the tools and child runs read to attribute their writes,
// and a detach function the executor calls when the task's run is
// over; detach may be nil. An error, or a nil context, makes the send
// fail before any task exists: the caller gets the error and nothing
// was run or stored.
//
// The transcript comes from the [ConversationStore], not from the
// record. After an aborted or failed run the store drops the calls
// that will never be answered, so the next message is a valid input,
// while the session keeps them as cut off; seed from the store, as
// the executor does, and treat the session as the record of what
// happened rather than as the conversation's source.
//
// A host records each conversation as its own session with the
// session module, which this one does not import:
//
//	a2a.WithRecorderFor(func(ctx context.Context, contextID string, a *agentturn.Agent) (context.Context, func(), error) {
//		rec, err := openRecorder(ctx, store, contextID) // session.Start or session.Resume
//		if err != nil {
//			return nil, nil, err
//		}
//		cfg := a.Config()
//		cfg.ToolRecorder = rec.RecordFunc()
//		if err := a.SetConfig(cfg); err != nil {
//			return nil, nil, err
//		}
//		return session.ContextWithSessionID(ctx, rec.SessionID()), rec.Attach(a), nil
//	})
type RecorderFor func(ctx context.Context, contextID string, a *agentturn.Agent) (context.Context, func(), error)

// WithRecorderFor records every conversation the executor serves
// through fn.
func WithRecorderFor(fn RecorderFor) Option {
	return func(e *Executor) { e.recorderFor = fn }
}

// WithHandoff is asked when a task's run stops on StopTerminate or
// StopPartialTerminate. A configuration it returns continues the same
// transcript within the same task: the executor sets it on the task's
// agent and continues, so what [WithRecorderFor] subscribed records
// the switch and the receiver's run, and the receiver's text streams
// into the task as the sender's did. The caller-owned tools are
// offered under it as under the executor's own configuration, and a
// ToolRecorder or ToolElicitor the configuration leaves nil is kept
// from the agent's, where a recorder put its own. false ends the task
// as it would without the option. fn is given the run's end and the
// tool_end events of the batch that stopped it, in completion order,
// calls a tool made with agentturn.Invoke left out: the destination is
// in a result's Details, which the model never sees, or in end.Items,
// the terminating call and its output. It is asked again each time a
// run it started stops the same way, so a pair of agents that hand
// back and forth runs until fn declines or the task is canceled; a
// host that wants a bound counts on a value it puts on the context
// RecorderFor returns, which fn is given.
//
// The receiver's run is continued under
// agentturn.ContextWithTrigger(ctx, agentturn.Trigger{Kind: "handoff",
// Ref: sender.Name}), the sender being the configuration whose run
// stopped, so the receiver's BeforeTurn context and its run_start, and
// so the record, say why it ran, as they do for a host that continues
// the receiver in process.
//
// Without the option, or when it declines, a terminating stop with no
// answer completes the task with the text of the last output of a
// call whose result set Terminate, as tools/agent reports the same
// stop.
//
// The handoff lasts for the task. The conversation's next task is
// started by [WithStart], or under the executor's own configuration
// without it.
func WithHandoff(fn func(ctx context.Context, end *agentturn.RunEnd, results []*agentturn.ToolEnd) (agentturn.Config, bool)) Option {
	return func(e *Executor) { e.handoff = fn }
}

// WithStart picks the configuration a task starts under from the
// conversation: the transcript the [ConversationStore] holds for the
// task's context ID with the message's items after it. A configuration
// it returns is used in place of the executor's, with the caller-owned
// tools offered under it as under the executor's own; false keeps the
// executor's. It is how the receiver of a handoff answers the
// conversation's later messages. The executor keeps no configuration
// between tasks, so the conversation, which holds the transfer call
// and its output, says who has it. The stored transcript is the
// executor's own, and a message may not add a function_call to it, so
// every transfer call fn sees is one the agent made. A host whose
// handoff tools are named for their destination takes the last one
// that ran, with [HandedTo]: a call is a handoff only when its output
// is the transfer tool's own text, so one a guard withheld, a hook
// blocked or the tool failed on is not. [WithTransfers] starts there
// too, and leaves out of the receiver's requests the reasoning items
// another model produced.
//
//	route := func(call *openresponses.FunctionCall) (agentturn.Config, string, bool) {
//		name := strings.TrimPrefix(call.Name, "transfer_to_")
//		cfg, ok := agents[name]
//		return cfg, transferText(name), ok
//	}
//	a2a.WithStart(func(_ context.Context, t agentturn.Transcript) (agentturn.Config, bool) {
//		return a2a.HandedTo(t, route)
//	})
func WithStart(fn func(ctx context.Context, t agentturn.Transcript) (agentturn.Config, bool)) Option {
	return func(e *Executor) { e.start = fn }
}

// WithTransfers says the conversation's handoffs are the transfer
// calls route names, as [Handoffs] finds them. A task starts where the
// last of them left the conversation, as [WithStart] does with
// [HandedTo]; a WithStart given after it picks the start in its place.
// And the reasoning items of the stored transcript are attributed, with
// responses.Attribute, to the agents that had the conversation when
// they were produced, those before the first transfer to the executor's
// own configuration, so a request leaves out another model's
// reasoning, whose signature its provider refuses. Without it the
// stored reasoning items are of unknown origin and sent to whichever
// model runs; the ones produced within a task, the sender's before a
// [WithHandoff] switch included, are attributed either way.
func WithTransfers(route Route) Option {
	return func(e *Executor) {
		e.route = route
		e.start = func(_ context.Context, t agentturn.Transcript) (agentturn.Config, bool) {
			return HandedTo(t, route)
		}
	}
}

// New builds an executor for cfg.
func New(cfg agentturn.Config, opts ...Option) *Executor {
	e := &Executor{cfg: cfg, cancels: map[a2a.TaskID]context.CancelFunc{}, busy: map[string]bool{}}
	for _, opt := range opts {
		opt(e)
	}
	if e.store == nil {
		e.store = &MemoryStore{}
	}
	return e
}

// Execute runs the loop for one message and translates the run into A2A
// events. It returns nil after a terminal or input-required status has
// been written; a returned error, including a failure to write an
// event, makes the SDK fail the task.
func (e *Executor) Execute(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue) error {
	if reqCtx.Message == nil {
		return fmt.Errorf("%w: message is required", a2a.ErrInvalidParams)
	}
	prompts, err := ItemsFromMessage(reqCtx.Message)
	if err != nil {
		return fmt.Errorf("%w: %w", a2a.ErrInvalidParams, err)
	}
	if len(prompts) == 0 {
		return fmt.Errorf("%w: message has no usable parts", a2a.ErrInvalidParams)
	}
	for i, item := range prompts {
		// A call is the agent's to make. One a caller wrote would sit in
		// the stored conversation as the agent's, a transfer WithStart
		// reads as a handoff among them; only an output answering a
		// pending call has a use in a message.
		if _, ok := item.(*openresponses.FunctionCall); ok {
			return fmt.Errorf("%w: item %d is a function_call; a message may carry function_call_output items answering pending calls, never a call", a2a.ErrInvalidParams, i)
		}
	}
	declared, err := callerTools(reqCtx.Message)
	if err != nil {
		return fmt.Errorf("%w: %w", a2a.ErrInvalidParams, err)
	}
	// The conversation is loaded, run and saved by one task at a time:
	// a second task on the same context ID would run on a stale
	// transcript, one save would drop the other's turn, and two
	// recorders would write one record at once. The second is refused
	// rather than made to wait, so a caller is not left holding a task
	// it cannot see, and a served agent that sends to its own
	// conversation fails instead of waiting on itself.
	release, err := e.claim(reqCtx.ContextID)
	if err != nil {
		return err
	}
	defer release()
	transcript, err := e.store.Load(ctx, reqCtx.ContextID)
	if err != nil {
		return fmt.Errorf("load conversation %q: %w", reqCtx.ContextID, err)
	}
	if err := checkAnswers(transcript, prompts); err != nil {
		// A follow-up that does not answer the pending calls leaves the
		// task where it was, input-required, with the calls repeated
		// and the rule stated; failing the task would make it
		// unrecoverable.
		return e.inputRequired(ctx, reqCtx, q, unanswered(transcript), err.Error())
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer e.track(reqCtx.TaskID, cancel)()
	caller := slices.Concat(e.callerTools, declared)
	start := e.cfg
	if e.start != nil {
		if cfg, ok := e.start(runCtx, slices.Concat(transcript, prompts)); ok {
			start = cfg
		}
	}
	var models agentturn.ReasoningModels
	if e.route != nil {
		models = responses.Attribute(transcript, e.cfg, Handoffs(transcript, e.route))
	}
	agent := agentturn.New(e.runConfig(start, caller), agentturn.WithTranscript(transcript), agentturn.WithReasoningModels(models))
	if e.recorderFor != nil {
		rctx, detach, err := e.recorderFor(runCtx, reqCtx.ContextID, agent)
		if detach != nil {
			defer detach()
		}
		if err == nil && rctx == nil {
			err = errors.New("RecorderFor returned a nil context")
		}
		if err != nil {
			return fmt.Errorf("record conversation %q: %w", reqCtx.ContextID, err)
		}
		runCtx = rctx
	}
	if err := q.Write(ctx, a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateWorking, nil)); err != nil {
		return err
	}
	out := e.relay(ctx, runCtx, cancel, reqCtx, q, agent, prompts, caller)
	if err := e.persist(ctx, reqCtx.ContextID, transcript, out); err != nil {
		return err
	}
	if out.writeErr != nil {
		// The run was cut short because an event could not be written;
		// the AgentExecutor contract is to return that error, not to
		// report the task as canceled.
		return out.writeErr
	}
	return e.conclude(ctx, reqCtx, q, out)
}

// track registers cancel for the task and returns the function that
// removes it, so a Cancel arriving during the run finds it.
func (e *Executor) track(id a2a.TaskID, cancel context.CancelFunc) func() {
	e.mu.Lock()
	if e.cancels == nil {
		e.cancels = map[a2a.TaskID]context.CancelFunc{}
	}
	e.cancels[id] = cancel
	e.mu.Unlock()
	return func() {
		e.mu.Lock()
		delete(e.cancels, id)
		e.mu.Unlock()
		cancel()
	}
}

// claim marks the conversation busy and returns the function that
// frees it, or the error a message on a busy conversation gets.
func (e *Executor) claim(contextID string) (func(), error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.busy == nil {
		e.busy = map[string]bool{}
	}
	if e.busy[contextID] {
		return nil, a2a.NewError(fmt.Errorf("%w: %w", a2a.ErrInvalidRequest, ErrConversationBusy),
			fmt.Sprintf("context %q has a task in flight; send again once it has ended", contextID))
	}
	e.busy[contextID] = true
	return func() {
		e.mu.Lock()
		delete(e.busy, contextID)
		e.mu.Unlock()
	}, nil
}

// outcome is what relay reports about a run.
type outcome struct {
	// end is the last run's end; items holds what every run appended,
	// the sender's and each receiver's after a handoff.
	end   *agentturn.RunEnd
	items openresponses.Items
	// results are the tool_end events of the last run's last batch.
	results  []*agentturn.ToolEnd
	lastText string
	// writeErr is the first failure to write an event to the queue; it
	// aborted the run.
	writeErr error
}

// relay prompts the agent and streams assistant text into artifacts
// from a subscriber, after whatever RecorderFor subscribed, and
// continues it under each configuration WithHandoff returns. Under a
// configuration with an OutputGuard a message's artifact is written
// whole from its item_end, the guard's replacement, so text the guard
// withheld never enters the task's artifacts. A failed write aborts
// the run through cancel and is reported on the outcome; the run's
// own end and error are reported alongside.
func (e *Executor) relay(ctx, runCtx context.Context, cancel context.CancelFunc, reqCtx *a2asrv.RequestContext, q eventqueue.Queue, agent *agentturn.Agent, prompts openresponses.Items, caller []*openresponses.FunctionTool) outcome {
	var out outcome
	var writer *artifactWriter
	// guarded says the configuration running has an OutputGuard, whose
	// item_end may carry other text than its deltas did.
	guarded := agent.Config().OutputGuard != nil
	fail := func(err error) {
		if out.writeErr == nil {
			out.writeErr = err
		}
		cancel()
	}
	unsubscribe := agent.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		switch ev := ev.(type) {
		case *agentturn.RunStart, *agentturn.TurnStart:
			out.results = nil
		case *agentturn.ToolEnd:
			if ev.Parent == "" {
				out.results = append(out.results, ev)
			}
		case *agentturn.ItemStart:
			if m, ok := ev.Item.(*openresponses.Message); ok && m.Role == openresponses.RoleAssistant {
				writer = newArtifactWriter(q, reqCtx, e.chunkSize)
			}
		case *agentturn.ItemUpdate:
			if writer == nil || guarded {
				return nil
			}
			var delta string
			switch s := ev.Stream.(type) {
			case *openresponses.OutputTextDeltaEvent:
				delta = s.Delta
			case *openresponses.RefusalDeltaEvent:
				delta = s.Delta
			}
			if delta != "" {
				if err := writer.write(ctx, delta); err != nil {
					fail(err)
				}
			}
		case *agentturn.ItemEnd:
			if m, ok := ev.Item.(*openresponses.Message); ok && m.Role == openresponses.RoleAssistant && writer != nil {
				out.lastText = m.Text()
				if guarded {
					if err := writer.write(ctx, messageText(m)); err != nil {
						fail(err)
					}
				}
				if err := writer.close(ctx); err != nil {
					fail(err)
				}
				writer = nil
			}
		}
		return nil
	})
	defer unsubscribe()
	end, err := agent.Prompt(runCtx, prompts...)
	for {
		if end == nil {
			// The run never started: a misuse the loop refused before
			// any event, which fails the task as a run would.
			end = &agentturn.RunEnd{Reason: agentturn.ReasonError, Err: err}
		}
		out.end = end
		out.items = append(out.items, end.Items...)
		if out.writeErr != nil || e.handoff == nil || !terminated(end) {
			return out
		}
		next, ok := e.handoff(runCtx, end, out.results)
		if !ok {
			return out
		}
		prev := agent.Config()
		cfg := e.runConfig(next, caller)
		if cfg.ToolRecorder == nil {
			cfg.ToolRecorder = prev.ToolRecorder
		}
		if cfg.ToolElicitor == nil {
			cfg.ToolElicitor = prev.ToolElicitor
		}
		if err = agent.SetConfig(cfg); err == nil {
			guarded = cfg.OutputGuard != nil
			end, err = agent.Continue(agentturn.ContextWithTrigger(runCtx, agentturn.Trigger{Kind: "handoff", Ref: prev.Name}))
		} else {
			end = nil
		}
	}
}

// messageText returns the text and refusal parts of m in order, what
// its deltas carry when they carry the message whole.
func messageText(m *openresponses.Message) string {
	var b strings.Builder
	for _, part := range m.Content {
		switch p := part.(type) {
		case *openresponses.OutputText:
			b.WriteString(p.Text)
		case *openresponses.Refusal:
			b.WriteString(p.Refusal)
		}
	}
	return b.String()
}

// terminated reports whether a terminating tool result stopped the run.
func terminated(end *agentturn.RunEnd) bool {
	return end.Reason == agentturn.ReasonStopped && (end.Cause == agentturn.StopTerminate || end.Cause == agentturn.StopPartialTerminate)
}

// terminatingText returns the text of the last output in items of a
// call whose result set Terminate. A sibling that did not, and a call
// blocked or failed, whose output is the reason or the error, is not
// the tools' answer.
func terminatingText(items openresponses.Items, results []*agentturn.ToolEnd) (string, bool) {
	answered := map[string]bool{}
	for _, res := range results {
		if res.Result.Terminate && !res.Blocked && res.Err == nil {
			answered[res.CallID] = true
		}
	}
	for i := len(items) - 1; i >= 0; i-- {
		if out, ok := items[i].(*openresponses.FunctionCallOutput); ok && answered[out.CallID] && out.Output.Text != "" {
			return out.Output.Text, true
		}
	}
	return "", false
}

// persist stores the conversation after a run: everything the run
// appended. A deferred call stays unanswered on purpose, since the
// caller answers it on the next message; after an abort or a failure
// the calls that will never be answered are dropped so the next message
// is a valid input.
func (e *Executor) persist(ctx context.Context, contextID string, transcript agentturn.Transcript, out outcome) error {
	next := append(transcript, out.items...)
	switch out.end.Reason {
	case agentturn.ReasonDone, agentturn.ReasonStopped, agentturn.ReasonInputRequired:
	default:
		next = stripUnanswered(next)
	}
	if err := e.store.Save(ctx, contextID, next); err != nil {
		return fmt.Errorf("save conversation %q: %w", contextID, err)
	}
	return nil
}

// conclude writes the task's final status from the run's reason.
func (e *Executor) conclude(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue, out outcome) error {
	switch out.end.Reason {
	case agentturn.ReasonInputRequired:
		return e.inputRequired(ctx, reqCtx, q, agentturn.PendingCalls(out.end.Pending), "")
	case agentturn.ReasonDone, agentturn.ReasonStopped:
		if _, ok := out.end.Answer(); out.end.Cause == agentturn.StopGuard && !ok {
			// A guard stopped the run before the agent answered, at
			// whichever hook: the agent refused the task, which neither
			// completed, empty or with a preamble, nor failed, which a
			// caller would retry or re-plan. The guard's error stays
			// with the host: its text may carry the rule a caller could
			// phrase around.
			return e.finish(ctx, reqCtx, q, a2a.TaskStateRejected, a2a.NewMessageForTask(a2a.MessageRoleAgent, reqCtx, a2a.TextPart{Text: RefusedText}))
		}
		text := out.lastText
		if _, ok := out.end.Answer(); terminated(out.end) && !ok {
			// A terminating result nobody handed off from answered on
			// the model's behalf; text before its call was a preamble.
			if t, ok := terminatingText(out.end.Items, out.results); ok {
				text = t
			}
		}
		var msg *a2a.Message
		if text != "" {
			msg = a2a.NewMessageForTask(a2a.MessageRoleAgent, reqCtx, a2a.TextPart{Text: text})
		}
		return e.finish(ctx, reqCtx, q, a2a.TaskStateCompleted, msg)
	case agentturn.ReasonAborted:
		// The canceler usually wrote the canceled status already and the
		// SDK then cancels this context; a failed write here is expected.
		_ = e.finish(context.WithoutCancel(ctx), reqCtx, q, a2a.TaskStateCanceled, nil)
		return nil
	default:
		return e.finish(ctx, reqCtx, q, a2a.TaskStateFailed, errorMessage(reqCtx, out.end.Err))
	}
}

// runConfig returns the config for one run: cfg's tools plus the
// caller-owned ones, offered to the model but never executed here, and
// a BeforeToolCall that defers every call to a caller-owned tool. The
// loop then ends the run with ReasonInputRequired and the pending calls
// on the RunEnd, which is the input-required boundary of the task. The
// agent's own hook runs first and its block or rewrite is respected.
func (e *Executor) runConfig(cfg agentturn.Config, caller []*openresponses.FunctionTool) agentturn.Config {
	if len(caller) == 0 {
		return cfg
	}
	owned := make(map[string]bool, len(caller))
	stubs := make([]agenttool.Tool, 0, len(caller))
	for _, ft := range caller {
		owned[ft.Name] = true
		var opts []agenttool.Option
		if ft.Strict != nil && *ft.Strict {
			opts = append(opts, agenttool.WithStrict())
		}
		// The stub advertises the caller's tool; the hook below defers
		// every call to it, so its function runs only if that hook was
		// bypassed, which is a bug worth an error output.
		name := ft.Name
		stubs = append(stubs, agenttool.NewFunc(ft.Name, ft.Description, ft.Parameters, func(context.Context, agenttool.Call) (agenttool.Result, error) {
			return agenttool.Result{}, fmt.Errorf("tool %q is owned by the caller and cannot run here", name)
		}, opts...))
	}
	local, provider := cfg.Tools, cfg.ToolProvider
	cfg.Tools = nil
	cfg.ToolProvider = func(ctx context.Context) []agenttool.Tool {
		base := local
		if provider != nil {
			base = provider(ctx)
		}
		return slices.Concat(base, stubs)
	}
	before := cfg.BeforeToolCall
	cfg.BeforeToolCall = func(ctx context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
		var decision *agentturn.ToolDecision
		if before != nil {
			var err error
			decision, err = before(ctx, info)
			if err != nil {
				return nil, err
			}
		}
		if !owned[info.Call.Name] || (decision != nil && decision.Action == agentturn.Block) {
			return decision, nil
		}
		if decision == nil {
			decision = &agentturn.ToolDecision{}
		}
		decision.Action = agentturn.Defer
		return decision, nil
	}
	return cfg
}

// answeredCalls returns the IDs of the calls in items that have an
// output.
func answeredCalls(items openresponses.Items) map[string]bool {
	answered := map[string]bool{}
	for _, item := range items {
		if fco, ok := item.(*openresponses.FunctionCallOutput); ok {
			answered[fco.CallID] = true
		}
	}
	return answered
}

// unanswered returns the function calls in t that have no output, in
// order.
func unanswered(t agentturn.Transcript) []*openresponses.FunctionCall {
	answered := answeredCalls(t)
	var out []*openresponses.FunctionCall
	for _, item := range t {
		if fc, ok := item.(*openresponses.FunctionCall); ok && !answered[fc.CallID] {
			out = append(out, fc)
		}
	}
	return out
}

// checkAnswers enforces the resume contract when the stored conversation
// waits on caller-owned calls: the message must carry exactly one
// function_call_output for each pending call and nothing else, the same
// rule agentturn.Agent.Resume applies.
func checkAnswers(t agentturn.Transcript, prompts openresponses.Items) error {
	pending := unanswered(t)
	if len(pending) == 0 {
		return nil
	}
	want := make(map[string]bool, len(pending))
	for _, fc := range pending {
		want[fc.CallID] = true
	}
	for _, item := range prompts {
		fco, ok := item.(*openresponses.FunctionCallOutput)
		if !ok {
			return fmt.Errorf("task is waiting on %d tool call(s); the message may only carry their function_call_output items", len(pending))
		}
		if !want[fco.CallID] {
			return fmt.Errorf("function_call_output %q does not answer a pending call", fco.CallID)
		}
		delete(want, fco.CallID)
	}
	if len(want) > 0 {
		return fmt.Errorf("%d pending call(s) unanswered", len(want))
	}
	return nil
}

// stripUnanswered drops every function_call item that has no output,
// so the stored conversation stays a valid input after an aborted or
// failed run.
func stripUnanswered(items openresponses.Items) openresponses.Items {
	answered := answeredCalls(items)
	out := make(openresponses.Items, 0, len(items))
	for _, item := range items {
		if fc, ok := item.(*openresponses.FunctionCall); ok && !answered[fc.CallID] {
			continue
		}
		out = append(out, item)
	}
	return out
}

// inputRequired ends the execution with the pending calls on the status
// message, preceded by note as a text part when it is not empty.
func (e *Executor) inputRequired(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue, pending []*openresponses.FunctionCall, note string) error {
	parts := make([]a2a.Part, 0, len(pending)+1)
	if note != "" {
		parts = append(parts, a2a.TextPart{Text: note})
	}
	for _, fc := range pending {
		dp, err := dataPart(fc)
		if err != nil {
			return err
		}
		parts = append(parts, dp)
	}
	// []any, not []string: the SDK's task store accepts only the types a
	// JSON decode produces, and so does every caller on the wire.
	ids := make([]any, 0, len(pending))
	for _, fc := range pending {
		ids = append(ids, fc.CallID)
	}
	msg := a2a.NewMessageForTask(a2a.MessageRoleAgent, reqCtx, parts...)
	msg.SetMeta(MetaPendingCalls, ids)
	return e.finish(ctx, reqCtx, q, a2a.TaskStateInputRequired, msg)
}

func (e *Executor) finish(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue, state a2a.TaskState, msg *a2a.Message) error {
	ev := a2a.NewStatusUpdateEvent(reqCtx, state, msg)
	ev.Final = true
	return q.Write(ctx, ev)
}

func errorMessage(info a2a.TaskInfoProvider, err error) *a2a.Message {
	if err == nil {
		err = errors.New("run failed")
	}
	return a2a.NewMessageForTask(a2a.MessageRoleAgent, info, a2a.TextPart{Text: err.Error()})
}

// Cancel aborts the task's run, if one is active, and marks the task
// canceled.
func (e *Executor) Cancel(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue) error {
	e.mu.Lock()
	cancel := e.cancels[reqCtx.TaskID]
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return e.finish(ctx, reqCtx, q, a2a.TaskStateCanceled, nil)
}

// taskText returns the text of a task's final message or, failing that,
// of its artifacts: what a caller reads from a completed task. The
// consume side, tools/a2a, has its own reader; this one serves the
// tests.
func taskText(task *a2a.Task) string {
	if task == nil {
		return ""
	}
	if task.Status.Message != nil {
		if s := partsText(task.Status.Message.Parts); s != "" {
			return s
		}
	}
	var b strings.Builder
	for _, a := range task.Artifacts {
		b.WriteString(partsText(a.Parts))
	}
	return b.String()
}

// partsText mirrors tools/a2a's partsText; change both together.
func partsText(parts a2a.ContentParts) string {
	var b strings.Builder
	for _, p := range parts {
		if t, ok := p.(a2a.TextPart); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String()
}
