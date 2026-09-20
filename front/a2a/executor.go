package a2a

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
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

	mu      sync.Mutex
	cancels map[a2a.TaskID]context.CancelFunc
}

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

// New builds an executor for cfg.
func New(cfg agentturn.Config, opts ...Option) *Executor {
	e := &Executor{cfg: cfg, cancels: map[a2a.TaskID]context.CancelFunc{}}
	for _, opt := range opts {
		opt(e)
	}
	if e.store == nil {
		e.store = NewMemoryStore()
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
	declared, err := callerTools(reqCtx.Message)
	if err != nil {
		return fmt.Errorf("%w: %w", a2a.ErrInvalidParams, err)
	}
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
	if err := q.Write(ctx, a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateWorking, nil)); err != nil {
		return err
	}
	cfg := e.runConfig(append(append([]*openresponses.FunctionTool(nil), e.callerTools...), declared...))
	out := e.relay(ctx, runCtx, cancel, reqCtx, q, transcript, prompts, cfg)
	if out.end == nil {
		// Run refused to start; nothing was appended.
		return e.finish(ctx, reqCtx, q, a2a.TaskStateFailed, errorMessage(reqCtx, out.runErr))
	}
	if err := e.persist(ctx, reqCtx.ContextID, transcript, out.end); err != nil {
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

// outcome is what relay reports about a run.
type outcome struct {
	end      *agentturn.RunEnd
	lastText string
	// writeErr is the first failure to write an event to the queue; it
	// aborted the run.
	writeErr error
	// runErr is the error Run yielded, which is the RunEnd's error or a
	// refusal to start.
	runErr error
}

// relay drives the loop and streams assistant text into artifacts. A
// failed write aborts the run through cancel and is reported on the
// outcome; the run's own end and error are reported alongside.
func (e *Executor) relay(ctx, runCtx context.Context, cancel context.CancelFunc, reqCtx *a2asrv.RequestContext, q eventqueue.Queue, transcript agentturn.Transcript, prompts openresponses.Items, cfg agentturn.Config) outcome {
	var out outcome
	var writer *artifactWriter
	fail := func(err error) {
		if out.writeErr == nil {
			out.writeErr = err
		}
		cancel()
	}
	for ev, err := range agentturn.Run(runCtx, transcript, prompts, cfg) {
		if err != nil {
			out.runErr = err
		}
		switch ev := ev.(type) {
		case *agentturn.ItemStart:
			if m, ok := ev.Item.(*openresponses.Message); ok && m.Role == openresponses.RoleAssistant {
				writer = newArtifactWriter(q, reqCtx, e.chunkSize)
			}
		case *agentturn.ItemUpdate:
			if writer == nil {
				continue
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
				if err := writer.close(ctx); err != nil {
					fail(err)
				}
				writer = nil
			}
		case *agentturn.RunEnd:
			out.end = ev
		}
	}
	return out
}

// persist stores the conversation after a run: everything the run
// appended. A deferred call stays unanswered on purpose, since the
// caller answers it on the next message; after an abort or a failure
// the calls that will never be answered are dropped so the next message
// is a valid input.
func (e *Executor) persist(ctx context.Context, contextID string, transcript agentturn.Transcript, end *agentturn.RunEnd) error {
	next := append(transcript, end.Items...)
	switch end.Reason {
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
		return e.inputRequired(ctx, reqCtx, q, out.end.Pending, "")
	case agentturn.ReasonDone, agentturn.ReasonStopped:
		var msg *a2a.Message
		if out.lastText != "" {
			msg = a2a.NewMessageForTask(a2a.MessageRoleAgent, reqCtx, a2a.TextPart{Text: out.lastText})
		}
		return e.finish(ctx, reqCtx, q, a2a.TaskStateCompleted, msg)
	case agentturn.ReasonAborted:
		// The canceler usually wrote the canceled status already and the
		// SDK then cancels this context; a failed write here is expected.
		_ = e.finish(context.WithoutCancel(ctx), reqCtx, q, a2a.TaskStateCanceled, nil)
		return nil
	default:
		err := out.end.Err
		if err == nil {
			err = out.runErr
		}
		return e.finish(ctx, reqCtx, q, a2a.TaskStateFailed, errorMessage(reqCtx, err))
	}
}

// runConfig returns the config for one run: the agent's tools plus the
// caller-owned ones, offered to the model but never executed here, and
// a BeforeToolCall that defers every call to a caller-owned tool. The
// loop then ends the run with ReasonInputRequired and the pending calls
// on the RunEnd, which is the input-required boundary of the task. The
// agent's own hook runs first and its block or rewrite is respected.
func (e *Executor) runConfig(caller []*openresponses.FunctionTool) agentturn.Config {
	cfg := e.cfg
	if len(caller) == 0 {
		return cfg
	}
	owned := make(map[string]bool, len(caller))
	stubs := make([]agenttool.Tool, 0, len(caller))
	for _, ft := range caller {
		owned[ft.Name] = true
		stubs = append(stubs, &agenttool.Func{
			ToolName:        ft.Name,
			ToolDescription: ft.Description,
			Schema:          ft.Parameters,
			StrictSchema:    ft.Strict != nil && *ft.Strict,
		})
	}
	local, provider := e.cfg.Tools, e.cfg.ToolProvider
	cfg.Tools = nil
	cfg.ToolProvider = func(ctx context.Context) []agenttool.Tool {
		base := local
		if provider != nil {
			base = provider(ctx)
		}
		return append(append([]agenttool.Tool(nil), base...), stubs...)
	}
	before := e.cfg.BeforeToolCall
	cfg.BeforeToolCall = func(ctx context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
		var decision *agentturn.ToolDecision
		if before != nil {
			var err error
			decision, err = before(ctx, info)
			if err != nil {
				return nil, err
			}
		}
		if !owned[info.Call.Name] || (decision != nil && decision.Block) {
			return decision, nil
		}
		if decision == nil {
			decision = &agentturn.ToolDecision{}
		}
		decision.Defer = true
		return decision, nil
	}
	return cfg
}

// unanswered returns the function calls in t that have no output, in
// order.
func unanswered(t agentturn.Transcript) []*openresponses.FunctionCall {
	answered := map[string]bool{}
	for _, item := range t {
		if fco, ok := item.(*openresponses.FunctionCallOutput); ok {
			answered[fco.CallID] = true
		}
	}
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
	answered := map[string]bool{}
	for _, item := range items {
		if fco, ok := item.(*openresponses.FunctionCallOutput); ok {
			answered[fco.CallID] = true
		}
	}
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

// Text returns the text of a task's final message or, failing that, of
// its artifacts. It is what a caller reads from a completed task.
func Text(task *a2a.Task) string {
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

func partsText(parts a2a.ContentParts) string {
	var b strings.Builder
	for _, p := range parts {
		if t, ok := p.(a2a.TextPart); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String()
}
