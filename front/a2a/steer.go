package a2a

import (
	"context"
	"fmt"
	"iter"

	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
)

// WithDeliverWhileBusy accepts a message that names a task still
// working, the steer A2A allows (§3.3.3, "Agents MAY accept additional
// messages for tasks in non-terminal states"): its text and files are
// handed to the task's running agent as a delivery, under the trigger
// of the caller that sent it, and the send is answered with that task,
// as it stands for a non-blocking send, followed to its end for a
// blocking one, its events for a stream. The delivery joins the run in
// flight when that run will still take it, and otherwise the task
// continues the agent with it once the run has ended, so a run the
// delivery starts belongs to the same task and the task does not end
// while the delivery is outstanding. A run that ends waiting on calls
// leaves it for the message that answers them, which takes it after
// the answered batch, unless the task is cancelled first; one that
// fails or is cancelled drops it, with the task. A task holds at most
// [MaxSteers] steers it has not taken. A [Redeliverer] the task's
// [RecorderFor] put on its context takes each steer, and each one
// owed to the answering message, so the record holds it before the
// agent does.
//
// The conversation still runs one task at a time. A message naming
// only the context, which the A2A server makes a new task, is still
// refused with [ErrConversationBusy] while a task is in flight: joining
// it to the running task would leave its caller holding a task that
// never runs, so a caller steers by naming the task. A steer carrying
// a data part, a call's output or an approval, or declaring caller
// tools, is refused while the task runs, since what it answers has not
// been asked yet; a message naming a task that is not working, one
// waiting on input among them, is served as without the option. The
// owner check applies as to any message. The steer is served by the
// handler [Executor.RequestHandler] builds; a handler built with
// a2asrv.NewHandler refuses it, as the A2A server refuses a second
// message for a task whose execution is in progress. Without the
// option every message on a busy conversation is refused.
func WithDeliverWhileBusy() Option { return func(e *Executor) { e.deliverBusy = true } }

// MaxSteers is how many steers a working task holds that its agent has
// not taken yet, the ones it keeps for the message answering its calls
// included; [WithDeliverWhileBusy] refuses the next with
// a2a.ErrInvalidRequest until the agent takes one. A steer the agent
// has taken no longer counts.
const MaxSteers = 16

// Redeliverer takes a steer into a task's running agent through what
// records the conversation, which writes it before the agent holds it,
// as agentturn/session's Recorder.Redeliver does, so a caller's input
// survives a quit as a local one does (RFC 0001 Steering, #244). A
// [RecorderFor] puts one on the context it returns, with
// [ContextWithRedeliverer]; [WithDeliverWhileBusy] then hands it every
// steer, and the steers a task left for the message answering its
// calls, rather than the agent's own Redeliver. A steer it refuses is
// refused to the caller.
type Redeliverer interface {
	Redeliver(ctx context.Context, a *agentturn.Agent, items ...openresponses.Item) error
}

type redelivererKey struct{}

// ContextWithRedeliverer returns ctx carrying r, for a [RecorderFor] to
// return.
func ContextWithRedeliverer(ctx context.Context, r Redeliverer) context.Context {
	return context.WithValue(ctx, redelivererKey{}, r)
}

// redelivererFrom returns the [Redeliverer] on ctx, or nil.
func redelivererFrom(ctx context.Context) Redeliverer {
	r, _ := ctx.Value(redelivererKey{}).(Redeliverer)
	return r
}

// running is a task whose run is in flight: its agent, and the steers
// callers delivered into it.
type running struct {
	task      a2a.TaskID
	contextID string
	agent     *agentturn.Agent
	// via is the [Redeliverer] the task's RecorderFor returned, or nil.
	via Redeliverer
	// open is whether the task still takes a steer; it is closed in the
	// same step as the check that finds none outstanding.
	open    bool
	steered []steer
}

// owedSteers are the steers a task left queued when it stopped for
// input, for the message that answers its calls.
type owedSteers struct {
	task   a2a.TaskID
	steers []steer
}

// steer is an item a caller delivered into a running task, with the
// caller's trigger.
type steer struct {
	item    openresponses.Item
	trigger agentturn.Trigger
}

// track registers agent as task's running agent and hands it the
// steers the conversation's last task left owed, waiting on calls the
// task now answers, through via when the task's RecorderFor gave one.
// It returns the task's record.
func (e *Executor) track(ctx context.Context, task a2a.TaskID, contextID string, agent *agentturn.Agent, via Redeliverer) *running {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running == nil {
		e.running = map[a2a.TaskID]*running{}
	}
	rt := &running{task: task, contextID: contextID, agent: agent, via: via, open: true}
	for _, s := range e.owed[contextID].steers {
		if rt.redeliver(agentturn.ContextWithTrigger(ctx, s.trigger), s.item) == nil {
			rt.steered = append(rt.steered, s)
		}
	}
	delete(e.owed, contextID)
	e.running[task] = rt
	return rt
}

// redeliver hands items to rt's agent as a steer, through its
// [Redeliverer] when it has one.
func (rt *running) redeliver(ctx context.Context, items ...openresponses.Item) error {
	if rt.via != nil {
		return rt.via.Redeliver(ctx, rt.agent, items...)
	}
	return rt.agent.Redeliver(ctx, items...)
}

// untaken drops from rt's steers the ones its agent has taken, and
// returns the rest. The caller holds e.mu.
func (rt *running) untaken() []steer {
	queued := map[openresponses.Item]bool{}
	for _, item := range rt.agent.State().Steered {
		queued[item] = true
	}
	var left []steer
	for _, s := range rt.steered {
		if queued[s.item] {
			left = append(left, s)
		}
	}
	rt.steered = left
	return left
}

// untrack forgets task's running agent.
func (e *Executor) untrack(task a2a.TaskID) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if rt := e.running[task]; rt != nil {
		rt.open = false
	}
	delete(e.running, task)
}

// outstanding reports the trigger to continue rt's agent under when a
// steer delivered into it is still queued after a run that ended with
// end, so the task runs on to take it. When none is, or the run ended
// in a way no continue follows, the task stops taking steers in the
// same step: steers left queued by a run waiting on calls are owed to
// the conversation's next task, which answers them; any other's are
// dropped with the task.
func (e *Executor) outstanding(rt *running, end *agentturn.RunEnd) (agentturn.Trigger, bool) {
	if rt == nil {
		return agentturn.Trigger{}, false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	left := rt.untaken()
	if len(left) > 0 && end != nil && (end.Reason == agentturn.ReasonDone || end.Reason == agentturn.ReasonStopped) {
		return left[0].trigger, true
	}
	rt.open = false
	if len(left) > 0 && end != nil && end.Reason == agentturn.ReasonInputRequired {
		if e.owed == nil {
			e.owed = map[string]owedSteers{}
		}
		e.owed[rt.contextID] = owedSteers{task: rt.task, steers: left}
	}
	return agentturn.Trigger{}, false
}

// dropOwed drops the steers task left for the message answering its
// calls, for a cancel of the task: the caller gave the task up, and its
// steers with it.
func (e *Executor) dropOwed(contextID string, task a2a.TaskID) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.owed[contextID].task == task {
		delete(e.owed, contextID)
	}
}

// deliver hands msg to the running agent of the task it names, under
// [WithDeliverWhileBusy]. It reports false, with nothing done, when
// the task has no run in flight that still takes a steer, so the
// message is served as any other; an error refuses the message.
func (e *Executor) deliver(ctx context.Context, msg *a2a.Message) (bool, error) {
	if !e.deliverBusy || msg == nil || msg.TaskID == "" {
		return false, nil
	}
	e.mu.Lock()
	rt := e.running[msg.TaskID]
	e.mu.Unlock()
	if rt == nil {
		return false, nil
	}
	ctx, _, err := e.authenticated(ctx)
	if err != nil {
		return false, err
	}
	for i, part := range msg.Parts {
		if _, ok := part.(a2a.DataPart); ok {
			return false, a2a.NewError(fmt.Errorf("%w: parts[%d] is a data part; a message to a working task carries text and files, and answers to its calls wait until it asks", a2a.ErrInvalidParams, i), "a working task takes text and files only")
		}
	}
	if declared, err := callerTools(msg); err != nil || len(declared) > 0 {
		return false, a2a.NewError(fmt.Errorf("%w: a message to a working task declares no tools", a2a.ErrInvalidParams), "a working task takes text and files only")
	}
	items, err := ItemsFromMessage(msg)
	if err != nil {
		return false, fmt.Errorf("%w: %w", a2a.ErrInvalidParams, err)
	}
	if len(items) == 0 {
		return false, fmt.Errorf("%w: message has no usable parts", a2a.ErrInvalidParams)
	}
	trigger := agentturn.TriggerFromContext(ctx)
	e.mu.Lock()
	defer e.mu.Unlock()
	if !rt.open {
		return false, nil
	}
	if left := rt.untaken(); len(left)+len(items) > MaxSteers {
		return false, a2a.NewError(fmt.Errorf("%w: the task holds %d steers it has not taken", a2a.ErrInvalidRequest, len(left)), "the task holds as many steers as it takes; send again once it has taken one")
	}
	if err := rt.redeliver(ctx, items...); err != nil {
		return false, err
	}
	for _, item := range items {
		rt.steered = append(rt.steered, steer{item: item, trigger: trigger})
	}
	return true, nil
}

// steering serves [WithDeliverWhileBusy]'s steer inside the interceptors,
// so the caller is authenticated and its ownership of the task checked
// before the message reaches a running agent, and passes everything
// else to the A2A server.
type steering struct {
	a2asrv.RequestHandler
	e *Executor
}

func (h steering) OnSendMessage(ctx context.Context, params *a2a.MessageSendParams) (a2a.SendMessageResult, error) {
	if params == nil {
		return h.RequestHandler.OnSendMessage(ctx, params)
	}
	ok, err := h.e.deliver(ctx, params.Message)
	if err != nil {
		return nil, err
	}
	if !ok {
		return h.RequestHandler.OnSendMessage(ctx, params)
	}
	id := &a2a.TaskIDParams{ID: params.Message.TaskID}
	if params.Config == nil || params.Config.Blocking == nil || *params.Config.Blocking {
		// A blocking send waits for the task, as it waits for any.
		for _, err := range h.RequestHandler.OnResubscribeToTask(ctx, id) {
			if err != nil {
				break
			}
		}
	}
	return h.RequestHandler.OnGetTask(ctx, &a2a.TaskQueryParams{ID: id.ID})
}

func (h steering) OnSendMessageStream(ctx context.Context, params *a2a.MessageSendParams) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		var ok bool
		var err error
		if params != nil {
			ok, err = h.e.deliver(ctx, params.Message)
		}
		if err != nil {
			yield(nil, err)
			return
		}
		if !ok {
			for ev, err := range h.RequestHandler.OnSendMessageStream(ctx, params) {
				if !yield(ev, err) {
					return
				}
			}
			return
		}
		// The stream is the task's from here on; one that ended in
		// between is answered with the task as it ended.
		id := params.Message.TaskID
		sent := false
		for ev, err := range h.RequestHandler.OnResubscribeToTask(ctx, &a2a.TaskIDParams{ID: id}) {
			if err != nil {
				break
			}
			sent = true
			if !yield(ev, nil) {
				return
			}
		}
		if !sent {
			task, err := h.RequestHandler.OnGetTask(ctx, &a2a.TaskQueryParams{ID: id})
			yield(task, err)
		}
	}
}
