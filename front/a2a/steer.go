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
// the answered batch; one that fails or is cancelled drops it, with
// the task.
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

// running is a task whose run is in flight: its agent, and the steers
// callers delivered into it.
type running struct {
	contextID string
	agent     *agentturn.Agent
	// open is whether the task still takes a steer; it is closed in the
	// same step as the check that finds none outstanding.
	open    bool
	steered []steer
}

// steer is an item a caller delivered into a running task, with the
// caller's trigger.
type steer struct {
	item    openresponses.Item
	trigger agentturn.Trigger
}

// track registers agent as task's running agent and hands it the
// steers the conversation's last task left owed, waiting on calls the
// task now answers. It returns the task's record.
func (e *Executor) track(ctx context.Context, task a2a.TaskID, contextID string, agent *agentturn.Agent) *running {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running == nil {
		e.running = map[a2a.TaskID]*running{}
	}
	rt := &running{contextID: contextID, agent: agent, open: true}
	for _, s := range e.owed[contextID] {
		if agent.Redeliver(agentturn.ContextWithTrigger(ctx, s.trigger), s.item) == nil {
			rt.steered = append(rt.steered, s)
		}
	}
	delete(e.owed, contextID)
	e.running[task] = rt
	return rt
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
	if len(left) > 0 && end != nil && (end.Reason == agentturn.ReasonDone || end.Reason == agentturn.ReasonStopped) {
		return left[0].trigger, true
	}
	rt.open = false
	if len(left) > 0 && end != nil && end.Reason == agentturn.ReasonInputRequired {
		if e.owed == nil {
			e.owed = map[string][]steer{}
		}
		e.owed[rt.contextID] = append(e.owed[rt.contextID], left...)
	}
	return agentturn.Trigger{}, false
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
	if err := rt.agent.Redeliver(ctx, items...); err != nil {
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
