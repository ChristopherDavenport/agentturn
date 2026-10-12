package a2a

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
	"github.com/a2aproject/a2a-go/a2asrv/eventqueue"
)

// Cancel aborts the task's run, if one is active, and marks the task
// canceled.
//
// With no run active, a task its last run left input-required has its
// calls pending in the conversation, and a caller that gives the task up
// is not asking for them any more. The cancel settles them before it
// marks the task canceled: each call the task's status message listed
// is refused, on a run of the conversation's agent that what
// [WithRecorderFor] attached records, with an output saying the caller
// canceled the task, so the record holds why, the stored conversation
// holds no unanswered call, and the conversation's next message starts
// clean. See the package documentation's section on cancel.
func (e *Executor) Cancel(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue) error {
	ctx, principal, err := e.authenticated(ctx)
	if err != nil {
		return err
	}
	if err := e.owns(ctx, principal, reqCtx.TaskID); err != nil {
		return err
	}
	// A caller sends the cancel and need not wait for it: one that hangs
	// up still has the calls settled. The settlement calls no model, and
	// the context keeps the caller's trigger.
	ctx = context.WithoutCancel(ctx)
	cancel, release := e.claimCancel(reqCtx.ContextID, reqCtx.TaskID)
	if cancel != nil {
		cancel()
	}
	if release != nil {
		defer release()
		if err := e.settle(ctx, reqCtx, principal); err != nil {
			return err
		}
		e.markCanceled(reqCtx.ContextID, reqCtx.TaskID, time.Now())
	}
	return e.finish(ctx, reqCtx, q, a2a.TaskStateCanceled, nil)
}

// canceledMarkAge is how long a cancel's mark outlives it: the mark is
// for a message the SDK let through before the cancel, and after the
// cancel the SDK refuses a message on the canceled task itself.
const canceledMarkAge = time.Minute

// canceledMark is the task a cancel ended with no run active, and when.
type canceledMark struct {
	task a2a.TaskID
	at   time.Time
}

// markCanceled marks task, canceled at now in its conversation, and
// drops the marks older than canceledMarkAge, so conversations that see
// no further message do not keep theirs.
func (e *Executor) markCanceled(contextID string, task a2a.TaskID, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for id, mark := range e.canceled {
		if now.Sub(mark.at) > canceledMarkAge {
			delete(e.canceled, id)
		}
	}
	if e.canceled == nil {
		e.canceled = map[string]canceledMark{}
	}
	e.canceled[contextID] = canceledMark{task: task, at: now}
}

// claimCancel returns the cancel of the task's run when one is active.
// Otherwise it claims the task's conversation, as a message's task
// does, and returns the function that frees it; or neither, when
// another task is in flight on the conversation, which by its end
// answers the calls this task left or lists them as its own. Looking
// for the run and claiming are one step, so no run starts between them.
func (e *Executor) claimCancel(contextID string, task a2a.TaskID) (context.CancelFunc, func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if cancel := e.cancels[task]; cancel != nil {
		return cancel, nil
	}
	if contextID == "" || e.busy[contextID] {
		return nil, nil
	}
	if e.busy == nil {
		e.busy = map[string]bool{}
	}
	e.busy[contextID] = true
	return nil, func() {
		e.mu.Lock()
		delete(e.busy, contextID)
		e.mu.Unlock()
	}
}

// settle refuses the calls an input-required task left pending in its
// conversation, as canceled by the caller, for a cancel that found no
// run active and holds the conversation.
//
// The calls are those the task's status message listed, while the
// conversation waits on the task ([WaitingStore]) and every call still
// unanswered in it is among them. A conversation another task has moved
// on in, answering the calls or listing them again as its own, holds
// none of this task's, and nothing is settled; with no task named,
// after a restart under a store that does not keep it, the calls alone
// decide. Each
// is answered with agentturn.Refuse, which ends the run without
// calling the model, since nobody asks for the work: the output, which
// the model reads on the conversation's next run, says the caller
// canceled the task, and so does the answer's reason. The run is under
// the caller's trigger and names nobody as the decider (By ""), as an
// [Approval]'s refusal does, so a session recorder writes each as a
// refusal of the held call by the caller. The stored conversation gets
// the outputs, and the calls the task put to its caller under
// [WithEscalation] are forgotten.
func (e *Executor) settle(ctx context.Context, reqCtx *a2asrv.RequestContext, principal string) error {
	if e.auth != nil {
		if err := e.mayWrite(ctx, principal, reqCtx.ContextID); err != nil {
			return err
		}
	}
	listed := listedCalls(reqCtx.StoredTask)
	if len(listed) == 0 {
		return nil
	}
	waiting, err := e.waitingStore().LoadWaiting(ctx, reqCtx.ContextID)
	if err != nil {
		return fmt.Errorf("load the task %q waits on: %w", reqCtx.ContextID, err)
	}
	if waiting != "" && waiting != string(reqCtx.TaskID) {
		return nil
	}
	transcript, err := e.store.Load(ctx, reqCtx.ContextID)
	if err != nil {
		return fmt.Errorf("load conversation %q: %w", reqCtx.ContextID, err)
	}
	// A call of the agent's own that the task did not list is dropped,
	// as a message's task drops it; one it listed is the task's to
	// settle, whoever would have answered it.
	mine := make(map[string]Escalated, len(listed))
	for id := range listed {
		mine[id] = Escalated{}
	}
	transcript, _ = e.closeOwnPending(ctx, transcript, e.callerTools, mine)
	pending := unanswered(transcript)
	if len(pending) == 0 {
		return nil
	}
	for _, fc := range pending {
		if !listed[fc.CallID] {
			return nil
		}
	}
	text := "the caller canceled the task, so this call was not run"
	if principal != "" {
		text = "the caller " + principal + " canceled the task, so this call was not run"
	}
	answers := make([]agentturn.Answer, 0, len(pending))
	for _, fc := range pending {
		answers = append(answers, agentturn.Refuse(&openresponses.FunctionCallOutput{CallID: fc.CallID, Output: openresponses.FunctionCallOutputData{Text: text}}).WithReason(text))
	}
	// The tools a message declared are not offered: the run makes no
	// request, and an output answers a call whatever tool it named.
	agent := agentturn.New(e.runConfig(e.startFor(ctx, transcript), e.callerTools), agentturn.WithTranscript(transcript))
	runCtx := ctx
	if e.recorderFor != nil {
		rctx, detach, err := e.recorderFor(ctx, reqCtx.ContextID, agent)
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
	end, err := agent.Resume(runCtx, answers...)
	if end == nil {
		return fmt.Errorf("settle the calls of task %q: %w", reqCtx.TaskID, err)
	}
	if err := e.persist(ctx, reqCtx.ContextID, transcript, outcome{end: end, items: end.Items}); err != nil {
		return err
	}
	if end.Reason != agentturn.ReasonStopped {
		// A subscriber failed the run, and the store dropped the calls
		// as after any failed run; the cancel fails, so the record's
		// failure is not taken for a settled task.
		cause := end.Err
		if cause == nil {
			cause = fmt.Errorf("run ended %s", end.Reason)
		}
		return fmt.Errorf("settle the calls of task %q: %w", reqCtx.TaskID, cause)
	}
	if e.escalate {
		all, err := e.escalationStore().LoadEscalated(ctx, reqCtx.ContextID)
		if err != nil {
			return fmt.Errorf("load escalated calls of %q: %w", reqCtx.ContextID, err)
		}
		var rest []Escalated
		for _, c := range all {
			if c.Task != string(reqCtx.TaskID) {
				rest = append(rest, c)
			}
		}
		if len(rest) != len(all) {
			if err := e.escalationStore().SaveEscalated(ctx, reqCtx.ContextID, rest); err != nil {
				return fmt.Errorf("save escalated calls of %q: %w", reqCtx.ContextID, err)
			}
		}
	}
	if err := e.waitingStore().SaveWaiting(ctx, reqCtx.ContextID, ""); err != nil {
		return fmt.Errorf("save the task %q waits on: %w", reqCtx.ContextID, err)
	}
	return nil
}

// listedCalls returns the IDs of the calls an input-required task's
// status message lists as pending: its function_call data parts, the
// caller's calls and those put to the caller alike.
func listedCalls(task *a2a.Task) map[string]bool {
	if task == nil || task.Status.State != a2a.TaskStateInputRequired || task.Status.Message == nil {
		return nil
	}
	out := map[string]bool{}
	for _, part := range task.Status.Message.Parts {
		dp, ok := part.(a2a.DataPart)
		if !ok || dp.Data["type"] != "function_call" {
			continue
		}
		if id, ok := dp.Data["call_id"].(string); ok && id != "" {
			out[id] = true
		}
	}
	return out
}
