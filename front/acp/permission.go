package acp

import (
	"context"
	"errors"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// ask puts each deferred call to the client and returns the answers to
// resume with. A cancelled turn returns its context's error, leaving the
// calls pending for the next prompt to close and marking them cancelled
// on the client.
func (t *turn) ask(ctx context.Context, pending []agentturn.PendingCall) ([]agentturn.Answer, error) {
	answers := make([]agentturn.Answer, 0, len(pending))
	for _, p := range pending {
		t.mu.Lock()
		reason := t.reasons[p.Call.CallID]
		t.mu.Unlock()
		args := p.Args
		if args == nil {
			args = []byte(p.Call.Arguments)
		}
		allowed, err := t.permit(ctx, p.Call.CallID, p.Call.Name, "", args, reason)
		if err != nil {
			t.abandon(ctx, pending)
			return nil, err
		}
		if allowed {
			answers = append(answers, agentturn.Approve(p.Call.CallID).WithBy("human"))
			continue
		}
		answers = append(answers, agentturn.Refuse(openresponses.NewFunctionCallOutput(p.Call.CallID, RefusedOutput)).WithBy("human"))
		if err := t.sink.update(ctx, p.Call.CallID, "", statusFailed, RefusedOutput, nil); err != nil {
			return nil, err
		}
	}
	return answers, nil
}

// abandon marks the calls cancelled as not run, for a turn that ends
// before they were answered. It sends on a context the turn's
// cancellation does not reach, so the client does not keep them
// pending.
func (t *turn) abandon(ctx context.Context, pending []agentturn.PendingCall) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for _, p := range pending {
		_ = t.sink.update(ctx, p.Call.CallID, "", statusCancelled, NotRunOutput, nil)
	}
}

// permit asks the client whether one call may run. A call the client
// has not seen is announced first, so the request names a tool call it
// can show.
func (t *turn) permit(ctx context.Context, callID, name, parent string, args []byte, reason string) (bool, error) {
	tool := t.tools[name]
	if !t.sess.seen(callID) {
		if err := t.propose(ctx, callID, name, parent, args); err != nil {
			return false, err
		}
	}
	allowed, err := t.sink.permit(ctx, callID, parent, title(name, tool), t.server.kind(name, tool), args, reason)
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return allowed, nil
}

// ErrNoQuestion is returned by [Elicitor] for a question it cannot put
// to the client: one asked outside a turn of this front, or one that is
// not about a call, such as an MCP server's form.
var ErrNoQuestion = errors.New("front/acp: the client cannot be asked this question")

// Elicitor is an agenttool.Elicitor for a configuration served by this
// front, as its ToolElicitor. A question about a call, one
// agentturn.Ask puts with the call on the context, is sent as a
// session/request_permission on the session whose turn asked it, under
// the asked call's ID: allow accepts and reject declines. A question
// the turn's cancellation cut short is a cancel.
func Elicitor(ctx context.Context, q agenttool.Elicitation) (agenttool.Answer, error) {
	t, ok := ctx.Value(turnKey{}).(*turn)
	if !ok {
		return agenttool.Answer{}, ErrNoQuestion
	}
	call, ok := agentturn.AskedCallFrom(ctx)
	if !ok {
		return agenttool.Answer{}, ErrNoQuestion
	}
	reason := q.Message
	if call.Decision != nil && call.Decision.Reason != "" {
		reason = call.Decision.Reason
	}
	allowed, err := t.permit(ctx, call.CallID, call.Name, call.Parent, call.Args, reason)
	switch {
	case ctx.Err() != nil:
		return agenttool.Answer{Action: agenttool.ActionCancel}, nil
	case err != nil:
		return agenttool.Answer{}, err
	case allowed:
		return agenttool.Answer{Action: agenttool.ActionAccept}, nil
	}
	return agenttool.Answer{Action: agenttool.ActionDecline}, nil
}
