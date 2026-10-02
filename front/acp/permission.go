package acp

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ironpark/acp-go/acp1"
)

// permissionOptions are the choices every request offers. There is no
// "always": the front keeps no rules, and a host that wants them puts
// a policy in BeforeToolCall.
var permissionOptions = []acp1.PermissionOption{
	acp1.NewPermissionOption(acp1.PermissionOptionKindAllowOnce, "Allow"),
	acp1.NewPermissionOption(acp1.PermissionOptionKindRejectOnce, "Reject"),
}

// ask puts each deferred call to the client and returns the answers to
// resume with. A cancelled turn returns its context's error, leaving the
// calls pending for the next prompt to close.
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
			t.fail(ctx, pending, NotRunOutput)
			return nil, err
		}
		if allowed {
			answers = append(answers, agentturn.Approve(p.Call.CallID).WithBy("human"))
			continue
		}
		answers = append(answers, agentturn.Refuse(openresponses.NewFunctionCallOutput(p.Call.CallID, RefusedOutput)).WithBy("human"))
		if err := t.stream.FailToolCall(ctx, acp1.ToolCallID(p.Call.CallID), acp1.WithToolContent(acp1.ToolText(RefusedOutput))); err != nil {
			return nil, err
		}
	}
	return answers, nil
}

// fail marks the calls failed with text, for a turn that ends before
// they were answered. It sends on a context the turn's cancellation does
// not reach, so the client does not keep them pending.
func (t *turn) fail(ctx context.Context, pending []agentturn.PendingCall, text string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for _, p := range pending {
		_ = t.stream.FailToolCall(ctx, acp1.ToolCallID(p.Call.CallID), acp1.WithToolContent(acp1.ToolText(text)))
	}
}

// permit sends one session/request_permission about a call and reports
// whether the user allowed it. A call the client has not seen is
// announced first, so the request names a tool call it can show.
func (t *turn) permit(ctx context.Context, callID, name, parent string, args []byte, reason string) (bool, error) {
	if !t.sess.seen(callID) {
		if err := t.propose(ctx, callID, name, parent, args); err != nil {
			return false, err
		}
	}
	update := acp1.ToolCallUpdate{
		ToolCallID: acp1.ToolCallID(callID),
		Status:     new(acp1.ToolCallStatusPending),
		RawInput:   jsontext.Value(args),
	}
	if reason != "" {
		update.Content = []acp1.ToolCallContent{acp1.ToolText(reason)}
	}
	_, allowed, err := t.on(parent).RequestPermission(ctx, update, permissionOptions...)
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return allowed, nil
}

// ErrNoQuestion is returned by [Elicitor] for a question it cannot put
// to the client: one asked outside a prompt turn of this front, or one
// that is not about a call, such as an MCP server's form.
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
