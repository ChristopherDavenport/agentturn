package agentturn

import (
	"context"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"github.com/ChristopherDavenport/agenttool"
)

// AskedCall is a call the loop, or a front, is putting to the user
// through the agenttool.Elicitor on the elicitor's context: a nested
// call the hook deferred, whose Parent is the call that made it, or a
// call a front asks about itself, with no Parent (front/a2a asks about
// a call to the agent's own tool that its hook held, since its caller
// answers only calls to the tools it owns). A front's elicitor reads it
// with [AskedCallFrom] to show choices of its own, or to turn an answer
// into a rule from Name and Args, where the question's text alone
// would have to be parsed. The question is still filed under the
// invoking call, which stays on the context as agenttool.CallFrom.
type AskedCall struct {
	// Parent is the ID of the call whose tool made this one, and empty
	// for a call the model made.
	Parent string
	CallID string
	Name   string
	// Args are the arguments the call would run with, whole, where the
	// question's text quotes the first 500 bytes.
	Args json.RawMessage
	// Decision is the hook's deferral as it stood when the question was
	// asked; its Reason is the rule that asked.
	Decision *ToolDecision
	// Callee names the agent that would run the call when it is not
	// this run's: the remote agent whose own policy escalated it to its
	// caller, as tools/a2a's WithAskCaller asks. Empty for a call of
	// this run or of a child it runs.
	Callee string
}

type askedCallKey struct{}

// AskedCallFrom returns the call the elicitor on ctx is being asked
// about, when [Ask] put one there.
func AskedCallFrom(ctx context.Context) (AskedCall, bool) {
	call, ok := ctx.Value(askedCallKey{}).(AskedCall)
	return call, ok
}

// ContextWithAskedCall returns ctx carrying call for [AskedCallFrom].
// [Ask] uses it; a front that asks through another route puts the
// call on the context the same way.
func ContextWithAskedCall(ctx context.Context, call AskedCall) context.Context {
	return context.WithValue(ctx, askedCallKey{}, call)
}

// Ask puts call to the elicitor on ctx, as the loop puts a nested call
// the hook deferred: with the question "Allow <name> with arguments
// <args>? <reason>", or "Allow agent <callee> to run <name> ..." for a
// call another agent would run ([AskedCall.Callee]), the arguments
// clipped at 500 bytes, and the call on the elicitor's context for
// [AskedCallFrom]. It returns the
// decision the answer makes, by the decider the answer names
// ([agenttool.Answer.By]), or "human" when it names none, since an
// answer that names nobody is the user's: Allow with the deferral's
// reason, or "allowed when asked" when it has none, on an accept, and
// Block with "declined when asked" and the reason on a decline, or the
// deferral's [ToolDecision.DeclineText] in their place when the host
// gave one. What
// the answerer said with the answer ([agenttool.Answer.Note]) reaches
// the model: on a decline it ends the reason the model is told, named
// for whoever said it, whichever text it follows, and on
// an accept, under a run's hook or tool, it is a message of its own,
// held with the call and appended after the outputs of the batch it
// belongs to, for a nested call the batch of the call that made it: a
// user message for "human", in the person's voice, and a developer
// message for a rule or another model, whose words are not the user's.
// The decision's Note stays the policy's, a developer message. Outside
// a run there is nothing to hold it with, nor for a call another agent
// would run, whose note its caller sends on, and the decision's Note
// carries it as "The user said: <note>" ("The policy said", "Another
// agent said"). It returns nil and false when ctx carries no elicitor,
// the ask failed, the answer was a cancel or it named a decider the
// session format does not know, which is refused rather than read as
// the user's; the caller then still holds the deferral it started
// with. call.Decision must be non-nil.
func Ask(ctx context.Context, call AskedCall) (*ToolDecision, bool) {
	elicit, ok := agenttool.ElicitorFrom(ctx)
	if !ok {
		return nil, false
	}
	msg := fmt.Sprintf("Allow %s with arguments %s?", call.Name, clip(string(call.Args), maxAskedArgs))
	if call.Callee != "" {
		msg = fmt.Sprintf("Allow agent %s to run %s with arguments %s?", call.Callee, call.Name, clip(string(call.Args), maxAskedArgs))
	}
	if call.Decision.Reason != "" {
		msg += " " + call.Decision.Reason
	}
	ans, err := elicit(ContextWithAskedCall(ctx, call), agenttool.Elicitation{Message: msg})
	if err != nil {
		return nil, false
	}
	// The answer names its decider; one that names nobody is the
	// user's. A name the format does not know is refused, never read
	// as the user's.
	if knownDecider(ans.By) != nil {
		return nil, false
	}
	by := ans.By
	if by == "" {
		by = agenttool.ByHuman
	}
	decided := *call.Decision
	decided.By = by
	switch ans.Action {
	case agenttool.ActionAccept:
		// The reason is the rule that raised the question, as a held
		// call's is; the record needs one to write the approval.
		decided.Action = Allow
		if decided.Reason == "" {
			decided.Reason = "allowed when asked"
		}
		if ans.Note != "" {
			if r, ok := ctx.Value(noteSinkKey{}).(*runner); ok && call.Callee == "" {
				// The answerer's words are theirs, a message in their
				// voice held with the call, apart from the decision's
				// own Note. A
				// nested call is tied to the call that made it, as
				// Invoke ties it, so the note waits with that call's
				// batch even when the loop never runs the nested call.
				// One tied to nothing is held with the call whose tool
				// asked.
				r.adopt(call.CallID, call.Parent)
				asking := ""
				if c, ok := agenttool.CallFrom(ctx); ok {
					asking = c.ID
				}
				r.sayUnder(answerNoteItem(ans.Note, by), call.CallID, call.Parent, asking)
			} else {
				// No run to hold it with, or the call is another
				// agent's, whose caller sends the note on: the decision
				// carries it.
				if decided.Note != "" {
					decided.Note += "\n"
				}
				decided.Note += saidBy(by, ans.Note)
			}
		}
	case agenttool.ActionDecline:
		decided.Action = Block
		switch {
		case call.Decision.DeclineText != "":
			// The host worded the refusal, as it words the question.
			decided.Reason = call.Decision.DeclineText
		case call.Decision.Reason != "":
			decided.Reason = "declined when asked: " + call.Decision.Reason
		default:
			decided.Reason = "declined when asked"
		}
		if ans.Note != "" {
			decided.Reason += "; " + lowerFirst(saidBy(by, ans.Note))
		}
	default:
		return nil, false
	}
	return &decided, true
}

// maxAskedArgs is the most of an asked call's arguments, in bytes, the
// question about it quotes: a script's call may carry a whole file.
const maxAskedArgs = 500

// clip returns s cut to at most n bytes on a rune boundary, with an
// ellipsis when anything was cut.
func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "…"
}
