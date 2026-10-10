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
// <args>? <reason>", the arguments clipped at 500 bytes, and the call
// on the elicitor's context for [AskedCallFrom]. It returns the
// decision the answer makes, by "human": Allow with the deferral's
// reason, or "allowed when asked" when it has none, on an accept, and
// Block with "declined when asked" and the reason on a decline. What
// the user said with the answer ([agenttool.Answer.Note]) reaches the
// model: on a decline it ends the reason the model is told, and on an
// accept it is the decision's Note, which the model reads with the
// call's result. It
// returns nil and false when ctx carries no elicitor, the ask failed
// or the answer was a cancel; the caller then still holds the
// deferral it started with. call.Decision must be non-nil.
func Ask(ctx context.Context, call AskedCall) (*ToolDecision, bool) {
	elicit, ok := agenttool.ElicitorFrom(ctx)
	if !ok {
		return nil, false
	}
	msg := fmt.Sprintf("Allow %s with arguments %s?", call.Name, clip(string(call.Args), maxAskedArgs))
	if call.Decision.Reason != "" {
		msg += " " + call.Decision.Reason
	}
	ans, err := elicit(ContextWithAskedCall(ctx, call), agenttool.Elicitation{Message: msg})
	if err != nil {
		return nil, false
	}
	// An elicitation is a question for the user, so the user decided.
	decided := *call.Decision
	decided.By = "human"
	switch ans.Action {
	case agenttool.ActionAccept:
		// The reason is the rule that raised the question, as a held
		// call's is; the record needs one to write the approval.
		decided.Action = Allow
		if decided.Reason == "" {
			decided.Reason = "allowed when asked"
		}
		if ans.Note != "" {
			if decided.Note != "" {
				decided.Note += "\n"
			}
			decided.Note += "The user said: " + ans.Note
		}
	case agenttool.ActionDecline:
		decided.Action = Block
		decided.Reason = "declined when asked"
		if call.Decision.Reason != "" {
			decided.Reason += ": " + call.Decision.Reason
		}
		if ans.Note != "" {
			decided.Reason += "; the user said: " + ans.Note
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
