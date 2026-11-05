package a2a

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/a2aproject/a2a-go/a2a"
)

const (
	// MetaEscalatedCalls, on an input-required status message, lists the
	// calls of the agent's own tools its policy asked about and
	// [WithEscalation] put to the caller: a JSON array of objects with
	// "call_id", "name", "arguments", the arguments the call runs with
	// if approved (the policy's rewrite, else the model's), and
	// "reason", the policy's reason for asking.
	// The calls are among the status message's function_call data
	// parts, and the caller answers each with an [Approval].
	MetaEscalatedCalls = "agentturn.escalated_calls"
	// ApprovalType is the "type" of a data part that answers an
	// escalated call: an [Approval].
	ApprovalType = "agentturn.approval"
)

// Approval answers a call of the agent's own tool that its policy asked
// about and [WithEscalation] put to the caller. The caller sends it as a
// data part ([ApprovalPart]) on a message for the same task, beside the
// function_call_output items of any calls to its own tools, one answer
// per pending call.
//
// Who answered is the authenticated principal: the run the answer
// resumes is prompted under the principal's trigger. Who decided, in
// the session format's terms, is By: empty, the decision names nobody,
// since the executor cannot tell a person on the caller's side from a
// program; "human", "policy" or "agent" only from a principal the host
// lets name one ([WithChosenBy]), and an approval naming one otherwise
// is refused, never recorded. A note is the caller's words, untrusted:
// an approval's reaches the model after the batch's outputs as "The
// caller <principal> said: <note>", a user message unless By names a
// rule or another model, whose words are a developer message, and
// never the serving side's own instructions; a refusal's is part of
// the refusal the model reads as the call's output.
type Approval struct {
	CallID string `json:"call_id"`
	// Approve runs the call; false refuses it.
	Approve bool   `json:"approve"`
	Note    string `json:"note,omitempty"`
	// By is who decided, "human", "policy" or "agent", taken only
	// under [WithChosenBy]; empty names nobody.
	By string `json:"by,omitempty"`
}

// WithChosenBy lets a principal the host names say who decided its
// approvals, with [Approval.By]: a policy engine or another agent that
// answers for its own side, whose host has a reason to believe it.
// chosen is the host's check, called with the request's context and
// the authenticated principal for each approval that names a decider.
// Without it, or when it says no, an approval that names a decider is
// refused and the message with it: the executor cannot tell a person
// on the caller's side from a program, and an unchecked claim is never
// recorded. An approval that names nobody is recorded as decided by
// nobody, this front's default, whatever the option. A host whose
// policy needs a person makes its check, or its BeforeToolCall, refuse
// an approval not by "human"; the package ships no such policy. A2A
// has no field for who answered (identity stays at the protocol
// layer), so the decider is a member of agentturn's own approval part.
func WithChosenBy(chosen func(ctx context.Context, principal string) bool) Option {
	return func(e *Executor) { e.chosenBy = chosen }
}

// ApprovalPart encodes an approval as the data part a message carries.
func ApprovalPart(a Approval) a2a.DataPart {
	data := map[string]any{"type": ApprovalType, "call_id": a.CallID, "approve": a.Approve}
	if a.Note != "" {
		data["note"] = a.Note
	}
	if a.By != "" {
		data["by"] = a.By
	}
	return a2a.DataPart{Data: data}
}

// WithEscalation puts a call of the agent's own tool that its policy
// asks about to the caller instead of the serving side's elicitor: the
// run ends input-required, the task enters input-required with the call
// among the status message's function_call data parts and the policy's
// reason under [MetaEscalatedCalls] and as text, and the caller's
// [Approval] runs or refuses it. Only the calls the policy deferred in
// that task may be approved, with the decision the policy made about
// them, its rewrite of the arguments and its note included; an approval
// runs the call as decided, without asking the policy again. It is for
// a headless agent nobody watches, whose caller's human plane should
// decide. Without it each system's policy is its own, and the serving
// side answers its own agent's questions, as the package documents.
//
// It requires [WithAuthenticator], since an approval lets a call run on
// the caller's word: [New] panics without it.
func WithEscalation() Option { return func(e *Executor) { e.escalate = true } }

// Escalated is a call a task put to its caller, or held beside one: the
// call, and the decision the policy made that deferred it, with its
// reason, its rewrite of the arguments and its note. A held call
// (Decision.Held), which the policy allowed and held only beside an
// asked sibling, is not put to the caller; it is released, by the
// policy, with the answers to the calls that were when every approval
// accepts, and refused unrun, by the policy, when the caller refused
// one.
type Escalated struct {
	Task     string                      `json:"task"`
	Call     *openresponses.FunctionCall `json:"call"`
	Decision *agentturn.ToolDecision     `json:"decision,omitempty"`
}

// EscalationStore keeps the calls each conversation's last task put to
// its caller, beside the transcript, so an approval after a restart
// runs the call as the policy decided. A [ConversationStore] that
// implements it keeps them; with one that does not, the executor keeps
// them in memory, and after a restart an approval of a call it no
// longer knows is refused and the call dropped, as an unanswerable one
// is. [MemoryStore] implements it.
type EscalationStore interface {
	LoadEscalated(ctx context.Context, contextID string) ([]Escalated, error)
	SaveEscalated(ctx context.Context, contextID string, calls []Escalated) error
}

// memoryEscalations is the executor's fallback [EscalationStore].
type memoryEscalations struct {
	mu    sync.Mutex
	calls map[string][]Escalated
}

func (m *memoryEscalations) LoadEscalated(_ context.Context, contextID string) ([]Escalated, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Escalated(nil), m.calls[contextID]...), nil
}

func (m *memoryEscalations) SaveEscalated(_ context.Context, contextID string, calls []Escalated) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.calls == nil {
		m.calls = map[string][]Escalated{}
	}
	if len(calls) == 0 {
		delete(m.calls, contextID)
		return nil
	}
	m.calls[contextID] = append([]Escalated(nil), calls...)
	return nil
}

// approvalsOf takes the [Approval] parts out of msg, returning them and
// a copy of msg with the other parts.
func approvalsOf(msg *a2a.Message) ([]Approval, *a2a.Message, error) {
	var out []Approval
	rest := *msg
	rest.Parts = nil
	for i, part := range msg.Parts {
		dp, ok := part.(a2a.DataPart)
		if !ok || dp.Data["type"] != ApprovalType {
			rest.Parts = append(rest.Parts, part)
			continue
		}
		raw, err := json.Marshal(dp.Data)
		if err != nil {
			return nil, nil, fmt.Errorf("parts[%d]: %w", i, err)
		}
		var a Approval
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, nil, fmt.Errorf("parts[%d]: %w", i, err)
		}
		if a.CallID == "" {
			return nil, nil, fmt.Errorf("parts[%d]: an approval names no call_id", i)
		}
		switch a.By {
		case "", agenttool.ByHuman, agenttool.ByPolicy, agenttool.ByAgent:
		default:
			return nil, nil, fmt.Errorf("parts[%d]: unknown decider %q: an approval is by human, policy or agent, or names nobody", i, a.By)
		}
		out = append(out, a)
	}
	return out, &rest, nil
}

// checkChosen refuses an approval that names who decided unless the
// host lets principal name one ([WithChosenBy]).
func (e *Executor) checkChosen(ctx context.Context, principal string, approvals []Approval) error {
	for _, a := range approvals {
		if a.By == "" {
			continue
		}
		if e.chosenBy == nil || !e.chosenBy(ctx, principal) {
			return fmt.Errorf("approval %q names who decided, and the host does not let %s say; the executor records the authenticated principal", a.CallID, principal)
		}
	}
	return nil
}

// answer is the agentturn answer an approval of call makes, said by
// principal. An approval runs the call with the policy's rewrite of its
// arguments; a refusal says the caller's note in its output, which the
// model reads, so the note is not said a second time. The refusal is
// the deferral's decline text when the host gave one (#304), else the
// front's own words, and the note follows either, named for the
// caller.
func (a Approval) answer(call Escalated, principal string) agentturn.Answer {
	if a.Approve {
		ans := agentturn.Approve(a.CallID)
		if call.Decision != nil && call.Decision.Args != nil {
			ans = agentturn.ApproveWith(a.CallID, call.Decision.Args)
		}
		ans = ans.WithReason("approved by the caller " + principal).WithBy(a.By)
		if a.Note != "" {
			// The loop says it in the decider's voice: the user's for
			// a person or nobody, the developer's for a rule or
			// another model the host let the caller name. Never the
			// serving side's own instructions: it is quoted.
			ans = ans.WithNote("The caller " + principal + " said: " + a.Note)
		}
		return ans
	}
	text := "the caller " + principal + " refused this call"
	said := "; the caller said: "
	if call.Decision != nil && call.Decision.DeclineText != "" {
		// The host worded the refusal; the caller is named with the
		// note, since the host's text may not name them.
		text = call.Decision.DeclineText
		said = "; the caller " + principal + " said: "
	}
	if a.Note != "" {
		text += said + a.Note
	}
	return agentturn.Output(&openresponses.FunctionCallOutput{CallID: a.CallID, Output: openresponses.FunctionCallOutputData{Text: text}}).WithBy(a.By)
}

// asked reports whether c is put to the caller rather than held beside
// a call that is.
func (c Escalated) asked() bool { return c.Decision == nil || !c.Decision.Held }

// args is what the call runs with when it is approved: the policy's
// rewrite, else the model's arguments.
func (c Escalated) args() string {
	if c.Decision != nil && c.Decision.Args != nil {
		return string(c.Decision.Args)
	}
	return c.Call.Arguments
}

// release is the policy's answer to a held call: it runs, as the policy
// allowed it, with its rewrite.
func (c Escalated) release() agentturn.Answer {
	ans := agentturn.Approve(c.Call.CallID)
	if c.Decision.Args != nil {
		ans = agentturn.ApproveWith(c.Call.CallID, c.Decision.Args)
	}
	return ans.WithBy("policy").WithReason("released: " + c.Decision.Reason)
}

// refuse answers a held call whose batch the caller refused: not run,
// by the policy that held it, since it was held only beside the call
// the caller said no to. The model reads the hold's own decline text
// when the host gave one (#304).
func (c Escalated) refuse(principal string) agentturn.Answer {
	text := "not run: the caller " + principal + " refused the call this one was held beside"
	if c.Decision.DeclineText != "" {
		text = c.Decision.DeclineText
	}
	return agentturn.Output(&openresponses.FunctionCallOutput{CallID: c.Call.CallID, Output: openresponses.FunctionCallOutputData{Text: text}}).WithBy("policy").WithReason(text)
}

// escalation is what an input-required status message says about the
// calls put to the caller: the text parts and the metadata, each with
// the arguments the call runs with if approved.
func escalation(calls []Escalated) ([]a2a.Part, []any) {
	var parts []a2a.Part
	var meta []any
	for _, c := range calls {
		if !c.asked() {
			continue
		}
		reason := ""
		if c.Decision != nil {
			reason = c.Decision.Reason
		}
		if reason == "" {
			reason = "the policy asks before this call runs"
		}
		parts = append(parts, a2a.TextPart{Text: fmt.Sprintf("The agent's policy asks before running %s (call %s) with %s: %s", c.Call.Name, c.Call.CallID, c.args(), reason)})
		meta = append(meta, map[string]any{"call_id": c.Call.CallID, "name": c.Call.Name, "arguments": c.args(), "reason": reason})
	}
	return parts, meta
}
