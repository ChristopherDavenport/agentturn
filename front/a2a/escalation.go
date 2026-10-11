package a2a

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/a2aproject/a2a-go/a2a"
)

const (
	// MetaEscalatedCalls, on an input-required status message, lists the
	// calls of the agent's own tools its policy asked about and
	// [WithEscalation] put to the caller: a JSON array of objects with
	// "call_id", "name" and "reason", the policy's reason for asking.
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
// Who decided is the authenticated principal, never what the caller
// claims: the run the answer resumes is prompted under the principal's
// trigger, and the decision names nobody in the session format's terms
// (By ""), since the executor cannot tell a person on the caller's side
// from a program. A note is the caller's words, untrusted: an
// approval's reaches the model after the batch's outputs as a user
// message, "The caller <principal> said: <note>", never in the
// developer's voice; a refusal's is part of the refusal the model reads
// as the call's output.
type Approval struct {
	CallID string `json:"call_id"`
	// Approve runs the call; false refuses it.
	Approve bool   `json:"approve"`
	Note    string `json:"note,omitempty"`
}

// ApprovalPart encodes an approval as the data part a message carries.
func ApprovalPart(a Approval) a2a.DataPart {
	data := map[string]any{"type": ApprovalType, "call_id": a.CallID, "approve": a.Approve}
	if a.Note != "" {
		data["note"] = a.Note
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

// Escalated is a call a task put to its caller: the call, and the
// decision the policy made that deferred it, with its reason, its
// rewrite of the arguments and its note.
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
		if _, ok := dp.Data["by"]; ok {
			return nil, nil, fmt.Errorf("parts[%d]: an approval names who decided; the executor records the authenticated principal", i)
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
		out = append(out, a)
	}
	return out, &rest, nil
}

// answer is the agentturn answer an approval of call makes, said by
// principal. An approval runs the call with the policy's rewrite of its
// arguments; a refusal says the caller's note in its output, which the
// model reads, so the note is not said a second time.
func (a Approval) answer(call Escalated, principal string) agentturn.Answer {
	if a.Approve {
		ans := agentturn.Approve(a.CallID)
		if call.Decision != nil && call.Decision.Args != nil {
			ans = agentturn.ApproveWith(a.CallID, call.Decision.Args)
		}
		ans = ans.WithReason("approved by the caller " + principal)
		if a.Note != "" {
			// By "" says it in the user's voice: the caller's words,
			// never the developer's.
			ans = ans.WithNote("The caller " + principal + " said: " + a.Note)
		}
		return ans
	}
	text := "the caller " + principal + " refused this call"
	if a.Note != "" {
		text += "; the caller said: " + a.Note
	}
	return agentturn.Output(&openresponses.FunctionCallOutput{CallID: a.CallID, Output: openresponses.FunctionCallOutputData{Text: text}})
}

// escalation is what an input-required status message says about the
// escalated calls: the text parts and the metadata.
func escalation(calls []Escalated) ([]a2a.Part, []any) {
	var parts []a2a.Part
	var meta []any
	for _, c := range calls {
		reason := ""
		if c.Decision != nil {
			reason = c.Decision.Reason
		}
		if reason == "" {
			reason = "the policy asks before this call runs"
		}
		parts = append(parts, a2a.TextPart{Text: fmt.Sprintf("The agent's policy asks before running %s (call %s): %s", c.Call.Name, c.Call.CallID, reason)})
		meta = append(meta, map[string]any{"call_id": c.Call.CallID, "name": c.Call.Name, "reason": reason})
	}
	return parts, meta
}
