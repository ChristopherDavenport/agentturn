package a2a

import (
	"encoding/json"
	"fmt"

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
type Approval struct {
	CallID string `json:"call_id"`
	// Approve runs the call; false refuses it, and the model reads the
	// refusal, with Note, as the call's output.
	Approve bool `json:"approve"`
	// Note is what the caller's side said when answering. An approval's
	// note reaches the model after the batch's outputs, in the voice of
	// By; a refusal's is part of the refusal the model reads.
	Note string `json:"note,omitempty"`
	// By says who decided on the caller's side: "human" for a person,
	// "agent" for a model or a program, which is what an empty By
	// means. A caller cannot answer as this agent's "policy". The
	// record names the caller's principal on the run that the answer
	// resumes, under [WithAuthenticator].
	By string `json:"by,omitempty"`
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
// [Approval] runs or refuses it. It is for a headless agent nobody
// watches, whose caller's human plane should decide. Without it each
// system's policy is its own, and the serving side answers its own
// agent's questions, as the package documents.
func WithEscalation() Option { return func(e *Executor) { e.escalate = true } }

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
		case "":
			a.By = "agent"
		case "human", "agent":
		default:
			return nil, nil, fmt.Errorf("parts[%d]: an approval by %q; a caller answers as \"human\" or \"agent\"", i, a.By)
		}
		out = append(out, a)
	}
	return out, &rest, nil
}

// answer is the agentturn answer an approval makes. A refusal says the
// caller's note in its output, which the model reads, so the note is
// not said a second time.
func (a Approval) answer() agentturn.Answer {
	if a.Approve {
		return agentturn.Approve(a.CallID).WithBy(a.By).WithNote(a.Note).WithReason("approved by the caller")
	}
	text := "the caller refused this call"
	if a.Note != "" {
		text += "; the caller said: " + a.Note
	}
	return agentturn.Output(&openresponses.FunctionCallOutput{CallID: a.CallID, Output: openresponses.FunctionCallOutputData{Text: text}}).WithBy(a.By)
}

// escalation is what an input-required status message says about the
// escalated calls among pending: the text parts and the metadata.
func escalation(pending []*openresponses.FunctionCall, caller map[string]bool, reasons map[string]string) ([]a2a.Part, []any) {
	var parts []a2a.Part
	var meta []any
	for _, fc := range pending {
		if caller[fc.Name] {
			continue
		}
		reason := reasons[fc.CallID]
		if reason == "" {
			reason = "the policy asks before this call runs"
		}
		parts = append(parts, a2a.TextPart{Text: fmt.Sprintf("The agent's policy asks before running %s (call %s): %s", fc.Name, fc.CallID, reason)})
		meta = append(meta, map[string]any{"call_id": fc.CallID, "name": fc.Name, "reason": reason})
	}
	return parts, meta
}
