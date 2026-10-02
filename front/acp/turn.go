package acp

import (
	"context"
	"encoding/json/jsontext"
	"sync"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ironpark/acp-go/acp1"
)

// MetaParent is the _meta key under which a nested call's tool_call and
// tool_call_update carry the ID of the call whose tool made it.
const MetaParent = "agentturn/parent"

// RefusedOutput is the output a call the user rejected is answered
// with.
const RefusedOutput = "The user rejected this call; it did not run. Wait for their instructions."

// NotRunOutput is the output a call left pending by a cancelled turn is
// answered with at the session's next prompt, when it did not run.
const NotRunOutput = "Not run: the turn was cancelled before the user answered."

// turn is one session/prompt: the run, the resumes its permission
// requests lead to, and the updates they stream.
type turn struct {
	server *Server
	sess   *session
	stream *acp1.SessionStream
	tools  map[string]agenttool.Tool

	mu      sync.Mutex
	reasons map[string]string // deferral reasons by call ID
	usage   acp1.Usage
	counted bool
}

func newTurn(server *Server, sess *session, stream *acp1.SessionStream) *turn {
	t := &turn{server: server, sess: sess, stream: stream, tools: map[string]agenttool.Tool{}, reasons: map[string]string{}}
	for _, tool := range sess.agent.Config().Tools {
		t.tools[tool.Name()] = tool
	}
	return t
}

type turnKey struct{}

// run prompts the session's agent with msg, asks the client about every
// call the run defers and resumes until the run ends otherwise.
func (t *turn) run(ctx context.Context, msg *openresponses.Message) (*acp1.PromptResponse, error) {
	unsubscribe := t.sess.agent.Subscribe(t.observe)
	defer unsubscribe()
	ctx = context.WithValue(ctx, turnKey{}, t)

	input := closePending(t.sess.agent.State().Pending)
	end, err := t.sess.agent.Prompt(ctx, append(input, msg)...)
	for err == nil && end.Reason == agentturn.ReasonInputRequired {
		var answers []agentturn.Answer
		answers, err = t.ask(ctx, end.Pending)
		if err != nil {
			break
		}
		end, err = t.sess.agent.Resume(ctx, answers...)
	}
	if err != nil {
		return nil, err
	}
	resp := &acp1.PromptResponse{StopReason: stopReason(end)}
	t.mu.Lock()
	if t.counted {
		usage := t.usage
		resp.Usage = &usage
	}
	t.mu.Unlock()
	return resp, nil
}

// closePending answers the calls an earlier, cancelled turn left
// pending, so the prompt that follows is accepted: a call that may have
// run with agentturn.OutcomeUnknown, a refused one with its refusal,
// and any other with NotRunOutput.
func closePending(pending []agentturn.PendingCall) openresponses.Items {
	var items openresponses.Items
	for _, p := range pending {
		id := p.Call.CallID
		switch {
		case p.Reason == agentturn.PendingRejected && p.Refused != "":
			items = append(items, openresponses.NewFunctionCallOutput(id, p.Refused))
		case p.MayHaveRun() || p.Reason == agentturn.PendingAnswered:
			items = append(items, agentturn.OutcomeUnknown(id).Output)
		default:
			items = append(items, openresponses.NewFunctionCallOutput(id, NotRunOutput))
		}
	}
	return items
}

func stopReason(end *agentturn.RunEnd) acp1.StopReason {
	switch end.Reason {
	case agentturn.ReasonAborted:
		return acp1.StopReasonCancelled
	case agentturn.ReasonStopped:
		switch end.Cause {
		case agentturn.StopGuard:
			return acp1.StopReasonRefusal
		case agentturn.StopMaxTurns:
			return acp1.StopReasonMaxTurnRequests
		}
	}
	return acp1.StopReasonEndTurn
}

// observe turns the loop's events into session updates. It runs on the
// loop's goroutine as a barrier, so updates go out in the loop's order.
func (t *turn) observe(ctx context.Context, ev agentturn.Event) error {
	switch e := ev.(type) {
	case *agentturn.ItemUpdate:
		switch d := e.Stream.(type) {
		case *openresponses.OutputTextDeltaEvent:
			return t.stream.SendText(ctx, d.Delta)
		case *openresponses.ReasoningDeltaEvent:
			return t.stream.SendThought(ctx, d.Delta)
		case *openresponses.ReasoningSummaryTextDeltaEvent:
			return t.stream.SendThought(ctx, d.Delta)
		}
	case *agentturn.ResponseEnd:
		t.count(e.Response)
	case *agentturn.ToolStart:
		return t.propose(ctx, e.CallID, e.Name, e.Parent, e.Args)
	case *agentturn.ToolDispatch:
		return t.on(e.Parent).UpdateToolCallStatus(ctx, acp1.ToolCallID(e.CallID), acp1.ToolCallStatusInProgress)
	case *agentturn.ToolUpdate:
		if text := e.Partial.Output.String(); text != "" {
			return t.stream.UpdateToolCallStatus(ctx, acp1.ToolCallID(e.CallID), acp1.ToolCallStatusInProgress,
				acp1.WithToolContent(acp1.ToolText(text)))
		}
	case *agentturn.ToolEnd:
		return t.end(ctx, e)
	}
	return nil
}

// propose reports a decided call as pending: as a new tool_call the
// first time, and as an update when an approval runs a call the client
// has already seen.
func (t *turn) propose(ctx context.Context, callID, name, parent string, args []byte) error {
	stream := t.on(parent)
	id := acp1.ToolCallID(callID)
	raw := acp1.WithRawInput(jsontext.Value(args))
	if t.sess.report(callID) {
		return stream.UpdateToolCallStatus(ctx, id, acp1.ToolCallStatusPending, raw)
	}
	tool := t.tools[name]
	return stream.ProposeToolCall(ctx, id, title(name, tool), t.server.kind(name, tool), raw)
}

func (t *turn) end(ctx context.Context, e *agentturn.ToolEnd) error {
	stream := t.on(e.Parent)
	id := acp1.ToolCallID(e.CallID)
	if e.Deferred {
		// It stays pending: the permission request about it follows.
		t.mu.Lock()
		t.reasons[e.CallID] = e.Reason
		t.mu.Unlock()
		return nil
	}
	out := e.Result.Output.String()
	if out == "" && e.Err != nil {
		out = e.Err.Error()
	}
	var opts []acp1.ToolCallOption
	if out != "" {
		opts = append(opts, acp1.WithToolContent(acp1.ToolText(out)))
	}
	if e.Err != nil || e.Blocked {
		return stream.FailToolCall(ctx, id, opts...)
	}
	return stream.CompleteToolCall(ctx, id, opts...)
}

// on returns the stream for a call's updates: a nested call's carry the
// invoking call's ID under MetaParent.
func (t *turn) on(parent string) *acp1.SessionStream {
	if parent == "" {
		return t.stream
	}
	var meta acp1.Meta
	if err := meta.Set(MetaParent, parent); err != nil {
		return t.stream
	}
	return t.stream.WithMeta(meta)
}

// count adds a response's usage to the turn's.
func (t *turn) count(r *openresponses.Response) {
	if r == nil || r.Usage == nil {
		return
	}
	u := r.Usage
	t.mu.Lock()
	defer t.mu.Unlock()
	t.counted = true
	t.usage.InputTokens += uint64(u.InputTokens)
	t.usage.OutputTokens += uint64(u.OutputTokens)
	t.usage.TotalTokens += uint64(u.TotalTokens)
	if n := u.OutputTokensDetails.ReasoningTokens; n > 0 {
		t.usage.ThoughtTokens = new(t.usage.GetThoughtTokens() + uint64(n))
	}
	if n := u.InputTokensDetails.CachedTokens; n > 0 {
		t.usage.CachedReadTokens = new(t.usage.GetCachedReadTokens() + uint64(n))
	}
}

// title is the tool's annotated title, or its name.
func title(name string, tool agenttool.Tool) string {
	if a, ok := tool.(agenttool.Annotated); ok {
		if t := a.Annotations().Title; t != "" {
			return t
		}
	}
	return name
}
