package acp

import (
	"context"
	"fmt"
	"sync"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// MetaParent is the _meta key under which a nested call's tool call
// updates carry the ID of the call whose tool made it.
const MetaParent = "agentturn/parent"

// RefusedOutput is the output a call the user rejected is answered
// with.
const RefusedOutput = "The user rejected this call; it did not run. Wait for their instructions."

// NotRunOutput is the output a call left pending by a cancelled turn is
// answered with at the session's next prompt, when it did not run.
const NotRunOutput = "Not run: the turn was cancelled before the user answered."

// status is a tool call's status in the terms both protocol versions
// share; v1 has no cancelled and reports failed.
type status int

const (
	statusPending status = iota
	statusRunning
	statusCompleted
	statusFailed
	statusCancelled
)

// sink is what one protocol version does with the loop's events.
type sink interface {
	text(ctx context.Context, messageID, delta string) error
	thought(ctx context.Context, messageID, delta string) error
	propose(ctx context.Context, callID, parent, title string, kind ToolKind, args []byte) error
	update(ctx context.Context, callID, parent string, st status, content string, args []byte) error
	// permit asks whether a call may run and reports whether it may.
	permit(ctx context.Context, callID, parent, title string, kind ToolKind, args []byte, reason string) (bool, error)
	// inserted reports a waiting prompt's message entering the
	// transcript, and releases its waiter.
	inserted(ctx context.Context, w *waiter) error
}

// usage is a turn's usage summed over its responses.
type usage struct {
	counted                       bool
	input, output, total, thought uint64
	cached                        uint64
}

// turn is one ACP turn: the runs it drives, the permission requests they
// lead to, and the updates they stream through its sink.
type turn struct {
	server *Server
	sess   *session
	sink   sink
	tools  map[string]agenttool.Tool

	mu      sync.Mutex
	reasons map[string]string // deferral reasons by call ID
	usage   usage
}

func newTurn(server *Server, sess *session, out sink) *turn {
	t := &turn{server: server, sess: sess, sink: out, tools: map[string]agenttool.Tool{}, reasons: map[string]string{}}
	for _, tool := range sess.agent.Config().Tools {
		t.tools[tool.Name()] = tool
	}
	return t
}

type turnKey struct{}

// drive runs the agent once, with input as a prompt or, when there is
// none, as a continuation of what is queued, and then asks the client
// about every call the run defers and resumes, until the run ends
// otherwise.
func (t *turn) drive(ctx context.Context, input openresponses.Items) (*agentturn.RunEnd, error) {
	var end *agentturn.RunEnd
	var err error
	if len(input) > 0 {
		end, err = t.sess.agent.Prompt(ctx, input...)
	} else {
		end, err = t.sess.agent.Continue(ctx)
	}
	for err == nil && end.Reason == agentturn.ReasonInputRequired {
		var answers []agentturn.Answer
		answers, err = t.ask(ctx, end.Pending)
		if err != nil {
			break
		}
		end, err = t.sess.agent.Resume(ctx, answers...)
	}
	return end, err
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

// stopReason is the stop reason both protocol versions spell the same.
func stopReason(end *agentturn.RunEnd) string {
	switch end.Reason {
	case agentturn.ReasonAborted:
		return "cancelled"
	case agentturn.ReasonStopped:
		switch end.Cause {
		case agentturn.StopGuard:
			return "refusal"
		case agentturn.StopMaxTurns:
			return "max_turn_requests"
		}
	}
	return "end_turn"
}

// observe turns the loop's events into session updates. It runs on the
// loop's goroutine as a barrier, so updates go out in the loop's order.
func (t *turn) observe(ctx context.Context, ev agentturn.Event) error {
	switch e := ev.(type) {
	case *agentturn.ItemUpdate:
		switch d := e.Stream.(type) {
		case *openresponses.OutputTextDeltaEvent:
			return t.sink.text(ctx, messageID(e, d.ItemID, d.OutputIndex), d.Delta)
		case *openresponses.ReasoningDeltaEvent:
			return t.sink.thought(ctx, messageID(e, d.ItemID, d.OutputIndex), d.Delta)
		case *openresponses.ReasoningSummaryTextDeltaEvent:
			return t.sink.thought(ctx, messageID(e, d.ItemID, d.OutputIndex), d.Delta)
		}
	case *agentturn.ItemEnd:
		if w := t.sess.take(e.Item); w != nil {
			return t.sink.inserted(ctx, w)
		}
	case *agentturn.ResponseEnd:
		t.count(e.Response)
	case *agentturn.ToolStart:
		return t.propose(ctx, e.CallID, e.Name, e.Parent, e.Args)
	case *agentturn.ToolDispatch:
		return t.sink.update(ctx, e.CallID, e.Parent, statusRunning, "", nil)
	case *agentturn.ToolUpdate:
		if text := e.Partial.Output.String(); text != "" {
			return t.sink.update(ctx, e.CallID, "", statusRunning, text, nil)
		}
	case *agentturn.ToolEnd:
		return t.end(ctx, e)
	}
	return nil
}

// messageID names the message a delta belongs to: the item's own ID, or
// one made from where the item sits when the model gave it none.
func messageID(e *agentturn.ItemUpdate, itemID string, index int) string {
	if itemID != "" {
		return itemID
	}
	return fmt.Sprintf("%s-%d-%d", e.RunID, e.Turn, index)
}

// propose reports a decided call as pending: as a new tool call the
// first time, and as an update when an approval runs a call the client
// has already seen.
func (t *turn) propose(ctx context.Context, callID, name, parent string, args []byte) error {
	if t.sess.report(callID) {
		return t.sink.update(ctx, callID, parent, statusPending, "", args)
	}
	tool := t.tools[name]
	return t.sink.propose(ctx, callID, parent, title(name, tool), t.server.kind(name, tool), args)
}

func (t *turn) end(ctx context.Context, e *agentturn.ToolEnd) error {
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
	st := statusCompleted
	if e.Err != nil || e.Blocked {
		st = statusFailed
	}
	return t.sink.update(ctx, e.CallID, e.Parent, st, out, nil)
}

// count adds a response's usage to the turn's.
func (t *turn) count(r *openresponses.Response) {
	if r == nil || r.Usage == nil {
		return
	}
	u := r.Usage
	t.mu.Lock()
	defer t.mu.Unlock()
	t.usage.counted = true
	t.usage.input += uint64(u.InputTokens)
	t.usage.output += uint64(u.OutputTokens)
	t.usage.total += uint64(u.TotalTokens)
	t.usage.thought += uint64(u.OutputTokensDetails.ReasoningTokens)
	t.usage.cached += uint64(u.InputTokensDetails.CachedTokens)
}

// summed returns the turn's usage so far.
func (t *turn) summed() usage {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.usage
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

// optionalCount is n as an optional count, nil for zero.
func optionalCount(n uint64) *uint64 {
	if n == 0 {
		return nil
	}
	return &n
}
