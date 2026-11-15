package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// stoppedShort answers with a message ended incomplete for reason: a
// refusal with the provider's category for content_filter, cut text
// for max_output_tokens.
type stoppedShort struct {
	reason openresponses.IncompleteReason
}

func (m stoppedShort) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	if m.reason == openresponses.IncompleteReasonContentFilter {
		next := sink
		sink = openresponses.EventSinkFunc(func(ev openresponses.StreamEvent) error {
			if e, ok := ev.(*openresponses.ResponseIncompleteEvent); ok {
				e.Response.IncompleteDetails.Extra = map[string]any{"anthropic.refusal_category": "cyber"}
			}
			return next.Send(ev)
		})
	}
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	w, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if m.reason == openresponses.IncompleteReasonContentFilter {
		err = w.Refusal("I can't help with that.")
	} else {
		err = w.Text("Once upon")
	}
	if err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Incomplete(m.reason)
}

// TestAChildsIncompleteEndReachesTheParent pins #314 one hop on: a
// child the provider's content filter stopped fails the call with its
// refusal text and the provider's category, so the parent's model
// reads why, and a child whose answer was cut at the output limit
// answers with the cut text; ChildInfo carries the details and the
// refusal for a host either way.
func TestAChildsIncompleteEndReachesTheParent(t *testing.T) {
	call := agenttool.Call{ID: "c1", Args: json.RawMessage(`{"input":"x"}`)}

	child := New(agentturn.Config{Name: "explore", Model: stoppedShort{reason: openresponses.IncompleteReasonContentFilter}})
	res, err := child.Execute(context.Background(), call)
	const want = `agent "explore" was refused by its model's provider (content_filter, anthropic.refusal_category: cyber): I can't help with that.`
	if err == nil || err.Error() != want {
		t.Errorf("content filter: err = %v, want %q", err, want)
	}
	info, ok := res.Details.(ChildInfo)
	if !ok || info.Cause != agentturn.StopContentFilter || info.Refusal != "I can't help with that." || info.Incomplete == nil || info.Incomplete.Extra["anthropic.refusal_category"] != "cyber" {
		t.Errorf("content filter: details = %+v", res.Details)
	}

	child = New(agentturn.Config{Name: "explore", Model: stoppedShort{reason: openresponses.IncompleteReasonMaxOutputTokens}})
	res, err = child.Execute(context.Background(), call)
	if err != nil || res.Output.Text != "Once upon" {
		t.Errorf("output limit: res = %q, err = %v", res.Output.Text, err)
	}
	info, ok = res.Details.(ChildInfo)
	if !ok || info.Cause != agentturn.StopMaxOutputTokens || info.Incomplete == nil || info.Incomplete.Reason != openresponses.IncompleteReasonMaxOutputTokens {
		t.Errorf("output limit: details = %+v", res.Details)
	}
}
