package a2a

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	fronta2a "github.com/ChristopherDavenport/agentturn/front/a2a"
	"github.com/ChristopherDavenport/openresponses"
)

// endsIncomplete answers with a message ended incomplete for reason:
// a refusal for content_filter, cut text for max_output_tokens.
type endsIncomplete struct {
	reason openresponses.IncompleteReason
}

func (m endsIncomplete) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
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

// TestARemoteIncompleteEndReachesTheParent pins #314 across A2A: a
// remote agent's answer cut at its output limit reaches the parent's
// model marked as cut, from front/a2a's MetaIncomplete, as tools/agent
// marks a local child's; and a remote content filter's rejection
// carries the refusal text from the task's artifacts, not only the
// fixed status text.
func TestARemoteIncompleteEndReachesTheParent(t *testing.T) {
	call := agenttool.Call{ID: "c", Args: json.RawMessage(`{"input":"go"}`)}
	for _, mode := range []string{"streaming", "blocking"} {
		t.Run(mode, func(t *testing.T) {
			var opts []Option
			if mode == "blocking" {
				opts = append(opts, WithBlocking())
			}
			cut := agentturn.Config{Model: endsIncomplete{reason: openresponses.IncompleteReasonMaxOutputTokens}, ModelName: "remote"}
			client, card := serve(t, fronta2a.New(cut), nil)
			res, err := New(client, card, opts...).Execute(context.Background(), call)
			const want = "Once upon\n\n[the agent's answer was cut short at its model's output limit (max_output_tokens)]"
			if err != nil || res.Output.Text != want {
				t.Errorf("cut: res = %q, err = %v, want %q", res.Output.Text, err, want)
			}

			filtered := agentturn.Config{Model: endsIncomplete{reason: openresponses.IncompleteReasonContentFilter}, ModelName: "remote"}
			client, card = serve(t, fronta2a.New(filtered), nil)
			_, err = New(client, card, opts...).Execute(context.Background(), call)
			const refused = "the agent rejected the task: " + fronta2a.FilteredText + ": I can't help with that."
			if err == nil || err.Error() != refused {
				t.Errorf("content filter: err = %v, want %q", err, refused)
			}
		})
	}
}
