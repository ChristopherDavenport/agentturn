package responses

import (
	"strings"

	"github.com/ChristopherDavenport/openresponses"
)

// relay re-emits items that arrive as loop or wire events through an
// Emitter, so the outgoing stream is a well-ordered Open Responses
// stream of its own: fresh item IDs, fresh indices, fresh bookends. It
// streams message text, refusals, function call arguments and reasoning
// as they arrive and catches up at item end with whatever the final item
// carries that was not streamed, so an upstream that only sends
// output_item.added and output_item.done still produces deltas here.
type relay struct {
	em *openresponses.Emitter

	msg       *openresponses.MessageWriter
	call      *openresponses.FunctionCallWriter
	reasoning *openresponses.ReasoningWriter

	text    strings.Builder // output_text streamed for the open message
	refusal strings.Builder // refusal streamed for the open message
	args    strings.Builder // arguments streamed for the open call
	summary int             // summary parts streamed for the open reasoning item
	rtext   strings.Builder // reasoning text streamed
}

func newRelay(sink openresponses.EventSink, resp *openresponses.Response) *relay {
	return &relay{em: openresponses.NewEmitter(sink, resp)}
}

// start opens a writer for a streamed item. Items that are never
// streamed (function call outputs, compactions, extensions) wait for end.
func (r *relay) start(item openresponses.Item) error {
	r.reset()
	switch v := item.(type) {
	case *openresponses.Message:
		if v.Role != openresponses.RoleAssistant {
			return nil
		}
		w, err := r.em.Message(v.Phase)
		if err != nil {
			return err
		}
		r.msg = w
	case *openresponses.FunctionCall:
		w, err := r.em.FunctionCall(v.CallID, v.Name)
		if err != nil {
			return err
		}
		r.call = w
	case *openresponses.ReasoningItem:
		w, err := r.em.Reasoning()
		if err != nil {
			return err
		}
		r.reasoning = w
	}
	return nil
}

func (r *relay) reset() {
	r.msg, r.call, r.reasoning = nil, nil, nil
	r.text.Reset()
	r.refusal.Reset()
	r.args.Reset()
	r.rtext.Reset()
	r.summary = 0
}

// update forwards one wire event for the open item.
func (r *relay) update(ev openresponses.StreamEvent) error {
	switch e := ev.(type) {
	case *openresponses.OutputTextDeltaEvent:
		if r.msg != nil {
			r.text.WriteString(e.Delta)
			return r.msg.Text(e.Delta)
		}
	case *openresponses.OutputTextAnnotationAddedEvent:
		if r.msg != nil && e.Annotation != nil {
			return r.msg.Annotation(e.Annotation)
		}
	case *openresponses.RefusalDeltaEvent:
		if r.msg != nil {
			r.refusal.WriteString(e.Delta)
			return r.msg.Refusal(e.Delta)
		}
	case *openresponses.FunctionCallArgumentsDeltaEvent:
		if r.call != nil {
			r.args.WriteString(e.Delta)
			return r.call.Arguments(e.Delta)
		}
	case *openresponses.ReasoningSummaryTextDeltaEvent:
		if r.reasoning != nil {
			return r.reasoning.Summary(e.Delta)
		}
	case *openresponses.ReasoningSummaryPartDoneEvent:
		if r.reasoning != nil {
			r.summary++
			return r.reasoning.EndSummary()
		}
	case *openresponses.ReasoningDeltaEvent:
		if r.reasoning != nil {
			r.rtext.WriteString(e.Delta)
			return r.reasoning.Text(e.Delta)
		}
	}
	return nil
}

// end closes the open writer, catching up with the final item, or emits
// an item that was never streamed whole.
func (r *relay) end(item openresponses.Item) error {
	defer r.reset()
	switch v := item.(type) {
	case *openresponses.Message:
		if r.msg == nil {
			if v.Role != openresponses.RoleAssistant {
				return r.em.Item(clone(item))
			}
			w, err := r.em.Message(v.Phase)
			if err != nil {
				return err
			}
			r.msg = w
		}
		text, refusal := messageText(v)
		if rest, ok := strings.CutPrefix(text, r.text.String()); ok && rest != "" {
			if err := r.msg.Text(rest); err != nil {
				return err
			}
		}
		if rest, ok := strings.CutPrefix(refusal, r.refusal.String()); ok && rest != "" {
			if err := r.msg.Refusal(rest); err != nil {
				return err
			}
		}
		if v.Status == openresponses.StatusIncomplete {
			r.msg.Item().Status = openresponses.StatusIncomplete
		}
		return r.msg.Close()
	case *openresponses.FunctionCall:
		if r.call == nil {
			w, err := r.em.FunctionCall(v.CallID, v.Name)
			if err != nil {
				return err
			}
			r.call = w
		}
		if rest, ok := strings.CutPrefix(v.Arguments, r.args.String()); ok && rest != "" {
			if err := r.call.Arguments(rest); err != nil {
				return err
			}
		}
		if v.Status == openresponses.StatusIncomplete {
			r.call.Item().Status = openresponses.StatusIncomplete
		}
		return r.call.Close()
	case *openresponses.ReasoningItem:
		if r.reasoning == nil {
			w, err := r.em.Reasoning()
			if err != nil {
				return err
			}
			r.reasoning = w
		}
		for i := r.summary; i < len(v.Summary); i++ {
			if err := r.reasoning.EndSummary(); err != nil {
				return err
			}
			if err := r.reasoning.Summary(partText(v.Summary[i])); err != nil {
				return err
			}
		}
		if rest, ok := strings.CutPrefix(v.Content.Text(), r.rtext.String()); ok && rest != "" {
			if err := r.reasoning.Text(rest); err != nil {
				return err
			}
		}
		r.reasoning.EncryptedContent(v.EncryptedContent)
		return r.reasoning.Close()
	default:
		return r.em.Item(clone(item))
	}
}

func messageText(m *openresponses.Message) (text, refusal string) {
	var t, rf strings.Builder
	for _, part := range m.Content {
		switch p := part.(type) {
		case *openresponses.OutputText:
			t.WriteString(p.Text)
		case *openresponses.Refusal:
			rf.WriteString(p.Refusal)
		}
	}
	return t.String(), rf.String()
}

func partText(c openresponses.Content) string {
	return openresponses.Contents{c}.Text()
}

// clone copies an item through its wire form so the emitter's status
// promotion never touches the loop's transcript.
func clone(item openresponses.Item) openresponses.Item {
	return openresponses.Items{item}.Clone()[0]
}
