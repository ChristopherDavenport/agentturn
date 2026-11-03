package agentturn

import (
	"context"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// wordByWord streams its answer a word at a time, and, while no tool
// has answered, a call whose arguments arrive in two pieces first.
type wordByWord struct{}

func (wordByWord) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if _, ok := req.Input[len(req.Input)-1].(*openresponses.FunctionCallOutput); !ok && len(req.Tools) > 0 {
		w, err := em.FunctionCall("", "upper")
		if err != nil {
			return err
		}
		for _, piece := range []string{`{"text":`, `"t"}`} {
			if err := w.Arguments(piece); err != nil {
				return err
			}
		}
		if err := w.Close(); err != nil {
			return err
		}
		return em.Complete()
	}
	w, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	for _, word := range []string{"alpha ", "beta ", "gamma ", "delta ", "epsilon"} {
		if err := w.Text(word); err != nil {
			return err
		}
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Complete()
}

// TestAnItemEventIsAFactAsOfTheEvent pins RFC 0001's Events (#218): the
// item an item_start or item_update carries is the item as accumulated
// at that event, and a consumer that keeps the event, as a view's render
// queue does, finds the same item in it after the run.
func TestAnItemEventIsAFactAsOfTheEvent(t *testing.T) {
	ag := New(Config{Model: wordByWord{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)}})
	type kept struct {
		ev   Event
		item openresponses.Item
		at   string
	}
	var queue []kept
	render := func(item openresponses.Item) string {
		switch v := item.(type) {
		case *openresponses.Message:
			return v.Text()
		case *openresponses.FunctionCall:
			return v.Arguments
		}
		return ""
	}
	ag.Subscribe(func(_ context.Context, ev Event) error {
		switch e := ev.(type) {
		case *ItemStart:
			queue = append(queue, kept{ev: e, item: e.Item, at: render(e.Item)})
		case *ItemUpdate:
			queue = append(queue, kept{ev: e, item: e.Item, at: render(e.Item)})
		}
		return nil
	})
	if _, err := ag.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	changed := 0
	for _, k := range queue {
		if now := render(k.item); now != k.at {
			changed++
			t.Logf("%s: %q at delivery, %q when drawn", k.ev.EventType(), k.at, now)
		}
	}
	if changed > 0 {
		t.Errorf("%d of %d kept item events changed after delivery", changed, len(queue))
	}
}
