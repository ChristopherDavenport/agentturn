package compact

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestAFoldKeepsThePersonsOwnMessages is #298: two folds over a
// conversation of several prompts keep each message the person typed
// verbatim and list them as pinned, while a paste over the bound and a
// message the host composed, which carries only a source of its own,
// are folded like any other item.
func TestAFoldKeepsThePersonsOwnMessages(t *testing.T) {
	s := &summarizer{reply: "They talked about things."}
	tr := NewLocal(s, WithBudget(3), WithKeepLast(1), WithEstimator(count), WithPersonBound(200))
	var folds []Fold
	WithOnFold(func(_ context.Context, f Fold) error {
		folds = append(folds, f)
		return nil
	})(tr)
	var reqs []openresponses.Request
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: tr.Transform})
	a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		if ts, ok := ev.(*agentturn.TurnStart); ok {
			reqs = append(reqs, ts.Request)
		}
		return nil
	})
	person := agentturn.ContextWithTrigger(context.Background(), agentturn.Trigger{Kind: agentturn.TriggerHuman})

	ask1 := openresponses.UserText("Please rename foo to bar.")
	paste := openresponses.UserText(strings.Repeat("log line\n", 100))
	reminder, err := agentturn.WithProvenance(openresponses.UserText("Remember: tests first."), agentturn.Provenance{Source: agentturn.Trigger{Kind: "reminder"}})
	if err != nil {
		t.Fatal(err)
	}
	ask2 := openresponses.UserText("Now add a test.")
	ask3 := openresponses.UserText("And document it.")
	for _, prompt := range []openresponses.Items{{ask1}, {paste}, {reminder, ask2}, {ask3}} {
		if _, err := a.Prompt(person, prompt...); err != nil {
			t.Fatal(err)
		}
	}
	if len(folds) != 2 {
		t.Fatalf("folds = %d, want 2", len(folds))
	}
	for i, want := range []openresponses.Items{{ask1}, {ask1, ask2}} {
		if !slices.Equal(folds[i].Pinned, want) {
			t.Errorf("fold %d pinned %v, want the person's messages %v", i+1, texts(folds[i].Pinned), texts(want))
		}
	}
	last := reqs[len(reqs)-1].Input
	for _, want := range []openresponses.Item{ask1, ask2, ask3} {
		if !slices.Contains(last, want) {
			t.Errorf("the last request lost %q verbatim: %v", texts(openresponses.Items{want}), texts(last))
		}
	}
	base, _ := agentturn.Unhide(reminder)
	for _, folded := range []openresponses.Item{paste, base} {
		if slices.Contains(last, folded) {
			t.Errorf("the last request kept %.30q, which a fold summarises", texts(openresponses.Items{folded}))
		}
	}
}

// texts is the text of each message of items, for a failure message.
func texts(items openresponses.Items) []string {
	var out []string
	for _, item := range items {
		if m, ok := item.(*openresponses.Message); ok {
			out = append(out, m.Text())
		} else {
			out = append(out, item.ItemType())
		}
	}
	return out
}
