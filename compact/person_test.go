package compact

import (
	"context"
	"fmt"
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
	tr := NewLocal(s, WithBudget(4), WithKeepLast(1), WithEstimator(count), WithPersonBound(200), WithPersonTotal(2))
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

// TestThePersonsKeptMessagesHaveATotal is the l6-3c review's M1: many
// small messages a person typed, across several folds, are kept up to
// a total, a quarter of the budget by default, the newest of them,
// while the older ones fold into the summary. Without the total the
// kept messages grow with every fold until the request no longer fits:
// the overflow retry folds with the same pins and fails again. A total
// a host sets at the budget or over it counts as half the budget (l6-3c
// round 2): kept messages that fill the request alone bring the refold
// loop back.
func TestThePersonsKeptMessagesHaveATotal(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
		most int
	}{
		{"default, a quarter of the budget", nil, 2},
		{"a total of the budget", []Option{WithPersonTotal(8)}, 4},
		{"a total over the budget", []Option{WithPersonTotal(100)}, 4},
	} {
		t.Run(tc.name, func(t *testing.T) { personTotalRun(t, tc.opts, tc.most) })
	}
}

func personTotalRun(t *testing.T, opts []Option, most int) {
	t.Helper()
	s := &summarizer{reply: "They talked about things."}
	tr := NewLocal(s, append([]Option{WithBudget(8), WithKeepLast(1), WithEstimator(count)}, opts...)...)
	var folds []Fold
	WithOnFold(func(_ context.Context, f Fold) error {
		folds = append(folds, f)
		return nil
	})(tr)
	m := &overflowing{limit: 8}
	var reqs []openresponses.Request
	a := agentturn.New(agentturn.Config{Model: m, ModelName: "m", Transform: tr.Transform})
	a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		if ts, ok := ev.(*agentturn.TurnStart); ok {
			reqs = append(reqs, ts.Request)
		}
		return nil
	})
	person := agentturn.ContextWithTrigger(context.Background(), agentturn.Trigger{Kind: agentturn.TriggerHuman})
	var asked openresponses.Items
	for i := range 12 {
		ask := openresponses.UserText(fmt.Sprintf("Step %d, please.", i))
		asked = append(asked, ask)
		end, err := a.Prompt(person, ask)
		if err != nil {
			t.Fatalf("prompt %d: %v", i, err)
		}
		if end.Reason != agentturn.ReasonDone {
			t.Fatalf("prompt %d ended %s (%v): the kept messages outgrew the request", i, end.Reason, end.Err)
		}
	}
	if len(folds) < 3 {
		t.Fatalf("folds = %d, want several", len(folds))
	}
	for i, f := range folds {
		if len(f.Pinned) > most {
			t.Errorf("fold %d pinned %d messages, want at most %d: %v", i+1, len(f.Pinned), most, texts(f.Pinned))
		}
		if len(f.Pinned) == 0 {
			t.Errorf("fold %d pinned nothing", i+1)
			continue
		}
		// The newest of them: a run of the person's messages with no
		// gap, so an older one folded and a newer one did not.
		first := slices.Index(asked, f.Pinned[0])
		if first < 0 || first+len(f.Pinned) > len(asked) || !slices.Equal(f.Pinned, asked[first:first+len(f.Pinned)]) {
			t.Errorf("fold %d pinned %v, want a run of the newest", i+1, texts(f.Pinned))
		}
		// Each prompt is one request, whose kept tail is the prompt
		// itself, so the folded part's newest message is the one before.
		if k := slices.Index(asked, f.First); k < 1 || f.Pinned[len(f.Pinned)-1] != asked[k-1] {
			t.Errorf("fold %d kept %v before %v, want the newest of the folded part", i+1, texts(f.Pinned), texts(openresponses.Items{f.First}))
		}
	}
	last := reqs[len(reqs)-1].Input
	if len(last) > m.limit {
		t.Errorf("the last request holds %d items, over the model's %d", len(last), m.limit)
	}
	if slices.Contains(last, asked[0]) {
		t.Errorf("the last request still kept the first message, which the total folds: %v", texts(last))
	}
}
