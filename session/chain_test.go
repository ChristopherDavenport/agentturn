package session

import (
	"context"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/compact"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestFoldAfterAProductTransform checks the recorder half of #115: a
// fold chained after a transform that drops items names the entry of
// the item it kept, not the entry at its index into the shaped
// transcript, and one the recorder cannot place, after a transform
// that replaces items or when the kept item was appended more than
// once, is written as unplaced while the run goes on.
func TestFoldAfterAProductTransform(t *testing.T) {
	dropFirst := func(_ context.Context, in agentturn.Transcript) (agentturn.Transcript, error) {
		if len(in) > 1 {
			return in[1:], nil
		}
		return in, nil
	}
	replaceAll := func(_ context.Context, in agentturn.Transcript) (agentturn.Transcript, error) {
		return openresponses.Items(in).Clone(), nil
	}
	static := openresponses.DeveloperText("reminder")
	for _, tc := range []struct {
		name     string
		product  func(context.Context, agentturn.Transcript) (agentturn.Transcript, error)
		repeated bool
		keep     int
		unplaced bool
	}{
		{"dropping items", dropFirst, false, 2, false},
		{"replacing items", replaceAll, false, 2, true},
		// The fold keeps only the last item, the one BeforeTurn appends
		// every turn, which the path holds three times.
		{"one item appended every turn", dropFirst, true, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			var folded compact.Fold
			tr := compact.NewLocal(&echo.Adapter{}, compact.WithBudget(3), compact.WithKeepLast(tc.keep), compact.WithEstimator(countItems),
				compact.WithOnFold(func(ctx context.Context, f compact.Fold) error {
					folded = f
					return rec.Fold(ctx, f)
				}))
			cfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: agentturn.ChainTransform(tc.product, tr.Transform)}
			if tc.repeated {
				cfg.BeforeTurn = func(context.Context, agentturn.TurnStartInfo) (openresponses.Items, error) {
					return openresponses.Items{static}, nil
				}
			}
			a := agentturn.New(cfg)
			defer rec.Attach(a)()
			for _, text := range []string{"one", "two", "three"} {
				if _, err := a.Prompt(context.Background(), openresponses.UserText(text)); err != nil {
					t.Fatal(err)
				}
			}
			if tc.unplaced {
				if len(customs(s, UnplacedFoldNS)) == 0 {
					t.Errorf("no unplaced fold in %q", entryTypes(s))
				}
				for _, e := range s.Entries() {
					if _, ok := e.(*agentsession.CompactionEntry); ok {
						t.Errorf("a compaction the recorder could not place: %q", entryTypes(s))
					}
				}
				return
			}
			var entry *agentsession.CompactionEntry
			for _, e := range s.Entries() {
				if c, ok := e.(*agentsession.CompactionEntry); ok {
					entry = c
				}
			}
			if entry == nil || folded.First == nil {
				t.Fatalf("no fold recorded in %q", entryTypes(s))
			}
			kept, _ := s.Entry(entry.FirstKept)
			if it, ok := kept.(*agentsession.ItemEntry); !ok || it.Item != folded.First {
				t.Errorf("first_kept holds %+v, the fold kept %+v", kept, folded.First)
			}
		})
	}
}
