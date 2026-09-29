package session

import (
	"context"
	"strings"
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
// transcript, and one after a transform that replaces items is refused
// rather than recorded against the wrong entry.
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
	for _, tc := range []struct {
		name    string
		product func(context.Context, agentturn.Transcript) (agentturn.Transcript, error)
		refused bool
	}{
		{"dropping items", dropFirst, false},
		{"replacing items", replaceAll, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			var folded compact.Fold
			tr := compact.NewLocal(&echo.Adapter{}, compact.WithBudget(3), compact.WithKeepLast(2), compact.WithEstimator(countItems),
				compact.WithOnFold(func(ctx context.Context, f compact.Fold) error {
					folded = f
					return rec.Fold(ctx, f)
				}))
			a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: agentturn.ChainTransform(tc.product, tr.Transform)})
			defer rec.Attach(a)()
			var perr error
			for _, text := range []string{"one", "two", "three"} {
				if _, perr = a.Prompt(context.Background(), openresponses.UserText(text)); perr != nil {
					break
				}
			}
			if tc.refused {
				if perr == nil || !strings.Contains(perr.Error(), "did not write") {
					t.Fatalf("a fold over replaced items: err = %v", perr)
				}
				return
			}
			if perr != nil {
				t.Fatal(perr)
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
