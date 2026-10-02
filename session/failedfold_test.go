package session

import (
	"context"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentsession/jsonl"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/compact"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestFailedFoldSurvivesARestart pins #194 and agenteval#48: the
// back-off after a failed fold is on the record, and a transform seeded
// from a resumed session, through CompactOptions of the session or of
// the recorder Resume seeded from it, does not ask again for the
// summary that failed.
func TestFailedFoldSurvivesARestart(t *testing.T) {
	cases := []struct {
		name string
		// seed is what seeds the transform after the restart: the
		// session's CompactOptions, the recorder's, or nothing.
		seed      string
		summaries int // summary calls after the restart
	}{
		{"seeded from the session", "session", 0},
		{"seeded from the recorder", "recorder", 0},
		{"not seeded", "", 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A store on disk, opened again for the restart, so the
			// transcript the second transform is given is the one the
			// record rebuilds.
			root := t.TempDir()
			store, err := jsonl.Open(root)
			if err != nil {
				t.Fatal(err)
			}
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			over := func(items openresponses.Items) int { return 100 + len(items) }
			opts := []compact.Option{compact.WithBudget(100), compact.WithKeepLast(3), compact.WithEstimator(over), compact.WithMinFold(0)}
			if f, err := LastFailedFold(s); err != nil || f != nil {
				t.Fatalf("before any fold: %+v, %v", f, err)
			}
			if opts := rec.CompactOptions(); opts != nil {
				t.Fatalf("a fresh session's recorder seeds %d options", len(opts))
			}
			model := &reasoningFold{}
			tr := compact.NewLocal(model, append(opts, compact.WithOnFold(rec.Fold))...)
			a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: tr.Transform})
			detach := rec.Attach(a)
			for _, text := range []string{"one", "two", "three"} {
				if end, err := a.Prompt(context.Background(), openresponses.UserText(text)); err != nil || end.Reason != agentturn.ReasonDone {
					t.Fatalf("%s: err=%v end=%+v", text, err, end)
				}
			}
			detach()
			if model.calls != 2 {
				t.Fatalf("summary calls before the restart = %d", model.calls)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}

			// Another process: the store opened again, the session
			// resumed, a fresh transform.
			store2, err := jsonl.Open(root)
			if err != nil {
				t.Fatal(err)
			}
			defer store2.Close()
			rec2, s2, err := Resume(context.Background(), store2, s.ID())
			if err != nil {
				t.Fatal(err)
			}
			f, err := LastFailedFold(s2)
			if err != nil || f == nil || f.Split != 2 || f.PrefixHash == "" || f.TokensBefore != 105 {
				t.Fatalf("last failed fold = %+v, %v", f, err)
			}
			aopts, err := AgentOptions(s2)
			if err != nil {
				t.Fatal(err)
			}
			switch tc.seed {
			case "session":
				copts, err := CompactOptions(s2)
				if err != nil || len(copts) != 1 {
					t.Fatalf("compact options = %d, %v", len(copts), err)
				}
				opts = append(opts, copts...)
			case "recorder":
				copts := rec2.CompactOptions()
				if len(copts) != 1 {
					t.Fatalf("the recorder's compact options = %d", len(copts))
				}
				opts = append(opts, copts...)
			}
			model2 := &reasoningFold{}
			tr2 := compact.NewLocal(model2, append(opts, compact.WithOnFold(rec2.Fold))...)
			a2 := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: tr2.Transform}, aopts...)
			defer rec2.Attach(a2)()
			if end, err := a2.Prompt(context.Background(), openresponses.UserText("four")); err != nil || end.Reason != agentturn.ReasonDone {
				t.Fatalf("four: err=%v end=%+v", err, end)
			}
			if model2.calls != tc.summaries {
				t.Errorf("summary calls after the restart = %d, want %d", model2.calls, tc.summaries)
			}
		})
	}
}
