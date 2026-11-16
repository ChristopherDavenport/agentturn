package session

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/compact"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// foldCalls returns the fold member of each compaction entry in s and
// the data of each failed fold entry, decoded.
func foldCalls(t *testing.T, s *agentsession.Session) ([]FoldCall, []FailedFold) {
	t.Helper()
	var applied []FoldCall
	var failed []FailedFold
	for _, e := range s.Entries() {
		switch v := e.(type) {
		case *agentsession.CompactionEntry:
			var call FoldCall
			raw, ok := v.Unknown[FoldMember]
			if !ok {
				t.Fatalf("compaction has no %q member: %+v", FoldMember, v.Unknown)
			}
			if err := json.Unmarshal(raw, &call); err != nil {
				t.Fatal(err)
			}
			applied = append(applied, call)
		case *agentsession.CustomEntry:
			if v.NS != FailedFoldNS {
				continue
			}
			var f FailedFold
			if err := json.Unmarshal(v.Data, &f); err != nil {
				t.Fatal(err)
			}
			failed = append(failed, f)
		}
	}
	return applied, failed
}

// TestAFoldRecordsItsBudgetAndModel pins #318's record: each fold, and
// each failed fold, carries the budget it was made under and the model
// its summary was asked of, read back from the entry; a budget changed
// mid-session is the next fold's, a budget of 0 is written as 0, and
// New's compaction call records its model as NewLocal's does.
func TestAFoldRecordsItsBudgetAndModel(t *testing.T) {
	prompts := []string{"one", "two", "three", "four", "five"}
	budget := func(c FoldCall) int {
		if c.Budget == nil {
			return -1
		}
		return *c.Budget
	}

	t.Run("the request's model, a budget cut mid-session", func(t *testing.T) {
		var tr *compact.Transform
		s := runWith(t, func(onFold compact.Option) *compact.Transform {
			tr = compact.NewLocal(&echo.Adapter{}, compact.WithModelOfRequest(), compact.WithBudget(4), compact.WithKeepLast(2), compact.WithEstimator(countItems), onFold)
			return tr
		}, prompts, func(i int) {
			if i == 4 {
				tr.SetBudget(3)
			}
		})
		applied, _ := foldCalls(t, s)
		if len(applied) < 2 {
			t.Fatalf("folds = %d, want two or more", len(applied))
		}
		first, last := applied[0], applied[len(applied)-1]
		if first.Model != "m" || budget(first) != 4 || first.RequestHash == "" {
			t.Errorf("first fold call = %+v budget %d", first, budget(first))
		}
		if last.Model != "m" || budget(last) != 3 {
			t.Errorf("last fold call = %+v budget %d, want the cut budget", last, budget(last))
		}
	})

	t.Run("New's model", func(t *testing.T) {
		s := runWith(t, func(onFold compact.Option) *compact.Transform {
			return compact.New(&echo.Adapter{}, compact.WithModel("compactor"), compact.WithBudget(4), compact.WithKeepLast(2), compact.WithEstimator(countItems), onFold)
		}, prompts, nil)
		applied, _ := foldCalls(t, s)
		if len(applied) == 0 || applied[0].Model != "compactor" || budget(applied[0]) != 4 || applied[0].RequestHash != "" {
			t.Errorf("fold calls = %+v", applied)
		}
	})

	t.Run("no budget, a fold asked for", func(t *testing.T) {
		var tr *compact.Transform
		s := runWith(t, func(onFold compact.Option) *compact.Transform {
			tr = compact.NewLocal(&echo.Adapter{}, compact.WithModelOfRequest(), compact.WithBudget(0), compact.WithKeepLast(2), compact.WithEstimator(countItems), onFold)
			return tr
		}, prompts, func(i int) {
			if i == 3 {
				tr.FoldNext("")
			}
		})
		applied, _ := foldCalls(t, s)
		if len(applied) != 1 || budget(applied[0]) != 0 || applied[0].Model != "m" {
			t.Errorf("fold calls = %+v, want one under budget 0", applied)
		}
	})

	t.Run("a failed fold", func(t *testing.T) {
		s := runWith(t, func(onFold compact.Option) *compact.Transform {
			return compact.NewLocal(callingFold{}, compact.WithModelOfRequest(), compact.WithBudget(40), compact.WithKeepLast(2), onFold)
		}, prompts[:3], nil)
		_, failed := foldCalls(t, s)
		if len(failed) != 1 || failed[0].Model != "m" || budget(failed[0].FoldCall) != 40 {
			t.Errorf("failed folds = %+v", failed)
		}
	})
}

// runWith runs prompts through an agent whose transform build makes,
// given the option that records its folds, calling between, when set,
// before each prompt with its index, and returns the verified session.
func runWith(t *testing.T, build func(onFold compact.Option) *compact.Transform, prompts []string, between func(i int)) *agentsession.Session {
	t.Helper()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	tr := build(compact.WithOnFold(rec.Fold))
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: tr.Transform})
	defer rec.Attach(a)()
	for i, text := range prompts {
		if between != nil {
			between(i)
		}
		if end, err := a.Prompt(context.Background(), openresponses.UserText(text)); err != nil || end.Reason != agentturn.ReasonDone {
			t.Fatalf("%s: err=%v end=%+v", text, err, end)
		}
	}
	verifyAll(t, s)
	return s
}
