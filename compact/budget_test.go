package compact

import (
	"context"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// TestABudgetOfZeroFoldsOnlyWhenAsked pins #318's decision: a budget of
// 0, from WithBudget or SetBudget, folds nothing by budget however long
// the transcript, and a fold FoldNext asks for still folds, reported
// with the budget it was made under.
func TestABudgetOfZeroFoldsOnlyWhenAsked(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
		set  bool
	}{
		{"WithBudget(0)", []Option{WithBudget(0)}, false},
		{"SetBudget(0)", []Option{WithBudget(3)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &summarizer{reply: "They talked about things."}
			var folds []Fold
			opts := append(tc.opts, WithKeepLast(1), WithEstimator(count), WithMinFold(0),
				WithOnFold(func(_ context.Context, f Fold) error { folds = append(folds, f); return nil }))
			tr := NewLocal(s, opts...)
			if tc.set {
				tr.SetBudget(0)
			}
			for _, n := range []int{4, 10, 40} {
				if out, err := tr.Transform(context.Background(), items(n)); err != nil || len(out) != n || len(s.reqs) != 0 {
					t.Fatalf("%d items: out = %d, err = %v, summaries = %d", n, len(out), err, len(s.reqs))
				}
			}
			tr.FoldNext("")
			out, err := tr.Transform(context.Background(), items(40))
			if err != nil || len(out) != 2 || len(s.reqs) != 1 {
				t.Fatalf("asked: out = %d, err = %v, summaries = %d", len(out), err, len(s.reqs))
			}
			if len(folds) != 1 || !folds[0].Requested || folds[0].Budget != 0 {
				t.Errorf("folds = %+v", folds)
			}
			// With no budget the summary is still capped.
			if got := s.reqs[0].MaxOutputTokens; got == nil || *got != DefaultSummaryMaxOutputTokens {
				t.Errorf("summary max_output_tokens = %v, want %d", got, DefaultSummaryMaxOutputTokens)
			}
		})
	}
}

// TestSetBudgetKeepsTheFoldMemory pins #318: a budget cut while the
// conversation goes on applies from the next call, which folds, and
// the transform still remembers its earlier fold, so the new fold
// folds the earlier summary rather than the prefix it stood for.
func TestSetBudgetKeepsTheFoldMemory(t *testing.T) {
	s := &summarizer{reply: "They talked about things."}
	var folds []Fold
	tr := NewLocal(s, WithBudget(100), WithKeepLast(1), WithEstimator(count), WithMinFold(0),
		WithOnFold(func(_ context.Context, f Fold) error { folds = append(folds, f); return nil }))
	tr.FoldNext("")
	if out, err := tr.Transform(context.Background(), items(6)); err != nil || len(out) != 2 || len(s.reqs) != 1 {
		t.Fatalf("first fold: out = %d, err = %v, summaries = %d", len(out), err, len(s.reqs))
	}
	// Within the budget: the remembered fold is sent, nothing asked.
	if out, err := tr.Transform(context.Background(), items(8)); err != nil || len(out) != 4 || len(s.reqs) != 1 {
		t.Fatalf("within: out = %d, err = %v, summaries = %d", len(out), err, len(s.reqs))
	}
	tr.SetBudget(2)
	out, err := tr.Transform(context.Background(), items(8))
	if err != nil || len(out) != 2 || len(s.reqs) != 2 {
		t.Fatalf("after the cut: out = %d, err = %v, summaries = %d", len(out), err, len(s.reqs))
	}
	// The second summary is asked of the first summary and the two items
	// after it, then the prompt, not of the five items the first summary
	// stood for.
	in := s.reqs[1].Input
	if len(in) != 4 || in[0] != folds[0].Summary {
		t.Errorf("second summary input = %d items, first %v, want the earlier summary first", len(in), in[0])
	}
	if len(folds) != 2 || folds[0].Budget != 100 || folds[1].Budget != 2 || folds[1].Forced {
		t.Errorf("folds = %+v", folds)
	}
}

// TestSetBudgetRecomputesADefaultedMinFold pins that NewLocal's default
// minimum fold follows the budget SetBudget gives, while one the caller
// set stays.
func TestSetBudgetRecomputesADefaultedMinFold(t *testing.T) {
	for _, tc := range []struct {
		name  string
		opts  []Option
		folds bool
	}{
		// An eighth of 800 is 100 items, which nine would not reach;
		// an eighth of 8 is 1, under the summary's floor of 2.
		{"defaulted", nil, true},
		{"set by the caller", []Option{WithMinFold(50)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &summarizer{reply: "gist"}
			tr := NewLocal(s, append([]Option{WithBudget(800), WithKeepLast(1), WithEstimator(count)}, tc.opts...)...)
			tr.SetBudget(8)
			if _, err := tr.Transform(context.Background(), items(10)); err != nil {
				t.Fatal(err)
			}
			if got := len(s.reqs) == 1; got != tc.folds {
				t.Errorf("folded = %v, want %v", got, tc.folds)
			}
		})
	}
}

// gate is a summary model that waits, once started, until released.
type gate struct {
	summarizer
	started chan struct{}
	release chan struct{}
}

func (g *gate) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	g.started <- struct{}{}
	<-g.release
	return g.summarizer.CreateStream(ctx, req, sink)
}

// TestSetBudgetDuringAFold pins that SetBudget is safe while a fold is
// in flight: the fold is made, and reported, under the budget it began
// with, its summary capped by that budget, and the next call uses the
// new one. Under the race detector it also checks nothing in the fold
// reads the budget without the lock.
func TestSetBudgetDuringAFold(t *testing.T) {
	g := &gate{summarizer: summarizer{reply: "gist"}, started: make(chan struct{}), release: make(chan struct{})}
	var mu sync.Mutex
	var folds []Fold
	tr := NewLocal(g, WithBudget(6), WithKeepLast(1), WithEstimator(count), WithMinFold(0),
		WithOnFold(func(_ context.Context, f Fold) error { mu.Lock(); folds = append(folds, f); mu.Unlock(); return nil }))
	done := make(chan error, 1)
	go func() {
		_, err := tr.Transform(context.Background(), items(10))
		done <- err
	}()
	<-g.started
	tr.SetBudget(1000)
	close(g.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(folds) != 1 || folds[0].Budget != 6 {
		t.Fatalf("folds = %+v, want one under budget 6", folds)
	}
	if got := folds[0].Request.MaxOutputTokens; got == nil || *got != 3 {
		t.Errorf("summary max_output_tokens = %v, want half the fold's budget", got)
	}
	// The next call is within the new budget: no fold.
	if out, err := tr.Transform(context.Background(), items(40)); err != nil || len(out) != 32 || len(folds) != 1 {
		t.Errorf("after: out = %d, err = %v, folds = %d", len(out), err, len(folds))
	}
}

// TestWithModelOfRequest pins #318's summary model: under
// WithModelOfRequest the summary is asked of the model, and under the
// reasoning, that the request being folded for names, so a model
// switched mid-session writes the summary; WithModel pins a model and
// wins over it, its reasoning not taken either; and the fold reports
// the model its summary was asked of.
func TestWithModelOfRequest(t *testing.T) {
	low := openresponses.ReasoningConfig{Effort: "low"}
	high := openresponses.ReasoningConfig{Effort: "high", Extra: map[string]any{"x.member": "v"}}
	on := func(model string, r openresponses.ReasoningConfig) context.Context {
		return agentturn.ContextWithRequest(context.Background(), openresponses.Request{Model: model, Reasoning: r})
	}
	for _, tc := range []struct {
		name   string
		opts   []Option
		ctxs   []context.Context
		models []string
		effort []string
	}{
		{"the request's model, switched", []Option{WithModelOfRequest()},
			[]context.Context{on("a", low), on("b", high)}, []string{"a", "b"}, []string{"low", "high"}},
		{"no request on the context", []Option{WithModelOfRequest()},
			[]context.Context{context.Background()}, []string{""}, []string{""}},
		{"WithModel wins", []Option{WithModelOfRequest(), WithModel("pinned")},
			[]context.Context{on("a", low)}, []string{"pinned"}, []string{""}},
		{"WithModel wins, given first", []Option{WithModel("pinned"), WithModelOfRequest()},
			[]context.Context{on("a", low)}, []string{"pinned"}, []string{""}},
		{"neither: the server's default", nil,
			[]context.Context{on("a", low)}, []string{""}, []string{""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &summarizer{reply: "gist"}
			var folds []Fold
			tr := NewLocal(s, append([]Option{WithBudget(3), WithKeepLast(1), WithEstimator(count), WithMinFold(0),
				WithOnFold(func(_ context.Context, f Fold) error { folds = append(folds, f); return nil })}, tc.opts...)...)
			for i, ctx := range tc.ctxs {
				// Each call grows the transcript past what the last fold
				// covers, so each folds.
				if _, err := tr.Transform(ctx, items(6+4*i)); err != nil {
					t.Fatal(err)
				}
				if len(s.reqs) != i+1 || len(folds) != i+1 {
					t.Fatalf("call %d: summaries = %d, folds = %d", i, len(s.reqs), len(folds))
				}
				req := s.reqs[i]
				if req.Model != tc.models[i] || string(req.Reasoning.Effort) != tc.effort[i] {
					t.Errorf("call %d: summary asked of %q under %+v, want %q %q", i, req.Model, req.Reasoning, tc.models[i], tc.effort[i])
				}
				if folds[i].Model != tc.models[i] {
					t.Errorf("call %d: fold model = %q, want %q", i, folds[i].Model, tc.models[i])
				}
			}
			if tc.name == "the request's model, switched" {
				// The reasoning is the summary request's own copy.
				if extra := s.reqs[1].Reasoning.Extra; extra == nil || extra["x.member"] != "v" {
					t.Errorf("summary reasoning extra = %v", extra)
				} else if extra["x.member"] = "changed"; high.Extra["x.member"] != "v" {
					t.Error("the summary request shares the turn's reasoning")
				}
			}
		})
	}
	// WithRequest still edits the summary request last, and the fold
	// reports the model it was asked of.
	s := &summarizer{reply: "gist"}
	var got Fold
	tr := NewLocal(s, WithModelOfRequest(), WithBudget(3), WithKeepLast(1), WithEstimator(count), WithMinFold(0),
		WithRequest(func(r *openresponses.Request) { r.Model = "edited" }),
		WithOnFold(func(_ context.Context, f Fold) error { got = f; return nil }))
	if _, err := tr.Transform(on("a", low), items(6)); err != nil {
		t.Fatal(err)
	}
	if s.reqs[0].Model != "edited" || got.Model != "edited" || s.reqs[0].Reasoning.Effort != "low" {
		t.Errorf("summary = %q %+v, fold model %q", s.reqs[0].Model, s.reqs[0].Reasoning, got.Model)
	}
}

// TestNewRecordsItsModel pins that New's fold reports the model its
// compaction request named, WithModel's or, under WithModelOfRequest,
// the request's, and the budget.
func TestNewRecordsItsModel(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
		want string
	}{
		{"WithModel", []Option{WithModel("m")}, "m"},
		{"WithModelOfRequest", []Option{WithModelOfRequest()}, "a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sent string
			probe := &probeCompactor{fn: func(req openresponses.CompactRequest) { sent = req.Model }}
			var folds []Fold
			tr := New(probe, append([]Option{WithBudget(3), WithKeepLast(1), WithEstimator(count),
				WithOnFold(func(_ context.Context, f Fold) error { folds = append(folds, f); return nil })}, tc.opts...)...)
			ctx := agentturn.ContextWithRequest(context.Background(), openresponses.Request{Model: "a"})
			if _, err := tr.Transform(ctx, items(6)); err != nil {
				t.Fatal(err)
			}
			if sent != tc.want || len(folds) != 1 || folds[0].Model != tc.want || folds[0].Budget != 3 {
				t.Errorf("sent %q, folds = %+v", sent, folds)
			}
		})
	}
}

// TestANegativeBudgetIsNone pins that a budget below 0, from WithBudget
// or SetBudget, is no budget and is reported as 0, as the docs of
// Fold.Budget and the record's budget say: nothing folds by size, and a
// fold asked for carries budget 0.
func TestANegativeBudgetIsNone(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
		set  bool
	}{
		{"WithBudget(-5)", []Option{WithBudget(-5)}, false},
		{"SetBudget(-5)", []Option{WithBudget(3)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &summarizer{reply: "gist"}
			var folds []Fold
			tr := NewLocal(s, append(tc.opts, WithKeepLast(1), WithEstimator(count), WithMinFold(0),
				WithOnFold(func(_ context.Context, f Fold) error { folds = append(folds, f); return nil }))...)
			if tc.set {
				tr.SetBudget(-5)
			}
			if out, err := tr.Transform(context.Background(), items(10)); err != nil || len(out) != 10 || len(s.reqs) != 0 {
				t.Fatalf("by size: out = %d, err = %v, summaries = %d", len(out), err, len(s.reqs))
			}
			tr.FoldNext("")
			if _, err := tr.Transform(context.Background(), items(10)); err != nil {
				t.Fatal(err)
			}
			if len(folds) != 1 || folds[0].Budget != 0 {
				t.Errorf("folds = %+v, want one with budget 0", folds)
			}
		})
	}
}
