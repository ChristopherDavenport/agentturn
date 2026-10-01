package compact

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

type counting struct {
	Compactor
	calls atomic.Int32
	err   error
}

func (c *counting) Compact(ctx context.Context, req openresponses.CompactRequest) (*openresponses.CompactResponse, error) {
	c.calls.Add(1)
	if c.err != nil {
		return nil, c.err
	}
	return c.Compactor.Compact(ctx, req)
}

func items(n int) agentturn.Transcript {
	var out agentturn.Transcript
	for i := 0; i < n; i++ {
		if i%2 == 0 {
			out = append(out, openresponses.UserText(strings.Repeat("user ", 10)))
		} else {
			out = append(out, openresponses.AssistantText(strings.Repeat("assistant ", 10)))
		}
	}
	return out
}

func count(items openresponses.Items) int { return len(items) }

type echoArgs struct {
	Text string `json:"text"`
}

func firstRequests(t *testing.T, seq func(func(agentturn.Event) bool)) ([]openresponses.Request, *agentturn.RunEnd, error) {
	t.Helper()
	var reqs []openresponses.Request
	var end *agentturn.RunEnd
	for ev := range seq {
		switch e := ev.(type) {
		case *agentturn.TurnStart:
			reqs = append(reqs, e.Request)
		case *agentturn.RunEnd:
			end = e
		}
	}
	if end == nil {
		t.Fatal("no run_end")
	}
	return reqs, end, end.Err
}

func TestTransformThroughRun(t *testing.T) {
	cases := []struct {
		name       string
		budget     int
		keepLast   int
		history    int
		tools      bool
		wantCalls  int32
		wantFirst  string // item type at input[0] of the first request
		wantInputs []int  // request input lengths per turn
	}{
		{name: "under budget", budget: 100, keepLast: 2, history: 4, wantCalls: 0, wantFirst: "message", wantInputs: []int{5}},
		{name: "over budget", budget: 4, keepLast: 2, history: 4, wantCalls: 1, wantFirst: "compaction", wantInputs: []int{3}},
		{name: "reused across turns", budget: 5, keepLast: 1, history: 6, tools: true, wantCalls: 1, wantFirst: "compaction", wantInputs: []int{2, 4}},
		{name: "compacted again when the tail outgrows the budget", budget: 3, keepLast: 1, history: 6, tools: true, wantCalls: 2, wantFirst: "compaction", wantInputs: []int{2, 3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &counting{Compactor: &echo.Adapter{}}
			tr := New(c, WithBudget(tc.budget), WithKeepLast(tc.keepLast), WithEstimator(count), WithModel("m"))
			cfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: tr.Transform}
			if tc.tools {
				cfg.Tools = []agenttool.Tool{agenttool.New("upper", "", func(_ context.Context, a echoArgs) (string, error) { return strings.ToUpper(a.Text), nil })}
			}
			reqs, end, err := firstRequests(t, agentturn.Run(context.Background(), items(tc.history), openresponses.Items{openresponses.UserText("latest")}, cfg))
			if err != nil {
				t.Fatal(err)
			}
			if end.Reason != agentturn.ReasonDone {
				t.Fatalf("end = %+v", end)
			}
			if got := c.calls.Load(); got != tc.wantCalls {
				t.Errorf("compact calls = %d, want %d", got, tc.wantCalls)
			}
			if len(reqs) != len(tc.wantInputs) {
				t.Fatalf("turns = %d, want %d", len(reqs), len(tc.wantInputs))
			}
			for i, want := range tc.wantInputs {
				if len(reqs[i].Input) != want {
					t.Errorf("turn %d input = %d items, want %d", i+1, len(reqs[i].Input), want)
				}
			}
			if got := reqs[0].Input[0].ItemType(); got != tc.wantFirst {
				t.Errorf("first item = %s, want %s", got, tc.wantFirst)
			}
			// The model still answers: echo expands its own compaction
			// and sees the whole conversation.
			final := end.Items[len(end.Items)-1].(*openresponses.Message).Text()
			want := "latest"
			if tc.tools {
				want = "Tool result: LATEST"
			}
			if final != want {
				t.Errorf("final = %q, want %q", final, want)
			}
			if (tr.Last() != nil) != (tc.wantCalls > 0) {
				t.Errorf("Last = %v", tr.Last())
			}
			// The working transcript was never replaced.
			if len(end.Items) < 2 {
				t.Errorf("added = %d", len(end.Items))
			}
		})
	}
}

func TestTransformError(t *testing.T) {
	boom := errors.New("compaction down")
	c := &counting{Compactor: &echo.Adapter{}, err: boom}
	tr := New(c, WithBudget(1), WithKeepLast(1), WithEstimator(count))
	cfg := agentturn.Config{Model: &echo.Adapter{}, Transform: tr.Transform}
	_, end, err := firstRequests(t, agentturn.Run(context.Background(), items(4), openresponses.Items{openresponses.UserText("x")}, cfg))
	if !errors.Is(err, boom) || end.Reason != agentturn.ReasonError {
		t.Errorf("err = %v end = %+v", err, end)
	}
}

func TestSplitKeepsCallsWithOutputs(t *testing.T) {
	tr := New(&echo.Adapter{}, WithKeepLast(3))
	transcript := agentturn.Transcript{
		openresponses.UserText("u"),
		&openresponses.FunctionCall{CallID: "c", Name: "f", Arguments: "{}"},
		openresponses.NewFunctionCallOutput("c", "o"),
		openresponses.AssistantText("a"),
		openresponses.UserText("u2"),
	}
	if got := tr.split(transcript); got != 1 {
		t.Errorf("split = %d, want 1", got)
	}
	if got := New(&echo.Adapter{}, WithKeepLast(10)).split(transcript); got != 0 {
		t.Errorf("split with large keepLast = %d", got)
	}
	// Nothing older than the kept tail: no compaction, no call.
	c := &counting{Compactor: &echo.Adapter{}}
	out, err := New(c, WithKeepLast(10), WithBudget(0), WithEstimator(count)).Transform(context.Background(), transcript)
	if err != nil || len(out) != len(transcript) || c.calls.Load() != 0 {
		t.Errorf("out = %d err = %v calls = %d", len(out), err, c.calls.Load())
	}
}

func TestTransformForgetsAChangedPrefix(t *testing.T) {
	c := &counting{Compactor: &echo.Adapter{}}
	tr := New(c, WithBudget(3), WithKeepLast(1), WithEstimator(count))
	first := items(4)
	if _, err := tr.Transform(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	// Same prefix, one more item: reused.
	if out, err := tr.Transform(context.Background(), append(append(agentturn.Transcript(nil), first...), openresponses.UserText("more"))); err != nil || out[0].ItemType() != openresponses.ItemTypeCompaction {
		t.Errorf("out = %v err = %v", out, err)
	}
	// A different conversation: the memory is dropped and compacted anew.
	other := agentturn.Transcript{openresponses.UserText("a"), openresponses.AssistantText("b"), openresponses.UserText("c"), openresponses.AssistantText("d")}
	if _, err := tr.Transform(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	if c.calls.Load() != 2 {
		t.Errorf("calls = %d, want 2", c.calls.Load())
	}
	if Estimate(other) == 0 {
		t.Error("estimate of non-empty items is zero")
	}
	// App-only items are filtered out of what the server compacts.
	seen := 0
	probe := &probeCompactor{fn: func(req openresponses.CompactRequest) { seen = len(req.Input) }}
	withNote := agentturn.Transcript{openresponses.UserText("a"), &openresponses.UnknownItem{Type: "app:note"}, openresponses.AssistantText("b"), openresponses.UserText("c")}
	if _, err := New(probe, WithBudget(1), WithKeepLast(1), WithEstimator(count)).Transform(context.Background(), withNote); err != nil {
		t.Fatal(err)
	}
	if seen != 2 {
		t.Errorf("server saw %d items, want 2", seen)
	}
}

type probeCompactor struct {
	fn func(openresponses.CompactRequest)
}

func (p *probeCompactor) Compact(ctx context.Context, req openresponses.CompactRequest) (*openresponses.CompactResponse, error) {
	p.fn(req)
	return (&echo.Adapter{}).Compact(ctx, req)
}

// summarizer answers every request with a fixed summary and records
// what it was asked, except that it answers its first calls requests
// with a function call and no text, the bloat requests after those
// with the summary repeated a hundred times, and the cut requests
// after those with the summary ended incomplete at max_output_tokens.
// Each response reports the request's number as its output tokens.
type summarizer struct {
	reqs  []openresponses.Request
	reply string
	fail  bool
	calls int
	bloat int
	cut   int
}

func (s *summarizer) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	s.reqs = append(s.reqs, req)
	if s.fail {
		return openresponses.ServerError("down", "no summary today")
	}
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	em.Response().Usage = &openresponses.Usage{OutputTokens: len(s.reqs)}
	if len(s.reqs) <= s.calls {
		fc, err := em.FunctionCall("call_1", "upper")
		if err != nil {
			return err
		}
		if err := fc.Arguments(`{"text":"x"}`); err != nil {
			return err
		}
		if err := fc.Close(); err != nil {
			return err
		}
		return em.Complete()
	}
	w, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	reply := s.reply
	if len(s.reqs) <= s.calls+s.bloat {
		reply = strings.Repeat(s.reply+" ", 100)
	}
	if err := w.Text(reply); err != nil {
		return err
	}
	if len(s.reqs) > s.calls+s.bloat && len(s.reqs) <= s.calls+s.bloat+s.cut {
		return em.Incomplete(openresponses.IncompleteReasonMaxOutputTokens)
	}
	return em.Complete()
}

func TestLocalSummaryThroughRun(t *testing.T) {
	s := &summarizer{reply: "They talked about things."}
	tr := NewLocal(s, WithBudget(5), WithKeepLast(1), WithEstimator(count), WithModel("small"))
	cfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: tr.Transform,
		Tools: []agenttool.Tool{agenttool.New("upper", "", func(_ context.Context, a echoArgs) (string, error) { return strings.ToUpper(a.Text), nil })}}
	reqs, end, err := firstRequests(t, agentturn.Run(context.Background(), items(6), openresponses.Items{openresponses.UserText("latest")}, cfg))
	if err != nil || end.Reason != agentturn.ReasonDone {
		t.Fatalf("err = %v end = %+v", err, end)
	}
	// One summary for two turns: the second turn reuses it.
	if len(s.reqs) != 1 {
		t.Fatalf("summary calls = %d", len(s.reqs))
	}
	sreq := s.reqs[0]
	if sreq.Model != "small" || len(sreq.Tools) != 0 || sreq.Store == nil || *sreq.Store {
		t.Errorf("summary request = %+v", sreq)
	}
	// The folded items come first, the prompt last.
	if n := len(sreq.Input); n != 7 || sreq.Input[n-1].(*openresponses.Message).Text() != DefaultSummaryPrompt {
		t.Errorf("summary input = %d items, last = %v", n, sreq.Input[len(sreq.Input)-1])
	}
	// The model saw the summary message in place of the folded prefix.
	if len(reqs) != 2 {
		t.Fatalf("turns = %d", len(reqs))
	}
	first := reqs[0].Input[0].(*openresponses.Message)
	if first.Role != openresponses.RoleUser || !strings.HasPrefix(first.Text(), "Summary of the conversation so far:") || !strings.HasSuffix(first.Text(), "They talked about things.") {
		t.Errorf("first item = %+v", first)
	}
	if len(reqs[0].Input) != 2 || len(reqs[1].Input) != 4 {
		t.Errorf("inputs = %d, %d", len(reqs[0].Input), len(reqs[1].Input))
	}
	if last, ok := tr.Last().(*openresponses.Message); !ok || last != first {
		t.Errorf("Last = %v", tr.Last())
	}
	// The model still answers.
	if final := end.Items[len(end.Items)-1].(*openresponses.Message).Text(); final != "Tool result: LATEST" {
		t.Errorf("final = %q", final)
	}
}

func TestLocalSummaryRequestAndRetry(t *testing.T) {
	cases := []struct {
		name      string
		calls     int
		cut       int
		asked     int
		wantErr   string // from Transform
		wantFold  string // on the reported fold
		wantTypes string
	}{
		{"text on the first call", 0, 0, 1, "", "", "message"},
		{"a call without text is asked again", 1, 0, 2, "", "", "message"},
		{"no text twice fails the turn", 2, 0, 2, "compact: summary response has no text", "compact: summary response has no text", "function_call"},
		{"an incomplete summary is asked again", 0, 1, 2, "", "", "message"},
		{"incomplete twice sends the transcript unfolded", 0, 2, 2, "", "compact: summary response is incomplete: max_output_tokens", "message"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &summarizer{reply: "They talked about things.", calls: tc.calls, cut: tc.cut}
			var folds []Fold
			tr := NewLocal(s, WithBudget(1), WithKeepLast(1), WithEstimator(count), WithModel("small"),
				WithRequest(func(req *openresponses.Request) {
					if req.Model != "small" || len(req.Input) != 4 || req.Store == nil {
						t.Errorf("request before the edit = %+v", req)
					}
					req.Reasoning = openresponses.ReasoningConfig{Effort: openresponses.ReasoningEffortNone}
				}),
				WithOnFold(func(_ context.Context, f Fold) error { folds = append(folds, f); return nil }))
			out, err := tr.Transform(context.Background(), items(4))
			if tc.wantErr == "" && err != nil || tc.wantErr != "" && (err == nil || err.Error() != tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
			if len(s.reqs) != tc.asked {
				t.Fatalf("summary calls = %d, want %d", len(s.reqs), tc.asked)
			}
			for i, req := range s.reqs {
				if req.Reasoning.Effort != openresponses.ReasoningEffortNone || req.Model != "small" {
					t.Errorf("request %d = %+v", i, req)
				}
			}
			if len(folds) != 1 {
				t.Fatalf("folds = %d", len(folds))
			}
			// Failed or not, the fold reports its last call, and the
			// usage of every call: the nth response reports n output
			// tokens.
			f := folds[0]
			if tc.wantFold == "" && f.Err != nil || tc.wantFold != "" && (f.Err == nil || f.Err.Error() != tc.wantFold) {
				t.Errorf("fold err = %v, want %q", f.Err, tc.wantFold)
			}
			if tc.wantFold != "" && tc.wantErr == "" {
				// Not applied: the transcript goes as it was.
				if !errors.Is(f.Err, ErrSummaryIncomplete) || len(out) != 4 || f.Summary != nil || tr.Last() != nil {
					t.Errorf("fold err = %v, out = %d items, summary = %v", f.Err, len(out), f.Summary)
				}
			}
			if f.Request == nil || f.Request.Reasoning.Effort != openresponses.ReasoningEffortNone {
				t.Errorf("fold request = %+v", f.Request)
			}
			wantUsage := tc.asked * (tc.asked + 1) / 2
			if f.Attempts != tc.asked || f.ResponseID == "" || f.Usage == nil || f.Usage.OutputTokens != wantUsage {
				t.Errorf("fold attempts = %d, response = %q, usage = %+v, want %d output tokens", f.Attempts, f.ResponseID, f.Usage, wantUsage)
			}
			if got := strings.Join(f.OutputTypes, ","); got != tc.wantTypes {
				t.Errorf("fold output types = %q, want %q", got, tc.wantTypes)
			}
		})
	}
}

func TestLocalSummaryLargerThanItsInput(t *testing.T) {
	cases := []struct {
		name    string
		bloat   int
		asked   int
		wantErr bool
	}{
		{"a smaller summary is applied", 0, 1, false},
		{"an oversized summary is asked again", 1, 2, false},
		{"oversized twice sends the transcript unfolded", 2, 2, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &summarizer{reply: "They talked about things.", bloat: tc.bloat}
			var folds []Fold
			tr := NewLocal(s, WithBudget(10), WithKeepLast(1),
				WithOnFold(func(_ context.Context, f Fold) error { folds = append(folds, f); return nil }))
			history := items(6)
			out, err := tr.Transform(context.Background(), history)
			if len(s.reqs) != tc.asked {
				t.Fatalf("summary calls = %d, want %d", len(s.reqs), tc.asked)
			}
			if len(folds) != 1 || folds[0].Attempts != tc.asked {
				t.Fatalf("folds = %+v", folds)
			}
			if !tc.wantErr {
				if err != nil {
					t.Fatal(err)
				}
				if Estimate(out[:1]) >= Estimate(history[:5]) {
					t.Errorf("applied summary of %d tokens for %d", Estimate(out[:1]), Estimate(history[:5]))
				}
				return
			}
			// The fold is reported failed, and the run goes on with the
			// transcript as it was.
			if err != nil || !errors.Is(folds[0].Err, ErrSummaryTooLarge) {
				t.Errorf("err = %v, fold err = %v", err, folds[0].Err)
			}
			if len(out) != len(history) || out[0] != history[0] {
				t.Errorf("out = %d items, want the %d unfolded", len(out), len(history))
			}
			// Nothing was applied: the summary is neither remembered nor
			// reported as one.
			if tr.Last() != nil || folds[0].Summary != nil || folds[0].Output != nil {
				t.Errorf("an oversized summary was applied: last = %v, fold = %+v", tr.Last(), folds[0])
			}
			if got := strings.Join(folds[0].OutputTypes, ","); got != "message" || folds[0].ResponseID == "" {
				t.Errorf("fold output types = %q, response = %q", got, folds[0].ResponseID)
			}
		})
	}
}

func TestLocalSummaryBacksOffAFailedFold(t *testing.T) {
	// Twelve items of about 41 tokens each against a budget of 400: the
	// first turn folds eight of them. Each later turn adds one short
	// item of 20 tokens, so four of them stay under the token margin of
	// 100 and only the item margin of four (WithKeepLast) lets a failed
	// prefix be asked about again.
	cases := []struct {
		name    string
		s       *summarizer
		want    error // on the folds; nil for a turn that fails
		wantErr string
		// summary calls made by the end of each turn
		calls []int
	}{
		{"an oversized summary waits for the prefix to grow",
			&summarizer{reply: "They talked about things.", bloat: 1000}, ErrSummaryTooLarge, "",
			[]int{2, 2, 2, 2, 4, 4, 4, 4, 6}},
		{"an incomplete summary waits for the prefix to grow",
			&summarizer{reply: "gist", cut: 1000}, ErrSummaryIncomplete, "",
			[]int{2, 2, 2, 2, 4, 4, 4, 4, 6}},
		{"a summary with no text fails every turn",
			&summarizer{reply: "gist", calls: 1000}, nil, "compact: summary response has no text",
			[]int{2, 4, 6}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var folds []Fold
			tr := NewLocal(tc.s, WithBudget(400), WithKeepLast(4),
				WithOnFold(func(_ context.Context, f Fold) error { folds = append(folds, f); return nil }))
			history := items(12)
			for turn, want := range tc.calls {
				out, err := tr.Transform(context.Background(), history)
				if tc.wantErr != "" {
					if err == nil || err.Error() != tc.wantErr {
						t.Fatalf("turn %d: err = %v, want %q", turn+1, err, tc.wantErr)
					}
				} else if err != nil || len(out) != len(history) {
					t.Fatalf("turn %d: err = %v, out = %d items, want the %d unfolded", turn+1, err, len(out), len(history))
				}
				if len(tc.s.reqs) != want {
					t.Fatalf("turn %d: summary calls = %d, want %d", turn+1, len(tc.s.reqs), want)
				}
				// Only a fold that was asked is reported.
				if len(folds) != want/2 {
					t.Fatalf("turn %d: folds reported = %d, want %d", turn+1, len(folds), want/2)
				}
				history = append(history, openresponses.UserText("ok"))
			}
			for _, f := range folds {
				if tc.want != nil && !errors.Is(f.Err, tc.want) {
					t.Errorf("fold err = %v, want %v", f.Err, tc.want)
				}
			}
		})
	}
}

func TestLocalSummaryBackOffMargins(t *testing.T) {
	// After a failed fold of twelve items, the next call is asked again
	// only past one of the two margins, or for another transcript.
	base := items(12)
	grown := func(extra ...openresponses.Item) agentturn.Transcript {
		return append(append(agentturn.Transcript(nil), base...), extra...)
	}
	other := items(13)[1:] // the same sizes, opening with the assistant
	cases := []struct {
		name  string
		next  agentturn.Transcript
		asked bool
	}{
		{"the same transcript", base, false},
		{"one short item more", grown(openresponses.UserText("ok")), false},
		{"four items more", grown(openresponses.UserText("a"), openresponses.UserText("b"), openresponses.UserText("c"), openresponses.UserText("d")), true},
		{"a quarter of the budget more", grown(openresponses.UserText(strings.Repeat("long ", 100))), true},
		{"another conversation", other, true},
		{"the transcript rewound and changed", append(append(agentturn.Transcript(nil), base[:6]...), items(7)[1:]...), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &summarizer{reply: "They talked about things.", bloat: 1000}
			tr := NewLocal(s, WithBudget(400), WithKeepLast(4))
			if _, err := tr.Transform(context.Background(), base); err != nil || len(s.reqs) != 2 {
				t.Fatalf("err = %v, summary calls = %d", err, len(s.reqs))
			}
			if _, err := tr.Transform(context.Background(), tc.next); err != nil {
				t.Fatal(err)
			}
			if asked := len(s.reqs) > 2; asked != tc.asked {
				t.Errorf("asked again = %v, want %v", asked, tc.asked)
			}
		})
	}
}

func TestLocalSummaryOfATinyPrefix(t *testing.T) {
	// A one-word prefix is smaller than any summary item, wrapper and
	// all; the summary's text is what is weighed, so a terse one folds.
	s := &summarizer{reply: "They said hi."}
	var folds []Fold
	tr := NewLocal(s, WithBudget(1), WithKeepLast(1),
		WithOnFold(func(_ context.Context, f Fold) error { folds = append(folds, f); return nil }))
	history := agentturn.Transcript{openresponses.UserText("hi"), openresponses.AssistantText("hello")}
	out, err := tr.Transform(context.Background(), history)
	if err != nil {
		t.Fatal(err)
	}
	if len(folds) != 1 || folds[0].Err != nil || folds[0].Summary == nil || len(s.reqs) != 1 {
		t.Fatalf("folds = %+v, summary calls = %d", folds, len(s.reqs))
	}
	if len(out) != 2 || out[0] != folds[0].Summary || out[1] != history[1] {
		t.Errorf("out = %v", out)
	}
}

func TestLocalSummaryOutputLimit(t *testing.T) {
	cases := []struct {
		name   string
		budget int
		edit   func(*openresponses.Request)
		want   int // 0 for no limit
	}{
		{"half the budget", 1000, nil, 500},
		{"at most the default cap", 100_000, nil, DefaultSummaryMaxOutputTokens},
		{"WithRequest sets its own", 1000, func(req *openresponses.Request) { n := 64; req.MaxOutputTokens = &n }, 64},
		{"WithRequest clears it", 1000, func(req *openresponses.Request) { req.MaxOutputTokens = nil }, 0},
		{"a budget too small to halve sets none", 1, nil, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &summarizer{reply: "gist"}
			opts := []Option{WithBudget(tc.budget)}
			if tc.edit != nil {
				opts = append(opts, WithRequest(tc.edit))
			}
			// The fold itself, so the budget shapes the request whatever
			// the transcript's size.
			if _, err := NewLocal(s, opts...).fold(context.Background(), openresponses.Items(items(4))); err != nil {
				t.Fatal(err)
			}
			got := 0
			if limit := s.reqs[0].MaxOutputTokens; limit != nil {
				got = *limit
			}
			if got != tc.want {
				t.Errorf("max_output_tokens = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestLocalSummaryFoldsThePreviousSummary(t *testing.T) {
	s := &summarizer{reply: "first summary"}
	custom := func(summary string) openresponses.Item { return openresponses.DeveloperText("[" + summary + "]") }
	tr := NewLocal(s, WithBudget(3), WithKeepLast(1), WithEstimator(count), WithSummaryPrompt("condense"), WithSummaryItem(custom))
	history := items(4)
	out, err := tr.Transform(context.Background(), history)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || out[0].(*openresponses.Message).Role != openresponses.RoleDeveloper || out[0].(*openresponses.Message).Text() != "[first summary]" {
		t.Errorf("out = %v", out)
	}
	if s.reqs[0].Input[len(s.reqs[0].Input)-1].(*openresponses.Message).Text() != "condense" {
		t.Error("custom prompt not sent")
	}
	// The tail outgrows the budget: the next fold starts from the
	// previous summary, not from the raw history again.
	s.reply = "second summary"
	longer := append(append(agentturn.Transcript(nil), history...), items(4)...)
	out, err = tr.Transform(context.Background(), longer)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.reqs) != 2 {
		t.Fatalf("summary calls = %d", len(s.reqs))
	}
	if in := s.reqs[1].Input; in[0].(*openresponses.Message).Text() != "[first summary]" || len(in) != 6 {
		t.Errorf("second fold input = %d items, first = %v", len(in), in[0])
	}
	if out[0].(*openresponses.Message).Text() != "[second summary]" || len(out) != 2 {
		t.Errorf("out after second fold = %v", out)
	}
	// A failing summary call is the transform's error.
	s.fail = true
	if _, err := NewLocal(s, WithBudget(1), WithKeepLast(1), WithEstimator(count)).Transform(context.Background(), history); err == nil || !strings.Contains(err.Error(), "no summary today") {
		t.Errorf("err = %v", err)
	}
}

func TestTransformDoesNotHoldLockAcrossFold(t *testing.T) {
	// Two concurrent transforms over the same conversation: the fold
	// runs unlocked, and only one memory is installed.
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	slow := &probeCompactor{fn: func(openresponses.CompactRequest) {
		started <- struct{}{}
		<-release
	}}
	tr := New(slow, WithBudget(3), WithKeepLast(1), WithEstimator(count))
	history := items(4)
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := tr.Transform(context.Background(), history)
			results <- err
		}()
	}
	// Both callers reach the compactor before either finishes, which a
	// lock held across the call would prevent.
	<-started
	<-started
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	out, err := tr.Transform(context.Background(), append(append(agentturn.Transcript(nil), history...), openresponses.UserText("more")))
	if err != nil || out[0].ItemType() != openresponses.ItemTypeCompaction || len(out) != 3 {
		t.Errorf("out = %v err = %v", out, err)
	}
}

func TestOnFoldReportsEveryAttempt(t *testing.T) {
	s := &summarizer{reply: "the gist"}
	var folds []Fold
	tr := NewLocal(s, WithBudget(5), WithKeepLast(1), WithEstimator(count), WithOnFold(func(_ context.Context, f Fold) error {
		folds = append(folds, f)
		return nil
	}))
	in := items(6)
	out, err := tr.Transform(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if len(folds) != 1 {
		t.Fatalf("folds = %d", len(folds))
	}
	f := folds[0]
	// Five items folded, the last kept; the summary stands in for them.
	if f.Split != 5 || f.Err != nil || len(f.Output) != 1 || f.Summary != f.Output[0] || f.Summary != tr.Last() || f.TokensBefore != 6 {
		t.Errorf("fold = %+v", f)
	}
	if len(out) != 2 || out[0] != f.Summary || out[1] != in[5] {
		t.Errorf("transform = %v", out)
	}
	// A call answered from memory is not a fold.
	if _, err := tr.Transform(context.Background(), append(in, openresponses.UserText("more"))); err != nil || len(folds) != 1 {
		t.Errorf("reused fold reported: err=%v folds=%d", err, len(folds))
	}

	// A failed fold is reported with the error and no output, and the
	// error is returned.
	s.fail = true
	folds = nil
	_, err = tr.Transform(context.Background(), append(items(20), openresponses.UserText("x")))
	if err == nil || len(folds) != 1 || folds[0].Err == nil || folds[0].Output != nil || folds[0].Summary != nil || folds[0].Split != 20 || folds[0].TokensBefore != 17 {
		t.Errorf("failed fold: err=%v folds=%+v", err, folds)
	}

	// The reporter's error fails the transform.
	boom := errors.New("cannot record")
	s.fail = false
	tr = NewLocal(s, WithBudget(5), WithKeepLast(1), WithEstimator(count), WithOnFold(func(context.Context, Fold) error { return boom }))
	if _, err := tr.Transform(context.Background(), items(6)); !errors.Is(err, boom) {
		t.Errorf("reporter error = %v", err)
	}
	// The compaction endpoint's fold reports the compaction item as the
	// summary.
	folds = nil
	tr = New(&echo.Adapter{}, WithBudget(5), WithKeepLast(1), WithEstimator(count), WithOnFold(func(_ context.Context, f Fold) error {
		folds = append(folds, f)
		return nil
	}))
	if _, err := tr.Transform(context.Background(), items(6)); err != nil {
		t.Fatal(err)
	}
	if len(folds) != 1 || folds[0].Summary == nil || folds[0].Summary.ItemType() != openresponses.ItemTypeCompaction {
		t.Errorf("endpoint fold = %+v", folds)
	}
}

// TestPinKeepsAnInjectedItemThroughAFold is the stream rule's case: a
// reminder injected once, then a fold, and the rule suppressed
// afterwards. What the model has of it must be the reminder, not what
// the summary made of it.
func TestPinKeepsAnInjectedItemThroughAFold(t *testing.T) {
	interrupt := openresponses.DeveloperText("<system-interrupt>never use Box::leak</system-interrupt>")
	isInterrupt := func(item openresponses.Item) bool {
		m, ok := item.(*openresponses.Message)
		return ok && strings.HasPrefix(m.Text(), "<system-interrupt>")
	}
	s := &summarizer{reply: "They talked about things."}
	tr := NewLocal(s, WithBudget(5), WithKeepLast(1), WithEstimator(count), WithPin(isInterrupt))
	var folds []Fold
	WithOnFold(func(_ context.Context, f Fold) error {
		folds = append(folds, f)
		return nil
	})(tr)

	history := append(agentturn.Transcript{interrupt}, items(6)...)
	out, err := tr.Transform(context.Background(), history)
	if err != nil {
		t.Fatal(err)
	}
	// The summary stands in for the folded prefix and the pinned item
	// follows it, before the kept tail.
	if len(out) != 3 {
		t.Fatalf("request = %d items: %v", len(out), out)
	}
	if m, ok := out[0].(*openresponses.Message); !ok || !strings.HasPrefix(m.Text(), "Summary of the conversation so far:") {
		t.Errorf("first item = %v", out[0])
	}
	if out[1] != openresponses.Item(interrupt) {
		t.Errorf("second item = %v, want the pinned interrupt", out[1])
	}
	if len(folds) != 1 || len(folds[0].Pinned) != 1 || folds[0].Pinned[0] != openresponses.Item(interrupt) {
		t.Fatalf("fold = %+v", folds)
	}
	// It survives the next fold too, which summarises it again so
	// nothing is lost if the pin is later dropped.
	longer := append(append(agentturn.Transcript(nil), history...), items(6)...)
	out, err = tr.Transform(context.Background(), longer)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range out {
		if item == openresponses.Item(interrupt) {
			found = true
		}
	}
	if !found {
		t.Errorf("the pinned item is gone after the second fold: %v", out)
	}
	if len(folds) != 2 || len(folds[1].Pinned) != 1 {
		t.Errorf("second fold = %+v", folds[1])
	}
	var summarised bool
	for _, item := range s.reqs[1].Input {
		if item == openresponses.Item(interrupt) {
			summarised = true
		}
	}
	if !summarised {
		t.Error("the pinned item was not part of the fold that summarised its prefix")
	}
	// Without a pin the injected item is whatever the summary made of
	// it, which is what the option exists to change.
	plain := NewLocal(&summarizer{reply: "They talked about things."}, WithBudget(5), WithKeepLast(1), WithEstimator(count))
	out, err = plain.Transform(context.Background(), history)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range out {
		if item == openresponses.Item(interrupt) {
			t.Error("an unpinned item survived the fold verbatim")
		}
	}
}

// TestOnFoldReadsNoSharedState holds one fold inside its reporter
// while another conversation is transformed on the same Transform,
// which forgets the memory that no longer matches. The Fold is the
// transform's report to its caller, so what it carries is the
// caller's: built from the fold's own result, never read out of state
// the lock was just released on. Under -race that is the assertion.
func TestOnFoldReadsNoSharedState(t *testing.T) {
	inFold := make(chan struct{})
	release := make(chan struct{})
	var seen int
	tr := New(&counting{Compactor: &echo.Adapter{}}, WithBudget(3), WithKeepLast(1), WithEstimator(count),
		WithOnFold(func(_ context.Context, f Fold) error {
			seen = len(f.Output)
			close(inFold)
			<-release
			return nil
		}))
	history := items(4)
	done := make(chan error, 1)
	go func() {
		_, err := tr.Transform(context.Background(), history)
		done <- err
	}()
	<-inFold
	// A conversation that does not begin with the folded prefix and
	// fits the budget, so the transform forgets what it remembered
	// without folding again: the write that races a reporter reading
	// the memory instead of its own fold.
	other := agentturn.Transcript{openresponses.UserText("elsewhere")}
	if _, err := tr.Transform(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if seen == 0 {
		t.Error("the fold reported no output")
	}
}

// TestFoldOutputAndPinnedAreDisjoint checks the two members name
// different things, so a consumer that joins them to see what was sent
// does not send anything twice.
func TestFoldOutputAndPinnedAreDisjoint(t *testing.T) {
	interrupt := openresponses.DeveloperText("<system-interrupt>never use Box::leak</system-interrupt>")
	s := &summarizer{reply: "They talked about things."}
	var fold Fold
	tr := NewLocal(s, WithBudget(5), WithKeepLast(1), WithEstimator(count),
		WithPin(func(item openresponses.Item) bool { return item == openresponses.Item(interrupt) }),
		WithOnFold(func(_ context.Context, f Fold) error {
			fold = f
			return nil
		}))
	history := append(agentturn.Transcript{interrupt}, items(6)...)
	out, err := tr.Transform(context.Background(), history)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range fold.Output {
		if item == openresponses.Item(interrupt) {
			t.Error("Output carries a pinned item, so Output plus Pinned sends it twice")
		}
	}
	if len(fold.Pinned) != 1 || fold.Pinned[0] != openresponses.Item(interrupt) {
		t.Fatalf("Pinned = %v", fold.Pinned)
	}
	// Output, then Pinned, then the items from Split on is what was
	// sent.
	want := append(append(openresponses.Items(nil), fold.Output...), fold.Pinned...)
	want = append(want, history[fold.Split:]...)
	if len(want) != len(out) {
		t.Fatalf("rebuilt %d items, sent %d", len(want), len(out))
	}
	for i := range want {
		if want[i] != out[i] {
			t.Errorf("item %d: rebuilt %v, sent %v", i, want[i], out[i])
		}
	}
}
