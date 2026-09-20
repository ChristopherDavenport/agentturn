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
