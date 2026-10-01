package agentturn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// TestToolDispatchIsPerCall pins #93: tool_dispatch is raised as each
// call is handed to its tool, after tool_start has been raised for the
// whole batch, so a call the cut reaches first has none.
func TestToolDispatchIsPerCall(t *testing.T) {
	started := make(chan struct{}, 1)
	blocking := func(ctx context.Context, _ echoArgs) (string, error) {
		started <- struct{}{}
		<-ctx.Done()
		return "", ctx.Err()
	}
	plain := func(context.Context, echoArgs) (string, error) { return "b", nil }
	// A serial batch: the second call waits for the first, which
	// blocks, and the abort reaches it before its turn.
	a := New(Config{Model: twoCalls{}, Tools: []agenttool.Tool{agenttool.New("a", "", blocking), agenttool.New("b", "", plain)}, ToolExecution: ExecSequential})
	var mu sync.Mutex
	var seq []string
	a.Subscribe(func(_ context.Context, ev Event) error {
		mu.Lock()
		defer mu.Unlock()
		switch e := ev.(type) {
		case *ToolStart:
			seq = append(seq, "start:"+e.Name)
		case *ToolDispatch:
			seq = append(seq, "dispatch:"+e.Name)
		case *ToolEnd:
			seq = append(seq, "end:"+e.Name)
		}
		return nil
	})
	go func() {
		<-started
		a.Abort()
	}()
	end, _ := a.Prompt(context.Background(), openresponses.UserText("x"))
	if end.Reason != ReasonAborted || len(end.Pending) != 2 {
		t.Fatalf("run: reason=%s pending=%v", end.Reason, pendingReasons(end))
	}
	mu.Lock()
	got := append([]string(nil), seq...)
	mu.Unlock()
	if len(got) < 3 || got[0] != "start:a" || got[1] != "start:b" || got[2] != "dispatch:a" {
		t.Errorf("order: %v", got)
	}
	for _, s := range got {
		if s == "dispatch:b" {
			t.Errorf("the second call of a serial batch was dispatched before its turn: %v", got)
		}
	}
	// A batch that runs to completion dispatches every call, each
	// after its own tool_start and before its tool_end.
	cfg := Config{Model: twoCalls{}, Tools: []agenttool.Tool{agenttool.New("a", "", plain), agenttool.New("b", "", plain)}, MaxTurns: 1}
	events, _, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	phase := map[string]int{}
	for _, ev := range events {
		switch e := ev.(type) {
		case *ToolStart:
			phase[e.CallID] = 1
		case *ToolDispatch:
			if phase[e.CallID] != 1 {
				t.Errorf("dispatch of %s at phase %d", e.CallID, phase[e.CallID])
			}
			phase[e.CallID] = 2
		case *ToolEnd:
			if phase[e.CallID] != 2 {
				t.Errorf("end of %s at phase %d", e.CallID, phase[e.CallID])
			}
			phase[e.CallID] = 3
		}
	}
	if len(phase) != 2 {
		t.Errorf("calls seen: %d", len(phase))
	}
}

// TestToolDispatchFailureStopsTheCall pins the other half of the
// dispatch rule: a subscriber that cannot record the dispatch stops
// the call, so no tool runs after a dispatch the record does not hold.
func TestToolDispatchFailureStopsTheCall(t *testing.T) {
	boom := errors.New("disk full")
	ran := false
	tool := agenttool.New("upper", "", func(_ context.Context, a echoArgs) (string, error) {
		ran = true
		return upper(context.Background(), a)
	})
	a := New(Config{Model: twoCalls{}, Tools: []agenttool.Tool{tool}, MaxTurns: 1})
	var events []Event
	a.Subscribe(func(_ context.Context, ev Event) error {
		events = append(events, ev)
		if _, ok := ev.(*ToolDispatch); ok {
			return boom
		}
		return nil
	})
	end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
	if !errors.Is(err, boom) || end.Reason != ReasonError {
		t.Fatalf("run: err=%v reason=%s", err, end.Reason)
	}
	if ran {
		t.Error("the tool ran after its dispatch could not be delivered")
	}
	if starts, ends, unpaired := pairing(events); starts != 1 || ends != 1 || len(unpaired) != 0 {
		t.Errorf("pairing: starts=%d ends=%d unpaired=%v", starts, ends, unpaired)
	}
	// The loop knows the tool never ran, which aborted, "may have run",
	// would not say.
	if len(end.Pending) != 1 || end.Pending[0].Reason != PendingUndispatched {
		t.Errorf("pending: %v", pendingReasons(end))
	}
}

// pgRecord is a record a tool leaves while it runs.
type pgRecord struct{ PGID int }

func (pgRecord) RecordNS() string { return "shell:process-group" }

// TestToolRecorderReachesTheTool pins #97: Config.ToolRecorder is the
// executor's recorder, so a tool's WriteRecord reaches the host under
// the loop with the call on the context.
func TestToolRecorderReachesTheTool(t *testing.T) {
	var mu sync.Mutex
	var got []string
	tool := agenttool.New("upper", "", func(ctx context.Context, a echoArgs) (string, error) {
		if err := agenttool.WriteRecord(ctx, pgRecord{PGID: 42}); err != nil {
			return "", err
		}
		return upper(ctx, a)
	})
	cfg := Config{Model: twoCalls{}, Tools: []agenttool.Tool{tool}, MaxTurns: 1,
		ToolRecorder: func(ctx context.Context, rec *agenttool.Record) error {
			call, ok := agenttool.CallFrom(ctx)
			if !ok {
				return errors.New("no call on the recorder's context")
			}
			mu.Lock()
			defer mu.Unlock()
			got = append(got, rec.NS+"@"+call.ID+"@"+RunIDFromContext(ctx))
			return nil
		}}
	_, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err != nil || end.Reason != ReasonStopped {
		t.Fatalf("run: err=%v reason=%s", err, end.Reason)
	}
	if len(got) != 1 || got[0] == "" || got[0][:len("shell:process-group@call")] != "shell:process-group@call" {
		t.Errorf("records: %v", got)
	}
	// Without one, WriteRecord is a no-op and the tool runs unchanged.
	cfg.ToolRecorder = nil
	if _, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg)); err != nil || end.Reason != ReasonStopped {
		t.Errorf("no recorder: err=%v reason=%s", err, end.Reason)
	}
}

// TestNestedCallIsDispatched pins that a nested call raises its own
// tool_dispatch, and that a consumer refusing it stops the nested tool
// and hands the failure to the invoking tool rather than the run.
func TestNestedCallIsDispatched(t *testing.T) {
	boom := errors.New("no room for the dispatch")
	innerRan := false
	inner := agenttool.New("c", "", func(context.Context, echoArgs) (string, error) {
		innerRan = true
		return "c", nil
	})
	outer := agenttool.New("b", "", func(ctx context.Context, _ echoArgs) (string, error) {
		res, err := Invoke(ctx, "c", json.RawMessage(`{"text":"t"}`))
		if err != nil {
			// The tool wraps the failure, as a tool may; the run must
			// not read the wrapped error as its own.
			return "", fmt.Errorf("inner call failed: %w", err)
		}
		return "b saw " + res.Output.Text, nil
	})
	for _, refuse := range []bool{false, true} {
		name := "dispatched"
		if refuse {
			name = "dispatch refused"
		}
		t.Run(name, func(t *testing.T) {
			innerRan = false
			a := New(Config{Model: callsNamed{names: []string{"b"}}, Tools: []agenttool.Tool{outer, inner}, MaxTurns: 1})
			var events []Event
			a.Subscribe(func(_ context.Context, ev Event) error {
				events = append(events, ev)
				if e, ok := ev.(*ToolDispatch); ok && e.Parent != "" && refuse {
					return boom
				}
				return nil
			})
			end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
			if err != nil || end.Reason != ReasonStopped {
				t.Fatalf("run: err=%v reason=%s", err, end.Reason)
			}
			dispatched := map[string]bool{}
			for _, ev := range events {
				if e, ok := ev.(*ToolDispatch); ok {
					dispatched[e.Name] = true
				}
			}
			if !dispatched["b"] || !dispatched["c"] {
				t.Errorf("dispatched: %v, want b and c", dispatched)
			}
			if starts, ends, unpaired := pairing(events); starts != 2 || ends != 2 || len(unpaired) != 0 {
				t.Errorf("pairing: starts=%d ends=%d unpaired=%v", starts, ends, unpaired)
			}
			if innerRan == refuse {
				t.Errorf("inner ran = %v, want %v", innerRan, !refuse)
			}
			var bOut string
			for _, it := range end.Items {
				if out, ok := it.(*openresponses.FunctionCallOutput); ok {
					bOut = out.Output.Text
				}
			}
			if refuse && !strings.Contains(bOut, "no room") || !refuse && bOut != "b saw c" {
				t.Errorf("b's output: %q", bOut)
			}
			if len(end.Pending) != 0 {
				t.Errorf("pending: %v", pendingReasons(end))
			}
		})
	}
}

// TestDistinctDispatchFailuresCutEveryCall pins that a consumer
// refusing every dispatch with a fresh error value leaves every call
// pending with no output, rather than reading the later failures as
// the tools' own results.
func TestDistinctDispatchFailuresCutEveryCall(t *testing.T) {
	sentinel := errors.New("disk full")
	plain := func(context.Context, echoArgs) (string, error) { return "x", nil }
	a := New(Config{Model: twoCalls{}, Tools: []agenttool.Tool{agenttool.New("a", "", plain), agenttool.New("b", "", plain)}})
	n := 0
	var events []Event
	a.Subscribe(func(_ context.Context, ev Event) error {
		events = append(events, ev)
		if _, ok := ev.(*ToolDispatch); ok {
			n++
			return fmt.Errorf("write dispatch %d: %w", n, sentinel)
		}
		return nil
	})
	end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
	if !errors.Is(err, sentinel) || end.Reason != ReasonError {
		t.Fatalf("run: err=%v reason=%s", err, end.Reason)
	}
	if got := itemTypes(end.Items); got != "user function_call function_call" {
		t.Errorf("items: %s, want no outputs", got)
	}
	if len(end.Pending) != 2 {
		t.Errorf("pending: %v, want both calls", pendingReasons(end))
	}
	if starts, ends, unpaired := pairing(events); starts != 2 || ends != 2 || len(unpaired) != 0 {
		t.Errorf("pairing: starts=%d ends=%d unpaired=%v", starts, ends, unpaired)
	}
}

// TestChainWaitsForAfterToolCall pins #196: the next call of a chain,
// a serial batch or calls sharing a resource, is dispatched only once
// AfterToolCall has settled the call before it, so what the hook
// records lands between the two. Calls in chains of their own wait for
// nothing, and a hook that fails cuts the chain rather than hanging it.
func TestChainWaitsForAfterToolCall(t *testing.T) {
	cases := []struct {
		name  string
		names []string
		// opts are the options of every tool.
		opts []agenttool.Option
		exec ExecutionMode
		max  int
		// ordered says each call follows the one before it.
		ordered bool
		// fail makes the hook fail on the first call.
		fail bool
	}{
		{name: "sequential", names: []string{"a", "b"}, exec: ExecSequential, ordered: true},
		{name: "one at a time", names: []string{"a", "b", "c"}, max: 1, ordered: true},
		{name: "shared resource", names: []string{"a", "b", "c"}, opts: []agenttool.Option{agenttool.WithResource("shell")}, ordered: true},
		{name: "sequential tool", names: []string{"a", "b"}, opts: []agenttool.Option{agenttool.WithSequential()}, ordered: true},
		{name: "chains of their own", names: []string{"a", "b"}},
		{name: "hook fails", names: []string{"a", "b"}, exec: ExecSequential, fail: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var seq []string
			log := func(s string) {
				mu.Lock()
				defer mu.Unlock()
				seq = append(seq, s)
			}
			var tools []agenttool.Tool
			for _, name := range tc.names {
				tools = append(tools, agenttool.New(name, "", func(context.Context, echoArgs) (string, error) {
					log("tool:" + name)
					return name, nil
				}, tc.opts...))
			}
			after := func(_ context.Context, info ToolResultInfo) (*ToolOverride, error) {
				log("after:" + info.Call.Name)
				// Long enough for a call dispatched alongside the hook
				// to show up inside it.
				time.Sleep(20 * time.Millisecond)
				if tc.fail {
					return nil, errors.New("checkpoint failed")
				}
				log("settled:" + info.Call.Name)
				return nil, nil
			}
			a := New(Config{Model: callsNamed{names: tc.names}, Tools: tools, ToolExecution: tc.exec, MaxParallelTools: tc.max, AfterToolCall: after, MaxTurns: 1})
			a.Subscribe(func(_ context.Context, ev Event) error {
				if e, ok := ev.(*ToolDispatch); ok {
					log("dispatch:" + e.Name)
				}
				return nil
			})
			end, _ := a.Prompt(context.Background(), openresponses.UserText("x"))
			mu.Lock()
			got := append([]string(nil), seq...)
			mu.Unlock()
			at := func(s string) int { return slices.Index(got, s) }
			if tc.fail {
				if end.Reason != ReasonError || at("dispatch:b") >= 0 {
					t.Errorf("reason=%s, order %v", end.Reason, got)
				}
				return
			}
			if end.Reason != ReasonStopped && end.Reason != ReasonDone {
				t.Fatalf("reason=%s err=%v", end.Reason, end.Err)
			}
			for i, name := range tc.names {
				if at("settled:"+name) < 0 {
					t.Errorf("%s not settled: %v", name, got)
				}
				if !tc.ordered || i == 0 {
					continue
				}
				prev := tc.names[i-1]
				if !(at("settled:"+prev) < at("dispatch:"+name) && at("dispatch:"+name) < at("tool:"+name)) {
					t.Errorf("%s dispatched before %s settled: %v", name, prev, got)
				}
			}
		})
	}
}
