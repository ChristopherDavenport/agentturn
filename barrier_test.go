package agentturn

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// TestBarrierRunsAloneInItsPlace pins how a batch with an
// agenttool.Barrier call runs: the barrier is dispatched only once
// every call before it has been settled, AfterToolCall included, the
// calls after it only once it has been, and the calls between two
// barriers alongside each other. A Sequential tool, or ExecSequential,
// still runs the whole batch one call at a time.
func TestBarrierRunsAloneInItsPlace(t *testing.T) {
	barrier := []agenttool.Option{agenttool.WithBarrier()}
	sequential := []agenttool.Option{agenttool.WithSequential()}
	type call struct {
		name string
		opts []agenttool.Option
	}
	cases := []struct {
		name  string
		calls []call
		exec  ExecutionMode
		// after names, for a call, the calls that must be settled before
		// it is dispatched.
		after map[string][]string
		// together are pairs of calls that must overlap: the first's tool
		// waits for the second's dispatch.
		together [][2]string
		// ordered says every call is dispatched after the one before it
		// is settled.
		ordered bool
	}{
		{
			name:     "in the middle",
			calls:    []call{{"r1", nil}, {"r2", nil}, {"b", barrier}, {"r3", nil}, {"r4", nil}},
			after:    map[string][]string{"b": {"r1", "r2"}, "r3": {"b"}, "r4": {"b"}},
			together: [][2]string{{"r1", "r2"}, {"r3", "r4"}},
		},
		{
			name:     "first",
			calls:    []call{{"b", barrier}, {"r1", nil}, {"r2", nil}},
			after:    map[string][]string{"r1": {"b"}, "r2": {"b"}},
			together: [][2]string{{"r1", "r2"}},
		},
		{
			name:     "last",
			calls:    []call{{"r1", nil}, {"r2", nil}, {"b", barrier}},
			after:    map[string][]string{"b": {"r1", "r2"}},
			together: [][2]string{{"r1", "r2"}},
		},
		{
			name:  "two adjacent",
			calls: []call{{"r1", nil}, {"b1", barrier}, {"b2", barrier}, {"r2", nil}},
			after: map[string][]string{"b1": {"r1"}, "b2": {"b1"}, "r2": {"b2"}},
		},
		{
			name:    "a sequential tool wins",
			calls:   []call{{"r1", nil}, {"b", barrier}, {"s", sequential}, {"r2", nil}},
			ordered: true,
		},
		{
			name:    "ExecSequential",
			calls:   []call{{"r1", nil}, {"r2", nil}, {"b", barrier}, {"r3", nil}},
			exec:    ExecSequential,
			ordered: true,
		},
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
			dispatched := map[string]chan struct{}{}
			waitsFor := map[string]string{}
			for _, p := range tc.together {
				waitsFor[p[0]] = p[1]
			}
			var names []string
			var tools []agenttool.Tool
			for _, c := range tc.calls {
				name := c.name
				names = append(names, name)
				dispatched[name] = make(chan struct{})
				tools = append(tools, agenttool.New(name, "", func(context.Context, echoArgs) (string, error) {
					if other, ok := waitsFor[name]; ok {
						select {
						case <-dispatched[other]:
						case <-time.After(5 * time.Second):
							log("alone:" + name)
						}
					}
					return name, nil
				}, c.opts...))
			}
			after := func(_ context.Context, info ToolResultInfo) (*ToolOverride, error) {
				// Long enough for a call dispatched before this settles to
				// show up inside it.
				time.Sleep(20 * time.Millisecond)
				log("settled:" + info.Call.Name)
				return nil, nil
			}
			a := New(Config{Model: callsNamed{names: names}, Tools: tools, ToolExecution: tc.exec, AfterToolCall: after, MaxTurns: 1})
			a.Subscribe(func(_ context.Context, ev Event) error {
				if e, ok := ev.(*ToolDispatch); ok {
					log("dispatch:" + e.Name)
					close(dispatched[e.Name])
				}
				return nil
			})
			end, _ := a.Prompt(context.Background(), openresponses.UserText("x"))
			if end.Reason != ReasonStopped && end.Reason != ReasonDone {
				t.Fatalf("reason=%s err=%v", end.Reason, end.Err)
			}
			mu.Lock()
			got := append([]string(nil), seq...)
			mu.Unlock()
			at := func(s string) int { return slices.Index(got, s) }
			for _, name := range names {
				if at("dispatch:"+name) < 0 || at("settled:"+name) < 0 {
					t.Fatalf("%s not dispatched and settled: %v", name, got)
				}
				if at("alone:"+name) >= 0 {
					t.Errorf("%s ran without %s alongside: %v", name, waitsFor[name], got)
				}
			}
			for name, before := range tc.after {
				for _, b := range before {
					if at("dispatch:"+name) < at("settled:"+b) {
						t.Errorf("%s dispatched before %s settled: %v", name, b, got)
					}
				}
			}
			if tc.ordered {
				for i := 1; i < len(names); i++ {
					if at("dispatch:"+names[i]) < at("settled:"+names[i-1]) {
						t.Errorf("%s dispatched before %s settled: %v", names[i], names[i-1], got)
					}
				}
			}
		})
	}
}
