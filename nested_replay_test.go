package agentturn

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// callThenAnswer asks for one call of eval on its first request and
// answers with text on every later one.
type callThenAnswer struct {
	mu    sync.Mutex
	calls int
}

func (m *callThenAnswer) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.mu.Lock()
	m.calls++
	first := m.calls == 1
	m.mu.Unlock()
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if first {
		w, err := em.FunctionCall("call_eval", "eval")
		if err != nil {
			return err
		}
		if err := w.Arguments(`{}`); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		return em.Complete()
	}
	w, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if err := w.Text("done"); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Complete()
}

// nestedHarness is an eval tool whose k-th execution runs script[k],
// beside a nested tool named "act" that records the call ID and key of
// every execution, and waits for its context while block is set.
type nestedHarness struct {
	mu      sync.Mutex
	execs   int
	script  []func(ctx context.Context) (string, error)
	ids     []string
	keys    []string
	block   bool
	results []string
	errs    []error
}

func (h *nestedHarness) tools(replay agenttool.Replay) []agenttool.Tool {
	eval := agenttool.New("eval", "", func(ctx context.Context, _ struct{}) (string, error) {
		h.mu.Lock()
		k := h.execs
		h.execs++
		h.mu.Unlock()
		return h.script[k](ctx)
	}, agenttool.WithReplay(func(context.Context, json.RawMessage) agenttool.Replay { return agenttool.ReplaySafe }))
	act := agenttool.New("act", "", func(ctx context.Context, a echoArgs) (string, error) {
		call, _ := agenttool.CallFrom(ctx)
		h.mu.Lock()
		h.ids = append(h.ids, call.ID)
		h.keys = append(h.keys, call.IdempotencyKey)
		block := h.block
		h.mu.Unlock()
		if block {
			<-ctx.Done()
			return "", ctx.Err()
		}
		return "acted on " + a.Text, nil
	}, agenttool.WithReplay(func(context.Context, json.RawMessage) agenttool.Replay { return replay }))
	return []agenttool.Tool{eval, act}
}

// invokes returns a script step that invokes act with each text in
// turn, records what each returned, then waits for its context when
// wait is set, and returns the cancellation when it was cut.
func (h *nestedHarness) invokes(wait bool, texts ...string) func(ctx context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		for _, text := range texts {
			res, err := Invoke(ctx, "act", json.RawMessage(`{"text":"`+text+`"}`))
			h.mu.Lock()
			h.results = append(h.results, res.Output.Text)
			h.errs = append(h.errs, err)
			h.mu.Unlock()
		}
		if wait {
			<-ctx.Done()
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
		return "evaluated", nil
	}
}

// TestANestedCallMadeAgainIsTheCallItWas pins #261: a nested call an
// earlier execution of the invoking call made is fixed as a model's
// call is. When an approval runs the invoking call again, the Invoke
// that names the same tool with the same arguments in the same place
// keeps that call's ID and key: one that ended returns its result
// without running, one dispatched and not ended runs again under its
// key as the replay rule allows, and an Invoke that differs is a new
// call, as is every one after it.
func TestANestedCallMadeAgainIsTheCallItWas(t *testing.T) {
	cases := []struct {
		name   string
		replay agenttool.Replay
		// first and second are the eval tool's two executions; block
		// has act wait for its context in the first.
		first, second func(h *nestedHarness) func(context.Context) (string, error)
		block         bool
		// cutAt is the event the first run is aborted at.
		cutAt     func(Event) bool
		elicit    *agenttool.Answer
		wantExecs int
		// wantSame is the number of act calls of the second execution,
		// run or replayed, that keep an ID the first gave.
		wantSame     int
		wantReplayed int
		wantErr      error
		wantResults  []string
	}{
		{
			name:   "an ended call is replayed",
			replay: agenttool.ReplayUnknown,
			first:  func(h *nestedHarness) func(context.Context) (string, error) { return h.invokes(true, "a") },
			second: func(h *nestedHarness) func(context.Context) (string, error) { return h.invokes(false, "a") },
			cutAt: func(ev Event) bool {
				e, ok := ev.(*ToolEnd)
				return ok && e.Parent != ""
			},
			wantExecs: 1, wantSame: 1, wantReplayed: 1,
			wantResults: []string{"acted on a", "acted on a"},
		},
		{
			name:   "a keyed call cut after its dispatch runs again under its key",
			replay: agenttool.ReplayKeyed,
			first:  func(h *nestedHarness) func(context.Context) (string, error) { return h.invokes(false, "a") },
			second: func(h *nestedHarness) func(context.Context) (string, error) { return h.invokes(false, "a") },
			block:  true,
			cutAt: func(ev Event) bool {
				e, ok := ev.(*ToolDispatch)
				return ok && e.Parent != ""
			},
			wantExecs: 2, wantSame: 1,
		},
		{
			name:   "an unknown call cut after its dispatch is not run again unasked",
			replay: agenttool.ReplayUnknown,
			first:  func(h *nestedHarness) func(context.Context) (string, error) { return h.invokes(false, "a") },
			second: func(h *nestedHarness) func(context.Context) (string, error) { return h.invokes(false, "a") },
			block:  true,
			cutAt: func(ev Event) bool {
				e, ok := ev.(*ToolDispatch)
				return ok && e.Parent != ""
			},
			wantExecs: 1, wantErr: ErrOutcomeUnknown,
		},
		{
			name:   "an unknown call cut after its dispatch runs again when the user allows it",
			replay: agenttool.ReplayUnknown,
			first:  func(h *nestedHarness) func(context.Context) (string, error) { return h.invokes(false, "a") },
			second: func(h *nestedHarness) func(context.Context) (string, error) { return h.invokes(false, "a") },
			block:  true,
			cutAt: func(ev Event) bool {
				e, ok := ev.(*ToolDispatch)
				return ok && e.Parent != ""
			},
			elicit:    &agenttool.Answer{Action: agenttool.ActionAccept},
			wantExecs: 2, wantSame: 1,
		},
		{
			name:   "an Invoke that differs is a new call, and so is every one after it",
			replay: agenttool.ReplaySafe,
			first:  func(h *nestedHarness) func(context.Context) (string, error) { return h.invokes(true, "a", "b") },
			second: func(h *nestedHarness) func(context.Context) (string, error) { return h.invokes(false, "z", "b") },
			cutAt: func(ev Event) bool {
				e, ok := ev.(*ToolEnd)
				return ok && e.Parent != "" && string(e.Result.Output.Text) == "acted on b"
			},
			wantExecs:   4,
			wantResults: []string{"acted on a", "acted on b", "acted on z", "acted on b"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &nestedHarness{block: tc.block}
			h.script = []func(context.Context) (string, error){tc.first(h), tc.second(h)}
			cfg := Config{Model: &callThenAnswer{}, Tools: h.tools(tc.replay)}
			if tc.elicit != nil {
				ans := *tc.elicit
				cfg.ToolElicitor = func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) { return ans, nil }
			}
			a := New(cfg)
			var replayed []*ToolEnd
			cut := true
			a.Subscribe(func(_ context.Context, ev Event) error {
				if cut && tc.cutAt(ev) {
					cut = false
					a.Abort()
				}
				if e, ok := ev.(*ToolEnd); ok && e.Replayed {
					replayed = append(replayed, e)
				}
				return nil
			})
			end, err := a.Prompt(context.Background(), openresponses.UserText("go"))
			if err != nil || end.Reason != ReasonAborted || len(end.Pending) != 1 {
				t.Fatalf("first run: end %+v, err %v", end, err)
			}
			first := end.Pending[0].Invoked
			if len(first) == 0 {
				t.Fatalf("pending call lists no nested calls")
			}
			h.mu.Lock()
			h.block = false
			before := len(h.ids)
			h.mu.Unlock()
			end, err = a.Resume(context.Background(), Approve("call_eval"))
			if err != nil || end.Reason != ReasonDone {
				t.Fatalf("second run: end %+v, err %v", end, err)
			}
			h.mu.Lock()
			defer h.mu.Unlock()
			if len(h.ids) != tc.wantExecs {
				t.Errorf("act ran %d times, want %d", len(h.ids), tc.wantExecs)
			}
			same := 0
			for i := before; i < len(h.ids); i++ {
				for _, c := range first {
					if c.CallID == h.ids[i] {
						same++
						if c.IdempotencyKey != "" && h.keys[i] != c.IdempotencyKey {
							t.Errorf("call %s ran again under key %q, want %q", c.CallID, h.keys[i], c.IdempotencyKey)
						}
					}
				}
			}
			if tc.wantReplayed > 0 {
				same += len(replayed)
				for _, e := range replayed {
					if e.CallID != first[0].CallID {
						t.Errorf("replayed %s, want %s", e.CallID, first[0].CallID)
					}
				}
			}
			if same != tc.wantSame {
				t.Errorf("%d calls kept their ID, want %d", same, tc.wantSame)
			}
			if len(replayed) != tc.wantReplayed {
				t.Errorf("%d replayed, want %d", len(replayed), tc.wantReplayed)
			}
			if tc.wantErr != nil && !errors.Is(h.errs[len(h.errs)-1], tc.wantErr) {
				t.Errorf("the invoking tool got %v, want %v", h.errs[len(h.errs)-1], tc.wantErr)
			}
			if tc.wantResults != nil {
				if len(h.results) != len(tc.wantResults) {
					t.Fatalf("results %q, want %q", h.results, tc.wantResults)
				}
				for i := range h.results {
					if h.results[i] != tc.wantResults[i] {
						t.Errorf("results %q, want %q", h.results, tc.wantResults)
						break
					}
				}
			}
		})
	}
}

// TestCarryInvoked pins the list a pending call is left with when an
// execution of it is cut again: what it made, then what it did not
// reach of a list it never left.
func TestCarryInvoked(t *testing.T) {
	a := InvokedCall{CallID: "a", Parent: "p", Dispatched: true}
	b := InvokedCall{CallID: "b", Parent: "p", Dispatched: true}
	c := InvokedCall{CallID: "c", Parent: "b", Dispatched: true}
	aEnded := InvokedCall{CallID: "a", Parent: "p", Dispatched: true, Ended: true}
	bEnded := InvokedCall{CallID: "b", Parent: "p", Dispatched: true, Ended: true}
	fresh := InvokedCall{CallID: "x", Parent: "p"}
	ids := func(cs []InvokedCall) []string {
		var out []string
		for _, c := range cs {
			out = append(out, c.CallID)
		}
		return out
	}
	cases := []struct {
		name        string
		before, now []InvokedCall
		want        []string
	}{
		{"nothing made keeps the list", []InvokedCall{a, b, c}, nil, []string{"a", "b", "c"}},
		{"the calls not reached follow", []InvokedCall{a, b, c}, []InvokedCall{aEnded}, []string{"a", "b", "c"}},
		{"a new call drops the rest under its parent", []InvokedCall{a, b, c}, []InvokedCall{fresh}, []string{"x"}},
		{"nothing under an ended call is kept", []InvokedCall{a, b, c}, []InvokedCall{aEnded, bEnded}, []string{"a", "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ids(CarryInvoked(tc.before, tc.now, "p"))
			if len(got) != len(tc.want) {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %q, want %q", got, tc.want)
				}
			}
		})
	}
}
