package agentturn

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// keyedTool is a tool named "act" whose replay is replay, and which
// records the idempotency key of every call it runs. While block is
// set it waits for its context, so an abort cuts it after dispatch.
type keyedTool struct {
	mu    sync.Mutex
	keys  []string
	block bool
}

func (k *keyedTool) tool(replay agenttool.Replay) agenttool.Tool {
	return agenttool.New("act", "", func(ctx context.Context, _ echoArgs) (string, error) {
		call, _ := agenttool.CallFrom(ctx)
		k.mu.Lock()
		k.keys = append(k.keys, call.IdempotencyKey)
		block := k.block
		k.mu.Unlock()
		if block {
			<-ctx.Done()
			return "", ctx.Err()
		}
		return "done", nil
	}, agenttool.WithReplay(func(context.Context, json.RawMessage) agenttool.Replay { return replay }))
}

// TestLoopMintsIdempotencyKeys pins #145: every call the loop hands to
// its tool carries a key, the one tool_dispatch reports; a call cut
// off after its dispatch is pending with that key, and an approval of
// it runs it again with the same one.
func TestLoopMintsIdempotencyKeys(t *testing.T) {
	k := &keyedTool{block: true}
	a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{k.tool(agenttool.ReplayKeyed)}})
	var dispatched []string
	a.Subscribe(func(_ context.Context, ev Event) error {
		if d, ok := ev.(*ToolDispatch); ok {
			dispatched = append(dispatched, d.IdempotencyKey)
			if len(dispatched) == 1 {
				a.Abort()
			}
		}
		return nil
	})
	end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
	if end.Reason != ReasonAborted || len(end.Pending) != 1 {
		t.Fatalf("end = %+v", end)
	}
	p := end.Pending[0]
	if len(k.keys) != 1 || k.keys[0] == "" || dispatched[0] != k.keys[0] {
		t.Fatalf("tool keys %q, dispatched %q", k.keys, dispatched)
	}
	if p.Reason != PendingAborted || p.IdempotencyKey != k.keys[0] {
		t.Errorf("pending = %+v, want aborted with %q", p, k.keys[0])
	}
	k.block = false
	if _, err := a.Resume(context.Background(), Approve(p.Call.CallID)); err != nil {
		t.Fatal(err)
	}
	if len(k.keys) != 2 || k.keys[1] != k.keys[0] || len(dispatched) != 2 || dispatched[1] != k.keys[0] {
		t.Errorf("keys %q, dispatched %q, want the first key again", k.keys, dispatched)
	}
}

// TestResumeAppliesTheReplayRule pins the loop half of #143: an
// approval of a call that may have run is held to agenttool's replay
// rule, whether the agent saw the cut itself or was seeded with what a
// record says; a call the loop cannot say about is not checked.
func TestResumeAppliesTheReplayRule(t *testing.T) {
	call := &openresponses.FunctionCall{CallID: "call_1", Name: "act", Arguments: `{"text":"t"}`}
	seeded := Transcript{openresponses.UserText("go"), call}
	cases := []struct {
		name   string
		replay agenttool.Replay
		// cut runs the agent into the pending call itself; otherwise
		// it is seeded with seeded and, when set, pending.
		cut     bool
		pending []PendingCall
		answer  func(callID string) Answer
		wantErr error
		// wantKey is the key the call runs again with, "" for any.
		wantKey string
	}{
		{name: "cut, safe", replay: agenttool.ReplaySafe, cut: true},
		{name: "cut, keyed", replay: agenttool.ReplayKeyed, cut: true},
		{name: "cut, unknown", replay: agenttool.ReplayUnknown, cut: true, wantErr: ErrAmbiguousCall},
		{name: "seeded aborted, keyed with its key", replay: agenttool.ReplayKeyed,
			pending: []PendingCall{{Call: call, Reason: PendingAborted, IdempotencyKey: "k1"}}, wantKey: "k1"},
		{name: "seeded aborted, keyed without a key", replay: agenttool.ReplayKeyed,
			pending: []PendingCall{{Call: call, Reason: PendingAborted}}, wantErr: ErrAmbiguousCall},
		{name: "seeded aborted, keyed, the answer's key", replay: agenttool.ReplayKeyed,
			pending: []PendingCall{{Call: call, Reason: PendingAborted}},
			answer:  func(id string) Answer { return Approve(id).WithIdempotencyKey("k2") }, wantKey: "k2"},
		{name: "seeded aborted, unknown", replay: agenttool.ReplayUnknown,
			pending: []PendingCall{{Call: call, Reason: PendingAborted, IdempotencyKey: "k1"}}, wantErr: ErrAmbiguousCall},
		{name: "seeded never started, unknown", replay: agenttool.ReplayUnknown,
			pending: []PendingCall{{Call: call, Reason: PendingUndispatched}}},
		{name: "seeded without a record, unknown", replay: agenttool.ReplayUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k := &keyedTool{block: tc.cut}
			cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{k.tool(tc.replay)}}
			var a *Agent
			if tc.cut {
				a = New(cfg)
				a.Subscribe(func(_ context.Context, ev Event) error {
					if _, ok := ev.(*ToolDispatch); ok && k.block {
						a.Abort()
					}
					return nil
				})
				if end, _ := a.Prompt(context.Background(), openresponses.UserText("go")); end.Reason != ReasonAborted {
					t.Fatalf("end = %+v", end)
				}
				k.block = false
			} else {
				a = New(cfg, WithPending(tc.pending), WithTranscript(seeded))
			}
			pending := a.State().Pending
			if len(pending) != 1 {
				t.Fatalf("pending = %+v", pending)
			}
			first := append([]string(nil), k.keys...)
			answer := Approve
			if tc.answer != nil {
				answer = tc.answer
			}
			_, err := a.Resume(context.Background(), answer(pending[0].Call.CallID))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("resume: err = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr != nil {
				if len(k.keys) != len(first) || len(a.State().Pending) != 1 {
					t.Fatalf("a refused approval ran the tool: keys %q", k.keys)
				}
				// The rule's answer for such a call goes through.
				if _, err := a.Resume(context.Background(), OutcomeUnknown(pending[0].Call.CallID)); err != nil {
					t.Fatal(err)
				}
				return
			}
			if len(k.keys) != len(first)+1 {
				t.Fatalf("keys = %q", k.keys)
			}
			got := k.keys[len(k.keys)-1]
			switch {
			case tc.wantKey != "" && got != tc.wantKey:
				t.Errorf("ran with key %q, want %q", got, tc.wantKey)
			case tc.cut && got != first[0]:
				t.Errorf("ran again with key %q, want the first, %q", got, first[0])
			case got == "":
				t.Error("ran with no key")
			}
		})
	}
}
