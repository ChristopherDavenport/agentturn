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
// records the idempotency key and the text argument of every call it
// runs. While block is set it waits for its context, so an abort cuts
// it after dispatch.
type keyedTool struct {
	mu    sync.Mutex
	keys  []string
	texts []string
	block bool
}

func (k *keyedTool) tool(replay agenttool.Replay) agenttool.Tool {
	return agenttool.New("act", "", func(ctx context.Context, args echoArgs) (string, error) {
		call, _ := agenttool.CallFrom(ctx)
		k.mu.Lock()
		k.keys = append(k.keys, call.IdempotencyKey)
		k.texts = append(k.texts, args.Text)
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
// record says, and a call the loop cannot say about is held to it too;
// a call run again runs with the arguments it was handed over with; a
// keyed call run with other arguments needs a key of its own; and
// WithRunAgain is the way past the rule.
func TestResumeAppliesTheReplayRule(t *testing.T) {
	call := &openresponses.FunctionCall{CallID: "call_1", Name: "act", Arguments: `{"text":"t"}`}
	seeded := Transcript{openresponses.UserText("go"), call}
	cases := []struct {
		name   string
		replay agenttool.Replay
		// cut runs the agent into the pending call itself; otherwise
		// it is seeded with seeded and, when set, pending.
		cut bool
		// rewrite has the decision hook rewrite the call's arguments
		// before the cut.
		rewrite bool
		pending []PendingCall
		answer  func(callID string) Answer
		wantErr error
		// wantKey is the key the call runs again with, "" for any, and
		// wantText the text argument, "" for the call's own.
		wantKey  string
		wantText string
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
		{name: "seeded without a record, unknown", replay: agenttool.ReplayUnknown, wantErr: ErrAmbiguousCall},
		{name: "seeded without a record, safe", replay: agenttool.ReplaySafe},
		{name: "seeded without a record, keyed", replay: agenttool.ReplayKeyed, wantErr: ErrAmbiguousCall},
		{name: "seeded without a record, unknown, run again", replay: agenttool.ReplayUnknown,
			answer: func(id string) Answer { return Approve(id).WithRunAgain() }},
		{name: "cut, unknown, run again", replay: agenttool.ReplayUnknown, cut: true,
			answer: func(id string) Answer { return Approve(id).WithRunAgain() }},
		{name: "cut after a rewrite, safe", replay: agenttool.ReplaySafe, cut: true, rewrite: true, wantText: "rewritten"},
		{name: "cut after a rewrite, keyed", replay: agenttool.ReplayKeyed, cut: true, rewrite: true, wantText: "rewritten"},
		{name: "seeded aborted with rewritten arguments, keyed", replay: agenttool.ReplayKeyed,
			pending: []PendingCall{{Call: call, Reason: PendingAborted, IdempotencyKey: "k1", Args: json.RawMessage(`{"text":"rewritten"}`)}},
			wantKey: "k1", wantText: "rewritten"},
		{name: "cut, keyed, other arguments", replay: agenttool.ReplayKeyed, cut: true,
			answer: func(id string) Answer { return ApproveWith(id, json.RawMessage(`{"text":"other"}`)) }, wantErr: ErrAmbiguousCall},
		{name: "cut, keyed, other arguments under a new key", replay: agenttool.ReplayKeyed, cut: true,
			answer: func(id string) Answer {
				return ApproveWith(id, json.RawMessage(`{"text":"other"}`)).WithIdempotencyKey("k3")
			}, wantKey: "k3", wantText: "other"},
		{name: "seeded aborted, keyed, other arguments under the first key named", replay: agenttool.ReplayKeyed,
			pending: []PendingCall{{Call: call, Reason: PendingAborted, IdempotencyKey: "k1"}},
			answer: func(id string) Answer {
				return ApproveWith(id, json.RawMessage(`{"text":"other"}`)).WithIdempotencyKey("k1")
			}, wantErr: ErrAmbiguousCall},
		{name: "cut, keyed, the same arguments respelled", replay: agenttool.ReplayKeyed, cut: true,
			answer: func(id string) Answer { return ApproveWith(id, json.RawMessage(`{ "text": "go" }`)) }},
		{name: "seeded held before dispatch, unknown", replay: agenttool.ReplayUnknown,
			pending: []PendingCall{{Call: call, Reason: PendingDeferred}}},
		{name: "seeded held after dispatch, unknown", replay: agenttool.ReplayUnknown,
			pending: []PendingCall{{Call: call, Reason: PendingDeferred, Dispatched: true, IdempotencyKey: "k1"}}, wantErr: ErrAmbiguousCall},
		{name: "seeded held after dispatch, keyed", replay: agenttool.ReplayKeyed,
			pending: []PendingCall{{Call: call, Reason: PendingDeferred, Dispatched: true, IdempotencyKey: "k1", Args: json.RawMessage(`{"text":"rewritten"}`)}},
			wantKey: "k1", wantText: "rewritten"},
		{name: "seeded answered", replay: agenttool.ReplaySafe,
			pending: []PendingCall{{Call: call, Reason: PendingAnswered}}, wantErr: ErrCallAnswered},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k := &keyedTool{block: tc.cut}
			cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{k.tool(tc.replay)}}
			var a *Agent
			if tc.rewrite {
				cfg.BeforeToolCall = func(context.Context, ToolCallInfo) (*ToolDecision, error) {
					return &ToolDecision{Args: json.RawMessage(`{"text":"rewritten"}`)}, nil
				}
			}
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
			if text := k.texts[len(k.texts)-1]; tc.wantText != "" && text != tc.wantText {
				t.Errorf("ran with text %q, want %q", text, tc.wantText)
			}
			got := k.keys[len(k.keys)-1]
			switch {
			case tc.wantKey != "" && got != tc.wantKey:
				t.Errorf("ran with key %q, want %q", got, tc.wantKey)
			case tc.cut && tc.wantKey == "" && got != first[0]:
				t.Errorf("ran again with key %q, want the first, %q", got, first[0])
			case got == "":
				t.Error("ran with no key")
			}
		})
	}
}

// TestApprovedCallCutBeforeDispatchIsUndispatched checks that a
// deferred call approved on resume and cut before it was handed to its
// tool reads as never handed over, not as still waiting for an answer.
func TestApprovedCallCutBeforeDispatchIsUndispatched(t *testing.T) {
	k := &keyedTool{}
	a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{k.tool(agenttool.ReplayUnknown)},
		BeforeToolCall: func(context.Context, ToolCallInfo) (*ToolDecision, error) {
			return &ToolDecision{Action: Defer}, nil
		}})
	end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
	if end.Reason != ReasonInputRequired || len(end.Pending) != 1 || end.Pending[0].Reason != PendingDeferred {
		t.Fatalf("end = %+v", end)
	}
	a.Subscribe(func(_ context.Context, ev Event) error {
		if _, ok := ev.(*ToolStart); ok {
			a.Abort()
		}
		return nil
	})
	end, _ = a.Resume(context.Background(), Approve(end.Pending[0].Call.CallID))
	if end.Reason != ReasonAborted || len(end.Pending) != 1 || end.Pending[0].Reason != PendingUndispatched || len(k.keys) != 0 {
		t.Errorf("end = %+v, pending %+v, ran %d", end, end.Pending, len(k.keys))
	}
}

// TestHeldAfterDispatchCutAgainStaysAmbiguous checks that a call a
// record held after its dispatch, approved and cut before it was
// handed to its tool again, reads as a call that may have run, with
// the key and arguments of the dispatch it would repeat, not as one
// that never started nor as one still held.
func TestHeldAfterDispatchCutAgainStaysAmbiguous(t *testing.T) {
	call := &openresponses.FunctionCall{CallID: "call_1", Name: "act", Arguments: `{"text":"t"}`}
	k := &keyedTool{}
	a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{k.tool(agenttool.ReplayKeyed)}},
		WithTranscript(Transcript{openresponses.UserText("go"), call}),
		WithPending([]PendingCall{{Call: call, Reason: PendingDeferred, Dispatched: true, IdempotencyKey: "k1", Args: json.RawMessage(`{"text":"r"}`)}}))
	a.Subscribe(func(_ context.Context, ev Event) error {
		if _, ok := ev.(*ToolStart); ok {
			a.Abort()
		}
		return nil
	})
	end, _ := a.Resume(context.Background(), Approve(call.CallID))
	if end == nil || end.Reason != ReasonAborted || len(end.Pending) != 1 || len(k.keys) != 0 {
		t.Fatalf("end = %+v, ran %d", end, len(k.keys))
	}
	if p := end.Pending[0]; p.Reason != PendingAborted || p.IdempotencyKey != "k1" || string(p.Args) != `{"text":"r"}` || !p.MayHaveRun() {
		t.Errorf("pending = %+v", p)
	}
}

// TestSetTranscriptKeepsWhatTheAgentKnew checks that a held call stays
// held across SetTranscript, so it is approved as a held call rather
// than refused as one that may have run, and that SetPending tells a
// live agent what a record knows of a call it did not.
func TestSetTranscriptKeepsWhatTheAgentKnew(t *testing.T) {
	k := &keyedTool{}
	cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{k.tool(agenttool.ReplayUnknown)},
		BeforeToolCall: func(context.Context, ToolCallInfo) (*ToolDecision, error) {
			return &ToolDecision{Action: Defer}, nil
		}}
	a := New(cfg)
	end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
	if end.Reason != ReasonInputRequired || len(end.Pending) != 1 {
		t.Fatalf("end = %+v", end)
	}
	if err := a.SetTranscript(a.State().Transcript); err != nil {
		t.Fatal(err)
	}
	if p := a.State().Pending; len(p) != 1 || p[0].Reason != PendingDeferred {
		t.Fatalf("pending after SetTranscript = %+v", p)
	}
	if _, err := a.Resume(context.Background(), Approve(end.Pending[0].Call.CallID)); err != nil || len(k.keys) != 1 {
		t.Fatalf("resume: err=%v ran %d", err, len(k.keys))
	}

	// A fresh agent learns it from the record, as after a rebase.
	b := New(cfg)
	if err := b.SetTranscript(Transcript{openresponses.UserText("go"), end.Pending[0].Call}); err != nil {
		t.Fatal(err)
	}
	id := end.Pending[0].Call.CallID
	if _, err := b.Resume(context.Background(), Approve(id)); !errors.Is(err, ErrAmbiguousCall) {
		t.Fatalf("unseeded: err = %v", err)
	}
	if err := b.SetPending([]PendingCall{{Call: end.Pending[0].Call, Reason: PendingDeferred}}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Resume(context.Background(), Approve(id)); err != nil || len(k.keys) != 2 {
		t.Fatalf("seeded: err=%v ran %d", err, len(k.keys))
	}
}

// TestSameArgs checks the comparison a keyed approval's arguments get:
// by JSON value, with numbers as written.
func TestSameArgs(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{`{"a":1,"b":"x"}`, `{ "b": "x", "a": 1 }`, true},
		{`{"a":1}`, `{"a":2}`, false},
		{`{"n":9007199254740993}`, `{"n":9007199254740992}`, false},
	}
	for _, tc := range cases {
		if got := sameArgs(json.RawMessage(tc.a), json.RawMessage(tc.b)); got != tc.want {
			t.Errorf("sameArgs(%s, %s) = %v", tc.a, tc.b, got)
		}
	}
}
