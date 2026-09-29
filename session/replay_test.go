package session

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestReplayAfterACrash pins #143, #144 and #145 on the record: a
// parallel batch of a safe, a keyed and an unknown tool is cut by a
// crash after every dispatch, and the session is resumed in a new
// process. Each dispatch carries the key the tool received; the replay
// rule runs the safe and the keyed calls again, the keyed one with the
// key its first attempt carried, and answers the unknown one with the
// outcome unknown; the calls run again have a second dispatch and a
// proceed saying why, and the one that was not an answer saying why
// and no second dispatch (#100).
func TestReplayAfterACrash(t *testing.T) {
	var (
		mu       sync.Mutex
		keys     = map[string][]string{}
		blocking atomic.Bool
	)
	blocking.Store(true)
	started := make(chan struct{}, 3)
	tool := func(name string, replay agenttool.Replay) agenttool.Tool {
		return agenttool.New(name, "", func(ctx context.Context, _ echoArgs) (string, error) {
			call, _ := agenttool.CallFrom(ctx)
			mu.Lock()
			keys[name] = append(keys[name], call.IdempotencyKey)
			mu.Unlock()
			if blocking.Load() {
				started <- struct{}{}
				<-ctx.Done()
				return "", ctx.Err()
			}
			return name + " ran", nil
		}, agenttool.WithReplay(func(context.Context, json.RawMessage) agenttool.Replay { return replay }))
	}
	tools := []agenttool.Tool{
		tool("lookup", agenttool.ReplaySafe),
		tool("charge", agenttool.ReplayKeyed),
		tool("notify", agenttool.ReplayUnknown),
	}

	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: tools})
	unsub := rec.Attach(a)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = a.Prompt(context.Background(), openresponses.UserText("go"))
	}()
	for range 3 {
		<-started
	}
	// The crash: the process is gone, so nothing more reaches the
	// record, and its run is left open.
	unsub()
	a.Abort()
	<-done
	blocking.Store(false)

	first := map[string]string{}
	for name, c := range callsOf(t, s) {
		if c.Dispatch == nil {
			t.Fatalf("%s has no dispatch", name)
		}
		first[name] = c.Dispatch.IdempotencyKey
		if first[name] == "" || first[name] != keys[name][0] {
			t.Errorf("%s: dispatch key %q, tool received %q", name, first[name], keys[name])
		}
	}

	rec2, s2, err := Resume(context.Background(), store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	pending, err := Pending(s2)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 3 {
		t.Fatalf("pending = %+v", pending)
	}
	for _, p := range pending {
		if p.Reason != agentturn.PendingAborted || p.IdempotencyKey != first[p.Call.Name] {
			t.Errorf("%s: reason %s key %q", p.Call.Name, p.Reason, p.IdempotencyKey)
		}
	}
	answers, err := ReplayAnswers(context.Background(), s2, tools)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]agentturn.Answer{}
	for _, ans := range answers {
		for _, p := range pending {
			if p.Call.CallID == ans.CallID {
				byName[p.Call.Name] = ans
			}
		}
	}
	if ans := byName["lookup"]; ans.Output != nil || ans.Reason != "run again: safe" {
		t.Errorf("lookup answer = %+v", ans)
	}
	if ans := byName["charge"]; ans.Output != nil || ans.IdempotencyKey != first["charge"] || ans.Reason != "run again: keyed" {
		t.Errorf("charge answer = %+v", ans)
	}
	if ans := byName["notify"]; ans.Output == nil || !strings.Contains(ans.Output.Output.Text, "outcome unknown") {
		t.Errorf("notify answer = %+v", ans)
	}

	opts, err := AgentOptions(s2)
	if err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: tools}, opts...)
	defer rec2.Attach(b)()
	end, err := b.Resume(context.Background(), answers...)
	if err != nil || end.Reason != agentturn.ReasonDone {
		t.Fatalf("resume: err=%v end=%+v", err, end)
	}
	if got := keys["charge"]; len(got) != 2 || got[1] != first["charge"] {
		t.Errorf("charge keys = %q, want the first twice", got)
	}
	if got := keys["notify"]; len(got) != 1 {
		t.Errorf("notify ran %d times", len(got))
	}

	dispatches := map[string][]*agentsession.DispatchEntry{}
	for _, e := range s2.Path(s2.Leaf()) {
		if d, ok := e.(*agentsession.DispatchEntry); ok {
			dispatches[d.CallID] = append(dispatches[d.CallID], d)
		}
	}
	for name, c := range callsOf(t, s2) {
		ds := dispatches[c.ID()]
		switch name {
		case "lookup", "charge":
			if len(ds) != 2 || ds[1].IdempotencyKey != keys[name][1] || c.IdempotencyKey() != ds[1].IdempotencyKey {
				t.Errorf("%s: %d dispatches", name, len(ds))
			}
			if len(c.Decisions) != 1 || c.Decisions[0].Verdict != agentsession.VerdictProceed || c.Decisions[0].By != agentsession.ByPolicy || !strings.HasPrefix(c.Decisions[0].Reason, "run again") {
				t.Errorf("%s: decisions = %+v", name, c.Decisions)
			}
		case "notify":
			if len(ds) != 1 || len(c.Decisions) != 1 || c.Decisions[0].Verdict != agentsession.VerdictAnswer || c.Decisions[0].By != agentsession.ByPolicy || c.Decisions[0].Reason != "not run again: replay unknown" {
				t.Errorf("notify: %d dispatches, decisions %+v", len(ds), c.Decisions)
			}
		}
		if c.State(s2.Header()) != agentsession.CallCompleted {
			t.Errorf("%s: state %v", name, c.State(s2.Header()))
		}
	}
	verifyAll(t, s2)
}

// TestReplayAnswersEachState checks the rule for each state a pending
// call can be in, on a session written by hand: a call that never
// started is approved, a held one is left to the caller, one refused
// before its output is owed that refusal (#159), and one that may have
// run is approved only as its tool's replay allows.
func TestReplayAnswersEachState(t *testing.T) {
	replayTool := func(name string, replay agenttool.Replay) agenttool.Tool {
		return agenttool.New(name, "", func(context.Context, echoArgs) (string, error) { return "", nil },
			agenttool.WithReplay(func(context.Context, json.RawMessage) agenttool.Replay { return replay }))
	}
	tools := []agenttool.Tool{replayTool("safe", agenttool.ReplaySafe), replayTool("keyed", agenttool.ReplayKeyed), replayTool("unknown", agenttool.ReplayUnknown)}
	cases := []struct {
		name    string
		tool    string
		records []string
		// state writes what the path holds for the call after its item.
		state   func(callID, target string) []agentsession.Entry
		want    string
		wantKey string
		// wantArgs are the arguments the approval carries, nil for the
		// call's own.
		wantArgs string
	}{
		{name: "never started", tool: "unknown", want: "approve"},
		{name: "held", tool: "unknown", want: "none", state: func(id, target string) []agentsession.Entry {
			return []agentsession.Entry{agentsession.NewDecision(id, target, agentsession.VerdictHold, agentsession.ByPolicy)}
		}},
		{name: "in flight, safe", tool: "safe", want: "approve", state: dispatched("")},
		{name: "in flight, keyed with its key", tool: "keyed", want: "approve", wantKey: "k1", state: dispatched("k1")},
		{name: "in flight, keyed without a key", tool: "keyed", want: "unknown", state: dispatched("")},
		{name: "in flight, unknown", tool: "unknown", want: "unknown", state: dispatched("k1")},
		{name: "in flight, no such tool", tool: "gone", want: "unknown", state: dispatched("")},
		{name: "in flight after a rewrite, keyed", tool: "keyed", want: "approve", wantKey: "k1", wantArgs: `{"text":"rewritten"}`,
			state: func(id, target string) []agentsession.Entry {
				return append([]agentsession.Entry{agentsession.NewDecision(id, target, agentsession.VerdictProceed, agentsession.ByPolicy).WithArgs(json.RawMessage(`{"text":"rewritten"}`))},
					dispatched("k1")(id, target)...)
			}},
		{name: "held after dispatch", tool: "keyed", want: "none", state: func(id, target string) []agentsession.Entry {
			return append(dispatched("k1")(id, target), agentsession.NewDecision(id, target, agentsession.VerdictHold, agentsession.ByPolicy))
		}},
		{name: "answered, output owed", tool: "safe", want: "unknown", state: func(id, target string) []agentsession.Entry {
			return append(dispatched("k1")(id, target), agentsession.NewDecision(id, target, agentsession.VerdictAnswer, agentsession.ByPolicy))
		}},
		{name: "run again under a new key", tool: "keyed", want: "approve", wantKey: "k2", wantArgs: `{"text":"rewritten"}`,
			state: func(id, target string) []agentsession.Entry {
				return append(append(dispatched("k1")(id, target),
					agentsession.NewDecision(id, target, agentsession.VerdictProceed, agentsession.ByHuman).WithArgs(json.RawMessage(`{"text":"rewritten"}`))),
					dispatched("k2")(id, target)...)
			}},
		{name: "rewritten after its dispatch, keyed", tool: "keyed", want: "approve", wantKey: "k1",
			state: func(id, target string) []agentsession.Entry {
				// The proceed's arguments were never handed over, so
				// the key is paired with the ones that were.
				return append(dispatched("k1")(id, target),
					agentsession.NewDecision(id, target, agentsession.VerdictProceed, agentsession.ByHuman).WithArgs(json.RawMessage(`{"text":"rewritten"}`)))
			}},
		{name: "rejected, refusal owed", tool: "unknown", want: "refusal", state: func(id, target string) []agentsession.Entry {
			return []agentsession.Entry{agentsession.NewDecision(id, target, agentsession.VerdictReject, agentsession.ByPolicy).WithReason("denied by rm")}
		}},
		{name: "held then rejected, refusal owed", tool: "unknown", want: "refusal", state: func(id, target string) []agentsession.Entry {
			return []agentsession.Entry{agentsession.NewDecision(id, target, agentsession.VerdictHold, agentsession.ByPolicy),
				agentsession.NewDecision(id, target, agentsession.VerdictReject, agentsession.ByHuman).WithReason("denied by rm")}
		}},
		{name: "no records, rejected", tool: "unknown", records: []string{}, want: "refusal", state: func(id, target string) []agentsession.Entry {
			return []agentsession.Entry{agentsession.NewDecision(id, target, agentsession.VerdictReject, agentsession.ByPolicy).WithReason("denied by rm")}
		}},
		{name: "no records, safe", tool: "safe", records: []string{}, want: "approve"},
		{name: "no records, unknown", tool: "unknown", records: []string{}, want: "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := agentsession.NewMemoryStore()
			records := tc.records
			if records == nil {
				records = agentsession.AllRecords
			}
			s, err := store.Create(ctx, agentsession.Header{Records: records})
			if err != nil {
				t.Fatal(err)
			}
			call := &openresponses.FunctionCall{CallID: "call_1", Name: tc.tool, Arguments: `{"text":"t"}`}
			target, err := store.Append(ctx, s.ID(), &agentsession.ItemEntry{Item: call})
			if err != nil {
				t.Fatal(err)
			}
			if tc.state != nil {
				for _, e := range tc.state(call.CallID, target) {
					if _, err := store.Append(ctx, s.ID(), e); err != nil {
						t.Fatal(err)
					}
				}
			}
			answers, err := ReplayAnswers(ctx, s, tools)
			if err != nil {
				t.Fatal(err)
			}
			got := "none"
			if len(answers) == 1 {
				got = "approve"
				switch out := answers[0].Output; {
				case out != nil && answers[0].Reason == "":
					// An output with no reason is the refusal the
					// record holds, and nothing else.
					got = "refusal"
					if out.Output.Text != "denied by rm" {
						t.Errorf("refusal output %q", out.Output.Text)
					}
				case out != nil:
					got = "unknown"
				}
				if answers[0].IdempotencyKey != tc.wantKey || answers[0].By != agentsession.ByPolicy || string(answers[0].Args) != tc.wantArgs {
					t.Errorf("answer = %+v, want key %q and args %s by policy", answers[0], tc.wantKey, tc.wantArgs)
				}
			}
			if got != tc.want || len(answers) > 1 {
				t.Errorf("answers = %+v, want %s", answers, tc.want)
			}
		})
	}
}

// dispatched writes a dispatch carrying key, or none when key is "".
func dispatched(key string) func(callID, target string) []agentsession.Entry {
	return func(callID, target string) []agentsession.Entry {
		return []agentsession.Entry{agentsession.NewDispatch(callID, target).WithIdempotencyKey(key)}
	}
}

// TestResumeHeldAndAnsweredCalls pins the two pending states format
// 0.8 adds, on sessions written by hand and resumed in a new process.
// A call held after its dispatch is deferred and may have run: an
// approval is held to the replay rule and runs it again under the key
// of the dispatch it repeats, a proceed and a second dispatch on the
// record, and an output answers it with an answer decision. A call an
// answer ended before the crash wrote its output is owed that output
// and nothing else: an approval is refused, and the output is written
// with no decision before it.
func TestResumeHeldAndAnsweredCalls(t *testing.T) {
	hold := func(id, target string) []agentsession.Entry {
		return append(dispatched("k1")(id, target), agentsession.NewDecision(id, target, agentsession.VerdictHold, agentsession.ByPolicy).WithReason("confirm a second charge"))
	}
	answered := func(id, target string) []agentsession.Entry {
		return append(dispatched("k1")(id, target), agentsession.NewDecision(id, target, agentsession.VerdictAnswer, agentsession.ByPolicy).WithReason("not run again: replay unknown"))
	}
	rejected := func(id, target string) []agentsession.Entry {
		return []agentsession.Entry{agentsession.NewDecision(id, target, agentsession.VerdictReject, agentsession.ByPolicy).WithReason("denied by rm")}
	}
	cases := []struct {
		name   string
		replay agenttool.Replay
		state  func(callID, target string) []agentsession.Entry
		// want is the reason Pending gives the call.
		want   agentturn.PendingReason
		answer func(callID string) agentturn.Answer
		// replayed answers with what ReplayAnswers gives instead, and
		// block has BeforeToolCall block every call.
		replayed   bool
		block      bool
		wantErr    error
		wantRuns   int
		wantRecord []string
		// wantKey is the key the call runs again with, "" for k1, and
		// wantBy and wantReason, when set, what the last decision says.
		wantKey    string
		wantBy     string
		wantReason string
	}{
		{name: "held after dispatch, keyed, approved", replay: agenttool.ReplayKeyed, state: hold, want: agentturn.PendingDeferred,
			answer: func(id string) agentturn.Answer {
				return agentturn.Approve(id).WithBy(agentsession.ByHuman).WithReason("confirmed")
			},
			wantRuns: 1, wantRecord: []string{"dispatch", "hold", "proceed", "dispatch", "output"}},
		{name: "held after dispatch, unknown, approved", replay: agenttool.ReplayUnknown, state: hold, want: agentturn.PendingDeferred,
			answer: agentturn.Approve, wantErr: agentturn.ErrAmbiguousCall},
		{name: "held after dispatch, answered", replay: agenttool.ReplayUnknown, state: hold, want: agentturn.PendingDeferred,
			answer: func(id string) agentturn.Answer {
				return agentturn.OutcomeUnknown(id).WithBy(agentsession.ByHuman).WithReason("declined a second charge")
			},
			wantRecord: []string{"dispatch", "hold", "answer", "output"}},
		{name: "answered, approved", replay: agenttool.ReplaySafe, state: answered, want: agentturn.PendingAnswered,
			answer: agentturn.Approve, wantErr: agentturn.ErrCallAnswered},
		{name: "answered, output", replay: agenttool.ReplaySafe, state: answered, want: agentturn.PendingAnswered,
			answer:     agentturn.OutcomeUnknown,
			wantRecord: []string{"dispatch", "answer", "output"}},
		// #159: the policy rules on a call that never started, and a
		// call it refused before the crash is owed that refusal.
		{name: "never started, replayed, the policy blocks", replay: agenttool.ReplaySafe, want: agentturn.PendingUndispatched,
			replayed: true, block: true, wantRecord: []string{"reject", "output"}},
		{name: "rejected, approved", replay: agenttool.ReplaySafe, state: rejected, want: agentturn.PendingRejected,
			answer: agentturn.Approve, wantErr: agentturn.ErrCallAnswered},
		{name: "rejected, replayed", replay: agenttool.ReplaySafe, state: rejected, want: agentturn.PendingRejected,
			replayed: true, wantRecord: []string{"reject", "output"}},
		// #166: a call an earlier run dispatched runs again on a
		// decision, which names who approved it and the rule that let
		// it.
		{name: "in flight, safe, approved by a person", replay: agenttool.ReplaySafe, state: dispatched("k1"), want: agentturn.PendingAborted,
			answer:   func(id string) agentturn.Answer { return agentturn.Approve(id).WithBy(agentsession.ByHuman) },
			wantRuns: 1, wantRecord: []string{"dispatch", "proceed", "dispatch", "output"},
			wantBy: agentsession.ByHuman, wantReason: agentturn.RunAgainSafeReason},
		// #165: a keyed call runs again under the key of the dispatch
		// it repeats; another key is another operation.
		{name: "in flight, keyed, another key", replay: agenttool.ReplayKeyed, state: dispatched("k1"), want: agentturn.PendingAborted,
			answer:  func(id string) agentturn.Answer { return agentturn.Approve(id).WithIdempotencyKey("retry-1") },
			wantErr: agentturn.ErrAmbiguousCall},
		{name: "in flight without a key, keyed, a key", replay: agenttool.ReplayKeyed, state: dispatched(""), want: agentturn.PendingAborted,
			answer:  func(id string) agentturn.Answer { return agentturn.Approve(id).WithIdempotencyKey("retry-1") },
			wantErr: agentturn.ErrAmbiguousCall},
		{name: "in flight, keyed, another key, run again", replay: agenttool.ReplayKeyed, state: dispatched("k1"), want: agentturn.PendingAborted,
			answer: func(id string) agentturn.Answer {
				return agentturn.Approve(id).WithIdempotencyKey("retry-1").WithRunAgain().WithBy(agentsession.ByHuman)
			},
			wantRuns: 1, wantKey: "retry-1", wantRecord: []string{"dispatch", "proceed", "dispatch", "output"},
			wantBy: agentsession.ByHuman, wantReason: agentturn.RunAgainReason},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var keys []string
			charge := agenttool.New("charge", "", func(ctx context.Context, _ echoArgs) (string, error) {
				call, _ := agenttool.CallFrom(ctx)
				keys = append(keys, call.IdempotencyKey)
				return "charged", nil
			}, agenttool.WithReplay(func(context.Context, json.RawMessage) agenttool.Replay { return tc.replay }))

			store := agentsession.NewMemoryStore()
			s, err := store.Create(ctx, agentsession.Header{Records: agentsession.AllRecords})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Append(ctx, s.ID(), &agentsession.ItemEntry{Item: openresponses.UserText("charge me")}); err != nil {
				t.Fatal(err)
			}
			call := &openresponses.FunctionCall{CallID: "call_1", Name: "charge", Arguments: `{"text":"t"}`}
			target, err := store.Append(ctx, s.ID(), &agentsession.ItemEntry{Item: call})
			if err != nil {
				t.Fatal(err)
			}
			if tc.state != nil {
				for _, e := range tc.state(call.CallID, target) {
					if _, err := store.Append(ctx, s.ID(), e); err != nil {
						t.Fatal(err)
					}
				}
			}

			rec, s2, err := Resume(ctx, store, s.ID())
			if err != nil {
				t.Fatal(err)
			}
			pending, err := Pending(s2)
			if err != nil {
				t.Fatal(err)
			}
			if len(pending) != 1 || pending[0].Reason != tc.want {
				t.Fatalf("pending = %+v, want %s", pending, tc.want)
			}
			if p := pending[0]; p.Reason == agentturn.PendingDeferred && (!p.Dispatched || p.IdempotencyKey != "k1" || !p.MayHaveRun()) {
				t.Errorf("a hold after a dispatch = %+v", p)
			}
			opts, err := AgentOptions(s2)
			if err != nil {
				t.Fatal(err)
			}
			cfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{charge}}
			if tc.block {
				cfg.BeforeToolCall = func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
					return &agentturn.ToolDecision{Action: agentturn.Block, Reason: "denied by rm"}, nil
				}
			}
			a := agentturn.New(cfg, opts...)
			defer rec.Attach(a)()
			var answers []agentturn.Answer
			if tc.replayed {
				if answers, err = ReplayAnswers(ctx, s2, []agenttool.Tool{charge}); err != nil {
					t.Fatal(err)
				}
			} else {
				answers = []agentturn.Answer{tc.answer(call.CallID)}
			}
			_, err = a.Resume(ctx, answers...)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("resume: err = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr != nil {
				if len(keys) != 0 {
					t.Errorf("a refused approval ran the tool")
				}
				return
			}
			wantKey := tc.wantKey
			if wantKey == "" {
				wantKey = "k1"
			}
			if len(keys) != tc.wantRuns || tc.wantRuns > 0 && keys[0] != wantKey {
				t.Errorf("ran with keys %q, want %d under %s", keys, tc.wantRuns, wantKey)
			}
			var record []string
			var last *agentsession.DecisionEntry
			for _, e := range s2.Path(s2.Leaf()) {
				switch e := e.(type) {
				case *agentsession.DispatchEntry:
					if e.CallID == call.CallID {
						record = append(record, "dispatch")
						if e.IdempotencyKey != "k1" && e.IdempotencyKey != wantKey {
							t.Errorf("dispatch key %q, want k1 or %s", e.IdempotencyKey, wantKey)
						}
					}
				case *agentsession.DecisionEntry:
					record = append(record, e.Verdict)
					last = e
				case *agentsession.ItemEntry:
					if out, ok := e.Item.(*openresponses.FunctionCallOutput); ok && out.CallID == call.CallID {
						record = append(record, "output")
					}
				}
			}
			if strings.Join(record, " ") != strings.Join(tc.wantRecord, " ") {
				t.Errorf("record = %q, want %q", record, tc.wantRecord)
			}
			// The decision the resume wrote says who answered and why;
			// an answered call's is the one on the path already.
			if want := answers[0]; tc.want == agentturn.PendingDeferred && (last.By != want.By || last.Reason != want.Reason) {
				t.Errorf("%s by %q for %q, want by %q for %q", last.Verdict, last.By, last.Reason, want.By, want.Reason)
			}
			if tc.wantBy != "" && (last.By != tc.wantBy || last.Reason != tc.wantReason) {
				t.Errorf("%s by %q for %q, want by %q for %q", last.Verdict, last.By, last.Reason, tc.wantBy, tc.wantReason)
			}
			verifyAll(t, s2)
			if err := s2.VerifyRecords(s2.Leaf()); err != nil {
				t.Errorf("verify records: %v", err)
			}
		})
	}
}

// TestRebaseSeedsHeldCalls checks the flow Rebase documents: after a
// rewind to a point where a call was held, the agent's new transcript
// reads the call as unknown until SetPending says what the session
// knows, and then the call is approved as the held call it is.
func TestRebaseSeedsHeldCalls(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	ran := 0
	act := agenttool.New("act", "", func(context.Context, echoArgs) (string, error) {
		ran++
		return "done", nil
	})
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{act},
		BeforeToolCall: func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			return &agentturn.ToolDecision{Action: agentturn.Defer}, nil
		}})
	defer rec.Attach(a)()
	end, err := a.Prompt(context.Background(), openresponses.UserText("go"))
	if err != nil || end.Reason != agentturn.ReasonInputRequired {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	heldAt := s.Leaf()
	id := end.Pending[0].Call.CallID
	if _, err := a.Resume(context.Background(), agentturn.Output(openresponses.NewFunctionCallOutput(id, "declined"))); err != nil {
		t.Fatal(err)
	}

	if err := rec.Rebase(s, heldAt); err != nil {
		t.Fatal(err)
	}
	cx, err := s.Context()
	if err != nil {
		t.Fatal(err)
	}
	if err := a.SetTranscript(cx.Items); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Resume(context.Background(), agentturn.Approve(id)); !errors.Is(err, agentturn.ErrAmbiguousCall) {
		t.Fatalf("before SetPending: err = %v", err)
	}
	pending, err := Pending(s)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.SetPending(pending); err != nil {
		t.Fatal(err)
	}
	if p := a.State().Pending; len(p) != 1 || p[0].Reason != agentturn.PendingDeferred {
		t.Fatalf("pending = %+v", p)
	}
	if _, err := a.Resume(context.Background(), agentturn.Approve(id).WithBy(agentsession.ByHuman)); err != nil || ran != 1 {
		t.Fatalf("resume: err=%v ran %d", err, ran)
	}
	if c := callsOf(t, s)["act"]; c == nil || c.Dispatch == nil || c.Output == nil || c.Rejected() {
		t.Errorf("call = %+v", c)
	}
	verifyAll(t, s)
}

// TestRunAgainIsAlwaysDecided pins the recorder half of #165 and #166
// for a driver that approves without a reason: a call an earlier run
// dispatched that goes to its tool again gets a proceed naming who
// approved it, written before its second dispatch with a reason of
// the recorder's own, and one the loop then refuses before any
// dispatch gets none.
func TestRunAgainIsAlwaysDecided(t *testing.T) {
	cases := []struct {
		name       string
		dispatch   bool
		wantRecord []string
	}{
		{name: "dispatched again", dispatch: true, wantRecord: []string{"dispatch", "proceed", "dispatch", "output"}},
		{name: "refused before its dispatch", wantRecord: []string{"dispatch", "answer", "output"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := agentsession.NewMemoryStore()
			s, err := store.Create(ctx, agentsession.Header{Records: agentsession.AllRecords})
			if err != nil {
				t.Fatal(err)
			}
			call := &openresponses.FunctionCall{CallID: "call_1", Name: "charge", Arguments: `{"text":"t"}`}
			target, err := store.Append(ctx, s.ID(), &agentsession.ItemEntry{Item: call})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Append(ctx, s.ID(), agentsession.NewDispatch(call.CallID, target).WithIdempotencyKey("k1")); err != nil {
				t.Fatal(err)
			}
			rec, s2, err := Resume(ctx, store, s.ID())
			if err != nil {
				t.Fatal(err)
			}
			const runID = "run_again"
			events := []agentturn.Event{
				&agentturn.RunStart{RunID: runID, Source: agentturn.SourceResume},
				&agentturn.ToolStart{RunID: runID, CallID: call.CallID, Name: call.Name, Args: json.RawMessage(call.Arguments), Decision: &agentturn.ToolDecision{By: agentsession.ByHuman}},
			}
			if tc.dispatch {
				events = append(events, &agentturn.ToolDispatch{RunID: runID, CallID: call.CallID, Name: call.Name, IdempotencyKey: "k1"},
					&agentturn.ToolEnd{RunID: runID, CallID: call.CallID, Name: call.Name})
			} else {
				events = append(events, &agentturn.ToolEnd{RunID: runID, CallID: call.CallID, Name: call.Name, Err: errors.New("unknown tool")})
			}
			events = append(events, &agentturn.ItemEnd{RunID: runID, Item: openresponses.NewFunctionCallOutput(call.CallID, "done")},
				&agentturn.RunEnd{RunID: runID, Reason: agentturn.ReasonStopped, Cause: agentturn.StopRefused})
			for _, ev := range events {
				if err := rec.Handle(ctx, ev); err != nil {
					t.Fatal(err)
				}
			}
			var record []string
			for _, e := range s2.Path(s2.Leaf()) {
				switch e := e.(type) {
				case *agentsession.DispatchEntry:
					record = append(record, "dispatch")
				case *agentsession.DecisionEntry:
					record = append(record, e.Verdict)
					if e.Verdict == agentsession.VerdictProceed && (e.By != agentsession.ByHuman || e.Reason == "") {
						t.Errorf("proceed by %q for %q, want by human with a reason", e.By, e.Reason)
					}
				case *agentsession.ItemEntry:
					if _, ok := e.Item.(*openresponses.FunctionCallOutput); ok {
						record = append(record, "output")
					}
				}
			}
			if strings.Join(record, " ") != strings.Join(tc.wantRecord, " ") {
				t.Errorf("record = %q, want %q", record, tc.wantRecord)
			}
		})
	}
}
