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
// proceed saying why, and the one that was not has neither.
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
		first[name] = DispatchKey(c.Dispatch)
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
			if len(ds) != 2 || DispatchKey(ds[1]) != keys[name][1] {
				t.Errorf("%s: %d dispatches", name, len(ds))
			}
			if len(c.Decisions) != 1 || c.Decisions[0].Verdict != agentsession.VerdictProceed || c.Decisions[0].By != agentsession.ByPolicy || !strings.HasPrefix(c.Decisions[0].Reason, "run again") {
				t.Errorf("%s: decisions = %+v", name, c.Decisions)
			}
		case "notify":
			if len(ds) != 1 || len(c.Decisions) != 0 {
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
// started is approved, a held one is left to the caller, and one that
// may have run is approved only as its tool's replay allows.
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
				if answers[0].Output != nil {
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
		d := agentsession.NewDispatch(callID, target)
		if key != "" {
			d.Unknown = map[string]json.RawMessage{IdempotencyKeyMember: json.RawMessage(`"` + key + `"`)}
		}
		return []agentsession.Entry{d}
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
