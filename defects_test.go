package agentturn

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// pairing counts tool_start and tool_end by call ID and reports the
// IDs that have a start without an end, or an end without a start.
func pairing(events []Event) (starts, ends int, unpaired []string) {
	open := map[string]int{}
	for _, ev := range events {
		switch e := ev.(type) {
		case *ToolStart:
			starts++
			open[e.CallID]++
		case *ToolEnd:
			ends++
			open[e.CallID]--
		}
	}
	for id, n := range open {
		if n != 0 {
			unpaired = append(unpaired, id)
		}
	}
	return starts, ends, unpaired
}

func pendingReasons(end *RunEnd) []PendingReason {
	var out []PendingReason
	for _, p := range end.Pending {
		out = append(out, p.Reason)
	}
	return out
}

// TestFailureMidBatchPairsEveryCall pins #101: whichever hook or
// consumer fails while a batch is in flight, every tool_start has its
// tool_end, the outputs of the calls that finished are appended and
// the rest are pending as aborted.
func TestFailureMidBatchPairsEveryCall(t *testing.T) {
	boom := errors.New("boom")
	blocking := func(ctx context.Context, _ echoArgs) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}
	fast := func(context.Context, echoArgs) (string, error) { return "a", nil }
	cases := []struct {
		name string
		cfg  func(*Config)
		// subscriber, when set, fails on the first event of that type.
		failOn string
		// starts is how many calls reach tool_start before the failure,
		// and pending how many the failure leaves without an output.
		starts  int
		pending int
		// items the run is expected to have appended after the calls.
		want string
	}{
		{
			name: "after-call hook fails on the fast call",
			cfg: func(c *Config) {
				c.AfterToolCall = func(_ context.Context, info ToolResultInfo) (*ToolOverride, error) {
					if info.Call.Name == "a" {
						return nil, boom
					}
					return nil, nil
				}
			},
			starts:  2,
			pending: 2,
			want:    "user function_call function_call",
		},
		{
			name: "before-call hook fails on the second call",
			cfg: func(c *Config) {
				c.BeforeToolCall = func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
					if info.Index == 1 {
						return nil, boom
					}
					return nil, nil
				}
			},
			starts:  1,
			pending: 2,
			want:    "user function_call function_call",
		},
		{
			// The fast call finished with a result of its own before
			// the consumer failed on its tool_end, so its output is
			// appended and only the cut call is pending.
			name:    "subscriber fails on the first tool_end",
			failOn:  EventToolEnd,
			starts:  2,
			pending: 1,
			want:    "user function_call function_call function_call_output",
		},
		{
			name:    "subscriber fails on the second tool_start",
			failOn:  EventToolStart,
			starts:  2,
			pending: 2,
			want:    "user function_call function_call",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Model: twoCalls{}, Tools: []agenttool.Tool{agenttool.New("a", "", fast), agenttool.New("b", "", blocking)}}
			if tc.cfg != nil {
				tc.cfg(&cfg)
			}
			a := New(cfg)
			var events []Event
			seen := 0
			a.Subscribe(func(_ context.Context, ev Event) error {
				events = append(events, ev)
				if ev.EventType() == tc.failOn {
					seen++
					if tc.failOn == EventToolStart && seen < 2 {
						return nil
					}
					return boom
				}
				return nil
			})
			end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
			if !errors.Is(err, boom) || end.Reason != ReasonError {
				t.Fatalf("run: err=%v reason=%s", err, end.Reason)
			}
			starts, ends, unpaired := pairing(events)
			if starts != tc.starts || ends != tc.starts || len(unpaired) != 0 {
				t.Errorf("pairing: starts=%d ends=%d unpaired=%v", starts, ends, unpaired)
			}
			if got := itemTypes(end.Items); got != tc.want {
				t.Errorf("items: got %q want %q", got, tc.want)
			}
			// A call handed to its tool may have run; one cut before
			// that did not.
			dispatched := map[string]bool{}
			for _, ev := range events {
				if d, ok := ev.(*ToolDispatch); ok {
					dispatched[d.CallID] = true
				}
			}
			for _, p := range end.Pending {
				want := PendingUndispatched
				if dispatched[p.Call.CallID] {
					want = PendingAborted
				}
				if p.Reason != want {
					t.Errorf("pending %s: reason %s, want %s", p.Call.CallID, p.Reason, want)
				}
			}
			if len(end.Pending) != tc.pending {
				t.Errorf("pending: %v, want %d", pendingReasons(end), tc.pending)
			}
		})
	}
}

// TestAbortPairsEveryCallWhenAConsumerFails pins the abort half of
// #101: a subscriber that fails on one cut call's tool_end does not
// leave the other cut calls without theirs.
func TestAbortPairsEveryCallWhenAConsumerFails(t *testing.T) {
	boom := errors.New("boom")
	started := make(chan struct{}, 2)
	blocking := func(ctx context.Context, _ echoArgs) (string, error) {
		started <- struct{}{}
		<-ctx.Done()
		return "", ctx.Err()
	}
	a := New(Config{Model: twoCalls{}, Tools: []agenttool.Tool{agenttool.New("a", "", blocking), agenttool.New("b", "", blocking)}})
	var mu sync.Mutex
	var events []Event
	a.Subscribe(func(_ context.Context, ev Event) error {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, ev)
		if e, ok := ev.(*ToolEnd); ok && e.Err != nil {
			return boom
		}
		return nil
	})
	go func() {
		<-started
		<-started
		a.Abort()
	}()
	end, _ := a.Prompt(context.Background(), openresponses.UserText("x"))
	if end.Reason != ReasonAborted || !errors.Is(end.Err, context.Canceled) || !errors.Is(end.Err, boom) {
		t.Fatalf("run: reason=%s err=%v", end.Reason, end.Err)
	}
	mu.Lock()
	defer mu.Unlock()
	if starts, ends, unpaired := pairing(events); starts != 2 || ends != 2 || len(unpaired) != 0 {
		t.Errorf("pairing: starts=%d ends=%d unpaired=%v", starts, ends, unpaired)
	}
}

// TestDecisionTerminateOnAnAllowedCall pins #102: a decision's
// Terminate composes with Allow for a call that runs, as the doc says,
// and the batch rule reads it as it reads a tool's own hint.
func TestDecisionTerminateOnAnAllowedCall(t *testing.T) {
	plain := func(context.Context, echoArgs) (string, error) { return "ok", nil }
	one := Config{Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{agenttool.New("upper", "", upper)},
		BeforeToolCall: func(context.Context, ToolCallInfo) (*ToolDecision, error) {
			return &ToolDecision{Action: Allow, Terminate: true}, nil
		}}
	events, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("abc")}, one))
	if err != nil || end.Reason != ReasonStopped || end.Cause != StopTerminate {
		t.Errorf("allow+terminate: err=%v reason=%s cause=%s", err, end.Reason, end.Cause)
	}
	for _, ev := range events {
		if e, ok := ev.(*ToolEnd); ok && !e.Result.Terminate {
			t.Errorf("tool_end for %s does not carry the hint", e.CallID)
		}
	}
	if got := itemTypes(end.Items); got != "user function_call function_call_output" {
		t.Errorf("items: %s", got)
	}
	// One decision of a two-call batch terminates: the batch did not
	// agree, and the run says so.
	some := Config{Model: twoCalls{}, Tools: []agenttool.Tool{agenttool.New("a", "", plain), agenttool.New("b", "", plain)}, MaxTurns: 2,
		BeforeToolCall: func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
			return &ToolDecision{Terminate: info.Call.Name == "a"}, nil
		}}
	_, end, err = collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, some))
	if err != nil || end.Reason != ReasonStopped || end.Cause != StopPartialTerminate {
		t.Errorf("partial: err=%v reason=%s cause=%s", err, end.Reason, end.Cause)
	}
	// An override does not clear the policy's hint.
	one.AfterToolCall = func(context.Context, ToolResultInfo) (*ToolOverride, error) {
		return &ToolOverride{Result: agenttool.Text("replaced")}, nil
	}
	_, end, err = collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("abc")}, one))
	if err != nil || end.Reason != ReasonStopped || end.Cause != StopTerminate {
		t.Errorf("overridden: err=%v reason=%s cause=%s", err, end.Reason, end.Cause)
	}
}

// errorEventModel answers its first attempts with a wire error event
// and the rest with a message. after, when set, opens a message before
// sending the error, so the attempt has committed.
type errorEventModel struct {
	fail  int
	after bool
	calls int
}

func (m *errorEventModel) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.calls++
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if m.calls <= m.fail {
		if m.after {
			msg, err := em.Message(openresponses.PhaseFinalAnswer)
			if err != nil {
				return err
			}
			if err := msg.Text("partial"); err != nil {
				return err
			}
		}
		return sink.Send(&openresponses.ErrorEvent{Status: 503, Error: openresponses.ErrorPayload{Type: "server_error", Code: "overloaded", Message: "overloaded"}})
	}
	msg, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if err := msg.Text("ok"); err != nil {
		return err
	}
	if err := msg.Close(); err != nil {
		return err
	}
	return em.Complete()
}

// TestRetryAfterAWireErrorEvent pins #103: an error event before the
// answer opens is a failed attempt and is retried; one after it is
// final.
func TestRetryAfterAWireErrorEvent(t *testing.T) {
	retry := Retry{MaxAttempts: 3, Backoff: func(int, error) time.Duration { return 0 }}
	m := &errorEventModel{fail: 1}
	events, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, Config{Model: m, Retry: retry}))
	if err != nil || end.Reason != ReasonDone || m.calls != 2 {
		t.Fatalf("before the answer: err=%v reason=%s calls=%d", err, end.Reason, m.calls)
	}
	retries := 0
	for _, ev := range events {
		if e, ok := ev.(*ModelRetry); ok {
			retries++
			var oe *openresponses.Error
			if !errors.As(e.Err, &oe) || oe.StatusCode != 503 {
				t.Errorf("model_retry carries %v, want the wire error", e.Err)
			}
		}
	}
	if retries != 1 || itemTypes(end.Items) != "user assistant" {
		t.Errorf("retries=%d items=%s", retries, itemTypes(end.Items))
	}
	// Exhausted: the wire error is the run's, unwrapped for the policy.
	m = &errorEventModel{fail: 3}
	_, end, err = collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, Config{Model: m, Retry: retry}))
	if end.Reason != ReasonError || m.calls != 3 || !strings.Contains(err.Error(), "overloaded") {
		t.Errorf("exhausted: err=%v reason=%s calls=%d", err, end.Reason, m.calls)
	}
	// After the answer opened, the failure is final.
	m = &errorEventModel{fail: 1, after: true}
	_, end, err = collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, Config{Model: m, Retry: retry}))
	if end.Reason != ReasonError || m.calls != 1 || err == nil {
		t.Errorf("after the answer: err=%v reason=%s calls=%d", err, end.Reason, m.calls)
	}
	// Without a policy, one attempt.
	m = &errorEventModel{fail: 1}
	_, end, _ = collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, Config{Model: m}))
	if end.Reason != ReasonError || m.calls != 1 {
		t.Errorf("no policy: reason=%s calls=%d", end.Reason, m.calls)
	}
}

// TestAbortWithFailingAfterCallHookPairsEveryCall pins the first
// blocker of the #104 review: an after-call hook that fails on a cut
// call, as one doing I/O on the cancelled context does, does not leave
// that call or the ones after it without a tool_end.
func TestAbortWithFailingAfterCallHookPairsEveryCall(t *testing.T) {
	boom := errors.New("hook boom")
	plain := func(context.Context, echoArgs) (string, error) { return "x", nil }
	hookCalls := 0
	a := New(Config{Model: twoCalls{}, Tools: []agenttool.Tool{agenttool.New("a", "", plain), agenttool.New("b", "", plain)},
		BeforeToolCall: func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
			return nil, nil
		},
		AfterToolCall: func(context.Context, ToolResultInfo) (*ToolOverride, error) {
			hookCalls++
			return nil, boom
		}})
	var events []Event
	a.Subscribe(func(_ context.Context, ev Event) error {
		events = append(events, ev)
		// Abort while the batch is being decided, so every call is
		// cut before it runs and settled by abortBatch.
		if e, ok := ev.(*ToolStart); ok && e.Name == "b" {
			a.Abort()
		}
		return nil
	})
	end, _ := a.Prompt(context.Background(), openresponses.UserText("x"))
	if end.Reason != ReasonAborted || !errors.Is(end.Err, boom) {
		t.Fatalf("run: reason=%s err=%v", end.Reason, end.Err)
	}
	if starts, ends, unpaired := pairing(events); starts != 2 || ends != 2 || len(unpaired) != 0 {
		t.Errorf("pairing: starts=%d ends=%d unpaired=%v", starts, ends, unpaired)
	}
	if hookCalls != 1 {
		t.Errorf("after-call hook ran %d times; once it has failed the remaining cut calls skip it", hookCalls)
	}
	if len(end.Pending) != 2 {
		t.Errorf("pending: %v", pendingReasons(end))
	}
}

// TestFailureMidBatchDrainsTheExecutor pins the second blocker: a
// failure stops the tools and the executor is drained before the batch
// is settled, so a tool's tool_end follows its return, the nested
// events it raises on the way out precede it, and a result it returns
// of its own is settled and appended rather than cut. The failure is a
// consumer's, so the hook still stands to judge the drained results.
func TestFailureMidBatchDrainsTheExecutor(t *testing.T) {
	boom := errors.New("consumer boom")
	// a returns only once b and c have started, so the consumer's
	// failure on a's tool_end cuts a running batch rather than one the
	// executor has not spawned yet. The wait is in the tool, not in the
	// subscriber: a subscriber holds the delivery barrier, which b's
	// and c's tool_dispatch need before their tools run.
	bStarted, cStarted := make(chan struct{}), make(chan struct{})
	var once sync.Once
	fast := agenttool.New("a", "", func(context.Context, echoArgs) (string, error) {
		<-bStarted
		<-cStarted
		return "a", nil
	})
	inner := agenttool.New("c", "", func(context.Context, echoArgs) (string, error) {
		once.Do(func() { close(cStarted) })
		return "c", nil
	})
	// b waits to be cut, then makes a nested call and returns a result
	// of its own: it handled the cancellation, as a tool may.
	slow := agenttool.New("b", "", func(ctx context.Context, _ echoArgs) (string, error) {
		close(bStarted)
		<-ctx.Done()
		// The nested call is refused under the cancelled context and
		// still raises its events as b's; b then answers on its own.
		_, _ = Invoke(ctx, "c", json.RawMessage(`{"text":"t"}`))
		return "b done", nil
	})
	cfg := Config{Model: twoCalls{}, Tools: []agenttool.Tool{fast, slow, inner}}
	// twoCalls calls every offered tool, c included, so c also runs as
	// a call of the batch; that one is plain and finishes on its own.
	a := New(cfg)
	var events []Event
	a.Subscribe(func(_ context.Context, ev Event) error {
		events = append(events, ev)
		if e, ok := ev.(*ToolEnd); ok && e.Name == "a" {
			return boom
		}
		return nil
	})
	end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
	if !errors.Is(err, boom) || end.Reason != ReasonError {
		t.Fatalf("run: err=%v reason=%s", err, end.Reason)
	}
	if starts, ends, unpaired := pairing(events); starts != 4 || ends != 4 || len(unpaired) != 0 {
		t.Errorf("pairing: starts=%d ends=%d unpaired=%v", starts, ends, unpaired)
	}
	// The nested call's events precede b's tool_end.
	var seq []string
	for _, ev := range events {
		switch e := ev.(type) {
		case *ToolStart:
			if e.Parent != "" {
				seq = append(seq, "nested_start")
			}
		case *ToolEnd:
			if e.Parent != "" {
				seq = append(seq, "nested_end")
			} else if e.Name == "b" {
				seq = append(seq, "end_b")
			}
		}
	}
	if len(seq) != 3 || seq[0] != "nested_start" || seq[1] != "nested_end" || seq[2] != "end_b" {
		t.Errorf("order: %v", seq)
	}
	// Every call finished with a result of its own: a's tool_end was
	// raised before the consumer refused it, and b and c returned
	// their own results during the drain; all three are appended and
	// nothing is pending, though the run failed.
	var outputs []string
	for _, it := range end.Items {
		if out, ok := it.(*openresponses.FunctionCallOutput); ok {
			outputs = append(outputs, out.Output.Text)
		}
	}
	if len(outputs) != 3 || !slices.Contains(outputs, "b done") {
		t.Errorf("outputs: %v, want a's, b's and c's", outputs)
	}
	if len(end.Pending) != 0 {
		t.Errorf("pending: %v, want none", pendingReasons(end))
	}
}

// TestNestedCallFailuresPair pins the nested site of #101: a failure
// of the loop's own around a nested call gives it its tool_end and
// returns to the tool, which decides its own result; the run goes on.
func TestNestedCallFailuresPair(t *testing.T) {
	boom := errors.New("boom")
	inner := agenttool.New("c", "", func(context.Context, echoArgs) (string, error) { return "c", nil })
	outer := agenttool.New("b", "", func(ctx context.Context, _ echoArgs) (string, error) {
		_, err := Invoke(ctx, "c", json.RawMessage(`{"text":"t"}`))
		if err != nil {
			return "b: " + err.Error(), nil
		}
		return "b ok", nil
	})
	cases := []struct {
		name   string
		cfg    func(*Config)
		failOn func(Event) bool
	}{
		{name: "after-call hook fails on the nested call", cfg: func(c *Config) {
			c.AfterToolCall = func(_ context.Context, info ToolResultInfo) (*ToolOverride, error) {
				if info.Call.Name == "c" {
					return nil, boom
				}
				return nil, nil
			}
		}},
		{name: "subscriber fails on the nested tool_start", failOn: func(ev Event) bool {
			e, ok := ev.(*ToolStart)
			return ok && e.Parent != ""
		}},
		{name: "subscriber fails on the nested tool_end", failOn: func(ev Event) bool {
			e, ok := ev.(*ToolEnd)
			return ok && e.Parent != ""
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The model calls b alone; c is offered so Invoke finds it.
			cfg := Config{Model: callsNamed{names: []string{"b"}}, Tools: []agenttool.Tool{outer, inner}, MaxTurns: 1}
			if tc.cfg != nil {
				tc.cfg(&cfg)
			}
			a := New(cfg)
			var events []Event
			a.Subscribe(func(_ context.Context, ev Event) error {
				events = append(events, ev)
				if tc.failOn != nil && tc.failOn(ev) {
					return boom
				}
				return nil
			})
			end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
			if err != nil || end.Reason != ReasonStopped {
				t.Fatalf("run: err=%v reason=%s", err, end.Reason)
			}
			if starts, ends, unpaired := pairing(events); starts != 2 || ends != 2 || len(unpaired) != 0 {
				t.Errorf("pairing: starts=%d ends=%d unpaired=%v", starts, ends, unpaired)
			}
			var bOut string
			for _, it := range end.Items {
				if out, ok := it.(*openresponses.FunctionCallOutput); ok && strings.HasPrefix(out.Output.Text, "b") {
					bOut = out.Output.Text
				}
			}
			if !strings.Contains(bOut, "boom") {
				t.Errorf("b did not see the failure: %q", bOut)
			}
		})
	}
}

// TestApprovedBatchFailuresPair pins the resume site of #101: a hook
// or a consumer that fails in the approved batch leaves no tool_start
// without its tool_end.
func TestApprovedBatchFailuresPair(t *testing.T) {
	boom := errors.New("boom")
	plain := func(context.Context, echoArgs) (string, error) { return "x", nil }
	cases := []struct {
		name   string
		cfg    func(*Config)
		failOn string
	}{
		{name: "after-call hook fails", cfg: func(c *Config) {
			c.AfterToolCall = func(context.Context, ToolResultInfo) (*ToolOverride, error) { return nil, boom }
		}},
		{name: "subscriber fails on tool_start", failOn: EventToolStart},
		{name: "subscriber fails on tool_end", failOn: EventToolEnd},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Model: twoCalls{}, Tools: []agenttool.Tool{agenttool.New("a", "", plain), agenttool.New("b", "", plain)},
				BeforeToolCall: func(context.Context, ToolCallInfo) (*ToolDecision, error) {
					return &ToolDecision{Action: Defer}, nil
				}}
			a := New(cfg)
			end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
			if err != nil || end.Reason != ReasonInputRequired || len(end.Pending) != 2 {
				t.Fatalf("first run: err=%v reason=%s", err, end.Reason)
			}
			if tc.cfg != nil {
				tc.cfg(&cfg)
				if err := a.SetConfig(cfg); err != nil {
					t.Fatal(err)
				}
			}
			var events []Event
			resumed := false
			a.Subscribe(func(_ context.Context, ev Event) error {
				if _, ok := ev.(*RunStart); ok {
					resumed = true
				}
				if !resumed {
					return nil
				}
				events = append(events, ev)
				if ev.EventType() == tc.failOn {
					return boom
				}
				return nil
			})
			end, err = a.Resume(context.Background(), Approve(end.Pending[0].Call.CallID), Approve(end.Pending[1].Call.CallID))
			if !errors.Is(err, boom) || end.Reason != ReasonError {
				t.Fatalf("resume: err=%v reason=%s", err, end.Reason)
			}
			starts, ends, unpaired := pairing(events)
			if starts == 0 || starts != ends || len(unpaired) != 0 {
				t.Errorf("pairing: starts=%d ends=%d unpaired=%v", starts, ends, unpaired)
			}
		})
	}
}

// TestDrainedResultsSeeTheHook pins the redaction rule of a failed
// batch: a result that arrives while the executor is drained goes
// through the after-call hook when a consumer caused the failure, and
// is cut when the hook itself did, so nothing the hook would have
// replaced reaches tool_end, a recorder or the transcript.
func TestDrainedResultsSeeTheHook(t *testing.T) {
	boom := errors.New("boom")
	for _, hookFails := range []bool{false, true} {
		name := "subscriber fails"
		if hookFails {
			name = "hook fails"
		}
		t.Run(name, func(t *testing.T) {
			bStarted := make(chan struct{})
			// a returns once b has started; the wait is in the tool so
			// that no subscriber holds the delivery barrier b's
			// tool_dispatch needs.
			fast := agenttool.New("a", "", func(context.Context, echoArgs) (string, error) {
				<-bStarted
				return "a", nil
			})
			slow := agenttool.New("b", "", func(ctx context.Context, _ echoArgs) (string, error) {
				close(bStarted)
				<-ctx.Done()
				if !errors.Is(context.Cause(ctx), boom) {
					t.Errorf("b sees cause %v, want the failure", context.Cause(ctx))
				}
				return "SECRET-TOKEN", nil
			})
			cfg := Config{Model: twoCalls{}, Tools: []agenttool.Tool{fast, slow},
				AfterToolCall: func(_ context.Context, info ToolResultInfo) (*ToolOverride, error) {
					if info.Call.Name == "a" {
						if hookFails {
							return nil, boom
						}
						return nil, nil
					}
					return &ToolOverride{Result: agenttool.Text("[redacted]")}, nil
				}}
			a := New(cfg)
			var events []Event
			a.Subscribe(func(_ context.Context, ev Event) error {
				events = append(events, ev)
				if e, ok := ev.(*ToolEnd); ok && e.Name == "a" && !hookFails {
					return boom
				}
				return nil
			})
			end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
			if !errors.Is(err, boom) || end.Reason != ReasonError {
				t.Fatalf("run: err=%v reason=%s", err, end.Reason)
			}
			if starts, ends, unpaired := pairing(events); starts != 2 || ends != 2 || len(unpaired) != 0 {
				t.Errorf("pairing: starts=%d ends=%d unpaired=%v", starts, ends, unpaired)
			}
			for _, ev := range events {
				if e, ok := ev.(*ToolEnd); ok && strings.Contains(e.Result.Output.Text, "SECRET") {
					t.Errorf("tool_end for %s carries the unredacted output", e.Name)
				}
			}
			var bOut string
			for _, it := range end.Items {
				if out, ok := it.(*openresponses.FunctionCallOutput); ok {
					bOut = out.Output.Text
				}
			}
			if strings.Contains(bOut, "SECRET") {
				t.Errorf("transcript carries the unredacted output: %q", bOut)
			}
			if hookFails {
				if bOut != "" || len(end.Pending) != 2 {
					t.Errorf("hook failed: output %q pending %v, want b cut", bOut, pendingReasons(end))
				}
			} else if bOut != "[redacted]" || len(end.Pending) != 0 {
				// a's tool_end was raised before the consumer refused
				// it, so a is finished too, and nothing is pending.
				t.Errorf("subscriber failed: output %q pending %v, want b redacted and appended", bOut, pendingReasons(end))
			}
		})
	}
}

// TestPanicStartingARunReleasesTheAgent pins that a panic while a run
// starts, from a tool the configuration holds, leaves the agent usable
// by a caller that recovers it rather than holding its lock for good,
// whichever call started the run.
func TestPanicStartingARunReleasesTheAgent(t *testing.T) {
	tests := []struct {
		name  string
		start func(*Agent) error
	}{
		{"prompt", func(a *Agent) error {
			_, err := a.Prompt(context.Background(), openresponses.UserText("hi"))
			return err
		}},
		{"deliver", func(a *Agent) error {
			_, _, err := a.Deliver(context.Background(), openresponses.UserText("hi"))
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{nil}})
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("a run with a nil tool did not panic")
					}
				}()
				_ = tt.start(a)
			}()
			done := make(chan error, 1)
			go func() {
				if err := a.SetConfig(Config{Model: &echo.Adapter{}}); err != nil {
					done <- err
					return
				}
				done <- tt.start(a)
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("run after the panic: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the agent's lock was held after the panic")
			}
		})
	}
}
