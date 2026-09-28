package agentturn

import (
	"context"
	"errors"
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
			for _, p := range end.Pending {
				if p.Reason != PendingAborted {
					t.Errorf("pending %s: reason %s, want aborted", p.Call.CallID, p.Reason)
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
	some := Config{Model: twoCalls{}, Tools: []agenttool.Tool{agenttool.New("a", "", plain), agenttool.New("b", "", plain)},
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
