package agentturn

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

func runStartOf(events []Event) *RunStart {
	for _, ev := range events {
		if e, ok := ev.(*RunStart); ok {
			return e
		}
	}
	return nil
}

func TestRunStartSourceAndTrigger(t *testing.T) {
	ctx := ContextWithTrigger(context.Background(), Trigger{Kind: "cron", Ref: "nightly"})
	cfg := Config{Model: &echo.Adapter{}}
	events, _, err := collect(t, Run(ctx, nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	rs := runStartOf(events)
	if rs == nil || rs.Source != SourceInput || rs.Trigger.Kind != "cron" || rs.Trigger.Ref != "nightly" || rs.Trigger.String() != "cron:nightly" {
		t.Errorf("run_start = %+v", rs)
	}
	if got := (Trigger{Ref: "msg-1"}).String(); got != "msg-1" || !(Trigger{}).IsZero() {
		t.Errorf("trigger strings: %q", got)
	}

	// A run without a trigger carries the zero value.
	events, _, _ = collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if rs := runStartOf(events); rs == nil || !rs.Trigger.IsZero() {
		t.Errorf("run_start without trigger = %+v", rs)
	}

	// A run that answers a pending call is a resume, whether through
	// Resume or through a prompt that opens with the outputs; the
	// prompt after that is an input again.
	deferAll := func(context.Context, ToolCallInfo) (*ToolDecision, error) { return &ToolDecision{Action: Defer}, nil }
	a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)}, BeforeToolCall: deferAll})
	rec := &recorder{}
	rec.subscribe(a)
	if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	pending := a.State().Pending
	if len(pending) != 1 || pending[0].Reason != PendingDeferred {
		t.Fatalf("pending = %+v", pending)
	}
	if _, err := a.Resume(context.Background(), Approve(pending[0].Call.CallID)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Prompt(context.Background(), openresponses.UserText("again")); err != nil {
		t.Fatal(err)
	}
	pending = a.State().Pending
	if _, err := a.Prompt(context.Background(), openresponses.NewFunctionCallOutput(pending[0].Call.CallID, "ABC"), openresponses.UserText("next")); err != nil {
		t.Fatal(err)
	}
	var sources []Source
	for _, ev := range rec.events {
		if e, ok := ev.(*RunStart); ok {
			sources = append(sources, e.Source)
		}
	}
	if want := []Source{SourceInput, SourceResume, SourceInput, SourceResume}; strings.Join(sourceStrings(sources), " ") != strings.Join(sourceStrings(want), " ") {
		t.Errorf("sources = %v, want %v", sources, want)
	}
}

func sourceStrings(s []Source) []string {
	out := make([]string, len(s))
	for i, v := range s {
		out[i] = string(v)
	}
	return out
}

func TestAbortAppendsFinishedOutputs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fast := agenttool.New("a", "", func(_ context.Context, _ echoArgs) (string, error) { return "wrote #1", nil })
	slow := agenttool.New("b", "", func(ctx context.Context, _ echoArgs) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	cfg := Config{Model: &twoCalls{}, Tools: []agenttool.Tool{fast, slow}}
	var events []Event
	var end *RunEnd
	for ev := range Run(ctx, nil, openresponses.Items{openresponses.UserText("x")}, cfg) {
		events = append(events, ev)
		// Abort once the fast call has finished; the slow one is still
		// blocked on the context.
		if e, ok := ev.(*ToolEnd); ok && e.Name == "a" {
			cancel()
		}
		if e, ok := ev.(*RunEnd); ok {
			end = e
		}
	}
	if end == nil || end.Reason != ReasonAborted {
		t.Fatalf("end = %+v", end)
	}
	// The finished call has its output in the transcript; the cut-off
	// one is the only pending call, and its reason says it was cut off.
	if got := itemTypes(end.Items); got != "user function_call function_call function_call_output" {
		t.Errorf("items = %q", got)
	}
	out := end.Items[3].(*openresponses.FunctionCallOutput)
	if out.CallID != end.Items[1].(*openresponses.FunctionCall).CallID || out.Output.Text != "wrote #1" {
		t.Errorf("output = %+v", out)
	}
	if len(end.Pending) != 1 || end.Pending[0].Call.Name != "b" || end.Pending[0].Reason != PendingAborted {
		t.Errorf("pending = %+v", end.Pending)
	}
	// The output's item events came after both tool_ends.
	seq := types(events)
	last := strings.Join(seq[len(seq)-4:], " ")
	if last != "tool_end item_start item_end run_end" && last != "item_start item_end tool_end run_end" {
		t.Errorf("events end = %v", seq)
	}
	if !CanContinue(append(Transcript(nil), end.Items...)) {
		// The transcript ends with an output, but the dangling call
		// still has to be answered before a run can continue.
		t.Error("transcript ending with an output should satisfy CanContinue")
	}
	if got := unansweredCalls(end.Items); len(got) != 1 || got[0].Name != "b" {
		t.Errorf("unanswered = %v", got)
	}
}

func TestPendingReasons(t *testing.T) {
	// Seeded: the loop cannot know.
	seed := Transcript{openresponses.UserText("x"), &openresponses.FunctionCall{CallID: "c1", Name: "upper", Arguments: "{}"}}
	a := New(Config{Model: &echo.Adapter{}}, WithTranscript(seed))
	if p := a.State().Pending; len(p) != 1 || p[0].Reason != PendingUnknown {
		t.Errorf("seeded pending = %+v", p)
	}
	// SetTranscript derives them the same way.
	b := New(Config{Model: &echo.Adapter{}})
	if err := b.SetTranscript(seed); err != nil {
		t.Fatal(err)
	}
	if p := b.State().Pending; len(p) != 1 || p[0].Reason != PendingUnknown {
		t.Errorf("set pending = %+v", p)
	}
	if got := PendingCalls(b.State().Pending); len(got) != 1 || got[0].CallID != "c1" {
		t.Errorf("PendingCalls = %v", got)
	}
	if PendingCalls(nil) != nil {
		t.Error("PendingCalls(nil) should be nil")
	}
	// A message appended after the dangling call hides nothing.
	c := New(Config{Model: &echo.Adapter{}}, WithTranscript(append(seed, openresponses.UserText("y"))))
	if p := c.State().Pending; len(p) != 1 {
		t.Errorf("pending behind a message = %+v", p)
	}
	if _, err := c.Continue(context.Background()); !errors.Is(err, ErrInputRequired) {
		t.Errorf("continue with a hidden dangling call: %v", err)
	}
}

func TestModelBlockedEvent(t *testing.T) {
	boom := errors.New("policy refused")
	cfg := Config{Model: &echo.Adapter{}, ModelName: "m", BeforeModelCall: func(context.Context, *openresponses.Request) error { return boom }}
	events, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("hello")}, cfg))
	if !errors.Is(err, boom) || end.Reason != ReasonError {
		t.Fatalf("err=%v end=%+v", err, end)
	}
	seq := types(events)
	if strings.Join(seq, " ") != "run_start item_start item_end model_blocked run_end" {
		t.Fatalf("events = %v", seq)
	}
	mb := events[3].(*ModelBlocked)
	if mb.Turn != 1 || !errors.Is(mb.Err, boom) || mb.Request.Model != "m" || len(mb.Request.Input) != 1 {
		t.Errorf("model_blocked = %+v", mb)
	}
}

func TestToolStartCarriesDecision(t *testing.T) {
	rewrite := json.RawMessage(`{"text":"rewritten"}`)
	cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)},
		BeforeToolCall: func(context.Context, ToolCallInfo) (*ToolDecision, error) {
			return &ToolDecision{Args: rewrite, By: "policy"}, nil
		}}
	events, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("abc")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	var ts *ToolStart
	for _, ev := range events {
		if e, ok := ev.(*ToolStart); ok {
			ts = e
		}
	}
	if ts == nil || ts.Decision == nil || string(ts.Decision.Args) != string(rewrite) || ts.Decision.By != "policy" || string(ts.Args) != string(rewrite) {
		t.Errorf("tool_start = %+v", ts)
	}
	// The function_call item keeps the model's arguments.
	if fc := end.Items[1].(*openresponses.FunctionCall); strings.Contains(fc.Arguments, "rewritten") {
		t.Errorf("function_call rewritten: %s", fc.Arguments)
	}
	if out := end.Items[2].(*openresponses.FunctionCallOutput); out.Output.Text != "REWRITTEN" {
		t.Errorf("tool ran with %q", out.Output.Text)
	}

	// Without a hook, Decision is nil; on an approval it carries the
	// caller's arguments.
	cfg.BeforeToolCall = nil
	events, _, _ = collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("abc")}, cfg))
	for _, ev := range events {
		if e, ok := ev.(*ToolStart); ok && e.Decision != nil {
			t.Errorf("tool_start without a hook has a decision: %+v", e.Decision)
		}
	}
	cfg.BeforeToolCall = func(context.Context, ToolCallInfo) (*ToolDecision, error) { return &ToolDecision{Action: Defer}, nil }
	a := New(cfg)
	if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	rec.subscribe(a)
	if _, err := a.Resume(context.Background(), ApproveWith(a.State().Pending[0].Call.CallID, rewrite)); err != nil {
		t.Fatal(err)
	}
	var approved *ToolStart
	for _, ev := range rec.events {
		if e, ok := ev.(*ToolStart); ok {
			approved = e
		}
	}
	if approved == nil || approved.Decision == nil || string(approved.Decision.Args) != string(rewrite) || approved.Decision.Action != Allow {
		t.Errorf("approved tool_start = %+v", approved)
	}
}

// TestAbortCause checks that a host's reason for cutting a run reaches
// RunEnd.Err, whether it aborted the agent or cancelled the context it
// prompted with, and that a bare abort still reads as a cancellation.
func TestAbortCause(t *testing.T) {
	rule := errors.New("ttsr: rule box-leak")
	blocking := agenttool.New("wait", "", func(ctx context.Context, _ echoArgs) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	newAgent := func(cut func(a *Agent, cancel context.CancelCauseFunc)) (*RunEnd, context.CancelCauseFunc) {
		cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{blocking}}
		a := New(cfg)
		ctx, cancel := context.WithCancelCause(context.Background())
		a.Subscribe(func(_ context.Context, ev Event) error {
			if _, ok := ev.(*ToolStart); ok {
				cut(a, cancel)
			}
			return nil
		})
		end, _ := a.Prompt(ctx, openresponses.UserText("go"))
		return end, cancel
	}
	t.Run("AbortCause names the rule that fired", func(t *testing.T) {
		end, cancel := newAgent(func(a *Agent, _ context.CancelCauseFunc) { a.AbortCause(rule) })
		defer cancel(nil)
		if end.Reason != ReasonAborted || !errors.Is(end.Err, rule) {
			t.Errorf("end = %+v", end)
		}
		// The cut-off call is the caller's to answer, as after any
		// abort: its output was not appended.
		if len(end.Pending) != 1 || end.Pending[0].Reason != PendingAborted {
			t.Errorf("pending = %+v", end.Pending)
		}
		if got := itemTypes(end.Items); got != "user function_call" {
			t.Errorf("items = %q", got)
		}
	})
	t.Run("a cause on the host's own context reaches the end", func(t *testing.T) {
		end, cancel := newAgent(func(_ *Agent, cancel context.CancelCauseFunc) { cancel(rule) })
		defer cancel(nil)
		if end.Reason != ReasonAborted || !errors.Is(end.Err, rule) {
			t.Errorf("end = %+v", end)
		}
	})
	t.Run("a bare abort is a cancellation", func(t *testing.T) {
		end, cancel := newAgent(func(a *Agent, _ context.CancelCauseFunc) { a.Abort() })
		defer cancel(nil)
		if end.Reason != ReasonAborted || !errors.Is(end.Err, context.Canceled) {
			t.Errorf("end = %+v", end)
		}
	})
}
