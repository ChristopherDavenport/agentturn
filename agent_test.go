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

type recorder struct {
	mu     sync.Mutex
	events []Event
}

func (r *recorder) subscribe(a *Agent) {
	a.Subscribe(func(_ context.Context, ev Event) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.events = append(r.events, ev)
		return nil
	})
}

func (r *recorder) types() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return types(r.events)
}

func TestAgentPromptAndState(t *testing.T) {
	a := New(Config{Model: &echo.Adapter{}, ModelName: "m"})
	rec := &recorder{}
	rec.subscribe(a)
	if _, err := a.Prompt(context.Background(), openresponses.UserText("hi")); err != nil {
		t.Fatal(err)
	}
	st := a.State()
	if st.Running || itemTypes(st.Transcript) != "user assistant" || st.RunID == "" || st.Turn != 1 {
		t.Errorf("state = %+v", st)
	}
	got := rec.types()
	if got[0] != "run_start" || got[len(got)-1] != "run_end" {
		t.Errorf("events = %v", got)
	}
	// A second prompt continues the same transcript.
	if _, err := a.Prompt(context.Background(), openresponses.UserText("again")); err != nil {
		t.Fatal(err)
	}
	if got := itemTypes(a.State().Transcript); got != "user assistant user assistant" {
		t.Errorf("transcript = %q", got)
	}
	if _, err := a.Prompt(context.Background()); !errors.Is(err, ErrNoPrompt) {
		t.Errorf("empty prompt err = %v", err)
	}
	if _, err := a.Continue(context.Background()); !errors.Is(err, ErrCannotContinue) {
		t.Errorf("continue after assistant err = %v", err)
	}
	if _, err := New(Config{}).Prompt(context.Background(), openresponses.UserText("x")); !errors.Is(err, ErrNoModel) {
		t.Errorf("no model err = %v", err)
	}
}

func TestAgentContinueAndWithTranscript(t *testing.T) {
	a := New(Config{Model: &echo.Adapter{}}, WithTranscript(Transcript{openresponses.UserText("resume me")}))
	if _, err := a.Continue(context.Background()); err != nil {
		t.Fatal(err)
	}
	tr := a.State().Transcript
	if itemTypes(tr) != "user assistant" || tr[1].(*openresponses.Message).Text() != "resume me" {
		t.Errorf("transcript = %q", itemTypes(tr))
	}
}

func TestAgentBarrier(t *testing.T) {
	// A slow subscriber on the assistant item_end must delay tool
	// preflight: tool_start cannot be delivered until it returns.
	const delay = 50 * time.Millisecond
	a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)}})
	var mu sync.Mutex
	var order []string
	var itemEndReturned, toolStarted time.Time
	a.Subscribe(func(_ context.Context, ev Event) error {
		mu.Lock()
		defer mu.Unlock()
		switch e := ev.(type) {
		case *ItemEnd:
			if _, ok := e.Item.(*openresponses.FunctionCall); ok {
				time.Sleep(delay)
				itemEndReturned = time.Now()
				order = append(order, "slow-item_end")
			}
		case *ToolStart:
			toolStarted = time.Now()
			order = append(order, "first-tool_start")
		}
		return nil
	})
	a.Subscribe(func(_ context.Context, ev Event) error {
		mu.Lock()
		defer mu.Unlock()
		if _, ok := ev.(*ToolStart); ok {
			order = append(order, "second-tool_start")
		}
		return nil
	})
	if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	if toolStarted.Before(itemEndReturned) {
		t.Errorf("tool_start delivered %v before the slow item_end subscriber returned", itemEndReturned.Sub(toolStarted))
	}
	want := []string{"slow-item_end", "first-tool_start", "second-tool_start"}
	for i, w := range want {
		if i >= len(order) || order[i] != w {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

func TestAgentSubscriberErrorEndsRun(t *testing.T) {
	boom := errors.New("write failed")
	a := New(Config{Model: &echo.Adapter{}})
	rec := &recorder{}
	rec.subscribe(a)
	a.Subscribe(func(_ context.Context, ev Event) error {
		if _, ok := ev.(*ItemEnd); ok {
			return boom
		}
		return nil
	})
	end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
	if !errors.Is(err, boom) || end == nil || end.Reason != ReasonError || !errors.Is(end.Err, boom) {
		t.Fatalf("err = %v end = %+v", err, end)
	}
	got := rec.types()
	if got[len(got)-1] != "run_end" {
		t.Errorf("events = %v", got)
	}
	if st := a.State(); st.Running || len(st.Transcript) != 1 {
		t.Errorf("state = %+v", st)
	}
}

func TestAgentSteer(t *testing.T) {
	a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)}, MaxTurns: 3})
	a.Subscribe(func(_ context.Context, ev Event) error {
		if e, ok := ev.(*ToolStart); ok && e.Turn == 1 {
			a.Steer(openresponses.UserText("steered"))
			if a.State().Steering != 1 {
				t.Error("steer not queued")
			}
		}
		return nil
	})
	if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	tr := a.State().Transcript
	if got := itemTypes(tr); got != "user function_call function_call_output user function_call function_call_output assistant" {
		t.Fatalf("transcript = %q", got)
	}
	if tr[3].(*openresponses.Message).Text() != "steered" {
		t.Error("steer message not in place")
	}
	if a.State().Steering != 0 {
		t.Error("steer queue not drained")
	}
}

func TestAgentFollowUp(t *testing.T) {
	a := New(Config{Model: &echo.Adapter{}})
	a.FollowUp(openresponses.UserText("and then"))
	rec := &recorder{}
	rec.subscribe(a)
	if _, err := a.Prompt(context.Background(), openresponses.UserText("first")); err != nil {
		t.Fatal(err)
	}
	tr := a.State().Transcript
	if got := itemTypes(tr); got != "user assistant user assistant" {
		t.Fatalf("transcript = %q", got)
	}
	if tr[3].(*openresponses.Message).Text() != "and then" {
		t.Error("follow-up not answered")
	}
	runEnds := 0
	for _, typ := range rec.types() {
		if typ == "run_end" {
			runEnds++
		}
	}
	if runEnds != 1 {
		t.Errorf("run_end count = %d", runEnds)
	}
}

func TestAgentAbortAndIdle(t *testing.T) {
	blocking := agenttool.New("upper", "", func(ctx context.Context, _ echoArgs) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{blocking}})
	started := make(chan struct{})
	a.Subscribe(func(_ context.Context, ev Event) error {
		if _, ok := ev.(*ToolStart); ok {
			close(started)
		}
		return nil
	})
	done := make(chan *RunEnd, 1)
	go func() {
		end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
		if err != nil {
			t.Errorf("aborted prompt returned an error: %v", err)
		}
		done <- end
	}()
	<-started
	if !a.State().Running {
		t.Error("agent should be running")
	}
	if _, err := a.Prompt(context.Background(), openresponses.UserText("y")); !errors.Is(err, ErrRunning) {
		t.Errorf("concurrent prompt err = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if err := a.WaitForIdle(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("WaitForIdle while running = %v", err)
	}
	cancel()
	a.Abort()
	if end := <-done; end == nil || end.Reason != ReasonAborted || !errors.Is(end.Err, context.Canceled) {
		t.Errorf("aborted run end = %+v", end)
	}
	if err := a.WaitForIdle(context.Background()); err != nil {
		t.Error(err)
	}
	st := a.State()
	if st.Running || itemTypes(st.Transcript) != "user function_call" {
		t.Errorf("state = %+v", st)
	}
	// Abort when idle is a no-op.
	a.Abort()

	// The cut-off call is pending: Prompt and Continue refuse, Resume
	// answers it and the run completes.
	if len(st.Pending) != 1 || st.Pending[0].Name != "upper" {
		t.Fatalf("pending = %v", st.Pending)
	}
	if _, err := a.Prompt(context.Background(), openresponses.UserText("z")); !errors.Is(err, ErrInputRequired) {
		t.Errorf("prompt after abort err = %v", err)
	}
	if _, err := a.Continue(context.Background()); !errors.Is(err, ErrInputRequired) {
		t.Errorf("continue after abort err = %v", err)
	}
	out := &openresponses.FunctionCallOutput{CallID: st.Pending[0].CallID, Output: openresponses.FunctionCallOutputData{Text: "Error: aborted"}}
	if _, err := a.Resume(context.Background(), out); err != nil {
		t.Fatalf("resume: %v", err)
	}
	st = a.State()
	if len(st.Pending) != 0 || itemTypes(st.Transcript) != "user function_call function_call_output assistant" {
		t.Errorf("state after resume = %q pending=%v", itemTypes(st.Transcript), st.Pending)
	}

	// A transcript seeded with the same dangling call is pending too.
	b := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{blocking}}, WithTranscript(st.Transcript[:2]))
	if p := b.State().Pending; len(p) != 1 || p[0].CallID != out.CallID {
		t.Errorf("seeded pending = %v", p)
	}
	if _, err := b.Prompt(context.Background(), openresponses.UserText("z")); !errors.Is(err, ErrInputRequired) {
		t.Errorf("seeded prompt err = %v", err)
	}
}

func TestAgentUnsubscribe(t *testing.T) {
	a := New(Config{Model: &echo.Adapter{}})
	calls := 0
	unsub := a.Subscribe(func(context.Context, Event) error { calls++; return nil })
	unsub()
	unsub()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("x")); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Errorf("unsubscribed subscriber called %d times", calls)
	}
}

func TestAgentResume(t *testing.T) {
	a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)},
		BeforeToolCall: func(context.Context, ToolCallInfo) (*ToolDecision, error) { return &ToolDecision{Action: Defer}, nil }})
	if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	st := a.State()
	if len(st.Pending) != 1 || itemTypes(st.Transcript) != "user function_call" {
		t.Fatalf("state = %+v", st)
	}
	call := st.Pending[0]
	if _, err := a.Prompt(context.Background(), openresponses.UserText("more")); !errors.Is(err, ErrInputRequired) {
		t.Errorf("prompt while pending = %v", err)
	}
	if _, err := a.Continue(context.Background()); !errors.Is(err, ErrInputRequired) {
		t.Errorf("continue while pending = %v", err)
	}
	if _, err := a.Resume(context.Background(), openresponses.NewFunctionCallOutput("other", "x")); !errors.Is(err, ErrNotPending) {
		t.Errorf("resume with unknown call = %v", err)
	}
	if _, err := a.Resume(context.Background()); !errors.Is(err, ErrNotPending) {
		t.Errorf("resume with nothing = %v", err)
	}
	if _, err := New(Config{Model: &echo.Adapter{}}).Resume(context.Background()); !errors.Is(err, ErrNotPending) {
		t.Errorf("resume while nothing pending = %v", err)
	}
	if len(a.State().Pending) != 1 {
		t.Fatal("a rejected resume must leave the call pending")
	}
	rec := &recorder{}
	rec.subscribe(a)
	if _, err := a.Resume(context.Background(), openresponses.NewFunctionCallOutput(call.CallID, "ABC")); err != nil {
		t.Fatal(err)
	}
	st = a.State()
	if len(st.Pending) != 0 || itemTypes(st.Transcript) != "user function_call function_call_output assistant" {
		t.Errorf("state after resume = %+v", st)
	}
	if st.Transcript[3].(*openresponses.Message).Text() != "Tool result: ABC" {
		t.Error("model did not see the output")
	}
	got := rec.types()
	if got[0] != "run_start" || got[1] != "item_start" || got[2] != "item_end" {
		t.Errorf("resume events = %v", got[:3])
	}
	// Answering twice is refused.
	if _, err := a.Resume(context.Background(), openresponses.NewFunctionCallOutput(call.CallID, "ABC")); !errors.Is(err, ErrNotPending) {
		t.Errorf("resume after resume = %v", err)
	}
}

func TestAgentPromptReturnsRunEnd(t *testing.T) {
	a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)},
		BeforeToolCall: func(context.Context, ToolCallInfo) (*ToolDecision, error) { return &ToolDecision{Action: Defer}, nil }})
	end, err := a.Prompt(context.Background(), openresponses.UserText("abc"))
	if err != nil || end == nil || end.Reason != ReasonInputRequired || len(end.Pending) != 1 || itemTypes(end.Items) != "user function_call" {
		t.Fatalf("paused prompt: err=%v end=%+v", err, end)
	}
	end, err = a.Resume(context.Background(), openresponses.NewFunctionCallOutput(end.Pending[0].CallID, "ABC"))
	if err != nil || end.Reason != ReasonDone || itemTypes(end.Items) != "function_call_output assistant" {
		t.Fatalf("resumed: err=%v end=%+v", err, end)
	}
	b := New(Config{Model: &echo.Adapter{}, MaxTurns: 1})
	if end, err := b.Prompt(context.Background(), openresponses.UserText("x")); err != nil || end.Reason != ReasonDone {
		t.Errorf("done: err=%v end=%+v", err, end)
	}
}

func TestAgentSetConfigAndSetTranscript(t *testing.T) {
	a := New(Config{Model: &echo.Adapter{}, ModelName: "one"})
	var models []string
	a.Subscribe(func(_ context.Context, ev Event) error {
		if e, ok := ev.(*TurnStart); ok {
			models = append(models, e.Request.Model)
		}
		return nil
	})
	if _, err := a.Prompt(context.Background(), openresponses.UserText("x")); err != nil {
		t.Fatal(err)
	}
	cfg := a.Config()
	cfg.ModelName = "two"
	if err := a.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	// Subscribers survive the change and see the new model.
	if _, err := a.Prompt(context.Background(), openresponses.UserText("y")); err != nil {
		t.Fatal(err)
	}
	if strings.Join(models, ",") != "one,two" {
		t.Errorf("models = %v", models)
	}
	// Switching branch: a transcript with a dangling call is pending.
	branch := Transcript{openresponses.UserText("b"), &openresponses.FunctionCall{CallID: "c1", Name: "f"}}
	if err := a.SetTranscript(branch); err != nil {
		t.Fatal(err)
	}
	if st := a.State(); itemTypes(st.Transcript) != "user function_call" || len(st.Pending) != 1 || st.Pending[0].CallID != "c1" {
		t.Errorf("state after SetTranscript = %+v", st)
	}
	if err := a.SetTranscript(Transcript{openresponses.UserText("clean")}); err != nil {
		t.Fatal(err)
	}
	if st := a.State(); len(st.Pending) != 0 {
		t.Errorf("pending after clean SetTranscript = %v", st.Pending)
	}
	// Both refuse during a run.
	blocking := agenttool.New("wait", "", func(ctx context.Context, _ echoArgs) (string, error) { <-ctx.Done(); return "", ctx.Err() })
	cfg.Tools = []agenttool.Tool{blocking}
	if err := a.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	a.Subscribe(func(_ context.Context, ev Event) error {
		if _, ok := ev.(*ToolStart); ok {
			close(started)
		}
		return nil
	})
	go func() { _, _ = a.Continue(context.Background()) }()
	<-started
	if err := a.SetConfig(cfg); !errors.Is(err, ErrRunning) {
		t.Errorf("SetConfig while running = %v", err)
	}
	if err := a.SetTranscript(nil); !errors.Is(err, ErrRunning) {
		t.Errorf("SetTranscript while running = %v", err)
	}
	a.Abort()
	if err := a.WaitForIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The zero Agent is idle.
	var zero Agent
	if err := zero.WaitForIdle(context.Background()); err != nil {
		t.Errorf("zero WaitForIdle = %v", err)
	}
}
