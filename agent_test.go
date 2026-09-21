package agentturn

import (
	"context"
	"encoding/json"
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
			_ = a.Steer(context.Background(), openresponses.UserText("steered"))
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
	_ = a.FollowUp(context.Background(), openresponses.UserText("and then"))
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
	// run_end reaches subscribers with a live context even after Abort,
	// so a recorder can still write on it.
	var endCtxErr error
	a.Subscribe(func(ctx context.Context, ev Event) error {
		if _, ok := ev.(*RunEnd); ok {
			endCtxErr = ctx.Err()
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
	if endCtxErr != nil {
		t.Errorf("run_end delivered with a cancelled context: %v", endCtxErr)
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
	if len(st.Pending) != 1 || st.Pending[0].Call.Name != "upper" {
		t.Fatalf("pending = %v", st.Pending)
	}
	if _, err := a.Prompt(context.Background(), openresponses.UserText("z")); !errors.Is(err, ErrInputRequired) {
		t.Errorf("prompt after abort err = %v", err)
	}
	if _, err := a.Continue(context.Background()); !errors.Is(err, ErrInputRequired) {
		t.Errorf("continue after abort err = %v", err)
	}
	out := &openresponses.FunctionCallOutput{CallID: st.Pending[0].Call.CallID, Output: openresponses.FunctionCallOutputData{Text: "Error: aborted"}}
	if _, err := a.Resume(context.Background(), Output(out)); err != nil {
		t.Fatalf("resume: %v", err)
	}
	st = a.State()
	if len(st.Pending) != 0 || itemTypes(st.Transcript) != "user function_call function_call_output assistant" {
		t.Errorf("state after resume = %q pending=%v", itemTypes(st.Transcript), st.Pending)
	}

	// A transcript seeded with the same dangling call is pending too.
	b := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{blocking}}, WithTranscript(st.Transcript[:2]))
	if p := b.State().Pending; len(p) != 1 || p[0].Call.CallID != out.CallID {
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
	call := st.Pending[0].Call
	if _, err := a.Prompt(context.Background(), openresponses.UserText("more")); !errors.Is(err, ErrInputRequired) {
		t.Errorf("prompt while pending = %v", err)
	}
	if _, err := a.Continue(context.Background()); !errors.Is(err, ErrInputRequired) {
		t.Errorf("continue while pending = %v", err)
	}
	if _, err := a.Resume(context.Background(), Output(openresponses.NewFunctionCallOutput("other", "x"))); !errors.Is(err, ErrNotPending) {
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
	if _, err := a.Resume(context.Background(), Output(openresponses.NewFunctionCallOutput(call.CallID, "ABC"))); err != nil {
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
	if _, err := a.Resume(context.Background(), Output(openresponses.NewFunctionCallOutput(call.CallID, "ABC"))); !errors.Is(err, ErrNotPending) {
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
	end, err = a.Resume(context.Background(), Output(openresponses.NewFunctionCallOutput(end.Pending[0].Call.CallID, "ABC")))
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
	if st := a.State(); itemTypes(st.Transcript) != "user function_call" || len(st.Pending) != 1 || st.Pending[0].Call.CallID != "c1" {
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

func TestAgentAbortDeliversEventsWithLiveContext(t *testing.T) {
	blocking := agenttool.New("upper", "", func(ctx context.Context, _ echoArgs) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	newAgent := func() (*Agent, chan struct{}) {
		a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{blocking}})
		started := make(chan struct{})
		a.Subscribe(func(_ context.Context, ev Event) error {
			if _, ok := ev.(*ToolStart); ok {
				close(started)
			}
			return nil
		})
		return a, started
	}
	abortDuringTool := func(t *testing.T, a *Agent, started chan struct{}) *RunEnd {
		t.Helper()
		done := make(chan *RunEnd, 1)
		go func() {
			end, _ := a.Prompt(context.Background(), openresponses.UserText("x"))
			done <- end
		}()
		<-started
		a.Abort()
		return <-done
	}

	// Every event after the abort, the tool_end of the cut-off call
	// included, reaches a subscriber with a context it can still use, and
	// a later subscriber sees the tool_end too.
	a, started := newAgent()
	var ctxErrs []error
	var seen []string
	a.Subscribe(func(ctx context.Context, ev Event) error {
		switch ev.(type) {
		case *ToolEnd, *TurnEnd, *RunEnd:
			ctxErrs = append(ctxErrs, ctx.Err())
		}
		return nil
	})
	a.Subscribe(func(_ context.Context, ev Event) error {
		if e, ok := ev.(*ToolEnd); ok {
			seen = append(seen, e.Name)
		}
		return nil
	})
	end := abortDuringTool(t, a, started)
	if end == nil || end.Reason != ReasonAborted || !errors.Is(end.Err, context.Canceled) {
		t.Fatalf("end = %+v", end)
	}
	for i, err := range ctxErrs {
		if err != nil {
			t.Errorf("event %d after abort delivered with a cancelled context: %v", i, err)
		}
	}
	if len(seen) != 1 || seen[0] != "upper" {
		t.Errorf("second subscriber saw tool_end for %v", seen)
	}

	// A subscriber that fails for a reason of its own during the abort
	// ends the run aborted with its error joined to the context error.
	a, started = newAgent()
	boom := errors.New("disk full")
	a.Subscribe(func(_ context.Context, ev Event) error {
		if _, ok := ev.(*ToolEnd); ok {
			return boom
		}
		return nil
	})
	end = abortDuringTool(t, a, started)
	if end == nil || end.Reason != ReasonAborted || !errors.Is(end.Err, context.Canceled) || !errors.Is(end.Err, boom) {
		t.Errorf("end = %+v", end)
	}
}

func TestAgentResumeApproves(t *testing.T) {
	deferAll := func(context.Context, ToolCallInfo) (*ToolDecision, error) { return &ToolDecision{Action: Defer}, nil }
	var after []string
	cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)}, BeforeToolCall: deferAll,
		AfterToolCall: func(_ context.Context, info ToolResultInfo) (*ToolOverride, error) {
			after = append(after, info.Call.Name)
			return nil, nil
		}}
	a := New(cfg)
	if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	call := a.State().Pending[0].Call
	rec := &recorder{}
	rec.subscribe(a)
	// The approved call runs inside the loop: its tool events fire with
	// Turn 0, AfterToolCall runs, the output is appended and the model
	// sees it. BeforeToolCall is not consulted again.
	end, err := a.Resume(context.Background(), Approve(call.CallID))
	if err != nil || end.Reason != ReasonDone {
		t.Fatalf("resume: err=%v end=%+v", err, end)
	}
	st := a.State()
	if itemTypes(st.Transcript) != "user function_call function_call_output assistant" || st.Transcript[3].(*openresponses.Message).Text() != "Tool result: ABC" {
		t.Errorf("transcript = %q", itemTypes(st.Transcript))
	}
	got := rec.types()
	want := []string{"run_start", "tool_start", "tool_end", "item_start", "item_end", "turn_start"}
	for i, w := range want {
		if i >= len(got) || got[i] != w {
			t.Fatalf("resume events = %v, want prefix %v", got, want)
		}
	}
	for _, ev := range rec.events {
		if e, ok := ev.(*ToolEnd); ok && (e.Turn != 0 || e.Err != nil || e.Result.Output.Text != "ABC") {
			t.Errorf("approved tool_end = %+v", e)
		}
	}
	if len(after) != 1 || after[0] != "upper" {
		t.Errorf("AfterToolCall ran for %v", after)
	}

	// ApproveWith rewrites the arguments; Output and Approve mix in one
	// batch and the outputs land in the calls' order.
	cfg.Model = &twoCalls{}
	cfg.Tools = []agenttool.Tool{agenttool.New("a", "", upper), agenttool.New("b", "", upper)}
	cfg.MaxTurns = 1
	a = New(cfg)
	if _, err := a.Prompt(context.Background(), openresponses.UserText("x")); err != nil {
		t.Fatal(err)
	}
	pending := PendingCalls(a.State().Pending)
	if len(pending) != 2 {
		t.Fatalf("pending = %v", pending)
	}
	end, err = a.Resume(context.Background(), Output(openresponses.NewFunctionCallOutput(pending[1].CallID, "denied")), ApproveWith(pending[0].CallID, json.RawMessage(`{"text":"rewritten"}`)))
	// The model then calls both tools again and the hook defers them.
	if err != nil || end.Reason != ReasonInputRequired || len(end.Pending) != 2 {
		t.Fatalf("mixed resume: err=%v end=%+v", err, end)
	}
	outputs := map[string]string{}
	for _, it := range end.Items {
		if o, ok := it.(*openresponses.FunctionCallOutput); ok {
			outputs[o.CallID] = o.Output.Text
		}
	}
	if outputs[pending[0].CallID] != "REWRITTEN" || outputs[pending[1].CallID] != "denied" {
		t.Errorf("outputs = %v", outputs)
	}
	// The output the caller gave comes first, then the approved batch.
	if got := itemTypes(end.Items); got != "function_call_output function_call_output function_call function_call" {
		t.Errorf("resume items = %q", got)
	}

	// An approved call for a tool that no longer exists is answered with
	// the error output, as it would be in a turn.
	a = New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)}, BeforeToolCall: deferAll})
	if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	call = a.State().Pending[0].Call
	if err := a.SetConfig(Config{Model: &echo.Adapter{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Resume(context.Background(), Approve(call.CallID)); err != nil {
		t.Fatal(err)
	}
	if out := a.State().Transcript[2].(*openresponses.FunctionCallOutput); out.Output.Text != `Error: unknown tool "upper"` {
		t.Errorf("unknown approved tool output = %q", out.Output.Text)
	}

	// A terminating approved batch ends the run stopped, without a model
	// call.
	terminating := agenttool.New("done", "", func(context.Context, echoArgs) (agenttool.Result, error) {
		return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "bye"}, Terminate: true}, nil
	})
	a = New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{terminating}, BeforeToolCall: deferAll})
	if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	end, err = a.Resume(context.Background(), Approve(a.State().Pending[0].Call.CallID))
	if err != nil || end.Reason != ReasonStopped || itemTypes(end.Items) != "function_call_output" {
		t.Errorf("terminating resume: err=%v end=%+v", err, end)
	}
}

func TestAgentPromptAnswersPending(t *testing.T) {
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
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.Prompt(context.Background(), openresponses.UserText("x"))
	}()
	<-started
	a.Abort()
	<-done
	pending := PendingCalls(a.State().Pending)
	if len(pending) != 1 {
		t.Fatalf("pending = %v", pending)
	}
	// Without tools the model answers the next message in text.
	if err := a.SetConfig(Config{Model: &echo.Adapter{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Prompt(context.Background(), openresponses.UserText("next")); !errors.Is(err, ErrInputRequired) {
		t.Errorf("prompt without the output = %v", err)
	}
	if _, err := a.Prompt(context.Background(), openresponses.NewFunctionCallOutput("other", "x"), openresponses.UserText("next")); !errors.Is(err, ErrNotPending) {
		t.Errorf("prompt with a stray output = %v", err)
	}
	if len(a.State().Pending) != 1 {
		t.Fatal("a refused prompt must leave the call pending")
	}
	// The outputs ahead of the message answer the calls through the
	// event stream, and the next model call sees both.
	rec := &recorder{}
	rec.subscribe(a)
	end, err := a.Prompt(context.Background(), openresponses.NewFunctionCallOutput(pending[0].CallID, "aborted"), openresponses.UserText("next"))
	if err != nil || end.Reason != ReasonDone {
		t.Fatalf("prompt with outputs: err=%v end=%+v", err, end)
	}
	st := a.State()
	if len(st.Pending) != 0 || itemTypes(st.Transcript) != "user function_call function_call_output user assistant" {
		t.Errorf("state = %q pending=%v", itemTypes(st.Transcript), st.Pending)
	}
	if got := rec.types(); got[1] != "item_start" || got[2] != "item_end" || got[3] != "item_start" || got[4] != "item_end" {
		t.Errorf("events = %v", got)
	}
	if got := st.Transcript[4].(*openresponses.Message).Text(); !strings.Contains(got, "next") {
		t.Errorf("model saw %q", got)
	}
}
