package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

func itemTypes(items openresponses.Items) string {
	parts := make([]string, 0, len(items))
	for _, it := range items {
		typ := it.ItemType()
		if m, ok := it.(*openresponses.Message); ok {
			typ = string(m.Role)
		}
		parts = append(parts, typ)
	}
	return strings.Join(parts, " ")
}

func TestToolRunsChild(t *testing.T) {
	var mu sync.Mutex
	var observed []string
	child := New(agentturn.Config{Name: "helper", Description: "helps", Model: &echo.Adapter{}, ModelName: "m"},
		WithObserver(func(_ context.Context, ev agentturn.Event) {
			mu.Lock()
			observed = append(observed, ev.EventType())
			mu.Unlock()
		}))
	if child.Name() != "helper" || child.Description() != "helps" {
		t.Errorf("name/description = %q/%q", child.Name(), child.Description())
	}
	if !strings.Contains(string(child.Parameters()), `"input"`) {
		t.Errorf("schema = %s", child.Parameters())
	}
	var updates []string
	res, err := child.Execute(context.Background(), agenttool.Call{ID: "c1", Args: json.RawMessage(`{"input":"do the thing"}`), OnUpdate: func(r agenttool.Result) {
		updates = append(updates, r.Output.Text)
	}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Output.Text != "do the thing" {
		t.Errorf("output = %q", res.Output.Text)
	}
	info, ok := res.Details.(ChildInfo)
	if !ok || info.RunID == "" || info.Reason != agentturn.ReasonDone || itemTypes(info.Items) != "user assistant" {
		t.Errorf("details = %+v", res.Details)
	}
	if len(updates) != 1 || updates[0] != "do the thing" {
		t.Errorf("updates = %v", updates)
	}
	if observed[0] != "run_start" || observed[len(observed)-1] != "run_end" {
		t.Errorf("observed = %v", observed)
	}
}

func TestToolErrors(t *testing.T) {
	cases := []struct {
		name string
		tool agenttool.Tool
		args string
		want string
	}{
		{"bad args", New(agentturn.Config{Name: "a", Model: &echo.Adapter{}}), `{"input":3}`, "invalid arguments"},
		{"no model", New(agentturn.Config{Name: "a"}), `{"input":"x"}`, "no model"},
		{"model failure", New(agentturn.Config{Name: "a", Model: failing{}}), `{"input":"x"}`, "model unavailable"},
		{"render nothing", New(agentturn.Config{Name: "a", Model: &echo.Adapter{}}, WithArgs(func(Input) openresponses.Items { return nil })), `{}`, "rendered no items"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.tool.Execute(context.Background(), agenttool.Call{Args: json.RawMessage(tc.args)})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
		})
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected panic for empty name")
			}
		}()
		New(agentturn.Config{Model: &echo.Adapter{}})
	}()
}

type failing struct{}

func (failing) CreateStream(context.Context, openresponses.Request, openresponses.EventSink) error {
	return openresponses.ServerError("down", "model unavailable")
}

func TestTypedArgsAndSeed(t *testing.T) {
	type review struct {
		File  string `json:"file" desc:"Path to review"`
		Focus string `json:"focus,omitempty"`
	}
	seedCalled := false
	child := New(agentturn.Config{Name: "reviewer", Model: &echo.Adapter{}},
		WithStrictArgs(func(r review) openresponses.Items {
			return openresponses.Items{openresponses.UserText("review " + r.File + " for " + r.Focus)}
		}),
		WithTranscript(func(parent agentturn.Transcript) agentturn.Transcript {
			seedCalled = true
			return parent
		}))
	if !agenttool.IsStrict(child) || !strings.Contains(string(child.Parameters()), `"additionalProperties":false`) {
		t.Errorf("schema = %s", child.Parameters())
	}
	ctx := agentturn.ContextWithTranscript(context.Background(), agentturn.Transcript{openresponses.UserText("earlier"), openresponses.AssistantText("ok")})
	res, err := child.Execute(ctx, agenttool.Call{Args: json.RawMessage(`{"file":"main.go","focus":"errors"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !seedCalled || res.Output.Text != "review main.go for errors" {
		t.Errorf("seed=%v output=%q", seedCalled, res.Output.Text)
	}
	// The child's added items exclude the seed.
	if got := itemTypes(res.Details.(ChildInfo).Items); got != "user assistant" {
		t.Errorf("items = %q", got)
	}
	if _, ok := agentturn.TranscriptFromContext(context.Background()); ok {
		t.Error("parent transcript found on a bare context")
	}
}

func TestSeedSeesParentTranscriptUnderALoop(t *testing.T) {
	var seen agentturn.Transcript
	child := New(agentturn.Config{Name: "child", Model: &echo.Adapter{}},
		WithTranscript(func(parent agentturn.Transcript) agentturn.Transcript {
			seen = parent
			return nil
		}))
	parent := agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{child}}
	// Nothing is attached to the context by hand: the loop does it.
	for ev := range agentturn.Run(context.Background(), nil, openresponses.Items{openresponses.UserText("delegate")}, parent) {
		if e, ok := ev.(*agentturn.RunEnd); ok && e.Err != nil {
			t.Fatal(e.Err)
		}
	}
	// The in-flight call is answered with a placeholder naming the
	// child, so the seed holds a valid input.
	if itemTypes(seen) != "user function_call function_call_output" {
		t.Errorf("seed saw %q, want the parent's user message, the call and a placeholder output", itemTypes(seen))
	}
	call := seen[1].(*openresponses.FunctionCall)
	out := seen[2].(*openresponses.FunctionCallOutput)
	if out.CallID != call.CallID || !strings.Contains(out.Output.Text, `"child"`) {
		t.Errorf("placeholder = %+v", out)
	}
}

func TestInputRequiredErrorMatchesSentinel(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", &InputRequiredError{Agent: "x"})
	if !errors.Is(err, agentturn.ErrInputRequired) {
		t.Error("InputRequiredError should match agentturn.ErrInputRequired")
	}
	defer func() {
		if recover() == nil {
			t.Error("New without a Name should panic")
		}
	}()
	New(agentturn.Config{Model: &echo.Adapter{}})
}

func TestAbortPropagates(t *testing.T) {
	blocking := agenttool.New("wait", "", func(ctx context.Context, _ struct {
		Text string `json:"text"`
	}) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	ctx, cancel := context.WithCancel(agentturn.ContextWithRunID(context.Background(), "run_parent"))
	var afterAbort []string
	var cfgSeen bool
	child := New(agentturn.Config{Name: "child", Model: &echo.Adapter{}, Tools: []agenttool.Tool{blocking}},
		WithObserver(func(ctx context.Context, ev agentturn.Event) {
			if cfg, ok := ConfigFromContext(ctx); ok && cfg.Name == "child" && agentturn.RunIDFromContext(ctx) == "run_parent" {
				cfgSeen = true
			}
			if _, ok := ev.(*agentturn.ToolStart); ok {
				cancel()
			}
			// The observer's context outlives the abort.
			if ctx.Err() == nil {
				afterAbort = append(afterAbort, ev.EventType())
			}
		}))
	res, err := child.Execute(ctx, agenttool.Call{Args: json.RawMessage(`{"input":"x"}`)})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if info := res.Details.(ChildInfo); info.Reason != agentturn.ReasonAborted {
		t.Errorf("details = %+v", info)
	}
	if !cfgSeen {
		t.Error("observer context lacks the child config or the parent run ID")
	}
	if n := len(afterAbort); n == 0 || afterAbort[n-1] != "run_end" || afterAbort[n-2] != "tool_end" {
		t.Errorf("events with a live context = %v", afterAbort)
	}
}

// TestThreeLevels nests three agents: the parent's tool is an agent whose
// tool is an agent whose tool is an agent. Every level is echo, so the
// answer threads back up through each level's "Tool result:" prefix.
func TestThreeLevels(t *testing.T) {
	level3 := New(agentturn.Config{Name: "level3", Description: "deepest", Model: &echo.Adapter{}})
	level2 := New(agentturn.Config{Name: "level2", Model: &echo.Adapter{}, Tools: []agenttool.Tool{level3}})
	level1 := New(agentturn.Config{Name: "level1", Model: &echo.Adapter{}, Tools: []agenttool.Tool{level2}})
	parent := agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{level1}}

	var end *agentturn.RunEnd
	var toolEnd *agentturn.ToolEnd
	for ev := range agentturn.Run(context.Background(), nil, openresponses.Items{openresponses.UserText("ping")}, parent) {
		switch e := ev.(type) {
		case *agentturn.ToolEnd:
			toolEnd = e
		case *agentturn.RunEnd:
			end = e
		}
	}
	if end.Reason != agentturn.ReasonDone {
		t.Fatalf("end = %+v", end)
	}
	final := end.Items[len(end.Items)-1].(*openresponses.Message).Text()
	if final != "Tool result: Tool result: Tool result: ping" {
		t.Errorf("final = %q", final)
	}
	info, ok := toolEnd.Result.Details.(ChildInfo)
	if !ok || itemTypes(info.Items) != "user function_call function_call_output assistant" {
		t.Errorf("level1 details = %+v", toolEnd.Result.Details)
	}
	// The nested child's details ride on its function call output only
	// as far as its own parent; the transcript carries plain outputs.
	fco := end.Items[2].(*openresponses.FunctionCallOutput)
	if fco.Output.Text != "Tool result: Tool result: ping" {
		t.Errorf("parent output = %q", fco.Output.Text)
	}
}

func TestChildInputRequired(t *testing.T) {
	childCfg := agentturn.Config{Name: "child", Description: "defers", Model: &echo.Adapter{},
		Tools: []agenttool.Tool{agenttool.New("lookup", "", func(context.Context, struct {
			Text string `json:"text"`
		}) (string, error) {
			return "never", nil
		})},
		BeforeToolCall: func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			return &agentturn.ToolDecision{Action: agentturn.Defer}, nil
		}}
	child := New(childCfg)
	res, err := child.Execute(context.Background(), agenttool.Call{ID: "c", Args: json.RawMessage(`{"input":"find"}`)})
	var ire *InputRequiredError
	if !errors.As(err, &ire) || ire.Agent != "child" || len(ire.Pending) != 1 || ire.Pending[0].Call.Name != "lookup" {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(err.Error(), `lookup({"text":"find"})`) {
		t.Errorf("message = %q", err)
	}
	info, ok := res.Details.(ChildInfo)
	if !ok || info.Reason != agentturn.ReasonInputRequired || len(info.Pending) != 1 || info.RunID != ire.RunID || len(info.Items) != 2 {
		t.Errorf("details = %+v", res.Details)
	}
	// The host can answer the child's call and continue it.
	transcript := append(agentturn.Transcript(nil), info.Items...)
	transcript = append(transcript, openresponses.NewFunctionCallOutput(info.Pending[0].Call.CallID, "FOUND"))
	var final string
	for ev := range agentturn.Continue(context.Background(), transcript, childCfg) {
		if e, ok := ev.(*agentturn.RunEnd); ok {
			final = e.Items[len(e.Items)-1].(*openresponses.Message).Text()
		}
	}
	if final != "Tool result: FOUND" {
		t.Errorf("continued child said %q", final)
	}
	// Through a parent loop the model sees the pause as a retryable
	// error output.
	parent := agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{child}, MaxTurns: 1}
	for ev := range agentturn.Run(context.Background(), nil, openresponses.Items{openresponses.UserText("find")}, parent) {
		if e, ok := ev.(*agentturn.ToolEnd); ok {
			if e.Err == nil || !strings.HasPrefix(e.Result.Output.Text, `Error: agent "child" needs input`) {
				t.Errorf("parent tool_end = %+v", e)
			}
			if _, ok := e.Result.Details.(ChildInfo); !ok {
				t.Error("ChildInfo lost on the error path")
			}
		}
	}
}

func TestChildWithoutAnAnswer(t *testing.T) {
	// A child that hits its turn budget while still calling tools has no
	// final message: the parent's model sees an error naming the cause.
	looping := agenttool.New("lookup", "", func(context.Context, struct {
		Text string `json:"text"`
	}) (string, error) {
		return "found", nil
	})
	child := New(agentturn.Config{Name: "explore", Model: &echo.Adapter{}, Tools: []agenttool.Tool{looping}, MaxTurns: 1})
	res, err := child.Execute(context.Background(), agenttool.Call{ID: "c1", Args: json.RawMessage(`{"input":"x"}`)})
	if err == nil || !strings.Contains(err.Error(), "max_turns") || !strings.Contains(err.Error(), "without a final answer") {
		t.Errorf("err = %v", err)
	}
	info, ok := res.Details.(ChildInfo)
	if !ok || info.Reason != agentturn.ReasonStopped || info.Cause != agentturn.StopMaxTurns {
		t.Errorf("details = %+v", res.Details)
	}
	// A terminating tool answered on the child's behalf: its output is
	// the answer.
	final := agenttool.New("final_answer", "", func(context.Context, struct {
		Text string `json:"text"`
	}) (agenttool.Result, error) {
		return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "42"}, Terminate: true}, nil
	})
	child = New(agentturn.Config{Name: "solver", Model: &echo.Adapter{}, Tools: []agenttool.Tool{final}})
	res, err = child.Execute(context.Background(), agenttool.Call{ID: "c2", Args: json.RawMessage(`{"input":"x"}`)})
	if err != nil || res.Output.Text != "42" || res.Details.(ChildInfo).Cause != agentturn.StopTerminate {
		t.Errorf("terminating child: res=%+v err=%v", res, err)
	}
	// The host chooses otherwise.
	child = New(agentturn.Config{Name: "explore", Model: &echo.Adapter{}, Tools: []agenttool.Tool{looping}, MaxTurns: 1},
		WithNoAnswer(func(info ChildInfo) (agenttool.Result, error) {
			return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "ran out of turns after " + string(info.Cause)}}, nil
		}))
	res, err = child.Execute(context.Background(), agenttool.Call{ID: "c3", Args: json.RawMessage(`{"input":"x"}`)})
	if err != nil || res.Output.Text != "ran out of turns after max_turns" || res.Details.(ChildInfo).RunID == "" {
		t.Errorf("custom no-answer: res=%+v err=%v", res, err)
	}
}

func TestToolNameIsValidated(t *testing.T) {
	mustPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), name) {
				t.Errorf("expected a panic naming %q, got %v", name, r)
			}
		}()
		fn()
	}
	mustPanic("Billing agent", func() { New(agentturn.Config{Name: "Billing agent", Model: &echo.Adapter{}}) })
	mustPanic("bad name", func() { WithToolName("bad name") })
	tool := New(agentturn.Config{Name: "Billing agent", Model: &echo.Adapter{}}, WithToolName("billing_agent"))
	if tool.Name() != "billing_agent" {
		t.Errorf("tool name = %q", tool.Name())
	}
	if New(agentturn.Config{Name: "ok-name_1", Model: &echo.Adapter{}}).Name() != "ok-name_1" {
		t.Error("a valid name is kept")
	}
}

func TestSeedAnswersEverySiblingCall(t *testing.T) {
	parent := agentturn.Transcript{
		openresponses.UserText("x"),
		&openresponses.FunctionCall{CallID: "mine", Name: "child", Arguments: "{}"},
		&openresponses.FunctionCall{CallID: "sibling", Name: "other", Arguments: "{}"},
		&openresponses.FunctionCall{CallID: "done", Name: "other", Arguments: "{}"},
		openresponses.NewFunctionCallOutput("done", "ok"),
	}
	got := answered(parent, "mine", "child")
	if itemTypes(got) != "user function_call function_call function_call function_call_output function_call_output function_call_output" {
		t.Fatalf("answered = %q", itemTypes(got))
	}
	mine := got[5].(*openresponses.FunctionCallOutput)
	sib := got[6].(*openresponses.FunctionCallOutput)
	if mine.CallID != "mine" || !strings.Contains(mine.Output.Text, `"child"`) || sib.CallID != "sibling" || !strings.Contains(sib.Output.Text, "alongside") {
		t.Errorf("placeholders = %+v %+v", mine, sib)
	}
	if len(parent) != 5 {
		t.Error("the parent's snapshot was changed")
	}
	if answered(nil, "x", "y") != nil {
		t.Error("nil seed should stay nil")
	}
	if !agentturn.CanContinue(got) {
		t.Error("the answered snapshot should be a valid input")
	}
}
