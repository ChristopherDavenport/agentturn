package agentturn

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestInvokeRunsNestedCallsThroughTheLoop is the code-execution case: a
// tool whose code calls the agent's other tools. The calls it makes go
// through the same policy, raise the same events and reach the same
// recorder as the model's own.
func TestInvokeRunsNestedCallsThroughTheLoop(t *testing.T) {
	var mu sync.Mutex
	var ran []string
	record := func(name string) {
		mu.Lock()
		defer mu.Unlock()
		ran = append(ran, name)
	}
	read := agenttool.New("read", "reads", func(_ context.Context, a echoArgs) (string, error) {
		record("read " + a.Text)
		return "the contents", nil
	})
	bash := agenttool.New("bash", "runs a command", func(_ context.Context, a echoArgs) (string, error) {
		record("bash " + a.Text)
		return "done", nil
	})
	var nestedErr error
	var nestedOut string
	eval := agenttool.New("eval", "runs code", func(ctx context.Context, _ echoArgs) (string, error) {
		res, err := Invoke(ctx, "read", json.RawMessage(`{"text":"go.mod"}`))
		if err != nil {
			return "", err
		}
		nestedOut = res.Output.Text
		_, nestedErr = Invoke(ctx, "bash", json.RawMessage(`{"text":"rm -rf /tmp/build"}`))
		return "ran two calls", nil
	})
	var saw []string
	policy := func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
		mu.Lock()
		saw = append(saw, info.Call.Name)
		mu.Unlock()
		if info.Call.Name == "bash" && strings.Contains(string(info.Args), "rm -rf") {
			return &ToolDecision{Action: Block, Reason: "denied by bash(rm:*)", By: "policy"}, nil
		}
		return nil, nil
	}
	cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{eval, read, bash}, BeforeToolCall: policy, MaxTurns: 1}
	events, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	// The policy saw every call, and the destructive one never ran.
	if strings.Join(saw, " ") != "eval read bash" {
		t.Errorf("the hook saw %v", saw)
	}
	if strings.Join(ran, " ") != "read go.mod" {
		t.Errorf("tools that ran = %v", ran)
	}
	if nestedOut != "the contents" {
		t.Errorf("the nested read returned %q", nestedOut)
	}
	if nestedErr == nil || !strings.Contains(nestedErr.Error(), "denied by bash(rm:*)") {
		t.Errorf("the blocked call returned %v", nestedErr)
	}
	// The loop reported the nested calls, each naming the call that
	// made it.
	var starts, ends []string
	parents := map[string]string{}
	for _, ev := range events {
		switch e := ev.(type) {
		case *ToolStart:
			starts = append(starts, e.Name)
			parents[e.Name] = e.Parent
		case *ToolEnd:
			ends = append(ends, e.Name)
			if e.Name == "bash" && !e.Blocked {
				t.Error("the nested bash call was not reported as blocked")
			}
		}
	}
	if strings.Join(starts, " ") != "eval read bash" || len(ends) != 3 {
		t.Errorf("tool_start = %v tool_end = %v", starts, ends)
	}
	evalCall := ""
	for _, item := range end.Items {
		if call, ok := item.(*openresponses.FunctionCall); ok {
			evalCall = call.CallID
		}
	}
	if parents["eval"] != "" || parents["read"] != evalCall || parents["bash"] != evalCall {
		t.Errorf("parents = %v, eval call %q", parents, evalCall)
	}
	// A nested call is the work of the call that made it: it adds no
	// items of its own.
	if got := itemTypes(end.Items); got != "user function_call function_call_output" {
		t.Errorf("items = %q", got)
	}
}

func TestInvokeOutsideALoop(t *testing.T) {
	if _, err := Invoke(context.Background(), "read", nil); err != ErrNoInvoker {
		t.Errorf("err = %v", err)
	}
}

// TestInvokeRefusesADeferredCall checks that a hook that would ask the
// caller about a nested call refuses it instead: there is nobody to
// ask, since the call belongs to a tool that is running.
func TestInvokeRefusesADeferredCall(t *testing.T) {
	read := agenttool.New("read", "reads", func(context.Context, echoArgs) (string, error) { return "contents", nil })
	var nestedErr error
	eval := agenttool.New("eval", "runs code", func(ctx context.Context, _ echoArgs) (string, error) {
		_, nestedErr = Invoke(ctx, "read", json.RawMessage(`{"text":"go.mod"}`))
		return "done", nil
	})
	defers := func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
		if info.Call.Name == "read" {
			return &ToolDecision{Action: Defer, Reason: "approval required by read"}, nil
		}
		return nil, nil
	}
	cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{eval, read}, BeforeToolCall: defers, MaxTurns: 1}
	_, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	if nestedErr == nil || !strings.Contains(nestedErr.Error(), "approval required by read") {
		t.Errorf("the deferred nested call returned %v", nestedErr)
	}
	if len(end.Pending) != 0 {
		t.Errorf("a nested call was handed to the caller: %+v", end.Pending)
	}
}

// callsNamed emits one function call per name, in one response.
type callsNamed struct{ names []string }

func (m callsNamed) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	for _, name := range m.names {
		w, err := em.FunctionCall("", name)
		if err != nil {
			return err
		}
		if err := w.Arguments(`{"text":"t"}`); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
	}
	return em.Complete()
}

// TestNestedCallsEnterTheHooksOneAtATime runs two tools of one batch in
// parallel and has both call Invoke at the same moment. The hooks are
// documented as running once per call, and a policy is entitled to keep
// state without a lock of its own, so the loop must not enter them from
// two goroutines at once. The maps here are unguarded on purpose: under
// -race they are the assertion.
func TestNestedCallsEnterTheHooksOneAtATime(t *testing.T) {
	before := map[string]int{}
	after := map[string]int{}
	var bar sync.WaitGroup
	bar.Add(2)
	read := agenttool.New("read", "reads", func(context.Context, echoArgs) (string, error) { return "contents", nil })
	evalTool := func(name string) agenttool.Tool {
		return agenttool.New(name, "runs code", func(ctx context.Context, _ echoArgs) (string, error) {
			// Both tools reach here before either invokes, so the two
			// nested calls overlap.
			bar.Done()
			bar.Wait()
			_, err := Invoke(ctx, "read", json.RawMessage(`{"text":"go.mod"}`))
			return "ran", err
		})
	}
	cfg := Config{
		Model:    callsNamed{names: []string{"eval_a", "eval_b"}},
		Tools:    []agenttool.Tool{evalTool("eval_a"), evalTool("eval_b"), read},
		MaxTurns: 1,
		BeforeToolCall: func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
			before[info.Call.Name]++
			return nil, nil
		},
		AfterToolCall: func(_ context.Context, info ToolResultInfo) (*ToolOverride, error) {
			after[info.Call.Name]++
			return nil, nil
		},
	}
	if _, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg)); err != nil {
		t.Fatalf("err=%v end=%+v", err, end)
	}
	if before["eval_a"] != 1 || before["eval_b"] != 1 || before["read"] != 2 {
		t.Errorf("before-tool-call saw %v", before)
	}
	if after["read"] != 2 {
		t.Errorf("after-tool-call saw %v", after)
	}
}
