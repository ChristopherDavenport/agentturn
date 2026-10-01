package agentturn

import (
	"context"
	"encoding/json"
	"errors"
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

// TestInvokeAsksAboutADeferredCall pins #200: a nested call the hook
// defers is put to the invoking tool's elicitor, under the call that
// made it, naming the call, its arguments and the reason. An accept
// runs it and a decline refuses it, either by "human" on its
// tool_start; a cancel, a failure to ask or no elicitor at all leaves
// it refused as deferred.
func TestInvokeAsksAboutADeferredCall(t *testing.T) {
	cases := []struct {
		name string
		// answer is the elicitor's; nil installs none.
		answer   func() (agenttool.Answer, error)
		ran      bool
		errHas   string
		action   ToolAction
		by       string
		noReason bool
	}{
		{name: "no elicitor", errHas: "a nested call cannot be deferred to the caller: approval required by read", action: Defer},
		{name: "accept", answer: func() (agenttool.Answer, error) { return agenttool.Answer{Action: agenttool.ActionAccept}, nil }, ran: true, action: Allow, by: "human"},
		{name: "accept, no reason", answer: func() (agenttool.Answer, error) { return agenttool.Answer{Action: agenttool.ActionAccept}, nil }, ran: true, action: Allow, by: "human", noReason: true},
		{name: "decline", answer: func() (agenttool.Answer, error) { return agenttool.Answer{Action: agenttool.ActionDecline}, nil }, errHas: "declined when asked: approval required by read", action: Block, by: "human"},
		{name: "cancel", answer: func() (agenttool.Answer, error) { return agenttool.Answer{Action: agenttool.ActionCancel}, nil }, errHas: "a nested call cannot be deferred to the caller", action: Defer},
		{name: "failure to ask", answer: func() (agenttool.Answer, error) { return agenttool.Answer{}, errors.New("no terminal") }, errHas: "a nested call cannot be deferred to the caller", action: Defer},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ran := false
			read := agenttool.New("read", "reads", func(context.Context, echoArgs) (string, error) {
				ran = true
				return "contents", nil
			})
			var nestedErr error
			eval := agenttool.New("eval", "runs code", func(ctx context.Context, _ echoArgs) (string, error) {
				_, nestedErr = Invoke(ctx, "read", json.RawMessage(`{"text":"go.mod"}`))
				return "done", nil
			})
			reason := "approval required by read"
			if tc.noReason {
				reason = ""
			}
			defers := func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
				if info.Call.Name == "read" {
					return &ToolDecision{Action: Defer, Reason: reason, By: "policy"}, nil
				}
				return nil, nil
			}
			var asked []agenttool.Elicitation
			var askedUnder []string
			cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{eval, read}, BeforeToolCall: defers, MaxTurns: 1}
			if tc.answer != nil {
				cfg.ToolElicitor = func(ctx context.Context, q agenttool.Elicitation) (agenttool.Answer, error) {
					asked = append(asked, q)
					call, _ := agenttool.CallFrom(ctx)
					askedUnder = append(askedUnder, call.ID)
					return tc.answer()
				}
			}
			events, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
			if err != nil {
				t.Fatal(err)
			}
			if ran != tc.ran {
				t.Errorf("the nested read ran: %v", ran)
			}
			if (tc.errHas == "") != (nestedErr == nil) || (nestedErr != nil && !strings.Contains(nestedErr.Error(), tc.errHas)) {
				t.Errorf("the nested call returned %v, want %q", nestedErr, tc.errHas)
			}
			if len(end.Pending) != 0 {
				t.Errorf("a nested call was handed to the caller: %+v", end.Pending)
			}
			var decision *ToolDecision
			evalCall := ""
			for _, ev := range events {
				if e, ok := ev.(*ToolStart); ok {
					if e.Parent == "" {
						evalCall = e.CallID
					} else {
						decision = e.Decision
					}
				}
			}
			if decision == nil || decision.Action != tc.action || (tc.by != "" && decision.By != tc.by) {
				t.Fatalf("the nested tool_start carries %+v", decision)
			}
			if tc.action == Allow && decision.Reason == "" {
				t.Error("an approval carries no reason for the record")
			}
			if tc.answer == nil {
				return
			}
			if len(asked) != 1 || askedUnder[0] != evalCall {
				t.Fatalf("asked %d questions under %v, want one under %s", len(asked), askedUnder, evalCall)
			}
			for _, want := range []string{"read", `{"text":"go.mod"}`, reason} {
				if !strings.Contains(asked[0].Message, want) {
					t.Errorf("question %q does not name %q", asked[0].Message, want)
				}
			}
		})
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
