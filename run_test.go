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

type echoArgs struct {
	Text string `json:"text"`
}

func upper(_ context.Context, a echoArgs) (string, error) { return strings.ToUpper(a.Text), nil }

func collect(t *testing.T, seq func(func(Event, error) bool)) ([]Event, *RunEnd, error) {
	t.Helper()
	var events []Event
	var end *RunEnd
	var lastErr error
	for ev, err := range seq {
		if err != nil {
			lastErr = err
		}
		if ev != nil {
			events = append(events, ev)
		}
		if e, ok := ev.(*RunEnd); ok {
			end = e
		}
	}
	return events, end, lastErr
}

func types(events []Event) []string {
	out := make([]string, 0, len(events))
	for _, ev := range events {
		out = append(out, ev.EventType())
	}
	return out
}

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

func TestRunMessageOnly(t *testing.T) {
	cfg := Config{Model: &echo.Adapter{}, ModelName: "m", Instructions: "be brief"}
	events, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("hello world")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	if end == nil || end.Reason != ReasonDone || end.Err != nil {
		t.Fatalf("end = %+v", end)
	}
	if got := itemTypes(end.Items); got != "user assistant" {
		t.Errorf("items = %q", got)
	}
	got := types(events)
	want := []string{"run_start", "item_start", "item_end", "turn_start", "item_start"}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("events = %v, want prefix %v", got, want)
		}
	}
	if got[len(got)-1] != "run_end" || got[len(got)-2] != "turn_end" || got[len(got)-3] != "response_end" || got[len(got)-4] != "item_end" {
		t.Errorf("events end = %v", got[len(got)-4:])
	}
	// Streamed items name their response; appended items do not.
	for _, ev := range events {
		if e, ok := ev.(*ItemEnd); ok {
			_, assistant := e.Item.(*openresponses.Message)
			if assistant && e.Item.(*openresponses.Message).Role == openresponses.RoleAssistant && e.ResponseID == "" {
				t.Error("assistant item_end without a response ID")
			}
			if !assistant || e.Item.(*openresponses.Message).Role != openresponses.RoleAssistant {
				if e.ResponseID != "" {
					t.Errorf("appended item carries response ID %q", e.ResponseID)
				}
			}
		}
	}
	// Deltas arrive as item_update carrying the wire event, with the
	// accumulated item alongside.
	var deltas []string
	for _, ev := range events {
		if u, ok := ev.(*ItemUpdate); ok {
			if d, ok := u.Stream.(*openresponses.OutputTextDeltaEvent); ok {
				deltas = append(deltas, d.Delta)
				if _, ok := u.Item.(*openresponses.Message); !ok {
					t.Errorf("item_update item = %T", u.Item)
				}
			}
		}
	}
	if strings.Join(deltas, "") != "hello world" {
		t.Errorf("deltas = %q", deltas)
	}
	// The request sent is the one on turn_start.
	ts := events[3].(*TurnStart)
	req := ts.Request
	if req.Model != "m" || req.Instructions != "be brief" || req.Store == nil || *req.Store || !req.Stream {
		t.Errorf("request = %+v", req)
	}
	if len(req.Metadata) != 0 {
		t.Errorf("metadata = %v", req.Metadata)
	}
	if len(req.Input) != 1 {
		t.Errorf("input = %v", req.Input)
	}
	te := events[len(events)-2].(*TurnEnd)
	if te.Response == nil || te.Response.Usage == nil || te.Response.OutputText() != "hello world" {
		t.Errorf("turn_end response = %+v", te.Response)
	}
}

func TestRunToolRoundTrip(t *testing.T) {
	var starts, ends, updates int
	cfg := Config{Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{
		agenttool.New("upper", "uppercase", func(ctx context.Context, a echoArgs) (string, error) {
			agenttool.Progress(ctx, agenttool.Text("working"))
			return upper(ctx, a)
		}),
	}}
	events, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("abc")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	if end.Reason != ReasonDone {
		t.Fatalf("reason = %s (%v)", end.Reason, end.Err)
	}
	if got := itemTypes(end.Items); got != "user function_call function_call_output assistant" {
		t.Errorf("items = %q", got)
	}
	fco := end.Items[2].(*openresponses.FunctionCallOutput)
	fc := end.Items[1].(*openresponses.FunctionCall)
	if fco.CallID != fc.CallID || fco.Output.Text != "ABC" {
		t.Errorf("output = %+v for call %+v", fco, fc)
	}
	if text := end.Items[3].(*openresponses.Message).Text(); text != "Tool result: ABC" {
		t.Errorf("final = %q", text)
	}
	var turns []int
	for _, ev := range events {
		switch e := ev.(type) {
		case *ToolStart:
			starts++
			if e.Name != "upper" || string(e.Args) != `{"text":"abc"}` {
				t.Errorf("tool_start = %+v", e)
			}
		case *ToolUpdate:
			updates++
		case *ToolEnd:
			ends++
			if e.Err != nil || e.Blocked || e.Result.Output.Text != "ABC" {
				t.Errorf("tool_end = %+v", e)
			}
		case *TurnEnd:
			turns = append(turns, e.Turn)
			if e.Turn == 1 && (len(e.ToolResults) != 1 || e.ToolResults[0].Output.Text != "ABC") {
				t.Errorf("turn 1 results = %+v", e.ToolResults)
			}
		}
	}
	if starts != 1 || ends != 1 || updates != 1 || len(turns) != 2 {
		t.Errorf("starts=%d ends=%d updates=%d turns=%v", starts, ends, updates, turns)
	}
	// Function call outputs get item events after the batch.
	seq := types(events)
	idx := func(s string) int {
		for i, v := range seq {
			if v == s {
				return i
			}
		}
		return -1
	}
	if idx("tool_end") > idx("turn_end") || idx("tool_start") < idx("turn_start") || idx("response_end") > idx("tool_start") {
		t.Errorf("ordering = %v", seq)
	}
	// The second request carries the tool definition and the full history.
	var second *TurnStart
	for _, ev := range events {
		if ts, ok := ev.(*TurnStart); ok && ts.Turn == 2 {
			second = ts
		}
	}
	if second == nil || len(second.Request.Input) != 3 || len(second.Request.Tools) != 1 {
		t.Fatalf("second request = %+v", second)
	}
	if ft := second.Request.Tools[0].(*openresponses.FunctionTool); ft.Name != "upper" || ft.Strict != nil {
		t.Errorf("tool def = %+v", ft)
	}
}

func TestRunHooks(t *testing.T) {
	type calls struct{ before, after int }
	newCfg := func(c *calls, before func(ToolCallInfo) *ToolDecision, after func(ToolResultInfo) *ToolOverride) Config {
		return Config{Model: &echo.Adapter{}, ModelName: "m",
			Tools: []agenttool.Tool{agenttool.New("upper", "", upper)},
			BeforeToolCall: func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
				c.before++
				if before == nil {
					return nil, nil
				}
				return before(info), nil
			},
			AfterToolCall: func(_ context.Context, info ToolResultInfo) (*ToolOverride, error) {
				c.after++
				if after == nil {
					return nil, nil
				}
				return after(info), nil
			},
		}
	}
	cases := []struct {
		name       string
		before     func(ToolCallInfo) *ToolDecision
		after      func(ToolResultInfo) *ToolOverride
		wantOutput string
		wantReason Reason
		wantBlock  bool
		wantErr    bool
		wantAfter  int
	}{
		{name: "allow", wantOutput: "ABC", wantReason: ReasonDone, wantAfter: 1},
		{name: "block", before: func(ToolCallInfo) *ToolDecision { return &ToolDecision{Block: true, Reason: "not allowed"} },
			wantOutput: "Error: not allowed", wantReason: ReasonDone, wantBlock: true, wantErr: true, wantAfter: 0},
		{name: "block and terminate", before: func(ToolCallInfo) *ToolDecision { return &ToolDecision{Block: true, Terminate: true} },
			wantOutput: "Error: call blocked", wantReason: ReasonStopped, wantBlock: true, wantErr: true},
		{name: "rewrite args", before: func(ToolCallInfo) *ToolDecision { return &ToolDecision{Args: json.RawMessage(`{"text":"xyz"}`)} },
			wantOutput: "XYZ", wantReason: ReasonDone, wantAfter: 1},
		{name: "override result", after: func(ToolResultInfo) *ToolOverride { return &ToolOverride{Result: agenttool.Text("replaced")} },
			wantOutput: "replaced", wantReason: ReasonDone, wantAfter: 1},
		{name: "override error", after: func(ToolResultInfo) *ToolOverride { return &ToolOverride{Err: errors.New("vetoed")} },
			wantOutput: "Error: vetoed", wantReason: ReasonDone, wantErr: true, wantAfter: 1},
		{name: "override terminate", after: func(ToolResultInfo) *ToolOverride {
			return &ToolOverride{Result: agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "end"}, Terminate: true}}
		},
			wantOutput: "end", wantReason: ReasonStopped, wantAfter: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c calls
			events, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("abc")}, newCfg(&c, tc.before, tc.after)))
			if err != nil {
				t.Fatal(err)
			}
			if end.Reason != tc.wantReason {
				t.Errorf("reason = %s, want %s", end.Reason, tc.wantReason)
			}
			var te *ToolEnd
			for _, ev := range events {
				if e, ok := ev.(*ToolEnd); ok {
					te = e
				}
			}
			if te == nil {
				t.Fatal("no tool_end")
			}
			if te.Result.Output.Text != tc.wantOutput || te.Blocked != tc.wantBlock || (te.Err != nil) != tc.wantErr {
				t.Errorf("tool_end = %+v", te)
			}
			if c.before != 1 || c.after != tc.wantAfter {
				t.Errorf("before=%d after=%d", c.before, c.after)
			}
			// Whatever happened, the transcript answers the call.
			fco := end.Items[2].(*openresponses.FunctionCallOutput)
			if fco.Output.Text != tc.wantOutput {
				t.Errorf("transcript output = %q", fco.Output.Text)
			}
		})
	}
}

func TestRunHookErrorsFailTheRun(t *testing.T) {
	boom := errors.New("boom")
	cases := map[string]Config{
		"before": {BeforeToolCall: func(context.Context, ToolCallInfo) (*ToolDecision, error) { return nil, boom }},
		"after":  {AfterToolCall: func(context.Context, ToolResultInfo) (*ToolOverride, error) { return nil, boom }},
		"stop":   {ShouldStopAfterTurn: func(context.Context, TurnInfo) (bool, error) { return false, boom }},
		"transform": {Transform: func(context.Context, Transcript) (Transcript, error) {
			return nil, boom
		}},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			cfg.Model = &echo.Adapter{}
			cfg.Tools = []agenttool.Tool{agenttool.New("upper", "", upper)}
			_, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
			if !errors.Is(err, boom) || end == nil || end.Reason != ReasonError || !errors.Is(end.Err, boom) {
				t.Errorf("err = %v, end = %+v", err, end)
			}
		})
	}
}

func TestRunStopAfterTurnAndMaxTurns(t *testing.T) {
	cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)},
		ShouldStopAfterTurn: func(_ context.Context, info TurnInfo) (bool, error) {
			return info.Turn == 1 && len(info.ToolResults) == 1 && len(info.Transcript) == 3, nil
		}}
	_, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err != nil || end.Reason != ReasonStopped || itemTypes(end.Items) != "user function_call function_call_output" {
		t.Errorf("stop after turn: err=%v end=%+v", err, end)
	}

	cfg = Config{Model: &echo.Adapter{}, MaxTurns: 1, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)}}
	_, end, err = collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err != nil || end.Reason != ReasonStopped || len(end.Items) != 3 {
		t.Errorf("max turns: err=%v end=%+v", err, end)
	}
}

func TestRunTerminateNeedsEveryResult(t *testing.T) {
	// twoCalls streams two function calls per turn so the batch has two
	// results.
	terminating := agenttool.New("a", "", func(context.Context, echoArgs) (agenttool.Result, error) {
		return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "a"}, Terminate: true}, nil
	})
	plain := agenttool.New("b", "", func(context.Context, echoArgs) (string, error) { return "b", nil })
	cfg := Config{Model: &twoCalls{}, Tools: []agenttool.Tool{terminating, plain}, MaxTurns: 2}
	_, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err != nil || end.Reason != ReasonStopped || len(end.Items) != 1+2*4 {
		t.Errorf("mixed batch should run to MaxTurns: err=%v reason=%s items=%s", err, end.Reason, itemTypes(end.Items))
	}
	cfg.Tools = []agenttool.Tool{terminating, terminating2()}
	_, end, err = collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err != nil || end.Reason != ReasonStopped || len(end.Items) != 1+4 {
		t.Errorf("terminating batch should stop after one turn: err=%v reason=%s items=%s", err, end.Reason, itemTypes(end.Items))
	}
}

func terminating2() agenttool.Tool {
	return agenttool.New("b", "", func(context.Context, echoArgs) (agenttool.Result, error) {
		return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "b"}, Terminate: true}, nil
	})
}

// twoCalls is a model that calls every offered tool once per turn.
type twoCalls struct{}

func (twoCalls) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	for _, tl := range req.Tools {
		ft := tl.(*openresponses.FunctionTool)
		w, err := em.FunctionCall("", ft.Name)
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

func TestRunUnknownToolAndBadArguments(t *testing.T) {
	cfg := Config{Model: &badCalls{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)}, MaxTurns: 1}
	events, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	var ends []*ToolEnd
	for _, ev := range events {
		if e, ok := ev.(*ToolEnd); ok {
			ends = append(ends, e)
		}
	}
	if len(ends) != 2 {
		t.Fatalf("tool_end = %d", len(ends))
	}
	byName := map[string]*ToolEnd{}
	for _, e := range ends {
		byName[e.Name] = e
	}
	if e := byName["missing"]; e == nil || e.Err == nil || !strings.Contains(e.Result.Output.Text, "unknown tool") {
		t.Errorf("missing = %+v", e)
	}
	if e := byName["upper"]; e == nil || e.Err == nil || !strings.Contains(e.Result.Output.Text, "JSON object") {
		t.Errorf("bad args = %+v", e)
	}
	if got := itemTypes(end.Items); got != "user function_call function_call function_call_output function_call_output" {
		t.Errorf("items = %q", got)
	}
}

type badCalls struct{}

func (badCalls) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	for _, c := range []struct{ name, args string }{{"missing", `{}`}, {"upper", `[1,2]`}} {
		w, err := em.FunctionCall("", c.name)
		if err != nil {
			return err
		}
		if err := w.Arguments(c.args); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
	}
	return em.Complete()
}

func TestRunFilterAndTransform(t *testing.T) {
	note := &openresponses.UnknownItem{Type: "agentturn:note", Raw: json.RawMessage(`{"type":"agentturn:note","text":"hidden"}`)}
	visible := &openresponses.UnknownItem{Type: "acme:visible", Raw: json.RawMessage(`{"type":"acme:visible"}`)}
	transcript := Transcript{openresponses.UserText("old"), openresponses.AssistantText("ok"), note, visible}
	var seen int
	cfg := Config{Model: &echo.Adapter{}, Filter: VisibleFilter("acme:visible"),
		Transform: func(_ context.Context, t Transcript) (Transcript, error) {
			seen = len(t)
			// Drop the first exchange for this call only.
			return t[2:], nil
		}}
	events, end, err := collect(t, Run(context.Background(), transcript, openresponses.Items{openresponses.UserText("new")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	if seen != 5 {
		t.Errorf("transform saw %d items, want the whole transcript", seen)
	}
	req := events[3].(*TurnStart).Request
	if got := itemTypes(req.Input); got != "acme:visible user" {
		t.Errorf("request input = %q", got)
	}
	// The working transcript was not replaced by the transform.
	if len(end.Items) != 2 {
		t.Errorf("added = %s", itemTypes(end.Items))
	}
	if got := itemTypes(DefaultFilter(transcript)); got != "user assistant" {
		t.Errorf("default filter = %q", got)
	}
}

func TestRunToolProvider(t *testing.T) {
	var turns int
	cfg := Config{Model: &echo.Adapter{}, ToolProvider: func(context.Context) []agenttool.Tool {
		turns++
		if turns == 1 {
			return []agenttool.Tool{agenttool.New("upper", "", upper)}
		}
		return nil
	}}
	_, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err != nil || itemTypes(end.Items) != "user function_call function_call_output assistant" || turns != 2 {
		t.Errorf("err=%v items=%s turns=%d", err, itemTypes(end.Items), turns)
	}
}

func TestRunModelFailure(t *testing.T) {
	cfg := Config{Model: failing{}}
	events, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err == nil || !strings.Contains(err.Error(), "model:") || end.Reason != ReasonError {
		t.Errorf("err = %v end = %+v", err, end)
	}
	if got := types(events); got[len(got)-1] != "run_end" {
		t.Errorf("events = %v", got)
	}
	if len(end.Items) != 1 {
		t.Errorf("items = %s", itemTypes(end.Items))
	}
}

type failing struct{}

func (failing) CreateStream(context.Context, openresponses.Request, openresponses.EventSink) error {
	return openresponses.ServerError("down", "model unavailable")
}

func TestRunPreconditions(t *testing.T) {
	cases := []struct {
		name string
		seq  func(func(Event, error) bool)
		want error
	}{
		{"no model", Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, Config{}), ErrNoModel},
		{"no prompt", Run(context.Background(), nil, nil, Config{Model: &echo.Adapter{}}), ErrNoPrompt},
		{"continue empty", Continue(context.Background(), nil, Config{Model: &echo.Adapter{}}), ErrCannotContinue},
		{"continue after assistant", Continue(context.Background(), Transcript{openresponses.AssistantText("x")}, Config{Model: &echo.Adapter{}}), ErrCannotContinue},
		{"duplicate tools", Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("a", "", upper), agenttool.New("a", "", upper)}}), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			events, _, err := collect(t, tc.seq)
			if err == nil || (tc.want != nil && !errors.Is(err, tc.want)) || len(events) != 0 {
				t.Errorf("err = %v, events = %v", err, events)
			}
		})
	}
}

func TestContinueFromToolOutput(t *testing.T) {
	transcript := Transcript{
		openresponses.UserText("x"),
		&openresponses.FunctionCall{CallID: "c1", Name: "upper", Arguments: `{"text":"x"}`, Status: openresponses.StatusCompleted},
		openresponses.NewFunctionCallOutput("c1", "X"),
	}
	_, end, err := collect(t, Continue(context.Background(), transcript, Config{Model: &echo.Adapter{}}))
	if err != nil || end.Reason != ReasonDone || itemTypes(end.Items) != "assistant" {
		t.Errorf("err=%v end=%+v", err, end)
	}
	if end.Items[0].(*openresponses.Message).Text() != "Tool result: X" {
		t.Errorf("text = %q", end.Items[0].(*openresponses.Message).Text())
	}
}

func TestRunAbortMidTool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blocking := agenttool.New("upper", "", func(ctx context.Context, _ echoArgs) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{blocking}}
	var events []Event
	var end *RunEnd
	for ev := range Run(ctx, nil, openresponses.Items{openresponses.UserText("x")}, cfg) {
		events = append(events, ev)
		if _, ok := ev.(*ToolStart); ok {
			cancel()
		}
		if e, ok := ev.(*RunEnd); ok {
			end = e
		}
	}
	if end == nil || end.Reason != ReasonAborted || !errors.Is(end.Err, context.Canceled) {
		t.Fatalf("end = %+v", end)
	}
	if got := itemTypes(end.Items); got != "user function_call" {
		t.Errorf("items = %q", got)
	}
	if got := types(events); got[len(got)-1] != "run_end" {
		t.Errorf("events = %v", got)
	}
	// The cut-off call is the caller's to answer.
	if len(end.Pending) != 1 || end.Pending[0].Name != "upper" {
		t.Errorf("pending = %v", end.Pending)
	}
	if canContinue(append(Transcript(nil), end.Items...)) {
		t.Error("transcript ending in an unanswered call must not continue")
	}
}

func TestUnansweredCalls(t *testing.T) {
	call := func(id string) *openresponses.FunctionCall { return &openresponses.FunctionCall{CallID: id, Name: "f"} }
	out := func(id string) *openresponses.FunctionCallOutput {
		return &openresponses.FunctionCallOutput{CallID: id}
	}
	tests := []struct {
		name string
		t    Transcript
		want []string
	}{
		{"empty", nil, nil},
		{"answered", Transcript{openresponses.UserText("x"), call("a"), out("a")}, nil},
		{"one unanswered", Transcript{openresponses.UserText("x"), call("a")}, []string{"a"}},
		{"mixed batch", Transcript{openresponses.UserText("x"), call("a"), call("b"), call("c"), out("b")}, []string{"a", "c"}},
		{"earlier turn ignored", Transcript{openresponses.UserText("x"), call("a"), openresponses.UserText("y"), call("b"), out("b")}, nil},
		{"answered later in tail", Transcript{openresponses.UserText("x"), call("a"), out("a"), call("b")}, []string{"b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []string
			for _, c := range unansweredCalls(tt.t) {
				got = append(got, c.CallID)
			}
			if strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Errorf("got %v want %v", got, tt.want)
			}
		})
	}
}

func TestRunBreakCancels(t *testing.T) {
	cfg := Config{Model: &echo.Adapter{}}
	n := 0
	for ev, err := range Run(context.Background(), nil, openresponses.Items{openresponses.UserText("a b c d e")}, cfg) {
		if err != nil {
			t.Fatal(err)
		}
		n++
		if _, ok := ev.(*ItemUpdate); ok {
			break
		}
	}
	if n == 0 {
		t.Fatal("no events")
	}
}

func TestRunRequestTemplateAndBeforeModelCall(t *testing.T) {
	store := true
	maxOut := 256
	parallel := false
	cfg := Config{
		Model: &echo.Adapter{}, ModelName: "named", Instructions: "from config",
		Request: openresponses.Request{
			Model:              "template",
			Instructions:       "from template",
			Input:              openresponses.Items{openresponses.UserText("ignored")},
			PreviousResponseID: "resp_old",
			Store:              &store,
			Stream:             false,
			ToolChoice:         openresponses.ToolChoice{Mode: openresponses.ToolChoiceAuto},
			ParallelToolCalls:  &parallel,
			MaxOutputTokens:    &maxOut,
			Include:            []openresponses.Include{openresponses.IncludeReasoningEncryptedContent},
			SafetyIdentifier:   "user-1",
			PromptCacheKey:     "cache-1",
			Truncation:         openresponses.TruncationAuto,
			Metadata:           map[string]string{"app": "test"},
			Extra:              map[string]any{"from_template": 1, "shared": "template"},
		},
		RequestExtra: map[string]any{"shared": "extra"},
		BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
			req.Metadata["hooked"] = "yes"
			return nil
		},
	}
	events, _, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("hello")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	req := events[3].(*TurnStart).Request
	// Loop-owned members win.
	if req.Model != "named" || req.Instructions != "from config" || len(req.Input) != 1 || req.Input[0].(*openresponses.Message).Text() != "hello" {
		t.Errorf("owned members = %q %q %v", req.Model, req.Instructions, req.Input)
	}
	if req.PreviousResponseID != "" || req.Store == nil || *req.Store || !req.Stream {
		t.Errorf("transport members = %q %v %v", req.PreviousResponseID, req.Store, req.Stream)
	}
	// Everything else passes through.
	if req.ToolChoice.Mode != openresponses.ToolChoiceAuto || req.ParallelToolCalls == nil || *req.ParallelToolCalls || req.MaxOutputTokens == nil || *req.MaxOutputTokens != 256 ||
		!req.Includes(openresponses.IncludeReasoningEncryptedContent) || req.SafetyIdentifier != "user-1" || req.PromptCacheKey != "cache-1" || req.Truncation != openresponses.TruncationAuto {
		t.Errorf("template members lost: %+v", req)
	}
	if req.Metadata["app"] != "test" || req.Metadata["hooked"] != "yes" || len(req.Metadata) != 2 {
		t.Errorf("metadata = %v", req.Metadata)
	}
	if req.Extra["from_template"] != 1 || req.Extra["shared"] != "extra" {
		t.Errorf("extra = %v", req.Extra)
	}
	// The template is not mutated across turns.
	if cfg.Request.Metadata["hooked"] != "" || len(cfg.Request.Metadata) != 1 {
		t.Errorf("template metadata mutated: %v", cfg.Request.Metadata)
	}

	// A hook error fails the run.
	boom := errors.New("no")
	cfg.BeforeModelCall = func(context.Context, *openresponses.Request) error { return boom }
	_, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("hello")}, cfg))
	if !errors.Is(err, boom) || end.Reason != ReasonError {
		t.Errorf("hook error: err=%v end=%+v", err, end)
	}
}

func TestRunDeferredCall(t *testing.T) {
	deferAll := func(context.Context, ToolCallInfo) (*ToolDecision, error) { return &ToolDecision{Defer: true}, nil }
	cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)}, BeforeToolCall: deferAll}
	events, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("abc")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	if end.Reason != ReasonInputRequired || len(end.Pending) != 1 || end.Pending[0].Name != "upper" {
		t.Fatalf("end = %+v", end)
	}
	if got := itemTypes(end.Items); got != "user function_call" {
		t.Errorf("items = %q", got)
	}
	var te *ToolEnd
	for _, ev := range events {
		if e, ok := ev.(*ToolEnd); ok {
			te = e
		}
	}
	if te == nil || !te.Deferred || te.Blocked || te.Err != nil || te.Result.Output.Text != "" {
		t.Errorf("tool_end = %+v", te)
	}
	seq := types(events)
	if seq[len(seq)-1] != "run_end" || seq[len(seq)-2] != "turn_end" {
		t.Errorf("events end = %v", seq[len(seq)-3:])
	}

	// The caller answers the call and continues; the hook is not
	// consulted again because the model has nothing left to call.
	transcript := append(Transcript(nil), end.Items...)
	transcript = append(transcript, openresponses.NewFunctionCallOutput(end.Pending[0].CallID, "ABC"))
	_, end, err = collect(t, Continue(context.Background(), transcript, cfg))
	if err != nil || end.Reason != ReasonDone || end.Items[0].(*openresponses.Message).Text() != "Tool result: ABC" {
		t.Errorf("resume: err=%v end=%+v", err, end)
	}

	// In a mixed batch the other calls run and only the deferred one is
	// pending; Block wins over Defer.
	cfg = Config{Model: &twoCalls{}, Tools: []agenttool.Tool{agenttool.New("a", "", upper), agenttool.New("b", "", upper), agenttool.New("c", "", upper)},
		BeforeToolCall: func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
			switch info.Call.Name {
			case "a":
				return &ToolDecision{Defer: true}, nil
			case "c":
				return &ToolDecision{Defer: true, Block: true, Reason: "refused"}, nil
			}
			return nil, nil
		}}
	_, end, err = collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err != nil || end.Reason != ReasonInputRequired || len(end.Pending) != 1 || end.Pending[0].Name != "a" {
		t.Fatalf("mixed: err=%v end=%+v", err, end)
	}
	if got := itemTypes(end.Items); got != "user function_call function_call function_call function_call_output function_call_output" {
		t.Errorf("mixed items = %q", got)
	}
	outputs := map[string]string{}
	for _, it := range end.Items {
		if o, ok := it.(*openresponses.FunctionCallOutput); ok {
			outputs[o.CallID] = o.Output.Text
		}
	}
	for _, it := range end.Items {
		if fc, ok := it.(*openresponses.FunctionCall); ok {
			switch fc.Name {
			case "b":
				if outputs[fc.CallID] != "T" {
					t.Errorf("b output = %q", outputs[fc.CallID])
				}
			case "c":
				if outputs[fc.CallID] != "Error: refused" {
					t.Errorf("c output = %q", outputs[fc.CallID])
				}
			case "a":
				if _, ok := outputs[fc.CallID]; ok {
					t.Error("deferred call has an output")
				}
			}
		}
	}
}
