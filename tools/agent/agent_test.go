package agent

import (
	"context"
	"encoding/json"
	"errors"
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
	child := Tool(agentturn.Config{Name: "helper", Description: "helps", Model: &echo.Adapter{}, ModelName: "m"},
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
		{"bad args", Tool(agentturn.Config{Name: "a", Model: &echo.Adapter{}}), `{"input":3}`, "invalid arguments"},
		{"no model", Tool(agentturn.Config{Name: "a"}), `{"input":"x"}`, "no model"},
		{"model failure", Tool(agentturn.Config{Name: "a", Model: failing{}}), `{"input":"x"}`, "model unavailable"},
		{"render nothing", Tool(agentturn.Config{Name: "a", Model: &echo.Adapter{}}, WithArgs(func(Input) openresponses.Items { return nil })), `{}`, "rendered no items"},
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
		Tool(agentturn.Config{Model: &echo.Adapter{}})
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
	child := Tool(agentturn.Config{Name: "reviewer", Model: &echo.Adapter{}},
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
	ctx := WithParentTranscript(context.Background(), agentturn.Transcript{openresponses.UserText("earlier"), openresponses.AssistantText("ok")})
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
	if _, ok := ParentTranscript(context.Background()); ok {
		t.Error("parent transcript found on a bare context")
	}
}

func TestAbortPropagates(t *testing.T) {
	blocking := agenttool.New("wait", "", func(ctx context.Context, _ struct {
		Text string `json:"text"`
	}) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	child := Tool(agentturn.Config{Name: "child", Model: &echo.Adapter{}, Tools: []agenttool.Tool{blocking}},
		WithObserver(func(_ context.Context, ev agentturn.Event) {
			if _, ok := ev.(*agentturn.ToolStart); ok {
				cancel()
			}
		}))
	res, err := child.Execute(ctx, agenttool.Call{Args: json.RawMessage(`{"input":"x"}`)})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if info := res.Details.(ChildInfo); info.Reason != agentturn.ReasonAborted {
		t.Errorf("details = %+v", info)
	}
}

// TestThreeLevels nests three agents: the parent's tool is an agent whose
// tool is an agent whose tool is an agent. Every level is echo, so the
// answer threads back up through each level's "Tool result:" prefix.
func TestThreeLevels(t *testing.T) {
	level3 := Tool(agentturn.Config{Name: "level3", Description: "deepest", Model: &echo.Adapter{}})
	level2 := Tool(agentturn.Config{Name: "level2", Model: &echo.Adapter{}, Tools: []agenttool.Tool{level3}})
	level1 := Tool(agentturn.Config{Name: "level1", Model: &echo.Adapter{}, Tools: []agenttool.Tool{level2}})
	parent := agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{level1}}

	var end *agentturn.RunEnd
	var toolEnd *agentturn.ToolEnd
	for ev, err := range agentturn.Run(context.Background(), nil, openresponses.Items{openresponses.UserText("ping")}, parent) {
		if err != nil {
			t.Fatal(err)
		}
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
			return &agentturn.ToolDecision{Defer: true}, nil
		}}
	child := Tool(childCfg)
	res, err := child.Execute(context.Background(), agenttool.Call{ID: "c", Args: json.RawMessage(`{"input":"find"}`)})
	var ire *InputRequiredError
	if !errors.As(err, &ire) || ire.Agent != "child" || len(ire.Pending) != 1 || ire.Pending[0].Name != "lookup" {
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
	transcript = append(transcript, openresponses.NewFunctionCallOutput(info.Pending[0].CallID, "FOUND"))
	var final string
	for ev, err := range agentturn.Continue(context.Background(), transcript, childCfg) {
		if err != nil {
			t.Fatal(err)
		}
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
	for ev, err := range agentturn.Run(context.Background(), nil, openresponses.Items{openresponses.UserText("find")}, parent) {
		if err != nil {
			t.Fatal(err)
		}
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
