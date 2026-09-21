package agentturn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestChainBeforeModelCallKeepsEveryLayer is the composition case: a
// memory layer edits the instructions and a guard inspects what will be
// sent. Assigning the field twice keeps one; the chain keeps both, in
// the order the caller gave.
func TestChainBeforeModelCallKeepsEveryLayer(t *testing.T) {
	var sawInstructions string
	memory := func(_ context.Context, req *openresponses.Request) error {
		req.Instructions += "\n\n<memory>the user prefers jsonl</memory>"
		return nil
	}
	guard := func(_ context.Context, req *openresponses.Request) error {
		sawInstructions = req.Instructions
		return nil
	}
	cfg := Config{Model: &echo.Adapter{}, Instructions: "be brief", MaxTurns: 1,
		BeforeModelCall: ChainBeforeModelCall(memory, guard)}
	var sent openresponses.Request
	a := New(cfg)
	a.Subscribe(func(_ context.Context, ev Event) error {
		if e, ok := ev.(*TurnStart); ok {
			sent = e.Request
		}
		return nil
	})
	if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sent.Instructions, "<memory>") || !strings.HasPrefix(sent.Instructions, "be brief") {
		t.Errorf("instructions sent = %q", sent.Instructions)
	}
	if sawInstructions != sent.Instructions {
		t.Errorf("the guard saw %q, the model got %q", sawInstructions, sent.Instructions)
	}

	// The first error stops the chain and ends the run.
	boom := errors.New("refused")
	ran := 0
	count := func(context.Context, *openresponses.Request) error { ran++; return nil }
	cfg.BeforeModelCall = ChainBeforeModelCall(func(context.Context, *openresponses.Request) error { return boom }, count)
	_, end, _ := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("go")}, cfg))
	if end.Reason != ReasonError || !errors.Is(end.Err, boom) || ran != 0 {
		t.Errorf("end = %+v, hooks after the error ran %d times", end, ran)
	}
	if ChainBeforeModelCall() != nil || ChainBeforeModelCall(nil) != nil {
		t.Error("a chain of nothing is not nil")
	}
}

func TestChainBeforeTurnConcatenates(t *testing.T) {
	one := func(context.Context, TurnStartInfo) (openresponses.Items, error) {
		return openresponses.Items{openresponses.DeveloperText("first")}, nil
	}
	none := func(context.Context, TurnStartInfo) (openresponses.Items, error) { return nil, nil }
	two := func(context.Context, TurnStartInfo) (openresponses.Items, error) {
		return openresponses.Items{openresponses.DeveloperText("second")}, nil
	}
	cfg := Config{Model: &echo.Adapter{}, MaxTurns: 1, BeforeTurn: ChainBeforeTurn(one, none, two)}
	_, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("go")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	var texts []string
	for _, it := range end.Items {
		if m, ok := it.(*openresponses.Message); ok && m.Role == openresponses.RoleDeveloper {
			texts = append(texts, m.Text())
		}
	}
	if strings.Join(texts, "|") != "first|second" {
		t.Errorf("items = %v", texts)
	}
}

func TestChainShouldStopAfterTurn(t *testing.T) {
	budget := errors.New("token budget reached")
	for _, tc := range []struct {
		name  string
		fns   []func(context.Context, TurnInfo) (bool, error)
		cause StopCause
		err   error
		ran   int
	}{
		{
			name: "nobody stops",
			fns: []func(context.Context, TurnInfo) (bool, error){
				func(context.Context, TurnInfo) (bool, error) { return false, nil },
				func(context.Context, TurnInfo) (bool, error) { return false, nil },
			},
			ran: 2,
		},
		{
			name: "the first to stop wins",
			fns: []func(context.Context, TurnInfo) (bool, error){
				func(context.Context, TurnInfo) (bool, error) { return true, nil },
				func(context.Context, TurnInfo) (bool, error) { return false, nil },
			},
			cause: StopHook,
			ran:   1,
		},
		{
			name: "a guard error names which layer fired",
			fns: []func(context.Context, TurnInfo) (bool, error){
				func(context.Context, TurnInfo) (bool, error) { return false, nil },
				func(context.Context, TurnInfo) (bool, error) {
					return false, fmt.Errorf("%w: %w", ErrGuard, budget)
				},
			},
			cause: StopGuard,
			err:   budget,
			ran:   2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ran := 0
			fns := make([]func(context.Context, TurnInfo) (bool, error), 0, len(tc.fns))
			for _, fn := range tc.fns {
				fns = append(fns, func(ctx context.Context, info TurnInfo) (bool, error) {
					ran++
					return fn(ctx, info)
				})
			}
			cfg := Config{Model: &echo.Adapter{}, ShouldStopAfterTurn: ChainShouldStopAfterTurn(fns...)}
			_, end, _ := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("go")}, cfg))
			if end.Cause != tc.cause {
				t.Errorf("cause = %q, want %q", end.Cause, tc.cause)
			}
			if tc.err != nil && !errors.Is(end.Err, tc.err) {
				t.Errorf("err = %v", end.Err)
			}
			if ran != tc.ran {
				t.Errorf("hooks run = %d, want %d", ran, tc.ran)
			}
		})
	}
}

func TestChainBeforeToolCallFolds(t *testing.T) {
	allow := func(context.Context, ToolCallInfo) (*ToolDecision, error) { return nil, nil }
	rewrite := func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
		return &ToolDecision{Args: json.RawMessage(`{"text":"redacted"}`)}, nil
	}
	ask := func(context.Context, ToolCallInfo) (*ToolDecision, error) {
		return &ToolDecision{Action: Defer, Reason: "approval required by upper"}, nil
	}
	askAgain := func(context.Context, ToolCallInfo) (*ToolDecision, error) {
		return &ToolDecision{Action: Defer, Reason: "and the second policy asks too"}, nil
	}
	deny := func(context.Context, ToolCallInfo) (*ToolDecision, error) {
		return &ToolDecision{Action: Block, Reason: "denied by bash(curl:*)", By: "policy"}, nil
	}
	sawArgs := func(seen *[]string) func(context.Context, ToolCallInfo) (*ToolDecision, error) {
		return func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
			*seen = append(*seen, string(info.Args))
			return nil, nil
		}
	}
	var seen []string
	for _, tc := range []struct {
		name   string
		fns    []func(context.Context, ToolCallInfo) (*ToolDecision, error)
		action ToolAction
		reason string
		by     string
	}{
		{name: "everybody allows", fns: []func(context.Context, ToolCallInfo) (*ToolDecision, error){allow, allow}},
		{
			name:   "ask beats allow",
			fns:    []func(context.Context, ToolCallInfo) (*ToolDecision, error){allow, ask, allow},
			action: Defer,
			reason: "approval required by upper",
		},
		{
			name:   "the first reason is kept",
			fns:    []func(context.Context, ToolCallInfo) (*ToolDecision, error){ask, askAgain},
			action: Defer,
			reason: "approval required by upper",
		},
		{
			name:   "deny beats ask, whichever came first",
			fns:    []func(context.Context, ToolCallInfo) (*ToolDecision, error){ask, deny},
			action: Block,
			reason: "denied by bash(curl:*)",
			by:     "policy",
		},
		{
			name:   "a rewrite reaches the hooks after it",
			fns:    []func(context.Context, ToolCallInfo) (*ToolDecision, error){rewrite, sawArgs(&seen)},
			action: Allow,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chain := ChainBeforeToolCall(tc.fns...)
			d, err := chain(context.Background(), ToolCallInfo{Args: json.RawMessage(`{"text":"secret"}`)})
			if err != nil {
				t.Fatal(err)
			}
			if d == nil {
				if tc.action != Allow || tc.reason != "" {
					t.Fatalf("decision = nil, want %v", tc.action)
				}
				return
			}
			if d.Action != tc.action || d.Reason != tc.reason || d.By != tc.by {
				t.Errorf("decision = %+v", d)
			}
		})
	}
	if len(seen) != 1 || seen[0] != `{"text":"redacted"}` {
		t.Errorf("the hook after the rewrite saw %v", seen)
	}
	// A denied call is never handed to a later hook, which might
	// prompt a user about a call that will not run.
	after := 0
	chain := ChainBeforeToolCall(deny, func(context.Context, ToolCallInfo) (*ToolDecision, error) {
		after++
		return nil, nil
	})
	if _, err := chain(context.Background(), ToolCallInfo{}); err != nil || after != 0 {
		t.Errorf("hooks after a block ran %d times (%v)", after, err)
	}
	// The fold reaches the loop: a chain on the config blocks the call.
	cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)}, MaxTurns: 1,
		BeforeToolCall: ChainBeforeToolCall(allow, deny)}
	events, _, _ := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("go")}, cfg))
	for _, ev := range events {
		if e, ok := ev.(*ToolEnd); ok && (!e.Blocked || !strings.Contains(e.Result.Output.Text, "denied by bash")) {
			t.Errorf("tool_end = %+v", e)
		}
	}
}

func TestChainOutputGuard(t *testing.T) {
	keep := func(context.Context, OutputInfo) (*openresponses.Message, error) { return nil, nil }
	var sawText string
	redact := func(_ context.Context, info OutputInfo) (*openresponses.Message, error) {
		return openresponses.AssistantText("[redacted]"), nil
	}
	last := func(_ context.Context, info OutputInfo) (*openresponses.Message, error) {
		sawText = info.Message.Text()
		return openresponses.AssistantText(info.Message.Text() + "!"), nil
	}
	cfg := Config{Model: &echo.Adapter{}, MaxTurns: 1, OutputGuard: ChainOutputGuard(keep, redact, last)}
	_, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("secret")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	if sawText != "[redacted]" {
		t.Errorf("the second guard saw %q, not what the first left", sawText)
	}
	m, ok := end.Items[len(end.Items)-1].(*openresponses.Message)
	if !ok || m.Text() != "[redacted]!" {
		t.Errorf("transcript holds %v", end.Items[len(end.Items)-1])
	}
}
