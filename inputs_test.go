package agentturn

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// modesOf renders inputs as "mode:type" words, the type being the role
// for a message, with "@trigger" after one that carries a trigger.
func modesOf(inputs []TurnInput) string {
	words := make([]string, 0, len(inputs))
	for _, in := range inputs {
		typ := in.Item.ItemType()
		if m, ok := in.Item.(*openresponses.Message); ok {
			typ = string(m.Role)
		}
		word := string(in.Mode) + ":" + typ
		if !in.Trigger.IsZero() {
			word += "@" + in.Trigger.String()
		}
		words = append(words, word)
	}
	return strings.Join(words, " ")
}

// inputsLog records what BeforeTurn and turn_start said arrived, one
// entry per turn, across every run of a scenario.
type inputsLog struct {
	mu      sync.Mutex
	hook    []string // TurnStartInfo.Inputs per BeforeTurn call
	arrived []string // TurnStart.Arrived per turn_start
	inputs  []string // TurnStart.Inputs per turn_start, by item type
}

func (l *inputsLog) beforeTurn(_ context.Context, info TurnStartInfo) (openresponses.Items, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.hook = append(l.hook, modesOf(info.Inputs))
	return nil, nil
}

func (l *inputsLog) event(_ context.Context, ev Event) error {
	if ts, ok := ev.(*TurnStart); ok {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.arrived = append(l.arrived, modesOf(ts.Arrived))
		l.inputs = append(l.inputs, itemTypes(ts.Inputs))
	}
	return nil
}

// TestTurnInputsSayHowEachArrived pins #157: BeforeTurn is told how
// each item of the turn's inputs arrived, so a hook that must act on a
// user's message tells a steer from a delivered output, and a Continue's
// first turn names what it answers, which TurnStart.Inputs never held.
func TestTurnInputsSayHowEachArrived(t *testing.T) {
	cron := Trigger{Kind: "cron", Ref: "nightly"}
	upperTool := agenttool.New("upper", "", upper)
	deferAll := func(context.Context, ToolCallInfo) (*ToolDecision, error) { return &ToolDecision{Action: Defer}, nil }
	cases := []struct {
		name string
		// drive runs the scenario: cfg's BeforeTurn records, and every
		// agent the scenario makes subscribes sub.
		drive func(t *testing.T, cfg Config, sub func(context.Context, Event) error)
		// hook is what BeforeTurn saw, one entry per turn; arrived and
		// inputs what turn_start carried, when checked.
		hook, arrived, inputs []string
	}{
		{
			name: "plain Prompt",
			drive: func(t *testing.T, cfg Config, sub func(context.Context, Event) error) {
				a := New(cfg)
				a.Subscribe(sub)
				if _, err := a.Prompt(context.Background(), openresponses.UserText("hi")); err != nil {
					t.Fatal(err)
				}
			},
			hook:    []string{"prompt:user"},
			arrived: []string{"prompt:user"},
			inputs:  []string{"user"},
		},
		{
			name: "Prompt with a trigger",
			drive: func(t *testing.T, cfg Config, sub func(context.Context, Event) error) {
				a := New(cfg)
				a.Subscribe(sub)
				if _, err := a.Prompt(ContextWithTrigger(context.Background(), cron), openresponses.UserText("hi"), openresponses.DeveloperText("note")); err != nil {
					t.Fatal(err)
				}
			},
			hook: []string{"prompt:user@cron:nightly prompt:developer@cron:nightly"},
		},
		{
			name: "low-level Run and Continue",
			drive: func(t *testing.T, cfg Config, sub func(context.Context, Event) error) {
				var transcript Transcript
				for ev := range Run(ContextWithTrigger(context.Background(), cron), nil, openresponses.Items{openresponses.UserText("hi")}, cfg) {
					if err := sub(context.Background(), ev); err != nil {
						t.Fatal(err)
					}
					if end, ok := ev.(*RunEnd); ok {
						transcript = end.Items
					}
				}
				transcript = append(transcript, openresponses.UserText("more"), openresponses.UserText("and more"))
				for ev := range Continue(context.Background(), transcript, cfg) {
					if err := sub(context.Background(), ev); err != nil {
						t.Fatal(err)
					}
				}
			},
			hook:    []string{"prompt:user@cron:nightly", "continued:user continued:user"},
			arrived: []string{"prompt:user@cron:nightly", "continued:user continued:user"},
			inputs:  []string{"user", ""},
		},
		{
			name: "second turn of a run",
			drive: func(t *testing.T, cfg Config, sub func(context.Context, Event) error) {
				cfg.Tools = []agenttool.Tool{upperTool}
				a := New(cfg)
				a.Subscribe(sub)
				if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
					t.Fatal(err)
				}
			},
			hook: []string{"prompt:user", "tool:function_call_output"},
		},
		{
			name: "a decision's note follows the batch's outputs",
			drive: func(t *testing.T, cfg Config, sub func(context.Context, Event) error) {
				// The echo adapter answers a developer message with
				// another call, so the budget ends the run.
				cfg.Tools, cfg.MaxTurns = []agenttool.Tool{upperTool}, 2
				cfg.BeforeToolCall = func(context.Context, ToolCallInfo) (*ToolDecision, error) {
					return &ToolDecision{Note: "careful"}, nil
				}
				a := New(cfg)
				a.Subscribe(sub)
				if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
					t.Fatal(err)
				}
			},
			hook: []string{"prompt:user", "tool:function_call_output tool:developer"},
		},
		{
			name: "Steer during a tool call, then Deliver of an output",
			drive: func(t *testing.T, cfg Config, sub func(context.Context, Event) error) {
				var a *Agent
				cfg.Tools = []agenttool.Tool{agenttool.New("work", "", func(ctx context.Context, _ echoArgs) (string, error) {
					a.Steer(openresponses.UserText("also do this"))
					if _, _, err := a.Deliver(ContextWithTrigger(ctx, Trigger{Kind: "task", Ref: "bg"}), openresponses.NewFunctionCallOutput("bg_1", "the background task is done")); err != nil {
						return "", err
					}
					return "started", nil
				})}
				a = New(cfg)
				a.Subscribe(sub)
				if _, err := a.Prompt(context.Background(), openresponses.UserText("start")); err != nil {
					t.Fatal(err)
				}
			},
			hook: []string{"prompt:user", "tool:function_call_output steer:user deliver:function_call_output@task:bg"},
		},
		{
			name: "Deliver of a user message to an idle agent",
			drive: func(t *testing.T, cfg Config, sub func(context.Context, Event) error) {
				a := New(cfg)
				a.Subscribe(sub)
				joined, end, err := a.Deliver(ContextWithTrigger(context.Background(), Trigger{Kind: "task", Ref: "bg"}), openresponses.UserText("the answer is 42"))
				if err != nil || joined || end == nil || end.Reason != ReasonDone {
					t.Fatalf("joined=%v end=%+v err=%v", joined, end, err)
				}
			},
			hook: []string{"deliver:user@task:bg"},
		},
		{
			name: "Queue with a trigger, steer and follow-up",
			drive: func(t *testing.T, cfg Config, sub func(context.Context, Event) error) {
				a := New(cfg)
				a.Subscribe(sub)
				if err := a.Queue(ContextWithTrigger(context.Background(), cron), QueueSteer, openresponses.UserText("steered")); err != nil {
					t.Fatal(err)
				}
				a.FollowUp(openresponses.UserText("and then"))
				if _, err := a.Prompt(context.Background(), openresponses.UserText("first")); err != nil {
					t.Fatal(err)
				}
			},
			hook: []string{"prompt:user steer:user@cron:nightly", "follow_up:user"},
		},
		{
			name: "Resume with an output and a note",
			drive: func(t *testing.T, cfg Config, sub func(context.Context, Event) error) {
				cfg.Tools, cfg.BeforeToolCall = []agenttool.Tool{upperTool}, deferAll
				a := New(cfg)
				a.Subscribe(sub)
				end, err := a.Prompt(context.Background(), openresponses.UserText("abc"))
				if err != nil || end.Reason != ReasonInputRequired || len(end.Pending) != 1 {
					t.Fatalf("end=%+v err=%v", end, err)
				}
				if _, err := a.Resume(ContextWithTrigger(context.Background(), cron), Output(openresponses.NewFunctionCallOutput(end.Pending[0].Call.CallID, "ABC")).WithNote("I ran it myself")); err != nil {
					t.Fatal(err)
				}
			},
			hook: []string{"prompt:user", "resume:function_call_output resume:user"},
		},
		{
			name: "Resume with an approval",
			drive: func(t *testing.T, cfg Config, sub func(context.Context, Event) error) {
				cfg.Tools, cfg.BeforeToolCall = []agenttool.Tool{upperTool}, deferAll
				a := New(cfg)
				a.Subscribe(sub)
				end, err := a.Prompt(context.Background(), openresponses.UserText("abc"))
				if err != nil || end.Reason != ReasonInputRequired || len(end.Pending) != 1 {
					t.Fatalf("end=%+v err=%v", end, err)
				}
				if _, err := a.Resume(context.Background(), Approve(end.Pending[0].Call.CallID).WithNote("go ahead")); err != nil {
					t.Fatal(err)
				}
			},
			hook: []string{"prompt:user", "tool:function_call_output tool:user"},
		},
		{
			name: "Prompt opening with the outputs of pending calls",
			drive: func(t *testing.T, cfg Config, sub func(context.Context, Event) error) {
				cfg.Tools, cfg.BeforeToolCall = []agenttool.Tool{upperTool}, deferAll
				a := New(cfg)
				a.Subscribe(sub)
				end, err := a.Prompt(context.Background(), openresponses.UserText("abc"))
				if err != nil || end.Reason != ReasonInputRequired || len(end.Pending) != 1 {
					t.Fatalf("end=%+v err=%v", end, err)
				}
				if _, err := a.Prompt(ContextWithTrigger(context.Background(), cron), openresponses.NewFunctionCallOutput(end.Pending[0].Call.CallID, "ABC"), openresponses.UserText("next")); err != nil {
					t.Fatal(err)
				}
			},
			hook: []string{"prompt:user", "resume:function_call_output prompt:user@cron:nightly"},
		},
		{
			name: "Continue after a terminating tool result",
			drive: func(t *testing.T, cfg Config, sub func(context.Context, Event) error) {
				cfg.Tools = []agenttool.Tool{terminating2()}
				a := New(cfg)
				a.Subscribe(sub)
				end, err := a.Prompt(context.Background(), openresponses.UserText("hand off"))
				if err != nil || end.Cause != StopTerminate {
					t.Fatalf("end=%+v err=%v", end, err)
				}
				if _, err := a.Continue(context.Background()); err != nil {
					t.Fatal(err)
				}
			},
			hook:    []string{"prompt:user", "continued:function_call_output"},
			arrived: []string{"prompt:user", "continued:function_call_output"},
			inputs:  []string{"user", ""},
		},
		{
			name: "Continue after a terminating tool result, with a steer held",
			drive: func(t *testing.T, cfg Config, sub func(context.Context, Event) error) {
				var a *Agent
				cfg.Tools = []agenttool.Tool{agenttool.New("transfer", "", func(context.Context, echoArgs) (agenttool.Result, error) {
					a.Steer(openresponses.UserText("one more thing"))
					return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "transferred"}, Terminate: true}, nil
				})}
				a = New(cfg)
				a.Subscribe(sub)
				end, err := a.Prompt(context.Background(), openresponses.UserText("hand off"))
				if err != nil || end.Cause != StopTerminate || a.State().Steering != 1 {
					t.Fatalf("end=%+v err=%v steering=%d", end, err, a.State().Steering)
				}
				if _, err := a.Continue(context.Background()); err != nil {
					t.Fatal(err)
				}
			},
			hook: []string{"prompt:user", "continued:function_call_output steer:user"},
		},
		{
			name: "BeforeTurn's items are in Arrived and not in Inputs",
			drive: func(t *testing.T, cfg Config, sub func(context.Context, Event) error) {
				cfg.BeforeTurn = ChainBeforeTurn(cfg.BeforeTurn, func(context.Context, TurnStartInfo) (openresponses.Items, error) {
					return openresponses.Items{openresponses.DeveloperText("the time is now"), Hidden(openresponses.UserText("psst"))}, nil
				})
				a := New(cfg)
				a.Subscribe(sub)
				if _, err := a.Prompt(context.Background(), openresponses.UserText("hi")); err != nil {
					t.Fatal(err)
				}
			},
			hook:    []string{"prompt:user"},
			arrived: []string{"prompt:user hook:developer hook:user"},
			inputs:  []string{"user developer user"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := &inputsLog{}
			tc.drive(t, Config{Model: &echo.Adapter{}, BeforeTurn: log.beforeTurn}, log.event)
			log.mu.Lock()
			defer log.mu.Unlock()
			if strings.Join(log.hook, " | ") != strings.Join(tc.hook, " | ") {
				t.Errorf("BeforeTurn saw %q, want %q", log.hook, tc.hook)
			}
			if tc.arrived != nil && strings.Join(log.arrived, " | ") != strings.Join(tc.arrived, " | ") {
				t.Errorf("turn_start arrived %q, want %q", log.arrived, tc.arrived)
			}
			if tc.inputs != nil && strings.Join(log.inputs, " | ") != strings.Join(tc.inputs, " | ") {
				t.Errorf("turn_start inputs %q, want %q", log.inputs, tc.inputs)
			}
		})
	}
}
