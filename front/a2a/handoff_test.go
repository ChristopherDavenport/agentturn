package a2a

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
)

// TestTerminatingStop pins #163: an agent whose only tool terminates
// completes the task with the tool's output when nothing takes the
// handoff, and with the receiver's answer, in the same task, when
// WithHandoff hands the transcript to another configuration, whose run
// the recorder sees and the store keeps.
func TestTerminatingStop(t *testing.T) {
	transfer := agenttool.NewFunc("transfer_to_billing", "hands the conversation to billing", json.RawMessage(`{"type":"object"}`),
		func(context.Context, agenttool.Call) (agenttool.Result, error) {
			return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "transferred"}, Terminate: true}, nil
		})
	billing := agentturn.Config{Model: &echo.Adapter{}, Instructions: "You are billing."}
	for _, tc := range []struct {
		name    string
		handoff func(context.Context, *agentturn.RunEnd, []*agentturn.ToolEnd) (agentturn.Config, bool)
		want    string
		runs    int
		stored  string
	}{
		{"no handoff", nil, "transferred", 1, "user assistant function_call function_call_output"},
		{"declined", func(context.Context, *agentturn.RunEnd, []*agentturn.ToolEnd) (agentturn.Config, bool) {
			return agentturn.Config{}, false
		}, "transferred", 1,
			"user assistant function_call function_call_output"},
		{"handed off", func(context.Context, *agentturn.RunEnd, []*agentturn.ToolEnd) (agentturn.Config, bool) {
			return billing, true
		}, "Tool result: transferred", 2,
			"user assistant function_call function_call_output assistant"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &fakeRecorder{}
			store := &MemoryStore{}
			opts := []Option{
				WithConversationStore(store),
				WithCallerTools(openresponses.NewFunctionTool("remote", "caller side", json.RawMessage(`{"type":"object"}`))),
				WithRecorderFor(rec.attach),
			}
			if tc.handoff != nil {
				opts = append(opts, WithHandoff(func(ctx context.Context, end *agentturn.RunEnd, results []*agentturn.ToolEnd) (agentturn.Config, bool) {
					if end.Cause != agentturn.StopTerminate {
						t.Errorf("handoff asked about %s/%s", end.Reason, end.Cause)
					}
					return tc.handoff(ctx, end, results)
				}))
			}
			h := a2asrv.NewHandler(New(agentturn.Config{Model: preamble{}, ModelName: "m", Instructions: "You are triage.", Tools: []agenttool.Tool{transfer}}, opts...))
			task := sendTask(t, h, userMessage("I was double charged"))
			if task.Status.State != a2a.TaskStateCompleted || task.Status.Message == nil || partsText(task.Status.Message.Parts) != tc.want {
				t.Fatalf("task = %s %q, want completed %q", task.Status.State, taskText(task), tc.want)
			}
			stored, _ := store.Load(context.Background(), task.ContextID)
			if got := itemTypes(stored); got != tc.stored {
				t.Errorf("stored = %s, want %s", got, tc.stored)
			}
			rec.mu.Lock()
			defer rec.mu.Unlock()
			var starts int
			for _, ev := range rec.runs[0] {
				if ev == agentturn.EventRunStart {
					starts++
				}
			}
			if starts != tc.runs || !slices.Contains(rec.runs[0], agentturn.EventRunEnd) {
				t.Errorf("recorded %d runs, want %d: %v", starts, tc.runs, rec.runs[0])
			}
		})
	}
}

// TestHandoffBatch pins what a handoff is given and what a declined
// one answers when only part of the batch terminated: the terminating
// result's Details reach the function, the task's answer is the
// terminating call's output rather than a sibling's that came after
// it, and a receiver whose configuration sets no ToolRecorder records
// its tools' writes through the one the recorder put on the agent.
func TestHandoffBatch(t *testing.T) {
	transfer := agenttool.NewFunc("transfer_to_billing", "hands the conversation to billing", json.RawMessage(`{"type":"object"}`),
		func(context.Context, agenttool.Call) (agenttool.Result, error) {
			return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "transferred"}, Terminate: true, Details: "billing"}, nil
		})
	lookup := agenttool.NewFunc("lookup", "looks something up", json.RawMessage(`{"type":"object"}`),
		func(context.Context, agenttool.Call) (agenttool.Result, error) {
			return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "looked up"}}, nil
		})
	note := agenttool.New("note", "writes a record", func(ctx context.Context, in struct {
		Q string `json:"q"`
	}) (string, error) {
		return "noted", agenttool.WriteRecord(ctx, noteRecord(in))
	})
	// billing greets on its first turn, so callsEveryTool calls note
	// rather than answering the transfer's output.
	billing := agentturn.Config{Model: callsEveryTool{}, Tools: []agenttool.Tool{note}, BeforeTurn: func(_ context.Context, info agentturn.TurnStartInfo) (openresponses.Items, error) {
		if info.Turn == 1 {
			return openresponses.Items{openresponses.DeveloperText("You are billing.")}, nil
		}
		return nil, nil
	}}
	for _, tc := range []struct {
		name    string
		handoff bool
		want    string
		records int
	}{
		{"declined", false, "transferred", 0},
		{"handed off", true, "transferred+looked up+noted", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &fakeRecorder{}
			asked := 0
			h := a2asrv.NewHandler(New(agentturn.Config{Model: callsEveryTool{}, Tools: []agenttool.Tool{transfer, lookup}},
				WithRecorderFor(rec.attach),
				WithHandoff(func(_ context.Context, end *agentturn.RunEnd, results []*agentturn.ToolEnd) (agentturn.Config, bool) {
					asked++
					if end.Cause != agentturn.StopPartialTerminate {
						t.Errorf("handoff asked about %s/%s", end.Reason, end.Cause)
					}
					var dest []any
					for _, res := range results {
						if res.Result.Terminate {
							dest = append(dest, res.Result.Details)
						}
					}
					if len(results) != 2 || len(dest) != 1 || dest[0] != "billing" {
						t.Errorf("handoff given %d results, destinations %v", len(results), dest)
					}
					return billing, tc.handoff
				})))
			task := sendTask(t, h, userMessage("I was double charged"))
			if task.Status.State != a2a.TaskStateCompleted || task.Status.Message == nil || partsText(task.Status.Message.Parts) != tc.want {
				t.Fatalf("task = %s %q, want completed %q", task.Status.State, taskText(task), tc.want)
			}
			if asked != 1 {
				t.Errorf("handoff asked %d times, want 1", asked)
			}
			rec.mu.Lock()
			defer rec.mu.Unlock()
			if len(rec.records) != tc.records {
				t.Errorf("records = %v, want %d", rec.records, tc.records)
			}
		})
	}
}

// transferToBilling is a handoff tool named for its destination: it
// answers on the model's behalf and ends the run.
var transferToBilling = agenttool.NewFunc("transfer_to_billing", "hands the conversation to billing", json.RawMessage(`{"type":"object"}`),
	func(context.Context, agenttool.Call) (agenttool.Result, error) {
		return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "transferred"}, Terminate: true}, nil
	})

// TestHandoffTrigger pins #182: the receiver's run after WithHandoff is
// continued under a handoff trigger naming the sender, which its
// BeforeTurn context carries and its run_start records; the sender's
// run carries none.
func TestHandoffTrigger(t *testing.T) {
	var turns []agentturn.Trigger
	seen := func(ctx context.Context, _ agentturn.TurnStartInfo) (openresponses.Items, error) {
		turns = append(turns, agentturn.TriggerFromContext(ctx))
		return nil, nil
	}
	billing := agentturn.Config{Name: "billing", Model: &echo.Adapter{}, BeforeTurn: seen}
	var starts []agentturn.Trigger
	exec := New(agentturn.Config{Name: "triage", Model: preamble{}, ModelName: "m", Tools: []agenttool.Tool{transferToBilling}, BeforeTurn: seen},
		WithHandoff(func(context.Context, *agentturn.RunEnd, []*agentturn.ToolEnd) (agentturn.Config, bool) {
			return billing, true
		}),
		WithRecorderFor(func(ctx context.Context, _ string, a *agentturn.Agent) (context.Context, func(), error) {
			return ctx, a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
				if s, ok := ev.(*agentturn.RunStart); ok {
					starts = append(starts, s.Trigger)
				}
				return nil
			}), nil
		}))
	task := sendTask(t, a2asrv.NewHandler(exec), userMessage("I was double charged"))
	if task.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("task = %s %q", task.Status.State, taskText(task))
	}
	want := []agentturn.Trigger{{}, {Kind: "handoff", Ref: "triage"}}
	if !reflect.DeepEqual(turns, want) {
		t.Errorf("BeforeTurn triggers = %+v, want %+v", turns, want)
	}
	if !reflect.DeepEqual(starts, want) {
		t.Errorf("run_start triggers = %+v, want %+v", starts, want)
	}
}

// TestStartAfterHandoff pins #180: the conversation's next task after a
// handoff starts under the configuration WithStart finds in the stored
// transcript, and under the executor's own without the option or when
// it declines.
func TestStartAfterHandoff(t *testing.T) {
	var served []string
	serves := func(name string) func(context.Context, *openresponses.Request) error {
		return func(context.Context, *openresponses.Request) error {
			served = append(served, name)
			return nil
		}
	}
	billing := agentturn.Config{Name: "billing", Model: &echo.Adapter{}, BeforeModelCall: serves("billing")}
	lastTransfer := func(_ context.Context, t agentturn.Transcript) (agentturn.Config, bool) {
		for i := len(t) - 1; i >= 0; i-- {
			if call, ok := t[i].(*openresponses.FunctionCall); ok && call.Name == "transfer_to_billing" {
				return billing, true
			}
		}
		return agentturn.Config{}, false
	}
	for _, tc := range []struct {
		name   string
		start  func(context.Context, agentturn.Transcript) (agentturn.Config, bool)
		want   []string
		answer string
	}{
		{"no start", nil, []string{"triage", "billing"}, "Tool result: transferred"},
		{"declined", func(context.Context, agentturn.Transcript) (agentturn.Config, bool) { return agentturn.Config{}, false }, []string{"triage", "billing"}, "Tool result: transferred"},
		{"last transfer", lastTransfer, []string{"billing"}, "when is the refund"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := []Option{WithHandoff(func(context.Context, *agentturn.RunEnd, []*agentturn.ToolEnd) (agentturn.Config, bool) {
				return billing, true
			})}
			if tc.start != nil {
				opts = append(opts, WithStart(tc.start))
			}
			h := a2asrv.NewHandler(New(agentturn.Config{Name: "triage", Model: preamble{}, ModelName: "m", Tools: []agenttool.Tool{transferToBilling}, BeforeModelCall: serves("triage")}, opts...))
			first := sendTask(t, h, userMessage("I was double charged"))
			served = nil
			next := userMessage("when is the refund")
			next.ContextID = first.ContextID
			task := sendTask(t, h, next)
			if task.Status.State != a2a.TaskStateCompleted || task.Status.Message == nil || partsText(task.Status.Message.Parts) != tc.answer {
				t.Errorf("task = %s %q, want completed %q", task.Status.State, taskText(task), tc.answer)
			}
			if !reflect.DeepEqual(served, tc.want) {
				t.Errorf("second task served by %v, want %v", served, tc.want)
			}
		})
	}
}

func itemTypes(items openresponses.Items) string {
	var out string
	for i, it := range items {
		typ := it.ItemType()
		if m, ok := it.(*openresponses.Message); ok {
			typ = string(m.Role)
		}
		if i > 0 {
			out += " "
		}
		out += typ
	}
	return out
}
