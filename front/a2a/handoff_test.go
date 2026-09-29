package a2a

import (
	"context"
	"encoding/json"
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
