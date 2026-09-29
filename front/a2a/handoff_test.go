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
		handoff func(context.Context, *agentturn.RunEnd) (agentturn.Config, bool)
		want    string
		runs    int
		stored  string
	}{
		{"no handoff", nil, "transferred", 1, "user assistant function_call function_call_output"},
		{"declined", func(context.Context, *agentturn.RunEnd) (agentturn.Config, bool) { return agentturn.Config{}, false }, "transferred", 1,
			"user assistant function_call function_call_output"},
		{"handed off", func(context.Context, *agentturn.RunEnd) (agentturn.Config, bool) { return billing, true }, "Tool result: transferred", 2,
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
				opts = append(opts, WithHandoff(func(ctx context.Context, end *agentturn.RunEnd) (agentturn.Config, bool) {
					if end.Cause != agentturn.StopTerminate {
						t.Errorf("handoff asked about %s/%s", end.Reason, end.Cause)
					}
					return tc.handoff(ctx, end)
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
