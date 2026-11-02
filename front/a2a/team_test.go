package a2a

import (
	"context"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/handoff"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
)

// TestATeamIsServedByItsRoute pins #274's binding: a handoff.Team's
// Route is what WithTransfers takes, so the conversation's next task
// starts under the agent the last transfer handed it to.
func TestATeamIsServedByItsRoute(t *testing.T) {
	var served []string
	serves := func(name string) func(context.Context, *openresponses.Request) error {
		return func(context.Context, *openresponses.Request) error {
			served = append(served, name)
			return nil
		}
	}
	team, err := handoff.New(
		handoff.Agent{Config: agentturn.Config{Name: "Triage agent", Model: preamble{}, BeforeModelCall: serves("triage")}, Handoffs: []handoff.To{{Agent: "Refund agent"}}},
		handoff.Agent{Config: agentturn.Config{Name: "Refund agent", Model: callsOnce{"issue_refund"}, Tools: []agenttool.Tool{issueRefundTool}, BeforeModelCall: serves("refunds")}},
	)
	if err != nil {
		t.Fatal(err)
	}
	h := a2asrv.NewHandler(New(team.Config("Triage agent"), WithTransfers(team.Route())))
	first := sendTask(t, h, userMessage("I was double charged"))
	if first.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("first task = %s %q", first.Status.State, taskText(first))
	}
	served = nil
	next := userMessage("please refund it")
	next.ContextID = first.ContextID
	task := sendTask(t, h, next)
	if task.Status.State != a2a.TaskStateCompleted || taskText(task) != "Tool result: refunded" {
		t.Errorf("second task = %s %q, want the refund agent's answer", task.Status.State, taskText(task))
	}
	if len(served) == 0 || served[0] != "refunds" {
		t.Errorf("second task served by %v, want the refund agent", served)
	}
}

// TestHandoffsWithinATaskAreBounded pins RFC 0001's MUST: a pair of
// agents that hand a task back and forth stop after the front's bound,
// WithMaxHandoffs or the default, and the task fails saying so rather
// than running until it is canceled or ending as if answered.
func TestHandoffsWithinATaskAreBounded(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
		want int
	}{
		// WithHandoff is asked once past the bound, which is how the
		// front knows the agents would go on.
		{"the default", nil, 11},
		{"WithMaxHandoffs", []Option{WithMaxHandoffs(3)}, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := agentturn.Config{Name: "a", Model: alwaysCalls{"transfer_to_billing"}, Tools: []agenttool.Tool{transferToBilling}}
			b := agentturn.Config{Name: "b", Model: alwaysCalls{"transfer_to_billing"}, Tools: []agenttool.Tool{transferToBilling}}
			asked := 0
			opts := append([]Option{WithHandoff(func(_ context.Context, end *agentturn.RunEnd, _ []*agentturn.ToolEnd) (agentturn.Config, bool) {
				asked++
				if asked%2 == 1 {
					return b, true
				}
				return a, true
			})}, tc.opts...)
			task := sendTask(t, a2asrv.NewHandler(New(a, opts...)), userMessage("go"))
			// The caller is told the agents stopped at the bound.
			if task.Status.State != a2a.TaskStateFailed || !strings.Contains(taskText(task), "handoffs") {
				t.Fatalf("task = %s %q, want failed naming the handoffs", task.Status.State, taskText(task))
			}
			if asked != tc.want {
				t.Errorf("handoffs taken = %d, want %d", asked, tc.want)
			}
		})
	}
}

var issueRefundTool = agenttool.NewFunc("issue_refund", "refunds a charge", nil,
	func(context.Context, agenttool.Call) (agenttool.Result, error) {
		return agenttool.Text("refunded"), nil
	})

// TestATeamTrimsWithinATask pins WithHandoffTrim with a team's Handoff
// and Trim: a transfer within a task is relayed to the receiver with
// the handoff's filter applied, so its first request holds no tool call
// and ends in the note naming the handoff, while the stored
// conversation keeps the history whole; the receiver's later tasks see
// the conversation trimmed the same way (#276).
func TestATeamTrimsWithinATask(t *testing.T) {
	var inputs []string
	team, err := handoff.New(
		handoff.Agent{Config: agentturn.Config{Name: "Triage agent", Model: preamble{}}, Handoffs: []handoff.To{{Agent: "Refund agent", Filter: handoff.RemoveAllTools}}},
		handoff.Agent{Config: agentturn.Config{Name: "Refund agent", Model: &echo.Adapter{}, BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
			inputs = append(inputs, itemTypes(req.Input))
			return nil
		}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	store := &MemoryStore{}
	h := a2asrv.NewHandler(New(team.Config("Triage agent"), WithConversationStore(store), WithTransfers(team.Route()), WithHandoff(team.Handoff), WithHandoffTrim(team.Trim)))
	task := sendTask(t, h, userMessage("I was double charged"))
	if task.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("task = %s %q", task.Status.State, taskText(task))
	}
	if len(inputs) != 1 || inputs[0] != "user assistant developer" {
		t.Errorf("receiver's inputs = %v, want the trimmed history ending in the note", inputs)
	}
	stored, _ := store.Load(context.Background(), task.ContextID)
	if got := itemTypes(stored); !strings.Contains(got, "function_call function_call_output") {
		t.Errorf("stored conversation = %s, want the transfer kept", got)
	}
	// The trim holds for the receiver's later tasks: the next message
	// starts under it, and it sees the conversation as trimmed.
	next := userMessage("when is the refund")
	next.ContextID = task.ContextID
	if task := sendTask(t, h, next); task.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("next task = %s %q", task.Status.State, taskText(task))
	}
	if len(inputs) != 2 || inputs[1] != "user assistant developer assistant user" {
		t.Errorf("receiver's inputs = %v, want the next task's trimmed too", inputs)
	}
}
