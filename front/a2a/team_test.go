package a2a

import (
	"context"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/handoff"
	"github.com/ChristopherDavenport/openresponses"
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
// WithMaxHandoffs or the default, and the task completes rather than
// running until it is canceled.
func TestHandoffsWithinATaskAreBounded(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
		want int
	}{
		{"the default", nil, 10},
		{"WithMaxHandoffs", []Option{WithMaxHandoffs(3)}, 3},
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
			if task.Status.State != a2a.TaskStateCompleted {
				t.Fatalf("task = %s %q", task.Status.State, taskText(task))
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
