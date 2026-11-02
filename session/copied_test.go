package session

import (
	"context"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentsession/export"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestAChildOpenedWithItsParentsConversationRecordsItAsCopied pins RFC
// 0001's item_end row (#243): items a child run is prompted with that
// were taken from its parent's conversation are written with
// copied_from, naming the parent session and the entry each repeats,
// or the session alone for the placeholder output made for the copy,
// and the recorder writes no decision for the copied call. A reader
// takes them as context: the child's ATIF document has no agent step
// of its own from them and no failed call. The shape is the round 9
// nous-trajectories check S2.
func TestAChildOpenedWithItsParentsConversationRecordsItAsCopied(t *testing.T) {
	type taskArgs struct {
		Input string `json:"input"`
	}
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	helper := agent.New(agentturn.Config{Name: "helper", Description: "helps", Model: &echo.Adapter{}, ModelName: "child"},
		agent.WithCallConfig(func(_ context.Context, a taskArgs, parent agentturn.Transcript, cfg agentturn.Config) (agentturn.Config, openresponses.Items, error) {
			return cfg, append(append(openresponses.Items(nil), parent...), openresponses.UserText(a.Input)), nil
		}),
		agent.WithObserver(rec.Observe), agent.WithRunContext(rec.ChildContext))
	a := agentturn.New(agentturn.Config{Model: scriptedCalls{{"helper", `{"input":"summarise"}`}}, ModelName: "parent", Tools: []agenttool.Tool{helper}})
	defer rec.Attach(a)()
	if end, err := a.Prompt(ctx, openresponses.UserText("delegate")); err != nil || end.Reason != agentturn.ReasonDone {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	l := links(s)
	if len(l) != 1 {
		t.Fatalf("links = %+v", l)
	}
	child, err := store.Open(ctx, l[0].Session)
	if err != nil {
		t.Fatal(err)
	}
	parentEntry := map[string]string{} // item type/role → the parent's entry
	for _, e := range s.Path(s.Leaf()) {
		if ie, ok := e.(*agentsession.ItemEntry); ok {
			switch it := ie.Item.(type) {
			case *openresponses.Message:
				if it.Text() == "delegate" {
					parentEntry["user"] = ie.ID
				}
			case *openresponses.FunctionCall:
				parentEntry["call"] = ie.ID
			}
		}
	}
	var copied, own []string
	for _, e := range child.Path(child.Leaf()) {
		switch v := e.(type) {
		case *agentsession.DecisionEntry:
			t.Errorf("decision %+v in the child: the copied call is the parent's", v)
		case *agentsession.ItemEntry:
			cf := v.CopiedFrom
			switch it := v.Item.(type) {
			case *openresponses.Message:
				switch {
				case it.Text() == "delegate":
					if cf == nil || cf.Session != s.ID() || cf.Entry != parentEntry["user"] {
						t.Errorf("parent's message copied_from = %+v, want %s in %s", cf, parentEntry["user"], s.ID())
					}
				case it.Text() == "summarise" && cf != nil:
					t.Errorf("the child's own prompt is marked copied: %+v", cf)
				}
			case *openresponses.FunctionCall:
				if cf == nil || cf.Session != s.ID() || cf.Entry != parentEntry["call"] {
					t.Errorf("parent's call copied_from = %+v, want %s in %s", cf, parentEntry["call"], s.ID())
				}
			case *openresponses.FunctionCallOutput:
				if cf == nil || cf.Session != s.ID() || cf.Entry != "" {
					t.Errorf("placeholder output copied_from = %+v, want the parent session and no entry", cf)
				}
			}
			if cf != nil {
				copied = append(copied, v.Item.ItemType())
			} else {
				own = append(own, v.Item.ItemType())
			}
		}
	}
	if len(copied) != 3 {
		t.Errorf("copied items = %v, want the parent's message, call and placeholder", copied)
	}
	verifyAll(t, child)

	tr, err := export.At(child, child.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	doc, err := export.ToATIF(tr, export.Options{})
	if err != nil {
		t.Fatal(err)
	}
	agentSteps := 0
	for _, st := range doc.Steps {
		isCopied := st.IsCopiedContext != nil && *st.IsCopiedContext
		for _, tc := range st.ToolCalls {
			if tc.FunctionName == "helper" && !isCopied {
				t.Errorf("step %d calls helper as the child's own: %+v", st.StepID, st)
			}
		}
		if st.Source == "agent" && !isCopied {
			agentSteps++
		}
	}
	if agentSteps != 1 {
		t.Errorf("the child's own agent steps = %d, want its one answer", agentSteps)
	}
}
