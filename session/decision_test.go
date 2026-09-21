package session

import (
	"context"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestDecisionsSayWhoDecidedAndWhy runs the confirmation prompt a CLI
// has: a policy holds a call with the rule that raised it, and a person
// at the terminal answers. The record names the rule on the hold and
// the person on the answer, whether they approved or wrote the output
// themselves.
func TestDecisionsSayWhoDecidedAndWhy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		answer  func(callID string) agentturn.Answer
		verdict string
		by      string
	}{
		{
			name:    "approved by a person",
			answer:  func(id string) agentturn.Answer { return agentturn.Approve(id).WithBy(agentsession.ByHuman) },
			verdict: agentsession.VerdictProceed,
			by:      agentsession.ByHuman,
		},
		{
			name: "refused by a person",
			answer: func(id string) agentturn.Answer {
				return agentturn.Output(openresponses.NewFunctionCallOutput(id, "denied by the user")).WithBy(agentsession.ByHuman)
			},
			verdict: agentsession.VerdictReject,
			by:      agentsession.ByHuman,
		},
		{
			name: "released by the policy that held it",
			answer: func(id string) agentturn.Answer {
				return agentturn.Approve(id).WithBy(agentsession.ByPolicy)
			},
			verdict: agentsession.VerdictProceed,
			by:      agentsession.ByPolicy,
		},
		{
			name:    "an answer that names nobody",
			answer:  agentturn.Approve,
			verdict: agentsession.VerdictProceed,
			by:      "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			hold := func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
				return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "approval required by upper(*)", By: agentsession.ByPolicy}, nil
			}
			a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{upper}, BeforeToolCall: hold})
			defer rec.Attach(a)()
			end, err := a.Prompt(context.Background(), openresponses.UserText("abc"))
			if err != nil || end.Reason != agentturn.ReasonInputRequired {
				t.Fatalf("prompt: err=%v end=%+v", err, end)
			}
			callID := end.Pending[0].Call.CallID
			if _, err := a.Resume(context.Background(), tc.answer(callID)); err != nil {
				t.Fatal(err)
			}
			verifyAll(t, s)
			c := callsOf(t, s)["upper"]
			if c == nil || len(c.Decisions) != 2 {
				t.Fatalf("decisions = %+v", c)
			}
			held := c.Decisions[0]
			if held.Verdict != agentsession.VerdictHold || held.By != agentsession.ByPolicy {
				t.Errorf("hold = %+v", held)
			}
			if held.Reason != "approval required by upper(*)" {
				t.Errorf("the hold does not say which rule raised it: %q", held.Reason)
			}
			answer := c.Decisions[1]
			if answer.Verdict != tc.verdict || answer.By != tc.by {
				t.Errorf("answer = verdict %q by %q, want %q by %q", answer.Verdict, answer.By, tc.verdict, tc.by)
			}
		})
	}
}

// TestHiddenItemsAreRecordedAsNotVisible checks a harness's own notice:
// the model reads it and a renderer of the file is told not to show it.
func TestHiddenItemsAreRecordedAsNotVisible(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m"})
	defer rec.Attach(a)()
	var sent openresponses.Items
	a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		if e, ok := ev.(*agentturn.TurnStart); ok {
			sent = e.Request.Input
		}
		return nil
	})
	notice := openresponses.DeveloperText("<system-interrupt>Do not use Box::leak.</system-interrupt>")
	if _, err := a.Prompt(context.Background(), openresponses.UserText("go"), agentturn.Hidden(notice)); err != nil {
		t.Fatal(err)
	}
	// The model read it, and it is the item itself on the wire.
	if len(sent) != 2 || sent[1].ItemType() != "message" {
		t.Fatalf("request input = %v", sent)
	}
	if m, ok := sent[1].(*openresponses.Message); !ok || m.Role != openresponses.RoleDeveloper {
		t.Errorf("the notice reached the model as %T", sent[1])
	}
	// The transcript holds the item, not the wrapper.
	if items := a.State().Transcript; len(items) != 3 {
		t.Fatalf("transcript = %d items", len(items))
	} else if _, ok := items[1].(*openresponses.Message); !ok {
		t.Errorf("transcript holds %T", items[1])
	}
	var entries []*agentsession.ItemEntry
	for _, e := range s.Entries() {
		if it, ok := e.(*agentsession.ItemEntry); ok {
			entries = append(entries, it)
		}
	}
	if len(entries) != 3 {
		t.Fatalf("item entries = %d", len(entries))
	}
	if !entries[0].IsVisible() {
		t.Error("the prompt is marked not visible")
	}
	if entries[1].IsVisible() {
		t.Error("the notice is not marked as hidden, so a reader shows it as a developer message")
	}
	if !entries[2].IsVisible() {
		t.Error("the answer is marked not visible")
	}
	// A hidden item is in the context, so the hashes still rebuild.
	if n := verifyAll(t, s); n != 1 || hashed(s) != 1 {
		t.Errorf("responses = %d hashed = %d", n, hashed(s))
	}
	cx, err := s.Context()
	if err != nil {
		t.Fatal(err)
	}
	if len(cx.Items) != 3 {
		t.Errorf("context = %d items, want the hidden one among them", len(cx.Items))
	}
}
