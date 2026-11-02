package session

import (
	"context"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/handoff"
	"github.com/ChristopherDavenport/openresponses"
)

// handsOff is a model that, on a user message, says a preamble and
// calls the transfer tool to to, and answers with its name otherwise or
// when it hands to nobody.
type handsOff struct{ name, to string }

func (m handsOff) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	last, _ := req.Input[len(req.Input)-1].(*openresponses.Message)
	if m.to == "" || last == nil || last.Role != openresponses.RoleUser {
		if err := em.Item(openresponses.AssistantText(m.name + " here")); err != nil {
			return err
		}
		return em.Complete()
	}
	if err := em.Item(openresponses.AssistantText("Let me get you to " + m.to + ".")); err != nil {
		return err
	}
	w, err := em.FunctionCall("", handoff.ToolName(m.to))
	if err != nil {
		return err
	}
	if err := w.Arguments(`{}`); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Complete()
}

// TestATrimmedHandoffsNoteIsRecorded pins #276's note on the record: a
// handoff whose trim leaves the sender's preamble last continues with a
// developer note, which reaches the receiver as its run's input, so the
// recorder writes it, and every response of the conversation verifies,
// the receiver's later ones included, with the note written once.
func TestATrimmedHandoffsNoteIsRecorded(t *testing.T) {
	ctx := context.Background()
	team, err := handoff.New(
		handoff.Agent{Config: agentturn.Config{Name: "Triage agent", ModelName: "m", Model: handsOff{name: "triage", to: "Refund agent"}},
			Handoffs: []handoff.To{{Agent: "Refund agent", Filter: handoff.RemoveAllTools}}},
		handoff.Agent{Config: agentturn.Config{Name: "Refund agent", ModelName: "m", Model: handsOff{name: "refunds"}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	rec, s, err := Start(ctx, agentsession.NewMemoryStore(), agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(team.Config("Triage agent"))
	defer rec.Attach(a)()
	for _, text := range []string{"I was double charged", "well?"} {
		end, path, err := team.Run(ctx, a, openresponses.UserText(text))
		if err != nil {
			t.Fatalf("run %q: %v (path %v)", text, err, path)
		}
		if got, _ := end.Answer(); got != "refunds here" {
			t.Fatalf("answer to %q = %q", text, got)
		}
	}
	verifyAll(t, s)
	notes := 0
	for _, e := range s.Entries() {
		if ie, ok := e.(*agentsession.ItemEntry); ok {
			if m, ok := ie.Item.(*openresponses.Message); ok && m.Role == openresponses.RoleDeveloper {
				notes++
			}
		}
	}
	if notes != 1 {
		t.Errorf("the record holds %d developer notes, want the one the handoff added: %s", notes, entryTypes(s))
	}
}
