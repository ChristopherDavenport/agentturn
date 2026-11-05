package session

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/compact"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestAResumeKnowsThePersonsMessages is #298 on the record: an agent
// seeded from a session by AgentOptions hands its transform the
// person's own messages of the transcript it resumes, the one a fold
// pinned into the compaction entry included, and not the summary nor
// a message the host composed, so the fold after a restart keeps them
// verbatim as the fold before it did.
func TestAResumeKnowsThePersonsMessages(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, _, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	tr := compact.NewLocal(&echo.Adapter{}, compact.WithBudget(4), compact.WithKeepLast(2),
		compact.WithEstimator(countItems), compact.WithOnFold(rec.Fold))
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: tr.Transform})
	detach := rec.Attach(a)
	person := agentturn.ContextWithTrigger(context.Background(), agentturn.Trigger{Kind: agentturn.TriggerHuman})
	reminder, err := agentturn.WithProvenance(openresponses.UserText("a reminder"), agentturn.Provenance{Source: agentturn.Trigger{Kind: "reminder"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, prompt := range []openresponses.Items{{openresponses.UserText("one")}, {reminder}, {openresponses.UserText("two")}, {openresponses.UserText("three")}} {
		if _, err := a.Prompt(person, prompt...); err != nil {
			t.Fatal(err)
		}
	}
	detach()

	rec2, s2, err := Resume(context.Background(), store, rec.SessionID())
	if err != nil {
		t.Fatal(err)
	}
	var pinned []string
	for _, e := range s2.Entries() {
		if c, ok := e.(*agentsession.CompactionEntry); ok {
			pinned = texts(c.Pinned)
		}
	}
	if !slices.Contains(pinned, "one") {
		t.Fatalf("the last fold before the restart pinned %q, not the first prompt", pinned)
	}
	opts, err := AgentOptions(s2, rec2.ReadOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	var seen []string
	tr2 := compact.NewLocal(&echo.Adapter{}, compact.WithBudget(4), compact.WithKeepLast(2),
		compact.WithEstimator(countItems), compact.WithOnFold(rec2.Fold))
	transform := func(ctx context.Context, t agentturn.Transcript) (agentturn.Transcript, error) {
		if seen == nil {
			for _, item := range t {
				if agentturn.PersonMessagesFromContext(ctx).Has(item) {
					seen = append(seen, texts(openresponses.Items{item})...)
				}
			}
		}
		return tr2.Transform(ctx, t)
	}
	b := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: transform}, opts...)
	defer rec2.Attach(b)()
	if _, err := b.Prompt(person, openresponses.UserText("four")); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"one", "three", "four"} {
		if !slices.Contains(seen, want) {
			t.Errorf("the resumed transform was told the person wrote %q, not %q", seen, want)
		}
	}
	for _, text := range seen {
		if strings.HasPrefix(text, "Summary of") || text == "a reminder" {
			t.Errorf("the resumed transform was told the person wrote %q", text)
		}
	}
}

// texts is the text of each message of items.
func texts(items openresponses.Items) []string {
	var out []string
	for _, item := range items {
		if m, ok := item.(*openresponses.Message); ok {
			out = append(out, m.Text())
		}
	}
	return out
}
