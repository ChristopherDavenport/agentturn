package agentturn

import (
	"context"
	"slices"
	"testing"

	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// personTexts is the text of each message of t that ctx names as the
// person's.
func personTexts(ctx context.Context, t Transcript) []string {
	persons := PersonMessagesFromContext(ctx)
	var out []string
	for _, item := range t {
		if persons.Has(item) {
			out = append(out, item.(*openresponses.Message).Text())
		}
	}
	return out
}

// TestTheTransformKnowsThePersonsMessages is the loop's half of #298:
// the transform's context names the user messages that came in under
// the person's trigger, and none the host composed, copied or hid, nor
// one that came in under no trigger.
func TestTheTransformKnowsThePersonsMessages(t *testing.T) {
	must := func(item openresponses.Item, err error) openresponses.Item {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return item
	}
	var seen []string
	a := New(Config{Model: &echo.Adapter{}, Transform: func(ctx context.Context, t Transcript) (Transcript, error) {
		seen = personTexts(ctx, t)
		return t, nil
	}})
	person := ContextWithTrigger(context.Background(), Trigger{Kind: TriggerHuman})
	_, err := a.Prompt(person,
		openresponses.UserText("typed"),
		Hidden(openresponses.UserText("hidden")),
		Copied(openresponses.UserText("copied")),
		must(WithProvenance(openresponses.UserText("layered"), Provenance{Layers: []Layer{{Source: "dax.prompts", ID: "review"}}})),
		must(WithProvenance(openresponses.UserText("reminder"), Provenance{Source: Trigger{Kind: "reminder"}})),
		must(WithProvenance(openresponses.UserText("claimed"), Provenance{Source: Trigger{Kind: TriggerHuman}})),
		openresponses.DeveloperText("developer"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"typed", "claimed"}; !slices.Equal(seen, want) {
		t.Errorf("the transform was told the person wrote %q, want %q", seen, want)
	}
	// A follow-up queued under the person's trigger is theirs; a prompt
	// under no trigger makes no claim. The first run's stay named.
	if err := a.Queue(person, QueueFollowUp, openresponses.UserText("followed")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Prompt(context.Background(), openresponses.UserText("untriggered")); err != nil {
		t.Fatal(err)
	}
	if want := []string{"typed", "claimed", "followed"}; !slices.Equal(seen, want) {
		t.Errorf("after a second run the transform was told the person wrote %q, want %q", seen, want)
	}
}

// TestAHostsPersonMessagesReachTheTransformAlone: what a host attaches
// for Run reaches the transform, and no hook, whose runs are prompted
// with copies.
func TestAHostsPersonMessagesReachTheTransformAlone(t *testing.T) {
	asked := openresponses.UserText("asked before")
	var seen []string
	hooked := true
	cfg := Config{
		Model: &echo.Adapter{},
		Transform: func(ctx context.Context, t Transcript) (Transcript, error) {
			seen = personTexts(ctx, t)
			return t, nil
		},
		BeforeTurn: func(ctx context.Context, _ TurnStartInfo) (openresponses.Items, error) {
			hooked = PersonMessagesFromContext(ctx) != nil
			return nil, nil
		},
	}
	ctx := ContextWithPersonMessages(context.Background(), PersonMessages{asked: true})
	for ev := range Run(ctx, Transcript{asked, openresponses.AssistantText("ok")}, openresponses.Items{openresponses.UserText("next")}, cfg) {
		if end, ok := ev.(*RunEnd); ok && end.Err != nil {
			t.Fatal(end.Err)
		}
	}
	if want := []string{"asked before"}; !slices.Equal(seen, want) {
		t.Errorf("the transform was told the person wrote %q, want %q", seen, want)
	}
	if hooked {
		t.Error("a hook's context names the person's messages")
	}
}
