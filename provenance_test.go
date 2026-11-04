package agentturn

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestProvenanceRidesOnTheItemEvents pins that a host or a hook can say
// where an item it composed came from and whether it is trusted, and
// that the loop carries it unread to the events a recorder sees: the
// item a hook returns, an item a run is prompted with and an item
// queued, each on its item_end, and the queued one on its queued report
// with its source as the trigger. A plain prompt carries none.
func TestProvenanceRidesOnTheItemEvents(t *testing.T) {
	agents, err := WithProvenance(openresponses.UserText("<reminder>AGENTS.md</reminder>"), Provenance{
		Layers: []Layer{{ID: "/repo/AGENTS.md", Source: "agentsmd"}},
		Trust:  &Trust{Reads: []agenttool.Label{agenttool.LabelUntrusted}},
	})
	if err != nil {
		t.Fatal(err)
	}
	mcp := Trigger{Kind: "mcp_prompt", Ref: "docs/review", Source: "mcp"}
	prompted, err := WithProvenance(openresponses.UserText("review this"), Provenance{Source: mcp, Trust: &Trust{}})
	if err != nil {
		t.Fatal(err)
	}
	steered, err := WithProvenance(Hidden(openresponses.UserText("resource text")), Provenance{Source: mcp, Trust: &Trust{Reads: []agenttool.Label{agenttool.LabelUntrusted}}})
	if err != nil {
		t.Fatal(err)
	}
	turns := 0
	a := New(Config{Model: &echo.Adapter{}, MaxTurns: 2,
		BeforeTurn: func(context.Context, TurnStartInfo) (openresponses.Items, error) {
			turns++
			if turns == 1 {
				return openresponses.Items{agents}, nil
			}
			return nil, nil
		}})
	got := map[string]*Provenance{}
	var queued *Queued
	arrived := map[string]Trigger{}
	a.Subscribe(func(_ context.Context, ev Event) error {
		switch e := ev.(type) {
		case *ItemEnd:
			if m, ok := e.Item.(*openresponses.Message); ok && e.ResponseID == "" {
				got[m.Text()] = e.Provenance
			}
		case *ItemStart:
			if m, ok := e.Item.(*openresponses.Message); ok && e.ResponseID == "" && m.Text() == "review this" && e.Provenance == nil {
				t.Error("item_start of a composed item carries no provenance")
			}
		case *TurnStart:
			for _, in := range e.Arrived {
				if m, ok := in.Item.(*openresponses.Message); ok {
					arrived[m.Text()] = in.Trigger
				}
			}
		case *Queued:
			queued = e
		}
		return nil
	})
	if err := a.Queue(context.Background(), QueueSteer, steered); err != nil {
		t.Fatal(err)
	}
	human := Trigger{Kind: "human", Ref: "tty"}
	if _, err := a.Prompt(ContextWithTrigger(context.Background(), human), openresponses.UserText("mine"), prompted); err != nil {
		t.Fatal(err)
	}
	// The turn's inputs name each item's own source over the run's
	// trigger, as its queued report does, and the run's for the rest.
	for text, want := range map[string]string{"mine": "human", "review this": "mcp_prompt", "resource text": "mcp_prompt", "<reminder>AGENTS.md</reminder>": "mcp_prompt"} {
		if text == "<reminder>AGENTS.md</reminder>" {
			// The hook's item names no source and arrives under none.
			if got, ok := arrived[text]; !ok || !got.IsZero() {
				t.Errorf("turn input %q trigger = %+v (%v), want none", text, got, ok)
			}
			continue
		}
		if got := arrived[text]; got.Kind != want {
			t.Errorf("turn input %q trigger = %+v, want kind %q", text, got, want)
		}
	}
	if p := got["mine"]; p != nil {
		t.Errorf("a plain prompt carries provenance %+v", p)
	}
	if p := got["<reminder>AGENTS.md</reminder>"]; p == nil || !slices.Equal(p.Layers, []Layer{{ID: "/repo/AGENTS.md", Source: "agentsmd"}}) || p.Trust == nil || !slices.Equal(p.Trust.Reads, []agenttool.Label{agenttool.LabelUntrusted}) {
		t.Errorf("hook's item provenance = %+v", p)
	}
	if p := got["review this"]; p == nil || p.Source.Kind != "mcp_prompt" || p.Trust == nil || len(p.Trust.Reads) != 0 {
		t.Errorf("prompted item provenance = %+v", p)
	}
	if p := got["resource text"]; p == nil || p.Source.Ref != "docs/review" || p.Trust == nil || p.Trust.Reads[0] != agenttool.LabelUntrusted {
		t.Errorf("queued item provenance = %+v", p)
	}
	if queued == nil || queued.Provenance == nil || !queued.Hidden || queued.Trigger.Kind != "mcp_prompt" || queued.Trigger.Ref != "docs/review" {
		t.Errorf("queued report = %+v, want the item's source as its trigger", queued)
	}
	for i, item := range a.State().Transcript {
		if _, ok := item.(markedItem); ok {
			t.Errorf("transcript[%d] is still wrapped", i)
		}
	}
}

// TestWithProvenanceRefusesWhatTheFormatCannotHold pins the checks
// made where the provenance is attached: a layer only on a message,
// each with a source, an instruction file's with its id, none named
// twice; trust labels without repeats; and a source the trigger rules
// allow.
func TestWithProvenanceRefusesWhatTheFormatCannotHold(t *testing.T) {
	msg := openresponses.UserText("x")
	for _, tc := range []struct {
		name string
		item openresponses.Item
		p    Provenance
		ok   bool
	}{
		{name: "layers on a message", item: msg, p: Provenance{Layers: []Layer{{Source: "dax.prompts", ID: "review.md"}, {Source: "agentmemory"}}}, ok: true},
		{name: "trust alone", item: msg, p: Provenance{Trust: &Trust{}}, ok: true},
		{name: "layers on a call output", item: openresponses.NewFunctionCallOutput("c", "x"), p: Provenance{Layers: []Layer{{Source: "agentmemory"}}}},
		{name: "a layer with no source", item: msg, p: Provenance{Layers: []Layer{{ID: "a"}}}},
		{name: "an instruction file with no id", item: msg, p: Provenance{Layers: []Layer{{Source: "agentsmd"}}}},
		{name: "a layer named twice", item: msg, p: Provenance{Layers: []Layer{{ID: "a", Source: "agentsmd"}, {ID: "a", Source: "agentsmd"}}}},
		{name: "a source named twice", item: msg, p: Provenance{Layers: []Layer{{Source: "agentmemory"}, {Source: "agentmemory"}}}},
		{name: "a repeated label", item: msg, p: Provenance{Trust: &Trust{Reads: []agenttool.Label{"private", "private"}}}},
		{name: "an empty label", item: msg, p: Provenance{Trust: &Trust{Reads: []agenttool.Label{""}}}},
		{name: "a bad source", item: msg, p: Provenance{Source: Trigger{Kind: "k", Extra: map[string]any{"kind": "x"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := WithProvenance(tc.item, tc.p)
			if tc.ok {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				p, ok := ProvenanceOf(got)
				if !ok || len(p.Layers) != len(tc.p.Layers) {
					t.Errorf("ProvenanceOf = %+v, %v", p, ok)
				}
				if base, _ := Unhide(got); base != tc.item {
					t.Errorf("Unhide = %v, want the item", base)
				}
				return
			}
			if !errors.Is(err, ErrProvenance) || got != nil {
				t.Errorf("WithProvenance = %v, %v, want ErrProvenance", got, err)
			}
		})
	}
	// A provenance that says nothing marks nothing, and no item stays
	// none, as Hidden(nil) does.
	if got, err := WithProvenance(msg, Provenance{}); err != nil || got != openresponses.Item(msg) {
		t.Errorf("WithProvenance with nothing = %v, %v, want the item itself", got, err)
	}
	if got, err := WithProvenance(nil, Provenance{Trust: &Trust{}}); err != nil || got != nil {
		t.Errorf("WithProvenance(nil) = %v, %v", got, err)
	}
	// The marks compose, and the provenance is the caller's copy, not
	// a list the caller may change after.
	reads := []agenttool.Label{agenttool.LabelPrivate}
	marked, err := WithProvenance(Copied(msg), Provenance{Trust: &Trust{Reads: reads}})
	if err != nil {
		t.Fatal(err)
	}
	reads[0] = "changed"
	marked = Hidden(marked)
	if p, ok := ProvenanceOf(marked); !ok || p.Trust.Reads[0] != agenttool.LabelPrivate {
		t.Errorf("provenance after the caller's change = %+v", p)
	}
	if _, hidden, copied := marks(marked); !hidden || !copied {
		t.Errorf("marks hidden=%v copied=%v, want both", hidden, copied)
	}
	if _, ok := ProvenanceOf(msg); ok {
		t.Error("a plain item has provenance")
	}
}
