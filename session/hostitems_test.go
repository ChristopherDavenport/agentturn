package session

import (
	"context"
	"slices"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// itemEntries returns the session's item entries holding a message,
// by its text.
func itemEntries(s *agentsession.Session) map[string]*agentsession.ItemEntry {
	out := map[string]*agentsession.ItemEntry{}
	for _, e := range s.Entries() {
		if it, ok := e.(*agentsession.ItemEntry); ok {
			if m, ok := it.Item.(*openresponses.Message); ok && it.ResponseID == "" {
				out[m.Text()] = it
			}
		}
	}
	return out
}

// TestHostComposedItemsAreRecordedWithTheirProvenance pins RFC 0001
// The record's rule for text the person did not write: a message a
// hook composed from an instruction file is recorded with
// instructions_part naming the file and the trust the host gave it, one
// composed from two layers names both, an item a host sent from an MCP
// server carries that server as its source, over the run's trigger,
// and its trust, and an item queued under a provenance is queued and
// drained with its source and recorded with its trust. A plain prompt
// is recorded as before: the run's trigger as its source, and no trust
// or instructions_part.
func TestHostComposedItemsAreRecordedWithTheirProvenance(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{Records: agentsession.AllRecords})
	if err != nil {
		t.Fatal(err)
	}
	untrusted := &agentturn.Trust{Reads: []agenttool.Label{agenttool.LabelUntrusted}}
	agentsMD, err := agentturn.WithProvenance(openresponses.UserText("<reminder>AGENTS.md says</reminder>"), agentturn.Provenance{
		Layers: []agentturn.Layer{{ID: "/repo/sub/AGENTS.md", Source: agentsession.LayerAgentsMD}},
		Trust:  untrusted,
	})
	if err != nil {
		t.Fatal(err)
	}
	twoLayers, err := agentturn.WithProvenance(agentturn.Hidden(openresponses.UserText("<reminder>file and memory</reminder>")), agentturn.Provenance{
		Layers: []agentturn.Layer{{ID: "~/AGENTS.md", Source: agentsession.LayerAgentsMD}, {Source: agentsession.LayerAgentMemory}},
	})
	if err != nil {
		t.Fatal(err)
	}
	mcp := agentturn.Trigger{Kind: "mcp_prompt", Ref: "docs/review", Source: "mcp"}
	fromServer, err := agentturn.WithProvenance(openresponses.UserText("review the docs"), agentturn.Provenance{Source: mcp, Trust: untrusted})
	if err != nil {
		t.Fatal(err)
	}
	resource, err := agentturn.WithProvenance(openresponses.UserText("resource text"), agentturn.Provenance{Source: mcp, Trust: &agentturn.Trust{}})
	if err != nil {
		t.Fatal(err)
	}
	turns := 0
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", MaxTurns: 2,
		BeforeTurn: func(context.Context, agentturn.TurnStartInfo) (openresponses.Items, error) {
			turns++
			if turns == 1 {
				return openresponses.Items{agentsMD, twoLayers}, nil
			}
			return nil, nil
		}})
	defer rec.Attach(a)()
	human := agentturn.Trigger{Kind: "human", Ref: "tty", Source: "dax"}
	if _, err := a.Prompt(agentturn.ContextWithTrigger(ctx, human), openresponses.UserText("mine"), fromServer); err != nil {
		t.Fatal(err)
	}
	if err := rec.Queue(ctx, a, agentturn.QueueSteer, resource); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Continue(ctx); err != nil {
		t.Fatal(err)
	}
	read, err := store.Open(ctx, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	verifyAll(t, read)
	if err := read.VerifyRecords(read.Leaf()); err != nil {
		t.Errorf("verify records: %v", err)
	}
	items := itemEntries(read)
	wantSource := &agentsession.Trigger{Kind: "mcp_prompt", Ref: "docs/review", Source: "mcp"}

	mine := items["mine"]
	if mine == nil || !mine.Source.Equal(&agentsession.Trigger{Kind: "human", Ref: "tty", Source: "dax"}) || mine.Trust != nil || mine.Layers() != nil {
		t.Errorf("plain prompt = %+v", mine)
	}
	if it := items["review the docs"]; it == nil || !it.Source.Equal(wantSource) || it.Trust == nil || !slices.Equal(it.Trust.Reads, []string{"untrusted"}) || it.Layers() != nil {
		t.Errorf("item from a server = %+v", it)
	}
	if it := items["<reminder>AGENTS.md says</reminder>"]; it == nil || it.InstructionsPart == nil || *it.InstructionsPart != (agentsession.LayerRef{ID: "/repo/sub/AGENTS.md", Source: agentsession.LayerAgentsMD}) ||
		it.Trust == nil || !slices.Equal(it.Trust.Reads, []string{"untrusted"}) || it.Source != nil {
		t.Errorf("hook's instruction file = %+v", it)
	}
	if it := items["<reminder>file and memory</reminder>"]; it == nil || !slices.Equal(it.Layers(), []agentsession.LayerRef{{ID: "~/AGENTS.md", Source: agentsession.LayerAgentsMD}, {Source: agentsession.LayerAgentMemory}}) ||
		it.Trust != nil || it.Visible == nil || *it.Visible {
		t.Errorf("hook's two layers = %+v", it)
	}
	var queued *agentsession.QueuedEntry
	for _, e := range read.Entries() {
		if q, ok := e.(*agentsession.QueuedEntry); ok {
			queued = q
		}
	}
	if queued == nil || !queued.Trigger.Equal(wantSource) {
		t.Errorf("queued = %+v, want the item's source as its trigger", queued)
	}
	if it := items["resource text"]; it == nil || queued == nil || it.QueuedFrom != queued.ID || !it.Source.Equal(wantSource) || it.Trust == nil || len(it.Trust.Reads) != 0 {
		t.Errorf("queued item = %+v", it)
	}
	// The instruction layers are not inputs of their run: a replay does
	// not send them again as the person's.
	pathInputs, _ := agentsession.PathInputs(read.Path(read.Leaf()))
	for _, in := range pathInputs {
		for _, it := range in.Inputs {
			if m, ok := it.Item.(*openresponses.Message); ok && m.Text() != "mine" && m.Text() != "review the docs" && m.Text() != "resource text" {
				t.Errorf("run input %q is a composed layer", m.Text())
			}
		}
	}
}
