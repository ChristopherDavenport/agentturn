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

// TestAnOwedQueuedInputKeepsItsMarksAcrossARestart pins that an input
// the host composed and queued, hidden, with instruction layers and
// trust, and owed when the process quits, is drained after a restart as
// what it was: the queued entry keeps the marks the format's queued
// entry has no member for, as agentturn:visible, agentturn:trust and
// agentturn:instructions_part, Requeue hands the input back marked, and
// the item that drains it is recorded hidden, with its layers and its
// trust, not as the person's visible words. A run end before the drain
// writes the entry again with them.
func TestAnOwedQueuedInputKeepsItsMarksAcrossARestart(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{Records: agentsession.AllRecords})
	if err != nil {
		t.Fatal(err)
	}
	cfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m"}
	a := agentturn.New(cfg)
	unsub := rec.Attach(a)
	if _, err := a.Prompt(ctx, openresponses.UserText("first")); err != nil {
		t.Fatal(err)
	}
	reminder, err := agentturn.WithProvenance(agentturn.Hidden(openresponses.UserText("<reminder>sub/AGENTS.md</reminder>")), agentturn.Provenance{
		Layers: []agentturn.Layer{{ID: "/repo/sub/AGENTS.md", Source: agentsession.LayerAgentsMD}},
		Trust:  &agentturn.Trust{Reads: []agenttool.Label{agenttool.LabelUntrusted}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Queue(ctx, a, agentturn.QueueFollowUp, reminder); err != nil {
		t.Fatal(err)
	}
	unsub()

	// The process quits with the input owed; a new one resumes.
	rec, s, err = Resume(ctx, store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	opts, err := AgentOptions(s, rec.ReadOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	// The new process's first run stops for an approval, so its end
	// closes the reminder's entry and writes it again, before a resume
	// drains it.
	asked := false
	cfgB := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{upper},
		BeforeToolCall: func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			if asked {
				return nil, nil
			}
			asked = true
			return &agentturn.ToolDecision{Action: agentturn.Defer}, nil
		}}
	b := agentturn.New(cfgB, opts...)
	defer rec.Attach(b)()
	if n, _ := rec.Requeue(ctx, b); n != 1 {
		t.Fatalf("requeued %d, want the reminder", n)
	}
	end, err := b.Prompt(ctx, openresponses.UserText("second"))
	if err != nil || end.Reason != agentturn.ReasonInputRequired {
		t.Fatalf("second: %v, %+v", err, end)
	}
	if _, err := b.Resume(ctx, agentturn.Approve(end.Pending[0].Call.CallID)); err != nil {
		t.Fatal(err)
	}
	read, err := store.Open(ctx, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	verifyAll(t, read)
	var queued []*agentsession.QueuedEntry
	for _, e := range read.Entries() {
		if q, ok := e.(*agentsession.QueuedEntry); ok {
			queued = append(queued, q)
		}
	}
	if len(queued) != 2 {
		t.Fatalf("queued entries = %d, want the reminder's and the one its run end wrote again", len(queued))
	}
	for _, q := range queued {
		if string(q.Unknown[VisibleMember]) != "false" || string(q.Unknown[TrustMember]) != `{"reads":["untrusted"]}` ||
			string(q.Unknown[InstructionsPartMember]) != `{"id":"/repo/sub/AGENTS.md","source":"agentsmd"}` {
			t.Errorf("queued entry %s members = %s", q.ID, q.Unknown)
		}
	}
	it := itemEntries(read)["<reminder>sub/AGENTS.md</reminder>"]
	if it == nil || it.QueuedFrom == "" || it.Visible == nil || *it.Visible ||
		it.InstructionsPart == nil || *it.InstructionsPart != (agentsession.LayerRef{ID: "/repo/sub/AGENTS.md", Source: agentsession.LayerAgentsMD}) ||
		it.Trust == nil || !slices.Equal(it.Trust.Reads, []string{"untrusted"}) {
		t.Errorf("drained reminder = %+v", it)
	}
}
