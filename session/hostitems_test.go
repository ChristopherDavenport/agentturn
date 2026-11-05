package session

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
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
// what it was: the queued entry keeps the marks in format 0.21's
// visible, trust and instructions_part (agentsession#278), and none of
// the interim agentturn: members v0.0.37 wrote, Requeue hands the input
// back marked, and
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
		if q.Visible == nil || *q.Visible || q.Trust == nil || !slices.Equal(q.Trust.Reads, []string{"untrusted"}) ||
			q.InstructionsPart == nil || *q.InstructionsPart != (agentsession.LayerRef{ID: "/repo/sub/AGENTS.md", Source: agentsession.LayerAgentsMD}) {
			t.Errorf("queued entry %s: visible %v trust %+v instructions_part %+v, want format 0.21's members", q.ID, q.Visible, q.Trust, q.InstructionsPart)
		}
		for name := range q.Unknown {
			if strings.HasPrefix(name, "agentturn:") {
				t.Errorf("queued entry %s carries the interim member %s", q.ID, name)
			}
		}
	}
	it := itemEntries(read)["<reminder>sub/AGENTS.md</reminder>"]
	if it == nil || it.QueuedFrom == "" || it.Visible == nil || *it.Visible ||
		it.InstructionsPart == nil || *it.InstructionsPart != (agentsession.LayerRef{ID: "/repo/sub/AGENTS.md", Source: agentsession.LayerAgentsMD}) ||
		it.Trust == nil || !slices.Equal(it.Trust.Reads, []string{"untrusted"}) {
		t.Errorf("drained reminder = %+v", it)
	}
}

// TestACopysTrustTakesTheHostsLabels pins agentsession#279's
// follow-up: a copied item carries the trust it was copied with, and a
// host's provenance labels are merged into it, since format 0.21 lets a
// copy's trust hold more labels than its source's, never fewer. An
// empty provenance trust adds nothing.
func TestACopysTrustTakesTheHostsLabels(t *testing.T) {
	copyOf := func(reads ...string) *agentsession.ItemEntry {
		return &agentsession.ItemEntry{Item: openresponses.UserText("copied"), CopiedFrom: &agentsession.CopiedFrom{Session: "s"}, Trust: &agentsession.Trust{Reads: reads}}
	}
	prov := func(reads ...agenttool.Label) *agentturn.Provenance {
		return &agentturn.Provenance{Trust: &agentturn.Trust{Reads: reads}}
	}
	e := copyOf("untrusted")
	if err := recordProvenance(e, prov(agenttool.LabelUntrusted)); err != nil || !slices.Equal(e.Trust.Reads, []string{"untrusted"}) {
		t.Errorf("held labels: err=%v trust=%+v", err, e.Trust)
	}
	e = &agentsession.ItemEntry{Item: openresponses.UserText("copied"), CopiedFrom: &agentsession.CopiedFrom{Session: "s"}}
	if err := recordProvenance(e, prov()); err != nil || e.Trust != nil {
		t.Errorf("an empty trust: err=%v trust=%+v, want nothing added", err, e.Trust)
	}
	e = copyOf("untrusted")
	if err := recordProvenance(e, prov(agenttool.LabelPrivate)); err != nil || !slices.Equal(e.Trust.Reads, []string{"untrusted", "private"}) {
		t.Errorf("a label the source lacks: err=%v trust=%+v, want it merged", err, e.Trust)
	}
	e = &agentsession.ItemEntry{Item: openresponses.UserText("copied"), CopiedFrom: &agentsession.CopiedFrom{Session: "s"}}
	if err := recordProvenance(e, prov(agenttool.LabelUntrusted)); err != nil || e.Trust == nil || !slices.Equal(e.Trust.Reads, []string{"untrusted"}) {
		t.Errorf("a copy with no trust: err=%v trust=%+v, want the label", err, e.Trust)
	}
}

// TestAnUnreadableLayerDropsOnlyTheLayers pins that an owed input whose
// queued entry holds an agentturn:instructions_part the provenance
// rules refuse, as a redaction that rewrote an instruction file's id
// away leaves it, is handed back hidden and with its trust, losing only
// the layers: the trust and the display flag still say it is not the
// person's visible words.
func TestAnUnreadableLayerDropsOnlyTheLayers(t *testing.T) {
	q := agentsession.NewQueued(openresponses.UserText("<reminder>[redacted]</reminder>"), agentsession.ModeFollowUp)
	q.Unknown = map[string]json.RawMessage{
		VisibleMember:          json.RawMessage(`false`),
		TrustMember:            json.RawMessage(`{"reads":["untrusted"]}`),
		InstructionsPartMember: json.RawMessage(`{"source":"agentsmd"}`),
	}
	in := &inboxItem{item: q.Item}
	in.hidden, in.prov = queuedMarks(q)
	item := in.marked()
	if _, hidden := agentturn.Unhide(item); !hidden {
		t.Error("the input lost its hidden mark")
	}
	p, ok := agentturn.ProvenanceOf(item)
	if !ok || p.Trust == nil || !slices.Equal(p.Trust.Reads, []agenttool.Label{agenttool.LabelUntrusted}) {
		t.Errorf("provenance = %+v (%v), want the trust kept", p, ok)
	}
	if len(p.Layers) != 0 {
		t.Errorf("layers = %+v, want the unreadable one dropped", p.Layers)
	}
}

// TestQueuedMarksReadBothForms pins agentsession#278's follow-up: an
// owed input's marks are read from format 0.21's visible, trust and
// instructions_part on its queued entry, and, for a session agentturn
// v0.0.37 recorded, from the interim agentturn: members it wrote there.
func TestQueuedMarksReadBothForms(t *testing.T) {
	visible := false
	current := agentsession.NewQueued(openresponses.UserText("<reminder>x</reminder>"), agentsession.ModeFollowUp)
	current.Visible = &visible
	current.Trust = &agentsession.Trust{Reads: []string{"untrusted"}}
	current.InstructionsLayers = []agentsession.LayerRef{{ID: "/repo/AGENTS.md", Source: agentsession.LayerAgentsMD}, {Source: agentsession.LayerAgentMemory}}
	interim := agentsession.NewQueued(openresponses.UserText("<reminder>x</reminder>"), agentsession.ModeFollowUp)
	interim.Unknown = map[string]json.RawMessage{
		VisibleMember:          json.RawMessage(`false`),
		TrustMember:            json.RawMessage(`{"reads":["untrusted"]}`),
		InstructionsPartMember: json.RawMessage(`[{"id":"/repo/AGENTS.md","source":"agentsmd"},{"source":"agentmemory"}]`),
	}
	wantLayers := []agentturn.Layer{{ID: "/repo/AGENTS.md", Source: agentsession.LayerAgentsMD}, {Source: agentsession.LayerAgentMemory}}
	for name, q := range map[string]*agentsession.QueuedEntry{"0.21": current, "v0.0.37": interim} {
		hidden, prov := queuedMarks(q)
		if !hidden {
			t.Errorf("%s: not hidden", name)
		}
		if prov == nil || prov.Trust == nil || !slices.Equal(prov.Trust.Reads, []agenttool.Label{agenttool.LabelUntrusted}) || !slices.Equal(prov.Layers, wantLayers) {
			t.Errorf("%s: provenance = %+v", name, prov)
		}
	}
}

// TestComposedInputIsNeverRecordedAsThePersons pins #303 under
// agentsession RFC 0001 0.21's rule for the trigger kind human: a
// message the host composed from instruction layers, prompted or
// queued under a run's or a context's trigger of kind human, is not
// recorded with that trigger as its source or its queued entry's
// trigger, so the record reads it as composed and never carries human
// beside instructions_part. What the person typed keeps the kind, a
// pasted message the host labels untrusted included, and the drained
// item still carries its queued entry's trigger.
func TestComposedInputIsNeverRecordedAsThePersons(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{Records: agentsession.AllRecords})
	if err != nil {
		t.Fatal(err)
	}
	template := func(text string) openresponses.Item {
		item, err := agentturn.WithProvenance(openresponses.UserText(text), agentturn.Provenance{
			Layers: []agentturn.Layer{{ID: "review.md", Source: "dax.prompts"}},
			Trust:  &agentturn.Trust{},
		})
		if err != nil {
			t.Fatal(err)
		}
		return item
	}
	pasted, err := agentturn.WithProvenance(openresponses.UserText("pasted"), agentturn.Provenance{Trust: &agentturn.Trust{Reads: []agenttool.Label{agenttool.LabelUntrusted}}})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m"})
	defer rec.Attach(a)()
	human := agentturn.ContextWithTrigger(ctx, agentturn.Trigger{Kind: agentturn.TriggerHuman, Source: "terminal"})
	if _, err := a.Prompt(human, openresponses.UserText("typed"), pasted, template("prompted template")); err != nil {
		t.Fatal(err)
	}
	if err := rec.Queue(human, a, agentturn.QueueSteer, template("queued template")); err != nil {
		t.Fatal(err)
	}
	if err := a.Queue(human, agentturn.QueueSteer, template("event template")); err != nil {
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
	for text, want := range map[string]agentsession.Words{
		"typed":             agentsession.WordsPerson,
		"pasted":            agentsession.WordsPerson,
		"prompted template": agentsession.WordsComposed,
		"queued template":   agentsession.WordsComposed,
		"event template":    agentsession.WordsComposed,
	} {
		it := items[text]
		if it == nil {
			t.Errorf("%q not recorded", text)
			continue
		}
		if got := agentsession.WordsOf(it); got != want {
			t.Errorf("%q reads as %v, want %v", text, got, want)
		}
	}
	for _, e := range read.Entries() {
		var trigger *agentsession.Trigger
		var layers []agentsession.LayerRef
		switch v := e.(type) {
		case *agentsession.ItemEntry:
			trigger, layers = v.Source, v.Layers()
		case *agentsession.QueuedEntry:
			trigger, layers = v.Trigger, v.Layers()
		default:
			continue
		}
		if layers != nil && trigger != nil && trigger.Kind == agentsession.TriggerHuman {
			t.Errorf("%s %s carries human beside instructions_part", e.EntryType(), e.Base().ID)
		}
	}
}
