package session

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/compact"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// reasonsAndCalls is a model that reasons before each turn: it calls
// upper after a user message and answers after the output, so one prompt
// is two requests, the second carrying the reasoning of the first. Its
// reasoning names the model and the request, and it records what each
// request carried.
type reasonsAndCalls struct {
	mu   sync.Mutex
	n    int
	sent [][]string
}

func (m *reasonsAndCalls) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.mu.Lock()
	m.n++
	n := m.n
	var sent []string
	for _, item := range req.Input {
		if r, ok := item.(*openresponses.ReasoningItem); ok {
			sent = append(sent, r.EncryptedContent)
		}
	}
	m.sent = append(m.sent, sent)
	m.mu.Unlock()
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	r, err := em.Reasoning()
	if err != nil {
		return err
	}
	r.EncryptedContent(fmt.Sprintf("%s#%d", req.Model, n))
	if err := r.Close(); err != nil {
		return err
	}
	if _, ok := req.Input[len(req.Input)-1].(*openresponses.FunctionCallOutput); ok {
		w, err := em.Message(openresponses.PhaseFinalAnswer)
		if err != nil {
			return err
		}
		if err := w.Text("done"); err != nil {
			return err
		}
		return em.Complete()
	}
	w, err := em.FunctionCall(fmt.Sprintf("call_%d", n), "upper")
	if err != nil {
		return err
	}
	if err := w.Arguments(`{"text":"x"}`); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Complete()
}

// requests returns the reasoning each request carried, joined.
func (m *reasonsAndCalls) requests() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.sent))
	for i, s := range m.sent {
		out[i] = strings.Join(s, ",")
	}
	return out
}

// omits returns the config entries of s that carry an omit member.
func omits(s *agentsession.Session) []*agentsession.ConfigEntry {
	var out []*agentsession.ConfigEntry
	for _, c := range configs(s) {
		if c.Omit != nil {
			out = append(out, c)
		}
	}
	return out
}

// unhashedEntries counts the custom entries naming a response the
// recorder declined to hash.
func unhashedEntries(s *agentsession.Session) int {
	n := 0
	for _, e := range s.Entries() {
		if c, ok := e.(*agentsession.CustomEntry); ok && c.NS == UnhashedNS {
			n++
		}
	}
	return n
}

// TestModelSwitchKeepsEveryResponseVerifiable pins the writer half of
// format 0.11's omit setting: a request to a model other than the one
// that produced the reasoning on the path leaves that reasoning out, as
// the loop does, and the record says so once, in the config entry that
// changes the model, so the requests that follow hash against the
// context the rule rebuilds and every response verifies, where each was
// recorded unhashed before. The shapes are the round 8 studies': a
// model switch between prompts, a Plan-to-Act switch that changes the
// tools and instructions with the model, and a switch after a restart.
func TestModelSwitchKeepsEveryResponseVerifiable(t *testing.T) {
	type step struct {
		model string
		// act sets the tools and instructions a Plan-to-Act switch
		// changes beside the model.
		act bool
	}
	cases := []struct {
		name          string
		before, after []step
		// sent is the reasoning each request carried, in order.
		sent []string
		// omits is how many config entries carry the omit member.
		omits int
	}{
		{name: "one model throughout", before: []step{{"a", false}, {"a", false}}, sent: []string{"", "a#1", "a#1,a#2", "a#1,a#2,a#3"}},
		{name: "a model switch", before: []step{{"a", false}, {"b", false}},
			sent: []string{"", "a#1", "", "b#3"}, omits: 1},
		{name: "a switch and back", before: []step{{"a", false}, {"b", false}, {"a", false}},
			sent: []string{"", "a#1", "", "b#3", "a#1,a#2", "a#1,a#2,a#5"}, omits: 1},
		// Clearing the model is a replace, which discards the omit with
		// the rest of the settings, so it carries the rule again.
		{name: "a replace in between", before: []step{{"a", false}, {"b", false}, {"", false}, {"b", false}},
			// A request under no model leaves out nothing, and what no
			// model produced is sent to every model.
			sent: []string{"", "a#1", "", "b#3", "a#1,a#2,b#3,b#4", "a#1,a#2,b#3,b#4,#5", "b#3,b#4,#5,#6", "b#3,b#4,#5,#6,b#7"}, omits: 2},
		{name: "plan to act", before: []step{{"plan", false}, {"act", true}},
			sent: []string{"", "plan#1", "", "act#3"}, omits: 1},
		{name: "a switch after a restart", before: []step{{"a", false}}, after: []step{{"b", false}, {"b", false}},
			sent: []string{"", "a#1", "", "b#3", "b#3,b#4", "b#3,b#4,b#5"}, omits: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			model := &reasonsAndCalls{}
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(ctx, store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			cfgFor := func(st step) agentturn.Config {
				cfg := agentturn.Config{Model: model, ModelName: st.model, Tools: []agenttool.Tool{upper}}
				if st.act {
					cfg.Instructions = "Act on the plan."
				}
				return cfg
			}
			a := agentturn.New(cfgFor(tc.before[0]))
			unsub := rec.Attach(a)
			run := func(a *agentturn.Agent, steps []step) {
				for _, st := range steps {
					if err := a.SetConfig(cfgFor(st)); err != nil {
						t.Fatal(err)
					}
					if end, err := a.Prompt(ctx, openresponses.UserText("go "+st.model)); err != nil || end.Reason != agentturn.ReasonDone {
						t.Fatalf("prompt under %s: err=%v end=%+v", st.model, err, end)
					}
				}
			}
			run(a, tc.before)
			unsub()
			if len(tc.after) > 0 {
				// A new process: what the record says is all there is.
				rec2, s2, err := Resume(ctx, store, s.ID())
				if err != nil {
					t.Fatal(err)
				}
				opts, err := AgentOptions(s2)
				if err != nil {
					t.Fatal(err)
				}
				b := agentturn.New(cfgFor(tc.after[0]), opts...)
				defer rec2.Attach(b)()
				run(b, tc.after)
				s = s2
			}
			if got := model.requests(); !slices.Equal(got, tc.sent) {
				t.Errorf("reasoning sent per request = %q, want %q", got, tc.sent)
			}
			if n := verifyAll(t, s); n != len(tc.sent) {
				t.Errorf("responses = %d, want %d", n, len(tc.sent))
			}
			if n := unhashedEntries(s); n != 0 {
				t.Errorf("%d unhashed entries: %s", n, entryTypes(s))
			}
			got := omits(s)
			if len(got) != tc.omits {
				t.Fatalf("config entries with an omit member = %d, want %d: %s", len(got), tc.omits, entryTypes(s))
			}
			for _, c := range got {
				if c.Omit.Reasoning != agentsession.OmitOtherModels || len(c.Omit.Items) != 0 {
					t.Errorf("omit = %+v, want the other_models rule alone", c.Omit)
				}
			}
			if tc.omits > 0 {
				// The first entry that carries the rule is the one that
				// changes the model.
				if got[0].Model == "" {
					t.Errorf("the omit is written beside no model change: %+v", got[0])
				}
				cx, err := s.Context()
				if err != nil {
					t.Fatal(err)
				}
				if cx.Settings.Omit.Reasoning != agentsession.OmitOtherModels {
					t.Errorf("omit in force = %+v", cx.Settings.Omit)
				}
			}
		})
	}
}

// TestHandBackNamesWhatThePathHolds pins the writer half of what format
// 0.11 lets a hand-back repeat by reference: an instruction part whose
// text the path already holds, though not in force, is written as its
// hash, and an omitted list an earlier config entry wrote is written
// as a keep that names that entry with of, where a 0.10 writer repeated
// the text and the list whole. The shapes are the round 8 letta-memory
// study's: a, b, a, once in one process and once across a restart, with
// every response still verifying.
func TestHandBackNamesWhatThePathHolds(t *testing.T) {
	list := func(from string) []agentsession.OmittedPart {
		var out []agentsession.OmittedPart
		for i := 0; i < 6; i++ {
			out = append(out, agentsession.OmittedPart{ID: fmt.Sprintf("%s%d", from, i), Reason: "over the budget of the skills layer", Size: 4000 + i, Source: "skills"})
		}
		return out
	}
	l := &layers{
		ids:     []string{"product", "agents_md", "memory"},
		text:    map[string]string{"product": strings.Repeat("p", 3000), "agents_md": strings.Repeat("a", 3000), "memory": "Memory:"},
		omitted: list("o"),
	}
	partsOf := func(context.Context, openresponses.Request) ([]agentsession.InstructionPart, []agentsession.OmittedPart) {
		return l.parts(), slices.Clone(l.omitted)
	}
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{}, WithInstructionsParts(partsOf))
	if err != nil {
		t.Fatal(err)
	}
	cfg := agentturn.Config{
		Model: &reasonsAndCalls{}, ModelName: "m", Instructions: l.render(), Tools: []agenttool.Tool{upper},
		BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
			req.Instructions = l.render()
			return nil
		},
	}
	a := agentturn.New(cfg)
	unsub := rec.Attach(a)
	seen := 0
	// prompt runs one prompt and returns the config entries it added.
	prompt := func(a *agentturn.Agent, s *agentsession.Session) []*agentsession.ConfigEntry {
		t.Helper()
		if _, err := a.Prompt(ctx, openresponses.UserText("turn")); err != nil {
			t.Fatal(err)
		}
		all := configs(s)
		added := all[seen:]
		seen = len(all)
		return added
	}
	prompt(a, s)
	first := configs(s)[0].ID
	l.text["agents_md"], l.omitted = strings.Repeat("b", 3000), list("n")
	if added := prompt(a, s); len(added) != 1 {
		t.Fatalf("a changed layer wrote %d config entries", len(added))
	}
	// Back to what the first entry wrote.
	l.text["agents_md"], l.omitted = strings.Repeat("a", 3000), list("o")
	added := prompt(a, s)
	if len(added) != 1 {
		t.Fatalf("a hand-back wrote %d config entries", len(added))
	}
	named := false
	for _, p := range added[0].InstructionsParts {
		if p.ID == "agents_md" {
			named = p.Hash == agentsession.HashText(strings.Repeat("a", 3000)) && p.Text == ""
		}
	}
	if !named {
		t.Errorf("the part out of force is not named by its hash: %+v", added[0].InstructionsParts)
	}
	if got := added[0].InstructionsOmitted; len(got) != 1 || got[0].Keep != 6 || got[0].Of != first {
		t.Errorf("the omitted list is not the keep of the first entry's: %+v, first entry %s", got, first)
	}
	unsub()

	// Across a restart the histories come from the path.
	rec2, s2, err := Resume(ctx, store, s.ID(), WithInstructionsParts(partsOf))
	if err != nil {
		t.Fatal(err)
	}
	opts, err := AgentOptions(s2)
	if err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(cfg, opts...)
	defer rec2.Attach(b)()
	l.text["agents_md"], l.omitted = strings.Repeat("b", 3000), list("n")
	prompt(b, s2)
	l.text["agents_md"], l.omitted = strings.Repeat("a", 3000), list("o")
	added = prompt(b, s2)
	if len(added) != 1 {
		t.Fatalf("a hand-back after a restart wrote %d config entries", len(added))
	}
	for _, p := range added[0].InstructionsParts {
		if p.ID == "agents_md" && (p.Hash == "" || p.Text != "") {
			t.Errorf("after a restart the part out of force is written whole: %+v", p)
		}
	}
	if got := added[0].InstructionsOmitted; len(got) != 1 || got[0].Keep != 6 || got[0].Of == "" {
		t.Errorf("after a restart the omitted list is written whole: %+v", got)
	}
	verifyAll(t, s2)
	cx, err := s2.Context()
	if err != nil {
		t.Fatal(err)
	}
	if cx.Settings.Instructions != l.render() || !slices.Equal(cx.Settings.InstructionsOmitted, list("o")) {
		t.Errorf("the path replays to other instructions than the last request sent")
	}
}

// TestHandBackAfterAFoldNamesNothingBeforeIt pins that a compaction
// starts the lists afresh, as the format has it: a checkpoint writes
// what is in force whole, so a hand-back after one names no config entry
// from before it. The omitted list written then resolves, and the part
// whose text only the folded entries held is written whole.
func TestHandBackAfterAFoldNamesNothingBeforeIt(t *testing.T) {
	list := func(from string) []agentsession.OmittedPart {
		var out []agentsession.OmittedPart
		for i := 0; i < 6; i++ {
			out = append(out, agentsession.OmittedPart{ID: fmt.Sprintf("%s%d", from, i), Reason: "over the budget of the skills layer", Size: 4000 + i, Source: "skills"})
		}
		return out
	}
	l := &layers{
		ids:     []string{"product", "agents_md"},
		text:    map[string]string{"product": strings.Repeat("p", 3000), "agents_md": strings.Repeat("a", 3000)},
		omitted: list("o"),
	}
	partsOf := func(context.Context, openresponses.Request) ([]agentsession.InstructionPart, []agentsession.OmittedPart) {
		return l.parts(), slices.Clone(l.omitted)
	}
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{}, WithInstructionsParts(partsOf))
	if err != nil {
		t.Fatal(err)
	}
	model := &echo.Adapter{}
	cfg := agentturn.Config{
		Model: model, ModelName: "m", Instructions: l.render(),
		BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
			req.Instructions = l.render()
			return nil
		},
		Transform: compact.NewLocal(model, compact.WithBudget(6), compact.WithKeepLast(2), compact.WithEstimator(countItems), compact.WithOnFold(rec.Fold)).Transform,
	}
	a := agentturn.New(cfg)
	defer rec.Attach(a)()
	prompt := func(text string, omitted []agentsession.OmittedPart, agents string) {
		t.Helper()
		l.omitted, l.text["agents_md"] = omitted, agents
		if _, err := a.Prompt(ctx, openresponses.UserText(text)); err != nil {
			t.Fatal(err)
		}
	}
	prompt("one", list("o"), strings.Repeat("a", 3000))
	for i := 0; i < 5; i++ {
		prompt(fmt.Sprintf("n%d", i), list("n"), strings.Repeat("b", 3000))
	}
	folded := false
	for _, e := range s.Entries() {
		if _, ok := e.(*agentsession.CompactionEntry); ok {
			folded = true
		}
	}
	if !folded {
		t.Fatalf("nothing folded: %s", entryTypes(s))
	}
	before := len(configs(s))
	prompt("back", list("o"), strings.Repeat("a", 3000))
	added := configs(s)[before:]
	if len(added) != 1 {
		t.Fatalf("a hand-back wrote %d config entries", len(added))
	}
	for _, p := range added[0].InstructionsOmitted {
		if p.Of != "" {
			t.Errorf("the omitted list names %s, an entry the checkpoint folded away: %+v", p.Of, added[0].InstructionsOmitted)
		}
	}
	cx, err := s.Context()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cx.Settings.InstructionsOmitted, list("o")) || cx.Settings.Instructions != l.render() {
		t.Errorf("the path replays to other settings than the last request carried: %+v", cx.Settings.InstructionsOmitted)
	}
	verifyAll(t, s)
}

// TestModelSwitchAcrossAFoldKeepsHashes pins that the omit rule and a
// compaction compose: the items a fold replaced are not in the context
// a request is hashed against, and the reasoning of another model that
// stays in the window the fold kept is left out of the request and of
// the rebuilt context alike.
func TestModelSwitchAcrossAFoldKeepsHashes(t *testing.T) {
	ctx := context.Background()
	model := &reasonsAndCalls{}
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	cfgFor := func(name string) agentturn.Config {
		return agentturn.Config{
			Model: model, ModelName: name, Tools: []agenttool.Tool{upper},
			Transform: compact.NewLocal(&echo.Adapter{}, compact.WithBudget(9), compact.WithKeepLast(4), compact.WithEstimator(countItems), compact.WithOnFold(rec.Fold)).Transform,
		}
	}
	a := agentturn.New(cfgFor("a"))
	defer rec.Attach(a)()
	for _, name := range []string{"a", "a", "b", "b", "a", "b"} {
		if err := a.SetConfig(cfgFor(name)); err != nil {
			t.Fatal(err)
		}
		if _, err := a.Prompt(ctx, openresponses.UserText("go "+name)); err != nil {
			t.Fatal(err)
		}
	}
	folds := 0
	for _, e := range s.Entries() {
		if _, ok := e.(*agentsession.CompactionEntry); ok {
			folds++
		}
	}
	if folds == 0 {
		t.Fatalf("nothing folded: %s", entryTypes(s))
	}
	verifyAll(t, s)
	if n := unhashedEntries(s); n != 0 {
		t.Errorf("%d unhashed entries: %s", n, entryTypes(s))
	}
}
