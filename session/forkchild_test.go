package session

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentsession/jsonl"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/compact"
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// sawInput records the input of every request it is sent and answers
// as the echo adapter does.
type sawInput struct {
	echo.Adapter
	inputs []int
	texts  []string
}

func (m *sawInput) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.inputs = append(m.inputs, len(req.Input))
	var b strings.Builder
	for _, it := range req.Input {
		if msg, ok := it.(*openresponses.Message); ok {
			b.WriteString(msg.Text())
			b.WriteString("|")
		}
	}
	m.texts = append(m.texts, b.String())
	return m.Adapter.CreateStream(ctx, req, sink)
}

// forkedAgent makes an agent for the forked child the context names,
// seeded from the child's session alone, and observed by rec.
func forkedAgent(t *testing.T, rec *Recorder, store agentsession.Store, cctx context.Context, cfg agentturn.Config) (*agentturn.Agent, *agentsession.Session) {
	t.Helper()
	cs, err := store.Open(context.Background(), SessionIDFromContext(cctx))
	if err != nil {
		t.Fatalf("open the forked child: %v", err)
	}
	opts, err := AgentOptions(cs, rec.ReadOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(cfg, opts...)
	a.Subscribe(func(ctx context.Context, ev agentturn.Event) error {
		rec.Observe(ctx, ev)
		return nil
	})
	return a, cs
}

func ownTypes(s *agentsession.Session) string {
	var parts []string
	for _, e := range OwnPath(s) {
		parts = append(parts, e.EntryType())
	}
	return strings.Join(parts, " ")
}

// TestAForkedChildCarriesItsParentsContextWithHashes pins ForkChild: a
// child the host starts as a fork of the parent's session holds the
// parent's path to the base as its prefix, its agent is seeded from its
// own session, its requests carry the parent's conversation and are
// recorded with hashes that verify, and a recorder resumed in another
// process continues it with the prefix and no seeding by the host.
// What the child wrote is told from what it inherited.
func TestAForkedChildCarriesItsParentsContextWithHashes(t *testing.T) {
	stores := map[string]func(t *testing.T) (first agentsession.Store, again func() agentsession.Store){
		"memory": func(t *testing.T) (agentsession.Store, func() agentsession.Store) {
			s := agentsession.NewMemoryStore()
			return s, func() agentsession.Store { return s }
		},
		"jsonl": func(t *testing.T) (agentsession.Store, func() agentsession.Store) {
			root := t.TempDir()
			s, err := jsonl.Open(root)
			if err != nil {
				t.Fatal(err)
			}
			return s, func() agentsession.Store {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				s2, err := jsonl.Open(root)
				if err != nil {
					t.Fatal(err)
				}
				return s2
			}
		},
	}
	for name, open := range stores {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store, again := open(t)
			rec, s, err := Start(ctx, store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m"})
			detach := rec.Attach(a)
			for _, text := range []string{"alpha", "beta"} {
				if _, err := a.Prompt(ctx, openresponses.UserText(text)); err != nil {
					t.Fatal(err)
				}
			}
			base := s.Leaf()

			spawnID := agentsession.NewSpawnID()
			cctx, err := rec.ForkChild(ctx, spawnID, "")
			if err != nil {
				t.Fatal(err)
			}
			want := agentsession.SpawnedSessionID(s.ID(), spawnID)
			if got := SessionIDFromContext(cctx); got != want {
				t.Fatalf("the context names %q, want the derived %q", got, want)
			}
			if l := links(s); len(l) != 1 || l[0].SpawnID != spawnID || l[0].Session != want {
				t.Fatalf("links = %+v", l)
			}

			model := &sawInput{}
			cfg := agentturn.Config{Name: "callee", Model: model, ModelName: "m"}
			child, cs := forkedAgent(t, rec, store, cctx, cfg)
			if h := cs.Header(); h.ParentSession != s.ID() || h.SpawnedBy != spawnID || h.Base != base {
				t.Errorf("child header parent %q spawned_by %q base %q, want %q %q %q", h.ParentSession, h.SpawnedBy, h.Base, s.ID(), spawnID, base)
			}
			if got := ownTypes(cs); got != "" {
				t.Errorf("a child forked after a run's end wrote %q before its run", got)
			}
			if _, err := child.Prompt(agent.ContextWithConfig(cctx, cfg), openresponses.UserText("gamma")); err != nil {
				t.Fatal(err)
			}
			// The parent's two exchanges and the child's input.
			if len(model.inputs) != 1 || model.inputs[0] != 5 || !strings.HasPrefix(model.texts[0], "alpha|") || !strings.HasSuffix(model.texts[0], "gamma|") {
				t.Errorf("the child's request held %v items, %q; want the parent's four and its own", model.inputs, model.texts)
			}
			if n := verifyAll(t, cs); n != 3 {
				t.Errorf("the child's session verifies %d responses, want the prefix's two and its own", n)
			}
			if n := verifyAll(t, s); n != 2 {
				t.Errorf("the parent holds %d responses, want its own two", n)
			}
			own := OwnPath(cs)
			if len(own) == 0 || len(own) >= len(cs.Path(cs.Leaf())) {
				t.Fatalf("own path holds %d of %d entries", len(own), len(cs.Path(cs.Leaf())))
			}
			if info, ok := own[0].(*agentsession.InfoEntry); !ok || info.Name != "callee" {
				t.Errorf("the child's first own entry = %+v, want its configuration's name", own[0])
			}
			for _, e := range own {
				if _, ok := s.Entry(e.Base().ID); ok {
					t.Errorf("own entry %s is the parent's", e.Base().ID)
				}
			}
			resolve := func(id string) (*agentsession.Session, error) {
				got, err := store.Open(ctx, id)
				if errors.Is(err, agentsession.ErrNoSession) {
					return nil, nil
				}
				return got, err
			}
			if err := s.VerifyLinks(resolve); err != nil {
				t.Errorf("verify links: %v", err)
			}
			if _, err := rec.ForkChild(ctx, spawnID, ""); !errors.Is(err, agentsession.ErrSpawnIDRepeated) {
				t.Errorf("ForkChild of a linked spawn = %v, want ErrSpawnIDRepeated", err)
			}
			if _, err := rec.ForkChild(ctx, agentsession.NewSpawnID(), "no-such-entry"); !errors.Is(err, agentsession.ErrNoEntry) {
				t.Errorf("ForkChild at an entry off the path = %v, want ErrNoEntry", err)
			}
			if len(links(s)) != 1 {
				t.Errorf("a refused fork left a link: %+v", links(s))
			}

			// Another process: the parent resumed, the child continued
			// from its own session.
			detach()
			store2 := again()
			rec2, _, err := Resume(ctx, store2, s.ID())
			if err != nil {
				t.Fatal(err)
			}
			cctx2, err := rec2.ContinueChild(ctx, spawnID)
			if err != nil {
				t.Fatal(err)
			}
			model2 := &sawInput{}
			cfg2 := agentturn.Config{Name: "callee", Model: model2, ModelName: "m"}
			child2, _ := forkedAgent(t, rec2, store2, cctx2, cfg2)
			if _, err := child2.Prompt(agent.ContextWithConfig(cctx2, cfg2), openresponses.UserText("delta")); err != nil {
				t.Fatal(err)
			}
			if len(model2.inputs) != 1 || model2.inputs[0] != 7 || !strings.HasPrefix(model2.texts[0], "alpha|") {
				t.Errorf("the continued child's request held %v items, %q; want the prefix, its first exchange and its input", model2.inputs, model2.texts)
			}
			cs2, err := store2.Open(ctx, want)
			if err != nil {
				t.Fatal(err)
			}
			if n := verifyAll(t, cs2); n != 4 {
				t.Errorf("the continued child verifies %d responses, want four", n)
			}
		})
	}
}

// TestAForkBeforeACallLeavesTheCallOut pins the base a host forks at
// for a call its own model made: EntryBefore gives the entry the call
// hangs from, the child's context holds nothing of the call, the run
// the base is inside is closed on the fork, interrupted, and the
// child's requests still carry hashes.
func TestAForkBeforeACallLeavesTheCallOut(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	type none struct{}
	task := agenttool.New("task", "", func(context.Context, none) (string, error) { return "ok", nil })
	a := agentturn.New(agentturn.Config{Model: &twoCalls{calls: []string{`{}`}}, ModelName: "m", Tools: []agenttool.Tool{task}})
	defer rec.Attach(a)()
	if _, err := a.Prompt(ctx, openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	var callID string
	for _, it := range a.State().Transcript {
		if c, ok := it.(*openresponses.FunctionCall); ok {
			callID = c.CallID
		}
	}
	at, ok := EntryBefore(s, callID)
	if !ok {
		t.Fatalf("no entry before call %q", callID)
	}
	if _, ok := EntryBefore(s, "no-such-call"); ok {
		t.Error("EntryBefore found a call the path does not hold")
	}
	cctx, err := rec.ForkChild(ctx, agentsession.NewSpawnID(), at)
	if err != nil {
		t.Fatal(err)
	}
	model := &sawInput{}
	cfg := agentturn.Config{Model: model, ModelName: "m"}
	child, cs := forkedAgent(t, rec, store, cctx, cfg)
	if cs.Header().Base != at {
		t.Errorf("base = %q, want %q", cs.Header().Base, at)
	}
	if got := ownTypes(cs); got != agentsession.TypeRun {
		t.Errorf("the fork's own entries before its run = %q, want the end of the run its base is inside", got)
	}
	for _, it := range child.State().Transcript {
		switch it.(type) {
		case *openresponses.FunctionCall, *openresponses.FunctionCallOutput:
			t.Errorf("the child's transcript holds %T of the call it was forked before", it)
		}
	}
	if _, err := child.Prompt(agent.ContextWithConfig(cctx, cfg), openresponses.UserText("work")); err != nil {
		t.Fatal(err)
	}
	if len(model.inputs) != 1 || model.inputs[0] != 2 {
		t.Errorf("the child's request held %v items, want the parent's input and its own", model.inputs)
	}
	cs, err = store.Open(ctx, cs.ID())
	if err != nil {
		t.Fatal(err)
	}
	if n := verifyAll(t, cs); n != 1 {
		t.Errorf("the child verifies %d responses, want its own", n)
	}
	verifyAll(t, s)
}

// TestAFoldInAForkedChildKeepsItsHashesAndItsBoundary pins compaction
// in a forked child: a fold that covers inherited items is a
// compaction entry of the child's own, its requests keep their hashes,
// the prefix stays on the child's path and is still told from what the
// child wrote, and the parent's session is untouched.
func TestAFoldInAForkedChildKeepsItsHashesAndItsBoundary(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m"})
	defer rec.Attach(a)()
	for _, text := range []string{"one", "two", "three"} {
		if _, err := a.Prompt(ctx, openresponses.UserText(text)); err != nil {
			t.Fatal(err)
		}
	}
	parentEntries := len(s.Entries())
	cctx, err := rec.ForkChild(ctx, agentsession.NewSpawnID(), "")
	if err != nil {
		t.Fatal(err)
	}
	tr := compact.NewLocal(&echo.Adapter{}, compact.WithBudget(4), compact.WithKeepLast(2), compact.WithEstimator(countItems), compact.WithOnFold(rec.Fold))
	cfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: tr.Transform}
	child, cs := forkedAgent(t, rec, store, cctx, cfg)
	prefix := len(cs.Path(cs.Leaf()))
	for _, text := range []string{"four", "five"} {
		if _, err := child.Prompt(agent.ContextWithConfig(cctx, cfg), openresponses.UserText(text)); err != nil {
			t.Fatal(err)
		}
	}
	cs, err = store.Open(ctx, cs.ID())
	if err != nil {
		t.Fatal(err)
	}
	var fold *agentsession.CompactionEntry
	for _, e := range OwnPath(cs) {
		if c, ok := e.(*agentsession.CompactionEntry); ok && fold == nil {
			fold = c
		}
	}
	if fold == nil {
		t.Fatalf("the child's own entries hold no compaction: %q", ownTypes(cs))
	}
	// The fold is made over the six inherited items and the child's
	// own: what it names is on the child's path, in the prefix or after.
	if _, ok := cs.Entry(fold.FirstKept); !ok {
		t.Errorf("first kept %q is not on the child's path", fold.FirstKept)
	}
	if n := verifyAll(t, cs); n != 5 {
		t.Errorf("the child verifies %d responses, want the prefix's three and its two", n)
	}
	path := cs.Path(cs.Leaf())
	inherited := 0
	for _, e := range path {
		if cs.Prefix(e.Base().ID) {
			inherited++
		}
	}
	if inherited != prefix || len(OwnPath(cs))+inherited != len(path) {
		t.Errorf("after the fold the path holds %d inherited entries of %d, want the %d it forked with", inherited, len(path), prefix)
	}
	cx, err := cs.Context()
	if err != nil {
		t.Fatal(err)
	}
	if len(cx.Items) >= 10 {
		t.Errorf("the child's context holds %d items after a fold, want fewer than the ten it was sent", len(cx.Items))
	}
	if got := len(s.Entries()); got != parentEntries+1 {
		t.Errorf("the parent holds %d entries, want its %d and the link", got, parentEntries)
	}
	verifyAll(t, s)
}

// TestContinueChildMakesAForkAStopCutOff pins the link's fork base: a
// link ForkChild wrote whose child session was never made, as a stop
// between the two leaves it, is completed by ContinueChild from the
// base the link names, so no agent runs the child without its prefix.
func TestContinueChildMakesAForkAStopCutOff(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m"})
	defer rec.Attach(a)()
	if _, err := a.Prompt(ctx, openresponses.UserText("hello")); err != nil {
		t.Fatal(err)
	}
	base := s.Leaf()
	// The link as ForkChild writes it, and no child session.
	spawnID := agentsession.NewSpawnID()
	id := agentsession.SpawnedSessionID(s.ID(), spawnID)
	link := agentsession.NewSpawnLink(id, spawnID)
	setMember(&link.EntryBase, ForkBaseMember, []byte(`"`+base+`"`))
	if _, err := store.Append(ctx, s.ID(), link); err != nil {
		t.Fatal(err)
	}
	rec2, _, err := Resume(ctx, store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	cctx, err := rec2.ContinueChild(ctx, spawnID)
	if err != nil {
		t.Fatal(err)
	}
	model := &sawInput{}
	cfg := agentturn.Config{Model: model, ModelName: "m"}
	child, cs := forkedAgent(t, rec2, store, cctx, cfg)
	if cs.Header().Base != base || cs.Header().SpawnedBy != spawnID {
		t.Errorf("the child ContinueChild made has base %q spawned_by %q", cs.Header().Base, cs.Header().SpawnedBy)
	}
	if _, err := child.Prompt(agent.ContextWithConfig(cctx, cfg), openresponses.UserText("more")); err != nil {
		t.Fatal(err)
	}
	if len(model.inputs) != 1 || model.inputs[0] != 3 {
		t.Errorf("the child's request held %v items, want the prefix's two and its own", model.inputs)
	}
	cs, err = store.Open(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if n := verifyAll(t, cs); n != 2 {
		t.Errorf("the child verifies %d responses, want two", n)
	}
}

// callIDs are the call IDs of the function calls a transcript holds, in
// order.
func callIDs(t agentturn.Transcript) []string {
	var ids []string
	for _, it := range t {
		if c, ok := it.(*openresponses.FunctionCall); ok {
			ids = append(ids, c.CallID)
		}
	}
	return ids
}

// entryIDs are the IDs of entries, joined.
func entryIDs(es []agentsession.Entry) string {
	var ids []string
	for _, e := range es {
		ids = append(ids, e.Base().ID)
	}
	return strings.Join(ids, " ")
}

// TestForkChildBeforeForksAtTheEntryBeforeACall pins ForkChildBefore:
// it forks the session ctx records to at EntryBefore of the call, so
// the child opens without the call and whatever followed it, a call
// that is not the last of its response leaves the earlier ones on the
// prefix, and a call the path does not hold is refused with ErrNoCall
// and nothing written.
func TestForkChildBeforeForksAtTheEntryBeforeACall(t *testing.T) {
	type none struct{}
	for _, tc := range []struct {
		name string
		// calls are the arguments of the calls the parent's model makes
		// in its one response, and pick which of them to fork before; -1
		// names a call the path does not hold.
		calls []string
		pick  int
		// kept is how many of the parent's calls the child's transcript
		// holds.
		kept int
		err  error
	}{
		{name: "the only call", calls: []string{`{}`}, pick: 0, kept: 0},
		{name: "the first of two", calls: []string{`{}`, `{}`}, pick: 0, kept: 0},
		{name: "not the first of its response", calls: []string{`{}`, `{}`}, pick: 1, kept: 1},
		{name: "a call the path does not hold", calls: []string{`{}`}, pick: -1, err: ErrNoCall},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(ctx, store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			task := agenttool.New("task", "", func(context.Context, none) (string, error) { return "ok", nil })
			a := agentturn.New(agentturn.Config{Model: &twoCalls{calls: tc.calls}, ModelName: "m", Tools: []agenttool.Tool{task}})
			defer rec.Attach(a)()
			if _, err := a.Prompt(ctx, openresponses.UserText("go")); err != nil {
				t.Fatal(err)
			}
			made := callIDs(a.State().Transcript)
			if len(made) != len(tc.calls) {
				t.Fatalf("the parent made %d calls, want %d", len(made), len(tc.calls))
			}
			callID := "no-such-call"
			if tc.pick >= 0 {
				callID = made[tc.pick]
			}
			before := len(s.Entries())
			cctx, err := rec.ForkChildBefore(ctx, agentsession.NewSpawnID(), callID)
			if tc.err != nil {
				if !errors.Is(err, tc.err) {
					t.Fatalf("err = %v, want %v", err, tc.err)
				}
				if got := len(s.Entries()); got != before {
					t.Errorf("a refused fork wrote %d entries", got-before)
				}
				if len(links(s)) != 0 {
					t.Error("a refused fork left a link")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			at, _ := EntryBefore(s, callID)
			cs, err := store.Open(ctx, SessionIDFromContext(cctx))
			if err != nil {
				t.Fatal(err)
			}
			if cs.Header().Base != at {
				t.Errorf("base = %q, want the entry before the call, %q", cs.Header().Base, at)
			}
			opts, err := AgentOptions(cs, rec.ReadOptions()...)
			if err != nil {
				t.Fatal(err)
			}
			held := callIDs(agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m"}, opts...).State().Transcript)
			if len(held) != tc.kept {
				t.Fatalf("the child holds %d of the parent's calls, want %d", len(held), tc.kept)
			}
			for i, id := range held {
				if id != made[i] {
					t.Errorf("the child's call %d = %q, want the parent's %q", i, id, made[i])
				}
			}
		})
	}
}

// TestPrefixPathIsWhatAForkInherited pins PrefixPath: empty for a
// session that is no fork, the parent's path to the base for a fork,
// and for a fork of a fork the first fork's prefix and what it wrote
// before the base; with OwnPath it is the whole path, in order.
func TestPrefixPathIsWhatAForkInherited(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m"})
	defer rec.Attach(a)()
	if _, err := a.Prompt(ctx, openresponses.UserText("alpha")); err != nil {
		t.Fatal(err)
	}
	parentPath := entryIDs(s.Path(s.Leaf()))

	// The first fork, at the parent's leaf, makes a call of its own.
	cctx, err := rec.ForkChild(ctx, agentsession.NewSpawnID(), "")
	if err != nil {
		t.Fatal(err)
	}
	type none struct{}
	task := agenttool.New("task", "", func(context.Context, none) (string, error) { return "ok", nil })
	cfg := agentturn.Config{Model: &twoCalls{calls: []string{`{}`}}, ModelName: "m", Tools: []agenttool.Tool{task}}
	child, cs := forkedAgent(t, rec, store, cctx, cfg)
	if _, err := child.Prompt(agent.ContextWithConfig(cctx, cfg), openresponses.UserText("beta")); err != nil {
		t.Fatal(err)
	}
	made := callIDs(child.State().Transcript)
	if len(made) != 1 {
		t.Fatalf("the child made %d calls, want 1", len(made))
	}
	if cs, err = store.Open(ctx, cs.ID()); err != nil {
		t.Fatal(err)
	}

	// The second forks the first before that call.
	gctx, err := rec.ForkChildBefore(cctx, agentsession.NewSpawnID(), made[0])
	if err != nil {
		t.Fatal(err)
	}
	gs, err := store.Open(ctx, SessionIDFromContext(gctx))
	if err != nil {
		t.Fatal(err)
	}
	if gs.Header().ParentSession != cs.ID() {
		t.Fatalf("the second fork's parent = %q, want the first fork %q", gs.Header().ParentSession, cs.ID())
	}
	at, _ := EntryBefore(cs, made[0])
	toBase := cs.Path(at)

	for _, tc := range []struct {
		name   string
		s      *agentsession.Session
		prefix string
	}{
		{name: "not a fork", s: s, prefix: ""},
		{name: "a fork", s: cs, prefix: parentPath},
		{name: "a fork of a fork", s: gs, prefix: entryIDs(toBase)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix, own := PrefixPath(tc.s), OwnPath(tc.s)
			if got := entryIDs(prefix); got != tc.prefix {
				t.Errorf("prefix = %q, want %q", got, tc.prefix)
			}
			if got, want := entryIDs(append(prefix, own...)), entryIDs(tc.s.Path(tc.s.Leaf())); got != want {
				t.Errorf("prefix and own = %q, want the path %q", got, want)
			}
		})
	}
	if !strings.HasPrefix(entryIDs(toBase), parentPath) {
		t.Errorf("the second fork's prefix %q does not open with the parent's path %q", entryIDs(toBase), parentPath)
	}
}
