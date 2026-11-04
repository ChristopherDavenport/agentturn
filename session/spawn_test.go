package session

import (
	"context"
	"errors"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentsession/export"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// runSpawned runs a child agent the host starts itself under the
// context StartChild returned, observed by rec, and returns the agent.
func runSpawned(t *testing.T, rec *Recorder, ctx context.Context, cfg agentturn.Config, input string) *agentturn.Agent {
	t.Helper()
	child := agentturn.New(cfg)
	child.Subscribe(func(ctx context.Context, ev agentturn.Event) error {
		rec.Observe(ctx, ev)
		return nil
	})
	if _, err := child.Prompt(agent.ContextWithConfig(ctx, cfg), openresponses.UserText(input)); err != nil {
		t.Fatal(err)
	}
	return child
}

// TestAHostStartedChildIsASubsession pins #294: a child the host starts
// itself, not through a model's function call, is recorded as a
// subsession of the session the recorder serves. The parent's link
// carries a spawn_id and is written before the child's header, the
// child's ID is derived over ["spawn",<parent>,<spawn_id>], its header
// names the parent and the spawn, and the parent's verify, cost and
// export reach its work. A spawn_id is refused when the session already
// carries it, by the recorder that wrote it and by one resumed later.
func TestAHostStartedChildIsASubsession(t *testing.T) {
	for _, media := range []string{"", agentsession.MediaSidecar} {
		t.Run("media "+media, func(t *testing.T) {
			ctx := context.Background()
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(ctx, store, agentsession.Header{Media: media})
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m"})
			defer rec.Attach(a)()
			if _, err := a.Prompt(ctx, openresponses.UserText("hello")); err != nil {
				t.Fatal(err)
			}

			spawnID := agentsession.NewSpawnID()
			cctx, err := rec.StartChild(ctx, spawnID)
			if err != nil {
				t.Fatal(err)
			}
			want := agentsession.SpawnedSessionID(s.ID(), spawnID)
			if got := SessionIDFromContext(cctx); got != want {
				t.Errorf("the context names %q, want the derived %q", got, want)
			}
			// The link is on the parent's path before the child's
			// header exists.
			l := links(s)
			if len(l) != 1 || l[0].SpawnID != spawnID || l[0].Session != want || l[0].CallID != "" {
				t.Fatalf("links = %+v", l)
			}
			if _, err := store.Open(ctx, want); err == nil {
				t.Error("the child's session exists before its run starts")
			}

			cfg := agentturn.Config{Name: "memory-pass", Model: &echo.Adapter{}, ModelName: "m"}
			runSpawned(t, rec, cctx, cfg, "consolidate")
			cs, err := store.Open(ctx, want)
			if err != nil {
				t.Fatalf("open the child: %v", err)
			}
			h := cs.Header()
			if h.ParentSession != s.ID() || h.SpawnedBy != spawnID || h.Media != media {
				t.Errorf("child header parent %q spawned_by %q media %q", h.ParentSession, h.SpawnedBy, h.Media)
			}
			if info, ok := cs.Entries()[0].(*agentsession.InfoEntry); !ok || info.Name != "memory-pass" {
				t.Errorf("the child's first entry = %+v, want its configuration's name", cs.Entries()[0])
			}
			if n := verifyAll(t, s); n != 1 || len(links(s)) != 1 {
				t.Errorf("the parent holds %d responses and links %+v after the child's run, want its own one and the spawn", n, links(s))
			}
			if verifyAll(t, cs) != 1 {
				t.Error("the child holds no response")
			}
			resolve := func(id string) (*agentsession.Session, error) {
				got, err := store.Open(ctx, id)
				if errors.Is(err, agentsession.ErrNoSession) {
					return nil, nil
				}
				return got, err
			}
			for _, x := range []*agentsession.Session{s, cs} {
				if err := x.VerifyRecords(x.Leaf()); err != nil {
					t.Errorf("verify records of %s: %v", x.ID(), err)
				}
			}
			if err := s.VerifyLinks(resolve); err != nil {
				t.Errorf("verify links: %v", err)
			}

			work, unread := agentsession.DelegatedWork(s.Path(s.Leaf()), resolve)
			if len(unread) != 0 || len(work) != 1 || work[0].Session != want || len(work[0].Calls) != 1 {
				t.Errorf("delegated work = %+v, unread %+v", work, unread)
			}

			tr, err := export.At(s, s.Leaf())
			if err != nil {
				t.Fatal(err)
			}
			doc, err := export.ToATIF(tr, export.Options{Subsessions: resolve})
			if err != nil {
				t.Fatal(err)
			}
			if len(doc.SubagentTrajectories) != 1 {
				t.Fatalf("subagent trajectories = %d, want the child's", len(doc.SubagentTrajectories))
			}
			agentSteps := 0
			for _, st := range doc.SubagentTrajectories[0].Steps {
				if st.Source == "agent" {
					agentSteps++
				}
			}
			if agentSteps != 1 {
				t.Errorf("the child's trajectory has %d agent steps, want 1", agentSteps)
			}

			// A spawn_id names one child in the session.
			if _, err := rec.StartChild(ctx, spawnID); !errors.Is(err, agentsession.ErrSpawnIDRepeated) {
				t.Errorf("a repeated spawn_id = %v, want ErrSpawnIDRepeated", err)
			}
			rec2, _, err := Resume(ctx, store, s.ID())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := rec2.StartChild(ctx, spawnID); !errors.Is(err, agentsession.ErrSpawnIDRepeated) {
				t.Errorf("a spawn_id repeated after a resume = %v, want ErrSpawnIDRepeated", err)
			}
			for _, bad := range []string{"", "a b", "spawn/1"} {
				if _, err := rec2.StartChild(ctx, bad); err == nil {
					t.Errorf("spawn_id %q taken", bad)
				}
			}
			if len(links(s)) != 1 {
				t.Errorf("a refused spawn wrote a link: %+v", links(s))
			}
		})
	}
}

// delegates calls the tool "helper" with an input, then answers once
// its output is in.
type delegates struct{}

func (delegates) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if _, ok := req.Input[len(req.Input)-1].(*openresponses.FunctionCallOutput); ok {
		msg, err := em.Message(openresponses.PhaseFinalAnswer)
		if err != nil {
			return err
		}
		if err := msg.Text("done"); err != nil {
			return err
		}
		return em.Complete()
	}
	call, err := em.FunctionCall("", "helper")
	if err != nil {
		return err
	}
	if err := call.Arguments(`{"input":"look"}`); err != nil {
		return err
	}
	if err := call.Close(); err != nil {
		return err
	}
	return em.Complete()
}

// TestAHostStartedChildPromptedAgainContinues checks that a host that
// keeps its child and prompts it again under the same context continues
// the child's session at its leaf, as a subagent messaged again does,
// and that a call child the spawned child runs through a tool is the
// spawned child's subsession, not a second spawn.
func TestAHostStartedChildPromptedAgainContinues(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	spawnID := agentsession.NewSpawnID()
	cctx, err := rec.StartChild(ctx, spawnID)
	if err != nil {
		t.Fatal(err)
	}
	helper := agent.New(agentturn.Config{Name: "helper", Model: &echo.Adapter{}},
		agent.WithObserver(rec.Observe))
	cfg := agentturn.Config{Name: "background", Model: delegates{}, ModelName: "m", Tools: []agenttool.Tool{helper}}
	child := runSpawned(t, rec, cctx, cfg, "first")
	if _, err := child.Prompt(agent.ContextWithConfig(cctx, cfg), openresponses.UserText("second")); err != nil {
		t.Fatal(err)
	}
	id := agentsession.SpawnedSessionID(s.ID(), spawnID)
	cs, err := store.Open(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if n := verifyAll(t, cs); n < 4 {
		t.Errorf("the child holds %d responses, want both runs' on one path", n)
	}
	if len(cs.Path(cs.Leaf())) != len(cs.Entries()) {
		t.Error("the second run did not continue the first's path")
	}
	if l := links(s); len(l) != 1 {
		t.Errorf("parent links = %+v, want the spawn alone", l)
	}
	cl := links(cs)
	if len(cl) == 0 || cl[0].CallID == "" || cl[0].SpawnID != "" {
		t.Fatalf("child links = %+v, want the helper's call", cl)
	}
	gs, err := store.Open(ctx, cl[0].Session)
	if err != nil {
		t.Fatal(err)
	}
	if gs.Header().ParentSession != id || gs.Header().SpawnedBy != cl[0].CallID {
		t.Errorf("grandchild header = %+v", gs.Header())
	}
}

// TestAHostStartedChildSurvivesAReopen checks the spawn link and the
// child's entries when the store reopens the parent at the link, as a
// remote store does when its session could not follow the server.
func TestAHostStartedChildSurvivesAReopen(t *testing.T) {
	ctx := context.Background()
	store := newReopenStore(func(_ string, e agentsession.Entry) bool {
		l, ok := e.(*agentsession.LinkEntry)
		return ok && l.SpawnID != ""
	})
	var handed []*agentsession.Session
	rec, _, err := Start(ctx, store, agentsession.Header{}, WithReopened(func(s *agentsession.Session) {
		handed = append(handed, s)
	}))
	if err != nil {
		t.Fatal(err)
	}
	spawnID := agentsession.NewSpawnID()
	cctx, err := rec.StartChild(ctx, spawnID)
	if err != nil {
		t.Fatal(err)
	}
	if store.reopens != 1 || len(handed) != 1 {
		t.Fatalf("reopens = %d, sessions handed = %d", store.reopens, len(handed))
	}
	runSpawned(t, rec, cctx, agentturn.Config{Model: &echo.Adapter{}, ModelName: "m"}, "go")
	live := handed[0]
	if l := links(live); len(l) != 1 || l[0].SpawnID != spawnID {
		t.Fatalf("links of the reopened session = %+v", l)
	}
	cs, err := store.Open(ctx, agentsession.SpawnedSessionID(live.ID(), spawnID))
	if err != nil {
		t.Fatal(err)
	}
	verifyAll(t, cs)
	if _, err := rec.StartChild(ctx, spawnID); !errors.Is(err, agentsession.ErrSpawnIDRepeated) {
		t.Errorf("a repeated spawn_id = %v, want ErrSpawnIDRepeated", err)
	}
}

// TestAHostStartedChildsWriterIsReleased pins the review of #294 (F4,
// F5): the writer of a spawned child's run is released at its run end,
// so a host running a pass after every turn holds none of them, a
// record made with the run's context still goes to the child, and a
// call child the spawned child started, prompted again after the
// spawned child's run ended, continues its own session under the
// spawned child's rather than being filed as the spawn.
func TestAHostStartedChildsWriterIsReleased(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	spawnID := agentsession.NewSpawnID()
	cctx, err := rec.StartChild(ctx, spawnID)
	if err != nil {
		t.Fatal(err)
	}
	var helpers []*agentturn.Agent
	helper := agent.New(agentturn.Config{Name: "helper", Model: &echo.Adapter{}},
		agent.WithObserver(rec.Observe),
		agent.WithSpawn(func(_ string, a *agentturn.Agent) { helpers = append(helpers, a) }))
	cfg := agentturn.Config{Name: "background", Model: delegates{}, ModelName: "m", Tools: []agenttool.Tool{helper}}
	var runID string
	child := agentturn.New(cfg)
	child.Subscribe(func(ctx context.Context, ev agentturn.Event) error {
		if e, ok := ev.(*agentturn.RunStart); ok {
			runID = e.RunID
		}
		rec.Observe(ctx, ev)
		return nil
	})
	if _, err := child.Prompt(agent.ContextWithConfig(cctx, cfg), openresponses.UserText("first")); err != nil {
		t.Fatal(err)
	}
	id := agentsession.SpawnedSessionID(s.ID(), spawnID)
	for r, w := range rec.runs {
		if w.id == id {
			t.Errorf("run %s still holds a writer of the spawned child after its end", r)
		}
	}
	if got := rec.SessionOf(agentturn.ContextWithRunID(ctx, runID)); got != id {
		t.Errorf("SessionOf the ended run = %q, want the child %q", got, id)
	}
	cs, err := store.Open(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	before := len(cs.Entries())
	cl := links(cs)
	if len(helpers) != 1 || len(cl) != 1 {
		t.Fatalf("helpers %d, child links %+v", len(helpers), cl)
	}
	// The helper is prompted again after the spawned child's run ended:
	// its observer's context carries the spawn and the helper's call.
	if _, err := helpers[0].Prompt(ctx, openresponses.UserText("again")); err != nil {
		t.Fatal(err)
	}
	if n := len(cs.Entries()); n != before {
		t.Errorf("the helper's second run wrote %d entries to the spawned child", n-before)
	}
	gs, err := store.Open(ctx, cl[0].Session)
	if err != nil {
		t.Fatal(err)
	}
	if n := verifyAll(t, gs); n != 2 {
		t.Errorf("the helper's session holds %d responses, want both runs'", n)
	}
	if l := links(cs); len(l) != 1 {
		t.Errorf("child links after the helper's second run = %+v, want the one", l)
	}
}
