package session

import (
	"context"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestChildSessionInheritsDirectoryAndIdentity checks the two things a
// child session was filed without: the directory its parent ran in,
// which a store buckets by, and an identity the run inside it can read,
// which is what a layer attributing its writes to a session needs.
func TestChildSessionInheritsDirectoryAndIdentity(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{CWD: "/work/repo"})
	if err != nil {
		t.Fatal(err)
	}
	var seen []string
	note := agenttool.New("note", "note something", func(ctx context.Context, _ echoArgs) (string, error) {
		seen = append(seen, SessionIDFromContext(ctx))
		return "noted", nil
	})
	childCfg := agentturn.Config{Name: "specialist", Description: "notes things", Model: &echo.Adapter{}, Tools: []agenttool.Tool{note}}
	specialist := agent.New(childCfg, agent.WithObserver(rec.Observe), agent.WithRunContext(rec.ChildContext))
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{specialist}})
	defer rec.Attach(a)()
	// The host names its own session the same way, so a tool of the
	// parent reads the parent's.
	ctx := ContextWithSessionID(context.Background(), rec.SessionID())
	if _, err := a.Prompt(ctx, openresponses.UserText("delegate")); err != nil {
		t.Fatal(err)
	}
	l := links(s)
	if len(l) != 1 {
		t.Fatalf("links = %+v", l)
	}
	child, err := store.Open(context.Background(), l[0].Session)
	if err != nil {
		t.Fatal(err)
	}
	if h := child.Header(); h.CWD != "/work/repo" {
		t.Errorf("child session filed under %q, want the parent's directory", h.CWD)
	}
	if len(seen) != 1 {
		t.Fatalf("the child's tool ran %d times", len(seen))
	}
	if seen[0] != child.ID() {
		t.Errorf("the child's tool saw session %q, want the child's %q", seen[0], child.ID())
	}
	if seen[0] == rec.SessionID() {
		t.Error("the child's writes are attributed to the parent")
	}
	verifyAll(t, s)
	verifyAll(t, child)
}

// TestSpawnedChildIsSteerableAndRevivable is the hub case: the host
// takes the child's agent from WithSpawn, talks to it while it runs,
// and prompts it again once the tool has returned. The second run
// continues the child's session at its leaf, because a subagent that is
// messaged again answers from its own context.
func TestSpawnedChildIsSteerableAndRevivable(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{CWD: "/work"})
	if err != nil {
		t.Fatal(err)
	}
	var children []*agentturn.Agent
	var spawnedFor []string
	childCfg := agentturn.Config{Name: "specialist", Model: &echo.Adapter{}}
	specialist := agent.New(childCfg,
		agent.WithObserver(rec.Observe),
		agent.WithRunContext(rec.ChildContext),
		agent.WithSpawn(func(callID string, child *agentturn.Agent) {
			spawnedFor = append(spawnedFor, callID)
			children = append(children, child)
			// Reaching a child before its run starts: the steered
			// message joins its first model call.
			child.Steer(openresponses.UserText("also note duplicates"))
		}))
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{specialist}})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("delegate")); err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 || len(spawnedFor) != 1 {
		t.Fatalf("spawned %d children for %v", len(children), spawnedFor)
	}
	child := children[0]
	l := links(s)
	if len(l) != 1 || l[0].CallID != spawnedFor[0] {
		t.Fatalf("links = %+v, spawned for %v", l, spawnedFor)
	}
	// The steered message reached the child with its input.
	if got := itemRoles(child.State().Transcript); got != "user user assistant" {
		t.Errorf("child transcript = %q", got)
	}
	cs, err := store.Open(context.Background(), l[0].Session)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(runsOf(t, cs)); n != 1 {
		t.Fatalf("child runs after the call = %d", n)
	}
	// Revived: the host prompts the finished child again.
	if _, err := child.Prompt(context.Background(), openresponses.UserText("which file was it?")); err != nil {
		t.Fatal(err)
	}
	cs, err = store.Open(context.Background(), l[0].Session)
	if err != nil {
		t.Fatal(err)
	}
	if n := roots(cs); n != 1 {
		t.Errorf("the revived child opened %d roots in %q", n, entryTypes(cs))
	}
	if n := len(runsOf(t, cs)); n != 2 {
		t.Errorf("child runs = %d", n)
	}
	// One turn in each run, every one of them on a path that rebuilds
	// its request.
	if n := verifyAll(t, cs); n != 2 || hashed(cs) != 2 {
		t.Errorf("child responses = %d hashed = %d", n, hashed(cs))
	}
	verifyAll(t, s)
}

// itemRoles names the messages of a transcript by role.
func itemRoles(items openresponses.Items) string {
	out := ""
	for _, it := range items {
		if out != "" {
			out += " "
		}
		if m, ok := it.(*openresponses.Message); ok {
			out += string(m.Role)
			continue
		}
		out += it.ItemType()
	}
	return out
}

// TestSessionOfNamesWhereARecordGoes pins #158: SessionOf names the
// session a record made with a context is written to, for a child
// built with WithObserver alone as for one wired with ChildContext:
// the child's during its run, when Annotate lands there with the
// call's ID; the child's after its run ended, when a job the child
// started writes with the run's context, which the recorder remembers
// for the run; and the recorder's own for a context with no run.
func TestSessionOfNamesWhereARecordGoes(t *testing.T) {
	cases := []struct {
		name  string
		wired bool
	}{
		{"with ChildContext", true},
		{"with WithObserver alone", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(ctx, store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			var jobCtx context.Context
			var during, fromContext, noteCall string
			note := agenttool.New("note", "note something", func(ctx context.Context, _ echoArgs) (string, error) {
				jobCtx = ctx
				call, _ := agenttool.CallFrom(ctx)
				noteCall = call.ID
				during = rec.SessionOf(ctx)
				fromContext = SessionIDFromContext(ctx)
				_, err := rec.Annotate(ctx, "test:manifest", map[string]string{"render": "r1"})
				return "noted", err
			})
			childCfg := agentturn.Config{Name: "helper", Description: "notes", Model: scriptedCalls{{"note", `{"text":"t"}`}}, Tools: []agenttool.Tool{note}}
			opts := []agent.Option{agent.WithObserver(rec.Observe)}
			if tc.wired {
				opts = append(opts, agent.WithRunContext(rec.ChildContext))
			}
			helper := agent.New(childCfg, opts...)
			a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{helper}})
			defer rec.Attach(a)()
			if _, err := a.Prompt(ctx, openresponses.UserText("delegate")); err != nil {
				t.Fatal(err)
			}
			l := links(s)
			if len(l) != 1 {
				t.Fatalf("links = %+v", l)
			}
			childID := l[0].Session
			if during != childID {
				t.Errorf("SessionOf during the child's run = %q, want the child's %q", during, childID)
			}
			if want := map[bool]string{true: childID, false: ""}[tc.wired]; fromContext != want {
				t.Errorf("SessionIDFromContext in the child = %q, want %q", fromContext, want)
			}
			// The run is over: the recorder remembers which session
			// it wrote, and a late job's record goes there too.
			if got := rec.SessionOf(jobCtx); got != childID {
				t.Errorf("SessionOf after the child's run = %q, want the child's %q", got, childID)
			}
			if got := rec.SessionOf(ctx); got != rec.SessionID() {
				t.Errorf("SessionOf with no run = %q, want the recorder's own %q", got, rec.SessionID())
			}
			if _, err := rec.Annotate(jobCtx, "test:job", map[string]string{"state": "done"}); err != nil {
				t.Fatal(err)
			}
			child, err := store.Open(ctx, childID)
			if err != nil {
				t.Fatal(err)
			}
			got := map[string]string{}
			for _, e := range child.Entries() {
				if c, ok := e.(*agentsession.CustomEntry); ok {
					got[c.NS] = c.CallID
				}
			}
			for _, ns := range []string{"test:manifest", "test:job"} {
				if callID, ok := got[ns]; !ok || callID != noteCall {
					t.Errorf("%s in the child: present=%v call_id=%q, want call %q", ns, ok, callID, noteCall)
				}
			}
			for _, e := range s.Entries() {
				if c, ok := e.(*agentsession.CustomEntry); ok {
					t.Errorf("%s was filed at the root", c.NS)
				}
			}
			verifyAll(t, s)
			verifyAll(t, child)
		})
	}
}

// TestReplayedChildOutputKeepsItsLink pins the session half of #206: a
// tools/agent child's call completes and is linked, a rebase to before
// its dispatch leaves the call on the path with its dispatch, link and
// output on the branch left, and a restart answers it with that output
// rather than running the child again. The new branch holds a link for
// the call to the same child session before the answer, so the record
// says whose work the output is, and the session still verifies.
func TestReplayedChildOutputKeepsItsLink(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	runs := 0
	childCfg := agentturn.Config{Name: "specialist", Description: "a child", Model: &echo.Adapter{}, ModelName: "c",
		BeforeTurn: func(context.Context, agentturn.TurnStartInfo) (openresponses.Items, error) { runs++; return nil, nil }}
	specialist := agent.New(childCfg, agent.WithObserver(rec.Observe))
	tools := []agenttool.Tool{specialist}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: tools})
	unsub := rec.Attach(a)
	if end, err := a.Prompt(ctx, openresponses.UserText("delegate")); err != nil || end.Reason != agentturn.ReasonDone {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	unsub()
	before := links(s)
	c := callsOf(t, s)["specialist"]
	if len(before) != 1 || c == nil || c.Dispatch == nil || c.Output == nil || before[0].CallID != c.ID() {
		t.Fatalf("links = %+v, call = %+v", before, c)
	}
	child := before[0].Session
	if runs != 1 {
		t.Fatalf("the child ran %d times", runs)
	}

	// A restart, then the rebase to before the dispatch.
	rec, s, err = Resume(ctx, store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Rebase(s, c.Dispatch.Parent); err != nil {
		t.Fatal(err)
	}
	answers, err := ReplayAnswers(ctx, s, tools, rec.ReadOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	if len(answers) != 1 || answers[0].Output == nil || answers[0].Reason != ranOffReason || answers[0].Origin != c.Output.ID {
		t.Fatalf("answers = %+v, want the output on the branch left with origin %s", answers, c.Output.ID)
	}
	opts, err := AgentOptions(s, rec.ReadOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: tools}, opts...)
	defer rec.Attach(b)()
	if end, err := b.Resume(ctx, answers...); err != nil || end.Reason != agentturn.ReasonDone {
		t.Fatalf("resume: err=%v end=%+v", err, end)
	}
	if runs != 1 {
		t.Errorf("the child ran %d times, want once", runs)
	}
	// The new branch: the link for the call to the same child, then
	// the answer and the output.
	var record []string
	for _, e := range s.Path(s.Leaf()) {
		switch e := e.(type) {
		case *agentsession.LinkEntry:
			record = append(record, "link")
			if e.Rel != agentsession.RelSubsession || e.CallID != c.ID() || e.Session != child {
				t.Errorf("link on the new branch = %+v, want %s linked to %s", e, c.ID(), child)
			}
		case *agentsession.DecisionEntry:
			record = append(record, e.Verdict)
		case *agentsession.DispatchEntry:
			record = append(record, "dispatch")
		case *agentsession.ItemEntry:
			if _, ok := e.Item.(*openresponses.FunctionCallOutput); ok {
				record = append(record, "output")
			}
		}
	}
	if got := strings.Join(record, " "); got != "link answer output" {
		t.Errorf("the new branch's record for the call = %q, want %q", got, "link answer output")
	}
	if err := s.VerifyRecords(s.Leaf()); err != nil {
		t.Errorf("verify records: %v", err)
	}
	verifyAll(t, s)
	cs, err := store.Open(ctx, child)
	if err != nil {
		t.Fatal(err)
	}
	verifyAll(t, cs)
}

// A child whose calls differ, through agent.WithCallConfig: each child
// session records the model its call ran, and one that starts from the
// parent's conversation, given as the call's items rather than a seed,
// is recorded whole and verifies, request hashes and all.
func TestAChildPerCallConfigIsRecordedAndVerifies(t *testing.T) {
	type taskArgs struct {
		Input string `json:"input"`
		Fork  bool   `json:"fork,omitempty"`
		Model string `json:"model,omitempty"`
	}
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{CWD: "/w"})
	if err != nil {
		t.Fatal(err)
	}
	task := agent.New(agentturn.Config{Name: "task", Description: "does tasks", Model: &echo.Adapter{}, ModelName: "small"},
		agent.WithCallConfig(func(_ context.Context, a taskArgs, parent agentturn.Transcript, cfg agentturn.Config) (agentturn.Config, openresponses.Items, error) {
			if a.Model != "" {
				cfg.ModelName = a.Model
			}
			var items openresponses.Items
			if a.Fork {
				items = append(items, parent...)
			}
			return cfg, append(items, openresponses.UserText(a.Input)), nil
		}),
		agent.WithObserver(rec.Observe), agent.WithRunContext(rec.ChildContext))
	parentModel := &twoCalls{calls: []string{`{"input":"fresh one"}`, `{"input":"forked one","fork":true,"model":"large"}`}}
	a := agentturn.New(agentturn.Config{Model: parentModel, Tools: []agenttool.Tool{task}})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("delegate twice")); err != nil {
		t.Fatal(err)
	}
	l := links(s)
	if len(l) != 2 {
		t.Fatalf("links = %+v", l)
	}
	models := map[string]bool{}
	for _, link := range l {
		child, err := store.Open(context.Background(), link.Session)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range child.Entries() {
			if c, ok := e.(*agentsession.ConfigEntry); ok && c.Model != "" {
				models[c.Model] = true
			}
		}
		if n := verifyAll(t, child); n == 0 {
			t.Errorf("child %s verified no responses", link.Session)
		}
	}
	if !models["small"] || !models["large"] {
		t.Errorf("child configs record models %v, want each call's", models)
	}
	verifyAll(t, s)
}

// twoCalls makes each of its calls to task in one batch, then answers.
type twoCalls struct{ calls []string }

func (m *twoCalls) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	for _, it := range req.Input {
		if _, ok := it.(*openresponses.FunctionCallOutput); ok {
			msg, err := em.Message(openresponses.PhaseFinalAnswer)
			if err != nil {
				return err
			}
			if err := msg.Text("done"); err != nil {
				return err
			}
			if err := msg.Close(); err != nil {
				return err
			}
			return em.Complete()
		}
	}
	for _, args := range m.calls {
		call, err := em.FunctionCall("", "task")
		if err != nil {
			return err
		}
		if err := call.Arguments(args); err != nil {
			return err
		}
		if err := call.Close(); err != nil {
			return err
		}
	}
	return em.Complete()
}
