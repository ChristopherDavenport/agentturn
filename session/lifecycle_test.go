package session

import (
	"context"
	"encoding/json"
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

// hashed counts the response entries that carry a request hash.
func hashed(s *agentsession.Session) int {
	n := 0
	for _, e := range s.Entries() {
		if r, ok := e.(*agentsession.ResponseEntry); ok && r.RequestHash != "" {
			n++
		}
	}
	return n
}

func configs(s *agentsession.Session) []*agentsession.ConfigEntry {
	var out []*agentsession.ConfigEntry
	for _, e := range s.Entries() {
		if c, ok := e.(*agentsession.ConfigEntry); ok {
			out = append(out, c)
		}
	}
	return out
}

func TestRebaseRecordsTheBranch(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Instructions: "first"}
	a := agentturn.New(cfg)
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("one")); err != nil {
		t.Fatal(err)
	}
	// The entry to branch from: the first turn's response.
	var branchAt string
	for _, e := range s.Entries() {
		if _, ok := e.(*agentsession.ResponseEntry); ok {
			branchAt = e.Base().ID
		}
	}
	cfg.Instructions = "second"
	if err := a.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Prompt(context.Background(), openresponses.UserText("two")); err != nil {
		t.Fatal(err)
	}
	if n := len(configs(s)); n != 2 {
		t.Fatalf("configs before the branch = %d", n)
	}
	// Branch back to the first turn and run under the second
	// instructions: the new branch needs its own delta, and gets it.
	if err := rec.Rebase(s, branchAt); err != nil {
		t.Fatal(err)
	}
	cx, err := s.Context()
	if err != nil {
		t.Fatal(err)
	}
	if err := a.SetTranscript(cx.Items); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Prompt(context.Background(), openresponses.UserText("two again")); err != nil {
		t.Fatal(err)
	}
	if n := verifyAll(t, s); n != 3 || hashed(s) != 3 {
		t.Errorf("responses = %d hashed = %d", n, hashed(s))
	}
	if n := len(configs(s)); n != 3 {
		t.Errorf("configs after the branch = %d", n)
	}
	cx, _ = s.Context()
	if cx.Settings.Instructions != "second" || len(cx.Items) != 4 {
		t.Errorf("context at the new leaf: %+v, %d items", cx.Settings, len(cx.Items))
	}
	if len(s.Leaves()) != 2 {
		t.Errorf("leaves = %v", s.Leaves())
	}
	// A rebase to an unknown entry, or of another session, is refused;
	// a rebase during a run is refused.
	if err := rec.Rebase(s, "nope"); !errors.Is(err, agentsession.ErrNoEntry) {
		t.Errorf("rebase to a missing entry: %v", err)
	}
	other, _ := store.Create(context.Background(), agentsession.Header{})
	if err := rec.Rebase(other, branchAt); err == nil {
		t.Error("rebase of another session accepted")
	}
	blocking := agenttool.New("wait", "", func(ctx context.Context, _ echoArgs) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	cfg.Tools = []agenttool.Tool{blocking}
	if err := a.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		if _, ok := ev.(*agentturn.ToolStart); ok {
			close(started)
		}
		return nil
	})
	go a.Prompt(context.Background(), openresponses.UserText("abc"))
	<-started
	if err := rec.Rebase(s, branchAt); !errors.Is(err, ErrRunActive) {
		t.Errorf("rebase during a run: %v", err)
	}
	a.Abort()
	_ = a.WaitForIdle(context.Background())
}

func TestRebaseRealignsFolds(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	tr := compact.NewLocal(&echo.Adapter{}, compact.WithBudget(4), compact.WithKeepLast(2), compact.WithEstimator(countItems), compact.WithOnFold(rec.Fold))
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: tr.Transform})
	defer rec.Attach(a)()
	for _, text := range []string{"one", "two"} {
		if _, err := a.Prompt(context.Background(), openresponses.UserText(text)); err != nil {
			t.Fatal(err)
		}
	}
	// Branch to the first response, then run enough turns to fold on
	// the new branch with a fresh transform: first_kept names an entry
	// of the branch.
	var first string
	for _, e := range s.Entries() {
		if _, ok := e.(*agentsession.ResponseEntry); ok {
			first = e.Base().ID
			break
		}
	}
	if err := rec.Rebase(s, first); err != nil {
		t.Fatal(err)
	}
	cx, _ := s.Context()
	tr2 := compact.NewLocal(&echo.Adapter{}, compact.WithBudget(4), compact.WithKeepLast(2), compact.WithEstimator(countItems), compact.WithOnFold(rec.Fold))
	if err := a.SetConfig(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: tr2.Transform}); err != nil {
		t.Fatal(err)
	}
	if err := a.SetTranscript(cx.Items); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"three", "four"} {
		if _, err := a.Prompt(context.Background(), openresponses.UserText(text)); err != nil {
			t.Fatal(err)
		}
	}
	if n := verifyAll(t, s); n != 4 || hashed(s) != 4 {
		t.Errorf("responses = %d hashed = %d", n, hashed(s))
	}
	path := s.Path(s.Leaf())
	onPath := map[string]bool{}
	for _, e := range path {
		onPath[e.Base().ID] = true
	}
	folds := 0
	for _, e := range path {
		if c, ok := e.(*agentsession.CompactionEntry); ok {
			folds++
			if !onPath[c.FirstKept] {
				t.Errorf("first_kept %s is not on the branch", c.FirstKept)
			}
		}
	}
	if folds == 0 {
		t.Error("no fold on the new branch")
	}
}

func TestFilterFollowsSetConfig(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	note := func() openresponses.Item {
		return &openresponses.UnknownItem{Type: "agentturn:note", Raw: json.RawMessage(`{"type":"agentturn:note","text":"marker"}`)}
	}
	cfg := agentturn.Config{Model: &echo.Adapter{}}
	a := agentturn.New(cfg)
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), note(), openresponses.UserText("one")); err != nil {
		t.Fatal(err)
	}
	// The second phase shows the note to the model; the recorder follows
	// without being told.
	cfg.Filter = agentturn.VisibleFilter("agentturn:note")
	if err := a.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Prompt(context.Background(), note(), openresponses.UserText("two")); err != nil {
		t.Fatal(err)
	}
	if got := entryTypes(s); got != "run config custom item:user item:assistant* response run run item:agentturn:note item:user item:assistant* response run" {
		t.Errorf("entries = %q", got)
	}
	// The second request carries the first note too, which the path
	// holds only as a custom entry, so that response is honestly
	// written without a hash rather than with one that mismatches.
	if n := verifyAll(t, s); n != 2 || hashed(s) != 1 {
		t.Errorf("responses = %d hashed = %d", n, hashed(s))
	}
	// Detached, the recorder stops following the agent.
	rec2, s2, _ := Start(context.Background(), store, agentsession.Header{})
	b := agentturn.New(agentturn.Config{Model: &echo.Adapter{}})
	unsub := rec2.Attach(b)
	unsub()
	if rec2.agent != nil {
		t.Error("recorder still follows a detached agent")
	}
	// Handle without an agent uses WithFilter.
	rec3 := New(store, s2.ID(), WithFilter(agentturn.VisibleFilter("agentturn:note")))
	ctx := context.Background()
	for ev := range agentturn.Run(ctx, nil, openresponses.Items{note(), openresponses.UserText("x")}, agentturn.Config{Model: &echo.Adapter{}, Filter: agentturn.VisibleFilter("agentturn:note")}) {
		if err := rec3.Handle(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	if got := entryTypes(s2); !strings.Contains(got, "item:agentturn:note") {
		t.Errorf("entries with WithFilter = %q", got)
	}
	if n := verifyAll(t, s2); n != 1 || hashed(s2) != 1 {
		t.Errorf("responses = %d hashed = %d", n, hashed(s2))
	}
}

func TestContinueRollsOver(t *testing.T) {
	root := t.TempDir()
	store, err := jsonl.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rec, s, err := Start(context.Background(), store, agentsession.Header{CWD: root, Harness: &agentsession.Harness{Name: "h"}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Instructions: "be brief", Tools: []agenttool.Tool{upper}}
	a := agentturn.New(cfg)
	unsub := rec.Attach(a)
	for _, text := range []string{"one", "two"} {
		if _, err := a.Prompt(context.Background(), openresponses.UserText(text)); err != nil {
			t.Fatal(err)
		}
	}
	unsub()
	if err := store.Release(s.ID()); err != nil {
		t.Fatal(err)
	}
	summary := openresponses.UserText("Summary of the conversation so far: two prompts were echoed.")
	rec2, next, err := Continue(context.Background(), store, s.ID(), summary)
	if err != nil {
		t.Fatal(err)
	}
	if h := next.Header(); h.ParentSession != s.ID() || h.Harness == nil || h.Harness.Name != "h" || strings.Join(h.Records, ",") != "run,dispatch,decision" {
		t.Errorf("successor header = %+v", h)
	}
	if got := entryTypes(next); got != "config item:user" {
		t.Errorf("successor entries = %q", got)
	}
	cx, err := next.Context()
	if err != nil {
		t.Fatal(err)
	}
	if cx.Settings.Instructions != "be brief" || cx.Settings.Model != "m" || len(cx.Settings.Tools) != 1 || len(cx.Items) != 1 {
		t.Errorf("successor context = %+v items %d", cx.Settings, len(cx.Items))
	}
	// The agent continues from the successor's context, under the same
	// configuration: no config is written, the requests verify.
	b := agentturn.New(cfg, agentturn.WithTranscript(cx.Items))
	defer rec2.Attach(b)()
	if _, err := b.Prompt(context.Background(), openresponses.UserText("three")); err != nil {
		t.Fatal(err)
	}
	if got := entryTypes(next); got != "config item:user run item:user item:function_call* response dispatch item:function_call_output item:assistant* response run" {
		t.Errorf("successor entries after a run = %q", got)
	}
	if n := verifyAll(t, next); n != 2 || hashed(next) != 2 {
		t.Errorf("successor responses = %d hashed = %d", n, hashed(next))
	}
	// The old session is superseded and lists as such.
	old, err := store.Open(context.Background(), s.ID())
	if err != nil {
		t.Fatal(err)
	}
	if old.SupersededBy() != next.ID() {
		t.Errorf("old session superseded by %q, want %s", old.SupersededBy(), next.ID())
	}
	// A memory store continues the same way, without a summary.
	mem := agentsession.NewMemoryStore()
	rec3, s3, _ := Start(context.Background(), mem, agentsession.Header{})
	c := agentturn.New(cfg)
	unsub = rec3.Attach(c)
	if _, err := c.Prompt(context.Background(), openresponses.UserText("x")); err != nil {
		t.Fatal(err)
	}
	unsub()
	if _, next3, err := Continue(context.Background(), mem, s3.ID(), nil); err != nil || entryTypes(next3) != "config" {
		t.Errorf("continue without a summary: err=%v entries=%q", err, entryTypes(next3))
	}
}

func TestAnnotateLandsBeforeTheTurnConfig(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	// Instructions change every turn, so every turn writes a config
	// delta; the hash still verifies since instructions are a setting.
	turn := 0
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{upper},
		BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
			turn++
			req.Instructions = "turn " + string(rune('0'+turn))
			return nil
		}})
	defer rec.Attach(a)()
	type manifest struct {
		Rendered []string `json:"rendered"`
		Turn     int      `json:"turn"`
	}
	// Registered after the recorder, so the recorder's own turn_start
	// handling has already run when this subscriber annotates.
	a.Subscribe(func(ctx context.Context, ev agentturn.Event) error {
		if e, ok := ev.(*agentturn.TurnStart); ok {
			return rec.Annotate(ctx, "app:render", manifest{Rendered: []string{"memory-1"}, Turn: e.Turn})
		}
		return nil
	})
	if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	if got := entryTypes(s); got != "run config item:user custom config item:function_call* response dispatch item:function_call_output custom config item:assistant* response run" {
		t.Errorf("entries = %q", got)
	}
	if n := verifyAll(t, s); n != 2 || hashed(s) != 2 {
		t.Errorf("responses = %d hashed = %d", n, hashed(s))
	}
	var notes []manifest
	for _, e := range s.Entries() {
		if c, ok := e.(*agentsession.CustomEntry); ok && c.NS == "app:render" {
			var m manifest
			if err := json.Unmarshal(c.Data, &m); err != nil {
				t.Fatal(err)
			}
			notes = append(notes, m)
		}
	}
	if len(notes) != 2 || notes[0].Turn != 1 || notes[1].Turn != 2 || notes[0].Rendered[0] != "memory-1" {
		t.Errorf("annotations = %+v", notes)
	}
	// The annotation contributes nothing to the context.
	cx, _ := s.Context()
	if len(cx.Items) != len(a.State().Transcript) {
		t.Errorf("context has %d items, agent %d", len(cx.Items), len(a.State().Transcript))
	}
	// Outside a run, the annotation goes to the recorder's session at
	// its leaf; with a child's run on the context, to the child's.
	if err := rec.Annotate(context.Background(), "app:idle", map[string]int{"n": 1}); err != nil {
		t.Fatal(err)
	}
	entries := s.Entries()
	if c, ok := entries[len(entries)-1].(*agentsession.CustomEntry); !ok || c.NS != "app:idle" {
		t.Errorf("idle annotation = %+v", entries[len(entries)-1])
	}
	child := agent.New(agentturn.Config{Name: "child", Model: &echo.Adapter{}},
		agent.WithObserver(func(ctx context.Context, ev agentturn.Event) {
			rec.Observe(ctx, ev)
			if e, ok := ev.(*agentturn.TurnStart); ok {
				_ = rec.Annotate(agentturn.ContextWithRunID(ctx, e.RunID), "app:child", "seen")
			}
		}))
	if err := a.SetConfig(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{child}}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Prompt(context.Background(), openresponses.UserText("delegate")); err != nil {
		t.Fatal(err)
	}
	l := links(s)
	cs, err := store.Open(context.Background(), l[len(l)-1].Session)
	if err != nil {
		t.Fatal(err)
	}
	if got := entryTypes(cs); !strings.Contains(got, "custom") {
		t.Errorf("child entries = %q", got)
	}
	if got := entryTypes(s); strings.Contains(got, "app:child") {
		t.Error("child annotation landed in the parent")
	}
}

func TestUnrecordedTransformOmitsHash(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	// A transform that injects context the record cannot rebuild.
	inject := func(_ context.Context, t agentturn.Transcript) (agentturn.Transcript, error) {
		return append(append(agentturn.Transcript(nil), t...), openresponses.DeveloperText("the time is now")), nil
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Transform: inject})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("x")); err != nil {
		t.Fatal(err)
	}
	if n := verifyAll(t, s); n != 1 || hashed(s) != 0 {
		t.Errorf("responses = %d hashed = %d", n, hashed(s))
	}
	// So does a BeforeModelCall that edits the input, and a seeded
	// child whose transcript the recorder never wrote; a hook that
	// edits a setting keeps the hash.
	store2 := agentsession.NewMemoryStore()
	rec2, s2, _ := Start(context.Background(), store2, agentsession.Header{})
	b := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
		req.Input = append(req.Input, openresponses.DeveloperText("injected"))
		return nil
	}})
	defer rec2.Attach(b)()
	if _, err := b.Prompt(context.Background(), openresponses.UserText("x")); err != nil {
		t.Fatal(err)
	}
	if hashed(s2) != 0 {
		t.Error("a request with injected input carried a hash")
	}
	store3 := agentsession.NewMemoryStore()
	rec3, s3, _ := Start(context.Background(), store3, agentsession.Header{})
	seeded := agent.New(agentturn.Config{Name: "seeded", Model: &echo.Adapter{}},
		agent.WithObserver(rec3.Observe),
		agent.WithTranscript(func(parent agentturn.Transcript) agentturn.Transcript {
			return agentturn.Transcript{openresponses.UserText("context from the parent")}
		}))
	c := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{seeded},
		BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
			req.Instructions = "edited"
			return nil
		}})
	defer rec3.Attach(c)()
	if _, err := c.Prompt(context.Background(), openresponses.UserText("delegate")); err != nil {
		t.Fatal(err)
	}
	if n := verifyAll(t, s3); n != 2 || hashed(s3) != 2 {
		t.Errorf("parent responses = %d hashed = %d", n, hashed(s3))
	}
	cs, err := store3.Open(context.Background(), links(s3)[0].Session)
	if err != nil {
		t.Fatal(err)
	}
	if n := verifyAll(t, cs); n != 1 || hashed(cs) != 0 {
		t.Errorf("seeded child responses = %d hashed = %d", n, hashed(cs))
	}
	// A blocked call under an unrecorded transform is likewise
	// written without a hash.
	store4 := agentsession.NewMemoryStore()
	rec4, s4, _ := Start(context.Background(), store4, agentsession.Header{})
	d := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Transform: inject, BeforeModelCall: func(context.Context, *openresponses.Request) error { return errors.New("no") }})
	defer rec4.Attach(d)()
	_, _ = d.Prompt(context.Background(), openresponses.UserText("x"))
	if n := verifyAll(t, s4); n != 1 || hashed(s4) != 0 {
		t.Errorf("blocked responses = %d hashed = %d", n, hashed(s4))
	}
}
