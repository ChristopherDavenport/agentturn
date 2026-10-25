package session

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// reopenStore is a store whose Write reports Reopen for the entries at
// picks, as a remote client's does when its session could not follow
// the server: the entry goes to a session rebuilt from what the store
// wrote, which the store holds from then on, and the session handed
// out before has neither it nor anything after it. Durable is never
// set, as a store that does not report it leaves it.
type reopenStore struct {
	*agentsession.MemoryStore
	at func(id string, e agentsession.Entry) bool

	mu      sync.Mutex
	cur     map[string]*agentsession.Session
	reopens int
	// refuse fails every Open from when it is set.
	refuse bool
}

func newReopenStore(at func(id string, e agentsession.Entry) bool) *reopenStore {
	return &reopenStore{MemoryStore: agentsession.NewMemoryStore(), at: at, cur: map[string]*agentsession.Session{}}
}

func (s *reopenStore) Open(ctx context.Context, id string) (*agentsession.Session, error) {
	s.mu.Lock()
	refuse := s.refuse
	s.mu.Unlock()
	if refuse {
		return nil, errors.New("store away")
	}
	return s.held(ctx, id)
}

func (s *reopenStore) held(ctx context.Context, id string) (*agentsession.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess, ok := s.cur[id]; ok {
		return sess, nil
	}
	return s.MemoryStore.Open(ctx, id)
}

func (s *reopenStore) Read(ctx context.Context, id string) (*agentsession.Session, error) {
	live, err := s.Open(ctx, id)
	if err != nil {
		return nil, err
	}
	return rebuilt(live)
}

func (s *reopenStore) Append(ctx context.Context, id string, e agentsession.Entry) (string, error) {
	r, err := s.Write(ctx, id, e)
	return r.ID, err
}

func (s *reopenStore) Write(ctx context.Context, id string, e agentsession.Entry) (agentsession.Result, error) {
	live, err := s.held(ctx, id)
	if err != nil {
		return agentsession.Result{}, err
	}
	if !s.at(id, e) {
		got, err := live.Append(e)
		return agentsession.Result{ID: got}, err
	}
	fresh, err := rebuilt(live)
	if err != nil {
		return agentsession.Result{}, err
	}
	got, err := fresh.Append(e)
	if err != nil {
		return agentsession.Result{}, err
	}
	s.mu.Lock()
	s.cur[id] = fresh
	s.reopens++
	s.mu.Unlock()
	return agentsession.Result{ID: got, Reopen: true}, nil
}

// rebuilt is a copy of s read back from its encoding.
func rebuilt(s *agentsession.Session) (*agentsession.Session, error) {
	var buf bytes.Buffer
	if err := agentsession.Write(&buf, s); err != nil {
		return nil, err
	}
	return agentsession.Read(&buf)
}

// TestAReopenedSessionIsTheOneRebased pins #268: an append the store
// reports with Reopen has the recorder open the session again and hand
// it to the host, and a rebase of the session the store replaced is
// refused, since the store would append the next entry at its own leaf
// and not the rebase's. The store never reports Durable, and the
// dispatches it acknowledges run their tools.
func TestAReopenedSessionIsTheOneRebased(t *testing.T) {
	first := true
	store := newReopenStore(func(_ string, e agentsession.Entry) bool {
		_, ok := e.(*agentsession.DispatchEntry)
		reopen := ok && first
		if reopen {
			first = false
		}
		return reopen
	})
	var handed []*agentsession.Session
	rec, s, err := Start(context.Background(), store, agentsession.Header{}, WithReopened(func(s *agentsession.Session) {
		handed = append(handed, s)
	}))
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{upper}})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	if store.reopens != 1 || len(handed) != 1 {
		t.Fatalf("reopens = %d, sessions handed = %d", store.reopens, len(handed))
	}
	live := handed[0]
	if open, _ := store.Open(context.Background(), s.ID()); open != live || live == s {
		t.Fatal("the session handed is not the one the store holds")
	}
	if _, ok := s.Entry(live.Leaf()); ok {
		t.Fatal("the replaced session holds the run's end")
	}
	var checkpoint string
	for _, e := range live.Entries() {
		if it, ok := e.(*agentsession.ItemEntry); ok {
			if _, ok := it.Item.(*openresponses.FunctionCallOutput); ok {
				checkpoint = e.Base().ID
			}
		}
	}
	if checkpoint == "" {
		t.Fatal("no tool output: a dispatch without Durable did not run its tool")
	}
	if err := rec.Rebase(s, checkpoint); !errors.Is(err, ErrSessionReplaced) {
		t.Fatalf("rebase of the replaced session: err = %v", err)
	}
	if err := rec.Rebase(live, checkpoint); err != nil {
		t.Fatal(err)
	}
	leaf, _ := live.Entry(live.Leaf())
	end, ok := leaf.(*agentsession.RunEntry)
	if !ok || !end.IsEnd() || end.Parent != checkpoint {
		t.Fatalf("leaf after the rewind = %+v", leaf)
	}
	cx, err := live.Context()
	if err != nil {
		t.Fatal(err)
	}
	if err := a.SetTranscript(cx.Items); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Prompt(context.Background(), openresponses.UserText("from the checkpoint")); err != nil {
		t.Fatal(err)
	}
	verifyAll(t, live)
}

// TestResumeReturnsTheSessionAReopenLeft pins #268: Resume closes a
// run a cut left open, and when the store reports that end with Reopen
// it seeds the recorder from, and returns, the session the store holds,
// which the end and the inputs written again after it are on.
func TestResumeReturnsTheSessionAReopenLeft(t *testing.T) {
	armed := false
	store := newReopenStore(func(_ string, e agentsession.Entry) bool {
		r, ok := e.(*agentsession.RunEntry)
		return armed && ok && r.IsEnd()
	})
	cut := cutRun(t, store)
	armed = true
	var handed []*agentsession.Session
	rec, s, err := Resume(context.Background(), store, cut.ID(), WithReopened(func(s *agentsession.Session) {
		handed = append(handed, s)
	}))
	if err != nil {
		t.Fatal(err)
	}
	armed = false
	open, _ := store.Open(context.Background(), cut.ID())
	if store.reopens != 1 || len(handed) != 1 || s != handed[0] || s != open || s == cut {
		t.Fatalf("reopens = %d, handed = %d; Resume returned the store's session: %v", store.reopens, len(handed), s == open)
	}
	owed, err := s.PendingQueued(s.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	if len(owed) != 1 {
		t.Fatalf("owed after resume = %+v", owed)
	}
	if err := s.VerifyRecords(s.Leaf()); err != nil {
		t.Errorf("verify records: %v", err)
	}
	// The recorder was seeded with the input written again, not the one
	// the end closed, so Requeue hands back the entry the path owes.
	cx, err := s.Context()
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m"}, agentturn.WithTranscript(cx.Items))
	defer rec.Attach(a)()
	if n := rec.Requeue(context.Background(), a); n != 1 {
		t.Fatalf("requeued %d", n)
	}
	if _, err := a.Prompt(context.Background(), openresponses.UserText("go on")); err != nil {
		t.Fatal(err)
	}
	var drained *agentsession.ItemEntry
	for _, e := range s.Entries() {
		if v, ok := e.(*agentsession.ItemEntry); ok && v.QueuedFrom != "" {
			drained = v
		}
	}
	if drained == nil || drained.QueuedFrom != owed[0].ID {
		t.Errorf("drained item = %+v, want queued_from %s", drained, owed[0].ID)
	}
	if err := rec.Rebase(s, s.Leaf()); err != nil {
		t.Errorf("rebase of the returned session: %v", err)
	}
}

// TestResumeFailsWhenItsReopenFails pins #268: when the store reports
// the end Resume writes with Reopen and the session cannot be opened
// again, Resume fails rather than return the session the store
// replaced, seeded from a path without that end. The cut run owed no
// input, so the end is the only entry Resume writes.
func TestResumeFailsWhenItsReopenFails(t *testing.T) {
	armed := false
	var store *reopenStore
	store = newReopenStore(func(_ string, e agentsession.Entry) bool {
		r, ok := e.(*agentsession.RunEntry)
		if armed && ok && r.IsEnd() {
			store.refuse = true
			return true
		}
		return false
	})
	ctx := context.Background()
	cut, err := store.Create(ctx, agentsession.Header{Records: agentsession.AllRecords})
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := agentsession.ConfigFromRequest(openresponses.Request{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []agentsession.Entry{
		agentsession.NewRunStart("run_cut", agentsession.SourceInput, ""),
		cfg,
		&agentsession.ItemEntry{Item: openresponses.UserText("hello")},
	} {
		if _, err := store.Append(ctx, cut.ID(), e); err != nil {
			t.Fatal(err)
		}
	}
	armed = true
	rec, s, err := Resume(ctx, store, cut.ID())
	if err == nil || rec != nil || s != nil {
		t.Fatalf("Resume after a failed reopen: err = %v", err)
	}
	if store.reopens != 1 {
		t.Errorf("reopens = %d", store.reopens)
	}
}

// TestAChildSessionsReopenIsTheRecordersOwn pins #268: a Reopen on an
// entry of a child session has the recorder open that session again,
// and the host, which holds only the root, is not handed it.
func TestAChildSessionsReopenIsTheRecordersOwn(t *testing.T) {
	var root string
	store := newReopenStore(func(id string, e agentsession.Entry) bool {
		_, ok := e.(*agentsession.DispatchEntry)
		return ok && id != root
	})
	var handed []*agentsession.Session
	rec, s, err := Start(context.Background(), store, agentsession.Header{}, WithReopened(func(s *agentsession.Session) {
		handed = append(handed, s)
	}))
	if err != nil {
		t.Fatal(err)
	}
	root = s.ID()
	note := agenttool.New("note", "note something", func(context.Context, echoArgs) (string, error) {
		return "noted", nil
	})
	childCfg := agentturn.Config{Name: "specialist", Description: "notes things", Model: &echo.Adapter{}, Tools: []agenttool.Tool{note}}
	specialist := agent.New(childCfg, agent.WithObserver(rec.Observe), agent.WithRunContext(rec.ChildContext))
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{specialist}})
	defer rec.Attach(a)()
	ctx := ContextWithSessionID(context.Background(), rec.SessionID())
	if _, err := a.Prompt(ctx, openresponses.UserText("delegate")); err != nil {
		t.Fatal(err)
	}
	if store.reopens != 1 || len(handed) != 0 {
		t.Fatalf("reopens = %d, sessions handed = %d", store.reopens, len(handed))
	}
	l := links(s)
	if len(l) != 1 {
		t.Fatalf("links = %+v", l)
	}
	child, err := store.Open(context.Background(), l[0].Session)
	if err != nil {
		t.Fatal(err)
	}
	outputs := 0
	for _, e := range child.Path(child.Leaf()) {
		if it, ok := e.(*agentsession.ItemEntry); ok {
			if _, ok := it.Item.(*openresponses.FunctionCallOutput); ok {
				outputs++
			}
		}
	}
	if outputs != 1 {
		t.Errorf("tool outputs on the child's reopened session = %d, want 1", outputs)
	}
	verifyAll(t, child)
	verifyAll(t, s)
	if err := rec.Rebase(s, s.Leaf()); err != nil {
		t.Errorf("rebase of the root: %v", err)
	}
}

// failingOpenStore reports Reopen for every entry and fails the Opens
// that follow until fail is cleared.
type failingOpenStore struct {
	*reopenStore
	fail  bool
	opens int
}

func (s *failingOpenStore) Open(ctx context.Context, id string) (*agentsession.Session, error) {
	s.opens++
	if s.fail {
		return nil, errors.New("store away")
	}
	return s.reopenStore.Open(ctx, id)
}

func (s *failingOpenStore) Write(ctx context.Context, id string, e agentsession.Entry) (agentsession.Result, error) {
	live, err := s.reopenStore.Open(ctx, id)
	if err != nil {
		return agentsession.Result{}, err
	}
	got, err := live.Append(e)
	return agentsession.Result{ID: got, Reopen: true}, err
}

// testNote is an entry of the test's own.
func testNote(text string) agentsession.Entry {
	return &agentsession.CustomEntry{NS: "test:note", Data: json.RawMessage(`"` + text + `"`)}
}

// TestAFailedReopenFailsTheNextEntry pins #268: the append the store
// reported with Reopen stands when the reopen fails, and the next entry
// fails, as the session is still out of step.
func TestAFailedReopenFailsTheNextEntry(t *testing.T) {
	store := &failingOpenStore{reopenStore: newReopenStore(func(string, agentsession.Entry) bool { return false }), fail: true}
	rec, _, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	w := rec.root
	if _, err := w.append(ctx, testNote("one")); err != nil {
		t.Fatalf("the reopened append: %v", err)
	}
	if _, err := w.append(ctx, testNote("two")); err == nil {
		t.Fatal("an entry after a failed reopen was written")
	}
	store.fail = false
	if _, err := w.append(ctx, testNote("three")); err != nil {
		t.Fatal(err)
	}
	if store.opens != 4 {
		t.Errorf("opens = %d, want 4", store.opens)
	}
}
