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
	"github.com/ChristopherDavenport/openresponses"
)

// reopenStore is a store whose Write reports Reopen for the entries at
// picks: the entry is appended, and the session the store holds is then
// one rebuilt from what it wrote, as a remote client's is when it could
// not follow the server. The session handed out before stays as it
// was, and appends go to the rebuilt one.
type reopenStore struct {
	*agentsession.MemoryStore
	at func(agentsession.Entry) bool

	mu      sync.Mutex
	cur     map[string]*agentsession.Session
	reopens int
}

func newReopenStore(at func(agentsession.Entry) bool) *reopenStore {
	return &reopenStore{MemoryStore: agentsession.NewMemoryStore(), at: at, cur: map[string]*agentsession.Session{}}
}

func (s *reopenStore) Open(ctx context.Context, id string) (*agentsession.Session, error) {
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
	live, err := s.Open(ctx, id)
	if err != nil {
		return agentsession.Result{}, err
	}
	got, err := live.Append(e)
	if err != nil {
		return agentsession.Result{}, err
	}
	r := agentsession.Result{ID: got, Durable: true}
	if s.at(e) {
		fresh, err := rebuilt(live)
		if err != nil {
			return agentsession.Result{}, err
		}
		s.mu.Lock()
		s.cur[id] = fresh
		s.reopens++
		s.mu.Unlock()
		r.Reopen = true
	}
	return r, nil
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
// and not the rebase's.
func TestAReopenedSessionIsTheOneRebased(t *testing.T) {
	first := true
	store := newReopenStore(func(e agentsession.Entry) bool {
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
	if len(live.Entries()) <= len(s.Entries()) {
		t.Fatalf("the replaced session has %d entries, the store's %d", len(s.Entries()), len(live.Entries()))
	}
	var checkpoint string
	for _, e := range live.Entries() {
		if it, ok := e.(*agentsession.ItemEntry); ok {
			if _, ok := it.Item.(*openresponses.FunctionCallOutput); ok {
				checkpoint = e.Base().ID
			}
		}
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
	store := &failingOpenStore{reopenStore: newReopenStore(func(agentsession.Entry) bool { return false }), fail: true}
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
