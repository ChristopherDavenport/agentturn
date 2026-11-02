package session

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// errInjected is the append failure moveStore injects.
var errInjected = errors.New("injected append failure")

// moveStore is a memory store whose next Open can be held until the
// test releases it, and whose next appends can be made to fail.
type moveStore struct {
	*agentsession.MemoryStore
	mu          sync.Mutex
	hold        chan struct{}
	entered     chan struct{}
	failAppends int
	// failOpen, when set, fails the failOpen-th Open from now.
	failOpen int
}

func (m *moveStore) Open(ctx context.Context, id string) (*agentsession.Session, error) {
	m.mu.Lock()
	hold, entered := m.hold, m.entered
	m.hold = nil
	fail := false
	if m.failOpen > 0 {
		m.failOpen--
		fail = m.failOpen == 0
	}
	m.mu.Unlock()
	if fail {
		return nil, errInjected
	}
	if hold != nil {
		close(entered)
		<-hold
	}
	return m.MemoryStore.Open(ctx, id)
}

func (m *moveStore) Append(ctx context.Context, id string, e agentsession.Entry) (string, error) {
	m.mu.Lock()
	fail := m.failAppends > 0
	if fail {
		m.failAppends--
	}
	m.mu.Unlock()
	if fail {
		return "", errInjected
	}
	return m.MemoryStore.Append(ctx, id, e)
}

// TestARewindIsOneOperation pins #220: MoveTo moves the record's leaf
// and the agent's transcript, pending calls, owed notes and reasoning
// attribution together, refusing a run that starts while it moves, and
// a failure after the record moved leaves the agent and the record as
// they were, with an error that does not claim an undo failed.
func TestARewindIsOneOperation(t *testing.T) {
	setup := func(t *testing.T) (*moveStore, *Recorder, *agentturn.Agent, string, string) {
		t.Helper()
		ctx := context.Background()
		store := &moveStore{MemoryStore: agentsession.NewMemoryStore()}
		rec, _, err := Start(ctx, store, agentsession.Header{})
		if err != nil {
			t.Fatal(err)
		}
		a := agentturn.New(agentturn.Config{Model: &reasonsAndCalls{}, ModelName: "m", Tools: []agenttool.Tool{upper}})
		rec.Attach(a)
		if _, err := a.Prompt(ctx, openresponses.UserText("one")); err != nil {
			t.Fatal(err)
		}
		first := a.State().Transcript
		end, ok := rec.EntryOf(ctx, first[len(first)-1])
		if !ok {
			t.Fatal("no entry for the first run's last item")
		}
		var mid string
		for _, item := range first {
			if _, isCall := item.(*openresponses.FunctionCall); isCall {
				mid, _ = rec.EntryOf(ctx, item)
			}
		}
		if _, err := a.Prompt(ctx, openresponses.UserText("two")); err != nil {
			t.Fatal(err)
		}
		return store, rec, a, end, mid
	}

	t.Run("a prompt raced into the move is refused", func(t *testing.T) {
		ctx := context.Background()
		store, rec, a, end, _ := setup(t)
		store.mu.Lock()
		store.hold, store.entered = make(chan struct{}), make(chan struct{})
		hold, entered := store.hold, store.entered
		store.mu.Unlock()
		done := make(chan error, 1)
		go func() { done <- MoveTo(ctx, a, rec, end) }()
		<-entered
		if _, err := a.Prompt(ctx, openresponses.UserText("raced")); !errors.Is(err, agentturn.ErrRunning) {
			t.Errorf("a prompt during the move: %v, want ErrRunning", err)
		}
		close(hold)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		s, err := store.Open(ctx, rec.SessionID())
		if err != nil {
			t.Fatal(err)
		}
		// The entry is inside the first run, which the move closes on
		// the new branch, so the leaf is that run's end, after it.
		onPath := false
		for _, e := range s.Path(s.Leaf()) {
			onPath = onPath || e.Base().ID == end
		}
		if !onPath {
			t.Errorf("the leaf %s is not on a branch through %s", s.Leaf(), end)
		}
		want, err := Transcript(s)
		if err != nil {
			t.Fatal(err)
		}
		if got := a.State().Transcript; !equalJSON(got, want) {
			t.Errorf("the agent holds %d items, the record %d at the entry", len(got), len(want))
		}
		if end, err := a.Prompt(ctx, openresponses.UserText("after")); err != nil || end.Reason != agentturn.ReasonDone {
			t.Fatalf("prompt after the move: err=%v end=%+v", err, end)
		}
		verifyAll(t, s)
	})

	t.Run("a failure after the record moved leaves both as they were", func(t *testing.T) {
		ctx := context.Background()
		store, rec, a, _, mid := setup(t)
		s, err := store.Open(ctx, rec.SessionID())
		if err != nil {
			t.Fatal(err)
		}
		from, before := s.Leaf(), a.State().Transcript
		// The move into the first run closes that run on the new
		// branch, and the store refuses the entry.
		store.mu.Lock()
		store.failAppends = 1
		store.mu.Unlock()
		err = MoveTo(ctx, a, rec, mid)
		if !errors.Is(err, errInjected) {
			t.Fatalf("move: %v, want the store's failure", err)
		}
		if strings.Contains(err.Error(), "putting the record back") {
			t.Errorf("the error claims the undo failed: %v", err)
		}
		if s, err = store.Open(ctx, rec.SessionID()); err != nil {
			t.Fatal(err)
		}
		if s.Leaf() != from {
			t.Errorf("leaf = %s, want it back at %s", s.Leaf(), from)
		}
		if got := a.State().Transcript; !equalJSON(got, before) {
			t.Errorf("the agent's transcript changed: %d items, was %d", len(got), len(before))
		}
		if end, err := a.Prompt(ctx, openresponses.UserText("after")); err != nil || end.Reason != agentturn.ReasonDone {
			t.Fatalf("prompt after the failed move: err=%v end=%+v", err, end)
		}
		verifyAll(t, s)
	})

	t.Run("an input the agent holds is not queued twice by the undo", func(t *testing.T) {
		ctx := context.Background()
		store, rec, a, end, _ := setup(t)
		// Accepted after the last run, so the agent holds it and the
		// path owes it.
		if err := rec.Queue(ctx, a, agentturn.QueueFollowUp, openresponses.UserText("later")); err != nil {
			t.Fatal(err)
		}
		s, err := store.Open(ctx, rec.SessionID())
		if err != nil {
			t.Fatal(err)
		}
		from := s.Leaf()
		// MoveTo opens the session, Rebase opens it again, and the third
		// Open, after the record moved, fails.
		store.mu.Lock()
		store.failOpen = 3
		store.mu.Unlock()
		if err := MoveTo(ctx, a, rec, end); !errors.Is(err, errInjected) {
			t.Fatalf("move: %v, want the store's failure", err)
		}
		if s, err = store.Open(ctx, rec.SessionID()); err != nil {
			t.Fatal(err)
		}
		if s.Leaf() != from {
			t.Errorf("leaf = %s, want it back at %s: %s", s.Leaf(), from, entryTypes(s))
		}
		owed, err := s.PendingQueued(s.Leaf())
		if err != nil {
			t.Fatal(err)
		}
		if len(owed) != 1 {
			t.Errorf("the path owes %d inputs, want the one the agent holds", len(owed))
		}
	})
}

// TestARebaseTakesAnOwedEntryOnlyWithItsTrigger pins that a rebase
// takes an entry the new branch owes as an input's own only when it
// holds the same input with the same trigger: an entry for the same
// item brought by something else is not the input's, and the input is
// queued on its own entry, with its trigger.
func TestARebaseTakesAnOwedEntryOnlyWithItsTrigger(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: answers{}, ModelName: "m"})
	defer rec.Attach(a)()
	if _, err := a.Prompt(ctx, openresponses.UserText("one")); err != nil {
		t.Fatal(err)
	}
	byCron := agentturn.ContextWithTrigger(ctx, agentturn.Trigger{Kind: "cron", Ref: "nightly"})
	if err := rec.Queue(byCron, a, agentturn.QueueFollowUp, openresponses.UserText("later")); err != nil {
		t.Fatal(err)
	}
	if s, err = store.Open(ctx, s.ID()); err != nil {
		t.Fatal(err)
	}
	leaf := s.Leaf()
	// The agent's input is the same item brought by a person instead,
	// and its entry is closed: the owed cron entry is not its own.
	rec.mu.Lock()
	in := rec.root.inbox[0]
	in.trigger, in.entry = &agentsession.Trigger{Kind: "user", Ref: "alice"}, ""
	rec.mu.Unlock()
	if err := rec.Rebase(s, leaf); err != nil {
		t.Fatal(err)
	}
	if s, err = store.Open(ctx, s.ID()); err != nil {
		t.Fatal(err)
	}
	owed, err := s.PendingQueued(s.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	user := 0
	for _, q := range owed {
		if q.Trigger != nil && q.Trigger.Kind == "user" {
			user++
		}
	}
	if user != 1 {
		t.Errorf("owed entries with the person's trigger = %d, want the input queued on its own: %s", user, entryTypes(s))
	}
}
