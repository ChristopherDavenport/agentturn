package a2a

import (
	"context"
	"sync"

	"github.com/ChristopherDavenport/agentturn"
)

// ConversationStore keeps the transcript of each A2A context, so a
// second message with the same context ID continues the conversation.
// Load returns nil for an unknown context.
//
// A store that outlives the process should also implement
// [EscalationStore] and [WaitingStore], ideally keeping the waiting task
// in the same record as the transcript so the two are one write; else a
// restart loses which calls were put to the caller and which task the
// conversation waits on, and a cancel then falls back to the calls the
// task's status listed. The executor finds them by type assertion, so a
// store that wraps another must implement them itself. Without them the
// executor keeps both in memory, one entry per conversation that waits
// on its caller.
type ConversationStore interface {
	Load(ctx context.Context, contextID string) (agentturn.Transcript, error)
	Save(ctx context.Context, contextID string, t agentturn.Transcript) error
}

// MemoryStore is the in-memory [ConversationStore] the executor uses by
// default. The zero value is ready to use.
type MemoryStore struct {
	mu            sync.Mutex
	conversations map[string]agentturn.Transcript
	escalated     map[string][]Escalated
	waiting       memoryWaiting
	owners        MemoryOwners
}

// Load returns a copy of the stored transcript, or nil.
func (m *MemoryStore) Load(_ context.Context, contextID string) (agentturn.Transcript, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.conversations[contextID]
	if !ok {
		return nil, nil
	}
	return append(agentturn.Transcript(nil), t...), nil
}

// Save replaces the stored transcript.
func (m *MemoryStore) Save(_ context.Context, contextID string, t agentturn.Transcript) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conversations == nil {
		m.conversations = map[string]agentturn.Transcript{}
	}
	m.conversations[contextID] = append(agentturn.Transcript(nil), t...)
	return nil
}

var _ ConversationStore = (*MemoryStore)(nil)

// LoadEscalated returns the calls the conversation's last task put to
// its caller.
func (m *MemoryStore) LoadEscalated(_ context.Context, contextID string) ([]Escalated, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Escalated(nil), m.escalated[contextID]...), nil
}

// SaveEscalated replaces them; none removes them.
func (m *MemoryStore) SaveEscalated(_ context.Context, contextID string, calls []Escalated) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.escalated == nil {
		m.escalated = map[string][]Escalated{}
	}
	if len(calls) == 0 {
		delete(m.escalated, contextID)
		return nil
	}
	m.escalated[contextID] = append([]Escalated(nil), calls...)
	return nil
}

var _ EscalationStore = (*MemoryStore)(nil)

// Owner returns key's owner, kept with the conversations.
func (m *MemoryStore) Owner(ctx context.Context, key string) (string, bool, error) {
	return m.owners.Owner(ctx, key)
}

// Claim sets key's owner unless it has one and returns the owner.
func (m *MemoryStore) Claim(ctx context.Context, key, principal string) (string, error) {
	return m.owners.Claim(ctx, key, principal)
}

var _ Owners = (*MemoryStore)(nil)

// LoadWaiting returns the task the conversation waits on, or "".
func (m *MemoryStore) LoadWaiting(ctx context.Context, contextID string) (string, error) {
	return m.waiting.LoadWaiting(ctx, contextID)
}

// SaveWaiting replaces it; "" removes it.
func (m *MemoryStore) SaveWaiting(ctx context.Context, contextID, task string) error {
	return m.waiting.SaveWaiting(ctx, contextID, task)
}

var _ WaitingStore = (*MemoryStore)(nil)

// WaitingStore keeps, beside the transcript, the task each conversation
// waits on: the last one to end input-required, with the calls it
// listed pending, until a task ends otherwise. A cancel settles what a
// task left pending only while the conversation waits on that task, so
// the cancel of a task whose calls a later task listed again leaves
// them to the later one. A [ConversationStore] that implements it keeps
// it; with one that does not, the executor keeps it in memory, and after
// a restart a cancel, finding no task named, settles the calls the task
// listed when they are every call the conversation holds unanswered.
// [MemoryStore] implements it.
type WaitingStore interface {
	LoadWaiting(ctx context.Context, contextID string) (string, error)
	SaveWaiting(ctx context.Context, contextID, task string) error
}

// memoryWaiting is the executor's fallback [WaitingStore].
type memoryWaiting struct {
	mu   sync.Mutex
	task map[string]string
}

func (m *memoryWaiting) LoadWaiting(_ context.Context, contextID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.task[contextID], nil
}

func (m *memoryWaiting) SaveWaiting(_ context.Context, contextID, task string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task == "" {
		delete(m.task, contextID)
		return nil
	}
	if m.task == nil {
		m.task = map[string]string{}
	}
	m.task[contextID] = task
	return nil
}
