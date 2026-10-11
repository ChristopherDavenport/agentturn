package a2a

import (
	"context"
	"sync"

	"github.com/ChristopherDavenport/agentturn"
)

// ConversationStore keeps the transcript of each A2A context, so a
// second message with the same context ID continues the conversation.
// Load returns nil for an unknown context.
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
