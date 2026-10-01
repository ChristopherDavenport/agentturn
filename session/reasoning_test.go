package session

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// thinker reasons before it answers, its reasoning item's encrypted
// content naming the request's model and the turn, and records the
// reasoning each request carries.
type thinker struct {
	mu   sync.Mutex
	n    int
	sent [][]string
}

func (m *thinker) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
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
	r.EncryptedContent(req.Model + "#" + string(rune('0'+n)))
	if err := r.Close(); err != nil {
		return err
	}
	w, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if err := w.Text("done"); err != nil {
		return err
	}
	return em.Complete()
}

// last is the reasoning the last request carried.
func (m *thinker) last() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return strings.Join(m.sent[len(m.sent)-1], ",")
}

// TestResumedReasoningKeepsItsModel pins the session half of #91: an
// agent seeded with AgentOptions knows which model produced each
// reasoning item on the path, the model the recorded settings named
// when its response was written, so after a resume a request to one
// model leaves out the reasoning another produced and keeps its own,
// as it does within one process. The recorder writes a change of
// model through SetConfig as a config entry naming the new one.
func TestResumedReasoningKeepsItsModel(t *testing.T) {
	cases := []struct {
		name string
		// before are the models of the runs before the restart, after
		// those of the runs after it, one prompt each.
		before, after []string
		// want is the reasoning each run after the restart sent.
		want []string
	}{
		{name: "switched before the restart", before: []string{"a", "b"}, after: []string{"b"}, want: []string{"b#2"}},
		{name: "switched back after the restart", before: []string{"a", "b"}, after: []string{"a"}, want: []string{"a#1"}},
		{name: "switched after the restart", before: []string{"a"}, after: []string{"b", "b"}, want: []string{"", "b#2"}},
		{name: "one model throughout", before: []string{"a"}, after: []string{"a"}, want: []string{"a#1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			model := &thinker{}
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(ctx, store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(agentturn.Config{Model: model, ModelName: tc.before[0]})
			unsub := rec.Attach(a)
			for i, name := range tc.before {
				if err := a.SetConfig(agentturn.Config{Model: model, ModelName: name}); err != nil {
					t.Fatal(err)
				}
				if _, err := a.Prompt(ctx, openresponses.UserText("before "+name)); err != nil {
					t.Fatal(err)
				}
				if i > 0 && name != tc.before[i-1] {
					var named bool
					for _, e := range s.Entries() {
						if c, ok := e.(*agentsession.ConfigEntry); ok && c.Model == name {
							named = true
						}
					}
					if !named {
						t.Errorf("no config entry names %s after SetConfig", name)
					}
				}
			}
			unsub()

			// A new process: nothing of the first agent's attribution
			// survives but what the record says.
			rec2, s2, err := Resume(ctx, store, s.ID())
			if err != nil {
				t.Fatal(err)
			}
			opts, err := AgentOptions(s2)
			if err != nil {
				t.Fatal(err)
			}
			b := agentturn.New(agentturn.Config{Model: model, ModelName: tc.after[0]}, opts...)
			defer rec2.Attach(b)()
			for i, name := range tc.after {
				if err := b.SetConfig(agentturn.Config{Model: model, ModelName: name}); err != nil {
					t.Fatal(err)
				}
				if _, err := b.Prompt(ctx, openresponses.UserText("after "+name)); err != nil {
					t.Fatal(err)
				}
				if got := model.last(); got != tc.want[i] {
					t.Errorf("run %d after the restart, to %s, sent reasoning %q, want %q", i, name, got, tc.want[i])
				}
			}
			if err := s2.VerifyRecords(s2.Leaf()); err != nil {
				t.Errorf("verify records: %v", err)
			}
		})
	}
}
