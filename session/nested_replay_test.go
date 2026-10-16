package session

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// evalOnly asks for one call of eval until the transcript ends with a
// call's output, and then answers.
type evalOnly struct{}

func (evalOnly) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if _, ok := req.Input[len(req.Input)-1].(*openresponses.FunctionCallOutput); ok {
		w, err := em.Message(openresponses.PhaseFinalAnswer)
		if err != nil {
			return err
		}
		if err := w.Text("done"); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		return em.Complete()
	}
	w, err := em.FunctionCall("", "eval")
	if err != nil {
		return err
	}
	if err := w.Arguments(`{}`); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Complete()
}

// TestNestedCallsSurviveARestart pins #261 on the record: a call whose
// tool made two nested calls is cut by a crash while the second runs.
// The record holds each nested call's start, its dispatch with the key
// and, for the first, its end; Pending reads them back as the pending
// call's Invoked. Run again in a new process, the first nested call
// returns its recorded result without running, written as replayed
// with no dispatch, and the second, keyed, runs again under the key
// and the ID it had.
func TestNestedCallsSurviveARestart(t *testing.T) {
	var (
		mu       sync.Mutex
		reads    int
		keys     []string
		ids      []string
		blocking atomic.Bool
	)
	blocking.Store(true)
	started := make(chan struct{}, 1)
	safe := agenttool.WithReplay(func(context.Context, json.RawMessage) agenttool.Replay { return agenttool.ReplaySafe })
	keyed := agenttool.WithReplay(func(context.Context, json.RawMessage) agenttool.Replay { return agenttool.ReplayKeyed })
	read := agenttool.New("read", "", func(context.Context, echoArgs) (string, error) {
		mu.Lock()
		reads++
		mu.Unlock()
		return "the contents", nil
	})
	charge := agenttool.New("charge", "", func(ctx context.Context, _ echoArgs) (string, error) {
		call, _ := agenttool.CallFrom(ctx)
		mu.Lock()
		keys = append(keys, call.IdempotencyKey)
		ids = append(ids, call.ID)
		mu.Unlock()
		if blocking.Load() {
			started <- struct{}{}
			<-ctx.Done()
			return "", ctx.Err()
		}
		return "charged", nil
	}, keyed)
	var got []string
	eval := agenttool.New("eval", "", func(ctx context.Context, _ struct{}) (string, error) {
		r, err := agentturn.Invoke(ctx, "read", json.RawMessage(`{"text":"a"}`))
		if err != nil {
			return "", err
		}
		c, err := agentturn.Invoke(ctx, "charge", json.RawMessage(`{"text":"5"}`))
		if err != nil {
			return "", err
		}
		mu.Lock()
		got = append(got, r.Output.Text, c.Output.Text)
		mu.Unlock()
		return "evaluated", nil
	}, safe)
	tools := []agenttool.Tool{eval, read, charge}

	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: evalOnly{}, Tools: tools})
	unsub := rec.Attach(a)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = a.Prompt(context.Background(), openresponses.UserText("go"))
	}()
	<-started
	// The crash: nothing more reaches the record.
	unsub()
	a.Abort()
	<-done
	blocking.Store(false)

	rec2, s2, err := Resume(context.Background(), store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	pending, err := Pending(s2)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Reason != agentturn.PendingAborted {
		t.Fatalf("pending = %+v", pending)
	}
	inv := pending[0].Invoked
	if len(inv) != 2 {
		t.Fatalf("invoked = %+v", inv)
	}
	if r := inv[0]; r.Name != "read" || !r.Dispatched || !r.Ended || r.Result.Output.Text != "the contents" || r.IdempotencyKey == "" {
		t.Errorf("read = %+v", r)
	}
	if c := inv[1]; c.Name != "charge" || !c.Dispatched || c.Ended || c.IdempotencyKey != keys[0] || c.CallID != ids[0] {
		t.Errorf("charge = %+v, want dispatched under %q as %s", c, keys[0], ids[0])
	}

	answers, err := ReplayAnswers(context.Background(), s2, tools)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := AgentOptions(s2)
	if err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(agentturn.Config{Model: evalOnly{}, Tools: tools}, opts...)
	defer rec2.Attach(b)()
	end, err := b.Resume(context.Background(), answers...)
	if err != nil || end.Reason != agentturn.ReasonDone {
		t.Fatalf("resume: err=%v end=%+v", err, end)
	}
	mu.Lock()
	defer mu.Unlock()
	if reads != 1 {
		t.Errorf("read ran %d times, want once", reads)
	}
	if len(keys) != 2 || keys[1] != keys[0] || ids[1] != ids[0] {
		t.Errorf("charge ran under keys %q as %q, want the first twice", keys, ids)
	}
	if len(got) != 2 || got[0] != "the contents" || got[1] != "charged" {
		t.Errorf("the invoking tool got %q", got)
	}

	// The second execution's record: read replayed, with no dispatch;
	// charge dispatched again under its key.
	var second []NestedCall
	seen := 0
	for _, e := range s2.Path(s2.Leaf()) {
		if d, ok := e.(*agentsession.DispatchEntry); ok && d.CallID == pending[0].Call.CallID {
			seen++
			continue
		}
		c, ok := e.(*agentsession.CustomEntry)
		if !ok || c.NS != NestedCallNS || seen < 2 {
			continue
		}
		var n NestedCall
		if err := json.Unmarshal(c.Data, &n); err != nil {
			t.Fatal(err)
		}
		second = append(second, n)
	}
	want := []struct {
		name, phase string
		replayed    bool
	}{
		{"read", agentsession.RunStart, true},
		{"read", agentsession.RunEnd, true},
		{"charge", agentsession.RunStart, false},
		{"charge", NestedDispatch, false},
		{"charge", agentsession.RunEnd, false},
	}
	if len(second) != len(want) {
		t.Fatalf("second execution's entries = %+v", second)
	}
	for i, w := range want {
		n := second[i]
		if n.Name != w.name || n.Phase != w.phase || n.Replayed != w.replayed {
			t.Errorf("entry %d = %+v, want %s %s replayed=%v", i, n, w.name, w.phase, w.replayed)
		}
	}
	if second[3].IdempotencyKey != keys[0] {
		t.Errorf("charge dispatched again under %q, want %q", second[3].IdempotencyKey, keys[0])
	}
	verifyAll(t, s2)
}
