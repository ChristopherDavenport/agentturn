package session

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// callsRunCode calls run_code once and answers with text once it has
// its output.
type callsRunCode struct{}

func (callsRunCode) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
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
	w, err := em.FunctionCall("", "run_code")
	if err != nil {
		return err
	}
	if err := w.Arguments(`{"text":"x"}`); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Complete()
}

// TestANestedCallMadeAgainAfterARestartKeepsItsIDAndKey pins RFC 0001's
// Nested calls rule (#261): a recorder writes each nested call's key
// at its hand-off, and Pending reads the nested calls under a pending
// call, so when the call's tool runs again after a restart and invokes
// them again, one that ended returns its recorded result without
// running, and one handed to its tool that never ended runs again
// under its recorded ID and key, as its keyed tool's replay allows.
func TestANestedCallMadeAgainAfterARestartKeepsItsIDAndKey(t *testing.T) {
	ctx := context.Background()
	store := &cutStore{Store: agentsession.NewMemoryStore()}
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	reads := 0
	var charges []string
	tools := []agenttool.Tool{
		agenttool.New("run_code", "", func(ctx context.Context, _ echoArgs) (string, error) {
			got, err := agentturn.Invoke(ctx, "read", json.RawMessage(`{"text":"a"}`))
			if err != nil {
				return "", err
			}
			if _, err := agentturn.Invoke(ctx, "charge", json.RawMessage(`{"text":"5"}`)); err != nil {
				return "", err
			}
			return "read " + got.Output.Text, nil
		}, agenttool.WithReplay(func(context.Context, json.RawMessage) agenttool.Replay { return agenttool.ReplaySafe })),
		agenttool.New("read", "", func(context.Context, echoArgs) (string, error) {
			mu.Lock()
			defer mu.Unlock()
			reads++
			return "contents", nil
		}),
		agenttool.New("charge", "", func(ctx context.Context, _ echoArgs) (string, error) {
			call, _ := agenttool.CallFrom(ctx)
			mu.Lock()
			charges = append(charges, call.IdempotencyKey)
			first := len(charges) == 1
			mu.Unlock()
			if first {
				store.cut.Store(true) // the process dies while charge runs
			}
			return "charged", nil
		}, agenttool.WithReplay(func(context.Context, json.RawMessage) agenttool.Replay { return agenttool.ReplayKeyed })),
	}
	cfg := agentturn.Config{Model: callsRunCode{}, ModelName: "m", MaxTurns: 2, Tools: tools}
	a := agentturn.New(cfg)
	unsub := rec.Attach(a)
	if end, err := a.Prompt(ctx, openresponses.UserText("go")); err == nil {
		t.Fatalf("the run survived the crash: %+v", end)
	}
	unsub()
	store.cut.Store(false)

	rec, s, err = Resume(ctx, store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	pending, err := Pending(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || len(pending[0].Nested) != 2 {
		t.Errorf("pending = %+v, want run_code with its two nested calls", pending)
	}
	answers, err := ReplayAnswers(ctx, s, tools)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := AgentOptions(s)
	if err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(cfg, opts...)
	defer rec.Attach(b)()
	var replayed []string
	b.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		if e, ok := ev.(*agentturn.ToolEnd); ok && e.Replayed {
			replayed = append(replayed, e.Name)
		}
		return nil
	})
	end, err := b.Resume(ctx, answers...)
	if err != nil {
		t.Fatal(err)
	}
	if end.Reason != agentturn.ReasonDone {
		t.Fatalf("resume ended %s: %v", end.Reason, end.Err)
	}

	if reads != 1 {
		t.Errorf("read ran %d times, want once: the ended nested call is given its recorded result", reads)
	}
	if len(replayed) != 1 || replayed[0] != "read" {
		t.Errorf("replayed = %v, want read", replayed)
	}
	if len(charges) != 2 || charges[0] == "" || charges[1] != charges[0] {
		t.Errorf("charge ran under keys %q, want twice under one key", charges)
	}
	if out := callsOf(t, s)["run_code"]; out == nil || out.Output == nil || out.Output.Item.(*openresponses.FunctionCallOutput).Output.Text != "read contents" {
		t.Errorf("run_code = %+v, want the recorded read in its output", out)
	}
	// Each nested call is one ID on the record, whichever run made it.
	ids := map[string]map[string]bool{}
	for _, e := range s.Path(s.Leaf()) {
		c, ok := e.(*agentsession.CustomEntry)
		if !ok || c.NS != NestedCallNS {
			continue
		}
		var n NestedCall
		if err := json.Unmarshal(c.Data, &n); err != nil {
			t.Fatal(err)
		}
		if ids[n.Name] == nil {
			ids[n.Name] = map[string]bool{}
		}
		ids[n.Name][n.CallID] = true
	}
	if len(ids["read"]) != 1 || len(ids["charge"]) != 1 {
		t.Errorf("nested call IDs = %v, want one each", ids)
	}
	verifyAll(t, s)
}
