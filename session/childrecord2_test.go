package session

import (
	"context"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// reusesCallID calls tool once a turn under the one call ID id, with
// the input each of inputs gives in turn, and answers "done" once
// every one has its output: a provider that numbers its calls afresh
// in each response.
type reusesCallID struct {
	id, tool string
	inputs   []string
}

func (m reusesCallID) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	outputs := 0
	for _, item := range req.Input {
		if _, ok := item.(*openresponses.FunctionCallOutput); ok {
			outputs++
		}
	}
	if outputs < len(m.inputs) {
		w, err := em.FunctionCall(m.id, m.tool)
		if err != nil {
			return err
		}
		if err := w.Arguments(`{"input":"` + m.inputs[outputs] + `"}`); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		return em.Complete()
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

// TestACallIDTheModelReusesStartsAFreshChild is l6-3e's M1: a provider
// that reuses call_0 for a second call in the same run must not have
// the second call continue, or return the end of, the child the first
// one started. The loop gives the second call an ID of its own, since
// the run's transcript holds the first (a fold leaves the working
// transcript whole), so its key differs and the loader finds no child
// under it; and a session refuses a call ID twice
// (agentsession.ErrCallIDRepeated), so the record cannot name two
// calls with one key either.
func TestACallIDTheModelReusesStartsAFreshChild(t *testing.T) {
	ctx := context.Background()
	rec, s, err := Start(ctx, agentsession.NewMemoryStore(), agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	loaded := 0
	load := func(ctx context.Context, call agenttool.Call, cfg agentturn.Config) (*agent.Record, error) {
		r, err := rec.ChildRecord(ctx, call, cfg)
		if r != nil {
			loaded++
		}
		return r, err
	}
	childCfg := agentturn.Config{Name: "child", Description: "echoes", Model: &echo.Adapter{}, ModelName: "m"}
	tool := agent.New(childCfg, agent.WithObserver(rec.Observe), agent.WithRunContext(rec.ChildContext), agent.WithRecord(load))
	a := agentturn.New(agentturn.Config{Model: reusesCallID{id: "call_0", tool: "child", inputs: []string{"one", "two"}}, ModelName: "m", Tools: []agenttool.Tool{tool}})
	defer rec.Attach(a)()
	end, err := a.Prompt(ctx, openresponses.UserText("go"))
	if err != nil {
		t.Fatal(err)
	}
	if end.Reason != agentturn.ReasonDone {
		t.Fatalf("run ended %s: %v", end.Reason, end.Err)
	}
	var ids, outputs []string
	for _, item := range end.Items {
		switch v := item.(type) {
		case *openresponses.FunctionCall:
			ids = append(ids, v.CallID)
		case *openresponses.FunctionCallOutput:
			outputs = append(outputs, v.Output.String())
		}
	}
	if len(ids) != 2 || ids[0] == ids[1] {
		t.Fatalf("call IDs = %v, want two calls told apart", ids)
	}
	if len(outputs) != 2 || outputs[0] != "one" || outputs[1] != "two" {
		t.Errorf("outputs = %q, want each call's own child's answer", outputs)
	}
	if loaded != 0 {
		t.Errorf("the loader returned %d records, want none for first calls", loaded)
	}
	for _, id := range ids {
		cs, err := rec.Store().Open(ctx, agentsession.SubsessionID(s.ID(), id))
		if err != nil {
			t.Fatal(err)
		}
		if runs, err := cs.Runs(cs.Leaf()); err != nil || len(runs) != 1 {
			t.Errorf("child of %s has %d runs (%v), want its own one", id, len(runs), err)
		}
	}
}
