package session

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
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

// linkCutStore is a process that dies as it writes its first
// subsession link: that append and every one after it fail.
type linkCutStore struct {
	agentsession.Store
	cut, died atomic.Bool
}

func (s *linkCutStore) Append(ctx context.Context, id string, e agentsession.Entry) (string, error) {
	if l, ok := e.(*agentsession.LinkEntry); ok && l.Rel == agentsession.RelSubsession && !s.died.Swap(true) {
		s.cut.Store(true)
	}
	if s.cut.Load() {
		return "", errors.New("the process died")
	}
	return s.Store.Append(ctx, id, e)
}

// TestACallRunAgainWithNoChildOnTheRecordIsNotRunAfresh is l6-3e's
// M2: a call run again under the key of a hand-off that may have
// started its child, when the record holds no child for it, fails with
// the outcome unknown rather than start the child afresh, which would
// run the child's tools a second time. The record holds none when the
// recorder keeps no child sessions, and when the process stopped after
// the child started and before the parent's link was written.
func TestACallRunAgainWithNoChildOnTheRecordIsNotRunAfresh(t *testing.T) {
	cases := []struct {
		name  string
		store interface {
			agentsession.Store
			Die()
			Live()
		}
		opts []Option
	}{
		{name: "without child sessions", store: &dyingStore{cutStore: &cutStore{Store: agentsession.NewMemoryStore()}}, opts: []Option{WithoutChildSessions()}},
		{name: "cut before the link", store: &dyingLinkStore{linkCutStore: &linkCutStore{Store: agentsession.NewMemoryStore()}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := tc.store
			rec, s, err := Start(ctx, store, agentsession.Header{}, tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			lookups := 0
			lookup := agenttool.New("lookup", "", func(context.Context, echoArgs) (string, error) {
				mu.Lock()
				defer mu.Unlock()
				lookups++
				return "FOUND", nil
			})
			childCfg := agentturn.Config{Name: "child", Description: "looks up", Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{lookup}}
			child := func(rec *Recorder) agenttool.Tool {
				return agent.New(childCfg, agent.WithObserver(rec.Observe), agent.WithRunContext(rec.ChildContext), agent.WithRecord(rec.ChildRecord))
			}
			parentCfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{child(rec)},
				AfterToolCall: func(context.Context, agentturn.ToolResultInfo) (*agentturn.ToolOverride, error) {
					// The process dies once the child has answered, if
					// it has not already.
					store.Die()
					return nil, nil
				}}
			a := agentturn.New(parentCfg)
			unsub := rec.Attach(a)
			_, _ = a.Prompt(ctx, openresponses.UserText("find"))
			unsub()
			store.Live()
			if lookups != 1 {
				t.Fatalf("lookup ran %d times before the crash, want once", lookups)
			}

			rec, s, err = Resume(ctx, store, s.ID(), tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			parentCfg.Tools, parentCfg.AfterToolCall = []agenttool.Tool{child(rec)}, nil
			answers, err := ReplayAnswers(ctx, s, parentCfg.Tools)
			if err != nil {
				t.Fatal(err)
			}
			if len(answers) != 1 || answers[0].Reason != agentturn.RunAgainKeyedReason {
				t.Fatalf("answers = %+v, want the call run again under its key", answers)
			}
			opts, err := AgentOptions(s)
			if err != nil {
				t.Fatal(err)
			}
			b := agentturn.New(parentCfg, opts...)
			defer rec.Attach(b)()
			end, err := b.Resume(ctx, answers...)
			if err != nil {
				t.Fatal(err)
			}
			if lookups != 1 {
				t.Errorf("lookup ran %d times, want once: the child was started afresh", lookups)
			}
			var out string
			for _, item := range end.Items {
				if o, ok := item.(*openresponses.FunctionCallOutput); ok && o.CallID == answers[0].CallID {
					out = o.Output.String()
				}
			}
			if !strings.Contains(out, "outcome unknown") {
				t.Errorf("the call's output = %q, want the outcome unknown", out)
			}
		})
	}
}

// dyingStore and dyingLinkStore give the two stores one way to die
// and come back.
type dyingStore struct{ *cutStore }

func (s *dyingStore) Die()  { s.cut.Store(true) }
func (s *dyingStore) Live() { s.cut.Store(false) }

type dyingLinkStore struct{ *linkCutStore }

func (s *dyingLinkStore) Die()  { s.cut.Store(true) }
func (s *dyingLinkStore) Live() { s.cut.Store(false) }

// steeredChild calls lookup once, and answers "took X" once a user
// message "also X" is in its input, "no X" otherwise.
type steeredChild struct{}

func (steeredChild) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	steered, answered := false, false
	for _, item := range req.Input {
		switch v := item.(type) {
		case *openresponses.Message:
			steered = steered || v.Role == openresponses.RoleUser && v.Text() == "also X"
		case *openresponses.FunctionCallOutput:
			answered = true
		}
	}
	if !steered && !answered {
		w, err := em.FunctionCall("", "lookup")
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
	text := "no X"
	if steered {
		text = "took X"
	}
	w, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if err := w.Text(text); err != nil {
		return err
	}
	return em.Complete()
}

// TestAChildsOwedInputIsHandedBackToIt is l6-3e's M3: an input the
// child accepted and had not taken when the process stopped is owed on
// the child's record, as RFC 0001 Queues says a recorder hands back the
// inputs it owes. A call run again under its key that continues the
// child hands the input back to it, so the continued run takes it,
// under the entry the record already holds, rather than leave it owed
// and drop it at the run's end.
func TestAChildsOwedInputIsHandedBackToIt(t *testing.T) {
	ctx := context.Background()
	store := &cutStore{Store: agentsession.NewMemoryStore()}
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	var spawned *agentturn.Agent
	var steer atomic.Bool
	steer.Store(true)
	lookup := agenttool.New("lookup", "", func(context.Context, echoArgs) (string, error) {
		if steer.Swap(false) {
			// A person steers the child while its tool runs.
			spawned.Steer(openresponses.UserText("also X"))
		}
		return "FOUND", nil
	})
	childCfg := agentturn.Config{Name: "child", Description: "looks up", Model: steeredChild{}, ModelName: "m", Tools: []agenttool.Tool{lookup}}
	var armed atomic.Bool
	child := func(rec *Recorder) agenttool.Tool {
		return agent.New(childCfg, agent.WithRunContext(rec.ChildContext), agent.WithRecord(rec.ChildRecord),
			agent.WithSpawn(func(_ string, a *agentturn.Agent) { spawned = a }),
			agent.WithObserver(func(ctx context.Context, ev agentturn.Event) {
				rec.Observe(ctx, ev)
				if _, ok := ev.(*agentturn.ToolEnd); ok && armed.Swap(false) {
					// The process dies once the steer's queued entry is
					// written and before the turn that would take it.
					store.cut.Store(true)
				}
			}))
	}
	armed.Store(true)
	parentCfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{child(rec)}}
	a := agentturn.New(parentCfg)
	unsub := rec.Attach(a)
	_, _ = a.Prompt(ctx, openresponses.UserText("find"))
	unsub()
	store.cut.Store(false)

	rec, s, err = Resume(ctx, store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	parentCfg.Tools = []agenttool.Tool{child(rec)}
	answers, err := ReplayAnswers(ctx, s, parentCfg.Tools)
	if err != nil {
		t.Fatal(err)
	}
	if len(answers) != 1 || answers[0].Reason != agentturn.RunAgainKeyedReason {
		t.Fatalf("answers = %+v, want the call run again under its key", answers)
	}
	opts, err := AgentOptions(s)
	if err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(parentCfg, opts...)
	defer rec.Attach(b)()
	end, err := b.Resume(ctx, answers...)
	if err != nil {
		t.Fatal(err)
	}
	if text, _ := end.Answer(); text != "Tool result: took X" {
		t.Errorf("the parent read %q, want the child to have taken its owed steer", text)
	}

	cs, err := store.Open(ctx, agentsession.SubsessionID(s.ID(), answers[0].CallID))
	if err != nil {
		t.Fatal(err)
	}
	var queued []string
	drained := ""
	for _, e := range cs.Entries() {
		switch v := e.(type) {
		case *agentsession.QueuedEntry:
			queued = append(queued, v.ID)
		case *agentsession.ItemEntry:
			if m, ok := v.Item.(*openresponses.Message); ok && m.Text() == "also X" {
				drained = v.QueuedFrom
			}
		}
	}
	if len(queued) != 2 || drained != queued[1] {
		t.Errorf("queued entries %v, the steer drained from %q: want it accepted, queued again at the cut, and taken from that entry", queued, drained)
	}
	if owed, err := cs.PendingQueued(cs.Leaf()); err != nil || len(owed) != 0 {
		t.Errorf("the child still owes %d inputs (%v), want none", len(owed), err)
	}
	verifyAll(t, cs)
}
