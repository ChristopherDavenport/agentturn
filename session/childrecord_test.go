package session

import (
	"context"
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

// TestAChildsOpenQuestionIsAskedAgainAfterARestart is #297: a parent's
// call to a tools/agent child whose own call was put to the parent's
// elicitor (agent.WithAskParent) is cut by a crash while the question
// is open. After the restart the parent's call, in flight, is run
// again under its dispatch's key, since the tool reports itself keyed;
// the child continues its session from the record rather than starting
// afresh on a new root; its undecided call goes back to its decision
// hook, so the same question is asked again under the parent's call;
// an accept runs the child's call once; and the parent's model reads
// the child's answer.
func TestAChildsOpenQuestionIsAskedAgainAfterARestart(t *testing.T) {
	ctx := context.Background()
	store := &cutStore{Store: agentsession.NewMemoryStore()}
	rec, s, err := Start(ctx, store, agentsession.Header{})
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
	childCfg := agentturn.Config{Name: "child", Description: "looks up", Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{lookup},
		BeforeToolCall: func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "a person approves lookups", By: agentsession.ByPolicy}, nil
		}}
	child := func(rec *Recorder) agenttool.Tool {
		return agent.New(childCfg, agent.WithAskParent(), agent.WithObserver(rec.Observe),
			agent.WithRunContext(rec.ChildContext), agent.WithRecord(rec.ChildRecord))
	}
	type question struct{ under, about string }
	var asked []question
	ask := func(answer func() (agenttool.Answer, error)) agenttool.Elicitor {
		return func(ctx context.Context, _ agenttool.Elicitation) (agenttool.Answer, error) {
			about, _ := agentturn.AskedCallFrom(ctx)
			under, _ := agenttool.CallFrom(ctx)
			asked = append(asked, question{under: under.ID, about: about.CallID})
			return answer()
		}
	}

	// The process dies while the person decides.
	parentCfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{child(rec)},
		ToolElicitor: rec.Elicitor("human", ask(func() (agenttool.Answer, error) {
			store.cut.Store(true)
			return agenttool.Answer{}, errors.New("the process died")
		}))}
	a := agentturn.New(parentCfg)
	unsub := rec.Attach(a)
	_, _ = a.Prompt(ctx, openresponses.UserText("find"))
	unsub()
	store.cut.Store(false)
	if len(asked) != 1 || asked[0].under == "" || asked[0].about == "" {
		t.Fatalf("asked %+v before the crash, want one question", asked)
	}
	first := asked[0]
	asked = nil

	rec, s, err = Resume(ctx, store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	tools := []agenttool.Tool{child(rec)}
	answers, err := ReplayAnswers(ctx, s, tools)
	if err != nil {
		t.Fatal(err)
	}
	if len(answers) != 1 || answers[0].CallID != first.under || answers[0].Output != nil || answers[0].Reason != agentturn.RunAgainKeyedReason {
		t.Fatalf("answers = %+v, want the parent's call run again under its key", answers)
	}
	opts, err := AgentOptions(s)
	if err != nil {
		t.Fatal(err)
	}
	parentCfg.Tools = tools
	parentCfg.ToolElicitor = rec.Elicitor("human", ask(func() (agenttool.Answer, error) {
		return agenttool.Answer{Action: agenttool.ActionAccept}, nil
	}))
	b := agentturn.New(parentCfg, opts...)
	defer rec.Attach(b)()
	end, err := b.Resume(ctx, answers...)
	if err != nil {
		t.Fatal(err)
	}
	if end.Reason != agentturn.ReasonDone {
		t.Fatalf("resume ended %s: %v", end.Reason, end.Err)
	}
	if len(asked) != 1 || asked[0] != first {
		t.Errorf("asked %+v after the restart, want the question %+v again", asked, first)
	}
	if lookups != 1 {
		t.Errorf("lookup ran %d times, want once", lookups)
	}
	if text, _ := end.Answer(); text != "Tool result: Tool result: FOUND" {
		t.Errorf("the parent's model answered %q, want it to read the child's answer", text)
	}

	cs, err := store.Open(ctx, agentsession.SubsessionID(s.ID(), first.under))
	if err != nil {
		t.Fatal(err)
	}
	calls := callsOf(t, cs)
	if c := calls["lookup"]; c == nil || c.ID() != first.about || c.Output == nil {
		t.Errorf("the child's lookup at its leaf = %+v, want the recorded call %s answered", c, first.about)
	}
	roots := 0
	for _, e := range cs.Entries() {
		if e.Base().Parent == "" {
			roots++
		}
	}
	if roots != 1 {
		t.Errorf("the child's session has %d roots, want the one it continued", roots)
	}
	runs, err := cs.Runs(cs.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[0].End == nil || runs[0].End.Reason != agentsession.ReasonError {
		t.Errorf("the child's runs = %d, want the cut one closed before the one that went on", len(runs))
	}
	verifyAll(t, cs)
	ps, err := store.Open(ctx, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	verifyAll(t, ps)
}

// TestAChildThatAnsweredIsNotRunAgainAfterARestart is #297's other
// side: the process stops after the child answered and before the
// parent's output for the call was written. The call run again under
// its key returns the answer the child's record holds, running
// nothing, and the child's session gets no run of its own for it.
func TestAChildThatAnsweredIsNotRunAgainAfterARestart(t *testing.T) {
	ctx := context.Background()
	store := &cutStore{Store: agentsession.NewMemoryStore()}
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	lookups := 0
	lookup := agenttool.New("lookup", "", func(context.Context, echoArgs) (string, error) {
		lookups++
		return "FOUND", nil
	})
	childCfg := agentturn.Config{Name: "child", Description: "looks up", Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{lookup}}
	child := func(rec *Recorder) agenttool.Tool {
		return agent.New(childCfg, agent.WithObserver(rec.Observe), agent.WithRunContext(rec.ChildContext), agent.WithRecord(rec.ChildRecord))
	}
	callID := ""
	parentCfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{child(rec)},
		AfterToolCall: func(_ context.Context, info agentturn.ToolResultInfo) (*agentturn.ToolOverride, error) {
			// The process dies once the child has answered.
			callID = info.Call.CallID
			store.cut.Store(true)
			return nil, nil
		}}
	a := agentturn.New(parentCfg)
	unsub := rec.Attach(a)
	_, _ = a.Prompt(ctx, openresponses.UserText("find"))
	unsub()
	store.cut.Store(false)
	if lookups != 1 || callID == "" {
		t.Fatalf("lookup ran %d times under %q before the crash, want once", lookups, callID)
	}

	rec, s, err = Resume(ctx, store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	parentCfg.Tools, parentCfg.AfterToolCall = []agenttool.Tool{child(rec)}, nil
	answers, err := ReplayAnswers(ctx, s, parentCfg.Tools)
	if err != nil {
		t.Fatal(err)
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
	if text, _ := end.Answer(); end.Reason != agentturn.ReasonDone || text != "Tool result: Tool result: FOUND" {
		t.Errorf("resume ended %s answering %q, want the child's recorded answer read", end.Reason, text)
	}
	if lookups != 1 {
		t.Errorf("lookup ran %d times, want once", lookups)
	}
	cs, err := store.Open(ctx, agentsession.SubsessionID(s.ID(), callID))
	if err != nil {
		t.Fatal(err)
	}
	runs, err := cs.Runs(cs.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Errorf("the child's session has %d runs, want the one that answered", len(runs))
	}
	verifyAll(t, cs)
}
