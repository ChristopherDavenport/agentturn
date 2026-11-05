package session

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// nestedChildCallID returns the ID of the nested call to the tool
// name on s's path: the call a tool made through agentturn.Invoke.
func nestedChildCallID(t *testing.T, s *agentsession.Session, name string) string {
	t.Helper()
	for _, e := range s.Path(s.Leaf()) {
		c, ok := e.(*agentsession.CustomEntry)
		if !ok || c.NS != NestedCallNS {
			continue
		}
		var n NestedCall
		if err := json.Unmarshal(c.Data, &n); err != nil {
			t.Fatal(err)
		}
		if n.Name == name {
			return n.CallID
		}
	}
	t.Fatalf("no nested call to %s on the path", name)
	return ""
}

// invokesChild is run_code: a tool that calls the tools/agent child
// through agentturn.Invoke, a nested call, and returns what it gave or
// the error it failed with.
func invokesChild() agenttool.Tool {
	return agenttool.New("run_code", "", func(ctx context.Context, _ echoArgs) (string, error) {
		got, err := agentturn.Invoke(ctx, "child", json.RawMessage(`{"input":"find"}`))
		if err != nil {
			return "failed: " + err.Error(), nil
		}
		return "child said " + got.Output.String(), nil
	}, agenttool.WithReplay(func(context.Context, json.RawMessage) agenttool.Replay { return agenttool.ReplaySafe }))
}

// TestANestedChildsOpenQuestionIsAskedAgainAfterARestart is #297 for a
// nested call: a tool's own call to a tools/agent child, made through
// agentturn.Invoke, whose child put its call to the parent's elicitor
// when the process stopped. The tool runs again after the restart and
// makes the nested call again under its recorded ID and key, and the
// child continues its session from the record, as a call the model
// made does, rather than start afresh on a new root.
func TestANestedChildsOpenQuestionIsAskedAgainAfterARestart(t *testing.T) {
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
	tools := func(rec *Recorder) []agenttool.Tool {
		return []agenttool.Tool{invokesChild(), agent.New(childCfg, agent.WithAskParent(), agent.WithObserver(rec.Observe),
			agent.WithRunContext(rec.ChildContext), agent.WithRecord(rec.ChildRecord))}
	}
	var asked []string
	ask := func(answer func() (agenttool.Answer, error)) agenttool.Elicitor {
		return func(ctx context.Context, _ agenttool.Elicitation) (agenttool.Answer, error) {
			about, _ := agentturn.AskedCallFrom(ctx)
			asked = append(asked, about.CallID)
			return answer()
		}
	}

	// The process dies while the person decides.
	parentCfg := agentturn.Config{Model: callsRunCode{}, ModelName: "m", MaxTurns: 2, Tools: tools(rec),
		ToolElicitor: rec.Elicitor("human", ask(func() (agenttool.Answer, error) {
			store.cut.Store(true)
			return agenttool.Answer{}, errors.New("the process died")
		}))}
	a := agentturn.New(parentCfg)
	unsub := rec.Attach(a)
	_, _ = a.Prompt(ctx, openresponses.UserText("go"))
	unsub()
	store.cut.Store(false)
	if len(asked) != 1 || asked[0] == "" {
		t.Fatalf("asked %v before the crash, want one question", asked)
	}
	first := asked[0]
	asked = nil

	rec, s, err = Resume(ctx, store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	nested := nestedChildCallID(t, s, "child")
	parentCfg.Tools = tools(rec)
	answers, err := ReplayAnswers(ctx, s, parentCfg.Tools)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := AgentOptions(s)
	if err != nil {
		t.Fatal(err)
	}
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
		t.Errorf("asked %v after the restart, want the question about %s again", asked, first)
	}
	if lookups != 1 {
		t.Errorf("lookup ran %d times, want once", lookups)
	}
	if out := callsOf(t, s)["run_code"]; out == nil || out.Output == nil || !strings.Contains(out.Output.Item.(*openresponses.FunctionCallOutput).Output.String(), "FOUND") {
		t.Errorf("run_code = %+v, want the child's answer in its output", out)
	}

	cs, err := store.Open(ctx, agentsession.SubsessionID(s.ID(), nested))
	if err != nil {
		t.Fatal(err)
	}
	if c := callsOf(t, cs)["lookup"]; c == nil || c.ID() != first || c.Output == nil {
		t.Errorf("the child's lookup at its leaf = %+v, want the recorded call %s answered", c, first)
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

// TestANestedCallRunAgainWithNoChildOnTheRecordIsNotRunAfresh is l6-3e's
// M2 for a nested call: the nested call to a tools/agent child made
// again under the key of a hand-off that may have started its child,
// when the record holds no child for it, fails with the outcome unknown
// rather than run the child's tools a second time.
func TestANestedCallRunAgainWithNoChildOnTheRecordIsNotRunAfresh(t *testing.T) {
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
				if lookups == 1 {
					// The process dies as the child's tool first runs.
					store.Die()
				}
				return "FOUND", nil
			})
			childCfg := agentturn.Config{Name: "child", Description: "looks up", Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{lookup}}
			tools := func(rec *Recorder) []agenttool.Tool {
				return []agenttool.Tool{invokesChild(), agent.New(childCfg, agent.WithObserver(rec.Observe),
					agent.WithRunContext(rec.ChildContext), agent.WithRecord(rec.ChildRecord))}
			}
			parentCfg := agentturn.Config{Model: callsRunCode{}, ModelName: "m", MaxTurns: 2, Tools: tools(rec)}
			a := agentturn.New(parentCfg)
			unsub := rec.Attach(a)
			_, _ = a.Prompt(ctx, openresponses.UserText("go"))
			unsub()
			store.Live()
			if lookups != 1 {
				t.Fatalf("lookup ran %d times before the crash, want once", lookups)
			}

			rec, s, err = Resume(ctx, store, s.ID(), tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			parentCfg.Tools = tools(rec)
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
			if lookups != 1 {
				t.Errorf("lookup ran %d times, want once: the nested child was started afresh", lookups)
			}
			var out string
			for _, item := range end.Items {
				if o, ok := item.(*openresponses.FunctionCallOutput); ok {
					out = o.Output.String()
				}
			}
			if !strings.Contains(out, "outcome unknown") {
				t.Errorf("run_code's output = %q, want the nested call's outcome unknown", out)
			}
		})
	}
}
