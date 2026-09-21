package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentsession/jsonl"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/compact"
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// allCalls is a model that calls every tool it is offered once, then
// answers with text once the outputs are in.
type allCalls struct{}

func (allCalls) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if _, ok := req.Input[len(req.Input)-1].(*openresponses.FunctionCallOutput); ok || len(req.Tools) == 0 {
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
	for _, tl := range req.Tools {
		ft := tl.(*openresponses.FunctionTool)
		w, err := em.FunctionCall("", ft.Name)
		if err != nil {
			return err
		}
		if err := w.Arguments(`{"text":"t"}`); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
	}
	return em.Complete()
}

func runsOf(t *testing.T, s *agentsession.Session) []*agentsession.Run {
	t.Helper()
	runs, err := s.Runs(s.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	return runs
}

func callsOf(t *testing.T, s *agentsession.Session) map[string]*agentsession.Call {
	t.Helper()
	calls, err := s.Calls(s.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*agentsession.Call{}
	for _, c := range calls {
		out[c.Call.Name] = c
	}
	return out
}

func TestRunEntriesCarrySourceTriggerAndReason(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	if h := s.Header(); strings.Join(h.Records, ",") != "run,dispatch,decision" {
		t.Errorf("header records = %v", h.Records)
	}
	deferAll := func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
		return &agentturn.ToolDecision{Action: agentturn.Defer, By: "human"}, nil
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{upper}, BeforeToolCall: deferAll})
	defer rec.Attach(a)()
	ctx := agentturn.ContextWithTrigger(context.Background(), agentturn.Trigger{Kind: "cron", Ref: "nightly"})
	end, err := a.Prompt(ctx, openresponses.UserText("abc"))
	if err != nil || end.Reason != agentturn.ReasonInputRequired {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	callID := end.Pending[0].Call.CallID
	// The refusal is an output the caller wrote: a reject, then the
	// output, and the run that carries them is a resume.
	if _, err := a.Resume(context.Background(), agentturn.Output(openresponses.NewFunctionCallOutput(callID, "no, not now"))); err != nil {
		t.Fatal(err)
	}
	verifyAll(t, s)
	runs := runsOf(t, s)
	if len(runs) != 2 {
		t.Fatalf("runs = %d", len(runs))
	}
	first, second := runs[0], runs[1]
	if first.Start.Source != agentsession.SourceInput || first.Start.Ref != "cron:nightly" || first.End == nil || first.End.Reason != agentsession.ReasonInputRequired || strings.Join(first.End.Pending, ",") != callID {
		t.Errorf("first run = %+v end %+v", first.Start, first.End)
	}
	if second.Start.Source != agentsession.SourceResume || second.Start.Ref != "" || second.End == nil || second.End.Reason != agentsession.ReasonDone || len(second.End.Pending) != 0 {
		t.Errorf("second run = %+v end %+v", second.Start, second.End)
	}
	c := callsOf(t, s)["upper"]
	if c == nil || len(c.Decisions) != 2 || c.Decisions[0].Verdict != agentsession.VerdictHold || c.Decisions[0].By != "human" ||
		c.Decisions[1].Verdict != agentsession.VerdictReject || c.Decisions[1].Reason != "no, not now" || c.Dispatch != nil || c.Output == nil {
		t.Errorf("call = %+v", c)
	}
	if got := c.State(s.Header()); got != agentsession.CallCompleted {
		t.Errorf("state = %v", got)
	}
}

func TestRunEndReasonsFollowTheCascade(t *testing.T) {
	cases := []struct {
		name   string
		cfg    func() agentturn.Config
		wantR  string
		refHas string
	}{
		{"error", func() agentturn.Config { return agentturn.Config{Model: failingModel{}} }, agentsession.ReasonError, "model unavailable"},
		{"stopped by a turn budget with calls", func() agentturn.Config {
			return agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{upper}, MaxTurns: 1}
		}, agentsession.ReasonStopped, ""},
		{"stopped by a guard on a final turn reads as done", func() agentturn.Config {
			return agentturn.Config{Model: &echo.Adapter{}, ShouldStopAfterTurn: func(context.Context, agentturn.TurnInfo) (bool, error) { return true, nil }}
		}, agentsession.ReasonDone, "hook"},
		{"stopped by a guard error reads as done with the guard as ref", func() agentturn.Config {
			return agentturn.Config{Model: &echo.Adapter{}, ShouldStopAfterTurn: func(context.Context, agentturn.TurnInfo) (bool, error) {
				return false, fmt.Errorf("%w: phone number", agentturn.ErrGuard)
			}}
		}, agentsession.ReasonDone, "guard"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(tc.cfg())
			defer rec.Attach(a)()
			_, _ = a.Prompt(context.Background(), openresponses.UserText("abc"))
			verifyAll(t, s)
			runs := runsOf(t, s)
			if len(runs) != 1 || runs[0].End == nil {
				t.Fatalf("runs = %+v", runs)
			}
			if end := runs[0].End; end.Reason != tc.wantR || !strings.Contains(end.Ref, tc.refHas) {
				t.Errorf("end = %+v", end)
			}
		})
	}

	// A resume whose approved batch terminates makes no model call, and
	// the format reads such a segment as aborted; the loop's reason
	// rides as ref.
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	terminating := agenttool.New("stop", "", func(_ context.Context, _ echoArgs) (string, error) { return "halt", nil })
	stopTool := agenttool.Tool(terminatingTool{terminating})
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{stopTool},
		BeforeToolCall: func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			return &agentturn.ToolDecision{Action: agentturn.Defer}, nil
		}})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	end, err := a.Resume(context.Background(), agentturn.Approve(a.State().Pending[0].Call.CallID))
	if err != nil || end.Reason != agentturn.ReasonStopped {
		t.Fatalf("resume: err=%v end=%+v", err, end)
	}
	verifyAll(t, s)
	runs := runsOf(t, s)
	if last := runs[len(runs)-1]; last.End == nil || last.End.Reason != agentsession.ReasonAborted || last.End.Ref != "terminate" {
		t.Errorf("terminating resume end = %+v", last.End)
	}
}

// terminatingTool wraps a tool so its result terminates the batch.
type terminatingTool struct{ agenttool.Tool }

func (t terminatingTool) Execute(ctx context.Context, call agenttool.Call) (agenttool.Result, error) {
	res, err := t.Tool.Execute(ctx, call)
	res.Terminate = true
	return res, err
}

func TestAbortRecordsInterruptedAndFinishedCalls(t *testing.T) {
	store := ctxStore{agentsession.NewMemoryStore()}
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	fast := agenttool.New("fast", "", func(_ context.Context, _ echoArgs) (string, error) { return "wrote #1", nil })
	slow := agenttool.New("slow", "", func(ctx context.Context, _ echoArgs) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{fast, slow}})
	defer rec.Attach(a)()
	fastDone := make(chan struct{})
	a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		if e, ok := ev.(*agentturn.ToolEnd); ok && e.Name == "fast" {
			close(fastDone)
		}
		return nil
	})
	done := make(chan *agentturn.RunEnd, 1)
	go func() {
		end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
		done <- end
	}()
	<-fastDone
	a.Abort()
	end := <-done
	if end == nil || end.Reason != agentturn.ReasonAborted || len(end.Pending) != 1 || end.Pending[0].Call.Name != "slow" || end.Pending[0].Reason != agentturn.PendingAborted {
		t.Fatalf("end = %+v", end)
	}
	if got := entryTypes(s); got != "run config item:user item:function_call* item:function_call* response dispatch dispatch item:function_call_output run" {
		t.Errorf("entries = %q", got)
	}
	verifyAll(t, s)
	calls := callsOf(t, s)
	slowID := calls["slow"].ID()
	if st := calls["fast"].State(s.Header()); st != agentsession.CallCompleted {
		t.Errorf("fast state = %v", st)
	}
	if st := calls["slow"].State(s.Header()); st != agentsession.CallInFlight {
		t.Errorf("slow state = %v", st)
	}
	runs := runsOf(t, s)
	if e := runs[0].End; e == nil || e.Reason != agentsession.ReasonInterrupted || strings.Join(e.Pending, ",") != slowID || !strings.Contains(e.Ref, "context canceled") {
		t.Errorf("end = %+v", e)
	}
	// The pending list on the record is what the store says too.
	pending, err := s.PendingCalls(s.Leaf())
	if err != nil || len(pending) != 1 || pending[0].ID() != slowID {
		t.Errorf("pending calls = %v err=%v", pending, err)
	}
	// Answering the cut-off call is a resume; the call was dispatched,
	// so no decision is needed for its output to verify.
	if _, err := a.Resume(context.Background(), agentturn.Output(openresponses.NewFunctionCallOutput(slowID, "outcome unknown"))); err != nil {
		t.Fatal(err)
	}
	verifyAll(t, s)
	runs = runsOf(t, s)
	if len(runs) != 2 || runs[1].Start.Source != agentsession.SourceResume || runs[1].End == nil || runs[1].End.Reason != agentsession.ReasonDone {
		t.Errorf("resume run = %+v", runs[1])
	}
	if st := callsOf(t, s)["slow"].State(s.Header()); st != agentsession.CallCompleted {
		t.Errorf("slow state after resume = %v", st)
	}
}

func TestDecisionsRecordWhatRan(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	rewrite := json.RawMessage(`{"text":"narrowed"}`)
	tool := func(name string) agenttool.Tool {
		return agenttool.New(name, "", func(_ context.Context, a echoArgs) (string, error) { return name + ":" + a.Text, nil })
	}
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{tool("plain"), tool("rewritten"), tool("blocked"), tool("held")},
		BeforeToolCall: func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			switch info.Call.Name {
			case "rewritten":
				return &agentturn.ToolDecision{Args: rewrite}, nil
			case "blocked":
				return &agentturn.ToolDecision{Action: agentturn.Block, Reason: "refused"}, nil
			case "held":
				return &agentturn.ToolDecision{Action: agentturn.Defer}, nil
			}
			return &agentturn.ToolDecision{}, nil
		}})
	defer rec.Attach(a)()
	end, err := a.Prompt(context.Background(), openresponses.UserText("go"))
	if err != nil || end.Reason != agentturn.ReasonInputRequired {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	verifyAll(t, s)
	calls := callsOf(t, s)
	// Nothing to record about an allowed call beyond its dispatch.
	if c := calls["plain"]; len(c.Decisions) != 0 || c.Dispatch == nil || c.Output == nil {
		t.Errorf("plain = %+v", c)
	}
	// A rewrite is a proceed carrying the arguments the tool ran with;
	// the item keeps the model's.
	if c := calls["rewritten"]; len(c.Decisions) != 1 || c.Decisions[0].Verdict != agentsession.VerdictProceed || c.Decisions[0].By != agentsession.ByPolicy ||
		c.Args() != string(rewrite) || c.Call.Arguments == string(rewrite) || c.Dispatch == nil || c.Output.Item.(*openresponses.FunctionCallOutput).Output.Text != "rewritten:narrowed" {
		t.Errorf("rewritten = %+v decisions %+v", c, c.Decisions)
	}
	// A block is a reject with its reason and no dispatch; the output
	// the model saw follows.
	if c := calls["blocked"]; len(c.Decisions) != 1 || c.Decisions[0].Verdict != agentsession.VerdictReject || c.Decisions[0].Reason != "refused" || c.Dispatch != nil || c.Output == nil || !c.Rejected() {
		t.Errorf("blocked = %+v decisions %+v", c, c.Decisions)
	}
	// A deferral is a hold; the call is held until the caller answers.
	if c := calls["held"]; len(c.Decisions) != 1 || c.Decisions[0].Verdict != agentsession.VerdictHold || !c.Held() || c.State(s.Header()) != agentsession.CallHeld {
		t.Errorf("held = %+v decisions %+v", c, c.Decisions)
	}
	// Approving with new arguments answers the hold with a proceed
	// carrying them, then the dispatch and the output.
	heldID := calls["held"].ID()
	if _, err := a.Resume(context.Background(), agentturn.ApproveWith(heldID, rewrite)); err != nil {
		t.Fatal(err)
	}
	verifyAll(t, s)
	c := callsOf(t, s)["held"]
	if len(c.Decisions) != 2 || c.Decisions[1].Verdict != agentsession.VerdictProceed || c.Decisions[1].By != "" || c.Args() != string(rewrite) || c.Dispatch == nil || c.Output == nil || c.State(s.Header()) != agentsession.CallCompleted {
		t.Errorf("held after approval = %+v decisions %+v", c, c.Decisions)
	}
	if got := c.Output.Item.(*openresponses.FunctionCallOutput).Output.Text; got != "held:narrowed" {
		t.Errorf("held ran with %q", got)
	}
	// An approval without new arguments is a proceed with none.
	store2 := agentsession.NewMemoryStore()
	rec2, s2, _ := Start(context.Background(), store2, agentsession.Header{})
	b := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{upper},
		BeforeToolCall: func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			return &agentturn.ToolDecision{Action: agentturn.Defer}, nil
		}})
	defer rec2.Attach(b)()
	if _, err := b.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Resume(context.Background(), agentturn.Approve(b.State().Pending[0].Call.CallID)); err != nil {
		t.Fatal(err)
	}
	verifyAll(t, s2)
	if c := callsOf(t, s2)["upper"]; len(c.Decisions) != 2 || c.Decisions[1].Verdict != agentsession.VerdictProceed || c.Decisions[1].Args != nil || c.Dispatch == nil {
		t.Errorf("approved = %+v decisions %+v", c, c.Decisions)
	}
}

func TestModelBlockedIsRecorded(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("classifier refused the request")
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", BeforeModelCall: func(context.Context, *openresponses.Request) error { return boom }})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("x")); !errors.Is(err, boom) {
		t.Fatalf("prompt err = %v", err)
	}
	if got := entryTypes(s); got != "run config item:user response run" {
		t.Fatalf("entries = %q", got)
	}
	resp := s.Entries()[3].(*agentsession.ResponseEntry)
	if resp.Status != openresponses.ResponseStatusFailed || resp.ResponseID != "" || resp.Error == nil || !strings.Contains(resp.Error.Message, "classifier refused") || !strings.HasPrefix(resp.RequestHash, HashPrefix) {
		t.Errorf("blocked response = %+v error=%+v", resp, resp.Error)
	}
	if n := verifyAll(t, s); n != 1 {
		t.Errorf("responses = %d", n)
	}
	if e := runsOf(t, s)[0].End; e == nil || e.Reason != agentsession.ReasonError {
		t.Errorf("end = %+v", e)
	}
}

func TestEnvIsWrittenWhenItChanges(t *testing.T) {
	store := agentsession.NewMemoryStore()
	cwd := "/work/a"
	env := func(context.Context) (*agentsession.EnvEntry, error) {
		e := agentsession.NewEnvEntry(cwd)
		e.SetWorkspace(agentsession.WorkspaceLocal, "")
		return e, nil
	}
	rec, s, err := Start(context.Background(), store, agentsession.Header{}, WithEnv(env))
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}})
	unsub := rec.Attach(a)
	for _, text := range []string{"one", "two"} {
		if _, err := a.Prompt(context.Background(), openresponses.UserText(text)); err != nil {
			t.Fatal(err)
		}
	}
	cwd = "/work/b"
	if _, err := a.Prompt(context.Background(), openresponses.UserText("three")); err != nil {
		t.Fatal(err)
	}
	unsub()
	envs := func(s *agentsession.Session) []string {
		var out []string
		for _, e := range s.Entries() {
			if env, ok := e.(*agentsession.EnvEntry); ok {
				out = append(out, env.CWD)
			}
		}
		return out
	}
	if got := envs(s); strings.Join(got, ",") != "/work/a,/work/b" {
		t.Errorf("env entries = %v", got)
	}
	if got := entryTypes(s); !strings.HasPrefix(got, "run env config item:user") {
		t.Errorf("entries = %q", got)
	}
	verifyAll(t, s)
	// A resumed recorder finds the last env on the path and writes
	// nothing for an unchanged one.
	rec2, s2, err := Resume(context.Background(), store, s.ID(), WithEnv(env))
	if err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(agentturn.Config{Model: &echo.Adapter{}}, agentturn.WithTranscript(a.State().Transcript))
	defer rec2.Attach(b)()
	if _, err := b.Prompt(context.Background(), openresponses.UserText("four")); err != nil {
		t.Fatal(err)
	}
	if got := envs(s2); len(got) != 2 {
		t.Errorf("env entries after resume = %v", got)
	}
	// A nil entry writes nothing; an error fails the run.
	store3 := agentsession.NewMemoryStore()
	rec3, s3, _ := Start(context.Background(), store3, agentsession.Header{}, WithEnv(func(context.Context) (*agentsession.EnvEntry, error) { return nil, nil }))
	c := agentturn.New(agentturn.Config{Model: &echo.Adapter{}})
	defer rec3.Attach(c)()
	if _, err := c.Prompt(context.Background(), openresponses.UserText("x")); err != nil {
		t.Fatal(err)
	}
	if len(envs(s3)) != 0 {
		t.Error("nil env entry was written")
	}
	boom := errors.New("no git")
	rec4, _, _ := Start(context.Background(), store3, agentsession.Header{}, WithEnv(func(context.Context) (*agentsession.EnvEntry, error) { return nil, boom }))
	d := agentturn.New(agentturn.Config{Model: &echo.Adapter{}})
	defer rec4.Attach(d)()
	if _, err := d.Prompt(context.Background(), openresponses.UserText("x")); !errors.Is(err, boom) {
		t.Errorf("env error: %v", err)
	}
}

func TestFoldCallIsRecorded(t *testing.T) {
	root := t.TempDir()
	store, err := jsonl.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rec, s, err := Start(context.Background(), store, agentsession.Header{CWD: root})
	if err != nil {
		t.Fatal(err)
	}
	tr := compact.NewLocal(&echo.Adapter{}, compact.WithBudget(4), compact.WithKeepLast(2), compact.WithEstimator(countItems), compact.WithModel("summariser"), compact.WithOnFold(rec.Fold))
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: tr.Transform})
	unsub := rec.Attach(a)
	for _, text := range []string{"one", "two", "three"} {
		if _, err := a.Prompt(context.Background(), openresponses.UserText(text)); err != nil {
			t.Fatal(err)
		}
	}
	unsub()
	verifyAll(t, s)
	id := s.ID()
	if err := store.Release(id); err != nil {
		t.Fatal(err)
	}
	// Read the file back through a fresh store: the fold member survives
	// the round trip and names the fold's call.
	store2, err := jsonl.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	s2, err := store2.Open(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	var folds []*agentsession.CompactionEntry
	for _, e := range s2.Entries() {
		if c, ok := e.(*agentsession.CompactionEntry); ok {
			folds = append(folds, c)
		}
	}
	if len(folds) == 0 {
		t.Fatalf("no compaction in %q", entryTypes(s2))
	}
	raw, ok := folds[0].Unknown[FoldMember]
	if !ok {
		t.Fatalf("compaction has no %q member: %+v", FoldMember, folds[0].Unknown)
	}
	var call FoldCall
	if err := json.Unmarshal(raw, &call); err != nil || call.ResponseID == "" || call.Model != "summariser" || !strings.HasPrefix(call.RequestHash, HashPrefix) {
		t.Errorf("fold call = %+v err=%v", call, err)
	}
	// The fold's hash is not a response's: nothing on the path claims it.
	for _, e := range s2.Entries() {
		if r, ok := e.(*agentsession.ResponseEntry); ok && r.RequestHash == call.RequestHash {
			t.Error("a response entry carries the fold's hash")
		}
	}
	if n := verifyAll(t, s2); n != 3 {
		t.Errorf("responses after reopen = %d", n)
	}
}

func TestChildSessionIDIsDerivedFromTheCall(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{}, WithHarness("test", "1"))
	if err != nil {
		t.Fatal(err)
	}
	observed := agent.New(agentturn.Config{Name: "observed", Model: &echo.Adapter{}}, agent.WithObserver(rec.Observe))
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{observed}})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("delegate")); err != nil {
		t.Fatal(err)
	}
	verifyAll(t, s)
	l := links(s)
	if len(l) != 1 {
		t.Fatalf("links = %+v", l)
	}
	if want := agentsession.SubsessionID(s.ID(), l[0].CallID); l[0].Session != want {
		t.Errorf("child session %s, want derived %s", l[0].Session, want)
	}
	cs, err := store.Open(context.Background(), l[0].Session)
	if err != nil {
		t.Fatal(err)
	}
	if h := cs.Header(); h.SpawnedBy != l[0].CallID || h.ParentSession != s.ID() || strings.Join(h.Records, ",") != "run,dispatch,decision" {
		t.Errorf("child header = %+v", h)
	}
	if n := verifyAll(t, cs); n != 1 {
		t.Errorf("child responses = %d", n)
	}
	// The link was written at dispatch: before the child's output.
	types := strings.Fields(entryTypes(s))
	if types[6] != "link" || types[7] != "item:function_call_output" {
		t.Errorf("link position in %q", entryTypes(s))
	}

	// An unobserved child gets the derived ID and spawned_by too, but
	// promises no records, since it is replayed from its items.
	store2 := agentsession.NewMemoryStore()
	rec2, s2, _ := Start(context.Background(), store2, agentsession.Header{})
	plain := agent.New(agentturn.Config{Name: "plain", Model: &echo.Adapter{}})
	b := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{plain}})
	defer rec2.Attach(b)()
	if _, err := b.Prompt(context.Background(), openresponses.UserText("delegate")); err != nil {
		t.Fatal(err)
	}
	l2 := links(s2)
	cs2, err := store2.Open(context.Background(), l2[0].Session)
	if err != nil {
		t.Fatal(err)
	}
	if h := cs2.Header(); h.ID != agentsession.SubsessionID(s2.ID(), l2[0].CallID) || h.SpawnedBy != l2[0].CallID || len(h.Records) != 0 {
		t.Errorf("replayed child header = %+v", h)
	}

	// A second run under the same call continues the child session at
	// its leaf rather than minting a second session or starting a new
	// root: a subagent that is messaged again answers from its own
	// context, so its run belongs after the one before it.
	call := agenttool.Call{ID: "call_again", Args: json.RawMessage(`{"input":"hi"}`)}
	for range 2 {
		if _, err := observed.Execute(context.Background(), call); err != nil {
			t.Fatal(err)
		}
	}
	again, err := store.Open(context.Background(), agentsession.SubsessionID(s.ID(), "call_again"))
	if err != nil {
		t.Fatal(err)
	}
	if n := roots(again); n != 1 {
		t.Errorf("second run under one call opened %d roots in %q", n, entryTypes(again))
	}
	if n := len(runsOf(t, again)); n != 2 {
		t.Errorf("runs on the child session = %d", n)
	}
	// Both runs hold a response, and the path to the leaf verifies.
	if n := verifyAll(t, again); n != 2 {
		t.Errorf("child responses = %d", n)
	}

	// A host that means a retry from a clean start says so, and the
	// leaf is reset as it was before.
	retryCall := agenttool.Call{ID: "call_retry", Args: json.RawMessage(`{"input":"hi"}`)}
	for i := range 2 {
		ctx := context.Background()
		if i == 1 {
			ctx = agent.ContextWithRetry(ctx)
		}
		if _, err := observed.Execute(ctx, retryCall); err != nil {
			t.Fatal(err)
		}
	}
	retry, err := store.Open(context.Background(), agentsession.SubsessionID(s.ID(), "call_retry"))
	if err != nil {
		t.Fatal(err)
	}
	if n := roots(retry); n != 2 {
		t.Errorf("retried child has %d roots in %q", n, entryTypes(retry))
	}
	if n := verifyAll(t, retry); n != 2 {
		t.Errorf("retried child responses = %d", n)
	}
}

func TestJSONLRoundTripVerifies(t *testing.T) {
	root := t.TempDir()
	store, err := jsonl.Open(root, jsonl.WithSync(jsonl.SyncOnResponse))
	if err != nil {
		t.Fatal(err)
	}
	rec, s, err := Start(context.Background(), store, agentsession.Header{CWD: root, Harness: &agentsession.Harness{Name: "test"}})
	if err != nil {
		t.Fatal(err)
	}
	child := agent.New(agentturn.Config{Name: "child", Model: &echo.Adapter{}, Tools: []agenttool.Tool{upper}}, agent.WithObserver(rec.Observe))
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{upper, child},
		BeforeToolCall: func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			if info.Call.Name == "upper" {
				return &agentturn.ToolDecision{Action: agentturn.Defer}, nil
			}
			return nil, nil
		}})
	unsub := rec.Attach(a)
	end, err := a.Prompt(agentturn.ContextWithTrigger(context.Background(), agentturn.Trigger{Kind: "test"}), openresponses.UserText("go"))
	if err != nil || end.Reason != agentturn.ReasonInputRequired {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	if _, err := a.Resume(context.Background(), agentturn.Approve(end.Pending[0].Call.CallID)); err != nil {
		t.Fatal(err)
	}
	unsub()
	id := s.ID()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store2, err := jsonl.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	s2, err := store2.Open(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if n := verifyAll(t, s2); n != 2 {
		t.Errorf("responses = %d in %q", n, entryTypes(s2))
	}
	runs := runsOf(t, s2)
	if len(runs) != 2 || runs[0].Start.Ref != "test" || runs[0].End.Reason != agentsession.ReasonInputRequired || runs[1].Start.Source != agentsession.SourceResume || runs[1].End.Reason != agentsession.ReasonDone {
		t.Errorf("runs = %+v %+v", runs[0], runs[1])
	}
	for name, c := range callsOf(t, s2) {
		if st := c.State(s2.Header()); st != agentsession.CallCompleted {
			t.Errorf("%s state = %v", name, st)
		}
	}
	l := links(s2)
	if len(l) != 1 {
		t.Fatalf("links = %+v", l)
	}
	cs, err := store2.Open(context.Background(), l[0].Session)
	if err != nil {
		t.Fatal(err)
	}
	if n := verifyAll(t, cs); n != 2 {
		t.Errorf("child responses = %d in %q", n, entryTypes(cs))
	}
}

// sideData is a Details value a tool keeps in the session.
type sideData struct {
	Path  string `json:"path"`
	Bytes int    `json:"bytes"`
}

func (sideData) RecordNS() string { return "acme:tool_output" }

func TestRecordableDetailsBecomeACustomEntry(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	keeper := agenttool.New("run", "", func(_ context.Context, _ echoArgs) (agenttool.Result, error) {
		return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "first 4 KB ... last 4 KB"}, Details: sideData{Path: "/tmp/out.log", Bytes: 300_000}}, nil
	})
	plain := agenttool.New("plain", "", func(_ context.Context, _ echoArgs) (string, error) { return "x", nil })
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{keeper, plain}})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	// Only the recordable value is written, between the dispatches and
	// the outputs; a Details value for subscribers alone is not.
	if got := entryTypes(s); got != "run config item:user item:function_call* item:function_call* response dispatch dispatch custom item:function_call_output item:function_call_output item:assistant* response run" {
		t.Errorf("entries = %q", got)
	}
	var found *agentsession.CustomEntry
	for _, e := range s.Entries() {
		if c, ok := e.(*agentsession.CustomEntry); ok {
			found = c
		}
	}
	var data sideData
	if found == nil || found.NS != "acme:tool_output" || json.Unmarshal(found.Data, &data) != nil || data.Path != "/tmp/out.log" || data.Bytes != 300_000 {
		t.Errorf("custom = %+v", found)
	}
	verifyAll(t, s)
	// A recordable with no namespace fails the run rather than writing
	// an entry nothing can find.
	bad := agenttool.New("bad", "", func(_ context.Context, _ echoArgs) (agenttool.Result, error) {
		return agenttool.Result{Details: noNS{}}, nil
	})
	b := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{bad}})
	defer rec.Attach(b)()
	if _, err := b.Prompt(context.Background(), openresponses.UserText("x")); err == nil || !strings.Contains(err.Error(), "empty namespace") {
		t.Errorf("empty namespace: %v", err)
	}
}

type noNS struct{}

func (noNS) RecordNS() string { return "" }

// TestAbortCauseIsTheRunsRef checks that the reason a host cut a run
// reaches the record, where "context canceled" stood for seven
// different things before.
func TestAbortCauseIsTheRunsRef(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	blocking := agenttool.New("wait", "waits", func(ctx context.Context, _ echoArgs) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{blocking}})
	defer rec.Attach(a)()
	a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		if _, ok := ev.(*agentturn.ToolStart); ok {
			a.AbortCause(errors.New("ttsr: rule box-leak"))
		}
		return nil
	})
	end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
	if end == nil || end.Reason != agentturn.ReasonAborted {
		t.Fatalf("end = %+v", end)
	}
	runs := runsOf(t, s)
	if len(runs) != 1 || runs[0].End == nil {
		t.Fatalf("runs = %+v", runs)
	}
	if runs[0].End.Reason != agentsession.ReasonInterrupted || runs[0].End.Ref != "ttsr: rule box-leak" {
		t.Errorf("run end = %+v", runs[0].End)
	}
	if len(runs[0].End.Pending) != 1 {
		t.Errorf("the cut call is not pending: %+v", runs[0].End)
	}
	verifyAll(t, s)
}

// TestNestedCallsAreRecorded checks that the calls a tool makes through
// agentturn.Invoke leave the same facts on the record as the model's
// own: one entry before each runs and one after, so a reader counting
// the calls of a turn counts three rather than one.
func TestNestedCallsAreRecorded(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	read := agenttool.New("read", "reads", func(context.Context, echoArgs) (string, error) { return "the contents", nil })
	eval := agenttool.New("eval", "runs code", func(ctx context.Context, _ echoArgs) (string, error) {
		if _, err := agentturn.Invoke(ctx, "read", json.RawMessage(`{"text":"go.mod"}`)); err != nil {
			return "", err
		}
		_, err := agentturn.Invoke(ctx, "bash", json.RawMessage(`{"text":"rm -rf /tmp/build"}`))
		if err == nil {
			t.Error("the blocked nested call returned no error")
		}
		return "ran two calls", nil
	})
	bash := agenttool.New("bash", "runs a command", func(context.Context, echoArgs) (string, error) { return "done", nil })
	policy := func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
		if info.Call.Name == "bash" {
			return &agentturn.ToolDecision{Action: agentturn.Block, Reason: "denied by bash(rm:*)", By: agentsession.ByPolicy}, nil
		}
		return nil, nil
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m",
		Tools: []agenttool.Tool{eval, read, bash}, BeforeToolCall: policy, MaxTurns: 1})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("x")); err != nil {
		t.Fatal(err)
	}
	var nested []NestedCall
	for _, e := range s.Entries() {
		c, ok := e.(*agentsession.CustomEntry)
		if !ok || c.NS != NestedCallNS {
			continue
		}
		var n NestedCall
		if err := json.Unmarshal(c.Data, &n); err != nil {
			t.Fatal(err)
		}
		nested = append(nested, n)
	}
	if len(nested) != 4 {
		t.Fatalf("nested call entries = %d", len(nested))
	}
	parent := callsOf(t, s)["eval"]
	if parent == nil || parent.Dispatch == nil {
		t.Fatalf("the call that made them = %+v", parent)
	}
	for _, n := range nested {
		if n.Parent != parent.ID() {
			t.Errorf("entry %+v does not name the call that made it (%s)", n, parent.ID())
		}
	}
	if nested[0].Name != "read" || nested[0].Phase != agentsession.RunStart || string(nested[0].Args) != `{"text":"go.mod"}` {
		t.Errorf("first entry = %+v", nested[0])
	}
	if nested[1].Phase != agentsession.RunEnd || nested[1].Output != "the contents" {
		t.Errorf("second entry = %+v", nested[1])
	}
	if nested[2].Name != "bash" || nested[2].Verdict != agentsession.VerdictReject || nested[2].Reason != "denied by bash(rm:*)" || nested[2].By != agentsession.ByPolicy {
		t.Errorf("third entry = %+v", nested[2])
	}
	if nested[3].Phase != agentsession.RunEnd || nested[3].Error == "" {
		t.Errorf("fourth entry = %+v", nested[3])
	}
	// The entries land between the call's dispatch and its output, and
	// the record still verifies.
	verifyAll(t, s)
}
