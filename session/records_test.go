package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
	if h := s.Header(); strings.Join(h.Records, ",") != "run,dispatch,decision,queued" {
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

	// A resume whose approved batch terminates makes no model call: the
	// segment holds no response of its own, and the format reads a
	// segment that answers a call an earlier model call made, and
	// leaves nothing pending, as stopped. The loop's reason rides as
	// ref.
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
	if last := runs[len(runs)-1]; last.End == nil || last.End.Reason != agentsession.ReasonStopped || last.End.Ref != "terminate" {
		t.Errorf("terminating resume end = %+v", last.End)
	}
}

// twoCallModel calls every offered tool once per turn, so a run can
// leave more than one call pending.
type twoCallModel struct{}

func (twoCallModel) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	for _, tl := range req.Tools {
		ft, ok := tl.(*openresponses.FunctionTool)
		if !ok {
			continue
		}
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

// TestResponselessRunReadsAsStoppedOnlyWhenItAnsweredACall drives the
// three shapes a run with no model call of its own can have. It goes
// through Handle rather than through a loop because the loop produces
// only the third: Resume refuses a partial answer, and every path it
// has to ReasonStopped without a model call has answered every pending
// call first. Handle is public for a host driving agentturn.Run itself,
// and the format's stopped step asks for both of the things the
// recorder checks — the segment answered a call, and no call on the
// path is left without an output — so each run is written and then
// checked against ComputeReason, the reader it has to agree with.
func TestResponselessRunReadsAsStoppedOnlyWhenItAnsweredACall(t *testing.T) {
	// Two calls, both deferred, so the path holds a response that made
	// them and two calls with no output.
	lower := agenttool.New("lower", "", func(_ context.Context, x echoArgs) (string, error) { return strings.ToLower(x.Text), nil })
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: twoCallModel{}, Tools: []agenttool.Tool{upper, lower},
		BeforeToolCall: func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			return &agentturn.ToolDecision{Action: agentturn.Defer}, nil
		}})
	unsub := rec.Attach(a)
	end, err := a.Prompt(context.Background(), openresponses.UserText("abc"))
	if err != nil || len(end.Pending) != 2 {
		t.Fatalf("prompt: err=%v pending=%d", err, len(end.Pending))
	}
	unsub()
	first, second := end.Pending[0].Call.CallID, end.Pending[1].Call.CallID

	// Each run below is a stop with no model call; they differ only in
	// what the segment holds. A run that answers nothing is an input,
	// as the format reads its shape.
	host := func(runID string, items ...openresponses.Item) {
		t.Helper()
		ctx := context.Background()
		source := agentturn.SourceInput
		if len(items) > 0 {
			source = agentturn.SourceResume
		}
		if err := rec.Handle(ctx, &agentturn.RunStart{RunID: runID, Source: source}); err != nil {
			t.Fatal(err)
		}
		for _, item := range items {
			if err := rec.Handle(ctx, &agentturn.ItemEnd{RunID: runID, Item: item}); err != nil {
				t.Fatal(err)
			}
		}
		if err := rec.Handle(ctx, &agentturn.RunEnd{RunID: runID, Reason: agentturn.ReasonStopped, Cause: agentturn.StopRefused}); err != nil {
			t.Fatal(err)
		}
	}
	// 1. Answers nothing. The stopped step wants a segment holding an
	//    output or a decision, and this holds neither.
	host("run_nothing")
	// 2. Answers one of the two. A call on the path is still without an
	//    output, which keeps a later run from reading as stopped.
	host("run_partial", openresponses.NewFunctionCallOutput(first, "one"))
	// 3. Answers the last one. Now the segment answered a call the
	//    response before it made and nothing on the path is pending.
	host("run_rest", openresponses.NewFunctionCallOutput(second, "two"))
	// 4. Answers nothing again, after a run that answered. Nothing is
	//    pending now, so this one reads as stopped unless the record of
	//    what the previous run answered was cleared with the run.
	host("run_after")

	want := []string{
		agentsession.ReasonInputRequired,
		agentsession.ReasonAborted,
		agentsession.ReasonAborted,
		agentsession.ReasonStopped,
		agentsession.ReasonAborted,
	}
	runs := runsOf(t, s)
	if len(runs) != len(want) {
		t.Fatalf("runs = %d, want %d, in %q", len(runs), len(want), entryTypes(s))
	}
	for i, r := range runs {
		if r.End == nil {
			t.Fatalf("run %d has no end", i)
		}
		if r.End.Reason != want[i] {
			t.Errorf("run %d wrote %s, want %s", i, r.End.Reason, want[i])
		}
		// The reader has to agree, which is the point of writing it.
		if got := agentsession.ComputeReason(r.Path, r.Segment); got != r.End.Reason {
			t.Errorf("run %d wrote %s, the format computes %s", i, r.End.Reason, got)
		}
	}
	verifyAll(t, s)
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
	// The abort comes once the fast call has ended and the slow one has
	// been handed to its tool, so the slow call is cut after its
	// dispatch whichever order the batch ran them in.
	fastDone, slowDispatched := make(chan struct{}), make(chan struct{})
	a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		switch e := ev.(type) {
		case *agentturn.ToolEnd:
			if e.Name == "fast" {
				close(fastDone)
			}
		case *agentturn.ToolDispatch:
			if e.Name == "slow" {
				close(slowDispatched)
			}
		}
		return nil
	})
	done := make(chan *agentturn.RunEnd, 1)
	go func() {
		end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
		done <- end
	}()
	<-fastDone
	<-slowDispatched
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

// oneCallATurn calls the tools it is offered one a turn, in order, and
// answers with text once each has its output.
type oneCallATurn struct{}

func (oneCallATurn) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	outputs := 0
	for _, item := range req.Input {
		if _, ok := item.(*openresponses.FunctionCallOutput); ok {
			outputs++
		}
	}
	if outputs%len(req.Tools) == 0 {
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
	}
	ft := req.Tools[outputs%len(req.Tools)].(*openresponses.FunctionTool)
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
	return em.Complete()
}

// TestEnvBetweenCalls pins #187: a workspace that moves between two
// calls of one run, reported by Recorder.Env from AfterToolCall, puts
// the second call under a new env entry, and the next run's start,
// finding it in force, writes none.
func TestEnvBetweenCalls(t *testing.T) {
	cases := []struct {
		name string
		// move is the node the first call's AfterToolCall moves the
		// workspace to.
		move     string
		wantEnvs string
		// wantRecord is the env entries and dispatches on the path.
		wantRecord string
	}{
		{"moved", "node-2", "node-1,node-2", "env:node-1 dispatch:a env:node-2 dispatch:b"},
		{"not moved", "node-1", "node-1", "env:node-1 dispatch:a dispatch:b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			node := "node-1"
			env := func(context.Context) (*agentsession.EnvEntry, error) {
				e := agentsession.NewEnvEntry("/work")
				if err := e.SetWorkspace(agentsession.WorkspaceContainer, "sandbox").SetMember("node", node); err != nil {
					return nil, err
				}
				return e, nil
			}
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(ctx, store, agentsession.Header{}, WithEnv(env))
			if err != nil {
				t.Fatal(err)
			}
			tool := func(name string) agenttool.Tool {
				return agenttool.New(name, "", func(context.Context, echoArgs) (string, error) { return "ok", nil })
			}
			a := agentturn.New(agentturn.Config{Model: oneCallATurn{}, Tools: []agenttool.Tool{tool("a"), tool("b")},
				AfterToolCall: func(ctx context.Context, info agentturn.ToolResultInfo) (*agentturn.ToolOverride, error) {
					if info.Call.Name == "a" {
						node = tc.move
						if err := rec.Env(ctx); err != nil {
							return nil, err
						}
					}
					return nil, nil
				}})
			defer rec.Attach(a)()
			for _, text := range []string{"one", "two"} {
				if end, err := a.Prompt(ctx, openresponses.UserText(text)); err != nil || end.Reason != agentturn.ReasonDone {
					t.Fatalf("prompt: err=%v end=%+v", err, end)
				}
				if text == "one" {
					var record []string
					for _, e := range s.Path(s.Leaf()) {
						switch e := e.(type) {
						case *agentsession.EnvEntry:
							var n string
							_ = json.Unmarshal(e.Workspace.Unknown["node"], &n)
							record = append(record, "env:"+n)
						case *agentsession.DispatchEntry:
							c := callsOf(t, s)
							for name, call := range c {
								if call.ID() == e.CallID {
									record = append(record, "dispatch:"+name)
								}
							}
						}
					}
					if got := strings.Join(record, " "); got != tc.wantRecord {
						t.Errorf("record = %q, want %q", got, tc.wantRecord)
					}
				}
			}
			var envs []string
			for _, e := range s.Entries() {
				if env, ok := e.(*agentsession.EnvEntry); ok {
					var n string
					_ = json.Unmarshal(env.Workspace.Unknown["node"], &n)
					envs = append(envs, n)
				}
			}
			if got := strings.Join(envs, ","); got != tc.wantEnvs {
				t.Errorf("env entries = %q, want %q", got, tc.wantEnvs)
			}
			if err := s.VerifyRecords(s.Leaf()); err != nil {
				t.Errorf("verify records: %v", err)
			}
			verifyAll(t, s)
		})
	}
}

// scriptedCalls makes the calls of its script one a turn, in order,
// and answers once every one has its output.
type scriptedCalls []struct{ name, args string }

func (m scriptedCalls) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	outputs := 0
	for _, item := range req.Input {
		if _, ok := item.(*openresponses.FunctionCallOutput); ok {
			outputs++
		}
	}
	if outputs < len(m) {
		w, err := em.FunctionCall("", m[outputs].name)
		if err != nil {
			return err
		}
		if err := w.Arguments(m[outputs].args); err != nil {
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

// isEnv reports whether e is an env entry.
func isEnv(e agentsession.Entry) bool {
	_, ok := e.(*agentsession.EnvEntry)
	return ok
}

// envOn is the env entry of a sandbox on node, the node a member of
// the workspace, where the format's substitution rule looks.
func envOn(node string) *agentsession.EnvEntry {
	e := agentsession.NewEnvEntry("/work")
	if err := e.SetWorkspace(agentsession.WorkspaceContainer, "sandbox").SetMember("node", node); err != nil {
		panic(err)
	}
	return e
}

// envNodes lists the node of each env entry in entries.
func envNodes(entries []agentsession.Entry) string {
	var nodes []string
	for _, e := range entries {
		if env, ok := e.(*agentsession.EnvEntry); ok {
			var n string
			_ = json.Unmarshal(env.Workspace.Unknown["node"], &n)
			nodes = append(nodes, n)
		}
	}
	return strings.Join(nodes, ",")
}

// TestEnvInAChild pins the review of #187 and #197: a child's first
// run start writes the env in force in its parent at that moment, right
// after the run's start, so the child's file names the workspace it ran
// in; Env made with the context of a child session whose run has ended
// is filed in that session, at its leaf, as Annotate's entry is,
// compared with the child's own env, not with its parent's, which may
// have moved since; and a grandchild starts under its parent's, the
// child's, not under the recorder's own session's.
func TestEnvInAChild(t *testing.T) {
	ctx := context.Background()
	node := "node-1"
	env := func(context.Context) (*agentsession.EnvEntry, error) {
		e := agentsession.NewEnvEntry("/work")
		if err := e.SetWorkspace(agentsession.WorkspaceContainer, "sandbox").SetMember("node", node); err != nil {
			return nil, err
		}
		return e, nil
	}
	childOf := func(t *testing.T, store agentsession.Store, s *agentsession.Session) *agentsession.Session {
		t.Helper()
		l := links(s)
		if len(l) != 1 {
			t.Fatalf("links = %d", len(l))
		}
		child, err := store.Open(ctx, l[0].Session)
		if err != nil {
			t.Fatal(err)
		}
		return child
	}

	t.Run("after the child's run", func(t *testing.T) {
		node = "node-1"
		store := agentsession.NewMemoryStore()
		rec, s, err := Start(ctx, store, agentsession.Header{}, WithEnv(env))
		if err != nil {
			t.Fatal(err)
		}
		childCfg := agentturn.Config{Name: "specialist", Description: "notes things", Model: &echo.Adapter{}}
		specialist := agent.New(childCfg, agent.WithObserver(rec.Observe), agent.WithRunContext(rec.ChildContext))
		a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{specialist}})
		defer rec.Attach(a)()
		if _, err := a.Prompt(ctx, openresponses.UserText("delegate")); err != nil {
			t.Fatal(err)
		}
		childID := childOf(t, store, s).ID()
		jobCtx := ContextWithSessionID(ctx, childID)
		// Unmoved, the child is under its own env, its parent's at its start.
		if err := rec.Env(jobCtx); err != nil {
			t.Fatal(err)
		}
		node = "node-2"
		for range 2 {
			if err := rec.Env(jobCtx); err != nil {
				t.Fatal(err)
			}
		}
		child := childOf(t, store, s)
		if got := envNodes(child.Entries()); got != "node-1,node-2" {
			t.Errorf("child env entries = %q, want node-1,node-2", got)
		}
		if first := child.Entries()[1]; !isEnv(first) {
			t.Errorf("the child's run start is followed by %s, want its env", first.EntryType())
		}
		if leaf, _ := child.Entry(child.Leaf()); !isEnv(leaf) {
			t.Errorf("the child's env is not at its leaf")
		}
		if got := envNodes(s.Entries()); got != "node-1" {
			t.Errorf("root env entries = %q, want node-1", got)
		}
		verifyAll(t, s)
		verifyAll(t, child)
	})

	t.Run("a grandchild", func(t *testing.T) {
		node = "node-1"
		store := agentsession.NewMemoryStore()
		rec, s, err := Start(ctx, store, agentsession.Header{}, WithEnv(env))
		if err != nil {
			t.Fatal(err)
		}
		var envErr error
		note := agenttool.New("note", "note something", func(ctx context.Context, _ echoArgs) (string, error) {
			envErr = errors.Join(envErr, rec.Env(ctx))
			return "noted", nil
		})
		move := agenttool.New("move", "move the workspace", func(ctx context.Context, _ agenttool.NoArgs) (string, error) {
			node = "node-2"
			envErr = errors.Join(envErr, rec.Env(ctx))
			return "moved", nil
		})
		grandCfg := agentturn.Config{Name: "grand", Description: "notes things", Model: &echo.Adapter{}, Tools: []agenttool.Tool{note}}
		grand := agent.New(grandCfg, agent.WithObserver(rec.Observe), agent.WithRunContext(rec.ChildContext))
		childCfg := agentturn.Config{Name: "specialist", Description: "moves and delegates",
			Model: scriptedCalls{{"move", `{}`}, {"grand", `{"input":"note it"}`}}, Tools: []agenttool.Tool{move, grand}}
		specialist := agent.New(childCfg, agent.WithObserver(rec.Observe), agent.WithRunContext(rec.ChildContext))
		a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{specialist}})
		defer rec.Attach(a)()
		if _, err := a.Prompt(ctx, openresponses.UserText("delegate")); err != nil {
			t.Fatal(err)
		}
		if envErr != nil {
			t.Fatal(envErr)
		}
		child := childOf(t, store, s)
		grandchild := childOf(t, store, child)
		if got := envNodes(child.Entries()); got != "node-1,node-2" {
			t.Errorf("child env entries = %q, want node-1,node-2", got)
		}
		if got := envNodes(grandchild.Entries()); got != "node-2" {
			t.Errorf("grandchild env entries = %q, want node-2: it starts under its parent's", got)
		}
		verifyAll(t, s)
		verifyAll(t, child)
		verifyAll(t, grandchild)
	})

	t.Run("a job after its parent moved", func(t *testing.T) {
		node = "node-1"
		store := agentsession.NewMemoryStore()
		rec, s, err := Start(ctx, store, agentsession.Header{}, WithEnv(env))
		if err != nil {
			t.Fatal(err)
		}
		childCfg := agentturn.Config{Name: "specialist", Description: "notes things", Model: &echo.Adapter{}}
		specialist := agent.New(childCfg, agent.WithObserver(rec.Observe), agent.WithRunContext(rec.ChildContext))
		a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{specialist}})
		defer rec.Attach(a)()
		if _, err := a.Prompt(ctx, openresponses.UserText("delegate")); err != nil {
			t.Fatal(err)
		}
		// The parent moves, and then the job the child started finds
		// itself on the parent's node: it moved too.
		node = "node-2"
		if err := rec.Env(ctx); err != nil {
			t.Fatal(err)
		}
		if err := rec.Env(ContextWithSessionID(ctx, childOf(t, store, s).ID())); err != nil {
			t.Fatal(err)
		}
		child := childOf(t, store, s)
		if got := envNodes(child.Entries()); got != "node-1,node-2" {
			t.Errorf("child env entries = %q, want node-1,node-2", got)
		}
		if got := envNodes(s.Entries()); got != "node-1,node-2" {
			t.Errorf("root env entries = %q, want node-1,node-2", got)
		}
		verifyAll(t, s)
		verifyAll(t, child)
	})
}

// nodeKey carries, on a tool call's context, the node a child's own
// sandbox is on, as a host that gives a sub-agent a container of its
// own puts the sandbox on the call's context.
type nodeKey struct{}

// onNode runs its tool with the node on the call's context.
type onNode struct {
	agenttool.Tool
	node string
}

func (o onNode) Execute(ctx context.Context, call agenttool.Call) (agenttool.Result, error) {
	return o.Tool.Execute(context.WithValue(ctx, nodeKey{}, o.node), call)
}

// TestChildWithItsOwnWorkspace pins #214: a child's first run start
// asks the WithEnv function with the child's context, so a child the
// host gave a sandbox of its own starts under it, its first dispatch
// is filed where it ran and a later Recorder.Env that finds it there
// writes nothing; a child the function returns nil for is where its
// parent is, and starts under a copy of its parent's env as before; an
// error from the function at the child's start ends the child's
// record there, since an observer cannot fail the child's run.
func TestChildWithItsOwnWorkspace(t *testing.T) {
	boom := errors.New("sandbox gone")
	cases := []struct {
		name string
		// childNode is what the env function returns for the child's
		// context, "" for nil.
		childNode string
		err       error
		// wantChild is the child's env entries, and wantFirst whether
		// its first entry after the run start is an env.
		wantChild string
		wantFirst bool
	}{
		{name: "a sandbox of its own", childNode: "node-9", wantChild: "node-9", wantFirst: true},
		{name: "where its parent is", wantChild: "node-1", wantFirst: true},
		{name: "the function fails", err: boom},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var asked []string
			// later is set once the child's run is over, when a job
			// of the child's finds itself on node-9.
			later := false
			env := func(ctx context.Context) (*agentsession.EnvEntry, error) {
				node, own := ctx.Value(nodeKey{}).(string)
				asked = append(asked, node)
				switch {
				case !own:
					return envOn("node-1"), nil
				case tc.err != nil:
					return nil, tc.err
				case later:
					return envOn("node-9"), nil
				case tc.childNode == "":
					return nil, nil
				}
				return envOn(tc.childNode), nil
			}
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(ctx, store, agentsession.Header{}, WithEnv(env))
			if err != nil {
				t.Fatal(err)
			}
			childCfg := agentturn.Config{Name: "specialist", Description: "builds in its own sandbox", Model: scriptedCalls{{"upper", `{"text":"t"}`}}, Tools: []agenttool.Tool{upper}}
			specialist := agent.New(childCfg, agent.WithObserver(rec.Observe), agent.WithRunContext(rec.ChildContext))
			a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{onNode{specialist, "node-9"}}})
			defer rec.Attach(a)()
			if _, err := a.Prompt(ctx, openresponses.UserText("delegate")); err != nil {
				t.Fatal(err)
			}
			if len(asked) < 2 || asked[1] != "node-9" {
				t.Fatalf("the env function was asked with %q, want the child's context second", asked)
			}
			child, err := store.Open(ctx, agentsession.SubsessionID(s.ID(), links(s)[0].CallID))
			if err != nil {
				t.Fatal(err)
			}
			if got := envNodes(child.Entries()); got != tc.wantChild {
				t.Errorf("child env entries = %q, want %q", got, tc.wantChild)
			}
			if tc.wantFirst {
				if first := child.Entries()[1]; !isEnv(first) {
					t.Errorf("the child's run start is followed by %s, want its env", first.EntryType())
				}
				// The dispatch is filed under the env the child ran in.
				types := strings.Fields(entryTypes(child))
				if !slices.Contains(types, "dispatch") || slices.Index(types, "env") > slices.Index(types, "dispatch") {
					t.Errorf("child entries = %q, want the env before the dispatch", entryTypes(child))
				}
				verifyAll(t, child)
			}
			if got := envNodes(s.Entries()); got != "node-1" {
				t.Errorf("root env entries = %q, want node-1", got)
			}
			verifyAll(t, s)
			if tc.err != nil {
				return
			}
			// A job the child started finds itself on node-9: nothing
			// is written for a child that started there, and a move
			// for one that started where its parent is.
			later = true
			if err := rec.Env(context.WithValue(ContextWithSessionID(ctx, child.ID()), nodeKey{}, "node-9")); err != nil {
				t.Fatal(err)
			}
			child, err = store.Open(ctx, child.ID())
			if err != nil {
				t.Fatal(err)
			}
			want := "node-9"
			if tc.childNode == "" {
				want = "node-1,node-9"
			}
			if got := envNodes(child.Entries()); got != want {
				t.Errorf("child env entries after Env = %q, want %q", got, want)
			}
		})
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

	// A second Execute under the same call opens a new root in the
	// child session rather than minting a second session or continuing
	// at the leaf (#87): tools/agent builds a fresh agent for each
	// Execute whose request is the new input alone, which holds
	// nothing of the path the leaf rebuilds, so every response on
	// either root carries a hash. A host marking the context with
	// ContextWithRetry itself changes nothing.
	call := agenttool.Call{ID: "call_again", Args: json.RawMessage(`{"input":"hi"}`)}
	for i := range 2 {
		ctx := context.Background()
		if i == 1 {
			ctx = agent.ContextWithRetry(ctx)
		}
		if _, err := observed.Execute(ctx, call); err != nil {
			t.Fatal(err)
		}
	}
	again, err := store.Open(context.Background(), agentsession.SubsessionID(s.ID(), "call_again"))
	if err != nil {
		t.Fatal(err)
	}
	if n := roots(again); n != 2 {
		t.Errorf("second run under one call opened %d roots in %q, want 2", n, entryTypes(again))
	}
	if n := len(runsOf(t, again)); n != 1 {
		t.Errorf("runs on the path at the child session's leaf = %d, want the second alone", n)
	}
	if n := verifyAll(t, again); n != 2 {
		t.Errorf("child responses = %d, want 2, each hashed", n)
	}
}

// TestSecondExecuteOpensANewRoot pins #87 in its realistic shape: the
// first Execute under a call is cut off inside a tool, leaving the
// call in flight at the child session's leaf, and a second Execute
// under the same call ID, as Agent.Resume running the call again
// makes, opens a new root in the child's session, so every response
// rebuilds and the leaf owes nothing; a host prompting again the agent
// it kept through WithSpawn, which holds its context, continues at the
// leaf.
func TestSecondExecuteOpensANewRoot(t *testing.T) {
	cases := []struct {
		name string
		// cut cuts the first run off inside its tool call.
		cut bool
	}{
		{name: "the first run cut off", cut: true},
		{name: "both runs complete"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			first, cancel := context.WithCancel(context.Background())
			defer cancel()
			// cutNow is set while the first run is the one running,
			// so the tool cuts that run alone.
			cutNow := tc.cut
			stop := agenttool.New("stop", "cuts the run", func(ctx context.Context, _ agenttool.NoArgs) (string, error) {
				if cutNow {
					cancel()
					<-ctx.Done()
					return "", ctx.Err()
				}
				return "went on", nil
			})
			var spawned *agentturn.Agent
			child := agent.New(agentturn.Config{Name: "worker", Model: scriptedCalls{{"stop", `{}`}}, Tools: []agenttool.Tool{stop}},
				agent.WithObserver(rec.Observe), agent.WithRunContext(rec.ChildContext),
				agent.WithSpawn(func(_ string, a *agentturn.Agent) { spawned = a }))
			// The parent is attached so the child has a parent run to
			// be filed under, as under Agent.Resume.
			a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{child}})
			defer rec.Attach(a)()
			call := agenttool.Call{ID: "call_cut", Args: json.RawMessage(`{"input":"go"}`)}
			_, err = child.Execute(first, call)
			cutNow = false
			if tc.cut != (err != nil) {
				t.Fatalf("first execute: err=%v, want cut %v", err, tc.cut)
			}
			cs, err := store.Open(context.Background(), agentsession.SubsessionID(s.ID(), call.ID))
			if err != nil {
				t.Fatal(err)
			}
			pending, err := Pending(cs)
			if err != nil {
				t.Fatal(err)
			}
			if tc.cut && (len(pending) != 1 || pending[0].Reason != agentturn.PendingAborted) {
				t.Fatalf("pending after the cut = %+v, want the stop call in flight", pending)
			}

			// Run again under the same call ID: a new root.
			if _, err := child.Execute(context.Background(), call); err != nil {
				t.Fatal(err)
			}
			cs, err = store.Open(context.Background(), cs.ID())
			if err != nil {
				t.Fatal(err)
			}
			if n := roots(cs); n != 2 {
				t.Errorf("the second execute left %d roots in %q, want 2", n, entryTypes(cs))
			}
			if pending, err := Pending(cs); err != nil || len(pending) != 0 {
				t.Errorf("pending at the leaf after the second execute = %+v, %v; want none", pending, err)
			}
			// Each complete run makes two model calls, the one that
			// calls stop and the one that answers; the cut run made
			// one.
			want := 4
			if tc.cut {
				want = 3
			}
			if n := verifyAll(t, cs); n != want {
				t.Errorf("child responses = %d, want %d, each hashed", n, want)
			}

			// The host prompts the agent it kept: that one holds its
			// context, and its run continues at the leaf.
			if spawned == nil {
				t.Fatal("WithSpawn gave no agent")
			}
			if _, err := spawned.Prompt(context.Background(), openresponses.UserText("and then?")); err != nil {
				t.Fatal(err)
			}
			cs, err = store.Open(context.Background(), cs.ID())
			if err != nil {
				t.Fatal(err)
			}
			if n := roots(cs); n != 2 {
				t.Errorf("the host's prompt left %d roots in %q, want still 2", n, entryTypes(cs))
			}
			if n := len(runsOf(t, cs)); n != 2 {
				t.Errorf("runs on the path at the leaf = %d, want the second execute's and the host's", n)
			}
			if n := verifyAll(t, cs); n != want+1 {
				t.Errorf("child responses = %d, want %d, each hashed", n, want+1)
			}
			verifyAll(t, s)
		})
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
	// A serial batch. The executor hands the second call over as soon as
	// the first returns, racing the loop settling the first, so the
	// record may fall on either side of the second dispatch.
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{keeper, plain}, ToolExecution: agentturn.ExecSequential})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	// Only the recordable value is written, after its call's dispatch
	// and before the outputs; a Details value for subscribers alone is
	// not.
	switch got := entryTypes(s); got {
	case "run config item:user item:function_call* item:function_call* response dispatch dispatch custom item:function_call_output item:function_call_output item:assistant* response run",
		"run config item:user item:function_call* item:function_call* response dispatch custom dispatch item:function_call_output item:function_call_output item:assistant* response run":
	default:
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

// TestAskedNestedCallsAreRecorded pins #200's record: a nested call
// the hook deferred and the user answered through the elicitor has the
// question under the call that made it, then the answer as its
// decision, by the user. What the user said with the answer is on the
// answer entry, and a refusal's reaches the decision's reason.
func TestAskedNestedCallsAreRecorded(t *testing.T) {
	cases := []struct {
		name    string
		action  agenttool.Action
		note    string
		verdict string
	}{
		{name: "accept", action: agenttool.ActionAccept, verdict: agentsession.VerdictProceed},
		{name: "decline", action: agenttool.ActionDecline, verdict: agentsession.VerdictReject},
		{name: "decline, with a note", action: agenttool.ActionDecline, note: "push from CI", verdict: agentsession.VerdictReject},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			bash := agenttool.New("bash", "runs a command", func(context.Context, echoArgs) (string, error) { return "pushed", nil })
			eval := agenttool.New("eval", "runs code", func(ctx context.Context, _ echoArgs) (string, error) {
				_, err := agentturn.Invoke(ctx, "bash", json.RawMessage(`{"text":"git push"}`))
				if (err == nil) != (tc.action == agenttool.ActionAccept) {
					t.Errorf("the nested call returned %v", err)
				}
				return "ran", nil
			})
			policy := func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
				if info.Call.Name == "bash" {
					return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "ask bash(git push:*)", By: agentsession.ByPolicy}, nil
				}
				return nil, nil
			}
			user := func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) {
				return agenttool.Answer{Action: tc.action, Note: tc.note}, nil
			}
			a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{eval, bash},
				BeforeToolCall: policy, ToolElicitor: rec.Elicitor(agentsession.ByHuman, user), MaxTurns: 1})
			defer rec.Attach(a)()
			if _, err := a.Prompt(context.Background(), openresponses.UserText("x")); err != nil {
				t.Fatal(err)
			}
			var order []string
			var start *NestedCall
			var questions []*agentsession.CustomEntry
			for _, e := range s.Entries() {
				c, ok := e.(*agentsession.CustomEntry)
				if !ok {
					continue
				}
				switch c.NS {
				case ElicitationNS:
					questions = append(questions, c)
					order = append(order, elicitationOf(t, c).Phase)
				case NestedCallNS:
					var n NestedCall
					if err := json.Unmarshal(c.Data, &n); err != nil {
						t.Fatal(err)
					}
					order = append(order, n.Phase)
					if n.Phase == agentsession.RunStart {
						start = &n
					}
				}
			}
			// #213: the question is on the record before it is put, the
			// answer after, both before the nested call's own entries.
			if strings.Join(order, " ") != "ask answer start end" {
				t.Errorf("entries = %v", order)
			}
			if start == nil || start.Verdict != tc.verdict || start.By != agentsession.ByHuman || !strings.Contains(start.Reason, "ask bash(git push:*)") {
				t.Fatalf("start entry = %+v", start)
			}
			// The question names the nested call it is about, its tool
			// and the call that made it, and is filed under that call;
			// the answer names the question's entry and repeats the
			// call, so a reader with two questions in a row ties each
			// to its nested call without counting.
			parent := callsOf(t, s)["eval"]
			ask, answer := elicitationOf(t, questions[0]), elicitationOf(t, questions[1])
			if ask.Call != start.CallID || ask.Tool != "bash" || ask.Parent != parent.ID() || !strings.Contains(ask.Message, "git push") {
				t.Errorf("ask entry = %+v, want it about nested call %s (bash) under %s", ask, start.CallID, parent.ID())
			}
			if answer.Call != start.CallID || answer.Tool != "bash" || answer.Parent != parent.ID() || answer.Asked != questions[0].ID {
				t.Errorf("answer entry = %+v, want it about nested call %s naming ask entry %s", answer, start.CallID, questions[0].ID)
			}
			if answer.Action != string(tc.action) || answer.Note != tc.note || answer.By != agentsession.ByHuman {
				t.Errorf("answer entry = %+v, want %s by human with note %q", answer, tc.action, tc.note)
			}
			if tc.note != "" && !strings.Contains(start.Reason, tc.note) {
				t.Errorf("start entry's reason %q leaves out the note %q", start.Reason, tc.note)
			}
			if questions[0].CallID != parent.ID() || questions[1].CallID != parent.ID() {
				t.Errorf("the entries name calls %q and %q, want the invoking call %s", questions[0].CallID, questions[1].CallID, parent.ID())
			}
			// A reader of the old shape, which finds the answer by its
			// action, still finds exactly one.
			var answered int
			for _, q := range questions {
				if elicitationOf(t, q).Action != "" {
					answered++
				}
			}
			if answered != 1 {
				t.Errorf("%d entries carry an action, want the answer alone", answered)
			}
			verifyAll(t, s)
		})
	}
}

// TestOpenQuestionSurvivesACut pins #213: a nested call's question is
// on the record before it is put, so a run cut while the person was
// deciding leaves the question under the in-flight call and no answer,
// and a restart reads the call as waiting on a person: ReplayAnswers
// names the open question, and the call it was about, in the reason of
// the outcome-unknown answer it gives the call.
func TestOpenQuestionSurvivesACut(t *testing.T) {
	ctx := context.Background()
	store := &cutStore{Store: agentsession.NewMemoryStore()}
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	bash := agenttool.New("bash", "runs a command", func(context.Context, echoArgs) (string, error) { return "pushed", nil })
	eval := agenttool.New("eval", "runs code", func(ctx context.Context, _ echoArgs) (string, error) {
		_, err := agentturn.Invoke(ctx, "bash", json.RawMessage(`{"text":"git push"}`))
		return "ran", err
	})
	policy := func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
		if info.Call.Name == "bash" {
			return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "ask bash(git push:*)", By: agentsession.ByPolicy}, nil
		}
		return nil, nil
	}
	user := func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) {
		// The process dies while the person decides: nothing more of
		// the run reaches the record.
		store.cut.Store(true)
		return agenttool.Answer{Action: agenttool.ActionAccept}, nil
	}
	tools := []agenttool.Tool{eval, bash}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Tools: tools,
		BeforeToolCall: policy, ToolElicitor: rec.Elicitor(agentsession.ByHuman, user), MaxTurns: 1})
	unsub := rec.Attach(a)
	a.Prompt(ctx, openresponses.UserText("x"))
	unsub()
	store.cut.Store(false)
	if got := entryTypes(s); !strings.HasSuffix(got, "dispatch custom") {
		t.Fatalf("entries = %q, want the record to end at the open question", got)
	}
	questions := customs(s, ElicitationNS)
	if len(questions) != 1 {
		t.Fatalf("elicitation entries = %d", len(questions))
	}
	ask := elicitationOf(t, questions[0])
	parent := callsOf(t, s)["eval"]
	if ask.Phase != ElicitationAsk || ask.Call == "" || ask.Tool != "bash" || ask.Parent != parent.ID() || questions[0].CallID != parent.ID() {
		t.Errorf("ask entry = %+v under %q, want the question about the nested bash call under %s", ask, questions[0].CallID, parent.ID())
	}

	// A restart.
	rec, s, err = Resume(ctx, store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	pending, err := Pending(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Reason != agentturn.PendingAborted {
		t.Fatalf("pending = %+v, want the eval call in flight", pending)
	}
	answers, err := ReplayAnswers(ctx, s, tools)
	if err != nil {
		t.Fatal(err)
	}
	if len(answers) != 1 || answers[0].Output == nil {
		t.Fatalf("answers = %+v, want one output", answers)
	}
	want := fmt.Sprintf("not run again: replay unknown; a question was open: %q about call %s (bash)", ask.Message, ask.Call)
	if answers[0].Reason != want {
		t.Errorf("reason = %q, want %q", answers[0].Reason, want)
	}
	// The resume records the answer, with the reason, and verifies.
	opts, err := AgentOptions(s)
	if err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Tools: tools, BeforeToolCall: policy}, opts...)
	defer rec.Attach(b)()
	if end, err := b.Resume(ctx, answers...); err != nil || end.Reason != agentturn.ReasonDone {
		t.Fatalf("resume: err=%v end=%+v", err, end)
	}
	c := callsOf(t, s)["eval"]
	if c == nil || c.Output == nil || len(c.Decisions) != 1 || c.Decisions[0].Verdict != agentsession.VerdictAnswer || c.Decisions[0].Reason != want {
		t.Errorf("call on the new path = %+v", c)
	}
	verifyAll(t, s)
}

// TestElicitationAskWriteFailureIsTheHarnessFailing pins the other
// half of #213: when the ask entry cannot be written, nobody is asked
// and the tool gets the failure, so a question never put is never on
// the record as answered.
func TestElicitationAskWriteFailureIsTheHarnessFailing(t *testing.T) {
	ctx := context.Background()
	store := &failingCustomStore{Store: agentsession.NewMemoryStore(), ns: ElicitationNS}
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	asked := 0
	user := func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) {
		asked++
		return agenttool.Answer{Action: agenttool.ActionAccept}, nil
	}
	var got error
	tool := agenttool.New("prune", "", func(ctx context.Context, _ echoArgs) (string, error) {
		ask, _ := agenttool.ElicitorFrom(ctx)
		_, got = ask(ctx, agenttool.Elicitation{Message: "delete the branch?"})
		return "stopped", nil
	})
	a := agentturn.New(agentturn.Config{Model: allCalls{}, MaxTurns: 1, Tools: []agenttool.Tool{tool}, ToolElicitor: rec.Elicitor(agentsession.ByHuman, user)})
	defer rec.Attach(a)()
	if _, err := a.Prompt(ctx, openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	if got == nil || !strings.Contains(got.Error(), "disk full") {
		t.Errorf("the tool got %v, want the write failure", got)
	}
	if asked != 0 {
		t.Errorf("the user was asked %d times, want none", asked)
	}
	if n := len(customs(s, ElicitationNS)); n != 0 {
		t.Errorf("elicitation entries = %d, want none", n)
	}
}

// cutStore fails every append while cut is set: the process died, and
// nothing more of the run reaches the record.
type cutStore struct {
	agentsession.Store
	cut atomic.Bool
}

func (s *cutStore) Append(ctx context.Context, id string, e agentsession.Entry) (string, error) {
	if s.cut.Load() {
		return "", errors.New("the process died")
	}
	return s.Store.Append(ctx, id, e)
}

// failingCustomStore fails every append of a custom entry in ns.
type failingCustomStore struct {
	agentsession.Store
	ns string
}

func (s *failingCustomStore) Append(ctx context.Context, id string, e agentsession.Entry) (string, error) {
	if c, ok := e.(*agentsession.CustomEntry); ok && c.NS == s.ns {
		return "", errors.New("disk full")
	}
	return s.Store.Append(ctx, id, e)
}

// switching answers 429 while the request names the primary model and
// answers as itself once it names another.
type switching struct{ primary string }

func (m switching) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	if req.Model == m.primary {
		return openresponses.TooManyRequests("rate", "rate limited")
	}
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	msg, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if err := msg.Text("answered by " + req.Model); err != nil {
		return err
	}
	if err := msg.Close(); err != nil {
		return err
	}
	return em.Complete()
}

// TestRevisedRetryIsAConfigDelta checks that a turn that moved to a
// fallback model records the model that answered: the settings in force
// at the response are the revised request's, and the path still
// rebuilds the request it hashed.
func TestRevisedRetryIsAConfigDelta(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: switching{primary: "a"}, ModelName: "a", Retry: agentturn.Retry{
		MaxAttempts: 3,
		Backoff:     func(int, error) time.Duration { return 0 },
		Revise: func(_ int, req *openresponses.Request, _ error) *openresponses.Request {
			req.Model = "b"
			return nil
		},
	}})
	defer rec.Attach(a)()
	end, err := a.Prompt(context.Background(), openresponses.UserText("go"))
	if err != nil || end.Reason != agentturn.ReasonDone {
		t.Fatalf("err=%v end=%+v", err, end)
	}
	if n := verifyAll(t, s); n != 1 || hashed(s) != 1 {
		t.Errorf("responses = %d hashed = %d", n, hashed(s))
	}
	cx, err := s.Context()
	if err != nil {
		t.Fatal(err)
	}
	if cx.Settings.Model != "b" {
		t.Errorf("settings name %q, the model that did not answer", cx.Settings.Model)
	}
	for _, e := range s.Entries() {
		if r, ok := e.(*agentsession.ResponseEntry); ok && r.Model != "b" {
			t.Errorf("response entry model = %q", r.Model)
		}
	}
	// The run opened with the agent's configuration and the switch is
	// one delta on top of it: not one config per attempt, since the
	// attempt that failed answered nothing and wrote nothing.
	cfgs := configs(s)
	if len(cfgs) != 2 || cfgs[0].Model != "a" || cfgs[1].Model != "b" {
		t.Errorf("config entries = %d in %q", len(cfgs), entryTypes(s))
	}
}

// chainLeg streams a reasoning item and then fails, unless the request
// names the last leg of the chain, in which case it answers: a
// provider under load behind a fallback chain.
type chainLeg struct{ last string }

func (m chainLeg) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	w, err := em.Reasoning()
	if err != nil {
		return err
	}
	if err := w.Summary("weighing the options"); err != nil {
		return err
	}
	if err := w.EndSummary(); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	if req.Model != m.last {
		return openresponses.ServerError("overloaded", "overloaded")
	}
	msg, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if err := msg.Text("answered by " + req.Model); err != nil {
		return err
	}
	if err := msg.Close(); err != nil {
		return err
	}
	return em.Complete()
}

// TestOnlyTheAttemptThatAnsweredIsConfigured checks that an attempt
// which streamed and then failed leaves no settings on the path: it
// answered nothing, so a config entry describing it would tell a reader
// the conversation ran under a model it never got an answer from.
func TestOnlyTheAttemptThatAnsweredIsConfigured(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	next := map[int]string{1: "b", 2: "c"}
	a := agentturn.New(agentturn.Config{Model: chainLeg{last: "c"}, ModelName: "a", Retry: agentturn.Retry{
		MaxAttempts: 3,
		Backoff:     func(int, error) time.Duration { return 0 },
		Revise: func(attempt int, req *openresponses.Request, _ error) *openresponses.Request {
			req.Model = next[attempt]
			return nil
		},
	}})
	defer rec.Attach(a)()
	end, err := a.Prompt(context.Background(), openresponses.UserText("go"))
	if err != nil || end.Reason != agentturn.ReasonDone {
		t.Fatalf("err=%v end=%+v", err, end)
	}
	cfgs := configs(s)
	if len(cfgs) != 2 || cfgs[0].Model != "a" || cfgs[1].Model != "c" {
		var models []string
		for _, c := range cfgs {
			models = append(models, c.Model)
		}
		t.Errorf("config entries name %v in %q", models, entryTypes(s))
	}
	// The attempts that failed left nothing else either.
	if n := verifyAll(t, s); n != 1 || hashed(s) != 1 {
		t.Errorf("responses = %d hashed = %d", n, hashed(s))
	}
	cx, err := s.Context()
	if err != nil {
		t.Fatal(err)
	}
	if cx.Settings.Model != "c" || len(cx.Items) != 3 {
		t.Errorf("context = %+v, %d items", cx.Settings, len(cx.Items))
	}
}

// sameIDModel makes, on each turn, one call to upper per ID in turns,
// then answers with nothing once the turns run out, as a provider that
// numbers its calls per response does. With doneOnly it sends each
// call's output_item.done alone, as a relay that passes on only
// finished items does.
type sameIDModel struct {
	turns    [][]string
	doneOnly bool
	calls    int
}

func (m *sameIDModel) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.calls++
	if m.doneOnly {
		next := sink
		sink = openresponses.EventSinkFunc(func(ev openresponses.StreamEvent) error {
			switch ev.(type) {
			case *openresponses.OutputItemAddedEvent, *openresponses.FunctionCallArgumentsDeltaEvent, *openresponses.FunctionCallArgumentsDoneEvent:
				return nil
			}
			return next.Send(ev)
		})
	}
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if m.calls <= len(m.turns) {
		for _, id := range m.turns[m.calls-1] {
			w, err := em.FunctionCall(id, "upper")
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
	}
	return em.Complete()
}

// TestRepeatedCallIDsAreRecorded checks that a model repeating a call
// ID records a session: the format refuses a function call whose ID is
// already on the path, and the loop gives such a call an ID of its
// own before the recorder writes it, a call the loop holds until its
// response arrives included.
func TestRepeatedCallIDsAreRecorded(t *testing.T) {
	cases := []struct {
		name     string
		turns    [][]string
		doneOnly bool
	}{
		{name: "across turns", turns: [][]string{{"call_0"}, {"call_0"}}},
		{name: "in a response", turns: [][]string{{"call_0", "call_0"}}},
		{name: "in a response, done only", turns: [][]string{{"c1", "c1"}}, doneOnly: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{Records: agentsession.AllRecords})
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(agentturn.Config{Model: &sameIDModel{turns: tc.turns, doneOnly: tc.doneOnly}, Tools: []agenttool.Tool{upper}})
			defer rec.Attach(a)()
			if end, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil || end.Reason != agentturn.ReasonDone {
				t.Fatalf("prompt: err=%v end=%+v", err, end)
			}
			calls, err := s.Calls(s.Leaf())
			if err != nil || len(calls) != 2 {
				t.Fatalf("calls = %d, %v", len(calls), err)
			}
			for _, c := range calls {
				if c.Output == nil || c.Dispatch == nil {
					t.Errorf("call %s = %+v", c.ID(), c)
				}
			}
			verifyAll(t, s)
			if err := s.VerifyRecords(s.Leaf()); err != nil {
				t.Errorf("verify records: %v", err)
			}
		})
	}
}

// perResponseModel numbers its calls per response, as some providers
// do: it calls upper as call_0 unless the input ends with an output,
// which it answers.
type perResponseModel struct{}

func (perResponseModel) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if _, ok := req.Input[len(req.Input)-1].(*openresponses.FunctionCallOutput); ok {
		if err := em.Item(openresponses.AssistantText("ok")); err != nil {
			return err
		}
		return em.Complete()
	}
	w, err := em.FunctionCall("call_0", "upper")
	if err != nil {
		return err
	}
	if err := w.Arguments(`{"text":"t"}`); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Complete()
}

// TestFoldedCallIDsStayReserved checks that an agent seeded from a
// session's context after a fold does not reuse the ID of a call the
// fold left out: the calls are still on the path, whose format refuses
// a repeated call ID. AgentOptions reserves them for a resume, and
// Rebase for the agent it is attached to. The renamed call keeps the
// model's ID beside its item.
func TestFoldedCallIDsStayReserved(t *testing.T) {
	for _, reseed := range []string{"resume", "rebase"} {
		t.Run(reseed, func(t *testing.T) {
			ctx := context.Background()
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(ctx, store, agentsession.Header{Records: agentsession.AllRecords})
			if err != nil {
				t.Fatal(err)
			}
			tr := compact.NewLocal(&echo.Adapter{}, compact.WithBudget(4), compact.WithKeepLast(2), compact.WithEstimator(countItems), compact.WithOnFold(rec.Fold))
			cfg := agentturn.Config{Model: perResponseModel{}, ModelName: "m", Tools: []agenttool.Tool{upper}, Transform: tr.Transform}
			a := agentturn.New(cfg)
			detach := rec.Attach(a)
			for _, text := range []string{"one", "two", "three"} {
				if _, err := a.Prompt(ctx, openresponses.UserText(text)); err != nil {
					t.Fatal(err)
				}
			}
			switch reseed {
			case "resume":
				detach()
				var opts []agentturn.Option
				if rec, s, err = Resume(ctx, store, s.ID()); err != nil {
					t.Fatal(err)
				}
				if opts, err = AgentOptions(s); err != nil {
					t.Fatal(err)
				}
				a = agentturn.New(cfg, opts...)
				detach = rec.Attach(a)
			case "rebase":
				if err := rec.Rebase(s, s.Leaf()); err != nil {
					t.Fatal(err)
				}
				cx, err := s.Context()
				if err != nil {
					t.Fatal(err)
				}
				if err := a.SetTranscript(cx.Items); err != nil {
					t.Fatal(err)
				}
			}
			defer detach()
			cx, err := s.Context()
			if err != nil {
				t.Fatal(err)
			}
			for _, item := range cx.Items {
				if call, ok := item.(*openresponses.FunctionCall); ok && call.CallID == "call_0" {
					t.Fatal("the fold kept call_0; the test needs it folded away")
				}
			}
			if _, err := a.Prompt(ctx, openresponses.UserText("four")); err != nil {
				t.Fatal(err)
			}
			if err := s.VerifyRecords(s.Leaf()); err != nil {
				t.Errorf("verify records: %v", err)
			}
			renamed := 0
			for _, e := range s.Path(s.Leaf()) {
				if ie, ok := e.(*agentsession.ItemEntry); ok {
					if _, ok := ie.Item.(*openresponses.FunctionCall); ok && ie.Unknown[ModelCallIDMember] != nil {
						renamed++
						if string(ie.Unknown[ModelCallIDMember]) != `"call_0"` {
							t.Errorf("model call ID = %s", ie.Unknown[ModelCallIDMember])
						}
					}
				}
			}
			if renamed != 3 {
				t.Errorf("%d calls keep the model's ID, want the three after the first", renamed)
			}
		})
	}
}

// TestEnvSurvivesACompaction pins the review of #197: the env in force
// is the last on the path, one a compaction left out of the context
// included, so a child reseeded for a second run under its call is
// under the node it moved to, not its parent's copied in again, and a
// root resumed after a compaction writes no second copy of its env.
func TestEnvSurvivesACompaction(t *testing.T) {
	ctx := context.Background()
	env := func(context.Context) (*agentsession.EnvEntry, error) { return envOn("node-1"), nil }
	cases := []struct {
		name string
		// child writes the env and the compaction in a child session,
		// which has moved to node-2; otherwise in the root.
		child bool
		want  string
	}{
		{name: "a reseeded child", child: true, want: "node-2"},
		{name: "a resumed root", want: "node-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(ctx, store, agentsession.Header{}, WithEnv(env))
			if err != nil {
				t.Fatal(err)
			}
			id, written := s.ID(), envOn("node-1")
			const callID = "call_x"
			if tc.child {
				if err := rec.root.writeEnv(ctx); err != nil {
					t.Fatal(err)
				}
				cs, err := store.Create(ctx, agentsession.Header{ParentSession: s.ID(), ID: agentsession.SubsessionID(s.ID(), callID), SpawnedBy: callID})
				if err != nil {
					t.Fatal(err)
				}
				id, written = cs.ID(), envOn("node-2")
			}
			var kept string
			for i, e := range []agentsession.Entry{written, &agentsession.ItemEntry{Item: openresponses.UserText("one")}, &agentsession.ItemEntry{Item: openresponses.UserText("two")}} {
				at, err := store.Append(ctx, id, e)
				if err != nil {
					t.Fatal(err)
				}
				if i == 2 {
					kept = at
				}
			}
			if _, err := store.Append(ctx, id, &agentsession.CompactionEntry{FirstKept: kept, Summary: openresponses.UserText("summary")}); err != nil {
				t.Fatal(err)
			}

			var w *writer
			if tc.child {
				if w, err = rec.newChild(ctx, rec.root, callID, false); err != nil {
					t.Fatal(err)
				}
			} else {
				rec2, _, err := Resume(ctx, store, id, WithEnv(env))
				if err != nil {
					t.Fatal(err)
				}
				w = rec2.root
			}
			if err := w.runStart(ctx, &agentturn.RunStart{RunID: "run_2", Source: agentturn.SourceInput}); err != nil {
				t.Fatal(err)
			}
			got, err := store.Open(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if nodes := envNodes(got.Entries()); nodes != tc.want {
				t.Errorf("env entries = %q, want %q alone", nodes, tc.want)
			}
		})
	}
}
