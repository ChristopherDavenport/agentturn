package session

import (
	"context"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// TestDispatchIsWrittenPerCall pins agentturn #93 on the record: in a
// serial batch cut while its first call runs, the first call has its
// dispatch and the second, which never reached a tool, has none and
// reads as never started.
func TestDispatchIsWrittenPerCall(t *testing.T) {
	store := ctxStore{agentsession.NewMemoryStore()}
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	blocking := agenttool.New("first", "", func(ctx context.Context, _ echoArgs) (string, error) {
		started <- struct{}{}
		<-ctx.Done()
		return "", ctx.Err()
	})
	plain := agenttool.New("second", "", func(_ context.Context, _ echoArgs) (string, error) { return "ran", nil })
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{blocking, plain}, ToolExecution: agentturn.ExecSequential})
	defer rec.Attach(a)()
	done := make(chan *agentturn.RunEnd, 1)
	go func() {
		end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
		done <- end
	}()
	<-started
	a.Abort()
	end := <-done
	if end == nil || end.Reason != agentturn.ReasonAborted || len(end.Pending) != 2 {
		t.Fatalf("end = %+v", end)
	}
	if got := entryTypes(s); got != "run config item:user item:function_call* item:function_call* response dispatch run" {
		t.Errorf("entries = %q", got)
	}
	verifyAll(t, s)
	calls := callsOf(t, s)
	if st := calls["first"].State(s.Header()); st != agentsession.CallInFlight {
		t.Errorf("first state = %v, want in flight", st)
	}
	if st := calls["second"].State(s.Header()); st != agentsession.CallNeverStarted {
		t.Errorf("second state = %v, want never started", st)
	}
}

// TestLoopRefusalIsARejectByPolicy: a call naming no tool is settled
// by the loop and its output appended; the record says the loop
// refused it, as a policy's reject, and holds no dispatch.
func TestLoopRefusalIsARejectByPolicy(t *testing.T) {
	store := ctxStore{agentsession.NewMemoryStore()}
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: nil, ToolProvider: func(context.Context) []agenttool.Tool { return nil }, MaxTurns: 1})
	defer rec.Attach(a)()
	// allCalls calls every offered tool; offer one the loop cannot find.
	a.SetConfig(agentturn.Config{Model: callsUnknown{}, MaxTurns: 1})
	if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	verifyAll(t, s)
	c := callsOf(t, s)["ghost"]
	if c == nil || c.Dispatch != nil || c.Output == nil || len(c.Decisions) != 1 || c.Decisions[0].Verdict != agentsession.VerdictReject || c.Decisions[0].By != agentsession.ByPolicy {
		t.Fatalf("call = %+v", c)
	}
}

// callsUnknown is a model that calls a tool nobody offered.
type callsUnknown struct{}

func (callsUnknown) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	w, err := em.FunctionCall("", "ghost")
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

// TestRecordFuncWritesDuringTheCall pins agentturn #97 on the record:
// a tool's WriteRecord reaches the session as a custom entry in the
// record's namespace, between the call's dispatch and its output.
func TestRecordFuncWritesDuringTheCall(t *testing.T) {
	store := ctxStore{agentsession.NewMemoryStore()}
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	tool := agenttool.New("fork", "", func(ctx context.Context, _ echoArgs) (string, error) {
		if err := agenttool.WriteRecord(ctx, pgRecord{PGID: 7}); err != nil {
			return "", err
		}
		return "forked", nil
	})
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{tool}, MaxTurns: 1, ToolRecorder: rec.RecordFunc()})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	if got := entryTypes(s); got != "run config item:user item:function_call* response dispatch custom item:function_call_output run" {
		t.Errorf("entries = %q", got)
	}
	verifyAll(t, s)
}

type pgRecord struct{ PGID int }

func (pgRecord) RecordNS() string { return "shell:process-group" }

// TestApprovedCallCutBeforeDispatch records the shape agentsession #78
// asks about: two calls approved on resume run as a serial batch, and
// an abort while the first runs leaves the second with its proceed and
// no dispatch, reading as never started; the caller's later answer is
// then a reject that names them.
func TestApprovedCallCutBeforeDispatch(t *testing.T) {
	store := ctxStore{agentsession.NewMemoryStore()}
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	blocking := agenttool.New("first", "", func(ctx context.Context, _ echoArgs) (string, error) {
		started <- struct{}{}
		<-ctx.Done()
		return "", ctx.Err()
	})
	plain := agenttool.New("second", "", func(_ context.Context, _ echoArgs) (string, error) { return "ran", nil })
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{blocking, plain}, ToolExecution: agentturn.ExecSequential,
		BeforeToolCall: func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			return &agentturn.ToolDecision{Action: agentturn.Defer}, nil
		}})
	defer rec.Attach(a)()
	end, err := a.Prompt(context.Background(), openresponses.UserText("go"))
	if err != nil || end.Reason != agentturn.ReasonInputRequired || len(end.Pending) != 2 {
		t.Fatalf("first run: err=%v end=%+v", err, end)
	}
	done := make(chan *agentturn.RunEnd, 1)
	go func() {
		end, _ := a.Resume(context.Background(), agentturn.Approve(end.Pending[0].Call.CallID).WithBy(agentsession.ByHuman), agentturn.Approve(end.Pending[1].Call.CallID).WithBy(agentsession.ByHuman))
		done <- end
	}()
	<-started
	a.Abort()
	end = <-done
	if end == nil || end.Reason != agentturn.ReasonAborted || len(end.Pending) != 2 {
		t.Fatalf("resume: end=%+v", end)
	}
	verifyAll(t, s)
	calls := callsOf(t, s)
	first, second := calls["first"], calls["second"]
	if first.Dispatch == nil || first.State(s.Header()) != agentsession.CallInFlight {
		t.Errorf("first: dispatch=%v state=%v", first.Dispatch != nil, first.State(s.Header()))
	}
	if second.Dispatch != nil || second.State(s.Header()) != agentsession.CallNeverStarted || len(second.Decisions) != 2 || second.Decisions[1].Verdict != agentsession.VerdictProceed || second.Decisions[1].By != agentsession.ByHuman {
		t.Errorf("second: %+v", second)
	}
}
