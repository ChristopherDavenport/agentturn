package agentturn

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

type hostKey struct{}

// TestRunContextOutlivesTheBatch pins #125: a tool's RunContext carries
// the run's values and is not cancelled when the batch or the run ends
// by itself, and is cancelled by an abort of the run, with its cause.
func TestRunContextOutlivesTheBatch(t *testing.T) {
	var rc context.Context
	var callCtx context.Context
	grab := agenttool.New("grab", "", func(ctx context.Context, _ echoArgs) (string, error) {
		rc, _ = RunContext(ctx)
		callCtx = ctx
		return "ok", nil
	})
	a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{grab}})
	end, err := a.Prompt(context.WithValue(context.Background(), hostKey{}, "host"), openresponses.UserText("go"))
	if err != nil {
		t.Fatal(err)
	}
	if rc == nil {
		t.Fatal("no run context on the call")
	}
	if rc.Err() != nil {
		t.Errorf("the run context ended with the run: %v", rc.Err())
	}
	if callCtx.Err() == nil {
		t.Error("the call's own context outlived its batch")
	}
	if RunIDFromContext(rc) != end.RunID || rc.Value(hostKey{}) != "host" {
		t.Errorf("run context values: run %q host %v", RunIDFromContext(rc), rc.Value(hostKey{}))
	}
	if _, ok := TranscriptFromContext(rc); ok {
		t.Error("the run context carries the batch's transcript")
	}
	if _, ok := RunContext(context.Background()); ok {
		t.Error("RunContext outside a loop")
	}

	// An abort of the run cuts it.
	stop := errors.New("user pressed escape")
	started := make(chan struct{})
	block := agenttool.New("grab", "", func(ctx context.Context, _ echoArgs) (string, error) {
		rc, _ = RunContext(ctx)
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	})
	b := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{block}})
	go func() {
		<-started
		b.AbortCause(stop)
	}()
	if _, err := b.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-rc.Done():
		if !errors.Is(context.Cause(rc), stop) {
			t.Errorf("cause = %v", context.Cause(rc))
		}
	case <-time.After(time.Second):
		t.Fatal("an abort did not cut the run context")
	}
}

// TestRunContextOfTheLowLevelLoop checks that the low-level loop's run
// context is cut when the consumer breaks out.
func TestRunContextOfTheLowLevelLoop(t *testing.T) {
	var rc context.Context
	grab := agenttool.New("grab", "", func(ctx context.Context, _ echoArgs) (string, error) {
		rc, _ = RunContext(ctx)
		return "ok", nil
	})
	for ev := range Run(context.Background(), nil, openresponses.Items{openresponses.UserText("go")}, Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{grab}}) {
		if _, ok := ev.(*ToolEnd); ok {
			break
		}
	}
	if rc == nil {
		t.Fatal("no run context on the call")
	}
	select {
	case <-rc.Done():
	case <-time.After(time.Second):
		t.Fatal("breaking out did not cut the run context")
	}
}

// TestSteeredReachesAWaitingTool pins #126: a tool waiting on Steered
// returns when an item is steered into its run, the item then joins
// the run after the batch, and the next batch listens afresh.
func TestSteeredReachesAWaitingTool(t *testing.T) {
	waiting := make(chan struct{}, 2)
	heard := 0
	wait := agenttool.New("wait", "", func(ctx context.Context, _ echoArgs) (string, error) {
		ch := Steered(ctx)
		if ch == nil {
			return "", errors.New("no steer signal")
		}
		waiting <- struct{}{}
		select {
		case <-ch:
			heard++
			return "interrupted by a steer", nil
		case <-time.After(50 * time.Millisecond):
			return "waited", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	a := New(Config{Model: callsTwice{}, Tools: []agenttool.Tool{wait}})
	go func() {
		<-waiting
		a.Steer(openresponses.UserText("stop waiting"))
	}()
	end, err := a.Prompt(context.Background(), openresponses.UserText("go"))
	if err != nil {
		t.Fatal(err)
	}
	if heard != 1 {
		t.Errorf("the tool heard %d steers, want 1: the second batch must listen afresh", heard)
	}
	found := false
	for _, item := range end.Items {
		if m, ok := item.(*openresponses.Message); ok && m.Text() == "stop waiting" {
			found = true
		}
	}
	if !found {
		t.Error("the steered item did not join the run")
	}
	if Steered(context.Background()) != nil {
		t.Error("Steered outside a loop")
	}
}

// callsTwice calls every tool it is offered on each of its first two
// turns, then answers.
type callsTwice struct{}

func (callsTwice) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	outputs := 0
	for _, item := range req.Input {
		if _, ok := item.(*openresponses.FunctionCallOutput); ok {
			outputs++
		}
	}
	if outputs >= 2 {
		return (&echo.Adapter{}).CreateStream(ctx, openresponses.Request{Model: req.Model, Input: openresponses.Items{openresponses.UserText("done")}}, sink)
	}
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	for _, tl := range req.Tools {
		w, err := em.FunctionCall("", tl.(*openresponses.FunctionTool).Name)
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

// TestPendingCallCarriesItsTool pins #112: a pending call carries the
// tool it resolved to, and a tool_end says why a call was blocked or
// deferred.
func TestPendingCallCarriesItsTool(t *testing.T) {
	tool := agenttool.New("upper", "", upper)
	for _, tc := range []struct {
		name   string
		action ToolAction
	}{{"deferred", Defer}, {"blocked", Block}} {
		t.Run(tc.name, func(t *testing.T) {
			a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{tool}, MaxTurns: 1,
				BeforeToolCall: func(context.Context, ToolCallInfo) (*ToolDecision, error) {
					return &ToolDecision{Action: tc.action, Reason: "rule 7"}, nil
				}})
			var ends []*ToolEnd
			a.Subscribe(func(_ context.Context, ev Event) error {
				if e, ok := ev.(*ToolEnd); ok {
					ends = append(ends, e)
				}
				return nil
			})
			end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
			if len(ends) != 1 || ends[0].Reason != "rule 7" {
				t.Fatalf("tool_end = %+v", ends)
			}
			if tc.action == Defer && (len(end.Pending) != 1 || end.Pending[0].Tool != tool) {
				t.Errorf("pending = %+v", end.Pending)
			}
		})
	}
}
