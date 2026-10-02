package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// gate is a model that answers once released, or fails with its
// context's cause.
type gate struct{ release chan struct{} }

func (g gate) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	select {
	case <-g.release:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	return (&echo.Adapter{}).CreateStream(ctx, req, sink)
}

// TestDetachReturnsOnceTheChildStarts pins #127: a detached call
// returns while the child works, the parent's run goes on and ends, the
// child's end reaches fn once, and an abort of the parent's run while
// it is still going cuts the child.
func TestDetachReturnsOnceTheChildStarts(t *testing.T) {
	t.Run("the child outlives the parent's run", func(t *testing.T) {
		release := make(chan struct{})
		ends := make(chan *agentturn.RunEnd, 2)
		var calls []string
		var mu sync.Mutex
		child := New(agentturn.Config{Name: "worker", Model: gate{release}},
			WithDetach(func(callID string, end *agentturn.RunEnd) {
				mu.Lock()
				calls = append(calls, callID)
				mu.Unlock()
				ends <- end
			}))
		parent := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{child}})
		pend, err := parent.Prompt(context.Background(), openresponses.UserText("start it"))
		if err != nil {
			t.Fatal(err)
		}
		var out *openresponses.FunctionCallOutput
		for _, item := range pend.Items {
			if o, ok := item.(*openresponses.FunctionCallOutput); ok {
				out = o
			}
		}
		if out == nil || out.Output.Text == "" {
			t.Fatalf("the call's output = %+v", out)
		}
		close(release)
		select {
		case end := <-ends:
			if end.Reason != agentturn.ReasonDone {
				t.Errorf("child end = %s %v", end.Reason, end.Err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("the child's end never reached fn")
		}
		mu.Lock()
		defer mu.Unlock()
		if len(calls) != 1 || calls[0] != out.CallID {
			t.Errorf("fn called for %v, want once for %s", calls, out.CallID)
		}
	})
	t.Run("an abort of the parent cuts it", func(t *testing.T) {
		stop := errors.New("stop everything")
		ends := make(chan *agentturn.RunEnd, 1)
		child := New(agentturn.Config{Name: "worker", Model: gate{make(chan struct{})}},
			WithDetach(func(_ string, end *agentturn.RunEnd) { ends <- end }))
		started := make(chan struct{})
		// The parent's second call waits, so the parent's run is still
		// going when the child has started.
		wait := agenttool.New("hold", "", func(ctx context.Context, _ struct{}) (string, error) {
			close(started)
			<-ctx.Done()
			return "", ctx.Err()
		})
		parent := agentturn.New(agentturn.Config{Model: everyTool{}, Tools: []agenttool.Tool{child, wait}, ToolExecution: agentturn.ExecSequential})
		go func() {
			<-started
			parent.AbortCause(stop)
		}()
		if _, err := parent.Prompt(context.Background(), openresponses.UserText("start it")); err != nil {
			t.Fatal(err)
		}
		select {
		case end := <-ends:
			if end.Reason != agentturn.ReasonAborted || !errors.Is(end.Err, stop) {
				t.Errorf("child end = %s %v", end.Reason, end.Err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("the parent's abort did not reach the detached child")
		}
	})
}

// TestRetryMarkReachesTheObserver pins #122 and #87: the run Execute
// starts carries the ContextWithRetry mark to the observer, since the
// child is a fresh agent, as does a host that prompts a spawned child
// again with ContextWithRetry, so a recorder starts the child's session
// afresh for either; a host's plain prompt carries none and continues
// it.
func TestRetryMarkReachesTheObserver(t *testing.T) {
	var handle *agentturn.Agent
	var marks []bool
	child := New(agentturn.Config{Name: "helper", Model: &echo.Adapter{}},
		WithSpawn(func(_ string, a *agentturn.Agent) { handle = a }),
		WithObserver(func(ctx context.Context, ev agentturn.Event) {
			if _, ok := ev.(*agentturn.RunStart); ok {
				marks = append(marks, RetryFromContext(ctx))
			}
		}))
	if _, err := child.Execute(context.Background(), agenttool.Call{ID: "c1", Args: []byte(`{"input":"x"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Prompt(ContextWithRetry(context.Background()), openresponses.UserText("again, from scratch")); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Prompt(context.Background(), openresponses.UserText("and continue")); err != nil {
		t.Fatal(err)
	}
	if len(marks) != 3 || !marks[0] || !marks[1] || marks[2] {
		t.Errorf("retry marks by run = %v, want [true true false]", marks)
	}
}

// everyTool calls every tool it is offered, then answers once the
// outputs are in.
type everyTool struct{}

func (everyTool) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	if _, ok := req.Input[len(req.Input)-1].(*openresponses.FunctionCallOutput); ok {
		return (&echo.Adapter{}).CreateStream(ctx, req, sink)
	}
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	for _, tl := range req.Tools {
		w, err := em.FunctionCall("", tl.(*openresponses.FunctionTool).Name)
		if err != nil {
			return err
		}
		if err := w.Arguments(`{"input":"x"}`); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
	}
	return em.Complete()
}
