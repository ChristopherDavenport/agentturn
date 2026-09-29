package session

import (
	"context"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// held answers once released.
type held struct{ release chan struct{} }

func (h held) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	select {
	case <-h.release:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	return (&echo.Adapter{}).CreateStream(ctx, req, sink)
}

// TestDetachedChildIsRecordedToItsEnd checks that a child whose call
// returned while it ran on, tools/agent's WithDetach, is written to its
// own session through its end, after the parent's run has ended, and
// that its writer is released then.
func TestDetachedChildIsRecordedToItsEnd(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	ended := make(chan *agentturn.RunEnd, 1)
	worker := agent.New(agentturn.Config{Name: "worker", Model: held{release}},
		agent.WithObserver(rec.Observe),
		agent.WithDetach(func(_ string, end *agentturn.RunEnd) { ended <- end }))
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{worker}})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("start it")); err != nil {
		t.Fatal(err)
	}
	l := links(s)
	if len(l) != 1 {
		t.Fatalf("links = %+v", l)
	}
	close(release)
	select {
	case <-ended:
	case <-time.After(2 * time.Second):
		t.Fatal("the child never ended")
	}
	cs, err := store.Open(context.Background(), l[0].Session)
	if err != nil {
		t.Fatal(err)
	}
	runs := runsOf(t, cs)
	if len(runs) != 1 || runs[0].End == nil || runs[0].End.Reason != agentsession.ReasonDone {
		t.Fatalf("child runs = %+v in %q", runs, entryTypes(cs))
	}
	if n := verifyAll(t, cs); n != 1 {
		t.Errorf("child responses = %d", n)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if _, ok := rec.runs[runs[0].RunID()]; ok {
		t.Error("the detached child's writer outlived its run")
	}
}
