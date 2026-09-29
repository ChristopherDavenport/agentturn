package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// cutRun writes the start of a run and its first item into a new
// session and stops there, as a process killed inside the run leaves
// it, with an input queued during the run and never appended.
func cutRun(t *testing.T, store agentsession.Store) *agentsession.Session {
	t.Helper()
	ctx := context.Background()
	s, err := store.Create(ctx, agentsession.Header{Records: append(append([]string(nil), agentsession.AllRecords...), agentsession.TypeQueued)})
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := agentsession.ConfigFromRequest(openresponses.Request{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []agentsession.Entry{
		agentsession.NewRunStart("run_cut", agentsession.SourceInput, ""),
		cfg,
		&agentsession.ItemEntry{Item: openresponses.UserText("hello")},
		agentsession.NewQueued(openresponses.UserText("and then this"), agentsession.ModeFollowUp).WithTrigger("cron", "03:00", "scheduler"),
	} {
		if _, err := store.Append(ctx, s.ID(), e); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// TestResumeClosesTheRunACutLeftOpen pins #120 and #94 by the rule
// agentsession #86 settled: the writer that continues a path with an
// open run it is not running closes it, with error after a cut; the
// inputs the cut run owed are queued again after the end, and Requeue
// hands them to the agent, whose next run drains them naming the entry.
func TestResumeClosesTheRunACutLeftOpen(t *testing.T) {
	store := agentsession.NewMemoryStore()
	s := cutRun(t, store)
	rec, s, err := Resume(context.Background(), store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	runs := runsOf(t, s)
	if len(runs) != 1 || runs[0].End == nil || runs[0].End.Reason != agentsession.ReasonError || !strings.Contains(runs[0].End.Ref, "cut off") {
		t.Fatalf("runs = %+v", runs)
	}
	owed, err := s.PendingQueued(s.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	if len(owed) != 1 || owed[0].Trigger == nil || owed[0].Trigger.Ref != "03:00" {
		t.Fatalf("owed after resume = %+v", owed)
	}
	if err := s.VerifyRecords(s.Leaf()); err != nil {
		t.Errorf("verify records: %v", err)
	}

	// A second resume finds the run closed and appends nothing.
	before := len(s.Entries())
	if _, _, err := Resume(context.Background(), store, s.ID()); err != nil {
		t.Fatal(err)
	}
	if n := len(s.Entries()); n != before {
		t.Errorf("a resume of a closed run appended %d entries", n-before)
	}

	cx, err := s.Context()
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m"}, agentturn.WithTranscript(cx.Items))
	defer rec.Attach(a)()
	if n := rec.Requeue(context.Background(), a); n != 1 {
		t.Fatalf("requeued %d", n)
	}
	if n := rec.Requeue(context.Background(), a); n != 0 {
		t.Errorf("requeued %d a second time", n)
	}
	if st := a.State(); st.FollowUps != 1 {
		t.Fatalf("follow-ups = %d", st.FollowUps)
	}
	if _, err := a.Prompt(context.Background(), openresponses.UserText("go on")); err != nil {
		t.Fatal(err)
	}
	var drained *agentsession.ItemEntry
	queuedEntries := 0
	for _, e := range s.Entries() {
		switch v := e.(type) {
		case *agentsession.ItemEntry:
			if v.QueuedFrom != "" {
				drained = v
			}
		case *agentsession.QueuedEntry:
			queuedEntries++
		}
	}
	if drained == nil || drained.QueuedFrom != owed[0].ID || drained.Source == nil || drained.Source.Kind != "cron" {
		t.Errorf("drained item = %+v, want queued_from %s", drained, owed[0].ID)
	}
	// The one written by the cut run and the one written after its end;
	// the handed-back input's queued event writes nothing.
	if queuedEntries != 2 {
		t.Errorf("queued entries = %d in %q", queuedEntries, entryTypes(s))
	}
	if owed, _ := s.PendingQueued(s.Leaf()); len(owed) != 0 {
		t.Errorf("still owed = %+v", owed)
	}
	if err := s.VerifyRecords(s.Leaf()); err != nil {
		t.Errorf("verify records: %v", err)
	}
}

// blockingModel calls "wait" once, then answers.
type blockingModel struct{}

func (blockingModel) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	if _, ok := req.Input[len(req.Input)-1].(*openresponses.FunctionCallOutput); ok {
		return (&echo.Adapter{}).CreateStream(ctx, req, sink)
	}
	return allCalls{}.CreateStream(ctx, req, sink)
}

// TestQueuedInputsAreDurable pins #67: an input accepted while a run is
// in flight is a queued entry before anything appends it; an abort's
// run end closes that entry and the recorder writes it again, since the
// agent still holds the input; the run that drains it names the entry.
// An item steered while the agent is idle is recorded at the next run's
// start, before the run.
func TestQueuedInputsAreDurable(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	wait := agenttool.New("wait", "", func(ctx context.Context, _ echoArgs) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	})
	a := agentturn.New(agentturn.Config{Model: blockingModel{}, Tools: []agenttool.Tool{wait}})
	defer rec.Attach(a)()
	done := make(chan error, 1)
	go func() {
		_, err := a.Prompt(context.Background(), openresponses.UserText("start"))
		done <- err
	}()
	<-started
	a.Queue(agentturn.ContextWithTrigger(context.Background(), agentturn.Trigger{Kind: "cron", Ref: "03:00"}), agentturn.QueueFollowUp, openresponses.UserText("the 03:00 firing"))
	a.Abort()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	owed, err := s.PendingQueued(s.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	if len(owed) != 1 || owed[0].Mode != agentsession.ModeFollowUp || owed[0].Trigger == nil || owed[0].Trigger.Ref != "03:00" {
		t.Fatalf("owed after the abort = %+v in %q", owed, entryTypes(s))
	}
	// Nothing owes the call's output but the host; answer it, and steer
	// while idle.
	a.Steer(openresponses.UserText("steered while idle"))
	pending := a.State().Pending
	if len(pending) != 1 {
		t.Fatalf("pending = %+v", pending)
	}
	if _, err := a.Prompt(context.Background(), openresponses.NewFunctionCallOutput(pending[0].Call.CallID, "cancelled"), openresponses.UserText("again")); err != nil {
		t.Fatal(err)
	}
	if owed, _ := s.PendingQueued(s.Leaf()); len(owed) != 0 {
		t.Errorf("still owed = %+v", owed)
	}
	names := map[string]string{}
	for _, e := range s.Entries() {
		if q, ok := e.(*agentsession.QueuedEntry); ok {
			names[e.Base().ID] = q.Item.(*openresponses.Message).Text()
		}
	}
	drained := map[string]bool{}
	for _, e := range s.Entries() {
		if it, ok := e.(*agentsession.ItemEntry); ok && it.QueuedFrom != "" {
			text := it.Item.(*openresponses.Message).Text()
			if names[it.QueuedFrom] != text {
				t.Errorf("item %q names queued entry %q", text, names[it.QueuedFrom])
			}
			drained[text] = true
		}
	}
	if !drained["the 03:00 firing"] || !drained["steered while idle"] {
		t.Errorf("drained = %v in %q", drained, entryTypes(s))
	}
	if err := s.VerifyRecords(s.Leaf()); err != nil {
		t.Errorf("verify records: %v", err)
	}
}

// TestRebaseIntoARunClosesIt checks the other half of the rule: a
// rewind to an entry inside a run leaves that run open on the new
// branch, and Rebase closes it, interrupted, naming the rewind; a
// rewind to an entry between runs appends nothing.
func TestRebaseIntoARunClosesIt(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{upper}})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	var checkpoint, runEnd string
	for _, e := range s.Entries() {
		if it, ok := e.(*agentsession.ItemEntry); ok {
			if _, ok := it.Item.(*openresponses.FunctionCallOutput); ok {
				checkpoint = e.Base().ID
			}
		}
		if r, ok := e.(*agentsession.RunEntry); ok && r.IsEnd() {
			runEnd = e.Base().ID
		}
	}
	before := len(s.Entries())
	if err := rec.Rebase(s, runEnd); err != nil {
		t.Fatal(err)
	}
	if n := len(s.Entries()); n != before {
		t.Errorf("a rebase between runs appended %d entries", n-before)
	}
	if err := rec.Rebase(s, checkpoint); err != nil {
		t.Fatal(err)
	}
	leaf, _ := s.Entry(s.Leaf())
	end, ok := leaf.(*agentsession.RunEntry)
	if !ok || !end.IsEnd() || end.Reason != agentsession.ReasonInterrupted || end.Ref != "rewind to "+checkpoint || end.Parent != checkpoint {
		t.Fatalf("leaf after the rewind = %+v", leaf)
	}
	cx, err := s.Context()
	if err != nil {
		t.Fatal(err)
	}
	if err := a.SetTranscript(cx.Items); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Prompt(context.Background(), openresponses.UserText("from the checkpoint")); err != nil {
		t.Fatal(err)
	}
	verifyAll(t, s)
}

// TestStartOnABaseSeedsTheRecorder pins #118: a session forked at an
// entry of another is seeded from the context there, so the agent
// seeded with that context records requests that carry hashes, and a
// fork inside a run closes that run on the fork.
func TestStartOnABaseSeedsTheRecorder(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, origin, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{upper}})
	unsub := rec.Attach(a)
	if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	unsub()
	var inside, after string
	for _, e := range origin.Entries() {
		if it, ok := e.(*agentsession.ItemEntry); ok {
			if _, ok := it.Item.(*openresponses.FunctionCallOutput); ok {
				inside = e.Base().ID
			}
		}
		if r, ok := e.(*agentsession.RunEntry); ok && r.IsEnd() {
			after = e.Base().ID
		}
	}
	for _, tc := range []struct {
		name, base string
		closes     bool
	}{{"between runs", after, false}, {"inside a run", inside, true}} {
		t.Run(tc.name, func(t *testing.T) {
			frec, fork, err := Start(context.Background(), store, agentsession.Header{Base: tc.base, ParentSession: origin.ID()})
			if err != nil {
				t.Fatal(err)
			}
			leaf, _ := fork.Entry(fork.Leaf())
			end, closed := leaf.(*agentsession.RunEntry)
			closed = closed && end.IsEnd() && end.Reason == agentsession.ReasonInterrupted && end.Ref == "fork at "+tc.base
			if closed != tc.closes {
				t.Fatalf("leaf of the fork = %+v", leaf)
			}
			cx, err := fork.Context()
			if err != nil {
				t.Fatal(err)
			}
			b := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{upper}}, agentturn.WithTranscript(cx.Items))
			defer frec.Attach(b)()
			if _, err := b.Prompt(context.Background(), openresponses.UserText("on the fork")); err != nil {
				t.Fatal(err)
			}
			if n := hashed(fork); n == 0 {
				t.Errorf("no response on the fork carries a hash: %q", entryTypes(fork))
			}
			verifyAll(t, fork)
			// The fork takes no config of its own: the base's settings
			// are the agent's.
			inOrigin := map[string]bool{}
			for _, e := range origin.Entries() {
				inOrigin[e.Base().ID] = true
			}
			for _, c := range configs(fork) {
				if !inOrigin[c.Base().ID] {
					t.Errorf("the fork wrote a config: %+v", c)
				}
			}
		})
	}
}

// TestGuardBeforeTheCallIsRecordedAsAStop checks the record of #116: a
// guard that stops a run before its model call leaves no failed
// response, and the run end names the guard.
func TestGuardBeforeTheCallIsRecordedAsAStop(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	spent := fmt.Errorf("%w: cost limit", agentturn.ErrGuard)
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, BeforeModelCall: func(context.Context, *openresponses.Request) error { return spent }})
	defer rec.Attach(a)()
	end, err := a.Prompt(context.Background(), openresponses.UserText("go"))
	if err != nil || end.Reason != agentturn.ReasonStopped || !errors.Is(end.Err, agentturn.ErrGuard) {
		t.Fatalf("end = %+v err=%v", end, err)
	}
	for _, e := range s.Entries() {
		if _, ok := e.(*agentsession.ResponseEntry); ok {
			t.Errorf("a response entry for a call never made: %q", entryTypes(s))
		}
	}
	blocked := customs(s, ModelBlockedNS)
	if len(blocked) != 1 {
		t.Fatalf("model_blocked entries in %q", entryTypes(s))
	}
	var mb ModelBlocked
	if err := json.Unmarshal(blocked[0].Data, &mb); err != nil {
		t.Fatal(err)
	}
	if mb.RequestHash == "" || !strings.Contains(mb.Error, "cost limit") {
		t.Errorf("model_blocked = %+v", mb)
	}
	runs := runsOf(t, s)
	if len(runs) != 1 || runs[0].End == nil || !strings.HasPrefix(runs[0].End.Ref, "guard: ") {
		t.Errorf("run end = %+v", runs[0].End)
	}
	if err := s.VerifyRecords(s.Leaf()); err != nil {
		t.Errorf("verify records: %v", err)
	}
}

// countQueued counts the queued entries of s.
func countQueued(s *agentsession.Session) int {
	n := 0
	for _, e := range s.Entries() {
		if _, ok := e.(*agentsession.QueuedEntry); ok {
			n++
		}
	}
	return n
}

// TestInboxHoldsOnlyWhatTheAgentHolds pins the rules the review of #67
// asked for: a rewind leaves the branch's queued inputs behind rather
// than writing them after every later run end; a resumed input nobody
// takes up is dropped at the next run end; a rebase between runs does
// not write again an input whose entry is still pending; and one item
// queued twice is two inputs.
func TestInboxHoldsOnlyWhatTheAgentHolds(t *testing.T) {
	t.Run("a rewind past a queued input", func(t *testing.T) {
		store := agentsession.NewMemoryStore()
		rec, s, err := Start(context.Background(), store, agentsession.Header{})
		if err != nil {
			t.Fatal(err)
		}
		var a *agentturn.Agent
		once := false
		follow := agenttool.New("follow", "", func(context.Context, echoArgs) (string, error) {
			if !once {
				once = true
				a.FollowUp(openresponses.UserText("later"))
			}
			return "ok", nil
		})
		a = agentturn.New(agentturn.Config{Model: blockingModel{}, Tools: []agenttool.Tool{follow}})
		defer rec.Attach(a)()
		if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
			t.Fatal(err)
		}
		// The first output: after the queued entry, before the item
		// that drained it.
		var output string
		for _, e := range s.Entries() {
			if it, ok := e.(*agentsession.ItemEntry); ok && output == "" {
				if _, ok := it.Item.(*openresponses.FunctionCallOutput); ok {
					output = e.Base().ID
				}
			}
		}
		if owed, _ := s.PendingQueued(output); len(owed) != 1 {
			t.Fatalf("owed at the rewind point = %d in %q", len(owed), entryTypes(s))
		}
		if err := rec.Rebase(s, output); err != nil {
			t.Fatal(err)
		}
		cx, _ := s.Context()
		if err := a.SetTranscript(cx.Items); err != nil {
			t.Fatal(err)
		}
		before := countQueued(s)
		for range 3 {
			if _, err := a.Prompt(context.Background(), openresponses.UserText("again")); err != nil {
				t.Fatal(err)
			}
		}
		if n := countQueued(s); n != before {
			t.Errorf("queued entries grew from %d to %d: %q", before, n, entryTypes(s))
		}
		if owed, _ := s.PendingQueued(s.Leaf()); len(owed) != 0 {
			t.Errorf("owed = %+v", owed)
		}
	})
	t.Run("a resumed input nobody takes up", func(t *testing.T) {
		store := agentsession.NewMemoryStore()
		s := cutRun(t, store)
		rec, s, err := Resume(context.Background(), store, s.ID())
		if err != nil {
			t.Fatal(err)
		}
		cx, _ := s.Context()
		a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m"}, agentturn.WithTranscript(cx.Items))
		defer rec.Attach(a)()
		before := countQueued(s)
		for range 2 {
			if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
				t.Fatal(err)
			}
		}
		if n := countQueued(s); n != before {
			t.Errorf("queued entries grew from %d to %d", before, n)
		}
		if owed, _ := s.PendingQueued(s.Leaf()); len(owed) != 0 {
			t.Errorf("owed = %+v", owed)
		}
		if n := rec.Requeue(context.Background(), a); n != 0 {
			t.Errorf("requeued %d after a run closed them", n)
		}
	})
	t.Run("a rebase between runs keeps a pending entry", func(t *testing.T) {
		store := agentsession.NewMemoryStore()
		rec, s, err := Start(context.Background(), store, agentsession.Header{})
		if err != nil {
			t.Fatal(err)
		}
		a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}})
		defer rec.Attach(a)()
		// Queued while a subscriber holds run_end: reported after the
		// end, so its entry follows it and is pending between runs.
		a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
			if _, ok := ev.(*agentturn.RunEnd); ok && a.State().FollowUps == 0 && countQueued(s) == 0 {
				a.FollowUp(openresponses.UserText("next"))
			}
			return nil
		})
		if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
			t.Fatal(err)
		}
		if n := countQueued(s); n != 1 {
			t.Fatalf("queued entries = %d in %q", n, entryTypes(s))
		}
		if err := rec.Rebase(s, s.Leaf()); err != nil {
			t.Fatal(err)
		}
		if n := countQueued(s); n != 1 {
			t.Errorf("a rebase to where the entry is pending wrote %d", n-1)
		}
	})
	t.Run("one item queued twice", func(t *testing.T) {
		store := agentsession.NewMemoryStore()
		rec, s, err := Start(context.Background(), store, agentsession.Header{})
		if err != nil {
			t.Fatal(err)
		}
		a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}})
		defer rec.Attach(a)()
		same := openresponses.UserText("twice")
		a.Steer(same)
		a.Steer(same)
		if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
			t.Fatal(err)
		}
		named := map[string]bool{}
		for _, e := range s.Entries() {
			if it, ok := e.(*agentsession.ItemEntry); ok && it.QueuedFrom != "" {
				named[it.QueuedFrom] = true
			}
		}
		if countQueued(s) != 2 || len(named) != 2 {
			t.Errorf("queued = %d, drained naming %d, in %q", countQueued(s), len(named), entryTypes(s))
		}
		if err := s.VerifyRecords(s.Leaf()); err != nil {
			t.Errorf("verify records: %v", err)
		}
	})
}

// TestQueueWritesBeforeItQueues checks that an input queued through the
// recorder while the agent is idle is on the record at once, and that
// its queued event and the item that drains it add no second entry.
func TestQueueWritesBeforeItQueues(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("first")); err != nil {
		t.Fatal(err)
	}
	ctx := agentturn.ContextWithTrigger(context.Background(), agentturn.Trigger{Kind: "cron", Ref: "03:00"})
	if err := rec.Queue(ctx, a, agentturn.QueueFollowUp, openresponses.UserText("the 03:00 firing")); err != nil {
		t.Fatal(err)
	}
	owed, err := s.PendingQueued(s.Leaf())
	if err != nil || len(owed) != 1 || owed[0].Trigger == nil || owed[0].Trigger.Ref != "03:00" {
		t.Fatalf("owed while idle = %+v, %v", owed, err)
	}
	if a.State().FollowUps != 1 {
		t.Fatalf("follow-ups = %d", a.State().FollowUps)
	}
	if _, err := a.Prompt(context.Background(), openresponses.UserText("second")); err != nil {
		t.Fatal(err)
	}
	if n := countQueued(s); n != 1 {
		t.Errorf("queued entries = %d in %q", n, entryTypes(s))
	}
	var drained *agentsession.ItemEntry
	for _, e := range s.Entries() {
		if it, ok := e.(*agentsession.ItemEntry); ok && it.QueuedFrom != "" {
			drained = it
		}
	}
	if drained == nil || drained.QueuedFrom != owed[0].ID {
		t.Errorf("drained = %+v", drained)
	}
	if err := s.VerifyRecords(s.Leaf()); err != nil {
		t.Errorf("verify records: %v", err)
	}
}
