package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
// response, and the run end names the guard. With no response of its
// own and nothing answered, the end reads as aborted (#140).
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
	if len(runs) != 1 || runs[0].End == nil || runs[0].End.Reason != agentsession.ReasonAborted || !strings.HasPrefix(runs[0].End.Ref, "guard: ") {
		t.Errorf("run end = %+v", runs[0].End)
	}
	if err := s.VerifyRecords(s.Leaf()); err != nil {
		t.Errorf("verify records: %v", err)
	}
}

// guardedSpeaker answers its first request with a message, after a
// function call when call is set, and every later one with a message.
type guardedSpeaker struct {
	call  bool
	calls int
}

func (m *guardedSpeaker) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.calls++
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if m.calls == 1 && m.call {
		fc, err := em.FunctionCall("c1", "upper")
		if err != nil {
			return err
		}
		if err := fc.Arguments(`{"text":"x"}`); err != nil {
			return err
		}
		if err := fc.Close(); err != nil {
			return err
		}
	}
	w, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if err := w.Text(fmt.Sprintf("reply %d", m.calls)); err != nil {
		return err
	}
	em.Response().Usage = &openresponses.Usage{InputTokens: 3, OutputTokens: 2, TotalTokens: 5}
	return em.Complete()
}

// TestOutputGuardStopIsRecordedWithheld checks the record of #181: an
// OutputGuard stop with the model call in flight writes the response
// incomplete with content_filter, the response's usage and none of the
// guard's text, a call
// the response made before the message is rejected by policy with the
// loop's fixed output, and the run end reads aborted, as the format
// reads a run whose last response is incomplete. The record verifies,
// and the next prompt goes ahead.
func TestOutputGuardStopIsRecordedWithheld(t *testing.T) {
	cases := []struct {
		name  string
		call  bool
		types string
	}{
		{"a message", false, "run config item:user response run"},
		{"a call then a message", true, "run config item:user item:function_call* response decision item:function_call_output run"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			rule := fmt.Errorf("%w: secret rule", agentturn.ErrGuard)
			a := agentturn.New(agentturn.Config{Model: &guardedSpeaker{call: tc.call}, ModelName: "m", Tools: []agenttool.Tool{upper},
				OutputGuard: func(_ context.Context, info agentturn.OutputInfo) (*openresponses.Message, error) {
					if info.Message.Text() == "reply 1" {
						return nil, rule
					}
					return nil, nil
				}})
			defer rec.Attach(a)()
			end, err := a.Prompt(context.Background(), openresponses.UserText("go"))
			if err != nil || end.Reason != agentturn.ReasonStopped || end.Cause != agentturn.StopGuard {
				t.Fatalf("end = %+v err=%v", end, err)
			}
			if got := entryTypes(s); got != tc.types {
				t.Errorf("entries = %q, want %q", got, tc.types)
			}
			for _, e := range s.Entries() {
				switch v := e.(type) {
				case *agentsession.ResponseEntry:
					if v.Status != openresponses.ResponseStatusIncomplete || v.Incomplete == nil || v.Incomplete.Reason != openresponses.IncompleteReasonContentFilter || v.Error != nil || v.RequestHash == "" || v.ResponseID == "" {
						t.Errorf("response = %+v", v)
					}
					if v.Usage == nil || v.Usage.TotalTokens != 5 {
						t.Errorf("response usage = %+v, want the withheld response's", v.Usage)
					}
					raw, err := json.Marshal(v)
					if err != nil || strings.Contains(string(raw), "secret rule") {
						t.Errorf("the response carries the guard's text: %s", raw)
					}
				case *agentsession.DecisionEntry:
					if v.Verdict != agentsession.VerdictReject || v.By != agentsession.ByPolicy || v.Reason != agentturn.WithheldCallOutput {
						t.Errorf("decision = %+v", v)
					}
				}
			}
			runs := runsOf(t, s)
			if len(runs) != 1 || runs[0].End == nil || runs[0].End.Reason != agentsession.ReasonAborted || !strings.HasPrefix(runs[0].End.Ref, "guard: ") {
				t.Errorf("run end = %+v", runs[0].End)
			}
			if err := s.VerifyRecords(s.Leaf()); err != nil {
				t.Errorf("verify records: %v", err)
			}
			if end, err := a.Prompt(context.Background(), openresponses.UserText("again")); err != nil || end.Reason != agentturn.ReasonDone {
				t.Fatalf("next prompt: end = %+v err=%v", end, err)
			}
			if err := s.VerifyRecords(s.Leaf()); err != nil {
				t.Errorf("verify records after the next prompt: %v", err)
			}
			verifyAll(t, s)
		})
	}
}

// nameless passes on its model's stream without response.created or
// response.in_progress, as a relay that forwards only the output items
// and the terminal event does, so no item event names the response.
type nameless struct{ m agentturn.Model }

func (n nameless) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	return n.m.CreateStream(ctx, req, openresponses.EventSinkFunc(func(ev openresponses.StreamEvent) error {
		switch ev.(type) {
		case *openresponses.ResponseCreatedEvent, *openresponses.ResponseInProgressEvent:
			return nil
		}
		return sink.Send(ev)
	}))
}

// unnamed answers its first request with a function call and every
// later one with a message. Wrapped in nameless, no item event names
// its response.
type unnamed struct {
	calls int
	// cut ends the first stream with an error after the call, before
	// its terminal event.
	cut bool
}

func (m *unnamed) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.calls++
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if m.calls == 1 {
		fc, err := em.FunctionCall("c1", "upper")
		if err != nil {
			return err
		}
		if err := fc.Arguments(`{"text":"x"}`); err != nil {
			return err
		}
		if err := fc.Close(); err != nil {
			return err
		}
		if m.cut {
			return errors.New("connection reset")
		}
		return em.Complete()
	}
	w, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if err := w.Text("ok"); err != nil {
		return err
	}
	return em.Complete()
}

// namedByTheirResponses checks that every model item entry names the
// response entry that follows it, so the context algorithm reads it as
// that response's output rather than as an input.
func namedByTheirResponses(t *testing.T, s *agentsession.Session) {
	t.Helper()
	var pending []*agentsession.ItemEntry
	for _, e := range s.Entries() {
		switch v := e.(type) {
		case *agentsession.ItemEntry:
			if isModelOutput(v.Item) {
				pending = append(pending, v)
			}
		case *agentsession.ResponseEntry:
			for _, it := range pending {
				if it.ResponseID != v.ResponseID {
					t.Errorf("%s item %s names response %q, want %q", it.Item.ItemType(), it.ID, it.ResponseID, v.ResponseID)
				}
			}
			pending = nil
		}
	}
	if len(pending) > 0 {
		t.Errorf("%d model items with no response after them in %q", len(pending), entryTypes(s))
	}
}

// TestUnnamedResponseIsNotWithheld pins the review of #181: the
// recorder writes a response withheld only on the loop's word, a
// response_end with Withheld set. An item with no response ID while a
// call is in flight is a stream that has not named its response, and
// its responses are written as they arrived, completed, with no
// response invented for them; each item is written with the ID the
// response ends with, before it, so the record rebuilds every request.
func TestUnnamedResponseIsNotWithheld(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: nameless{&unnamed{}}, ModelName: "m", Tools: []agenttool.Tool{upper}})
	defer rec.Attach(a)()
	if end, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil || end.Reason != agentturn.ReasonDone {
		t.Fatalf("end = %+v err=%v", end, err)
	}
	if got, want := entryTypes(s), "run config item:user item:function_call* response dispatch item:function_call_output item:assistant* response run"; got != want {
		t.Errorf("entries = %q, want %q", got, want)
	}
	for _, e := range s.Entries() {
		if v, ok := e.(*agentsession.ResponseEntry); ok && (v.Status != openresponses.ResponseStatusCompleted || v.Incomplete != nil || v.ResponseID == "") {
			t.Errorf("response = %q %s %+v, want completed and named", v.ResponseID, v.Status, v.Incomplete)
		}
	}
	runs := runsOf(t, s)
	if len(runs) != 1 || runs[0].End == nil || runs[0].End.Reason != agentsession.ReasonDone {
		t.Errorf("run end = %+v", runs[0].End)
	}
	namedByTheirResponses(t, s)
	if n := verifyAll(t, s); n != 2 {
		t.Errorf("responses = %d, want 2", n)
	}
}

// TestUnnamedItemsTakeTheirResponsesID pins the other ways a model call
// whose stream names no response ends: an OutputGuard stop, whose
// withheld response_end names the response; a call tried again, whose
// failed attempt left nothing; and a stream cut off before its terminal
// event, which never names it, so its items name no response and its
// failed response carries no request hash, since the record cannot
// rebuild a request whose output it cannot tell from its input, and an
// unhashed entry before it says so.
func TestUnnamedItemsTakeTheirResponsesID(t *testing.T) {
	rule := fmt.Errorf("%w: secret rule", agentturn.ErrGuard)
	for _, tc := range []struct {
		name     string
		cfg      agentturn.Config
		reason   agentturn.Reason
		types    string
		again    bool
		unhashed int
	}{{
		name: "withheld",
		cfg: agentturn.Config{Model: nameless{&guardedSpeaker{call: true}}, Tools: []agenttool.Tool{upper},
			OutputGuard: func(_ context.Context, info agentturn.OutputInfo) (*openresponses.Message, error) {
				if info.Message.Text() == "reply 1" {
					return nil, rule
				}
				return nil, nil
			}},
		reason: agentturn.ReasonStopped,
		types:  "run config item:user item:function_call* response decision item:function_call_output run",
		again:  true,
	}, {
		name: "retried",
		cfg: agentturn.Config{Model: nameless{flaky{fails: func() *atomic.Int32 { var n atomic.Int32; n.Store(1); return &n }()}},
			Retry: agentturn.Retry{MaxAttempts: 3, Backoff: func(int, error) time.Duration { return 0 }}},
		reason: agentturn.ReasonDone,
		types:  "run config item:user custom item:assistant* response run",
		again:  true,
	}, {
		name:     "cut off",
		cfg:      agentturn.Config{Model: nameless{&unnamed{cut: true}}, Tools: []agenttool.Tool{upper}},
		reason:   agentturn.ReasonError,
		types:    "run config item:user item:function_call custom response run",
		unhashed: 1,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			tc.cfg.ModelName = "m"
			a := agentturn.New(tc.cfg)
			defer rec.Attach(a)()
			end, err := a.Prompt(context.Background(), openresponses.UserText("go"))
			if end.Reason != tc.reason {
				t.Fatalf("end = %+v err=%v", end, err)
			}
			if got := entryTypes(s); got != tc.types {
				t.Errorf("entries = %q, want %q", got, tc.types)
			}
			namedByTheirResponses(t, s)
			if tc.again {
				if end, err := a.Prompt(context.Background(), openresponses.UserText("again")); err != nil || end.Reason != agentturn.ReasonDone {
					t.Fatalf("next prompt: end = %+v err=%v", end, err)
				}
				namedByTheirResponses(t, s)
			}
			verifyAllUnhashed(t, s, tc.unhashed)
			// The response left unhashed says why.
			if why := unhashedOf(t, s); len(why) != tc.unhashed || tc.unhashed > 0 && why[0].Reason != unnamedReason {
				t.Errorf("unhashed = %+v, want %d naming the unnamed response", why, tc.unhashed)
			}
		})
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
