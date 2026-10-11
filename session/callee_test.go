package session

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// rawRecord is a record in any namespace with any JSON, as tools/a2a's
// Callee and CancelSent marshal, which this module does not import.
type rawRecord struct {
	ns   string
	data string
}

func (r rawRecord) RecordNS() string             { return r.ns }
func (r rawRecord) MarshalJSON() ([]byte, error) { return []byte(r.data), nil }

func calleeRecord(url, task, context, relation string) rawRecord {
	data, _ := json.Marshal(map[string]string{"url": url, "task": task, "context": context, "relation": relation})
	return rawRecord{a2aCalleeNS, string(data)}
}

func cancelRecord(url, task string) rawRecord {
	data, _ := json.Marshal(map[string]string{"url": url, "task": task})
	return rawRecord{a2aCancelNS, string(data)}
}

// recordsTool returns a tool named remote that writes records through
// agenttool.WriteRecord, as tools/a2a does while a call runs, and then
// answers.
func recordsTool(records ...rawRecord) agenttool.Tool {
	return agenttool.New("remote", "", func(ctx context.Context, _ echoArgs) (string, error) {
		for _, r := range records {
			if err := agenttool.WriteRecord(ctx, r); err != nil {
				return "", err
			}
		}
		return "answered", nil
	})
}

// noLinkedSession resolves no session: a link VerifyLinks asks about is
// one it took for a child of this store.
func noLinkedSession(t *testing.T) func(string) (*agentsession.Session, error) {
	return func(id string) (*agentsession.Session, error) {
		t.Errorf("VerifyLinks resolved %q, which names no child of this store", id)
		return nil, nil
	}
}

// calleeShape is what a test expects of one entry of Session.Callees.
type calleeShape struct {
	rel, task      string
	cancelled, ans bool
}

func shapesOf(cs []agentsession.CalleeLink) []calleeShape {
	var out []calleeShape
	for _, c := range cs {
		out = append(out, calleeShape{c.Rel, c.Task, c.Cancelled, c.Answered})
	}
	return out
}

// TestA2ARecordsAreLinks pins the recorder's reading of the records
// tools/a2a writes: a callee record is the call's subsession or peer
// link, naming the callee in another system, and a cancel record is
// the notice naming that link, each where the custom entry was. A
// record it cannot write as a link stays the custom entry it was.
func TestA2ARecordsAreLinks(t *testing.T) {
	const url = "https://remote.example/a2a"
	for _, tc := range []struct {
		name    string
		records []rawRecord
		// entries is the call's record between its dispatch and its
		// output, links as link:<rel>, notices as cancel, custom
		// entries as their namespace.
		entries string
		callees []calleeShape
	}{
		{
			name:    "subsession callee",
			records: []rawRecord{calleeRecord(url, "task-1", "ctx-1", "subsession")},
			entries: "link:subsession",
			callees: []calleeShape{{agentsession.RelSubsession, "task-1", false, true}},
		},
		{
			name:    "peer callee",
			records: []rawRecord{calleeRecord(url, "task-1", "ctx-1", "peer")},
			entries: "link:peer",
			callees: []calleeShape{{agentsession.RelPeer, "task-1", false, true}},
		},
		{
			name:    "a message answer names no task",
			records: []rawRecord{calleeRecord(url, "", "ctx-1", "subsession")},
			entries: "link:subsession",
			callees: []calleeShape{{agentsession.RelSubsession, "", false, true}},
		},
		{
			name:    "a cancel names its callee's link",
			records: []rawRecord{calleeRecord(url, "task-1", "ctx-1", "subsession"), cancelRecord(url, "task-1")},
			entries: "link:subsession cancel",
			callees: []calleeShape{{agentsession.RelSubsession, "task-1", true, false}},
		},
		{
			name: "a cancel names the link of its own task",
			records: []rawRecord{
				calleeRecord(url, "task-1", "ctx-1", "subsession"),
				calleeRecord(url, "task-2", "ctx-1", "subsession"),
				cancelRecord(url, "task-1"),
			},
			entries: "link:subsession link:subsession cancel",
			callees: []calleeShape{
				{agentsession.RelSubsession, "task-1", true, false},
				{agentsession.RelSubsession, "task-2", false, true},
			},
		},
		{
			name:    "an unknown relation stays a custom entry",
			records: []rawRecord{calleeRecord(url, "task-1", "ctx-1", "sibling")},
			entries: a2aCalleeNS,
		},
		{
			name:    "a callee that does not decode stays a custom entry",
			records: []rawRecord{{a2aCalleeNS, `["not", "a", "callee"]`}},
			entries: a2aCalleeNS,
		},
		{
			name:    "a callee with no url stays a custom entry",
			records: []rawRecord{calleeRecord("", "task-1", "ctx-1", "subsession")},
			entries: a2aCalleeNS,
		},
		{
			name:    "a cancel with no callee link stays a custom entry",
			records: []rawRecord{cancelRecord(url, "task-1")},
			entries: a2aCancelNS,
		},
		{
			name:    "a cancel for another task stays a custom entry",
			records: []rawRecord{calleeRecord(url, "task-1", "ctx-1", "subsession"), cancelRecord(url, "task-2")},
			entries: "link:subsession " + a2aCancelNS,
			callees: []calleeShape{{agentsession.RelSubsession, "task-1", false, true}},
		},
		{
			name:    "a cancel for another url stays a custom entry",
			records: []rawRecord{calleeRecord(url, "task-1", "ctx-1", "subsession"), cancelRecord("https://other.example", "task-1")},
			entries: "link:subsession " + a2aCancelNS,
			callees: []calleeShape{{agentsession.RelSubsession, "task-1", false, true}},
		},
		{
			name:    "a cancel that does not decode stays a custom entry",
			records: []rawRecord{calleeRecord(url, "task-1", "ctx-1", "subsession"), {a2aCancelNS, `{"task": 7}`}},
			entries: "link:subsession " + a2aCancelNS,
			callees: []calleeShape{{agentsession.RelSubsession, "task-1", false, true}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(ctx, store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{recordsTool(tc.records...)}, MaxTurns: 2, ToolRecorder: rec.RecordFunc()})
			defer rec.Attach(a)()
			if _, err := a.Prompt(ctx, openresponses.UserText("go")); err != nil {
				t.Fatal(err)
			}
			call := callsOf(t, s)["remote"]
			if call == nil || call.Dispatch == nil || call.Output == nil {
				t.Fatalf("call = %+v", call)
			}
			if got := callRecordBetween(t, s, call.Dispatch.ID, call.Output.ID); got != tc.entries {
				t.Errorf("between dispatch and output = %q, want %q", got, tc.entries)
			}
			callees, err := s.Callees(s.Leaf())
			if err != nil {
				t.Fatal(err)
			}
			if got := shapesOf(callees); fmt.Sprint(got) != fmt.Sprint(tc.callees) {
				t.Errorf("callees = %+v, want %+v", got, tc.callees)
			}
			for _, c := range callees {
				if c.CallID != call.ID() || c.URL != url || c.Context != "ctx-1" || c.Session != "" || c.Dispatch != call.Dispatch.ID {
					t.Errorf("callee link = %+v, dispatch %s, want call %s at %s on dispatch %s", c.LinkEntry, c.Dispatch, call.ID(), url, call.Dispatch.ID)
				}
			}
			for _, c := range customs(s, a2aCalleeNS) {
				if c.CallID != call.ID() {
					t.Errorf("custom callee entry names call %q, want %q", c.CallID, call.ID())
				}
			}
			verifyAll(t, s)
			if err := s.VerifyLinks(noLinkedSession(t)); err != nil {
				t.Errorf("verify links: %v", err)
			}
		})
	}
}

// callRecordBetween names the entries on the path strictly between
// from and to, as TestA2ARecordsAreLinks spells them.
func callRecordBetween(t *testing.T, s *agentsession.Session, from, to string) string {
	t.Helper()
	var parts []string
	in := false
	for _, e := range s.Path(to) {
		id := e.Base().ID
		if id == to {
			break
		}
		if in {
			switch v := e.(type) {
			case *agentsession.LinkEntry:
				if v.Cancel != "" {
					parts = append(parts, "cancel")
				} else {
					parts = append(parts, "link:"+v.Rel)
				}
			case *agentsession.CustomEntry:
				parts = append(parts, v.NS)
			default:
				parts = append(parts, e.EntryType())
			}
		}
		if id == from {
			in = true
		}
	}
	return strings.Join(parts, " ")
}

// TestA2ARecordsOfARecordableResult: a tool's result whose Details is
// a callee record is written as the link too, after the records the
// tool wrote while it ran.
func TestA2ARecordsOfARecordableResult(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	tool := agenttool.NewFunc("remote", "", json.RawMessage(`{"type":"object"}`), func(ctx context.Context, _ agenttool.Call) (agenttool.Result, error) {
		if err := agenttool.WriteRecord(ctx, calleeRecord("https://r.example", "task-1", "", "subsession")); err != nil {
			return agenttool.Result{}, err
		}
		res := agenttool.Text("done")
		res.Details = cancelRecord("https://r.example", "task-1")
		return res, nil
	})
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{tool}, MaxTurns: 2, ToolRecorder: rec.RecordFunc()})
	defer rec.Attach(a)()
	if _, err := a.Prompt(ctx, openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	callees, err := s.Callees(s.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	if len(callees) != 1 || !callees[0].Cancelled || callees[0].Task != "task-1" {
		t.Errorf("callees = %+v", shapesOf(callees))
	}
	verifyAll(t, s)
}

// TestA2ACalleeAcrossARestart covers the crash windows: a call cut
// after its callee was recorded and before its output leaves the link
// and no output; the call dispatched again after a restart reaches a
// new task and gets a link of its own, beside the first rather than in
// its place, and a cancel the second dispatch sends for the first task
// names the link the restarted recorder found on the path.
func TestA2ACalleeAcrossARestart(t *testing.T) {
	ctx := context.Background()
	const url = "https://remote.example/a2a"
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	first := agenttool.New("remote", "", func(ctx context.Context, _ echoArgs) (string, error) {
		if err := agenttool.WriteRecord(ctx, calleeRecord(url, "task-1", "ctx-1", "subsession")); err != nil {
			return "", err
		}
		started <- struct{}{}
		<-ctx.Done()
		return "", ctx.Err()
	})
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{first}, ToolRecorder: rec.RecordFunc()})
	unsub := rec.Attach(a)
	done := make(chan error, 1)
	go func() {
		_, err := a.Prompt(ctx, openresponses.UserText("go"))
		done <- err
	}()
	<-started
	a.Abort()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	unsub()
	call := callsOf(t, s)["remote"]
	if call == nil || call.Output != nil || len(call.Dispatches) != 1 {
		t.Fatalf("after the cut: call = %+v", call)
	}
	callees, err := s.Callees(s.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	if got := shapesOf(callees); fmt.Sprint(got) != fmt.Sprint([]calleeShape{{agentsession.RelSubsession, "task-1", false, false}}) {
		t.Fatalf("callees after the cut = %+v", got)
	}

	// A restart: the call is dispatched again, reaches task-2, and asks
	// task-1 to stop.
	rec, s, err = Resume(ctx, store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	second := recordsTool(cancelRecord(url, "task-1"), calleeRecord(url, "task-2", "ctx-1", "subsession"))
	opts, err := AgentOptions(s, rec.ReadOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{second}, ToolRecorder: rec.RecordFunc()}, opts...)
	defer rec.Attach(b)()
	if _, err := b.Resume(ctx, agentturn.Approve(call.ID()).WithRunAgain().WithBy(agentsession.ByHuman)); err != nil {
		t.Fatal(err)
	}
	call = callsOf(t, s)["remote"]
	if call == nil || call.Output == nil || len(call.Dispatches) != 2 {
		t.Fatalf("after the restart: call = %+v", call)
	}
	callees, err = s.Callees(s.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	want := []calleeShape{
		{agentsession.RelSubsession, "task-1", true, false},
		{agentsession.RelSubsession, "task-2", false, true},
	}
	if got := shapesOf(callees); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("callees after the restart = %+v, want %+v", got, want)
	}
	if len(callees) == 2 && (callees[0].Dispatch != call.Dispatches[0].ID || callees[1].Dispatch != call.Dispatches[1].ID) {
		t.Errorf("dispatches = %s, %s, want one link per dispatch", callees[0].Dispatch, callees[1].Dispatch)
	}
	if n := len(customs(s, a2aCancelNS)); n != 0 {
		t.Errorf("%d cancels written as custom entries", n)
	}
	verifyAll(t, s)
}

// TestReplayedA2AOutputKeepsItsCallees is TestReplayedChildOutputKeepsItsLink
// for a callee in another system: a rebase to before the call's
// dispatch leaves its callee links, a cancel notice and its output on
// the branch left, and the output answered on the new branch brings
// each link whole, with the notice pointed at the copy. No in-process
// subsession link is written for it, since its link names no child.
func TestReplayedA2AOutputKeepsItsCallees(t *testing.T) {
	ctx := context.Background()
	const url = "https://remote.example/a2a"
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	runs := 0
	tool := agenttool.New("remote", "", func(ctx context.Context, _ echoArgs) (string, error) {
		runs++
		for _, r := range []rawRecord{
			calleeRecord(url, "task-1", "ctx-1", "subsession"),
			cancelRecord(url, "task-1"),
			calleeRecord(url, "task-2", "ctx-1", "peer"),
		} {
			if err := agenttool.WriteRecord(ctx, r); err != nil {
				return "", err
			}
		}
		return "answered", nil
	})
	tools := []agenttool.Tool{tool}
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: tools, ToolRecorder: rec.RecordFunc()})
	unsub := rec.Attach(a)
	if end, err := a.Prompt(ctx, openresponses.UserText("go")); err != nil || end.Reason != agentturn.ReasonDone {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	unsub()
	c := callsOf(t, s)["remote"]
	left, err := s.Callees(s.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	if c == nil || c.Dispatch == nil || c.Output == nil || len(left) != 2 {
		t.Fatalf("call = %+v, callees = %+v", c, shapesOf(left))
	}

	rec, s, err = Resume(ctx, store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Rebase(s, c.Dispatch.Parent); err != nil {
		t.Fatal(err)
	}
	answers, err := ReplayAnswers(ctx, s, tools, rec.ReadOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	if len(answers) != 1 || answers[0].Output == nil || answers[0].Origin != c.Output.ID {
		t.Fatalf("answers = %+v, want the output on the branch left with origin %s", answers, c.Output.ID)
	}
	opts, err := AgentOptions(s, rec.ReadOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: tools, ToolRecorder: rec.RecordFunc()}, opts...)
	defer rec.Attach(b)()
	if end, err := b.Resume(ctx, answers...); err != nil || end.Reason != agentturn.ReasonDone {
		t.Fatalf("resume: err=%v end=%+v", err, end)
	}
	if runs != 1 {
		t.Errorf("the tool ran %d times, want once", runs)
	}

	var record []string
	for _, e := range s.Path(s.Leaf()) {
		switch e := e.(type) {
		case *agentsession.LinkEntry:
			switch {
			case e.Cancel != "":
				record = append(record, "cancel")
			case e.URL == "":
				t.Errorf("an in-process link on the new branch: %+v", e)
			default:
				record = append(record, "link:"+e.Rel)
			}
		case *agentsession.DecisionEntry:
			record = append(record, e.Verdict)
		case *agentsession.DispatchEntry:
			record = append(record, "dispatch")
		case *agentsession.ItemEntry:
			if _, ok := e.Item.(*openresponses.FunctionCallOutput); ok {
				record = append(record, "output")
			}
		}
	}
	if got, want := strings.Join(record, " "), "link:subsession cancel link:peer answer output"; got != want {
		t.Errorf("the new branch's record for the call = %q, want %q", got, want)
	}
	callees, err := s.Callees(s.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	if len(callees) != 2 {
		t.Fatalf("callees on the new branch = %+v", shapesOf(callees))
	}
	onPath := map[string]bool{}
	for _, e := range s.Path(s.Leaf()) {
		onPath[e.Base().ID] = true
	}
	for i, cl := range callees {
		was := left[i]
		if cl.ID == was.ID || !onPath[cl.ID] {
			t.Errorf("callee %d is %s, the branch left's %s, want a copy on the new branch", i, cl.ID, was.ID)
		}
		if cl.Rel != was.Rel || cl.Session != was.Session || cl.CallID != was.CallID || cl.URL != was.URL || cl.Task != was.Task || cl.Context != was.Context {
			t.Errorf("callee %d = %+v, want a whole copy of %+v", i, cl.LinkEntry, was.LinkEntry)
		}
		if cl.Cancelled != was.Cancelled {
			t.Errorf("callee %d cancelled = %v, the branch left's %v", i, cl.Cancelled, was.Cancelled)
		}
	}
	for _, e := range s.Path(s.Leaf()) {
		if l, ok := e.(*agentsession.LinkEntry); ok && l.Cancel != "" && l.Target != callees[0].ID {
			t.Errorf("the notice names %s, want the copy %s", l.Target, callees[0].ID)
		}
	}
	verifyAll(t, s)
	if err := s.VerifyLinks(noLinkedSession(t)); err != nil {
		t.Errorf("verify links: %v", err)
	}
}

// TestSubsessionOnIgnoresCallees: the child session a branch links a
// call to is named by an in-process subsession link alone; a link
// naming a callee in another system, its session set or not, and a
// cancel notice name no child of this store.
func TestSubsessionOnIgnoresCallees(t *testing.T) {
	remote := agentsession.Callee{URL: "https://remote.example", Task: "task-1", Session: "remote-session"}
	for _, tc := range []struct {
		name  string
		links func(*agentsession.Session) []agentsession.Entry
		want  string
	}{
		{"an in-process child", func(*agentsession.Session) []agentsession.Entry {
			return []agentsession.Entry{agentsession.NewSubsessionLink("child", "call_1")}
		}, "child"},
		{"a callee in another system", func(*agentsession.Session) []agentsession.Entry {
			return []agentsession.Entry{agentsession.NewCalleeSubsessionLink("call_1", remote)}
		}, ""},
		{"a child, then a callee and its notice", func(s *agentsession.Session) []agentsession.Entry {
			callee := agentsession.NewCalleeSubsessionLink("call_1", remote)
			return []agentsession.Entry{agentsession.NewSubsessionLink("child", "call_1"), callee}
		}, "child"},
		{"another call's child", func(*agentsession.Session) []agentsession.Entry {
			return []agentsession.Entry{agentsession.NewSubsessionLink("child", "call_2")}
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := agentsession.NewMemoryStore().Create(context.Background(), agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Append(agentsession.NewItemEntry(&openresponses.FunctionCall{CallID: "call_1", Name: "remote", Arguments: "{}"})); err != nil {
				t.Fatal(err)
			}
			for _, e := range tc.links(s) {
				if _, err := s.Append(e); err != nil {
					t.Fatal(err)
				}
				// A callee link is followed by its cancel notice.
				if l, ok := e.(*agentsession.LinkEntry); ok && l.URL != "" {
					if _, err := s.Append(agentsession.NewCancelSentLink(l)); err != nil {
						t.Fatal(err)
					}
				}
			}
			if got := subsessionOn(s, s.Leaf(), "call_1"); got != tc.want {
				t.Errorf("subsessionOn = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestA2ACancelOffThePath: a cancel whose callee's link is on a branch
// a rebase left names nothing on the path it is written to, and is
// written as the custom entry it was rather than as a notice whose
// target the path does not hold.
func TestA2ACancelOffThePath(t *testing.T) {
	ctx := context.Background()
	const url = "https://remote.example/a2a"
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{recordsTool(calleeRecord(url, "task-1", "", "subsession"))}, ToolRecorder: rec.RecordFunc()})
	unsub := rec.Attach(a)
	if _, err := a.Prompt(ctx, openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	unsub()
	if n := len(links(s)); n != 1 {
		t.Fatalf("links = %d, want the callee's", n)
	}
	first := s.Path(s.Leaf())[0].Base().ID
	if err := rec.Rebase(s, first); err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{recordsTool(cancelRecord(url, "task-1"))}, ToolRecorder: rec.RecordFunc()})
	defer rec.Attach(b)()
	if _, err := b.Prompt(ctx, openresponses.UserText("again")); err != nil {
		t.Fatal(err)
	}
	if n := len(links(s)); n != 1 {
		t.Errorf("links = %d, want the branch left's alone", n)
	}
	if n := len(customs(s, a2aCancelNS)); n != 1 {
		t.Errorf("cancels written as custom entries = %d, want 1", n)
	}
	verifyAll(t, s)
}
