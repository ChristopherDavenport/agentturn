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
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
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
			name:    "a callee with no relation stays a custom entry",
			records: []rawRecord{{a2aCalleeNS, `{"url":"` + url + `","task":"task-1","context":"ctx-1"}`}},
			entries: a2aCalleeNS,
		},
		{
			name:    "a callee with a member the link has no place for stays a custom entry",
			records: []rawRecord{{a2aCalleeNS, `{"url":"` + url + `","task":"task-1","context":"ctx-1","relation":"subsession","session":"s-1"}`}},
			entries: a2aCalleeNS,
		},
		{
			name: "a cancel with a member the link has no place for stays a custom entry",
			records: []rawRecord{
				calleeRecord(url, "task-1", "ctx-1", "subsession"),
				{a2aCancelNS, `{"url":"` + url + `","task":"task-1","reason":"timeout"}`},
			},
			entries: "link:subsession " + a2aCancelNS,
			callees: []calleeShape{{agentsession.RelSubsession, "task-1", false, true}},
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
		// The output on the new branch is the harness's answer: a copy
		// belongs to no dispatch of this branch and did not answer.
		if cl.Answered || cl.Dispatch != "" {
			t.Errorf("callee %d answered = %v on dispatch %q, want a copy that answered nothing", i, cl.Answered, cl.Dispatch)
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
		{"a notice naming an earlier child", func(s *agentsession.Session) []agentsession.Entry {
			a := agentsession.NewSubsessionLink("child-a", "call_1")
			if _, err := s.Append(a); err != nil {
				panic(err)
			}
			if _, err := s.Append(agentsession.NewSubsessionLink("child-b", "call_1")); err != nil {
				panic(err)
			}
			return []agentsession.Entry{agentsession.NewCancelSentLink(a)}
		}, "child-b"},
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

// TestA2ANestedCallRecordStaysCustom: a record written by a call a tool
// made through agentturn.Invoke names the call the session holds, the
// one whose tool made it, and a link of that call would read as its
// callee having answered it; it stays a custom entry. The outer call's
// own record is its link.
func TestA2ANestedCallRecordStaysCustom(t *testing.T) {
	ctx := context.Background()
	const url = "https://remote.example/a2a"
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	outer := agenttool.New("outer", "", func(ctx context.Context, _ echoArgs) (string, error) {
		if err := agenttool.WriteRecord(ctx, calleeRecord(url, "outer-task", "", "subsession")); err != nil {
			return "", err
		}
		if _, err := agentturn.Invoke(ctx, "remote", json.RawMessage(`{"text":"x"}`)); err != nil {
			return "", err
		}
		return "combined", nil
	})
	inner := recordsTool(calleeRecord(url, "inner-task", "", "subsession"), cancelRecord(url, "outer-task"))
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{outer, inner}, MaxTurns: 1, ToolRecorder: rec.RecordFunc()})
	defer rec.Attach(a)()
	if _, err := a.Prompt(ctx, openresponses.UserText("x")); err != nil {
		t.Fatal(err)
	}
	call := callsOf(t, s)["outer"]
	if call == nil {
		t.Fatalf("no outer call in %q", entryTypes(s))
	}
	callees, err := s.Callees(s.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := shapesOf(callees), []calleeShape{{agentsession.RelSubsession, "outer-task", false, true}}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("callees = %+v, want the outer call's alone, %+v", got, want)
	}
	for _, ns := range []string{a2aCalleeNS, a2aCancelNS} {
		cs := customs(s, ns)
		if len(cs) != 1 || cs[0].CallID != call.ID() {
			t.Errorf("%s custom entries = %+v, want the nested call's one, under %s", ns, cs, call.ID())
		}
	}
	verifyAll(t, s)
}

// TestA2AJobCancelInAChild: a cancel a job in a child run writes after
// the run ended goes to the child's session, reopened at its leaf, and
// names the callee link the child's run wrote there.
func TestA2AJobCancelInAChild(t *testing.T) {
	ctx := context.Background()
	const url = "https://remote.example/a2a"
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	var jobCtx context.Context
	remote := agenttool.New("remote", "", func(ctx context.Context, _ echoArgs) (string, error) {
		jobCtx = ctx
		return "started", agenttool.WriteRecord(ctx, calleeRecord(url, "task-1", "", "subsession"))
	})
	childCfg := agentturn.Config{Name: "helper", Description: "delegates", Model: scriptedCalls{{"remote", `{"text":"t"}`}},
		Tools: []agenttool.Tool{remote}, ToolRecorder: rec.RecordFunc()}
	helper := agent.New(childCfg, agent.WithObserver(rec.Observe))
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{helper}})
	defer rec.Attach(a)()
	if _, err := a.Prompt(ctx, openresponses.UserText("delegate")); err != nil {
		t.Fatal(err)
	}
	l := links(s)
	if len(l) != 1 || jobCtx == nil {
		t.Fatalf("links = %+v", l)
	}
	data, _ := json.Marshal(map[string]string{"url": url, "task": "task-1"})
	if err := rec.RecordFunc()(jobCtx, &agenttool.Record{NS: a2aCancelNS, Data: data}); err != nil {
		t.Fatal(err)
	}
	child, err := store.Open(ctx, l[0].Session)
	if err != nil {
		t.Fatal(err)
	}
	callees, err := child.Callees(child.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := shapesOf(callees), []calleeShape{{agentsession.RelSubsession, "task-1", true, false}}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("child callees = %+v, want %+v", got, want)
	}
	if n := len(customs(child, a2aCancelNS)); n != 0 {
		t.Errorf("%d cancels written as custom entries in the child", n)
	}
	verifyAll(t, s)
	verifyAll(t, child)
}

// TestCalleeCopyKeepsExtensionMembers: a copy carries the members of
// the link the library does not define, so it is the link whole, and
// leaves a target or cancel member behind, which a callee link has
// none of.
func TestCalleeCopyKeepsExtensionMembers(t *testing.T) {
	l := agentsession.NewCalleeSubsessionLink("call_1", agentsession.Callee{URL: "https://remote.example", Task: "task-1", Context: "ctx-1", Session: "s-1"})
	l.ID = "old"
	l.Unknown = map[string]json.RawMessage{"x:priority": json.RawMessage(`"high"`), "target": json.RawMessage(`7`), "cancel": json.RawMessage(`true`)}
	c := calleeCopy(l, "new")
	if c.ID != "new" || !sameCallee(c, l) {
		t.Errorf("copy = %+v, want %+v as new", c, l)
	}
	if len(c.Unknown) != 1 || string(c.Unknown["x:priority"]) != `"high"` {
		t.Errorf("copy's members = %v, want x:priority alone", c.Unknown)
	}
	c.Unknown["x:priority"] = json.RawMessage(`"low"`)
	if string(l.Unknown["x:priority"]) != `"high"` {
		t.Error("the copy shares the link's members")
	}
	s, err := agentsession.NewMemoryStore().Create(context.Background(), agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(agentsession.NewItemEntry(&openresponses.FunctionCall{CallID: "call_1", Name: "remote", Arguments: "{}"})); err != nil {
		t.Fatal(err)
	}
	id, err := s.Append(calleeCopy(l, ""))
	if err != nil {
		t.Fatal(err)
	}
	e, _ := s.Entry(id)
	if got, ok := e.(*agentsession.LinkEntry); !ok || string(got.Unknown["x:priority"]) != `"high"` || got.URL != l.URL {
		t.Errorf("stored copy = %+v", e)
	}
}

// failingNotice fails the first cancel notice appended while armed, as
// a store that runs out of room between two appends does.
type failingNotice struct {
	agentsession.Store
	armed *bool
}

func (s failingNotice) Append(ctx context.Context, id string, e agentsession.Entry) (string, error) {
	if l, ok := e.(*agentsession.LinkEntry); ok && l.Cancel != "" && *s.armed {
		*s.armed = false
		return "", errors.New("disk full")
	}
	return s.Store.Append(ctx, id, e)
}

// TestReplayedA2ACopyIsFinished pins the copy's idempotence: a copy of
// the branch left's callee links cut short after the link and before
// its notice is finished by the next attempt, by the same recorder or
// by one resumed after a restart, which keeps the copy it finds, writes
// the notice it lacks, and writes no second copy.
func TestReplayedA2ACopyIsFinished(t *testing.T) {
	const url = "https://remote.example/a2a"
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%v", restart), func(t *testing.T) {
			ctx := context.Background()
			armed := false
			store := failingNotice{agentsession.NewMemoryStore(), &armed}
			rec, s, err := Start(ctx, store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			tool := recordsTool(calleeRecord(url, "task-1", "", "subsession"), cancelRecord(url, "task-1"), calleeRecord(url, "task-2", "", "peer"))
			tools := []agenttool.Tool{tool}
			a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: tools, ToolRecorder: rec.RecordFunc()})
			unsub := rec.Attach(a)
			if _, err := a.Prompt(ctx, openresponses.UserText("go")); err != nil {
				t.Fatal(err)
			}
			unsub()
			c := callsOf(t, s)["remote"]
			if err := rec.Rebase(s, c.Dispatch.Parent); err != nil {
				t.Fatal(err)
			}
			answers, err := ReplayAnswers(ctx, s, tools, rec.ReadOptions()...)
			if err != nil {
				t.Fatal(err)
			}
			opts, err := AgentOptions(s, rec.ReadOptions()...)
			if err != nil {
				t.Fatal(err)
			}
			b := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: tools, ToolRecorder: rec.RecordFunc()}, opts...)
			unsub = rec.Attach(b)
			armed = true
			if end, err := b.Resume(ctx, answers...); err == nil && end.Reason == agentturn.ReasonDone {
				t.Fatalf("the cut copy did not fail the run: %+v", end)
			}
			unsub()
			if armed {
				t.Fatal("no notice was appended")
			}
			// The failed run gave the call up; the next attempt seeds
			// an agent from the record again.
			if restart {
				rec, s, err = Resume(ctx, store, s.ID())
				if err != nil {
					t.Fatal(err)
				}
			}
			if answers, err = ReplayAnswers(ctx, s, tools, rec.ReadOptions()...); err != nil {
				t.Fatal(err)
			}
			if opts, err = AgentOptions(s, rec.ReadOptions()...); err != nil {
				t.Fatal(err)
			}
			b = agentturn.New(agentturn.Config{Model: allCalls{}, Tools: tools, ToolRecorder: rec.RecordFunc()}, opts...)
			defer rec.Attach(b)()
			if end, err := b.Resume(ctx, answers...); err != nil || end.Reason != agentturn.ReasonDone {
				t.Fatalf("retry: err=%v end=%+v", err, end)
			}
			var record []string
			for _, e := range s.Path(s.Leaf()) {
				switch e := e.(type) {
				case *agentsession.LinkEntry:
					if e.Cancel != "" {
						record = append(record, "cancel")
					} else {
						record = append(record, "link:"+e.Rel)
					}
				case *agentsession.DecisionEntry:
					record = append(record, e.Verdict)
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
			want := []calleeShape{{agentsession.RelSubsession, "task-1", true, false}, {agentsession.RelPeer, "task-2", false, false}}
			if got := shapesOf(callees); fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("callees = %+v, want %+v", got, want)
			}
			// The cut run started as a resume and holds the copied
			// link and no decision, which VerifyRecords reads as the
			// shape of an input, as it reads any resume cut before its
			// first decision; every other run verifies.
			cut := ""
			for _, e := range s.Path(s.Leaf()) {
				if r, ok := e.(*agentsession.RunEntry); ok && r.Phase == agentsession.RunStart {
					cut = r.RunID
				}
				if _, ok := e.(*agentsession.LinkEntry); ok {
					break
				}
			}
			if err := s.VerifyRecords(s.Leaf()); err != nil && (cut == "" || !strings.Contains(err.Error(), cut)) {
				t.Errorf("verify records: %v, want only the cut run %q reported", err, cut)
			}
			if err := s.VerifyLinks(noLinkedSession(t)); err != nil {
				t.Errorf("verify links: %v", err)
			}
		})
	}
}

// TestForkCopiesNoCallees: a fork that answers a call with the output
// its origin holds writes none of the origin's callee links or cancel
// notices: the fork started none of that work and sent none of those
// cancels, and its fork_of reaches the origin, which holds them.
func TestForkCopiesNoCallees(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	origin, err := store.Create(ctx, agentsession.Header{Records: agentsession.AllRecords})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, origin.ID(), &agentsession.ItemEntry{Item: openresponses.UserText("ask the remote")}); err != nil {
		t.Fatal(err)
	}
	call := &openresponses.FunctionCall{CallID: "call_1", Name: "remote", Arguments: `{"text":"t"}`}
	target, err := store.Append(ctx, origin.ID(), &agentsession.ItemEntry{Item: call, ResponseID: "resp_1"})
	if err != nil {
		t.Fatal(err)
	}
	base, err := store.Append(ctx, origin.ID(), &agentsession.ResponseEntry{ResponseID: "resp_1", Status: openresponses.ResponseStatusCompleted})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, origin.ID(), agentsession.NewDispatch(call.CallID, target)); err != nil {
		t.Fatal(err)
	}
	link := agentsession.NewCalleeSubsessionLink(call.CallID, agentsession.Callee{URL: "https://remote.example", Task: "task-1"})
	if _, err := store.Append(ctx, origin.ID(), link); err != nil {
		t.Fatal(err)
	}
	for _, e := range []agentsession.Entry{
		agentsession.NewCancelSentLink(link),
		&agentsession.ItemEntry{Item: openresponses.NewFunctionCallOutput(call.CallID, "answered")},
	} {
		if _, err := store.Append(ctx, origin.ID(), e); err != nil {
			t.Fatal(err)
		}
	}
	rec, fork, err := Start(ctx, store, agentsession.Header{Base: base, ParentSession: origin.ID()})
	if err != nil {
		t.Fatal(err)
	}
	tools := []agenttool.Tool{recordsTool()}
	answers, err := ReplayAnswers(ctx, fork, tools, rec.ReadOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	if len(answers) != 1 || answers[0].Output == nil || answers[0].Reason != ranInOriginReason {
		t.Fatalf("answers = %+v, want the origin's output", answers)
	}
	opts, err := AgentOptions(fork, rec.ReadOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: tools}, opts...)
	defer rec.Attach(a)()
	if end, err := a.Resume(ctx, answers...); err != nil || end.Reason != agentturn.ReasonDone {
		t.Fatalf("resume: err=%v end=%+v", err, end)
	}
	var record []string
	for _, e := range fork.Path(fork.Leaf()) {
		if fork.Prefix(e.Base().ID) {
			continue
		}
		switch e := e.(type) {
		case *agentsession.LinkEntry:
			t.Errorf("the fork wrote a link: %+v", e)
		case *agentsession.DecisionEntry:
			record = append(record, e.Verdict)
		case *agentsession.ItemEntry:
			if _, ok := e.Item.(*openresponses.FunctionCallOutput); ok {
				record = append(record, "output")
			}
		}
	}
	if got := strings.Join(record, " "); got != "answer output" {
		t.Errorf("the fork's record for the call = %q, want %q", got, "answer output")
	}
	if err := fork.VerifyRecords(fork.Leaf()); err != nil {
		t.Errorf("verify records: %v", err)
	}
}
