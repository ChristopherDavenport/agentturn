package session

import (
	"context"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// fromLinkOf returns the from_link of the item entry of s holding the
// message text, and whether there is one.
func fromLinkOf(s *agentsession.Session, text string) (string, bool) {
	for _, e := range s.Entries() {
		if it, ok := e.(*agentsession.ItemEntry); ok {
			if m, ok := it.Item.(*openresponses.Message); ok && m.Text() == text {
				return it.FromLink, true
			}
		}
	}
	return "", false
}

// TestALateResultNamesThePeerLink pins RFC 0001 Composition, as a peer
// (#290): a detached call's late result, delivered under the callee
// trigger naming the call and its task, is written with format 0.15's
// from_link naming that call's peer link, whether the delivery starts
// its run at once or is owed across a restart; a trigger naming a call
// with no peer link, or another task, names none.
func TestALateResultNamesThePeerLink(t *testing.T) {
	const url = "https://remote.example/a2a"
	for _, tc := range []struct {
		name     string
		relation string
		task     string
		restart  bool
		want     bool
	}{
		{name: "delivered", relation: "peer", task: "task-1", want: true},
		{name: "delivered, the trigger naming no task", relation: "peer", want: true},
		{name: "owed across a restart", relation: "peer", task: "task-1", restart: true, want: true},
		{name: "another task", relation: "peer", task: "task-2"},
		{name: "a subsession", relation: "subsession", task: "task-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(ctx, store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			cfg := agentturn.Config{Model: allCalls{}, ModelName: "m", Tools: []agenttool.Tool{recordsTool(calleeRecord(url, "task-1", "ctx-1", tc.relation))}, ToolRecorder: rec.RecordFunc()}
			a := agentturn.New(cfg)
			unsub := rec.Attach(a)
			end, err := a.Prompt(ctx, openresponses.UserText("start the remote work"))
			if err != nil || end.Reason != agentturn.ReasonDone {
				t.Fatalf("first run: %v, %+v", err, end)
			}
			var callID string
			for _, item := range end.Items {
				if c, ok := item.(*openresponses.FunctionCall); ok {
					callID = c.CallID
				}
			}
			var linkID string
			for _, l := range links(s) {
				if l.CallID == callID && l.Task == "task-1" {
					linkID = l.ID
				}
			}
			if linkID == "" {
				t.Fatalf("no link for the call in %s", entryTypes(s))
			}
			const late = "the remote agent finished: 42"
			if tc.restart {
				// Written and accepted, then the process quit before any
				// run took it.
				d := agentsession.NewDelivery(openresponses.UserText(late))
				d.Trigger, _ = sessionTrigger(agentturn.CalleeTrigger(callID, tc.task))
				if _, err := store.Append(ctx, s.ID(), d); err != nil {
					t.Fatal(err)
				}
				unsub()
				if rec, s, err = Resume(ctx, store, s.ID()); err != nil {
					t.Fatal(err)
				}
				opts, err := AgentOptions(s)
				if err != nil {
					t.Fatal(err)
				}
				cfg.ToolRecorder = rec.RecordFunc()
				a = agentturn.New(cfg, opts...)
				unsub = rec.Attach(a)
				if n, deliver := rec.Requeue(ctx, a); n != 1 || !deliver {
					t.Fatalf("Requeue = %d, %v", n, deliver)
				}
				if _, err := a.Continue(ctx); err != nil {
					t.Fatal(err)
				}
			} else if _, _, err := a.Deliver(agentturn.ContextWithTrigger(ctx, agentturn.CalleeTrigger(callID, tc.task)), openresponses.UserText(late)); err != nil {
				t.Fatal(err)
			}
			unsub()
			s, err = store.Open(ctx, s.ID())
			if err != nil {
				t.Fatal(err)
			}
			from, ok := fromLinkOf(s, late)
			if !ok {
				t.Fatalf("the late result is not on the path: %s", entryTypes(s))
			}
			want := ""
			if tc.want {
				want = linkID
			}
			if from != want {
				t.Errorf("from_link = %q, want %q", from, want)
			}
			if err := s.VerifyRecords(s.Leaf()); err != nil {
				t.Errorf("VerifyRecords: %v", err)
			}
			verifyAll(t, s)
		})
	}
}
