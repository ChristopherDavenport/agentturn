package session

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
)

// firstEntryOf returns the ID of the first item entry on the path whose
// item is a user message with text.
func firstEntryOf(t *testing.T, s *agentsession.Session, text string) string {
	t.Helper()
	for _, e := range s.Path(s.Leaf()) {
		if ie, ok := e.(*agentsession.ItemEntry); ok {
			if m, ok := ie.Item.(*openresponses.Message); ok && m.Role == openresponses.RoleUser && strings.Contains(string(mustJSON(t, m)), text) {
				return ie.ID
			}
		}
	}
	t.Fatalf("no user message %q on the path", text)
	return ""
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestSiblingCallIDsStayReserved checks that an agent writing a branch
// of a session does not take the call ID of a call on another branch:
// the format names one call by a call ID in the whole session, and
// agentsession refuses a function call whose ID any branch holds. A
// rewind by Rebase, a /clear by Rebase to "", and a resume on a fork
// by AgentOptions each seed an agent whose transcript lacks call_0,
// which the model then gives again.
func TestSiblingCallIDsStayReserved(t *testing.T) {
	for _, reseed := range []string{"rewind", "clear", "fork"} {
		t.Run(reseed, func(t *testing.T) {
			ctx := context.Background()
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(ctx, store, agentsession.Header{Records: agentsession.AllRecords})
			if err != nil {
				t.Fatal(err)
			}
			cfg := agentturn.Config{Model: perResponseModel{}, ModelName: "m", Tools: []agenttool.Tool{upper}}
			first := agentturn.New(cfg)
			detach := rec.Attach(first)
			if _, err := first.Prompt(ctx, openresponses.UserText("one")); err != nil {
				t.Fatal(err)
			}
			one := firstEntryOf(t, s, "one")
			detach()
			// Each case seeds a new agent, so only the record can say
			// which IDs the session holds.
			a := agentturn.New(cfg)
			switch reseed {
			case "rewind", "clear":
				detach = rec.Attach(a)
				at := one
				if reseed == "clear" {
					at = ""
				}
				if err := rec.Rebase(s, at); err != nil {
					t.Fatal(err)
				}
				cx, err := s.Context()
				if err != nil {
					t.Fatal(err)
				}
				if err := a.SetTranscript(cx.Items); err != nil {
					t.Fatal(err)
				}
			case "fork":
				// A branch from "one" that makes no call leaves the
				// store's leaf off call_0's branch.
				detach = rec.Attach(a)
				if err := rec.Rebase(s, one); err != nil {
					t.Fatal(err)
				}
				cx, err := s.Context()
				if err != nil {
					t.Fatal(err)
				}
				if err := a.SetTranscript(cx.Items); err != nil {
					t.Fatal(err)
				}
				text := a.Config()
				text.Model = &sameIDModel{}
				if err := a.SetConfig(text); err != nil {
					t.Fatal(err)
				}
				if _, err := a.Prompt(ctx, openresponses.UserText("aside")); err != nil {
					t.Fatal(err)
				}
				detach()
				if rec, s, err = Resume(ctx, store, s.ID()); err != nil {
					t.Fatal(err)
				}
				opts, err := AgentOptions(s)
				if err != nil {
					t.Fatal(err)
				}
				a = agentturn.New(cfg, opts...)
				detach = rec.Attach(a)
			}
			defer detach()
			for _, item := range s.Path(s.Leaf()) {
				if ie, ok := item.(*agentsession.ItemEntry); ok {
					if call, ok := ie.Item.(*openresponses.FunctionCall); ok && call.CallID == "call_0" {
						t.Fatal("call_0 is on the new branch; the test needs it on a sibling")
					}
				}
			}
			end, err := a.Prompt(ctx, openresponses.UserText("two"))
			if err != nil || end.Reason != agentturn.ReasonDone {
				t.Fatalf("prompt on the branch: err=%v end=%+v", err, end)
			}
			ids := map[string]int{}
			for _, e := range s.Entries() {
				if ie, ok := e.(*agentsession.ItemEntry); ok {
					if call, ok := ie.Item.(*openresponses.FunctionCall); ok {
						ids[call.CallID]++
					}
				}
			}
			if len(ids) != 2 || ids["call_0"] != 1 {
				t.Errorf("call IDs in the session = %v, want call_0 and one of the loop's own", ids)
			}
			verifyAll(t, s)
		})
	}
}

// TestReopenedChildSessionCallIDsStayReserved checks that a child
// session reopened under the same call, the call run again, holds no
// repeated call ID: the child's agent is new and its model numbers its
// calls from call_0 again, and ChildContext reserves the IDs the child
// session holds. The observer swallows a store's refusal, so without
// the reservation the second run is missing from the child session.
func TestReopenedChildSessionCallIDsStayReserved(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, _, err := Start(ctx, store, agentsession.Header{Records: agentsession.AllRecords})
	if err != nil {
		t.Fatal(err)
	}
	childCfg := agentturn.Config{Name: "specialist", Description: "calls upper", Model: perResponseModel{}, Tools: []agenttool.Tool{upper}}
	specialist := agent.New(childCfg, agent.WithObserver(rec.Observe), agent.WithRunContext(rec.ChildContext))
	call := agenttool.Call{ID: "task_1", Args: json.RawMessage(`{"input":"go"}`)}
	for range 2 {
		if _, err := specialist.Execute(ctx, call); err != nil {
			t.Fatal(err)
		}
	}
	child, err := store.Open(ctx, agentsession.SubsessionID(rec.SessionID(), call.ID))
	if err != nil {
		t.Fatal(err)
	}
	calls, err := child.Calls(child.Leaf())
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[0].ID() == calls[1].ID() {
		t.Fatalf("child session holds %d calls: %q", len(calls), entryTypes(child))
	}
	if err := child.VerifyRecords(child.Leaf()); err != nil {
		t.Errorf("verify child records: %v", err)
	}
}

// sentModel records the requests it is sent and answers as the model
// it wraps does.
type sentModel struct {
	openresponses.Streamer
	mu   sync.Mutex
	sent []openresponses.Request
}

func (m *sentModel) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.mu.Lock()
	m.sent = append(m.sent, req)
	m.mu.Unlock()
	return m.Streamer.CreateStream(ctx, req, sink)
}

var callIDAlphabet = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// TestRenamedCallIDIsSent checks that the ID the loop gives a call
// whose ID the model repeated is the one the provider is sent next,
// and that every response's request_hash, rebuilt from the path, is the
// hash of the request actually sent: the format asks a writer that
// makes up its own IDs to send them, or omit the hash. The ID is drawn
// from the alphabet every provider takes, whatever the model's was.
func TestRenamedCallIDIsSent(t *testing.T) {
	for _, id := range []string{"call_0", "call.0:x"} {
		t.Run(id, func(t *testing.T) {
			ctx := context.Background()
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(ctx, store, agentsession.Header{Records: agentsession.AllRecords})
			if err != nil {
				t.Fatal(err)
			}
			m := &sentModel{Streamer: &sameIDModel{turns: [][]string{{id}, {id}}}}
			a := agentturn.New(agentturn.Config{Model: m, ModelName: "m", Tools: []agenttool.Tool{upper}})
			defer rec.Attach(a)()
			if end, err := a.Prompt(ctx, openresponses.UserText("abc")); err != nil || end.Reason != agentturn.ReasonDone {
				t.Fatalf("prompt: err=%v end=%+v", err, end)
			}
			if len(m.sent) != 3 {
				t.Fatalf("requests = %d", len(m.sent))
			}
			var ids []string
			for _, item := range m.sent[2].Input {
				if call, ok := item.(*openresponses.FunctionCall); ok {
					ids = append(ids, call.CallID)
				}
			}
			if len(ids) != 2 || ids[0] != id || ids[1] == id {
				t.Errorf("the last request names calls %q", ids)
			}
			if len(ids) == 2 && !callIDAlphabet.MatchString(ids[1]) {
				t.Errorf("renamed call ID %q is outside [A-Za-z0-9_-]", ids[1])
			}
			var hashes []string
			for _, e := range s.Path(s.Leaf()) {
				if r, ok := e.(*agentsession.ResponseEntry); ok {
					hashes = append(hashes, r.RequestHash)
				}
			}
			if len(hashes) != len(m.sent) {
				t.Fatalf("responses = %d, requests = %d", len(hashes), len(m.sent))
			}
			for i, req := range m.sent {
				want, err := RequestHash(Canonical(req))
				if err != nil {
					t.Fatal(err)
				}
				if hashes[i] != want {
					t.Errorf("response %d hash %q, the request sent hashes to %q", i, hashes[i], want)
				}
			}
			verifyAll(t, s)
		})
	}
}

// TestNoRecordAfterAnOutput checks that the recorder writes no
// decision or dispatch for a call whose output it has written, which
// the format refuses: the output ends the call. A host driving
// Handle, or a loop handed a call already answered, can raise one; a
// decision is skipped and a dispatch refused, so the tool does not
// run a second time as the same call.
func TestNoRecordAfterAnOutput(t *testing.T) {
	cases := []struct {
		name string
		// after are the events raised for the call after its output;
		// dispatchErr says a dispatch among them is refused.
		after       func(runID, callID string) []agentturn.Event
		dispatchErr bool
	}{
		{name: "blocked", after: func(runID, callID string) []agentturn.Event {
			return []agentturn.Event{&agentturn.ToolStart{RunID: runID, CallID: callID, Name: "upper", Args: json.RawMessage(`{"text":"t"}`),
				Decision: &agentturn.ToolDecision{Action: agentturn.Block, Reason: "no"}}}
		}},
		{name: "deferred", after: func(runID, callID string) []agentturn.Event {
			return []agentturn.Event{&agentturn.ToolStart{RunID: runID, CallID: callID, Name: "upper", Args: json.RawMessage(`{"text":"t"}`),
				Decision: &agentturn.ToolDecision{Action: agentturn.Defer}}}
		}},
		{name: "output again", after: func(runID, callID string) []agentturn.Event {
			return []agentturn.Event{&agentturn.ItemEnd{RunID: runID, Item: openresponses.NewFunctionCallOutput(callID, "again")}}
		}},
		{name: "dispatched again", dispatchErr: true, after: func(runID, callID string) []agentturn.Event {
			return []agentturn.Event{
				&agentturn.ToolStart{RunID: runID, CallID: callID, Name: "upper", Args: json.RawMessage(`{"text":"t"}`)},
				&agentturn.ToolDispatch{RunID: runID, CallID: callID, Name: "upper"},
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(ctx, store, agentsession.Header{Records: agentsession.AllRecords})
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(agentturn.Config{Model: &sameIDModel{turns: [][]string{{"call_a"}}}, Tools: []agenttool.Tool{upper}})
			unsub := rec.Attach(a)
			if _, err := a.Prompt(ctx, openresponses.UserText("abc")); err != nil {
				t.Fatal(err)
			}
			unsub()
			// The same writer, in a later run, and then a recorder
			// seeded from the store, since each keeps what it knows of
			// the call its own way.
			for i, r := range []*Recorder{rec, nil} {
				if r == nil {
					if r, s, err = Resume(ctx, store, s.ID()); err != nil {
						t.Fatal(err)
					}
				}
				runID := "run_after_" + string(rune('a'+i))
				if err := r.Handle(ctx, &agentturn.RunStart{RunID: runID, Source: agentturn.SourceInput}); err != nil {
					t.Fatal(err)
				}
				for _, ev := range tc.after(runID, "call_a") {
					err := r.Handle(ctx, ev)
					if _, ok := ev.(*agentturn.ToolDispatch); ok && tc.dispatchErr {
						if !errors.Is(err, agentsession.ErrCallCompleted) {
							t.Errorf("dispatch after the output: %v, want ErrCallCompleted", err)
						}
						continue
					}
					if err != nil {
						t.Fatalf("%T after the output: %v", ev, err)
					}
				}
				if err := r.Handle(ctx, &agentturn.RunEnd{RunID: runID, Reason: agentturn.ReasonAborted, Err: context.Canceled}); err != nil {
					t.Fatal(err)
				}
			}
			dispatches := 0
			for _, e := range s.Entries() {
				switch v := e.(type) {
				case *agentsession.DecisionEntry:
					t.Errorf("decision %s written for %s", v.Verdict, v.CallID)
				case *agentsession.DispatchEntry:
					dispatches++
				}
			}
			if dispatches != 1 {
				t.Errorf("%d dispatches written, want the tool's one", dispatches)
			}
			if err := s.VerifyRecords(s.Leaf()); err != nil {
				t.Errorf("verify records: %v", err)
			}
		})
	}
}

// TestRunSourceIsTheOpeningTakeUp checks the source the recorder
// writes for a run that takes up a held call against the format's
// rule, the first output, decision or dispatch of the run, messages
// before it aside: an approval whose note is written before the call's
// output, an output with a note, and a prompt that opens with the
// output and goes on with a message.
func TestRunSourceIsTheOpeningTakeUp(t *testing.T) {
	cases := []struct {
		name string
		take func(ctx context.Context, a *agentturn.Agent, callID string) error
	}{
		{name: "approval with a note", take: func(ctx context.Context, a *agentturn.Agent, callID string) error {
			_, err := a.Resume(ctx, agentturn.Approve(callID).WithNote("go ahead"))
			return err
		}},
		{name: "output with a note", take: func(ctx context.Context, a *agentturn.Agent, callID string) error {
			_, err := a.Resume(ctx, agentturn.Output(openresponses.NewFunctionCallOutput(callID, "done by hand")).WithNote("did it myself"))
			return err
		}},
		{name: "prompt opening with the output", take: func(ctx context.Context, a *agentturn.Agent, callID string) error {
			_, err := a.Prompt(ctx, openresponses.NewFunctionCallOutput(callID, "done by hand"), openresponses.UserText("and then"))
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(ctx, store, agentsession.Header{Records: agentsession.AllRecords})
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(agentturn.Config{Model: &sameIDModel{turns: [][]string{{"call_a"}}}, Tools: []agenttool.Tool{upper},
				BeforeToolCall: func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
					return &agentturn.ToolDecision{Action: agentturn.Defer}, nil
				}})
			defer rec.Attach(a)()
			if end, err := a.Prompt(ctx, openresponses.UserText("abc")); err != nil || end.Reason != agentturn.ReasonInputRequired {
				t.Fatalf("prompt: err=%v end=%+v", err, end)
			}
			if err := tc.take(ctx, a, "call_a"); err != nil {
				t.Fatal(err)
			}
			runs := runsOf(t, s)
			if len(runs) != 2 || runs[1].Start.Source != agentsession.SourceResume {
				t.Fatalf("runs = %d, in %q", len(runs), entryTypes(s))
			}
			verifyAll(t, s)
		})
	}
}

// TestAppOnlyCallTakesNoRecord checks that a function call the filter
// keeps from the model, written as a custom entry, takes no decision or
// dispatch: a record names its call by target, which must be a
// function call item, and agentsession refuses one naming anything
// else with ErrBadTarget.
func TestAppOnlyCallTakesNoRecord(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	noCalls := func(tr agentturn.Transcript) agentturn.Transcript {
		var out agentturn.Transcript
		for _, item := range tr {
			if _, ok := item.(*openresponses.FunctionCall); !ok {
				out = append(out, item)
			}
		}
		return out
	}
	rec, s, err := Start(ctx, store, agentsession.Header{Records: agentsession.AllRecords}, WithFilter(noCalls))
	if err != nil {
		t.Fatal(err)
	}
	const runID = "run_app"
	call := &openresponses.FunctionCall{CallID: "call_app", Name: "upper", Arguments: `{"text":"t"}`}
	for _, ev := range []agentturn.Event{
		&agentturn.RunStart{RunID: runID, Source: agentturn.SourceInput},
		&agentturn.ItemEnd{RunID: runID, Item: openresponses.UserText("abc")},
		&agentturn.ItemEnd{RunID: runID, Item: call},
		&agentturn.ToolStart{RunID: runID, CallID: call.CallID, Name: call.Name, Args: json.RawMessage(call.Arguments),
			Decision: &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "ask"}},
		&agentturn.ToolDispatch{RunID: runID, CallID: call.CallID, Name: call.Name},
		&agentturn.RunEnd{RunID: runID, Reason: agentturn.ReasonAborted, Err: context.Canceled},
	} {
		if err := rec.Handle(ctx, ev); err != nil {
			t.Fatalf("%T: %v", ev, err)
		}
	}
	if got := entryTypes(s); strings.Contains(got, "decision") || strings.Contains(got, "dispatch") {
		t.Errorf("entries = %q", got)
	}
	if err := s.VerifyRecords(s.Leaf()); err != nil {
		t.Errorf("verify records: %v", err)
	}
}
