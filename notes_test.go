package agentturn

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// TestEveryNoteFollowsItsBatch pins the one rule for a note: whatever
// the user said reaches the model after every output of the batch it
// belongs to, never between the calls and their outputs. An accept's
// note on a nested call (#245), at any depth, and a note on a tool's own
// question each follow the invoking call's batch, as a model call's note
// does.
func TestEveryNoteFollowsItsBatch(t *testing.T) {
	accept := func(note string) agenttool.Elicitor {
		return func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) {
			return agenttool.Answer{Action: agenttool.ActionAccept, Note: note}, nil
		}
	}
	read := agenttool.New("read", "reads", func(context.Context, echoArgs) (string, error) { return "contents", nil })
	other := agenttool.New("other", "", upper)
	invokes := func(name, target string) agenttool.Tool {
		return agenttool.New(name, "", func(ctx context.Context, _ echoArgs) (string, error) {
			if _, err := Invoke(ctx, target, json.RawMessage(`{"text":"t"}`)); err != nil {
				return "", err
			}
			return "done", nil
		})
	}
	confirm := agenttool.New("confirm", "", func(ctx context.Context, _ echoArgs) (string, error) {
		elicit, _ := agenttool.ElicitorFrom(ctx)
		if _, err := elicit(ctx, agenttool.Elicitation{Message: "go ahead?"}); err != nil {
			return "", err
		}
		return "confirmed", nil
	})
	defersRead := func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
		if info.Call.Name == "read" {
			return &ToolDecision{Action: Defer, Reason: "approval required by read"}, nil
		}
		return nil, nil
	}
	cases := []struct {
		name  string
		tools []agenttool.Tool
		calls []string
		note  string
	}{
		{name: "an accept on a nested call", tools: []agenttool.Tool{invokes("eval", "read"), read, other}, calls: []string{"eval", "other"}, note: "The user said: only the unit tests"},
		{name: "an accept two calls deep", tools: []agenttool.Tool{invokes("eval", "inner"), invokes("inner", "read"), read, other}, calls: []string{"eval", "other"}, note: "The user said: only the unit tests"},
		{name: "a tool's own question", tools: []agenttool.Tool{confirm, other}, calls: []string{"confirm", "other"}, note: "The user said: only the unit tests"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Model: callsNamed{names: tc.calls}, Tools: tc.tools, BeforeToolCall: defersRead, ToolElicitor: accept("only the unit tests"), MaxTurns: 1}
			_, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
			if err != nil {
				t.Fatal(err)
			}
			if got := itemTypes(end.Items); got != "user function_call function_call function_call_output function_call_output developer" {
				t.Fatalf("items = %q", got)
			}
			if out := end.Items[3].(*openresponses.FunctionCallOutput); strings.HasPrefix(out.Output.Text, "error") {
				t.Errorf("the first call failed: %q", out.Output.Text)
			}
			if m := end.Items[5].(*openresponses.Message); m.Text() != tc.note {
				t.Errorf("note = %q, want %q", m.Text(), tc.note)
			}
		})
	}
}

// TestANoteWaitsForADeferredCall pins that a batch's notes wait while a
// call of it is deferred: the deferred call's output comes from the
// Resume, and the notes follow it, the batch's own and then the
// Resume's, so no message splits the batch's calls from their outputs.
func TestANoteWaitsForADeferredCall(t *testing.T) {
	hook := func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
		if info.Call.Name == "a" {
			return &ToolDecision{Note: "a ran in the sandbox"}, nil
		}
		return &ToolDecision{Action: Defer, Reason: "ask"}, nil
	}
	a := New(Config{Model: &twoCalls{}, Tools: []agenttool.Tool{agenttool.New("a", "", upper), agenttool.New("b", "", upper)}, BeforeToolCall: hook, MaxTurns: 1})
	end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
	if err != nil || end.Reason != ReasonInputRequired {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	if got := itemTypes(a.State().Transcript); got != "user function_call function_call function_call_output" {
		t.Fatalf("transcript while b waits = %q", got)
	}
	b := end.Pending[0].Call.CallID
	if _, err := a.Resume(context.Background(), Output(openresponses.NewFunctionCallOutput(b, "did it myself")).WithNote("that one I handled")); err != nil {
		t.Fatal(err)
	}
	tr := a.State().Transcript
	if got := itemTypes(tr[:6]); got != "user function_call function_call function_call_output function_call_output developer" {
		t.Fatalf("transcript = %q", itemTypes(tr))
	}
	if m := tr[5].(*openresponses.Message); m.Text() != "a ran in the sandbox" {
		t.Errorf("the batch's note = %q", m.Text())
	}
	if m, ok := tr[6].(*openresponses.Message); !ok || m.Role != openresponses.RoleUser || m.Text() != "that one I handled" {
		t.Errorf("the Resume's note = %#v", tr[6])
	}
}

// deferB allows a with a note and defers b.
func deferB(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
	if info.Call.Name == "a" {
		return &ToolDecision{Note: "a ran in the sandbox"}, nil
	}
	return &ToolDecision{Action: Defer, Reason: "ask"}, nil
}

func abTools() []agenttool.Tool {
	return []agenttool.Tool{agenttool.New("a", "", upper), agenttool.New("b", "", upper)}
}

// noteItems is the items of notes.
func noteItems(notes []Note) []openresponses.Item {
	var out []openresponses.Item
	for _, n := range notes {
		out = append(out, n.Item)
	}
	return out
}

// texts lists the text of every message in items, role first.
func texts(items []openresponses.Item) []string {
	var out []string
	for _, it := range items {
		if m, ok := it.(*openresponses.Message); ok {
			out = append(out, string(m.Role)+": "+m.Text())
		}
	}
	return out
}

// TestHeldNotesTravelWithThePendingCalls pins review finding 1: the
// notes a deferred batch holds are on the run end's pending calls, so a
// caller of the stateless Run passes them back after its outputs, and
// an agent seeded with the pending calls, as after a restart, appends
// them after the outputs its Resume brings.
func TestHeldNotesTravelWithThePendingCalls(t *testing.T) {
	cfg := Config{Model: &twoCalls{}, Tools: abTools(), BeforeToolCall: deferB, MaxTurns: 1}
	_, end, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	if len(end.Pending) != 1 {
		t.Fatalf("pending = %v", pendingReasons(end))
	}
	notes := end.Pending[0].Notes
	if got := texts(noteItems(notes)); strings.Join(got, "|") != "developer: a ran in the sandbox" {
		t.Fatalf("pending notes = %q", got)
	}
	b := end.Pending[0].Call.CallID

	// A restart: a new agent seeded with the transcript and the pending
	// calls as the run left them.
	a := New(Config{Model: &twoCalls{}, Tools: abTools(), MaxTurns: 1}, WithTranscript(end.Items), WithPending(end.Pending))
	if _, err := a.Resume(context.Background(), Output(openresponses.NewFunctionCallOutput(b, "did it myself"))); err != nil {
		t.Fatal(err)
	}
	tr := a.State().Transcript
	if got := itemTypes(tr[:6]); got != "user function_call function_call function_call_output function_call_output developer" {
		t.Errorf("transcript after the restart's resume = %q", itemTypes(tr))
	}
}

// TestACutCallsNoteWaitsForItsAnswer pins review finding 2: the note on
// a call an abort cut is held with the batch and follows the call's
// output when a Resume answers it.
func TestACutCallsNoteWaitsForItsAnswer(t *testing.T) {
	started := make(chan struct{})
	slow := agenttool.New("a", "", func(ctx context.Context, _ echoArgs) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	})
	note := func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
		if info.Call.Name == "a" {
			return &ToolDecision{Note: "a ran in the sandbox"}, nil
		}
		return nil, nil
	}
	a := New(Config{Model: &twoCalls{}, Tools: []agenttool.Tool{slow, agenttool.New("b", "", upper)}, BeforeToolCall: note, MaxTurns: 1})
	go func() {
		<-started
		a.Abort()
	}()
	end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
	if err != nil || end.Reason != ReasonAborted {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	if len(end.Pending) != 1 || strings.Join(texts(noteItems(end.Pending[0].Notes)), "|") != "developer: a ran in the sandbox" {
		t.Fatalf("pending = %+v", end.Pending)
	}
	if _, err := a.Resume(context.Background(), OutcomeUnknown(end.Pending[0].Call.CallID)); err != nil {
		t.Fatal(err)
	}
	tr := a.State().Transcript
	if got := itemTypes(tr[:6]); got != "user function_call function_call function_call_output function_call_output developer" {
		t.Errorf("transcript = %q", itemTypes(tr))
	}
}

// TestSetTranscriptKeepsNotesWhileTheirCallsWait pins review finding
// 3: SetTranscript keeps the notes while a call they wait on is still
// pending, moving them to the last such call, and drops them with the
// calls.
func TestSetTranscriptKeepsNotesWhileTheirCallsWait(t *testing.T) {
	tools := append(abTools(), agenttool.New("c", "", upper))
	a := New(Config{Model: &twoCalls{}, Tools: tools, BeforeToolCall: deferB, MaxTurns: 1})
	end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
	if err != nil {
		t.Fatal(err)
	}
	if len(end.Pending) != 2 || len(end.Pending[1].Notes) != 1 {
		t.Fatalf("pending = %+v, want b and c, the note on c", end.Pending)
	}
	tr := a.State().Transcript
	// c answered by the host: b still waits, and the note with it.
	answered := append(append(Transcript(nil), tr...), openresponses.NewFunctionCallOutput(end.Pending[1].Call.CallID, "done"))
	if err := a.SetTranscript(answered); err != nil {
		t.Fatal(err)
	}
	if p := a.State().Pending; len(p) != 1 || strings.Join(texts(noteItems(p[0].Notes)), "|") != "developer: a ran in the sandbox" {
		t.Fatalf("pending after c was answered = %+v, want b with a's note", p)
	}
	if err := a.SetTranscript(tr[:1]); err != nil {
		t.Fatal(err)
	}
	if err := a.SetTranscript(tr); err != nil {
		t.Fatal(err)
	}
	for _, p := range a.State().Pending {
		if len(p.Notes) != 0 {
			t.Errorf("pending after the calls were gone = %+v, want the notes dropped with them", p)
		}
	}
}

// TestAFailedResumeKeepsItsNotesOnce pins review finding 4: a Resume
// that fails before its outputs are appended leaves the notes as they
// were, so its retry says them once.
func TestAFailedResumeKeepsItsNotesOnce(t *testing.T) {
	deferAll := func(context.Context, ToolCallInfo) (*ToolDecision, error) { return &ToolDecision{Action: Defer}, nil }
	a := New(Config{Model: &twoCalls{}, Tools: abTools(), BeforeToolCall: deferAll, MaxTurns: 1})
	end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
	if err != nil {
		t.Fatal(err)
	}
	fail := true
	a.Subscribe(func(_ context.Context, ev Event) error {
		if _, ok := ev.(*RunStart); ok && fail {
			fail = false
			return errors.New("not now")
		}
		return nil
	})
	answers := []Answer{
		Output(openresponses.NewFunctionCallOutput(end.Pending[0].Call.CallID, "no")).WithNote("use ci/ instead"),
		Output(openresponses.NewFunctionCallOutput(end.Pending[1].Call.CallID, "no")),
	}
	if _, err := a.Resume(context.Background(), answers...); err == nil {
		t.Fatal("the first resume did not fail")
	}
	if _, err := a.Resume(context.Background(), answers...); err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, s := range texts(a.State().Transcript) {
		if s == "user: use ci/ instead" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("the note is in the transcript %d times, want once: %q", n, texts(a.State().Transcript))
	}
}

// TestAnApprovalsNoteSurvivesTheHook pins review finding 5: a person's
// note on an approval and the note the hook gives the call both reach
// the model, the person's first, and a hook that defers the call holds
// nothing: the call never ran, and the person's note comes back with the
// answer that approves it again (review of v0.0.23, R2).
func TestAnApprovalsNoteSurvivesTheHook(t *testing.T) {
	call := &openresponses.FunctionCall{CallID: "call_a", Name: "a", Arguments: `{"text":"t"}`}
	seed := Transcript{openresponses.UserText("x"), call}
	for _, tc := range []struct {
		name string
		hook *ToolDecision
		want string
	}{
		{name: "allowed with a note", hook: &ToolDecision{Note: "a ran in the sandbox"}, want: "user: only the unit tests|developer: a ran in the sandbox"},
		{name: "deferred", hook: &ToolDecision{Action: Defer, Reason: "ask again"}, want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hook := func(context.Context, ToolCallInfo) (*ToolDecision, error) { return tc.hook, nil }
			a := New(Config{Model: &twoCalls{}, Tools: abTools(), BeforeToolCall: hook, MaxTurns: 1},
				WithTranscript(seed), WithPending([]PendingCall{{Call: call, Reason: PendingUndispatched}}))
			end, err := a.Resume(context.Background(), Approve("call_a").WithNote("only the unit tests"))
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			if tc.hook.Action == Defer {
				if len(end.Pending) != 1 {
					t.Fatalf("pending = %v", pendingReasons(end))
				}
				got = texts(noteItems(end.Pending[0].Notes))
			} else {
				got = texts(a.State().Transcript[3:5])
			}
			if strings.Join(got, "|") != tc.want {
				t.Errorf("notes = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestAChildsNoteGoesToTheChild pins review finding 6: a question a
// child agent's tool asks through the parent's elicitor leaves its note
// with the child's batch, not lost under a call the parent never made.
func TestAChildsNoteGoesToTheChild(t *testing.T) {
	confirm := agenttool.New("confirm", "", func(ctx context.Context, _ echoArgs) (string, error) {
		elicit, _ := agenttool.ElicitorFrom(ctx)
		if _, err := elicit(ctx, agenttool.Elicitation{Message: "go ahead?"}); err != nil {
			return "", err
		}
		return "confirmed", nil
	})
	var child *RunEnd
	delegate := agenttool.New("delegate", "", func(ctx context.Context, _ echoArgs) (string, error) {
		_, end, err := collect(t, Run(ctx, nil, openresponses.Items{openresponses.UserText("sub")}, Config{Model: callsNamed{names: []string{"confirm"}}, Tools: []agenttool.Tool{confirm}, MaxTurns: 1}))
		child = end
		return "delegated", err
	})
	accept := func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) {
		return agenttool.Answer{Action: agenttool.ActionAccept, Note: "only staging"}, nil
	}
	cfg := Config{Model: callsNamed{names: []string{"delegate"}}, Tools: []agenttool.Tool{delegate}, ToolElicitor: accept, MaxTurns: 1}
	_, parent, err := collect(t, Run(context.Background(), nil, openresponses.Items{openresponses.UserText("x")}, cfg))
	if err != nil {
		t.Fatal(err)
	}
	if child == nil || strings.Join(texts(child.Items), "|") != "user: sub|developer: The user said: only staging" {
		t.Errorf("child items = %q", texts(child.Items))
	}
	if got := texts(parent.Items); strings.Join(got, "|") != "user: x" {
		t.Errorf("parent items = %q", got)
	}
}

// count is how many messages of items read want.
func count(items []openresponses.Item, want string) int {
	n := 0
	for _, s := range texts(items) {
		if s == want {
			n++
		}
	}
	return n
}

// TestACallThatNeverRanHoldsNoNote pins R1: a call an abort cut before
// its dispatch holds no note, since the hook decides it again when it is
// approved, and says its note then, once.
func TestACallThatNeverRanHoldsNoNote(t *testing.T) {
	started := make(chan struct{})
	slow := agenttool.New("a", "", func(ctx context.Context, _ echoArgs) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	})
	hook := func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
		if info.Call.Name == "b" {
			return &ToolDecision{Note: "b ran in the sandbox"}, nil
		}
		return nil, nil
	}
	a := New(Config{Model: &twoCalls{}, Tools: []agenttool.Tool{slow, agenttool.New("b", "", upper)}, BeforeToolCall: hook, ToolExecution: ExecSequential, MaxTurns: 1})
	go func() {
		<-started
		a.Abort()
	}()
	end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
	if err != nil || end.Reason != ReasonAborted || len(end.Pending) != 2 || end.Pending[1].Reason != PendingUndispatched {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	if _, err := a.Resume(context.Background(), OutcomeUnknown(end.Pending[0].Call.CallID), Approve(end.Pending[1].Call.CallID)); err != nil {
		t.Fatal(err)
	}
	// The resume's batch: the calls, their outputs, the note once; the
	// next turn calls both again, and its batch has the note once more.
	tr := a.State().Transcript
	want := "user function_call function_call function_call_output function_call_output developer function_call function_call function_call_output function_call_output developer"
	if got := itemTypes(tr); got != want {
		t.Errorf("transcript = %q\nwant         %q", got, want)
	}
}

// TestAnApprovalOfACallThatNeverRanHoldsNoNote pins R2: a person's note
// on an approval whose call then never reaches its tool is not held, so
// the host's retry, which brings it again, says it once.
func TestAnApprovalOfACallThatNeverRanHoldsNoNote(t *testing.T) {
	decided := 0
	hook := func(context.Context, ToolCallInfo) (*ToolDecision, error) {
		decided++
		if decided == 1 {
			return &ToolDecision{Action: Defer, Reason: "ask"}, nil
		}
		return nil, nil
	}
	a := New(Config{Model: callsNamed{names: []string{"b"}}, Tools: []agenttool.Tool{agenttool.New("b", "", upper)}, BeforeToolCall: hook, MaxTurns: 1})
	end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
	if err != nil || len(end.Pending) != 1 {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	b := end.Pending[0].Call.CallID
	refuse := true
	a.Subscribe(func(_ context.Context, ev Event) error {
		if _, ok := ev.(*ToolDispatch); ok && refuse {
			refuse = false
			return errors.New("the audit log is down")
		}
		return nil
	})
	if _, err := a.Resume(context.Background(), Approve(b).WithNote("only the unit tests")); err == nil {
		t.Fatal("the first resume did not fail")
	}
	if p := a.State().Pending; len(p) != 1 || len(p[0].Notes) != 0 {
		t.Fatalf("pending after the withdrawn dispatch = %+v, want b with no note", p)
	}
	if _, err := a.Resume(context.Background(), Approve(b).WithNote("only the unit tests")); err != nil {
		t.Fatal(err)
	}
	if n := count(a.State().Transcript, "user: only the unit tests"); n != 1 {
		t.Errorf("the note is in the transcript %d times, want once: %q", n, texts(a.State().Transcript))
	}
}

// TestANoteIsKeptUntilItIsAppended pins R3: a note is off the waiting
// set only once it is in the transcript, so a failure part way through
// leaves the rest owed to the next run, and an answer's note waits from
// the moment its own output is in.
func TestANoteIsKeptUntilItIsAppended(t *testing.T) {
	t.Run("a failure between notes", func(t *testing.T) {
		hook := func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
			switch info.Call.Name {
			case "a":
				return &ToolDecision{Note: "n1"}, nil
			case "c":
				return &ToolDecision{Note: "n2"}, nil
			}
			return &ToolDecision{Action: Defer, Reason: "ask"}, nil
		}
		tools := append(abTools(), agenttool.New("c", "", upper))
		a := New(Config{Model: &twoCalls{}, Tools: tools, BeforeToolCall: hook, MaxTurns: 1})
		end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
		if err != nil || len(end.Pending) != 1 {
			t.Fatalf("prompt: err=%v end=%+v", err, end)
		}
		fail := true
		a.Subscribe(func(_ context.Context, ev Event) error {
			if e, ok := ev.(*ItemStart); ok && fail {
				if m, ok := e.Item.(*openresponses.Message); ok && m.Text() == "n2" {
					fail = false
					return errors.New("disk full")
				}
			}
			return nil
		})
		end, err = a.Resume(context.Background(), Output(openresponses.NewFunctionCallOutput(end.Pending[0].Call.CallID, "no")))
		if err == nil {
			t.Fatal("the resume did not fail")
		}
		if len(end.Pending) != 0 || len(end.Notes) != 1 {
			t.Fatalf("run end = %+v, want n2 owed", end)
		}
		if _, err := a.Prompt(context.Background(), openresponses.UserText("next")); err != nil {
			t.Fatal(err)
		}
		tr := a.State().Transcript
		if count(tr, "developer: n1") != 1 || count(tr, "developer: n2") != 1 {
			t.Fatalf("transcript = %q", texts(tr))
		}
		if got := strings.Join(texts(tr)[:4], "|"); got != "user: x|developer: n1|developer: n2|user: next" {
			t.Errorf("order = %q", got)
		}
	})
	t.Run("a failure between outputs", func(t *testing.T) {
		deferAll := func(context.Context, ToolCallInfo) (*ToolDecision, error) { return &ToolDecision{Action: Defer}, nil }
		a := New(Config{Model: &twoCalls{}, Tools: abTools(), BeforeToolCall: deferAll, MaxTurns: 1})
		end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
		if err != nil || len(end.Pending) != 2 {
			t.Fatalf("prompt: err=%v end=%+v", err, end)
		}
		first, second := end.Pending[0].Call.CallID, end.Pending[1].Call.CallID
		fail := true
		a.Subscribe(func(_ context.Context, ev Event) error {
			if e, ok := ev.(*ItemStart); ok && fail {
				if o, ok := e.Item.(*openresponses.FunctionCallOutput); ok && o.CallID == second {
					fail = false
					return errors.New("disk full")
				}
			}
			return nil
		})
		end, err = a.Resume(context.Background(),
			Output(openresponses.NewFunctionCallOutput(first, "no")).WithNote("use ci/ instead"),
			Output(openresponses.NewFunctionCallOutput(second, "no")))
		if err == nil {
			t.Fatal("the resume did not fail")
		}
		if len(end.Pending) != 1 || strings.Join(texts(noteItems(end.Pending[0].Notes)), "|") != "user: use ci/ instead" {
			t.Fatalf("pending = %+v, want the second call with the first's note", end.Pending)
		}
		if _, err := a.Resume(context.Background(), Output(openresponses.NewFunctionCallOutput(second, "no"))); err != nil {
			t.Fatal(err)
		}
		if n := count(a.State().Transcript, "user: use ci/ instead"); n != 1 {
			t.Errorf("the note is in the transcript %d times, want once", n)
		}
	})
}

// TestAHeldNoteKeepsHowItArrived pins R5: a decision's note held across
// runs reaches the next turn as the tool's, not as the Resume's.
func TestAHeldNoteKeepsHowItArrived(t *testing.T) {
	var modes []string
	cfg := Config{Model: &twoCalls{}, Tools: abTools(), BeforeToolCall: deferB, MaxTurns: 1,
		BeforeTurn: func(_ context.Context, info TurnStartInfo) (openresponses.Items, error) {
			for _, in := range info.Inputs {
				if m, ok := in.Item.(*openresponses.Message); ok && m.Role == openresponses.RoleDeveloper {
					modes = append(modes, string(in.Mode))
				}
			}
			return nil, nil
		}}
	a := New(cfg)
	end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
	if err != nil || len(end.Pending) != 1 {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	if n := end.Pending[0].Notes; len(n) != 1 || n[0].Mode != InputTool {
		t.Fatalf("held notes = %+v", n)
	}
	if _, err := a.Resume(context.Background(), Output(openresponses.NewFunctionCallOutput(end.Pending[0].Call.CallID, "no"))); err != nil {
		t.Fatal(err)
	}
	if strings.Join(modes, " ") != "tool" {
		t.Errorf("the note arrived as %q, want tool", modes)
	}
}

// TestAHeldCallsPolicyNoteIsSaidOnRelease pins R8: the note of the
// decision that held a call is the policy's, said by the loop as a
// developer message once an approval releases the call, in this
// process and after a restart.
func TestAHeldCallsPolicyNoteIsSaidOnRelease(t *testing.T) {
	hook := func(context.Context, ToolCallInfo) (*ToolDecision, error) {
		return &ToolDecision{Action: Defer, Reason: "ask", Note: "b runs in the sandbox"}, nil
	}
	cfg := Config{Model: callsNamed{names: []string{"b"}}, Tools: []agenttool.Tool{agenttool.New("b", "", upper)}, BeforeToolCall: hook, MaxTurns: 1}
	a := New(cfg)
	end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
	if err != nil || len(end.Pending) != 1 {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	for _, tc := range []struct {
		name  string
		agent *Agent
	}{
		{"in process", a},
		{"after a restart", New(cfg, WithTranscript(end.Items), WithPending(end.Pending))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.agent.Resume(context.Background(), Approve(end.Pending[0].Call.CallID).WithNote("go ahead")); err != nil {
				t.Fatal(err)
			}
			tr := tc.agent.State().Transcript
			if got := strings.Join(texts(tr)[:3], "|"); got != "user: x|user: go ahead|developer: b runs in the sandbox" {
				t.Errorf("messages = %q", texts(tr))
			}
		})
	}
}
