package session

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// TestAFailedCallsOutputIsMarkedFailed pins RFC 0001's item_end row
// (#278): the output of a call whose tool_end carried the tool's error
// is marked failed, and a refusal, recorded by its reject, is not.
func TestAFailedCallsOutputIsMarkedFailed(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	tools := []agenttool.Tool{
		agenttool.New("read", "", func(context.Context, echoArgs) (string, error) {
			return "", errors.New("no such file")
		}),
		agenttool.New("list", "", func(context.Context, echoArgs) (string, error) { return "a b", nil }),
		agenttool.New("rm", "", func(context.Context, echoArgs) (string, error) { return "gone", nil }),
	}
	block := func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
		if info.Call.Name == "rm" {
			return &agentturn.ToolDecision{Action: agentturn.Block, Reason: "not here"}, nil
		}
		return nil, nil
	}
	a := agentturn.New(agentturn.Config{Model: allCalls{}, ModelName: "m", MaxTurns: 1, Tools: tools, BeforeToolCall: block})
	defer rec.Attach(a)()
	if _, err := a.Prompt(ctx, openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	calls := callsOf(t, s)
	for name, want := range map[string]bool{"read": true, "list": false, "rm": false} {
		c := calls[name]
		if c == nil || c.Output == nil {
			t.Fatalf("%s: no output in %q", name, entryTypes(s))
		}
		if c.Failed() != want {
			t.Errorf("%s: failed = %v, want %v", name, c.Failed(), want)
		}
	}
	if err := s.VerifyRecords(s.Leaf()); err != nil {
		t.Errorf("verify: %v", err)
	}
}

// TestAnOutcomeUnknownIsMarkedFailed pins the other half of the
// item_end row (#278): an output answering a call the harness gave up,
// one cut off whose outcome is unknown, is marked failed, whether the
// host answers it live or a restart answers it from the record.
func TestAnOutcomeUnknownIsMarkedFailed(t *testing.T) {
	for _, restarted := range []bool{false, true} {
		name := "live"
		if restarted {
			name = "after a restart"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(ctx, store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			started := make(chan struct{})
			bash := agenttool.New("bash", "", func(ctx context.Context, _ echoArgs) (string, error) {
				close(started)
				<-ctx.Done()
				return "", ctx.Err()
			})
			tools := []agenttool.Tool{bash}
			cfg := agentturn.Config{Model: allCalls{}, ModelName: "m", MaxTurns: 1, Tools: tools}
			a := agentturn.New(cfg)
			unsub := rec.Attach(a)
			go func() {
				<-started
				a.Abort()
			}()
			end, err := a.Prompt(ctx, openresponses.UserText("go"))
			if err != nil || end.Reason != agentturn.ReasonAborted || len(end.Pending) != 1 {
				t.Fatalf("prompt: err=%v end=%+v", err, end)
			}
			if !restarted {
				defer unsub()
				if _, err := a.Resume(ctx, agentturn.OutcomeUnknown(end.Pending[0].Call.CallID)); err != nil {
					t.Fatal(err)
				}
			} else {
				unsub()
				rec, s, err = Resume(ctx, store, s.ID())
				if err != nil {
					t.Fatal(err)
				}
				answers, err := ReplayAnswers(ctx, s, tools)
				if err != nil {
					t.Fatal(err)
				}
				opts, err := AgentOptions(s)
				if err != nil {
					t.Fatal(err)
				}
				b := agentturn.New(cfg, opts...)
				defer rec.Attach(b)()
				if _, err := b.Resume(ctx, answers...); err != nil {
					t.Fatal(err)
				}
			}
			c := callsOf(t, s)["bash"]
			if c == nil || c.Output == nil {
				t.Fatalf("no output in %q", entryTypes(s))
			}
			if !c.Failed() {
				t.Errorf("the cut call's outcome-unknown output is not marked failed")
			}
		})
	}
}

// TestAWithdrawalAnEarlierVersionWroteStillReads pins the reading of
// the agentturn:withdrawn entry agentturn v0.0.34 and earlier wrote in
// place of the format's withdrawal (#278): the call whose only dispatch
// it withdrew never started, is approved by the replay rule, and the
// file still verifies.
func TestAWithdrawalAnEarlierVersionWroteStillReads(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	s, err := store.Create(ctx, agentsession.Header{Records: agentsession.AllRecords})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, s.ID(), &agentsession.ItemEntry{Item: openresponses.UserText("notify")}); err != nil {
		t.Fatal(err)
	}
	call := &openresponses.FunctionCall{CallID: "call_1", Name: "notify", Arguments: `{"text":"t"}`}
	target, err := store.Append(ctx, s.ID(), &agentsession.ItemEntry{Item: call, ResponseID: "resp_1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, s.ID(), &agentsession.ResponseEntry{ResponseID: "resp_1", Status: openresponses.ResponseStatusCompleted}); err != nil {
		t.Fatal(err)
	}
	d, err := store.Append(ctx, s.ID(), agentsession.NewDispatch(call.CallID, target))
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(Withdrawn{Dispatch: d, Reason: "the audit log is down"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, s.ID(), &agentsession.CustomEntry{NS: WithdrawnNS, Data: data, CallID: call.CallID}); err != nil {
		t.Fatal(err)
	}
	if s, err = store.Open(ctx, s.ID()); err != nil {
		t.Fatal(err)
	}
	pending, err := Pending(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Reason != agentturn.PendingUndispatched || pending[0].MayHaveRun() {
		t.Fatalf("pending = %+v, want the call never started", pending)
	}
	notify := agenttool.New("notify", "", func(context.Context, echoArgs) (string, error) { return "sent", nil })
	answers, err := ReplayAnswers(ctx, s, []agenttool.Tool{notify})
	if err != nil {
		t.Fatal(err)
	}
	if len(answers) != 1 || answers[0].Output != nil {
		t.Errorf("answers = %+v, want an approval", answers)
	}
	if err := s.VerifyRecords(s.Leaf()); err != nil {
		t.Errorf("verify: %v", err)
	}
}
