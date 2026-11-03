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

// callThenGuard completes a function call, opens a message and then
// fails the call with an error marked as a guard's, as a moderation
// wrapper around the model refuses an answer it has begun.
type callThenGuard struct{}

func (callThenGuard) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	fc, err := em.FunctionCall("", "notify")
	if err != nil {
		return err
	}
	if err := fc.Arguments(`{"text":"t"}`); err != nil {
		return err
	}
	if err := fc.Close(); err != nil {
		return err
	}
	w, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if err := w.Text("the secret is"); err != nil {
		return err
	}
	return fmt.Errorf("moderation: %w", agentturn.ErrGuard)
}

// TestAModelGuardStopIsRecordedAsAWithheldResponse pins the record of
// #275's model guard stop: the response is written withheld, incomplete
// with content_filter, before the outputs that close the calls it made,
// and those are the policy's rejects, as for an output guard's stop.
func TestAModelGuardStopIsRecordedAsAWithheldResponse(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	ran := 0
	notify := agenttool.New("notify", "", func(context.Context, echoArgs) (string, error) {
		ran++
		return "sent", nil
	})
	a := agentturn.New(agentturn.Config{Model: callThenGuard{}, ModelName: "m", Tools: []agenttool.Tool{notify}})
	defer rec.Attach(a)()
	end, err := a.Prompt(ctx, openresponses.UserText("go"))
	if err != nil || end.Reason != agentturn.ReasonStopped || end.Cause != agentturn.StopGuard {
		t.Fatalf("end = %+v, err = %v", end, err)
	}
	if ran != 0 {
		t.Error("the guarded response's call ran")
	}
	if got, want := entryTypes(s), "run config item:user item:function_call* response decision item:function_call_output run"; got != want {
		t.Errorf("entries = %q, want %q", got, want)
	}
	for _, e := range s.Entries() {
		switch v := e.(type) {
		case *agentsession.ResponseEntry:
			if v.Incomplete == nil || v.Incomplete.Reason != openresponses.IncompleteReasonContentFilter || v.Error != nil {
				t.Errorf("response = %+v, want incomplete with content_filter and no error", v)
			}
		case *agentsession.DecisionEntry:
			if v.Verdict != agentsession.VerdictReject || v.By != agentsession.ByPolicy {
				t.Errorf("decision = %+v, want the policy's reject", v)
			}
		}
	}
	if err := s.VerifyRecords(s.Leaf()); err != nil {
		t.Errorf("verify: %v", err)
	}
}

// guardBeforeAnswer opens its response, completes a reasoning item and
// then fails with an error marked as a guard's, before any message or
// call has opened: nothing of the answer was committed.
type guardBeforeAnswer struct{}

func (guardBeforeAnswer) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	r, err := em.Reasoning()
	if err != nil {
		return err
	}
	r.EncryptedContent("thinking")
	if err := r.Close(); err != nil {
		return err
	}
	return fmt.Errorf("moderation: %w", agentturn.ErrGuard)
}

// TestAModelGuardBeforeTheAnswerIsRecordedAsARefusedCall pins the
// other half of a model guard's record: a guard error before the
// attempt committed to an answer leaves nothing of the response in the
// transcript, so no response entry is written for it, which would be an
// empty withheld one; the refused call is the model_blocked record, as
// for a BeforeModelCall guard, with the request's hash, and the record
// verifies.
func TestAModelGuardBeforeTheAnswerIsRecordedAsARefusedCall(t *testing.T) {
	ctx := context.Background()
	rec, s, err := Start(ctx, agentsession.NewMemoryStore(), agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: guardBeforeAnswer{}, ModelName: "m"})
	defer rec.Attach(a)()
	end, err := a.Prompt(ctx, openresponses.UserText("go"))
	if err != nil || end.Reason != agentturn.ReasonStopped || end.Cause != agentturn.StopGuard {
		t.Fatalf("end = %+v, err = %v", end, err)
	}
	if got, want := entryTypes(s), "run config item:user custom run"; got != want {
		t.Errorf("entries = %q, want %q", got, want)
	}
	blocked := 0
	for _, e := range s.Entries() {
		if c, ok := e.(*agentsession.CustomEntry); ok && c.NS == ModelBlockedNS {
			var mb ModelBlocked
			if err := json.Unmarshal(c.Data, &mb); err != nil {
				t.Fatal(err)
			}
			if mb.RequestHash == "" || !strings.Contains(mb.Error, "moderation") || mb.Model != "m" {
				t.Errorf("model_blocked = %+v", mb)
			}
			blocked++
		}
	}
	if blocked != 1 {
		t.Errorf("model_blocked records = %d, want 1", blocked)
	}
	if err := s.VerifyRecords(s.Leaf()); err != nil {
		t.Errorf("verify: %v", err)
	}
}

// billedThenGuard opens a response that already reports usage, as a
// provider that bills the prompt up front does, and then fails with an
// error marked as a guard's before any answer opened.
type billedThenGuard struct{}

func (billedThenGuard) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	resp := openresponses.NewResponse(req)
	resp.Usage = &openresponses.Usage{InputTokens: 7, TotalTokens: 7}
	em := openresponses.NewEmitter(sink, resp)
	r, err := em.Reasoning()
	if err != nil {
		return err
	}
	r.EncryptedContent("thinking")
	if err := r.Close(); err != nil {
		return err
	}
	return fmt.Errorf("moderation: %w", agentturn.ErrGuard)
}

// TestAModelGuardsRefusalCarriesTheUsageReported pins that the
// model_blocked record of a call a model's guard refused before its
// answer carries the usage the provider reported, so a call that may
// have been billed is counted.
func TestAModelGuardsRefusalCarriesTheUsageReported(t *testing.T) {
	ctx := context.Background()
	rec, s, err := Start(ctx, agentsession.NewMemoryStore(), agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: billedThenGuard{}, ModelName: "m"})
	defer rec.Attach(a)()
	if end, err := a.Prompt(ctx, openresponses.UserText("go")); err != nil || end.Cause != agentturn.StopGuard {
		t.Fatalf("end = %+v, err = %v", end, err)
	}
	found := false
	for _, e := range s.Entries() {
		c, ok := e.(*agentsession.CustomEntry)
		if !ok || c.NS != ModelBlockedNS {
			continue
		}
		found = true
		var data struct {
			Usage *openresponses.Usage `json:"usage"`
		}
		if err := json.Unmarshal(c.Data, &data); err != nil {
			t.Fatal(err)
		}
		if data.Usage == nil || data.Usage.InputTokens != 7 {
			t.Errorf("model_blocked usage = %+v, want the 7 input tokens reported: %s", data.Usage, c.Data)
		}
	}
	if !found {
		t.Fatalf("no model_blocked record: %s", entryTypes(s))
	}
	if err := s.VerifyRecords(s.Leaf()); err != nil {
		t.Errorf("verify: %v", err)
	}
}
