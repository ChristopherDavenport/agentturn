package session

import (
	"context"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// cutShort answers its first request with a function call when call is
// set, then a message, ended incomplete for reason with category as
// an extension member of the details when set; every later request is
// answered with a message.
type cutShort struct {
	call     bool
	reason   openresponses.IncompleteReason
	category string
	calls    int
}

func (m *cutShort) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.calls++
	if m.calls == 1 && m.category != "" {
		next := sink
		sink = openresponses.EventSinkFunc(func(ev openresponses.StreamEvent) error {
			if e, ok := ev.(*openresponses.ResponseIncompleteEvent); ok {
				e.Response.IncompleteDetails.Extra = map[string]any{"anthropic.refusal_category": m.category}
			}
			return next.Send(ev)
		})
	}
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
	if m.calls > 1 {
		if err := w.Text("again"); err != nil {
			return err
		}
		return em.Complete()
	}
	if m.reason == openresponses.IncompleteReasonContentFilter {
		err = w.Refusal("I can't help with that.")
	} else {
		err = w.Text("Once upon")
	}
	if err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Incomplete(m.reason)
}

// TestIncompleteLastResponseIsRecordedAborted pins #314's record: a run
// whose last response ended incomplete is written aborted, as the
// format's cascade reads its segment, with the loop's cause as ref: a
// content filter's stop, a final answer cut at the output limit, or
// another stop after a response so cut. A call
// a content-filtered response made is rejected by policy with the
// loop's fixed output, and the response keeps the provider's extension
// members. The record verifies, and the next prompt goes ahead.
func TestIncompleteLastResponseIsRecordedAborted(t *testing.T) {
	cases := []struct {
		name     string
		model    *cutShort
		maxTurns int
		reason   agentturn.Reason
		cause    agentturn.StopCause
		ref      string
		types    string
	}{
		{"content_filter, a message", &cutShort{reason: openresponses.IncompleteReasonContentFilter, category: "cyber"}, 0,
			agentturn.ReasonStopped, agentturn.StopContentFilter, "content_filter",
			"run config item:user item:assistant* response run"},
		{"content_filter, a call then a message", &cutShort{call: true, reason: openresponses.IncompleteReasonContentFilter, category: "cyber"}, 0,
			agentturn.ReasonStopped, agentturn.StopContentFilter, "content_filter",
			"run config item:user item:function_call* item:assistant* response decision item:function_call_output run"},
		{"max_output_tokens, an answer", &cutShort{reason: openresponses.IncompleteReasonMaxOutputTokens}, 0,
			agentturn.ReasonStopped, agentturn.StopMaxOutputTokens, "max_output_tokens",
			"run config item:user item:assistant* response run"},
		{"max_output_tokens, a call at the turn limit", &cutShort{call: true, reason: openresponses.IncompleteReasonMaxOutputTokens}, 1,
			agentturn.ReasonStopped, agentturn.StopMaxTurns, "max_turns",
			"run config item:user item:function_call* item:assistant* response dispatch item:function_call_output run"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(agentturn.Config{Model: tc.model, ModelName: "m", Tools: []agenttool.Tool{upper}, MaxTurns: tc.maxTurns})
			defer rec.Attach(a)()
			end, err := a.Prompt(context.Background(), openresponses.UserText("go"))
			if err != nil || end.Reason != tc.reason || end.Cause != tc.cause {
				t.Fatalf("end = %+v err=%v", end, err)
			}
			if got := entryTypes(s); got != tc.types {
				t.Errorf("entries = %q, want %q", got, tc.types)
			}
			for _, e := range s.Entries() {
				switch v := e.(type) {
				case *agentsession.ResponseEntry:
					if v.Status != openresponses.ResponseStatusIncomplete || v.Incomplete == nil || v.Incomplete.Reason != tc.model.reason {
						t.Errorf("response = %+v", v)
					}
					if tc.model.category != "" && (v.Incomplete == nil || v.Incomplete.Extra["anthropic.refusal_category"] != tc.model.category) {
						t.Errorf("response incomplete = %+v, want the provider's category kept", v.Incomplete)
					}
				case *agentsession.DecisionEntry:
					if tc.cause == agentturn.StopContentFilter && (v.Verdict != agentsession.VerdictReject || v.By != agentsession.ByPolicy || v.Reason != agentturn.RefusedCallOutput) {
						t.Errorf("decision = %+v", v)
					}
				}
			}
			runs := runsOf(t, s)
			if len(runs) != 1 || runs[0].End == nil || runs[0].End.Reason != agentsession.ReasonAborted || runs[0].End.Ref != tc.ref {
				t.Errorf("run end = %+v, want aborted %q", runs[0].End, tc.ref)
			}
			if err := runs[0].Verify(); err != nil {
				t.Errorf("run verify: %v", err)
			}
			if end, err := a.Prompt(context.Background(), openresponses.UserText("again")); err != nil || end.Reason != agentturn.ReasonDone {
				t.Fatalf("next prompt: end = %+v err=%v", end, err)
			}
			if runs := runsOf(t, s); len(runs) != 2 || runs[1].End == nil || runs[1].End.Reason != agentsession.ReasonDone {
				t.Errorf("next run end = %+v", runs[len(runs)-1].End)
			}
			verifyAll(t, s)
		})
	}
}

// TestRecordedEndReadsAnIncompleteEnd pins how a run end #314 writes
// aborted reads back for a call that runs again: a content filter
// stop as that stop, no answer, and an answer cut at max_output_tokens
// as that stop, its answer standing.
func TestRecordedEndReadsAnIncompleteEnd(t *testing.T) {
	items := openresponses.Items{openresponses.UserText("go"), openresponses.AssistantText("Once upon")}
	for _, tc := range []struct {
		ref    string
		reason agentturn.Reason
		cause  agentturn.StopCause
		answer bool
	}{
		{"content_filter", agentturn.ReasonStopped, agentturn.StopContentFilter, false},
		{"max_output_tokens", agentturn.ReasonStopped, agentturn.StopMaxOutputTokens, true},
		// A guard's stop still reads as the failed run it read as
		// before; Answer reads the items alone.
		{"guard: rule", agentturn.ReasonError, "", true},
	} {
		t.Run(tc.ref, func(t *testing.T) {
			run := &agentsession.Run{Start: agentsession.NewRunStart("run_1", "input", ""), End: agentsession.NewRunEnd("run_1", agentsession.ReasonAborted, tc.ref, nil)}
			end := recordedEnd(run, items)
			if end == nil || end.Reason != tc.reason || end.Cause != tc.cause {
				t.Fatalf("end = %+v", end)
			}
			if _, ok := end.Answer(); ok != tc.answer {
				t.Errorf("answer = %v, want %v", ok, tc.answer)
			}
		})
	}
}
