package agentturn

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// counting is the echo model, counting its calls.
type counting struct{ calls int }

func (m *counting) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.calls++
	return (&echo.Adapter{}).CreateStream(ctx, req, sink)
}

// TestGuardBeforeTheCallStops pins #116: an error wrapping ErrGuard from
// the hooks that run before the model call stops the run as a policy
// stop, with no model call, a model_blocked carrying the request from
// BeforeModelCall; any other error is still a failure.
func TestGuardBeforeTheCallStops(t *testing.T) {
	spent := fmt.Errorf("%w: cost limit reached", ErrGuard)
	boom := errors.New("boom")
	for _, tc := range []struct {
		name        string
		turn        func(context.Context, TurnStartInfo) (openresponses.Items, error)
		call        func(context.Context, *openresponses.Request) error
		reason      Reason
		wantErr     error
		wantBlocked bool
	}{
		{name: "before turn, guard", turn: func(context.Context, TurnStartInfo) (openresponses.Items, error) { return nil, spent }, reason: ReasonStopped, wantErr: spent},
		{name: "before model call, guard", call: func(context.Context, *openresponses.Request) error { return spent }, reason: ReasonStopped, wantErr: spent, wantBlocked: true},
		{name: "before turn, failure", turn: func(context.Context, TurnStartInfo) (openresponses.Items, error) { return nil, boom }, reason: ReasonError, wantErr: boom},
		{name: "before model call, failure", call: func(context.Context, *openresponses.Request) error { return boom }, reason: ReasonError, wantErr: boom, wantBlocked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			model := &counting{}
			a := New(Config{Model: model, BeforeTurn: tc.turn, BeforeModelCall: tc.call})
			var types []string
			a.Subscribe(func(_ context.Context, ev Event) error {
				types = append(types, ev.EventType())
				return nil
			})
			end, _ := a.Prompt(context.Background(), openresponses.UserText("go"))
			if end.Reason != tc.reason || !errors.Is(end.Err, tc.wantErr) {
				t.Fatalf("end = %s %v", end.Reason, end.Err)
			}
			if tc.reason == ReasonStopped && end.Cause != StopGuard {
				t.Errorf("cause = %q", end.Cause)
			}
			if model.calls != 0 {
				t.Errorf("the model was called %d times", model.calls)
			}
			blocked := false
			for _, typ := range types {
				blocked = blocked || typ == EventModelBlocked
				if typ == EventTurnStart {
					t.Errorf("a turn_start for a turn that made no call: %v", types)
				}
			}
			if blocked != tc.wantBlocked {
				t.Errorf("model_blocked raised = %v in %v", blocked, types)
			}
		})
	}
}

// TestSteerWhileIdleJoinsTheFirstCall pins #123: an item steered while
// the agent is idle is in the first turn's request and inputs, after
// the prompt, rather than one model call late; a refusal on Resume
// leaves it for the run that follows.
func TestSteerWhileIdleJoinsTheFirstCall(t *testing.T) {
	a := New(Config{Model: &echo.Adapter{}})
	var first *TurnStart
	a.Subscribe(func(_ context.Context, ev Event) error {
		if e, ok := ev.(*TurnStart); ok && first == nil {
			first = e
		}
		return nil
	})
	a.Steer(openresponses.UserText("also this"))
	if _, err := a.Prompt(context.Background(), openresponses.UserText("do that")); err != nil {
		t.Fatal(err)
	}
	if first == nil || len(first.Inputs) != 2 || first.Inputs[1].(*openresponses.Message).Text() != "also this" {
		t.Fatalf("first turn's inputs = %+v", first)
	}
	in := first.Request.Input
	if len(in) != 2 || in[0].(*openresponses.Message).Text() != "do that" {
		t.Errorf("first request input = %+v", in)
	}
}

// TestQueueCarriesItsTrigger checks that Queue reports the trigger on
// the context with each item, and that Steer and FollowUp report none.
func TestQueueCarriesItsTrigger(t *testing.T) {
	a := New(Config{Model: &echo.Adapter{}})
	var got []*Queued
	a.Subscribe(func(_ context.Context, ev Event) error {
		if q, ok := ev.(*Queued); ok {
			got = append(got, q)
		}
		return nil
	})
	cron := Trigger{Kind: "cron", Ref: "03:00", Source: "scheduler"}
	a.Queue(ContextWithTrigger(context.Background(), cron), QueueFollowUp, openresponses.UserText("nightly"))
	a.Steer(openresponses.UserText("plain"))
	a.Queue(context.Background(), "bogus", openresponses.UserText("unknown mode"))
	if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Trigger != cron || got[0].Mode != QueueFollowUp || !got[1].Trigger.IsZero() || got[1].Mode != QueueSteer || got[2].Mode != QueueFollowUp {
		t.Errorf("queued = %+v", got)
	}
}

// TestRunEndAnswer pins the answer #138's consumers share: the assistant
// message that ends the run's items, when it has text.
func TestRunEndAnswer(t *testing.T) {
	call := &openresponses.FunctionCall{CallID: "c1", Name: "lookup", Arguments: `{}`}
	for _, tc := range []struct {
		name  string
		items Transcript
		want  string
		ok    bool
	}{
		{"nothing", nil, "", false},
		{"an answer", Transcript{openresponses.UserText("q"), openresponses.AssistantText("a")}, "a", true},
		{"an empty message", Transcript{openresponses.UserText("q"), openresponses.AssistantText("")}, "", false},
		{"a preamble and a call", Transcript{openresponses.UserText("q"), openresponses.AssistantText("Let me check."), call, openresponses.NewFunctionCallOutput("c1", "found")}, "", false},
		{"a user message last", Transcript{openresponses.AssistantText("a"), openresponses.UserText("and?")}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := (&RunEnd{Items: tc.items}).Answer()
			if got != tc.want || ok != tc.ok {
				t.Errorf("Answer() = %q, %v, want %q, %v", got, ok, tc.want, tc.ok)
			}
		})
	}
}
