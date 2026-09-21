package agentturn

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestHiddenItemsReachTheModelAndAreMarked checks every seam that
// appends a caller's item: the model reads the item, the transcript
// holds the item itself rather than a wrapper, and only the item events
// say it is hidden.
func TestHiddenItemsReachTheModelAndAreMarked(t *testing.T) {
	notice := openresponses.DeveloperText("<notice>think first</notice>")
	reminder := openresponses.UserText("<reminder>still in the sandbox</reminder>")
	cfg := Config{Model: &echo.Adapter{}, MaxTurns: 1,
		BeforeTurn: func(context.Context, TurnStartInfo) (openresponses.Items, error) {
			return openresponses.Items{Hidden(reminder)}, nil
		}}
	var sent openresponses.Items
	var hidden, shown []string
	a := New(cfg)
	a.Subscribe(func(_ context.Context, ev Event) error {
		switch e := ev.(type) {
		case *TurnStart:
			sent = e.Request.Input
		case *ItemEnd:
			text := ""
			if m, ok := e.Item.(*openresponses.Message); ok {
				text = m.Text()
			}
			if e.Hidden {
				hidden = append(hidden, text)
			} else {
				shown = append(shown, text)
			}
		}
		return nil
	})
	if _, err := a.Prompt(context.Background(), openresponses.UserText("go"), Hidden(notice)); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 3 {
		t.Fatalf("request input = %d items", len(sent))
	}
	for i, item := range sent {
		if _, ok := item.(*openresponses.Message); !ok {
			t.Errorf("input[%d] = %T, want the item itself", i, item)
		}
	}
	for i, item := range a.State().Transcript {
		if _, ok := item.(hiddenItem); ok {
			t.Errorf("transcript[%d] is still wrapped", i)
		}
	}
	if len(hidden) != 2 || hidden[0] != notice.Text() || hidden[1] != reminder.Text() {
		t.Errorf("hidden item_end = %q", hidden)
	}
	if len(shown) != 2 || shown[0] != "go" {
		t.Errorf("shown item_end = %q", shown)
	}
}

func TestHiddenWrapper(t *testing.T) {
	msg := openresponses.UserText("hello")
	h := Hidden(msg)
	if Hidden(h) != h {
		t.Error("Hidden is not idempotent")
	}
	if Hidden(nil) != nil {
		t.Error("Hidden(nil) is not nil")
	}
	if h.ItemType() != msg.ItemType() {
		t.Errorf("item type = %q", h.ItemType())
	}
	// A wrapper that reaches a store or a server anyway is the item.
	want, _ := json.Marshal(msg)
	got, err := json.Marshal(h)
	if err != nil || string(got) != string(want) {
		t.Errorf("marshal = %s (%v), want %s", got, err, want)
	}
	if base, ok := Unhide(h); !ok || base != openresponses.Item(msg) {
		t.Errorf("Unhide = %v, %v", base, ok)
	}
	if base, ok := Unhide(msg); ok || base != openresponses.Item(msg) {
		t.Errorf("Unhide of a plain item = %v, %v", base, ok)
	}
}

// TestHiddenOutputAnswersAPendingCall checks that the checks a prompt
// and a resume run see through the mark.
func TestHiddenOutputAnswersAPendingCall(t *testing.T) {
	deferAll := func(context.Context, ToolCallInfo) (*ToolDecision, error) { return &ToolDecision{Action: Defer}, nil }
	cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)}, BeforeToolCall: deferAll, MaxTurns: 2}
	a := New(cfg)
	end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
	if err != nil || end.Reason != ReasonInputRequired {
		t.Fatalf("err=%v end=%+v", err, end)
	}
	callID := end.Pending[0].Call.CallID
	out := openresponses.NewFunctionCallOutput(callID, "did it myself")
	end, err = a.Prompt(context.Background(), Hidden(out), openresponses.UserText("carry on"))
	if err != nil || end.Reason == ReasonError {
		t.Fatalf("prompt with a hidden output: err=%v end=%+v", err, end)
	}
	if got := itemTypes(a.State().Transcript[:4]); got != "user function_call function_call_output user" {
		t.Errorf("transcript = %q", got)
	}
}

// TestAnswerBySaysWhoDecided checks that an approval carries who
// answered onto the decision the loop synthesises for it.
func TestAnswerBySaysWhoDecided(t *testing.T) {
	deferAll := func(context.Context, ToolCallInfo) (*ToolDecision, error) {
		return &ToolDecision{Action: Defer, Reason: "approval required by upper"}, nil
	}
	cfg := Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)}, BeforeToolCall: deferAll, MaxTurns: 2}
	a := New(cfg)
	var decisions []*ToolDecision
	a.Subscribe(func(_ context.Context, ev Event) error {
		if e, ok := ev.(*ToolStart); ok {
			decisions = append(decisions, e.Decision)
		}
		return nil
	})
	end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
	if err != nil || end.Reason != ReasonInputRequired {
		t.Fatalf("err=%v end=%+v", err, end)
	}
	callID := end.Pending[0].Call.CallID
	var deciders map[string]string
	a.Subscribe(func(ctx context.Context, ev Event) error {
		if _, ok := ev.(*RunStart); ok {
			deciders = map[string]string{callID: DeciderFromContext(ctx, callID)}
		}
		return nil
	})
	if _, err := a.Resume(context.Background(), Approve(callID).WithBy("human").WithNote("go ahead")); err != nil {
		t.Fatal(err)
	}
	if len(decisions) < 2 {
		t.Fatalf("tool_start decisions = %d", len(decisions))
	}
	if d := decisions[0]; d == nil || d.Action != Defer || d.Reason != "approval required by upper" {
		t.Errorf("hold decision = %+v", d)
	}
	if d := decisions[1]; d == nil || d.By != "human" || d.Note != "go ahead" {
		t.Errorf("approval decision = %+v", d)
	}
	// The same answer names its decider on the run's context, where a
	// recorder writing an output the caller supplied finds it.
	if deciders[callID] != "human" {
		t.Errorf("decider on the context = %q", deciders[callID])
	}
}
