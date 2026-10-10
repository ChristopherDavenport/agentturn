package agentturn

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// asker is a tool that asks the user through the elicitor on its
// context and returns the action it was answered with, or the error.
func asker(name string) agenttool.Tool {
	return agenttool.New(name, "asks", func(ctx context.Context, _ echoArgs) (string, error) {
		elicit, ok := agenttool.ElicitorFrom(ctx)
		if !ok {
			return "", errors.New("no elicitor")
		}
		ans, err := elicit(ctx, agenttool.Elicitation{Message: "delete the branch?"})
		if err != nil {
			return "", err
		}
		return string(ans.Action), nil
	})
}

// questionAgent builds an agent over cfg with its QuestionElicitor
// installed, as a host turns it on.
func questionAgent(t *testing.T, cfg Config) *Agent {
	t.Helper()
	a := New(cfg)
	cfg.ToolElicitor = a.QuestionElicitor()
	if err := a.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	return a
}

// TestQuestionsAreEvents pins #224: a tool's own question and a nested
// call the hook deferred reach a subscriber as a Question, are answered
// with Reply from inside the event, and close with the answer, in order
// with the run's events.
func TestQuestionsAreEvents(t *testing.T) {
	read := agenttool.New("read", "reads", func(context.Context, echoArgs) (string, error) { return "contents", nil })
	eval := agenttool.New("eval", "runs code", func(ctx context.Context, a echoArgs) (string, error) {
		if _, err := Invoke(ctx, "read", json.RawMessage(`{"text":"go.mod"}`)); err != nil {
			return "refused", nil
		}
		return "read", nil
	})
	deferRead := func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
		if info.Call.Name == "read" {
			return &ToolDecision{Action: Defer, Reason: "rule r", By: "policy"}, nil
		}
		return nil, nil
	}
	cases := []struct {
		name   string
		cfg    Config
		answer agenttool.Answer
		// nested is set when the question is about a nested call.
		nested bool
		want   string
	}{
		{name: "a tool's question, accepted", cfg: Config{Tools: []agenttool.Tool{asker("ask")}},
			answer: agenttool.Answer{Action: agenttool.ActionAccept}, want: "accept"},
		{name: "a tool's question, declined", cfg: Config{Tools: []agenttool.Tool{asker("ask")}},
			answer: agenttool.Answer{Action: agenttool.ActionDecline}, want: "decline"},
		{name: "a nested call, allowed", cfg: Config{Tools: []agenttool.Tool{eval, read}, BeforeToolCall: deferRead},
			answer: agenttool.Answer{Action: agenttool.ActionAccept}, nested: true, want: "read"},
		{name: "a nested call, declined", cfg: Config{Tools: []agenttool.Tool{eval, read}, BeforeToolCall: deferRead},
			answer: agenttool.Answer{Action: agenttool.ActionDecline}, nested: true, want: "refused"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.cfg.Model, tc.cfg.MaxTurns = &echo.Adapter{}, 1
			a := questionAgent(t, tc.cfg)
			var events []Event
			var replyErr error
			a.Subscribe(func(_ context.Context, ev Event) error {
				events = append(events, ev)
				if q, ok := ev.(*Question); ok {
					replyErr = a.Reply(q.ID, tc.answer)
				}
				return nil
			})
			end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
			if err != nil {
				t.Fatal(err)
			}
			if replyErr != nil {
				t.Fatalf("Reply: %v", replyErr)
			}
			var q *Question
			var closed *QuestionClosed
			at := map[string]int{}
			for i, ev := range events {
				switch e := ev.(type) {
				case *Question:
					q = e
					at["question"] = i
				case *QuestionClosed:
					closed = e
					at["closed"] = i
				case *ToolDispatch:
					if e.Parent == "" {
						at["dispatch"] = i
					}
				case *ToolStart:
					if e.Parent != "" {
						at["nested start"] = i
						if e.Decision == nil || e.Decision.By != "human" {
							t.Errorf("nested tool_start decision = %+v, want the answer's, by human", e.Decision)
						}
					}
				case *ToolEnd:
					if e.Parent == "" {
						at["end"] = i
					}
				}
			}
			if q == nil || closed == nil {
				t.Fatalf("events = %v, want a question and its close", types(events))
			}
			if q.RunID != end.RunID || q.CallID == "" || q.Elicitation.Message == "" {
				t.Errorf("question = %+v", q)
			}
			if tc.nested != (q.Call != nil) {
				t.Fatalf("question.Call = %+v, nested %v", q.Call, tc.nested)
			}
			if tc.nested && (q.Call.Name != "read" || q.Call.Parent != q.CallID || q.Call.Decision.Reason != "rule r") {
				t.Errorf("question.Call = %+v", q.Call)
			}
			if closed.ID != q.ID || closed.Answer == nil || closed.Answer.Action != tc.answer.Action {
				t.Errorf("closed = %+v, want %s answered %s", closed, q.ID, tc.answer.Action)
			}
			if !(at["dispatch"] < at["question"] && at["question"] < at["closed"] && at["closed"] < at["end"]) {
				t.Errorf("order: %v in %v", at, types(events))
			}
			if tc.nested && at["closed"] > at["nested start"] {
				t.Errorf("the nested call started before its question closed: %v", at)
			}
			if out := lastOutput(end.Items); out != tc.want {
				t.Errorf("tool output = %q, want %q", out, tc.want)
			}
		})
	}
}

// lastOutput is the text of the last function call output of items.
func lastOutput(items openresponses.Items) string {
	for _, it := range slices.Backward(items) {
		if out, ok := it.(*openresponses.FunctionCallOutput); ok {
			return out.Output.Text
		}
	}
	return ""
}

// TestAQuestionWaitingIsSentToANewSubscriber pins that a subscriber
// attaching while a question waits is sent it, before its close, and
// can answer it; one that attaches after it closed is not.
func TestAQuestionWaitingIsSentToANewSubscriber(t *testing.T) {
	a := questionAgent(t, Config{Model: &echo.Adapter{}, MaxTurns: 1, Tools: []agenttool.Tool{asker("ask")}})
	asked := make(chan string, 1)
	a.Subscribe(func(_ context.Context, ev Event) error {
		if q, ok := ev.(*Question); ok {
			asked <- q.ID
		}
		return nil
	})
	done := make(chan *RunEnd, 1)
	go func() {
		end, _ := a.Prompt(context.Background(), openresponses.UserText("x"))
		done <- end
	}()
	id := <-asked

	var mu sync.Mutex
	var late []string
	a.Subscribe(func(_ context.Context, ev Event) error {
		mu.Lock()
		defer mu.Unlock()
		late = append(late, ev.EventType())
		if q, ok := ev.(*Question); ok {
			if q.ID != id {
				t.Errorf("sent %s, want the waiting %s", q.ID, id)
			}
			if err := a.Reply(q.ID, agenttool.Answer{Action: agenttool.ActionAccept}); err != nil {
				t.Errorf("Reply: %v", err)
			}
		}
		return nil
	})
	var end *RunEnd
	select {
	case end = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the late subscriber was never sent the question")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(late) < 2 || late[0] != EventQuestion || late[1] != EventQuestionClosed {
		t.Errorf("late subscriber got %v, want the question, then its close", late)
	}
	if out := lastOutput(end.Items); out != "accept" {
		t.Errorf("tool output = %q", out)
	}

	if len(a.waiting) != 0 {
		t.Errorf("questions still waiting after the run: %d", len(a.waiting))
	}
}

// TestAbortClosesAWaitingQuestion pins that a question the call gives
// up, here by an abort, closes with no answer, returns the context's
// error to the asking tool, and can no longer be answered.
func TestAbortClosesAWaitingQuestion(t *testing.T) {
	var toolErr error
	tool := agenttool.New("ask", "asks", func(ctx context.Context, _ echoArgs) (string, error) {
		elicit, _ := agenttool.ElicitorFrom(ctx)
		_, toolErr = elicit(ctx, agenttool.Elicitation{Message: "wait"})
		return "", toolErr
	})
	a := questionAgent(t, Config{Model: &echo.Adapter{}, MaxTurns: 1, Tools: []agenttool.Tool{tool}})
	asked := make(chan string, 1)
	var closed *QuestionClosed
	a.Subscribe(func(_ context.Context, ev Event) error {
		switch e := ev.(type) {
		case *Question:
			asked <- e.ID
		case *QuestionClosed:
			closed = e
		}
		return nil
	})
	done := make(chan *RunEnd, 1)
	go func() {
		end, _ := a.Prompt(context.Background(), openresponses.UserText("x"))
		done <- end
	}()
	id := <-asked
	a.Abort()
	end := <-done
	if end.Reason != ReasonAborted {
		t.Errorf("run ended %s", end.Reason)
	}
	if closed == nil || closed.ID != id || closed.Answer != nil {
		t.Errorf("closed = %+v, want %s with no answer", closed, id)
	}
	if !errors.Is(toolErr, context.Canceled) {
		t.Errorf("the tool's elicitor returned %v, want the context's error", toolErr)
	}
	if err := a.Reply(id, agenttool.Answer{Action: agenttool.ActionAccept}); !errors.Is(err, ErrNoQuestion) {
		t.Errorf("Reply after the abort = %v, want ErrNoQuestion", err)
	}
}

// TestReplyToNoQuestion pins Reply's refusal of an ID it does not know,
// and of a second answer to one question.
func TestReplyToNoQuestion(t *testing.T) {
	a := New(Config{Model: &echo.Adapter{}})
	if err := a.Reply("question_x", agenttool.Answer{Action: agenttool.ActionAccept}); !errors.Is(err, ErrNoQuestion) {
		t.Errorf("Reply = %v, want ErrNoQuestion", err)
	}
	a = questionAgent(t, Config{Model: &echo.Adapter{}, MaxTurns: 1, Tools: []agenttool.Tool{asker("ask")}})
	var second error
	a.Subscribe(func(_ context.Context, ev Event) error {
		if q, ok := ev.(*Question); ok {
			_ = a.Reply(q.ID, agenttool.Answer{Action: agenttool.ActionAccept})
			second = a.Reply(q.ID, agenttool.Answer{Action: agenttool.ActionDecline})
		}
		return nil
	})
	end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(second, ErrNoQuestion) {
		t.Errorf("second Reply = %v, want ErrNoQuestion", second)
	}
	if out := lastOutput(end.Items); out != "accept" {
		t.Errorf("tool output = %q, want the first answer", out)
	}
}

// TestASubscriberThatFailsOnAQuestionGivesItUp pins that a question a
// subscriber could not take is closed with no answer and the asking
// tool gets the subscriber's error.
func TestASubscriberThatFailsOnAQuestionGivesItUp(t *testing.T) {
	a := questionAgent(t, Config{Model: &echo.Adapter{}, MaxTurns: 1, Tools: []agenttool.Tool{asker("ask")}})
	var closed *QuestionClosed
	a.Subscribe(func(_ context.Context, ev Event) error {
		switch e := ev.(type) {
		case *Question:
			return errors.New("cannot show it")
		case *QuestionClosed:
			closed = e
		}
		return nil
	})
	end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
	if err != nil {
		t.Fatal(err)
	}
	if closed == nil || closed.Answer != nil {
		t.Errorf("closed = %+v, want no answer", closed)
	}
	if out := lastOutput(end.Items); !strings.Contains(out, "cannot show it") {
		t.Errorf("tool output = %q, want the subscriber's error", out)
	}
}

// TestPendingCarriesTheDeferringDecision pins #224's third part: a
// deferred call's PendingCall carries the decision that held it, on the
// run's end and in State, and keeps it through a resume that fails
// before its batch; a call that is not deferred carries none. #227: a
// hook's Held mark travels with it.
func TestPendingCarriesTheDeferringDecision(t *testing.T) {
	a := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", upper)},
		BeforeToolCall: func(context.Context, ToolCallInfo) (*ToolDecision, error) {
			return &ToolDecision{Action: Defer, Reason: "rule r", By: "policy", Held: true}, nil
		}})
	end, err := a.Prompt(context.Background(), openresponses.UserText("abc"))
	if err != nil {
		t.Fatal(err)
	}
	check := func(where string, pending []PendingCall) {
		t.Helper()
		if len(pending) != 1 || pending[0].Reason != PendingDeferred {
			t.Fatalf("%s: pending = %+v", where, pending)
		}
		if d := pending[0].Decision; d == nil || d.Action != Defer || d.Reason != "rule r" || d.By != "policy" || !d.Held {
			t.Errorf("%s: decision = %+v", where, d)
		}
	}
	check("run end", end.Pending)
	check("state", a.State().Pending)

	unsub := a.Subscribe(func(_ context.Context, ev Event) error {
		if _, ok := ev.(*RunStart); ok {
			return errors.New("subscriber down")
		}
		return nil
	})
	id := end.Pending[0].Call.CallID
	if end, _ := a.Resume(context.Background(), Approve(id)); end.Reason != ReasonError {
		t.Fatalf("failed resume ended %s", end.Reason)
	}
	unsub()
	check("after a failed resume", a.State().Pending)

	// A call an abort cut off carries no decision.
	running := make(chan struct{})
	b := New(Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{agenttool.New("upper", "", func(ctx context.Context, _ echoArgs) (string, error) {
		close(running)
		<-ctx.Done()
		return "", ctx.Err()
	})}})
	go func() {
		<-running
		b.Abort()
	}()
	end, _ = b.Prompt(context.Background(), openresponses.UserText("abc"))
	if len(end.Pending) != 1 || end.Pending[0].Reason != PendingAborted {
		t.Fatalf("aborted run's pending = %+v", end.Pending)
	}
	for _, p := range end.Pending {
		if p.Decision != nil {
			t.Errorf("a %s call carries a decision: %+v", p.Reason, p.Decision)
		}
	}
}
