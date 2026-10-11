package agentturn

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// orderLog records, in delivery order, each notes_held as the texts it
// holds and each appended output by its call's name.
type orderLog struct {
	names  map[string]string
	events []string
}

func (l *orderLog) event(_ context.Context, ev Event) error {
	switch e := ev.(type) {
	case *ItemEnd:
		switch it := e.Item.(type) {
		case *openresponses.FunctionCall:
			if l.names == nil {
				l.names = map[string]string{}
			}
			l.names[it.CallID] = it.Name
		case *openresponses.FunctionCallOutput:
			l.events = append(l.events, "output "+l.names[it.CallID])
		}
	case *NotesHeld:
		var held []string
		for _, n := range e.Notes {
			held = append(held, n.Item.(*openresponses.Message).Text())
		}
		l.events = append(l.events, "held ["+strings.Join(held, ",")+"]")
	}
	return nil
}

// heldBefore reports whether a notes_held holding note comes before the
// first output of the call named name, that output being there.
func (l *orderLog) heldBefore(note, name string) bool {
	for _, e := range l.events {
		if strings.HasPrefix(e, "held [") && strings.Contains(e, note) {
			return true
		}
		if e == "output "+name {
			return false
		}
	}
	return false
}

// TestNotesAreHeldBeforeTheirOutputs pins the durable-notes rule: every
// note is reported held before the first output it waits on is
// appended, so a recorder never holds an output without its note.
func TestNotesAreHeldBeforeTheirOutputs(t *testing.T) {
	deferAll := func(context.Context, ToolCallInfo) (*ToolDecision, error) {
		return &ToolDecision{Action: Defer, Reason: "ask"}, nil
	}
	t.Run("a Resume's output answer, nothing approved", func(t *testing.T) {
		log := &orderLog{}
		a := New(Config{Model: &twoCalls{}, Tools: abTools(), BeforeToolCall: deferAll, MaxTurns: 1})
		a.Subscribe(log.event)
		end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
		if err != nil || len(end.Pending) != 2 {
			t.Fatalf("prompt: err=%v end=%+v", err, end)
		}
		if _, err := a.Resume(context.Background(),
			Output(openresponses.NewFunctionCallOutput(end.Pending[0].Call.CallID, "no")).WithNote("use ci/ instead"),
			Output(openresponses.NewFunctionCallOutput(end.Pending[1].Call.CallID, "no"))); err != nil {
			t.Fatal(err)
		}
		if !log.heldBefore("use ci/ instead", "a") {
			t.Errorf("events = %q, want the note held before a's output", log.events)
		}
	})
	t.Run("a hook's note on a batch that completes", func(t *testing.T) {
		log := &orderLog{}
		hook := func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
			if info.Call.Name == "a" {
				return &ToolDecision{Note: "a ran in the sandbox"}, nil
			}
			return nil, nil
		}
		a := New(Config{Model: &twoCalls{}, Tools: abTools(), BeforeToolCall: hook, MaxTurns: 1})
		a.Subscribe(log.event)
		if _, err := a.Prompt(context.Background(), openresponses.UserText("x")); err != nil {
			t.Fatal(err)
		}
		if !log.heldBefore("a ran in the sandbox", "a") {
			t.Errorf("events = %q, want the note held before a's output", log.events)
		}
	})
	t.Run("an approval's note, its batch completing", func(t *testing.T) {
		log := &orderLog{}
		decided := 0
		hook := func(context.Context, ToolCallInfo) (*ToolDecision, error) {
			decided++
			if decided <= 2 {
				return &ToolDecision{Action: Defer, Reason: "ask"}, nil
			}
			return nil, nil
		}
		a := New(Config{Model: &twoCalls{}, Tools: abTools(), BeforeToolCall: hook, MaxTurns: 1})
		a.Subscribe(log.event)
		end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
		if err != nil || len(end.Pending) != 2 {
			t.Fatalf("prompt: err=%v end=%+v", err, end)
		}
		if _, err := a.Resume(context.Background(),
			Approve(end.Pending[0].Call.CallID).WithNote("only the unit tests"),
			Output(openresponses.NewFunctionCallOutput(end.Pending[1].Call.CallID, "no"))); err != nil {
			t.Fatal(err)
		}
		if !log.heldBefore("only the unit tests", "a") {
			t.Errorf("events = %q, want the note held before a's output", log.events)
		}
	})
	t.Run("a released call's policy note", func(t *testing.T) {
		log := &orderLog{}
		hold := func(context.Context, ToolCallInfo) (*ToolDecision, error) {
			return &ToolDecision{Action: Defer, Reason: "ask", Note: "b runs in the sandbox"}, nil
		}
		a := New(Config{Model: callsNamed{names: []string{"b"}}, Tools: abTools(), BeforeToolCall: hold, MaxTurns: 1})
		a.Subscribe(log.event)
		end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
		if err != nil || len(end.Pending) != 1 {
			t.Fatalf("prompt: err=%v end=%+v", err, end)
		}
		if _, err := a.Resume(context.Background(), Approve(end.Pending[0].Call.CallID)); err != nil {
			t.Fatal(err)
		}
		if !log.heldBefore("b runs in the sandbox", "b") {
			t.Errorf("events = %q, want the note held before b's output", log.events)
		}
	})
	t.Run("a finished call's note in a failed batch", func(t *testing.T) {
		log := &orderLog{}
		hook := func(_ context.Context, info ToolCallInfo) (*ToolDecision, error) {
			if info.Call.Name == "b" {
				return &ToolDecision{Note: "b ran in the sandbox"}, nil
			}
			return nil, nil
		}
		tools := []agenttool.Tool{agenttool.New("b", "", upper), agenttool.New("a", "", upper)}
		a := New(Config{Model: &twoCalls{}, Tools: tools, BeforeToolCall: hook, ToolExecution: ExecSequential, MaxTurns: 1})
		a.Subscribe(log.event)
		a.Subscribe(func(_ context.Context, ev Event) error {
			if e, ok := ev.(*ToolDispatch); ok && e.Name == "a" {
				return errors.New("the audit log is down")
			}
			return nil
		})
		end, err := a.Prompt(context.Background(), openresponses.UserText("x"))
		if err == nil || len(end.Pending) != 1 {
			t.Fatalf("prompt: err=%v end=%+v", err, end)
		}
		if !log.heldBefore("b ran in the sandbox", "b") {
			t.Errorf("events = %q, want the note held before b's output", log.events)
		}
	})
}
