package agent

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// TestAChildsDeferralAsksTheParentsElicitor pins #84: under
// WithAskParent a call the child's own hook defers is put to the
// elicitor of the call that started the child, as a nested call's
// deferral is, and the child goes on with the answer. The call fails
// with input-required only when there is nobody to ask, and then names
// what it waits on through agentturn.InputRequired: each call's ID,
// name and arguments, and the child's run as the callee.
func TestAChildsDeferralAsksTheParentsElicitor(t *testing.T) {
	lookup := agenttool.New("lookup", "", func(context.Context, struct {
		Text string `json:"text"`
	}) (string, error) {
		return "FOUND", nil
	})
	for _, tc := range []struct {
		name string
		// ask installs WithAskParent; answer is the parent's
		// elicitor, nil for none.
		ask    bool
		answer *agenttool.Answer
		held   bool
		// decline is the deferral's DeclineText.
		decline string
		// want is the parent's call output; input says it failed
		// with input-required.
		want  string
		input bool
		asked bool
	}{
		{name: "accepted", ask: true, answer: &agenttool.Answer{Action: agenttool.ActionAccept}, want: "Tool result: FOUND", asked: true},
		{name: "declined", ask: true, answer: &agenttool.Answer{Action: agenttool.ActionDecline}, want: "Tool result: Error: declined when asked: a person approves lookups", asked: true},
		{name: "declined with the host's text", ask: true, decline: "lookups stay with the host", answer: &agenttool.Answer{Action: agenttool.ActionDecline, Note: "use the cache"}, want: "Tool result: Error: lookups stay with the host; the user said: use the cache", asked: true},
		{name: "nobody to ask", ask: true, input: true},
		{name: "a cancel", ask: true, answer: &agenttool.Answer{Action: agenttool.ActionCancel}, input: true, asked: true},
		{name: "a held call beside no asked one stays held", ask: true, held: true, answer: &agenttool.Answer{Action: agenttool.ActionAccept}, input: true},
		{name: "without the option", answer: &agenttool.Answer{Action: agenttool.ActionAccept}, input: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			childCfg := agentturn.Config{Name: "child", Description: "looks up", Model: &echo.Adapter{}, Tools: []agenttool.Tool{lookup},
				BeforeToolCall: func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
					return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "a person approves lookups", By: "policy", Held: tc.held, DeclineText: tc.decline}, nil
				}}
			var opts []Option
			if tc.ask {
				opts = append(opts, WithAskParent())
			}
			child := New(childCfg, opts...)
			var asked []agentturn.AskedCall
			var under []string
			parent := agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{child}, MaxTurns: 1}
			if tc.answer != nil {
				parent.ToolElicitor = func(ctx context.Context, q agenttool.Elicitation) (agenttool.Answer, error) {
					c, _ := agentturn.AskedCallFrom(ctx)
					asked = append(asked, c)
					call, _ := agenttool.CallFrom(ctx)
					under = append(under, call.ID)
					return *tc.answer, nil
				}
			}
			var end *agentturn.ToolEnd
			var start *agentturn.ToolStart
			for ev := range agentturn.Run(context.Background(), nil, openresponses.Items{openresponses.UserText("find")}, parent) {
				switch e := ev.(type) {
				case *agentturn.ToolStart:
					start = e
				case *agentturn.ToolEnd:
					end = e
				}
			}
			if end == nil || start == nil {
				t.Fatal("the parent never called the child")
			}
			if (len(asked) > 0) != tc.asked {
				t.Fatalf("asked %+v, want asked %v", asked, tc.asked)
			}
			if tc.asked && (asked[0].Name != "lookup" || string(asked[0].Args) != `{"text":"find"}` || under[0] != start.CallID) {
				t.Errorf("asked %+v under %v, want lookup filed under the parent's call %s", asked[0], under, start.CallID)
			}
			if !tc.input {
				if end.Err != nil || end.Result.Output.Text != tc.want {
					t.Errorf("parent's call = %q, %v; want %q", end.Result.Output.Text, end.Err, tc.want)
				}
				return
			}
			if !errors.Is(end.Err, agentturn.ErrInputRequired) {
				t.Fatalf("parent's call err = %v, want input required", end.Err)
			}
			var waiting agentturn.InputRequired
			if !errors.As(end.Err, &waiting) {
				t.Fatalf("err %T is no agentturn.InputRequired", end.Err)
			}
			info, _ := end.Result.Details.(ChildInfo)
			items := waiting.PendingCalls()
			if len(items) != 1 || items[0].Name != "lookup" || string(items[0].Args) != `{"text":"find"}` ||
				items[0].ID != info.Pending[0].Call.CallID || items[0].Callee != info.RunID || info.RunID == "" {
				t.Errorf("waiting = %+v, want the child's lookup on run %s", items, info.RunID)
			}
			if !strings.Contains(end.Err.Error(), "needs input") {
				t.Errorf("err = %v", end.Err)
			}
		})
	}
}

// TestAnAskedChildCallIsByTheAnswersDecider pins that the answer the
// parent's elicitor gives a child's deferred call decides it by the
// decider the answer names, and that a rule's note reaches the child's
// model in the developer's voice, after the call's output.
func TestAnAskedChildCallIsByTheAnswersDecider(t *testing.T) {
	lookup := agenttool.New("lookup", "", func(context.Context, struct {
		Text string `json:"text"`
	}) (string, error) {
		return "FOUND", nil
	})
	var reqs []openresponses.Items
	var starts []*agentturn.ToolStart
	// Two turns: echo calls the tool again on a request that ends in
	// the note, so the child is cut off after the one it reads it in.
	childCfg := agentturn.Config{Name: "child", Model: &echo.Adapter{}, Tools: []agenttool.Tool{lookup}, MaxTurns: 2,
		BeforeToolCall: func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "a person approves lookups"}, nil
		},
		BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
			reqs = append(reqs, append(openresponses.Items(nil), req.Input...))
			return nil
		}}
	child := New(childCfg, WithAskParent(), WithObserver(func(_ context.Context, ev agentturn.Event) {
		if s, ok := ev.(*agentturn.ToolStart); ok {
			starts = append(starts, s)
		}
	}))
	parent := agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{child}, MaxTurns: 1,
		ToolElicitor: func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) {
			return agenttool.Answer{Action: agenttool.ActionAccept, By: agenttool.ByPolicy, Note: "n"}, nil
		}}
	for range agentturn.Run(context.Background(), nil, openresponses.Items{openresponses.UserText("find")}, parent) {
	}
	if len(starts) == 0 || starts[0].Decision == nil || starts[0].Decision.Action != agentturn.Allow || starts[0].Decision.By != "policy" {
		t.Fatalf("child's tool_start = %+v, want allowed by policy", starts)
	}
	if len(reqs) < 2 {
		t.Fatalf("child's requests = %d, want 2", len(reqs))
	}
	last := reqs[1][len(reqs[1])-1]
	if m, ok := last.(*openresponses.Message); !ok || m.Role != openresponses.RoleDeveloper || m.Text() != "n" {
		t.Errorf("child's second request ends with %+v, want the developer message n", last)
	}
}

// twoCalls calls first and second in one response, then answers "done"
// once the input holds an output.
type twoCalls struct{ first, second string }

func (m twoCalls) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	for _, item := range req.Input {
		if _, ok := item.(*openresponses.FunctionCallOutput); ok {
			if err := em.Item(openresponses.AssistantText("done")); err != nil {
				return err
			}
			return em.Complete()
		}
	}
	for _, name := range []string{m.first, m.second} {
		w, err := em.FunctionCall("", name)
		if err != nil {
			return err
		}
		if err := w.Arguments(`{}`); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
	}
	return em.Complete()
}

// TestAHeldSiblingFollowsTheAnswer pins that a call the child's policy
// holds only beside an asked one runs when the asked call is accepted,
// in either order, and is refused unrun when it is declined, the child
// going on; only when nobody answered does it stay held, the call
// failing input-required naming it.
func TestAHeldSiblingFollowsTheAnswer(t *testing.T) {
	for _, tc := range []struct {
		name       string
		heldFirst  bool
		action     agenttool.Action
		wantRan    int
		wantFailed bool
	}{
		{"accepted, held second", false, agenttool.ActionAccept, 1, false},
		{"accepted, held first", true, agenttool.ActionAccept, 1, false},
		{"declined, held second", false, agenttool.ActionDecline, 0, false},
		{"declined, held first", true, agenttool.ActionDecline, 0, false},
		{"nobody answered, held second", false, agenttool.ActionCancel, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ran := 0
			lookup := agenttool.NewFunc("lookup", "", nil, func(context.Context, agenttool.Call) (agenttool.Result, error) {
				return agenttool.Text("FOUND"), nil
			})
			note := agenttool.NewFunc("note", "", nil, func(context.Context, agenttool.Call) (agenttool.Result, error) {
				ran++
				return agenttool.Text("noted"), nil
			})
			model := twoCalls{"lookup", "note"}
			if tc.heldFirst {
				model = twoCalls{"note", "lookup"}
			}
			childCfg := agentturn.Config{Name: "child", Model: model, Tools: []agenttool.Tool{lookup, note},
				BeforeToolCall: func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
					if info.Call.Name == "lookup" {
						return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "a person approves lookups", By: "policy"}, nil
					}
					return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "held beside lookup", By: "policy", Held: true}, nil
				}}
			parent := agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{New(childCfg, WithAskParent())}, MaxTurns: 1,
				ToolElicitor: func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) {
					return agenttool.Answer{Action: tc.action}, nil
				}}
			var end *agentturn.ToolEnd
			for ev := range agentturn.Run(context.Background(), nil, openresponses.Items{openresponses.UserText("find")}, parent) {
				if e, ok := ev.(*agentturn.ToolEnd); ok {
					end = e
				}
			}
			if end == nil {
				t.Fatal("the child never ran")
			}
			if ran != tc.wantRan {
				t.Errorf("the held call ran %d times, want %d", ran, tc.wantRan)
			}
			var waiting agentturn.InputRequired
			if failed := errors.As(end.Err, &waiting); failed != tc.wantFailed {
				t.Fatalf("parent's call = %q, %v; want input required %v", end.Result.Output.Text, end.Err, tc.wantFailed)
			}
			if tc.wantFailed {
				if items := waiting.PendingCalls(); len(items) != 2 {
					t.Errorf("waiting = %+v, want the asked lookup and the held note", items)
				}
			} else if end.Result.Output.Text != "done" {
				t.Errorf("parent's call = %q, want the child's answer", end.Result.Output.Text)
			}
		})
	}
}
