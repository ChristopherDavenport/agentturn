package agentturn

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/openresponses"
)

// thinker answers every request with a reasoning item whose encrypted
// content names the requested model, then a message, or, with call set
// and a user message last, a call to the first tool.
type thinker struct{ call bool }

func (m thinker) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if err := em.Item(&openresponses.ReasoningItem{Summary: openresponses.Contents{}, EncryptedContent: "sig:" + req.Model}); err != nil {
		return err
	}
	if last, ok := req.Input[len(req.Input)-1].(*openresponses.Message); ok && m.call && last.Role == openresponses.RoleUser {
		w, err := em.FunctionCall("", req.Tools[0].(*openresponses.FunctionTool).Name)
		if err != nil {
			return err
		}
		if err := w.Arguments(`{}`); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		return em.Complete()
	}
	if err := em.Item(openresponses.AssistantText("ok")); err != nil {
		return err
	}
	return em.Complete()
}

// signatures lists the encrypted content of the reasoning items in
// items, in order.
func signatures(items openresponses.Items) []string {
	sigs := []string{}
	for _, item := range items {
		if r, ok := item.(*openresponses.ReasoningItem); ok {
			sigs = append(sigs, r.EncryptedContent)
		}
	}
	return sigs
}

// TestReasoningAcrossModels pins #91: a request leaves out the
// reasoning items a model with another ModelName produced, after
// SetConfig between runs, and keeps the model's own, those of a model
// switched back to, and every one when no model is named; the
// transcript keeps them all.
func TestReasoningAcrossModels(t *testing.T) {
	lookup := agenttool.NewFunc("lookup", "looks something up", json.RawMessage(`{"type":"object"}`),
		func(context.Context, agenttool.Call) (agenttool.Result, error) {
			return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "found"}}, nil
		})
	for _, tc := range []struct {
		name   string
		models []string // one run per model, a SetConfig between
		call   bool
		want   []string // the reasoning the last request carries
	}{
		{"another model", []string{"a", "b"}, false, []string{}},
		{"same model", []string{"a", "a"}, false, []string{"sig:a"}},
		{"back again", []string{"a", "b", "a"}, false, []string{"sig:a"}},
		{"unnamed models", []string{"", ""}, false, []string{"sig:"}},
		{"unnamed after named", []string{"a", ""}, false, []string{"sig:a"}},
		{"own turn that called a tool", []string{"a"}, true, []string{"sig:a"}},
		{"another model after a tool turn", []string{"a", "b"}, true, []string{"sig:b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var last openresponses.Items
			record := func(_ context.Context, req *openresponses.Request) error {
				last = req.Input
				return nil
			}
			a := New(Config{})
			for _, name := range tc.models {
				if err := a.SetConfig(Config{Model: thinker{call: tc.call}, ModelName: name, Tools: []agenttool.Tool{lookup}, BeforeModelCall: record}); err != nil {
					t.Fatal(err)
				}
				if _, err := a.Prompt(context.Background(), openresponses.UserText("hello")); err != nil {
					t.Fatal(err)
				}
			}
			if got := signatures(last); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("last request's reasoning = %v, want %v", got, tc.want)
			}
			if got, want := len(signatures(a.State().Transcript)), len(tc.models)*map[bool]int{false: 1, true: 2}[tc.call]; got != want {
				t.Errorf("transcript holds %d reasoning items, want %d", got, want)
			}
		})
	}
}

// TestReasoningModelsSeeded pins #91 for a transcript the loop has not
// seen: its reasoning items are sent until a host attributes them, with
// WithReasoningModels for an agent or ContextWithReasoningModels for
// Continue.
func TestReasoningModelsSeeded(t *testing.T) {
	for _, tc := range []struct {
		name string
		seed string // "", "option" or "context"
		low  bool   // drive the low-level Continue rather than an agent
		want []string
	}{
		{"agent, unattributed", "", false, []string{"sig:a"}},
		{"agent, option", "option", false, []string{}},
		{"agent, context", "context", false, []string{}},
		{"continue, unattributed", "", true, []string{"sig:a"}},
		{"continue, context", "context", true, []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &openresponses.ReasoningItem{Summary: openresponses.Contents{}, EncryptedContent: "sig:a"}
			stored := Transcript{openresponses.UserText("hello"), r, openresponses.AssistantText("ok"), openresponses.UserText("again")}
			seed := ReasoningModels{r: "a"}
			var first openresponses.Items
			cfg := Config{Model: thinker{}, ModelName: "b", BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
				if first == nil {
					first = req.Input
				}
				return nil
			}}
			ctx := context.Background()
			if tc.seed == "context" {
				ctx = ContextWithReasoningModels(ctx, seed)
			}
			if tc.low {
				for ev := range Continue(ctx, stored, cfg) {
					if end, ok := ev.(*RunEnd); ok && end.Err != nil {
						t.Fatal(end.Err)
					}
				}
			} else {
				opts := []Option{WithTranscript(stored)}
				if tc.seed == "option" {
					opts = append(opts, WithReasoningModels(seed))
				}
				if _, err := New(cfg, opts...).Continue(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if got := signatures(first); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("request's reasoning = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestReasoningModelsFor pins what For leaves out.
func TestReasoningModelsFor(t *testing.T) {
	a := &openresponses.ReasoningItem{EncryptedContent: "a"}
	b := &openresponses.ReasoningItem{EncryptedContent: "b"}
	unknown := &openresponses.ReasoningItem{EncryptedContent: "?"}
	user := openresponses.UserText("hi")
	m := ReasoningModels{}
	m.Attribute("a", openresponses.Items{user, a})
	m.Attribute("b", openresponses.Items{b, a})
	m.Attribute("", openresponses.Items{unknown})
	for _, tc := range []struct {
		name  string
		model string
		t     Transcript
		want  []string
	}{
		{"own kept", "a", Transcript{a, user}, []string{"a"}},
		{"foreign first", "b", Transcript{a, user, b}, []string{"b"}},
		{"unknown kept", "c", Transcript{unknown, a, b}, []string{"?"}},
		{"unnamed model", "", Transcript{a, b}, []string{"a", "b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := m.For(tc.model, tc.t)
			if sigs := signatures(got); !reflect.DeepEqual(sigs, tc.want) {
				t.Errorf("For(%q) reasoning = %v, want %v", tc.model, sigs, tc.want)
			}
			if len(got) != len(tc.t)-(len(signatures(tc.t))-len(tc.want)) {
				t.Errorf("For(%q) kept %d items of %d", tc.model, len(got), len(tc.t))
			}
		})
	}
	if m[a] != "a" || m[b] != "b" {
		t.Errorf("Attribute overwrote an attribution: %v", m)
	}
}

// TestAToolsRunLeavesOutTheParentsReasoning pins #242's loop half: the
// context a tool runs under carries the run's reasoning attribution, so
// a run the tool starts over the parent's items under another model
// leaves out the reasoning the parent's model produced, as a
// configuration switch does, and one under the same model keeps it.
func TestAToolsRunLeavesOutTheParentsReasoning(t *testing.T) {
	for _, tc := range []struct {
		child string
		want  []string
	}{
		{"child", []string{}},
		{"parent", []string{"sig:parent"}},
	} {
		t.Run(tc.child, func(t *testing.T) {
			var sent openresponses.Items
			child := Config{Model: thinker{}, ModelName: tc.child, BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
				sent = req.Input
				return nil
			}}
			delegate := agenttool.NewFunc("delegate", "runs a child over the conversation", json.RawMessage(`{"type":"object"}`),
				func(ctx context.Context, _ agenttool.Call) (agenttool.Result, error) {
					parent, _ := TranscriptFromContext(ctx)
					for range Run(ctx, nil, append(parent, openresponses.UserText("go on")), child) {
					}
					return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "done"}}, nil
				})
			a := New(Config{Model: thinker{call: true}, ModelName: "parent", Tools: []agenttool.Tool{delegate}})
			if _, err := a.Prompt(context.Background(), openresponses.UserText("delegate")); err != nil {
				t.Fatal(err)
			}
			if got := signatures(sent); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("reasoning the child's request carried = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestEveryHookContextCarriesTheAttribution pins RFC 0001 The request
// step 3 for hooks as well as tools: BeforeTurn, the transform,
// BeforeModelCall, OutputGuard and ShouldStopAfterTurn run under a
// context that carries the run's reasoning attribution (#242).
func TestEveryHookContextCarriesTheAttribution(t *testing.T) {
	seen := map[string]int{}
	note := func(name string, ctx context.Context) { seen[name] = len(reasoningFromContext(ctx)) }
	cfg := Config{Model: thinker{}, ModelName: "a",
		BeforeTurn: func(ctx context.Context, _ TurnStartInfo) (openresponses.Items, error) {
			note("before_turn", ctx)
			return nil, nil
		},
		Transform: func(ctx context.Context, t Transcript) (Transcript, error) {
			note("transform", ctx)
			return t, nil
		},
		BeforeModelCall: func(ctx context.Context, _ *openresponses.Request) error {
			note("before_model_call", ctx)
			return nil
		},
		OutputGuard: func(ctx context.Context, _ OutputInfo) (*openresponses.Message, error) {
			note("output_guard", ctx)
			return nil, nil
		},
		ShouldStopAfterTurn: func(ctx context.Context, _ TurnInfo) (bool, error) {
			note("should_stop", ctx)
			return false, nil
		},
	}
	a := New(cfg)
	for _, text := range []string{"one", "two"} {
		if _, err := a.Prompt(context.Background(), openresponses.UserText(text)); err != nil {
			t.Fatal(err)
		}
	}
	for _, hook := range []string{"before_turn", "transform", "before_model_call", "output_guard", "should_stop"} {
		if seen[hook] == 0 {
			t.Errorf("%s ran under no reasoning attribution on the second run", hook)
		}
	}
}

// fallbackThinker answers as thinker does, calling the first tool until
// the input holds two outputs, records the model and the reasoning of
// every request, and, with fail set, fails the second request with an
// overloaded server error, before anything of an answer.
type fallbackThinker struct {
	fail bool
	n    int
	sent []string
}

func (m *fallbackThinker) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.n++
	m.sent = append(m.sent, req.Model+" "+fmt.Sprint(signatures(req.Input)))
	if m.fail && m.n == 2 {
		return openresponses.ServerError("overloaded", "overloaded")
	}
	outputs := 0
	for _, item := range req.Input {
		if _, ok := item.(*openresponses.FunctionCallOutput); ok {
			outputs++
		}
	}
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if err := em.Item(&openresponses.ReasoningItem{Summary: openresponses.Contents{}, EncryptedContent: "sig:" + req.Model}); err != nil {
		return err
	}
	if outputs < 2 {
		w, err := em.FunctionCall("", req.Tools[0].(*openresponses.FunctionTool).Name)
		if err != nil {
			return err
		}
		if err := w.Arguments(`{}`); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		return em.Complete()
	}
	if err := em.Item(openresponses.AssistantText("ok")); err != nil {
		return err
	}
	return em.Complete()
}

// TestARequestIsBuiltForTheModelItNames pins #286: a retry the policy
// moves to another model sends an input built for that model, without
// the reasoning the first model produced, and the reasoning the
// fallback produced is credited to the fallback, so the next turn, on
// the first model again, leaves it out. A before-model-call hook that
// moves a turn to another model and keeps the loop's input is held to
// the same rule.
func TestARequestIsBuiltForTheModelItNames(t *testing.T) {
	lookup := agenttool.NewFunc("lookup", "looks something up", json.RawMessage(`{"type":"object"}`),
		func(context.Context, agenttool.Call) (agenttool.Result, error) {
			return agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "found"}}, nil
		})
	moveToB := func(_ int, req *openresponses.Request, _ error) *openresponses.Request {
		req.Model = "b"
		return nil
	}
	for _, tc := range []struct {
		name string
		cfg  func(m *fallbackThinker) Config
		want []string
	}{
		{"a retry revised to another model", func(m *fallbackThinker) Config {
			m.fail = true
			return Config{Model: m, ModelName: "a", Tools: []agenttool.Tool{lookup},
				Retry: Retry{MaxAttempts: 2, Backoff: func(int, error) time.Duration { return 0 }, Revise: moveToB}}
		}, []string{
			"a []",
			"a [sig:a]",
			"b []",      // the fallback is not sent a's reasoning
			"a [sig:a]", // nor a b's
		}},
		{"a hook that moves a turn", func(m *fallbackThinker) Config {
			return Config{Model: m, ModelName: "a", Tools: []agenttool.Tool{lookup},
				BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
					if m.n == 1 {
						req.Model = "b"
					}
					return nil
				}}
		}, []string{"a []", "b []", "a [sig:a]"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &fallbackThinker{}
			a := New(tc.cfg(m))
			end, err := a.Prompt(context.Background(), openresponses.UserText("look it up"))
			if err != nil || end.Reason != ReasonDone {
				t.Fatalf("err=%v end=%+v", err, end)
			}
			if !reflect.DeepEqual(m.sent, tc.want) {
				t.Errorf("requests = %q, want %q", m.sent, tc.want)
			}
		})
	}
}
