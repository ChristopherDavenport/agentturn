package session

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// layers is a product's instructions as named parts, rendered into the
// request by a BeforeModelCall hook, as a memory block is.
type layers struct {
	ids     []string
	text    map[string]string
	omitted []agentsession.OmittedPart
}

func (l *layers) parts() []agentsession.InstructionPart {
	out := make([]agentsession.InstructionPart, len(l.ids))
	for i, id := range l.ids {
		out[i] = agentsession.InstructionPart{ID: id, Text: l.text[id], Source: "layer"}
	}
	return out
}

func (l *layers) render() string { return agentsession.JoinInstructions(l.parts()) }

// step is one prompt of an instructions parts case.
type step struct {
	// change runs before the prompt; nil changes nothing.
	change func(*layers)
	// want describes the config entries the prompt adds.
	want func(t *testing.T, added []*agentsession.ConfigEntry)
}

// stringSteps are the prompts of a case whose parts the recorder
// cannot use: the entries carry the string and nothing omitted.
var stringSteps = []step{{
	want: func(t *testing.T, added []*agentsession.ConfigEntry) {
		if len(added) != 1 || added[0].Instructions == nil || len(added[0].InstructionsParts) != 0 || len(added[0].InstructionsOmitted) != 0 {
			t.Errorf("first config = %+v", added)
		}
	},
}, {
	change: func(l *layers) { l.text["memory"] += "Likes Bristol" },
	want: func(t *testing.T, added []*agentsession.ConfigEntry) {
		if len(added) != 1 || added[0].Instructions == nil || len(added[0].InstructionsParts) != 0 {
			t.Errorf("delta = %+v", added)
		}
	},
}}

func TestInstructionsPartsAreRecorded(t *testing.T) {
	big := func(s string) string { return strings.Repeat(s, 4000) }
	cases := []struct {
		name string
		// mangle, when set, spoils what the host names: parts that do
		// not join, or that the format refuses, so the recorder falls
		// back to the string rather than fail the run.
		mangle func([]agentsession.InstructionPart, []agentsession.OmittedPart) ([]agentsession.InstructionPart, []agentsession.OmittedPart)
		steps  []step
	}{{
		name: "a memory write is a delta naming the one part",
		steps: []step{{
			want: func(t *testing.T, added []*agentsession.ConfigEntry) {
				if len(added) != 1 || !added[0].Replace || added[0].Instructions != nil || len(added[0].InstructionsParts) != 3 {
					t.Fatalf("first config = %+v", added)
				}
				if len(added[0].InstructionsOmitted) != 1 || added[0].InstructionsOmitted[0].ID != "skills" {
					t.Errorf("omitted = %+v", added[0].InstructionsOmitted)
				}
			},
		}, {
			change: func(l *layers) { l.text["memory"] += "Likes Bristol" },
			want: func(t *testing.T, added []*agentsession.ConfigEntry) {
				if len(added) != 1 || added[0].Replace || added[0].Instructions != nil {
					t.Fatalf("delta = %+v", added)
				}
				// The two unchanged 4000 byte layers are one keep, so the
				// delta costs the memory layer, not the prompt.
				ps := added[0].InstructionsParts
				if len(ps) != 2 || ps[0].Keep != 2 || ps[1].ID != "memory" || ps[1].Text == "" {
					t.Errorf("delta parts = %+v, want a keep of 2 and the memory text", ps)
				}
				if n := jsonLen(added[0]); n > 1000 {
					t.Errorf("delta is %d bytes", n)
				}
			},
		}, {
			want: func(t *testing.T, added []*agentsession.ConfigEntry) {
				if len(added) != 0 {
					t.Errorf("an unchanged composition wrote %+v", added)
				}
			},
		}, {
			change: func(l *layers) {
				l.omitted = append(l.omitted, agentsession.OmittedPart{ID: "notes", Reason: "budget"})
			},
			want: func(t *testing.T, added []*agentsession.ConfigEntry) {
				if len(added) != 1 || len(added[0].InstructionsOmitted) != 2 || len(added[0].InstructionsParts) != 0 || added[0].Instructions != nil {
					t.Errorf("omitted-only change = %+v", added)
				}
			},
		}},
	}, {
		name: "parts that do not join fall back to the string",
		mangle: func(p []agentsession.InstructionPart, o []agentsession.OmittedPart) ([]agentsession.InstructionPart, []agentsession.OmittedPart) {
			return p[:2], o
		},
		steps: stringSteps,
	}, {
		name: "parts the format refuses fall back to the string",
		mangle: func(p []agentsession.InstructionPart, o []agentsession.OmittedPart) ([]agentsession.InstructionPart, []agentsession.OmittedPart) {
			// They join, but two share an ID.
			p[1].ID = p[0].ID
			return p, o
		},
		steps: stringSteps,
	}, {
		name: "an omitted part with no ID is dropped alone",
		mangle: func(p []agentsession.InstructionPart, o []agentsession.OmittedPart) ([]agentsession.InstructionPart, []agentsession.OmittedPart) {
			return p, append(o, agentsession.OmittedPart{Reason: "nameless"})
		},
		steps: []step{{
			want: func(t *testing.T, added []*agentsession.ConfigEntry) {
				if len(added) != 1 || len(added[0].InstructionsParts) != 3 || len(added[0].InstructionsOmitted) != 1 {
					t.Errorf("first config = %+v", added)
				}
			},
		}},
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := &layers{
				ids:     []string{"product", "agents_md", "memory"},
				text:    map[string]string{"product": big("p"), "agents_md": big("a"), "memory": "Memory:"},
				omitted: []agentsession.OmittedPart{{ID: "skills", Reason: "out of scope", Size: 12}},
			}
			partsOf := func(_ context.Context, req openresponses.Request) ([]agentsession.InstructionPart, []agentsession.OmittedPart) {
				parts, omitted := l.parts(), append([]agentsession.OmittedPart(nil), l.omitted...)
				if tc.mangle != nil {
					return tc.mangle(parts, omitted)
				}
				return parts, omitted
			}
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{}, WithInstructionsParts(partsOf))
			if err != nil {
				t.Fatal(err)
			}
			cfg := agentturn.Config{
				Model: &echo.Adapter{}, ModelName: "m", Instructions: l.render(),
				// The render happens in the hook, so the parts must be
				// taken from the request it leaves.
				BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
					req.Instructions = l.render()
					return nil
				},
			}
			a := agentturn.New(cfg)
			defer rec.Attach(a)()
			seen := 0
			for i, st := range tc.steps {
				if st.change != nil {
					st.change(l)
				}
				if _, err := a.Prompt(context.Background(), openresponses.UserText("turn")); err != nil {
					t.Fatalf("prompt %d: %v", i, err)
				}
				all := configs(s)
				st.want(t, all[seen:])
				seen = len(all)
			}
			verifyAll(t, s)
			cx, err := s.Context()
			if err != nil {
				t.Fatal(err)
			}
			if cx.Settings.Instructions != l.render() {
				t.Errorf("the path replays to %d bytes of instructions, the last request sent %d", len(cx.Settings.Instructions), len(l.render()))
			}
		})
	}
}

// TestInstructionsPartsSurviveResume checks that a recorder resumed on a
// session whose settings carry parts writes nothing for an unchanged
// composition and names the moved part by itself afterwards.
func TestInstructionsPartsSurviveResume(t *testing.T) {
	l := &layers{ids: []string{"a", "b"}, text: map[string]string{"a": strings.Repeat("a", 3000), "b": "b"}}
	partsOf := func(context.Context, openresponses.Request) ([]agentsession.InstructionPart, []agentsession.OmittedPart) {
		return l.parts(), nil
	}
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{}, WithInstructionsParts(partsOf))
	if err != nil {
		t.Fatal(err)
	}
	cfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Instructions: l.render()}
	a := agentturn.New(cfg)
	unsub := rec.Attach(a)
	if _, err := a.Prompt(context.Background(), openresponses.UserText("one")); err != nil {
		t.Fatal(err)
	}
	unsub()

	rec2, s2, err := Resume(context.Background(), store, s.ID(), WithInstructionsParts(partsOf))
	if err != nil {
		t.Fatal(err)
	}
	b := agentturn.New(cfg, agentturn.WithTranscript(a.State().Transcript))
	unsub = rec2.Attach(b)
	if _, err := b.Prompt(context.Background(), openresponses.UserText("two")); err != nil {
		t.Fatal(err)
	}
	if n := len(configs(s2)); n != 1 {
		t.Fatalf("configs after an unchanged resume = %d, want 1", n)
	}
	l.text["b"] = "b, changed"
	cfg.Instructions = l.render()
	unsub()
	if err := b.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	defer rec2.Attach(b)()
	if _, err := b.Prompt(context.Background(), openresponses.UserText("three")); err != nil {
		t.Fatal(err)
	}
	all := configs(s2)
	if len(all) != 2 {
		t.Fatalf("configs = %d, want 2", len(all))
	}
	raw, _ := json.Marshal(all[1])
	if strings.Contains(string(raw), strings.Repeat("a", 100)) {
		t.Errorf("the delta repeats the unchanged part: %s", raw)
	}
	verifyAll(t, s2)
}

// TestInstructionsPartsReachChildSessions pins #142: the parts function
// is asked for a child run's session too, with a context naming it, so
// a child with layers of its own records them rather than the string,
// whether or not the host put the child's ID on the run's context.
func TestInstructionsPartsReachChildSessions(t *testing.T) {
	root := &layers{ids: []string{"product"}, text: map[string]string{"product": "You delegate."}}
	child := &layers{
		ids:     []string{"prompt", "memory"},
		text:    map[string]string{"prompt": strings.Repeat("s", 3000), "memory": "Memory:"},
		omitted: []agentsession.OmittedPart{{ID: "notes", Reason: "budget", Size: 40}},
	}
	for _, tc := range []struct {
		name     string
		childCtx bool
	}{
		{"with the child's context", true},
		{"without it", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			var rec *Recorder
			asked := map[string]bool{}
			partsOf := func(ctx context.Context, req openresponses.Request) ([]agentsession.InstructionPart, []agentsession.OmittedPart) {
				id := SessionIDFromContext(ctx)
				asked[id] = true
				if id == rec.SessionID() {
					return root.parts(), nil
				}
				return child.parts(), child.omitted
			}
			rec, s, err := Start(context.Background(), store, agentsession.Header{}, WithInstructionsParts(partsOf))
			if err != nil {
				t.Fatal(err)
			}
			opts := []agent.Option{agent.WithObserver(rec.Observe)}
			if tc.childCtx {
				opts = append(opts, agent.WithRunContext(rec.ChildContext))
			}
			childCfg := agentturn.Config{Name: "specialist", Description: "remembers", Model: &echo.Adapter{}, ModelName: "m", Instructions: child.render()}
			specialist := agent.New(childCfg, opts...)
			a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Instructions: root.render(), Tools: []agenttool.Tool{specialist}})
			defer rec.Attach(a)()
			if _, err := a.Prompt(context.Background(), openresponses.UserText("delegate")); err != nil {
				t.Fatal(err)
			}
			l := links(s)
			if len(l) != 1 {
				t.Fatalf("links = %+v", l)
			}
			cs, err := store.Open(context.Background(), l[0].Session)
			if err != nil {
				t.Fatal(err)
			}
			if !asked[cs.ID()] || !asked[s.ID()] {
				t.Errorf("parts asked for %v, want the root %q and the child %q", asked, s.ID(), cs.ID())
			}
			for _, sess := range []*agentsession.Session{s, cs} {
				cfgs := configs(sess)
				if len(cfgs) == 0 || len(cfgs[0].InstructionsParts) == 0 || cfgs[0].Instructions != nil {
					t.Errorf("session %s's first config = %+v, want parts", sess.ID(), cfgs)
				}
			}
			if cfgs := configs(cs); len(cfgs) == 0 || len(cfgs[0].InstructionsOmitted) != 1 {
				t.Errorf("the child's omitted parts = %+v", cfgs)
			}
			verifyAll(t, s)
			verifyAll(t, cs)
		})
	}
}

// TestInstructionsPartsSurviveEveryResume checks that a stateless host,
// which resumes the session for every turn under a hook that renders
// its parts into the request, pays nothing per resume for an unchanged
// composition: the base request its configuration names is not what
// the parts stand for, so settling it at run start would replace them
// with a string that was never sent and turn start would write every
// part again. A path that recorded no base is held to the same.
func TestInstructionsPartsSurviveEveryResume(t *testing.T) {
	l := &layers{ids: []string{"product", "memory"}, text: map[string]string{"product": strings.Repeat("p", 5000), "memory": "Memory:"}}
	partsOf := func(context.Context, openresponses.Request) ([]agentsession.InstructionPart, []agentsession.OmittedPart) {
		return l.parts(), nil
	}
	cfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Instructions: "unrendered",
		BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
			req.Instructions = l.render()
			return nil
		}}
	for _, unrecorded := range []bool{false, true} {
		name := "recorded base"
		if unrecorded {
			name = "no recorded base"
		}
		t.Run(name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{}, WithInstructionsParts(partsOf))
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(cfg)
			var unsub func()
			if unrecorded {
				unsub = a.Subscribe(rec.Handle)
			} else {
				unsub = rec.Attach(a)
			}
			if _, err := a.Prompt(context.Background(), openresponses.UserText("one")); err != nil {
				t.Fatal(err)
			}
			unsub()
			for i := range 3 {
				rec, s, err = Resume(context.Background(), store, s.ID(), WithInstructionsParts(partsOf))
				if err != nil {
					t.Fatal(err)
				}
				cx, err := s.Context()
				if err != nil {
					t.Fatal(err)
				}
				b := agentturn.New(cfg, agentturn.WithTranscript(cx.Items))
				before, from := jsonLen(configs(s)), len(s.Entries())
				unsub := rec.Attach(b)
				if _, err := b.Prompt(context.Background(), openresponses.UserText("again")); err != nil {
					t.Fatal(err)
				}
				unsub()
				if n := jsonLen(configs(s)) - before; n > 0 {
					t.Errorf("resume %d wrote %d bytes of config for an unchanged composition", i+1, n)
				}
				for _, e := range s.Entries()[from:] {
					if c, ok := e.(*agentsession.ConfigEntry); ok && c.Instructions != nil {
						t.Errorf("resume %d set the instructions to %q", i+1, *c.Instructions)
					}
				}
			}
			verifyAll(t, s)
		})
	}
}
