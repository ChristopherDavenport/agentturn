package session

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentsession/jsonl"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/compact"
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

// TestOmittedPartsStayInForce pins #147: the omitted list stays in
// force until a config changes it, so a memory larger than its block,
// with 474 omissions, pays for the list once and for the part that
// moved on each write after, rather than repeating the list on every
// delta, which cost more than the joined string. A list that changes
// is written, on an entry of its own when nothing else moved; one that
// empties is written as []; and a resumed recorder, seeded with the
// list in force, writes nothing for it.
func TestOmittedPartsStayInForce(t *testing.T) {
	l := &layers{ids: []string{"product", "memory"}, text: map[string]string{"product": strings.Repeat("p", 2000), "memory": "Memory: likes tea"}}
	for i := range 474 {
		l.omitted = append(l.omitted, agentsession.OmittedPart{ID: fmt.Sprintf("memory/%03d", i), Reason: "budget", Size: 80})
	}
	listBytes := jsonLen(l.omitted)
	partsOf := func(context.Context, openresponses.Request) ([]agentsession.InstructionPart, []agentsession.OmittedPart) {
		return l.parts(), append([]agentsession.OmittedPart(nil), l.omitted...)
	}
	cfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Instructions: "unrendered",
		BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
			req.Instructions = l.render()
			return nil
		}}
	// omittedMember is the instructions_omitted member of a config
	// entry as written, "" when it has none.
	omittedMember := func(t *testing.T, c *agentsession.ConfigEntry) string {
		t.Helper()
		raw, err := agentsession.MarshalEntry(c)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]json.RawMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		return string(m["instructions_omitted"])
	}
	steps := []struct {
		name   string
		change func()
		// resume reopens the session in a new recorder and agent
		// before the prompt.
		resume bool
		// want checks the config entries the prompt added.
		want func(t *testing.T, added []*agentsession.ConfigEntry)
	}{
		{"first", nil, false, func(t *testing.T, added []*agentsession.ConfigEntry) {
			// The base the hook renders over, then the request sent.
			if len(added) != 2 || len(added[1].InstructionsOmitted) != 474 {
				t.Errorf("first configs = %d entries", len(added))
			}
		}},
		{"a memory write", func() { l.text["memory"] += ", Bristol" }, false, func(t *testing.T, added []*agentsession.ConfigEntry) {
			if len(added) != 1 || omittedMember(t, added[0]) != "" {
				t.Fatalf("delta = %+v", added)
			}
			n := jsonLen(added[0])
			t.Logf("a %d byte patch under %d omitted parts: a %d byte delta, the list %d bytes", len(", Bristol"), len(l.omitted), n, listBytes)
			if n >= listBytes/10 {
				t.Errorf("the delta for a %d byte patch is %d bytes, the list %d", len(", Bristol"), n, listBytes)
			}
		}},
		{"another", func() { l.text["memory"] += ", cats" }, false, func(t *testing.T, added []*agentsession.ConfigEntry) {
			if len(added) != 1 || omittedMember(t, added[0]) != "" {
				t.Errorf("delta = %+v", added)
			}
		}},
		{"the list moves alone", func() { l.omitted = l.omitted[1:] }, false, func(t *testing.T, added []*agentsession.ConfigEntry) {
			// A keep counts from the head of the list in force, so the
			// part after the one dropped is named and the other 472
			// are one keep.
			if len(added) != 1 || omittedMember(t, added[0]) != `[{"id":"memory/001","reason":"budget","size":80},{"keep":472}]` || added[0].InstructionsParts != nil || added[0].Instructions != nil {
				t.Errorf("entry = %+v", added)
			}
		}},
		{"nothing moves", nil, false, func(t *testing.T, added []*agentsession.ConfigEntry) {
			if len(added) != 0 {
				t.Errorf("configs = %+v", added)
			}
		}},
		{"a resume", nil, true, func(t *testing.T, added []*agentsession.ConfigEntry) {
			if len(added) != 0 {
				t.Errorf("configs = %+v", added)
			}
		}},
		{"the list empties", func() { l.omitted = nil }, false, func(t *testing.T, added []*agentsession.ConfigEntry) {
			if len(added) != 1 || omittedMember(t, added[0]) != "[]" || added[0].InstructionsParts != nil {
				t.Errorf("entry = %+v", added)
			}
		}},
		{"an empty list after a resume", nil, true, func(t *testing.T, added []*agentsession.ConfigEntry) {
			if len(added) != 0 {
				t.Errorf("configs = %+v", added)
			}
		}},
	}
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{}, WithInstructionsParts(partsOf))
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(cfg)
	unsub := rec.Attach(a)
	for _, st := range steps {
		if st.change != nil {
			st.change()
		}
		if st.resume {
			unsub()
			rec, s, err = Resume(context.Background(), store, s.ID(), WithInstructionsParts(partsOf))
			if err != nil {
				t.Fatal(err)
			}
			opts, err := AgentOptions(s)
			if err != nil {
				t.Fatal(err)
			}
			a = agentturn.New(cfg, opts...)
			unsub = rec.Attach(a)
		}
		seen := len(configs(s))
		if _, err := a.Prompt(context.Background(), openresponses.UserText(st.name)); err != nil {
			t.Fatalf("%s: %v", st.name, err)
		}
		st.want(t, configs(s)[seen:])
		cx, err := s.Context()
		if err != nil {
			t.Fatal(err)
		}
		if len(cx.InstructionsOmitted()) != len(l.omitted) {
			t.Errorf("%s: %d omitted in force, the host left out %d", st.name, len(cx.InstructionsOmitted()), len(l.omitted))
		}
	}
	unsub()
	verifyAll(t, s)
}

// TestOmittedDeltaKeepsRuns pins format 0.9 (agentsession #115): a
// part moving across a memory's budget writes that part and a keep for
// each run of the list in force around it, not the list again, and the
// file read back resolves the list whole. A replace discards the list
// its keeps would count over, so it carries the list whole; a resumed
// recorder and a fold's checkpoint start from the list the context
// resolved, so the keeps written after them count over it.
func TestOmittedDeltaKeepsRuns(t *testing.T) {
	l := &layers{ids: []string{"product", "memory"}, text: map[string]string{"product": strings.Repeat("p", 2000), "memory": "Memory: likes tea"}}
	for i := range 400 {
		l.omitted = append(l.omitted, agentsession.OmittedPart{ID: fmt.Sprintf("memory/%03d", i), Reason: "budget", Size: 80})
	}
	listBytes := jsonLen(l.omitted)
	partsOf := func(context.Context, openresponses.Request) ([]agentsession.InstructionPart, []agentsession.OmittedPart) {
		return l.parts(), append([]agentsession.OmittedPart(nil), l.omitted...)
	}
	// save puts a new fact under the budget at i, as a memory's save
	// pushes one out of its block.
	save := func(i int, id string) func() {
		return func() {
			l.omitted = slices.Insert(l.omitted, i, agentsession.OmittedPart{ID: id, Reason: "budget", Size: 80})
		}
	}
	model := "m"
	cfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Instructions: "unrendered",
		BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
			req.Instructions = l.render()
			req.Model = model
			return nil
		}}
	// written counts the keeps and the parts named whole in a config's
	// instructions_omitted.
	written := func(c *agentsession.ConfigEntry) (keeps, whole int) {
		for _, p := range c.InstructionsOmitted {
			if p.Keep > 0 {
				keeps++
			} else {
				whole++
			}
		}
		return keeps, whole
	}
	// moved wants one delta naming the part that moved, the runs
	// around it kept, at a tenth of the list or less.
	moved := func(t *testing.T, added []*agentsession.ConfigEntry) {
		t.Helper()
		if len(added) != 1 || added[0].Replace {
			t.Fatalf("configs = %+v", added)
		}
		if keeps, whole := written(added[0]); keeps == 0 || whole != 1 {
			t.Errorf("omitted = %+v, want keeps around one part", added[0].InstructionsOmitted)
		}
		if n := jsonLen(added[0]); n >= listBytes/10 {
			t.Errorf("one part moving under %d omitted wrote %d bytes, the list is %d", len(l.omitted), n, listBytes)
		}
	}
	// folding folds the transcript every few prompts, recorded by rec.
	folding := func(rec *Recorder) {
		cfg.Transform = compact.NewLocal(&echo.Adapter{}, compact.WithBudget(6), compact.WithKeepLast(2), compact.WithEstimator(countItems), compact.WithModel("summariser"), compact.WithOnFold(rec.Fold)).Transform
	}
	steps := []struct {
		name   string
		change func()
		resume bool
		want   func(t *testing.T, added []*agentsession.ConfigEntry)
	}{
		{"first", nil, false, func(t *testing.T, added []*agentsession.ConfigEntry) {
			// The base the hook renders over, then the request sent,
			// with nothing in force for a keep to count over.
			if keeps, whole := written(added[len(added)-1]); keeps != 0 || whole != 400 {
				t.Errorf("first config wrote %d keeps and %d parts", keeps, whole)
			}
		}},
		{"a save", save(200, "memory/new-1"), false, func(t *testing.T, added []*agentsession.ConfigEntry) {
			moved(t, added)
			want := `[{"keep":200},{"id":"memory/new-1","reason":"budget","size":80},{"keep":200}]`
			if got, _ := json.Marshal(added[0].InstructionsOmitted); string(got) != want {
				t.Errorf("omitted = %s, want %s", got, want)
			}
		}},
		{"a forget", func() { l.omitted = slices.Delete(l.omitted, 3, 4) }, false, func(t *testing.T, added []*agentsession.ConfigEntry) {
			if len(added) != 1 || jsonLen(added[0]) >= listBytes/10 {
				t.Errorf("configs = %+v", added)
			}
		}},
		{"a replace", func() { model = ""; save(0, "memory/new-2")() }, false, func(t *testing.T, added []*agentsession.ConfigEntry) {
			// Clearing the model is a replace, which discards the list
			// a keep counts over: the list is written whole.
			if len(added) != 1 || !added[0].Replace {
				t.Fatalf("configs = %+v", added)
			}
			if keeps, whole := written(added[0]); keeps != 0 || whole != len(l.omitted) {
				t.Errorf("replace wrote %d keeps and %d parts, want the %d parts", keeps, whole, len(l.omitted))
			}
		}},
		{"a save after the replace", func() { model = "m"; save(10, "memory/new-3")() }, false, moved},
		{"a resume", nil, true, func(t *testing.T, added []*agentsession.ConfigEntry) {
			if len(added) != 0 {
				t.Errorf("configs = %+v", added)
			}
		}},
		{"a save after the resume", save(300, "memory/new-4"), false, moved},
		{"another save", save(len(l.omitted)-1, "memory/new-5"), false, moved},
	}
	root := t.TempDir()
	store, err := jsonl.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	opts := []Option{WithInstructionsParts(partsOf)}
	rec, s, err := Start(context.Background(), store, agentsession.Header{CWD: root}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	folding(rec)
	a := agentturn.New(cfg)
	unsub := rec.Attach(a)
	for _, st := range steps {
		if st.change != nil {
			st.change()
		}
		if st.resume {
			unsub()
			if err := store.Release(s.ID()); err != nil {
				t.Fatal(err)
			}
			rec, s, err = Resume(context.Background(), store, s.ID(), opts...)
			if err != nil {
				t.Fatal(err)
			}
			aopts, err := AgentOptions(s)
			if err != nil {
				t.Fatal(err)
			}
			folding(rec)
			a = agentturn.New(cfg, aopts...)
			unsub = rec.Attach(a)
		}
		seen := len(configs(s))
		if _, err := a.Prompt(context.Background(), openresponses.UserText(st.name)); err != nil {
			t.Fatalf("%s: %v", st.name, err)
		}
		t.Run(st.name, func(t *testing.T) { st.want(t, configs(s)[seen:]) })
		cx, err := s.Context()
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(cx.InstructionsOmitted(), l.omitted) {
			t.Errorf("%s: the list in force is not the one the host left out", st.name)
		}
	}
	unsub()
	verifyAll(t, s)
	id := s.ID()
	if err := store.Release(id); err != nil {
		t.Fatal(err)
	}

	// Read the file back through a fresh store: the keeps resolve to
	// the list whole, and a fold's checkpoint holds it whole.
	store2, err := jsonl.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	s2, err := store2.Open(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	cx, err := s2.Context()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cx.InstructionsOmitted(), l.omitted) {
		t.Errorf("read back, %d omitted in force, the host left out %d", len(cx.InstructionsOmitted()), len(l.omitted))
	}
	folds := 0
	for _, e := range s2.Entries() {
		if c, ok := e.(*agentsession.CompactionEntry); ok {
			folds++
			for _, p := range c.Config.InstructionsOmitted {
				if p.Keep > 0 || p.Unresolved() {
					t.Errorf("a checkpoint holds %+v", p)
				}
			}
		}
	}
	if folds == 0 {
		t.Errorf("no fold in %q", entryTypes(s2))
	}
}
