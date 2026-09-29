package session

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agentturn"
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

func TestInstructionsPartsAreRecorded(t *testing.T) {
	big := func(s string) string { return strings.Repeat(s, 4000) }
	type step struct {
		// change runs before the prompt; nil changes nothing.
		change func(*layers)
		// want describes the config entries the prompt adds.
		want func(t *testing.T, added []*agentsession.ConfigEntry)
	}
	cases := []struct {
		name string
		// wrong makes the parts the host names disagree with the
		// request, so the recorder falls back to the string.
		wrong bool
		steps []step
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
				texts, hashes := 0, 0
				for _, p := range added[0].InstructionsParts {
					if p.Text != "" {
						texts++
					} else if p.Hash != "" {
						hashes++
					}
				}
				if texts != 1 || hashes != 2 {
					t.Errorf("delta parts: %d with text, %d by hash; want 1 and 2", texts, hashes)
				}
				// The two unchanged 4000 byte layers are hashes, so the
				// delta costs the memory layer, not the prompt.
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
		name:  "parts that do not join fall back to the string",
		wrong: true,
		steps: []step{{
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
		}},
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := &layers{
				ids:     []string{"product", "agents_md", "memory"},
				text:    map[string]string{"product": big("p"), "agents_md": big("a"), "memory": "Memory:"},
				omitted: []agentsession.OmittedPart{{ID: "skills", Reason: "out of scope", Size: 12}},
			}
			partsOf := func(req openresponses.Request) ([]agentsession.InstructionPart, []agentsession.OmittedPart) {
				parts := l.parts()
				if tc.wrong {
					parts = parts[:2]
				}
				return parts, l.omitted
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
	partsOf := func(openresponses.Request) ([]agentsession.InstructionPart, []agentsession.OmittedPart) {
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
