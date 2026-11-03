package session

import (
	"context"
	"errors"
	"testing"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// trimTools leaves the function calls and their outputs out of t, as a
// handoff's input filter that removes every tool item does.
func trimTools(t agentturn.Transcript) agentturn.Transcript {
	var out agentturn.Transcript
	for _, item := range t {
		switch item.(type) {
		case *openresponses.FunctionCall, *openresponses.FunctionCallOutput:
			continue
		}
		out = append(out, item)
	}
	return out
}

// clearOutputs sends every function call output of t as one
// placeholder text, as a host that clears old tool results does.
func clearOutputs(t agentturn.Transcript) agentturn.Transcript {
	out := make(agentturn.Transcript, len(t))
	for i, item := range t {
		out[i] = item
		if o, ok := item.(*openresponses.FunctionCallOutput); ok {
			sent := *o
			sent.Output = openresponses.FunctionCallOutputData{Text: "[cleared]"}
			out[i] = &sent
		}
	}
	return out
}

// dropImages sends every image part of t as one placeholder text, as a
// host that prunes old screenshots does.
func dropImages(t agentturn.Transcript) agentturn.Transcript {
	out := make(agentturn.Transcript, len(t))
	for i, item := range t {
		out[i] = item
		m, ok := item.(*openresponses.Message)
		if !ok {
			continue
		}
		sent := *m
		sent.Content = append(openresponses.Contents(nil), m.Content...)
		for k, p := range sent.Content {
			if _, ok := p.(*openresponses.InputImage); ok {
				sent.Content[k] = &openresponses.InputText{Text: "[image]"}
				out[i] = &sent
			}
		}
	}
	return out
}

// dropOlderImages sends every image part of t but the last as one
// placeholder text, as a host that keeps only the latest screenshot
// does.
func dropOlderImages(t agentturn.Transcript) agentturn.Transcript {
	last := -1
	for i, item := range t {
		if m, ok := item.(*openresponses.Message); ok {
			for _, p := range m.Content {
				if _, ok := p.(*openresponses.InputImage); ok {
					last = i
				}
			}
		}
	}
	out := make(agentturn.Transcript, len(t))
	for i, item := range t {
		out[i] = item
		m, ok := item.(*openresponses.Message)
		if !ok {
			continue
		}
		sent := *m
		sent.Content = append(openresponses.Contents(nil), m.Content...)
		images := 0
		for _, p := range m.Content {
			if _, ok := p.(*openresponses.InputImage); ok {
				images++
			}
		}
		for k, p := range sent.Content {
			if _, ok := p.(*openresponses.InputImage); ok && (i != last || images > 1) {
				sent.Content[k] = &openresponses.InputText{Text: "[image]"}
				out[i] = &sent
				images--
			}
		}
	}
	return out
}

// rewrite changes the text of the first user message of t, which the
// session format cannot describe.
func rewrite(t agentturn.Transcript) agentturn.Transcript {
	out := append(agentturn.Transcript(nil), t...)
	for i, item := range out {
		if m, ok := item.(*openresponses.Message); ok && m.Role == openresponses.RoleUser {
			out[i] = openresponses.UserText("said otherwise")
			break
		}
	}
	return out
}

// TestATrimTheFormatDescribesKeepsEveryResponseVerifiable pins #233: a
// host that trims the transcript between runs, as a handoff's input
// filter does, sends an input that differs from what the path holds by
// item entries left out, outputs sent as a placeholder or image parts
// sent as one, and the recorder writes the omit that describes it
// before the response, so every response after the trim verifies, as
// it does across a restart. An edit the format cannot describe still
// costs the hash. The shape is the framework-patterns handoff with
// remove_all_tools.
func TestATrimTheFormatDescribesKeepsEveryResponseVerifiable(t *testing.T) {
	cases := []struct {
		name string
		trim func(agentturn.Transcript) agentturn.Transcript
		// check says whether the omit in force at the leaf describes the
		// trim.
		check func(agentsession.Omit) bool
		// unhashed is how many responses the record cannot rebuild.
		unhashed int
		restart  bool
		// keep restarts without trimming again: the agent the record
		// seeds holds the trim.
		keep bool
	}{
		{name: "the tools' items left out", trim: trimTools,
			check: func(o agentsession.Omit) bool { return len(o.Items) == 2 && o.Outputs == nil && o.Images == nil }},
		{name: "outputs cleared", trim: clearOutputs,
			check: func(o agentsession.Omit) bool {
				return len(o.Items) == 0 && o.Outputs != nil && o.Outputs.Text == "[cleared]" && len(o.Outputs.Items) == 1
			}},
		{name: "images pruned", trim: dropImages,
			check: func(o agentsession.Omit) bool {
				return len(o.Items) == 0 && o.Images != nil && o.Images.Keep == 0 && o.Images.Placeholder == "[image]"
			}},
		// The second trim leaves out the tools' items of both runs.
		{name: "older images pruned, the last kept", trim: dropOlderImages,
			check: func(o agentsession.Omit) bool {
				return o.Images != nil && o.Images.Keep == 1 && o.Images.Placeholder == "[image]"
			}},
		{name: "a restart keeps older images pruned", trim: dropOlderImages, restart: true, keep: true,
			check: func(o agentsession.Omit) bool { return o.Images != nil && o.Images.Keep == 1 }},
		{name: "a restart after the trim", trim: trimTools, restart: true,
			check: func(o agentsession.Omit) bool { return len(o.Items) == 4 }},
		{name: "a restart keeps the tools' items left out", trim: trimTools, restart: true, keep: true,
			check: func(o agentsession.Omit) bool { return len(o.Items) == 2 && !o.IsZero() }},
		{name: "a restart keeps outputs cleared", trim: clearOutputs, restart: true, keep: true,
			check: func(o agentsession.Omit) bool { return o.Outputs != nil && len(o.Outputs.Items) == 1 }},
		{name: "a restart keeps images pruned", trim: dropImages, restart: true, keep: true,
			check: func(o agentsession.Omit) bool { return o.Images != nil && o.Images.Keep == 0 }},
		{name: "an edit the format cannot describe", trim: rewrite, unhashed: 2,
			check: func(o agentsession.Omit) bool { return o.IsZero() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(ctx, store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			cfg := agentturn.Config{Model: &reasonsAndCalls{}, ModelName: "m", Tools: []agenttool.Tool{upper}}
			a := agentturn.New(cfg)
			unsub := rec.Attach(a)
			defer func() { unsub() }()
			first := openresponses.UserMessage(
				&openresponses.InputText{Text: "look"},
				&openresponses.InputImage{ImageURL: "data:image/png;base64,iVBORw0KGgo="},
				&openresponses.InputImage{ImageURL: "data:image/png;base64,iVBORw0KGgp="},
			)
			if _, err := a.Prompt(ctx, first); err != nil {
				t.Fatal(err)
			}
			// The host trims what the next agent sees, between runs.
			if err := a.SetTranscript(tc.trim(a.State().Transcript)); err != nil {
				t.Fatal(err)
			}
			if _, err := a.Prompt(ctx, openresponses.UserText("again")); err != nil {
				t.Fatal(err)
			}
			if tc.restart {
				unsub()
				rec2, s2, err := Resume(ctx, store, s.ID())
				if err != nil {
					t.Fatal(err)
				}
				opts, err := AgentOptions(s2)
				if err != nil {
					t.Fatal(err)
				}
				b := agentturn.New(cfg, opts...)
				if !tc.keep {
					// Trimming again changes nothing more than the
					// trim the agent was seeded with.
					if err := b.SetTranscript(tc.trim(b.State().Transcript)); err != nil {
						t.Fatal(err)
					}
				}
				unsub = rec2.Attach(b)
				if _, err := b.Prompt(ctx, openresponses.UserText("after")); err != nil {
					t.Fatal(err)
				}
				s = s2
			}
			verifyAllUnhashed(t, s, tc.unhashed)
			if n := unhashedEntries(s); n != min(tc.unhashed, 1) {
				t.Errorf("%d unhashed entries, want %d: %s", n, min(tc.unhashed, 1), entryTypes(s))
			}
			cx, err := s.Context()
			if err != nil {
				t.Fatal(err)
			}
			if tc.keep {
				for _, c := range configs(s)[1:] {
					if c.Replace {
						t.Errorf("a replace after the restart undid the trim: %s", entryTypes(s))
					}
				}
			}
			if !tc.check(cx.Settings.Omit) {
				t.Errorf("omit in force = %+v, outputs %+v, images %+v", cx.Settings.Omit, cx.Settings.Omit.Outputs, cx.Settings.Omit.Images)
			}
		})
	}
}

// TestAnItemLeftOutAndSentAgainIsAReplace pins the other half of #233's
// rule: a transform that leaves one call's items out of a prompt's
// requests and another's out of the next, sending the first again,
// sends what no delta can describe, since a delta only adds to the
// items left out, and the recorder writes a replace that carries the
// omit setting whole; one that sends everything again clears it with
// an empty omit. Every response verifies.
func TestAnItemLeftOutAndSentAgainIsAReplace(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	trim := ""
	cfg := agentturn.Config{
		Model: &reasonsAndCalls{}, ModelName: "m", Tools: []agenttool.Tool{upper}, MaxTurns: 4,
		Transform: func(_ context.Context, t agentturn.Transcript) (agentturn.Transcript, error) {
			if trim == "" {
				return t, nil
			}
			var out agentturn.Transcript
			for _, item := range t {
				switch v := item.(type) {
				case *openresponses.FunctionCall:
					if v.CallID == trim {
						continue
					}
				case *openresponses.FunctionCallOutput:
					if v.CallID == trim {
						continue
					}
				}
				out = append(out, item)
			}
			return out, nil
		},
	}
	a := agentturn.New(cfg)
	defer rec.Attach(a)()
	// reasonsAndCalls names the call of its n-th request call_n.
	for _, step := range []string{"", "call_1", "call_3", ""} {
		trim = step
		if end, err := a.Prompt(ctx, openresponses.UserText("go")); err != nil || end.Reason != agentturn.ReasonDone {
			t.Fatalf("prompt: err=%v end=%+v", err, end)
		}
	}
	verifyAll(t, s)
	replaces := 0
	for _, c := range configs(s)[1:] {
		if c.Replace {
			replaces++
		}
	}
	if replaces != 1 {
		t.Errorf("replaces after the first config = %d, want 1: %s", replaces, entryTypes(s))
	}
	cx, err := s.Context()
	if err != nil {
		t.Fatal(err)
	}
	if !cx.Settings.Omit.IsZero() {
		t.Errorf("omit in force after the items were sent again = %+v", cx.Settings.Omit)
	}
}

// TestAHostNamesWhatItTrimmed pins Recorder.Omit, the host's way of
// naming the entries it trimmed rather than leaving the recorder to find
// them: the config entry is written when the host says so, between the
// runs, the next run's requests match it without another, and an ID
// that names no item entry of the context is refused.
func TestAHostNamesWhatItTrimmed(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &reasonsAndCalls{}, ModelName: "m", Tools: []agenttool.Tool{upper}})
	defer rec.Attach(a)()
	if _, err := a.Prompt(ctx, openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, item := range a.State().Transcript {
		switch item.(type) {
		case *openresponses.FunctionCall, *openresponses.FunctionCallOutput:
			id, ok := rec.EntryOf(ctx, item)
			if !ok {
				t.Fatalf("no entry for %s", item.ItemType())
			}
			ids = append(ids, id)
		}
	}
	if err := rec.Omit(ctx, "no-such-entry"); !errors.Is(err, ErrNotOmittable) {
		t.Errorf("omit of an unknown entry: %v, want ErrNotOmittable", err)
	}
	before := len(configs(s))
	if err := rec.Omit(ctx, ids...); err != nil {
		t.Fatal(err)
	}
	if got := configs(s)[before:]; len(got) != 1 || got[0].Omit == nil || len(got[0].Omit.Items) != 2 {
		t.Fatalf("Omit wrote %d config entries: %s", len(got), entryTypes(s))
	}
	if err := a.SetTranscript(trimTools(a.State().Transcript)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Prompt(ctx, openresponses.UserText("again")); err != nil {
		t.Fatal(err)
	}
	if n := len(configs(s)); n != before+1 {
		t.Errorf("the run after Omit wrote %d more config entries: %s", n-before-1, entryTypes(s))
	}
	verifyAll(t, s)
}
