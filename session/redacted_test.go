package session

import (
	"bytes"
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

// holdsRedacted reports whether e, as written, carries a redacted
// member in any form.
func holdsRedacted(t *testing.T, e agentsession.Entry) bool {
	t.Helper()
	data, err := agentsession.MarshalEntry(e)
	if err != nil {
		t.Fatal(err)
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(data, &members); err != nil {
		t.Fatal(err)
	}
	_, ok := members["redacted"]
	return ok
}

// redactInto redacts orig into a redacted session of its own and
// imports it into store, as a redactor and a publisher do: each entry
// in file order with the secret taken out, its references re-pointed to
// the entries already written, and marked by mark, which a redactor
// does with SetRedacted and its position. A redacted session hashes
// independently, so each response is given the request_hash of the
// request the redacted session rebuilds for it, never the original's.
func redactInto(t *testing.T, store *agentsession.MemoryStore, orig *agentsession.Session, mark func(agentsession.Entry, int) error) *agentsession.Session {
	t.Helper()
	h := agentsession.Header{
		ID:           agentsession.RedactedSessionID(orig.ID(), "test.secret@1"),
		CWD:          orig.Header().CWD,
		Redacted:     true,
		RedactedFrom: &agentsession.RedactedFrom{Session: orig.ID(), Profile: "test.secret@1"},
	}
	red := agentsession.New(h)
	rw := agentsession.NewRewriter(func(s string) string { return strings.ReplaceAll(s, "hunter2", "[redacted]") })
	ids := map[string]string{}
	remap := agentsession.Remap{Entry: func(session, entry string) (string, bool) {
		if session != "" && session != orig.ID() {
			return "", false
		}
		id, ok := ids[entry]
		return id, ok
	}}
	for i, e := range agentsession.FileOrder(orig) {
		body, err := rw.Rewrite(e)
		if err != nil {
			t.Fatalf("rewrite %s: %v", e.Base().ID, err)
		}
		rr, err := agentsession.RemapRefs(body, remap)
		if err != nil {
			t.Fatalf("remap %s: %v", e.Base().ID, err)
		}
		n := rr.Entry
		n.Base().ID = ""
		if err := mark(n, i+1); err != nil {
			t.Fatal(err)
		}
		if r, ok := n.(*agentsession.ResponseEntry); ok {
			if err := red.SetRequestHash(r); err != nil {
				t.Fatalf("request hash for %s: %v", e.Base().ID, err)
			}
		}
		res, err := red.Commit(n)
		if err != nil {
			t.Fatalf("commit %s: %v", e.Base().ID, err)
		}
		ids[e.Base().ID] = res.ID
	}
	var file bytes.Buffer
	if err := agentsession.Write(&file, red); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(file.String(), "hunter2") {
		t.Fatalf("the redaction kept the secret:\n%s", file.String())
	}
	s, err := store.Import(context.Background(), &file, false)
	if err != nil {
		t.Fatalf("import the redacted session: %v", err)
	}
	return s
}

// A redacted session is resumed as any other: the recorder appends its
// run natively, so nothing it writes carries the redacted member, in
// the session or in a child whose env it copies from the redacted env
// in force, and the entries the redaction made keep theirs. The format
// says a writer clears the member on an entry it appends natively from
// one it took from a redacted session; agentsession keeps the member in
// Unknown when it is in a form the format does not define, which is the
// second case. Every response verifies, the redaction's included, since
// a redacted session hashes independently.
func TestResumeARedactedSession(t *testing.T) {
	ctx := context.Background()
	for name, mark := range map[string]func(agentsession.Entry, int) error{
		"redacted entries": func(e agentsession.Entry, pos int) error {
			return agentsession.SetRedacted(e, []agentsession.Span{{From: pos, To: pos}})
		},
		"a redacted member in an undefined form": func(e agentsession.Entry, pos int) error {
			if err := agentsession.SetRedacted(e, []agentsession.Span{{From: pos, To: pos}}); err != nil {
				return err
			}
			if _, ok := e.(*agentsession.EnvEntry); ok {
				if err := agentsession.ClearRedacted(e); err != nil {
					return err
				}
				e.Base().Unknown = map[string]json.RawMessage{"redacted": json.RawMessage(`"all"`)}
			}
			return nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			env := func(context.Context) (*agentsession.EnvEntry, error) {
				e := agentsession.NewEnvEntry("/work")
				if err := e.SetWorkspace(agentsession.WorkspaceContainer, "sandbox").SetMember("node", "node-1"); err != nil {
					return nil, err
				}
				return e, nil
			}
			rec, orig, err := Start(ctx, store, agentsession.Header{CWD: "/work"}, WithEnv(env))
			if err != nil {
				t.Fatal(err)
			}
			cfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m"}
			a := agentturn.New(cfg)
			unsub := rec.Attach(a)
			if _, err := a.Prompt(ctx, openresponses.UserText("my password is hunter2")); err != nil {
				t.Fatal(err)
			}
			unsub()

			red := redactInto(t, store, orig, mark)
			made := red.Len()
			transcript, err := Transcript(red)
			if err != nil {
				t.Fatal(err)
			}

			// Resumed with an env function that says nothing, so the
			// child is where its parent is and is written a copy of the
			// env in force: the redacted session's own.
			unmoved := func(context.Context) (*agentsession.EnvEntry, error) { return nil, nil }
			rec2, s, err := Resume(ctx, store, red.ID(), WithEnv(unmoved))
			if err != nil {
				t.Fatal(err)
			}
			childCfg := agentturn.Config{Name: "specialist", Description: "notes things", Model: &echo.Adapter{}}
			specialist := agent.New(childCfg, agent.WithObserver(rec2.Observe), agent.WithRunContext(rec2.ChildContext))
			cfg.Tools = []agenttool.Tool{specialist}
			b := agentturn.New(cfg, agentturn.WithTranscript(transcript))
			unsub = rec2.Attach(b)
			if _, err := b.Prompt(ctx, openresponses.UserText("delegate")); err != nil {
				t.Fatalf("a run on a redacted session: %v", err)
			}
			unsub()

			back, err := store.Open(ctx, red.ID())
			if err != nil {
				t.Fatal(err)
			}
			if h := back.Header(); !h.Redacted || h.RedactedFrom == nil || h.RedactedFrom.Session != orig.ID() || h.Format != agentsession.Format {
				t.Errorf("header after the resumed run = %+v, want redacted from %s at %s", h, orig.ID(), agentsession.Format)
			}
			entries := s.Entries()
			if len(entries) <= made {
				t.Fatalf("the resumed run appended nothing: %s", entryTypes(s))
			}
			redacted := 0
			for i, e := range entries[:made] {
				if _, ok := agentsession.RedactedOf(e); !ok && !isEnv(e) {
					t.Errorf("entry %d (%s), made by the redaction, is no longer redacted", i, e.EntryType())
				}
				if _, ok := e.(*agentsession.ResponseEntry); ok {
					redacted++
					if err := s.Verify(e.Base().ID); err != nil {
						t.Errorf("verify the redacted response %s: %v", e.Base().ID, err)
					}
				}
			}
			if redacted == 0 {
				t.Errorf("the redaction made no response: %s", entryTypes(s))
			}
			responses := 0
			for _, e := range entries[made:] {
				if holdsRedacted(t, e) {
					t.Errorf("the recorder wrote a redacted member on its %s", e.EntryType())
				}
				if _, ok := e.(*agentsession.ResponseEntry); ok {
					responses++
					if err := s.Verify(e.Base().ID); err != nil {
						t.Errorf("verify the native response %s: %v", e.Base().ID, err)
					}
				}
			}
			if responses == 0 {
				t.Errorf("the resumed run wrote no response: %s", entryTypes(s))
			}

			l := links(s)
			if len(l) != 1 {
				t.Fatalf("links = %d, want the child's", len(l))
			}
			child, err := store.Open(ctx, l[0].Session)
			if err != nil {
				t.Fatal(err)
			}
			if h := child.Header(); h.Redacted || h.RedactedFrom != nil {
				t.Errorf("the child of a redacted session is marked redacted: %+v", h)
			}
			copied := false
			for _, e := range child.Entries() {
				if holdsRedacted(t, e) {
					t.Errorf("the recorder wrote a redacted member on the child's %s", e.EntryType())
				}
				copied = copied || isEnv(e)
			}
			if !copied {
				t.Errorf("the child holds no copy of the env in force: %s", entryTypes(child))
			}
		})
	}
}

// calleeCopy builds a link appended natively, so a redacted member the
// link it copies holds, typed or kept in Unknown, is left behind, as
// target and cancel are, and the members the library does not define
// are kept.
func TestCalleeCopyLeavesRedactedBehind(t *testing.T) {
	for name, mark := range map[string]func(*agentsession.LinkEntry) error{
		"typed": func(l *agentsession.LinkEntry) error {
			return agentsession.SetRedacted(l, []agentsession.Span{{From: 3, To: 3}})
		},
		"undefined form": func(l *agentsession.LinkEntry) error {
			l.Unknown["redacted"] = json.RawMessage(`"all"`)
			return nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			l := agentsession.NewSubsessionLink("child", "call_1")
			l.URL = "https://agent.example/a2a"
			l.Unknown = map[string]json.RawMessage{"x.note": json.RawMessage(`"kept"`)}
			if err := mark(l); err != nil {
				t.Fatal(err)
			}
			c := calleeCopy(l, "")
			if _, ok := agentsession.RedactedOf(c); ok || holdsRedacted(t, c) {
				t.Errorf("the copy carries the redacted member: %+v", c.Unknown)
			}
			if string(c.Unknown["x.note"]) != `"kept"` {
				t.Errorf("the copy lost an extension member: %+v", c.Unknown)
			}
		})
	}
}
