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

// TestAHeldCallIsMarkedAndReadBack pins RFC 0001 The record's rule for
// a batch hold (#299): a call deferred only because another call of
// its batch was asked about is written as a hold marked held, with the
// part of the call the deferral is about, and a reader that seeds
// pending calls from the record reads both back. The asked call reads
// as asked, so a front over a restarted session asks about it alone. It
// is checked on a sidecar session, read with the recorder's read
// options as a host restarting one does, through Pending, AgentOptions
// and the agent they seed.
func TestAHeldCallIsMarkedAndReadBack(t *testing.T) {
	ctx := context.Background()
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(ctx, store, agentsession.Header{Media: agentsession.MediaSidecar, Records: agentsession.AllRecords})
	if err != nil {
		t.Fatal(err)
	}
	run := func(context.Context, echoArgs) (string, error) { return "ran", nil }
	tools := []agenttool.Tool{agenttool.New("ask", "", run), agenttool.New("beside", "", run)}
	decide := func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
		if info.Call.Name == "ask" {
			return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "rule r", By: agentsession.ByPolicy, Subject: "rm -rf build", DeclineText: "the build stays"}, nil
		}
		return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "held beside ask", By: agentsession.ByPolicy, Held: true, Subject: "git status", DeclineText: "status waits on the clean"}, nil
	}
	a := agentturn.New(agentturn.Config{Model: twoCallModel{}, ModelName: "m", Tools: tools, BeforeToolCall: decide})
	unsub := rec.Attach(a)
	// The prompt carries an image, which a sidecar session holds as a
	// blob: reading the record back needs the recorder's read options.
	end, err := a.Prompt(ctx, openresponses.UserMessage(&openresponses.InputText{Text: "clean up"}, &openresponses.InputImage{ImageURL: pngURL(screenshots[0])}))
	if err != nil || end.Reason != agentturn.ReasonInputRequired {
		t.Fatalf("prompt: err=%v end=%+v", err, end)
	}
	unsub()

	read, err := store.Open(ctx, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	// The hold decisions as written.
	holds := map[string]*agentsession.DecisionEntry{}
	for _, e := range read.Entries() {
		if d, ok := e.(*agentsession.DecisionEntry); ok && d.Verdict == agentsession.VerdictHold {
			holds[d.CallID] = d
		}
	}
	names := map[string]string{}
	for _, item := range end.Items {
		if c, ok := item.(*openresponses.FunctionCall); ok {
			names[c.Name] = c.CallID
		}
	}
	if d := holds[names["beside"]]; d == nil || string(d.Unknown[HeldMember]) != "true" || string(d.Unknown[SubjectMember]) != `"git status"` || string(d.Unknown[DeclineTextMember]) != `"status waits on the clean"` {
		t.Errorf("held call's hold = %+v, want it marked held with its subject and decline text", d)
	}
	if d := holds[names["ask"]]; d == nil || d.Unknown[HeldMember] != nil || string(d.Unknown[SubjectMember]) != `"rm -rf build"` || string(d.Unknown[DeclineTextMember]) != `"the build stays"` {
		t.Errorf("asked call's hold = %+v, want it unmarked with its subject and decline text", d)
	}

	check := func(where string, pending []agentturn.PendingCall) {
		t.Helper()
		got := map[string]*agentturn.ToolDecision{}
		for _, p := range pending {
			if p.Reason != agentturn.PendingDeferred {
				t.Errorf("%s: call %s is %s, want deferred", where, p.Call.Name, p.Reason)
			}
			got[p.Call.Name] = p.Decision
		}
		// The decline text survives, so a child reopened from its
		// record still refuses in the host's words (#304).
		if d := got["beside"]; d == nil || !d.Held || d.Subject != "git status" || d.Reason != "held beside ask" || d.DeclineText != "status waits on the clean" {
			t.Errorf("%s: held call's decision = %+v", where, d)
		}
		if d := got["ask"]; d == nil || d.Held || d.Subject != "rm -rf build" || d.Reason != "rule r" || d.DeclineText != "the build stays" {
			t.Errorf("%s: asked call's decision = %+v", where, d)
		}
	}
	pending, err := Pending(read, rec.ReadOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	check("Pending", pending)
	if _, err := AgentOptions(read); !errors.Is(err, ErrNoBlobReader) {
		t.Fatalf("AgentOptions without the read options = %v, want ErrNoBlobReader: the image is not held as a blob", err)
	}
	opts, err := AgentOptions(read, rec.ReadOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	seeded := agentturn.New(agentturn.Config{Model: twoCallModel{}, ModelName: "m", Tools: tools, BeforeToolCall: decide}, opts...)
	check("the seeded agent", seeded.State().Pending)

	// A recorder resumed on the session reads them the same way.
	rec2, s2, err := Resume(ctx, store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	pending, err = Pending(s2, rec2.ReadOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	check("after Resume", pending)
}
