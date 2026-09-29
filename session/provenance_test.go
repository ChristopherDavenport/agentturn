package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

func customs(s *agentsession.Session, ns string) []*agentsession.CustomEntry {
	var out []*agentsession.CustomEntry
	for _, e := range s.Entries() {
		if c, ok := e.(*agentsession.CustomEntry); ok && c.NS == ns {
			out = append(out, c)
		}
	}
	return out
}

func decisions(s *agentsession.Session) []*agentsession.DecisionEntry {
	var out []*agentsession.DecisionEntry
	for _, e := range s.Entries() {
		if d, ok := e.(*agentsession.DecisionEntry); ok {
			out = append(out, d)
		}
	}
	return out
}

// TestAllowReasonIsRecorded pins #130: a hook that allows a call and
// says why leaves a proceed decision carrying the reason, and one that
// says nothing leaves nothing.
func TestAllowReasonIsRecorded(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reason string
		want   int
	}{
		{"a reason is a proceed", "granted by skill deploy", 1},
		{"no reason is nothing", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{upper},
				BeforeToolCall: func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
					return &agentturn.ToolDecision{Reason: tc.reason}, nil
				}})
			defer rec.Attach(a)()
			if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
				t.Fatal(err)
			}
			ds := decisions(s)
			if len(ds) != tc.want {
				t.Fatalf("decisions = %+v", ds)
			}
			if tc.want == 1 && (ds[0].Verdict != agentsession.VerdictProceed || ds[0].Reason != tc.reason || ds[0].By != agentsession.ByPolicy || ds[0].Args != nil) {
				t.Errorf("decision = %+v", ds[0])
			}
			verifyAll(t, s)
		})
	}
}

// TestGuardStopRecordsItsError pins #110: a guard's error follows the
// cause in the run end's ref; the other causes keep the cause alone.
func TestGuardStopRecordsItsError(t *testing.T) {
	for _, tc := range []struct {
		name string
		stop func(context.Context, agentturn.TurnInfo) (bool, error)
		want string
	}{
		{"guard", func(context.Context, agentturn.TurnInfo) (bool, error) {
			return false, fmt.Errorf("%w: budget of $1 spent", agentturn.ErrGuard)
		}, "guard: agentturn: guard stopped the run: budget of $1 spent"},
		{"hook", func(context.Context, agentturn.TurnInfo) (bool, error) { return true, nil }, "hook"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ShouldStopAfterTurn: tc.stop})
			defer rec.Attach(a)()
			if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
				t.Fatal(err)
			}
			runs := runsOf(t, s)
			if len(runs) != 1 || runs[0].End == nil || runs[0].End.Ref != tc.want {
				t.Errorf("run end = %+v, want ref %q", runs[0].End, tc.want)
			}
		})
	}
}

// flaky fails its first n calls with a 503 and then answers.
type flaky struct{ fails *atomic.Int32 }

func (m flaky) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	if m.fails.Add(-1) >= 0 {
		return openresponses.ServerError("overloaded", "try again")
	}
	return (&echo.Adapter{}).CreateStream(ctx, req, sink)
}

// TestModelRetryIsRecorded pins #117: every failed attempt that is
// tried again leaves a model_retry record before the response of the
// one that answered, saying whether Revise changed the request, and
// the hashes still verify.
func TestModelRetryIsRecorded(t *testing.T) {
	for _, tc := range []struct {
		name   string
		model  agentturn.Model
		revise func(int, *openresponses.Request, error) *openresponses.Request
		want   []ModelRetry
	}{{
		name:  "the same request twice",
		model: flaky{fails: func() *atomic.Int32 { var n atomic.Int32; n.Store(2); return &n }()},
		want:  []ModelRetry{{Attempt: 1, Model: "a"}, {Attempt: 2, Model: "a"}},
	}, {
		name:  "a fallback to another model",
		model: switching{primary: "a"},
		revise: func(_ int, req *openresponses.Request, _ error) *openresponses.Request {
			req.Model = "b"
			return nil
		},
		want: []ModelRetry{{Attempt: 1, Model: "a", Revised: true}},
	}, {
		name:  "a revision that edits a map in place",
		model: flaky{fails: func() *atomic.Int32 { var n atomic.Int32; n.Store(1); return &n }()},
		revise: func(_ int, req *openresponses.Request, _ error) *openresponses.Request {
			req.Metadata["route"] = "fallback"
			return nil
		},
		want: []ModelRetry{{Attempt: 1, Model: "a", Revised: true}},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(agentturn.Config{Model: tc.model, ModelName: "a", Request: openresponses.Request{Metadata: map[string]string{"route": "primary"}}, Retry: agentturn.Retry{
				MaxAttempts: 3,
				Backoff:     func(int, error) time.Duration { return 7 * time.Millisecond },
				Revise:      tc.revise,
			}})
			defer rec.Attach(a)()
			if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
				t.Fatal(err)
			}
			got := customs(s, ModelRetryNS)
			if len(got) != len(tc.want) {
				t.Fatalf("retry records = %d in %q", len(got), entryTypes(s))
			}
			for i, c := range got {
				var r ModelRetry
				if err := json.Unmarshal(c.Data, &r); err != nil {
					t.Fatal(err)
				}
				w := tc.want[i]
				if r.Attempt != w.Attempt || r.Model != w.Model || r.Revised != w.Revised || r.DelayMS != 7 || r.Error == "" {
					t.Errorf("retry %d = %+v, want %+v", i, r, w)
				}
			}
			// The records precede the response of the attempt that
			// answered, and only one config per turn is written.
			types := entryTypes(s)
			if i, j := strings.Index(types, "custom"), strings.Index(types, "response"); i < 0 || i > j {
				t.Errorf("entries = %q", types)
			}
			if n := verifyAll(t, s); n != 1 {
				t.Errorf("responses = %d", n)
			}
		})
	}
}

// TestResponseCarriesAttempts pins the recorder's half of agentsession
// #93: the response entry of a call Retry tried again carries the
// calls it took, the one that answered or failed last included, and a
// call that took one leaves the member off. An abort during the
// backoff counts the attempt that was due, since the loop reports the
// retry before the delay and nothing after it until the attempt
// streams.
func TestResponseCarriesAttempts(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fails int32
		// prompts is how many prompts the case sends; the fails fall on
		// the first.
		prompts int
		failed  bool
		// abort cancels the run in the first backoff.
		abort bool
		want  []int
	}{
		{name: "no retry", fails: 0, prompts: 1, want: []int{0}},
		{name: "two failures then an answer", fails: 2, prompts: 1, want: []int{3}},
		{name: "the count is per call", fails: 1, prompts: 2, want: []int{2, 0}},
		{name: "retries exhausted", fails: 5, prompts: 1, failed: true, want: []int{3}},
		{name: "aborted in the backoff", fails: 1, prompts: 1, failed: true, abort: true, want: []int{2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			var n atomic.Int32
			n.Store(tc.fails)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			a := agentturn.New(agentturn.Config{Model: flaky{fails: &n}, ModelName: "a", Retry: agentturn.Retry{
				MaxAttempts: 3,
				Backoff: func(int, error) time.Duration {
					if tc.abort {
						cancel()
						return time.Hour
					}
					return 0
				},
			}})
			defer rec.Attach(a)()
			for i := range tc.prompts {
				end, err := a.Prompt(ctx, openresponses.UserText("go"))
				if err == nil && end.Reason == agentturn.ReasonAborted {
					err = context.Canceled
				}
				if (err != nil) != tc.failed {
					t.Fatalf("prompt %d: %v", i, err)
				}
			}
			var got []int
			for _, e := range s.Entries() {
				if r, ok := e.(*agentsession.ResponseEntry); ok {
					got = append(got, r.Attempts)
					if (r.Status == openresponses.ResponseStatusFailed) != tc.failed {
						t.Errorf("response status = %s", r.Status)
					}
				}
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("attempts = %v, want %v", got, tc.want)
			}
			verifyAll(t, s)
		})
	}
}

// TestSetConfigSettlesBeforeTheRunsItems pins #109: a configuration
// set between runs is on the path before the items its BeforeTurn
// appends, and neither an unchanged configuration nor one whose hook
// edits the request every turn writes a config at run start.
func TestSetConfigSettlesBeforeTheRunsItems(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	// The first configuration edits its instructions in a hook, as a
	// memory block does, so its base request never matches what is
	// sent.
	triage := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Instructions: "triage",
		BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
			req.Instructions = "triage, with memory"
			return nil
		}}
	a := agentturn.New(triage)
	defer rec.Attach(a)()
	prompt := func() string {
		t.Helper()
		before := len(s.Entries())
		if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
			t.Fatal(err)
		}
		var types []string
		for _, e := range s.Entries()[before:] {
			types = append(types, e.EntryType())
			if it, ok := e.(*agentsession.ItemEntry); ok {
				if m, ok := it.Item.(*openresponses.Message); ok {
					types[len(types)-1] = string(m.Role)
				}
			}
		}
		return strings.Join(types, " ")
	}
	if got := prompt(); got != "run config user config assistant response run" {
		t.Errorf("first run = %q", got)
	}
	if got := prompt(); got != "run user assistant response run" {
		t.Errorf("an unchanged configuration = %q", got)
	}
	billing := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Instructions: "billing",
		BeforeTurn: func(_ context.Context, info agentturn.TurnStartInfo) (openresponses.Items, error) {
			if info.Turn == 1 {
				return openresponses.Items{openresponses.DeveloperText("Transferred from triage.")}, nil
			}
			return nil, nil
		}}
	if err := a.SetConfig(billing); err != nil {
		t.Fatal(err)
	}
	if got := prompt(); got != "run config user developer assistant response run" {
		t.Errorf("after SetConfig = %q", got)
	}
	if got := prompt(); got != "run user developer assistant response run" {
		t.Errorf("the same configuration again = %q", got)
	}
	cx, err := s.ContextAt(func() string {
		for _, e := range s.Entries() {
			if it, ok := e.(*agentsession.ItemEntry); ok {
				if m, ok := it.Item.(*openresponses.Message); ok && m.Role == openresponses.RoleDeveloper {
					return e.Base().ID
				}
			}
		}
		return ""
	}())
	if err != nil {
		t.Fatal(err)
	}
	if cx.Settings.Instructions != "billing" {
		t.Errorf("the transfer note is filed under %q", cx.Settings.Instructions)
	}
	verifyAll(t, s)
}

// TestSeededWriterSettlesBeforeTheRunsItems pins #139: a writer seeded
// from a path, by Resume or by Start on a based header, compares its
// first run's configuration with the base the path's last run start
// recorded, so a new configuration's BeforeTurn items are filed under
// it and an unchanged one writes nothing at run start, even when a
// hook edits the request. A path that recorded no base, as one written
// through Handle without a configuration, is compared by its settings
// at the leaf.
func TestSeededWriterSettlesBeforeTheRunsItems(t *testing.T) {
	triage := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Instructions: "triage"}
	// memory edits its instructions in a hook, so its base request
	// never matches the leaf: only a path with no recorded base pays a
	// delta at run start for it, and the edited request its own at
	// turn start.
	memory := triage
	memory.BeforeModelCall = func(_ context.Context, req *openresponses.Request) error {
		req.Instructions = "triage, with memory"
		return nil
	}
	tooled := triage
	tooled.Tools = []agenttool.Tool{upper}
	billing := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Instructions: "billing",
		BeforeTurn: func(_ context.Context, info agentturn.TurnStartInfo) (openresponses.Items, error) {
			if info.Turn == 1 {
				return openresponses.Items{openresponses.DeveloperText("Transferred from triage.")}, nil
			}
			return nil, nil
		}}
	resume := func(t *testing.T, store agentsession.Store, origin *agentsession.Session) (*Recorder, *agentsession.Session) {
		rec, s, err := Resume(context.Background(), store, origin.ID())
		if err != nil {
			t.Fatal(err)
		}
		return rec, s
	}
	fork := func(t *testing.T, store agentsession.Store, origin *agentsession.Session) (*Recorder, *agentsession.Session) {
		rec, s, err := Start(context.Background(), store, agentsession.Header{Base: origin.Leaf(), ParentSession: origin.ID()})
		if err != nil {
			t.Fatal(err)
		}
		return rec, s
	}
	cases := []struct {
		name        string
		first, next agentturn.Config
		// unrecorded writes the first run through Handle with no
		// configuration, so its run start records no base.
		unrecorded bool
		seed       func(*testing.T, agentsession.Store, *agentsession.Session) (*Recorder, *agentsession.Session)
		want       string
		filedUnder string
	}{
		{"resume under a new configuration", triage, billing, false, resume, "run config user developer assistant response run", "billing"},
		{"fork under a new configuration", triage, billing, false, fork, "run config user developer assistant response run", "billing"},
		{"resume under the same configuration", triage, triage, false, resume, "run user assistant response run", ""},
		{"fork under the same configuration with tools", tooled, tooled, false, fork, "run user item response dispatch item assistant response run", ""},
		{"resume under a hook that edits the request", memory, memory, false, resume, "run user assistant response run", ""},
		{"resume with no recorded base under a new configuration", triage, billing, true, resume, "run config user developer assistant response run", "billing"},
		{"resume with no recorded base under the same configuration", triage, triage, true, resume, "run user assistant response run", ""},
		{"resume with no recorded base under a hook that edits the request", memory, memory, true, resume, "run config user config assistant response run", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, origin, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(tc.first)
			var unsub func()
			if tc.unrecorded {
				unsub = a.Subscribe(rec.Handle)
			} else {
				unsub = rec.Attach(a)
			}
			if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
				t.Fatal(err)
			}
			unsub()
			for _, e := range origin.Entries() {
				if run, ok := e.(*agentsession.RunEntry); ok && run.IsStart() {
					raw, err := agentsession.MarshalEntry(run)
					if err != nil {
						t.Fatal(err)
					}
					if got := strings.Contains(string(raw), `"`+ConfigBaseMember+`":"sha256:`); got == tc.unrecorded {
						t.Errorf("run start = %s", raw)
					}
				}
			}

			rec2, s := tc.seed(t, store, origin)
			cx, err := s.Context()
			if err != nil {
				t.Fatal(err)
			}
			b := agentturn.New(tc.next, agentturn.WithTranscript(cx.Items))
			defer rec2.Attach(b)()
			before := len(s.Entries())
			if _, err := b.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
				t.Fatal(err)
			}
			var types []string
			note := ""
			for _, e := range s.Entries()[before:] {
				types = append(types, e.EntryType())
				if it, ok := e.(*agentsession.ItemEntry); ok {
					if m, ok := it.Item.(*openresponses.Message); ok {
						types[len(types)-1] = string(m.Role)
						if m.Role == openresponses.RoleDeveloper {
							note = e.Base().ID
						}
					}
				}
			}
			if got := strings.Join(types, " "); got != tc.want {
				t.Errorf("seeded run = %q, want %q", got, tc.want)
			}
			if tc.filedUnder != "" {
				at, err := s.ContextAt(note)
				if err != nil {
					t.Fatal(err)
				}
				if at.Settings.Instructions != tc.filedUnder {
					t.Errorf("the transfer note is filed under %q, want %q", at.Settings.Instructions, tc.filedUnder)
				}
			}
			verifyAll(t, s)
		})
	}
}

// TestEnvUnknownMembersAreCompared pins #128: an env entry that differs
// only in a member the library does not define is a change, and one
// that is the same, unknown members included, writes nothing.
func TestEnvUnknownMembersAreCompared(t *testing.T) {
	boot := "boot-1"
	env := func(context.Context) (*agentsession.EnvEntry, error) {
		e := &agentsession.EnvEntry{CWD: "/work"}
		e.Unknown = map[string]json.RawMessage{"container_boot": json.RawMessage(`"` + boot + `"`)}
		return e, nil
	}
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{}, WithEnv(env))
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}})
	defer rec.Attach(a)()
	count := func() int {
		n := 0
		for _, e := range s.Entries() {
			if _, ok := e.(*agentsession.EnvEntry); ok {
				n++
			}
		}
		return n
	}
	for i, want := range []int{1, 1, 2} {
		if i == 2 {
			boot = "boot-2"
		}
		if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
			t.Fatal(err)
		}
		if got := count(); got != want {
			t.Errorf("after run %d: %d env entries, want %d", i+1, got, want)
		}
	}
}

// TestEnvWorkspaceIsComparedCanonically checks that a workspace whose
// members are the same in canonical form writes nothing, however its
// host encoded them, and that one member changing is a new env entry.
func TestEnvWorkspaceIsComparedCanonically(t *testing.T) {
	var instance string
	env := func(context.Context) (*agentsession.EnvEntry, error) {
		ws := &agentsession.Workspace{Kind: "container", Ref: "sha256:abc"}
		ws.Unknown = map[string]json.RawMessage{"instance": json.RawMessage(instance)}
		return &agentsession.EnvEntry{CWD: "/work", Workspace: ws}, nil
	}
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{}, WithEnv(env))
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}})
	defer rec.Attach(a)()
	for i, run := range []struct {
		instance string
		want     int
	}{
		{`{"id":"i-1","zone":"a"}`, 1},
		{`{"zone": "a", "id": "i-1"}`, 1},
		{`{"id":"i-2","zone":"a"}`, 2},
	} {
		instance = run.instance
		if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, e := range s.Entries() {
			if _, ok := e.(*agentsession.EnvEntry); ok {
				n++
			}
		}
		if n != run.want {
			t.Errorf("after run %d: %d env entries, want %d", i+1, n, run.want)
		}
	}
}

// TestEntryOfNamesTheItemsEntry pins #124: a subscriber registered
// before the recorder finds the entry of every item the run appended
// once the recorder has written it, whatever the order.
func TestEntryOfNamesTheItemsEntry(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: allCalls{}, Tools: []agenttool.Tool{upper}})
	found := map[string]openresponses.Item{}
	a.Subscribe(func(ctx context.Context, ev agentturn.Event) error {
		if e, ok := ev.(*agentturn.RunEnd); ok {
			for _, item := range e.Items {
				id, ok := rec.EntryOf(ctx, item)
				if !ok {
					return fmt.Errorf("no entry for %s", item.ItemType())
				}
				found[id] = item
			}
		}
		return nil
	})
	defer rec.Attach(a)()
	end, err := a.Prompt(context.Background(), openresponses.UserText("go"))
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != len(end.Items) {
		t.Fatalf("found %d entries for %d items", len(found), len(end.Items))
	}
	for id, item := range found {
		e, ok := s.Entry(id)
		if !ok {
			t.Fatalf("no entry %s", id)
		}
		if it, ok := e.(*agentsession.ItemEntry); !ok || it.Item != item {
			t.Errorf("entry %s holds %+v", id, e)
		}
	}
	if _, ok := rec.EntryOf(context.Background(), openresponses.UserText("never written")); ok {
		t.Error("an item the recorder did not write has an entry")
	}
}

// TestRunStartCarriesTheTriggerInParts pins the recorder half of
// agentsession #82: the run entry carries the trigger's members apart
// beside the joined ref.
func TestRunStartCarriesTheTriggerInParts(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}})
	defer rec.Attach(a)()
	ctx := agentturn.ContextWithTrigger(context.Background(), agentturn.Trigger{Kind: "cron", Ref: "nightly", Source: "scheduler"})
	if _, err := a.Prompt(ctx, openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Prompt(context.Background(), openresponses.UserText("again")); err != nil {
		t.Fatal(err)
	}
	runs := runsOf(t, s)
	start := runs[0].Start
	if start.Ref != "cron:nightly" || !start.Trigger.Equal(&agentsession.Trigger{Kind: "cron", Ref: "nightly", Source: "scheduler"}) {
		t.Errorf("run start = %+v trigger %+v", start, start.Trigger)
	}
	if runs[1].Start.Trigger != nil || runs[1].Start.Ref != "" {
		t.Errorf("a run with no trigger = %+v", runs[1].Start)
	}
}

// TestTriggerExtraReachesTheTrigger pins #146 under format 0.8: a
// firing's own facts, its due time and attempt, are members of the run
// start's trigger object, where agentsession #101 put them, not of the
// run entry; the prompt's item carries the same trigger as its source;
// a name the run entry has is the trigger's to use; and one of the
// trigger's own three is refused before anything of the run is
// written.
func TestTriggerExtraReachesTheTrigger(t *testing.T) {
	cases := []struct {
		name    string
		extra   map[string]any
		wantErr bool
	}{
		{"facts", map[string]any{"due_at": "2026-09-29T03:00:00Z", "attempt": 2}, false},
		{"a run entry member", map[string]any{"run_id": "run_7", ConfigBaseMember: "sha256:0"}, false},
		{"kind", map[string]any{"kind": "forged"}, true},
		{"source", map[string]any{"source": "forged"}, true},
		{"not JSON", map[string]any{"due_at": func() {}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}})
			defer rec.Attach(a)()
			trigger := agentturn.Trigger{Kind: "cron", Ref: "nightly", Source: "scheduler", Extra: tc.extra}
			prompt := openresponses.UserText("go")
			_, err = a.Prompt(agentturn.ContextWithTrigger(context.Background(), trigger), prompt)
			if tc.wantErr {
				if !errors.Is(err, agentturn.ErrTriggerExtra) {
					t.Fatalf("err = %v, want ErrTriggerExtra", err)
				}
				if leaf := s.Leaf(); leaf != "" {
					t.Errorf("a refused run start left entry %s", leaf)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := &agentsession.Trigger{Kind: "cron", Ref: "nightly", Source: "scheduler"}
			for name, v := range tc.extra {
				if err := want.SetMember(name, v); err != nil {
					t.Fatal(err)
				}
			}
			start := runsOf(t, s)[0].Start
			if !start.Trigger.Equal(want) {
				t.Errorf("run start trigger = %+v, want %+v", start.Trigger, want)
			}
			for name := range tc.extra {
				if _, ok := start.Unknown[name]; ok && name != ConfigBaseMember {
					t.Errorf("run start carries %q as a member of its own: %s", name, start.Unknown)
				}
			}
			if start.RunID == "run_7" {
				t.Errorf("the trigger's run_id reached the run entry")
			}
			id, ok := rec.EntryOf(context.Background(), prompt)
			if !ok {
				t.Fatal("the prompt has no entry")
			}
			e, _ := s.Entry(id)
			if it, ok := e.(*agentsession.ItemEntry); !ok || !it.Source.Equal(start.Trigger) {
				t.Errorf("prompt entry = %+v", e)
			}
			verifyAll(t, s)
		})
	}
}

// TestQueuedTriggerKeepsItsExtra pins #146 and agentsession #101 for a
// queued input: a firing queued behind a busy run keeps its due time
// and attempt on the queued entry, through a crash and Requeue, and
// on the item that drains it, the same object each time.
func TestQueuedTriggerKeepsItsExtra(t *testing.T) {
	cases := []struct {
		name string
		// queue queues the item on a with ctx, through the recorder or
		// the agent.
		queue func(ctx context.Context, rec *Recorder, a *agentturn.Agent, item openresponses.Item) error
	}{
		{"Recorder.Queue", func(ctx context.Context, rec *Recorder, a *agentturn.Agent, item openresponses.Item) error {
			return rec.Queue(ctx, a, agentturn.QueueFollowUp, item)
		}},
		{"Agent.Queue", func(ctx context.Context, _ *Recorder, a *agentturn.Agent, item openresponses.Item) error {
			a.Queue(ctx, agentturn.QueueFollowUp, item)
			return nil
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}})
			unsub := rec.Attach(a)
			if _, err := a.Prompt(context.Background(), openresponses.UserText("first")); err != nil {
				t.Fatal(err)
			}
			firing := agentturn.Trigger{Kind: "cron", Ref: "03:00", Extra: map[string]any{"due_at": "2026-09-29T03:00:00Z", "attempt": 2}}
			item := openresponses.UserText("the 03:00 firing")
			if err := tc.queue(agentturn.ContextWithTrigger(context.Background(), firing), rec, a, item); err != nil {
				t.Fatal(err)
			}
			if tc.name == "Agent.Queue" {
				// The agent's queued event waits for a run; a run
				// the crash cuts short takes the event and not the
				// item.
				if _, err := a.Prompt(context.Background(), openresponses.UserText("second")); err != nil {
					t.Fatal(err)
				}
			}
			want := &agentsession.Trigger{Kind: "cron", Ref: "03:00"}
			for name, v := range firing.Extra {
				if err := want.SetMember(name, v); err != nil {
					t.Fatal(err)
				}
			}
			var queued *agentsession.QueuedEntry
			for _, e := range s.Entries() {
				if q, ok := e.(*agentsession.QueuedEntry); ok {
					queued = q
				}
			}
			if queued == nil || !queued.Trigger.Equal(want) {
				t.Fatalf("queued = %+v", queued)
			}
			if tc.name == "Agent.Queue" {
				// The second run drained it already.
				checkDrained(t, s, want)
				return
			}
			// The crash: a new process resumes and hands the owed
			// input back.
			unsub()
			rec2, s2, err := Resume(context.Background(), store, s.ID())
			if err != nil {
				t.Fatal(err)
			}
			opts, err := AgentOptions(s2)
			if err != nil {
				t.Fatal(err)
			}
			b := agentturn.New(agentturn.Config{Model: &echo.Adapter{}}, opts...)
			defer rec2.Attach(b)()
			if n := rec2.Requeue(context.Background(), b); n != 1 {
				t.Fatalf("requeued %d", n)
			}
			if _, err := b.Prompt(context.Background(), openresponses.UserText("second")); err != nil {
				t.Fatal(err)
			}
			checkDrained(t, s2, want)
		})
	}
}

// TestBadTriggerExtraIsRefusedAtTheQueue checks that an input queued
// with a trigger whose Extra cannot be written is refused by the call
// that queues it, so it is neither queued nor recorded, and the next
// run, which it used to fail, records as any other.
func TestBadTriggerExtraIsRefusedAtTheQueue(t *testing.T) {
	cases := []struct {
		name  string
		queue func(ctx context.Context, rec *Recorder, a *agentturn.Agent) error
	}{
		{"Agent.Queue", func(ctx context.Context, _ *Recorder, a *agentturn.Agent) error {
			return a.Queue(ctx, agentturn.QueueSteer, openresponses.UserText("steered"))
		}},
		{"Recorder.Queue", func(ctx context.Context, rec *Recorder, a *agentturn.Agent) error {
			return rec.Queue(ctx, a, agentturn.QueueFollowUp, openresponses.UserText("queued"))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}})
			defer rec.Attach(a)()
			ctx := agentturn.ContextWithTrigger(context.Background(), agentturn.Trigger{Kind: "cron", Extra: map[string]any{"ref": "forged"}})
			if err := tc.queue(ctx, rec, a); !errors.Is(err, agentturn.ErrTriggerExtra) {
				t.Fatalf("err = %v, want ErrTriggerExtra", err)
			}
			if _, err := a.Prompt(context.Background(), openresponses.UserText("next")); err != nil {
				t.Fatalf("the next prompt: %v", err)
			}
			if n := countQueued(s); n != 0 {
				t.Errorf("queued entries = %d in %q", n, entryTypes(s))
			}
			if runs := runsOf(t, s); len(runs) != 1 || runs[0].End == nil || runs[0].End.Reason != agentsession.ReasonDone {
				t.Errorf("runs = %+v", runs)
			}
			verifyAll(t, s)
		})
	}
}

// checkDrained checks that the item a queue drained carries want as its
// source, and that the path verifies.
func checkDrained(t *testing.T, s *agentsession.Session, want *agentsession.Trigger) {
	t.Helper()
	var drained *agentsession.ItemEntry
	for _, e := range s.Path(s.Leaf()) {
		if it, ok := e.(*agentsession.ItemEntry); ok && it.QueuedFrom != "" {
			drained = it
		}
	}
	if drained == nil || !drained.Source.Equal(want) {
		t.Errorf("drained = %+v", drained)
	}
	verifyAll(t, s)
}

type note struct{ Text string }

func (note) RecordNS() string { return "app:note" }

// TestRecordsNameTheirCall pins the recorder half of agentsession #87:
// the records the calls of a parallel batch write carry the call that
// wrote them, a nested call's record carries the call whose tool made
// it, and a child run's own record does not carry its parent's call.
func TestRecordsNameTheirCall(t *testing.T) {
	store := ctxStore{agentsession.NewMemoryStore()}
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	writer := func(name string) agenttool.Tool {
		return agenttool.New(name, "", func(ctx context.Context, _ echoArgs) (string, error) {
			if err := agenttool.WriteRecord(ctx, note{Text: name}); err != nil {
				return "", err
			}
			if name == "outer" {
				if _, err := agentturn.Invoke(ctx, "inner", json.RawMessage(`{"text":"x"}`)); err != nil {
					return "", err
				}
			}
			return name, nil
		})
	}
	// The child's observer annotates while the child runs: its context
	// carries the parent's call, which the child's session does not
	// hold.
	var childNote string
	var childErr error
	child := agent.New(agentturn.Config{Name: "child", Model: &echo.Adapter{}},
		agent.WithObserver(func(ctx context.Context, ev agentturn.Event) {
			rec.Observe(ctx, ev)
			if e, ok := ev.(*agentturn.TurnStart); ok {
				childNote, childErr = rec.Annotate(agentturn.ContextWithRunID(ctx, e.RunID), "app:child", "seen")
			}
		}))
	a := agentturn.New(agentturn.Config{Model: allCalls{}, MaxTurns: 1, ToolRecorder: rec.RecordFunc(),
		Tools: []agenttool.Tool{writer("left"), writer("right"), writer("outer"), writer("inner"), child}})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
		t.Fatal(err)
	}
	callOf := map[string]string{}
	for _, e := range s.Entries() {
		if it, ok := e.(*agentsession.ItemEntry); ok {
			if c, ok := it.Item.(*openresponses.FunctionCall); ok {
				callOf[c.Name] = c.CallID
			}
		}
	}
	// Every tool is called by the model once, and inner once more by
	// outer's tool; that nested call has no function call of its own
	// on the path, so its record is the work of outer.
	got := map[string]int{}
	for _, c := range customs(s, "app:note") {
		var n note
		if err := json.Unmarshal(c.Data, &n); err != nil {
			t.Fatal(err)
		}
		got[n.Text+"@"+c.CallID]++
	}
	want := map[string]int{}
	for _, name := range []string{"left", "right", "outer", "inner"} {
		want[name+"@"+callOf[name]]++
	}
	want["inner@"+callOf["outer"]]++
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("records by call = %v, want %v", got, want)
	}
	for _, c := range customs(s, NestedCallNS) {
		if c.CallID != callOf["outer"] {
			t.Errorf("nested call record names %q, want %q", c.CallID, callOf["outer"])
		}
	}
	if childErr != nil || childNote == "" {
		t.Fatalf("child annotation: %q %v", childNote, childErr)
	}
	id := childNote
	cs, err := store.Open(context.Background(), agentsession.SubsessionID(s.ID(), callOf["child"]))
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := cs.Entry(id); !ok {
		t.Error("the child's annotation is not in the child's session")
	} else if c := e.(*agentsession.CustomEntry); c.CallID != "" {
		t.Errorf("the child's annotation names %q", c.CallID)
	}
	verifyAll(t, s)
}

// TestElicitationIsRecordedUnderTheCall checks that a question a tool
// asks through the loop's elicitor is written under the call that
// asked, with the answer and who gave it, before the tool goes on, and
// that a failure to ask is recorded as such.
func TestElicitationIsRecordedUnderTheCall(t *testing.T) {
	for _, tc := range []struct {
		name string
		ask  agenttool.Elicitor
		want Elicitation
	}{{
		name: "accepted",
		ask: func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) {
			return agenttool.Answer{Action: agenttool.ActionAccept, Content: json.RawMessage(`{"ok":true}`)}, nil
		},
		want: Elicitation{Message: "delete the branch?", Action: "accept", Content: json.RawMessage(`{"ok":true}`), By: agentsession.ByHuman},
	}, {
		name: "nobody to ask",
		want: Elicitation{Message: "delete the branch?", Action: "cancel"},
	}, {
		name: "the harness failed to ask",
		ask: func(context.Context, agenttool.Elicitation) (agenttool.Answer, error) {
			return agenttool.Answer{}, errors.New("terminal closed")
		},
		want: Elicitation{Message: "delete the branch?", Error: "terminal closed"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			store := agentsession.NewMemoryStore()
			rec, s, err := Start(context.Background(), store, agentsession.Header{})
			if err != nil {
				t.Fatal(err)
			}
			var answered agenttool.Action
			tool := agenttool.New("prune", "", func(ctx context.Context, _ echoArgs) (string, error) {
				ask, ok := agenttool.ElicitorFrom(ctx)
				if !ok {
					return "", errors.New("no elicitor on the call")
				}
				ans, err := ask(ctx, agenttool.Elicitation{Message: "delete the branch?"})
				if err != nil {
					return "", err
				}
				answered = ans.Action
				// The question is on the record before the tool acts on
				// the answer.
				if n := len(customs(s, ElicitationNS)); n != 1 {
					return "", fmt.Errorf("%d elicitation entries while the tool runs", n)
				}
				return string(ans.Action), nil
			})
			a := agentturn.New(agentturn.Config{Model: allCalls{}, MaxTurns: 1, Tools: []agenttool.Tool{tool}, ToolElicitor: rec.Elicitor(agentsession.ByHuman, tc.ask)})
			defer rec.Attach(a)()
			if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
				t.Fatal(err)
			}
			got := customs(s, ElicitationNS)
			if len(got) != 1 {
				t.Fatalf("elicitation entries in %q", entryTypes(s))
			}
			var e Elicitation
			if err := json.Unmarshal(got[0].Data, &e); err != nil {
				t.Fatal(err)
			}
			if e.Message != tc.want.Message || e.Action != tc.want.Action || string(e.Content) != string(tc.want.Content) || e.By != tc.want.By || e.Error != tc.want.Error {
				t.Errorf("entry = %+v, want %+v", e, tc.want)
			}
			if got[0].CallID == "" {
				t.Error("the entry names no call")
			}
			if tc.want.Error == "" && string(answered) != tc.want.Action {
				t.Errorf("the tool got %q", answered)
			}
			if types := entryTypes(s); !strings.Contains(types, "dispatch custom item:function_call_output") {
				t.Errorf("entries = %q", types)
			}
		})
	}
}

// TestEntryOfAfterRebase checks that the items of an agent seeded from
// the session's context after a rebase are found, as the items a run
// appends are.
func TestEntryOfAfterRebase(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}})
	defer rec.Attach(a)()
	for _, text := range []string{"one", "two"} {
		if _, err := a.Prompt(context.Background(), openresponses.UserText(text)); err != nil {
			t.Fatal(err)
		}
	}
	var mark string
	for _, e := range s.Entries() {
		if it, ok := e.(*agentsession.ItemEntry); ok && it.ResponseID != "" {
			mark = e.Base().ID
			break
		}
	}
	if err := rec.Rebase(s, mark); err != nil {
		t.Fatal(err)
	}
	cx, err := s.Context()
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range cx.Items {
		if _, ok := rec.EntryOf(context.Background(), item); !ok {
			t.Errorf("no entry for the seeded %s", item.ItemType())
		}
	}
}

// TestProviderIsNotAskedAtEveryRunStart checks that comparing the
// configuration at run_start does not call a tool provider, so one that
// lists its tools in a different order each time writes no config
// entries for a configuration that did not change.
func TestProviderIsNotAskedAtEveryRunStart(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	other := agenttool.New("lower", "lowercase", func(_ context.Context, a echoArgs) (string, error) { return strings.ToLower(a.Text), nil })
	asked := 0
	provider := func(context.Context) []agenttool.Tool {
		asked++
		if asked%2 == 0 {
			return []agenttool.Tool{other, upper}
		}
		return []agenttool.Tool{upper, other}
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ToolProvider: provider})
	turns := 0
	a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		if _, ok := ev.(*agentturn.TurnStart); ok {
			turns++
		}
		return nil
	})
	defer rec.Attach(a)()
	for range 3 {
		if _, err := a.Prompt(context.Background(), openresponses.UserText("go")); err != nil {
			t.Fatal(err)
		}
	}
	// The first run settles in full, which asks the provider once;
	// each turn asks it once more, and nothing else does.
	if asked != turns+1 {
		t.Errorf("the provider was asked %d times over %d turns", asked, turns)
	}
	verifyAll(t, s)
}
