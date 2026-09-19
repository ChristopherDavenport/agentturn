package session

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agentsession"
	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/agentturn/tools/agent"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

type echoArgs struct {
	Text string `json:"text"`
}

var upper = agenttool.New("upper", "uppercase", func(_ context.Context, a echoArgs) (string, error) { return strings.ToUpper(a.Text), nil })

func entryTypes(s *agentsession.Session) string {
	var parts []string
	for _, e := range s.Entries() {
		typ := e.EntryType()
		if it, ok := e.(*agentsession.ItemEntry); ok {
			typ = "item:" + it.Item.ItemType()
			if m, ok := it.Item.(*openresponses.Message); ok {
				typ = "item:" + string(m.Role)
			}
			if it.ResponseID != "" {
				typ += "*"
			}
		}
		parts = append(parts, typ)
	}
	return strings.Join(parts, " ")
}

// verifyAll checks every response entry's hash against the rebuilt
// request and returns how many there were.
func verifyAll(t *testing.T, s *agentsession.Session) int {
	t.Helper()
	n := 0
	for _, e := range s.Entries() {
		if _, ok := e.(*agentsession.ResponseEntry); ok {
			n++
			if err := s.Verify(e.Base().ID); err != nil {
				t.Errorf("verify %s: %v", e.Base().ID, err)
			}
		}
	}
	return n
}

func TestRecordsEveryEventAndVerifies(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{CWD: "/work"})
	if err != nil {
		t.Fatal(err)
	}
	maxOut := 512
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Instructions: "be brief", Tools: []agenttool.Tool{upper},
		Reasoning: openresponses.ReasoningConfig{Effort: openresponses.ReasoningEffortLow}, RequestExtra: map[string]any{"acme_flag": true},
		Request: openresponses.Request{
			ToolChoice:       openresponses.ToolChoice{Mode: openresponses.ToolChoiceAuto},
			MaxOutputTokens:  &maxOut,
			Include:          []openresponses.Include{openresponses.IncludeReasoningEncryptedContent},
			Truncation:       openresponses.TruncationAuto,
			SafetyIdentifier: "user-1",
		}})
	defer rec.Attach(a)()
	if err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	want := "config item:user config item:function_call* response item:function_call_output config item:assistant* response"
	if got := entryTypes(s); got != want {
		t.Fatalf("entries = %q\nwant      %q", got, want)
	}
	if n := verifyAll(t, s); n != 2 {
		t.Errorf("responses = %d", n)
	}
	// The first config is a full replace from the agent's configuration,
	// before any item; the later ones are deltas for the per-turn
	// metadata.
	entries := s.Entries()
	first := entries[0].(*agentsession.ConfigEntry)
	if !first.Replace || first.Model != "m" || first.Instructions == nil || *first.Instructions != "be brief" || len(first.ToolsAdded) != 1 || first.Extra["acme_flag"] == nil || first.Extra["metadata"] != nil {
		t.Errorf("first config = %+v", first)
	}
	for _, k := range []string{"tool_choice", "max_output_tokens", "include", "truncation", "safety_identifier"} {
		if first.Extra[k] == nil {
			t.Errorf("first config lacks request member %q", k)
		}
	}
	for _, i := range []int{2, 6} {
		delta := entries[i].(*agentsession.ConfigEntry)
		if delta.Replace || delta.Model != "" || delta.Instructions != nil || len(delta.ToolsAdded) != 0 || len(delta.Extra) != 1 || delta.Extra["metadata"] == nil {
			t.Errorf("delta config %d = %+v", i, delta)
		}
	}
	// Response entries carry the folded response's fields.
	resp := entries[4].(*agentsession.ResponseEntry)
	if resp.ResponseID == "" || resp.Status != openresponses.ResponseStatusCompleted || resp.Usage == nil || resp.Usage.TotalTokens == 0 || !strings.HasPrefix(resp.RequestHash, HashPrefix) {
		t.Errorf("response = %+v", resp)
	}
	if entries[3].(*agentsession.ItemEntry).ResponseID != resp.ResponseID {
		t.Error("output item does not name its response")
	}
	// The stored context is the transcript the agent holds.
	cx, err := s.Context()
	if err != nil {
		t.Fatal(err)
	}
	if len(cx.Items) != len(a.State().Transcript) {
		t.Errorf("context has %d items, agent %d", len(cx.Items), len(a.State().Transcript))
	}
	// A second run continues the same session and still verifies.
	if err := a.Prompt(context.Background(), openresponses.UserText("again")); err != nil {
		t.Fatal(err)
	}
	if n := verifyAll(t, s); n != 4 {
		t.Errorf("responses after second run = %d", n)
	}
}

func TestAppOnlyItemsBecomeCustomEntries(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}})
	defer rec.Attach(a)()
	note := &openresponses.UnknownItem{Type: "agentturn:note", Raw: json.RawMessage(`{"type":"agentturn:note","text":"ui marker"}`)}
	if err := a.Prompt(context.Background(), note, openresponses.UserText("hi")); err != nil {
		t.Fatal(err)
	}
	if got := entryTypes(s); got != "config custom item:user config item:assistant* response" {
		t.Fatalf("entries = %q", got)
	}
	custom := s.Entries()[1].(*agentsession.CustomEntry)
	if custom.NS != "agentturn:note" || !strings.Contains(string(custom.Data), "ui marker") {
		t.Errorf("custom = %+v", custom)
	}
	verifyAll(t, s)

	// With a filter that shows the note to the model, it is an item.
	store2 := agentsession.NewMemoryStore()
	rec2, s2, _ := Start(context.Background(), store2, agentsession.Header{}, WithFilter(agentturn.VisibleFilter("agentturn:note")))
	a2 := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Filter: agentturn.VisibleFilter("agentturn:note")})
	defer rec2.Attach(a2)()
	if err := a2.Prompt(context.Background(), note, openresponses.UserText("hi")); err != nil {
		t.Fatal(err)
	}
	if got := entryTypes(s2); got != "config item:agentturn:note item:user config item:assistant* response" {
		t.Fatalf("entries = %q", got)
	}
	verifyAll(t, s2)
}

// slowStore delays appends of assistant items so the barrier is
// observable.
type slowStore struct {
	agentsession.Store
	delay time.Duration
	mu    sync.Mutex
	done  time.Time
}

func (s *slowStore) Append(ctx context.Context, id string, e agentsession.Entry) (string, error) {
	if it, ok := e.(*agentsession.ItemEntry); ok {
		if _, isCall := it.Item.(*openresponses.FunctionCall); isCall {
			time.Sleep(s.delay)
			s.mu.Lock()
			s.done = time.Now()
			s.mu.Unlock()
		}
	}
	return s.Store.Append(ctx, id, e)
}

func TestSlowAppendDelaysToolPreflight(t *testing.T) {
	store := &slowStore{Store: agentsession.NewMemoryStore(), delay: 50 * time.Millisecond}
	rec, _, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{upper}})
	defer rec.Attach(a)()
	var toolStarted time.Time
	a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		if _, ok := ev.(*agentturn.ToolStart); ok && toolStarted.IsZero() {
			toolStarted = time.Now()
		}
		return nil
	})
	if err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	written := store.done
	store.mu.Unlock()
	if written.IsZero() || toolStarted.Before(written) {
		t.Errorf("tool preflight at %v, assistant item durable at %v", toolStarted, written)
	}
}

func TestChildRunProducesLink(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{}, WithHarness("test", "1"))
	if err != nil {
		t.Fatal(err)
	}
	child := agent.Tool(agentturn.Config{Name: "specialist", Description: "a child", Model: &echo.Adapter{}})
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{child}})
	defer rec.Attach(a)()
	if err := a.Prompt(context.Background(), openresponses.UserText("delegate")); err != nil {
		t.Fatal(err)
	}
	var link *agentsession.LinkEntry
	for _, e := range s.Entries() {
		if l, ok := e.(*agentsession.LinkEntry); ok {
			link = l
		}
	}
	if link == nil {
		t.Fatalf("no link entry in %q", entryTypes(s))
	}
	call := a.State().Transcript[1].(*openresponses.FunctionCall)
	if link.Rel != "subsession" || link.CallID != call.CallID || link.Session == "" {
		t.Errorf("link = %+v", link)
	}
	childSession, err := store.Open(context.Background(), link.Session)
	if err != nil {
		t.Fatal(err)
	}
	h := childSession.Header()
	if h.ParentSession != s.ID() || h.Harness == nil || h.Harness.Name != "test" {
		t.Errorf("child header = %+v", h)
	}
	if got := entryTypes(childSession); got != "item:user item:assistant*" {
		t.Errorf("child entries = %q", got)
	}
	verifyAll(t, s)

	// Opt out.
	store2 := agentsession.NewMemoryStore()
	rec2, s2, _ := Start(context.Background(), store2, agentsession.Header{}, WithoutChildSessions())
	a2 := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{child}})
	defer rec2.Attach(a2)()
	if err := a2.Prompt(context.Background(), openresponses.UserText("delegate")); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(entryTypes(s2), "link") {
		t.Error("link written with child sessions disabled")
	}
}

func TestSettingsChangesWriteConfig(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "one", Instructions: "first"}
	a := agentturn.New(cfg)
	unsub := rec.Attach(a)
	if err := a.Prompt(context.Background(), openresponses.UserText("x")); err != nil {
		t.Fatal(err)
	}
	unsub()
	// Same recorder, a differently configured agent over the same
	// transcript: instructions, model and a tool change.
	cfg.ModelName = "two"
	cfg.Instructions = "second"
	cfg.Tools = []agenttool.Tool{upper}
	cfg.Text = openresponses.TextConfig{Verbosity: openresponses.VerbosityLow}
	b := agentturn.New(cfg, agentturn.WithTranscript(a.State().Transcript))
	defer rec.Attach(b)()
	if err := b.Prompt(context.Background(), openresponses.UserText("y")); err != nil {
		t.Fatal(err)
	}
	if n := verifyAll(t, s); n != 3 {
		t.Errorf("responses = %d", n)
	}
	var configs []*agentsession.ConfigEntry
	for _, e := range s.Entries() {
		if c, ok := e.(*agentsession.ConfigEntry); ok {
			configs = append(configs, c)
		}
	}
	// Initial full config and a metadata delta for the first run; a
	// full replace when the second agent's tools differ, then a delta.
	var replaces int
	for _, c := range configs {
		if c.Replace {
			replaces++
		}
	}
	if len(configs) != 4 || replaces != 2 || configs[2].Model != "two" || !configs[2].Replace || configs[3].Replace {
		t.Errorf("configs = %+v", configs)
	}
	cx, _ := s.Context()
	if cx.Settings.Model != "two" || cx.Settings.Instructions != "second" || len(cx.Settings.Tools) != 1 || cx.Settings.Text.Verbosity != openresponses.VerbosityLow {
		t.Errorf("settings = %+v", cx.Settings)
	}
}

func TestConfigDelta(t *testing.T) {
	instr := func(s string) *string { return &s }
	base := agentsession.Settings{Model: "m", Instructions: "i", Extra: map[string]json.RawMessage{"a": json.RawMessage(`1`), "b": json.RawMessage(`2`)}}
	full := &agentsession.ConfigEntry{Model: "m", Replace: true}
	cases := []struct {
		name string
		next agentsession.Settings
		want func(*agentsession.ConfigEntry) bool
	}{
		{"equal", base, func(d *agentsession.ConfigEntry) bool { return d == nil }},
		{"model", agentsession.Settings{Model: "n", Instructions: "i", Extra: base.Extra}, func(d *agentsession.ConfigEntry) bool {
			return d.Model == "n" && d.Instructions == nil && d.Extra == nil
		}},
		{"instructions cleared", agentsession.Settings{Model: "m", Extra: base.Extra}, func(d *agentsession.ConfigEntry) bool {
			return d.Instructions != nil && *d.Instructions == "" && d.Model == ""
		}},
		{"extra changed and removed", agentsession.Settings{Model: "m", Instructions: "i", Extra: map[string]json.RawMessage{"a": json.RawMessage(`3`)}}, func(d *agentsession.ConfigEntry) bool {
			return string(d.Extra["a"]) == "3" && string(d.Extra["b"]) == "null" && len(d.Extra) == 2
		}},
		{"model cleared needs replace", agentsession.Settings{Instructions: "i", Extra: base.Extra}, func(d *agentsession.ConfigEntry) bool { return d == full }},
		{"tools need replace", agentsession.Settings{Model: "m", Instructions: "i", Extra: base.Extra, Tools: openresponses.Tools{openresponses.NewFunctionTool("t", "", nil)}}, func(d *agentsession.ConfigEntry) bool { return d == full }},
	}
	_ = instr
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := configDelta(base, tc.next, full)
			if !tc.want(d) {
				t.Errorf("delta = %+v", d)
			}
			if d != nil && d != full {
				if got := base.Apply(d); !equalJSON(got, tc.next) {
					t.Errorf("applying the delta gives %+v, want %+v", got, tc.next)
				}
			}
		})
	}
}

func TestStoreErrorEndsRun(t *testing.T) {
	boom := errors.New("disk full")
	store := &failingStore{Store: agentsession.NewMemoryStore(), err: boom}
	rec, _, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}})
	defer rec.Attach(a)()
	if err := a.Prompt(context.Background(), openresponses.UserText("x")); !errors.Is(err, boom) {
		t.Errorf("err = %v", err)
	}
}

type failingModel struct{ err error }

func (m failingModel) Create(context.Context, openresponses.Request) (*openresponses.Response, error) {
	return nil, m.err
}

func (m failingModel) CreateStream(context.Context, openresponses.Request, openresponses.EventSink) error {
	return m.err
}

func (m failingModel) Compact(context.Context, openresponses.CompactRequest) (*openresponses.CompactResponse, error) {
	return nil, m.err
}

func TestFailedModelCallIsRecordedAndClearedForRecorderReuse(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("unknown model")
	failedAgent := agentturn.New(agentturn.Config{Model: failingModel{err: boom}, ModelName: "missing"})
	unsubscribe := rec.Attach(failedAgent)
	if err := failedAgent.Prompt(context.Background(), openresponses.UserText("first")); !errors.Is(err, boom) {
		t.Fatalf("failed model call error = %v", err)
	}
	unsubscribe()

	entries := s.Entries()
	var failed *agentsession.ResponseEntry
	for _, entry := range entries {
		if response, ok := entry.(*agentsession.ResponseEntry); ok {
			failed = response
		}
	}
	if failed == nil {
		t.Fatal("failed model call was not recorded")
	}
	if failed.Status != openresponses.ResponseStatusFailed || failed.ResponseID != "" || failed.Error == nil {
		t.Errorf("failed response entry = %+v", failed)
	} else if !strings.Contains(failed.Error.Message, boom.Error()) {
		t.Errorf("failed response error = %q, want it to contain %q", failed.Error.Message, boom.Error())
	}
	if failed.RequestHash == "" {
		t.Error("failed response entry has no request hash")
	}
	if rec.pending != "" || !rec.started.IsZero() {
		t.Fatal("failed call left stale request state in the recorder")
	}

	goodAgent := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "working"}, agentturn.WithTranscript(failedAgent.State().Transcript))
	defer rec.Attach(goodAgent)()
	if err := goodAgent.Prompt(context.Background(), openresponses.UserText("second")); err != nil {
		t.Fatal(err)
	}
	var lastResponse *agentsession.ResponseEntry
	for _, entry := range s.Entries() {
		if response, ok := entry.(*agentsession.ResponseEntry); ok {
			lastResponse = response
		}
	}
	if lastResponse == nil || lastResponse.Status != openresponses.ResponseStatusCompleted {
		t.Fatalf("successful response entry = %+v", lastResponse)
	}
	if lastResponse.RequestHash == failed.RequestHash {
		t.Error("successful call reused the failed call's request hash")
	}
}

type failingStore struct {
	agentsession.Store
	err error
}

func (s *failingStore) Append(ctx context.Context, id string, e agentsession.Entry) (string, error) {
	if _, ok := e.(*agentsession.ResponseEntry); ok {
		return "", s.err
	}
	return s.Store.Append(ctx, id, e)
}

func TestLowLevelLoopWithHandle(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for ev, err := range agentturn.Run(ctx, nil, openresponses.Items{openresponses.UserText("x")}, agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{upper}}) {
		if err != nil {
			t.Fatal(err)
		}
		if err := rec.Handle(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	if n := verifyAll(t, s); n != 2 {
		t.Errorf("responses = %d", n)
	}
	if rec.SessionID() != s.ID() {
		t.Error("session id")
	}
}

func TestDeferredCallsRecordAndResume(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{upper},
		BeforeToolCall: func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			return &agentturn.ToolDecision{Defer: true}, nil
		}})
	defer rec.Attach(a)()
	if err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	if got := entryTypes(s); got != "config item:user config item:function_call* response" {
		t.Fatalf("entries after defer = %q", got)
	}
	call := a.State().Pending[0]
	if err := a.Resume(context.Background(), openresponses.NewFunctionCallOutput(call.CallID, "ABC")); err != nil {
		t.Fatal(err)
	}
	if got := entryTypes(s); got != "config item:user config item:function_call* response item:function_call_output config item:assistant* response" {
		t.Fatalf("entries after resume = %q", got)
	}
	if n := verifyAll(t, s); n != 2 {
		t.Errorf("responses = %d", n)
	}
}
