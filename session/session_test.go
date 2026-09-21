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
	"github.com/ChristopherDavenport/agentturn/compact"
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
// request, checks the record entries on the path to the leaf, and
// returns how many responses there were.
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
	if err := s.VerifyRecords(s.Leaf()); err != nil {
		t.Errorf("verify records: %v", err)
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
	if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	want := "run config item:user item:function_call* response dispatch item:function_call_output item:assistant* response run"
	if got := entryTypes(s); got != want {
		t.Fatalf("entries = %q\nwant      %q", got, want)
	}
	if n := verifyAll(t, s); n != 2 {
		t.Errorf("responses = %d", n)
	}
	// The only config is a full replace from the agent's configuration,
	// before any item: the settings never change, so no delta follows.
	entries := s.Entries()
	first := entries[1].(*agentsession.ConfigEntry)
	if !first.Replace || first.Model != "m" || first.Instructions == nil || *first.Instructions != "be brief" || len(first.ToolsAdded) != 1 || first.Extra["acme_flag"] == nil || first.Extra["metadata"] != nil {
		t.Errorf("first config = %+v", first)
	}
	for _, k := range []string{"tool_choice", "max_output_tokens", "include", "truncation", "safety_identifier"} {
		if first.Extra[k] == nil {
			t.Errorf("first config lacks request member %q", k)
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
	if _, err := a.Prompt(context.Background(), openresponses.UserText("again")); err != nil {
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
	if _, err := a.Prompt(context.Background(), note, openresponses.UserText("hi")); err != nil {
		t.Fatal(err)
	}
	if got := entryTypes(s); got != "run config custom item:user item:assistant* response run" {
		t.Fatalf("entries = %q", got)
	}
	custom := s.Entries()[2].(*agentsession.CustomEntry)
	if custom.NS != "agentturn:note" || !strings.Contains(string(custom.Data), "ui marker") {
		t.Errorf("custom = %+v", custom)
	}
	verifyAll(t, s)

	// With a filter that shows the note to the model, it is an item.
	store2 := agentsession.NewMemoryStore()
	rec2, s2, _ := Start(context.Background(), store2, agentsession.Header{}, WithFilter(agentturn.VisibleFilter("agentturn:note")))
	a2 := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Filter: agentturn.VisibleFilter("agentturn:note")})
	defer rec2.Attach(a2)()
	if _, err := a2.Prompt(context.Background(), note, openresponses.UserText("hi")); err != nil {
		t.Fatal(err)
	}
	if got := entryTypes(s2); got != "run config item:agentturn:note item:user item:assistant* response run" {
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
	if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
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
	child := agent.New(agentturn.Config{Name: "specialist", Description: "a child", Model: &echo.Adapter{}})
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{child}})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("delegate")); err != nil {
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
	if _, err := a2.Prompt(context.Background(), openresponses.UserText("delegate")); err != nil {
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
	if _, err := a.Prompt(context.Background(), openresponses.UserText("x")); err != nil {
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
	if _, err := b.Prompt(context.Background(), openresponses.UserText("y")); err != nil {
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
	// The initial full config for the first run, then one delta when the
	// second agent differs, the tool it adds as tools_added, and nothing
	// per turn.
	if len(configs) != 2 || !configs[0].Replace || configs[1].Model != "two" || configs[1].Replace || len(configs[1].ToolsAdded) != 1 || agentsession.ToolName(configs[1].ToolsAdded[0]) != "upper" {
		t.Errorf("configs = %+v", configs)
	}
	cx, _ := s.Context()
	if cx.Settings.Model != "two" || cx.Settings.Instructions != "second" || len(cx.Settings.Tools) != 1 || cx.Settings.Text.Verbosity != openresponses.VerbosityLow {
		t.Errorf("settings = %+v", cx.Settings)
	}
}

func TestConfigDelta(t *testing.T) {
	tool := func(name, desc string) openresponses.Tool {
		return openresponses.NewFunctionTool(name, desc, json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}}}`))
	}
	a, b, c := tool("a", "first"), tool("b", "second"), tool("c", "third")
	base := agentsession.Settings{Model: "m", Instructions: strings.Repeat("be helpful ", 20), Tools: openresponses.Tools{a, b},
		Extra: map[string]json.RawMessage{"a": json.RawMessage(`1`), "b": json.RawMessage(`2`)}}
	// full is what settle would write: the whole of next as a replace.
	full := func(next agentsession.Settings) *agentsession.ConfigEntry {
		req, err := next.Request(nil)
		if err != nil {
			t.Fatal(err)
		}
		entry, err := agentsession.ConfigFromRequest(req)
		if err != nil {
			t.Fatal(err)
		}
		return entry
	}
	with := func(tools ...openresponses.Tool) agentsession.Settings {
		next := base
		next.Tools = tools
		return next
	}
	names := func(tools openresponses.Tools) string {
		var out []string
		for _, t := range tools {
			out = append(out, agentsession.ToolName(t))
		}
		return strings.Join(out, " ")
	}
	cases := []struct {
		name string
		next agentsession.Settings
		want func(*agentsession.ConfigEntry) bool
	}{
		{"equal", base, func(d *agentsession.ConfigEntry) bool { return d == nil }},
		{"model", agentsession.Settings{Model: "n", Instructions: base.Instructions, Tools: base.Tools, Extra: base.Extra}, func(d *agentsession.ConfigEntry) bool {
			return d.Model == "n" && d.Instructions == nil && d.Extra == nil && !d.Replace
		}},
		{"instructions cleared", agentsession.Settings{Model: "m", Tools: base.Tools, Extra: base.Extra}, func(d *agentsession.ConfigEntry) bool {
			return d.Instructions != nil && *d.Instructions == "" && d.Model == "" && !d.Replace
		}},
		{"extra changed and removed", agentsession.Settings{Model: "m", Instructions: base.Instructions, Tools: base.Tools, Extra: map[string]json.RawMessage{"a": json.RawMessage(`3`)}}, func(d *agentsession.ConfigEntry) bool {
			return string(d.Extra["a"]) == "3" && string(d.Extra["b"]) == "null" && len(d.Extra) == 2 && !d.Replace
		}},
		{"model cleared needs replace", agentsession.Settings{Instructions: base.Instructions, Tools: base.Tools, Extra: base.Extra}, func(d *agentsession.ConfigEntry) bool { return d.Replace }},
		{"tool added", with(a, b, c), func(d *agentsession.ConfigEntry) bool {
			return !d.Replace && names(d.ToolsAdded) == "c" && len(d.ToolsRemoved) == 0 && d.Instructions == nil
		}},
		{"tool removed", with(a), func(d *agentsession.ConfigEntry) bool {
			return !d.Replace && len(d.ToolsAdded) == 0 && strings.Join(d.ToolsRemoved, " ") == "b"
		}},
		{"last tool redefined", with(a, tool("b", "changed")), func(d *agentsession.ConfigEntry) bool {
			return !d.Replace && names(d.ToolsAdded) == "b" && len(d.ToolsRemoved) == 0
		}},
		{"first tool redefined needs replace", with(tool("a", "changed"), b), func(d *agentsession.ConfigEntry) bool { return d.Replace }},
		{"tool inserted first needs replace", with(c, a, b), func(d *agentsession.ConfigEntry) bool { return d.Replace }},
		{"every tool replaced is still a delta when smaller", with(tool("x", ""), tool("y", "")), func(d *agentsession.ConfigEntry) bool {
			return !d.Replace && names(d.ToolsAdded) == "x y" && strings.Join(d.ToolsRemoved, " ") == "a b"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := configDelta(base, tc.next, full(tc.next))
			if !tc.want(d) {
				t.Errorf("delta = %+v", d)
			}
			if d != nil {
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
	if _, err := a.Prompt(context.Background(), openresponses.UserText("x")); !errors.Is(err, boom) {
		t.Errorf("err = %v", err)
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
	for ev := range agentturn.Run(ctx, nil, openresponses.Items{openresponses.UserText("x")}, agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{upper}}) {
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
			return &agentturn.ToolDecision{Action: agentturn.Defer}, nil
		}})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("abc")); err != nil {
		t.Fatal(err)
	}
	if got := entryTypes(s); got != "run config item:user item:function_call* response decision run" {
		t.Fatalf("entries after defer = %q", got)
	}
	call := a.State().Pending[0].Call
	if _, err := a.Resume(context.Background(), agentturn.Output(openresponses.NewFunctionCallOutput(call.CallID, "ABC"))); err != nil {
		t.Fatal(err)
	}
	if got := entryTypes(s); got != "run config item:user item:function_call* response decision run run decision item:function_call_output item:assistant* response run" {
		t.Fatalf("entries after resume = %q", got)
	}
	if n := verifyAll(t, s); n != 2 {
		t.Errorf("responses = %d", n)
	}
}

type failingModel struct{}

func (failingModel) CreateStream(context.Context, openresponses.Request, openresponses.EventSink) error {
	return openresponses.ServerError("down", "model unavailable")
}

func TestFailedCallIsRecordedAndRecorderReusable(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	a := agentturn.New(agentturn.Config{Model: failingModel{}, ModelName: "m"})
	unsub := rec.Attach(a)
	if _, err := a.Prompt(context.Background(), openresponses.UserText("x")); err == nil {
		t.Fatal("prompt should fail")
	}
	unsub()
	// The call that never produced a response is a failed response
	// entry carrying the error and the hash of the request sent.
	if got := entryTypes(s); got != "run config item:user response run" {
		t.Fatalf("entries after failure = %q", got)
	}
	entries := s.Entries()
	failed := entries[3].(*agentsession.ResponseEntry)
	if failed.Status != openresponses.ResponseStatusFailed || failed.ResponseID != "" || failed.Error == nil || failed.Error.Code != "down" || !strings.HasPrefix(failed.RequestHash, HashPrefix) {
		t.Errorf("failed response = %+v error=%+v", failed, failed.Error)
	}
	if n := verifyAll(t, s); n != 1 {
		t.Errorf("responses = %d", n)
	}
	// The same recorder on a working agent attributes the next response
	// to its own request, not to the failed one.
	b := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m"}, agentturn.WithTranscript(a.State().Transcript))
	defer rec.Attach(b)()
	if _, err := b.Prompt(context.Background(), openresponses.UserText("y")); err != nil {
		t.Fatal(err)
	}
	if got := entryTypes(s); got != "run config item:user response run run item:user item:assistant* response run" {
		t.Fatalf("entries after reuse = %q", got)
	}
	ok := s.Entries()[8].(*agentsession.ResponseEntry)
	if ok.Status != openresponses.ResponseStatusCompleted || ok.RequestHash == failed.RequestHash {
		t.Errorf("reused recorder response = %+v", ok)
	}
	if n := verifyAll(t, s); n != 2 {
		t.Errorf("responses = %d", n)
	}
}

func TestResumeContinuesWithoutDuplicateConfig(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{Harness: &agentsession.Harness{Name: "h", Version: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	if rec.harness == nil || rec.harness.Name != "h" {
		t.Errorf("Start did not default the child harness: %+v", rec.harness)
	}
	if rec.Store() != store {
		t.Error("Store() is not the store given to Start")
	}
	cfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Instructions: "be brief"}
	a := agentturn.New(cfg)
	unsub := rec.Attach(a)
	if _, err := a.Prompt(context.Background(), openresponses.UserText("x")); err != nil {
		t.Fatal(err)
	}
	unsub()
	before := entryTypes(s)

	// A recorder resumed on the same session with the same configuration
	// writes no config at all for its run.
	rec2, s2, err := Resume(context.Background(), store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	if rec2.harness == nil || rec2.harness.Name != "h" {
		t.Errorf("Resume did not take the header's harness: %+v", rec2.harness)
	}
	b := agentturn.New(cfg, agentturn.WithTranscript(a.State().Transcript))
	unsub = rec2.Attach(b)
	if _, err := b.Prompt(context.Background(), openresponses.UserText("y")); err != nil {
		t.Fatal(err)
	}
	unsub()
	if got := entryTypes(s2); got != before+" run item:user item:assistant* response run" {
		t.Fatalf("entries after resume = %q\nbefore = %q", got, before)
	}
	if n := verifyAll(t, s2); n != 2 {
		t.Errorf("responses = %d", n)
	}

	// Resumed with a changed configuration, it writes the delta, not a
	// full replace.
	rec3, s3, err := Resume(context.Background(), store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	cfg.Instructions = "be thorough"
	c := agentturn.New(cfg, agentturn.WithTranscript(b.State().Transcript))
	defer rec3.Attach(c)()
	if _, err := c.Prompt(context.Background(), openresponses.UserText("z")); err != nil {
		t.Fatal(err)
	}
	entries := s3.Entries()
	var configs []*agentsession.ConfigEntry
	for _, e := range entries {
		if ce, ok := e.(*agentsession.ConfigEntry); ok {
			configs = append(configs, ce)
		}
	}
	if len(configs) != 2 || !configs[0].Replace || configs[1].Replace || configs[1].Instructions == nil || *configs[1].Instructions != "be thorough" || configs[1].Model != "" {
		t.Errorf("configs = %+v", configs)
	}
	cx, _ := s3.Context()
	if cx.Settings.Instructions != "be thorough" || cx.Settings.Model != "m" {
		t.Errorf("settings = %+v", cx.Settings)
	}
	if n := verifyAll(t, s3); n != 3 {
		t.Errorf("responses = %d", n)
	}

	// Resume on an empty session behaves like New.
	empty, err := store.Create(context.Background(), agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	rec4, _, err := Resume(context.Background(), store, empty.ID())
	if err != nil || rec4.root.wroteConfig {
		t.Errorf("resume on empty session: err=%v wroteConfig=%v", err, rec4.root.wroteConfig)
	}
	if _, _, err := Resume(context.Background(), store, "missing"); err == nil {
		t.Error("resume of a missing session should fail")
	}
}

// ctxStore fails every write on a cancelled context, as a file store
// does.
type ctxStore struct {
	agentsession.Store
}

func (s ctxStore) Create(ctx context.Context, h agentsession.Header) (*agentsession.Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.Store.Create(ctx, h)
}

func (s ctxStore) Append(ctx context.Context, id string, e agentsession.Entry) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return s.Store.Append(ctx, id, e)
}

func links(s *agentsession.Session) []*agentsession.LinkEntry {
	var out []*agentsession.LinkEntry
	for _, e := range s.Entries() {
		if l, ok := e.(*agentsession.LinkEntry); ok {
			out = append(out, l)
		}
	}
	return out
}

func TestObservedChildIsWrittenLive(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{}, WithHarness("test", "1"))
	if err != nil {
		t.Fatal(err)
	}
	// Three levels: the parent's tool is a child whose tool is a
	// grandchild whose tool is upper. One observer serves both.
	grandchild := agent.New(agentturn.Config{Name: "grandchild", Model: &echo.Adapter{}, ModelName: "gc", Instructions: "deepest", Tools: []agenttool.Tool{upper}},
		agent.WithObserver(rec.Observe))
	child := agent.New(agentturn.Config{Name: "child", Model: &echo.Adapter{}, ModelName: "c", Instructions: "middle", Tools: []agenttool.Tool{grandchild}},
		agent.WithObserver(rec.Observe))
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "p", Tools: []agenttool.Tool{child}})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("delegate")); err != nil {
		t.Fatal(err)
	}
	if got := entryTypes(s); got != "run config item:user item:function_call* response dispatch link item:function_call_output item:assistant* response run" {
		t.Fatalf("parent entries = %q", got)
	}
	verifyAll(t, s)
	parentLinks := links(s)
	if len(parentLinks) != 1 || parentLinks[0].CallID != s.Entries()[3].(*agentsession.ItemEntry).Item.(*openresponses.FunctionCall).CallID {
		t.Fatalf("parent links = %+v", parentLinks)
	}
	// The child is a full session: config first, then items and
	// responses as they happened, its own link to the grandchild, and
	// every hash verifies.
	cs, err := store.Open(context.Background(), parentLinks[0].Session)
	if err != nil {
		t.Fatal(err)
	}
	if h := cs.Header(); h.ParentSession != s.ID() || h.Harness == nil || h.Harness.Name != "test" {
		t.Errorf("child header = %+v", h)
	}
	if got := entryTypes(cs); got != "run config item:user item:function_call* response dispatch link item:function_call_output item:assistant* response run" {
		t.Fatalf("child entries = %q", got)
	}
	if n := verifyAll(t, cs); n != 2 {
		t.Errorf("child responses verified = %d", n)
	}
	cx, err := cs.Context()
	if err != nil {
		t.Fatal(err)
	}
	if cx.Settings.Model != "c" || cx.Settings.Instructions != "middle" || len(cx.Settings.Tools) != 1 {
		t.Errorf("child settings = %+v", cx.Settings)
	}
	childLinks := links(cs)
	if len(childLinks) != 1 {
		t.Fatalf("child links = %+v", childLinks)
	}
	gs, err := store.Open(context.Background(), childLinks[0].Session)
	if err != nil {
		t.Fatal(err)
	}
	if h := gs.Header(); h.ParentSession != cs.ID() {
		t.Errorf("grandchild header = %+v", h)
	}
	if got := entryTypes(gs); got != "run config item:user item:function_call* response dispatch item:function_call_output item:assistant* response run" {
		t.Fatalf("grandchild entries = %q", got)
	}
	if n := verifyAll(t, gs); n != 2 {
		t.Errorf("grandchild responses verified = %d", n)
	}
	gx, _ := gs.Context()
	if gx.Settings.Model != "gc" || gx.Settings.Instructions != "deepest" {
		t.Errorf("grandchild settings = %+v", gx.Settings)
	}
	// Nothing is left behind once the links are written.
	if len(rec.runs) != 1 {
		t.Errorf("writers still registered = %d", len(rec.runs))
	}

	// A child of a tool run outside any loop is parented to the
	// recorder's session.
	res, err := grandchild.Execute(context.Background(), agenttool.Call{ID: "call_x", Args: json.RawMessage(`{"input":"hi"}`)})
	if err != nil {
		t.Fatal(err)
	}
	w, ok := rec.runs[res.Details.(agent.ChildInfo).RunID]
	if !ok {
		t.Fatal("unlinked child has no writer")
	}
	os, _ := store.Open(context.Background(), w.id)
	if os.Header().ParentSession != s.ID() {
		t.Errorf("orphan child parent = %q", os.Header().ParentSession)
	}
}

func TestAbortDuringChildRunStillLinks(t *testing.T) {
	store := ctxStore{agentsession.NewMemoryStore()}
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	blocking := agenttool.New("wait", "", func(ctx context.Context, _ echoArgs) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	started := make(chan struct{})
	child := agent.New(agentturn.Config{Name: "child", Model: &echo.Adapter{}, Tools: []agenttool.Tool{blocking}},
		agent.WithObserver(func(ctx context.Context, ev agentturn.Event) {
			rec.Observe(ctx, ev)
			if _, ok := ev.(*agentturn.ToolStart); ok {
				close(started)
			}
		}))
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, Tools: []agenttool.Tool{child}})
	defer rec.Attach(a)()
	// A print front registered after the recorder.
	var seen []string
	a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		if e, ok := ev.(*agentturn.ToolEnd); ok {
			seen = append(seen, e.Name)
		}
		return nil
	})
	done := make(chan *agentturn.RunEnd, 1)
	go func() {
		end, _ := a.Prompt(context.Background(), openresponses.UserText("delegate"))
		done <- end
	}()
	<-started
	a.Abort()
	end := <-done
	if end == nil || end.Reason != agentturn.ReasonAborted || len(end.Pending) != 1 {
		t.Fatalf("end = %+v", end)
	}
	if got := entryTypes(s); got != "run config item:user item:function_call* response dispatch link run" {
		t.Errorf("parent entries = %q", got)
	}
	if len(seen) != 1 || seen[0] != "child" {
		t.Errorf("later subscriber saw tool_end for %v", seen)
	}
	l := links(s)
	if len(l) != 1 {
		t.Fatal("no link written for the aborted child")
	}
	cs, err := store.Open(context.Background(), l[0].Session)
	if err != nil {
		t.Fatal(err)
	}
	// The child's cut-off call has its tool_end; its run_end wrote
	// nothing more since no model call was in flight.
	if got := entryTypes(cs); got != "run config item:user item:function_call* response dispatch run" {
		t.Errorf("child entries = %q", got)
	}
	if verifyAll(t, cs) != 1 {
		t.Error("child response did not verify")
	}
}

// countItems is an estimator in items, so a budget is easy to reason
// about.
func countItems(items openresponses.Items) int { return len(items) }

func TestFoldRecordsCompaction(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	summarizer := &echo.Adapter{}
	tr := compact.NewLocal(summarizer, compact.WithBudget(4), compact.WithKeepLast(2), compact.WithEstimator(countItems), compact.WithOnFold(rec.Fold))
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: tr.Transform})
	defer rec.Attach(a)()
	for _, text := range []string{"one", "two", "three", "four", "five"} {
		if _, err := a.Prompt(context.Background(), openresponses.UserText(text)); err != nil {
			t.Fatal(err)
		}
	}
	got := entryTypes(s)
	if !strings.Contains(got, "compaction") {
		t.Fatalf("no compaction entry in %q", got)
	}
	if n := verifyAll(t, s); n != 5 {
		t.Errorf("responses verified = %d", n)
	}
	var folds []*agentsession.CompactionEntry
	for _, e := range s.Entries() {
		if c, ok := e.(*agentsession.CompactionEntry); ok {
			folds = append(folds, c)
		}
	}
	// The third prompt is the first to exceed four items: the summary
	// keeps the last two, the user message just appended and the
	// assistant reply before it.
	first := folds[0]
	kept, ok := s.Entry(first.FirstKept)
	if !ok {
		t.Fatalf("first_kept %s not found", first.FirstKept)
	}
	if m, ok := kept.(*agentsession.ItemEntry).Item.(*openresponses.Message); !ok || m.Role != openresponses.RoleAssistant {
		t.Errorf("first kept = %+v", kept)
	}
	if first.Config.Model != "m" || first.TokensBefore != 5 || first.Usage == nil {
		t.Errorf("compaction = %+v", first)
	}
	if sum, ok := first.Summary.(*openresponses.Message); !ok || !strings.HasPrefix(sum.Text(), "Summary of the conversation so far:") {
		t.Errorf("summary = %+v", first.Summary)
	}
	// The context at the leaf is what the transform sent.
	cx, _ := s.Context()
	if len(cx.Items) == 0 || cx.Items[0] != folds[len(folds)-1].Summary {
		t.Errorf("context does not open with the last summary")
	}
}

func TestFoldWithAppOnlyItemsAndResume(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	tr := compact.NewLocal(&echo.Adapter{}, compact.WithBudget(4), compact.WithKeepLast(2), compact.WithEstimator(countItems), compact.WithOnFold(rec.Fold))
	cfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: tr.Transform}
	a := agentturn.New(cfg)
	unsub := rec.Attach(a)
	// App-only items sit in the working transcript, so a split index
	// into it must count them.
	for _, text := range []string{"one", "two", "three"} {
		note := &openresponses.UnknownItem{Type: "agentturn:note", Raw: json.RawMessage(`{"type":"agentturn:note","text":"ui marker"}`)}
		if _, err := a.Prompt(context.Background(), note, openresponses.UserText(text)); err != nil {
			t.Fatal(err)
		}
	}
	unsub()
	if n := verifyAll(t, s); n != 3 {
		t.Errorf("responses verified = %d", n)
	}
	if !strings.Contains(entryTypes(s), "compaction") {
		t.Fatalf("no compaction in %q", entryTypes(s))
	}

	// A new process resumes from the context at the leaf with a fresh
	// transform: the recorder's alignment comes from the context.
	rec2, s2, err := Resume(context.Background(), store, s.ID())
	if err != nil {
		t.Fatal(err)
	}
	cx, _ := s2.Context()
	tr2 := compact.NewLocal(&echo.Adapter{}, compact.WithBudget(4), compact.WithKeepLast(2), compact.WithEstimator(countItems), compact.WithOnFold(rec2.Fold))
	cfg.Transform = tr2.Transform
	b := agentturn.New(cfg, agentturn.WithTranscript(cx.Items))
	defer rec2.Attach(b)()
	for _, text := range []string{"four", "five"} {
		if _, err := b.Prompt(context.Background(), openresponses.UserText(text)); err != nil {
			t.Fatal(err)
		}
	}
	if n := verifyAll(t, s2); n != 5 {
		t.Errorf("responses verified after resume = %d", n)
	}
	var folds int
	for _, e := range s2.Entries() {
		if _, ok := e.(*agentsession.CompactionEntry); ok {
			folds++
		}
	}
	if folds < 2 {
		t.Errorf("folds = %d", folds)
	}

	// A recorder that did not write the transcript cannot name the
	// first kept entry, and says so rather than guess.
	rec3 := New(store, s.ID())
	if err := rec3.Fold(context.Background(), compact.Fold{Split: 1, Summary: openresponses.UserText("s")}); err == nil {
		t.Error("fold without alignment should fail")
	}
}

type failingFold struct{}

func (failingFold) CreateStream(context.Context, openresponses.Request, openresponses.EventSink) error {
	return errors.New("summary model down")
}

func TestFailedFoldLeavesATrace(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	tr := compact.NewLocal(failingFold{}, compact.WithBudget(4), compact.WithKeepLast(2), compact.WithEstimator(countItems), compact.WithOnFold(rec.Fold))
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: tr.Transform})
	defer rec.Attach(a)()
	for _, text := range []string{"one", "two"} {
		if _, err := a.Prompt(context.Background(), openresponses.UserText(text)); err != nil {
			t.Fatal(err)
		}
	}
	end, err := a.Prompt(context.Background(), openresponses.UserText("three"))
	if err == nil || end.Reason != agentturn.ReasonError {
		t.Fatalf("third prompt: err=%v end=%+v", err, end)
	}
	// The failed fold is the last entry before the run's end.
	entries := s.Entries()
	last, ok := entries[len(entries)-2].(*agentsession.CustomEntry)
	if !ok || last.NS != FailedFoldNS {
		t.Fatalf("entry before the end = %+v, entries %q", entries[len(entries)-2], entryTypes(s))
	}
	var data FailedFold
	if err := json.Unmarshal(last.Data, &data); err != nil || !strings.Contains(data.Error, "summary model down") || data.TokensBefore != 5 {
		t.Errorf("failed fold = %+v err=%v", data, err)
	}
	// The record still verifies: the fold changed nothing.
	if n := verifyAll(t, s); n != 2 {
		t.Errorf("responses verified = %d", n)
	}
}

// TestPinnedFoldNamesWhatItKept checks what the record says about a
// fold that kept items of the folded prefix verbatim: the compaction
// entry names them, and the calls after it carry no request hash,
// because the path rebuilds the summary and the kept tail and knows
// nothing of an item the transform put between them. Verify reports
// them as unverified rather than mismatched.
func TestPinnedFoldNamesWhatItKept(t *testing.T) {
	store := agentsession.NewMemoryStore()
	rec, s, err := Start(context.Background(), store, agentsession.Header{})
	if err != nil {
		t.Fatal(err)
	}
	notice := openresponses.DeveloperText("<system-interrupt>never use Box::leak</system-interrupt>")
	pin := func(item openresponses.Item) bool { return item == openresponses.Item(notice) }
	tr := compact.NewLocal(&echo.Adapter{}, compact.WithBudget(4), compact.WithKeepLast(2),
		compact.WithEstimator(countItems), compact.WithOnFold(rec.Fold), compact.WithPin(pin))
	a := agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Transform: tr.Transform})
	defer rec.Attach(a)()
	if _, err := a.Prompt(context.Background(), openresponses.UserText("one"), notice); err != nil {
		t.Fatal(err)
	}
	var sent []openresponses.Items
	a.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		if e, ok := ev.(*agentturn.TurnStart); ok {
			sent = append(sent, e.Request.Input)
		}
		return nil
	})
	for _, text := range []string{"two", "three", "four"} {
		if _, err := a.Prompt(context.Background(), openresponses.UserText(text)); err != nil {
			t.Fatal(err)
		}
	}
	var folds []*agentsession.CompactionEntry
	for _, e := range s.Entries() {
		if c, ok := e.(*agentsession.CompactionEntry); ok {
			folds = append(folds, c)
		}
	}
	if len(folds) == 0 {
		t.Fatalf("no compaction entry in %q", entryTypes(s))
	}
	raw, ok := folds[0].Unknown[FoldMember]
	if !ok {
		t.Fatalf("the compaction entry names no fold call: %+v", folds[0])
	}
	var call FoldCall
	if err := json.Unmarshal(raw, &call); err != nil {
		t.Fatal(err)
	}
	if len(call.Pinned) != 1 {
		t.Fatalf("the fold names %d pinned items", len(call.Pinned))
	}
	if m, ok := call.Pinned[0].(*openresponses.Message); !ok || m.Text() != notice.Text() {
		t.Errorf("pinned = %+v", call.Pinned[0])
	}
	// The model kept reading the notice after the fold.
	last := sent[len(sent)-1]
	found := false
	for _, item := range last {
		if m, ok := item.(*openresponses.Message); ok && m.Text() == notice.Text() {
			found = true
		}
	}
	if !found {
		t.Errorf("the last request lost the pinned item: %v", last)
	}
	// Nothing mismatches; the calls after the fold are simply not
	// verifiable until the format can describe a pinned item.
	verifyAll(t, s)
	responses := 0
	for _, e := range s.Entries() {
		if r, ok := e.(*agentsession.ResponseEntry); ok && r.RequestHash == "" {
			responses++
		}
	}
	if responses == 0 {
		t.Error("a call after the pinned fold carries a hash the path cannot rebuild")
	}
}
