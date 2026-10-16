package agentturn_test

// The conformance runner drives the reference loop through the scenario
// corpus in testdata/loop, which RFC 0001's Conformance section
// specifies. It uses only the package's exported API, as a second
// binding's runner would use its own.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

type scenario struct {
	Name            string                      `json:"name"`
	Description     string                      `json:"description"`
	Rules           []string                    `json:"rules"`
	KnownDivergence string                      `json:"known_divergence"`
	Config          scenarioConfig              `json:"config"`
	Tools           []scenarioTool              `json:"tools"`
	Results         map[string]*scriptedResult  `json:"results"`
	Decisions       map[string]scriptedDecision `json:"decisions"`
	Transcript      openresponses.Items         `json:"transcript"`
	Model           []scriptedAttempt           `json:"model"`
	Runs            []scenarioRun               `json:"runs"`
	Executions      map[string]int              `json:"executions"`
}

type scenarioConfig struct {
	Model            string `json:"model"`
	Instructions     string `json:"instructions"`
	MaxTurns         int    `json:"max_turns"`
	ToolExecution    string `json:"tool_execution"`
	MaxParallelTools int    `json:"max_parallel_tools"`
	Retry            struct {
		MaxAttempts int `json:"max_attempts"`
	} `json:"retry"`
}

type scenarioTool struct {
	Name       string            `json:"name"`
	Sequential bool              `json:"sequential"`
	Replay     string            `json:"replay"`
	Result     *scriptedResult   `json:"result"`
	Sequence   []*scriptedResult `json:"sequence"`
}

type scriptedResult struct {
	Output    *string         `json:"output"`
	Terminate bool            `json:"terminate"`
	Error     string          `json:"error"`
	Wait      string          `json:"wait"`
	Invoke    scriptedInvokes `json:"invoke"`
}

type scriptedInvoke struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

// scriptedInvokes is a scripted result's invoke: one nested call, or a
// list of them made in order.
type scriptedInvokes []scriptedInvoke

func (s *scriptedInvokes) UnmarshalJSON(b []byte) error {
	if bytes.HasPrefix(bytes.TrimSpace(b), []byte("[")) {
		var list []scriptedInvoke
		if err := strictJSON(b, &list); err != nil {
			return err
		}
		*s = list
		return nil
	}
	var one scriptedInvoke
	if err := strictJSON(b, &one); err != nil {
		return err
	}
	*s = scriptedInvokes{one}
	return nil
}

func strictJSON(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

type scriptedDecision struct {
	Action    string `json:"action"`
	Reason    string `json:"reason"`
	Terminate bool   `json:"terminate"`
}

type scriptedAttempt struct {
	ID      string              `json:"id"`
	Output  openresponses.Items `json:"output"`
	Partial openresponses.Items `json:"partial"`
	Error   *scriptedError      `json:"error"`
}

type scriptedError struct {
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func (e *scriptedError) Error() string { return e.Message }

type scenarioRun struct {
	Prompt     openresponses.Items `json:"prompt"`
	Continue   bool                `json:"continue"`
	Resume     []scriptedAnswer    `json:"resume"`
	CancelAt   json.RawMessage     `json:"cancel_at"`
	FailAt     json.RawMessage     `json:"fail_at"`
	SteerAt    *queueAt            `json:"steer_at"`
	FollowUpAt *queueAt            `json:"follow_up_at"`
	Expect     scenarioExpect      `json:"expect"`
}

type scriptedAnswer struct {
	CallID    string  `json:"call_id"`
	Output    *string `json:"output"`
	Approve   bool    `json:"approve"`
	Terminate bool    `json:"terminate"`
}

type queueAt struct {
	Event json.RawMessage     `json:"event"`
	Items openresponses.Items `json:"items"`
}

type scenarioExpect struct {
	Refused    bool            `json:"refused"`
	Events     json.RawMessage `json:"events"`
	End        json.RawMessage `json:"end"`
	Transcript json.RawMessage `json:"transcript"`
}

func TestConformance(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "loop", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no scenarios in testdata/loop")
	}
	anchors := rfcAnchors(t)
	for _, file := range files {
		name := strings.TrimSuffix(filepath.Base(file), ".json")
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			var sc scenario
			dec := json.NewDecoder(strings.NewReader(string(data)))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&sc); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if sc.Name != name {
				t.Fatalf("name %q does not match the file", sc.Name)
			}
			for _, r := range sc.Rules {
				if !anchors[r] {
					t.Errorf("rule %s names no section of RFC 0001", r)
				}
			}
			if sc.KnownDivergence != "" {
				t.Skipf("the reference diverges: %s", sc.KnownDivergence)
			}
			runScenario(t, &sc)
		})
	}
}

// rfcAnchors returns the link anchor of every heading of RFC 0001, as
// a Markdown renderer derives it, so a scenario's rules name sections
// that exist.
func rfcAnchors(t *testing.T) map[string]bool {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("docs", "rfcs", "0001-agent-loop.md"))
	if err != nil {
		t.Fatal(err)
	}
	anchors := map[string]bool{}
	for line := range strings.Lines(string(data)) {
		heading, ok := strings.CutPrefix(line, "#")
		if !ok {
			continue
		}
		heading = strings.ToLower(strings.TrimSpace(strings.TrimLeft(heading, "#")))
		var b strings.Builder
		for _, r := range heading {
			switch {
			case r == ' ':
				b.WriteRune('-')
			case r == '-' || r == '_' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9':
				b.WriteRune(r)
			}
		}
		anchors["#"+b.String()] = true
	}
	return anchors
}

func runScenario(t *testing.T, sc *scenario) {
	model := &scriptedModel{attempts: sc.Model}
	cfg := agentturn.Config{
		Model:            model,
		ModelName:        sc.Config.Model,
		Instructions:     sc.Config.Instructions,
		MaxTurns:         sc.Config.MaxTurns,
		MaxParallelTools: sc.Config.MaxParallelTools,
		Retry: agentturn.Retry{
			MaxAttempts: sc.Config.Retry.MaxAttempts,
			Backoff:     func(int, error) time.Duration { return 0 },
			Retryable: func(err error) bool {
				var se *scriptedError
				return errors.As(err, &se) && se.Retryable
			},
		},
	}
	switch sc.Config.ToolExecution {
	case "", "parallel":
	case "sequential":
		cfg.ToolExecution = agentturn.ExecSequential
	default:
		t.Fatalf("tool_execution %q", sc.Config.ToolExecution)
	}
	tools := make([]*scriptedTool, 0, len(sc.Tools))
	for _, st := range sc.Tools {
		switch st.Replay {
		case "", "unknown", "safe", "keyed":
		default:
			t.Fatalf("tool %s: replay %q", st.Name, st.Replay)
		}
		tool := &scriptedTool{spec: st, results: sc.Results}
		tools = append(tools, tool)
		cfg.Tools = append(cfg.Tools, tool)
	}
	if sc.Decisions != nil {
		cfg.BeforeToolCall = func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			d, ok := sc.Decisions[info.Call.CallID]
			if !ok || info.Parent != "" {
				return nil, nil
			}
			out := &agentturn.ToolDecision{Reason: d.Reason, Terminate: d.Terminate}
			switch d.Action {
			case "allow":
			case "block":
				out.Action = agentturn.Block
			case "defer":
				out.Action = agentturn.Defer
			default:
				return nil, fmt.Errorf("scenario: decision action %q", d.Action)
			}
			return out, nil
		}
	}
	var opts []agentturn.Option
	if sc.Transcript != nil {
		opts = append(opts, agentturn.WithTranscript(agentturn.Transcript(sc.Transcript)))
	}
	agent := agentturn.New(cfg, opts...)

	b := binds{}
	for i, run := range sc.Runs {
		ok := t.Run(fmt.Sprintf("run %d", i+1), func(t *testing.T) {
			driveRun(t, agent, run, b)
		})
		if !ok {
			return
		}
	}
	if err := model.leftover(); err != nil {
		t.Error(err)
	}
	for _, tool := range tools {
		want, ok := sc.Executions[tool.spec.Name]
		if got := tool.executions(); ok && got != want {
			t.Errorf("%s ran %d times, want %d", tool.spec.Name, got, want)
		}
	}
	for name := range sc.Executions {
		if !slices.ContainsFunc(tools, func(tool *scriptedTool) bool { return tool.spec.Name == name }) {
			t.Errorf("executions names %s, which is no tool of the scenario", name)
		}
	}
}

func driveRun(t *testing.T, agent *agentturn.Agent, run scenarioRun, b binds) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		mu     sync.Mutex
		events []any
	)
	matches := func(at json.RawMessage, ev map[string]any) bool {
		if at == nil {
			return false
		}
		var want any
		if err := json.Unmarshal(at, &want); err != nil {
			t.Errorf("scenario: %v", err)
			return false
		}
		return binds(nil).subset(want, ev, "") == ""
	}
	unsubscribe := agent.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		p := project(t, ev)
		mu.Lock()
		if p != nil {
			events = append(events, p)
		}
		mu.Unlock()
		if p == nil {
			return nil
		}
		if matches(run.CancelAt, p) {
			cancel()
		}
		if run.SteerAt != nil && matches(run.SteerAt.Event, p) {
			agent.Steer(run.SteerAt.Items...)
		}
		if run.FollowUpAt != nil && matches(run.FollowUpAt.Event, p) {
			agent.FollowUp(run.FollowUpAt.Items...)
		}
		if matches(run.FailAt, p) {
			return errors.New("scenario: the subscriber fails here")
		}
		return nil
	})
	defer unsubscribe()

	var (
		end *agentturn.RunEnd
		err error
	)
	switch {
	case run.Prompt != nil:
		end, err = agent.Prompt(ctx, run.Prompt...)
	case run.Continue:
		end, err = agent.Continue(ctx)
	case run.Resume != nil:
		answers := make([]agentturn.Answer, 0, len(run.Resume))
		for _, a := range run.Resume {
			var ans agentturn.Answer
			switch {
			case a.Output != nil:
				ans = agentturn.Output(openresponses.NewFunctionCallOutput(a.CallID, *a.Output))
			case a.Approve:
				ans = agentturn.Approve(a.CallID)
			default:
				t.Fatalf("scenario: answer for %s is neither an output nor an approval", a.CallID)
			}
			ans.Terminate = a.Terminate
			answers = append(answers, ans)
		}
		end, err = agent.Resume(ctx, answers...)
	default:
		t.Fatal("scenario: a run is a prompt, a continue or a resume")
	}

	mu.Lock()
	got := events
	mu.Unlock()
	if run.Expect.Refused {
		if err == nil {
			t.Errorf("run was not refused; it ended %s", end.Reason)
		}
		if len(got) != 0 {
			t.Errorf("a refused run delivered %d events", len(got))
		}
		return
	}
	if err != nil {
		t.Fatalf("run refused: %v", err)
	}
	if run.Expect.Events != nil {
		compareEvents(t, run.Expect.Events, got, b)
	}
	if run.Expect.End != nil {
		check(t, "end", run.Expect.End, toJSON(t, projectEnd(end)), b)
	}
	if run.Expect.Transcript != nil {
		check(t, "transcript", run.Expect.Transcript, toJSON(t, agent.State().Transcript), b)
	}
}

func compareEvents(t *testing.T, raw json.RawMessage, got []any, b binds) {
	t.Helper()
	var want []any
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatalf("scenario: events: %v", err)
	}
	n := max(len(want), len(got))
	failed := false
	for i := range n {
		switch {
		case i >= len(want):
			t.Errorf("event %d: unexpected %s", i, describe(got[i]))
			failed = true
		case i >= len(got):
			t.Errorf("event %d: missing %s", i, describe(want[i]))
			failed = true
		default:
			if diff := b.subset(want[i], got[i], ""); diff != "" {
				b, _ := json.Marshal(got[i])
				t.Errorf("event %d (%s): %s %s", i, describe(got[i]), diff, b)
				failed = true
			}
		}
	}
	if failed {
		var types []string
		for _, ev := range got {
			types = append(types, describe(ev))
		}
		t.Logf("events delivered:\n  %s", strings.Join(types, "\n  "))
	}
}

func check(t *testing.T, what string, raw json.RawMessage, got any, b binds) {
	t.Helper()
	var want any
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatalf("scenario: %s: %v", what, err)
	}
	if diff := b.subset(want, got, ""); diff != "" {
		out, _ := json.MarshalIndent(got, "", "  ")
		t.Errorf("%s: %s\ngot %s", what, diff, out)
	}
}

func describe(ev any) string {
	m, ok := ev.(map[string]any)
	if !ok {
		return fmt.Sprint(ev)
	}
	s := fmt.Sprint(m["type"])
	if id, ok := m["call_id"]; ok && id != "" {
		s += " " + fmt.Sprint(id)
	}
	if item, ok := m["item"].(map[string]any); ok {
		s += " " + fmt.Sprint(item["type"])
		if role, ok := item["role"]; ok {
			s += "/" + fmt.Sprint(role)
		}
	}
	return s
}

// binds holds the values the scenario's binding patterns met first,
// by name, across its runs. A nil binds matches a binding pattern to
// any value, as an event pattern a run acts at does.
type binds map[string]any

// subset reports where want is not a subset of got, or "" when it is.
// An object's listed keys must be present and match, an array matches
// an array of the same length elementwise, and a null matches a value
// that is absent or null. An object whose one member is $bind is a
// binding pattern: it binds the name it gives to the value it first
// meets, which must be present, and matches only that value after.
func (b binds) subset(want, got any, path string) string {
	switch w := want.(type) {
	case map[string]any:
		if name, ok := w["$bind"].(string); ok && len(w) == 1 {
			if got == nil {
				return fmt.Sprintf("%s: want a value to bind to %s, got none", orRoot(path), name)
			}
			if b == nil {
				return ""
			}
			bound, seen := b[name]
			if !seen {
				b[name] = got
				return ""
			}
			if !reflect.DeepEqual(bound, got) {
				return fmt.Sprintf("%s: want %v, bound to %s, got %v", orRoot(path), bound, name, got)
			}
			return ""
		}
		g, ok := got.(map[string]any)
		if !ok {
			return fmt.Sprintf("%s: want an object, got %v", orRoot(path), got)
		}
		keys := make([]string, 0, len(w))
		for k := range w {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			gv, present := g[k]
			if !present && w[k] != nil {
				return fmt.Sprintf("%s.%s: missing", path, k)
			}
			if diff := b.subset(w[k], gv, path+"."+k); diff != "" {
				return diff
			}
		}
		return ""
	case []any:
		g, ok := got.([]any)
		if !ok {
			return fmt.Sprintf("%s: want an array, got %v", orRoot(path), got)
		}
		if len(g) != len(w) {
			return fmt.Sprintf("%s: want %d elements, got %d", orRoot(path), len(w), len(g))
		}
		for i := range w {
			if diff := b.subset(w[i], g[i], fmt.Sprintf("%s[%d]", path, i)); diff != "" {
				return diff
			}
		}
		return ""
	case nil:
		if got != nil {
			return fmt.Sprintf("%s: want null, got %v", orRoot(path), got)
		}
		return ""
	default:
		if !reflect.DeepEqual(want, got) {
			return fmt.Sprintf("%s: want %v, got %v", orRoot(path), want, got)
		}
		return ""
	}
}

func orRoot(path string) string {
	if path == "" {
		return "."
	}
	return path
}

// toJSON round-trips v through JSON into the generic form subset reads.
func toJSON(t *testing.T, v any) any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

// project maps an event onto the member names of RFC 0001's event
// table. It returns nil for the events the corpus does not compare,
// item_update and queued.
func project(t *testing.T, ev agentturn.Event) map[string]any {
	m := map[string]any{"type": ev.EventType()}
	switch e := ev.(type) {
	case *agentturn.ItemUpdate, *agentturn.Queued:
		return nil
	case *agentturn.RunStart:
		m["source"] = string(e.Source)
	case *agentturn.TurnStart:
		m["turn"] = e.Turn
		m["request"] = toJSON(t, e.Request)
		m["inputs"] = toJSON(t, e.Inputs)
		arrived := []any{}
		for _, in := range e.Arrived {
			arrived = append(arrived, map[string]any{"item": toJSON(t, in.Item), "mode": string(in.Mode)})
		}
		m["arrived"] = arrived
	case *agentturn.ModelRetry:
		m["turn"] = e.Turn
		m["attempt"] = e.Attempt
		m["error"] = e.Err != nil
		m["request"] = toJSON(t, e.Request)
	case *agentturn.ModelBlocked:
		m["turn"] = e.Turn
		m["request"] = toJSON(t, e.Request)
		m["error"] = e.Err != nil
	case *agentturn.ItemStart:
		m["turn"] = e.Turn
		m["item"] = toJSON(t, e.Item)
		m["response_id"] = e.ResponseID
		m["hidden"] = e.Hidden
	case *agentturn.ItemEnd:
		m["turn"] = e.Turn
		m["item"] = toJSON(t, e.Item)
		m["response_id"] = e.ResponseID
		m["hidden"] = e.Hidden
	case *agentturn.ResponseEnd:
		m["turn"] = e.Turn
		m["response"] = toJSON(t, e.Response)
		m["withheld"] = e.Withheld
	case *agentturn.ToolStart:
		m["turn"] = e.Turn
		m["call_id"] = e.CallID
		m["name"] = e.Name
		m["args"] = rawJSON(t, e.Args)
		m["parent"] = e.Parent
		m["replayed"] = e.Replayed
		if e.Decision != nil {
			m["decision"] = map[string]any{
				"action":    action(e.Decision.Action),
				"reason":    e.Decision.Reason,
				"terminate": e.Decision.Terminate,
			}
		}
	case *agentturn.ToolDispatch:
		m["turn"] = e.Turn
		m["call_id"] = e.CallID
		m["name"] = e.Name
		m["parent"] = e.Parent
		m["idempotency_key"] = e.IdempotencyKey
	case *agentturn.ToolUpdate:
		m["turn"] = e.Turn
		m["call_id"] = e.CallID
		m["name"] = e.Name
	case *agentturn.ToolEnd:
		m["turn"] = e.Turn
		m["call_id"] = e.CallID
		m["name"] = e.Name
		m["result"] = map[string]any{"output": toJSON(t, e.Result.Output), "terminate": e.Result.Terminate}
		m["error"] = e.Err != nil
		m["blocked"] = e.Blocked
		m["deferred"] = e.Deferred
		m["reason"] = e.Reason
		m["parent"] = e.Parent
		m["cut"] = e.Cut
		m["replayed"] = e.Replayed
	case *agentturn.TurnEnd:
		m["turn"] = e.Turn
		m["response"] = toJSON(t, e.Response)
		results := []any{}
		for _, r := range e.ToolResults {
			results = append(results, map[string]any{"output": toJSON(t, r.Output), "terminate": r.Terminate})
		}
		m["tool_results"] = results
	case *agentturn.RunEnd:
		for k, v := range projectEnd(e) {
			m[k] = v
		}
		m["items"] = toJSON(t, e.Items)
	default:
		t.Errorf("unprojected event %s", ev.EventType())
	}
	return toJSON(t, m).(map[string]any)
}

func projectEnd(e *agentturn.RunEnd) map[string]any {
	pending := []any{}
	for _, p := range e.Pending {
		entry := map[string]any{"reason": string(p.Reason)}
		if p.Call != nil {
			entry["call_id"] = p.Call.CallID
			entry["name"] = p.Call.Name
		}
		invoked := []any{}
		for _, c := range p.Invoked {
			invoked = append(invoked, map[string]any{
				"call_id":         c.CallID,
				"parent":          c.Parent,
				"name":            c.Name,
				"args":            json.RawMessage(orObject(c.Args)),
				"idempotency_key": c.IdempotencyKey,
				"dispatched":      c.Dispatched,
				"ended":           c.Ended,
			})
		}
		entry["invoked"] = invoked
		pending = append(pending, entry)
	}
	return map[string]any{
		"reason":   string(e.Reason),
		"cause":    string(e.Cause),
		"error":    e.Err != nil,
		"pending":  pending,
		"withheld": e.Withheld,
	}
}

func rawJSON(t *testing.T, b json.RawMessage) any {
	if len(b) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Errorf("args: %v", err)
	}
	return v
}

func action(a agentturn.ToolAction) string {
	switch a {
	case agentturn.Allow:
		return "allow"
	case agentturn.Block:
		return "block"
	case agentturn.Defer:
		return "defer"
	}
	return fmt.Sprint(int(a))
}

// scriptedModel answers each request with the next scripted attempt,
// across every run of the scenario.
type scriptedModel struct {
	mu       sync.Mutex
	attempts []scriptedAttempt
	next     int
}

func (m *scriptedModel) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	m.mu.Lock()
	if m.next >= len(m.attempts) {
		m.mu.Unlock()
		return &scriptedError{Message: fmt.Sprintf("scenario: the model was called %d times and scripted %d", m.next+1, len(m.attempts))}
	}
	at := m.attempts[m.next]
	m.next++
	m.mu.Unlock()

	resp := openresponses.NewResponse(req)
	if at.ID != "" {
		resp.ID = at.ID
	}
	em := openresponses.NewEmitter(sink, resp)
	if err := em.Start(); err != nil {
		return err
	}
	items := at.Output
	if at.Error != nil {
		items = at.Partial
	}
	for _, item := range items {
		if err := stream(em, item); err != nil {
			return err
		}
	}
	if at.Error != nil {
		return at.Error
	}
	return em.Complete()
}

func (m *scriptedModel) leftover() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.next != len(m.attempts) {
		return fmt.Errorf("the model was called %d times and scripted %d", m.next, len(m.attempts))
	}
	return nil
}

// stream sends one output item as the wire would: opened, its text or
// arguments as one delta, then done.
func stream(em *openresponses.Emitter, item openresponses.Item) error {
	switch it := item.(type) {
	case *openresponses.Message:
		w, err := em.Message(it.Phase)
		if err != nil {
			return err
		}
		if err := w.Text(it.Text()); err != nil {
			return err
		}
		return w.Close()
	case *openresponses.FunctionCall:
		w, err := em.FunctionCall(it.CallID, it.Name)
		if err != nil {
			return err
		}
		if err := w.Arguments(it.Arguments); err != nil {
			return err
		}
		return w.Close()
	case *openresponses.ReasoningItem:
		w, err := em.Reasoning()
		if err != nil {
			return err
		}
		for _, part := range it.Summary {
			if st, ok := part.(*openresponses.SummaryText); ok {
				if err := w.Summary(st.Text); err != nil {
					return err
				}
				if err := w.EndSummary(); err != nil {
					return err
				}
			}
		}
		return w.Close()
	default:
		return em.Item(item)
	}
}

// scriptedTool answers each call with the result the scenario gives its
// call ID, or else the tool's own.
type scriptedTool struct {
	spec    scenarioTool
	results map[string]*scriptedResult
	mu      sync.Mutex
	ran     int
}

func (s *scriptedTool) executions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ran
}

// Replay is the tool's replay for any arguments, as the scenario says.
func (s *scriptedTool) Replay(context.Context, json.RawMessage) agenttool.Replay {
	switch s.spec.Replay {
	case "safe":
		return agenttool.ReplaySafe
	case "keyed":
		return agenttool.ReplayKeyed
	}
	return agenttool.ReplayUnknown
}

func (s *scriptedTool) Name() string                { return s.spec.Name }
func (s *scriptedTool) Description() string         { return "" }
func (s *scriptedTool) Parameters() json.RawMessage { return json.RawMessage(`{"type":"object"}`) }
func (s *scriptedTool) Sequential() bool            { return s.spec.Sequential }

func (s *scriptedTool) Execute(ctx context.Context, call agenttool.Call) (agenttool.Result, error) {
	s.mu.Lock()
	k := s.ran
	s.ran++
	s.mu.Unlock()
	r := s.results[call.ID]
	if r == nil && k < len(s.spec.Sequence) {
		r = s.spec.Sequence[k]
	}
	if r == nil {
		r = s.spec.Result
	}
	if r == nil {
		return agenttool.Result{}, fmt.Errorf("scenario: no result for call %s of %s", call.ID, s.spec.Name)
	}
	for _, inv := range r.Invoke {
		if _, err := agentturn.Invoke(ctx, inv.Name, inv.Arguments); err != nil {
			return agenttool.Result{}, err
		}
	}
	if r.Wait == "cancel" {
		<-ctx.Done()
		return agenttool.Result{}, ctx.Err()
	}
	if r.Error != "" {
		return agenttool.Result{}, errors.New(r.Error)
	}
	out := ""
	if r.Output != nil {
		out = *r.Output
	}
	res := agenttool.Text(out)
	res.Terminate = r.Terminate
	return res, nil
}

// orObject is raw, or an empty object for none.
func orObject(raw json.RawMessage) []byte {
	if len(raw) == 0 {
		return []byte("{}")
	}
	return raw
}
