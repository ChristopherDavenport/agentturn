package control

import (
	"bytes"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// sampleEvents is one event of every type, its fields filled, keyed by
// the type's name in package agentturn.
func sampleEvents(t *testing.T) map[string]agentturn.Event {
	t.Helper()
	call := &openresponses.FunctionCall{CallID: "call_1", Name: "upper", Arguments: `{"text":"a"}`}
	out := openresponses.NewFunctionCallOutput("call_1", "A")
	trigger := agentturn.Trigger{Kind: "control", Ref: "alice", Extra: map[string]any{"n": 1.0}}
	decision := &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "rule r", By: "policy", Held: true, Args: json.RawMessage(`{"text":"b"}`), Note: "n", Terminate: true}
	resp := openresponses.NewResponse(openresponses.Request{Model: "m"})
	resp.Output = openresponses.Items{call}
	req := openresponses.Request{Model: "m", Input: openresponses.Items{openresponses.UserText("hi")}}
	result := agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "A"}, Details: map[string]any{"k": "v"}, Terminate: true}
	stream, err := openresponses.DecodeEvent([]byte(`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"he","sequence_number":3}`))
	if err != nil {
		t.Fatal(err)
	}
	return map[string]agentturn.Event{
		"RunStart":     &agentturn.RunStart{RunID: "run_1", Source: agentturn.SourceInput, Trigger: trigger},
		"TurnStart":    &agentturn.TurnStart{RunID: "run_1", Turn: 1, Request: req, Inputs: openresponses.Items{openresponses.UserText("hi")}, Arrived: []agentturn.TurnInput{{Item: openresponses.UserText("hi"), Mode: agentturn.InputPrompt, Trigger: trigger}}},
		"ModelRetry":   &agentturn.ModelRetry{RunID: "run_1", Turn: 1, Attempt: 2, Err: agentturn.ErrNoModel, Delay: 1500 * time.Millisecond, Request: req},
		"ModelBlocked": &agentturn.ModelBlocked{RunID: "run_1", Turn: 1, Request: req, Err: agentturn.ErrGuard},
		"ItemStart":    &agentturn.ItemStart{RunID: "run_1", Turn: 1, Item: call, ResponseID: "resp_1", Hidden: true},
		"ItemUpdate":   &agentturn.ItemUpdate{RunID: "run_1", Turn: 1, Item: call, Stream: stream, ResponseID: "resp_1"},
		"ItemEnd":      &agentturn.ItemEnd{RunID: "run_1", Turn: 1, Item: call, ResponseID: "resp_1", Hidden: true, Trigger: trigger, ModelCallID: "m_1"},
		"ResponseEnd":  &agentturn.ResponseEnd{RunID: "run_1", Turn: 1, Response: resp, Withheld: true},
		"ToolStart":    &agentturn.ToolStart{RunID: "run_1", Turn: 1, CallID: "call_1", Name: "upper", Args: json.RawMessage(`{"text":"a"}`), Decision: decision, Parent: "call_0"},
		"ToolDispatch": &agentturn.ToolDispatch{RunID: "run_1", Turn: 1, CallID: "call_1", Name: "upper", Parent: "call_0", IdempotencyKey: "k"},
		"ToolUpdate":   &agentturn.ToolUpdate{RunID: "run_1", Turn: 1, CallID: "call_1", Name: "upper", Partial: result},
		"ToolEnd":      &agentturn.ToolEnd{RunID: "run_1", Turn: 1, CallID: "call_1", Name: "upper", Result: result, Err: errors.New("boom"), Blocked: true, Deferred: true, Reason: "r", Parent: "call_0"},
		"TurnEnd":      &agentturn.TurnEnd{RunID: "run_1", Turn: 1, Response: resp, ToolResults: []agenttool.Result{result}},
		"RunEnd": &agentturn.RunEnd{RunID: "run_1", Items: openresponses.Items{openresponses.UserText("hi"), call, out}, Reason: agentturn.ReasonInputRequired,
			Cause: agentturn.StopGuard, Err: agentturn.ErrRunning, Withheld: true, Pending: []agentturn.PendingCall{{
				Call: call, Reason: agentturn.PendingDeferred, Tool: remoteUpper(), Dispatched: true, IdempotencyKey: "k", Args: json.RawMessage(`{"text":"b"}`),
				Ran: out, RanWhere: "branch", Refused: "no", Decision: decision,
			}}},
		"Queued":         &agentturn.Queued{RunID: "run_1", Item: openresponses.UserText("more"), Mode: agentturn.QueueSteer, Hidden: true, Trigger: trigger},
		"Question":       &agentturn.Question{ID: "question_1", RunID: "run_1", CallID: "call_0", Call: &agentturn.AskedCall{Parent: "call_0", CallID: "call_1", Name: "upper", Args: json.RawMessage(`{}`), Decision: decision}, Elicitation: agenttool.Elicitation{Message: "ok?", Schema: json.RawMessage(`{"type":"object"}`), URL: "https://example.test"}},
		"QuestionClosed": &agentturn.QuestionClosed{ID: "question_1", RunID: "run_1", Answer: &agenttool.Answer{Action: agenttool.ActionDecline, Content: json.RawMessage(`{"a":1}`), Note: "not now"}},
	}
}

func remoteUpper() *RemoteTool {
	return &RemoteTool{name: "upper", description: "uppercases", parameters: json.RawMessage(`{"type":"object"}`), annotations: agenttool.Annotations{Title: "Upper", ReadOnly: true}}
}

// eventTypeNames reads package agentturn's source for every type with
// an EventType method, so a new event without a codec case fails the
// test below rather than a client in the field.
func eventTypeNames(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir("../..")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	fset := token.NewFileSet()
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join("../..", e.Name()), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		if f.Name.Name != "agentturn" {
			continue
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != "EventType" || fn.Recv == nil || len(fn.Recv.List) != 1 {
				continue
			}
			if star, ok := fn.Recv.List[0].Type.(*ast.StarExpr); ok {
				if id, ok := star.X.(*ast.Ident); ok {
					names = append(names, id.Name)
				}
			}
		}
	}
	sort.Strings(names)
	return names
}

func TestTheCodecCoversEveryEventType(t *testing.T) {
	samples := sampleEvents(t)
	names := eventTypeNames(t)
	if len(names) < 17 {
		t.Fatalf("found %d event types in agentturn, which is fewer than exist: %v", len(names), names)
	}
	for _, name := range names {
		if _, ok := samples[name]; !ok {
			t.Errorf("agentturn.%s has no sample here: give it a codec case and a sample", name)
		}
		if _, err := wireOf(samples[name]); err != nil && samples[name] != nil {
			t.Errorf("agentturn.%s: %v", name, err)
		}
	}
	if len(decoders) != len(names) {
		t.Errorf("%d decoders for %d event types", len(decoders), len(names))
	}
}

func TestEventsRoundTrip(t *testing.T) {
	for name, ev := range sampleEvents(t) {
		t.Run(name, func(t *testing.T) {
			data, err := MarshalEvent(ev)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.ContainsAny(data, "\n\r") {
				t.Errorf("the JSON form has a line break, which a server-sent event frame cannot carry: %s", data)
			}
			got, err := UnmarshalEvent(data)
			if err != nil {
				t.Fatalf("%v\n%s", err, data)
			}
			if reflect.TypeOf(got) != reflect.TypeOf(ev) {
				t.Fatalf("decoded %T, want %T", got, ev)
			}
			again, err := MarshalEvent(got)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(again, data) {
				t.Errorf("round trip changed it:\n%s\n%s", data, again)
			}
		})
	}
}

// TestWhatTravelsAsADescription pins the fields that are not data.
func TestWhatTravelsAsADescription(t *testing.T) {
	samples := sampleEvents(t)
	decode := func(ev agentturn.Event) agentturn.Event {
		t.Helper()
		data, err := MarshalEvent(ev)
		if err != nil {
			t.Fatal(err)
		}
		got, err := UnmarshalEvent(data)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	end := decode(samples["RunEnd"]).(*agentturn.RunEnd)
	if !errors.Is(end.Err, agentturn.ErrRunning) || errors.Is(end.Err, agentturn.ErrGuard) || end.Err.Error() != agentturn.ErrRunning.Error() {
		t.Errorf("error = %#v", end.Err)
	}
	p := end.Pending[0]
	tool, ok := p.Tool.(*RemoteTool)
	if !ok || tool.Name() != "upper" || tool.Description() != "uppercases" || agenttool.AnnotationsOf(tool) != (agenttool.Annotations{Title: "Upper", ReadOnly: true}) {
		t.Errorf("tool = %#v", p.Tool)
	}
	if _, err := tool.Execute(t.Context(), agenttool.Call{}); !errors.Is(err, ErrRemoteTool) {
		t.Errorf("a remote tool ran: %v", err)
	}
	if p.Decision == nil || !p.Decision.Held || p.Decision.Action != agentturn.Defer || p.Decision.Reason != "rule r" {
		t.Errorf("decision = %+v", p.Decision)
	}
	if p.Call.CallID != "call_1" || p.Ran.CallID != "call_1" {
		t.Errorf("pending = %+v", p)
	}
	retry := decode(samples["ModelRetry"]).(*agentturn.ModelRetry)
	if retry.Delay != 1500*time.Millisecond || !errors.Is(retry.Err, agentturn.ErrNoModel) {
		t.Errorf("retry = %+v", retry)
	}
	te := decode(samples["ToolEnd"]).(*agentturn.ToolEnd)
	if d, ok := te.Result.Details.(json.RawMessage); !ok || string(d) != `{"k":"v"}` {
		t.Errorf("details = %#v", te.Result.Details)
	}
	closed := decode(samples["QuestionClosed"]).(*agentturn.QuestionClosed)
	if closed.Answer == nil || closed.Answer.Note != "not now" || closed.Answer.Action != agenttool.ActionDecline {
		t.Errorf("closed = %+v", closed)
	}
}

func TestUnmarshalEventRefusesWhatItCannotRead(t *testing.T) {
	for _, data := range []string{``, `{`, `[]`, `{"type":"nope"}`, `{"type":"run_end","pending":[{"call":{"type":"message"}}]}`,
		`{"type":"tool_start","decision":{"action":"maybe"}}`, `{"type":"model_retry","delay":"soon"}`, `{"type":"item_end","item":7}`} {
		if ev, err := UnmarshalEvent([]byte(data)); err == nil {
			t.Errorf("%s decoded to %#v", data, ev)
		}
	}
}
