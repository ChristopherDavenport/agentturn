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
	decision := &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "rule r", By: "policy", Held: true, Args: json.RawMessage(`{"text":"b"}`), Note: "n", Terminate: true, Subject: "rm -rf build"}
	resp := openresponses.NewResponse(openresponses.Request{Model: "m"})
	resp.Output = openresponses.Items{call}
	req := openresponses.Request{Model: "m", Input: openresponses.Items{openresponses.UserText("hi")}}
	result := agenttool.Result{Output: openresponses.FunctionCallOutputData{Text: "A"}, Details: map[string]any{"k": "v"}, Terminate: true}
	stream, err := openresponses.DecodeEvent([]byte(`{"type":"response.output_text.delta","item_id":"m1","output_index":0,"content_index":0,"delta":"he","sequence_number":3}`))
	if err != nil {
		t.Fatal(err)
	}
	prov := &agentturn.Provenance{Layers: []agentturn.Layer{{ID: "/repo/AGENTS.md", Source: "agentsmd"}, {Source: "agentmemory"}},
		Source: agentturn.Trigger{Kind: "mcp_prompt", Ref: "docs/review", Source: "mcp", Extra: map[string]any{"server": "docs"}},
		Trust:  &agentturn.Trust{Reads: []agenttool.Label{agenttool.LabelUntrusted}}}
	return map[string]agentturn.Event{
		"RunStart":     &agentturn.RunStart{RunID: "run_1", Source: agentturn.SourceInput, Trigger: trigger},
		"TurnStart":    &agentturn.TurnStart{RunID: "run_1", Turn: 1, Request: req, Inputs: openresponses.Items{openresponses.UserText("hi")}, Arrived: []agentturn.TurnInput{{Item: openresponses.UserText("hi"), Mode: agentturn.InputPrompt, Trigger: trigger}}},
		"ModelRetry":   &agentturn.ModelRetry{RunID: "run_1", Turn: 1, Attempt: 2, Err: agentturn.ErrNoModel, Delay: 1500 * time.Millisecond, Request: req},
		"ModelBlocked": &agentturn.ModelBlocked{RunID: "run_1", Turn: 1, Request: req, Err: agentturn.ErrGuard},
		"ItemStart":    &agentturn.ItemStart{RunID: "run_1", Turn: 1, Item: call, ResponseID: "resp_1", Hidden: true, Copied: true, Provenance: prov},
		"ItemUpdate":   &agentturn.ItemUpdate{RunID: "run_1", Turn: 1, Item: call, Stream: stream, ResponseID: "resp_1"},
		"ItemEnd":      &agentturn.ItemEnd{RunID: "run_1", Turn: 1, Item: call, ResponseID: "resp_1", Hidden: true, Copied: true, Trigger: trigger, ModelCallID: "m_1", Provenance: prov},
		"ResponseEnd":  &agentturn.ResponseEnd{RunID: "run_1", Turn: 1, Response: resp, Withheld: true},
		"ToolStart":    &agentturn.ToolStart{RunID: "run_1", Turn: 1, CallID: "call_1", Name: "upper", Args: json.RawMessage(`{"text":"a"}`), Decision: decision, Parent: "call_0", Invoked: json.RawMessage(`{"text":"A"}`), Replayed: true},
		"ToolDispatch": &agentturn.ToolDispatch{RunID: "run_1", Turn: 1, CallID: "call_1", Name: "upper", Parent: "call_0", IdempotencyKey: "k"},
		"ToolUpdate":   &agentturn.ToolUpdate{RunID: "run_1", Turn: 1, CallID: "call_1", Name: "upper", Partial: result},
		"ToolEnd":      &agentturn.ToolEnd{RunID: "run_1", Turn: 1, CallID: "call_1", Name: "upper", Result: result, Err: errors.New("boom"), Blocked: true, Deferred: true, Reason: "r", Parent: "call_0", Withdrawn: true, Cut: true, Replayed: true},
		"TurnEnd":      &agentturn.TurnEnd{RunID: "run_1", Turn: 1, Response: resp, ToolResults: []agenttool.Result{result}},
		"RunEnd": &agentturn.RunEnd{RunID: "run_1", Items: openresponses.Items{openresponses.UserText("hi"), call, out}, Reason: agentturn.ReasonInputRequired,
			Cause: agentturn.StopGuard, Err: agentturn.ErrRunning, Failure: errors.New("disk full"), Withheld: true, Pending: []agentturn.PendingCall{{
				Call: call, Reason: agentturn.PendingDeferred, Tool: remoteUpper(), Dispatched: true, IdempotencyKey: "k", Args: json.RawMessage(`{"text":"b"}`),
				Ran: out, RanWhere: "branch", Refused: "no", Decision: decision,
				Notes:    []agentturn.Note{{Item: openresponses.UserText("only the unit tests"), Mode: agentturn.InputResume, Trigger: trigger, CallID: "call_1"}},
				Question: &agentturn.OpenQuestion{ID: "entry_1", Message: "Allow notify?", Call: &agentturn.AskedCall{Parent: "call_1", CallID: "call_2", Name: "notify"}},
			}}, Notes: []agentturn.Note{{Item: openresponses.DeveloperText("owed"), Mode: agentturn.InputTool}}},
		"Queued":          &agentturn.Queued{RunID: "run_1", Item: openresponses.UserText("more"), Mode: agentturn.QueueSteer, Input: agentturn.InputDeliver, Hidden: true, Trigger: trigger, Provenance: prov},
		"Question":        &agentturn.Question{ID: "question_1", RunID: "run_1", CallID: "call_0", Call: &agentturn.AskedCall{Parent: "call_0", CallID: "call_1", Name: "upper", Args: json.RawMessage(`{}`), Decision: decision}, Elicitation: agenttool.Elicitation{Message: "ok?", Schema: json.RawMessage(`{"type":"object"}`), URL: "https://example.test"}},
		"QuarantineAdded": &agentturn.QuarantineAdded{},
		"NotesHeld":       &agentturn.NotesHeld{RunID: "run_1", Notes: []agentturn.Note{{Item: openresponses.DeveloperText("a ran in the sandbox"), Mode: agentturn.InputTool}}},
		"QuestionClosed":  &agentturn.QuestionClosed{ID: "question_1", RunID: "run_1", Answer: &agenttool.Answer{Action: agenttool.ActionDecline, Content: json.RawMessage(`{"a":1}`), Note: "not now"}, By: trigger},
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
	if p.Decision == nil || !p.Decision.Held || p.Decision.Action != agentturn.Defer || p.Decision.Reason != "rule r" || p.Decision.Subject != "rm -rf build" {
		t.Errorf("decision = %+v", p.Decision)
	}
	if len(end.Notes) != 1 || end.Notes[0].Mode != agentturn.InputTool {
		t.Errorf("run end notes = %+v", end.Notes)
	}
	if p.Call.CallID != "call_1" || p.Ran.CallID != "call_1" {
		t.Errorf("pending = %+v", p)
	}
	if len(p.Notes) != 1 || p.Notes[0].Item.(*openresponses.Message).Text() != "only the unit tests" || p.Notes[0].Mode != agentturn.InputResume || p.Notes[0].Trigger.Ref != "alice" || p.Notes[0].CallID != "call_1" {
		t.Errorf("notes = %v", p.Notes)
	}
	if q := p.Question; q == nil || q.ID != "entry_1" || q.Reason() != `a question was open: "Allow notify?" about call call_2 (notify)` || q.Call.Parent != "call_1" {
		t.Errorf("question = %+v", p.Question)
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
	if closed.Answer == nil || closed.Answer.Note != "not now" || closed.Answer.Action != agenttool.ActionDecline || closed.By.Ref != "alice" {
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

// TestNotesOfAnOlderServerDecode pins that a pending call's notes as
// agentturn v0.0.23 sent them, bare items, decode as notes with only
// their item known, and that a note with no item is left out.
func TestNotesOfAnOlderServerDecode(t *testing.T) {
	item, err := json.Marshal(openresponses.DeveloperText("a ran in the sandbox"))
	if err != nil {
		t.Fatal(err)
	}
	data := `{"type":"run_end","run_id":"run_1","reason":"input_required","pending":[{"call":{"type":"function_call","call_id":"call_1","name":"upper","arguments":"{}"},"reason":"deferred","notes":[` +
		string(item) + `,{"item":null,"mode":"tool"}]}]}`
	ev, err := UnmarshalEvent([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	notes := ev.(*agentturn.RunEnd).Pending[0].Notes
	if len(notes) != 1 || notes[0].Item.(*openresponses.Message).Text() != "a ran in the sandbox" || notes[0].Mode != "" {
		t.Errorf("notes = %+v", notes)
	}
}

// TestAnswersCarryEveryField pins that an answer crosses the wire whole:
// every field of agentturn.Answer is set in the sample, and reads back
// as it was sent. A failed mark lost on the way would leave the record
// of the output unmarked (#278).
func TestAnswersCarryEveryField(t *testing.T) {
	in := agentturn.Answer{
		CallID: "call_1", Output: openresponses.NewFunctionCallOutput("call_1", "gone"),
		Args: json.RawMessage(`{"text":"b"}`), Note: "n", Terminate: true, By: "human", Reason: "r",
		Origin: "sha256:e", IdempotencyKey: "k", RunAgain: true, Failed: true,
	}
	v := reflect.ValueOf(in)
	for i := range v.NumField() {
		if v.Field(i).IsZero() {
			t.Errorf("the sample leaves Answer.%s unset: set it so the test covers it", v.Type().Field(i).Name)
		}
	}
	wire, err := answersOut([]agentturn.Answer{in})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	var back []wireAnswer
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	out, err := answersIn(back)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("answers = %d", len(out))
	}
	got := out[0]
	gotOut, _ := json.Marshal(got.Output)
	wantOut, _ := json.Marshal(in.Output)
	got.Output, in.Output = nil, nil
	if !reflect.DeepEqual(got, in) || !bytes.Equal(gotOut, wantOut) {
		t.Errorf("answer after the wire = %+v %s, want %+v %s", got, gotOut, in, wantOut)
	}
}

// TestANestedCallsMatchCrossesTheWire pins that a remote view tells a
// nested call given its recorded result from one that ran, and sees the
// arguments it was invoked with (#261).
func TestANestedCallsMatchCrossesTheWire(t *testing.T) {
	samples := sampleEvents(t)
	for _, name := range []string{"ToolStart", "ToolEnd"} {
		data, err := MarshalEvent(samples[name])
		if err != nil {
			t.Fatal(err)
		}
		got, err := UnmarshalEvent(data)
		if err != nil {
			t.Fatal(err)
		}
		switch e := got.(type) {
		case *agentturn.ToolStart:
			if !e.Replayed || string(e.Invoked) != `{"text":"A"}` {
				t.Errorf("tool_start = %+v, want replayed and invoked", e)
			}
		case *agentturn.ToolEnd:
			if !e.Replayed {
				t.Errorf("tool_end = %+v, want replayed", e)
			}
		}
	}
}

// TestAQueuedReportSaysHowTheInputArrived pins that a remote view
// tells a delivery from a steer on the queued report (#244).
func TestAQueuedReportSaysHowTheInputArrived(t *testing.T) {
	data, err := MarshalEvent(sampleEvents(t)["Queued"])
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalEvent(data)
	if err != nil {
		t.Fatal(err)
	}
	if q := got.(*agentturn.Queued); q.Input != agentturn.InputDeliver {
		t.Errorf("queued = %+v, want input deliver", q)
	}
}

// TestACopiedItemCrossesTheWire pins that a remote view tells an item
// taken from another conversation from one this one produced (#243).
func TestACopiedItemCrossesTheWire(t *testing.T) {
	for _, name := range []string{"ItemStart", "ItemEnd"} {
		data, err := MarshalEvent(sampleEvents(t)[name])
		if err != nil {
			t.Fatal(err)
		}
		got, err := UnmarshalEvent(data)
		if err != nil {
			t.Fatal(err)
		}
		switch e := got.(type) {
		case *agentturn.ItemStart:
			if !e.Copied {
				t.Errorf("item_start = %+v, want copied", e)
			}
		case *agentturn.ItemEnd:
			if !e.Copied {
				t.Errorf("item_end = %+v, want copied", e)
			}
		}
	}
}

// TestProvenanceCrossesTheWire pins that a remote view or recorder
// reads where text the person did not write came from and whether it
// is trusted, as a local one does (RFC 0001 The record): the layers,
// the source with its own members and the trust, an empty trust kept
// apart from none.
func TestProvenanceCrossesTheWire(t *testing.T) {
	want := &agentturn.Provenance{Layers: []agentturn.Layer{{ID: "/repo/AGENTS.md", Source: "agentsmd"}, {Source: "agentmemory"}},
		Source: agentturn.Trigger{Kind: "mcp_prompt", Ref: "docs/review", Source: "mcp", Extra: map[string]any{"server": "docs"}},
		Trust:  &agentturn.Trust{Reads: []agenttool.Label{agenttool.LabelUntrusted}}}
	for _, name := range []string{"ItemStart", "ItemEnd", "Queued"} {
		data, err := MarshalEvent(sampleEvents(t)[name])
		if err != nil {
			t.Fatal(err)
		}
		got, err := UnmarshalEvent(data)
		if err != nil {
			t.Fatal(err)
		}
		var p *agentturn.Provenance
		switch e := got.(type) {
		case *agentturn.ItemStart:
			p = e.Provenance
		case *agentturn.ItemEnd:
			p = e.Provenance
		case *agentturn.Queued:
			p = e.Provenance
		}
		if !reflect.DeepEqual(p, want) {
			t.Errorf("%s provenance = %+v, want %+v", name, p, want)
		}
	}
	judged := &agentturn.ItemEnd{RunID: "run_1", Item: openresponses.UserText("x"), Provenance: &agentturn.Provenance{Trust: &agentturn.Trust{Reads: []agenttool.Label{}}}}
	data, err := MarshalEvent(judged)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalEvent(data)
	if err != nil {
		t.Fatal(err)
	}
	if p := got.(*agentturn.ItemEnd).Provenance; p == nil || p.Trust == nil || len(p.Trust.Reads) != 0 {
		t.Errorf("a judged item's provenance = %+v, want an empty trust", p)
	}
	plain, err := UnmarshalEvent([]byte(`{"type":"item_end","run_id":"run_1","turn":1,"item":{"type":"message","role":"user","content":"x"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if p := plain.(*agentturn.ItemEnd).Provenance; p != nil {
		t.Errorf("a plain item's provenance = %+v, want none", p)
	}
}

// TestTheQuarantineDoesNotTravel: what a guard withheld is not put on
// the wire for a remote viewer, in the events that carry the run's
// quarantine or in quarantine_added.
func TestTheQuarantineDoesNotTravel(t *testing.T) {
	secret := openresponses.UserText("the withheld text")
	for _, ev := range []agentturn.Event{
		&agentturn.QuarantineAdded{Items: openresponses.Items{secret}, Quarantined: openresponses.Items{secret}},
		&agentturn.TurnStart{RunID: "run_1", Turn: 1, Quarantined: openresponses.Items{secret}},
		&agentturn.RunEnd{RunID: "run_1", Reason: agentturn.ReasonDone, Quarantined: openresponses.Items{secret}},
	} {
		data, err := MarshalEvent(ev)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte("withheld text")) {
			t.Errorf("%s carries the quarantined text: %s", ev.EventType(), data)
		}
	}
}
