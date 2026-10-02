package responses

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	"github.com/ChristopherDavenport/openresponses/streamtest"
)

type echoArgs struct {
	Text string `json:"text"`
}

var upper = agenttool.New("upper", "uppercase", func(_ context.Context, a echoArgs) (string, error) {
	return strings.ToUpper(a.Text), nil
})

func itemTypes(items openresponses.Items) string {
	parts := make([]string, 0, len(items))
	for _, it := range items {
		typ := it.ItemType()
		if m, ok := it.(*openresponses.Message); ok {
			typ = string(m.Role)
		}
		parts = append(parts, typ)
	}
	return strings.Join(parts, " ")
}

func request(items ...openresponses.Item) openresponses.Request {
	return openresponses.Request{Model: "caller-model", Input: openresponses.Items(items)}
}

func TestFullRun(t *testing.T) {
	cases := []struct {
		name      string
		opts      []Option
		wantItems string
	}{
		{"messages only", nil, "assistant"},
		{"with tool items", []Option{WithToolItems()}, "function_call function_call_output assistant"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "agent-model", Instructions: "inst", Tools: []agenttool.Tool{upper}}, tc.opts...)
			sink, err := streamtest.Run(context.Background(), a, request(openresponses.UserText("abc")))
			if err != nil {
				t.Fatal(err)
			}
			resp := sink.Response()
			if got := itemTypes(resp.Output); got != tc.wantItems {
				t.Errorf("output = %q", got)
			}
			if resp.OutputText() != "Tool result: ABC" {
				t.Errorf("text = %q", resp.OutputText())
			}
			if resp.Model != "agent-model" || resp.Instructions == nil || *resp.Instructions != "inst" {
				t.Errorf("model = %q instructions = %v", resp.Model, resp.Instructions)
			}
			if resp.Usage == nil || resp.Usage.TotalTokens == 0 {
				t.Errorf("usage = %+v", resp.Usage)
			}
			// Two model calls contributed to the usage.
			single, _ := (&echo.Adapter{}).Create(context.Background(), openresponses.Request{Model: "m", Input: openresponses.Items{openresponses.UserText("abc")}})
			if resp.Usage.TotalTokens <= single.Usage.TotalTokens {
				t.Errorf("usage %d does not look summed over turns (one call: %d)", resp.Usage.TotalTokens, single.Usage.TotalTokens)
			}
			deltas := 0
			for _, ev := range sink.Events() {
				if _, ok := ev.(*openresponses.OutputTextDeltaEvent); ok {
					deltas++
				}
			}
			if deltas < 2 {
				t.Errorf("deltas = %d, want streamed text", deltas)
			}
			if tc.wantItems != "assistant" {
				fc := resp.Output[0].(*openresponses.FunctionCall)
				fco := resp.Output[1].(*openresponses.FunctionCallOutput)
				if fc.Name != "upper" || fc.CallID == "" || fco.CallID != fc.CallID || fco.Output.Text != "ABC" || fco.Status != openresponses.StatusCompleted {
					t.Errorf("fc = %+v fco = %+v", fc, fco)
				}
			}
		})
	}
}

func TestOneTurnWithCallerTools(t *testing.T) {
	callerTool := openresponses.NewFunctionTool("lookup", "caller owned", json.RawMessage(`{"type":"object","required":["q"]}`))
	executed := false
	agentTool := agenttool.New("upper", "", func(_ context.Context, a echoArgs) (string, error) { executed = true; return "", nil })

	t.Run("caller tool is called, not executed", func(t *testing.T) {
		a := New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m"})
		req := request(openresponses.UserText("abc"))
		req.Tools = openresponses.Tools{callerTool}
		sink, err := streamtest.Run(context.Background(), a, req)
		if err != nil {
			t.Fatal(err)
		}
		resp := sink.Response()
		calls := resp.FunctionCalls()
		if len(calls) != 1 || calls[0].Name != "lookup" || calls[0].Arguments != `{"q":"abc"}` {
			t.Fatalf("calls = %+v", calls)
		}
		if resp.Usage == nil {
			t.Error("usage missing")
		}
		// The caller answers and gets a message back.
		req.Input = append(req.Input, calls[0], openresponses.NewFunctionCallOutput(calls[0].CallID, "found"))
		resp, err = a.Create(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.OutputText() != "Tool result: found" {
			t.Errorf("text = %q", resp.OutputText())
		}
	})

	t.Run("agent tool is advertised but not executed", func(t *testing.T) {
		a := New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{agentTool}})
		req := request(openresponses.UserText("abc"))
		req.Tools = openresponses.Tools{callerTool}
		sink, err := streamtest.Run(context.Background(), a, req)
		if err != nil {
			t.Fatal(err)
		}
		calls := sink.Response().FunctionCalls()
		if len(calls) != 1 || calls[0].Name != "upper" || executed {
			t.Errorf("calls = %+v executed = %v", calls, executed)
		}
	})

	t.Run("name clash is rejected", func(t *testing.T) {
		a := New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{agentTool}})
		req := request(openresponses.UserText("abc"))
		req.Tools = openresponses.Tools{openresponses.NewFunctionTool("upper", "", nil)}
		if _, err := a.Create(context.Background(), req); !openresponses.IsInvalidRequest(err) {
			t.Errorf("err = %v", err)
		}
	})

	t.Run("request instructions", func(t *testing.T) {
		a := New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Instructions: "agent"}, WithRequestInstructions())
		if got := a.instructions("caller"); got != "agent\n\ncaller" {
			t.Errorf("instructions = %q", got)
		}
		if got := New(agentturn.Config{Instructions: "agent"}).instructions("caller"); got != "agent" {
			t.Errorf("instructions = %q", got)
		}
		if got := New(agentturn.Config{}, WithRequestInstructions()).instructions("caller"); got != "caller" {
			t.Errorf("instructions = %q", got)
		}
	})
}

func TestRejections(t *testing.T) {
	a := New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m"})
	cases := []struct {
		name string
		req  openresponses.Request
		want func(error) bool
	}{
		{"previous_response_id", openresponses.Request{Model: "m", PreviousResponseID: "resp_1", Input: openresponses.Items{openresponses.UserText("x")}}, openresponses.IsNotFound},
		{"ends with assistant", request(openresponses.UserText("x"), openresponses.AssistantText("y")), openresponses.IsInvalidRequest},
		{"empty input", request(), openresponses.IsInvalidRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := a.Create(context.Background(), tc.req)
			if err == nil || !tc.want(err) {
				t.Errorf("err = %v", err)
			}
		})
	}
	if _, err := New(agentturn.Config{}).Create(context.Background(), request(openresponses.UserText("x"))); err == nil {
		t.Error("no model accepted")
	}
	if _, err := New(agentturn.Config{Model: failing{}}).Create(context.Background(), request(openresponses.UserText("x"))); err == nil || !strings.Contains(err.Error(), "down") {
		t.Errorf("model failure = %v", err)
	}
}

type failing struct{}

func (failing) CreateStream(context.Context, openresponses.Request, openresponses.EventSink) error {
	return openresponses.ServerError("down", "model unavailable")
}

func TestCompact(t *testing.T) {
	a := New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "agent-model"})
	resp, err := a.Compact(context.Background(), openresponses.CompactRequest{Model: "caller", Input: openresponses.Items{openresponses.UserText("x"), openresponses.AssistantText("y")}})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Output) != 1 || resp.Output[0].ItemType() != openresponses.ItemTypeCompaction {
		t.Errorf("output = %v", resp.Output)
	}
	_, err = New(agentturn.Config{Model: failing{}}).Compact(context.Background(), openresponses.CompactRequest{Model: "m"})
	var oe *openresponses.Error
	if !errors.As(err, &oe) || oe.Code != openresponses.CodeCompactionNotSupported {
		t.Errorf("err = %v", err)
	}
}

// TestAsModelOverHTTP composes two agents over the network: the inner
// agent is served by a Handler and the outer loop points its Model at it
// through a Client.
func TestAsModelOverHTTP(t *testing.T) {
	inner := New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "inner", Tools: []agenttool.Tool{upper}})
	srv := httptest.NewServer(openresponses.NewHandler(inner))
	defer srv.Close()
	client := openresponses.NewClient(srv.URL, openresponses.WithAPIKey("test"))

	outer := agentturn.Config{Model: client.AsAdapter(), ModelName: "outer"}
	var end *agentturn.RunEnd
	var deltas int
	for ev := range agentturn.Run(context.Background(), nil, openresponses.Items{openresponses.UserText("abc")}, outer) {
		switch e := ev.(type) {
		case *agentturn.ItemUpdate:
			if _, ok := e.Stream.(*openresponses.OutputTextDeltaEvent); ok {
				deltas++
			}
		case *agentturn.RunEnd:
			end = e
		}
	}
	if end.Reason != agentturn.ReasonDone || itemTypes(end.Items) != "user assistant" {
		t.Fatalf("end = %+v items = %s", end, itemTypes(end.Items))
	}
	if text := end.Items[1].(*openresponses.Message).Text(); text != "Tool result: ABC" {
		t.Errorf("text = %q", text)
	}
	if deltas < 2 {
		t.Errorf("deltas over HTTP = %d", deltas)
	}
	// Non-streaming Create through the handler also works.
	resp, err := client.Create(context.Background(), openresponses.Request{Model: "x", Input: openresponses.Items{openresponses.UserText("hi")}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.OutputText() != "Tool result: HI" || resp.Model != "inner" {
		t.Errorf("resp = %+v", resp)
	}
}

// TestRelayCatchesUp checks that an upstream which sends items whole,
// without deltas, still yields a valid stream with the full content.
func TestRelayCatchesUp(t *testing.T) {
	a := New(agentturn.Config{Model: whole{}, ModelName: "m"})
	sink, err := streamtest.Run(context.Background(), a, request(openresponses.UserText("x")))
	if err != nil {
		t.Fatal(err)
	}
	resp := sink.Response()
	if got := itemTypes(resp.Output); got != "reasoning assistant" {
		t.Fatalf("output = %q", got)
	}
	rs := resp.Output[0].(*openresponses.ReasoningItem)
	if rs.Summary.Text() != "thinking" || rs.Content.Text() != "deep" || rs.EncryptedContent != "enc" {
		t.Errorf("reasoning = %+v", rs)
	}
	msg := resp.Output[1].(*openresponses.Message)
	if len(msg.Content) != 2 || msg.Content[0].(*openresponses.OutputText).Text != "hello" || msg.Content[1].(*openresponses.Refusal).Refusal != "no" {
		t.Errorf("message = %+v", msg.Content)
	}
}

// TestRelayCarriesTheLoopsCallID pins that a call the loop renamed,
// because the model repeated an earlier call's ID, reaches the caller
// under the loop's ID, the one its output names.
func TestRelayCarriesTheLoopsCallID(t *testing.T) {
	a := New(agentturn.Config{Model: numbering{}, ModelName: "m", Tools: []agenttool.Tool{upper}}, WithToolItems())
	sink, err := streamtest.Run(context.Background(), a, request(openresponses.UserText("x")))
	if err != nil {
		t.Fatal(err)
	}
	resp := sink.Response()
	if got := itemTypes(resp.Output); got != "function_call function_call_output function_call function_call_output assistant" {
		t.Fatalf("output = %q", got)
	}
	first, second := resp.Output[0].(*openresponses.FunctionCall), resp.Output[2].(*openresponses.FunctionCall)
	if first.CallID != "call_0" || second.CallID == "call_0" || !strings.HasPrefix(second.CallID, "call_0") {
		t.Errorf("call IDs %q %q", first.CallID, second.CallID)
	}
	for i, call := range []*openresponses.FunctionCall{first, second} {
		if out := resp.Output[2*i+1].(*openresponses.FunctionCallOutput); out.CallID != call.CallID {
			t.Errorf("output %q answers call %q", out.CallID, call.CallID)
		}
	}
}

// TestCallIDsOnTheWire pins that the caller sees one call ID per call,
// the one the transcript holds, from its output_item.added through its
// output_item.done and the function_call_output that answers it: the
// loop decides the ID as the call opens, so a call that repeats
// another's ID or carries none streams under the loop's from the
// start. A model that sends only finished items gets the same.
func TestCallIDsOnTheWire(t *testing.T) {
	for _, doneOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("done only %t", doneOnly), func(t *testing.T) {
			m := &oneResponse{ids: []string{"call_0", "call_0", "-"}, doneOnly: doneOnly}
			a := New(agentturn.Config{Model: m, ModelName: "m", Tools: []agenttool.Tool{upper}}, WithToolItems())
			sink, err := streamtest.Run(context.Background(), a, request(openresponses.UserText("x")))
			if err != nil {
				t.Fatal(err)
			}
			var added, done, outputs []string
			for _, ev := range sink.Events() {
				switch e := ev.(type) {
				case *openresponses.OutputItemAddedEvent:
					if call, ok := e.Item.(*openresponses.FunctionCall); ok {
						added = append(added, call.CallID)
					}
				case *openresponses.OutputItemDoneEvent:
					switch item := e.Item.(type) {
					case *openresponses.FunctionCall:
						done = append(done, item.CallID)
					case *openresponses.FunctionCallOutput:
						outputs = append(outputs, item.CallID)
					}
				}
			}
			seen := map[string]bool{}
			for _, id := range done {
				if id == "" || seen[id] {
					t.Errorf("call ID %q empty or repeated", id)
				}
				seen[id] = true
			}
			if len(done) != 3 || done[0] != "call_0" || !strings.HasPrefix(done[1], "call_0_") {
				t.Errorf("call IDs %q", done)
			}
			if strings.Join(added, " ") != strings.Join(done, " ") || strings.Join(outputs, " ") != strings.Join(done, " ") {
				t.Errorf("output_item.added %q, output_item.done %q, outputs %q", added, done, outputs)
			}
		})
	}
}

// oneResponse is a model that calls upper once per ID in ids, then
// answers once the outputs are in. An ID of "-" is sent empty, as a
// provider that gives none does. With doneOnly it sends each call's
// output_item.done alone, as a relay that passes on only finished items
// does.
type oneResponse struct {
	ids      []string
	doneOnly bool
}

func (m *oneResponse) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	filter := openresponses.EventSinkFunc(func(ev openresponses.StreamEvent) error {
		var items openresponses.Items
		switch e := ev.(type) {
		case *openresponses.OutputItemAddedEvent:
			if m.doneOnly {
				return nil
			}
			items = openresponses.Items{e.Item}
		case *openresponses.FunctionCallArgumentsDeltaEvent, *openresponses.FunctionCallArgumentsDoneEvent:
			if m.doneOnly {
				return nil
			}
		case *openresponses.OutputItemDoneEvent:
			items = openresponses.Items{e.Item}
		}
		if resp, ok := openresponses.TerminalResponse(ev); ok {
			items = resp.Output
		}
		for _, item := range items {
			if call, ok := item.(*openresponses.FunctionCall); ok && call.CallID == "-" {
				call.CallID = ""
			}
		}
		return sink.Send(ev)
	})
	em := openresponses.NewEmitter(filter, openresponses.NewResponse(req))
	if _, ok := req.Input[len(req.Input)-1].(*openresponses.FunctionCallOutput); ok {
		if err := em.Item(openresponses.AssistantText("done")); err != nil {
			return err
		}
		return em.Complete()
	}
	for _, id := range m.ids {
		w, err := em.FunctionCall(id, "upper")
		if err != nil {
			return err
		}
		if err := w.Arguments(`{"text":"t"}`); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
	}
	return em.Complete()
}

// numbering is a model that numbers its calls per response, as some
// providers do: it calls upper as call_0 until two outputs are in, then
// answers.
type numbering struct{}

func (numbering) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	outputs := 0
	for _, item := range req.Input {
		if _, ok := item.(*openresponses.FunctionCallOutput); ok {
			outputs++
		}
	}
	if outputs >= 2 {
		if err := em.Item(openresponses.AssistantText("done")); err != nil {
			return err
		}
		return em.Complete()
	}
	w, err := em.FunctionCall("call_0", "upper")
	if err != nil {
		return err
	}
	if err := w.Arguments(`{"text":"t"}`); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Complete()
}

// whole is a model that emits complete items with no deltas.
type whole struct{}

func (whole) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if err := em.Item(&openresponses.ReasoningItem{
		Summary:          openresponses.Contents{&openresponses.SummaryText{Text: "thinking"}},
		Content:          openresponses.Contents{&openresponses.ReasoningText{Text: "deep"}},
		EncryptedContent: "enc",
	}); err != nil {
		return err
	}
	if err := em.Item(&openresponses.Message{Role: openresponses.RoleAssistant, Content: openresponses.Contents{
		&openresponses.OutputText{Text: "hello"}, &openresponses.Refusal{Refusal: "no"},
	}}); err != nil {
		return err
	}
	return em.Complete()
}

func TestFullRunDefersToCaller(t *testing.T) {
	deferAll := func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
		return &agentturn.ToolDecision{Action: agentturn.Defer}, nil
	}
	cfg := agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", Tools: []agenttool.Tool{upper}, BeforeToolCall: deferAll}
	for _, withTrace := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "with tool items"}[withTrace], func(t *testing.T) {
			var opts []Option
			if withTrace {
				opts = append(opts, WithToolItems())
			}
			a := New(cfg, opts...)
			sink, err := streamtest.Run(context.Background(), a, request(openresponses.UserText("abc")))
			if err != nil {
				t.Fatal(err)
			}
			resp := sink.Response()
			calls := resp.FunctionCalls()
			if resp.Status != openresponses.ResponseStatusCompleted || len(calls) != 1 || calls[0].Name != "upper" {
				t.Fatalf("status=%s calls=%+v output=%d", resp.Status, calls, len(resp.Output))
			}
			if last := resp.Output[len(resp.Output)-1]; last != openresponses.Item(calls[0]) {
				t.Errorf("pending call is not last: %T", last)
			}
			// The caller runs the tool and sends the conversation back.
			input := openresponses.Items{openresponses.UserText("abc")}
			input = append(input, resp.Output...)
			input = append(input, openresponses.NewFunctionCallOutput(calls[0].CallID, "ABC"))
			sink, err = streamtest.Run(context.Background(), a, openresponses.Request{Model: "m", Input: input})
			if err != nil {
				t.Fatal(err)
			}
			if got := sink.Response().OutputText(); got != "Tool result: ABC" {
				t.Errorf("resumed text = %q", got)
			}
		})
	}
}

// capturing records the request it was sent and answers as echo does.
type capturing struct {
	echo.Adapter
	reqs []openresponses.Request
}

func (c *capturing) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	c.reqs = append(c.reqs, req)
	return c.Adapter.CreateStream(ctx, req, sink)
}

func TestRequestMembersReachModelInBothModes(t *testing.T) {
	maxOut := 321
	model := &capturing{}
	cfg := agentturn.Config{Model: model, ModelName: "m", Instructions: "inst", Tools: []agenttool.Tool{upper},
		RequestExtra: map[string]any{"acme": true},
		Request: openresponses.Request{
			MaxOutputTokens:  &maxOut,
			Include:          []openresponses.Include{openresponses.IncludeReasoningEncryptedContent},
			SafetyIdentifier: "user-1",
			PromptCacheKey:   "cache-1",
			Truncation:       openresponses.TruncationAuto,
			Metadata:         map[string]string{"app": "test"},
		},
		BeforeModelCall: func(_ context.Context, r *openresponses.Request) error {
			r.Metadata["hooked"] = "yes"
			return nil
		},
	}
	a := New(cfg)
	full := request(openresponses.UserText("abc"))
	if _, err := a.Create(context.Background(), full); err != nil {
		t.Fatal(err)
	}
	one := request(openresponses.UserText("abc"))
	one.Tools = openresponses.Tools{openresponses.NewFunctionTool("lookup", "caller owned", nil)}
	one.ToolChoice = openresponses.ToolChoice{Mode: openresponses.ToolChoiceRequired}
	if _, err := a.Create(context.Background(), one); err != nil {
		t.Fatal(err)
	}
	if len(model.reqs) < 2 {
		t.Fatalf("model saw %d requests", len(model.reqs))
	}
	for i, r := range []openresponses.Request{model.reqs[0], model.reqs[len(model.reqs)-1]} {
		mode := [...]string{"full run", "one turn"}[i]
		if r.MaxOutputTokens == nil || *r.MaxOutputTokens != maxOut || !r.Includes(openresponses.IncludeReasoningEncryptedContent) ||
			r.SafetyIdentifier != "user-1" || r.PromptCacheKey != "cache-1" || r.Truncation != openresponses.TruncationAuto {
			t.Errorf("%s: request members lost: %+v", mode, r)
		}
		if r.Model != "m" || r.Instructions != "inst" || r.Store == nil || *r.Store || !r.Stream {
			t.Errorf("%s: loop-owned members wrong: model=%q instructions=%q", mode, r.Model, r.Instructions)
		}
		if r.Extra["acme"] != true || r.Metadata["app"] != "test" || r.Metadata["hooked"] != "yes" {
			t.Errorf("%s: extra=%v metadata=%v", mode, r.Extra, r.Metadata)
		}
	}
	last := model.reqs[len(model.reqs)-1]
	if len(last.Tools) != 2 || last.ToolChoice.Mode != openresponses.ToolChoiceRequired {
		t.Errorf("one turn: tools=%d tool_choice=%+v", len(last.Tools), last.ToolChoice)
	}
	if cfg.Request.Metadata["hooked"] != "" {
		t.Error("template metadata mutated by the hook")
	}
}

// reusedIndex is a model that streams its calls to upper the way
// Ollama 0.23 does: every call at output_index 0, each opened and
// closed in turn, while the response it completes with lists them at
// their own positions. The events are written out, not made through
// an emitter, which numbers the items itself, and are kept for the
// test to check against the lifecycle with the reuse allowed. With
// noID the items carry no item ID.
type reusedIndex struct {
	calls []string
	// say ends the first stream with a message holding it, at the same
	// index, after the calls.
	say  string
	noID bool
	// tool is the function the calls name, upper when it is empty.
	tool string
	// streams are the events of each call to the model.
	streams [][]openresponses.StreamEvent
}

func (m *reusedIndex) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	resp := openresponses.NewResponse(req)
	tool := m.tool
	if tool == "" {
		tool = "upper"
	}
	var seq int64
	m.streams = append(m.streams, nil)
	send := func(ev openresponses.StreamEvent) error {
		n := seq
		seq++
		switch e := ev.(type) {
		case *openresponses.ResponseCreatedEvent:
			e.SequenceNumber = n
		case *openresponses.ResponseInProgressEvent:
			e.SequenceNumber = n
		case *openresponses.OutputItemAddedEvent:
			e.SequenceNumber = n
		case *openresponses.FunctionCallArgumentsDeltaEvent:
			e.SequenceNumber = n
		case *openresponses.FunctionCallArgumentsDoneEvent:
			e.SequenceNumber = n
		case *openresponses.OutputItemDoneEvent:
			e.SequenceNumber = n
		case *openresponses.ResponseCompletedEvent:
			e.SequenceNumber = n
		}
		m.streams[len(m.streams)-1] = append(m.streams[len(m.streams)-1], ev)
		return sink.Send(ev)
	}
	if err := send(&openresponses.ResponseCreatedEvent{Response: resp}); err != nil {
		return err
	}
	if err := send(&openresponses.ResponseInProgressEvent{Response: resp}); err != nil {
		return err
	}
	var listed openresponses.Items
	if _, ok := req.Input[len(req.Input)-1].(*openresponses.FunctionCallOutput); ok {
		msg := &openresponses.Message{ID: "msg_done", Role: openresponses.RoleAssistant, Status: openresponses.StatusCompleted, Content: openresponses.Contents{&openresponses.OutputText{Text: "done"}}}
		listed = openresponses.Items{msg}
		if err := send(&openresponses.OutputItemAddedEvent{OutputIndex: 0, Item: msg}); err != nil {
			return err
		}
		if err := send(&openresponses.OutputItemDoneEvent{OutputIndex: 0, Item: msg}); err != nil {
			return err
		}
	} else {
		for i, id := range m.calls {
			itemID := fmt.Sprintf("fc_%d", i)
			if m.noID {
				itemID = ""
			}
			args := fmt.Sprintf(`{"text":"t%d"}`, i)
			open := &openresponses.FunctionCall{ID: itemID, CallID: id, Name: tool, Status: openresponses.StatusInProgress}
			full := &openresponses.FunctionCall{ID: itemID, CallID: id, Name: tool, Arguments: args, Status: openresponses.StatusCompleted}
			listed = append(listed, full)
			for _, ev := range []openresponses.StreamEvent{
				&openresponses.OutputItemAddedEvent{OutputIndex: 0, Item: open},
				&openresponses.FunctionCallArgumentsDeltaEvent{ItemID: itemID, OutputIndex: 0, Delta: args},
				&openresponses.FunctionCallArgumentsDoneEvent{ItemID: itemID, OutputIndex: 0, Arguments: args},
				&openresponses.OutputItemDoneEvent{OutputIndex: 0, Item: full},
			} {
				if err := send(ev); err != nil {
					return err
				}
			}
		}
	}
	if m.say != "" && len(m.streams) == 1 {
		msg := &openresponses.Message{ID: "msg_say", Role: openresponses.RoleAssistant, Status: openresponses.StatusCompleted, Content: openresponses.Contents{&openresponses.OutputText{Text: m.say}}}
		listed = append(listed, msg)
		for _, ev := range []openresponses.StreamEvent{
			&openresponses.OutputItemAddedEvent{OutputIndex: 0, Item: &openresponses.Message{ID: msg.ID, Role: msg.Role, Status: openresponses.StatusInProgress}},
			&openresponses.OutputItemDoneEvent{OutputIndex: 0, Item: msg},
		} {
			if err := send(ev); err != nil {
				return err
			}
		}
	}
	resp.Status = openresponses.ResponseStatusCompleted
	resp.Output = listed
	return send(&openresponses.ResponseCompletedEvent{Response: resp})
}

// TestRelayCarriesEveryCallOfAReusedIndex pins that a model that
// streams all its calls at output_index 0 has each reach the caller as
// the call it is, with its own arguments, where the accumulator's
// Output[0] held only the first of them once it kept every item: the
// calls the front relays are the ones the transcript holds, run and
// answered each once. The model's own stream is checked first, against
// the lifecycle with the reuse allowed, so the test streams what a
// provider does and not what the validator happens to accept.
func TestRelayCarriesEveryCallOfAReusedIndex(t *testing.T) {
	for _, noID := range []bool{false, true} {
		t.Run(fmt.Sprintf("no item IDs %t", noID), func(t *testing.T) {
			m := &reusedIndex{calls: []string{"call_a", "call_b"}, noID: noID}
			a := New(agentturn.Config{Model: m, ModelName: "m", Tools: []agenttool.Tool{upper}}, WithToolItems())
			sink, err := streamtest.Run(context.Background(), a, request(openresponses.UserText("x")))
			if err != nil {
				t.Fatalf("the front's stream: %v", err)
			}
			if len(m.streams) != 2 {
				t.Fatalf("model called %d times, want 2", len(m.streams))
			}
			if err := streamtest.Validate(m.streams[0], streamtest.WithOutputIndexReuse()); err != nil {
				t.Fatalf("the model's stream is not the reused-index lifecycle: %v", err)
			}
			if streamtest.Validate(m.streams[0]) == nil {
				t.Fatal("the model's stream reuses no index")
			}
			resp := sink.Response()
			if got := itemTypes(resp.Output); got != "function_call function_call function_call_output function_call_output assistant" {
				t.Fatalf("output = %q", got)
			}
			for i, want := range []struct{ id, args, out string }{{"call_a", `{"text":"t0"}`, "T0"}, {"call_b", `{"text":"t1"}`, "T1"}} {
				call := resp.Output[i].(*openresponses.FunctionCall)
				out := resp.Output[2+i].(*openresponses.FunctionCallOutput)
				if call.CallID != want.id || call.Arguments != want.args || out.CallID != want.id || out.Output.String() != want.out {
					t.Errorf("call %d: %s %s answered %s with %q, want %+v", i, call.CallID, call.Arguments, out.CallID, out.Output.String(), want)
				}
			}
		})
	}
}

// TestGuardSeesTheItemsBeforeAMessageAtAReusedIndex pins that the
// items an OutputGuard is given ahead of a message are the ones the
// stream opened before it, whatever output index each was at: a
// message opened at an index the calls before it reused has both calls
// ahead of it, not none.
func TestGuardSeesTheItemsBeforeAMessageAtAReusedIndex(t *testing.T) {
	m := &reusedIndex{calls: []string{"call_a", "call_b"}, say: "hello", tool: "lookup"}
	var before []string
	a := New(agentturn.Config{Model: m, ModelName: "m",
		OutputGuard: func(_ context.Context, info agentturn.OutputInfo) (*openresponses.Message, error) {
			for _, item := range info.Output {
				if call, ok := item.(*openresponses.FunctionCall); ok {
					before = append(before, call.CallID)
				}
			}
			return nil, nil
		}})
	req := request(openresponses.UserText("x"))
	req.Tools = openresponses.Tools{openresponses.NewFunctionTool("lookup", "caller owned", json.RawMessage(`{"type":"object"}`))}
	if _, err := streamtest.Run(context.Background(), a, req); err != nil {
		t.Fatal(err)
	}
	if err := streamtest.Validate(m.streams[0], streamtest.WithOutputIndexReuse()); err != nil {
		t.Fatalf("the model's stream is not the reused-index lifecycle: %v", err)
	}
	if strings.Join(before, " ") != "call_a call_b" {
		t.Errorf("the guard saw the calls %q ahead of the message, want call_a call_b", before)
	}
}
