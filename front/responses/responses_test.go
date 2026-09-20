package responses

import (
	"context"
	"encoding/json"
	"errors"
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
	for ev, err := range agentturn.Run(context.Background(), nil, openresponses.Items{openresponses.UserText("abc")}, outer) {
		if err != nil {
			t.Fatal(err)
		}
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
		return &agentturn.ToolDecision{Defer: true}, nil
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
