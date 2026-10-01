package responses

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	"github.com/ChristopherDavenport/openresponses/streamtest"
)

// TestOutputGuardReachesTheCaller pins #108: a message the output guard
// replaced reaches the caller as the replacement, in the response and
// in the done events, in a full run and in a single turn with the
// caller's tools; a message the guard kept streams as before.
func TestOutputGuardReachesTheCaller(t *testing.T) {
	const withheld = "[withheld]"
	replace := func(context.Context, agentturn.OutputInfo) (*openresponses.Message, error) {
		return openresponses.AssistantText(withheld), nil
	}
	keep := func(context.Context, agentturn.OutputInfo) (*openresponses.Message, error) { return nil, nil }
	callerTool := openresponses.NewFunctionTool("lookup", "caller owned", json.RawMessage(`{"type":"object"}`))
	fullRun := request(openresponses.UserText("abc"))
	oneTurn := request(openresponses.UserText("abc"), &openresponses.FunctionCall{CallID: "c1", Name: "lookup", Arguments: `{}`}, openresponses.NewFunctionCallOutput("c1", "found"))
	oneTurn.Tools = openresponses.Tools{callerTool}
	for _, tc := range []struct {
		name  string
		req   openresponses.Request
		guard func(context.Context, agentturn.OutputInfo) (*openresponses.Message, error)
		want  string
	}{
		{"full run, replaced", fullRun, replace, withheld},
		{"single turn, replaced", oneTurn, replace, withheld},
		{"full run, kept", fullRun, keep, "abc"},
		{"single turn, kept", oneTurn, keep, "found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seen []agentturn.OutputInfo
			guard := func(ctx context.Context, info agentturn.OutputInfo) (*openresponses.Message, error) {
				seen = append(seen, info)
				return tc.guard(ctx, info)
			}
			a := New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "m", OutputGuard: guard})
			sink, err := streamtest.Run(context.Background(), a, tc.req)
			if err != nil {
				t.Fatal(err)
			}
			if len(seen) != 1 {
				t.Fatalf("the guard saw %d messages", len(seen))
			}
			resp := sink.Response()
			if got := resp.OutputText(); !strings.Contains(got, tc.want) || (tc.want == withheld && got != withheld) {
				t.Errorf("response text = %q, want %q", got, tc.want)
			}
			var deltas strings.Builder
			for _, ev := range sink.Events() {
				switch e := ev.(type) {
				case *openresponses.OutputTextDeltaEvent:
					deltas.WriteString(e.Delta)
				case *openresponses.OutputTextDoneEvent:
					if e.Text != resp.OutputText() {
						t.Errorf("output_text.done = %q, response %q", e.Text, resp.OutputText())
					}
				case *openresponses.OutputItemDoneEvent:
					if m, ok := e.Item.(*openresponses.Message); ok && m.Content.Text() != resp.OutputText() {
						t.Errorf("output_item.done = %q, response %q", m.Content.Text(), resp.OutputText())
					}
				}
			}
			// A kept message streams its text whole, as before.
			if tc.want != withheld && deltas.String() != resp.OutputText() {
				t.Errorf("deltas %q, text %q", deltas.String(), resp.OutputText())
			}
		})
	}
}

// TestOutputGuardSeesWhatPrecedes pins #111: the guard is given the
// items of the response that precede the message.
func TestOutputGuardSeesWhatPrecedes(t *testing.T) {
	var info agentturn.OutputInfo
	a := agentturn.New(agentturn.Config{Model: whole{}, OutputGuard: func(_ context.Context, i agentturn.OutputInfo) (*openresponses.Message, error) {
		info = i
		return nil, nil
	}})
	if _, err := a.Prompt(context.Background(), openresponses.UserText("x")); err != nil {
		t.Fatal(err)
	}
	if got := itemTypes(info.Output); got != "reasoning" || info.ResponseID == "" {
		t.Errorf("output before the message = %q, response %q", got, info.ResponseID)
	}
}

// TestRelayReplacementIsWellFormed drives the relay with the shapes a
// guard's replacement can take over what was streamed, and checks that
// the stream stays valid and ends holding the replacement.
func TestRelayReplacementIsWellFormed(t *testing.T) {
	text := func(s string) openresponses.Content { return &openresponses.OutputText{Text: s} }
	refusal := func(s string) openresponses.Content { return &openresponses.Refusal{Refusal: s} }
	type delta struct{ text, refusal string }
	for _, tc := range []struct {
		name        string
		streamed    []delta
		replacement openresponses.Contents
		wantText    string
		wantRefusal string
	}{
		{"text over text", []delta{{text: "secret"}}, openresponses.Contents{text("X")}, "X", ""},
		{"text over text then refusal", []delta{{text: "secret"}, {refusal: "no"}}, openresponses.Contents{text("X")}, "X", ""},
		{"text and refusal over text", []delta{{text: "secret"}}, openresponses.Contents{text("X"), refusal("R")}, "X", "R"},
		{"refusal over text", []delta{{text: "secret"}}, openresponses.Contents{refusal("R")}, "", "R"},
		{"two texts over text", []delta{{text: "secret"}}, openresponses.Contents{text("a"), text("b")}, "ab", ""},
		{"nothing over text", []delta{{text: "secret"}}, nil, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := &streamtest.Sink{}
			resp := openresponses.NewResponse(openresponses.Request{Model: "m"})
			rl := newRelay(sink, resp)
			if err := rl.start(&openresponses.Message{Role: openresponses.RoleAssistant}); err != nil {
				t.Fatal(err)
			}
			for _, d := range tc.streamed {
				var ev openresponses.StreamEvent = &openresponses.OutputTextDeltaEvent{Delta: d.text}
				if d.refusal != "" {
					ev = &openresponses.RefusalDeltaEvent{Delta: d.refusal}
				}
				if err := rl.update(ev); err != nil {
					t.Fatal(err)
				}
			}
			if err := rl.end(&openresponses.Message{Role: openresponses.RoleAssistant, Content: tc.replacement}); err != nil {
				t.Fatal(err)
			}
			if err := rl.em.Complete(); err != nil {
				t.Fatal(err)
			}
			if err := streamtest.Validate(sink.Events()); err != nil {
				t.Fatalf("stream: %v", err)
			}
			msg := sink.Response().Output[0].(*openresponses.Message)
			gotText, gotRefusal := messageText(msg)
			if gotText != tc.wantText || gotRefusal != tc.wantRefusal {
				t.Errorf("message = %q / %q, want %q / %q", gotText, gotRefusal, tc.wantText, tc.wantRefusal)
			}
		})
	}
}

// TestGuardStopBeforeAnswerIsARefusal pins #138, #164 and #181: a full
// run a guard stopped before the model answered, whichever hook the
// guard is on, OutputGuard included, and a single turn BeforeModelCall
// or OutputGuard refused as a guard, end the
// response incomplete with reason content_filter over HTTP, collected
// and streamed, and the guard's reason reaches the caller in no form.
// A run a guard stopped after the answer completes with it, or with the
// output guard's replacement, unless the replacement has no text.
func TestGuardStopBeforeAnswerIsARefusal(t *testing.T) {
	const reason = "homework tripwire"
	refuse := fmt.Errorf("%w: %s", agentturn.ErrGuard, reason)
	onToolOutput := func(_ context.Context, req *openresponses.Request) error {
		for _, item := range req.Input {
			if _, ok := item.(*openresponses.FunctionCallOutput); ok {
				return refuse
			}
		}
		return nil
	}
	afterTurn := func(context.Context, agentturn.TurnInfo) (bool, error) { return false, refuse }
	withhold := func(context.Context, agentturn.OutputInfo) (*openresponses.Message, error) { return nil, refuse }
	lookup := agenttool.New("lookup", "", func(context.Context, agenttool.NoArgs) (string, error) { return "found", nil })
	callerTool := openresponses.NewFunctionTool("remote", "caller owned", json.RawMessage(`{"type":"object"}`))
	for _, tc := range []struct {
		name   string
		cfg    agentturn.Config
		caller bool
		want   string
	}{
		{"before turn", agentturn.Config{BeforeTurn: func(context.Context, agentturn.TurnStartInfo) (openresponses.Items, error) {
			return nil, refuse
		}}, false, ""},
		{"before model call", agentturn.Config{BeforeModelCall: func(context.Context, *openresponses.Request) error {
			return refuse
		}}, false, ""},
		{"before model call, single turn", agentturn.Config{BeforeModelCall: func(context.Context, *openresponses.Request) error {
			return refuse
		}}, true, ""},
		{"before turn 2, after a preamble", agentturn.Config{Model: preamble{}, Tools: []agenttool.Tool{lookup}, BeforeTurn: func(_ context.Context, info agentturn.TurnStartInfo) (openresponses.Items, error) {
			if info.Turn > 1 {
				return nil, refuse
			}
			return nil, nil
		}}, false, ""},
		{"before model call 2, after a preamble", agentturn.Config{Model: preamble{}, Tools: []agenttool.Tool{lookup}, BeforeModelCall: onToolOutput}, false, ""},
		{"after a turn that called tools", agentturn.Config{Model: preamble{}, Tools: []agenttool.Tool{lookup}, ShouldStopAfterTurn: afterTurn}, false, ""},
		{"output guard", agentturn.Config{OutputGuard: withhold}, false, ""},
		{"output guard, single turn", agentturn.Config{Model: answering{}, OutputGuard: withhold}, true, ""},
		{"after the answer", agentturn.Config{ShouldStopAfterTurn: afterTurn}, false, "solve x"},
		{"after the answer, withheld", agentturn.Config{ShouldStopAfterTurn: afterTurn, OutputGuard: func(context.Context, agentturn.OutputInfo) (*openresponses.Message, error) {
			return openresponses.AssistantText("[withheld]"), nil
		}}, false, "[withheld]"},
		{"after the answer, withheld as nothing", agentturn.Config{ShouldStopAfterTurn: afterTurn, OutputGuard: func(context.Context, agentturn.OutputInfo) (*openresponses.Message, error) {
			return openresponses.AssistantText(""), nil
		}}, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			if cfg.Model == nil {
				cfg.Model = &echo.Adapter{}
			}
			cfg.ModelName = "m"
			srv := httptest.NewServer(openresponses.NewHandler(New(cfg)))
			defer srv.Close()
			req := request(openresponses.UserText("solve x"))
			if tc.caller {
				req.Tools = openresponses.Tools{callerTool}
			}

			status, body := post(t, srv.URL, req)
			if strings.Contains(body, reason) {
				t.Errorf("collected body carries the guard's reason: %s", body)
			}
			var resp openresponses.Response
			if err := json.Unmarshal([]byte(body), &resp); err != nil || status != http.StatusOK {
				t.Fatalf("collected: %d %s (%v)", status, body, err)
			}
			if tc.want == "" {
				if resp.Status != openresponses.ResponseStatusIncomplete || resp.IncompleteDetails == nil || resp.IncompleteDetails.Reason != openresponses.IncompleteReasonContentFilter {
					t.Errorf("collected = %s %+v, want incomplete content_filter", resp.Status, resp.IncompleteDetails)
				}
				// A message OutputGuard withheld keeps its place, emptied.
				if strings.Contains(resp.OutputText(), "solve x") {
					t.Errorf("collected output holds the refused answer: %q", resp.OutputText())
				}
			} else if resp.Status != openresponses.ResponseStatusCompleted || resp.OutputText() != tc.want {
				t.Errorf("collected = %s %q, want completed %q", resp.Status, resp.OutputText(), tc.want)
			}

			req.Stream = true
			status, body = post(t, srv.URL, req)
			if strings.Contains(body, reason) {
				t.Errorf("stream carries the guard's reason: %s", body)
			}
			terminal := "event: response.completed"
			if tc.want == "" {
				terminal = "event: response.incomplete"
			}
			if status != http.StatusOK || !strings.Contains(body, terminal) || strings.Contains(body, "event: error") {
				t.Errorf("streamed: %d, want %q and no error:\n%s", status, terminal, body)
			}
		})
	}
}

// post sends req to a handler at base and returns the status and body.
func post(t *testing.T, base string, req openresponses.Request) (int, string) {
	t.Helper()
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.Post(base+"/v1/responses", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	out, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, string(out)
}

// answering is the echo model offered no tools, so it answers in text
// whatever the caller declares.
type answering struct{}

func (answering) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	req.Tools = nil
	return (&echo.Adapter{}).CreateStream(ctx, req, sink)
}

// preamble says something and calls the first tool it is offered, then
// echoes once a tool has answered.
type preamble struct{}

func (preamble) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	if _, ok := req.Input[len(req.Input)-1].(*openresponses.FunctionCallOutput); ok {
		return (&echo.Adapter{}).CreateStream(ctx, req, sink)
	}
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if err := em.Item(openresponses.AssistantText("Let me check.")); err != nil {
		return err
	}
	w, err := em.FunctionCall("", req.Tools[0].(*openresponses.FunctionTool).Name)
	if err != nil {
		return err
	}
	if err := w.Arguments(`{}`); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Complete()
}
