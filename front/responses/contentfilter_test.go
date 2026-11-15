package responses

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
)

// filtered answers as a provider's content filter does: a call to the
// first tool it is offered when call is set, then a message holding a
// refusal, ended incomplete with content_filter, the provider's
// category as an extension member of the details, and usage. A request
// that answers the call is a test failure.
type filtered struct{ call bool }

func (m filtered) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	if _, ok := req.Input[len(req.Input)-1].(*openresponses.FunctionCallOutput); ok {
		return errors.New("a refused response's call was answered")
	}
	next := sink
	sink = openresponses.EventSinkFunc(func(ev openresponses.StreamEvent) error {
		if e, ok := ev.(*openresponses.ResponseIncompleteEvent); ok {
			e.Response.IncompleteDetails.Extra = map[string]any{"anthropic.refusal_category": "cyber"}
		}
		return next.Send(ev)
	})
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	if m.call {
		w, err := em.FunctionCall("c1", req.Tools[0].(*openresponses.FunctionTool).Name)
		if err != nil {
			return err
		}
		if err := w.Arguments(`{}`); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
	}
	w, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if err := w.Refusal("I can't help with that."); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	em.Response().Usage = &openresponses.Usage{InputTokens: 3, OutputTokens: 2, TotalTokens: 5}
	return em.Incomplete(openresponses.IncompleteReasonContentFilter)
}

// TestAContentFilterResponsePassesThrough pins #314: a response the
// provider's content filter ended reaches the caller incomplete with
// the provider's incomplete_details, extension members included, and
// its usage, in a full run, whose agent runs none of its calls, and in
// a single turn with the caller's tools.
func TestAContentFilterResponsePassesThrough(t *testing.T) {
	ran := 0
	probe := agenttool.New("probe", "", func(context.Context, agenttool.NoArgs) (string, error) {
		ran++
		return "open", nil
	})
	callerTool := openresponses.NewFunctionTool("remote", "caller owned", json.RawMessage(`{"type":"object"}`))
	for _, tc := range []struct {
		name   string
		call   bool
		caller bool
	}{
		{"full run", false, false},
		{"full run, a call", true, false},
		{"single turn", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := agentturn.Config{Model: filtered{call: tc.call}, ModelName: "m", Tools: []agenttool.Tool{probe}}
			srv := httptest.NewServer(openresponses.NewHandler(New(cfg)))
			defer srv.Close()
			req := request(openresponses.UserText("probe a"))
			if tc.caller {
				req.Tools = openresponses.Tools{callerTool}
			}
			status, body := post(t, srv.URL, req)
			var resp openresponses.Response
			if err := json.Unmarshal([]byte(body), &resp); err != nil || status != http.StatusOK {
				t.Fatalf("collected: %d %s (%v)", status, body, err)
			}
			if resp.Status != openresponses.ResponseStatusIncomplete || resp.IncompleteDetails == nil || resp.IncompleteDetails.Reason != openresponses.IncompleteReasonContentFilter {
				t.Fatalf("collected = %s %+v, want incomplete content_filter", resp.Status, resp.IncompleteDetails)
			}
			if got := resp.IncompleteDetails.Extra["anthropic.refusal_category"]; got != "cyber" {
				t.Errorf("incomplete_details = %+v, want the provider's category", resp.IncompleteDetails)
			}
			if resp.Usage == nil || resp.Usage.TotalTokens != 5 {
				t.Errorf("usage = %+v, want the refused response's", resp.Usage)
			}
			if ran != 0 {
				t.Errorf("the agent's tool ran %d times", ran)
			}
			req.Stream = true
			status, body = post(t, srv.URL, req)
			if status != http.StatusOK || !strings.Contains(body, "event: response.incomplete") || !strings.Contains(body, `"anthropic.refusal_category":"cyber"`) {
				t.Errorf("streamed: %d, want response.incomplete with the category:\n%s", status, body)
			}
		})
	}
}

// cutShort answers with a message cut at the output limit.
type cutShort struct{}

func (cutShort) CreateStream(_ context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	em := openresponses.NewEmitter(sink, openresponses.NewResponse(req))
	w, err := em.Message(openresponses.PhaseFinalAnswer)
	if err != nil {
		return err
	}
	if err := w.Text("Once upon"); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return em.Incomplete(openresponses.IncompleteReasonMaxOutputTokens)
}

// TestACutAnswerEndsIncomplete pins #314's max_output_tokens decision
// in a full run: the caller's response holds the cut text and ends
// incomplete with reason max_output_tokens, as the model's did.
func TestACutAnswerEndsIncomplete(t *testing.T) {
	srv := httptest.NewServer(openresponses.NewHandler(New(agentturn.Config{Model: cutShort{}, ModelName: "m"})))
	defer srv.Close()
	status, body := post(t, srv.URL, request(openresponses.UserText("write at length")))
	var resp openresponses.Response
	if err := json.Unmarshal([]byte(body), &resp); err != nil || status != http.StatusOK {
		t.Fatalf("collected: %d %s (%v)", status, body, err)
	}
	if resp.Status != openresponses.ResponseStatusIncomplete || resp.IncompleteDetails == nil || resp.IncompleteDetails.Reason != openresponses.IncompleteReasonMaxOutputTokens {
		t.Errorf("collected = %s %+v, want incomplete max_output_tokens", resp.Status, resp.IncompleteDetails)
	}
	if resp.OutputText() != "Once upon" {
		t.Errorf("output = %q", resp.OutputText())
	}
}
