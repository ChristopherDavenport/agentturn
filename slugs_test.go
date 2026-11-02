package agentturn

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
)

// strictUpstream stands for a provider's own Open Responses API: it
// refuses an input item whose type it does not define, as OpenAI's does
// an item under another provider's slug, and records the types it was
// sent.
type strictUpstream struct {
	echo.Adapter
	sent [][]string
}

func (u *strictUpstream) CreateStream(ctx context.Context, req openresponses.Request, sink openresponses.EventSink) error {
	var types []string
	for _, item := range req.Input {
		types = append(types, item.ItemType())
		if strings.Contains(item.ItemType(), ".") {
			return openresponses.InvalidRequest("invalid_value", "unknown input item type "+item.ItemType(), "input")
		}
	}
	u.sent = append(u.sent, types)
	return u.Adapter.CreateStream(ctx, req, sink)
}

// TestAnotherProvidersItemIsLeftOutByTheAdapter pins #285 as the loop
// meets it: a transcript holding Claude's anthropic.redacted_thinking
// block, which the loop's filter keeps, since a provider slug is not an
// app-only one, moves to a model behind a provider's own API. The
// loop sends the item as the transcript holds it, and the adapter leaves
// it out (openresponses docs/extensions.md section 1.6): a ClientAdapter
// does so once WithProviderSlugs says what the upstream accepts, so the
// run succeeds; one not told forwards it, and the upstream refuses it.
func TestAnotherProvidersItemIsLeftOutByTheAdapter(t *testing.T) {
	claude := openresponses.Items{
		openresponses.UserText("think hard"),
		&openresponses.UnknownItem{Type: "anthropic.redacted_thinking", Raw: json.RawMessage(`{"type":"anthropic.redacted_thinking","data":"EmwKAhgB"}`)},
		openresponses.AssistantText("done"),
	}
	for _, tc := range []struct {
		name string
		opts []openresponses.AdapterOption
		want Reason
	}{
		{name: "told the upstream accepts no provider's slug", opts: []openresponses.AdapterOption{openresponses.WithProviderSlugs()}, want: ReasonDone},
		{name: "not told", want: ReasonError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := &strictUpstream{}
			srv := httptest.NewServer(openresponses.NewHandler(up))
			defer srv.Close()
			var sent openresponses.Items
			a := New(Config{Model: openresponses.NewClient(srv.URL).AsAdapter(tc.opts...), ModelName: "gpt",
				BeforeModelCall: func(_ context.Context, req *openresponses.Request) error {
					sent = req.Input
					return nil
				}}, WithTranscript(claude))
			end, err := a.Prompt(context.Background(), openresponses.UserText("go on"))
			if end == nil {
				t.Fatal(err)
			}
			if end.Reason != tc.want {
				t.Fatalf("end = %s (%v), want %s", end.Reason, end.Err, tc.want)
			}
			// The loop sends the item: leaving it out is the adapter's.
			if len(sent) != 4 || sent[1].ItemType() != "anthropic.redacted_thinking" {
				t.Errorf("the loop's request input = %v", itemTypes(sent))
			}
			if tc.want == ReasonDone && (len(up.sent) != 1 || strings.Join(up.sent[0], " ") != "message message message") {
				t.Errorf("the upstream was sent %v", up.sent)
			}
		})
	}
}
