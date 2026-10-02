package acp

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	"github.com/ironpark/acp-go/acp1"
	"github.com/ironpark/acp-go/acp1/acp1test"
)

type lookArgs struct {
	Path string `json:"path" desc:"What to look at"`
}

// look is a tool that counts its runs and answers "seen <path>".
func look(runs *atomic.Int32, opts ...agenttool.Option) agenttool.Tool {
	return agenttool.New("look", "Look at something",
		func(_ context.Context, a lookArgs) (string, error) {
			runs.Add(1)
			return "seen " + a.Path, nil
		}, opts...)
}

// open connects a client to a server whose sessions run cfg, and starts
// a session.
func open(t *testing.T, cfg agentturn.Config, client *acp1test.Client) *acp1.ClientSession {
	t.Helper()
	srv := New(func(context.Context, Session) (*agentturn.Agent, error) {
		return agentturn.New(cfg), nil
	}, WithInfo("test", "v0"))
	conn := acp1test.Connect(t, srv.AgentV1, client)
	ctx := context.Background()
	init, err := conn.Initialize(ctx, &acp1.InitializeRequest{})
	if err != nil {
		t.Fatalf("initialize: %v", err)
	}
	if got := init.GetAgentInfo().Name; got != "test" {
		t.Errorf("agent name = %q, want test", got)
	}
	sess, err := conn.StartSession(ctx, &acp1.NewSessionRequest{Cwd: "/work"})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	return sess
}

func prompt(t *testing.T, sess *acp1.ClientSession, text string) *acp1.PromptResponse {
	t.Helper()
	turn, err := sess.Prompt(context.Background(), acp1.TextBlock(text))
	if err != nil {
		t.Fatalf("prompt: %v", err)
	}
	resp, err := turn.Wait()
	if err != nil {
		t.Fatalf("turn: %v", err)
	}
	return resp
}

// toolUpdates returns the statuses the session reported for callID, in
// order, with the text of the last content.
func toolUpdates(client *acp1test.Client) (statuses []acp1.ToolCallStatus, content string) {
	for _, n := range client.Updates() {
		if tc, ok := n.Update.As[acp1.SessionUpdateToolCall](); ok {
			statuses = append(statuses, tc.GetStatus())
			content = contentText(tc.Content, content)
		}
		if tu, ok := n.Update.As[acp1.SessionUpdateToolCallUpdate](); ok {
			statuses = append(statuses, tu.GetStatus())
			content = contentText(tu.Content, content)
		}
	}
	return statuses, content
}

func contentText(content []acp1.ToolCallContent, last string) string {
	for _, c := range content {
		if v, ok := c.As[acp1.ToolCallContentContent](); ok {
			if text, ok := acp1.TextOf(v.Content); ok {
				last = text
			}
		}
	}
	return last
}

func TestPromptStreamsTheAnswer(t *testing.T) {
	client := &acp1test.Client{}
	sess := open(t, agentturn.Config{Model: &echo.Adapter{}, ModelName: "echo"}, client)

	resp := prompt(t, sess, "hello there")
	if resp.StopReason != acp1.StopReasonEndTurn {
		t.Errorf("stop reason = %q, want end_turn", resp.StopReason)
	}
	if text := client.Text(sess.ID); !strings.Contains(text, "hello there") {
		t.Errorf("streamed %q, want the echo of the prompt", text)
	}
	if resp.Usage == nil || resp.Usage.TotalTokens == 0 {
		t.Errorf("usage = %+v, want the response's", resp.Usage)
	}
}

func TestToolCallLifecycle(t *testing.T) {
	var runs atomic.Int32
	client := &acp1test.Client{}
	sess := open(t, agentturn.Config{Model: &echo.Adapter{}, ModelName: "echo", Tools: []agenttool.Tool{look(&runs)}}, client)

	prompt(t, sess, "the garden")
	if runs.Load() != 1 {
		t.Fatalf("tool ran %d times, want 1", runs.Load())
	}
	statuses, content := toolUpdates(client)
	want := []acp1.ToolCallStatus{acp1.ToolCallStatusPending, acp1.ToolCallStatusInProgress, acp1.ToolCallStatusCompleted}
	if !equal(statuses, want) {
		t.Errorf("statuses = %v, want %v", statuses, want)
	}
	if !strings.HasPrefix(content, "seen ") {
		t.Errorf("tool content = %q, want the tool's output", content)
	}
	if len(client.Permissions()) != 0 {
		t.Errorf("asked %d permissions, want none", len(client.Permissions()))
	}
}

func TestPermission(t *testing.T) {
	cases := []struct {
		name     string
		answer   func(*acp1.RequestPermissionRequest) *acp1.RequestPermissionResponse
		runs     int32
		statuses []acp1.ToolCallStatus
	}{
		{
			name:   "allowed runs the call",
			answer: acp1test.AllowOnce,
			runs:   1,
			// proposed, asked about, approved and announced again, dispatched, done
			statuses: []acp1.ToolCallStatus{acp1.ToolCallStatusPending, acp1.ToolCallStatusPending, acp1.ToolCallStatusInProgress, acp1.ToolCallStatusCompleted},
		},
		{
			name:     "rejected does not",
			answer:   acp1test.Reject,
			runs:     0,
			statuses: []acp1.ToolCallStatus{acp1.ToolCallStatusPending, acp1.ToolCallStatusFailed},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var runs atomic.Int32
			client := &acp1test.Client{Permission: tc.answer}
			cfg := agentturn.Config{
				Model: &echo.Adapter{}, ModelName: "echo", Tools: []agenttool.Tool{look(&runs)},
				BeforeToolCall: func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
					return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "looking needs a yes"}, nil
				},
			}
			sess := open(t, cfg, client)

			resp := prompt(t, sess, "the shed")
			if resp.StopReason != acp1.StopReasonEndTurn {
				t.Errorf("stop reason = %q, want end_turn", resp.StopReason)
			}
			if runs.Load() != tc.runs {
				t.Errorf("tool ran %d times, want %d", runs.Load(), tc.runs)
			}
			asked := client.Permissions()
			if len(asked) != 1 {
				t.Fatalf("asked %d permissions, want 1", len(asked))
			}
			if got := contentText(asked[0].ToolCall.Content, ""); got != "looking needs a yes" {
				t.Errorf("permission content = %q, want the deferral's reason", got)
			}
			statuses, _ := toolUpdates(client)
			if !equal(statuses, tc.statuses) {
				t.Errorf("statuses = %v, want %v", statuses, tc.statuses)
			}
		})
	}
}

// TestCancelWhileAsking cancels the turn while the client is choosing:
// the turn ends cancelled, the call stays pending, and the next prompt
// answers it and goes ahead.
func TestCancelWhileAsking(t *testing.T) {
	var runs atomic.Int32
	asking := make(chan struct{}, 1)
	release := make(chan struct{})
	client := &acp1test.Client{Permission: func(*acp1.RequestPermissionRequest) *acp1.RequestPermissionResponse {
		asking <- struct{}{}
		<-release
		return acp1.PermissionCancelled()
	}}
	deferred := atomic.Bool{}
	deferred.Store(true)
	cfg := agentturn.Config{
		Model: &echo.Adapter{}, ModelName: "echo", Tools: []agenttool.Tool{look(&runs)},
		BeforeToolCall: func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			if deferred.Load() {
				return &agentturn.ToolDecision{Action: agentturn.Defer}, nil
			}
			return nil, nil
		},
	}
	sess := open(t, cfg, client)

	ctx := context.Background()
	turn, err := sess.Prompt(ctx, acp1.TextBlock("the attic"))
	if err != nil {
		t.Fatalf("prompt: %v", err)
	}
	select {
	case <-asking:
	case <-time.After(5 * time.Second):
		t.Fatal("no permission request")
	}
	if err := sess.Cancel(ctx); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	close(release)
	resp, err := turn.Wait()
	if err != nil {
		t.Fatalf("turn: %v", err)
	}
	if resp.StopReason != acp1.StopReasonCancelled {
		t.Errorf("stop reason = %q, want cancelled", resp.StopReason)
	}
	if runs.Load() != 0 {
		t.Errorf("tool ran %d times, want 0", runs.Load())
	}
	statuses, content := toolUpdates(client)
	if len(statuses) == 0 || statuses[len(statuses)-1] != acp1.ToolCallStatusFailed || content != NotRunOutput {
		t.Errorf("statuses = %v with %q, want the call failed as not run", statuses, content)
	}

	deferred.Store(false)
	if resp := prompt(t, sess, "never mind"); resp.StopReason != acp1.StopReasonEndTurn {
		t.Errorf("next stop reason = %q, want end_turn", resp.StopReason)
	}
}

func TestMessageV1(t *testing.T) {
	text, err := acp1.NewEmbeddedResourceResource(acp1.TextResourceContents{URI: "file:///a.go", Text: "package a"})
	if err != nil {
		t.Fatal(err)
	}
	blob, err := acp1.NewEmbeddedResourceResource(acp1.BlobResourceContents{URI: "file:///a.bin", Blob: "AA=="})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		in    []acp1.ContentBlock
		parts []string
		err   bool
	}{
		{name: "text", in: []acp1.ContentBlock{acp1.TextBlock("hi")}, parts: []string{"hi"}},
		{
			name:  "resource link",
			in:    []acp1.ContentBlock{acp1.NewContentBlock(acp1.ContentBlockResourceLink{Name: "a.go", URI: "file:///a.go"})},
			parts: []string{"[a.go](file:///a.go)"},
		},
		{
			name:  "text resource",
			in:    []acp1.ContentBlock{acp1.TextBlock("see"), acp1.NewContentBlock(acp1.ContentBlockResource{Resource: text})},
			parts: []string{"see", "<context ref=\"file:///a.go\">\npackage a\n</context>"},
		},
		{
			name:  "image",
			in:    []acp1.ContentBlock{acp1.NewContentBlock(acp1.ContentBlockImage{MIMEType: "image/png", Data: "iVBO"})},
			parts: []string{"image:data:image/png;base64,iVBO"},
		},
		{name: "blob resource", in: []acp1.ContentBlock{acp1.NewContentBlock(acp1.ContentBlockResource{Resource: blob})}, err: true},
		{name: "audio", in: []acp1.ContentBlock{acp1.NewContentBlock(acp1.ContentBlockAudio{MIMEType: "audio/wav", Data: "AA=="})}, err: true},
		{name: "empty", in: nil, err: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg, err := messageV1(tc.in)
			if tc.err {
				if err == nil {
					t.Fatalf("message = %+v, want an error", msg)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var parts []string
			for _, c := range msg.Content {
				switch p := c.(type) {
				case *openresponses.InputText:
					parts = append(parts, p.Text)
				case *openresponses.InputImage:
					parts = append(parts, "image:"+p.ImageURL)
				}
			}
			if !equal(parts, tc.parts) {
				t.Errorf("parts = %q, want %q", parts, tc.parts)
			}
		})
	}
}

func TestStopReason(t *testing.T) {
	cases := []struct {
		end  agentturn.RunEnd
		want acp1.StopReason
	}{
		{agentturn.RunEnd{Reason: agentturn.ReasonDone}, acp1.StopReasonEndTurn},
		{agentturn.RunEnd{Reason: agentturn.ReasonAborted}, acp1.StopReasonCancelled},
		{agentturn.RunEnd{Reason: agentturn.ReasonStopped, Cause: agentturn.StopGuard}, acp1.StopReasonRefusal},
		{agentturn.RunEnd{Reason: agentturn.ReasonStopped, Cause: agentturn.StopMaxTurns}, acp1.StopReasonMaxTurnRequests},
		{agentturn.RunEnd{Reason: agentturn.ReasonStopped, Cause: agentturn.StopRefused}, acp1.StopReasonEndTurn},
		{agentturn.RunEnd{Reason: agentturn.ReasonStopped, Cause: agentturn.StopTerminate}, acp1.StopReasonEndTurn},
	}
	for _, tc := range cases {
		// v2 spells every one of these the same.
		if got := acp1.StopReason(stopReason(&tc.end)); got != tc.want {
			t.Errorf("%s/%s = %q, want %q", tc.end.Reason, tc.end.Cause, got, tc.want)
		}
	}
}

func TestSessionFunctionFails(t *testing.T) {
	boom := errors.New("boom")
	cases := []struct {
		name string
		fn   NewSessionFunc
	}{
		{"error", func(context.Context, Session) (*agentturn.Agent, error) { return nil, boom }},
		{"no agent", func(context.Context, Session) (*agentturn.Agent, error) { return nil, nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := acp1test.Connect(t, New(tc.fn).AgentV1, &acp1test.Client{})
			ctx := context.Background()
			if _, err := conn.Initialize(ctx, &acp1.InitializeRequest{}); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.StartSession(ctx, &acp1.NewSessionRequest{Cwd: "/work"}); err == nil {
				t.Fatal("new session succeeded, want an error")
			}
		})
	}
}

func TestDefaultToolKind(t *testing.T) {
	var runs atomic.Int32
	readOnly := look(&runs, agenttool.WithAnnotations(agenttool.Annotations{ReadOnly: true}))
	if got := DefaultToolKind("look", readOnly); got != ToolKindRead {
		t.Errorf("read-only kind = %q, want read", got)
	}
	if got := DefaultToolKind("look", look(&runs)); got != ToolKindOther {
		t.Errorf("kind = %q, want other", got)
	}
	if got := DefaultToolKind("gone", nil); got != ToolKindOther {
		t.Errorf("unknown tool kind = %q, want other", got)
	}
}

func equal[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestElicitorAsksAboutANestedCall has a tool invoke another whose call
// the hook defers: the question reaches the client as a permission
// request under the nested call's ID, announced with its parent.
func TestElicitorAsksAboutANestedCall(t *testing.T) {
	cases := []struct {
		name   string
		answer func(*acp1.RequestPermissionRequest) *acp1.RequestPermissionResponse
		runs   int32
		last   acp1.ToolCallStatus
	}{
		{"allowed", acp1test.AllowOnce, 1, acp1.ToolCallStatusCompleted},
		{"rejected", acp1test.Reject, 0, acp1.ToolCallStatusFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var runs atomic.Int32
			outer := agenttool.New("outer", "Look through another tool",
				func(ctx context.Context, a lookArgs) (string, error) {
					res, err := agentturn.Invoke(ctx, "look", []byte(`{"path":"inside"}`))
					if err != nil {
						return "", err
					}
					return "outer: " + res.Output.String(), nil
				})
			client := &acp1test.Client{Permission: tc.answer}
			cfg := agentturn.Config{
				Model: &echo.Adapter{}, ModelName: "echo",
				Tools: []agenttool.Tool{outer, look(&runs)},
				BeforeToolCall: func(_ context.Context, info agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
					if info.Parent != "" {
						return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "nested looks need a yes"}, nil
					}
					return nil, nil
				},
				ToolElicitor: Elicitor,
			}
			sess := open(t, cfg, client)

			prompt(t, sess, "the cellar")
			if runs.Load() != tc.runs {
				t.Errorf("nested tool ran %d times, want %d", runs.Load(), tc.runs)
			}
			asked := client.Permissions()
			if len(asked) != 1 {
				t.Fatalf("asked %d permissions, want 1", len(asked))
			}
			nested := asked[0].ToolCall.ToolCallID
			var parent string
			var last acp1.ToolCallStatus
			for _, n := range client.Updates() {
				if tc, ok := n.Update.As[acp1.SessionUpdateToolCall](); ok && tc.ToolCallID == nested {
					parent, _, _ = n.Meta.Get[string](MetaParent)
				}
				if tu, ok := n.Update.As[acp1.SessionUpdateToolCallUpdate](); ok && tu.ToolCallID == nested {
					last = tu.GetStatus()
				}
			}
			if parent == "" {
				t.Error("the nested call was announced with no parent")
			}
			if last != tc.last {
				t.Errorf("nested call ended %q, want %q", last, tc.last)
			}
		})
	}
}
