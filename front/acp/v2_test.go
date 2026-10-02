package acp

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	acpgo "github.com/ironpark/acp-go"
	"github.com/ironpark/acp-go/acp1"
	"github.com/ironpark/acp-go/acp1/acp1test"
	"github.com/ironpark/acp-go/acp2"
	"github.com/ironpark/acp-go/acp2/acp2test"
)

// openV2 connects a v2 client to a server whose sessions are built by
// fn, and starts a session.
func openV2(t *testing.T, fn NewSessionFunc, client *acp2test.Client) *acp2.ClientSession {
	t.Helper()
	conn := acp2test.Connect(t, New(fn, WithInfo("test", "v0")).AgentV2, client)
	ctx := context.Background()
	init, err := conn.Initialize(ctx, &acp2.InitializeRequest{})
	if err != nil {
		t.Fatalf("initialize: %v", err)
	}
	if init.Info.Name != "test" {
		t.Errorf("agent name = %q, want test", init.Info.Name)
	}
	sess, err := conn.StartSession(ctx, &acp2.NewSessionRequest{Cwd: "/work"})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	return sess
}

func agentOf(cfg agentturn.Config, into **agentturn.Agent) NewSessionFunc {
	return func(context.Context, Session) (*agentturn.Agent, error) {
		a := agentturn.New(cfg)
		if into != nil {
			*into = a
		}
		return a, nil
	}
}

// states returns the turn states the session reported, in order: idle
// with its stop reason.
func states(client *acp2test.Client) []string {
	var out []string
	for _, n := range client.Updates() {
		su, ok := n.Update.As[acp2.SessionUpdateStateUpdate]()
		if !ok {
			continue
		}
		switch v := su.Value.Variant().(type) {
		case acp2.StateUpdateRunning:
			out = append(out, "running")
		case acp2.StateUpdateRequiresAction:
			out = append(out, "requires_action")
		case acp2.StateUpdateIdle:
			out = append(out, "idle:"+string(*v.StopReason))
		}
	}
	return out
}

// userMessages returns the IDs of the user messages the session echoed.
func userMessages(client *acp2test.Client) []acp2.MessageID {
	var out []acp2.MessageID
	for _, n := range client.Updates() {
		if um, ok := n.Update.As[acp2.SessionUpdateUserMessage](); ok {
			out = append(out, um.MessageID)
		}
	}
	return out
}

// lastToolStatus returns the last status the session reported for any
// tool call.
func lastToolStatus(client *acp2test.Client) acp2.ToolCallStatus {
	var last acp2.ToolCallStatus
	for _, n := range client.Updates() {
		if tu, ok := n.Update.As[acp2.SessionUpdateToolCallUpdate](); ok {
			if st, ok := tu.Status.Get(); ok {
				last = st
			}
		}
	}
	return last
}

func countUser(a *agentturn.Agent, text string) int {
	n := 0
	for _, item := range a.State().Transcript {
		if m, ok := item.(*openresponses.Message); ok && m.Role == openresponses.RoleUser && m.Text() == text {
			n++
		}
	}
	return n
}

func TestV2PromptRunsToIdle(t *testing.T) {
	client := &acp2test.Client{}
	sess := openV2(t, agentOf(agentturn.Config{Model: &echo.Adapter{}, ModelName: "echo"}, nil), client)

	turn, id, err := sess.Prompt(context.Background(), acp2.TextBlock("hello there"))
	if err != nil {
		t.Fatalf("prompt: %v", err)
	}
	reason, err := turn.Wait()
	if err != nil || reason != acp2.StopReasonEndTurn {
		t.Fatalf("turn = %q, %v; want end_turn", reason, err)
	}
	if text := client.Text(sess.ID); !strings.Contains(text, "hello there") {
		t.Errorf("streamed %q, want the echo of the prompt", text)
	}
	if got := userMessages(client); len(got) != 1 || got[0] != id {
		t.Errorf("user messages = %v, want the prompt's %q", got, id)
	}
	if got := strings.Join(states(client), " "); got != "running idle:end_turn" {
		t.Errorf("states = %q", got)
	}
	var usage *acp2.Usage
	for _, n := range client.Updates() {
		if su, ok := n.Update.As[acp2.SessionUpdateStateUpdate](); ok {
			if idle, ok := su.Value.Variant().(acp2.StateUpdateIdle); ok {
				usage = idle.Usage
			}
		}
	}
	if usage == nil || usage.TotalTokens == 0 {
		t.Errorf("idle usage = %+v, want the response's", usage)
	}
}

// TestV2SteerJoinsTheRunningTurn prompts while a tool runs: the second
// prompt joins the turn, is answered once the run takes the message
// after the tool's batch, and the turn goes idle once.
func TestV2SteerJoinsTheRunningTurn(t *testing.T) {
	release := make(chan struct{})
	var first sync.Once
	slow := agenttool.New("look", "Look at something", func(_ context.Context, a lookArgs) (string, error) {
		first.Do(func() { <-release })
		return "seen " + a.Path, nil
	})
	var agent *agentturn.Agent
	client := &acp2test.Client{}
	sess := openV2(t, agentOf(agentturn.Config{Model: &echo.Adapter{}, ModelName: "echo", Tools: []agenttool.Tool{slow}}, &agent), client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	turn, _, err := sess.Prompt(ctx, acp2.TextBlock("first"))
	if err != nil {
		t.Fatalf("prompt: %v", err)
	}
	if _, err := client.WaitFor(ctx, func(n *acp2.UpdateSessionNotification) bool {
		tu, ok := n.Update.As[acp2.SessionUpdateToolCallUpdate]()
		st, _ := tu.Status.Get()
		return ok && st == acp2.ToolCallStatusInProgress
	}); err != nil {
		t.Fatalf("the tool never ran: %v", err)
	}
	type answer struct {
		id  acp2.MessageID
		err error
	}
	second := make(chan answer, 1)
	go func() {
		_, id, err := sess.Prompt(ctx, acp2.TextBlock("second"))
		second <- answer{id, err}
	}()
	// Let the steer land before the tool returns, so the batch drains it.
	for agent.State().Steering == 0 {
		time.Sleep(time.Millisecond)
	}
	close(release)
	got := <-second
	if got.err != nil {
		t.Fatalf("second prompt: %v", got.err)
	}
	reason, err := turn.Wait()
	if err != nil || reason != acp2.StopReasonEndTurn {
		t.Fatalf("turn = %q, %v; want end_turn", reason, err)
	}
	if ids := userMessages(client); len(ids) != 2 || ids[1] != got.id {
		t.Errorf("user messages = %v, want the second's %q last", ids, got.id)
	}
	if s := strings.Join(states(client), " "); s != "running idle:end_turn" {
		t.Errorf("states = %q, want one turn", s)
	}
	if n := countUser(agent, "second"); n != 1 {
		t.Errorf("the steered message is in the transcript %d times, want 1", n)
	}
	if text := client.Text(sess.ID); !strings.Contains(text, "second") {
		t.Errorf("streamed %q, want the steered message answered", text)
	}
}

// TestV2SteerAfterTheLastDrain prompts after the run has taken its last
// steer but before the turn ends: the message waits in the agent's
// queue, and the turn runs again for it rather than going idle.
func TestV2SteerAfterTheLastDrain(t *testing.T) {
	var agent *agentturn.Agent
	reached := make(chan struct{})
	var once atomic.Bool
	fn := func(context.Context, Session) (*agentturn.Agent, error) {
		agent = agentturn.New(agentturn.Config{Model: &echo.Adapter{}, ModelName: "echo"})
		agent.Subscribe(func(_ context.Context, ev agentturn.Event) error {
			if _, ok := ev.(*agentturn.RunEnd); !ok || !once.CompareAndSwap(false, true) {
				return nil
			}
			close(reached)
			// Hold the run's end until the second prompt is queued.
			deadline := time.Now().Add(5 * time.Second)
			for agent.State().Steering == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			return nil
		})
		return agent, nil
	}
	client := &acp2test.Client{}
	sess := openV2(t, fn, client)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	turn, _, err := sess.Prompt(ctx, acp2.TextBlock("first"))
	if err != nil {
		t.Fatalf("prompt: %v", err)
	}
	<-reached
	_, id, err := sess.Prompt(ctx, acp2.TextBlock("second"))
	if err != nil {
		t.Fatalf("second prompt: %v", err)
	}
	reason, err := turn.Wait()
	if err != nil || reason != acp2.StopReasonEndTurn {
		t.Fatalf("turn = %q, %v; want end_turn", reason, err)
	}
	if ids := userMessages(client); len(ids) != 2 || ids[1] != id {
		t.Errorf("user messages = %v, want the second's %q last", ids, id)
	}
	if s := strings.Join(states(client), " "); s != "running idle:end_turn" {
		t.Errorf("states = %q, want one turn", s)
	}
	if text := client.Text(sess.ID); !strings.Contains(text, "second") {
		t.Errorf("streamed %q, want the second message answered", text)
	}
}

func TestV2PermissionRequiresAction(t *testing.T) {
	var runs atomic.Int32
	client := &acp2test.Client{}
	cfg := agentturn.Config{
		Model: &echo.Adapter{}, ModelName: "echo", Tools: []agenttool.Tool{look(&runs)},
		BeforeToolCall: func(context.Context, agentturn.ToolCallInfo) (*agentturn.ToolDecision, error) {
			return &agentturn.ToolDecision{Action: agentturn.Defer, Reason: "looking needs a yes"}, nil
		},
	}
	sess := openV2(t, agentOf(cfg, nil), client)

	turn, _, err := sess.Prompt(context.Background(), acp2.TextBlock("the shed"))
	if err != nil {
		t.Fatalf("prompt: %v", err)
	}
	if reason, err := turn.Wait(); err != nil || reason != acp2.StopReasonEndTurn {
		t.Fatalf("turn = %q, %v; want end_turn", reason, err)
	}
	if runs.Load() != 1 {
		t.Errorf("tool ran %d times, want 1", runs.Load())
	}
	if s := strings.Join(states(client), " "); s != "running requires_action running idle:end_turn" {
		t.Errorf("states = %q", s)
	}
	asked := client.Permissions()
	if len(asked) != 1 {
		t.Fatalf("asked %d permissions, want 1", len(asked))
	}
	if asked[0].Title != "look" {
		t.Errorf("permission title = %q, want the tool's", asked[0].Title)
	}
	if st := lastToolStatus(client); st != acp2.ToolCallStatusCompleted {
		t.Errorf("tool call ended %q, want completed", st)
	}
}

func TestV2CancelWhileAsking(t *testing.T) {
	var runs atomic.Int32
	asking := make(chan struct{}, 1)
	release := make(chan struct{})
	client := &acp2test.Client{Permission: func(*acp2.RequestPermissionRequest) *acp2.RequestPermissionResponse {
		asking <- struct{}{}
		<-release
		return acp2.PermissionCancelled()
	}}
	var deferred atomic.Bool
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
	sess := openV2(t, agentOf(cfg, nil), client)
	ctx := context.Background()

	turn, _, err := sess.Prompt(ctx, acp2.TextBlock("the attic"))
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
	if reason, err := turn.Wait(); err != nil || reason != acp2.StopReasonCancelled {
		t.Fatalf("turn = %q, %v; want cancelled", reason, err)
	}
	if runs.Load() != 0 {
		t.Errorf("tool ran %d times, want 0", runs.Load())
	}
	if st := lastToolStatus(client); st != acp2.ToolCallStatusCancelled {
		t.Errorf("tool call ended %q, want cancelled", st)
	}

	deferred.Store(false)
	next, _, err := sess.Prompt(ctx, acp2.TextBlock("never mind"))
	if err != nil {
		t.Fatalf("next prompt: %v", err)
	}
	if reason, err := next.Wait(); err != nil || reason != acp2.StopReasonEndTurn {
		t.Errorf("next turn = %q, %v; want end_turn", reason, err)
	}
}

func TestMessageV2(t *testing.T) {
	msg, err := messageV2([]acp2.ContentBlock{
		acp2.TextBlock("see"),
		acp2.NewContentBlock(acp2.ContentBlockImage{MIMEType: "image/png", Data: "iVBO"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(msg.Content) != 2 {
		t.Fatalf("content = %d parts, want 2", len(msg.Content))
	}
	if img, ok := msg.Content[1].(*openresponses.InputImage); !ok || img.ImageURL != "data:image/png;base64,iVBO" {
		t.Errorf("image part = %+v", msg.Content[1])
	}
	if _, err := messageV2(nil); err == nil {
		t.Error("empty prompt accepted")
	}
}

// TestServeSpeaksBothVersions runs Serve over a pipe, once with a v1
// client and once with a v2 one.
func TestServeSpeaksBothVersions(t *testing.T) {
	srv := New(agentOf(agentturn.Config{Model: &echo.Adapter{}, ModelName: "echo"}, nil))
	connect := func(t *testing.T) acpgo.Transport {
		ctx, cancel := context.WithCancel(context.Background())
		agentIn, clientOut := io.Pipe()
		clientIn, agentOut := io.Pipe()
		go func() { _ = srv.Serve(ctx, acpgo.NewStdioTransport(agentIn, agentOut)) }()
		t.Cleanup(func() {
			cancel()
			_ = agentIn.Close()
			_ = clientIn.Close()
		})
		return acpgo.NewStdioTransport(clientIn, clientOut)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("v1", func(t *testing.T) {
		client := &acp1test.Client{}
		agent := acp1.ConnectAgent(ctx, connect(t), func(*acp1.ClientSideConnection) acp1.Client { return client })
		defer agent.Close()
		if _, err := agent.Initialize(ctx, &acp1.InitializeRequest{}); err != nil {
			t.Fatal(err)
		}
		sess, err := agent.StartSession(ctx, &acp1.NewSessionRequest{Cwd: "/work"})
		if err != nil {
			t.Fatal(err)
		}
		turn, err := sess.Prompt(ctx, acp1.TextBlock("over v1"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := turn.Wait(); err != nil {
			t.Fatal(err)
		}
		if text := client.Text(sess.ID); !strings.Contains(text, "over v1") {
			t.Errorf("streamed %q", text)
		}
	})
	t.Run("v2", func(t *testing.T) {
		client := &acp2test.Client{}
		agent := acp2.ConnectAgent(ctx, connect(t), func(*acp2.ClientSideConnection) acp2.Client { return client })
		defer agent.Close()
		if _, err := agent.Initialize(ctx, &acp2.InitializeRequest{}); err != nil {
			t.Fatal(err)
		}
		sess, err := agent.StartSession(ctx, &acp2.NewSessionRequest{Cwd: "/work"})
		if err != nil {
			t.Fatal(err)
		}
		turn, _, err := sess.Prompt(ctx, acp2.TextBlock("over v2"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := turn.Wait(); err != nil {
			t.Fatal(err)
		}
		if text := client.Text(sess.ID); !strings.Contains(text, "over v2") {
			t.Errorf("streamed %q", text)
		}
	})
}
