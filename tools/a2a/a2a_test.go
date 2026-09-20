package a2a

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	fronta2a "github.com/ChristopherDavenport/agentturn/front/a2a"
	"github.com/ChristopherDavenport/openresponses"
	"github.com/ChristopherDavenport/openresponses/echo"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
	"github.com/a2aproject/a2a-go/a2asrv"
	"github.com/a2aproject/a2a-go/a2asrv/eventqueue"
)

// serve exposes an executor over JSON-RPC on an httptest server and
// returns a client for it.
func serve(t *testing.T, exec a2asrv.AgentExecutor, card *a2a.AgentCard) (*a2aclient.Client, *a2a.AgentCard) {
	t.Helper()
	srv := httptest.NewServer(a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(exec)))
	t.Cleanup(srv.Close)
	if card == nil {
		card = &a2a.AgentCard{Name: "Remote", Description: "remote agent", Capabilities: a2a.AgentCapabilities{Streaming: true}}
	}
	card.URL = srv.URL
	card.PreferredTransport = a2a.TransportProtocolJSONRPC
	client, err := a2aclient.NewFromCard(context.Background(), card)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Destroy() })
	return client, card
}

func TestRoundTripThroughFront(t *testing.T) {
	remoteCfg := agentturn.Config{Name: "Echo Specialist", Description: "Echoes what it is told.", Model: &echo.Adapter{}, ModelName: "remote"}
	client, card := serve(t, fronta2a.New(remoteCfg), fronta2a.AgentCard(context.Background(), remoteCfg, "", ""))

	for _, mode := range []string{"streaming", "blocking"} {
		t.Run(mode, func(t *testing.T) {
			var opts []Option
			if mode == "blocking" {
				opts = append(opts, WithBlocking())
			}
			remote := New(client, card, opts...)
			if remote.Name() != "echo_specialist" || !strings.Contains(remote.Description(), "Echoes what it is told.") {
				t.Errorf("name = %q description = %q", remote.Name(), remote.Description())
			}
			var updates, ends int
			var final *agentturn.RunEnd
			local := agentturn.Config{Model: &echo.Adapter{}, ModelName: "local", Tools: []agenttool.Tool{remote}}
			for ev := range agentturn.Run(context.Background(), nil, openresponses.Items{openresponses.UserText("ping the specialist")}, local) {
				switch e := ev.(type) {
				case *agentturn.ToolUpdate:
					updates++
				case *agentturn.ToolEnd:
					ends++
					if e.Err != nil || e.Result.Output.Text != "ping the specialist" {
						t.Errorf("tool_end = %+v", e)
					}
					d, ok := e.Result.Details.(TaskInfo)
					if !ok || d.TaskID == "" || d.ContextID == "" || d.State != a2a.TaskStateCompleted {
						t.Errorf("details = %+v", e.Result.Details)
					}
				case *agentturn.RunEnd:
					final = e
				}
			}
			if final == nil || final.Reason != agentturn.ReasonDone || ends != 1 {
				t.Fatalf("end = %+v ends = %d", final, ends)
			}
			if got := final.Items[len(final.Items)-1].(*openresponses.Message).Text(); got != "Tool result: ping the specialist" {
				t.Errorf("final text = %q", got)
			}
			if mode == "streaming" && updates == 0 {
				t.Error("no progress updates while streaming")
			}
			if mode == "blocking" && updates != 0 {
				t.Error("blocking mode should not report progress")
			}
		})
	}
}

func TestSharedContext(t *testing.T) {
	store := fronta2a.NewMemoryStore()
	remoteCfg := agentturn.Config{Model: &echo.Adapter{}}
	client, card := serve(t, fronta2a.New(remoteCfg, fronta2a.WithConversationStore(store)), nil)
	remote := New(client, card, WithContextID("ctx-1"), WithName("remote"))
	for _, in := range []string{"one", "two"} {
		args, _ := json.Marshal(Args{Input: in})
		if _, err := remote.Execute(context.Background(), agenttool.Call{ID: in, Args: args}); err != nil {
			t.Fatal(err)
		}
	}
	tr, _ := store.Load(context.Background(), "ctx-1")
	if len(tr) != 4 {
		t.Errorf("shared context transcript = %d items, want 4", len(tr))
	}
	if store.Len() != 1 {
		t.Errorf("contexts = %d, want 1", store.Len())
	}
}

// scripted is an executor that ends every task in a fixed state.
type scripted struct {
	state   a2a.TaskState
	message string
	reply   bool // answer with a bare message instead of a task
}

func (s scripted) Execute(ctx context.Context, reqCtx *a2asrv.RequestContext, q eventqueue.Queue) error {
	if s.reply {
		return q.Write(ctx, a2a.NewMessage(a2a.MessageRoleAgent, a2a.TextPart{Text: s.message}))
	}
	if err := q.Write(ctx, a2a.NewStatusUpdateEvent(reqCtx, a2a.TaskStateWorking, nil)); err != nil {
		return err
	}
	if err := q.Write(ctx, a2a.NewArtifactEvent(reqCtx, a2a.FilePart{File: a2a.FileURI{FileMeta: a2a.FileMeta{MimeType: "image/png"}, URI: "https://x/img.png"}})); err != nil {
		return err
	}
	var msg *a2a.Message
	if s.message != "" {
		msg = a2a.NewMessageForTask(a2a.MessageRoleAgent, reqCtx, a2a.TextPart{Text: s.message})
	}
	ev := a2a.NewStatusUpdateEvent(reqCtx, s.state, msg)
	ev.Final = true
	return q.Write(ctx, ev)
}

func (scripted) Cancel(context.Context, *a2asrv.RequestContext, eventqueue.Queue) error { return nil }

func TestTaskStates(t *testing.T) {
	cases := []struct {
		name     string
		exec     scripted
		wantText string
		wantErr  string
		input    bool
	}{
		{name: "completed with file", exec: scripted{state: a2a.TaskStateCompleted, message: "done"}, wantText: "done"},
		{name: "input required", exec: scripted{state: a2a.TaskStateInputRequired, message: "which one?"}, wantErr: "needs more input: which one?", input: true},
		{name: "input required silent", exec: scripted{state: a2a.TaskStateInputRequired}, wantErr: "needs more input before", input: true},
		{name: "failed", exec: scripted{state: a2a.TaskStateFailed, message: "boom"}, wantErr: "failed: boom"},
		{name: "canceled", exec: scripted{state: a2a.TaskStateCanceled}, wantErr: "canceled"},
		{name: "rejected", exec: scripted{state: a2a.TaskStateRejected, message: "no"}, wantErr: "rejected"},
		{name: "bare message", exec: scripted{reply: true, message: "hi"}, wantText: "hi"},
	}
	for _, tc := range cases {
		for _, blocking := range []bool{false, true} {
			name := tc.name
			if blocking {
				name += " blocking"
			}
			t.Run(name, func(t *testing.T) {
				client, card := serve(t, tc.exec, nil)
				opts := []Option{WithName("scripted"), WithDescription("d"), WithSequential()}
				if blocking {
					opts = append(opts, WithBlocking())
				}
				remote := New(client, card, opts...)
				if !agenttool.IsSequential(remote) || remote.Description() != "d" {
					t.Error("options not applied")
				}
				res, err := remote.Execute(context.Background(), agenttool.Call{ID: "c", Args: json.RawMessage(`{"input":"go"}`)})
				if tc.wantErr != "" {
					if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
						t.Fatalf("err = %v, want %q", err, tc.wantErr)
					}
					var ir *InputRequiredError
					if errors.As(err, &ir) != tc.input {
						t.Errorf("InputRequiredError = %v, want %v", !tc.input, tc.input)
					}
					if tc.input && (ir.TaskID == "" || ir.ContextID == "") {
						t.Errorf("input required ids = %+v", ir)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if tc.exec.reply {
					if res.Output.Text != tc.wantText {
						t.Errorf("output = %+v", res.Output)
					}
					return
				}
				// A completed task with a file artifact answers with parts.
				if len(res.Output.Parts) != 2 || res.Output.Parts[0].(*openresponses.Text).Text != tc.wantText {
					t.Fatalf("parts = %+v", res.Output.Parts)
				}
				if img, ok := res.Output.Parts[1].(*openresponses.InputImage); !ok || img.ImageURL != "https://x/img.png" {
					t.Errorf("image part = %+v", res.Output.Parts[1])
				}
				if d := res.Details.(TaskInfo); d.State != a2a.TaskStateCompleted || d.TaskID == "" {
					t.Errorf("details = %+v", d)
				}
			})
		}
	}
}

func TestArgumentsAndNames(t *testing.T) {
	remote := New(nil, &a2a.AgentCard{Name: "My  Fancy-Agent v2!", Skills: []a2a.AgentSkill{{Name: "sum", Description: "adds"}}})
	if remote.Name() != "my_fancy_agent_v2" {
		t.Errorf("name = %q", remote.Name())
	}
	if !strings.Contains(remote.Description(), "- sum: adds") {
		t.Errorf("description = %q", remote.Description())
	}
	if New(nil, nil).Name() != "agent" {
		t.Error("nil card should give a default name")
	}
	if !strings.Contains(string(remote.Parameters()), `"required":["input"]`) {
		t.Errorf("schema = %s", remote.Parameters())
	}
	for _, args := range []string{`{}`, `{"input":"  "}`, `[1]`} {
		if _, err := remote.Execute(context.Background(), agenttool.Call{Args: json.RawMessage(args)}); err == nil {
			t.Errorf("args %s accepted", args)
		}
	}
	if functionName("") != "" || functionName("--") != "" {
		t.Error("empty names")
	}
}
