package acp

import (
	"context"
	"encoding/json/jsontext"

	acpgo "github.com/ironpark/acp-go"
	"github.com/ironpark/acp-go/acp1"
)

// AgentV1 returns the ACP v1 agent of one connection, the constructor
// acp1.NewAgentSideConnection takes.
func (s *Server) AgentV1(c *acp1.AgentSideConnection) acp1.Agent {
	return &connV1{SessionManager: s.v1, server: s, client: c}
}

// connV1 is the agent side of one v1 connection. The embedded manager
// answers session/new, session/cancel, session/resume, session/close
// and session/delete.
type connV1 struct {
	*acp1.SessionManager[*session]
	server *Server
	client *acp1.AgentSideConnection
}

func (c *connV1) Initialize(context.Context, *acp1.InitializeRequest) (*acp1.InitializeResponse, error) {
	caps := acp1.CapabilitiesOf(c)
	caps.PromptCapabilities = &acp1.PromptCapabilities{Image: new(true), EmbeddedContext: new(true)}
	return &acp1.InitializeResponse{
		ProtocolVersion:   acp1.ProtocolVersion,
		AgentCapabilities: caps,
		AgentInfo:         &acp1.Implementation{Name: c.server.name, Version: c.server.version},
	}, nil
}

// Prompt runs one v1 turn: the prompt, the resumes its permission
// requests lead to, and the updates they stream, answered once the run
// ends with its stop reason and usage.
func (c *connV1) Prompt(ctx context.Context, params *acp1.PromptRequest) (*acp1.PromptResponse, error) {
	msg, err := messageV1(params.Prompt)
	if err != nil {
		return nil, acpgo.InvalidParams(err.Error())
	}
	return c.RunTurnResponse(ctx, params.SessionID, func(ctx context.Context, s *session) (*acp1.PromptResponse, error) {
		t := newTurn(c.server, s, &sinkV1{stream: acp1.NewSessionStream(c.client, params.SessionID)})
		unsubscribe := s.agent.Subscribe(t.observe)
		defer unsubscribe()
		ctx = context.WithValue(ctx, turnKey{}, t)
		end, err := t.drive(ctx, append(closePending(s.agent.State().Pending), msg))
		if err != nil {
			return nil, err
		}
		resp := &acp1.PromptResponse{StopReason: acp1.StopReason(stopReason(end))}
		if u := t.summed(); u.counted {
			resp.Usage = &acp1.Usage{
				InputTokens: u.input, OutputTokens: u.output, TotalTokens: u.total,
				ThoughtTokens: optionalCount(u.thought), CachedReadTokens: optionalCount(u.cached),
			}
		}
		return resp, nil
	})
}

// sinkV1 sends a turn's updates as v1 session updates.
type sinkV1 struct {
	stream *acp1.SessionStream
}

func (s *sinkV1) on(parent string) *acp1.SessionStream {
	if parent == "" {
		return s.stream
	}
	var meta acp1.Meta
	if err := meta.Set(MetaParent, parent); err != nil {
		return s.stream
	}
	return s.stream.WithMeta(meta)
}

func (s *sinkV1) text(ctx context.Context, _ string, delta string) error {
	return s.stream.SendText(ctx, delta)
}

func (s *sinkV1) thought(ctx context.Context, _ string, delta string) error {
	return s.stream.SendThought(ctx, delta)
}

func (s *sinkV1) propose(ctx context.Context, callID, parent, title string, kind ToolKind, args []byte) error {
	return s.on(parent).ProposeToolCall(ctx, acp1.ToolCallID(callID), title, acp1.ToolKind(kind), acp1.WithRawInput(jsontext.Value(args)))
}

func (s *sinkV1) update(ctx context.Context, callID, parent string, st status, content string, args []byte) error {
	var opts []acp1.ToolCallOption
	if content != "" {
		opts = append(opts, acp1.WithToolContent(acp1.ToolText(content)))
	}
	if args != nil {
		opts = append(opts, acp1.WithRawInput(jsontext.Value(args)))
	}
	return s.on(parent).UpdateToolCallStatus(ctx, acp1.ToolCallID(callID), statusV1(st), opts...)
}

func statusV1(st status) acp1.ToolCallStatus {
	switch st {
	case statusPending:
		return acp1.ToolCallStatusPending
	case statusRunning:
		return acp1.ToolCallStatusInProgress
	case statusCompleted:
		return acp1.ToolCallStatusCompleted
	}
	return acp1.ToolCallStatusFailed
}

var permissionsV1 = []acp1.PermissionOption{
	acp1.NewPermissionOption(acp1.PermissionOptionKindAllowOnce, "Allow"),
	acp1.NewPermissionOption(acp1.PermissionOptionKindRejectOnce, "Reject"),
}

func (s *sinkV1) permit(ctx context.Context, callID, parent, _ string, _ ToolKind, args []byte, reason string) (bool, error) {
	update := acp1.ToolCallUpdate{
		ToolCallID: acp1.ToolCallID(callID),
		Status:     new(acp1.ToolCallStatusPending),
		RawInput:   jsontext.Value(args),
	}
	if reason != "" {
		update.Content = []acp1.ToolCallContent{acp1.ToolText(reason)}
	}
	_, allowed, err := s.on(parent).RequestPermission(ctx, update, permissionsV1...)
	return allowed, err
}

// inserted has nothing to do in v1, where a prompt waits for its turn.
func (s *sinkV1) inserted(context.Context, *waiter) error { return nil }
