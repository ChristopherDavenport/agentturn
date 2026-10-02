package acp

import (
	"context"
	"encoding/json/jsontext"

	"github.com/ChristopherDavenport/openresponses"
	acpgo "github.com/ironpark/acp-go"
	"github.com/ironpark/acp-go/acp2"
	"github.com/ironpark/acp-go/schema/optional"
)

// AgentV2 returns the ACP v2 agent of one connection, the constructor
// acp2.NewAgentSideConnection takes.
func (s *Server) AgentV2(c *acp2.AgentSideConnection) acp2.Agent {
	return &connV2{SessionManager: s.v2, server: s, client: c}
}

// connV2 is the agent side of one v2 connection. The embedded manager
// answers session/new, session/list, session/cancel, session/resume,
// session/close and session/delete, and tracks each session's turn.
type connV2 struct {
	*acp2.SessionManager[*session]
	server *Server
	client *acp2.AgentSideConnection
}

func (c *connV2) Initialize(context.Context, *acp2.InitializeRequest) (*acp2.InitializeResponse, error) {
	caps := acp2.CapabilitiesOf(c)
	if caps.Session == nil {
		caps.Session = &acp2.SessionCapabilities{}
	}
	caps.Session.Prompt = &acp2.PromptCapabilities{
		Image:           &acp2.PromptImageCapabilities{},
		EmbeddedContext: &acp2.PromptEmbeddedContextCapabilities{},
	}
	return &acp2.InitializeResponse{
		ProtocolVersion: acp2.ProtocolVersion,
		Info:            acp2.Implementation{Name: c.server.name, Version: c.server.version},
		Capabilities:    caps,
	}, nil
}

// Prompt accepts a user message into the session and answers once the
// message is in the transcript. With no turn running it starts one; with
// one running it steers the message into the run, which takes it after
// the current tool batch.
func (c *connV2) Prompt(ctx context.Context, params *acp2.PromptRequest) (*acp2.PromptResponse, error) {
	msg, err := messageV2(params.Prompt)
	if err != nil {
		return nil, acpgo.InvalidParams(err.Error())
	}
	s, err := c.Lookup(ctx, params.SessionID)
	if err != nil {
		return nil, err
	}
	w := &waiter{id: acp2.GenerateMessageID(), content: params.Prompt, done: make(chan struct{})}
	s.expect(msg, w)
	if err := c.submit(ctx, s, params.SessionID, msg); err != nil {
		s.take(msg)
		return nil, err
	}
	select {
	case <-w.done:
		return &acp2.PromptResponse{MessageID: w.id}, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

// submit steers msg into the session's running turn, or starts a turn
// with it. A nil msg starts a turn for what is already queued, unless
// one is running to take it.
func (c *connV2) submit(ctx context.Context, s *session, id acp2.SessionID, msg openresponses.Item) error {
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	turn, done, joined := c.JoinTurn(ctx, id)
	if joined {
		if msg != nil {
			s.agent.Steer(msg)
		}
		return nil
	}
	stream := acp2.NewSessionStream(c.client, id)
	if err := stream.Running(context.WithoutCancel(turn)); err != nil {
		done()
		return err
	}
	go c.work(turn, s, id, stream, msg, done)
	return nil
}

// work runs a turn from running to idle: the run msg starts, or a
// continuation of what is queued when msg is nil, and another run for
// every message steered after a run stopped taking them. A cancelled
// turn ends idle at once; a message it left queued starts the next
// turn, as the agent hands queued items to its next run.
func (c *connV2) work(ctx context.Context, s *session, id acp2.SessionID, stream *acp2.SessionStream, msg openresponses.Item, done func()) {
	t := newTurn(c.server, s, &sinkV2{stream: stream})
	unsubscribe := s.agent.Subscribe(t.observe)
	ctx = context.WithValue(ctx, turnKey{}, t)
	report := context.WithoutCancel(ctx)

	input := closePending(s.agent.State().Pending)
	if msg != nil {
		input = append(input, msg)
	}
	reason, failure := acp2.StopReasonEndTurn, error(nil)
	for {
		end, err := t.drive(ctx, input)
		if acpgo.TurnCancelled(ctx) {
			reason = acp2.StopReasonCancelled
			break
		}
		if err != nil {
			reason, failure = acp2.StopReasonInternalError, err
		} else {
			reason = acp2.StopReason(stopReason(end))
		}
		s.turnMu.Lock()
		queued := s.agent.State().Steering > 0
		if !queued {
			// Every prompt that joined has been taken: the second settle
			// is true, since a join needs turnMu.
			for !c.SettleTurn(id) {
			}
			s.turnMu.Unlock()
			break
		}
		s.turnMu.Unlock()
		if end == nil {
			// The run could not start, and would not on a retry.
			break
		}
		input = closePending(s.agent.State().Pending)
	}
	unsubscribe()
	idle := acp2.StateUpdateIdle{StopReason: &reason}
	if u := t.summed(); u.counted {
		idle.Usage = &acp2.Usage{
			InputTokens: u.input, OutputTokens: u.output, TotalTokens: u.total,
			ThoughtTokens: optionalCount(u.thought), CachedReadTokens: optionalCount(u.cached),
		}
	}
	if failure != nil {
		var meta acp2.Meta
		_ = meta.Set("error", failure.Error())
		stream = stream.WithMeta(meta)
	}
	_ = stream.Send(report, acp2.SessionUpdateStateUpdate{Value: acp2.NewStateUpdate(idle)})
	done()
	if reason == acp2.StopReasonCancelled && s.agent.State().Steering > 0 {
		_ = c.submit(report, s, id, nil)
	}
}

// sinkV2 sends a turn's updates as v2 session updates.
type sinkV2 struct {
	stream *acp2.SessionStream
}

func (s *sinkV2) on(parent string) *acp2.SessionStream {
	if parent == "" {
		return s.stream
	}
	var meta acp2.Meta
	if err := meta.Set(MetaParent, parent); err != nil {
		return s.stream
	}
	return s.stream.WithMeta(meta)
}

func (s *sinkV2) text(ctx context.Context, messageID, delta string) error {
	return s.stream.SendText(ctx, acp2.MessageID(messageID), delta)
}

func (s *sinkV2) thought(ctx context.Context, messageID, delta string) error {
	return s.stream.SendThought(ctx, acp2.MessageID(messageID), delta)
}

func (s *sinkV2) propose(ctx context.Context, callID, parent, title string, kind ToolKind, args []byte) error {
	return s.on(parent).ProposeToolCall(ctx, acp2.ToolCallID(callID), title, acp2.ToolKind(kind), acp2.WithRawInput(jsontext.Value(args)))
}

func (s *sinkV2) update(ctx context.Context, callID, parent string, st status, content string, args []byte) error {
	var opts []acp2.ToolCallOption
	if content != "" {
		opts = append(opts, acp2.WithToolContent(acp2.ToolText(content)))
	}
	if args != nil {
		opts = append(opts, acp2.WithRawInput(jsontext.Value(args)))
	}
	return s.on(parent).UpdateToolCallStatus(ctx, acp2.ToolCallID(callID), statusV2(st), opts...)
}

func statusV2(st status) acp2.ToolCallStatus {
	switch st {
	case statusPending:
		return acp2.ToolCallStatusPending
	case statusRunning:
		return acp2.ToolCallStatusInProgress
	case statusCompleted:
		return acp2.ToolCallStatusCompleted
	case statusCancelled:
		return acp2.ToolCallStatusCancelled
	}
	return acp2.ToolCallStatusFailed
}

var permissionsV2 = []acp2.PermissionOption{
	acp2.NewPermissionOption(acp2.PermissionOptionKindAllowOnce, "Allow"),
	acp2.NewPermissionOption(acp2.PermissionOptionKindRejectOnce, "Reject"),
}

// permit reports the turn as requiring action while the client
// chooses, and as running again once it has.
func (s *sinkV2) permit(ctx context.Context, callID, parent, title string, kind ToolKind, args []byte, reason string) (bool, error) {
	report := context.WithoutCancel(ctx)
	if err := s.stream.RequiresAction(report); err != nil {
		return false, err
	}
	update := acp2.ToolCallUpdate{
		ToolCallID: acp2.ToolCallID(callID),
		Title:      optional.Of(title),
		Kind:       optional.Of(acp2.ToolKind(kind)),
		Status:     optional.Of(acp2.ToolCallStatusPending),
		RawInput:   jsontext.Value(args),
	}
	if reason != "" {
		update.Content = optional.Of([]acp2.ToolCallContent{acp2.ToolText(reason)})
	}
	subject := acp2.NewRequestPermissionSubject(acp2.RequestPermissionSubjectToolCall{ToolCall: update})
	_, allowed, err := s.on(parent).RequestPermission(ctx, title, subject, permissionsV2...)
	if err != nil || ctx.Err() != nil {
		return allowed, err
	}
	return allowed, s.stream.Running(report)
}

// inserted echoes the prompt's message under the ID its response
// carries, as v2 asks, and releases the prompt.
func (s *sinkV2) inserted(ctx context.Context, w *waiter) error {
	defer close(w.done)
	return s.stream.SendUserMessage(ctx, w.id, w.content...)
}
