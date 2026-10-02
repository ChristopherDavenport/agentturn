package acp

import (
	"context"
	"errors"
	"sync"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	acpgo "github.com/ironpark/acp-go"
	"github.com/ironpark/acp-go/acp1"
)

// Session describes the ACP session an agent is built for.
type Session struct {
	// ID is the session ID the client will prompt with.
	ID string
	// Cwd is the absolute working directory the client gave the
	// session; tools that touch files should be rooted there.
	Cwd string
}

// NewSessionFunc builds the agent of one ACP session. The agent is used
// for that session alone, and only by the server, one prompt at a time.
// A host that records the session attaches its recorder here.
type NewSessionFunc func(ctx context.Context, s Session) (*agentturn.Agent, error)

// ToolKindFunc names the ACP tool kind of a call, so a client can pick
// an icon or a layout: one of the acp1.ToolKind constants, such as
// acp1.ToolKindRead, acp1.ToolKindEdit or acp1.ToolKindExecute. tool is
// nil when no tool of the agent's configuration has the name.
type ToolKindFunc func(name string, tool agenttool.Tool) acp1.ToolKind

// Server serves agentturn agents over ACP v1. Its sessions live in
// memory and are shared by every connection it serves.
type Server struct {
	newSession NewSessionFunc
	info       *acp1.Implementation
	kind       ToolKindFunc
	sessions   *acp1.SessionManager[*session]
}

// Option configures a [Server].
type Option func(*Server)

// WithInfo names the agent to clients in the initialize response.
func WithInfo(name, version string) Option {
	return func(s *Server) {
		s.info = &acp1.Implementation{Name: name, Version: version}
	}
}

// WithToolKind replaces [DefaultToolKind].
func WithToolKind(fn ToolKindFunc) Option {
	return func(s *Server) {
		if fn != nil {
			s.kind = fn
		}
	}
}

// DefaultToolKind reports read for a tool annotated read-only and other
// for everything else.
func DefaultToolKind(_ string, tool agenttool.Tool) acp1.ToolKind {
	if a, ok := tool.(agenttool.Annotated); ok && a.Annotations().ReadOnly {
		return acp1.ToolKindRead
	}
	return acp1.ToolKindOther
}

// New returns a server whose sessions are built by newSession.
func New(newSession NewSessionFunc, opts ...Option) *Server {
	s := &Server{newSession: newSession, kind: DefaultToolKind}
	for _, opt := range opts {
		opt(s)
	}
	s.sessions = acp1.NewSessionManager(acp1.NewMemoryStore[*session](), s.create)
	return s
}

// ErrNoAgent is returned for a session/new whose [NewSessionFunc]
// returned no agent and no error.
var ErrNoAgent = errors.New("front/acp: the session function returned no agent")

func (s *Server) create(ctx context.Context, params *acp1.NewSessionRequest) (acp1.SessionID, *session, error) {
	id := acp1.GenerateSessionID()
	a, err := s.newSession(ctx, Session{ID: string(id), Cwd: params.Cwd})
	if err != nil {
		return "", nil, err
	}
	if a == nil {
		return "", nil, ErrNoAgent
	}
	return id, &session{agent: a}, nil
}

// Agent returns the ACP agent of one connection, the constructor
// acp1.NewAgentSideConnection takes.
func (s *Server) Agent(c *acp1.AgentSideConnection) acp1.Agent {
	return &connection{SessionManager: s.sessions, server: s, client: c}
}

// Serve runs one ACP connection over transport until it ends.
func (s *Server) Serve(ctx context.Context, transport acpgo.Transport, opts ...acpgo.Option) error {
	return acp1.NewAgentSideConnection(s.Agent, transport, opts...).Start(ctx)
}

// session is the state of one ACP session: its agent and the tool calls
// already reported to the client, so a call reported pending and later
// approved is updated rather than announced twice.
type session struct {
	agent *agentturn.Agent

	mu       sync.Mutex
	reported map[string]bool
}

// seen says whether the call has been reported.
func (s *session) seen(callID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reported[callID]
}

// report marks the call reported and says whether it already was.
func (s *session) report(callID string) (already bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reported == nil {
		s.reported = map[string]bool{}
	}
	already = s.reported[callID]
	s.reported[callID] = true
	return already
}

// connection is the agent side of one ACP connection. The embedded
// manager answers session/new, session/cancel, session/resume,
// session/close and session/delete.
type connection struct {
	*acp1.SessionManager[*session]
	server *Server
	client *acp1.AgentSideConnection
}

func (c *connection) Initialize(context.Context, *acp1.InitializeRequest) (*acp1.InitializeResponse, error) {
	caps := acp1.CapabilitiesOf(c)
	caps.PromptCapabilities = &acp1.PromptCapabilities{Image: new(true), EmbeddedContext: new(true)}
	return &acp1.InitializeResponse{
		ProtocolVersion:   acp1.ProtocolVersion,
		AgentCapabilities: caps,
		AgentInfo:         c.server.info,
	}, nil
}

func (c *connection) Prompt(ctx context.Context, params *acp1.PromptRequest) (*acp1.PromptResponse, error) {
	msg, err := message(params.Prompt)
	if err != nil {
		return nil, acpgo.InvalidParams(err.Error())
	}
	return c.RunTurnResponse(ctx, params.SessionID, func(ctx context.Context, s *session) (*acp1.PromptResponse, error) {
		t := newTurn(c.server, s, acp1.NewSessionStream(c.client, params.SessionID))
		return t.run(ctx, msg)
	})
}
