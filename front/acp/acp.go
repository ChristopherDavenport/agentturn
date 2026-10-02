package acp

import (
	"context"
	"errors"
	"sync"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
	"github.com/ChristopherDavenport/openresponses"
	acpgo "github.com/ironpark/acp-go"
	"github.com/ironpark/acp-go/acp1"
	"github.com/ironpark/acp-go/acp2"
	"github.com/ironpark/acp-go/router"
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
// for that session alone, and only by the server. A host that records
// the session attaches its recorder here.
type NewSessionFunc func(ctx context.Context, s Session) (*agentturn.Agent, error)

// ToolKind is the ACP tool kind of a call, which a client uses to pick
// an icon or a layout. The values are the protocol's, the same in v1 and
// v2.
type ToolKind string

// Tool kinds.
const (
	ToolKindRead    ToolKind = "read"
	ToolKindEdit    ToolKind = "edit"
	ToolKindDelete  ToolKind = "delete"
	ToolKindMove    ToolKind = "move"
	ToolKindSearch  ToolKind = "search"
	ToolKindExecute ToolKind = "execute"
	ToolKindThink   ToolKind = "think"
	ToolKindFetch   ToolKind = "fetch"
	ToolKindOther   ToolKind = "other"
)

// ToolKindFunc names the kind of a call. tool is nil when no tool of the
// agent's configuration has the name.
type ToolKindFunc func(name string, tool agenttool.Tool) ToolKind

// DefaultToolKind reports read for a tool annotated read-only and other
// for everything else.
func DefaultToolKind(_ string, tool agenttool.Tool) ToolKind {
	if a, ok := tool.(agenttool.Annotated); ok && a.Annotations().ReadOnly {
		return ToolKindRead
	}
	return ToolKindOther
}

// Server serves agentturn agents over ACP v1 and v2. Its sessions live
// in memory and are shared by every connection of the same protocol
// version it serves.
type Server struct {
	newSession NewSessionFunc
	name       string
	version    string
	kind       ToolKindFunc
	v1         *acp1.SessionManager[*session]
	v2         *acp2.SessionManager[*session]
}

// Option configures a [Server].
type Option func(*Server)

// WithInfo names the agent to clients in the initialize response.
func WithInfo(name, version string) Option {
	return func(s *Server) {
		s.name, s.version = name, version
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

// New returns a server whose sessions are built by newSession.
func New(newSession NewSessionFunc, opts ...Option) *Server {
	s := &Server{newSession: newSession, name: "agentturn", kind: DefaultToolKind}
	for _, opt := range opts {
		opt(s)
	}
	s.v1 = acp1.NewSessionManager(acp1.NewMemoryStore[*session](), func(ctx context.Context, params *acp1.NewSessionRequest) (acp1.SessionID, *session, error) {
		id := acp1.GenerateSessionID()
		sess, err := s.create(ctx, string(id), params.Cwd)
		return id, sess, err
	})
	s.v2 = acp2.NewSessionManager(acp2.NewMemoryStore[*session](), func(ctx context.Context, params *acp2.NewSessionRequest) (acp2.SessionID, *session, error) {
		id := acp2.GenerateSessionID()
		sess, err := s.create(ctx, string(id), string(params.Cwd))
		return id, sess, err
	})
	return s
}

// ErrNoAgent is returned for a session/new whose [NewSessionFunc]
// returned no agent and no error.
var ErrNoAgent = errors.New("front/acp: the session function returned no agent")

func (s *Server) create(ctx context.Context, id, cwd string) (*session, error) {
	a, err := s.newSession(ctx, Session{ID: id, Cwd: cwd})
	if err != nil {
		return nil, err
	}
	if a == nil {
		return nil, ErrNoAgent
	}
	return &session{agent: a, cwd: cwd}, nil
}

// Serve runs one ACP connection over transport until it ends, speaking
// whichever of v1 and v2 the client's initialize asks for.
func (s *Server) Serve(ctx context.Context, transport acpgo.Transport, opts ...acpgo.Option) error {
	return router.New(opts...).WithV1(s.AgentV1).WithV2(s.AgentV2).Serve(ctx, transport)
}

// session is the state of one ACP session: its agent, the tool calls
// already reported to the client, so a call reported pending and later
// approved is updated rather than announced twice, and, in v2, the
// prompts waiting for their message to be inserted.
type session struct {
	agent *agentturn.Agent
	cwd   string

	mu       sync.Mutex
	reported map[string]bool
	waiting  map[openresponses.Item]*waiter

	// turnMu makes joining a v2 turn and steering into it one step, and
	// deciding that the turn may end another, so a prompt cannot join a
	// turn after it has looked at the queue for the last time.
	turnMu sync.Mutex
}

// SessionInfo describes the session in a v2 session/list.
func (s *session) SessionInfo() acp2.SessionInfo {
	return acp2.SessionInfo{Cwd: acp2.AbsolutePath(s.cwd)}
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

// waiter is a v2 prompt waiting for its message to enter the
// transcript.
type waiter struct {
	id      acp2.MessageID
	content []acp2.ContentBlock
	done    chan struct{}
}

// expect registers w to be released when item is inserted.
func (s *session) expect(item openresponses.Item, w *waiter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.waiting == nil {
		s.waiting = map[openresponses.Item]*waiter{}
	}
	s.waiting[item] = w
}

// take returns and forgets the waiter for item, nil when there is none.
func (s *session) take(item openresponses.Item) *waiter {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.waiting[item]
	delete(s.waiting, item)
	return w
}
