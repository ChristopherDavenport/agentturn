package control

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ChristopherDavenport/agenttool"
	"github.com/ChristopherDavenport/agentturn"
)

// Protocol names the wire this package speaks, as GET / reports it.
const Protocol = "agentturn-control/1"

// Scope is what a principal may do through a Handler.
type Scope string

const (
	// ScopeRead follows the agent: Subscribe and State.
	ScopeRead Scope = "read"
	// ScopeControl drives it: Prompt, Queue and Abort.
	ScopeControl Scope = "control"
	// ScopeAnswer answers it: Resume and Reply, which let a call run
	// that the policy asked a person about. No other scope implies it.
	ScopeAnswer Scope = "answer"
	// ScopeCommand runs the product's commands ([WithCommands]).
	ScopeCommand Scope = "command"
)

// Principal is who a request is from, as an [Authenticator] resolved
// it, and what they may do.
type Principal struct {
	// Name says who, for the record: a run a principal starts or
	// answers carries it as the Ref of an agentturn.Trigger of kind
	// "control".
	Name   string
	Scopes []Scope
}

// Has reports whether p holds s.
func (p Principal) Has(s Scope) bool { return slices.Contains(p.Scopes, s) }

// AllScopes is every scope, for a principal that may do anything.
var AllScopes = []Scope{ScopeRead, ScopeControl, ScopeAnswer, ScopeCommand}

// Authenticator resolves the principal a request is from. An error
// refuses the request as unauthenticated; its text is not sent.
type Authenticator func(r *http.Request) (Principal, error)

// ErrUnauthenticated is a request no authenticator accepted, and
// ErrForbidden one from a principal without the scope its method
// needs. A [Client] call refused for either matches it with errors.Is.
var (
	ErrUnauthenticated = errors.New("control: unauthenticated")
	ErrForbidden       = errors.New("control: the principal lacks the scope this needs")
)

// BearerTokens is an [Authenticator] over static bearer tokens: a
// request whose Authorization header is "Bearer <token>" for a token
// in tokens is that token's principal. Tokens are compared in constant
// time, through their SHA-256, so neither their contents nor their
// lengths leak through timing.
func BearerTokens(tokens map[string]Principal) Authenticator {
	type entry struct {
		sum [sha256.Size]byte
		p   Principal
	}
	entries := make([]entry, 0, len(tokens))
	for tok, p := range tokens {
		if tok == "" {
			continue
		}
		entries = append(entries, entry{sha256.Sum256([]byte(tok)), p})
	}
	return func(r *http.Request) (Principal, error) {
		h := r.Header.Get("Authorization")
		tok, ok := strings.CutPrefix(h, "Bearer ")
		if !ok || tok == "" {
			return Principal{}, ErrUnauthenticated
		}
		sum := sha256.Sum256([]byte(tok))
		var found Principal
		matched := 0
		for _, e := range entries {
			if subtle.ConstantTimeCompare(sum[:], e.sum[:]) == 1 {
				found, matched = e.p, 1
			}
		}
		if matched == 0 {
			return Principal{}, ErrUnauthenticated
		}
		return found, nil
	}
}

type principalKey struct{}

// PrincipalFrom returns the principal a Handler put on the context of
// the call it makes, so a [agentturn.Control] that wraps an agent can
// say who prompted or answered.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// Command is a product's control the loop does not have, served by
// name under [ScopeCommand]: switching the model, adding a server. It
// stays out of agentturn.Control, whose methods are the loop's own.
type Command struct {
	// Description says what it does, for a client listing the
	// commands.
	Description string
	// Schema is the JSON Schema of its arguments; nil takes any.
	Schema json.RawMessage
	// Run does it, with the arguments as the client sent them.
	Run func(ctx context.Context, args json.RawMessage) (json.RawMessage, error)
}

// ErrUnknownCommand is a command no [WithCommands] names.
var ErrUnknownCommand = errors.New("control: no such command")

// Option configures a Handler.
type Option func(*handler)

// WithAuthenticator sets how requests are authenticated. Without it,
// and without [WithInsecureNoAuth], the handler refuses every request.
func WithAuthenticator(a Authenticator) Option {
	return func(h *handler) { h.auth = a }
}

// WithInsecureNoAuth serves every request as a principal with every
// scope, for tests and for a socket only the host's user can reach.
// Answering a call a policy asked about is then open to anyone who can
// connect.
func WithInsecureNoAuth() Option {
	return func(h *handler) { h.insecure = true }
}

// WithCommands serves the product's commands by name.
func WithCommands(cmds map[string]Command) Option {
	return func(h *handler) {
		if h.commands == nil {
			h.commands = map[string]Command{}
		}
		for name, c := range cmds {
			h.commands[name] = c
		}
	}
}

// WithMaxRequestBytes bounds a request body; a larger one is refused.
// The default is 16 MiB, room for a prompt with images.
func WithMaxRequestBytes(n int64) Option {
	return func(h *handler) { h.maxBody = n }
}

// WithEventBuffer bounds how many events a subscriber's stream may
// fall behind the agent. A subscriber is called on the agent's
// delivery path, so a stream that cannot keep up is not waited for:
// once it is this far behind its stream ends with an "overflow" frame,
// and the client catches up from the record. The default is 4096.
func WithEventBuffer(n int) Option {
	return func(h *handler) { h.buffer = n }
}

type handler struct {
	c        agentturn.Control
	auth     Authenticator
	insecure bool
	commands map[string]Command
	maxBody  int64
	buffer   int
	ping     time.Duration
	mux      *http.ServeMux
}

// Handler serves c over HTTP: JSON requests for its methods and a
// server-sent event stream for Subscribe. Mount it under a path with
// http.StripPrefix. The routes, relative to that path:
//
//	GET  /                 the protocol, for Dial            any scope
//	GET  /state            State                             read
//	GET  /events           Subscribe, as server-sent events  read
//	POST /prompt           Prompt; returns the run's end     control
//	POST /queue            Queue: steer or follow_up         control
//	POST /abort            Abort                             control
//	POST /resume           Resume; returns the run's end     answer
//	POST /reply            Reply                             answer
//	GET  /commands         the commands, described           command
//	POST /commands/{name}  run one                           command
//
// Every request is authenticated ([WithAuthenticator]); the principal
// rides on the call's context ([PrincipalFrom]), and a run it starts or
// answers carries an agentturn.Trigger of kind "control" naming it.
// Prompt and Resume run on the request's context with its
// cancellation lifted (context.WithoutCancel, the principal and the
// trigger kept), so a run goes on when its client goes away: over a
// wire a dropped connection is not the person's intent. Only POST
// /abort aborts a run; a [Client] whose caller cancels sends it. The
// run's end still reaches the event stream and the record when the
// response that would have carried it cannot be written.
//
// The stream carries the agent's events in [MarshalEvent]'s form, one
// per "data:" frame, in the order the agent delivers them, after a
// "ready" frame that says the subscription is in place. Nothing past
// is replayed but a question still waiting, which the agent sends a
// new subscriber; what is committed, a client reads from the record.
func Handler(c agentturn.Control, opts ...Option) http.Handler {
	h := &handler{c: c, maxBody: 16 << 20, buffer: 4096, ping: 15 * time.Second}
	for _, o := range opts {
		o(h)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", h.guard("", h.info))
	mux.HandleFunc("GET /state", h.guard(ScopeRead, h.state))
	mux.HandleFunc("GET /events", h.guard(ScopeRead, h.events))
	mux.HandleFunc("POST /prompt", h.guard(ScopeControl, h.prompt))
	mux.HandleFunc("POST /queue", h.guard(ScopeControl, h.queue))
	mux.HandleFunc("POST /abort", h.guard(ScopeControl, h.abort))
	mux.HandleFunc("POST /resume", h.guard(ScopeAnswer, h.resume))
	mux.HandleFunc("POST /reply", h.guard(ScopeAnswer, h.reply))
	mux.HandleFunc("GET /commands", h.guard(ScopeCommand, h.listCommands))
	mux.HandleFunc("POST /commands/{name}", h.guard(ScopeCommand, h.runCommand))
	h.mux = mux
	return h
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

// guard authenticates r, checks the principal holds scope (none for
// ""), and puts the principal on the context.
func (h *handler) guard(scope Scope, next func(http.ResponseWriter, *http.Request, Principal)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var p Principal
		switch {
		case h.auth != nil:
			var err error
			if p, err = h.auth(r); err != nil {
				writeFailure(w, http.StatusUnauthorized, ErrUnauthenticated)
				return
			}
		case h.insecure:
			p = Principal{Scopes: AllScopes}
		default:
			writeFailure(w, http.StatusUnauthorized, fmt.Errorf("%w: the handler has no authenticator", ErrUnauthenticated))
			return
		}
		if scope != "" && !p.Has(scope) {
			writeFailure(w, http.StatusForbidden, fmt.Errorf("%w: %s", ErrForbidden, scope))
			return
		}
		ctx := context.WithValue(r.Context(), principalKey{}, p)
		if p.Name != "" {
			ctx = agentturn.ContextWithTrigger(ctx, agentturn.Trigger{Kind: "control", Ref: p.Name})
		}
		next(w, r.WithContext(ctx), p)
	}
}

func (h *handler) info(w http.ResponseWriter, _ *http.Request, _ Principal) {
	writeJSON(w, http.StatusOK, struct {
		Protocol string `json:"protocol"`
	}{Protocol})
}

func (h *handler) state(w http.ResponseWriter, _ *http.Request, _ Principal) {
	s, err := stateOut(h.c.State())
	if err != nil {
		writeFailure(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, s)
}

// runReply is Prompt's and Resume's answer: how the run ended and the
// error it returned, either or both.
type runReply struct {
	End *wireRunEnd `json:"end,omitempty"`
	Err *wireError  `json:"error,omitempty"`
}

func (h *handler) prompt(w http.ResponseWriter, r *http.Request, _ Principal) {
	var req struct {
		Items json.RawMessage `json:"items"`
	}
	if !h.decode(w, r, &req) {
		return
	}
	items, err := itemsIn(req.Items)
	if err != nil {
		writeFailure(w, http.StatusBadRequest, err)
		return
	}
	h.writeRun(w, func() (*agentturn.RunEnd, error) { return h.c.Prompt(context.WithoutCancel(r.Context()), items...) })
}

func (h *handler) resume(w http.ResponseWriter, r *http.Request, _ Principal) {
	var req struct {
		Answers []wireAnswer `json:"answers"`
	}
	if !h.decode(w, r, &req) {
		return
	}
	answers, err := answersIn(req.Answers)
	if err != nil {
		writeFailure(w, http.StatusBadRequest, err)
		return
	}
	h.writeRun(w, func() (*agentturn.RunEnd, error) { return h.c.Resume(context.WithoutCancel(r.Context()), answers...) })
}

func (h *handler) writeRun(w http.ResponseWriter, run func() (*agentturn.RunEnd, error)) {
	end, err := run()
	out := runReply{Err: errorOut(err)}
	if end != nil {
		we, merr := runEndOut(end)
		if merr != nil {
			writeFailure(w, http.StatusInternalServerError, merr)
			return
		}
		out.End = &we
	}
	writeJSON(w, http.StatusOK, out)
}

// errReply is the answer of a method that returns only an error.
type errReply struct {
	Err *wireError `json:"error,omitempty"`
}

func (h *handler) queue(w http.ResponseWriter, r *http.Request, _ Principal) {
	var req struct {
		Mode  string          `json:"mode"`
		Items json.RawMessage `json:"items"`
	}
	if !h.decode(w, r, &req) {
		return
	}
	switch agentturn.QueueMode(req.Mode) {
	case agentturn.QueueSteer, agentturn.QueueFollowUp:
	default:
		// The agent takes any other mode as a follow-up; a client's
		// typo is refused here rather than quietly becoming one.
		writeFailure(w, http.StatusBadRequest, fmt.Errorf("control: queue: unknown mode %q", req.Mode))
		return
	}
	items, err := itemsIn(req.Items)
	if err != nil {
		writeFailure(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, errReply{errorOut(h.c.Queue(r.Context(), agentturn.QueueMode(req.Mode), items...))})
}

func (h *handler) abort(w http.ResponseWriter, _ *http.Request, _ Principal) {
	h.c.Abort()
	writeJSON(w, http.StatusOK, errReply{})
}

func (h *handler) reply(w http.ResponseWriter, r *http.Request, _ Principal) {
	var req struct {
		ID     string           `json:"id"`
		Answer wireElicitAnswer `json:"answer"`
	}
	if !h.decode(w, r, &req) {
		return
	}
	a := elicitAnswerIn(&req.Answer)
	switch a.Action {
	case agenttool.ActionAccept, agenttool.ActionDecline, agenttool.ActionCancel:
	default:
		writeFailure(w, http.StatusBadRequest, fmt.Errorf("control: reply: unknown action %q", a.Action))
		return
	}
	writeJSON(w, http.StatusOK, errReply{errorOut(h.c.Reply(req.ID, *a))})
}

type commandInfo struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Schema      json.RawMessage `json:"schema,omitempty"`
}

func (h *handler) listCommands(w http.ResponseWriter, _ *http.Request, _ Principal) {
	out := make([]commandInfo, 0, len(h.commands))
	for name, c := range h.commands {
		out = append(out, commandInfo{Name: name, Description: c.Description, Schema: c.Schema})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	writeJSON(w, http.StatusOK, out)
}

type commandReply struct {
	Result json.RawMessage `json:"result,omitempty"`
	Err    *wireError      `json:"error,omitempty"`
}

func (h *handler) runCommand(w http.ResponseWriter, r *http.Request, _ Principal) {
	name := r.PathValue("name")
	c, ok := h.commands[name]
	if !ok || c.Run == nil {
		writeFailure(w, http.StatusNotFound, fmt.Errorf("%w: %q", ErrUnknownCommand, name))
		return
	}
	var args json.RawMessage
	if !h.decode(w, r, &args) {
		return
	}
	res, err := c.Run(r.Context(), args)
	if err == nil && len(res) > 0 && !json.Valid(res) {
		err = fmt.Errorf("control: command %q returned a result that is not JSON", name)
	}
	if err != nil {
		res = nil
	}
	writeJSON(w, http.StatusOK, commandReply{Result: res, Err: errorOut(err)})
}

// decode reads r's body as JSON into v, bounded; on failure it has
// answered and returns false. An empty body leaves v as it is.
func (h *handler) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.maxBody))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeFailure(w, http.StatusRequestEntityTooLarge, fmt.Errorf("control: request body over %d bytes", h.maxBody))
			return false
		}
		writeFailure(w, http.StatusBadRequest, err)
		return false
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return true
	}
	if err := json.Unmarshal(body, v); err != nil {
		writeFailure(w, http.StatusBadRequest, fmt.Errorf("control: request body: %w", err))
		return false
	}
	return true
}

// events serves Subscribe as a server-sent event stream.
func (h *handler) events(w http.ResponseWriter, r *http.Request, _ Principal) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeFailure(w, http.StatusInternalServerError, errors.New("control: the response cannot stream"))
		return
	}
	frames := make(chan []byte, h.buffer)
	behind := make(chan struct{})
	var once sync.Once
	// The subscriber runs on the agent's delivery path: it writes the
	// event out (a copy, so a live item it holds can change after) and
	// returns, and a stream too far behind is cut rather than waited on.
	unsubscribe := h.c.Subscribe(func(_ context.Context, ev agentturn.Event) error {
		data, err := MarshalEvent(ev)
		if err != nil {
			data, _ = json.Marshal(struct {
				Type      string     `json:"type"`
				EventType string     `json:"event_type"`
				Err       *wireError `json:"error"`
			}{"codec_error", ev.EventType(), errorOut(err)})
		}
		select {
		case frames <- data:
		default:
			once.Do(func() { close(behind) })
		}
		return nil
	})
	defer unsubscribe()

	hdr := w.Header()
	hdr.Set("Content-Type", "text/event-stream")
	hdr.Set("Cache-Control", "no-cache")
	hdr.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if writeFrame(w, "ready", []byte("{}")) != nil {
		return
	}
	flusher.Flush()

	// drain writes what else is queued, without waiting for more.
	drain := func() bool {
		for {
			select {
			case data := <-frames:
				if writeFrame(w, "", data) != nil {
					return false
				}
			default:
				return true
			}
		}
	}
	ping := time.NewTicker(h.ping)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-behind:
			// Send what was queued before the cut, then say so.
			if !drain() {
				return
			}
			_ = writeFrame(w, "overflow", []byte(`{"message":"the stream fell too far behind the agent; catch up from the record and subscribe again"}`))
			flusher.Flush()
			return
		case data := <-frames:
			if writeFrame(w, "", data) != nil || !drain() {
				return
			}
			flusher.Flush()
		case <-ping.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// writeFrame writes one server-sent event: data is one line of JSON.
func writeFrame(w io.Writer, event string, data []byte) error {
	var b strings.Builder
	if event != "" {
		b.WriteString("event: ")
		b.WriteString(event)
		b.WriteByte('\n')
	}
	b.WriteString("data: ")
	b.Write(data)
	b.WriteString("\n\n")
	_, err := io.WriteString(w, b.String())
	return err
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		status = http.StatusInternalServerError
		data, _ = json.Marshal(errReply{errorOut(err)})
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func writeFailure(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, errReply{errorOut(err)})
}
