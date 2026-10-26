package a2a

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"sync"

	"github.com/ChristopherDavenport/agentturn"
	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2asrv"
)

// Authenticator says who sent a request: the principal's name, or an
// error when the request carries no credentials it accepts. Scheme is
// what the agent card declares under Name, so a caller knows what to
// send.
//
// The package ships none. The program decides who a request is from,
// by whatever credentials it issues, and states on the card how a
// caller presents them; a program taking bearer tokens declares
//
//	Authenticator{
//		Name:      "bearer",
//		Scheme:    a2a.HTTPAuthSecurityScheme{Scheme: "Bearer"},
//		Principal: principalOf, // the program's check of its tokens
//	}
type Authenticator struct {
	// Name is the scheme's name on the card, which a Scheme needs:
	// [New] panics on a Scheme with no Name.
	Name a2a.SecuritySchemeName
	// Scheme is the security scheme the card declares. An HTTP
	// scheme's name is the WWW-Authenticate challenge
	// [Executor.Handler] answers a refused request with.
	Scheme a2a.SecurityScheme
	// Principal authenticates one request from its headers: the HTTP
	// request's under [Executor.Handler], and under the interceptor
	// ([Executor.Interceptor]) a request built from the call's metadata,
	// which every transport carries, so it reads headers and nothing
	// else. Its error is not sent to the caller, which is told only that
	// it is unauthenticated.
	Principal func(*http.Request) (string, error)
}

// WithAuthenticator makes every request the executor serves name a
// principal, and makes each task and conversation its creator's:
//
//   - [Executor.Interceptor], which [Executor.RequestHandler] installs,
//     authenticates every call of the A2A handler from the call's
//     metadata, on every method and transport, and refuses one it does
//     not accept with a2a.ErrUnauthenticated before the call reaches the
//     executor or the task store, so an unauthenticated message naming
//     a task leaves that task as it was.
//   - The interceptor refuses, as a2a.ErrTaskNotFound, a call about a
//     task or a context another principal owns: a message continuing
//     it or referencing another's task, tasks/get, tasks/cancel,
//     resubscribe and the push configuration methods. A task or a
//     context belongs to the principal whose message created it, as
//     [Owners] records, claimed as soon as the answer names it; a
//     stored conversation nobody owns is refused to everyone.
//     tasks/list is not served (a2a.ErrUnsupportedOperation): it is
//     paged before another's tasks could be taken out.
//   - The executor refuses a message or a cancel that did not pass the
//     interceptor, so a handler built without it runs no task; it
//     cannot guard the methods that never reach the executor, which is
//     why the interceptor is the guard.
//   - Each task's run is prompted under the trigger
//     {Kind: "a2a", Ref: principal}, which a recorder writes on the run,
//     so the record says who asked, as front/control's does.
//
// [Executor.Handler] refuses an HTTP request early with 401;
// [Executor.Secure] declares the scheme on the card, which is served
// apart, unauthenticated, so a caller can learn it. TLS is the
// deployment's.
//
// An Authenticator with a Scheme must name it: [New] panics on one with
// a Scheme and no Name, rather than serve a card that leaves the scheme
// out. One with neither authenticates and declares nothing.
func WithAuthenticator(auth Authenticator) Option {
	return func(e *Executor) { e.auth = &auth }
}

// Owners records which principal owns each task and conversation under
// [WithAuthenticator]. Claim sets key's owner to principal unless it
// has one, and returns the owner it has after the call, so two claims
// of one key agree on one owner. A key is "task:<id>" or
// "context:<id>". The executor keeps them where the conversations are
// when its [ConversationStore] implements Owners, as [MemoryStore]
// does, else in memory ([WithOwners] names a store of its own). A task
// nobody owns is not found, and so is a conversation nobody owns that
// the store holds, after a restart that lost its owner or on a replica
// that does not share them: it fails closed rather than going to its
// first writer. A new conversation is claimed by the principal whose
// message starts it, whatever ID the caller chose; one principal can
// take an ID another has not used yet, which leaks nothing.
type Owners interface {
	Owner(ctx context.Context, key string) (string, bool, error)
	Claim(ctx context.Context, key, principal string) (string, error)
}

// WithOwners keeps the owners of tasks and conversations in o, a store
// that outlives the process, in place of memory.
func WithOwners(o Owners) Option { return func(e *Executor) { e.owners = o } }

// MemoryOwners is the in-memory [Owners]. The zero value is ready.
type MemoryOwners struct {
	mu     sync.Mutex
	owners map[string]string
}

// Owner returns key's owner.
func (m *MemoryOwners) Owner(_ context.Context, key string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.owners[key]
	return p, ok, nil
}

// Claim sets key's owner unless it has one and returns the owner.
func (m *MemoryOwners) Claim(_ context.Context, key, principal string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.owners == nil {
		m.owners = map[string]string{}
	}
	if p, ok := m.owners[key]; ok {
		return p, nil
	}
	m.owners[key] = principal
	return principal, nil
}

type principalKey struct{}

// guardedKey marks a context that passed the interceptor.
type guardedKey struct{}

// ContextWithPrincipal returns ctx naming principal as the
// authenticated sender of the request it serves. [Executor.Handler] and
// the interceptor set it.
func ContextWithPrincipal(ctx context.Context, principal string) context.Context {
	return context.WithValue(ctx, principalKey{}, principal)
}

// PrincipalFrom returns the principal [ContextWithPrincipal] named.
func PrincipalFrom(ctx context.Context) (string, bool) {
	p, ok := ctx.Value(principalKey{}).(string)
	return p, ok && p != ""
}

// Handler wraps next, the HTTP handler serving the executor, so a
// request the [WithAuthenticator] option does not accept gets 401 with
// a WWW-Authenticate header naming the scheme before the A2A handler
// sees it. It is an early answer for HTTP; the guard is
// [Executor.Interceptor]. Without the option it returns next.
func (e *Executor) Handler(next http.Handler) http.Handler {
	if e.auth == nil {
		return next
	}
	auth := *e.auth
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := auth.Principal(r)
		if err != nil || principal == "" {
			if s, ok := auth.Scheme.(a2a.HTTPAuthSecurityScheme); ok && s.Scheme != "" {
				w.Header().Set("WWW-Authenticate", s.Scheme)
			}
			http.Error(w, "unauthenticated", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(ContextWithPrincipal(r.Context(), principal)))
	})
}

// RequestHandler returns the A2A request handler for the executor, with
// [Executor.Interceptor] installed after opts' own interceptors.
func (e *Executor) RequestHandler(opts ...a2asrv.RequestHandlerOption) a2asrv.RequestHandler {
	return a2asrv.NewHandler(e, append(opts, a2asrv.WithCallInterceptor(e.Interceptor()))...)
}

// Interceptor returns the call interceptor that authenticates and
// guards every call under [WithAuthenticator]; without the option it
// passes every call through. [Executor.RequestHandler] installs it; a
// host that builds its handler with a2asrv.NewHandler passes it with
// a2asrv.WithCallInterceptor.
func (e *Executor) Interceptor() a2asrv.CallInterceptor { return guard{e} }

type guard struct{ e *Executor }

func (g guard) Before(ctx context.Context, callCtx *a2asrv.CallContext, req *a2asrv.Request) (context.Context, error) {
	e := g.e
	if e.auth == nil {
		return ctx, nil
	}
	r := &http.Request{Header: http.Header{}}
	if callCtx != nil {
		for k, v := range callCtx.RequestMeta().List() {
			for _, s := range v {
				r.Header.Add(k, s)
			}
		}
	}
	principal, err := e.auth.Principal(r.WithContext(ctx))
	if err != nil || principal == "" {
		return ctx, unauthenticated()
	}
	if callCtx != nil {
		callCtx.User = &a2asrv.AuthenticatedUser{UserName: principal}
	}
	ctx = context.WithValue(ContextWithPrincipal(ctx, principal), guardedKey{}, true)
	var task a2a.TaskID
	var conversation string
	var refs []a2a.TaskID
	if req != nil {
		switch p := req.Payload.(type) {
		case *a2a.MessageSendParams:
			if p != nil && p.Message != nil {
				task, conversation, refs = p.Message.TaskID, p.Message.ContextID, p.Message.ReferenceTasks
			}
		case *a2a.ListTasksRequest:
			// A list is paged before the guard could take another's
			// tasks out, so its pages and totals would mislead; it is
			// not served under ownership.
			return ctx, a2a.NewError(a2a.ErrUnsupportedOperation, "tasks/list is not served to an authenticated caller")
		case *a2a.TaskQueryParams:
			task = p.ID
		case *a2a.TaskIDParams:
			task = p.ID
		case *a2a.GetTaskPushConfigParams:
			task = p.TaskID
		case *a2a.ListTaskPushConfigParams:
			task = p.TaskID
		case *a2a.DeleteTaskPushConfigParams:
			task = p.TaskID
		case *a2a.TaskPushConfig:
			task = p.TaskID
		}
	}
	// A task is its creator's; one nobody owns was not created
	// through this executor, or before a restart, and is not found
	// either. A message's referenced tasks are held to the same rule.
	for _, t := range append([]a2a.TaskID{task}, refs...) {
		if t == "" {
			continue
		}
		owner, ok, err := e.owners.Owner(ctx, "task:"+string(t))
		if err != nil {
			return ctx, err
		}
		if !ok || owner != principal {
			return ctx, notFound()
		}
	}
	if conversation != "" {
		if err := e.mayWrite(ctx, principal, conversation); err != nil {
			return ctx, err
		}
	}
	return ctx, nil
}

// mayWrite refuses a conversation another principal owns, and one
// nobody owns that the store holds: its owner was lost, to a restart
// with owners kept in memory or to a replica that does not share them,
// and the first writer must not take it. Only a new conversation is
// free to claim.
func (e *Executor) mayWrite(ctx context.Context, principal, conversation string) error {
	owner, ok, err := e.owners.Owner(ctx, "context:"+conversation)
	if err != nil {
		return err
	}
	if ok {
		if owner != principal {
			return notFound()
		}
		return nil
	}
	held, err := e.store.Load(ctx, conversation)
	if err != nil {
		return err
	}
	if len(held) > 0 {
		return notFound()
	}
	return nil
}

// After makes the task and conversation a message send answered with
// the sender's, as soon as the answer names them: a send that does not
// wait for the run can answer before the executor claims them, and the
// creator gets and follows its task at once.
func (g guard) After(ctx context.Context, callCtx *a2asrv.CallContext, resp *a2asrv.Response) error {
	if g.e.auth == nil || resp == nil || resp.Err != nil || callCtx == nil {
		return nil
	}
	if m := callCtx.Method(); m != "OnSendMessage" && m != "OnSendMessageStream" {
		return nil
	}
	info, ok := resp.Payload.(a2a.TaskInfoProvider)
	if !ok || info == nil {
		return nil
	}
	principal, ok := PrincipalFrom(ctx)
	if !ok {
		return nil
	}
	ti := info.TaskInfo()
	return g.e.takeOwnership(ctx, principal, ti.TaskID, ti.ContextID)
}

func unauthenticated() error {
	return a2a.NewError(fmt.Errorf("%w: no principal", a2a.ErrUnauthenticated), "the request is not authenticated")
}

func notFound() error {
	return a2a.NewError(a2a.ErrTaskNotFound, "task not found")
}

// Secure returns a copy of card that declares the [WithAuthenticator]
// scheme as the one every interaction requires; card itself is not
// changed, and a card that already requires it gets no second
// requirement. Without the option, or with an [Authenticator] that
// declares no Scheme, it returns card.
func (e *Executor) Secure(card *a2a.AgentCard) *a2a.AgentCard {
	if e.auth == nil || card == nil || e.auth.Scheme == nil || e.auth.Name == "" {
		return card
	}
	out := *card
	out.SecuritySchemes = maps.Clone(card.SecuritySchemes)
	if out.SecuritySchemes == nil {
		out.SecuritySchemes = a2a.NamedSecuritySchemes{}
	}
	out.SecuritySchemes[e.auth.Name] = e.auth.Scheme
	out.Security = slices.Clone(card.Security)
	for _, req := range out.Security {
		if _, ok := req[e.auth.Name]; ok && len(req) == 1 {
			return &out
		}
	}
	out.Security = append(out.Security, a2a.SecurityRequirements{e.auth.Name: {}})
	return &out
}

// authenticated returns ctx under the principal's trigger, or the error
// a request gets that did not pass the interceptor when the executor
// authenticates.
func (e *Executor) authenticated(ctx context.Context) (context.Context, string, error) {
	if e.auth == nil {
		return ctx, "", nil
	}
	principal, ok := PrincipalFrom(ctx)
	if guarded, _ := ctx.Value(guardedKey{}).(bool); !ok || !guarded {
		return nil, "", unauthenticated()
	}
	return agentturn.ContextWithTrigger(ctx, agentturn.Trigger{Kind: "a2a", Ref: principal}), principal, nil
}

// takeOwnership makes the task and its conversation the principal's,
// or refuses one another principal owns.
func (e *Executor) takeOwnership(ctx context.Context, principal string, task a2a.TaskID, conversation string) error {
	if e.auth == nil {
		return nil
	}
	if conversation != "" {
		if err := e.mayWrite(ctx, principal, conversation); err != nil {
			return err
		}
	}
	for _, key := range []string{"context:" + conversation, "task:" + string(task)} {
		if key == "context:" || key == "task:" {
			continue
		}
		owner, err := e.owners.Claim(ctx, key, principal)
		if err != nil {
			return err
		}
		if owner != principal {
			return notFound()
		}
	}
	return nil
}

// owns reports whether principal owns task, for a cancel.
func (e *Executor) owns(ctx context.Context, principal string, task a2a.TaskID) error {
	if e.auth == nil {
		return nil
	}
	owner, ok, err := e.owners.Owner(ctx, "task:"+string(task))
	if err != nil {
		return err
	}
	if !ok || owner != principal {
		return notFound()
	}
	return nil
}
