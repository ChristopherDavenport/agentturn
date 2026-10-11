package a2a

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/ChristopherDavenport/agentturn"
	"github.com/a2aproject/a2a-go/a2a"
)

// Authenticator says who sent a request: the principal's name, or an
// error when the request carries no credentials it accepts. Scheme is
// what the agent card declares under Name, so a caller knows what to
// send.
type Authenticator struct {
	// Name is the scheme's name on the card, "bearer" when empty.
	Name a2a.SecuritySchemeName
	// Scheme is the security scheme the card declares.
	Scheme a2a.SecurityScheme
	// Principal authenticates one request. Its error is not sent to the
	// caller, which is told only that it is unauthenticated.
	Principal func(*http.Request) (string, error)
}

// BearerTokens accepts a request whose Authorization header is
// "Bearer <token>" for one of tokens, and names the principal tokens
// maps it to. Tokens are compared in constant time.
func BearerTokens(tokens map[string]string) Authenticator {
	return Authenticator{
		Name:   "bearer",
		Scheme: a2a.HTTPAuthSecurityScheme{Scheme: "Bearer"},
		Principal: func(r *http.Request) (string, error) {
			got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || got == "" {
				return "", errors.New("no bearer token")
			}
			for token, principal := range tokens {
				if subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1 {
					return principal, nil
				}
			}
			return "", errors.New("unknown bearer token")
		},
	}
}

// WithAuthenticator makes every message and cancel the executor
// serves name a principal: [Executor.Handler] authenticates each HTTP
// request with auth and refuses one it does not accept with 401, and
// the executor refuses a message or a cancel whose context names no
// principal, so a handler mounted without it serves nobody. Each task's
// run is prompted under the trigger {Kind: "a2a", Ref: principal}, which
// a recorder writes on the run, so the record says who asked, as
// front/control's does. [Executor.Secure] declares the scheme on the
// card. TLS is the deployment's.
func WithAuthenticator(auth Authenticator) Option {
	return func(e *Executor) {
		if auth.Name == "" {
			auth.Name = "bearer"
		}
		e.auth = &auth
	}
}

type principalKey struct{}

// ContextWithPrincipal returns ctx naming principal as the
// authenticated sender of the request it serves. [Executor.Handler]
// sets it; a host with its own authentication sets it the same way.
func ContextWithPrincipal(ctx context.Context, principal string) context.Context {
	return context.WithValue(ctx, principalKey{}, principal)
}

// PrincipalFrom returns the principal [ContextWithPrincipal] named.
func PrincipalFrom(ctx context.Context) (string, bool) {
	p, ok := ctx.Value(principalKey{}).(string)
	return p, ok && p != ""
}

// Handler wraps next, the JSON-RPC handler serving the executor, so
// each request is authenticated by the [WithAuthenticator] option: one
// it refuses gets 401 with a WWW-Authenticate header naming the
// scheme, and one it accepts reaches next with the principal on its
// context, which the SDK keeps on the context the executor runs under.
// Without the option it returns next. The agent card is served apart,
// unauthenticated, so a caller can learn the scheme.
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

// Secure declares the [WithAuthenticator] scheme on card, as the one
// scheme every interaction requires, and returns card. Without the
// option it returns card unchanged.
func (e *Executor) Secure(card *a2a.AgentCard) *a2a.AgentCard {
	if e.auth == nil || card == nil || e.auth.Scheme == nil {
		return card
	}
	if card.SecuritySchemes == nil {
		card.SecuritySchemes = a2a.NamedSecuritySchemes{}
	}
	card.SecuritySchemes[e.auth.Name] = e.auth.Scheme
	card.Security = append(card.Security, a2a.SecurityRequirements{e.auth.Name: {}})
	return card
}

// authenticated returns ctx under the principal's trigger, or the
// error a request with none gets when the executor authenticates.
func (e *Executor) authenticated(ctx context.Context) (context.Context, string, error) {
	if e.auth == nil {
		return ctx, "", nil
	}
	principal, ok := PrincipalFrom(ctx)
	if !ok {
		return nil, "", a2a.NewError(fmt.Errorf("%w: no principal", a2a.ErrUnauthenticated), "the request is not authenticated")
	}
	return agentturn.ContextWithTrigger(ctx, agentturn.Trigger{Kind: "a2a", Ref: principal}), principal, nil
}
