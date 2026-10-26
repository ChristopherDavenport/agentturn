package a2a

import (
	"errors"
	"net/http"
	"strings"

	"github.com/a2aproject/a2a-go/a2a"
)

// tokenAuth is the tests' authenticator, standing in for a program's: a
// request whose Authorization header is "Bearer <token>", the scheme in
// any case, for one of tokens is the principal tokens maps it to, and
// the card declares the bearer scheme.
func tokenAuth(tokens map[string]string) Authenticator {
	return Authenticator{
		Name:   "bearer",
		Scheme: a2a.HTTPAuthSecurityScheme{Scheme: "Bearer"},
		Principal: func(r *http.Request) (string, error) {
			scheme, token, _ := strings.Cut(r.Header.Get("Authorization"), " ")
			if !strings.EqualFold(scheme, "Bearer") || token == "" {
				return "", errors.New("no bearer token")
			}
			principal, ok := tokens[strings.TrimSpace(token)]
			if !ok || principal == "" {
				return "", errors.New("unknown bearer token")
			}
			return principal, nil
		},
	}
}
