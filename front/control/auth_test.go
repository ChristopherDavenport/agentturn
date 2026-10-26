package control

import (
	"net/http"
	"strings"
)

// tokenAuth is the tests' authenticator, standing in for a program's: a
// request whose Authorization header is "Bearer <token>" for a token in
// tokens is that token's principal, and any other is unauthenticated.
func tokenAuth(tokens map[string]Principal) Authenticator {
	return func(r *http.Request) (Principal, error) {
		tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		p, known := tokens[tok]
		if !ok || tok == "" || !known {
			return Principal{}, ErrUnauthenticated
		}
		return p, nil
	}
}

// bearerTransport adds token to every request as a program's transport
// would, and nothing when token is empty.
type bearerTransport struct{ token string }

func (b bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if b.token != "" {
		r = r.Clone(r.Context())
		r.Header.Set("Authorization", "Bearer "+b.token)
	}
	return http.DefaultTransport.RoundTrip(r)
}

// withToken is the dial option that sends token through
// [bearerTransport].
func withToken(token string) DialOption {
	return WithHTTPClient(&http.Client{Transport: bearerTransport{token}})
}
