package api

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"
)

type clientIDKey struct{}

// requireToken authenticates "Authorization: Bearer <token>" against tokens
// (token → client ID) and stores the client ID in the request context.
func requireToken(tokens map[string]string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientID, ok := authenticate(tokens, r.Header.Get("Authorization"))
		if !ok {
			writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid bearer token", "")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), clientIDKey{}, clientID)))
	})
}

func authenticate(tokens map[string]string, header string) (string, bool) {
	const prefix = "bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	presented := []byte(header[len(prefix):])

	// Compare against every token in constant time so response timing does
	// not reveal how much of a token matched.
	clientID, found := "", false
	for token, id := range tokens {
		if subtle.ConstantTimeCompare(presented, []byte(token)) == 1 {
			clientID, found = id, true
		}
	}
	return clientID, found
}

func clientIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(clientIDKey{}).(string)
	return id
}
