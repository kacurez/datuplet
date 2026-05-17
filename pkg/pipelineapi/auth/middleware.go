package auth

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// writeJSONError mirrors pkg/pipelineapi/http.writeError so the middleware
// emits Content-Type: application/json, consistent with the rest of the API.
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// SessionCookieName is the cookie that carries the opaque session UUID.
const SessionCookieName = "pipeline_api_session"

type ctxKey struct{}

// WithUser is an HTTP middleware that resolves the request's user via
// the given resolver and attaches it to the request context. On auth
// failure it returns 401; on infrastructure error it returns 500.
//
// # Token-kind cross-validation
//
// Every JWT the API mints carries token_kind ∈ {user, run, impersonation,
// local-cli}. The cross-check `aud=datuplet-api ⇒
// token_kind=user; aud=datuplet-catalog ⇒ token_kind ∈ {run,
// impersonation, local-cli}` is enforced at the JWT-verifying party:
//
//   - pipeline-api's own browser/CLI handlers use OPAQUE session cookies
//     (see PostgresResolver / LocalResolver). There is NO JWT to inspect
//     here, so WithUser doesn't run a token_kind check.
//   - lakekeeper's OIDC validator (LAKEKEEPER__OPENID_PROVIDER_URI) is
//     the verifier for run + impersonation tokens. It validates
//     signature + audience + expiry against pipeline-api's JWKS;
//     audience-mismatch tokens are rejected at lakekeeper, NOT at this
//     middleware.
//
// If pipeline-api ever adopts JWT-based session auth (replacing the
// cookie shape), THIS function is the place to add the token_kind check.
// Until then, the session resolvers are the only auth surface and the
// type-level guarantee on MintRunToken/MintImpersonation (actor derived
// from ctx — see tokens.subjectFromCtx) prevents forgery at the source.
//
// Cookie refresh semantics are resolver-specific — PostgresResolver
// refreshes the session cookie before returning; LocalResolver never
// touches cookies.
func WithUser(resolver UserResolver, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, authed, err := resolver.UserFor(w, r)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "auth error")
			return
		}
		if !authed {
			writeJSONError(w, http.StatusUnauthorized, "not authenticated")
			return
		}
		ctx := WithCtxUser(r.Context(), user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// WithCtxUser binds u to ctx using the same key UserFromContext reads.
// Exported so tests + token-mint helpers (tokens.subjectFromCtx) can
// build authenticated contexts without spinning up an http.Server +
// resolver chain.
func WithCtxUser(ctx context.Context, u *store.User) context.Context {
	return context.WithValue(ctx, ctxKey{}, u)
}

// UserFromContext returns the user attached by WithUser. Returns (nil, false)
// when no user is in ctx.
func UserFromContext(ctx context.Context) (*store.User, bool) {
	u, ok := ctx.Value(ctxKey{}).(*store.User)
	return u, ok && u != nil
}
