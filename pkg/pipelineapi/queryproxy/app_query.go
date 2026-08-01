package queryproxy

// App-principal query route (RFC 028 P5): app-worker presents an app's own
// per-render impersonation JWT (token_kind=app, minted by
// tokens.MintAppToken — RFC 028 P4) directly as the Authorization bearer
// credential, in place of a platform session. This is registered as a
// SEPARATE handler/route (AppHTTPHandler, mounted at
// POST /internal/v1/projects/{pid}/query in pkg/pipelineapi/http/server.go)
// from the browser/CLI-facing HTTPHandler (POST /api/v1/projects/{pid}/query):
// that route's mux registration wraps auth.WithUser, which hard-requires
// resolving a *store.User — an app has no such row (task-P0-report.md §D).
// Extending auth.WithUser in place would mean either changing its contract
// for every existing session-based caller or re-implementing its
// cookie/error-handling behaviour here for no benefit.
//
// Both routes share every other seam: the same *Core (gates, worker client,
// signer, audit counter), the same projectgate.Gate FGA check
// (h.cfg.Gate.QualifiedWarehouse, inside serveWithAudit), and the same audit
// emission. That sharing is what satisfies the security requirement that an
// app can reach nothing a project viewer grant would not also reach: the
// app path is not a bypass of the browser path's authorization, it is the
// same authorization call with a different (still-bare) subject string.
//
// Security posture: the app JWT itself is the entire credential on this
// route. It is short-lived (60s, tokens.AppTokenLifetime), narrowly scoped
// (aud=datuplet-catalog, token_kind=app only), and every query it
// authorizes is independently re-checked against FGA via the Gate call
// above — a leaked token expires on its own within a minute regardless
// (spec §5.4's blast-radius property). No additional service-credential
// wrapper is layered on top: that would only prove "this is app-worker
// calling", which the six existing /internal/v1/* apps routes already do
// for the impersonate-mint step that produces this very token; it would add
// nothing the JWT signature + per-query FGA check don't already enforce.
import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// appTokenClockSkew mirrors auth.BearerJWTResolver's leeway. The app
// token's own TTL (tokens.AppTokenLifetime) is 60s — RS256 signature + exp
// are what actually gate access; the skew only tolerates clock drift
// between pipeline-api and whatever minted the token (itself).
const appTokenClockSkew = 60 * time.Second

// appPrincipal is the resolved, verified identity of an app-render caller.
type appPrincipal struct {
	// sub is the raw JWT `sub` claim ("app-<uuid>", identity.AppJWTSubject's
	// shape) — bare, no oidc~/user: prefix. It is fed into
	// projectgate.Gate/serveWithAudit exactly like a *store.User's UUID
	// string: the gate composes the FGA prefix itself (authz.UserObject), so
	// passing an already-prefixed value here would double-prefix and never
	// match the viewer tuple apps.identityManager.Register wrote.
	sub string
	// jti is the app token's own jti claim, carried into query_audit
	// verbatim so a query_audit row can be joined back to the
	// impersonation_minted control-plane record that produced this token
	// (task-P4-report.md).
	jti string
	// rawToken is the verbatim presented JWT. MintAppToken already scopes it
	// aud=datuplet-catalog (task-P4-report.md), so it IS the catalog
	// credential outright — executeRaw forwards it unchanged instead of
	// minting a fresh one (there is no ctx-bound *store.User subject to mint
	// from for an app in the first place; task-P0-report.md §B).
	rawToken string
	// appID is the token's `app_id` claim: the bare app UUID (no "app-"
	// prefix). executeRaw borrows it to satisfy MintInternalQueryToken's
	// ctx-bound-subject requirement for the pipeline-api→query-worker hop
	// token — that hop is aud=datuplet-query-worker, internal-only, and the
	// worker's own verifier only requires *a* non-empty sub (never presented
	// to lakekeeper, never FGA-checked), so using the app's own id there
	// does not misrepresent anything.
	appID string
}

// parseAppBearer extracts and verifies an app-kind JWT from the request's
// Authorization header. It returns (nil, false) for anything that is not a
// currently-valid, token_kind=app / aud=datuplet-catalog JWT signed by
// signer — a missing/malformed header, a different scheme, an expired or
// not-yet-valid token, a bad signature or kid, or any OTHER token_kind
// (run/impersonation/local-cli/query/cli-api/internal-query/...) presented
// here. That last case is the regression guard: only the "app" kind added
// by RFC 028 P4 is accepted on this route (task-P0-report.md §A6's
// existing-kinds matrix must not gain a new hole).
//
// Verification mirrors auth.BearerJWTResolver's contract (RS256 signature,
// kid, iss, exp/nbf with clock skew) with the app-specific aud/kind pinned
// in place of cli-api's.
func parseAppBearer(r *http.Request, signer *tokens.Signer) (*appPrincipal, bool) {
	if signer == nil {
		return nil, false
	}
	authzHeader := r.Header.Get("Authorization")
	if authzHeader == "" {
		return nil, false
	}
	if !strings.HasPrefix(strings.ToLower(authzHeader), "bearer ") {
		return nil, false
	}
	raw := strings.TrimSpace(authzHeader[len("Bearer "):])
	if raw == "" {
		return nil, false
	}

	tok, err := jwt.Parse(raw, func(t *jwt.Token) (any, error) {
		if t.Method.Alg() != "RS256" {
			return nil, fmt.Errorf("unexpected alg %q", t.Method.Alg())
		}
		if kid, _ := t.Header["kid"].(string); kid != signer.KeyID {
			return nil, fmt.Errorf("unknown kid %q", kid)
		}
		return signer.Public(), nil
	},
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithLeeway(appTokenClockSkew),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
	)
	if err != nil || !tok.Valid {
		return nil, false
	}
	claims, ok := tok.Claims.(jwt.MapClaims)
	if !ok {
		return nil, false
	}
	if iss, _ := claims["iss"].(string); iss != "datuplet-api" {
		return nil, false
	}
	if aud, _ := claims["aud"].(string); aud != tokens.TableTokenAudience {
		return nil, false
	}
	if kind, _ := claims["token_kind"].(string); kind != tokens.TokenKindApp {
		return nil, false
	}
	sub, _ := claims["sub"].(string)
	if sub == "" || !strings.HasPrefix(sub, tokens.AppSubjectPrefix) {
		return nil, false
	}
	jti, _ := claims["jti"].(string)
	appID, _ := claims["app_id"].(string)
	return &appPrincipal{sub: sub, jti: jti, rawToken: raw, appID: appID}, true
}

// appHandler wraps the same *handler the browser/CLI route uses; ServeHTTP
// resolves the principal from the presented app JWT instead of
// auth.UserFromContext.
type appHandler struct {
	h *handler
}

// AppHTTPHandler returns the POST .../query handler for app-worker's
// per-render queries (RFC 028 P5). See the package doc above for why this
// is a distinct handler/route from HTTPHandler's browser/CLI path.
func (c *Core) AppHTTPHandler() http.Handler {
	return &appHandler{h: c.h}
}

func (a *appHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	principal, ok := parseAppBearer(r, a.h.signer)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized", "not authenticated")
		return
	}
	a.h.serveWithAudit(w, r, principal.sub, time.Now(), principal)
}
