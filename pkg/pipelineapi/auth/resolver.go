package auth

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// UserResolver maps an inbound HTTP request to a User (or 401).
// PostgresResolver validates the session cookie against a Postgres
// sessions/users store.
//
// UserFor returns (user, authed, err):
//   - authed=false, err=nil  → the request has no valid session (401).
//   - authed=false, err!=nil → infrastructure error (500).
//   - authed=true            → user is the authenticated user.
//
// PostgresResolver may mutate w to refresh the session cookie; that's
// why this takes (w, r) rather than just r.
//
// Mode returns the deployment mode tag exposed to the UI.
// SupportsLogin gates registration of login/logout routes.
type UserResolver interface {
	UserFor(w http.ResponseWriter, r *http.Request) (*store.User, bool, error)
	Mode() string
	SupportsLogin() bool
}

// PostgresResolver looks up the session cookie in the sessions table,
// loads the linked user, refreshes the cookie, and slides the session
// expiry via TouchSession. Mirrors the old WithUser body 1:1.
type PostgresResolver struct {
	pool         *pgxpool.Pool
	cookieSecure bool
}

// NewPostgresResolver constructs the cluster-mode resolver. cookieSecure
// must match the Server.WithCookieSecure setting so the refresh cookie
// keeps the same Secure flag as the one issued at login.
func NewPostgresResolver(pool *pgxpool.Pool, cookieSecure bool) *PostgresResolver {
	return &PostgresResolver{pool: pool, cookieSecure: cookieSecure}
}

// UserFor mirrors the old WithUser body: parse the session cookie, look
// up the session and user, reject if missing/expired/disabled, touch the
// session, and refresh the browser cookie's MaxAge.
//
// A missing or malformed cookie, an expired session, a deleted or
// disabled user all map to (nil, false, nil) — i.e. unauthenticated but
// not an infrastructure error. Only real DB/lookup failures return err.
func (p *PostgresResolver) UserFor(w http.ResponseWriter, r *http.Request) (*store.User, bool, error) {
	c, err := r.Cookie(SessionCookieName)
	if err != nil {
		return nil, false, nil
	}
	sid, err := uuid.Parse(c.Value)
	if err != nil {
		return nil, false, nil
	}
	sess, err := LookupSession(r.Context(), p.pool, sid)
	if err != nil {
		if errors.Is(err, ErrSessionNotFound) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("session lookup: %w", err)
	}
	user, err := store.GetUserByID(r.Context(), p.pool, sess.UserID)
	if errors.Is(err, store.ErrUserNotFound) {
		// User row removed between LookupSession and GetUserByID
		// (e.g., admin deleted the account). The session is
		// effectively invalid — treat as unauthenticated.
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("user lookup: %w", err)
	}
	if user.DisabledAt != nil {
		return nil, false, nil
	}

	// Best-effort inline touch — one small UPDATE adds <1ms to an
	// authenticated request and avoids unbounded goroutine proliferation
	// under DB contention. Errors are swallowed so a transient touch
	// failure never fails a request the user is otherwise authorized for.
	_ = TouchSession(r.Context(), p.pool, sid)

	// Refresh the browser cookie's MaxAge so the server-side sliding
	// expiry is reflected client-side; without this the cookie would
	// expire 24h after login even though the DB session has been
	// extended.
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    c.Value,
		Path:     "/",
		HttpOnly: true,
		Secure:   p.cookieSecure,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(SessionLifetime.Seconds()),
	})
	return user, true, nil
}

// Mode returns "cluster" — PostgresResolver runs against a full Postgres
// deployment.
func (p *PostgresResolver) Mode() string { return "cluster" }

// SupportsLogin returns true — PostgresResolver accepts POST /auth/login
// to exchange a password for a session cookie.
func (p *PostgresResolver) SupportsLogin() bool { return true }

// Ensure PostgresResolver satisfies UserResolver.
var _ UserResolver = (*PostgresResolver)(nil)
