package auth

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// BearerJWTResolver authenticates requests bearing a JWT minted by
// MintCLIAPIToken. Validation contract:
//
//   - Authorization: Bearer <jwt>  (case-insensitive scheme)
//   - RS256 signature, key resolved by `kid` against the API's signer
//   - iss == "datuplet-api"
//   - aud == "datuplet-api"          (token_kind=cli-api scope)
//   - token_kind == "cli-api"
//   - exp/nbf/iat checked with ±60s clock skew
//   - sub parses as user UUID; user exists and is not disabled
//
// On any failure the resolver returns (nil, false, nil) — i.e. the
// request is unauthenticated, and the middleware can fall through to
// another resolver (the session-cookie one) or 401.
//
// Designed for chaining via ChainResolver so cookie-bearing browser
// requests still work alongside Bearer-bearing CLI requests.
type BearerJWTResolver struct {
	PublicKey *rsa.PublicKey
	KeyID     string
	Pool      UserLookup // narrow interface — see below
}

// UserLookup decouples this resolver from pgxpool — accepts either a
// pgxpool-backed implementation or a test fake.
type UserLookup interface {
	GetUserByID(ctx context.Context, id uuid.UUID) (*store.User, error)
}

const (
	bearerClockSkew         = 60 * time.Second
	bearerExpectedIssuer    = "datuplet-api"
	bearerExpectedAudience  = "datuplet-api"
	bearerExpectedTokenKind = "cli-api"
)

// UserFor implements the core of BearerJWTResolver. See type-level doc
// for the full validation contract.
func (r *BearerJWTResolver) UserFor(_ http.ResponseWriter, req *http.Request) (*store.User, bool, error) {
	authz := req.Header.Get("Authorization")
	if authz == "" {
		return nil, false, nil
	}
	// Case-insensitive Bearer scheme.
	if !strings.HasPrefix(strings.ToLower(authz), "bearer ") {
		return nil, false, nil
	}
	tokStr := strings.TrimSpace(authz[len("Bearer "):])
	if tokStr == "" {
		return nil, false, nil
	}

	tok, err := jwt.Parse(tokStr, func(t *jwt.Token) (any, error) {
		if t.Method.Alg() != "RS256" {
			return nil, fmt.Errorf("unexpected alg %q", t.Method.Alg())
		}
		if kid, _ := t.Header["kid"].(string); kid != r.KeyID {
			return nil, fmt.Errorf("unknown kid %q", kid)
		}
		return r.PublicKey, nil
	},
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithLeeway(bearerClockSkew),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
	)
	if err != nil || !tok.Valid {
		return nil, false, nil
	}

	claims, ok := tok.Claims.(jwt.MapClaims)
	if !ok {
		return nil, false, nil
	}

	if iss, _ := claims["iss"].(string); iss != bearerExpectedIssuer {
		return nil, false, nil
	}
	// aud may be string or []any in jwt.MapClaims
	if !audienceContains(claims["aud"], bearerExpectedAudience) {
		return nil, false, nil
	}
	if tk, _ := claims["token_kind"].(string); tk != bearerExpectedTokenKind {
		return nil, false, nil
	}

	subStr, _ := claims["sub"].(string)
	uid, err := uuid.Parse(subStr)
	if err != nil {
		return nil, false, nil
	}

	user, err := r.Pool.GetUserByID(req.Context(), uid)
	if errors.Is(err, store.ErrUserNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("bearer user lookup: %w", err)
	}
	if user.DisabledAt != nil {
		return nil, false, nil
	}
	return user, true, nil
}

// Mode returns "bearer" — BearerJWTResolver is always combined with a
// primary resolver via ChainResolver; the ChainResolver delegates Mode()
// to the first (primary) resolver, so this value is informational only.
func (r *BearerJWTResolver) Mode() string { return "bearer" }

// SupportsLogin returns false — BearerJWTResolver accepts pre-minted JWTs,
// not password grants.
func (r *BearerJWTResolver) SupportsLogin() bool { return false }

// Ensure BearerJWTResolver satisfies UserResolver.
var _ UserResolver = (*BearerJWTResolver)(nil)

func audienceContains(claim any, want string) bool {
	switch v := claim.(type) {
	case string:
		return v == want
	case []any:
		for _, item := range v {
			if s, ok := item.(string); ok && s == want {
				return true
			}
		}
	case []string:
		for _, s := range v {
			if s == want {
				return true
			}
		}
	}
	return false
}

// ChainResolver tries each child resolver in order, returning the first
// authenticated result. Errors short-circuit. Use for cookie+bearer.
//
// Mode() and SupportsLogin() delegate to Primary when set; Primary is the
// resolver that owns deployment-mode and login-route gating (typically the
// session-cookie resolver). UserFor() still tries every resolver in order.
type ChainResolver struct {
	Resolvers []UserResolver
	// Primary is the resolver that owns Mode()/SupportsLogin() answers
	// (deployment mode + login-route gating). UserFor() still tries every
	// Resolver in order. When nil, falls back to the first resolver in the
	// chain; safe default, but prefer explicit wiring.
	Primary UserResolver
}

// UserFor tries each resolver in order, returning the first authenticated
// (user, true, nil) result. Infrastructure errors short-circuit.
func (c *ChainResolver) UserFor(w http.ResponseWriter, r *http.Request) (*store.User, bool, error) {
	for _, child := range c.Resolvers {
		user, ok, err := child.UserFor(w, r)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return user, true, nil
		}
	}
	return nil, false, nil
}

// Mode delegates to Primary when set, otherwise falls back to the first
// resolver in the chain.
func (c *ChainResolver) Mode() string {
	if c.Primary != nil {
		return c.Primary.Mode()
	}
	if len(c.Resolvers) > 0 {
		return c.Resolvers[0].Mode()
	}
	return "browser" // safe default; never reached in production
}

// SupportsLogin delegates to Primary when set, otherwise falls back to the
// first resolver in the chain.
func (c *ChainResolver) SupportsLogin() bool {
	if c.Primary != nil {
		return c.Primary.SupportsLogin()
	}
	if len(c.Resolvers) > 0 {
		return c.Resolvers[0].SupportsLogin()
	}
	return false
}

// Ensure ChainResolver satisfies UserResolver.
var _ UserResolver = (*ChainResolver)(nil)

// PgxUserLookup adapts *pgxpool.Pool to the UserLookup interface so
// BearerJWTResolver can be constructed without holding a pgxpool reference
// directly in the resolver type — useful for test fakes.
type PgxUserLookup struct {
	pool *pgxpool.Pool
}

// NewPgxUserLookup wraps a *pgxpool.Pool as a UserLookup.
func NewPgxUserLookup(pool *pgxpool.Pool) *PgxUserLookup {
	return &PgxUserLookup{pool: pool}
}

// GetUserByID satisfies UserLookup.
func (p *PgxUserLookup) GetUserByID(ctx context.Context, id uuid.UUID) (*store.User, error) {
	return store.GetUserByID(ctx, p.pool, id)
}
