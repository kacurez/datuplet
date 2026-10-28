package auth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// testSignerAndKey builds a Signer and returns it alongside the raw RSA key
// so tests can use Public() for the resolver.
func testSignerAndKey(t *testing.T) (*tokens.Signer, *rsa.PrivateKey) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen: %v", err)
	}
	dir := t.TempDir()
	privDER, _ := x509.MarshalPKCS8PrivateKey(priv)
	privPath := filepath.Join(dir, "priv.pem")
	_ = os.WriteFile(privPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}), 0o400)
	signer, err := tokens.LoadPrivateKeyFromPEMFile(privPath, "test-kid")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return signer, priv
}

// fakeUserLookup is a test fake for auth.UserLookup.
type fakeUserLookup struct {
	users map[uuid.UUID]*store.User
	err   error
}

func (f *fakeUserLookup) GetUserByID(_ context.Context, id uuid.UUID) (*store.User, error) {
	if f.err != nil {
		return nil, f.err
	}
	u, ok := f.users[id]
	if !ok {
		return nil, store.ErrUserNotFound
	}
	return u, nil
}

// mintCLIAPIToken is a test helper that mints a cli-api token for a given user.
func mintCLIAPIToken(t *testing.T, signer *tokens.Signer, userID uuid.UUID, lifetime time.Duration) string {
	t.Helper()
	u := &store.User{ID: userID}
	ctx := auth.WithCtxUser(context.Background(), u)
	tok, _, err := tokens.MintCLIAPIToken(ctx, signer, lifetime)
	if err != nil {
		t.Fatalf("MintCLIAPIToken: %v", err)
	}
	return tok
}

// bearerRequest builds an *http.Request with an Authorization: Bearer header.
func bearerRequest(t *testing.T, tok string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	return req
}

// buildResolver builds a BearerJWTResolver backed by the signer's public key
// and the given fake pool.
func buildResolver(signer *tokens.Signer, pool auth.UserLookup) *auth.BearerJWTResolver {
	return &auth.BearerJWTResolver{
		PublicKey: signer.Public(),
		KeyID:     signer.KeyID,
		Pool:      pool,
	}
}

func TestBearerJWTResolver_NoAuthorizationHeader(t *testing.T) {
	signer, _ := testSignerAndKey(t)
	r := buildResolver(signer, &fakeUserLookup{users: map[uuid.UUID]*store.User{}})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	user, ok, err := r.UserFor(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok || user != nil {
		t.Errorf("expected (nil, false, nil) for missing header, got ok=%v user=%v", ok, user)
	}
}

func TestBearerJWTResolver_InfraError(t *testing.T) {
	signer, _ := testSignerAndKey(t)
	userID := uuid.New()
	pool := &fakeUserLookup{
		err: fmt.Errorf("connection refused"),
	}
	r := buildResolver(signer, pool)

	req := bearerRequest(t, mintCLIAPIToken(t, signer, userID, time.Hour))
	_, ok, err := r.UserFor(httptest.NewRecorder(), req)
	if err == nil {
		t.Fatal("expected infrastructure error to be returned")
	}
	if ok {
		t.Error("expected ok=false on infra error")
	}
}

func TestChainResolver_FirstMatchWins(t *testing.T) {
	signer, _ := testSignerAndKey(t)
	userID := uuid.New()
	pool := &fakeUserLookup{
		users: map[uuid.UUID]*store.User{
			userID: {ID: userID},
		},
	}
	bearer := &auth.BearerJWTResolver{
		PublicKey: signer.Public(),
		KeyID:     signer.KeyID,
		Pool:      pool,
	}
	// A resolver that always says "not authenticated".
	cookieFallback := &neverAuthResolver{}

	chain := &auth.ChainResolver{Resolvers: []auth.UserResolver{bearer, cookieFallback}}

	req := bearerRequest(t, mintCLIAPIToken(t, signer, userID, time.Hour))
	user, ok, err := chain.UserFor(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok || user == nil {
		t.Fatal("expected chain to resolve via bearer")
	}
	if user.ID != userID {
		t.Errorf("user.ID = %v, want %v", user.ID, userID)
	}
}

func TestChainResolver_FallsThrough(t *testing.T) {
	signer, _ := testSignerAndKey(t)
	pool := &fakeUserLookup{users: map[uuid.UUID]*store.User{}}

	bearer := &auth.BearerJWTResolver{
		PublicKey: signer.Public(),
		KeyID:     signer.KeyID,
		Pool:      pool,
	}
	chain := &auth.ChainResolver{Resolvers: []auth.UserResolver{bearer}}

	// Request with no Authorization header.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	user, ok, err := chain.UserFor(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok || user != nil {
		t.Errorf("expected (nil, false, nil) when no resolver matches")
	}
}

// neverAuthResolver always returns (nil, false, nil).
type neverAuthResolver struct{}

func (n *neverAuthResolver) UserFor(_ http.ResponseWriter, _ *http.Request) (*store.User, bool, error) {
	return nil, false, nil
}
func (n *neverAuthResolver) Mode() string        { return "never" }
func (n *neverAuthResolver) SupportsLogin() bool { return false }

// alwaysLoginResolver always reports SupportsLogin=true and a specific mode.
// Used to test ChainResolver Primary delegation.
type alwaysLoginResolver struct{ mode string }

func (a *alwaysLoginResolver) UserFor(_ http.ResponseWriter, _ *http.Request) (*store.User, bool, error) {
	return nil, false, nil
}
func (a *alwaysLoginResolver) Mode() string        { return a.mode }
func (a *alwaysLoginResolver) SupportsLogin() bool { return true }

func TestBearerJWTResolver_ValidToken(t *testing.T) {
	cases := []struct {
		name   string
		scheme string
	}{
		{"bearer_prefix", "Bearer "},
		{"lower_case_bearer_prefix", "bearer "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			signer, _ := testSignerAndKey(t)
			userID := uuid.New()
			pool := &fakeUserLookup{
				users: map[uuid.UUID]*store.User{
					userID: {ID: userID},
				},
			}
			r := buildResolver(signer, pool)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Authorization", tc.scheme+mintCLIAPIToken(t, signer, userID, time.Hour))
			user, ok, err := r.UserFor(httptest.NewRecorder(), req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !ok {
				t.Fatalf("expected ok=true for %q prefix", tc.scheme)
			}
			if user == nil || user.ID != userID {
				t.Errorf("user = %v, want id=%v", user, userID)
			}
		})
	}
}

// TestBearerJWTResolver_RejectsInvalidToken: every claim/header the resolver
// validates, broken one at a time against an otherwise valid cli-api token,
// must be rejected as (nil, false, nil) — unauthenticated, not an error.
func TestBearerJWTResolver_RejectsInvalidToken(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(c jwt.MapClaims)
		hs256  bool   // sign with HS256 instead of RS256
		kid    string // override the kid header ("" = the signer's)
	}{
		{name: "wrong_alg", hs256: true},
		{name: "wrong_kid", kid: "wrong-kid"},
		{name: "wrong_iss", mutate: func(c jwt.MapClaims) { c["iss"] = "wrong-issuer" }},
		// wrong: catalog, not api
		{name: "wrong_aud", mutate: func(c jwt.MapClaims) { c["aud"] = "datuplet-catalog" }},
		{name: "wrong_token_kind", mutate: func(c jwt.MapClaims) { c["token_kind"] = "local-cli" }},
		// Expired by 2 minutes — outside the 60s leeway.
		{name: "expired", mutate: func(c jwt.MapClaims) {
			c["exp"] = time.Now().Add(-2 * time.Minute).Unix()
			c["nbf"] = time.Now().Add(-3 * time.Minute).Unix()
			c["iat"] = time.Now().Add(-3 * time.Minute).Unix()
		}},
		{name: "sub_not_uuid", mutate: func(c jwt.MapClaims) { c["sub"] = "not-a-uuid" }},
		// WithExpirationRequired enforcement.
		{name: "no_exp", mutate: func(c jwt.MapClaims) { delete(c, "exp") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			signer, priv := testSignerAndKey(t)
			userID := uuid.New()

			claims := jwt.MapClaims{
				"iss":        "datuplet-api",
				"aud":        "datuplet-api",
				"sub":        userID.String(),
				"token_kind": "cli-api",
				"exp":        time.Now().Add(time.Hour).Unix(),
				"nbf":        time.Now().Unix(),
				"iat":        time.Now().Unix(),
			}
			if tc.mutate != nil {
				tc.mutate(claims)
			}
			var signed string
			if tc.hs256 {
				signed, _ = jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte("secret"))
			} else {
				tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
				tok.Header["kid"] = signer.KeyID
				if tc.kid != "" {
					tok.Header["kid"] = tc.kid
				}
				signed, _ = tok.SignedString(priv)
			}

			r := buildResolver(signer, &fakeUserLookup{users: map[uuid.UUID]*store.User{userID: {ID: userID}}})
			user, ok, err := r.UserFor(httptest.NewRecorder(), bearerRequest(t, signed))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok || user != nil {
				t.Errorf("expected rejection, got ok=%v user=%v", ok, user)
			}
		})
	}
}

func TestBearerJWTResolver_RejectsUnusableUser(t *testing.T) {
	now := time.Now()
	userID := uuid.New()
	cases := []struct {
		name string
		pool *fakeUserLookup
	}{
		{"user_not_found", &fakeUserLookup{users: map[uuid.UUID]*store.User{}}},
		{"user_disabled", &fakeUserLookup{users: map[uuid.UUID]*store.User{userID: {ID: userID, DisabledAt: &now}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			signer, _ := testSignerAndKey(t)
			r := buildResolver(signer, tc.pool)
			_, ok, err := r.UserFor(httptest.NewRecorder(), bearerRequest(t, mintCLIAPIToken(t, signer, userID, time.Hour)))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok {
				t.Error("expected rejection")
			}
		})
	}
}

// TestChainResolver_Primary asserts that Mode()/SupportsLogin() delegate to
// Primary, NOT to the first resolver in Resolvers, and that a nil Primary
// falls back to the first resolver (backward compat).
func TestChainResolver_Primary(t *testing.T) {
	signer, _ := testSignerAndKey(t)
	bearerR := &auth.BearerJWTResolver{
		PublicKey: signer.Public(),
		KeyID:     signer.KeyID,
		Pool:      &fakeUserLookup{users: map[uuid.UUID]*store.User{}},
	}
	cookieR := &alwaysLoginResolver{mode: "cluster"}
	first := &alwaysLoginResolver{mode: "first-mode"}
	second := &alwaysLoginResolver{mode: "second-mode"}

	cases := []struct {
		name      string
		chain     *auth.ChainResolver
		wantMode  string
		wantLogin bool
	}{
		{"primary_delegation", &auth.ChainResolver{
			Resolvers: []auth.UserResolver{bearerR, cookieR},
			Primary:   cookieR,
		}, "cluster", true},
		{"no_primary_falls_back_to_first", &auth.ChainResolver{
			// Primary intentionally nil.
			Resolvers: []auth.UserResolver{first, second},
		}, "first-mode", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.chain.Mode(); got != tc.wantMode {
				t.Errorf("Mode() = %q, want %q", got, tc.wantMode)
			}
			if got := tc.chain.SupportsLogin(); got != tc.wantLogin {
				t.Errorf("SupportsLogin() = %v, want %v", got, tc.wantLogin)
			}
		})
	}
}
