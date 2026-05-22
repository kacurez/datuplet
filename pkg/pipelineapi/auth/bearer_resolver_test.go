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

func TestBearerJWTResolver_ValidToken(t *testing.T) {
	signer, _ := testSignerAndKey(t)
	userID := uuid.New()
	pool := &fakeUserLookup{
		users: map[uuid.UUID]*store.User{
			userID: {ID: userID},
		},
	}
	r := buildResolver(signer, pool)

	req := bearerRequest(t, mintCLIAPIToken(t, signer, userID, time.Hour))
	user, ok, err := r.UserFor(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true for valid token")
	}
	if user == nil || user.ID != userID {
		t.Errorf("user = %v, want id=%v", user, userID)
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

func TestBearerJWTResolver_LowerCaseBearer(t *testing.T) {
	signer, _ := testSignerAndKey(t)
	userID := uuid.New()
	pool := &fakeUserLookup{
		users: map[uuid.UUID]*store.User{
			userID: {ID: userID},
		},
	}
	r := buildResolver(signer, pool)

	tok := mintCLIAPIToken(t, signer, userID, time.Hour)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "bearer "+tok) // lowercase
	user, ok, err := r.UserFor(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true for lowercase 'bearer' prefix")
	}
	if user == nil || user.ID != userID {
		t.Errorf("user.ID = %v, want %v", user.ID, userID)
	}
}

func TestBearerJWTResolver_WrongAlg(t *testing.T) {
	signer, _ := testSignerAndKey(t)
	r := buildResolver(signer, &fakeUserLookup{users: map[uuid.UUID]*store.User{}})

	// Mint an HS256 token manually.
	claims := jwt.MapClaims{
		"iss":        "datuplet-api",
		"aud":        "datuplet-api",
		"sub":        uuid.New().String(),
		"token_kind": "cli-api",
		"exp":        time.Now().Add(time.Hour).Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokStr, _ := tok.SignedString([]byte("secret"))

	req := bearerRequest(t, tokStr)
	user, ok, err := r.UserFor(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok || user != nil {
		t.Errorf("expected rejection of HS256 token, got ok=%v", ok)
	}
}

func TestBearerJWTResolver_WrongKid(t *testing.T) {
	signer, priv := testSignerAndKey(t)

	// Build a token with a different kid header.
	userID := uuid.New()
	u := &store.User{ID: userID}
	ctx := auth.WithCtxUser(context.Background(), u)
	// Mint with the signer (correct key) but manually override kid.
	tok, _, err := tokens.MintCLIAPIToken(ctx, signer, time.Hour)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	// Decode, change kid, re-sign with the same key.
	parsed, _ := jwt.Parse(tok, func(t *jwt.Token) (any, error) { return &priv.PublicKey, nil },
		jwt.WithValidMethods([]string{"RS256"}))
	claims := parsed.Claims.(jwt.MapClaims)
	newTok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	newTok.Header["kid"] = "wrong-kid"
	signed, _ := newTok.SignedString(priv)

	r := buildResolver(signer, &fakeUserLookup{users: map[uuid.UUID]*store.User{userID: {ID: userID}}})
	req := bearerRequest(t, signed)
	_, ok, err := r.UserFor(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected rejection of wrong kid")
	}
}

func TestBearerJWTResolver_WrongIss(t *testing.T) {
	signer, priv := testSignerAndKey(t)
	userID := uuid.New()

	claims := jwt.MapClaims{
		"iss":        "wrong-issuer",
		"aud":        "datuplet-api",
		"sub":        userID.String(),
		"token_kind": "cli-api",
		"exp":        time.Now().Add(time.Hour).Unix(),
		"nbf":        time.Now().Unix(),
		"iat":        time.Now().Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signer.KeyID
	signed, _ := tok.SignedString(priv)

	r := buildResolver(signer, &fakeUserLookup{users: map[uuid.UUID]*store.User{userID: {ID: userID}}})
	req := bearerRequest(t, signed)
	_, ok, err := r.UserFor(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected rejection of wrong iss")
	}
}

func TestBearerJWTResolver_WrongAud(t *testing.T) {
	signer, priv := testSignerAndKey(t)
	userID := uuid.New()

	claims := jwt.MapClaims{
		"iss":        "datuplet-api",
		"aud":        "datuplet-catalog", // wrong: catalog, not api
		"sub":        userID.String(),
		"token_kind": "cli-api",
		"exp":        time.Now().Add(time.Hour).Unix(),
		"nbf":        time.Now().Unix(),
		"iat":        time.Now().Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signer.KeyID
	signed, _ := tok.SignedString(priv)

	r := buildResolver(signer, &fakeUserLookup{users: map[uuid.UUID]*store.User{userID: {ID: userID}}})
	req := bearerRequest(t, signed)
	_, ok, err := r.UserFor(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected rejection of wrong aud")
	}
}

func TestBearerJWTResolver_WrongTokenKind(t *testing.T) {
	signer, priv := testSignerAndKey(t)
	userID := uuid.New()

	claims := jwt.MapClaims{
		"iss":        "datuplet-api",
		"aud":        "datuplet-api",
		"sub":        userID.String(),
		"token_kind": "local-cli", // wrong kind
		"exp":        time.Now().Add(time.Hour).Unix(),
		"nbf":        time.Now().Unix(),
		"iat":        time.Now().Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signer.KeyID
	signed, _ := tok.SignedString(priv)

	r := buildResolver(signer, &fakeUserLookup{users: map[uuid.UUID]*store.User{userID: {ID: userID}}})
	req := bearerRequest(t, signed)
	_, ok, err := r.UserFor(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected rejection of wrong token_kind")
	}
}

func TestBearerJWTResolver_Expired(t *testing.T) {
	signer, priv := testSignerAndKey(t)
	userID := uuid.New()

	// Expired by 2 minutes — outside the 60s leeway.
	claims := jwt.MapClaims{
		"iss":        "datuplet-api",
		"aud":        "datuplet-api",
		"sub":        userID.String(),
		"token_kind": "cli-api",
		"exp":        time.Now().Add(-2 * time.Minute).Unix(),
		"nbf":        time.Now().Add(-3 * time.Minute).Unix(),
		"iat":        time.Now().Add(-3 * time.Minute).Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signer.KeyID
	signed, _ := tok.SignedString(priv)

	r := buildResolver(signer, &fakeUserLookup{users: map[uuid.UUID]*store.User{userID: {ID: userID}}})
	req := bearerRequest(t, signed)
	_, ok, err := r.UserFor(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected rejection of expired token")
	}
}

func TestBearerJWTResolver_SubNotUUID(t *testing.T) {
	signer, priv := testSignerAndKey(t)

	claims := jwt.MapClaims{
		"iss":        "datuplet-api",
		"aud":        "datuplet-api",
		"sub":        "not-a-uuid",
		"token_kind": "cli-api",
		"exp":        time.Now().Add(time.Hour).Unix(),
		"nbf":        time.Now().Unix(),
		"iat":        time.Now().Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signer.KeyID
	signed, _ := tok.SignedString(priv)

	r := buildResolver(signer, &fakeUserLookup{users: map[uuid.UUID]*store.User{}})
	req := bearerRequest(t, signed)
	_, ok, err := r.UserFor(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected rejection of non-UUID sub")
	}
}

func TestBearerJWTResolver_UserNotFound(t *testing.T) {
	signer, _ := testSignerAndKey(t)
	userID := uuid.New()
	// Pool with no users.
	r := buildResolver(signer, &fakeUserLookup{users: map[uuid.UUID]*store.User{}})

	req := bearerRequest(t, mintCLIAPIToken(t, signer, userID, time.Hour))
	_, ok, err := r.UserFor(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected rejection when user not found")
	}
}

func TestBearerJWTResolver_UserDisabled(t *testing.T) {
	signer, _ := testSignerAndKey(t)
	userID := uuid.New()
	now := time.Now()
	pool := &fakeUserLookup{
		users: map[uuid.UUID]*store.User{
			userID: {ID: userID, DisabledAt: &now},
		},
	}
	r := buildResolver(signer, pool)

	req := bearerRequest(t, mintCLIAPIToken(t, signer, userID, time.Hour))
	_, ok, err := r.UserFor(httptest.NewRecorder(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected rejection for disabled user")
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
func (n *neverAuthResolver) Mode() string         { return "never" }
func (n *neverAuthResolver) SupportsLogin() bool  { return false }
