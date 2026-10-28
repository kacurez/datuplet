package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

const (
	testKID = "test-kid-1"
	testSub = "11111111-1111-1111-1111-111111111111"
)

// stubKeyProvider is a test double for KeyProvider holding a kid->key map.
type stubKeyProvider struct {
	keys map[string]*rsa.PublicKey
}

func (s *stubKeyProvider) KeyFor(_ context.Context, kid string) (*rsa.PublicKey, error) {
	k, ok := s.keys[kid]
	if !ok {
		return nil, errors.New("stub: kid not found: " + kid)
	}
	return k, nil
}

// genKey generates a 2048-bit RSA key pair. Fatal on error.
func genKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	return priv
}

// signClaims signs claims with priv as RS256, setting kid in the header.
func signClaims(t *testing.T, priv *rsa.PrivateKey, kid string, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = kid
	s, err := tok.SignedString(priv)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return s
}

// validClaims returns a claim set that passes every check: a well-formed
// internal-query token bound to testSub with a per-request jti.
func validClaims(sub string) jwt.MapClaims {
	now := time.Now()
	return jwt.MapClaims{
		"iss":        "datuplet-api",
		"aud":        tokens.QueryWorkerAudience,
		"sub":        sub,
		"actor":      sub,
		"token_kind": tokens.TokenKindInternalQuery,
		"iat":        now.Unix(),
		"nbf":        now.Unix(),
		"exp":        now.Add(5 * time.Minute).Unix(),
		"jti":        fmt.Sprintf("iqry-%s-%d", sub, now.UnixNano()),
	}
}

func provider(t *testing.T, priv *rsa.PrivateKey) *stubKeyProvider {
	t.Helper()
	return &stubKeyProvider{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}
}

func TestVerify_Valid(t *testing.T) {
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	claims := validClaims(testSub)
	wantJTI, _ := claims["jti"].(string)
	tokenStr := signClaims(t, priv, testKID, claims)

	sub, jti, err := v.Verify(context.Background(), tokenStr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sub != testSub {
		t.Errorf("sub = %q, want %q", sub, testSub)
	}
	if jti != wantJTI {
		t.Errorf("jti = %q, want %q", jti, wantJTI)
	}
}

// mintFn builds the token string for one verification-matrix row.
type mintFn func(t *testing.T, priv *rsa.PrivateKey, c jwt.MapClaims) string

func mintRS256(t *testing.T, priv *rsa.PrivateKey, c jwt.MapClaims) string {
	return signClaims(t, priv, testKID, c)
}

// TestVerify_ClaimMatrix is the JWT verification matrix: each row mutates a
// valid claim set (or the signing step) and asserts accept/reject.
func TestVerify_ClaimMatrix(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(c jwt.MapClaims)
		mint    mintFn // nil = RS256 with testKID
		wantErr bool
	}{
		{
			// Cross-token replay: a query token minted for lakekeeper
			// (aud=datuplet-catalog) must NOT be accepted by the query-worker.
			name: "catalog-audience query token rejected",
			mutate: func(c jwt.MapClaims) {
				c["aud"] = tokens.TableTokenAudience
				c["token_kind"] = tokens.TokenKindQuery
			},
			wantErr: true,
		},
		{
			// aud already QueryWorkerAudience, but token_kind=query.
			name:    "token_kind=query with worker aud rejected",
			mutate:  func(c jwt.MapClaims) { c["token_kind"] = tokens.TokenKindQuery },
			wantErr: true,
		},
		{
			// A run token (token_kind=run, aud=datuplet-catalog) must be rejected.
			name: "run-token shape rejected",
			mutate: func(c jwt.MapClaims) {
				c["aud"] = tokens.TableTokenAudience
				c["token_kind"] = tokens.TokenKindRun
			},
			wantErr: true,
		},
		{
			// 10 minutes in the past, well beyond the +-60s skew window.
			name: "expired rejected",
			mutate: func(c jwt.MapClaims) {
				c["exp"] = time.Now().Add(-10 * time.Minute).Unix()
				c["nbf"] = time.Now().Add(-15 * time.Minute).Unix()
			},
			wantErr: true,
		},
		{
			// nbf 10 minutes in the future, beyond the skew window.
			name:    "not yet valid rejected",
			mutate:  func(c jwt.MapClaims) { c["nbf"] = time.Now().Add(10 * time.Minute).Unix() },
			wantErr: true,
		},
		{
			// exp 30s in the past: inside the +-60s leeway window.
			name:   "expired within skew accepted",
			mutate: func(c jwt.MapClaims) { c["exp"] = time.Now().Add(-30 * time.Second).Unix() },
		},
		{
			name: "alg=none rejected",
			mint: func(t *testing.T, _ *rsa.PrivateKey, c jwt.MapClaims) string {
				tok := jwt.NewWithClaims(jwt.SigningMethodNone, c)
				tok.Header["kid"] = testKID
				s, err := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
				if err != nil {
					t.Fatalf("mint none-alg token: %v", err)
				}
				return s
			},
			wantErr: true,
		},
		{
			// Sign with HS256 using arbitrary bytes; WithValidMethods must reject.
			name: "HS256 rejected",
			mint: func(t *testing.T, _ *rsa.PrivateKey, c jwt.MapClaims) string {
				tok := jwt.NewWithClaims(jwt.SigningMethodHS256, c)
				tok.Header["kid"] = testKID
				s, err := tok.SignedString([]byte("attacker-shared-secret"))
				if err != nil {
					t.Fatalf("mint HS256 token: %v", err)
				}
				return s
			},
			wantErr: true,
		},
		{
			// Right key, but a kid the provider doesn't know.
			name: "unknown kid rejected",
			mint: func(t *testing.T, priv *rsa.PrivateKey, c jwt.MapClaims) string {
				return signClaims(t, priv, "unknown-kid", c)
			},
			wantErr: true,
		},
		{
			name:    "empty sub rejected",
			mutate:  func(c jwt.MapClaims) { c["sub"] = "" },
			wantErr: true,
		},
		{
			name:    "wrong issuer rejected",
			mutate:  func(c jwt.MapClaims) { c["iss"] = "evil-issuer" },
			wantErr: true,
		},
		{
			// nbf 30s in the future: inside the +-60s leeway window.
			name:   "nbf within skew accepted",
			mutate: func(c jwt.MapClaims) { c["nbf"] = time.Now().Add(30 * time.Second).Unix() },
		},
		{
			// []string signs to a JSON array, so the parse path sees a
			// []interface{} and exercises GetAudience's array branch.
			name:   "aud JSON array single element accepted",
			mutate: func(c jwt.MapClaims) { c["aud"] = []string{queryWorkerAudience} },
		},
		{
			// Strict single-audience: worker + catalog audience is a
			// dual-audience replay and must be rejected outright.
			name:    "aud JSON array multi element rejected",
			mutate:  func(c jwt.MapClaims) { c["aud"] = []string{queryWorkerAudience, "datuplet-catalog"} },
			wantErr: true,
		},
		{
			// Must be rejected without panicking on the type assertion.
			name:    "non-string token_kind rejected",
			mutate:  func(c jwt.MapClaims) { c["token_kind"] = 42 },
			wantErr: true,
		},
		{
			// Pins jwt.WithExpirationRequired.
			name:    "missing exp rejected",
			mutate:  func(c jwt.MapClaims) { delete(c, "exp") },
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			priv := genKey(t)
			v := newTokenVerifier(provider(t, priv))

			c := validClaims(testSub)
			if tc.mutate != nil {
				tc.mutate(c)
			}
			mint := tc.mint
			if mint == nil {
				mint = mintRS256
			}
			_, _, err := v.Verify(context.Background(), mint(t, priv, c))
			if tc.wantErr && err == nil {
				t.Fatal("expected rejection, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected acceptance, got: %v", err)
			}
		})
	}
}

func TestVerify_BadSignature(t *testing.T) {
	signerKey := genKey(t)
	differentKey := genKey(t)

	// Provider returns the DIFFERENT key for testKID; the token was signed
	// with signerKey → signature mismatch.
	v := newTokenVerifier(&stubKeyProvider{keys: map[string]*rsa.PublicKey{testKID: &differentKey.PublicKey}})
	tokenStr := signClaims(t, signerKey, testKID, validClaims(testSub))

	if _, _, err := v.Verify(context.Background(), tokenStr); err == nil {
		t.Fatal("expected rejection of bad signature, got nil")
	}
}

// TestVerify_ErrorNeverLeaksToken proves the sanitized-error discipline: a
// rejection error must not embed the token text, nor the attacker-controlled
// claim value (static iss-mismatch message).
func TestVerify_ErrorNeverLeaksToken(t *testing.T) {
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	c := validClaims(testSub)
	// Attacker-controlled iss with a newline-injection payload. The static
	// "iss mismatch" message must NOT echo it back into the error.
	c["iss"] = "evil\nINJECTED"
	tokenStr := signClaims(t, priv, testKID, c)

	_, _, err := v.Verify(context.Background(), tokenStr)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if strings.Contains(err.Error(), tokenStr) {
		t.Errorf("error message leaked the token text: %v", err)
	}
	if strings.Contains(err.Error(), "evil") {
		t.Errorf("error message leaked the attacker-controlled iss claim: %v", err)
	}
	if !strings.Contains(err.Error(), "internal token rejected") {
		t.Errorf("error should carry the stable prefix, got: %v", err)
	}
}

// TestConstantsParity pins the locally-declared verifier constants to the
// canonical tokens-package values. verify.go re-declares them rather than
// importing tokens (to keep the auth→store→pgx/openfga chain out of the worker
// binary); this test — which IS allowed to import tokens, since test files do
// not link into the binary — guards against the two drifting apart.
func TestConstantsParity(t *testing.T) {
	if queryWorkerAudience != tokens.QueryWorkerAudience {
		t.Errorf("queryWorkerAudience = %q, want tokens.QueryWorkerAudience = %q",
			queryWorkerAudience, tokens.QueryWorkerAudience)
	}
	if tokenKindInternalQuery != tokens.TokenKindInternalQuery {
		t.Errorf("tokenKindInternalQuery = %q, want tokens.TokenKindInternalQuery = %q",
			tokenKindInternalQuery, tokens.TokenKindInternalQuery)
	}
}
