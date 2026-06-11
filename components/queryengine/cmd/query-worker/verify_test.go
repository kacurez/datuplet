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

func TestVerify_WrongAud_CatalogQueryToken(t *testing.T) {
	// Cross-token replay: a query token minted for lakekeeper
	// (aud=datuplet-catalog) must NOT be accepted by the query-worker.
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	c := validClaims(testSub)
	c["aud"] = tokens.TableTokenAudience // datuplet-catalog
	c["token_kind"] = tokens.TokenKindQuery
	tokenStr := signClaims(t, priv, testKID, c)

	if _, _, err := v.Verify(context.Background(), tokenStr); err == nil {
		t.Fatal("expected rejection of catalog-audience token, got nil")
	}
}

func TestVerify_WrongTokenKind_QueryWithWorkerAud(t *testing.T) {
	// Hand-crafted: aud forced to the worker audience but token_kind=query.
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	c := validClaims(testSub)
	c["token_kind"] = tokens.TokenKindQuery // aud already QueryWorkerAudience
	tokenStr := signClaims(t, priv, testKID, c)

	if _, _, err := v.Verify(context.Background(), tokenStr); err == nil {
		t.Fatal("expected rejection of token_kind=query, got nil")
	}
}

func TestVerify_RunTokenShape(t *testing.T) {
	// A run token (token_kind=run, aud=datuplet-catalog) must be rejected.
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	c := validClaims(testSub)
	c["aud"] = tokens.TableTokenAudience
	c["token_kind"] = tokens.TokenKindRun
	tokenStr := signClaims(t, priv, testKID, c)

	if _, _, err := v.Verify(context.Background(), tokenStr); err == nil {
		t.Fatal("expected rejection of run-token shape, got nil")
	}
}

func TestVerify_Expired(t *testing.T) {
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	c := validClaims(testSub)
	// 10 minutes in the past — well beyond the ±60s skew window.
	c["exp"] = time.Now().Add(-10 * time.Minute).Unix()
	c["nbf"] = time.Now().Add(-15 * time.Minute).Unix()
	tokenStr := signClaims(t, priv, testKID, c)

	if _, _, err := v.Verify(context.Background(), tokenStr); err == nil {
		t.Fatal("expected rejection of expired token, got nil")
	}
}

func TestVerify_NotYetValid(t *testing.T) {
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	c := validClaims(testSub)
	// nbf 10 minutes in the future — beyond the skew window.
	c["nbf"] = time.Now().Add(10 * time.Minute).Unix()
	tokenStr := signClaims(t, priv, testKID, c)

	if _, _, err := v.Verify(context.Background(), tokenStr); err == nil {
		t.Fatal("expected rejection of not-yet-valid token, got nil")
	}
}

func TestVerify_ExpiredWithinSkew_Accepted(t *testing.T) {
	// exp 30s in the past: inside the ±60s leeway window → ACCEPTED.
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	c := validClaims(testSub)
	c["exp"] = time.Now().Add(-30 * time.Second).Unix()
	tokenStr := signClaims(t, priv, testKID, c)

	if _, _, err := v.Verify(context.Background(), tokenStr); err != nil {
		t.Fatalf("token expired 30s ago should be accepted within ±60s skew, got: %v", err)
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

func TestVerify_AlgNoneRejected(t *testing.T) {
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	c := validClaims(testSub)
	tok := jwt.NewWithClaims(jwt.SigningMethodNone, c)
	tok.Header["kid"] = testKID
	tokenStr, err := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("mint none-alg token: %v", err)
	}

	if _, _, err := v.Verify(context.Background(), tokenStr); err == nil {
		t.Fatal("expected rejection of alg=none token, got nil")
	}
}

func TestVerify_HS256Rejected(t *testing.T) {
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	// Sign with HS256 using arbitrary bytes; WithValidMethods must reject.
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, validClaims(testSub))
	tok.Header["kid"] = testKID
	tokenStr, err := tok.SignedString([]byte("attacker-shared-secret"))
	if err != nil {
		t.Fatalf("mint HS256 token: %v", err)
	}

	if _, _, err := v.Verify(context.Background(), tokenStr); err == nil {
		t.Fatal("expected rejection of HS256 token, got nil")
	}
}

func TestVerify_UnknownKid(t *testing.T) {
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	// Sign with the right key but stamp a kid the provider doesn't know.
	tokenStr := signClaims(t, priv, "unknown-kid", validClaims(testSub))

	if _, _, err := v.Verify(context.Background(), tokenStr); err == nil {
		t.Fatal("expected rejection of unknown kid, got nil")
	}
}

func TestVerify_EmptySub(t *testing.T) {
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	c := validClaims(testSub)
	c["sub"] = ""
	tokenStr := signClaims(t, priv, testKID, c)

	if _, _, err := v.Verify(context.Background(), tokenStr); err == nil {
		t.Fatal("expected rejection of empty sub, got nil")
	}
}

func TestVerify_WrongIssuer(t *testing.T) {
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	c := validClaims(testSub)
	c["iss"] = "evil-issuer"
	tokenStr := signClaims(t, priv, testKID, c)

	if _, _, err := v.Verify(context.Background(), tokenStr); err == nil {
		t.Fatal("expected rejection of wrong issuer, got nil")
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

func TestVerify_NbfWithinSkew_Accepted(t *testing.T) {
	// nbf 30s in the future: inside the ±60s leeway window → ACCEPTED.
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	c := validClaims(testSub)
	c["nbf"] = time.Now().Add(30 * time.Second).Unix()
	tokenStr := signClaims(t, priv, testKID, c)

	if _, _, err := v.Verify(context.Background(), tokenStr); err != nil {
		t.Fatalf("token with nbf +30s should be accepted within ±60s skew, got: %v", err)
	}
}

func TestVerify_AudJSONArray_SingleElement_Accepted(t *testing.T) {
	// aud as a JSON array with the single worker audience. Setting it to
	// []string here means that after signing + JSON round-trip the parse path
	// sees a []interface{} — exercising GetAudience's array branch.
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	c := validClaims(testSub)
	c["aud"] = []string{queryWorkerAudience}
	tokenStr := signClaims(t, priv, testKID, c)

	if _, _, err := v.Verify(context.Background(), tokenStr); err != nil {
		t.Fatalf("single-element aud array should be accepted, got: %v", err)
	}
}

func TestVerify_AudJSONArray_MultiElement_Rejected(t *testing.T) {
	// Strict single-audience: a token carrying both the worker audience and
	// the catalog audience must be rejected outright (dual-audience replay).
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	c := validClaims(testSub)
	c["aud"] = []string{queryWorkerAudience, "datuplet-catalog"}
	tokenStr := signClaims(t, priv, testKID, c)

	if _, _, err := v.Verify(context.Background(), tokenStr); err == nil {
		t.Fatal("expected rejection of multi-audience token, got nil")
	}
}

func TestVerify_NonStringTokenKind_Rejected(t *testing.T) {
	// token_kind as a number must be rejected (not match the expected string)
	// and must not panic on the type assertion.
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	c := validClaims(testSub)
	c["token_kind"] = 42
	tokenStr := signClaims(t, priv, testKID, c)

	if _, _, err := v.Verify(context.Background(), tokenStr); err == nil {
		t.Fatal("expected rejection of non-string token_kind, got nil")
	}
}

func TestVerify_MissingExp_Rejected(t *testing.T) {
	// A token without exp must be rejected (pins jwt.WithExpirationRequired).
	priv := genKey(t)
	v := newTokenVerifier(provider(t, priv))

	c := validClaims(testSub)
	delete(c, "exp")
	tokenStr := signClaims(t, priv, testKID, c)

	if _, _, err := v.Verify(context.Background(), tokenStr); err == nil {
		t.Fatal("expected rejection of token missing exp, got nil")
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
