package runtoken_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/datuplet/datuplet/pkg/datagateway/runtoken"
)

// fakeJWKSClient is a test double for JWKSClient that holds a map of kid ->
// *rsa.PublicKey. Use wrongKeyClient to simulate a wrong-key scenario.
type fakeJWKSClient struct {
	keys map[string]*rsa.PublicKey
}

func (f *fakeJWKSClient) KeyFor(_ context.Context, kid string) (*rsa.PublicKey, error) {
	k, ok := f.keys[kid]
	if !ok {
		return nil, errors.New("test: kid not found: " + kid)
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

// mintClaims signs the given MapClaims with priv, setting kid=kid in the token
// header. Helper used by all test cases.
func mintClaims(t *testing.T, priv *rsa.PrivateKey, kid string, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = kid
	s, err := tok.SignedString(priv)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return s
}

// writeToken writes tokenStr to a temp file and returns its path.
func writeToken(t *testing.T, tokenStr string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte(tokenStr), 0600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	return path
}

// validClaims returns a jwt.MapClaims that passes all 8 checks when signed
// with the correct key and the expectedRunID matches runID.
func validClaims(runID string) jwt.MapClaims {
	now := time.Now()
	return jwt.MapClaims{
		"iss":        "datuplet-api",
		"aud":        "datuplet-catalog",
		"sub":        runID,
		"token_kind": "run",
		"project_id": "proj-123",
		"warehouse":  "my-warehouse",
		"run_id":     runID,
		"iat":        now.Unix(),
		"nbf":        now.Unix(),
		"exp":        now.Add(24 * time.Hour).Unix(),
		"jti":        "run-tok-" + runID,
	}
}

const (
	testKID   = "test-kid-1"
	testRunID = "run-abc-123"
)

func TestHappyPath(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	tokenStr := mintClaims(t, priv, testKID, validClaims(testRunID))
	path := writeToken(t, tokenStr)

	claims, err := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, testRunID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if claims.RunID != testRunID {
		t.Errorf("RunID = %q, want %q", claims.RunID, testRunID)
	}
	if claims.Subject != testRunID {
		t.Errorf("Subject = %q, want %q", claims.Subject, testRunID)
	}
	if claims.ProjectID != "proj-123" {
		t.Errorf("ProjectID = %q, want proj-123", claims.ProjectID)
	}
	if claims.Warehouse != "my-warehouse" {
		t.Errorf("Warehouse = %q, want my-warehouse", claims.Warehouse)
	}
	if claims.Issuer != "datuplet-api" {
		t.Errorf("Issuer = %q, want datuplet-api", claims.Issuer)
	}
	if claims.Audience != "datuplet-catalog" {
		t.Errorf("Audience = %q, want datuplet-catalog", claims.Audience)
	}
	if claims.Expiry.IsZero() {
		t.Error("Expiry should not be zero")
	}
}

func TestWrongSig(t *testing.T) {
	signerKey := genKey(t)
	differentKey := genKey(t)

	// Register the DIFFERENT key in JWKS, sign with signerKey → sig invalid.
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &differentKey.PublicKey}}
	tokenStr := mintClaims(t, signerKey, testKID, validClaims(testRunID))
	path := writeToken(t, tokenStr)

	_, err := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, testRunID)
	if err == nil {
		t.Fatal("expected signature error, got nil")
	}
	if !strings.Contains(err.Error(), "check 1") {
		t.Errorf("error should mention check 1, got: %v", err)
	}
}

func TestWrongIss(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	c := validClaims(testRunID)
	c["iss"] = "wrong-issuer"
	path := writeToken(t, mintClaims(t, priv, testKID, c))

	_, err := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, testRunID)
	if err == nil {
		t.Fatal("expected issuer error, got nil")
	}
	if !strings.Contains(err.Error(), "check 2") {
		t.Errorf("error should mention check 2, got: %v", err)
	}
}

func TestWrongAud(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	c := validClaims(testRunID)
	c["aud"] = "wrong-audience"
	path := writeToken(t, mintClaims(t, priv, testKID, c))

	_, err := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, testRunID)
	if err == nil {
		t.Fatal("expected audience error, got nil")
	}
	if !strings.Contains(err.Error(), "check 3") {
		t.Errorf("error should mention check 3, got: %v", err)
	}
}

func TestExpired(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	c := validClaims(testRunID)
	// Set exp 5 minutes in the past (beyond ClockSkewSeconds tolerance of 60s).
	c["exp"] = time.Now().Add(-5 * time.Minute).Unix()
	c["nbf"] = time.Now().Add(-10 * time.Minute).Unix()
	path := writeToken(t, mintClaims(t, priv, testKID, c))

	_, err := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, testRunID)
	if err == nil {
		t.Fatal("expected expiry error, got nil")
	}
	// The error from jwt.ParseWithClaims is wrapped under check 1.
	if !strings.Contains(err.Error(), "check 1") {
		t.Errorf("error should mention check 1 (exp validation is done by ParseWithClaims), got: %v", err)
	}
}

func TestNotYetValid(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	c := validClaims(testRunID)
	// Set nbf 5 minutes in the future (beyond ClockSkewSeconds tolerance of 60s).
	c["nbf"] = time.Now().Add(5 * time.Minute).Unix()
	path := writeToken(t, mintClaims(t, priv, testKID, c))

	_, err := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, testRunID)
	if err == nil {
		t.Fatal("expected nbf error, got nil")
	}
	// The error from jwt.ParseWithClaims is wrapped under check 1.
	if !strings.Contains(err.Error(), "check 1") {
		t.Errorf("error should mention check 1 (nbf validation is done by ParseWithClaims), got: %v", err)
	}
}

func TestWrongTokenKind(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	c := validClaims(testRunID)
	c["token_kind"] = "user"
	path := writeToken(t, mintClaims(t, priv, testKID, c))

	_, err := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, testRunID)
	if err == nil {
		t.Fatal("expected token_kind error, got nil")
	}
	if !strings.Contains(err.Error(), "check 5") {
		t.Errorf("error should mention check 5, got: %v", err)
	}
}

func TestMissingProjectID(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	c := validClaims(testRunID)
	delete(c, "project_id")
	path := writeToken(t, mintClaims(t, priv, testKID, c))

	_, err := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, testRunID)
	if err == nil {
		t.Fatal("expected missing project_id error, got nil")
	}
	if !strings.Contains(err.Error(), "check 6") {
		t.Errorf("error should mention check 6, got: %v", err)
	}
}

func TestMissingWarehouse(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	c := validClaims(testRunID)
	delete(c, "warehouse")
	path := writeToken(t, mintClaims(t, priv, testKID, c))

	_, err := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, testRunID)
	if err == nil {
		t.Fatal("expected missing warehouse error, got nil")
	}
	if !strings.Contains(err.Error(), "check 6") {
		t.Errorf("error should mention check 6, got: %v", err)
	}
}

func TestMissingRunID(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	c := validClaims(testRunID)
	delete(c, "run_id")
	path := writeToken(t, mintClaims(t, priv, testKID, c))

	_, err := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, testRunID)
	if err == nil {
		t.Fatal("expected missing run_id error, got nil")
	}
	if !strings.Contains(err.Error(), "check 6") {
		t.Errorf("error should mention check 6, got: %v", err)
	}
}

func TestMissingSub(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	c := validClaims(testRunID)
	delete(c, "sub")
	path := writeToken(t, mintClaims(t, priv, testKID, c))

	_, err := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, testRunID)
	if err == nil {
		t.Fatal("expected missing sub error, got nil")
	}
	// jwt-go v5 may catch missing sub or our check 6 does.
	if !strings.Contains(err.Error(), "check 1") && !strings.Contains(err.Error(), "check 6") {
		t.Errorf("error should mention check 1 or check 6, got: %v", err)
	}
}

func TestSubNotEqualRunID(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	c := validClaims(testRunID)
	c["sub"] = "different-subject"
	// run_id still = testRunID; sub != run_id
	path := writeToken(t, mintClaims(t, priv, testKID, c))

	_, err := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, testRunID)
	if err == nil {
		t.Fatal("expected sub != run_id error, got nil")
	}
	if !strings.Contains(err.Error(), "check 7") {
		t.Errorf("error should mention check 7, got: %v", err)
	}
}

func TestRunIDNotEqualExpected(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	c := validClaims(testRunID)
	// sub == run_id == testRunID but expectedRunID is different.
	path := writeToken(t, mintClaims(t, priv, testKID, c))

	_, err := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, "different-expected-run-id")
	if err == nil {
		t.Fatal("expected run_id != env error, got nil")
	}
	if !strings.Contains(err.Error(), "check 8") {
		t.Errorf("error should mention check 8, got: %v", err)
	}
}

func TestPathEmpty(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	_, err := runtoken.LoadAndValidateRunToken(context.Background(), "", jwksClient, testRunID)
	if err == nil {
		t.Fatal("expected error for empty path, got nil")
	}
}

func TestPathMissing(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	_, err := runtoken.LoadAndValidateRunToken(context.Background(), "/nonexistent/path/token", jwksClient, testRunID)
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestOversizedFile(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	dir := t.TempDir()
	path := filepath.Join(dir, "big-token")
	// Write more than maxRunTokenFileBytes (2 MiB) to the file.
	bigContent := strings.Repeat("x", (2<<20)+1024)
	if err := os.WriteFile(path, []byte(bigContent), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, err := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, testRunID)
	if err == nil {
		t.Fatal("expected error for oversized file, got nil")
	}
	// The exact error is a JWT parse failure (truncated content won't parse).
}

func TestMalformedJWT(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	path := writeToken(t, "not.a.valid.jwt.at.all")

	_, err := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, testRunID)
	if err == nil {
		t.Fatal("expected error for malformed JWT, got nil")
	}
}

// TestRS384Rejected proves that algorithm-confusion is prevented. Before the
// fix, the keyfunc checked `t.Method.(*jwt.SigningMethodRSA)` which succeeded
// for RS256, RS384, AND RS512 (all three are *SigningMethodRSA instances). An
// RS384 token signed with the legitimate private key and matching kid would
// verify cleanly.
//
// After the fix the keyfunc demands pointer-equality with the RS256 singleton
// AND jwt.WithValidMethods([]string{"RS256"}) is applied at the parser level
// so the parser refuses the token before the keyfunc even runs. Either layer
// alone closes the gap; both together is defense-in-depth.
func TestRS384Rejected(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	// Sign with RS384 using the legitimate private key. JWKS returns the
	// matching public key for the kid; pre-fix this would verify successfully.
	tok := jwt.NewWithClaims(jwt.SigningMethodRS384, validClaims(testRunID))
	tok.Header["kid"] = testKID
	tokenStr, err := tok.SignedString(priv)
	if err != nil {
		t.Fatalf("mint RS384 token: %v", err)
	}
	path := writeToken(t, tokenStr)

	_, valErr := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, testRunID)
	if valErr == nil {
		t.Fatal("expected RS384 token to be rejected, got nil error")
	}
	if !strings.Contains(valErr.Error(), "check 1") {
		t.Errorf("error should mention check 1, got: %v", valErr)
	}
}

// TestCheck8ErrorMessageDoesNotLeakRunIDs proves the P1-B fix: check 8's
// error message must not include either run ID. Mirrors check 7's discipline.
func TestCheck8ErrorMessageDoesNotLeakRunIDs(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	const envRunID = "env-supplied-run-id-XYZ"
	const jwtRunID = "jwt-claim-run-id-ABC"

	c := validClaims(jwtRunID) // sub == run_id == jwtRunID; passes check 7
	path := writeToken(t, mintClaims(t, priv, testKID, c))

	_, err := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, envRunID)
	if err == nil {
		t.Fatal("expected check 8 error, got nil")
	}
	msg := err.Error()
	if strings.Contains(msg, envRunID) {
		t.Errorf("check 8 error must NOT contain env run ID %q, got: %v", envRunID, err)
	}
	if strings.Contains(msg, jwtRunID) {
		t.Errorf("check 8 error must NOT contain JWT run ID %q, got: %v", jwtRunID, err)
	}
	if !strings.Contains(msg, "check 8") {
		t.Errorf("error should mention check 8, got: %v", err)
	}
}

func TestUnsignedAlgNoneRejected(t *testing.T) {
	priv := genKey(t)
	jwksClient := &fakeJWKSClient{keys: map[string]*rsa.PublicKey{testKID: &priv.PublicKey}}

	// Build a token with alg=none by crafting the claims with SigningMethodNone.
	// golang-jwt/jwt/v5 requires UnsafeAllowNoneSignatureType for this.
	c := validClaims(testRunID)
	tok := jwt.NewWithClaims(jwt.SigningMethodNone, c)
	tok.Header["kid"] = testKID
	tokenStr, err := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("mint none-alg token: %v", err)
	}
	path := writeToken(t, tokenStr)

	_, valErr := runtoken.LoadAndValidateRunToken(context.Background(), path, jwksClient, testRunID)
	if valErr == nil {
		t.Fatal("expected error for alg=none token, got nil")
	}
	// The keyfunc pins RS256; none triggers the "unexpected signing method" branch.
	if !strings.Contains(valErr.Error(), "check 1") {
		t.Errorf("error should mention check 1, got: %v", valErr)
	}
}
