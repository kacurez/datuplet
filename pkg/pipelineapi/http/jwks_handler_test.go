package http_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// newJWKSTestServer returns an httptest.Server whose Handler is the full
// pipeline-api ServeMux with a signer attached. No DB pool — JWKS must work
// independently of DB state so the /healthz-only mode still publishes a
// stable JWKS for operators.
func newJWKSTestServer(t *testing.T) (*httptest.Server, *tokens.Signer) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen: %v", err)
	}
	dir := t.TempDir()
	der, _ := x509.MarshalPKCS8PrivateKey(priv)
	path := filepath.Join(dir, "priv.pem")
	_ = os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o400)
	signer, err := tokens.LoadPrivateKeyFromPEMFile(path, "kid-test")
	if err != nil {
		t.Fatalf("load signer: %v", err)
	}
	srv := apihttp.NewServer(nil).WithSigner(signer)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, signer
}

func TestJWKS_Shape(t *testing.T) {
	ts, _ := newJWKSTestServer(t)

	resp, err := stdhttp.Get(ts.URL + "/api/v1/auth/jwks.json")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var body tokens.JWKS
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Keys) != 1 {
		t.Fatalf("keys len = %d, want 1", len(body.Keys))
	}
	k := body.Keys[0]
	if k.Kid != "kid-test" || k.Alg != "RS256" || k.Kty != "RSA" || k.Use != "sig" {
		t.Errorf("unexpected JWK: %+v", k)
	}
	if k.N == "" || k.E == "" {
		t.Error("N/E empty")
	}
}

func TestJWKS_NotServedWhenSignerAbsent(t *testing.T) {
	// No signer → route must not be registered; server returns 404.
	srv := apihttp.NewServer(nil)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	resp, err := stdhttp.Get(ts.URL + "/api/v1/auth/jwks.json")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("status = %d, want 404 when no signer configured", resp.StatusCode)
	}
}
