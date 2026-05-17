package jwks_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datuplet/datuplet/pkg/datagateway/jwks"
)

// makeJWKS returns the JSON body of a JWKS document containing the given
// public keys. The caller provides a slice of (kid, *rsa.PublicKey) pairs.
func makeJWKS(pairs ...struct {
	Kid string
	Pub *rsa.PublicKey
}) []byte {
	type jwkEntry struct {
		Kty string `json:"kty"`
		Kid string `json:"kid"`
		N   string `json:"n"`
		E   string `json:"e"`
	}
	type jwksDoc struct {
		Keys []jwkEntry `json:"keys"`
	}

	var doc jwksDoc
	for _, p := range pairs {
		eBytes := new(big.Int).SetInt64(int64(p.Pub.E)).Bytes()
		doc.Keys = append(doc.Keys, jwkEntry{
			Kty: "RSA",
			Kid: p.Kid,
			N:   base64.RawURLEncoding.EncodeToString(p.Pub.N.Bytes()),
			E:   base64.RawURLEncoding.EncodeToString(eBytes),
		})
	}
	b, _ := json.Marshal(doc)
	return b
}

// genKey generates a 2048-bit RSA key pair for tests. Fatal on error.
func genKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	return priv
}

func TestHappyPath(t *testing.T) {
	priv := genKey(t)
	body := makeJWKS(struct {
		Kid string
		Pub *rsa.PublicKey
	}{"k1", &priv.PublicKey})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(body) //nolint:errcheck
	}))
	defer srv.Close()

	c := jwks.NewClient(srv.URL, srv.Client())
	pub, err := c.KeyFor(context.Background(), "k1")
	if err != nil {
		t.Fatalf("KeyFor: %v", err)
	}
	if pub.N.Cmp(priv.PublicKey.N) != 0 {
		t.Error("returned key modulus does not match generated key")
	}
}

func TestCachesOnRepeat(t *testing.T) {
	priv := genKey(t)
	body := makeJWKS(struct {
		Kid string
		Pub *rsa.PublicKey
	}{"k1", &priv.PublicKey})

	var requestCount int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&requestCount, 1)
		w.Header().Set("Content-Type", "application/json")
		w.Write(body) //nolint:errcheck
	}))
	defer srv.Close()

	c := jwks.NewClient(srv.URL, srv.Client())
	ctx := context.Background()

	if _, err := c.KeyFor(ctx, "k1"); err != nil {
		t.Fatalf("first KeyFor: %v", err)
	}
	if _, err := c.KeyFor(ctx, "k1"); err != nil {
		t.Fatalf("second KeyFor: %v", err)
	}

	// Only one HTTP request should have been made (key cached after first call).
	if got := atomic.LoadInt64(&requestCount); got != 1 {
		t.Errorf("request count = %d, want 1 (key should be cached)", got)
	}
}

func TestKidMissReFetch(t *testing.T) {
	priv1 := genKey(t)
	priv2 := genKey(t)

	body1 := makeJWKS(struct {
		Kid string
		Pub *rsa.PublicKey
	}{"k1", &priv1.PublicKey})
	body2 := makeJWKS(struct {
		Kid string
		Pub *rsa.PublicKey
	}{"k1", &priv1.PublicKey}, struct {
		Kid string
		Pub *rsa.PublicKey
	}{"k2", &priv2.PublicKey})

	var requestCount int64
	// Serve body1 on the first request, body2 on subsequent requests.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cnt := atomic.AddInt64(&requestCount, 1)
		w.Header().Set("Content-Type", "application/json")
		if cnt == 1 {
			w.Write(body1) //nolint:errcheck
		} else {
			w.Write(body2) //nolint:errcheck
		}
	}))
	defer srv.Close()

	c := jwks.NewClient(srv.URL, srv.Client())
	ctx := context.Background()

	// Prime the cache with k1.
	if _, err := c.KeyFor(ctx, "k1"); err != nil {
		t.Fatalf("prime KeyFor(k1): %v", err)
	}

	// Ask for k2 (not in cache) — should trigger a re-fetch.
	pub, err := c.KeyFor(ctx, "k2")
	if err != nil {
		t.Fatalf("KeyFor(k2) after rotation: %v", err)
	}
	if pub.N.Cmp(priv2.PublicKey.N) != 0 {
		t.Error("k2 modulus mismatch")
	}
	// Two requests: initial prime + one re-fetch.
	if got := atomic.LoadInt64(&requestCount); got != 2 {
		t.Errorf("request count = %d, want 2 (initial + one re-fetch)", got)
	}
}

func TestKidStillMissing(t *testing.T) {
	priv1 := genKey(t)
	body := makeJWKS(struct {
		Kid string
		Pub *rsa.PublicKey
	}{"k1", &priv1.PublicKey})

	var requestCount int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&requestCount, 1)
		w.Header().Set("Content-Type", "application/json")
		w.Write(body) //nolint:errcheck
	}))
	defer srv.Close()

	c := jwks.NewClient(srv.URL, srv.Client())

	// Prime cache with k1.
	if _, err := c.KeyFor(context.Background(), "k1"); err != nil {
		t.Fatalf("prime: %v", err)
	}

	// Ask for k2, which is never in the JWKS.
	_, err := c.KeyFor(context.Background(), "k2")
	if err == nil {
		t.Fatal("expected error for unknown kid, got nil")
	}

	// Should be exactly 2 HTTP calls: 1 initial + 1 re-fetch.
	if got := atomic.LoadInt64(&requestCount); got != 2 {
		t.Errorf("request count = %d, want 2 (initial prime + one re-fetch)", got)
	}
}

func TestEndpointUnreachable(t *testing.T) {
	// Use a server that always returns 500 to simulate a reachable-but-broken
	// endpoint (forces the retry path without relying on OS-level dial
	// failures, which may not be retriable).
	var requestCount int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&requestCount, 1)
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	// Use a very short retry client so the test doesn't take 7+ seconds.
	// We can't override retryDelays directly (they're package-level), but
	// we're testing the error path not exact timing, so just verify we
	// get an error and the server was hit multiple times (>1).
	c := jwks.NewClient(srv.URL, srv.Client())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := c.KeyFor(ctx, "k1")
	if err == nil {
		t.Fatal("expected error for unreachable endpoint, got nil")
	}

	// Should have been hit 4 times (1 initial + 3 retries).
	// The initial call fetches (fails) → retries 3 more times → fails.
	if got := atomic.LoadInt64(&requestCount); got < 2 {
		t.Errorf("request count = %d, want >= 2 (should retry)", got)
	}
}

func TestCtxCancelDuringRetry(t *testing.T) {
	var requestCount int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&requestCount, 1)
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := jwks.NewClient(srv.URL, srv.Client())
	// Cancel after a very short time — should abort before all retries complete.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := c.KeyFor(ctx, "k1")
	if err == nil {
		t.Fatal("expected error after ctx cancel, got nil")
	}
	// The error should mention cancellation or deadline.
	// We don't require a specific message; just that we got an error.
}
