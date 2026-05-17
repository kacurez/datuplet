package jwks

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"time"
)

// jwksDocument is the JSON shape returned by /api/v1/auth/jwks.json
// (RFC 7517 §5). We only handle RSA keys (kty=RSA).
type jwksDocument struct {
	Keys []jwkEntry `json:"keys"`
}

// jwkEntry is a single public-key entry in a JWKS document.
type jwkEntry struct {
	Kty string `json:"kty"` // "RSA"
	Kid string `json:"kid"` // key ID
	N   string `json:"n"`   // base64url(modulus), no padding
	E   string `json:"e"`   // base64url(public exponent), no padding
}

// maxJWKSBytes caps the JWKS response body to guard against runaway responses.
// Pipeline-api returns a single-key JWKS (~500 bytes); 1 MiB is very generous.
const maxJWKSBytes = 1 << 20

// retryDelays is the fixed backoff schedule for JWKS fetch retries:
// 3 attempts, 1s / 2s / 4s backoff.
var retryDelays = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}

// fetchAndDecode GETs the JWKS endpoint, decodes the JWK array, and converts
// each RSA key to *rsa.PublicKey. Returns a map from kid to public key.
//
// Bounded retry: 3 attempts with 1s, 2s, 4s backoff before failing closed.
// Honours ctx cancel between retries.
func fetchAndDecode(ctx context.Context, url string, hc *http.Client) (map[string]*rsa.PublicKey, error) {
	var lastErr error
	for attempt := 0; attempt <= len(retryDelays); attempt++ {
		if attempt > 0 {
			delay := retryDelays[attempt-1]
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("jwks fetch cancelled after %d attempt(s): %w", attempt, ctx.Err())
			case <-time.After(delay):
			}
		}

		keys, err := doFetch(ctx, url, hc)
		if err == nil {
			return keys, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("jwks fetch failed after %d attempt(s): %w", len(retryDelays)+1, lastErr)
}

// doFetch performs a single HTTP GET of the JWKS endpoint and decodes the
// response into a kid→*rsa.PublicKey map.
func doFetch(ctx context.Context, url string, hc *http.Client) (map[string]*rsa.PublicKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned HTTP %d", url, resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxJWKSBytes))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	var doc jwksDocument
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("decode JWKS JSON: %w", err)
	}

	keys := make(map[string]*rsa.PublicKey, len(doc.Keys))
	for _, entry := range doc.Keys {
		if entry.Kty != "RSA" {
			// Skip non-RSA keys; pipeline-api only emits RSA.
			continue
		}
		pub, err := decodeRSAPublicKey(entry)
		if err != nil {
			// Non-fatal: skip malformed entries; if the target kid was one of
			// them, the caller will catch the missing-kid error.
			continue
		}
		keys[entry.Kid] = pub
	}
	return keys, nil
}

// decodeRSAPublicKey converts a jwkEntry into an *rsa.PublicKey by decoding
// the base64url-encoded N and E fields (RFC 7518 §6.3).
func decodeRSAPublicKey(entry jwkEntry) (*rsa.PublicKey, error) {
	if entry.N == "" || entry.E == "" {
		return nil, fmt.Errorf("missing N or E for kid %q", entry.Kid)
	}

	nBytes, err := base64.RawURLEncoding.DecodeString(entry.N)
	if err != nil {
		return nil, fmt.Errorf("decode N for kid %q: %w", entry.Kid, err)
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(entry.E)
	if err != nil {
		return nil, fmt.Errorf("decode E for kid %q: %w", entry.Kid, err)
	}

	n := new(big.Int).SetBytes(nBytes)
	e := int(new(big.Int).SetBytes(eBytes).Int64())
	if n.Sign() == 0 || e == 0 {
		return nil, fmt.Errorf("invalid N or E for kid %q", entry.Kid)
	}

	return &rsa.PublicKey{N: n, E: e}, nil
}
