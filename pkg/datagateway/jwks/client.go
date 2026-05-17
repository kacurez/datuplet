// Package jwks fetches and caches RSA public keys from pipeline-api's JWKS
// endpoint (/api/v1/auth/jwks.json).
//
// Cache is keyed by kid. On kid-miss, the client re-fetches JWKS exactly once
// (handles pipeline-api key rotation between fetches) before failing.
//
// Fail-closed: any fetch error after the bounded retry surfaces to the caller,
// which MUST treat it as a startup-fatal condition.
package jwks

import (
	"context"
	"crypto/rsa"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
)

// Client fetches and caches RSA public keys from a JWKS endpoint.
// Safe for concurrent use.
type Client struct {
	// URL is the JWKS endpoint to fetch (e.g.
	// http://pipeline-api.datuplet.svc.cluster.local:8081/api/v1/auth/jwks.json).
	URL string

	// HTTPClient is the HTTP client used for fetching. If nil, http.DefaultClient
	// is used.
	HTTPClient *http.Client

	mu   sync.Mutex
	keys map[string]*rsa.PublicKey // kid -> key; nil = not yet fetched
}

// NewClient constructs a Client for the given JWKS endpoint URL. httpClient may
// be nil; http.DefaultClient is used in that case.
//
// HTTPS posture: the JWKS URL is trusted (it's used to fetch the keys that
// gate ALL run-token verification). In-cluster, Service DNS resolves to plain
// HTTP — acceptable for in-cluster communication. If the URL is anything other
// than https:// or in-cluster .svc/.svc.cluster.local, we emit a startup
// warning so the operator notices the insecure path. A strict mode is deferred
// to a future hardening pass; for now the warning is the deterrent.
func NewClient(rawURL string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	if u, err := url.Parse(rawURL); err == nil && u.Scheme == "http" {
		host := u.Hostname()
		// Allow in-cluster Service DNS without warning (in-cluster traffic over
		// HTTP is the documented POC posture). Anything else over plain HTTP is
		// flagged — a JWKS MITM owns every downstream JWT verification.
		if host == "localhost" ||
			host == "127.0.0.1" ||
			endsWith(host, ".svc") ||
			endsWith(host, ".svc.cluster.local") {
			// in-cluster / loopback: silent.
		} else {
			log.Printf("jwks: WARNING: JWKS endpoint %q uses plain HTTP on a non-cluster host — JWT verification is only as trustworthy as the network path to %s", rawURL, host)
		}
	}
	return &Client{
		URL:        rawURL,
		HTTPClient: httpClient,
	}
}

// endsWith is a tiny helper to avoid importing strings just for this check.
func endsWith(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

// KeyFor returns the RSA public key for the given kid.
//
// On the first call (or after any prior call resulted in no key cache), the
// client fetches the JWKS endpoint. If the requested kid is absent after the
// initial fetch, the client re-fetches exactly once (handles key rotation
// between pod start and token mint). If the kid is still absent after the
// re-fetch, KeyFor returns an error.
//
// Subsequent calls for cached kids do not make additional HTTP requests.
//
// Concurrency note: the mutex is held across the HTTP fetch, which serializes
// all concurrent callers behind any in-flight kid-miss re-fetch (up to ~7s
// with backoff). This is acceptable because KeyFor is called ONLY at startup
// (DG sidecar + TableCommit job, once each, fail-closed on error). If a
// future caller wires KeyFor into a per-request path, this becomes a DOS
// vector — split the lock (RWMutex with double-checked locking) before doing so.
func (c *Client) KeyFor(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// First attempt: if we already have a key set in cache, check it.
	if c.keys != nil {
		if k, ok := c.keys[kid]; ok {
			return k, nil
		}
		// kid absent in the cached key set — re-fetch once to handle key rotation.
		fresh, err := fetchAndDecode(ctx, c.URL, c.HTTPClient)
		if err != nil {
			return nil, fmt.Errorf("jwks re-fetch (kid=%q not in cache): %w", kid, err)
		}
		c.keys = fresh
		if k, ok := c.keys[kid]; ok {
			return k, nil
		}
		return nil, fmt.Errorf("jwks: kid %q not found after re-fetch", kid)
	}

	// First call: populate cache.
	fresh, err := fetchAndDecode(ctx, c.URL, c.HTTPClient)
	if err != nil {
		return nil, fmt.Errorf("jwks initial fetch: %w", err)
	}
	c.keys = fresh
	if k, ok := c.keys[kid]; ok {
		return k, nil
	}
	// kid absent on first fetch — re-fetch once before failing.
	fresh2, err := fetchAndDecode(ctx, c.URL, c.HTTPClient)
	if err != nil {
		return nil, fmt.Errorf("jwks re-fetch (kid=%q not in initial fetch): %w", kid, err)
	}
	c.keys = fresh2
	if k, ok := c.keys[kid]; ok {
		return k, nil
	}
	return nil, fmt.Errorf("jwks: kid %q not found after re-fetch", kid)
}
