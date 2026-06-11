package main

// keycache.go provides a read-through cache layer over a KeyProvider.
//
// jwks.Client.KeyFor holds its mutex across the HTTP re-fetch, so calling it
// on every incoming request would serialize all concurrent queries behind any
// in-flight JWKS network round-trip — effectively a DOS at modest load.
//
// cachingKeyProvider wraps any KeyProvider and memoizes successful lookups in
// a local map. The hit path uses an RWMutex read-lock and never touches the
// network. On a kid-miss a plain write-lock is taken and next.KeyFor is called
// once (the underlying jwks.Client already handles its own kid-miss re-fetch
// internally). Only successful lookups are cached: unknown kids are not stored,
// so each novel kid gets exactly one downstream call.
//
// No TTL/eviction: public key rotation adds new kids to the JWKS; old kids
// remain valid until the JWKS endpoint removes them. Because jwks.Client
// re-fetches on kid-miss, the cache here only memoizes successes — it never
// blocks a legitimate new key from being resolved. Key rotation beyond what
// jwks.Client's own re-fetch covers (e.g. a key removed from the JWKS) is
// picked up by a worker restart.

import (
	"context"
	"crypto/rsa"
	"sync"
)

// cachingKeyProvider is a read-through cache in front of a KeyProvider.
// Safe for concurrent use.
type cachingKeyProvider struct {
	mu   sync.RWMutex
	keys map[string]*rsa.PublicKey
	next KeyProvider
}

// newCachingKeyProvider wraps next in a caching layer.
func newCachingKeyProvider(next KeyProvider) *cachingKeyProvider {
	return &cachingKeyProvider{
		keys: make(map[string]*rsa.PublicKey),
		next: next,
	}
}

// KeyFor returns the RSA public key for kid. Cached hits take an RLock and
// return without any network call. On a miss, a write-lock is taken and
// next.KeyFor is called once; the result is cached only on success.
func (c *cachingKeyProvider) KeyFor(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	// Fast path: read-lock cache lookup.
	c.mu.RLock()
	if k, ok := c.keys[kid]; ok {
		c.mu.RUnlock()
		return k, nil
	}
	c.mu.RUnlock()

	// Slow path: write-lock, double-check, then delegate to underlying provider.
	c.mu.Lock()
	defer c.mu.Unlock()
	// Double-check: another goroutine may have populated the cache while we
	// waited for the write lock.
	if k, ok := c.keys[kid]; ok {
		return k, nil
	}
	k, err := c.next.KeyFor(ctx, kid)
	if err != nil {
		// Do not cache failures: an unknown kid may appear in the JWKS on the
		// next rotation, and jwks.Client already retries on kid-miss.
		return nil, err
	}
	c.keys[kid] = k
	return k, nil
}
