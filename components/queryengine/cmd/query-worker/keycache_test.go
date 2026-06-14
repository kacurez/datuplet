package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"sync/atomic"
	"testing"
)

// countingKeyProvider records how many times KeyFor is called and delegates
// to a simple kid→key map. Used to verify cache hit/miss behaviour.
type countingKeyProvider struct {
	calls atomic.Int64
	keys  map[string]*rsa.PublicKey
}

func (p *countingKeyProvider) KeyFor(_ context.Context, kid string) (*rsa.PublicKey, error) {
	p.calls.Add(1)
	k, ok := p.keys[kid]
	if !ok {
		return nil, errors.New("countingKeyProvider: kid not found: " + kid)
	}
	return k, nil
}

// genCacheTestKey generates a 2048-bit RSA key. Fatal on error.
func genCacheTestKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	return priv
}

// TestCachingKeyProvider_SameKidCalledOnce verifies that two Verify-equivalent
// calls for the same kid result in exactly one call to the underlying provider.
func TestCachingKeyProvider_SameKidCalledOnce(t *testing.T) {
	priv := genCacheTestKey(t)
	const kid = "cache-test-kid"

	underlying := &countingKeyProvider{
		keys: map[string]*rsa.PublicKey{kid: &priv.PublicKey},
	}
	cache := newCachingKeyProvider(underlying)
	ctx := context.Background()

	k1, err := cache.KeyFor(ctx, kid)
	if err != nil {
		t.Fatalf("first KeyFor: %v", err)
	}
	k2, err := cache.KeyFor(ctx, kid)
	if err != nil {
		t.Fatalf("second KeyFor: %v", err)
	}
	if k1 != k2 {
		t.Error("expected the same *rsa.PublicKey pointer on both calls")
	}

	if n := underlying.calls.Load(); n != 1 {
		t.Errorf("underlying.KeyFor called %d times, want exactly 1", n)
	}
}

// TestCachingKeyProvider_UnknownKidErrorPropagatesNotCached verifies that an
// unknown kid returns an error and is not cached (so a subsequent call for the
// same unknown kid still calls the underlying provider).
func TestCachingKeyProvider_UnknownKidErrorPropagatesNotCached(t *testing.T) {
	underlying := &countingKeyProvider{keys: map[string]*rsa.PublicKey{}}
	cache := newCachingKeyProvider(underlying)
	ctx := context.Background()

	_, err1 := cache.KeyFor(ctx, "no-such-kid")
	if err1 == nil {
		t.Fatal("expected error for unknown kid, got nil")
	}

	_, err2 := cache.KeyFor(ctx, "no-such-kid")
	if err2 == nil {
		t.Fatal("expected error for unknown kid on second call, got nil")
	}

	// Both calls must have reached the underlying provider (error not cached).
	if n := underlying.calls.Load(); n != 2 {
		t.Errorf("underlying.KeyFor called %d times for two unknown-kid lookups, want 2", n)
	}
}
