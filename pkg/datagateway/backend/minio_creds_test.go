package backend

import (
	"testing"
	"time"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

// vendedCredsProvider is exercised against the same fakeFetcher used by
// the GCS-side tests (defined in gcs_test.go).

// TestVendedCredsProviderCapsExpiry verifies that when lakekeeper hands
// out S3 STS creds with a nominal 1-hour expiry, IsExpired() flips back
// to true after maxS3CredsCacheLifetime — forcing minio-go to call
// Retrieve() again, which in turn lets VendedCreds.Get() re-evaluate
// internal renewal. Without this cap, minio-go would reuse the first
// SessionToken indefinitely while STS rotated it server-side.
func TestVendedCredsProviderCapsExpiry(t *testing.T) {
	hourFromNow := time.Now().Add(1 * time.Hour)
	f := &fakeFetcher{c: catalogwriter.S3Creds{
		AccessKeyID:     "AKIA-test",
		SecretAccessKey: "secret",
		SessionToken:    "session",
		Issued:          time.Now(),
		Expires:         hourFromNow,
	}}
	p := &vendedCredsProvider{vc: f}

	before := time.Now()
	v, err := p.Retrieve()
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	after := time.Now()
	if v.AccessKeyID != "AKIA-test" {
		t.Fatalf("AccessKeyID = %q, want AKIA-test", v.AccessKeyID)
	}
	if v.SessionToken != "session" {
		t.Fatalf("SessionToken not propagated: %q", v.SessionToken)
	}

	// cachedExp must land in [before+cap, after+cap] — i.e. capped.
	lo := before.Add(maxS3CredsCacheLifetime)
	hi := after.Add(maxS3CredsCacheLifetime)
	p.mu.Lock()
	got := p.cachedExp
	p.mu.Unlock()
	if got.Before(lo) || got.After(hi) {
		t.Fatalf("cachedExp = %v, want in [%v, %v] (capped)", got, lo, hi)
	}
	if !got.Before(hourFromNow) {
		t.Fatalf("cachedExp = %v should be < hourFromNow %v (cap not applied)", got, hourFromNow)
	}
}

// TestVendedCredsProviderShortExpiryNotInflated verifies that when
// lakekeeper hands out short-lived creds (Expires < cap), we honor that
// shorter expiry rather than inflating it. Mirror of the GCS test.
func TestVendedCredsProviderShortExpiryNotInflated(t *testing.T) {
	shortExpiry := time.Now().Add(10 * time.Second) // < 60s cap
	f := &fakeFetcher{c: catalogwriter.S3Creds{
		AccessKeyID:     "AKIA-short",
		SecretAccessKey: "secret",
		SessionToken:    "session-short",
		Issued:          time.Now(),
		Expires:         shortExpiry,
	}}
	p := &vendedCredsProvider{vc: f}

	if _, err := p.Retrieve(); err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	p.mu.Lock()
	got := p.cachedExp
	p.mu.Unlock()
	if !got.Equal(shortExpiry) {
		t.Fatalf("cachedExp = %v, want shortExpiry %v (no inflation)", got, shortExpiry)
	}
}

// TestVendedCredsProviderZeroExpiryUsesCap defends against lakekeeper
// returning an unset Expires: must still set a finite cachedExp so we
// don't treat the creds as eternal. Mirror of the GCS test.
func TestVendedCredsProviderZeroExpiryUsesCap(t *testing.T) {
	f := &fakeFetcher{c: catalogwriter.S3Creds{
		AccessKeyID:     "AKIA-noexp",
		SecretAccessKey: "secret",
		SessionToken:    "session-noexp",
		Issued:          time.Now(),
		// Expires deliberately zero
	}}
	p := &vendedCredsProvider{vc: f}

	before := time.Now()
	if _, err := p.Retrieve(); err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	after := time.Now()
	p.mu.Lock()
	got := p.cachedExp
	p.mu.Unlock()
	if got.IsZero() {
		t.Fatal("cachedExp must not be zero — minio-go would treat creds as eternal")
	}
	lo := before.Add(maxS3CredsCacheLifetime)
	hi := after.Add(maxS3CredsCacheLifetime)
	if got.Before(lo) || got.After(hi) {
		t.Fatalf("cachedExp = %v, want in [%v, %v] (cap)", got, lo, hi)
	}
}

// TestVendedCredsProviderIsExpiredLifecycle verifies the IsExpired
// contract:
//
//   - Before any Retrieve: IsExpired() == true (forces minio-go to call us)
//   - Just after Retrieve: IsExpired() == false (within the cap window)
//   - After cap elapses (simulated by mutating cachedExp): true again
//
// This is the load-bearing fix for the v0.2.4 streaming-upload
// regression: previously IsExpired hardcoded false, so step 3 never
// fired and minio-go reused the stale SessionToken indefinitely.
func TestVendedCredsProviderIsExpiredLifecycle(t *testing.T) {
	f := &fakeFetcher{c: catalogwriter.S3Creds{
		AccessKeyID:     "AKIA",
		SecretAccessKey: "secret",
		SessionToken:    "session",
		Issued:          time.Now(),
		Expires:         time.Now().Add(time.Hour),
	}}
	p := &vendedCredsProvider{vc: f}

	if !p.IsExpired() {
		t.Fatal("IsExpired before first Retrieve should be true (forces fetch)")
	}
	if _, err := p.Retrieve(); err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if p.IsExpired() {
		t.Fatal("IsExpired just after Retrieve should be false (creds fresh)")
	}

	// Simulate the cap window elapsing by rewinding cachedExp into the
	// past. Within s3CredsExpiryGrace of the cap → IsExpired() flips true.
	p.mu.Lock()
	p.cachedExp = time.Now().Add(-time.Second)
	p.mu.Unlock()
	if !p.IsExpired() {
		t.Fatal("IsExpired after cap elapses should be true (forces re-fetch)")
	}
}

// TestVendedCredsProviderRejectsGCS verifies the runtime type guard:
// even if lakekeeper somehow returns GCSCreds for the S3 backend, we
// must fail closed rather than send wrong-family credentials to AWS.
func TestVendedCredsProviderRejectsGCS(t *testing.T) {
	f := &fakeFetcher{c: catalogwriter.GCSCreds{
		OAuthToken: "ya29.test",
		Issued:     time.Now(),
		Expires:    time.Now().Add(time.Hour),
	}}
	p := &vendedCredsProvider{vc: f}

	_, err := p.Retrieve()
	if err == nil {
		t.Fatal("expected wrong-family rejection, got nil")
	}
}
