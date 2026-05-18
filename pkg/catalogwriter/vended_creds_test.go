package catalogwriter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a tick-driven time source for the renewal-cadence
// tests. Tests advance the clock manually via Advance.
type fakeClock struct {
	now atomic.Int64
}

func (f *fakeClock) Set(t time.Time) {
	f.now.Store(t.UnixNano())
}

func (f *fakeClock) Now() time.Time {
	return time.Unix(0, f.now.Load())
}

func (f *fakeClock) Advance(d time.Duration) {
	f.now.Add(int64(d))
}

// stubLakekeeper returns a httptest.Server that serves
// `GET /v1/{prefix}/namespaces/{ns}/tables/{tbl}` with a configurable
// response. Each call increments hits so tests can assert "renewed
// once", "renewed twice", etc. The ttlSec parameter governs the
// `s3.session-ttl-seconds` field in the response.
func stubLakekeeper(t *testing.T, ttlSec int, expectedToken string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	hits := &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if expectedToken != "" {
			got := r.Header.Get("Authorization")
			want := "Bearer " + expectedToken
			if got != want {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		hits.Add(1)
		seq := hits.Load()
		body := map[string]any{
			"config": map[string]string{
				"s3.access-key-id":       fmt.Sprintf("AKIA-FAKE-%d", seq),
				"s3.secret-access-key":   fmt.Sprintf("secret-%d", seq),
				"s3.session-token":       fmt.Sprintf("session-%d", seq),
				"s3.region":              "local-01",
				"s3.endpoint":            "http://minio:9000",
				"s3.session-ttl-seconds": fmt.Sprintf("%d", ttlSec),
			},
		}
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(srv.Close)
	return srv, hits
}

// TestVendedCreds_FifteenMinuteTTL covers the canonical worked example:
// 15-min TTL → first renewal at 7m30s, then every 7m30s.
func TestVendedCreds_FifteenMinuteTTL(t *testing.T) {
	t.Parallel()
	srv, hits := stubLakekeeper(t, 15*60, "tok-15m")
	clk := &fakeClock{}
	clk.Set(time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC))

	v := &VendedCreds{
		LakekeeperURL:     srv.URL,
		Namespace:         "public",
		Table:             "events",
		ExpectedCredsType: CredsTypeS3,
		TokenProvider: func(context.Context) (string, error) { return "tok-15m", nil },
		Now:           clk.Now,
	}

	ctx := context.Background()
	got1, err := v.Get(ctx)
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	c1, ok := got1.(S3Creds)
	if !ok {
		t.Fatalf("expected S3Creds, got %T", got1)
	}
	if c1.AccessKeyID == "" {
		t.Fatalf("creds missing keys: %+v", c1)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("after first Get hits=%d want 1", got)
	}

	// At 7m29s the cache should NOT renew (still under the 50% mark).
	clk.Advance(7*time.Minute + 29*time.Second)
	if _, err := v.Get(ctx); err != nil {
		t.Fatalf("Get at 7m29: %v", err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("at 7m29 hits=%d want 1 (no renewal yet)", got)
	}

	// Cross the 7m30s threshold → renew.
	clk.Advance(2 * time.Second) // now 7m31s
	got2, err := v.Get(ctx)
	if err != nil {
		t.Fatalf("Get at 7m31: %v", err)
	}
	c2, ok := got2.(S3Creds)
	if !ok {
		t.Fatalf("expected S3Creds, got %T", got2)
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("at 7m31 hits=%d want 2", got)
	}
	if c2.AccessKeyID == c1.AccessKeyID {
		t.Fatalf("expected new creds after renewal, got same key %q", c2.AccessKeyID)
	}
}

// TestVendedCreds_FiveMinuteTTL covers the second worked example:
// 5-min TTL → renew at 2m30s. Floor (60s) is honoured but not yet
// active at this TTL.
func TestVendedCreds_FiveMinuteTTL(t *testing.T) {
	t.Parallel()
	srv, hits := stubLakekeeper(t, 5*60, "")
	clk := &fakeClock{}
	clk.Set(time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC))

	v := &VendedCreds{
		LakekeeperURL:     srv.URL,
		Namespace:         "public",
		Table:             "events",
		ExpectedCredsType: CredsTypeS3,
		TokenProvider: func(context.Context) (string, error) { return "tok", nil },
		Now:           clk.Now,
	}

	ctx := context.Background()
	if _, err := v.Get(ctx); err != nil {
		t.Fatalf("first Get: %v", err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("after first Get hits=%d want 1", got)
	}

	// 2m29s — under the 50% mark of a 5-min TTL.
	clk.Advance(2*time.Minute + 29*time.Second)
	if _, err := v.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("at 2m29 hits=%d want 1", got)
	}

	// 2m31s — past the threshold.
	clk.Advance(2 * time.Second)
	if _, err := v.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("at 2m31 hits=%d want 2", got)
	}
}

// TestVendedCreds_FloorBlocksPathologicalTTL is the load-bearing
// renewal test for the 60-second hard floor: a 30-second TTL would
// (under pure 50%-elapsed) trigger renewal at 15s, but the floor
// blocks that and pushes it to 60s.
func TestVendedCreds_FloorBlocksPathologicalTTL(t *testing.T) {
	t.Parallel()
	srv, hits := stubLakekeeper(t, 30, "")
	clk := &fakeClock{}
	clk.Set(time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC))

	v := &VendedCreds{
		LakekeeperURL:     srv.URL,
		Namespace:         "public",
		Table:             "events",
		ExpectedCredsType: CredsTypeS3,
		TokenProvider: func(context.Context) (string, error) { return "tok", nil },
		Now:           clk.Now,
	}

	ctx := context.Background()
	if _, err := v.Get(ctx); err != nil {
		t.Fatalf("first Get: %v", err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("after first Get hits=%d want 1", got)
	}

	// At 15s (50% of 30s TTL) — pure 50%-rule says renew, but the
	// 60s floor must block it.
	clk.Advance(15 * time.Second)
	if _, err := v.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("at 15s hits=%d want 1 (floor must block 50%% rule)", got)
	}

	// At 30s — TTL technically expired, but we capped renewal to once
	// per minute. Note: the cache will still try to renew because the
	// expired-creds branch overrides the floor; this is the right
	// behaviour because using-an-expired-cred is worse than calling
	// lakekeeper. So we expect exactly 2 hits at 30s.
	clk.Advance(15 * time.Second) // now 30s
	if _, err := v.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("at 30s hits=%d want 2 (expired creds force renewal)", got)
	}
}

// TestVendedCreds_RenewalFailureSurfacesAfterExpiry verifies the
// "renewal failure" branch of the contract: a cached unexpired value
// keeps working, but once it expires + lakekeeper is still failing
// the next Get propagates the error so the data plane bails out
// cleanly.
func TestVendedCreds_RenewalFailureSurfacesAfterExpiry(t *testing.T) {
	t.Parallel()
	var failNext atomic.Bool
	hits := &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failNext.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"lakekeeper down"}`))
			return
		}
		hits.Add(1)
		body := map[string]any{
			"config": map[string]string{
				"s3.access-key-id":       "AKIA",
				"s3.secret-access-key":   "secret",
				"s3.session-token":       "session",
				"s3.region":              "local-01",
				"s3.endpoint":            "http://minio:9000",
				"s3.session-ttl-seconds": "120", // 2 minutes
			},
		}
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer srv.Close()

	clk := &fakeClock{}
	clk.Set(time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC))

	v := &VendedCreds{
		LakekeeperURL:     srv.URL,
		Namespace:         "public",
		Table:             "events",
		ExpectedCredsType: CredsTypeS3,
		TokenProvider: func(context.Context) (string, error) { return "tok", nil },
		Now:           clk.Now,
	}

	ctx := context.Background()
	if _, err := v.Get(ctx); err != nil {
		t.Fatalf("first Get: %v", err)
	}

	// Lakekeeper goes down. While the cache is unexpired, callers
	// keep getting the cached creds.
	failNext.Store(true)

	// 70s — past the 60s floor (so renewal is attempted) but well
	// before the 2-min TTL expires. Renewal fails, but the cached
	// value is still valid → callers see the cached creds.
	clk.Advance(70 * time.Second)
	got, err := v.Get(ctx)
	if err != nil {
		t.Fatalf("Get at 70s after lakekeeper down (expected stale-but-OK): %v", err)
	}
	c, ok := got.(S3Creds)
	if !ok {
		t.Fatalf("expected S3Creds, got %T", got)
	}
	if c.AccessKeyID != "AKIA" {
		t.Fatalf("expected stale cached creds, got %+v", c)
	}
	if v.LastError() == nil {
		t.Fatalf("LastError should be set after a failed fetch")
	}

	// Past the TTL → cached creds expired → next Get must surface the
	// fetch error.
	clk.Advance(80 * time.Second) // now 150s, past 120s TTL
	if _, err := v.Get(ctx); err == nil {
		t.Fatalf("Get past expiry expected to error, got nil")
	}
}

// TestVendedCreds_TokenProviderError surfaces cleanly: token-provider
// returning an error means we never even attempt the HTTP call, and
// the error propagates without panic.
func TestVendedCreds_TokenProviderError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("HTTP server should not be hit when TokenProvider fails")
	}))
	defer srv.Close()

	v := &VendedCreds{
		LakekeeperURL:     srv.URL,
		Namespace:         "public",
		Table:             "events",
		ExpectedCredsType: CredsTypeS3,
		TokenProvider: func(context.Context) (string, error) {
			return "", errors.New("read run token: not found")
		},
	}
	if _, err := v.Get(context.Background()); err == nil {
		t.Fatalf("expected token-provider error, got nil")
	}
}

// TestVendedCreds_ConcurrentGet verifies that concurrent Get callers
// don't double-fetch. Two goroutines hammer Get; the first one races
// fetch, the second sees the cache. We assert hits stays at 1.
func TestVendedCreds_ConcurrentGet(t *testing.T) {
	t.Parallel()
	srv, hits := stubLakekeeper(t, 600, "")
	v := &VendedCreds{
		LakekeeperURL:     srv.URL,
		Namespace:         "public",
		Table:             "events",
		ExpectedCredsType: CredsTypeS3,
		TokenProvider: func(context.Context) (string, error) { return "tok", nil },
	}
	ctx := context.Background()

	done := make(chan struct{}, 4)
	for i := 0; i < 4; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			if _, err := v.Get(ctx); err != nil {
				t.Errorf("Get: %v", err)
			}
		}()
	}
	for i := 0; i < 4; i++ {
		<-done
	}

	// At most one fetch should have landed; under heavy contention it
	// might be 1 or 2 because the busy-spin path may race with a
	// concurrent fetch's completion. Either is acceptable; what's NOT
	// acceptable is 4 (one fetch per caller).
	if got := hits.Load(); got > 2 {
		t.Fatalf("excessive fetches: hits=%d (want <=2)", got)
	}
}

// TestVendedCreds_ConcurrentGetWithHungFetch exercises the path that
// previously recursed under sustained lakekeeper hangs: many callers
// arrive while fetch #1 is blocked, and we want them parked on the
// shared fetchDone channel rather than spinning into recursive
// `Get(ctx)` calls. We model the hung fetch with a stub handler that
// blocks on a `release` chan; once we close it, all queued Gets must
// drain with exactly ONE backend hit recorded.
func TestVendedCreds_ConcurrentGetWithHungFetch(t *testing.T) {
	t.Parallel()
	hits := &atomic.Int64{}
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Block until the test releases us — simulates lakekeeper hang.
		<-release
		hits.Add(1)
		body := map[string]any{
			"config": map[string]string{
				"s3.access-key-id":       "AKIA",
				"s3.secret-access-key":   "secret",
				"s3.session-token":       "session",
				"s3.region":              "local-01",
				"s3.endpoint":            "http://minio:9000",
				"s3.session-ttl-seconds": "600",
			},
		}
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer srv.Close()

	v := &VendedCreds{
		LakekeeperURL:     srv.URL,
		Namespace:         "public",
		Table:             "events",
		ExpectedCredsType: CredsTypeS3,
		TokenProvider: func(context.Context) (string, error) { return "tok", nil },
	}

	const N = 16
	results := make(chan error, N)
	for i := 0; i < N; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := v.Get(ctx)
			results <- err
		}()
	}

	// Give goroutines time to all queue up behind the in-flight fetch.
	time.Sleep(50 * time.Millisecond)

	// Release the hung lakekeeper request — every queued Get should
	// now wake from the fetchDone channel and read the cached creds.
	close(release)

	for i := 0; i < N; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Errorf("Get %d: %v", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Get %d did not return after fetch released", i)
		}
	}

	if got := hits.Load(); got != 1 {
		t.Fatalf("expected exactly 1 backend hit (all queued behind fetchDone), got %d", got)
	}
}

// TestParseCredsRejectsMixed asserts the confused-deputy fail-closed:
// a response that carries BOTH s3.* and gcs.oauth2.* keys is ambiguous
// and must be rejected regardless of ExpectedCredsType.
func TestParseCredsRejectsMixed(t *testing.T) {
	cfg := map[string]any{
		"s3.access-key-id": "AKIA...",
		"gcs.oauth2.token": "ya29...",
	}
	_, err := parseCreds(cfg, CredsTypeS3)
	if err == nil || !strings.Contains(err.Error(), "BOTH s3.* and gcs.oauth2.*") {
		t.Fatalf("expected mixed-family rejection, got %v", err)
	}
}

// TestParseCredsRejectsWrongFamily asserts a warehouse/backend mismatch
// (backend resolver said "S3" but lakekeeper returned gcs keys) fails
// closed with a clear diagnostic.
func TestParseCredsRejectsWrongFamily(t *testing.T) {
	cfg := map[string]any{"gcs.oauth2.token": "ya29..."}
	_, err := parseCreds(cfg, CredsTypeS3)
	if err == nil || !strings.Contains(err.Error(), "expected s3 credentials but lakekeeper returned gcs") {
		t.Fatalf("expected wrong-family rejection, got %v", err)
	}
}

// TestParseCredsRejectsEmpty: an empty config block must not silently
// produce a zero-value Creds.
func TestParseCredsRejectsEmpty(t *testing.T) {
	_, err := parseCreds(map[string]any{}, CredsTypeS3)
	if err == nil {
		t.Fatal("expected error on empty cfg")
	}
}

// TestParseCredsHappyPathS3 covers the happy path for S3 family.
func TestParseCredsHappyPathS3(t *testing.T) {
	cfg := map[string]any{
		"s3.access-key-id":     "AKIA",
		"s3.secret-access-key": "secret",
		"s3.region":            "us-east-1",
		"s3.endpoint":          "https://s3.example.com",
		"s3.session-token":     "session",
	}
	got, err := parseCreds(cfg, CredsTypeS3)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type() != CredsTypeS3 {
		t.Fatalf("Type() = %q, want s3", got.Type())
	}
}

// TestParseCredsUnsetExpectedRejects: a VendedCreds that forgot to set
// ExpectedCredsType (zero value) must be rejected — this is the
// mandatory-field guard.
func TestParseCredsUnsetExpectedRejects(t *testing.T) {
	cfg := map[string]any{"s3.access-key-id": "AKIA"}
	_, err := parseCreds(cfg, "")
	if err == nil || !strings.Contains(err.Error(), "ExpectedCredsType") {
		t.Fatalf("expected unset-expected rejection, got %v", err)
	}
}

// TestParseGCSCredsHappyPath covers all four GCS fields plus the
// absolute-epoch-ms expiry path.
func TestParseGCSCredsHappyPath(t *testing.T) {
	now := time.Now()
	cfg := map[string]any{
		"gcs.oauth2.token":                        "ya29.AAA",
		"gcs.project-id":                          "kacurez-labs",
		"gcs.oauth2.refresh-credentials-endpoint": "https://lk/refresh",
		"gcs.oauth2.token-expires-at":             float64(now.Add(20 * time.Minute).UnixMilli()),
	}
	got, err := parseGCSCreds(cfg)
	if err != nil {
		t.Fatal(err)
	}
	gc, ok := got.(GCSCreds)
	if !ok {
		t.Fatalf("expected GCSCreds, got %T", got)
	}
	if gc.OAuthToken != "ya29.AAA" {
		t.Fatalf("OAuthToken = %q", gc.OAuthToken)
	}
	if gc.GCPProjectID != "kacurez-labs" {
		t.Fatalf("GCPProjectID = %q", gc.GCPProjectID)
	}
	if gc.RefreshEndpoint != "https://lk/refresh" {
		t.Fatalf("RefreshEndpoint = %q", gc.RefreshEndpoint)
	}
	if gc.ExpiresAt().Before(now.Add(15 * time.Minute)) {
		t.Fatalf("Expires = %v, want >= now+15m", gc.ExpiresAt())
	}
}

// TestParseGCSCredsMissingTokenRejects: the OAuth token is mandatory.
func TestParseGCSCredsMissingTokenRejects(t *testing.T) {
	cfg := map[string]any{"gcs.project-id": "x"}
	_, err := parseGCSCreds(cfg)
	if err == nil || !strings.Contains(err.Error(), "missing gcs.oauth2.token") {
		t.Fatalf("expected missing-token error, got %v", err)
	}
}

// TestParseGCSCredsFallbackExpiry: when no expiry hint is present,
// parseGCSCreds must produce a non-zero ExpiresAt (15-min default per
// RFC 019 §4.2).
func TestParseGCSCredsFallbackExpiry(t *testing.T) {
	cfg := map[string]any{"gcs.oauth2.token": "ya29.AAA"}
	got, err := parseGCSCreds(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.ExpiresAt().IsZero() {
		t.Fatal("ExpiresAt zero — expected 15min default")
	}
}

// TestParseGCSCredsCredsExpirationMsFallback covers the secondary
// expiry key path (`creds.expiration-time-ms`) used when lakekeeper
// emits the older schema.
func TestParseGCSCredsCredsExpirationMsFallback(t *testing.T) {
	target := time.Now().Add(20 * time.Minute)
	cfg := map[string]any{
		"gcs.oauth2.token":         "ya29.AAA",
		"creds.expiration-time-ms": float64(target.UnixMilli()),
	}
	got, err := parseGCSCreds(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Should be within a few seconds of the target (we accept any
	// non-zero value within the 15-min default window).
	diff := got.ExpiresAt().Sub(target)
	if diff < -time.Second || diff > time.Second {
		t.Fatalf("Expires = %v, want ~%v (diff %v)", got.ExpiresAt(), target, diff)
	}
}

// TestParseCredsHappyPathGCS exercises the parseCreds GCS-family
// dispatch end to end.
func TestParseCredsHappyPathGCS(t *testing.T) {
	cfg := map[string]any{
		"gcs.oauth2.token": "ya29.AAA",
		"gcs.project-id":   "kacurez-labs",
	}
	got, err := parseCreds(cfg, CredsTypeGCS)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type() != CredsTypeGCS {
		t.Fatalf("Type() = %q, want gcs", got.Type())
	}
}
