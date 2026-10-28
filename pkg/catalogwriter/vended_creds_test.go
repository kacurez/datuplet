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

	"github.com/prometheus/client_golang/prometheus/testutil"
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

// TestVendedCreds_RenewalAtHalfTTL covers the worked examples of the 50%-elapsed
// renewal rule: 15-min TTL → first renewal at 7m30s; 5-min TTL → renew at
// 2m30s (the 60s floor is honoured but not yet active at these TTLs).
func TestVendedCreds_RenewalAtHalfTTL(t *testing.T) {
	t.Parallel()

	type step struct {
		advance  time.Duration
		wantHits int64
	}
	tests := []struct {
		name          string
		ttlSec        int
		stubToken     string // token the stub lakekeeper insists on ("" = any)
		providerToken string
		steps         []step
	}{
		{
			name: "15 minute TTL renews at 7m30s", ttlSec: 15 * 60,
			stubToken: "tok-15m", providerToken: "tok-15m",
			steps: []step{
				{7*time.Minute + 29*time.Second, 1}, // under the 50% mark: no renewal yet
				{2 * time.Second, 2},                // 7m31s: past the threshold
			},
		},
		{
			name: "5 minute TTL renews at 2m30s", ttlSec: 5 * 60,
			providerToken: "tok",
			steps: []step{
				{2*time.Minute + 29*time.Second, 1}, // under the 50% mark of a 5-min TTL
				{2 * time.Second, 2},                // 2m31s: past the threshold
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, hits := stubLakekeeper(t, tc.ttlSec, tc.stubToken)
			clk := &fakeClock{}
			clk.Set(time.Date(2026, 4, 26, 12, 0, 0, 0, time.UTC))

			v := &VendedCreds{
				LakekeeperURL:     srv.URL,
				Namespace:         "public",
				Table:             "events",
				ExpectedCredsType: CredsTypeS3,
				TokenProvider:     func(context.Context) (string, error) { return tc.providerToken, nil },
				Now:               clk.Now,
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
				t.Fatalf("creds missing keys: access-key-id=%q region=%q", c1.AccessKeyID, c1.Region)
			}
			if got := hits.Load(); got != 1 {
				t.Fatalf("after first Get hits=%d want 1", got)
			}

			var last Creds
			for _, s := range tc.steps {
				clk.Advance(s.advance)
				if last, err = v.Get(ctx); err != nil {
					t.Fatalf("Get after +%v: %v", s.advance, err)
				}
				if got := hits.Load(); got != s.wantHits {
					t.Fatalf("after +%v hits=%d want %d", s.advance, got, s.wantHits)
				}
			}
			c2, ok := last.(S3Creds)
			if !ok {
				t.Fatalf("expected S3Creds, got %T", last)
			}
			if c2.AccessKeyID == c1.AccessKeyID {
				t.Fatalf("expected new creds after renewal, got same key %q", c2.AccessKeyID)
			}
		})
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
		TokenProvider:     func(context.Context) (string, error) { return "tok", nil },
		Now:               clk.Now,
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
		TokenProvider:     func(context.Context) (string, error) { return "tok", nil },
		Now:               clk.Now,
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
		t.Fatalf("expected stale cached creds: access-key-id=%q (want %q)", c.AccessKeyID, "AKIA")
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
		TokenProvider:     func(context.Context) (string, error) { return "tok", nil },
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
		TokenProvider:     func(context.Context) (string, error) { return "tok", nil },
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
	for _, expected := range []CredsType{CredsTypeS3, CredsTypeGCS} {
		t.Run("expected "+string(expected), func(t *testing.T) {
			_, err := parseCreds(cfg, expected)
			if err == nil || !strings.Contains(err.Error(), "BOTH s3.* and gcs.oauth2.*") {
				t.Fatalf("expected mixed-family rejection, got %v", err)
			}
		})
	}
}

// TestParseCredsRejects covers the other fail-closed cases: a warehouse/backend
// family mismatch (backend resolver said one family but lakekeeper returned the
// other), an empty config block (must not silently produce a zero-value Creds),
// and a VendedCreds that forgot to set ExpectedCredsType (the mandatory-field
// guard). The error wording is not the contract; failing closed is.
func TestParseCredsRejects(t *testing.T) {
	tests := []struct {
		name     string
		cfg      map[string]any
		expected CredsType
	}{
		{"s3 expected, gcs returned", map[string]any{"gcs.oauth2.token": "ya29..."}, CredsTypeS3},
		{"gcs expected, s3 returned", map[string]any{"s3.access-key-id": "AKIA", "s3.secret-access-key": "secret"}, CredsTypeGCS},
		{"empty config", map[string]any{}, CredsTypeS3},
		{"ExpectedCredsType unset", map[string]any{"s3.access-key-id": "AKIA"}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseCreds(tc.cfg, tc.expected); err == nil {
				t.Fatal("expected parseCreds to reject, got nil error")
			}
		})
	}
}

// TestParseCreds_Accepts covers the happy paths and expiry handling of the
// S3 and GCS parsers.
func TestParseCreds_Accepts(t *testing.T) {
	now := time.Now()
	target := now.Add(20 * time.Minute)
	tests := []struct {
		name       string
		parse      func() (Creds, error)
		wantErrSub string // non-empty: parse must fail with this text
		check      func(t *testing.T, got Creds)
	}{
		{
			name: "S3 via parseCreds",
			parse: func() (Creds, error) {
				return parseCreds(map[string]any{
					"s3.access-key-id":     "AKIA",
					"s3.secret-access-key": "secret",
					"s3.region":            "us-east-1",
					"s3.endpoint":          "https://s3.example.com",
					"s3.session-token":     "session",
				}, CredsTypeS3)
			},
			check: func(t *testing.T, got Creds) {
				if got.Type() != CredsTypeS3 {
					t.Fatalf("Type() = %q, want s3", got.Type())
				}
			},
		},
		{
			name: "GCS via parseCreds",
			parse: func() (Creds, error) {
				return parseCreds(map[string]any{
					"gcs.oauth2.token": "ya29.AAA",
					"gcs.project-id":   "kacurez-labs",
				}, CredsTypeGCS)
			},
			check: func(t *testing.T, got Creds) {
				if got.Type() != CredsTypeGCS {
					t.Fatalf("Type() = %q, want gcs", got.Type())
				}
			},
		},
		{
			// All four GCS fields plus the absolute-epoch-ms expiry path.
			name: "GCS all fields and absolute expiry",
			parse: func() (Creds, error) {
				return parseGCSCreds(map[string]any{
					"gcs.oauth2.token":                        "ya29.AAA",
					"gcs.project-id":                          "kacurez-labs",
					"gcs.oauth2.refresh-credentials-endpoint": "https://lk/refresh",
					"gcs.oauth2.token-expires-at":             float64(now.Add(20 * time.Minute).UnixMilli()),
				})
			},
			check: func(t *testing.T, got Creds) {
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
			},
		},
		{
			name: "GCS missing token is rejected (token is mandatory)",
			parse: func() (Creds, error) {
				return parseGCSCreds(map[string]any{"gcs.project-id": "x"})
			},
			wantErrSub: "missing gcs.oauth2.token",
		},
		{
			// parseGCSCreds intentionally leaves Expires zero when no expiry key
			// is present — VendedCreds.fetch applies the 15-min default via
			// v.now(), same pattern as parseS3Creds.
			name: "GCS without expiry hint leaves Expires zero",
			parse: func() (Creds, error) {
				return parseGCSCreds(map[string]any{"gcs.oauth2.token": "ya29.AAA"})
			},
			check: func(t *testing.T, got Creds) {
				if !got.ExpiresAt().IsZero() {
					t.Fatalf("ExpiresAt = %v, want zero (fetch fills it in)", got.ExpiresAt())
				}
			},
		},
		{
			// Secondary expiry key (`creds.expiration-time-ms`) used when
			// lakekeeper emits the older schema.
			name: "GCS creds.expiration-time-ms fallback",
			parse: func() (Creds, error) {
				return parseGCSCreds(map[string]any{
					"gcs.oauth2.token":         "ya29.AAA",
					"creds.expiration-time-ms": float64(target.UnixMilli()),
				})
			},
			check: func(t *testing.T, got Creds) {
				diff := got.ExpiresAt().Sub(target)
				if diff < -time.Second || diff > time.Second {
					t.Fatalf("Expires = %v, want ~%v (diff %v)", got.ExpiresAt(), target, diff)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.parse()
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErrSub, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			tc.check(t, got)
		})
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

// TestScrubBody verifies that bearer material is redacted from error-body
// interpolation (GCP OAuth2 access tokens, GCS and AWS signed-URL parameters,
// bare JWTs) and that benign bodies are not mangled.
func TestScrubBody(t *testing.T) {
	tests := []struct {
		name           string
		in             string
		mustNotContain []string
		mustContain    []string
		wantUnchanged  bool
	}{
		{
			name:           "redacts ya29 access tokens",
			in:             `{"error":"401","reason":"token ya29.AAA-BBB_CCC expired"}`,
			mustNotContain: []string{"ya29.AAA"},
		},
		{
			name:           "redacts GCS signed URL params",
			in:             `https://storage.googleapis.com/b/k?X-Goog-Signature=DEAD%2BBEEF&X-Goog-Date=20260518T120000Z`,
			mustNotContain: []string{"DEAD"},
		},
		{
			name:          "keeps benign content",
			in:            `{"ok":true,"region":"us-east-1"}`,
			wantUnchanged: true,
		},
		{
			// A JWT without a `Bearer ` prefix (e.g. in a lakekeeper error-body
			// JSON field) is replaced by `<redacted-jwt>`.
			name:           "redacts bare JWT",
			in:             `{"error":"invalid token: eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1c2VyIn0.signature_here"}`,
			mustNotContain: []string{"eyJhbGci"},
			mustContain:    []string{"<redacted-jwt>"},
		},
		{
			name:           "redacts AWS signed URL params",
			in:             `X-Amz-Security-Token=IQoJb3JpZ2luX2Vj&X-Amz-Signature=ABCDEF`,
			mustNotContain: []string{"IQoJb3J", "ABCDEF"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := scrubBody([]byte(tc.in))
			if tc.wantUnchanged && out != tc.in {
				t.Fatalf("scrubBody mangled benign content: %s", out)
			}
			for _, bad := range tc.mustNotContain {
				if strings.Contains(out, bad) {
					t.Fatalf("%q leaked: %s", bad, out)
				}
			}
			for _, want := range tc.mustContain {
				if !strings.Contains(out, want) {
					t.Fatalf("expected %q in output, got: %s", want, out)
				}
			}
		})
	}
}

// resetMetrics resets both creds-refresh counter-vecs between subtests so
// that each subtest starts from a clean slate. It calls Reset() on both vecs,
// which zeroes all label-series that have been observed so far. This is safe
// because the counters are package-level vars in the same package.
func resetMetrics() {
	credsRefreshTotal.Reset()
	credsRefreshFailuresTotal.Reset()
}

// TestCredsRefreshMetrics_SuccessCounter asserts that a successful fetch
// increments datuplet_creds_refresh_total{type=<family>} exactly once and
// leaves the failure counter untouched.
func TestCredsRefreshMetrics_SuccessCounter(t *testing.T) {
	tests := []struct {
		name     string
		expected CredsType
		config   map[string]string
	}{
		{
			name:     "s3",
			expected: CredsTypeS3,
			config: map[string]string{
				"s3.access-key-id":       "AKIA",
				"s3.secret-access-key":   "secret",
				"s3.session-token":       "session",
				"s3.region":              "local-01",
				"s3.endpoint":            "http://minio:9000",
				"s3.session-ttl-seconds": "900",
			},
		},
		{
			name:     "gcs",
			expected: CredsTypeGCS,
			config: map[string]string{
				"gcs.oauth2.token": "ya29.AAA",
				"gcs.project-id":   "test-project",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetMetrics()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"config": tc.config})
			}))
			defer srv.Close()

			v := &VendedCreds{
				LakekeeperURL:     srv.URL,
				Namespace:         "public",
				Table:             "events",
				ExpectedCredsType: tc.expected,
				TokenProvider:     func(context.Context) (string, error) { return "tok", nil },
			}
			if _, err := v.Get(context.Background()); err != nil {
				t.Fatalf("Get: %v", err)
			}

			got := testutil.ToFloat64(credsRefreshTotal.WithLabelValues(tc.name))
			if got != 1 {
				t.Errorf("datuplet_creds_refresh_total{type=%s} = %v, want 1", tc.name, got)
			}
			// Failure counter must not have been touched.
			if n := testutil.CollectAndCount(credsRefreshFailuresTotal); n != 0 {
				t.Errorf("datuplet_creds_refresh_failures_total unexpected series count = %d, want 0", n)
			}
		})
	}
}

// TestCredsRefreshMetrics_HttpFailure asserts that an HTTP 500 from lakekeeper
// increments datuplet_creds_refresh_failures_total{type="s3",reason="http"}.
func TestCredsRefreshMetrics_HttpFailure(t *testing.T) {
	resetMetrics()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"down"}`))
	}))
	defer srv.Close()

	v := &VendedCreds{
		LakekeeperURL:     srv.URL,
		Namespace:         "public",
		Table:             "events",
		ExpectedCredsType: CredsTypeS3,
		TokenProvider:     func(context.Context) (string, error) { return "tok", nil },
	}
	if _, err := v.Get(context.Background()); err == nil {
		t.Fatal("expected error from 500 response, got nil")
	}

	got := testutil.ToFloat64(credsRefreshFailuresTotal.WithLabelValues("s3", "http"))
	if got != 1 {
		t.Errorf("datuplet_creds_refresh_failures_total{type=s3,reason=http} = %v, want 1", got)
	}
	// Success counter must not have been touched.
	if n := testutil.CollectAndCount(credsRefreshTotal); n != 0 {
		t.Errorf("datuplet_creds_refresh_total unexpected series count = %d, want 0", n)
	}
}

// TestCredsRefreshMetrics_TokenProviderFailure asserts that a TokenProvider
// error increments datuplet_creds_refresh_failures_total{type="s3",reason="token_provider"}.
func TestCredsRefreshMetrics_TokenProviderFailure(t *testing.T) {
	resetMetrics()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("HTTP server should not be called when TokenProvider fails")
	}))
	defer srv.Close()

	v := &VendedCreds{
		LakekeeperURL:     srv.URL,
		Namespace:         "public",
		Table:             "events",
		ExpectedCredsType: CredsTypeS3,
		TokenProvider: func(context.Context) (string, error) {
			return "", errors.New("token not found")
		},
	}
	if _, err := v.Get(context.Background()); err == nil {
		t.Fatal("expected error from failing TokenProvider, got nil")
	}

	got := testutil.ToFloat64(credsRefreshFailuresTotal.WithLabelValues("s3", "token_provider"))
	if got != 1 {
		t.Errorf("datuplet_creds_refresh_failures_total{type=s3,reason=token_provider} = %v, want 1", got)
	}
}

// TestCredsRefreshMetrics_ParseFailure asserts that a malformed JSON response
// increments datuplet_creds_refresh_failures_total{type="s3",reason="parse"}.
func TestCredsRefreshMetrics_ParseFailure(t *testing.T) {
	resetMetrics()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return valid JSON but with no config block so parseCreds rejects it.
		_, _ = w.Write([]byte(`{"metadata":{}}`))
	}))
	defer srv.Close()

	v := &VendedCreds{
		LakekeeperURL:     srv.URL,
		Namespace:         "public",
		Table:             "events",
		ExpectedCredsType: CredsTypeS3,
		TokenProvider:     func(context.Context) (string, error) { return "tok", nil },
	}
	if _, err := v.Get(context.Background()); err == nil {
		t.Fatal("expected error from no-config response, got nil")
	}

	got := testutil.ToFloat64(credsRefreshFailuresTotal.WithLabelValues("s3", "parse"))
	if got != 1 {
		t.Errorf("datuplet_creds_refresh_failures_total{type=s3,reason=parse} = %v, want 1", got)
	}
}
