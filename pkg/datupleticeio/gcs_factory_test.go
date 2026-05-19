package datupleticeio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	iceio "github.com/apache/iceberg-go/io"
	"gocloud.dev/blob/memblob"
	"golang.org/x/oauth2"
)

// newTestGcsIO returns a *gcsIO backed by memblob (in-memory) so the
// WriteFileIO path can be exercised end-to-end without touching the
// network. Use a fixed bucketName so preprocess() can strip the prefix.
func newTestGcsIO(t *testing.T) *gcsIO {
	t.Helper()
	b := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = b.Close() })
	return &gcsIO{
		bucket:     b,
		bucketName: "test-bucket",
		ctx:        context.Background(),
	}
}

func TestGcsIOCreateRoundTrip(t *testing.T) {
	g := newTestGcsIO(t)
	f, err := g.Create("gs://test-bucket/round-trip/key.bin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := f.Write([]byte("hello world")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := g.Open("gs://test-bucket/round-trip/key.bin")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("round-trip mismatch: got %q", got)
	}
}

func TestGcsIOWriteFileByteSlice(t *testing.T) {
	g := newTestGcsIO(t)
	payload := []byte("byte-slice payload")
	if err := g.WriteFile("gs://test-bucket/wf/file.bin", payload); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	r, err := g.Open("gs://test-bucket/wf/file.bin")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	got, _ := io.ReadAll(r)
	if !bytes.Equal(got, payload) {
		t.Fatalf("got %q want %q", got, payload)
	}
}

// TestGcsIOCreateReadFromCopy validates that *gcsWriteFile's ReadFrom
// delegates to io.Copy so iceberg-go's chunked-write callers (which
// pipe data through io.Copy) work without an intermediate Write-loop.
func TestGcsIOCreateReadFromCopy(t *testing.T) {
	g := newTestGcsIO(t)
	f, err := g.Create("gs://test-bucket/readfrom/key.bin")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	payload := bytes.Repeat([]byte("abc"), 4096) // 12 KiB, exceeds any single-Write buffer
	// Lock in the io.ReaderFrom contract on iceio.FileWriter — callers like
	// io.Copy dispatch through this assertion, so the test must too.
	rf, ok := f.(io.ReaderFrom)
	if !ok {
		t.Fatal("Create-returned FileWriter must implement io.ReaderFrom")
	}
	n, err := rf.ReadFrom(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if n != int64(len(payload)) {
		t.Fatalf("ReadFrom returned n=%d want %d", n, len(payload))
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	r, _ := g.Open("gs://test-bucket/readfrom/key.bin")
	defer r.Close()
	got, _ := io.ReadAll(r)
	if !bytes.Equal(got, payload) {
		t.Fatalf("ReadFrom round-trip mismatch (len=%d, expected %d)", len(got), len(payload))
	}
}

func TestInitRegistersGS(t *testing.T) {
	// LoadFS with a gs:// URI must call OUR factory, not the upstream one.
	// The factory fails with "missing gcs.oauth2.token" when props is empty —
	// that error message is our fingerprint.
	_, err := iceio.LoadFS(context.Background(), nil, "gs://test-bucket")
	if err == nil {
		t.Fatal("expected missing-token error from empty props")
	}
	if !strings.Contains(err.Error(), "missing gcs.oauth2.token") {
		t.Fatalf("error did not come from datupletGCSFactory: %v", err)
	}
}

func TestRefreshingTokenSourceRenewsBeforeExpiry(t *testing.T) {
	now := time.Now()
	calls := 0
	refresh := func(ctx context.Context) (*oauth2.Token, error) {
		calls++
		return &oauth2.Token{
			AccessToken: fmt.Sprintf("tok-%d", calls),
			Expiry:      now.Add(time.Duration(calls) * time.Hour),
		}, nil
	}
	rts := newRefreshingTokenSource(&oauth2.Token{
		AccessToken: "initial",
		Expiry:      now.Add(-1 * time.Second), // already-expired
	}, refresh)

	tok, err := rts.Token()
	if err != nil {
		t.Fatal(err)
	}
	if tok.AccessToken != "tok-1" {
		t.Fatalf("first Token = %q", tok.AccessToken)
	}
	if calls != 1 {
		t.Fatalf("calls = %d", calls)
	}
}

func TestRefreshingTokenSourceErrorSurfaces(t *testing.T) {
	refresh := func(ctx context.Context) (*oauth2.Token, error) {
		return nil, errors.New("refresh fail")
	}
	rts := newRefreshingTokenSource(nil, refresh)
	_, err := rts.Token()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRefreshingTokenSourceDoesNotReturnStaleOnFailure(t *testing.T) {
	// Even if a previous token was cached, a refresh failure must surface,
	// not return the stale token. RFC 019 §4.5.3 hard-error contract.
	now := time.Now()
	first := true
	refresh := func(ctx context.Context) (*oauth2.Token, error) {
		if first {
			first = false
			// First refresh: return a token already within the 1-minute
			// expiry window so the next Token() call triggers another refresh.
			return &oauth2.Token{AccessToken: "ok", Expiry: now.Add(30 * time.Second)}, nil
		}
		return nil, errors.New("refresh fail")
	}
	// Seed with an expired token so Token() will refresh.
	rts := newRefreshingTokenSource(&oauth2.Token{AccessToken: "stale", Expiry: now.Add(-1 * time.Second)}, refresh)
	tok, err := rts.Token()
	if err != nil {
		t.Fatalf("first Token: %v", err)
	}
	if tok.AccessToken != "ok" {
		t.Fatalf("first Token = %q", tok.AccessToken)
	}
	// Second call: cached token is within the 1-minute refresh-ahead window,
	// so Token() will call refresh again, which now fails.
	_, err = rts.Token()
	if err == nil {
		t.Fatal("expected hard error on refresh failure")
	}
}

// TestRefreshingTokenSourceStringRedacts ensures default fmt verbs on
// *refreshingTokenSource never expand the wrapped *oauth2.Token (whose
// AccessToken field holds the live bearer). RFC 019 §4.10.
func TestRefreshingTokenSourceStringRedacts(t *testing.T) {
	rts := newRefreshingTokenSource(&oauth2.Token{
		AccessToken: "ya29.bearer-must-not-leak",
		Expiry:      time.Now().Add(time.Hour),
	}, nil)
	for _, verb := range []string{"%v", "%+v", "%s"} {
		got := fmt.Sprintf(verb, rts)
		if strings.Contains(got, "ya29.bearer-must-not-leak") {
			t.Fatalf("%s leaked bearer: %s", verb, got)
		}
	}
}

// TestPickRefreshEndpointActivationFailsClosed verifies that datupletGCSFactory
// fails fast at construction time when gcs.oauth2.refresh-credentials-endpoint
// is present in props AND gcs.oauth2.refresh-credentials-enabled=true (explicit
// opt-in). Without this, the unvalidated endpointRefresh path would only surface
// as an error ~15 minutes later when the initial token expires mid-transaction
// (P1-S1). The endpoint path requires explicit opt-in via enabled=true.
func TestPickRefreshEndpointActivationFailsClosed(t *testing.T) {
	parsed, _ := url.Parse("gs://test-bucket")
	props := map[string]string{
		"gcs.oauth2.token":                         "ya29.test",
		"gcs.oauth2.refresh-credentials-endpoint":  "https://lakekeeper.example/refresh",
		"gcs.oauth2.refresh-credentials-enabled":   "true", // explicit opt-in
	}
	_, err := datupletGCSFactory(context.Background(), parsed, props)
	if err == nil {
		t.Fatal("expected fail-fast error when endpoint refresh is explicitly opted in")
	}
	if !strings.Contains(err.Error(), "not yet validated end-to-end in v0.2") {
		t.Fatalf("error did not match fail-fast message: %v", err)
	}
}

// TestPickRefreshEndpointPresentButNotOptedIn verifies that when Lakekeeper
// emits gcs.oauth2.refresh-credentials-endpoint in the loadTable response
// (the default deployment shape) but the caller has NOT set
// gcs.oauth2.refresh-credentials-enabled=true, the factory falls through to
// loadTableRefresh and construction succeeds. This is the critical regression
// test for the P1 opt-in fix: the prior `enabled != "false"` guard would
// fail-fast here, breaking every WIF TableCommit.
func TestPickRefreshEndpointPresentButNotOptedIn(t *testing.T) {
	parsed, _ := url.Parse("gs://test-bucket")
	props := map[string]string{
		"gcs.oauth2.token":                        "ya29.test",
		"gcs.oauth2.refresh-credentials-endpoint": "https://lakekeeper.example/refresh",
		// gcs.oauth2.refresh-credentials-enabled NOT set — default deployment shape
	}
	gcsio, err := datupletGCSFactory(context.Background(), parsed, props)
	if err != nil {
		t.Fatalf("expected success (loadTableRefresh fallback), got %v", err)
	}
	if gcsio == nil {
		t.Fatal("expected non-nil IO")
	}
	_ = gcsio.(*gcsIO).Close()
}

// TestPickRefreshEndpointExplicitlyDisabledSucceeds verifies that setting
// gcs.oauth2.refresh-credentials-enabled=false also suppresses the endpoint
// path (belt-and-suspenders: any value other than "true" keeps the fallback).
func TestPickRefreshEndpointExplicitlyDisabledSucceeds(t *testing.T) {
	parsed, _ := url.Parse("gs://test-bucket")
	props := map[string]string{
		"gcs.oauth2.token":                         "ya29.test",
		"gcs.oauth2.refresh-credentials-endpoint":  "https://lakekeeper.example/refresh",
		"gcs.oauth2.refresh-credentials-enabled":   "false",
	}
	gcsio, err := datupletGCSFactory(context.Background(), parsed, props)
	if err != nil {
		t.Fatalf("expected success with endpoint disabled, got %v", err)
	}
	if gcsio == nil {
		t.Fatal("expected non-nil IO")
	}
	_ = gcsio.(*gcsIO).Close()
}

// TestPickRefreshLoadTableDefault verifies that without the endpoint prop
// the factory succeeds (loadTableRefresh is the default).
func TestPickRefreshLoadTableDefault(t *testing.T) {
	parsed, _ := url.Parse("gs://test-bucket")
	props := map[string]string{
		"gcs.oauth2.token": "ya29.test",
	}
	gcsio, err := datupletGCSFactory(context.Background(), parsed, props)
	if err != nil {
		t.Fatalf("expected success with loadTableRefresh default, got %v", err)
	}
	if gcsio == nil {
		t.Fatal("expected non-nil IO")
	}
	_ = gcsio.(*gcsIO).Close()
}

// TestRefreshErrorDoesNotLeakBearer locks in the no-leak contract:
// when a refresh fails, the error message must not contain the bearer token.
// RFC 019 §4.5.3.
func TestRefreshErrorDoesNotLeakBearer(t *testing.T) {
	sentinel := "ya29.secret-bearer-value"
	rts := newRefreshingTokenSource(nil, func(ctx context.Context) (*oauth2.Token, error) {
		return nil, errors.New("transient failure")
	})
	rts.cur = &oauth2.Token{AccessToken: sentinel, Expiry: time.Now().Add(-1 * time.Second)}
	_, err := rts.Token()
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), sentinel) {
		t.Fatalf("bearer leaked into error: %v", err)
	}
}

// TestDatupletGCSFactoryEndToEnd is a skeleton for an integration test
// against fake-gcs-server. Slice B's report flagged the fake-gcs-server
// testcontainers harness as currently broken on this dev machine, so
// this test is gated behind INTEGRATION=1 and left as a placeholder.
// The unit tests for refreshingTokenSource are the load-bearing
// coverage for the credentials-refresh contract.
//
// TODO(rfc-019): wire up fake-gcs-server harness once it's working
// again; assert round-trip Open + Read of a small object via the
// datupletGCSFactory-returned IO.
func TestDatupletGCSFactoryEndToEnd(t *testing.T) {
	if os.Getenv("INTEGRATION") != "1" {
		t.Skip("INTEGRATION=1 required (uses fake-gcs-server, currently disabled)")
	}
	t.Skip("end-to-end harness not yet wired up; tracked in RFC 019 follow-on slice")
}
