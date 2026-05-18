package datupleticeio

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	iceio "github.com/apache/iceberg-go/io"
	"golang.org/x/oauth2"
)

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
