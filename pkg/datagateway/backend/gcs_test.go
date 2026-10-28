package backend

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

// Compile-time: *gcsBackend must satisfy the fileReader interface shape used
// in server_v2_reading.go — i.e. it must have OpenReaderForFiles with the
// correct signature. The private interface lives in the datagateway package, so
// we can't write var _ fileReader = ... here; this method-value assignment is
// the equivalent check.
var _ = (*gcsBackend).OpenReaderForFiles

// fakeFetcher satisfies the credsFetcher interface for testing
// vendedTokenSource in isolation — no HTTP / no real VendedCreds machinery.
type fakeFetcher struct {
	c   catalogwriter.Creds
	err error
}

func (f *fakeFetcher) Get(_ context.Context) (catalogwriter.Creds, error) {
	return f.c, f.err
}

func TestVendedTokenSource_Token(t *testing.T) {
	expiry := time.Now().Add(15 * time.Minute)
	fetchErr := errors.New("network fail")
	tests := []struct {
		name    string
		fetcher *fakeFetcher
		wantErr error // errors.Is target; nil with wantAny false means success
		wantAny bool  // any error is acceptable
	}{
		{
			name: "returns bearer token with expiry",
			fetcher: &fakeFetcher{c: catalogwriter.GCSCreds{
				OAuthToken: "ya29.test",
				Issued:     time.Now(),
				Expires:    expiry,
			}},
		},
		{
			name: "rejects S3 creds (wrong type)",
			fetcher: &fakeFetcher{c: catalogwriter.S3Creds{
				AccessKeyID:     "AKIA",
				SecretAccessKey: "secret",
				Issued:          time.Now(),
				Expires:         time.Now().Add(time.Hour),
			}},
			wantAny: true,
		},
		{
			name:    "surfaces fetch error",
			fetcher: &fakeFetcher{err: fetchErr},
			wantErr: fetchErr,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := &vendedTokenSource{vc: tc.fetcher}
			tok, err := ts.Token()
			if tc.wantAny || tc.wantErr != nil {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
					t.Fatalf("error = %v, want wraps %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Token() error: %v", err)
			}
			if tok.AccessToken != "ya29.test" {
				t.Fatalf("AccessToken = %q, want ya29.test", tok.AccessToken)
			}
			if tok.TokenType != "Bearer" {
				t.Fatalf("TokenType = %q, want Bearer", tok.TokenType)
			}
			if !tok.Expiry.Equal(expiry) {
				t.Fatalf("Expiry = %v, want %v", tok.Expiry, expiry)
			}
		})
	}
}

func TestGCSToObjectKey(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"gs://mybucket/path/to/file", "path/to/file"},
		{"gs://otherbucket/path/to/file", "path/to/file"},
		{"gs://mybucket/single", "single"},
		{"gs://mybucket", ""}, // no path after bucket
		{"plain/relative/path", "plain/relative/path"},
		{"", ""},
	}
	g := &gcsBackend{bucket: "mybucket"}
	for _, c := range cases {
		got := g.toObjectKey(c.in)
		if got != c.want {
			t.Errorf("toObjectKey(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestVendedTokenSourceStringRedacts ensures default fmt verbs on
// *vendedTokenSource never recurse into the underlying credsFetcher
// (which holds the live bearer). RFC 019 §4.10.
func TestVendedTokenSourceStringRedacts(t *testing.T) {
	f := &fakeFetcher{c: catalogwriter.GCSCreds{
		OAuthToken: "ya29.bearer-must-not-leak",
		Issued:     time.Now(),
		Expires:    time.Now().Add(15 * time.Minute),
	}}
	ts := &vendedTokenSource{vc: f}
	for _, verb := range []string{"%v", "%+v", "%s"} {
		got := fmt.Sprintf(verb, ts)
		if strings.Contains(got, "ya29.bearer-must-not-leak") {
			t.Fatalf("%s leaked bearer: %s", verb, got)
		}
	}
}
