package backend

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

// fakeFetcher satisfies the credsFetcher interface for testing
// vendedTokenSource in isolation — no HTTP / no real VendedCreds machinery.
type fakeFetcher struct {
	c   catalogwriter.Creds
	err error
}

func (f *fakeFetcher) Get(_ context.Context) (catalogwriter.Creds, error) {
	return f.c, f.err
}

func TestVendedTokenSourceReturnsBearer(t *testing.T) {
	expiry := time.Now().Add(15 * time.Minute)
	f := &fakeFetcher{c: catalogwriter.GCSCreds{
		OAuthToken: "ya29.test",
		Issued:     time.Now(),
		Expires:    expiry,
	}}
	ts := &vendedTokenSource{vc: f}
	tok, err := ts.Token()
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
}

func TestVendedTokenSourceRejectsS3(t *testing.T) {
	f := &fakeFetcher{c: catalogwriter.S3Creds{
		AccessKeyID:     "AKIA",
		SecretAccessKey: "secret",
		Issued:          time.Now(),
		Expires:         time.Now().Add(time.Hour),
	}}
	ts := &vendedTokenSource{vc: f}
	_, err := ts.Token()
	if err == nil {
		t.Fatal("expected wrong-type rejection, got nil")
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

func TestVendedTokenSourceSurfacesError(t *testing.T) {
	want := errors.New("network fail")
	f := &fakeFetcher{err: want}
	ts := &vendedTokenSource{vc: f}
	_, err := ts.Token()
	if err == nil {
		t.Fatal("expected propagated error, got nil")
	}
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want wraps %v", err, want)
	}
}
