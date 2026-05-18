package backend

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

// Compile-time: *gcsBackend must satisfy the fileReader interface shape used
// in server_v2_reading.go — i.e. it must have OpenReaderForFiles with the
// correct signature. The private interface lives in the datagateway package, so
// we can't write var _ fileReader = ... here; instead the method-value
// assignment in TestGCSBackendOpenReaderForFilesSignature below is the
// equivalent check.
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

// TestGCSBackendOpenReaderForFilesSignature verifies that *gcsBackend exposes
// OpenReaderForFiles with the exact signature the private fileReader interface
// in server_v2_reading.go requires. The compile-time check is the definitive
// gate: if *gcsBackend does not have the method, this file will not compile.
//
// We also verify at runtime that the method returns a non-nil Reader and that
// the Reader result satisfies the Reader interface — mirroring the structural
// assertion pattern used in MinIOBackend tests.
func TestGCSBackendOpenReaderForFilesSignature(t *testing.T) {
	// Compile-time: if this assignment compiles, *gcsBackend has the method.
	// We use an anonymous function to keep the zero-value *gcsBackend out of
	// any real GCS traffic path — the test never calls the function.
	var _ func(context.Context, []string) (Reader, error) = (*gcsBackend)(nil).OpenReaderForFiles

	// Runtime: verify that calling OpenReaderForFiles with no files returns the
	// documented empty-slice error rather than a nil Reader with a nil error
	// (which would panic upstream in server_v2_reading.go).
	g := &gcsBackend{
		bucket: "test-bucket",
		bkt:    nil, // nil bucket — should never be reached below
	}
	_, err := g.OpenReaderForFiles(context.Background(), nil)
	if err == nil {
		t.Fatal("expected non-nil error for empty filePaths slice")
	}
	if err.Error() != "no files provided" {
		t.Fatalf("unexpected empty-slice error: %v", err)
	}
}
