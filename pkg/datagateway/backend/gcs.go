package backend

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

// GCSConfig is the static-key path used by tests and by the local-mode
// dev flow. Production callers use NewGCSBackendWithProvider — the
// VendedCreds path with lakekeeper-vended OAuth tokens.
type GCSConfig struct {
	Bucket            string
	ServiceAccountKey []byte // optional; falls back to ADC when nil
}

// GCSProviderConfig is the production path. VendedCreds must have
// ExpectedCredsType set to CredsTypeGCS.
type GCSProviderConfig struct {
	Bucket      string
	VendedCreds *catalogwriter.VendedCreds
}

// gcsBackend implements the StorageBackend interface using GCS as the
// object-storage backend. The 9 StorageBackend methods land in Slice B.3.
type gcsBackend struct {
	bucket string
	client *storage.Client
	bkt    *storage.BucketHandle
}

// NewGCSBackend constructs a backend using static credentials.
func NewGCSBackend(cfg GCSConfig) (*gcsBackend, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("gcs: bucket required")
	}
	ctx := context.Background()
	opts := []option.ClientOption{option.WithUserAgent("datuplet-datagateway")}
	if len(cfg.ServiceAccountKey) > 0 {
		opts = append(opts, option.WithCredentialsJSON(cfg.ServiceAccountKey))
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &gcsBackend{bucket: cfg.Bucket, client: client, bkt: client.Bucket(cfg.Bucket)}, nil
}

// NewGCSBackendWithProvider constructs a backend using lakekeeper-vended
// OAuth tokens. The TokenSource refreshes via VendedCreds.Get() — see RFC
// 019 §4.3.
func NewGCSBackendWithProvider(cfg GCSProviderConfig) (*gcsBackend, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("gcs: bucket required")
	}
	if cfg.VendedCreds == nil {
		return nil, fmt.Errorf("gcs: VendedCreds required")
	}
	if cfg.VendedCreds.ExpectedCredsType != catalogwriter.CredsTypeGCS {
		return nil, fmt.Errorf("gcs: VendedCreds.ExpectedCredsType must be CredsTypeGCS, got %q", cfg.VendedCreds.ExpectedCredsType)
	}
	ctx := context.Background()
	ts := &vendedTokenSource{vc: cfg.VendedCreds}
	client, err := storage.NewClient(ctx,
		option.WithTokenSource(ts),
		option.WithUserAgent("datuplet-datagateway"),
	)
	if err != nil {
		return nil, err
	}
	return &gcsBackend{bucket: cfg.Bucket, client: client, bkt: client.Bucket(cfg.Bucket)}, nil
}

// credsFetcher is the abstraction vendedTokenSource depends on.
// Production: *catalogwriter.VendedCreds. Tests: in-package fake.
// Keeping this tiny (single method) is intentional — it isolates the
// token-source's lifecycle from VendedCreds's caching machinery and
// makes Token() unit-testable without spinning up an HTTP server.
type credsFetcher interface {
	Get(ctx context.Context) (catalogwriter.Creds, error)
}

// vendedTokenSource adapts VendedCreds to oauth2.TokenSource. This is the
// ONE place GCSCreds.OAuthToken is read out of the sealed-interface value;
// the bearer flows from here into the *storage.Client and MUST NOT be
// logged or formatted anywhere downstream (see RFC 019 §4.10).
type vendedTokenSource struct {
	vc credsFetcher
}

// Compile-time assertion that vendedTokenSource satisfies oauth2.TokenSource.
var _ oauth2.TokenSource = (*vendedTokenSource)(nil)

// Token fetches the current vended creds, asserts they're GCSCreds, and
// returns an *oauth2.Token suitable for the storage client. The
// type-assertion is the load-bearing safety check: even though
// NewGCSBackendWithProvider validates ExpectedCredsType up front, this
// runtime check defends against a misconfigured/poisoned VendedCreds that
// somehow returns the wrong family — fail closed, never hand a non-GCS
// secret to the GCS client.
//
// Redaction: the wrong-type error formats %T (type-only); never %v / %+v
// — formatting the Creds value would leak the bearer. See RFC 019 §4.10.
func (t *vendedTokenSource) Token() (*oauth2.Token, error) {
	c, err := t.vc.Get(context.Background())
	if err != nil {
		return nil, err
	}
	gc, ok := c.(catalogwriter.GCSCreds)
	if !ok {
		return nil, fmt.Errorf("vendedTokenSource: expected GCSCreds, got %T", c)
	}
	return &oauth2.Token{
		AccessToken: gc.OAuthToken,
		Expiry:      gc.ExpiresAt(),
		TokenType:   "Bearer",
	}, nil
}

// toObjectKey converts a storage path that may be a full GCS URL
// ("gs://bucket/path") into the bucket-relative object key ("path").
// Mirrors MinIOBackend.toObjectKey with the gs:// scheme.
//
// Examples:
//   - "gs://mybucket/path/to/file" → "path/to/file"
//   - "gs://otherbucket/path/to/file" → "path/to/file" (extracts path regardless of bucket)
//   - "path/to/file" → "path/to/file" (already relative)
func (g *gcsBackend) toObjectKey(storagePath string) string {
	const scheme = "gs://"
	if len(storagePath) >= len(scheme) && storagePath[:len(scheme)] == scheme {
		withoutScheme := storagePath[len(scheme):]
		for i := 0; i < len(withoutScheme); i++ {
			if withoutScheme[i] == '/' {
				return withoutScheme[i+1:]
			}
		}
		// Edge case: gs://bucket with no path
		return ""
	}
	return storagePath
}

// PutObject uploads raw bytes to the given path. The path may be a full
// "gs://bucket/key" URL or a bucket-relative key; toObjectKey normalises it.
func (g *gcsBackend) PutObject(ctx context.Context, storagePath string, data []byte) error {
	objectKey := g.toObjectKey(storagePath)
	w := g.bkt.Object(objectKey).NewWriter(ctx)
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return fmt.Errorf("gcs: put %q: %w", objectKey, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("gcs: put %q: close: %w", objectKey, err)
	}
	return nil
}

// Close releases the underlying storage client. Idempotent.
func (g *gcsBackend) Close() error {
	if g.client == nil {
		return nil
	}
	return g.client.Close()
}

// GetObject downloads raw bytes from the given path. The path may be a full
// "gs://bucket/key" URL or a bucket-relative key.
func (g *gcsBackend) GetObject(ctx context.Context, storagePath string) ([]byte, error) {
	objectKey := g.toObjectKey(storagePath)
	r, err := g.bkt.Object(objectKey).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("gcs: get %q: %w", objectKey, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("gcs: read %q: %w", objectKey, err)
	}
	return data, nil
}
