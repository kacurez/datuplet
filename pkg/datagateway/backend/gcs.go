package backend

import (
	"context"
	"fmt"

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

// Token is implemented in Slice B.2. The stub keeps the package compilable
// while B.1 lands the scaffold; B.2 replaces this body with the real
// type-switched implementation + tests.
func (t *vendedTokenSource) Token() (*oauth2.Token, error) {
	return nil, fmt.Errorf("vendedTokenSource.Token: not implemented (Slice B.2)")
}
