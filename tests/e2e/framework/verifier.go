// Package framework — lakekeeper-vended verifier paths.
//
// Lakekeeper allocates UUID-keyed paths
// (s3://<warehouse>/<storage-uuid>/<table-uuid>/...) that don't carry
// project identity in the path. The harness therefore must query
// lakekeeper for the actual `Table.Location()` per scenario and feed
// that to DuckDB.
//
// This file owns the LoadTable wrapping. assertions.go consumes the
// resolved Location() string instead of building the path itself; the
// scenarios pass `bucket` (== lakekeeper namespace) and `table` (==
// table identifier) as before, so scenario definitions don't change.
package framework

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/iceberg-go/catalog"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

// LakekeeperVerifier resolves table identities (namespace, name) into
// the concrete location strings DuckDB needs for iceberg_scan().
//
// Constructed once per test scenario from FGAHarness, but the
// underlying catalog client is built lazily per `LocationFor` call so
// each read mints a fresh token. iceberg-go's REST catalog snapshots
// the bearer at construction time (no refresh callback), so a long-
// running pipeline run will outlive a 60-second impersonation JWT
// captured at scenario start. Lazy build trades one extra HTTPS
// handshake per assertion for correctness.
type LakekeeperVerifier struct {
	harness  *FGAHarness
	provider catalogwriter.TokenProvider
}

// NewVerifier records the harness + tokenProvider for later use.
// `tokenProvider` is invoked at every `LocationFor` call (once per
// underlying client construction) — pass a closure over
// MintTestUserImpersonation when scenario assertions need user-scoped
// reads, or a service-token minter for "verify against ground truth"
// reads that bypass FGA (only the bootstrap path should do that).
func NewVerifier(ctx context.Context, h *FGAHarness, tokenProvider catalogwriter.TokenProvider) (*LakekeeperVerifier, error) {
	if h == nil || h.LakekeeperBaseURL == "" {
		return nil, errors.New("NewVerifier: harness has no LakekeeperBaseURL")
	}
	if tokenProvider == nil {
		return nil, errors.New("NewVerifier: tokenProvider is required")
	}
	return &LakekeeperVerifier{harness: h, provider: tokenProvider}, nil
}

// LocationFor returns the concrete `Table.Location()` lakekeeper has
// allocated for (namespace, table). Builds a fresh catalog client per
// call so the embedded bearer is never older than ~60s (impersonation
// TTL). Shape is typically `s3://<warehouse>/<storage-uuid>/<table-uuid>/`;
// DuckDB consumes it directly.
func (v *LakekeeperVerifier) LocationFor(ctx context.Context, namespace, table string) (string, error) {
	if v == nil || v.harness == nil {
		return "", errors.New("LakekeeperVerifier: not initialised")
	}
	cli, err := catalogwriter.NewClient(ctx, catalogwriter.ClientConfig{
		Name:          "datuplet-e2e-verifier",
		URI:           v.harness.CatalogURI(),
		Warehouse:     v.harness.WarehouseName,
		ProjectID:     v.harness.LakekeeperProjectID,
		TokenProvider: v.provider,
	})
	if err != nil {
		return "", fmt.Errorf("open lakekeeper catalog: %w", err)
	}
	tbl, err := cli.Catalog.LoadTable(ctx, catalog.ToIdentifier(namespace, table))
	if err != nil {
		return "", fmt.Errorf("LoadTable %s.%s: %w", namespace, table, err)
	}
	return tbl.Location(), nil
}
