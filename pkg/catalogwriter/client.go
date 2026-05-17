package catalogwriter

import (
	"context"
	"errors"
	"fmt"

	iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergtable "github.com/apache/iceberg-go/table"

	// Anonymous-import gocloud's io provider so that the `s3://`, `s3a://`
	// and `s3n://` schemes are registered with iceberg-go's IO registry.
	// Without this, `tbl.FS(ctx)` returns "io scheme not registered for
	// path s3://..." once we try to write parquet against a lakekeeper-
	// vended path. This is load-bearing — keep it here in the shared
	// library so every binary that links catalogwriter inherits the
	// registration.
	_ "github.com/apache/iceberg-go/io/gocloud"
)

// TokenProvider returns a bearer JWT used as the `Authorization: Bearer
// <jwt>` header on every outbound lakekeeper request. It's called per
// request (not cached) so the caller can rotate by simply returning a
// different token next time — useful when a single Client serves
// multiple namespaces/tables with distinct JWTs.
//
// Returning an error aborts the catalog operation. Callers should
// surface that as a non-zero exit (FailedApplication per the
// pipeline-runner contract).
type TokenProvider func(ctx context.Context) (string, error)

// ClientConfig drives Client construction. URI is the lakekeeper REST
// catalog base URL (e.g. http://lakekeeper.lakekeeper.svc:8181/catalog —
// note the `/catalog` suffix required by lakekeeper). Warehouse is the
// lakekeeper warehouse name, optional but recommended; lakekeeper's
// per-warehouse path prefix is derived from it. TokenProvider is called
// once at NewClient time to source the initial bearer; rotation across
// the Client's lifetime is the responsibility of the caller (re-create
// the Client, or use VendedCreds for the data-plane writes).
//
// ProjectID, when non-empty, is forwarded as the `x-project-id` HTTP
// header on every outbound catalog call. Lakekeeper uses it to route
// requests to the per-project scope. Without it, lakekeeper falls back
// to the default project (UUID 00000000-...) where the run's synthetic
// identity carries no grants — every Check returns deny.
type ClientConfig struct {
	Name          string
	URI           string
	Warehouse     string
	TokenProvider TokenProvider
	ProjectID     string
}

// Client wraps an iceberg-go catalog.Catalog so the rest of Datuplet
// can work with lakekeeper's REST catalog or a local SQLite catalog
// without sprouting iceberg-go imports across DG / TableCommit.
//
// The Catalog field is the catalog.Catalog interface, satisfied by
// both *rest.Catalog (production / lakekeeper) and *sqlcat.Catalog
// (local-file SQLite mode). Callers only touch
// catalog.Catalog interface methods (LoadTable, CreateTable,
// ListNamespaces, ListTables, CheckNamespaceExists, CreateNamespace).
//
// The current implementation snapshots the bearer at construction time
// and passes it via `rest.WithOAuthToken`. That matches lakekeeper's
// auth shape (it accepts the same JWT shape on every request) and
// keeps the iceberg-go API surface narrow. Per-request token rotation
// for the catalog control plane (vs the data plane, which rotates via
// VendedCreds) lands in a follow-up slice if it turns out to be needed.
type Client struct {
	Catalog catalog.Catalog
	cfg     ClientConfig
}

// NewClient constructs a Client. Returns the same error shapes as
// `rest.NewCatalog` (transport / handshake errors) plus a wrapped
// error from the TokenProvider.
func NewClient(ctx context.Context, cfg ClientConfig) (*Client, error) {
	if cfg.URI == "" {
		return nil, fmt.Errorf("catalogwriter: URI is required")
	}
	if cfg.TokenProvider == nil {
		return nil, fmt.Errorf("catalogwriter: TokenProvider is required")
	}
	tok, err := cfg.TokenProvider(ctx)
	if err != nil {
		return nil, fmt.Errorf("catalogwriter: token provider: %w", err)
	}

	name := cfg.Name
	if name == "" {
		name = "datuplet-catalog"
	}

	opts := []rest.Option{
		rest.WithOAuthToken(tok),
	}
	if cfg.Warehouse != "" {
		opts = append(opts, rest.WithWarehouseLocation(cfg.Warehouse))
	}
	// Tag every catalog request with `x-project-id` so lakekeeper routes
	// to the correct per-project scope. The header is only added when
	// ProjectID is non-empty so single-project deploys
	// (LAKEKEEPER__ENABLE_DEFAULT_PROJECT=true) still work.
	if cfg.ProjectID != "" {
		opts = append(opts, rest.WithHeaders(map[string]string{
			"x-project-id": cfg.ProjectID,
		}))
	}

	cat, err := rest.NewCatalog(ctx, name, cfg.URI, opts...)
	if err != nil {
		return nil, fmt.Errorf("catalogwriter: new rest catalog: %w", err)
	}
	return &Client{Catalog: cat, cfg: cfg}, nil
}

// DropTable removes a table's metadata from the catalog WITHOUT deleting
// underlying data files. Use for manifest-only workspace input clones
// (their manifest entries reference production parquet that must NOT be
// deleted). Works against both REST and SQL/SQLite catalogs.
func (c *Client) DropTable(ctx context.Context, ident []string) error {
	return c.Catalog.DropTable(ctx, icebergtable.Identifier(ident))
}

// PurgeTable removes a table's metadata AND its underlying data files.
// REST-only: iceberg-go's *rest.Catalog has a separate PurgeTable method;
// SQL/SQLite catalog has no purge support. Use for workspace OUTPUT tables
// in cluster mode. Returns an error wrapped with the underlying catalog
// type when the catalog is not REST.
//
// REST catalogs send DELETE /namespaces/{ns}/tables/{tbl}?purgeRequested=true.
func (c *Client) PurgeTable(ctx context.Context, ident []string) error {
	return purgeTableOnRestCatalog(ctx, c.Catalog, ident)
}

// DropNamespace removes an empty namespace. Returns an error if the
// namespace still contains tables — the caller must drop all tables first.
func (c *Client) DropNamespace(ctx context.Context, ns []string) error {
	return c.Catalog.DropNamespace(ctx, ns)
}

// purgeTableOnRestCatalog is the shared implementation for PurgeTable. It
// type-asserts cat to *rest.Catalog (REST catalogs have a separate
// PurgeTable method that sends purgeRequested=true; SQL/SQLite catalogs
// don't support purge at the catalog level — local-file mode must use
// backend.RemoveAll on the workspace prefix instead, per Task 34).
//
// Package-level so workspace.go can call it directly without constructing a
// full Client (same shape as RetryOnConflict — package-level utility).
func purgeTableOnRestCatalog(ctx context.Context, cat catalog.Catalog, ident []string) error {
	rc, ok := cat.(*rest.Catalog)
	if !ok {
		return fmt.Errorf("%w; got %T (use backend.RemoveAll on the workspace prefix in local-file mode)", ErrPurgeNotSupported, cat)
	}
	return rc.PurgeTable(ctx, icebergtable.Identifier(ident))
}

// ErrPurgeNotSupported is returned by PurgeTable when the underlying catalog
// is not a REST catalog (iceberg-go's SQL/SQLite catalog has no purge API
// per Spike 0.2). Callers should fall back to plain DropTable plus
// backend.RemoveAll on the workspace prefix in local-file mode.
//
// Use errors.Is to test rather than comparing error strings:
//
//	if errors.Is(err, catalogwriter.ErrPurgeNotSupported) { ... fallback ... }
var ErrPurgeNotSupported = errors.New("catalogwriter: PurgeTable requires REST catalog")

// PurgeTable is a package-level convenience wrapper for callers that hold a
// raw catalog.Catalog (e.g. workspace.go, which passes cat directly from the
// catalog-open helper without constructing a Client). It delegates to
// purgeTableOnRestCatalog so the REST-vs-SQLite type-assertion lives in one
// place.
func PurgeTable(ctx context.Context, cat catalog.Catalog, ident []string) error {
	return purgeTableOnRestCatalog(ctx, cat, ident)
}

// ReplaceDataFiles is the FULL_LOAD-mode iceberg op: replaces filesToDelete
// with filesToAdd in the table's current snapshot via OpOverwrite.
//
// iceberg-go's table.Transaction.ReplaceDataFiles signature — passing nil for
// filesToDelete on a fresh table delegates to AddFiles internally.
//
// Package-level (not a Client method) because the transaction is
// already in the caller's hands by the time this is called — it doesn't
// need catalog-level fields from Client. Same style as RetryOnConflict.
func ReplaceDataFiles(ctx context.Context, txn *icebergtable.Transaction, filesToDelete, filesToAdd []string, props iceberg.Properties) error {
	return txn.ReplaceDataFiles(ctx, filesToDelete, filesToAdd, props)
}

