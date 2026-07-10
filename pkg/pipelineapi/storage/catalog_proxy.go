package storage

import (
	"context"
	"fmt"

	"github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"
	"golang.org/x/sync/errgroup"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

// catalogProxy adapts the storage handlers to a `pkg/catalogwriter.Client`.
// The handlers consume two operations:
//
//   - ListNamespacedTables — used by GET /tables to enumerate every
//     (namespace, table) pair in the warehouse.
//   - LoadTable — used by GET /tables/{ns}/{t}/{info,schema,preview}.
//
// Constructed lazily per request from Service.LakekeeperURL +
// Service.Minter so a 1h+ idle pipeline-api always mints a fresh
// per-user impersonation JWT and never holds stale catalog connections.
// The minted ImpersonationToken's Reveal() is the sole path the raw JWT
// takes to lakekeeper; everywhere else the redacting wrapper protects
// against accidental log/format leaks.
//
// A single shared warehouse (`datuplet`) is used with per-project
// lakekeeper Projects and FGA grants. The per-request `x-project-id`
// header (via catalogwriter.ClientConfig.ProjectID, resolved from the
// URL pid by Service.LakekeeperProjectIDFor) scopes lakekeeper to the
// user's per-project FGA grants.
type catalogProxy struct {
	cli *catalogwriter.Client
}

// newCatalogProxy opens a fresh REST catalog connection using an
// impersonation JWT minted by svc.Minter for the authenticated user in
// ctx. Returns an error (rather than nil) when the proxy isn't
// configured so handlers can surface a clean 503/500 instead of a
// nil-deref.
//
// Minter is REQUIRED — both cluster mode and local mode wire a real
// minter that calls tokens.MintImpersonation(ctx, signer). A nil Minter
// is treated as a misconfiguration error rather than silently falling
// back to anonymous calls.
//
// projectID is the lakekeeper Project UUID forwarded as x-project-id so
// lakekeeper scopes the catalog response to the right project; pass ""
// to omit the header (pre-provisioned or fallback).
//
// warehouse is the lakekeeper warehouse name, resolved per-request via
// svc.WarehouseResolver and passed here by the caller.
func newCatalogProxy(ctx context.Context, svc *Service, projectID, warehouse string) (*catalogProxy, error) {
	if svc == nil || svc.LakekeeperURL == "" {
		return nil, fmt.Errorf("lakekeeper URL not configured")
	}
	if svc.Minter == nil {
		return nil, fmt.Errorf("Minter is required: wire tokens.MintImpersonation when calling Service.WithLakekeeper")
	}
	if warehouse == "" {
		return nil, fmt.Errorf("warehouse name is required: pass per-request from svc.WarehouseResolver")
	}
	tp := func(ctx context.Context) (string, error) {
		tok, err := svc.Minter(ctx)
		if err != nil {
			return "", err
		}
		return tok.Reveal(), nil
	}
	cli, err := catalogwriter.NewClient(ctx, catalogwriter.ClientConfig{
		Name:          "datuplet-pipeline-api-storage",
		URI:           svc.LakekeeperURL,
		Warehouse:     warehouse,
		ProjectID:     projectID,
		TokenProvider: tp,
	})
	if err != nil {
		return nil, fmt.Errorf("open lakekeeper catalog: %w", err)
	}
	return &catalogProxy{cli: cli}, nil
}

// listAllTables returns every (namespace, table) the proxy can see in
// the configured warehouse. The shape mirrors what walker.go's
// ListTables produces so handlers/list_tables.go stays uniform across
// the two backing paths.
//
// Lakekeeper-managed namespaces correspond 1:1 to pipeline-api buckets
// (the YAML's bucket name is forwarded as the namespace; lakekeeper
// allocates a namespace under the warehouse root for each one).
//
// Trust boundary: pipeline-api's FGA datuplet_member check is the
// authoritative project-scoping gate in resolveProject. The caller must
// already have passed that check before reaching this method. There is
// intentionally no metadata-location filter here — lakekeeper's UUID-keyed
// paths don't carry project identity, so any prefix check would be either
// useless or wrong.
func (p *catalogProxy) listAllTables(ctx context.Context) ([]TableRef, error) {
	// Listing is cheap; LoadTable dominates wall-clock (one REST call +
	// credential vend per table). Collect identifiers first, then load
	// with bounded parallelism (RFC 025 §4.3).
	var idents []icebergtable.Identifier
	namespaces, err := p.cli.Catalog.ListNamespaces(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("list namespaces: %w", err)
	}
	for _, ns := range namespaces {
		for ident, tErr := range p.cli.Catalog.ListTables(ctx, ns) {
			if tErr != nil {
				return nil, fmt.Errorf("list tables in %v: %w", ns, tErr)
			}
			idents = append(idents, ident)
		}
	}

	loaded := make([]*TableRef, len(idents))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for i, ident := range idents {
		g.Go(func() error {
			tbl, lErr := p.cli.Catalog.LoadTable(gctx, ident)
			if lErr != nil {
				// A table that fails to load (orphan, mid-creation,
				// permission denied) is skipped rather than failing the
				// whole list — same behaviour as the sequential version.
				return nil
			}
			var snapID int64
			if cur := tbl.CurrentSnapshot(); cur != nil {
				snapID = cur.SnapshotID
			}
			loaded[i] = &TableRef{
				Namespace:         joinNS(ident),
				Name:              shortName(ident),
				MetadataLocation:  tbl.MetadataLocation(),
				CurrentSnapshotID: snapID,
			}
			return nil
		})
	}
	// goroutines only ever return nil (per-table load failures are
	// intentionally swallowed above), so g.Wait()'s error is always nil —
	// ctx cancellation no longer surfaces here; it's checked explicitly
	// below via ctx.Err().
	_ = g.Wait()

	// A cancelled/deadline-exceeded request context must surface as an
	// error, not a silently-empty (or silently-partial) table list.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}

	out := make([]TableRef, 0, len(loaded))
	for _, ref := range loaded {
		if ref != nil {
			out = append(out, *ref)
		}
	}

	// Distinguish "every LoadTable call failed" (likely a lakekeeper/STS
	// outage) from a genuinely empty catalog. A partial result (some
	// loaded, some skipped) is left as success — that's the intended
	// orphan/mid-creation/permission-denied skip behavior above, and
	// can't be distinguished from a partial outage without more signal.
	if len(idents) > 0 && len(out) == 0 {
		return nil, fmt.Errorf("failed to load any of %d catalog tables (lakekeeper/STS outage?)", len(idents))
	}

	return out, nil
}

// loadTableForRead fetches the (ns, table) pair via the catalog. ns is
// the bucket-shaped lakekeeper namespace; tbl is the table name.
func (p *catalogProxy) loadTableForRead(ctx context.Context, ns, tbl string) (*icebergtable.Table, error) {
	ident := catalog.ToIdentifier(ns, tbl)
	t, err := p.cli.Catalog.LoadTable(ctx, ident)
	if err != nil {
		return nil, err
	}
	return t, nil
}

// joinNS flattens a multi-segment lakekeeper namespace identifier into
// a "."-separated string for the wire response. iceberg-go's
// table.Identifier is "[ns0, ns1, ..., name]"; we drop the last element
// and join the rest. For the single-level namespace shape Datuplet uses
// today this is a passthrough.
func joinNS(ident icebergtable.Identifier) string {
	if len(ident) <= 1 {
		return ""
	}
	out := ident[0]
	for _, s := range ident[1 : len(ident)-1] {
		out += "." + s
	}
	return out
}

func shortName(ident icebergtable.Identifier) string {
	if len(ident) == 0 {
		return ""
	}
	return ident[len(ident)-1]
}
