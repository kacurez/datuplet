// Package lakekeeper wires DataGateway's per-table parquet writes
// directly to lakekeeper-vended STS credentials. Lakekeeper is the
// catalog of record.
//
// The package exists primarily as a glue layer between three already-
// existing pieces:
//
//   - `pkg/catalogwriter` — Client (REST catalog), VendedCreds (STS
//     fetch + 50%-elapsed renewal contract).
//   - the data plane (`pkg/datagateway.DataPlane`), which writes the
//     table's Parquet files with those credentials.
//   - lakekeeper itself — owns table location allocation; the only
//     party that knows the per-table S3 prefix vended STS will accept.
//
// On first write to (namespace, table) the gateway calls Resolver to:
//
//  1. Open a `catalogwriter.Client` against lakekeeper using the
//     write-intent JWT.
//  2. LoadTable; on NotFound, CreateTable using a schema converted
//     from the gateway-side schema. (DG owns table creation rather
//     than TableCommit because vended STS scoped to the table prefix
//     can only be issued once the table exists in lakekeeper.)
//  3. Return the table's data prefix plus a VendedCreds credential
//     source the data plane writes it with.
//
// Reads do not go through the Resolver: the data plane ATTACHes the
// catalog itself.
//
// The package is deliberately small: production logic lives in the
// callers (server_v2_writing.go, the commit pool). This file only owns
// the catalog-and-creds plumbing.
package lakekeeper

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	"github.com/datuplet/datuplet/pkg/datagateway/manifest"
	dgschema "github.com/datuplet/datuplet/pkg/datagateway/schema"
	// Blank-import the centralised iceberg-go IO scheme registration package
	// so isolated tests of this package (e.g. go test ./pkg/datagateway/lakekeeper/...)
	// use the Datuplet-overridden gs:// factory rather than the upstream
	// gocloud default. Without this, pkg/datupleticeio's init() is absent
	// from the binary when lakekeeper is tested in isolation, silently
	// activating the wrong factory.
	_ "github.com/datuplet/datuplet/pkg/datupleticeio"
)

// Resolver translates (namespace, table) into a writable per-table
// location plus its vended credentials.
//
// Lakekeeper REST + vended-creds STS is the only supported catalog backend.
//
// One Resolver instance is created per gateway (cheap; just config),
// then shared across writers. The actual catalog client is
// short-lived and re-opened per call to keep the auth path simple
// (per-call bearer attach) and to dodge any iceberg-go session
// affinity gotchas.
type Resolver struct {
	URL       string // lakekeeper REST base URL (e.g. http://lakekeeper:8181/catalog)
	Warehouse string // optional; lakekeeper warehouse name
	Token     string // single per-run JWT; empty for unauthenticated lakekeeper
	// ProjectID is the lakekeeper Project UUID forwarded as the
	// `x-project-id` HTTP header on every catalog/STS call. Empty
	// disables the header — single-project deploys
	// (LAKEKEEPER__ENABLE_DEFAULT_PROJECT=true) still work.
	ProjectID string
}

// NewResolver constructs a REST-catalog Resolver. URL must be non-empty;
// token may be empty for dev/Docker paths against an unauthenticated
// lakekeeper. projectID may be empty for single-project deploys.
func NewResolver(url, warehouse, token, projectID string) (*Resolver, error) {
	if url == "" {
		return nil, errors.New("lakekeeper: URL is required")
	}
	return &Resolver{
		URL:       url,
		Warehouse: warehouse,
		Token:     token,
		ProjectID: projectID,
	}, nil
}

// Close is a no-op for the REST-only Resolver. The *catalogwriter.Client
// per call has no long-lived state worth tearing down.
func (r *Resolver) Close() error {
	return nil
}

// WriterTarget describes a writable per-table location.
type WriterTarget struct {
	// BasePath is the opaque data prefix the data plane writes parquet
	// files under (e.g. s3://warehouse/<warehouse-uuid>/<table-uuid>/data/).
	// The trailing slash is included so joinStoragePath builds a clean
	// child URL.
	BasePath string

	// VendedCreds sources the table's lakekeeper-vended storage
	// credentials (STS for s3://, OAuth bearer for gs://) through one
	// renewal cache. nil for file:// prefixes (local tests).
	VendedCreds *catalogwriter.VendedCreds
}

// SchemaProvider returns the gateway-side schema for a table. The
// resolver calls this only on first-write (table doesn't exist in
// lakekeeper yet); subsequent writes load the table via the catalog
// and don't need a Provider hit.
//
// Returning an error fails the OpenWriter call.
type SchemaProvider func(ctx context.Context) (*dgschema.Schema, error)

// LoadOrCreateForWrite ensures (ns, tbl) exists in lakekeeper and
// returns a WriterTarget rooted at the table's data prefix.
//
// Behaviour matrix:
//
//   - Table exists → LoadTable; reuse existing schema; build
//     WriterTarget at the loaded data path.
//   - Table missing + schemaProvider returns a schema → ensure
//     namespace, CreateTable with that schema, build WriterTarget at
//     the new table's data path.
//   - Table missing + schemaProvider is nil OR returns nil → return
//     an error. Callers must defer the call until they have a schema
//     (typically until first chunk is parsed). Surfacing this as an
//     explicit error matches the `OpenWriter` contract: schema is the
//     load-bearing input.
func (r *Resolver) LoadOrCreateForWrite(ctx context.Context, ns, tbl string, schemaProvider SchemaProvider) (*WriterTarget, error) {
	cli, err := r.newClient(ctx, r.Token)
	if err != nil {
		return nil, fmt.Errorf("lakekeeper: new client: %w", err)
	}

	ident := catalog.ToIdentifier(ns, tbl)
	t, err := cli.Catalog.LoadTable(ctx, ident)
	if err != nil {
		if !errors.Is(err, catalog.ErrNoSuchTable) {
			return nil, fmt.Errorf("lakekeeper: load %s.%s: %w", ns, tbl, err)
		}
		if schemaProvider == nil {
			return nil, fmt.Errorf("lakekeeper: table %s.%s missing and no schema available to create it", ns, tbl)
		}
		gwSchema, schErr := schemaProvider(ctx)
		if schErr != nil {
			return nil, fmt.Errorf("lakekeeper: schema provider for %s.%s: %w", ns, tbl, schErr)
		}
		if gwSchema == nil {
			return nil, fmt.Errorf("lakekeeper: schema provider returned nil for %s.%s", ns, tbl)
		}
		if err := r.ensureNamespace(ctx, cli.Catalog, ns); err != nil {
			return nil, err
		}
		icSchema := manifest.SchemaToIceberg(gwSchema)
		if icSchema == nil {
			return nil, fmt.Errorf("lakekeeper: cannot convert gateway schema for %s.%s", ns, tbl)
		}
		t, err = cli.Catalog.CreateTable(ctx, ident, icSchema)
		if err != nil {
			// Concurrent committer race: re-LoadTable.
			if errors.Is(err, catalog.ErrTableAlreadyExists) {
				t, err = cli.Catalog.LoadTable(ctx, ident)
				if err != nil {
					return nil, fmt.Errorf("lakekeeper: load %s.%s after race: %w", ns, tbl, err)
				}
			} else {
				return nil, fmt.Errorf("lakekeeper: create %s.%s: %w", ns, tbl, err)
			}
		}
	}

	// Compute the data prefix from the loaded table's location. Both
	// branches (load + create) come back here so the path stays
	// uniform.
	loc := t.Location()
	if loc == "" {
		return nil, fmt.Errorf("lakekeeper: table %s.%s has empty location", ns, tbl)
	}
	dataPrefix := strings.TrimRight(loc, "/") + "/data/"

	vc, err := r.writeCreds(dataPrefix, ns, tbl)
	if err != nil {
		return nil, err
	}
	return &WriterTarget{
		BasePath:    dataPrefix,
		VendedCreds: vc,
	}, nil
}

// writeCreds returns the vended credentials a writer of (ns, tbl) uses for
// dataPrefix: S3 STS for s3://, GCS OAuth bearer for gs://, nil for file://.
func (r *Resolver) writeCreds(dataPrefix, ns, tbl string) (*catalogwriter.VendedCreds, error) {
	switch {
	case strings.HasPrefix(dataPrefix, "file://"):
		return nil, nil
	case strings.HasPrefix(dataPrefix, "s3://"):
		return r.newVendedCreds(ns, tbl, catalogwriter.CredsTypeS3), nil
	case strings.HasPrefix(dataPrefix, "gs://"):
		return r.newVendedCreds(ns, tbl, catalogwriter.CredsTypeGCS), nil
	default:
		return nil, fmt.Errorf("lakekeeper: unsupported scheme in %q (need s3://, gs://, or file://)", dataPrefix)
	}
}

// newVendedCreds builds the per-table credential source. VendedCreds
// discovers the per-warehouse REST URL prefix lazily on first fetch via
// `GET /v1/config?warehouse=<name>` (matching iceberg-go's REST catalog
// handshake). The data-path UUID embedded in the data prefix is NOT
// necessarily the warehouse-id used in the REST URL — lakekeeper assigns
// separate identifiers — so deriving the prefix from the path is unsafe.
func (r *Resolver) newVendedCreds(ns, tbl string, typ catalogwriter.CredsType) *catalogwriter.VendedCreds {
	tok := r.Token
	return &catalogwriter.VendedCreds{
		LakekeeperURL:     r.URL,
		WarehouseName:     r.Warehouse,
		ProjectID:         r.ProjectID,
		Namespace:         ns,
		Table:             tbl,
		TokenProvider:     func(context.Context) (string, error) { return tok, nil },
		ExpectedCredsType: typ,
	}
}

// Catalog returns an iceberg catalog handle bound to the resolver's run
// token. A fresh *catalogwriter.Client is opened per call (same pattern
// as LoadOrCreateForWrite) — there is no shared
// catalog state, so no lifecycle coupling with concurrent callers.
// Used by the inline commit pool (RFC 021).
func (r *Resolver) Catalog(ctx context.Context) (catalog.Catalog, error) {
	cli, err := r.newClient(ctx, r.Token)
	if err != nil {
		return nil, fmt.Errorf("lakekeeper: open catalog client: %w", err)
	}
	return cli.Catalog, nil
}

// newClient builds a per-call catalogwriter.Client backed by lakekeeper's
// REST API (the only supported catalog backend).
func (r *Resolver) newClient(ctx context.Context, token string) (*catalogwriter.Client, error) {
	tp := func(context.Context) (string, error) { return token, nil }
	return catalogwriter.NewClient(ctx, catalogwriter.ClientConfig{
		Name:          "datuplet-datagateway",
		URI:           r.URL,
		Warehouse:     r.Warehouse,
		TokenProvider: tp,
		ProjectID:     r.ProjectID,
	})
}

// ensureNamespace mirrors icebergjob.ensureNamespace. Idempotent:
// ErrNamespaceAlreadyExists is squashed.
func (r *Resolver) ensureNamespace(ctx context.Context, cat catalogClient, ns string) error {
	id := catalog.ToIdentifier(ns)
	exists, err := cat.CheckNamespaceExists(ctx, id)
	if err == nil && exists {
		return nil
	}
	if err := cat.CreateNamespace(ctx, id, nil); err != nil {
		if errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
			return nil
		}
		return fmt.Errorf("lakekeeper: create namespace %s: %w", ns, err)
	}
	return nil
}

// catalogClient is the iceberg-go REST catalog surface ensureNamespace
// touches. Defined as an interface here so the package's tests can
// inject a stub without sprouting an iceberg-go dep at the test
// boundary.
type catalogClient interface {
	CheckNamespaceExists(ctx context.Context, ns icebergtable.Identifier) (bool, error)
	CreateNamespace(ctx context.Context, ns icebergtable.Identifier, props iceberg.Properties) error
}

// The single per-run token in r.Token covers every lakekeeper call;
// authz happens server-side via FGA against the synthetic identity in
// the JWT's `sub` claim.
