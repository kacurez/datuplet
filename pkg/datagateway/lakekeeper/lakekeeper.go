// Package lakekeeper wires DataGateway's per-table parquet writes
// directly to lakekeeper-vended STS credentials. Lakekeeper is the
// catalog of record.
//
// The package exists primarily as a glue layer between three already-
// existing pieces:
//
//   - `pkg/catalogwriter` — Client (REST catalog), VendedCreds (STS
//     fetch + 50%-elapsed renewal contract), AWSCredentialsProvider.
//   - `pkg/datagateway/backend` — the StorageBackend abstraction the
//     buffer manager writes through.
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
//  3. Construct a vended-creds-backed MinIO backend rooted at the
//     table's data path (per iceberg-go's LocationProvider).
//
// Reads use Resolver.LoadTableForRead which returns the parquet path
// list under the current snapshot plus a similar minio backend. The
// run-token map provides the read-intent JWT.
//
// The package is deliberately small: production logic lives in the
// callers (server_v2_writing.go, server_v2_reading.go). This file
// only owns the catalog-and-creds plumbing.
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
	"github.com/datuplet/datuplet/pkg/datagateway/backend"
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

// Resolver translates (namespace, table) into a writable / readable
// per-table location plus a backend.StorageBackend.
//
// Lakekeeper REST + vended-creds STS is the only supported catalog backend.
//
// One Resolver instance is created per gateway (cheap; just config),
// then shared across writers/readers. The actual catalog client is
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
	// BasePath is the s3:// URL the BufferManager should write parquet
	// files under (e.g. s3://warehouse/<warehouse-uuid>/<table-uuid>/data/).
	// The trailing slash is included so joinStoragePath builds a clean
	// child URL.
	BasePath string

	// Backend wraps a minio-go client whose credentials are sourced from
	// `pkg/catalogwriter.VendedCreds` — every PutObject call retrieves a
	// fresh-or-cached STS credential pair via the renewal contract.
	Backend backend.StorageBackend

	// VendedCreds is held so the caller can poll LastError() for
	// observability, and so renewals stay coordinated through one cache.
	VendedCreds *catalogwriter.VendedCreds
}

// ReaderTarget describes a readable per-table snapshot.
type ReaderTarget struct {
	// DataFiles is the list of parquet paths under the current snapshot.
	// Always s3:// or file:// URLs; empty when the table has no data.
	DataFiles []string

	// SchemaJSON is the iceberg-go schema serialised to JSON. Empty when
	// the table has no current schema (shouldn't happen in practice —
	// CreateTable always sets one).
	SchemaJSON []byte

	// TotalRows is the sum of record counts across DataFiles.
	TotalRows int64

	// Backend reads parquet via vended STS creds, same shape as
	// WriterTarget.Backend.
	Backend backend.StorageBackend
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

	be, vc, err := r.buildBackend(dataPrefix, ns, tbl)
	if err != nil {
		return nil, err
	}
	return &WriterTarget{
		BasePath:    dataPrefix,
		Backend:     be,
		VendedCreds: vc,
	}, nil
}

// LoadTableForRead returns the parquet paths in the current snapshot
// plus a vended-creds-backed backend the caller can read them through.
// On a cold-start table (no current snapshot) DataFiles is empty.
func (r *Resolver) LoadTableForRead(ctx context.Context, ns, tbl string) (*ReaderTarget, error) {
	cli, err := r.newClient(ctx, r.Token)
	if err != nil {
		return nil, fmt.Errorf("lakekeeper: new client: %w", err)
	}
	t, err := cli.Catalog.LoadTable(ctx, catalog.ToIdentifier(ns, tbl))
	if err != nil {
		return nil, fmt.Errorf("lakekeeper: load %s.%s for read: %w", ns, tbl, err)
	}

	// SchemaJSON: marshal the iceberg-go schema as JSON so the existing
	// gateway-side parser (parseIcebergSchemaJSON) can consume it
	// without changing its shape.
	var schemaBytes []byte
	if sch := t.Schema(); sch != nil {
		schemaBytes, _ = sch.MarshalJSON()
	}

	// Walk the current snapshot's manifests for data file paths +
	// total row count. Empty when the table is brand-new (no snapshot
	// yet) — callers handle that as "no rows to read".
	var dataFiles []string
	var totalRows int64
	if cur := t.CurrentSnapshot(); cur != nil {
		fs, fsErr := t.FS(ctx)
		if fsErr != nil {
			return nil, fmt.Errorf("lakekeeper: open table FS for %s.%s: %w", ns, tbl, fsErr)
		}
		manifests, mErr := cur.Manifests(fs)
		if mErr != nil {
			return nil, fmt.Errorf("lakekeeper: list manifests for %s.%s: %w", ns, tbl, mErr)
		}
		for _, m := range manifests {
			entries, eErr := m.FetchEntries(fs, true)
			if eErr != nil {
				return nil, fmt.Errorf("lakekeeper: fetch manifest entries for %s.%s: %w", ns, tbl, eErr)
			}
			for _, e := range entries {
				df := e.DataFile()
				if df == nil {
					continue
				}
				dataFiles = append(dataFiles, df.FilePath())
				totalRows += df.Count()
			}
		}
	}

	loc := t.Location()
	if loc == "" {
		return nil, fmt.Errorf("lakekeeper: table %s.%s has empty location", ns, tbl)
	}
	dataPrefix := strings.TrimRight(loc, "/") + "/data/"
	be, _, err := r.buildBackend(dataPrefix, ns, tbl)
	if err != nil {
		return nil, err
	}

	return &ReaderTarget{
		DataFiles:  dataFiles,
		SchemaJSON: schemaBytes,
		TotalRows:  totalRows,
		Backend:    be,
	}, nil
}

// Catalog returns an iceberg catalog handle bound to the resolver's run
// token. A fresh *catalogwriter.Client is opened per call (same pattern
// as LoadTableForRead / LoadOrCreateForWrite) — there is no shared
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

// buildBackend dispatches to the scheme-appropriate backend constructor.
// Supported schemes: s3://, gs://, file://. Unknown schemes return an
// "unsupported scheme" error that names all three supported variants so
// operators can diagnose a misconfigured lakekeeper warehouse quickly.
func (r *Resolver) buildBackend(dataPrefix, ns, tbl string) (backend.StorageBackend, *catalogwriter.VendedCreds, error) {
	switch {
	case strings.HasPrefix(dataPrefix, "file://"):
		return backend.NewLocalBackend(backend.LocalConfig{}), nil, nil
	case strings.HasPrefix(dataPrefix, "s3://"):
		return r.buildS3Backend(dataPrefix, ns, tbl)
	case strings.HasPrefix(dataPrefix, "gs://"):
		return r.buildGCSBackend(dataPrefix, ns, tbl)
	default:
		return nil, nil, fmt.Errorf("lakekeeper: unsupported scheme in %q (need s3://, gs://, or file://)", dataPrefix)
	}
}

// buildS3Backend constructs a backend.StorageBackend for `s3://...`
// data prefixes by wiring `pkg/catalogwriter.VendedCreds` into a
// minio-go credentials provider. Returns the backend, the vendedCreds
// (so callers can observe LastError), and any setup error.
func (r *Resolver) buildS3Backend(dataPrefix, ns, tbl string) (backend.StorageBackend, *catalogwriter.VendedCreds, error) {
	tok := r.Token
	tp := func(context.Context) (string, error) { return tok, nil }
	// VendedCreds discovers the per-warehouse REST URL prefix lazily on
	// first fetch via `GET /v1/config?warehouse=<name>` (matching
	// iceberg-go's REST catalog handshake). The data-path UUID embedded
	// in `dataPrefix` is NOT necessarily the warehouse-id used in the
	// REST URL — lakekeeper assigns separate identifiers — so deriving
	// prefix from the path string is unsafe.
	vc := &catalogwriter.VendedCreds{
		LakekeeperURL:     r.URL,
		WarehouseName:     r.Warehouse,
		ProjectID:         r.ProjectID,
		Namespace:         ns,
		Table:             tbl,
		TokenProvider:     tp,
		ExpectedCredsType: catalogwriter.CredsTypeS3,
	}

	// Bucket name comes from the table prefix. iceberg-go writes the
	// scheme as `s3://<bucket>/<warehouse-uuid>/...`; everything after
	// the bucket lands in the object key, so the backend itself only
	// needs the bucket name.
	rest := strings.TrimPrefix(dataPrefix, "s3://")
	parts := strings.SplitN(rest, "/", 2)
	bucket := parts[0]
	if bucket == "" {
		return nil, nil, fmt.Errorf("lakekeeper: cannot derive bucket from %q", dataPrefix)
	}

	be, err := backend.NewMinIOBackendWithProvider(backend.MinIOProviderConfig{
		Bucket:      bucket,
		VendedCreds: vc,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("lakekeeper: build vended-creds minio backend: %w", err)
	}
	return be, vc, nil
}

// buildGCSBackend mirrors buildS3Backend for `gs://...` data prefixes,
// wiring lakekeeper-vended GCS credentials into a GCS storage backend.
// Returns the backend, the VendedCreds (for observability), and any setup error.
func (r *Resolver) buildGCSBackend(dataPrefix, ns, tbl string) (backend.StorageBackend, *catalogwriter.VendedCreds, error) {
	tok := r.Token
	tp := func(context.Context) (string, error) { return tok, nil }
	vc := &catalogwriter.VendedCreds{
		LakekeeperURL:     r.URL,
		WarehouseName:     r.Warehouse,
		ProjectID:         r.ProjectID,
		Namespace:         ns,
		Table:             tbl,
		TokenProvider:     tp,
		ExpectedCredsType: catalogwriter.CredsTypeGCS,
	}

	// Bucket name comes from the table prefix. lakekeeper writes the
	// scheme as `gs://<bucket>/<warehouse-uuid>/...`; only the bucket
	// name is needed by the backend.
	rest := strings.TrimPrefix(dataPrefix, "gs://")
	parts := strings.SplitN(rest, "/", 2)
	bucket := parts[0]
	if bucket == "" {
		return nil, nil, fmt.Errorf("lakekeeper: cannot derive bucket from %q", dataPrefix)
	}

	be, err := backend.NewGCSBackendWithProvider(backend.GCSProviderConfig{
		Bucket:      bucket,
		VendedCreds: vc,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("lakekeeper: build vended-creds gcs backend: %w", err)
	}
	return be, vc, nil
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
