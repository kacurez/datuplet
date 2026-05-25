// Package icebergjob is the thin orchestrator that commits a run's
// per-table parquet outputs to the Iceberg catalog (lakekeeper).
//
// Iceberg metadata is managed by iceberg-go and lakekeeper; this
// package drives the per-table commit flow:
//
//  1. LoadTable via lakekeeper to recover the iceberg-managed
//     `Table.Location()` (plus an iceberg-go FS handle).
//  2. Derive `<table-base>/.run-state/<runID>/files.json` from the
//     loaded location (FilesManifestPath).
//  3. Read the per-table manifest via the table's own FS handle (so the
//     read uses lakekeeper-vended creds, not a long-lived MinIO mount).
//  4. RetryOnConflict(5) wrapping
//     `txn := tbl.NewTransaction(); txn.AddFiles(paths, nil, false);
//     txn.Commit(ctx)`.
//
// Each (namespace, table) gets its own files.json under the table's
// iceberg-managed prefix — the same prefix DG's vended-creds STS scope
// covers. The Execute() loop supports one TableCommit pod committing N
// tables.
package icebergjob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	"github.com/datuplet/datuplet/pkg/datagateway/jwks"
	runtokenpkg "github.com/datuplet/datuplet/pkg/datagateway/runtoken"

	// Blank-import the centralised iceberg-go IO scheme registration
	// package so the `gs://` factory is the Datuplet override whenever
	// this commit path runs (iceberg-job pod). See pkg/datupleticeio/doc.go
	// and RFC 019 §4.5.
	_ "github.com/datuplet/datuplet/pkg/datupleticeio"
)

// jsonUnmarshalStrict decodes body into out via stdlib json.Unmarshal.
// Wrapped here so a future need to swap in a strict (DisallowUnknownFields)
// path lands in one place.
func jsonUnmarshalStrict(body []byte, out any) error {
	return json.Unmarshal(body, out)
}

// WriteMode specifies how data should be written to the table.
//
// Both APPEND and FULL_LOAD are implemented end-to-end against
// lakekeeper: APPEND uses iceberg-go's `txn.AddFiles`, FULL_LOAD uses
// `txn.ReplaceDataFiles`. The `CommitTable` function in commit_shared.go
// owns the actual dispatch.
//
// UPSERT is a future addition and would extend this type plus the
// files.json schema (delete_paths) without changing CommitTable's
// locked signature.
type WriteMode string

const (
	// WriteModeAppend adds new data files to the table.
	WriteModeAppend WriteMode = "APPEND"
	// WriteModeFullLoad replaces all existing data in the table —
	// implemented via iceberg-go's `txn.ReplaceDataFiles` against
	// the table's current snapshot.
	WriteModeFullLoad WriteMode = "FULL_LOAD"
)

// Config configures the TableCommit job.
//
// Per-table manifests live inside each table's iceberg-managed prefix
// (the same prefix DG's vended-creds STS scope covers). Reads go
// through iceberg-go's per-table FS handle (lakekeeper-vended creds).
// Long-lived S3 credentials are not accepted — the commit Job uses only
// the run-token JWT and lakekeeper-vended creds.
type Config struct {
	// RunID is the unique run identifier. Used to locate
	// `<table-base>/.run-state/<runID>/files.json`.
	RunID string

	// LakekeeperURL is the catalog REST base URL (e.g.
	// http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog).
	LakekeeperURL string

	// Tables narrows what gets committed. Each entry is a
	// (namespace, table) pair — when empty, the orchestrator discovers
	// every table in lakekeeper's `Namespace` (operator flow) and
	// commits each one.
	Tables []TableConfig

	// Namespace optionally restricts auto-discovery to a single
	// namespace (DG's "bucket"). When set + Tables empty, the
	// orchestrator queries lakekeeper for every table under this
	// namespace and commits each in turn. Ignored when Tables is
	// non-empty (explicit list wins).
	Namespace string

	// WriteMode is preserved on results for downstream observability.
	WriteMode WriteMode

	// RunTokenPath is the filesystem path to the projected single per-run
	// JWT (raw string, no JSON wrapping). Empty path = unauthenticated
	// (dev/test mode only). The run-token + lakekeeper-vended creds are
	// the only credential path; no long-lived S3 credentials are used.
	RunTokenPath string

	// PipelineAPIJWKSURL is the JWKS endpoint pipeline-api serves. Required
	// whenever RunTokenPath is set; the binary validates the mounted JWT
	// against this URL. Empty disables validation — only acceptable when
	// RunTokenPath is also empty (dev paths against an allowall lakekeeper).
	PipelineAPIJWKSURL string
}

// TableConfig identifies one table to commit in table-commit mode.
type TableConfig struct {
	// Namespace is the Iceberg namespace (DG's "bucket").
	Namespace string
	// Table is the table name within the namespace.
	Table string
	// WriteMode is preserved on the result for downstream observability.
	// See WriteMode for the current semantics.
	WriteMode WriteMode
}

// Result contains the result of a TableCommit operation.
type Result struct {
	Success    bool
	CommitMode WriteMode
	Tables     []TableResult
	Error      string
}

// TableResult contains the result for a single table commit.
type TableResult struct {
	Namespace  string
	Table      string
	Success    bool
	FilesAdded int
	// Empty when the manifest entry was absent or empty (nothing to
	// commit). Populated with the catalog identifier ("ns.table") on
	// success so operator logs can correlate.
	Identifier string
	Error      string
}

// TableCommitter commits write sessions to Iceberg tables via lakekeeper.
type TableCommitter struct {
	config *Config

	// validatedClaims holds the JWT claims validated at Execute time.
	// Non-nil when the run-token was successfully validated against
	// pipeline-api's JWKS. warehouseForCall / projectIDForCall read from
	// this field; dev-mode paths leave it nil.
	validatedClaims *runtokenpkg.ValidatedClaims
}

// New creates a new TableCommitter. Validates required fields up front
// so the operator surfaces misconfiguration loudly rather than via a
// downstream lakekeeper or storage error. Tables/Namespace may both be
// empty — in that case Execute discovers every table the catalog can
// see.
func New(config *Config) (*TableCommitter, error) {
	if config == nil {
		return nil, errors.New("icebergjob: config is required")
	}
	if config.RunID == "" {
		return nil, errors.New("icebergjob: run_id is required")
	}
	if config.LakekeeperURL == "" {
		return nil, errors.New("icebergjob: lakekeeper_url is required")
	}
	for i, t := range config.Tables {
		if t.Namespace == "" || t.Table == "" {
			return nil, fmt.Errorf("icebergjob: table[%d] must have namespace + table", i)
		}
	}
	return &TableCommitter{config: config}, nil
}

// Execute commits every table requested. When config.Tables is empty
// the orchestrator discovers tables via lakekeeper (optionally
// filtered to config.Namespace). Errors at the per-table level are
// recorded on the per-table TableResult; only catastrophic errors
// (token map parse failure, catalog reach failure during discovery)
// abort the loop. The overall Result.Success is the AND of per-table
// successes.
func (c *TableCommitter) Execute(ctx context.Context) (*Result, error) {
	// Validate the mounted run-token JWT against pipeline-api's JWKS, then
	// read warehouse + project_id claims for lakekeeper routing. Fail-closed
	// if RunTokenPath is set but PipelineAPIJWKSURL is not — that is a
	// misconfiguration. Dev paths without a mounted JWT (RunTokenPath == "")
	// skip validation.
	var (
		tok       string
		validated *runtokenpkg.ValidatedClaims
	)
	if c.config.RunTokenPath != "" {
		if c.config.PipelineAPIJWKSURL == "" {
			msg := "RunTokenPath set but PipelineAPIJWKSURL empty — refusing to commit without JWT validation"
			return &Result{Success: false, Error: msg}, errors.New(msg)
		}
		expectedRunID := os.Getenv("RUN_ID")
		if expectedRunID == "" {
			msg := "RUN_ID env not set — required for Secret-swap defence"
			return &Result{Success: false, Error: msg}, errors.New(msg)
		}
		jwksClient := jwks.NewClient(c.config.PipelineAPIJWKSURL, nil)
		claims, err := runtokenpkg.LoadAndValidateRunToken(ctx, c.config.RunTokenPath, jwksClient, expectedRunID)
		if err != nil {
			return &Result{Success: false, Error: fmt.Sprintf("run-token validation failed: %v", err)},
				fmt.Errorf("run-token validation failed: %w", err)
		}
		log.Printf("table-commit: run-token validated; routing to warehouse=%q project_id=%q", claims.Warehouse, claims.ProjectID)
		validated = claims
		c.validatedClaims = validated
		// Keep the raw JWT for outbound Bearer headers on lakekeeper calls.
		raw, rerr := readBoundedFile(c.config.RunTokenPath)
		if rerr != nil {
			return &Result{Success: false, Error: fmt.Sprintf("re-read validated token: %v", rerr)},
				fmt.Errorf("re-read validated token: %w", rerr)
		}
		tok = strings.TrimSpace(string(raw))
	}

	targets, err := c.resolveTargets(ctx, tok)
	if err != nil {
		return &Result{
			Success: false,
			Error:   fmt.Sprintf("resolve targets: %v", err),
		}, fmt.Errorf("resolve targets: %w", err)
	}

	overallMode := c.config.WriteMode
	if overallMode == "" {
		overallMode = WriteModeAppend
	}
	results := make([]TableResult, 0, len(targets))
	allOK := true
	for _, tc := range targets {
		if tc.WriteMode != "" {
			overallMode = tc.WriteMode
		}
		r := c.commitTable(ctx, tc, tok)
		results = append(results, r)
		if !r.Success {
			allOK = false
		}
	}

	return &Result{
		Success:    allOK,
		CommitMode: overallMode,
		Tables:     results,
	}, nil
}

// warehouseForCall returns the warehouse to use in lakekeeper catalog calls.
// The validated JWT claim is the sole source. Returns empty string in dev
// paths (no JWT) — lakekeeper will error if a warehouse is required but
// absent, surfacing the misconfiguration clearly.
func (c *TableCommitter) warehouseForCall() string {
	if c.validatedClaims != nil {
		return c.validatedClaims.Warehouse
	}
	return ""
}

// projectIDForCall returns the lakekeeper project ID to forward as
// x-project-id on catalog calls. The validated JWT claim is the sole
// source. Returns empty string in dev paths (no JWT) — single-project
// deploys (LAKEKEEPER__ENABLE_DEFAULT_PROJECT=true) still work.
func (c *TableCommitter) projectIDForCall() string {
	if c.validatedClaims != nil {
		return c.validatedClaims.ProjectID
	}
	return ""
}

// resolveTargets returns the (namespace, table, write-mode) triples to
// commit. Explicit Tables list wins; otherwise we ask lakekeeper for
// every table under config.Namespace (or every table in every namespace
// when config.Namespace is empty).
//
// Every catalog call uses the same per-run JWT.
func (c *TableCommitter) resolveTargets(ctx context.Context, tok string) ([]TableConfig, error) {
	if len(c.config.Tables) > 0 {
		return c.config.Tables, nil
	}

	cli, err := newDiscoveryClient(ctx, c, tok)
	if err != nil {
		return nil, fmt.Errorf("new catalog client for discovery: %w", err)
	}

	var nss []icebergtable.Identifier
	if c.config.Namespace != "" {
		nss = []icebergtable.Identifier{catalog.ToIdentifier(c.config.Namespace)}
	} else {
		var listErr error
		nss, listErr = cli.Catalog.ListNamespaces(ctx, nil)
		if listErr != nil {
			return nil, fmt.Errorf("list namespaces: %w", listErr)
		}
	}

	var out []TableConfig
	for _, ns := range nss {
		for ident, tErr := range cli.Catalog.ListTables(ctx, ns) {
			if tErr != nil {
				return nil, fmt.Errorf("list tables in %v: %w", ns, tErr)
			}
			out = append(out, TableConfig{
				Namespace: nsString(ident),
				Table:     ident[len(ident)-1],
				WriteMode: c.config.WriteMode,
			})
		}
	}
	return out, nil
}

// nsString flattens a multi-segment namespace identifier (everything
// except the last element) into "."-joined form. Mirrors what
// pkg/pipelineapi/storage/joinNS produces; kept local to avoid pulling
// the storage package into tablecommit's import graph.
func nsString(ident icebergtable.Identifier) string {
	if len(ident) <= 1 {
		return ""
	}
	out := ident[0]
	for _, s := range ident[1 : len(ident)-1] {
		out += "." + s
	}
	return out
}

// newDiscoveryClient opens a catalogwriter.Client used solely for the
// ListNamespaces / ListTables calls during target discovery.
// warehouse + project_id come from the validated JWT claims
// (via warehouseForCall / projectIDForCall); dev-mode paths leave
// these empty.
func newDiscoveryClient(ctx context.Context, c *TableCommitter, token string) (*catalogwriter.Client, error) {
	tp := func(context.Context) (string, error) { return token, nil }
	return catalogwriter.NewClient(ctx, catalogwriter.ClientConfig{
		Name:          "datuplet-icebergjob-discovery",
		URI:           c.config.LakekeeperURL,
		Warehouse:     c.warehouseForCall(),
		TokenProvider: tp,
		ProjectID:     c.projectIDForCall(),
	})
}

// catalogOps is the narrow surface of iceberg-go's catalog interface
// the orchestrator actually touches. Defined here so tests can stub it
// without standing up a lakekeeper.
type catalogOps interface {
	CheckNamespaceExists(ctx context.Context, ns icebergtable.Identifier) (bool, error)
	CreateNamespace(ctx context.Context, ns icebergtable.Identifier, props iceberg.Properties) error
	LoadTable(ctx context.Context, id icebergtable.Identifier) (*icebergtable.Table, error)
	CreateTable(ctx context.Context, id icebergtable.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*icebergtable.Table, error)
}

// Compile-time interface assertion: a future iceberg-go signature drift
// (e.g. an extra options arg added to LoadTable) lights up this file
// rather than a runtime "method not found" surprise inside a commit.
// Cheap insurance — costs nothing and makes upgrades visible.
var _ catalogOps = (*rest.Catalog)(nil)

// commitFunc abstracts the actual `txn.AddFiles + txn.Commit` step so
// tests can substitute a stub that returns
// `rest.ErrCommitFailed` on the first N calls (exercising the
// retry-on-409 contract without standing up a lakekeeper).
//
// Production callers no longer use this seam — `commitTable` (the
// per-table arm of Execute) now delegates straight to the shared
// `CommitTable` function in commit_shared.go, which owns the
// AddFiles/ReplaceDataFiles dispatch and the RetryOnConflict envelope.
// `commitFunc` remains as the test-injection point used by `commitOne`
// for the LoadTable/CreateTable race + 409-retry tests.
type commitFunc func(ctx context.Context, cat catalogOps, ns, table string, paths []string) error

// commitTable is the per-table arm of Execute. Each step has at most
// one external dependency so failures map cleanly to a single error
// message.
//
// Flow:
//   - LoadTable for the (ns, tbl) — the table MUST exist (DG created
//     it on first write). If it doesn't, the run produced no parquet
//     for this target and TableCommit treats that as success-zero.
//   - Derive the per-table manifest path from the loaded
//     `Table.Location()` and read it through iceberg-go's FS (so the
//     read uses lakekeeper-vended creds, not a long-lived MinIO mount).
//   - Empty paths → success-zero. Otherwise AddFiles + Commit with the
//     standard 409 retry envelope.
func (c *TableCommitter) commitTable(ctx context.Context, tc TableConfig, tok string) TableResult {
	r := TableResult{Namespace: tc.Namespace, Table: tc.Table}

	// One JWT per run; catalogwriter ships it as Bearer on every catalog call.
	// warehouse + project_id come from the validated JWT claims when
	// validation ran at Execute time.
	tokenProvider := func(context.Context) (string, error) { return tok, nil }
	cli, err := catalogwriter.NewClient(ctx, catalogwriter.ClientConfig{
		Name:          "datuplet-icebergjob",
		URI:           c.config.LakekeeperURL,
		Warehouse:     c.warehouseForCall(),
		TokenProvider: tokenProvider,
		ProjectID:     c.projectIDForCall(),
	})
	if err != nil {
		r.Error = fmt.Sprintf("new catalog client: %v", err)
		return r
	}

	ident := catalog.ToIdentifier(tc.Namespace, tc.Table)

	// LoadTable up front: the manifest lives under the table's
	// iceberg-managed prefix, so we MUST resolve the table before reading
	// the manifest. ErrNoSuchTable is treated as success-zero (the run
	// produced no parquet for this target — otherwise DG would have
	// created the table on first write).
	//
	// Done here (rather than letting CommitTable load) so we can apply
	// the table-commit-mode-specific success-zero policy on a missing
	// table (a future caller that pre-creates a target table shell
	// would treat the same case as a real error instead).
	tbl, err := cli.Catalog.LoadTable(ctx, ident)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			r.Success = true
			return r
		}
		r.Error = fmt.Sprintf("load table %s.%s: %v", tc.Namespace, tc.Table, err)
		return r
	}

	manifestPath := FilesManifestPath(tbl.Location(), c.config.RunID)
	if manifestPath == "" {
		r.Error = fmt.Sprintf("derive manifest path: empty (location=%q runID=%q)", tbl.Location(), c.config.RunID)
		return r
	}

	mode := tc.WriteMode
	if mode == "" {
		mode = c.config.WriteMode
	}
	if mode == "" {
		mode = WriteModeAppend
	}

	// Build audit snapshot properties from the run-token JWT claims.
	// ParseUnverified is intentional — lakekeeper already validated the
	// signature on every REST call before we got here. tok may be empty
	// in dev/test mode; BuildSnapshotSummary returns nil in that case
	// and iceberg writes no datuplet.* keys.
	snapshotProps := BuildSnapshotSummary(tok, c.config.RunID)

	// CommitTable is the shared entry point. It owns the manifest read
	// (via tbl.FS() vended creds), the AddFiles/ReplaceDataFiles dispatch,
	// the Commit, and the RetryOnConflict envelope.
	res, err := CommitTable(ctx, cli.Catalog, ident, manifestPath, mode, snapshotProps)
	if err != nil {
		r.Error = fmt.Sprintf("commit table: %v", err)
		return r
	}

	r.Identifier = tc.Namespace + "." + tc.Table
	r.Success = true
	if res != nil {
		r.FilesAdded = res.DataFilesAdded
	}
	return r
}

// commitOne is the catalog-bound half of commitTable, factored out so
// tests can drive it with a stub catalogOps + stub commitFunc.
// commitOne assumes the table already exists — the LoadTable in
// commitTable runs first. The legacy loadOrCreateTable path stays here
// for test fixtures that drive commitOne directly without the LoadTable
// prologue.
func (c *TableCommitter) commitOne(ctx context.Context, cat catalogOps, commit commitFunc, tc TableConfig, paths []string) TableResult {
	r := TableResult{Namespace: tc.Namespace, Table: tc.Table}

	if _, err := loadOrCreateTable(ctx, cat, tc.Namespace, tc.Table); err != nil {
		r.Error = fmt.Sprintf("load or create table: %v", err)
		return r
	}
	r.Identifier = tc.Namespace + "." + tc.Table

	// Retry-on-409 around the actual append. iceberg-go surfaces a
	// 409 from lakekeeper as `rest.ErrCommitFailed`; catalogwriter
	// detects + retries those with bounded backoff.
	if err := catalogwriter.RetryOnConflict(ctx, catalogwriter.RetryOpts{}, func(ctx context.Context) error {
		return commit(ctx, cat, tc.Namespace, tc.Table, paths)
	}); err != nil {
		r.Error = fmt.Sprintf("commit transaction: %v", err)
		return r
	}

	r.Success = true
	r.FilesAdded = len(paths)
	return r
}

// Manifest reads happen inside CommitTable in commit_shared.go, which
// uses the per-table FS handle from iceberg-go and supports both v1 +
// v2 of the files.json schema. parseTableManifest below stays for the
// legacy FilesManifest decode test, which still asserts the v1 wire shape.

// isNotFoundErr is a best-effort detector for "object not found" errors
// surfaced by iceberg-go's FS abstraction. The iceberg-go library
// doesn't expose a sentinel for s3 NoSuchKey or filesystem
// os.ErrNotExist — both bubble up as wrapped errors with provider-
// specific messages. We match on substring rather than type because
// that's what the surface gives us today; if iceberg-go grows a real
// sentinel in the future this is the one place to switch over.
//
// Backends seen in the wild:
//   - s3:        `NoSuchKey`, `404`, "key does not exist"
//   - GCS:       "object doesn't exist" (note the contraction — doesn't,
//                so `strings.Contains(msg, "not exist")` does NOT match),
//                and a gocloud `(code=NotFound)` suffix on the wrapped error
//   - localfs:   "no such file or directory"
//   - iceberg-go's own paths: "not found"
func isNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not exist") ||
		strings.Contains(msg, "doesn't exist") || // GCS via gocloud.dev
		strings.Contains(msg, "code=notfound") || // gocloud.dev wrapping
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "nosuchkey") ||
		strings.Contains(msg, "404") ||
		strings.Contains(msg, "no such file")
}

// parseTableManifest decodes the per-table manifest body into a
// FilesManifest struct. Kept private because Execute()-level code only
// cares about the Paths field; the rest of the wire shape is internal.
func parseTableManifest(body []byte) (*FilesManifest, error) {
	doc := &FilesManifest{}
	if err := jsonUnmarshalStrict(body, doc); err != nil {
		return nil, err
	}
	return doc, nil
}

// loadOrCreateTable looks up (ns, tbl) in the catalog. If LoadTable returns
// ErrNoSuchTable the table does not exist yet — schema inference is not
// supported. DG must call CreateTable on first write; TableCommit only
// appends to tables that already exist.
//
// Other LoadTable errors propagate verbatim so the operator sees the true cause.
func loadOrCreateTable(ctx context.Context, cat catalogOps, ns, table string) (*icebergtable.Table, error) {
	if err := ensureNamespace(ctx, cat, ns); err != nil {
		return nil, err
	}

	ident := catalog.ToIdentifier(ns, table)
	tbl, err := cat.LoadTable(ctx, ident)
	if err == nil {
		return tbl, nil
	}
	if !errors.Is(err, catalog.ErrNoSuchTable) {
		return nil, fmt.Errorf("load table %s.%s: %w", ns, table, err)
	}

	// Schema inference is not supported. Cold-start tables (tables that DG
	// has not yet created via lakekeeper) fail loudly here. DG must call
	// CreateTable on first write.
	return nil, fmt.Errorf("table %s.%s not found in catalog (DG must call CreateTable on first write)", ns, table)
}

// ensureNamespace creates the namespace if it doesn't exist. Idempotent:
// ErrNamespaceAlreadyExists is squashed.
func ensureNamespace(ctx context.Context, cat catalogOps, ns string) error {
	id := catalog.ToIdentifier(ns)
	exists, err := cat.CheckNamespaceExists(ctx, id)
	if err == nil && exists {
		return nil
	}
	// CheckNamespaceExists may surface transport errors that mask the
	// "exists" case; fall through to CreateNamespace which is the
	// authoritative idempotent path.
	if err := cat.CreateNamespace(ctx, id, nil); err != nil {
		if errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
			return nil
		}
		return fmt.Errorf("create namespace %s: %w", ns, err)
	}
	return nil
}

