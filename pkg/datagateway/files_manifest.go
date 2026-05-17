// Package datagateway — files.json manifest emission.
//
// At end-of-stream the gateway writes ONE manifest per (namespace, table)
// the run touched. Each manifest lists the parquet files DG produced for
// that table; TableCommit consumes it via iceberg-go's
// `txn.AddFiles(paths, nil, false)` (the catalog re-reads the parquet
// footers and extracts column statistics on its side, so the manifest
// itself only carries paths — no DataFileRecord stats).
//
// Manifest placement:
//
//	<table-base>/.run-state/<run-id>/files.json
//
// `<table-base>` is the iceberg-managed table prefix that lakekeeper
// allocates (e.g. `s3://<bucket>/<storage-uuid>/<table-uuid>/`) — the
// SAME prefix the per-table vended-creds STS scope covers. An earlier
// design wrote a single aggregate manifest at `<warehouse>/.run-state/...`,
// which sat OUTSIDE every per-table STS scope and therefore failed with
// 403 once vended creds were enabled. Per-table placement keeps
// the write inside the credential scope DG already holds.
//
// The `.run-state/` prefix keeps the manifest out of any catalog-tracked
// space (so iceberg-go list/preview won't see it as table metadata).
package datagateway

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/datuplet/datuplet/pkg/datagateway/backend"
)

// FilesManifest accumulates the parquet file paths a DataGateway run
// produced, grouped by (namespace, table). It is safe for concurrent
// use: Append may be called from any number of writer goroutines while
// the run is streaming, and WriteJSONForTable serialises a per-table
// snapshot on shutdown.
type FilesManifest struct {
	runID string

	mu     sync.Mutex
	tables map[string]*tableFiles // key = "<namespace>.<table>"
}

// tableFiles is the per-table accumulator inside FilesManifest.
type tableFiles struct {
	Namespace string
	Table     string
	Paths     []string
}

// TableFiles is the per-table entry exposed for snapshot/iteration. It
// is the same shape as the on-disk JSON document (one manifest file per table).
type TableFiles struct {
	Namespace string   `json:"namespace"`
	Table     string   `json:"table"`
	Paths     []string `json:"paths"`
}

// tableManifestJSON is the on-the-wire shape of the per-table manifest.
// One manifest blob = one (namespace, table); no `tables` array.
// Consumed by TableCommit via iceberg-go's txn.AddFiles.
type tableManifestJSON struct {
	RunID     string   `json:"run_id"`
	Namespace string   `json:"namespace"`
	Table     string   `json:"table"`
	Paths     []string `json:"paths"`
}

// NewFilesManifest constructs an empty manifest for the given run.
func NewFilesManifest(runID string) *FilesManifest {
	return &FilesManifest{
		runID:  runID,
		tables: make(map[string]*tableFiles),
	}
}

// RunID returns the run identifier the manifest was constructed with.
func (m *FilesManifest) RunID() string { return m.runID }

// Append records a single parquet file write. namespace == bucket,
// table == table name. path is the fully-qualified URL the file was written
// to (passed verbatim to TableCommit later).
//
// Safe for concurrent use. Within a (namespace, table) bucket the call
// order is preserved so the resulting manifest is deterministic for a
// fixed call sequence. Calling Append on a nil receiver is a no-op
// — that lets older test fixtures that construct ServerV2 by hand
// (rather than through NewServerV2) keep working without surfacing a
// nil-pointer panic from the production code path.
func (m *FilesManifest) Append(namespace, table, path string) {
	if m == nil || path == "" {
		return
	}
	key := namespace + "." + table
	m.mu.Lock()
	defer m.mu.Unlock()
	tf, ok := m.tables[key]
	if !ok {
		tf = &tableFiles{Namespace: namespace, Table: table}
		m.tables[key] = tf
	}
	tf.Paths = append(tf.Paths, path)
}

// Tables returns a deterministic snapshot of every (namespace, table)
// the manifest has accumulated. The slice and its contents are owned by
// the caller (paths are copied) — safe to iterate and mutate without
// touching the manifest's internal state. Tables come back sorted
// alphabetically by "<ns>.<tbl>" so iteration order matches what the
// pre-Slice-10b aggregate manifest emitted.
func (m *FilesManifest) Tables() []TableFiles {
	m.mu.Lock()
	defer m.mu.Unlock()

	keys := make([]string, 0, len(m.tables))
	for k := range m.tables {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]TableFiles, 0, len(keys))
	for _, k := range keys {
		src := m.tables[k]
		out = append(out, TableFiles{
			Namespace: src.Namespace,
			Table:     src.Table,
			Paths:     append([]string(nil), src.Paths...),
		})
	}
	return out
}

// MarshalTableJSON renders one table's manifest as the wire-shape document.
// Returns (nil, nil, false) when the manifest has no entry for (ns, tbl) —
// the caller treats that as "nothing to write".
func (m *FilesManifest) MarshalTableJSON(namespace, table string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tf, ok := m.tables[namespace+"."+table]
	if !ok {
		return nil, false, nil
	}
	doc := tableManifestJSON{
		RunID:     m.runID,
		Namespace: tf.Namespace,
		Table:     tf.Table,
		Paths:     append([]string(nil), tf.Paths...),
	}
	body, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, false, err
	}
	return body, true, nil
}

// WriteJSONForTable serialises the per-table manifest and uploads it
// via the supplied StorageBackend at manifestPath. The backend must
// already be authorised for that location (DG uses the per-writer
// backend it created from lakekeeper-vended STS creds — those creds
// are scoped to the table's own prefix, which is exactly where the
// manifest is placed).
//
// Returns (false, nil) when there is no entry for (ns, tbl) — caller
// treats that as a no-op and skips the write.
//
// The caller is responsible for picking a manifestPath of the form
// `<table-base>/.run-state/<run-id>/files.json`. See
// ResolveTableManifestPath for the canonical placement.
func (m *FilesManifest) WriteJSONForTable(ctx context.Context, b backend.StorageBackend, namespace, table, manifestPath string) (bool, error) {
	if b == nil {
		return false, fmt.Errorf("files manifest: backend is nil")
	}
	if manifestPath == "" {
		return false, fmt.Errorf("files manifest: empty path")
	}
	body, ok, err := m.MarshalTableJSON(namespace, table)
	if err != nil {
		return false, fmt.Errorf("files manifest: marshal %s.%s: %w", namespace, table, err)
	}
	if !ok {
		return false, nil
	}
	if err := b.PutObject(ctx, manifestPath, body); err != nil {
		return false, fmt.Errorf("files manifest: write %s: %w", manifestPath, err)
	}
	return true, nil
}

// ResolveTableManifestPath derives the per-table manifest URL from the
// table's base path plus the runID.
//
// `tableBase` is the iceberg-managed table prefix as returned by
// lakekeeper / iceberg-go's `Table.Location()`. Common shapes:
//
//   - `s3://<bucket>/<storage-uuid>/<table-uuid>` (production lakekeeper)
//   - `s3://<bucket>/<storage-uuid>/<table-uuid>/` (with trailing /)
//   - `file:///warehouse/<storage-uuid>/<table-uuid>` (local-mode)
//
// The returned path appends `/.run-state/<runID>/files.json`. The
// `.run-state/` segment keeps the manifest out of any catalog-tracked
// space; placing it inside the table prefix keeps the write inside the
// per-table STS scope DG holds.
//
// `basePath` is the path the BufferManager wrote parquet to. DG
// constructs it as `<tableBase>/data/`; we strip the trailing `/data/`
// (or `/data`) to recover `<tableBase>` so callers can pass either. If
// neither suffix is present, we use `basePath` verbatim as the table
// base — the lakekeeper resolver guarantees a sensible shape, and a
// future code path that emits a different layout will fail loudly at
// the next stage rather than silently land in the wrong place.
//
// Returns ("", false) when basePath or runID is empty.
func ResolveTableManifestPath(basePath, runID string) (string, bool) {
	if basePath == "" || runID == "" {
		return "", false
	}
	tableBase := strings.TrimSuffix(basePath, "/")
	tableBase = strings.TrimSuffix(tableBase, "/data")
	return tableBase + "/.run-state/" + runID + "/files.json", true
}
