package storage

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/projectgate"
	"github.com/datuplet/datuplet/pkg/pipelineapi/queryproxy"
)

// EmailLookup resolves a user UUID (as it appears in JWT actor claims
// and iceberg snapshot datuplet.actor keys) into the user's current
// email address. Used by the snapshot history endpoint to populate
// actor_email alongside the canonical actor UUID. The UUID stays the
// stable audit-trail identifier in snapshot metadata; email is a
// display-only convenience that can change between calls if the user
// rotates emails. Empty string return for not-found / DB error.
type EmailLookup interface {
	EmailByID(ctx context.Context, id uuid.UUID) string
}

// HTTPHandlers wires the four /api/v1/storage handlers against a
// Service + the pipeline-api authz seam (Authorizer). The UserResolver
// is applied by the caller via auth.WithUser middleware before each
// request reaches these handlers — so we only need an Authorizer here
// to decide whether the already-authenticated user may access a project.
// Emails (optional — nil means "don't enrich actor_email") resolves the
// snapshot-history actor UUID → email for display.
// Constructed once at server startup; handlers are safe for concurrent
// use.
type HTTPHandlers struct {
	Svc        *Service
	Authorizer authz.Authorizer
	Emails     EmailLookup
	// Gate is the shared project-authz + warehouse prologue (RFC 025 §4.6
	// amendment) — the same instance the query proxy uses.
	Gate *projectgate.Gate
	// Query runs storage previews through the shared query-service core.
	// nil (query service not configured) → preview returns 501 query_disabled.
	Query PreviewRunner
}

// PreviewRunner is the seam Preview uses to run the server-generated
// SELECT ... LIMIT statement through the query-worker (RFC 025 §4.1).
// queryproxy.Core satisfies it; tests use a fake.
type PreviewRunner interface {
	Preview(ctx context.Context, sub, qualifiedWarehouse, ns, table string, lim queryproxy.PreviewLimits) (*queryproxy.Result, *queryproxy.QueryError)
}

// PreviewResponse is the JSON body returned by the preview handler.
// One entry per column in Columns; one slice per row in Rows.
type PreviewResponse struct {
	Columns   []ColumnInfo `json:"columns"`
	Rows      [][]any      `json:"rows"`
	Truncated bool         `json:"truncated"`
}

// ColumnInfo describes one column in a PreviewResponse. Type is the
// DuckDB type name (e.g. "INTEGER", "VARCHAR") as reported by the
// query-worker — not the Iceberg/Arrow type string. This is a
// documented v3 change (RFC 025 Task 3.1): previously Type carried the
// Arrow type string (e.g. "int64", "utf8").
type ColumnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Preview caps. Kept as package-level variables (not const) so tests
// can drive them to small values without building giant fixtures. The
// byte cap stays a var for symmetry.
var (
	previewRowCap  = 100
	previewByteCap = 1 << 20 // 1 MiB
)

// writeJSONResp marshals body to JSON at the given status. Matches the
// pipelineapi/http package's writeJSON helper but lives here so the
// storage package has no upward dep on pkg/pipelineapi/http.
func writeJSONResp(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeErrResp(w http.ResponseWriter, status int, msg string) {
	writeJSONResp(w, status, map[string]string{"error": msg})
}

// resolveProject pulls pid from the URL, validates it, enforces
// FGA datuplet_member authorization via the shared projectgate.Gate, and
// returns the parsed project UUID plus the lakekeeper project UUID (so
// callers don't have to re-resolve it). On any failure it writes the
// appropriate HTTP error and returns ok=false; callers must return
// immediately.
//
// Shared by all four handlers — inlining the same checks in every
// caller would be busier and easier to get wrong than a single helper.
func (h *HTTPHandlers) resolveProject(w http.ResponseWriter, r *http.Request) (projectID uuid.UUID, lkPID string, ok bool) {
	u, authed := auth.UserFromContext(r.Context())
	if !authed {
		writeErrResp(w, http.StatusUnauthorized, "not authenticated")
		return uuid.Nil, "", false
	}
	// Nil-safe: storage routes register on (storage+resolver+authzr) non-nil
	// (server.go), but the gate is built in the lakekeeper/signer wiring
	// block — a signer-less deployment could register these routes with a
	// nil gate. Soft-degrade with 503 instead of panicking.
	if h.Gate == nil {
		writeErrResp(w, http.StatusServiceUnavailable, "storage backend not fully configured")
		return uuid.Nil, "", false
	}
	pid, lk, gerr := h.Gate.Authorize(r.Context(), u.ID.String(), r.PathValue("pid"))
	if gerr != nil {
		writeErrResp(w, gerr.Status, gerr.Msg)
		return uuid.Nil, "", false
	}
	return pid, lk, true
}

// projectURI builds the absolute URI rooted at the project's
// directory: <warehouse>/orgs/<org>/projects/<pid>. Used for
// containment checks — the handler guards against paths that escape
// this root.
func (h *HTTPHandlers) projectURI(pid string) string {
	return joinURI(h.Svc.WarehouseURI, path.Join("orgs", h.Svc.OrgName, "projects", pid))
}

// tableURI appends /tables/<ns>/<name> to the project URI — mirroring
// the layout testdata.ProjectRoot + the walker produce
// (<warehouse>/orgs/<org>/projects/<pid>/tables/<ns>/<name>). Callers
// must have validated ns and name via ValidIdentifier.
func (h *HTTPHandlers) tableURI(pid, ns, name string) string {
	return joinURI(h.projectURI(pid), path.Join("tables", ns, name))
}

// localPathOf strips the file:// prefix from a file:// URI. Returns
// "" for non-file URIs. Used before calling RejectSymlinks because
// RejectSymlinks needs a local absolute path, not a URI.
func localPathOf(uri string) string {
	if !strings.HasPrefix(uri, "file://") {
		return ""
	}
	return strings.TrimPrefix(uri, "file://")
}

// validateTableIdentifiers rejects ns + name with 400 when either
// fails ValidIdentifier.
func validateTableIdentifiers(w http.ResponseWriter, ns, name string) bool {
	if !ValidIdentifier(ns) {
		writeErrResp(w, http.StatusBadRequest, "invalid namespace")
		return false
	}
	if !ValidIdentifier(name) {
		writeErrResp(w, http.StatusBadRequest, "invalid table name")
		return false
	}
	return true
}

// guardTablePath runs the shared containment + (local-only) symlink
// rejection step. Returns false on rejection with a 400 already written.
func (h *HTTPHandlers) guardTablePath(w http.ResponseWriter, tableURI string) bool {
	if !ContainedUnder(h.Svc.WarehouseURI, tableURI) {
		writeErrResp(w, http.StatusBadRequest, "table path escapes warehouse")
		return false
	}
	if h.Svc.AllowLocal {
		if lp := localPathOf(tableURI); lp != "" {
			if err := RejectSymlinks(lp); err != nil {
				writeErrResp(w, http.StatusBadRequest, "table path traverses symlink")
				return false
			}
		}
	}
	return true
}

// resolveWarehouse resolves the bare warehouse name for an authorized
// lakekeeper project. Returns a *projectgate.Error (nil on success) so the
// caller can map status/kind directly.
func (h *HTTPHandlers) resolveWarehouse(ctx context.Context, lkPID string) (string, *projectgate.Error) {
	return h.Gate.Warehouse(ctx, lkPID)
}

// loadRequestedTable runs the full per-table prologue shared by
// TableInfo / TableSchema / Preview: project membership + identifier
// validation + table load. On any failure it writes the HTTP error
// already and returns ok=false; on success ok=true, the Table is
// loaded, and metaURI is the absolute URI of the current metadata.json.
//
// Two backing paths (mirrors ListTables):
//   - LakekeeperURL configured → loadTable via the catalog proxy.
//   - LakekeeperURL empty → fall back to ResolveCurrentMetadata against
//     the directory walker (tests + legacy).
func (h *HTTPHandlers) loadRequestedTable(w http.ResponseWriter, r *http.Request) (tbl *table.Table, metaURI string, ok bool) {
	pid, lkPID, ok := h.resolveProject(w, r)
	if !ok {
		return nil, "", false
	}
	ns := r.PathValue("ns")
	name := r.PathValue("t")
	if !validateTableIdentifiers(w, ns, name) {
		return nil, "", false
	}

	if h.Svc.LakekeeperURL != "" {
		warehouse, gerr := h.resolveWarehouse(r.Context(), lkPID)
		if gerr != nil {
			writeErrResp(w, gerr.Status, gerr.Msg)
			return nil, "", false
		}
		proxy, err := newCatalogProxy(r.Context(), h.Svc, lkPID, warehouse)
		if err != nil {
			writeErrResp(w, http.StatusInternalServerError, "open catalog")
			return nil, "", false
		}
		t, err := proxy.loadTableForRead(r.Context(), ns, name)
		if err != nil {
			writeErrResp(w, http.StatusNotFound, "table not found")
			return nil, "", false
		}
		return t, t.MetadataLocation(), true
	}

	tURI := h.tableURI(pid.String(), ns, name)
	if !h.guardTablePath(w, tURI) {
		return nil, "", false
	}
	dl, err := h.Svc.dataLakeFor()
	if err != nil {
		writeErrResp(w, http.StatusInternalServerError, "storage backend")
		return nil, "", false
	}
	tablePrefix := path.Join("orgs", h.Svc.OrgName, "projects", pid.String(), "tables", ns, name)
	metaURI, err = ResolveCurrentMetadata(r.Context(), dl, h.Svc.WarehouseURI, tablePrefix, h.Svc.S3Props)
	if err != nil {
		if errors.Is(err, ErrNoMetadata) {
			writeErrResp(w, http.StatusNotFound, "table not found")
			return nil, "", false
		}
		writeErrResp(w, http.StatusInternalServerError, "resolve metadata")
		return nil, "", false
	}
	// Defense-in-depth: ResolveCurrentMetadata may have followed a
	// symlinked metadata/ dir or vN.metadata.json in local mode. Re-check
	// the resolved metadata path against the warehouse root + symlink
	// guard so a write-capable user can't redirect reads with a crafted
	// symlink at /tables/{ns}/{t}/metadata/v*.
	if h.Svc.AllowLocal {
		if !ContainedUnder(h.Svc.WarehouseURI, metaURI) {
			writeErrResp(w, http.StatusBadRequest, "metadata path escapes warehouse")
			return nil, "", false
		}
		if lp := localPathOf(metaURI); lp != "" {
			if err := RejectSymlinks(lp); err != nil {
				writeErrResp(w, http.StatusBadRequest, "metadata path traverses symlink")
				return nil, "", false
			}
		}
	}
	tbl, err = table.NewFromLocation(r.Context(),
		table.Identifier{filepath.Base(metaURI)},
		metaURI,
		loaderFn(h.Svc, metaURI),
		nil,
	)
	if err != nil {
		writeErrResp(w, http.StatusInternalServerError, "load table")
		return nil, "", false
	}
	return tbl, metaURI, true
}

// ----- ListTables ----------------------------------------------------

// tableListEntry is one row in the GET /tables response. Kept as a
// wire-shape struct (not iceberg TableRef) so a future internal change
// to the walker's output doesn't leak across the API boundary.
type tableListEntry struct {
	Namespace         string `json:"namespace"`
	Name              string `json:"name"`
	CurrentSnapshotID int64  `json:"current_snapshot_id"`
}

// ListTables handles GET /api/v1/storage/projects/{pid}/tables. Unknown
// projects return 200 with an empty tables array — matching Iceberg
// catalog semantics (listing a namespace that doesn't exist yet is not
// an error), and avoiding enumeration oracles for project IDs the
// membership guard has already passed.
//
// Two backing paths:
//   - When LakekeeperURL is configured (production), the handler proxies
//     ListNamespaces + ListTables through the lakekeeper REST catalog
//     using a service-account JWT.
//   - When LakekeeperURL is empty (tests + legacy fixture warehouses),
//     it falls back to the directory walker.
func (h *HTTPHandlers) ListTables(w http.ResponseWriter, r *http.Request) {
	pid, lkPID, ok := h.resolveProject(w, r)
	if !ok {
		return
	}

	if h.Svc.LakekeeperURL != "" {
		warehouse, gerr := h.resolveWarehouse(r.Context(), lkPID)
		if gerr != nil {
			log.Printf("storage: resolve warehouse (lakekeeper=%s lkPID=%s): %s", h.Svc.LakekeeperURL, lkPID, gerr.Msg)
			writeErrResp(w, gerr.Status, gerr.Msg)
			return
		}
		proxy, err := newCatalogProxy(r.Context(), h.Svc, lkPID, warehouse)
		if err != nil {
			log.Printf("storage: open catalog (lakekeeper=%s warehouse=%s): %v", h.Svc.LakekeeperURL, warehouse, err)
			writeErrResp(w, http.StatusInternalServerError, "open catalog: "+err.Error())
			return
		}
		refs, err := proxy.listAllTables(r.Context())
		if err != nil {
			writeErrResp(w, http.StatusInternalServerError, "list tables")
			return
		}
		out := make([]tableListEntry, 0, len(refs))
		for _, t := range refs {
			out = append(out, tableListEntry{
				Namespace:         t.Namespace,
				Name:              t.Name,
				CurrentSnapshotID: t.CurrentSnapshotID,
			})
		}
		writeJSONResp(w, http.StatusOK, map[string]any{"tables": out})
		return
	}

	dl, err := h.Svc.dataLakeFor()
	if err != nil {
		writeErrResp(w, http.StatusInternalServerError, "storage backend")
		return
	}
	refs, err := ListTables(r.Context(), dl, h.Svc.WarehouseURI, h.Svc.OrgName, pid.String(), h.Svc.S3Props)
	if err != nil {
		writeErrResp(w, http.StatusInternalServerError, "list tables")
		return
	}
	out := make([]tableListEntry, 0, len(refs))
	for _, t := range refs {
		out = append(out, tableListEntry{
			Namespace:         t.Namespace,
			Name:              t.Name,
			CurrentSnapshotID: t.CurrentSnapshotID,
		})
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"tables": out})
}

// ----- TableInfo -----------------------------------------------------

type snapshotBrief struct {
	ID          int64  `json:"id"`
	ParentID    *int64 `json:"parent_id,omitempty"`
	TimestampMS int64  `json:"timestamp_ms"`
	Operation   string `json:"operation,omitempty"`
}

type infoResp struct {
	MetadataLocation  string          `json:"metadata_location"`
	CurrentSnapshotID int64           `json:"current_snapshot_id"`
	Snapshots         []snapshotBrief `json:"snapshots"`
	// RowCount/DataFileCount come from the current snapshot's summary
	// (total-records / total-data-files). nil = summary absent (foreign
	// writer); the UI renders "—". RFC 025 §4.2 replaced the manifest
	// walk this used to do.
	RowCount      *int64 `json:"row_count"`
	DataFileCount *int64 `json:"data_file_count"`
}

// parseSummaryInt parses an Iceberg summary property value ("" = absent).
func parseSummaryInt(v string) (int64, bool) {
	if v == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// TableInfo handles GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/info.
func (h *HTTPHandlers) TableInfo(w http.ResponseWriter, r *http.Request) {
	tbl, metaURI, ok := h.loadRequestedTable(w, r)
	if !ok {
		return
	}

	resp := infoResp{
		MetadataLocation: metaURI,
		Snapshots:        []snapshotBrief{},
	}
	if cur := tbl.CurrentSnapshot(); cur != nil {
		resp.CurrentSnapshotID = cur.SnapshotID
	}
	for _, s := range tbl.Metadata().Snapshots() {
		op := ""
		if s.Summary != nil {
			op = string(s.Summary.Operation)
		}
		resp.Snapshots = append(resp.Snapshots, snapshotBrief{
			ID:          s.SnapshotID,
			ParentID:    s.ParentSnapshotID,
			TimestampMS: s.TimestampMs,
			Operation:   op,
		})
	}

	// Row/file counts come straight from the current snapshot's summary
	// totals — no manifest walk. Absent or unparseable totals (foreign
	// writer that didn't populate them) leave the pointers nil.
	if cur := tbl.CurrentSnapshot(); cur != nil && cur.Summary != nil && cur.Summary.Properties != nil {
		if n, ok := parseSummaryInt(cur.Summary.Properties["total-records"]); ok {
			resp.RowCount = &n
		}
		if n, ok := parseSummaryInt(cur.Summary.Properties["total-data-files"]); ok {
			resp.DataFileCount = &n
		}
	}

	writeJSONResp(w, http.StatusOK, resp)
}

// ----- TableSchema ---------------------------------------------------

type columnInfo struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

type schemaResp struct {
	Columns []columnInfo `json:"columns"`
}

// TableSchema handles GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/schema.
func (h *HTTPHandlers) TableSchema(w http.ResponseWriter, r *http.Request) {
	tbl, _, ok := h.loadRequestedTable(w, r)
	if !ok {
		return
	}
	fields := tbl.Schema().Fields()
	cols := make([]columnInfo, 0, len(fields))
	for _, f := range fields {
		cols = append(cols, columnInfo{
			ID:       f.ID,
			Name:     f.Name,
			Type:     f.Type.String(),
			Nullable: !f.Required,
		})
	}
	writeJSONResp(w, http.StatusOK, schemaResp{Columns: cols})
}

// ----- Preview -------------------------------------------------------

// Preview handles GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/preview.
// The data read runs inside the query-worker sandbox (RFC 025 §4.1);
// pipeline-api never touches parquet bytes. Column types are DuckDB type
// names (e.g. VARCHAR), not Iceberg ones — a documented v3 change.
func (h *HTTPHandlers) Preview(w http.ResponseWriter, r *http.Request) {
	_, lkPID, ok := h.resolveProject(w, r) // shared projectgate prologue (Task 0.2)
	if !ok {
		return
	}
	ns, name := r.PathValue("ns"), r.PathValue("t")
	if !validateTableIdentifiers(w, ns, name) {
		return
	}
	if h.Query == nil {
		writeJSONResp(w, http.StatusNotImplemented, map[string]string{
			"error": "preview requires the query service (queryWorker.enabled=true)",
			"kind":  "query_disabled",
		})
		return
	}
	u, _ := auth.UserFromContext(r.Context()) // resolveProject already 401'd if absent
	warehouse, gerr := h.Gate.Warehouse(r.Context(), lkPID)
	if gerr != nil {
		writeJSONResp(w, gerr.Status, map[string]string{"error": gerr.Msg, "kind": gerr.Kind})
		return
	}
	res, qerr := h.Query.Preview(r.Context(), u.ID.String(), lkPID+"/"+warehouse, ns, name,
		queryproxy.PreviewLimits{TimeoutS: 30, MaxRows: previewRowCap, MaxBytes: previewByteCap})
	if qerr != nil {
		msg := qerr.Msg
		if qerr.Kind == "result_too_large" {
			msg = "table too wide to preview (schema exceeds the preview byte cap)"
		}
		writeJSONResp(w, qerr.Status, map[string]string{"error": msg, "kind": qerr.Kind})
		return
	}
	resp := PreviewResponse{Columns: make([]ColumnInfo, len(res.Schema)), Rows: res.Rows, Truncated: res.Truncated}
	for i, c := range res.Schema {
		resp.Columns[i] = ColumnInfo{Name: c.Name, Type: c.Type}
	}
	writeJSONResp(w, http.StatusOK, resp)
}

// loaderFn is the FSysF closure iceberg-go's table.NewFromLocation
// expects. This is the single dispatch boundary for storage-scheme
// routing in the walker fallback path (RFC §4.9): the URI scheme
// determines which props map to pass to LoadFS.
//
//   - file:// → nil props (LocalFS needs none)
//   - s3://   → svc.S3Props (carries STS access-key/secret/session-token)
//   - gs://   → svc.GCSProps (carries gcs.oauth2.token + expires-at)
//
// In the lakekeeper proxy path, tables are loaded via catalog.LoadTable
// and their FS closure is set by the REST catalog response — loaderFn
// is not called at all. loaderFn is only used by the walker fallback
// (LakekeeperURL == ""), which handles file:// and s3:// warehouses.
func loaderFn(svc *Service, fileURI string) table.FSysF {
	return func(ctx context.Context) (iceio.IO, error) {
		var props map[string]string
		switch {
		case strings.HasPrefix(fileURI, "gs://"):
			props = svc.GCSProps
		default:
			// file:// and s3://: S3Props is either nil (file) or carries
			// STS creds (s3). LoadFS handles both correctly.
			props = svc.S3Props
		}
		return LoadFS(ctx, fileURI, props)
	}
}
