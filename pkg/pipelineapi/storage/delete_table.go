package storage

import (
	"errors"
	"log"
	"net/http"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
)

// DeleteTable handles DELETE /api/v1/storage/projects/{pid}/tables/{ns}/{t}.
//
// This is the only destructive storage route, so it differs from its read-only
// siblings in three ways:
//
//   - Authorization requires FGA "data_admin", not the "datuplet_member" the
//     read handlers use. data_admin is the same relation that gates pipeline
//     PUT and run triggers and resolves upward through `editor`, so a project
//     viewer cannot delete data they can merely read.
//   - It PURGES (metadata + data files) rather than dropping metadata only.
//     A metadata-only drop would strand every parquet file the table wrote:
//     still paid for, no longer reachable through any catalog.
//   - It is lakekeeper-only. The directory-walker fallback used by the read
//     handlers in tests and legacy fixture warehouses has no delete path, and
//     synthesizing one out of raw prefix removal is exactly the kind of
//     "delete by path arithmetic" this codebase avoids. Unconfigured
//     lakekeeper returns 501 rather than silently doing something else.
//
// Deletion is NOT recoverable and there is no server-side confirmation
// parameter — the confirmation belongs in the client (the CLI requires
// --yes, the UI requires typing the table name), because a machine-readable
// API that demands a magic query param invites clients to hardcode it.
func (h *HTTPHandlers) DeleteTable(w http.ResponseWriter, r *http.Request) {
	u, authed := auth.UserFromContext(r.Context())
	if !authed {
		writeErrResp(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	if h.Gate == nil {
		writeErrResp(w, http.StatusServiceUnavailable, "storage backend not fully configured")
		return
	}
	_, lkPID, gerr := h.Gate.AuthorizeRelation(r.Context(), u.ID.String(), r.PathValue("pid"), "data_admin")
	if gerr != nil {
		writeGateErrResp(w, gerr)
		return
	}

	ns, name := r.PathValue("ns"), r.PathValue("t")
	if !validateTableIdentifiers(w, ns, name) {
		return
	}

	if h.Svc == nil || h.Svc.LakekeeperURL == "" {
		writeErrResp(w, http.StatusNotImplemented, "table deletion requires a lakekeeper catalog; this pipeline-api instance has none configured")
		return
	}

	warehouse, gerr := h.resolveWarehouse(r.Context(), lkPID)
	if gerr != nil {
		writeGateErrResp(w, gerr)
		return
	}
	proxy, err := newCatalogProxy(r.Context(), h.Svc, lkPID, warehouse)
	if err != nil {
		log.Printf("storage: delete %s.%s: open catalog (warehouse=%s): %v", ns, name, warehouse, err)
		writeErrResp(w, http.StatusInternalServerError, "open catalog")
		return
	}

	// Load first so a missing table is a clean 404 instead of a catalog error
	// surfaced as a 500 — and so the audit line below only ever records a
	// delete that had a real target.
	if _, err := proxy.loadTableForRead(r.Context(), ns, name); err != nil {
		writeErrResp(w, http.StatusNotFound, "table not found")
		return
	}

	if err := proxy.purgeTable(r.Context(), ns, name); err != nil {
		if errors.Is(err, catalogwriter.ErrPurgeNotSupported) {
			log.Printf("storage: delete %s.%s: catalog does not support purge: %v", ns, name, err)
			writeErrResp(w, http.StatusNotImplemented, "this catalog does not support table deletion")
			return
		}
		log.Printf("storage: delete %s.%s (project=%s warehouse=%s): %v", ns, name, lkPID, warehouse, err)
		writeErrResp(w, http.StatusInternalServerError, "delete table: "+err.Error())
		return
	}

	// Audit: a destructive, unrecoverable action needs a durable record of
	// who deleted what, independent of any client-side log.
	log.Printf("storage: DELETED table %s.%s (project=%s warehouse=%s actor=%s)", ns, name, lkPID, warehouse, u.ID)

	writeJSONResp(w, http.StatusOK, map[string]any{
		"deleted":   true,
		"namespace": ns,
		"table":     name,
	})
}
