package storage

import (
	"errors"
	"log"
	"net/http"
	"strings"

	"github.com/apache/iceberg-go/catalog"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
)

// isTableGone reports whether an error means the table no longer exists.
// iceberg-go's catalogs return the typed catalog.ErrNoSuchTable; the REST
// catalog can also surface lakekeeper's 404 as an untyped error, so the
// message check is a fallback rather than the primary test.
func isTableGone(err error) bool {
	if errors.Is(err, catalog.ErrNoSuchTable) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such table") || strings.Contains(msg, "table not found") ||
		strings.Contains(msg, "notfound") || strings.Contains(msg, "does not exist")
}

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
// --confirm <ns>.<table>, the UI requires typing the same reference), because
// a machine-readable API that demands a magic query param invites clients to
// hardcode it.
//
// KNOWN HAZARD — deleting a table a run is actively writing. A Data Gateway
// writer resolves its data prefix and vended backend once at OpenWriter and
// holds them (datagateway/lakekeeper.LoadOrCreateForWrite), so purging the
// table mid-run does not stop the writer: it keeps writing parquet under the
// old prefix and then fails at commit, where icebergjob re-loads the table
// from the catalog and finds it gone. The run fails and the files written
// after the purge are orphaned in the bucket.
//
// This is deliberately NOT guarded here. The runs table carries no output-table
// column, so a pre-flight check would mean reading PipelineRun CRDs and
// scanning resolvedSpec — and it still could not close the window, since a run
// can be triggered immediately after the check passes. A guard that looks
// authoritative but isn't is worse than a documented hazard, so the contract
// is: don't delete a table while a pipeline is writing it. The failure is at
// least loud — the gateway now logs the commit error naming the table.
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
		// The load above can succeed and the purge still find the table gone:
		// a concurrent delete, or a client retry after a timeout on a request
		// that actually completed. Report that as 404 (the same answer a
		// second delete deserves) rather than 500 — a destructive endpoint
		// that fails retries with a server error pushes operators toward
		// guessing whether their first call took effect.
		if isTableGone(err) {
			writeErrResp(w, http.StatusNotFound, "table not found")
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
