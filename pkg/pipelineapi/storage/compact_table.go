package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipeline/config"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"
	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/runbackend"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

const compactorComponent = "iceberg-compactor"

const compactUnlockTimeout = 5 * time.Second

// SystemCompactPipelineName names the hidden per-table compaction pipeline.
// Hashing keeps it a valid DNS-1123 label whatever the identifiers contain,
// and the NUL separator keeps (a, b.c) and (a.b, c) apart.
func SystemCompactPipelineName(bucket, table string) string {
	sum := sha256.Sum256([]byte(bucket + "\x00" + table))
	return "sys-compact-" + hex.EncodeToString(sum[:])[:16]
}

type compactRequest struct {
	TargetFileSizeMB *int `json:"target_file_size_mb,omitempty"`
}

// CompactTable handles POST /api/v1/storage/projects/{pid}/tables/{ns}/{t}/compact.
//
// It saves a hidden system pipeline for the table and triggers it through the
// ordinary run backend. Admission is one-at-a-time per table: a non-blocking
// session advisory lock is held from the active-run check until TriggerRun has
// reserved the run row, so two requests can never both start a compaction or
// overwrite each other's doc mid-trigger. A busy lock is an immediate 409 so
// waiting requests never pin pool connections.
func (h *HTTPHandlers) CompactTable(w http.ResponseWriter, r *http.Request) {
	u, authed := auth.UserFromContext(r.Context())
	if !authed {
		writeErrResp(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	if h.Gate == nil {
		writeErrResp(w, http.StatusServiceUnavailable, "storage backend not fully configured")
		return
	}
	projectID, lkPID, gerr := h.Gate.AuthorizeRelation(r.Context(), u.ID.String(), r.PathValue("pid"), "data_admin")
	if gerr != nil {
		writeGateErrResp(w, gerr)
		return
	}

	ns, name := r.PathValue("ns"), r.PathValue("t")
	if !validateTableIdentifiers(w, ns, name) {
		return
	}

	var req compactRequest
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeErrResp(w, http.StatusRequestEntityTooLarge, "body too large")
			return
		}
		writeErrResp(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	if len(bytes.TrimSpace(body)) > 0 {
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			writeErrResp(w, http.StatusBadRequest, "invalid body: "+err.Error())
			return
		}
	}

	if h.Svc == nil || h.Svc.LakekeeperURL == "" {
		writeErrResp(w, http.StatusNotImplemented, "table compaction requires a lakekeeper catalog; this pipeline-api instance has none configured")
		return
	}
	warehouse, gerr := h.resolveWarehouse(r.Context(), lkPID)
	if gerr != nil {
		writeGateErrResp(w, gerr)
		return
	}
	proxy, err := newCatalogProxy(r.Context(), h.Svc, lkPID, warehouse)
	if err != nil {
		log.Printf("storage: compact %s.%s: open catalog (warehouse=%s): %v", ns, name, warehouse, err)
		writeErrResp(w, http.StatusInternalServerError, "open catalog")
		return
	}
	if _, err := proxy.loadTableForRead(r.Context(), ns, name); err != nil {
		writeErrResp(w, http.StatusNotFound, "table not found")
		return
	}

	pipelineName := SystemCompactPipelineName(ns, name)
	comp := config.Component{
		Name:      "compact",
		Component: compactorComponent,
		Outputs:   &config.OutputSpec{Tables: []config.OutputTableSpec{{Bucket: ns, Name: name}}},
	}
	if req.TargetFileSizeMB != nil {
		comp.Config = map[string]any{"target_file_size_mb": *req.TargetFileSizeMB}
	}
	raw, err := json.Marshal(config.Pipeline{
		Name:        pipelineName,
		Description: "Compaction of " + ns + "." + name,
		Stages:      []config.Stage{{Name: "compact", Components: []config.Component{comp}}},
	})
	if err != nil {
		writeErrResp(w, http.StatusInternalServerError, "encode pipeline")
		return
	}
	if h.Registry == nil {
		writeErrResp(w, http.StatusServiceUnavailable, "compactor_unavailable")
		return
	}
	_, findings := validate.ValidatePipelineDoc(raw, pipelineName, h.Registry, nil)
	var errFindings []validate.Finding
	for _, f := range findings {
		if f.Severity != "error" {
			continue
		}
		if strings.HasSuffix(f.Path, ".component") || strings.HasSuffix(f.Path, ".version") {
			writeErrResp(w, http.StatusServiceUnavailable, "compactor_unavailable")
			return
		}
		errFindings = append(errFindings, f)
	}
	if len(errFindings) > 0 {
		writeJSONResp(w, http.StatusBadRequest, map[string]any{"error": "validation failed", "findings": errFindings})
		return
	}
	doc, err := config.Parse(raw)
	if err != nil {
		writeErrResp(w, http.StatusInternalServerError, "re-parse pipeline")
		return
	}
	canonical, err := json.Marshal(doc)
	if err != nil {
		writeErrResp(w, http.StatusInternalServerError, "encode pipeline")
		return
	}

	if h.DB == nil || h.Runs == nil {
		writeErrResp(w, http.StatusServiceUnavailable, "compaction is not configured on this pipeline-api instance")
		return
	}
	if !h.tryCompactSlot() {
		writeErrResp(w, http.StatusServiceUnavailable, "compaction_busy")
		return
	}
	defer h.releaseCompactSlot()
	conn, err := h.DB.Acquire(r.Context())
	if err != nil {
		writeErrResp(w, http.StatusServiceUnavailable, "acquire database connection")
		return
	}
	lockKey := projectID.String() + "/" + pipelineName
	var locked bool
	if err := conn.QueryRow(r.Context(), `SELECT pg_try_advisory_lock(hashtextextended($1, 0))`, lockKey).Scan(&locked); err != nil {
		conn.Release()
		writeErrResp(w, http.StatusInternalServerError, "compaction lock")
		return
	}
	if !locked {
		conn.Release()
		writeErrResp(w, http.StatusConflict, "compaction_running")
		return
	}
	defer unlockCompaction(r.Context(), conn, lockKey)

	active, err := store.ActiveRunForPipeline(r.Context(), h.DB, projectID, pipelineName)
	if err != nil {
		writeErrResp(w, http.StatusInternalServerError, "check active runs")
		return
	}
	if active != uuid.Nil {
		writeJSONResp(w, http.StatusConflict, map[string]string{"error": "compaction_running", "run_id": active.String()})
		return
	}
	pipelineID, err := store.UpsertSystemPipeline(r.Context(), h.DB, projectID, pipelineName, doc.Description, canonical)
	if err != nil {
		log.Printf("storage: compact %s.%s (project=%s): save pipeline: %v", ns, name, projectID, err)
		writeErrResp(w, http.StatusInternalServerError, "save compaction pipeline")
		return
	}
	resp, err := h.Runs.TriggerRun(r.Context(), runbackend.TriggerRequest{
		ProjectID:    projectID,
		UserID:       u.ID,
		PipelineName: pipelineName,
		PipelineID:   pipelineID,
		Doc:          doc,
	})
	if err != nil {
		log.Printf("storage: compact %s.%s (project=%s): trigger: %v", ns, name, projectID, err)
		writeErrResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	log.Printf("storage: COMPACTION triggered for %s.%s (project=%s run=%s actor=%s)", ns, name, projectID, resp.RunID, u.ID)
	writeJSONResp(w, http.StatusAccepted, map[string]string{
		"run_id":   resp.RunID.String(),
		"pipeline": pipelineName,
		"target":   ns + "." + name,
	})
}

// tryCompactSlot takes a process-wide admission slot without blocking. Each
// admitted request pins one pooled connection for its advisory lock and needs
// a second for its own queries, so at most MaxConns/2 may be in flight at once
// across all tables, or concurrent compactions could exhaust the pool.
func (h *HTTPHandlers) tryCompactSlot() bool {
	h.compactOnce.Do(func() {
		h.compactSlots = make(chan struct{}, max(1, int(h.DB.Config().MaxConns)/2))
	})
	select {
	case h.compactSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (h *HTTPHandlers) releaseCompactSlot() { <-h.compactSlots }

// unlockCompaction releases the admission lock and returns conn to the pool.
// Releasing a pooled connection does not drop its session locks, so when the
// unlock cannot be confirmed the connection is closed instead.
func unlockCompaction(ctx context.Context, conn *pgxpool.Conn, lockKey string) {
	uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), compactUnlockTimeout)
	defer cancel()
	var unlocked bool
	err := conn.QueryRow(uctx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, lockKey).Scan(&unlocked)
	if err == nil && unlocked {
		conn.Release()
		return
	}
	log.Printf("storage: compaction advisory unlock (unlocked=%v): %v; closing connection", unlocked, err)
	if cerr := conn.Hijack().Close(uctx); cerr != nil {
		log.Printf("storage: close compaction lock connection: %v", cerr)
	}
}
