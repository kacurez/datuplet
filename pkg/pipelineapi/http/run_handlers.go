package http

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipeline/config"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"
	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/runbackend"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

type runJSON struct {
	ID           string `json:"id"`
	ProjectID    string `json:"project_id,omitempty"`
	PipelineID   string `json:"pipeline_id,omitempty"`
	PipelineName string `json:"pipeline_name,omitempty"`
	Phase        string `json:"phase"`
	CurrentStage string `json:"current_stage,omitempty"`
	Message      string `json:"message,omitempty"`
	CreatedAt    string `json:"created_at"`
	StartedAt    string `json:"started_at,omitempty"`
	CompletedAt  string `json:"completed_at,omitempty"`
}

func runToJSON(v store.RunView) runJSON {
	j := runJSON{
		ID: v.ID.String(),
		Phase: v.Phase, CurrentStage: v.CurrentStage, Message: v.Message,
		CreatedAt:    v.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		PipelineName: v.PipelineName,
	}
	if v.PipelineID != uuid.Nil {
		j.PipelineID = v.PipelineID.String()
	}
	if v.ProjectID != nil {
		j.ProjectID = v.ProjectID.String()
	}
	if v.StartedAt != nil {
		j.StartedAt = v.StartedAt.Format("2006-01-02T15:04:05Z07:00")
	}
	if v.CompletedAt != nil {
		j.CompletedAt = v.CompletedAt.Format("2006-01-02T15:04:05Z07:00")
	}
	return j
}

// handleTriggerRun is POST /api/v1/projects/:pid/pipelines/:name/runs.
//
// The handler is a thin adapter: it resolves auth, looks up the pipeline
// YAML, parses it once, and hands everything off to s.backend.TriggerRun.
// The backend owns ordering of DB insert, CRD apply, token mint, and
// PipelineRun + Secret creation (cluster mode) or Docker execution
// (local mode).
func (s *Server) handleTriggerRun(w http.ResponseWriter, r *http.Request) {
	user, authed := auth.UserFromContext(r.Context())
	if !authed {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	_, projectID, ok := s.mustHaveRelation(w, r, "data_admin")
	if !ok {
		return
	}
	pipelineName := r.PathValue("name")

	pipe, err := s.pipelines.Get(r.Context(), projectID, pipelineName)
	if errors.Is(err, errStoreNotFound) {
		writeError(w, http.StatusNotFound, "pipeline not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get pipeline")
		return
	}

	// Parse + validate the stored doc exactly once. ValidatePipelineDoc
	// decodes the doc, converts it to the typed CRD, and runs the shared
	// semantic rules; the returned CR feeds both the ReferencedSecrets walk
	// (below) and — via config.CRToDoc — the TriggerRequest.Doc the backend
	// renders back into the applied Pipeline CR. An error finding here would
	// mean the stored bytes changed since the PUT that validated them, so it
	// maps to the same 500 the pre-RFC-027 code used for its "cannot fail in
	// practice" re-decode. No run row is inserted yet, so this doesn't leave
	// a ghost row behind.
	crd, findings := validate.ValidatePipelineDoc(pipe.Doc, pipelineName, nil, nil)
	for _, f := range findings {
		if f.Severity == "error" {
			writeError(w, http.StatusInternalServerError, "re-validate pipeline")
			return
		}
	}
	refs := validate.ReferencedSecrets(crd)
	missing, err := s.missingSecretRefs(r.Context(), projectID, refs)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "check secrets")
		return
	}
	if len(missing) > 0 {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("pipeline references secret(s) not set in this project: %s", strings.Join(missing, ", ")))
		return
	}

	pipelineID, err := uuid.Parse(pipe.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "invalid stored pipeline id")
		return
	}
	resp, err := s.backend.TriggerRun(r.Context(), runbackend.TriggerRequest{
		ProjectID:    projectID,
		UserID:       user.ID,
		PipelineName: pipelineName,
		PipelineID:   pipelineID,
		Doc:          config.CRToDoc(crd),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"id":     resp.RunID.String(),
		"status": "Pending",
		"k8s_ns": resp.Namespace,
	})
}

// knownPhases bounds the ?phase= filter to the run phases the system emits.
var knownPhases = map[string]bool{
	"Pending": true, "Running": true, "Succeeded": true,
	"FailedUser": true, "FailedApplication": true,
	"Cancelled": true, "Expired": true,
}

type runsPageJSON struct {
	Runs       []runJSON `json:"runs"`
	NextCursor *string   `json:"next_cursor"` // nil → JSON null on the last page (RFC contract)
}

func (s *Server) handleListRuns(w http.ResponseWriter, r *http.Request) {
	_, projectID, ok := s.mustHaveRelation(w, r, "describe")
	if !ok {
		return
	}
	q := r.URL.Query()
	opts := store.RunListOpts{
		Cursor:         q.Get("cursor"),
		PipelineSubstr: q.Get("pipeline"),
		Phase:          q.Get("phase"),
	}
	if opts.Phase != "" && !knownPhases[opts.Phase] {
		writeError(w, http.StatusBadRequest, "unknown phase")
		return
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			if n < 1 {
				n = 1
			} else if n > 200 {
				n = 200
			}
			opts.Limit = n // explicit ?limit= clamped to 1..200; absent → store default 50
		}
	}
	if v := q.Get("pipeline_id"); v != "" {
		pid, err := uuid.Parse(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid pipeline_id")
			return
		}
		opts.PipelineID = pid
	}
	page, err := s.runs.ListPage(r.Context(), projectID, opts)
	if err != nil {
		if errors.Is(err, store.ErrBadCursor) {
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		writeError(w, http.StatusInternalServerError, "list runs")
		return
	}
	out := runsPageJSON{Runs: make([]runJSON, 0, len(page.Runs))}
	if page.NextCursor != "" {
		out.NextCursor = &page.NextCursor // last page → stays nil → JSON null
	}
	for _, rn := range page.Runs {
		// page.Runs is []*store.Run; runToJSON takes store.RunView.
		out.Runs = append(out.Runs, runToJSON(store.ToRunView(rn)))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	_, projectID, ok := s.mustHaveRelation(w, r, "describe")
	if !ok {
		return
	}
	rid, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid run id")
		return
	}
	run, err := s.runs.GetByID(r.Context(), projectID, rid)
	if errors.Is(err, errStoreNotFound) {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get run")
		return
	}
	detail := struct {
		runJSON
		Timeline []timelineStage `json:"timeline"`
	}{runJSON: runToJSON(run)}

	// Best-effort timeline: a missing doc or parse error leaves Timeline nil
	// (UI shows "no stage timeline recorded"); it never fails the run fetch.
	if run.PipelineID != uuid.Nil {
		if raw, err := s.pipelines.GetDocByID(r.Context(), run.PipelineID.String()); err == nil {
			if doc, err := config.Parse(raw); err == nil {
				if tl, err := buildTimeline(run.StageStatuses, doc); err == nil {
					detail.Timeline = tl
				}
			}
		}
	}
	writeJSON(w, http.StatusOK, detail)
}

// handleCancelRun is POST /api/v1/projects/:pid/runs/:id/cancel.
//
// The handler is a thin adapter around s.backend.CancelRun; the backend
// owns the terminal-phase guard, CRD deletion, detached-context DB
// update, and token revocation.
func (s *Server) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	_, projectID, ok := s.mustHaveRelation(w, r, "data_admin")
	if !ok {
		return
	}
	rid, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid run id")
		return
	}
	err = s.backend.CancelRun(r.Context(), runbackend.CancelRequest{
		ProjectID: projectID,
		RunID:     rid,
	})
	if errors.Is(err, store.ErrRunNotFound) {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
