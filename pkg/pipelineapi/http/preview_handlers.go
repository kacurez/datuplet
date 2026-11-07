package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/config"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"
	"github.com/datuplet/datuplet/pkg/pipelineapi/preview"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// previewService is the preview.Service surface the handlers use.
type previewService interface {
	Create(ctx context.Context, r preview.CreateRequest) (*preview.CreateResponse, error)
	Get(ctx context.Context, projectID, id uuid.UUID, resample bool) (*preview.Result, error)
	List(ctx context.Context, projectID uuid.UUID, workspace string, limit int, cursor string) ([]store.Preview, string, error)
	Cancel(ctx context.Context, projectID, id uuid.UUID) error
	Delete(ctx context.Context, projectID, id uuid.UUID) error
	DeleteWorkspace(ctx context.Context, projectID uuid.UUID, ws string) (deleted []uuid.UUID, failed map[uuid.UUID]string)
}

const defaultPreviewDocName = "preview"

type createPreviewJSON struct {
	Doc            json.RawMessage `json:"doc"`
	Workspace      string          `json:"workspace"`
	RowLimit       int             `json:"row_limit"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	TTLSeconds     int             `json:"ttl_seconds"`
	UntilStage     string          `json:"until_stage"`
}

type createPreviewResponseJSON struct {
	ID                string    `json:"id"`
	Workspace         string    `json:"workspace"`
	Phase             string    `json:"phase"`
	ScratchNamespaces []string  `json:"scratch_namespaces"`
	ExpiresAt         time.Time `json:"expires_at"`
}

type previewSummaryJSON struct {
	ID         string    `json:"id"`
	Workspace  string    `json:"workspace"`
	Phase      string    `json:"phase"`
	DocName    string    `json:"doc_name"`
	UntilStage *string   `json:"until_stage"`
	CreatedAt  time.Time `json:"created_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

type previewsPageJSON struct {
	Previews   []previewSummaryJSON `json:"previews"`
	NextCursor *string              `json:"next_cursor"`
}

// handleCreatePreview is POST /api/v1/projects/{pid}/previews.
func (s *Server) handleCreatePreview(w http.ResponseWriter, r *http.Request) {
	userID, projectID, ok := s.mustHaveRelation(w, r, "data_admin")
	if !ok {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req createPreviewJSON
	if err := readJSON(r, &req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "body exceeds 1 MiB")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	body := []byte(req.Doc)
	if len(body) == 0 || string(body) == "null" {
		writeError(w, http.StatusBadRequest, "doc is required")
		return
	}
	var head struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(body, &head)
	name := head.Name
	if name == "" {
		name = defaultPreviewDocName
	}

	// The gates below mirror handlePutPipeline's create path (no stored
	// pipeline, so the "old" spec is empty); keep the two in step.
	pl, findings := validate.ValidatePipelineDoc(body, name, s.registry, nil)
	if pl == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "validation failed", "findings": findings,
		})
		return
	}
	isSuperadmin, ok := s.isRequestSuperadmin(w, r, userID)
	if !ok {
		return
	}
	if !isSuperadmin {
		if resourcesModified(&datupletv1.Pipeline{}, pl) {
			writeError(w, http.StatusForbidden,
				"modifying component resources requires superadmin privileges")
			return
		}
		_, findings = validate.ValidatePipelineDoc(body, name, s.registry, s.policy)
		for _, f := range findings {
			if f.Severity == "error" && strings.HasPrefix(f.Path, "gateway.") {
				writeError(w, http.StatusForbidden,
					"modifying gateway settings beyond policy bounds requires superadmin privileges")
				return
			}
		}
	}
	for _, f := range findings {
		if f.Severity == "error" {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error": "validation failed", "findings": findings,
			})
			return
		}
	}

	missing, err := s.missingSecretRefs(r.Context(), projectID, validate.ReferencedSecrets(pl))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "check secrets")
		return
	}
	if len(missing) > 0 {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("pipeline references secret(s) not set in this project: %s", strings.Join(missing, ", ")))
		return
	}

	resp, err := s.previews.Create(r.Context(), preview.CreateRequest{
		ProjectID:      projectID,
		UserID:         userID,
		Doc:            config.CRToDoc(pl),
		Workspace:      req.Workspace,
		RowLimit:       req.RowLimit,
		TimeoutSeconds: req.TimeoutSeconds,
		TTLSeconds:     req.TTLSeconds,
		UntilStage:     req.UntilStage,
	})
	var capErr *store.PreviewCapError
	switch {
	case errors.As(err, &capErr):
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":                "too_many_previews",
			"running_in_workspace": capErr.RunningInWorkspace,
			"running_in_project":   capErr.RunningInProject,
			"live_in_project":      capErr.LiveInProject,
		})
		return
	case errors.Is(err, preview.ErrBadRequest):
		writeError(w, http.StatusBadRequest, err.Error())
		return
	case err != nil:
		log.Printf("pipeline-api: create preview in project %s: %v", projectID, err)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := createPreviewResponseJSON{
		ID: resp.ID.String(), Workspace: resp.Workspace, Phase: resp.Phase,
		ScratchNamespaces: resp.ScratchNamespaces, ExpiresAt: resp.ExpiresAt,
	}
	if out.ScratchNamespaces == nil {
		out.ScratchNamespaces = []string{}
	}
	writeJSON(w, http.StatusAccepted, out)
}

// handleListPreviews is GET /api/v1/projects/{pid}/previews.
func (s *Server) handleListPreviews(w http.ResponseWriter, r *http.Request) {
	_, projectID, ok := s.mustHaveRelation(w, r, "data_admin")
	if !ok {
		return
	}
	q := r.URL.Query()
	ws := q.Get("workspace")
	if ws == "" {
		ws = preview.DefaultWorkspace
	}
	limit := 0
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	rows, next, err := s.previews.List(r.Context(), projectID, ws, limit, q.Get("cursor"))
	if errors.Is(err, store.ErrBadCursor) {
		writeError(w, http.StatusBadRequest, "invalid cursor")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list previews")
		return
	}
	out := previewsPageJSON{Previews: make([]previewSummaryJSON, 0, len(rows))}
	if next != "" {
		out.NextCursor = &next
	}
	for _, p := range rows {
		var head struct {
			Name string `json:"name"`
		}
		_ = json.Unmarshal(p.Doc, &head)
		item := previewSummaryJSON{
			ID: p.ID.String(), Workspace: p.Workspace, Phase: p.Phase, DocName: head.Name,
			CreatedAt: p.CreatedAt, ExpiresAt: p.ExpiresAt,
		}
		if p.UntilStage != "" {
			item.UntilStage = &p.UntilStage
		}
		out.Previews = append(out.Previews, item)
	}
	writeJSON(w, http.StatusOK, out)
}

// previewTarget runs the data_admin gate and parses the {id} path segment.
func (s *Server) previewTarget(w http.ResponseWriter, r *http.Request) (projectID, id uuid.UUID, ok bool) {
	_, projectID, ok = s.mustHaveRelation(w, r, "data_admin")
	if !ok {
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid preview id")
		return projectID, id, false
	}
	return projectID, id, true
}

// handleGetPreview is GET /api/v1/projects/{pid}/previews/{id}[?resample=true].
func (s *Server) handleGetPreview(w http.ResponseWriter, r *http.Request) {
	projectID, id, ok := s.previewTarget(w, r)
	if !ok {
		return
	}
	res, err := s.previews.Get(r.Context(), projectID, id, r.URL.Query().Get("resample") == "true")
	if errors.Is(err, store.ErrPreviewNotFound) {
		writeError(w, http.StatusNotFound, "preview not found")
		return
	}
	if err != nil {
		log.Printf("pipeline-api: get preview %s: %v", id, err)
		writeError(w, http.StatusInternalServerError, "get preview")
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleCancelPreview is POST /api/v1/projects/{pid}/previews/{id}/cancel.
func (s *Server) handleCancelPreview(w http.ResponseWriter, r *http.Request) {
	projectID, id, ok := s.previewTarget(w, r)
	if !ok {
		return
	}
	err := s.previews.Cancel(r.Context(), projectID, id)
	if errors.Is(err, store.ErrPreviewNotFound) {
		writeError(w, http.StatusNotFound, "preview not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// handleDeletePreview is DELETE /api/v1/projects/{pid}/previews/{id}. The
// purge runs synchronously (it may wait for pods to exit).
func (s *Server) handleDeletePreview(w http.ResponseWriter, r *http.Request) {
	projectID, id, ok := s.previewTarget(w, r)
	if !ok {
		return
	}
	err := s.previews.Delete(r.Context(), projectID, id)
	if errors.Is(err, store.ErrPreviewNotFound) {
		writeError(w, http.StatusNotFound, "preview not found")
		return
	}
	if err != nil {
		log.Printf("pipeline-api: delete preview %s: %v", id, err)
		writeError(w, http.StatusBadGateway, "purge_failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleDeletePreviewWorkspace is DELETE /api/v1/projects/{pid}/previews?workspace=X.
func (s *Server) handleDeletePreviewWorkspace(w http.ResponseWriter, r *http.Request) {
	_, projectID, ok := s.mustHaveRelation(w, r, "data_admin")
	if !ok {
		return
	}
	ws := r.URL.Query().Get("workspace")
	if ws == "" {
		writeError(w, http.StatusBadRequest, "workspace query parameter is required")
		return
	}
	deleted, failed := s.previews.DeleteWorkspace(r.Context(), projectID, ws)
	if msg, listFailed := failed[uuid.Nil]; listFailed {
		log.Printf("pipeline-api: delete preview workspace %q in project %s: %s", ws, projectID, msg)
		writeError(w, http.StatusInternalServerError, "list workspace failed")
		return
	}
	out := struct {
		Deleted []string          `json:"deleted"`
		Failed  map[string]string `json:"failed"`
	}{Deleted: make([]string, 0, len(deleted)), Failed: make(map[string]string, len(failed))}
	for _, id := range deleted {
		out.Deleted = append(out.Deleted, id.String())
	}
	for id, msg := range failed {
		out.Failed[id.String()] = msg
	}
	writeJSON(w, http.StatusAccepted, out)
}
