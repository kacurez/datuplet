package http

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/datuplet/datuplet/pkg/pipeline/config"
	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
)

// mustHaveRelation is the project-scoped authz guard. Reads the {pid}
// path value, parses it, looks up the lakekeeper project UUID via
// s.projects.GetByID, and runs
// authzr.Check(user, relation, project:<lakekeeper-uuid>).
//
// Returns (userID, datupletProjectID, true) on success. On failure it
// writes the appropriate HTTP error and returns ok=false; callers must
// return immediately.
//
// Error mapping:
//
//   - 401: no authenticated user.
//   - 400: malformed UUID in {pid}.
//   - 404: project not found in our store.
//   - 503: authz backend unavailable (ErrAuthzUnavailable).
//   - 500: unexpected DB / authz error.
//   - 403: authz backend says no.
//
// Soft-degrade: when the project's LakekeeperProjectID is empty
// (lakekeeper not yet provisioned for this Datuplet project), we return
// 503. The reconciler in cmd/pipeline-api/admin.go fills the row in once
// provisioning succeeds.
func (s *Server) mustHaveRelation(w http.ResponseWriter, r *http.Request, relation string) (userID, projectID uuid.UUID, ok bool) {
	user, authed := auth.UserFromContext(r.Context())
	if !authed {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	pid, err := uuid.Parse(r.PathValue("pid"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid project id")
		return
	}
	proj, err := s.projects.GetByID(r.Context(), pid)
	if errors.Is(err, errStoreNotFound) {
		writeError(w, http.StatusNotFound, "project not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get project")
		return
	}
	if proj.LakekeeperProjectID == "" {
		writeError(w, http.StatusServiceUnavailable,
			"project authz not yet provisioned (lakekeeper project pending)")
		return
	}
	allowed, checkErr := s.authzr.Check(r.Context(),
		authz.UserObject(user.ID.String()).String(),
		relation,
		authz.ProjectObject(proj.LakekeeperProjectID))
	if errors.Is(checkErr, authz.ErrAuthzUnavailable) {
		writeError(w, http.StatusServiceUnavailable, "authz backend unavailable")
		return
	}
	if checkErr != nil {
		writeError(w, http.StatusInternalServerError, "check authz")
		return
	}
	if !allowed {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}
	return user.ID, pid, true
}

// pipelineDetailJSON is the full shape returned by GetByName. Fields
// that are zero in local mode (ID) are rendered as their zero-UUID
// string for wire stability.
type pipelineDetailJSON struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	YAML      string `json:"yaml"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// pipelineRefJSON is the summary shape returned by List. Timestamps are
// omitted when zero so local mode (which doesn't stat files on list)
// doesn't emit a misleading 0001-01-01 value.
type pipelineRefJSON struct {
	Name      string `json:"name"`
	CreatedAt string `json:"created_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

const pipelineTimeLayout = "2006-01-02T15:04:05Z07:00"

func pipelineDetailToJSON(p PipelineDetail) pipelineDetailJSON {
	return pipelineDetailJSON{
		ID: p.ID.String(), Name: p.Name, YAML: p.YAML,
		CreatedAt: p.CreatedAt.Format(pipelineTimeLayout),
		UpdatedAt: p.UpdatedAt.Format(pipelineTimeLayout),
	}
}

func pipelineRefToJSON(p PipelineRef) pipelineRefJSON {
	out := pipelineRefJSON{Name: p.Name}
	if !p.CreatedAt.IsZero() {
		out.CreatedAt = p.CreatedAt.Format(pipelineTimeLayout)
	}
	if !p.UpdatedAt.IsZero() {
		out.UpdatedAt = p.UpdatedAt.Format(pipelineTimeLayout)
	}
	return out
}

func (s *Server) handlePutPipeline(w http.ResponseWriter, r *http.Request) {
	_, projectID, ok := s.mustHaveRelation(w, r, "data_admin")
	if !ok {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "pipeline name required")
		return
	}
	// The name becomes a K8s Pipeline/PipelineRun resource name, so it
	// must satisfy DNS-1123 subdomain rules (lowercase alphanumerics
	// plus '-' and '.', max 253 chars). Rejecting here yields a clean
	// 400 instead of a late 500 at run-trigger time when ApplyPipelineCRD
	// fails to create the CRD.
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		writeError(w, http.StatusBadRequest, "invalid pipeline name: "+strings.Join(errs, "; "))
		return
	}

	// MaxBytesReader fails hard on oversize (vs. io.LimitReader's silent
	// truncation), so a 1.1 MiB body yields a 413 and never reaches Parse.
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "body exceeds 1 MiB: "+err.Error())
		return
	}
	parsed, err := config.Parse(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid pipeline: "+err.Error())
		return
	}
	// metadata.name must match the URL name: otherwise ApplyPipelineCRD at
	// run-trigger time would create a CRD under the YAML's name while the
	// PipelineRun would reference the URL name, and the operator would fail
	// to find the Pipeline.
	if parsed.Metadata.Name != "" && parsed.Metadata.Name != name {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("YAML metadata.name %q does not match URL name %q", parsed.Metadata.Name, name))
		return
	}

	// PUT is upsert — pgx adapter tries UPDATE first and falls back to
	// INSERT; local filesystem is last-write-wins. No conflict path today.
	if err := s.pipelines.Put(r.Context(), projectID, name, body); err != nil {
		writeError(w, http.StatusInternalServerError, "put pipeline")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleListPipelines(w http.ResponseWriter, r *http.Request) {
	_, projectID, ok := s.mustHaveRelation(w, r, "describe")
	if !ok {
		return
	}
	ps, err := s.pipelines.List(r.Context(), projectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list pipelines")
		return
	}
	out := make([]pipelineRefJSON, 0, len(ps))
	for _, p := range ps {
		out = append(out, pipelineRefToJSON(p))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleGetPipeline(w http.ResponseWriter, r *http.Request) {
	_, projectID, ok := s.mustHaveRelation(w, r, "describe")
	if !ok {
		return
	}
	name := r.PathValue("name")
	p, err := s.pipelines.GetByName(r.Context(), projectID, name)
	if errors.Is(err, errStoreNotFound) {
		writeError(w, http.StatusNotFound, "pipeline not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get pipeline")
		return
	}
	writeJSON(w, http.StatusOK, pipelineDetailToJSON(*p))
}

func (s *Server) handleDeletePipeline(w http.ResponseWriter, r *http.Request) {
	_, projectID, ok := s.mustHaveRelation(w, r, "data_admin")
	if !ok {
		return
	}
	name := r.PathValue("name")
	if err := s.pipelines.Delete(r.Context(), projectID, name); err != nil {
		if errors.Is(err, errStoreNotFound) {
			writeError(w, http.StatusNotFound, "pipeline not found")
			return
		}
		if errors.Is(err, errPipelineInUse) {
			writeError(w, http.StatusConflict, "pipeline has active runs")
			return
		}
		writeError(w, http.StatusInternalServerError, "delete pipeline")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
