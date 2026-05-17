package http

import (
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
)

type projectJSON struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	K8sNamespace string `json:"k8s_namespace"`
}

func projectViewToJSON(p ProjectView) projectJSON {
	return projectJSON{ID: p.ID.String(), Name: p.Name, K8sNamespace: p.K8sNamespace}
}

func (s *Server) handleListProjects(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	projects, err := s.projects.ListForUser(r.Context(), user.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list projects")
		return
	}
	out := make([]projectJSON, 0, len(projects))
	for _, p := range projects {
		out = append(out, projectViewToJSON(p))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleGetProject(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return
	}
	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid project id")
		return
	}

	// Authorize via FGA `viewer` on the lakekeeper project. Order: load
	// project (404 if unknown), check `viewer`, then serialize. The
	// 404→403 transition matches the rest of the project-scoped handlers.
	p, err := s.projects.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, errStoreNotFound) {
			writeError(w, http.StatusNotFound, "project not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "get project")
		return
	}
	if p.LakekeeperProjectID == "" {
		writeError(w, http.StatusServiceUnavailable,
			"project authz not yet provisioned (lakekeeper project pending)")
		return
	}
	allowed, checkErr := s.authzr.Check(r.Context(),
		authz.UserObject(user.ID.String()).String(),
		"describe",
		authz.ProjectObject(p.LakekeeperProjectID))
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
	writeJSON(w, http.StatusOK, projectViewToJSON(*p))
}
