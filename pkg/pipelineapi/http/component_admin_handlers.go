package http

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/datuplet/datuplet/pkg/pipelineapi/registry"
)

// ComponentRegistryWriter is the write seam behind the superadmin-gated
// PUT/DELETE /api/v1/admin/components/{name} routes. Put upserts a
// ComponentDefinition (definition validation stays with the async
// componentdefinition-controller); Delete hard-deletes it. Production wiring
// is *registry.Writer over the ComponentDefinition K8s client; tests supply
// fakes.
type ComponentRegistryWriter interface {
	Put(ctx context.Context, name string, specYAML []byte) error
	Delete(ctx context.Context, name string) error
}

// handlePutComponentDefinition serves PUT /api/v1/admin/components/{name}:
// superadmin-gated upsert of a ComponentDefinition spec (JSON or YAML body).
func (s *Server) handlePutComponentDefinition(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.mustBeSuperadmin(w, r); !ok {
		return
	}
	name := r.PathValue("name")
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		writeError(w, http.StatusBadRequest, "invalid component name: "+strings.Join(errs, "; "))
		return
	}
	// MaxBytesReader fails hard on oversize (vs. io.LimitReader's silent
	// truncation), so a >1 MiB body yields a 413 and never reaches the writer.
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "body exceeds 1 MiB: "+err.Error())
		return
	}
	switch err := s.componentAdmin.Put(r.Context(), name, body); {
	case errors.Is(err, registry.ErrInvalidDefinition):
		writeError(w, http.StatusBadRequest, err.Error())
	case err != nil:
		writeError(w, http.StatusInternalServerError, "put component definition")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// handleDeleteComponentDefinition serves DELETE /api/v1/admin/components/{name}:
// superadmin-gated hard delete. An unknown component maps to 404.
func (s *Server) handleDeleteComponentDefinition(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.mustBeSuperadmin(w, r); !ok {
		return
	}
	name := r.PathValue("name")
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		writeError(w, http.StatusBadRequest, "invalid component name: "+strings.Join(errs, "; "))
		return
	}
	switch err := s.componentAdmin.Delete(r.Context(), name); {
	case apierrors.IsNotFound(err):
		writeError(w, http.StatusNotFound, "component not found")
	case err != nil:
		writeError(w, http.StatusInternalServerError, "delete component definition")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}
