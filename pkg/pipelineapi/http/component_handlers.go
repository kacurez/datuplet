package http

import (
	"context"
	"net/http"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"
)

// ComponentRegistry is the read surface pipeline-api needs from the
// component registry. Resolve satisfies validate.RegistryView and is
// threaded into ValidatePipeline on the pipeline save path; List backs the
// /api/v1/components catalog handlers below. Production wiring is
// *registry.View (pkg/pipelineapi/registry); tests supply lightweight fakes.
type ComponentRegistry interface {
	validate.RegistryView
	List(ctx context.Context) ([]datupletv1.ComponentDefinition, error)
}

// componentVersionJSON is one entry of componentSummaryJSON.Versions — the
// catalog-picker shape (spec §4.7). ConfigSchema is deliberately omitted
// here; it's only returned by the per-component detail endpoint.
type componentVersionJSON struct {
	Version    string `json:"version"`
	Prerelease bool   `json:"prerelease"`
	Image      string `json:"image"`
}

// componentIOJSON is the catalog's io capability object (RFC 027 §4.3),
// always present with resolved (never-empty) mode strings — see
// datupletv1.ComponentIO.InputsMode/OutputsMode.
type componentIOJSON struct {
	Inputs  string `json:"inputs"`
	Outputs string `json:"outputs"`
}

func componentIOToJSON(io *datupletv1.ComponentIO) componentIOJSON {
	return componentIOJSON{Inputs: io.InputsMode(), Outputs: io.OutputsMode()}
}

// componentSummaryJSON is one entry of GET /api/v1/components.
type componentSummaryJSON struct {
	Name           string                 `json:"name"`
	DisplayName    string                 `json:"displayName"`
	Description    string                 `json:"description"`
	Deprecated     bool                   `json:"deprecated"`
	DefaultVersion string                 `json:"defaultVersion"`
	IO             componentIOJSON        `json:"io"`
	Versions       []componentVersionJSON `json:"versions"`
}

// componentDetailVersionJSON adds ConfigSchema to componentVersionJSON, for
// the per-component detail endpoint only.
type componentDetailVersionJSON struct {
	Version      string `json:"version"`
	Prerelease   bool   `json:"prerelease"`
	Image        string `json:"image"`
	ConfigSchema string `json:"configSchema"`
}

// componentDetailJSON is the body of GET /api/v1/components/{name}.
type componentDetailJSON struct {
	Name           string                       `json:"name"`
	DisplayName    string                       `json:"displayName"`
	Description    string                       `json:"description"`
	Deprecated     bool                         `json:"deprecated"`
	DefaultVersion string                       `json:"defaultVersion"`
	IO             componentIOJSON              `json:"io"`
	Versions       []componentDetailVersionJSON `json:"versions"`
}

func componentSummaryToJSON(d datupletv1.ComponentDefinition) componentSummaryJSON {
	out := componentSummaryJSON{
		Name:           d.Name,
		DisplayName:    d.Spec.DisplayName,
		Description:    d.Spec.Description,
		Deprecated:     d.Spec.Deprecated,
		DefaultVersion: d.Spec.DefaultVersion,
		IO:             componentIOToJSON(d.Spec.IO),
		Versions:       make([]componentVersionJSON, 0, len(d.Spec.Versions)),
	}
	for _, v := range d.Spec.Versions {
		out.Versions = append(out.Versions, componentVersionJSON{
			Version: v.Version, Prerelease: v.Prerelease, Image: v.Image,
		})
	}
	return out
}

func componentDetailToJSON(d datupletv1.ComponentDefinition) componentDetailJSON {
	out := componentDetailJSON{
		Name:           d.Name,
		DisplayName:    d.Spec.DisplayName,
		Description:    d.Spec.Description,
		Deprecated:     d.Spec.Deprecated,
		DefaultVersion: d.Spec.DefaultVersion,
		IO:             componentIOToJSON(d.Spec.IO),
		Versions:       make([]componentDetailVersionJSON, 0, len(d.Spec.Versions)),
	}
	for _, v := range d.Spec.Versions {
		out.Versions = append(out.Versions, componentDetailVersionJSON{
			Version: v.Version, Prerelease: v.Prerelease, Image: v.Image, ConfigSchema: v.ConfigSchema,
		})
	}
	return out
}

// handleListComponents serves GET /api/v1/components: the shared component
// picker, open to any authenticated user (WithUser only — no project-scoped
// authz check, spec §4.7).
func (s *Server) handleListComponents(w http.ResponseWriter, r *http.Request) {
	defs, err := s.registry.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list components")
		return
	}
	out := make([]componentSummaryJSON, 0, len(defs))
	for _, d := range defs {
		out = append(out, componentSummaryToJSON(d))
	}
	writeJSON(w, http.StatusOK, out)
}

// handleGetComponent serves GET /api/v1/components/{name}: the summary shape
// plus per-version configSchema. 404 for an unknown name.
func (s *Server) handleGetComponent(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	defs, err := s.registry.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get component")
		return
	}
	for _, d := range defs {
		if d.Name == name {
			writeJSON(w, http.StatusOK, componentDetailToJSON(d))
			return
		}
	}
	writeError(w, http.StatusNotFound, "component not found")
}
