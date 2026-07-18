package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/util/validation"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/config"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"
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

// pipelineDetailJSON is the full shape returned by Get. Doc is the raw
// canonical-JSON PipelineDoc (RFC 027 §5.1). This is the default (JSON)
// representation; GET ?format=yaml renders the doc as YAML instead (S6).
type pipelineDetailJSON struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Doc       json.RawMessage `json:"doc"`
	CreatedAt string          `json:"created_at"`
	UpdatedAt string          `json:"updated_at"`
}

// pipelineRefJSON is the summary shape returned by List. Description is the
// doc's top-level description, surfaced here so the catalog can show it without
// fetching each doc (RFC 027 §5.2). Timestamps are omitted when zero so local
// mode (which doesn't stat files on list) doesn't emit a misleading
// 0001-01-01 value.
type pipelineRefJSON struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	CreatedAt   string `json:"created_at,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
}

const pipelineTimeLayout = "2006-01-02T15:04:05Z07:00"

func pipelineDetailToJSON(p PipelineDetail) pipelineDetailJSON {
	return pipelineDetailJSON{
		ID: p.ID, Name: p.Name, Doc: p.Doc,
		CreatedAt: p.CreatedAt,
		UpdatedAt: p.UpdatedAt,
	}
}

func pipelineRefToJSON(p PipelineRef) pipelineRefJSON {
	out := pipelineRefJSON{Name: p.Name, Description: p.Description}
	if !p.CreatedAt.IsZero() {
		out.CreatedAt = p.CreatedAt.Format(pipelineTimeLayout)
	}
	if !p.UpdatedAt.IsZero() {
		out.UpdatedAt = p.UpdatedAt.Format(pipelineTimeLayout)
	}
	return out
}

func (s *Server) handlePutPipeline(w http.ResponseWriter, r *http.Request) {
	userID, projectID, ok := s.mustHaveRelation(w, r, "data_admin")
	if !ok {
		return
	}
	name := r.PathValue("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "pipeline name required")
		return
	}
	// "validate" is reserved: POST /api/v1/projects/{pid}/pipelines/validate
	// is the S7 validate-without-save endpoint. It can never collide with
	// this route (different HTTP method), but a pipeline literally named
	// "validate" would still be a confusing footgun, so it's rejected here
	// with the same findings-shaped 400 the rest of PUT's early checks use.
	if name == "validate" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "validation failed",
			"findings": []validate.Finding{{
				Path:     "name",
				Message:  `pipeline name "validate" is reserved`,
				Severity: "error",
			}},
		})
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
	// Content-Type gates strictness: application/json means the body must be
	// valid JSON syntax; anything else (or absent) is treated as YAML, which
	// is what ValidatePipelineDoc/config.Parse already do below. Left
	// unchecked, a JSON-content-typed body that is valid YAML but not valid
	// JSON (e.g. an unquoted flow-style mapping) would silently pass, since
	// config.Parse's YAML decoder accepts JSON as a subset — this check
	// rejects that case before it ever reaches config.Parse.
	if mediaType, _, mtErr := mime.ParseMediaType(r.Header.Get("Content-Type")); mtErr == nil && mediaType == "application/json" {
		if !json.Valid(body) {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error": "validation failed",
				"findings": []validate.Finding{{
					Message:  "Content-Type is application/json but the body is not valid JSON",
					Severity: "error",
				}},
			})
			return
		}
	}
	// s.registry is nil when WithRegistry hasn't been wired; ValidatePipelineDoc
	// treats a nil RegistryView as "skip resolution" (see R5), so this stays
	// a soft-degrade rather than a nil-deref. Validate with pol=nil here — the
	// identity-correct policy is applied in the diff-gate below.
	pl, findings := validate.ValidatePipelineDoc(body, name, s.registry, nil)
	// A strict-decode failure yields pl==nil with a single error finding;
	// surface it with the findings contract before touching the diff-gate.
	if pl == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "validation failed", "findings": findings,
		})
		return
	}
	// The doc's own name (if set) must match the URL name: otherwise
	// ApplyPipelineCRD at run-trigger time would create a CRD under the
	// doc's name while the PipelineRun would reference the URL name, and
	// the operator would fail to find the Pipeline. ValidatePipelineDoc
	// already folds this into `findings` as an error Finding, but that
	// check runs after the diff-gate below (which can 403 first) — keep
	// this fast, early-returning duplicate so a name mismatch always wins
	// with a clear 400, matching the pre-RFC-027 behavior.
	if pl.Name != "" && pl.Name != name {
		writeError(w, http.StatusBadRequest,
			fmt.Sprintf("pipeline name %q does not match URL name %q", pl.Name, name))
		return
	}

	// RFC 026 §4.4 resources/gateway diff-gate. A non-superadmin may not
	// add/alter/remove any component `resources` block, nor exceed a gateway
	// bound; either is a 403 pointing at superadmin. A superadmin bypasses both
	// the modification gate and the gateway bounds (validating with pol=nil),
	// but the registry `Max` ceiling still applies to everyone — over-max
	// findings come from the registry via ValidateTyped, not the policy.
	// Precedence: the 403 gates run BEFORE surfacing over-max/other findings as
	// 400, so an unprivileged resources edit gets the clear "superadmin
	// required" 403 rather than a confusing over-max 400.
	isSuperadmin, ok := s.isRequestSuperadmin(w, r, userID)
	if !ok {
		return // 503/500 already written
	}
	if !isSuperadmin {
		oldP, err := s.storedPipelineSpec(r.Context(), projectID, name)
		if err != nil {
			// Fail closed: without the stored spec we cannot verify the
			// diff-gate. Defaulting to "no old resources" on a transient read
			// error would let a non-superadmin strip (or edit) an admin-set
			// resources block through. 503 is retryable.
			writeError(w, http.StatusServiceUnavailable,
				"could not read current pipeline to verify the resources gate")
			return
		}
		if resourcesModified(oldP, pl) {
			writeError(w, http.StatusForbidden,
				"modifying component resources requires superadmin privileges")
			return
		}
		// Re-validate against the policy so gateway-bound violations surface;
		// reject any as 403 for a non-superadmin (before the 400 findings path).
		findings = validate.ValidateTyped(pl, s.registry, s.policy)
		for _, f := range findings {
			if f.Severity == "error" && strings.HasPrefix(f.Path, "gateway.") {
				writeError(w, http.StatusForbidden,
					"modifying gateway settings beyond policy bounds requires superadmin privileges")
				return
			}
		}
	}
	// Split findings by severity: only ERROR-severity findings (resource-over-max,
	// other validation) block the save with a 400 — checked AFTER the 403 gates
	// per the precedence above. WARNING findings (e.g. a deprecated-but-resolvable
	// component) never block a save; they ride along in the final 200 next to the
	// missing-secret warnings. findings holds the pol=nil parse result for a
	// superadmin and the policy-applied result for a non-superadmin.
	var warnFindings, errFindings []validate.Finding
	for _, f := range findings {
		if f.Severity == "error" {
			errFindings = append(errFindings, f)
		} else {
			warnFindings = append(warnFindings, f)
		}
	}
	if len(errFindings) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "validation failed", "findings": findings,
		})
		return
	}

	// Store the canonical doc, not the raw request body: the body may be
	// YAML (or JSON with arbitrary formatting), while the stored `doc`
	// column is canonical JSON (RFC 027 §5.1). config.Parse can't fail
	// here — ValidatePipelineDoc already proved body decodes cleanly —
	// but errors are handled rather than ignored.
	doc, err := config.Parse(body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "re-parse pipeline")
		return
	}
	canonical, err := json.Marshal(doc)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "encode pipeline")
		return
	}

	// PUT is upsert — pgx adapter tries UPDATE first and falls back to
	// INSERT. No conflict path today.
	if err := s.pipelines.Put(r.Context(), projectID, name, canonical, doc.Description); err != nil {
		writeError(w, http.StatusInternalServerError, "put pipeline")
		return
	}

	// Secrets save-warn ladder (RFC 026 P1.5 §7): the pipeline is saved
	// regardless — a missing $[key] is a warning here, not a hard failure.
	// Trigger time (handleTriggerRun) is where a missing key hard-rejects.
	refs := validate.ReferencedSecrets(pl)
	missing, err := s.missingSecretRefs(r.Context(), projectID, refs)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "check secrets")
		return
	}
	// Merge the validation warnings kept from the split above with the
	// missing-secret warnings; neither source blocks the save. A non-empty
	// combined set is a 200-with-findings, otherwise a clean 204.
	warnings := warnFindings
	for _, key := range missing {
		warnings = append(warnings, validate.Finding{
			Path:     "secrets." + key,
			Message:  fmt.Sprintf("secret %q is referenced but not yet set in this project's secret store", key),
			Severity: "warning",
		})
	}
	if len(warnings) > 0 {
		writeJSON(w, http.StatusOK, map[string]any{"findings": warnings})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleValidatePipeline runs the full PUT-time validation without
// persisting (RFC 027 §5.2). Status contract: 200 {"findings":[...]} for
// every readable body — validation outcomes (errors and warnings alike) are
// findings, never HTTP errors; 400/413 stay reserved for a body that cannot
// be read/decoded as a PipelineDoc at all or exceeds the size cap; 5xx is
// reserved for infrastructure failures (the store read backing the resource
// gate, or the superadmin check). Same authz as PUT.
//
// name is optional (?name=): when given, the resource/gateway diff-gate below
// loads the stored doc under that name and diffs against it, mirroring PUT;
// when absent (or nothing stored under it), the diff-gate validates with
// create semantics (any component resources from a non-superadmin is a
// finding, same as PUT's first-create path).
func (s *Server) handleValidatePipeline(w http.ResponseWriter, r *http.Request) {
	userID, projectID, ok := s.mustHaveRelation(w, r, "data_admin")
	if !ok {
		return
	}
	name := r.URL.Query().Get("name")

	// Same body-read pattern as PUT (S6): 1 MiB cap, and a declared
	// Content-Type of application/json must actually be valid JSON. Both are
	// "unreadable body" cases — this handler's only 413/400-for-unreadable
	// paths — everything past this point becomes a Finding, never an HTTP
	// error.
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "body exceeds 1 MiB: "+err.Error())
		return
	}
	if mediaType, _, mtErr := mime.ParseMediaType(r.Header.Get("Content-Type")); mtErr == nil && mediaType == "application/json" {
		if !json.Valid(body) {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error": "validation failed",
				"findings": []validate.Finding{{
					Message:  "Content-Type is application/json but the body is not valid JSON",
					Severity: "error",
				}},
			})
			return
		}
	}

	pl, findings := validate.ValidatePipelineDoc(body, name, s.registry, nil)
	// pl is nil only when the body could not be decoded as a PipelineDoc at
	// all (e.g. the legacy Kubernetes CR envelope, or plain invalid
	// YAML/JSON syntax) — the one case that stays an "unreadable body" 400
	// rather than a finding; every other outcome (name mismatch, unknown
	// component, over-max resources, ...) rides in `findings` below with a
	// 200, per the status contract.
	if pl == nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "validation failed", "findings": findings,
		})
		return
	}

	// RFC 026 §4.4 resources/gateway diff-gate, mirrored from PUT — but
	// surfaced as a Finding rather than a 403: validate never returns an HTTP
	// error for a validation outcome.
	isSuperadmin, ok := s.isRequestSuperadmin(w, r, userID)
	if !ok {
		return // 503/500 already written
	}
	if !isSuperadmin {
		oldP := &datupletv1.Pipeline{}
		if name != "" {
			oldP, err = s.storedPipelineSpec(r.Context(), projectID, name)
			if err != nil {
				// Fail closed, same rationale as PUT's diff-gate: without the
				// stored spec the resource-gate finding below can't be trusted.
				http.Error(w, "could not read current pipeline to verify the resources gate", http.StatusInternalServerError)
				return
			}
		}
		modified := resourcesModified(oldP, pl)
		// Re-run the same doc through the policy-applied ruleset so
		// gateway-bound violations surface for a non-superadmin too (PUT
		// rejects these with 403; here they ride along as ordinary error
		// findings instead). PUT gets away with a plain ValidateTyped
		// re-check here because it already early-returns on a name-format or
		// name-mismatch problem before ever reaching the diff-gate; validate
		// never early-returns on a readable body, so re-deriving via
		// ValidateTyped alone would silently drop those two ValidatePipelineDoc-
		// only findings. Re-running ValidatePipelineDoc instead keeps them.
		_, findings = validate.ValidatePipelineDoc(body, name, s.registry, s.policy)
		if modified {
			findings = append(findings, validate.Finding{
				Message:  "modifying component resources requires superadmin privileges",
				Severity: "error",
			})
		}
	}

	if findings == nil {
		findings = []validate.Finding{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"findings": findings})
}

// isRequestSuperadmin reports whether the request's user is a platform
// superadmin for the diff-gate. A nil checker (authz disabled) is treated as
// "not superadmin". On an authz-backend outage it writes 503; on an unexpected
// error it writes 500 — both return ok=false so the caller returns immediately.
// (RFC 026 §4.4.)
func (s *Server) isRequestSuperadmin(w http.ResponseWriter, r *http.Request, userID uuid.UUID) (isAdmin, ok bool) {
	if s.serverAdmin == nil {
		return false, true
	}
	admin, err := s.serverAdmin.IsServerAdmin(r.Context(), userID.String())
	if errors.Is(err, authz.ErrAuthzUnavailable) {
		writeError(w, http.StatusServiceUnavailable, "authz backend unavailable")
		return false, false
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "check superadmin")
		return false, false
	}
	return admin, true
}

// storedPipelineSpec loads the currently-stored pipeline and parses it into a
// typed spec for the diff-gate.
//
//   - Missing pipeline (first create): empty spec, nil error — a legitimate
//     "no old resources".
//   - Any other read error: propagated so the caller can FAIL CLOSED. A
//     transient read must not default to "no resources" — that would let a
//     non-superadmin strip an admin-set block through on a flake (RFC 026 §4.4).
//   - Unparseable stored doc: empty spec, nil error (logged). Stored docs
//     always passed validation on their way in, so this is near-impossible;
//     treat-as-none rather than blocking a save on it.
func (s *Server) storedPipelineSpec(ctx context.Context, projectID uuid.UUID, name string) (*datupletv1.Pipeline, error) {
	stored, err := s.pipelines.Get(ctx, projectID, name)
	switch {
	case errors.Is(err, errStoreNotFound):
		return &datupletv1.Pipeline{}, nil
	case err != nil:
		return nil, err
	}
	p, _ := validate.ValidatePipelineDoc(stored.Doc, name, s.registry, nil)
	if p == nil {
		log.Printf("put-pipeline: stored doc for %s/%s failed to parse; treating old resources as none", projectID, name)
		return &datupletv1.Pipeline{}, nil
	}
	return p, nil
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
	p, err := s.pipelines.Get(r.Context(), projectID, name)
	if errors.Is(err, errStoreNotFound) {
		writeError(w, http.StatusNotFound, "pipeline not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get pipeline")
		return
	}
	// ?format=yaml renders the stored canonical-JSON doc back to deterministic
	// YAML for human editing (RFC 027 §5.2). The stored doc always passed
	// config.Parse on its way in, so these re-parse/render steps can't fail in
	// practice — errors are surfaced as 500 rather than ignored.
	if r.URL.Query().Get("format") == "yaml" {
		doc, err := config.Parse(p.Doc)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "parse stored pipeline")
			return
		}
		out, err := config.RenderYAML(doc)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "render pipeline yaml")
			return
		}
		w.Header().Set("Content-Type", "text/yaml; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(out)
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
