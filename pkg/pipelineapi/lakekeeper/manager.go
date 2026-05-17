// Package lakekeeper wraps the subset of lakekeeper's REST management API
// (POST /management/v1/project, DELETE /management/v1/project/{id},
// GET /management/v1/project-list, etc.) that pipeline-api needs to keep
// 1:1 parity between Datuplet projects and lakekeeper Projects.
//
// All calls require a JWT — pipeline-api's signing keypair issues a
// short-lived service token via tokens.MintServiceToken. The Manager
// constructor takes a token-minter func rather than the signer directly,
// so callers can inject test minters and so the package never imports
// pkg/pipelineapi/tokens (avoids a possible circular import via the
// authz reconciler).
package lakekeeper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// TokenMinter mints a fresh service-account JWT for a single management
// call. Implementations are typically a closure over pipeline-api's
// keypair calling tokens.MintServiceToken — see
// ensureLocalProjectAuthz (cmd/pipeline-api/main.go) and the admin
// bootstrap subcommands for the canonical shape (5-minute lifetime,
// sub="pipeline-api-bootstrap" so lakekeeper grants project_admin on
// projects this minter creates). Note: storage browse no longer uses
// this — it uses per-user impersonation tokens instead
// (pkg/pipelineapi/storage/service.go::Minter).
type TokenMinter func() (string, error)

// Manager is a thin client for lakekeeper's REST management API.
//
// Safe for concurrent use — the underlying http.Client is safe and the
// TokenMinter is required to be safe by contract (callers minting RS256
// JWTs from a single Signer satisfy this trivially).
type Manager struct {
	baseURL string      // trimmed of trailing /
	minter  TokenMinter // mints a fresh JWT per call
	httpc   *http.Client
}

// New constructs a Manager. baseURL is the lakekeeper management base
// (e.g. "http://lakekeeper.lakekeeper.svc.cluster.local:8181"); the
// trailing /management/v1 prefix is appended internally so callers don't
// have to hard-code it.
//
// minter must be non-nil — every management call carries a fresh Bearer
// token. Returning an error from the minter is treated as the call
// failing.
//
// timeout is the per-HTTP-request deadline; 0 falls back to 30s.
func New(baseURL string, minter TokenMinter, timeout time.Duration) (*Manager, error) {
	if baseURL == "" {
		return nil, errors.New("lakekeeper: baseURL is required")
	}
	if minter == nil {
		return nil, errors.New("lakekeeper: minter is required")
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	// The shared DATUPLET_LAKEKEEPER_URL env carries the iceberg REST
	// prefix `/catalog` so the storage proxy can talk catalog endpoints.
	// The management API lives at the bare root — trim the suffix so
	// callers can reuse the same env var without hitting a 404.
	trimmed := strings.TrimRight(baseURL, "/")
	trimmed = strings.TrimSuffix(trimmed, "/catalog")
	return &Manager{
		baseURL: trimmed,
		minter:  minter,
		httpc:   &http.Client{Timeout: timeout},
	}, nil
}

// projectInfo mirrors lakekeeper's GetProjectResponse / ListProjectsResponse
// item shape.
type projectInfo struct {
	ID   string `json:"project-id"`
	Name string `json:"project-name"`
}

// CreateProject POSTs /management/v1/project with the given name and
// returns the lakekeeper-allocated project UUID.
//
// The call is NOT idempotent on `name` at the lakekeeper side — re-posting
// the same name returns a 409. Callers (admin create-project +
// EnsureProjectAuthz) probe FindProjectIDByName first; CreateProject
// itself just performs the raw POST.
func (m *Manager) CreateProject(ctx context.Context, name string) (string, error) {
	if name == "" {
		return "", errors.New("CreateProject: name is required")
	}
	body := map[string]any{"project-name": name}
	var resp projectInfo
	if err := m.do(ctx, http.MethodPost, "/management/v1/project", body, http.StatusCreated, &resp); err != nil {
		return "", fmt.Errorf("create project %q: %w", name, err)
	}
	if resp.ID == "" {
		return "", errors.New("create project: lakekeeper returned empty project-id")
	}
	return resp.ID, nil
}

// DeleteProject DELETEs /management/v1/project/{id}. The endpoint is
// marked deprecated in lakekeeper's OpenAPI in favour of
// `DELETE /management/v1/project` + `x-project-id` header, but the
// path-based form is still accepted and is more ergonomic; we'll switch
// when lakekeeper drops it.
//
// Returns nil on 204 (success) AND on 404 (already gone) so callers can
// re-run delete safely. Other 4xx/5xx propagate as errors.
func (m *Manager) DeleteProject(ctx context.Context, projectID string) error {
	if projectID == "" {
		return errors.New("DeleteProject: projectID is required")
	}
	path := "/management/v1/project/" + projectID
	if err := m.do(ctx, http.MethodDelete, path, nil, http.StatusNoContent, nil); err != nil {
		// Idempotent: a missing project is a successful delete.
		if isNotFoundErr(err) {
			return nil
		}
		return fmt.Errorf("delete project %q: %w", projectID, err)
	}
	return nil
}

// FindProjectIDByName scans /management/v1/project-list and returns the
// project-id whose project-name matches `name`, or "" if none. Used by
// the admin create-project flow + EnsureProjectAuthz to detect "already
// provisioned" without relying on lakekeeper to error on duplicate POSTs.
//
// The list endpoint is paginated only above a few thousand projects;
// Datuplet's per-customer scale puts us comfortably below that, so we
// don't paginate here. If we ever cross that threshold the fix is local
// — wrap this call.
func (m *Manager) FindProjectIDByName(ctx context.Context, name string) (string, error) {
	if name == "" {
		return "", errors.New("FindProjectIDByName: name is required")
	}
	var resp struct {
		Projects []projectInfo `json:"projects"`
	}
	if err := m.do(ctx, http.MethodGet, "/management/v1/project-list", nil, http.StatusOK, &resp); err != nil {
		return "", fmt.Errorf("list projects: %w", err)
	}
	for _, p := range resp.Projects {
		if p.Name == name {
			return p.ID, nil
		}
	}
	return "", nil
}

// ProjectExists probes /management/v1/project/{id} and returns true iff
// lakekeeper has a project with that ID we can read. Used by
// EnsureProjectAuthz to detect the "Postgres has the id but lakekeeper
// lost it" edge case (e.g. lakekeeper's DB was wiped).
//
// 404 → false (project doesn't exist). 403 → false (we don't have
// `get_metadata` on it, which from this bootstrap's perspective means the
// recorded ID is no longer ours — almost always the wiped-pg /
// re-bootstrapped case, since the bootstrap admin owns every project it
// creates). Treating 403 as missing lets EnsureProjectAuthz re-allocate
// rather than wedging on a stale UUID; the alternative is forcing the
// operator to manually `rm project.json`.
func (m *Manager) ProjectExists(ctx context.Context, projectID string) (bool, error) {
	if projectID == "" {
		return false, errors.New("ProjectExists: projectID is required")
	}
	path := "/management/v1/project/" + projectID
	err := m.do(ctx, http.MethodGet, path, nil, http.StatusOK, nil)
	if err == nil {
		return true, nil
	}
	if isNotFoundErr(err) || isForbiddenErr(err) {
		return false, nil
	}
	return false, fmt.Errorf("get project %q: %w", projectID, err)
}

// S3WarehouseProfile carries the storage-profile + storage-credential
// body lakekeeper expects on POST /management/v1/warehouse. Mirrors the
// shape used by `pipeline-api admin lakekeeper-bootstrap` — see
// cmd/pipeline-api/admin_lakekeeper.go for the canonical example.
//
// Bucket is the S3 bucket that holds the warehouse data. Endpoint is the
// fully-qualified URL of the S3-compatible API (must be reachable from
// the lakekeeper container itself, not just the host). AccessKey /
// SecretKey are the S3 credentials lakekeeper uses both directly and as
// the source of STS-vended short-lived credentials.
//
// Region defaults to "local-01" (matching the bootstrap default) and
// PathStyle defaults to true (MinIO requires it).
type S3WarehouseProfile struct {
	Bucket    string
	Endpoint  string
	AccessKey string
	SecretKey string
	Region    string // optional; defaults to "local-01"
	PathStyle *bool  // optional; defaults to true (MinIO)
}

// EnsureWarehouseInProject creates the named warehouse inside the given
// lakekeeper Project, idempotently. The call is idempotent — if the
// warehouse already exists the method returns nil without re-creating it.
//
// The probe-first pattern matches FindProjectIDByName: a 200 with the
// warehouse name in the listing returns nil; otherwise we POST. On
// HTTP failure the method propagates the error verbatim — no silent
// fallbacks — so a missing storage profile surfaces in the bootstrap
// log loud.
//
// All calls are scoped via the `x-project-id` HTTP header so lakekeeper
// routes them to the requested project rather than the default one.
func (m *Manager) EnsureWarehouseInProject(ctx context.Context, projectID, warehouseName string, profile S3WarehouseProfile) error {
	if projectID == "" || warehouseName == "" {
		return errors.New("EnsureWarehouseInProject: projectID and warehouseName are required")
	}
	if profile.Bucket == "" || profile.Endpoint == "" || profile.AccessKey == "" || profile.SecretKey == "" {
		return errors.New("EnsureWarehouseInProject: S3WarehouseProfile.Bucket / Endpoint / AccessKey / SecretKey are required")
	}
	// Probe: GET /management/v1/warehouse with x-project-id header,
	// match warehouse-name. lakekeeper scopes the list by project when
	// the header is present.
	probe, err := m.listWarehousesInProject(ctx, projectID)
	if err != nil {
		return fmt.Errorf("list warehouses in project %q: %w", projectID, err)
	}
	for _, w := range probe {
		if w == warehouseName {
			return nil
		}
	}

	region := profile.Region
	if region == "" {
		region = "local-01"
	}
	pathStyle := true
	if profile.PathStyle != nil {
		pathStyle = *profile.PathStyle
	}
	body := map[string]any{
		"warehouse-name": warehouseName,
		"project-id":     projectID,
		"storage-profile": map[string]any{
			"type":              "s3",
			"bucket":            profile.Bucket,
			"region":            region,
			"sts-enabled":       true,
			"flavor":            "s3-compat",
			"endpoint":          profile.Endpoint,
			"path-style-access": pathStyle,
		},
		"storage-credential": map[string]any{
			"type":                  "s3",
			"credential-type":       "access-key",
			"aws-access-key-id":     profile.AccessKey,
			"aws-secret-access-key": profile.SecretKey,
		},
		"delete-profile": map[string]any{"type": "hard"},
	}
	return m.createWarehouseInProject(ctx, projectID, body)
}

// createWarehouseInProject POSTs the create body with x-project-id header.
// Surfaced as a method (not a doInProject helper) because the do() shape
// doesn't expose request-header customisation; cleaner to keep the small
// duplicate than refactor every consumer of do().
func (m *Manager) createWarehouseInProject(ctx context.Context, projectID string, body map[string]any) error {
	jwt, err := m.minter()
	if err != nil {
		return fmt.Errorf("mint token: %w", err)
	}
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		return fmt.Errorf("encode body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.baseURL+"/management/v1/warehouse", buf)
	if err != nil {
		return err
	}
	req.Header.Set("authorization", "Bearer "+jwt)
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-project-id", projectID)
	resp, err := m.httpc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create warehouse in project %q: HTTP %d: %s", projectID, resp.StatusCode, string(b))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (m *Manager) listWarehousesInProject(ctx context.Context, projectID string) ([]string, error) {
	jwt, err := m.minter()
	if err != nil {
		return nil, fmt.Errorf("mint token: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.baseURL+"/management/v1/warehouse", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("authorization", "Bearer "+jwt)
	req.Header.Set("x-project-id", projectID)
	resp, err := m.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(b))
	}
	var out struct {
		Warehouses []struct {
			Name string `json:"name"`
		} `json:"warehouses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode warehouses: %w", err)
	}
	names := make([]string, 0, len(out.Warehouses))
	for _, w := range out.Warehouses {
		names = append(names, w.Name)
	}
	return names, nil
}

// httpStatusError carries the HTTP status from a non-success response so
// callers can distinguish "not found" (acceptable for delete/probe) from
// real failures.
type httpStatusError struct {
	Status int
	Body   string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.Status, e.Body)
}

func isNotFoundErr(err error) bool {
	var se *httpStatusError
	return errors.As(err, &se) && se.Status == http.StatusNotFound
}

// isForbiddenErr returns true on a 403 from lakekeeper. Used by
// ProjectExists to treat "I don't have access" as "doesn't exist for our
// purposes" — see ProjectExists doc for the rationale.
func isForbiddenErr(err error) bool {
	var se *httpStatusError
	return errors.As(err, &se) && se.Status == http.StatusForbidden
}

// do executes a JSON-bodied request against lakekeeper's management API.
// On non-`expect` status, returns *httpStatusError so callers can
// distinguish 404 from other failures via isNotFoundErr.
//
// out, when non-nil, is JSON-decoded from a successful response body.
// expect is the success status code (typically 200, 201, 204).
func (m *Manager) do(ctx context.Context, method, path string, body any, expect int, out any) error {
	jwt, err := m.minter()
	if err != nil {
		return fmt.Errorf("mint token: %w", err)
	}

	var reqBody io.Reader
	if body != nil {
		buf := &bytes.Buffer{}
		if err := json.NewEncoder(buf).Encode(body); err != nil {
			return fmt.Errorf("encode body: %w", err)
		}
		reqBody = buf
	}

	req, err := http.NewRequestWithContext(ctx, method, m.baseURL+path, reqBody)
	if err != nil {
		return err
	}
	req.Header.Set("authorization", "Bearer "+jwt)
	if body != nil {
		req.Header.Set("content-type", "application/json")
	}

	resp, err := m.httpc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != expect {
		b, _ := io.ReadAll(resp.Body)
		return &httpStatusError{Status: resp.StatusCode, Body: string(b)}
	}

	if out == nil || resp.StatusCode == http.StatusNoContent {
		// Drain the body so the connection can be reused.
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}
