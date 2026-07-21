// Package framework — pipeline-api HTTP run-driving helpers.
//
// RFC 027 E2 migrated the K8s tier's central run primitive
// (K8sBackend.RunPipeline) off the hand-crafted-CR + kubectl-apply path and
// onto the same public REST surface a real user drives: authenticate →
// PUT the PipelineDoc → POST a run → poll the run to a terminal phase. The
// low-level request builders (login / PUT / trigger / poll) live here so they
// sit in package `framework` alongside RunPipeline (the test package's own
// copies in scenarios_*_test.go cannot be reached from here without an import
// cycle).
//
// pipeline-api owns run identity now: its trigger path mints the per-run JWT,
// writes the synthetic-run-user FGA tuple, and creates the PipelineRun CR. The
// harness therefore no longer mints tokens, writes tuples, or applies CRs
// itself — it only speaks HTTP as an authenticated user.
package framework

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// apiSessionCookieName is the session cookie pipeline-api sets on login.
const apiSessionCookieName = "pipeline_api_session"

// e2e POC default DB admin, seeded by scripts/register.sh. Alice maps to this
// account (the sole DB user register.sh seeds); Bob is provisioned on demand as
// a real DB login user (bobLogin* below) the first time an fga-matrix scenario
// drives the HTTP run path as him. Production installs override these.
const (
	e2eAdminEmail    = "admin@datuplet.local"
	e2eAdminPassword = "changeme"

	// e2eBobLogin* is the real DB login user provisioned for the
	// fga-matrix-bob scenario. K8sBackend.apiSessionFor create-users it (via
	// `pipeline-api admin create-user`, idempotent) the first time Bob's
	// identity drives a run, then logs in and seeds Bob's REAL minted UUID with
	// the `editor` relation SetupFGABootstrap grants the fixed BobID. See the
	// FGA-identity note in K8sBackend.apiSessionFor.
	e2eBobLoginEmail    = "e2e-bob-login@datuplet.local"
	e2eBobLoginPassword = "changeme-bob"
)

// apiCredsFor maps a framework TestUser identity to the pipeline-api login
// credentials the run authenticates with.
//
// Production run-identity is the authenticated caller, so a per-user authz
// scenario must drive the API AS that user. Alice maps to the seeded DB admin
// (the account register.sh creates); Bob maps to a dedicated DB login user the
// backend provisions on first use. Charlie/Dora are not exercised over the HTTP
// run path (their negative-authz paths are covered at the unit level — see the
// fga-matrix note in scenarios_test.go), so this returns ok=false for them. The
// mechanism is deliberately a lookup table so adding a row is the only change
// needed to extend it.
func apiCredsFor(userID uuid.UUID) (email, password string, ok bool) {
	switch userID {
	case AliceID:
		return e2eAdminEmail, e2eAdminPassword, true
	case BobID:
		return e2eBobLoginEmail, e2eBobLoginPassword, true
	default:
		return "", "", false
	}
}

// apiLogin POSTs /api/v1/auth/login and returns the session cookie value.
// pipeline-api answers 204 (cookie in Set-Cookie, no body) or 200.
func apiLogin(ctx context.Context, baseURL, email, password string) (string, error) {
	body, err := json.Marshal(map[string]string{"email": email, "password": password})
	if err != nil {
		return "", err
	}
	u := strings.TrimRight(baseURL, "/") + "/api/v1/auth/login"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return "", fmt.Errorf("POST %s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("login HTTP %d: %s", resp.StatusCode, string(b))
	}
	for _, c := range resp.Cookies() {
		if c.Name == apiSessionCookieName {
			return c.Value, nil
		}
	}
	return "", fmt.Errorf("no %s cookie in login response", apiSessionCookieName)
}

// apiPutPipelineDoc PUTs an envelope-free PipelineDoc (YAML bytes) to
// /api/v1/projects/{pid}/pipelines/{name}. A save may answer 204 (clean) or
// 200 (accepted with non-blocking warning findings) — both are success.
func apiPutPipelineDoc(ctx context.Context, baseURL, cookie, projectID, name string, doc []byte) error {
	u := fmt.Sprintf("%s/api/v1/projects/%s/pipelines/%s", strings.TrimRight(baseURL, "/"), projectID, name)
	status, body, err := apiDo(ctx, http.MethodPut, u, cookie, "application/yaml", doc)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusNoContent {
		return fmt.Errorf("PUT pipeline %q: status=%d body=%s", name, status, truncate(body, 512))
	}
	return nil
}

// apiTriggerRun POSTs /api/v1/projects/{pid}/pipelines/{name}/runs and returns
// the minted run id. pipeline-api answers 201 with {"id": "<uuid>", ...}.
func apiTriggerRun(ctx context.Context, baseURL, cookie, projectID, name string) (uuid.UUID, error) {
	u := fmt.Sprintf("%s/api/v1/projects/%s/pipelines/%s/runs", strings.TrimRight(baseURL, "/"), projectID, name)
	status, body, err := apiDo(ctx, http.MethodPost, u, cookie, "application/json", []byte("{}"))
	if err != nil {
		return uuid.Nil, err
	}
	if status != http.StatusCreated {
		return uuid.Nil, fmt.Errorf("trigger run %q: status=%d body=%s", name, status, truncate(body, 512))
	}
	var resp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return uuid.Nil, fmt.Errorf("decode trigger response: %w (body=%s)", err, truncate(body, 256))
	}
	id, err := uuid.Parse(resp.ID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("parse run id %q: %w", resp.ID, err)
	}
	return id, nil
}

// apiRunView is the subset of GET /api/v1/projects/{pid}/runs/{id} the harness
// polls (pkg/pipelineapi/http/run_handlers.go runJSON).
type apiRunView struct {
	Phase   string `json:"phase"`
	Message string `json:"message"`
}

// apiGetRun fetches a run's current phase + message. A 404 (run row not yet
// visible) is surfaced as an empty phase with no error so the caller keeps
// polling, mirroring the old kubectl "resource may not exist yet" tolerance.
func apiGetRun(ctx context.Context, baseURL, cookie, projectID, runID string) (apiRunView, error) {
	u := fmt.Sprintf("%s/api/v1/projects/%s/runs/%s", strings.TrimRight(baseURL, "/"), projectID, runID)
	status, body, err := apiDo(ctx, http.MethodGet, u, cookie, "", nil)
	if err != nil {
		return apiRunView{}, err
	}
	if status == http.StatusNotFound {
		return apiRunView{}, nil
	}
	if status != http.StatusOK {
		return apiRunView{}, fmt.Errorf("get run %s: status=%d body=%s", runID, status, truncate(body, 512))
	}
	var v apiRunView
	if err := json.Unmarshal(body, &v); err != nil {
		return apiRunView{}, fmt.Errorf("decode run view: %w (body=%s)", err, truncate(body, 256))
	}
	return v, nil
}

// apiDeletePipeline best-effort deletes a stored pipeline (used at cleanup so
// repeated runs of the same fixture under the same project don't collide).
func apiDeletePipeline(ctx context.Context, baseURL, cookie, projectID, name string) error {
	u := fmt.Sprintf("%s/api/v1/projects/%s/pipelines/%s", strings.TrimRight(baseURL, "/"), projectID, name)
	status, body, err := apiDo(ctx, http.MethodDelete, u, cookie, "", nil)
	if err != nil {
		return err
	}
	if status != http.StatusNoContent && status != http.StatusOK && status != http.StatusNotFound {
		return fmt.Errorf("delete pipeline %q: status=%d body=%s", name, status, truncate(body, 256))
	}
	return nil
}

// apiFindProjectIDByName lists the caller's projects (GET /api/v1/projects) and
// returns the Datuplet projects-store UUID whose name matches. This is the
// value pipeline-api resolves {pid} against (projects.GetByID) — NOT the
// lakekeeper project UUID. Mirrors the test package's remoteCLIFindProject:
// exact-name match, single-project fallback. Call with an ADMIN session — a
// non-admin caller only sees projects it holds a relation on.
func apiFindProjectIDByName(ctx context.Context, baseURL, cookie, name string) (string, error) {
	u := strings.TrimRight(baseURL, "/") + "/api/v1/projects"
	status, body, err := apiDo(ctx, http.MethodGet, u, cookie, "", nil)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("list projects: status=%d body=%s", status, truncate(body, 256))
	}
	var projects []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(body, &projects); err != nil {
		return "", fmt.Errorf("decode projects: %w (body=%s)", err, truncate(body, 256))
	}
	for _, p := range projects {
		if p.Name == name {
			return p.ID, nil
		}
	}
	if len(projects) == 1 {
		return projects[0].ID, nil
	}
	return "", fmt.Errorf("project %q not found in %d projects", name, len(projects))
}

// apiMeUserID returns the authenticated caller's REAL DB user UUID via
// GET /api/v1/auth/me. Login answers 204 with no body, so /me is the only path
// that surfaces the minted UUID — and that UUID (not the fixed TestUser UUID)
// is the `sub` the run-trigger's mustHaveRelation FGA check runs against.
func apiMeUserID(ctx context.Context, baseURL, cookie string) (string, error) {
	u := strings.TrimRight(baseURL, "/") + "/api/v1/auth/me"
	status, body, err := apiDo(ctx, http.MethodGet, u, cookie, "", nil)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("GET /auth/me: status=%d body=%s", status, truncate(body, 256))
	}
	var me struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &me); err != nil {
		return "", fmt.Errorf("decode /auth/me: %w (body=%s)", err, truncate(body, 256))
	}
	if me.ID == "" {
		return "", fmt.Errorf("/auth/me returned empty id")
	}
	return me.ID, nil
}

// apiDo issues one authenticated request and returns (status, body).
func apiDo(ctx context.Context, method, url, cookie, contentType string, body []byte) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return 0, nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if cookie != "" {
		req.AddCookie(&http.Cookie{Name: apiSessionCookieName, Value: cookie})
	}
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("%s %s: %w", method, url, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, respBody, nil
}

// truncate bounds a body snippet for error messages.
func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}
