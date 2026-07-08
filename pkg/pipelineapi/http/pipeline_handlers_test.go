package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	stdhttp "net/http"
	"strings"
	"testing"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// findingsBody is the wire shape of a 400 validation-failure response from
// PUT /pipelines: {"error":"validation failed","findings":[{path,message,severity}]}.
type findingsBody struct {
	Error    string `json:"error"`
	Findings []struct {
		Path     string `json:"path"`
		Message  string `json:"message"`
		Severity string `json:"severity"`
	} `json:"findings"`
}

const validPipelineYAML = `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: etl
spec:
  stages:
    - name: extract
      components:
        - name: c1
          image: datuplet/test:latest
          outputs:
            defaultBucket: raw
            defaultWriteMode: APPEND
`

// pipelineYAMLWithSecretRef references the "api_token" secret key via the
// whole-scalar $[name] syntax (pkg/lib/secrets), so validate.ReferencedSecrets
// picks it up and the S7 save/trigger ladder has something to check.
const pipelineYAMLWithSecretRef = `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: etl-secret
spec:
  stages:
    - name: extract
      components:
        - name: c1
          image: datuplet/test:latest
          config:
            token: "$[api_token]"
          outputs:
            defaultBucket: raw
            defaultWriteMode: APPEND
`

// secretsFindingsBody is the wire shape of a 200 warning response from
// PUT /pipelines under the S7 ladder: {"findings":[{path,message,severity}]}
// — note no top-level "error" field, unlike findingsBody's 400 shape.
type secretsFindingsBody struct {
	Findings []struct {
		Path     string `json:"path"`
		Message  string `json:"message"`
		Severity string `json:"severity"`
	} `json:"findings"`
}

func putYAML(t *testing.T, url string, yaml []byte, cookie *stdhttp.Cookie) *stdhttp.Response {
	t.Helper()
	req, _ := stdhttp.NewRequest("PUT", url, bytes.NewReader(yaml))
	req.Header.Set("Content-Type", "application/yaml")
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	return resp
}

func TestPutPipeline_ValidYAML(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-put-valid"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl", []byte(validPipelineYAML), cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Errorf("status = %d, want 204", resp.StatusCode)
	}

	got, err := store.GetPipelineByName(ctx, pool, proj.ID, "etl")
	if err != nil {
		t.Fatalf("GetPipelineByName: %v", err)
	}
	if got.YAML != validPipelineYAML {
		t.Error("YAML not persisted")
	}
}

func TestPutPipeline_InvalidYAML(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-put-invalid"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/broken", []byte("not: [valid yaml"), cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestListPipelines(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-list-pipelines"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "describe", authz.ProjectObject(lkID))
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "p1", validPipelineYAML)
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "p2", validPipelineYAML)

	req, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines", nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestGetPipeline_NotMember(t *testing.T) {
	ts, pool, _, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, _ := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	// Set a lakekeeper_project_id so we pass the empty-lkID guard,
	// but do NOT seed any FGA tuple for alice — mustHaveRelation returns 403.
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, "lk-not-member"); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "secret", validPipelineYAML)

	req, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/secret", nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Errorf("status = %d, want 403 (no FGA tuple for alice)", resp.StatusCode)
	}
}

func TestDeletePipeline(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-delete-pipeline"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "etl", validPipelineYAML)

	req, _ := stdhttp.NewRequest("DELETE", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl", nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Errorf("status = %d, want 204", resp.StatusCode)
	}
}

func TestPutPipeline_NameMismatch(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-name-mismatch"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))

	// YAML says metadata.name: "actual"; URL says "requested". Must 400.
	yaml := `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: actual
spec:
  stages:
    - name: extract
      components:
        - name: c1
          image: datuplet/test:latest
          outputs:
            defaultBucket: raw
            defaultWriteMode: APPEND
`
	resp := putYAML(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/requested", []byte(yaml), cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("status = %d, want 400 (YAML name vs URL name mismatch)", resp.StatusCode)
	}
}

// TestPutPipeline_UnknownFieldFinding covers the strict-decode path: an
// unknown field (typo) fails yaml.UnmarshalStrict inside validate.ValidatePipeline,
// which comes back as a single Finding carrying the decode error with an
// empty Path (see pkg/pipeline/validate.ValidatePipeline doc comment) — not a
// per-field semantic Finding. We assert the response shape and that at least
// one finding is present, but deliberately do NOT assert a non-empty path
// here (see TestPutPipeline_SemanticFindingHasPath for that case).
func TestPutPipeline_UnknownFieldFinding(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-unknown-field"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))

	// "writeMod" is a typo of "writeMode" — an unknown field under strict decode.
	yaml := `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: etl
spec:
  stages:
    - name: extract
      components:
        - name: c1
          image: datuplet/test:latest
          outputs:
            buckets:
              - name: raw
                writeMod: APPEND
`
	resp := putYAML(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl", []byte(yaml), cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var body findingsBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Error != "validation failed" {
		t.Errorf("error = %q, want %q", body.Error, "validation failed")
	}
	if len(body.Findings) == 0 {
		t.Fatal("want at least one finding")
	}
	if body.Findings[0].Severity != "error" {
		t.Errorf("severity = %q, want %q", body.Findings[0].Severity, "error")
	}
}

// TestPutPipeline_SemanticFindingHasPath covers a semantic validation
// failure (as opposed to a strict-decode/unknown-field failure): an invalid
// bucket name. ValidateTyped attaches a field-scoped Path to these findings,
// unlike the empty-Path strict-decode Finding.
func TestPutPipeline_SemanticFindingHasPath(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-semantic-finding"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))

	// "RAW!" fails bucketNameRegex (uppercase + '!' not allowed).
	yaml := `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: etl
spec:
  stages:
    - name: extract
      components:
        - name: c1
          image: datuplet/test:latest
          outputs:
            defaultBucket: "RAW!"
            defaultWriteMode: APPEND
`
	resp := putYAML(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl", []byte(yaml), cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var body findingsBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Error != "validation failed" {
		t.Errorf("error = %q, want %q", body.Error, "validation failed")
	}
	if len(body.Findings) == 0 {
		t.Fatal("want at least one finding")
	}
	if body.Findings[0].Path == "" {
		t.Error("want non-empty path for a semantic validation finding")
	}
}

// TestPutPipeline_UnknownSecretKey_ReturnsWarning covers the S7 save-warn
// rung: a pipeline referencing "$[api_token]" saved into a project whose
// managed Secret doesn't have that key yet gets a 200 with a warning
// Finding naming the key — the save itself still succeeds (unlike the
// hard-validation 400 path).
func TestPutPipeline_UnknownSecretKey_ReturnsWarning(t *testing.T) {
	ts, pool, fakeAuthz, _, _, cleanup := freshServerWithSecrets(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-secret-warn"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl-secret", []byte(pipelineYAMLWithSecretRef), cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := readAll(resp)
		t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, body)
	}
	var body secretsFindingsBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Findings) != 1 {
		t.Fatalf("findings = %+v, want exactly 1", body.Findings)
	}
	if body.Findings[0].Severity != "warning" {
		t.Errorf("severity = %q, want warning", body.Findings[0].Severity)
	}
	if !strings.Contains(body.Findings[0].Message, "api_token") {
		t.Errorf("message = %q, want it to name the missing key api_token", body.Findings[0].Message)
	}

	// The pipeline must still be persisted despite the warning-level response.
	saved, err := store.GetPipelineByName(ctx, pool, proj.ID, "etl-secret")
	if err != nil {
		t.Fatalf("GetPipelineByName: %v", err)
	}
	if saved.YAML != pipelineYAMLWithSecretRef {
		t.Error("pipeline YAML not persisted despite the 200-with-warning response")
	}
}

// TestPutPipeline_KnownSecretKey_Returns204 covers the clean-save rung:
// once the referenced key exists in the project's managed Secret, the
// same YAML saves as a plain 204 — no findings envelope.
func TestPutPipeline_KnownSecretKey_Returns204(t *testing.T) {
	ts, pool, fakeAuthz, _, _, cleanup := freshServerWithSecrets(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-secret-known"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))

	putSecretReq(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/secrets/api_token", "shh", cookie).Body.Close()

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl-secret", []byte(pipelineYAMLWithSecretRef), cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		body, _ := readAll(resp)
		t.Fatalf("status = %d, want 204; body=%s", resp.StatusCode, body)
	}
}

// TestPutPipeline_HardValidationError_StaysBadRequestWithSecretsWired proves
// the secrets ladder never masks a hard validation failure: even with
// WithSecrets wired, a structurally invalid pipeline still 400s before the
// ladder ever runs.
func TestPutPipeline_HardValidationError_StaysBadRequestWithSecretsWired(t *testing.T) {
	ts, pool, fakeAuthz, _, _, cleanup := freshServerWithSecrets(t)
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-secret-hardfail"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/broken", []byte("not: [valid yaml"), cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}
