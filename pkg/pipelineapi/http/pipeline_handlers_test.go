package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/config"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"
	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz/authztest"
	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
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

// validPipelineYAML is envelope-free PipelineDoc content (RFC 027 §3),
// written as JSON — JSON is a valid YAML subset, so it exercises both the
// PUT endpoint (which accepts YAML/JSON bodies) and the direct
// store.CreatePipeline seeding calls below (which need valid JSON: the
// `doc` column is jsonb).
const validPipelineYAML = `{
  "name": "etl",
  "stages": [
    {
      "name": "extract",
      "components": [
        {
          "name": "c1",
          "component": "datuplet/test:latest",
          "outputs": {"defaultBucket": "raw", "defaultWriteMode": "APPEND"}
        }
      ]
    }
  ]
}`

// pipelineYAMLWithSecretRef references the "api_token" secret key via the
// whole-scalar $[name] syntax (pkg/lib/secrets), so validate.ReferencedSecrets
// picks it up and the S7 save/trigger ladder has something to check.
const pipelineYAMLWithSecretRef = `{
  "name": "etl-secret",
  "stages": [
    {
      "name": "extract",
      "components": [
        {
          "name": "c1",
          "component": "datuplet/test:latest",
          "config": {"token": "$[api_token]"},
          "outputs": {"defaultBucket": "raw", "defaultWriteMode": "APPEND"}
        }
      ]
    }
  ]
}`

// docsEqual compares two JSON byte slices by value rather than by exact
// bytes: the PUT handler stores a re-marshaled canonical form (field order
// may differ from a hand-written fixture), so byte-for-byte comparison
// would be flaky.
func docsEqual(t *testing.T, got, want []byte) bool {
	t.Helper()
	var gotVal, wantVal any
	if err := json.Unmarshal(got, &gotVal); err != nil {
		t.Fatalf("unmarshal got doc: %v", err)
	}
	if err := json.Unmarshal(want, &wantVal); err != nil {
		t.Fatalf("unmarshal want doc: %v", err)
	}
	return reflect.DeepEqual(gotVal, wantVal)
}

// canonicalDoc runs raw through the same parse+marshal path handlePutPipeline
// uses to build the stored `doc` column, so tests can compare persisted bytes
// against what PUT is actually expected to store (e.g. config.Pipeline's
// non-pointer Gateway field always marshals as "gateway":{} even when unset —
// encoding/json's `omitempty` has no effect on struct-typed fields — so a
// hand-written fixture without that key would never byte/structurally match).
func canonicalDoc(t *testing.T, raw string) []byte {
	t.Helper()
	doc, err := config.Parse([]byte(raw))
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	return b
}

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
	if !docsEqual(t, got.Doc, canonicalDoc(t, validPipelineYAML)) {
		t.Errorf("doc not persisted: got %s", got.Doc)
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

// TestPutPipeline_UnknownComponent_ReturnsRegistryFinding is R9's headline
// regression test: once a real ComponentRegistry is wired via WithRegistry,
// saving a pipeline that references a component absent from the registry
// must surface R5's "unknown component" finding — R5 landed this check but
// it stayed dark because handlePutPipeline called ValidatePipeline with a
// hard-coded nil registry.
func TestPutPipeline_UnknownComponent_ReturnsRegistryFinding(t *testing.T) {
	// Empty registry: no ComponentDefinitions registered at all, so
	// "datuplet/test:latest" (used by every other fixture in this file)
	// resolves to "unknown component".
	ts, pool, fakeAuthz, cleanup := freshServerWithRegistry(t, newFakeComponentRegistry())
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-unknown-component"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl", []byte(validPipelineYAML), cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var body findingsBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	var found bool
	for _, f := range body.Findings {
		if strings.Contains(f.Message, `unknown component "datuplet/test:latest"`) {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected an 'unknown component' finding, got %+v", body.Findings)
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
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "p1", "", []byte(validPipelineYAML))
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "p2", "", []byte(validPipelineYAML))

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
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "secret", "", []byte(validPipelineYAML))

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
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "etl", "", []byte(validPipelineYAML))

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

	// Doc says name: "actual"; URL says "requested". Must 400.
	yaml := `{
  "name": "actual",
  "stages": [
    {
      "name": "extract",
      "components": [
        {"name": "c1", "component": "datuplet/test:latest", "outputs": {"defaultBucket": "raw", "defaultWriteMode": "APPEND"}}
      ]
    }
  ]
}`
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
	yaml := `{
  "name": "etl",
  "stages": [
    {
      "name": "extract",
      "components": [
        {
          "name": "c1",
          "component": "datuplet/test:latest",
          "outputs": {"buckets": [{"name": "raw", "writeMod": "APPEND"}]}
        }
      ]
    }
  ]
}`
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
	yaml := `{
  "name": "etl",
  "stages": [
    {
      "name": "extract",
      "components": [
        {"name": "c1", "component": "datuplet/test:latest", "outputs": {"defaultBucket": "RAW!", "defaultWriteMode": "APPEND"}}
      ]
    }
  ]
}`
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
	if !docsEqual(t, saved.Doc, canonicalDoc(t, pipelineYAMLWithSecretRef)) {
		t.Error("pipeline doc not persisted despite the 200-with-warning response")
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

// TestPutPipeline_FreshProjectForbiddenSecretRead_ReturnsWarning proves the S7
// save-warn rung survives a BRAND-NEW project whose managed-Secret Get is
// RBAC-Forbidden. After S6 removed pipeline-api's cluster-wide Secret verbs, a
// project that has never had a secret PUT has no per-namespace `datuplet-
// secrets` Role/RoleBinding yet, so the ladder's read is denied. The handler
// must treat that denial as "no keys set" → 200 warning naming the key, NEVER
// a 500. (The NotFound/absent variant is covered by
// TestPutPipeline_UnknownSecretKey_ReturnsWarning; the fake client can't model
// RBAC, so this test injects Forbidden via an interceptor client.)
func TestPutPipeline_FreshProjectForbiddenSecretRead_ReturnsWarning(t *testing.T) {
	ts, pool, fakeAuthz, _, _, cleanup := freshServerWithSecretsClient(t, forbiddenSecretGetClient(t))
	defer cleanup()
	ctx := context.Background()

	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	lkID := "lk-secret-forbidden"
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl-secret", []byte(pipelineYAMLWithSecretRef), cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := readAll(resp)
		t.Fatalf("status = %d, want 200 (fresh-project ladder, not 500); body=%s", resp.StatusCode, body)
	}
	var body secretsFindingsBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Findings) != 1 || body.Findings[0].Severity != "warning" || !strings.Contains(body.Findings[0].Message, "api_token") {
		t.Fatalf("findings = %+v, want exactly one warning naming api_token", body.Findings)
	}
}

// --- RFC 026 P3 (T6): resources/gateway diff-gate on PUT ---

// stubServerAdminT6 is a test double for authz.ServerAdminChecker used by the
// diff-gate handler tests (the internal-package stubServerAdmin isn't reachable
// from this external http_test package).
type stubServerAdminT6 struct {
	result bool
	err    error
}

func (s stubServerAdminT6) ServerObject(context.Context) (string, error) { return "server:x", s.err }
func (s stubServerAdminT6) IsServerAdmin(context.Context, string) (bool, error) {
	return s.result, s.err
}

// freshServerT6 is freshServerWithRegistry plus the Phase-3 diff-gate wiring: a
// superadmin checker and an (optional) pipeline policy.
func freshServerT6(t *testing.T, reg apihttp.ComponentRegistry, pol *validate.Policy, admin authz.ServerAdminChecker) (*httptest.Server, *pgxpool.Pool, *authztest.Fake, func()) {
	t.Helper()
	return freshServerT6WithStore(t, reg, pol, admin, nil)
}

// freshServerT6WithStore is freshServerT6 with an optional pipeline-store
// override. A nil override uses the default pgx-backed store; tests pass a fake
// to exercise store-error paths (e.g. a transient GetByName failure feeding the
// diff-gate's fail-closed 503).
func freshServerT6WithStore(t *testing.T, reg apihttp.ComponentRegistry, pol *validate.Policy, admin authz.ServerAdminChecker, storeOverride apihttp.PipelineStore) (*httptest.Server, *pgxpool.Pool, *authztest.Fake, func()) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if _, err := pool.Exec(context.Background(), "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"); err != nil {
		pool.Close()
		t.Fatalf("reset: %v", err)
	}
	if err := pipelineapidb.Migrate(context.Background(), pool); err != nil {
		pool.Close()
		t.Fatalf("migrate: %v", err)
	}
	var pstore apihttp.PipelineStore = apihttp.NewPgxPipelineStore(pool)
	if storeOverride != nil {
		pstore = storeOverride
	}
	authzr := authztest.New()
	srv := apihttp.NewServer(pool).
		WithUserResolver(auth.NewPostgresResolver(pool, false)).
		WithAuthorizer(authzr).
		WithProjectReader(apihttp.NewPgxProjectReader(pool, authzr)).
		WithPipelineStore(pstore).
		WithRunReader(apihttp.NewPgxRunReader(pool)).
		WithRegistry(reg).
		WithServerAdmin(admin).
		WithPipelinePolicy(pol)
	ts := httptest.NewServer(srv.Handler())
	cleanup := func() {
		ts.Close()
		pool.Close()
	}
	return ts, pool, authzr, cleanup
}

// errGetByNamePipelineStore is a PipelineStore whose Get always fails with
// a non-NotFound error, to exercise the diff-gate's fail-closed 503 path. The
// handler returns before touching any other method.
type errGetByNamePipelineStore struct{}

func (errGetByNamePipelineStore) List(context.Context, uuid.UUID) ([]apihttp.PipelineRef, error) {
	return nil, nil
}

func (errGetByNamePipelineStore) Get(context.Context, uuid.UUID, string) (*apihttp.PipelineDetail, error) {
	return nil, errors.New("transient store read failure")
}

func (errGetByNamePipelineStore) GetDocByID(context.Context, string) ([]byte, error) {
	return nil, nil
}

func (errGetByNamePipelineStore) Put(context.Context, uuid.UUID, string, []byte, string) error {
	return nil
}

func (errGetByNamePipelineStore) Delete(context.Context, uuid.UUID, string) error { return nil }

// componentDefWithMaxCPU builds a ComponentDefinition whose single version
// bounds cpu at maxCPU (the registry Max ceiling).
func componentDefWithMaxCPU(name, maxCPU string) datupletv1.ComponentDefinition {
	return datupletv1.ComponentDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: datupletv1.ComponentDefinitionSpec{
			DisplayName:    "Display " + name,
			DefaultVersion: "v1.0.0",
			Versions: []datupletv1.VersionSpec{{
				Version: "v1.0.0",
				Image:   "datuplet/test:v1.0.0",
				Resources: &datupletv1.ComponentResources{
					Max: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(maxCPU)},
				},
			}},
		},
	}
}

// t6Registry resolves "datuplet/test:latest" (the ref every fixture uses) with
// the given cpu ceiling.
func t6Registry(maxCPU string) *fakeComponentRegistry {
	return newFakeComponentRegistry(componentDefWithMaxCPU("datuplet/test:latest", maxCPU))
}

// componentDefDeprecated builds a ComponentDefinition marked deprecated so
// validate.Resolve returns the deprecation *warning* finding (not an error) for
// a still-resolvable component.
func componentDefDeprecated(name string) datupletv1.ComponentDefinition {
	return datupletv1.ComponentDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: datupletv1.ComponentDefinitionSpec{
			DisplayName:    "Display " + name,
			DefaultVersion: "v1.0.0",
			Deprecated:     true,
			Versions: []datupletv1.VersionSpec{{
				Version: "v1.0.0",
				Image:   "datuplet/test:v1.0.0",
			}},
		},
	}
}

// t6Seed creates a user+session, a project with a lakekeeper id, and the
// data_admin FGA tuple that mustHaveRelation checks. Returns the session cookie
// and the datuplet project id.
func t6Seed(t *testing.T, ts *httptest.Server, pool *pgxpool.Pool, fakeAuthz *authztest.Fake, lkID string) (*stdhttp.Cookie, uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	cookie, alice := seedUserAndLogin(t, pool, ts.URL, "a@example.com", "x")
	proj, _ := store.CreateProject(ctx, pool, "proj")
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, lkID); err != nil {
		t.Fatalf("SetLakekeeperProjectID: %v", err)
	}
	fakeAuthz.Allow(authz.UserObject(alice.ID.String()).String(), "data_admin", authz.ProjectObject(lkID))
	return cookie, proj.ID
}

// pipelineYAMLWithCPU builds a doc whose component sets resources.cpu — the
// new envelope-free doc shape is flat ({"cpu": "..."}), unlike the old CRD's
// nested resources.limits.cpu; config.DocToCR maps it into the CRD's nested
// corev1.ResourceRequirements before the diff-gate ever sees it.
func pipelineYAMLWithCPU(cpu string) []byte {
	return []byte(`{
  "name": "etl",
  "stages": [
    {
      "name": "extract",
      "components": [
        {
          "name": "c1",
          "component": "datuplet/test:latest",
          "resources": {"cpu": "` + cpu + `"},
          "outputs": {"defaultBucket": "raw", "defaultWriteMode": "APPEND"}
        }
      ]
    }
  ]
}`)
}

// pipelineYAMLBigBuffer sets gateway.bufferSize (no resources block) so the
// gateway-bound gate is what trips, not the resources modification gate.
const pipelineYAMLBigBuffer = `{
  "name": "etl",
  "gateway": {"bufferSize": 200},
  "stages": [
    {
      "name": "extract",
      "components": [
        {"name": "c1", "component": "datuplet/test:latest", "outputs": {"defaultBucket": "raw", "defaultWriteMode": "APPEND"}}
      ]
    }
  ]
}`

// Case 1: a non-superadmin adding a resources block where the stored pipeline
// had none is rejected with 403 pointing at superadmin.
func TestPutPipeline_NonSuperadmin_AddResources_Forbidden(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerT6(t, t6Registry("4"), nil, stubServerAdminT6{result: false})
	defer cleanup()
	cookie, pid := t6Seed(t, ts, pool, fakeAuthz, "lk-add-res")
	if _, err := store.CreatePipeline(context.Background(), pool, pid, "etl", "", []byte(validPipelineYAML)); err != nil {
		t.Fatalf("seed stored pipeline: %v", err)
	}

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+pid.String()+"/pipelines/etl", pipelineYAMLWithCPU("2"), cookie)
	defer resp.Body.Close()
	body, _ := readAll(resp)
	if resp.StatusCode != 403 {
		t.Fatalf("status = %d, want 403; body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "superadmin") {
		t.Errorf("403 body = %q, want it to mention superadmin", body)
	}
}

// Case 2: a non-superadmin resubmitting the identical stored resources block
// passes (unchanged is not a modification).
func TestPutPipeline_NonSuperadmin_ResubmitSameResources_OK(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerT6(t, t6Registry("4"), nil, stubServerAdminT6{result: false})
	defer cleanup()
	cookie, pid := t6Seed(t, ts, pool, fakeAuthz, "lk-resubmit")
	yaml := pipelineYAMLWithCPU("2")
	if _, err := store.CreatePipeline(context.Background(), pool, pid, "etl", "", yaml); err != nil {
		t.Fatalf("seed stored pipeline: %v", err)
	}

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+pid.String()+"/pipelines/etl", yaml, cookie)
	defer resp.Body.Close()
	body, _ := readAll(resp)
	if resp.StatusCode != 204 {
		t.Fatalf("status = %d, want 204; body=%s", resp.StatusCode, body)
	}
}

// Case 3: a non-superadmin changing limits.cpu from the stored value is 403.
func TestPutPipeline_NonSuperadmin_ChangeCPU_Forbidden(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerT6(t, t6Registry("4"), nil, stubServerAdminT6{result: false})
	defer cleanup()
	cookie, pid := t6Seed(t, ts, pool, fakeAuthz, "lk-change-cpu")
	if _, err := store.CreatePipeline(context.Background(), pool, pid, "etl", "", pipelineYAMLWithCPU("1")); err != nil {
		t.Fatalf("seed stored pipeline: %v", err)
	}

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+pid.String()+"/pipelines/etl", pipelineYAMLWithCPU("2"), cookie)
	defer resp.Body.Close()
	body, _ := readAll(resp)
	if resp.StatusCode != 403 {
		t.Fatalf("status = %d, want 403; body=%s", resp.StatusCode, body)
	}
}

// Case 4: "1000m" against a stored "1" is semantically equal, so a
// non-superadmin resubmit passes (204).
func TestPutPipeline_NonSuperadmin_SemanticEqualResources_OK(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerT6(t, t6Registry("4"), nil, stubServerAdminT6{result: false})
	defer cleanup()
	cookie, pid := t6Seed(t, ts, pool, fakeAuthz, "lk-semeq")
	if _, err := store.CreatePipeline(context.Background(), pool, pid, "etl", "", pipelineYAMLWithCPU("1")); err != nil {
		t.Fatalf("seed stored pipeline: %v", err)
	}

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+pid.String()+"/pipelines/etl", pipelineYAMLWithCPU("1000m"), cookie)
	defer resp.Body.Close()
	body, _ := readAll(resp)
	if resp.StatusCode != 204 {
		t.Fatalf("status = %d, want 204 (1000m == 1); body=%s", resp.StatusCode, body)
	}
}

// Case 5: a superadmin setting a fresh resources block within Max bypasses the
// modification gate (204).
func TestPutPipeline_Superadmin_FreshResourcesWithinMax_OK(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerT6(t, t6Registry("4"), nil, stubServerAdminT6{result: true})
	defer cleanup()
	cookie, pid := t6Seed(t, ts, pool, fakeAuthz, "lk-super-fresh")
	if _, err := store.CreatePipeline(context.Background(), pool, pid, "etl", "", []byte(validPipelineYAML)); err != nil {
		t.Fatalf("seed stored pipeline: %v", err)
	}

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+pid.String()+"/pipelines/etl", pipelineYAMLWithCPU("2"), cookie)
	defer resp.Body.Close()
	body, _ := readAll(resp)
	if resp.StatusCode != 204 {
		t.Fatalf("status = %d, want 204; body=%s", resp.StatusCode, body)
	}
}

// Case 6: the registry Max ceiling applies to superadmins too — an over-Max
// block is 400 with a reject finding, not a diff-gate bypass.
func TestPutPipeline_Superadmin_ResourcesOverMax_BadRequest(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerT6(t, t6Registry("2"), nil, stubServerAdminT6{result: true})
	defer cleanup()
	cookie, pid := t6Seed(t, ts, pool, fakeAuthz, "lk-super-overmax")

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+pid.String()+"/pipelines/etl", pipelineYAMLWithCPU("4"), cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		body, _ := readAll(resp)
		t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, body)
	}
	var fb findingsBody
	if err := json.NewDecoder(resp.Body).Decode(&fb); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var found bool
	for _, f := range fb.Findings {
		if strings.Contains(f.Message, "exceeds registry max") {
			found = true
		}
	}
	if !found {
		t.Fatalf("want an 'exceeds registry max' finding, got %+v", fb.Findings)
	}
}

// Case 7: a non-superadmin exceeding a gateway bound is 403 (the handler maps a
// gateway-bound finding to 403, not the 400 the finding would otherwise be).
func TestPutPipeline_NonSuperadmin_OverGatewayBound_Forbidden(t *testing.T) {
	pol := &validate.Policy{Gateway: validate.GatewayBounds{MaxBufferSize: 100}}
	ts, pool, fakeAuthz, cleanup := freshServerT6(t, t6Registry("4"), pol, stubServerAdminT6{result: false})
	defer cleanup()
	cookie, pid := t6Seed(t, ts, pool, fakeAuthz, "lk-gw-bound")
	if _, err := store.CreatePipeline(context.Background(), pool, pid, "etl", "", []byte(validPipelineYAML)); err != nil {
		t.Fatalf("seed stored pipeline: %v", err)
	}

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+pid.String()+"/pipelines/etl", []byte(pipelineYAMLBigBuffer), cookie)
	defer resp.Body.Close()
	body, _ := readAll(resp)
	if resp.StatusCode != 403 {
		t.Fatalf("status = %d, want 403; body=%s", resp.StatusCode, body)
	}
}

// Case 8: an unavailable superadmin checker fails the PUT with 503, not a
// silent pass.
func TestPutPipeline_SuperadminCheckerUnavailable_ServiceUnavailable(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerT6(t, t6Registry("4"), nil, stubServerAdminT6{err: authz.ErrAuthzUnavailable})
	defer cleanup()
	cookie, pid := t6Seed(t, ts, pool, fakeAuthz, "lk-checker-503")

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+pid.String()+"/pipelines/etl", []byte(validPipelineYAML), cookie)
	defer resp.Body.Close()
	body, _ := readAll(resp)
	if resp.StatusCode != 503 {
		t.Fatalf("status = %d, want 503; body=%s", resp.StatusCode, body)
	}
}

// Case 9 (MAJOR 1): a non-superadmin PUT of an otherwise-valid pipeline whose
// component is DEPRECATED is SAVED with 200 + the deprecation warning — never a
// 400. Warnings never block a save (Phase-1.5 ladder convention); only
// error-severity findings do.
func TestPutPipeline_NonSuperadmin_DeprecatedComponent_Warns200(t *testing.T) {
	reg := newFakeComponentRegistry(componentDefDeprecated("datuplet/test:latest"))
	ts, pool, fakeAuthz, cleanup := freshServerT6(t, reg, nil, stubServerAdminT6{result: false})
	defer cleanup()
	cookie, pid := t6Seed(t, ts, pool, fakeAuthz, "lk-deprecated")

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+pid.String()+"/pipelines/etl", []byte(validPipelineYAML), cookie)
	defer resp.Body.Close()
	body, _ := readAll(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 (deprecation is a warning, not a 400); body=%s", resp.StatusCode, body)
	}
	var fb secretsFindingsBody
	if err := json.Unmarshal([]byte(body), &fb); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var found bool
	for _, f := range fb.Findings {
		if f.Severity == "warning" && strings.Contains(f.Message, "deprecated") {
			found = true
		}
	}
	if !found {
		t.Fatalf("want a deprecation warning finding, got %+v", fb.Findings)
	}
}

// Case 10 (MAJOR 2): a transient (non-NotFound) stored-pipeline read error makes
// the diff-gate fail CLOSED with 503, rather than defaulting to "no old
// resources" and letting a non-superadmin slip a resources edit through.
func TestPutPipeline_NonSuperadmin_StoreReadError_ServiceUnavailable(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerT6WithStore(t, t6Registry("4"), nil, stubServerAdminT6{result: false}, errGetByNamePipelineStore{})
	defer cleanup()
	cookie, pid := t6Seed(t, ts, pool, fakeAuthz, "lk-store-read-err")

	resp := putYAML(t, ts.URL+"/api/v1/projects/"+pid.String()+"/pipelines/etl", []byte(validPipelineYAML), cookie)
	defer resp.Body.Close()
	body, _ := readAll(resp)
	if resp.StatusCode != 503 {
		t.Fatalf("status = %d, want 503 (fail closed on read error); body=%s", resp.StatusCode, body)
	}
}
