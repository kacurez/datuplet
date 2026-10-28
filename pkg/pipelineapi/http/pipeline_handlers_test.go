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

// putJSON PUTs a doc body with Content-Type: application/json, exercising the
// content-negotiation branch (JSON body → validated → stored as canonical JSON).
func putJSON(t *testing.T, url string, doc []byte, cookie *stdhttp.Cookie) *stdhttp.Response {
	t.Helper()
	req, _ := stdhttp.NewRequest("PUT", url, bytes.NewReader(doc))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	return resp
}

// pipelineDocWithDescription is an envelope-free PipelineDoc carrying a
// top-level description, used to prove the description round-trips into both
// the detail `doc` object and the List `description` field.
const pipelineDocWithDescription = `{
  "name": "etl",
  "description": "nightly customer load",
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

// legacyEnvelopeYAML is a pre-RFC-027 Kubernetes CR body (apiVersion/kind/
// metadata). config.Parse rejects it with a pointed error, so PUT must 400.
const legacyEnvelopeYAML = `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: etl
spec:
  stages: []`

// pipelineDetailBody is the wire shape of the GET detail response
// ({id, name, doc, created_at, updated_at}); doc is a raw object.
type pipelineDetailBody struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Doc       json.RawMessage `json:"doc"`
	CreatedAt string          `json:"created_at"`
	UpdatedAt string          `json:"updated_at"`
}

// pipelineRefBody is the wire shape of a List item; it must carry description.
type pipelineRefBody struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

// TestPutPipeline_JSON_GetReturnsDocObject_ListHasDescription covers the S6
// happy path: a JSON-content-type PUT stores the doc; GET returns doc as a JSON
// object (not a string, no legacy "yaml" key); List surfaces the description.
func TestPutPipeline_JSON_GetReturnsDocObject_ListHasDescription(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()

	cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-json-get", "data_admin", "describe")

	base := ts.URL + "/api/v1/projects/" + proj.ID.String() + "/pipelines"
	putResp := putJSON(t, base+"/etl", []byte(pipelineDocWithDescription), cookie)
	putResp.Body.Close()
	if putResp.StatusCode != 204 {
		t.Fatalf("PUT status = %d, want 204", putResp.StatusCode)
	}

	// GET detail: doc must decode as an object with name "etl" and no "yaml" key.
	getReq, _ := stdhttp.NewRequest("GET", base+"/etl", nil)
	getReq.AddCookie(cookie)
	getResp, err := stdhttp.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != 200 {
		t.Fatalf("GET status = %d, want 200", getResp.StatusCode)
	}
	var detail pipelineDetailBody
	if err := json.NewDecoder(getResp.Body).Decode(&detail); err != nil {
		t.Fatalf("decode detail: %v", err)
	}
	if detail.Name != "etl" {
		t.Errorf("detail.name = %q, want etl", detail.Name)
	}
	var docObj map[string]any
	if err := json.Unmarshal(detail.Doc, &docObj); err != nil {
		t.Fatalf("doc is not a JSON object: %v (raw=%s)", err, detail.Doc)
	}
	if docObj["name"] != "etl" {
		t.Errorf("doc.name = %v, want etl", docObj["name"])
	}
	if docObj["description"] != "nightly customer load" {
		t.Errorf("doc.description = %v, want the seeded description", docObj["description"])
	}

	// LIST: the item must carry the description.
	listReq, _ := stdhttp.NewRequest("GET", base, nil)
	listReq.AddCookie(cookie)
	listResp, err := stdhttp.DefaultClient.Do(listReq)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	defer listResp.Body.Close()
	var items []pipelineRefBody
	if err := json.NewDecoder(listResp.Body).Decode(&items); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("list len = %d, want 1", len(items))
	}
	if items[0].Description != "nightly customer load" {
		t.Errorf("list item description = %q, want the seeded description", items[0].Description)
	}
}

// TestGetPipeline_FormatYAML covers ?format=yaml: a YAML/JSON PUT can be read
// back as text/yaml that re-parses through config.Parse without loss.
func TestGetPipeline_FormatYAML(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()

	cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-format-yaml", "data_admin", "describe")

	base := ts.URL + "/api/v1/projects/" + proj.ID.String() + "/pipelines"
	putResp := putYAML(t, base+"/etl", []byte(pipelineDocWithDescription), cookie)
	putResp.Body.Close()
	if putResp.StatusCode != 204 {
		t.Fatalf("PUT status = %d, want 204", putResp.StatusCode)
	}

	getReq, _ := stdhttp.NewRequest("GET", base+"/etl?format=yaml", nil)
	getReq.AddCookie(cookie)
	getResp, err := stdhttp.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("get yaml: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != 200 {
		t.Fatalf("GET ?format=yaml status = %d, want 200", getResp.StatusCode)
	}
	if ct := getResp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/yaml") {
		t.Errorf("Content-Type = %q, want text/yaml", ct)
	}
	body, _ := readAll(getResp)
	// A JSON body would start with '{'; YAML output must not.
	if strings.HasPrefix(strings.TrimSpace(body), "{") {
		t.Errorf("body looks like JSON, not YAML: %s", body)
	}
	pl, err := config.Parse([]byte(body))
	if err != nil {
		t.Fatalf("rendered YAML did not re-parse via config.Parse: %v\nbody=%s", err, body)
	}
	if pl.Name != "etl" {
		t.Errorf("re-parsed name = %q, want etl", pl.Name)
	}
	if pl.Description != "nightly customer load" {
		t.Errorf("re-parsed description = %q, want the seeded description", pl.Description)
	}
}

// yamlFlowNotJSON is a complete, otherwise-valid PipelineDoc written as an
// unquoted YAML flow-style mapping: config.Parse decodes it into a fully
// valid Pipeline (one stage, one component) with zero validation findings,
// so a YAML-content-typed PUT of these exact bytes succeeds with 204. But
// the bytes are NOT valid JSON syntax — JSON requires quoted string keys and
// values, and "datuplet/test:latest" (an unquoted scalar containing a bare
// colon) is not valid outside quotes either. It is the case the Content-Type
// gate exists for: a JSON-content-typed PUT of these bytes must be rejected,
// not silently accepted because config.Parse's YAML decoder also understands
// them.
const yamlFlowNotJSON = `{name: etl, stages: [{name: extract, components: [{name: c1, component: datuplet/test:latest, outputs: {defaultBucket: raw, defaultWriteMode: APPEND}}]}]}`

func TestPutPipeline_ValidYAML(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-put-valid", "data_admin")

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

// TestPutPipeline_ValidationOutcomes covers how PUT /pipelines reports bodies
// that cannot be saved. Each row is one (body, content-type, registry) input
// and the status plus finding shape it must produce.
func TestPutPipeline_ValidationOutcomes(t *testing.T) {
	// Doc says name "actual"; the URL says "requested".
	const nameMismatchDoc = `{
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
	// "writeMod" is a typo of "writeMode" — an unknown field under strict
	// decode, which comes back as a single Finding with an empty Path.
	const unknownFieldDoc = `{
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
	// "RAW!" fails bucketNameRegex; a semantic failure carries a field-scoped Path.
	const badBucketDoc = `{
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

	tests := []struct {
		name         string
		registry     *fakeComponentRegistry // nil: no registry wired
		pipeline     string                 // pipeline name in the URL
		json         bool                   // Content-Type: application/json instead of application/yaml
		body         string
		wantStatus   int
		wantFinding  string // substring of some finding message
		wantBody     string // substring of the raw response body
		wantEnvelope bool   // error == "validation failed" and at least one finding
		wantSeverity string // severity of the first finding
		wantPath     bool   // first finding carries a non-empty path
	}{
		{name: "invalid yaml", pipeline: "broken", body: "not: [valid yaml", wantStatus: 400},
		{
			name: "legacy k8s envelope", pipeline: "etl", body: legacyEnvelopeYAML,
			wantStatus: 400, wantFinding: "legacy Kubernetes CR format",
		},
		// A YAML flow mapping that is not valid JSON: the Content-Type gate must
		// reject it under application/json…
		{
			name: "json content type rejects non-json body", pipeline: "etl", json: true, body: yamlFlowNotJSON,
			wantStatus: 400, wantFinding: "not valid JSON",
		},
		// …while the same bytes sent as YAML are accepted, proving the rejection
		// above is Content-Type-driven rather than a config.Parse rejection.
		{name: "same body accepted as yaml", pipeline: "etl", body: yamlFlowNotJSON, wantStatus: 204},
		{
			// R9: with a real registry wired, a component absent from it surfaces
			// R5's "unknown component" finding.
			name: "unknown component", registry: newFakeComponentRegistry(), pipeline: "etl", body: validPipelineYAML,
			wantStatus: 400, wantFinding: `unknown component "datuplet/test:latest"`,
		},
		{name: "name mismatch", pipeline: "requested", body: nameMismatchDoc, wantStatus: 400},
		{
			name: "unknown field", pipeline: "etl", body: unknownFieldDoc,
			wantStatus: 400, wantEnvelope: true, wantSeverity: "error",
		},
		{
			name: "semantic finding has path", pipeline: "etl", body: badBucketDoc,
			wantStatus: 400, wantEnvelope: true, wantPath: true,
		},
		{
			// "validate" is reserved: it would make the literal
			// /pipelines/validate segment ambiguous with the POST validate route.
			name: "reserved name validate", pipeline: "validate", body: validPipelineYAML,
			wantStatus: 400, wantBody: "reserved",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				ts        *httptest.Server
				pool      *pgxpool.Pool
				fakeAuthz *authztest.Fake
				cleanup   func()
			)
			if tc.registry != nil {
				ts, pool, fakeAuthz, cleanup = freshServerWithRegistry(t, tc.registry)
			} else {
				ts, pool, fakeAuthz, cleanup = freshServer(t)
			}
			defer cleanup()
			cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-put-outcomes", "data_admin")

			put := putYAML
			if tc.json {
				put = putJSON
			}
			resp := put(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/"+tc.pipeline, []byte(tc.body), cookie)
			defer resp.Body.Close()
			raw, _ := readAll(resp)
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", resp.StatusCode, tc.wantStatus, raw)
			}
			if tc.wantBody != "" && !strings.Contains(raw, tc.wantBody) {
				t.Errorf("body = %q, want it to contain %q", raw, tc.wantBody)
			}
			if tc.wantFinding == "" && !tc.wantEnvelope && !tc.wantPath {
				return
			}
			var body findingsBody
			if err := json.Unmarshal([]byte(raw), &body); err != nil {
				t.Fatalf("decode: %v; body=%s", err, raw)
			}
			if tc.wantFinding != "" {
				var found bool
				for _, f := range body.Findings {
					if strings.Contains(f.Message, tc.wantFinding) {
						found = true
					}
				}
				if !found {
					t.Fatalf("want a finding containing %q, got %+v", tc.wantFinding, body.Findings)
				}
			}
			if tc.wantEnvelope {
				if body.Error != "validation failed" {
					t.Errorf("error = %q, want %q", body.Error, "validation failed")
				}
				if len(body.Findings) == 0 {
					t.Fatal("want at least one finding")
				}
			}
			if tc.wantSeverity != "" && body.Findings[0].Severity != tc.wantSeverity {
				t.Errorf("severity = %q, want %q", body.Findings[0].Severity, tc.wantSeverity)
			}
			if tc.wantPath && body.Findings[0].Path == "" {
				t.Error("want non-empty path for a semantic validation finding")
			}
		})
	}
}

// TestPutPipeline_SecretLadder covers the S7 save ladder: a pipeline
// referencing "$[api_token]" saves with a 200 plus a warning Finding naming
// the key when the project's managed Secret lacks it (the save still
// persists, unlike the hard-validation 400 path), and as a plain 204 once the
// key exists. The "forbidden" row is a BRAND-NEW project whose managed-Secret
// Get is RBAC-Forbidden (S6 removed pipeline-api's cluster-wide Secret verbs;
// the per-namespace Role is only created on the first secret PUT): that
// denial must read as "no keys set" → 200 warning, NEVER a 500. The fake
// client can't model RBAC, so that row injects Forbidden via an interceptor.
func TestPutPipeline_SecretLadder(t *testing.T) {
	tests := []struct {
		name          string
		forbiddenRead bool // managed-Secret Get returns RBAC Forbidden
		seedKey       bool // api_token already exists in the managed Secret
		wantStatus    int
		wantPersisted bool // doc is stored despite a warning-level response
	}{
		{name: "unknown key warns", wantStatus: 200, wantPersisted: true},
		{name: "known key saves clean", seedKey: true, wantStatus: 204},
		{name: "fresh project forbidden secret read warns", forbiddenRead: true, wantStatus: 200},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				ts        *httptest.Server
				pool      *pgxpool.Pool
				fakeAuthz *authztest.Fake
				cleanup   func()
			)
			if tc.forbiddenRead {
				ts, pool, fakeAuthz, _, _, cleanup = freshServerWithSecretsClient(t, forbiddenSecretGetClient(t))
			} else {
				ts, pool, fakeAuthz, _, _, cleanup = freshServerWithSecrets(t)
			}
			defer cleanup()
			ctx := context.Background()
			cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-secret-ladder", "data_admin")
			base := ts.URL + "/api/v1/projects/" + proj.ID.String()
			if tc.seedKey {
				putSecretReq(t, base+"/secrets/api_token", "shh", cookie).Body.Close()
			}

			resp := putYAML(t, base+"/pipelines/etl-secret", []byte(pipelineYAMLWithSecretRef), cookie)
			defer resp.Body.Close()
			if resp.StatusCode != tc.wantStatus {
				raw, _ := readAll(resp)
				t.Fatalf("status = %d, want %d; body=%s", resp.StatusCode, tc.wantStatus, raw)
			}
			if tc.wantStatus == 200 {
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
			}
			if tc.wantPersisted {
				saved, err := store.GetPipelineByName(ctx, pool, proj.ID, "etl-secret")
				if err != nil {
					t.Fatalf("GetPipelineByName: %v", err)
				}
				if !docsEqual(t, saved.Doc, canonicalDoc(t, pipelineYAMLWithSecretRef)) {
					t.Error("pipeline doc not persisted despite the 200-with-warning response")
				}
			}
		})
	}
}

// TestListPipelines_EmptyDescriptionKeyPresent proves the RFC 027 S6 fix: the
// list item's "description" field has no `omitempty`, so an empty description
// must still appear as `"description":""` in the JSON, not be dropped from the
// object entirely. Decoding into a typed Go struct can't distinguish those two
// cases (an absent key would just decode to the zero value ""), so this test
// decodes into a raw map and checks key presence directly.
func TestListPipelines_EmptyDescriptionKeyPresent(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-list-empty-desc", "describe")
	_, _ = store.CreatePipeline(ctx, pool, proj.ID, "etl", "", []byte(validPipelineYAML))

	req, _ := stdhttp.NewRequest("GET", ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines", nil)
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var items []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("list len = %d, want 1", len(items))
	}
	desc, ok := items[0]["description"]
	if !ok {
		t.Fatalf("list item missing %q key entirely, want key present with empty value; item = %+v", "description", items[0])
	}
	if desc != "" {
		t.Errorf("list item description = %v, want empty string", desc)
	}
}

func TestGetPipeline_NotMember(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	// A lakekeeper_project_id passes the empty-lkID guard, but no FGA tuple
	// is seeded for alice — mustHaveRelation returns 403.
	cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-not-member")
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

	cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-delete-pipeline", "data_admin")
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

// TestPutPipeline_NonSuperadminResourceDiffGate covers the resources diff-gate
// for a non-superadmin: adding a block or changing a value is 403 pointing at
// superadmin; resubmitting the stored block, or a semantically equal one
// ("1000m" vs "1"), is not a modification and passes.
func TestPutPipeline_NonSuperadminResourceDiffGate(t *testing.T) {
	tests := []struct {
		name       string
		stored     []byte
		put        []byte
		wantStatus int
		wantBody   string
	}{
		{name: "add resources forbidden", stored: []byte(validPipelineYAML), put: pipelineYAMLWithCPU("2"), wantStatus: 403, wantBody: "superadmin"},
		{name: "resubmit same resources ok", stored: pipelineYAMLWithCPU("2"), put: pipelineYAMLWithCPU("2"), wantStatus: 204},
		{name: "change cpu forbidden", stored: pipelineYAMLWithCPU("1"), put: pipelineYAMLWithCPU("2"), wantStatus: 403},
		{name: "semantically equal resources ok", stored: pipelineYAMLWithCPU("1"), put: pipelineYAMLWithCPU("1000m"), wantStatus: 204},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts, pool, fakeAuthz, cleanup := freshServerT6(t, t6Registry("4"), nil, stubServerAdminT6{result: false})
			defer cleanup()
			cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-diff-gate", "data_admin")
			if _, err := store.CreatePipeline(context.Background(), pool, proj.ID, "etl", "", tc.stored); err != nil {
				t.Fatalf("seed stored pipeline: %v", err)
			}

			resp := putYAML(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl", tc.put, cookie)
			defer resp.Body.Close()
			body, _ := readAll(resp)
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", resp.StatusCode, tc.wantStatus, body)
			}
			if tc.wantBody != "" && !strings.Contains(body, tc.wantBody) {
				t.Errorf("body = %q, want it to mention %q", body, tc.wantBody)
			}
		})
	}
}

// TestPutPipeline_DiffGateFailsClosed503 covers the two ways the diff-gate
// must fail CLOSED with 503 instead of silently passing: the superadmin
// checker being unavailable, and a transient (non-NotFound) stored-pipeline
// read error (which must not default to "no old resources" and let a
// non-superadmin slip a resources edit through).
func TestPutPipeline_DiffGateFailsClosed503(t *testing.T) {
	tests := []struct {
		name  string
		admin authz.ServerAdminChecker
		store apihttp.PipelineStore // nil: default pgx-backed store
	}{
		{name: "superadmin checker unavailable", admin: stubServerAdminT6{err: authz.ErrAuthzUnavailable}},
		{name: "stored pipeline read error", admin: stubServerAdminT6{result: false}, store: errGetByNamePipelineStore{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts, pool, fakeAuthz, cleanup := freshServerT6WithStore(t, t6Registry("4"), nil, tc.admin, tc.store)
			defer cleanup()
			cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-fail-closed", "data_admin")

			resp := putYAML(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/etl", []byte(validPipelineYAML), cookie)
			defer resp.Body.Close()
			body, _ := readAll(resp)
			if resp.StatusCode != 503 {
				t.Fatalf("status = %d, want 503; body=%s", resp.StatusCode, body)
			}
		})
	}
}

// Case 5: a superadmin setting a fresh resources block within Max bypasses the
// modification gate (204).
func TestPutPipeline_Superadmin_FreshResourcesWithinMax_OK(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerT6(t, t6Registry("4"), nil, stubServerAdminT6{result: true})
	defer cleanup()
	cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-super-fresh", "data_admin")
	pid := proj.ID
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
	cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-super-overmax", "data_admin")
	pid := proj.ID

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
	cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-gw-bound", "data_admin")
	pid := proj.ID
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

// Case 9 (MAJOR 1): a non-superadmin PUT of an otherwise-valid pipeline whose
// component is DEPRECATED is SAVED with 200 + the deprecation warning — never a
// 400. Warnings never block a save (Phase-1.5 ladder convention); only
// error-severity findings do.
func TestPutPipeline_NonSuperadmin_DeprecatedComponent_Warns200(t *testing.T) {
	reg := newFakeComponentRegistry(componentDefDeprecated("datuplet/test:latest"))
	ts, pool, fakeAuthz, cleanup := freshServerT6(t, reg, nil, stubServerAdminT6{result: false})
	defer cleanup()
	cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-deprecated", "data_admin")
	pid := proj.ID

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

// --- RFC 027 S7: POST /pipelines/validate ---

// postValidate POSTs body to the validate endpoint. contentType may be empty
// to exercise the default (YAML-or-JSON-as-YAML-subset) parse path.
func postValidate(t *testing.T, url string, body []byte, contentType string, cookie *stdhttp.Cookie) *stdhttp.Response {
	t.Helper()
	req, _ := stdhttp.NewRequest("POST", url, bytes.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.AddCookie(cookie)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post validate: %v", err)
	}
	return resp
}

// TestValidatePipeline_ValidDoc_EmptyFindingsNotStored covers the headline
// happy path: a valid doc gets 200 {"findings":[]} (literal empty array, not
// null) and validate never writes to the store.
func TestValidatePipeline_ValidDoc_EmptyFindingsNotStored(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()
	ctx := context.Background()

	cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-validate-valid", "data_admin")

	resp := postValidate(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/validate", []byte(validPipelineYAML), "application/yaml", cookie)
	defer resp.Body.Close()
	body, _ := readAll(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"findings":[]`) {
		t.Fatalf("body = %s, want a literal empty findings array, not null", body)
	}

	if _, err := store.GetPipelineByName(ctx, pool, proj.ID, "etl"); err == nil {
		t.Fatalf("validate must not persist anything, but GetPipelineByName found a row")
	}
}

// TestValidatePipeline_ErrorFinding_Not400 proves the core status-code
// contract difference from PUT: a doc with an error-severity finding (here,
// an unresolvable component) still comes back 200 — validation outcomes are
// findings, never HTTP errors (spec §5.2).
func TestValidatePipeline_ErrorFinding_Not400(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerWithRegistry(t, newFakeComponentRegistry())
	defer cleanup()

	cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-validate-error-finding", "data_admin")

	resp := postValidate(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/validate", []byte(validPipelineYAML), "application/yaml", cookie)
	defer resp.Body.Close()
	body, _ := readAll(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 (errors are findings, not 400); body=%s", resp.StatusCode, body)
	}
	var fb secretsFindingsBody
	if err := json.Unmarshal([]byte(body), &fb); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var found bool
	for _, f := range fb.Findings {
		if f.Severity == "error" && strings.Contains(f.Message, `unknown component "datuplet/test:latest"`) {
			found = true
		}
	}
	if !found {
		t.Fatalf("want an 'unknown component' error finding, got %+v", fb.Findings)
	}
}

// TestValidatePipeline_UnreadableBody_BadRequest covers the one case that
// stays a 400: a body that cannot be decoded as a PipelineDoc at all.
func TestValidatePipeline_UnreadableBody_BadRequest(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServer(t)
	defer cleanup()

	cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-validate-unreadable", "data_admin")

	resp := postValidate(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/validate", []byte("not: [valid yaml"), "application/yaml", cookie)
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		body, _ := readAll(resp)
		t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, body)
	}
}

// TestValidatePipeline_ResourceGate covers the resource-gate diff for a
// non-superadmin on POST /pipelines/validate. With ?name= the diff runs against
// the stored doc: resubmitting the identical block is not a modification (no
// finding, mirroring PUT's 204) and adding a block yields an error finding
// naming superadmin (a finding, never the 403 PUT returns). Without ?name= the
// gate validates with create semantics — there is no stored doc, so any
// resources block is a finding.
func TestValidatePipeline_ResourceGate(t *testing.T) {
	tests := []struct {
		name        string
		stored      []byte // nil: no stored pipeline
		query       string
		body        []byte
		wantFinding bool // an error finding naming superadmin
	}{
		{name: "same resources no finding", stored: pipelineYAMLWithCPU("2"), query: "?name=etl", body: pipelineYAMLWithCPU("2")},
		{name: "added resources finding", stored: []byte(validPipelineYAML), query: "?name=etl", body: pipelineYAMLWithCPU("2"), wantFinding: true},
		{name: "no name create semantics finding", body: pipelineYAMLWithCPU("2"), wantFinding: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts, pool, fakeAuthz, cleanup := freshServerT6(t, t6Registry("4"), nil, stubServerAdminT6{result: false})
			defer cleanup()
			cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-validate-resgate", "data_admin")
			if tc.stored != nil {
				if _, err := store.CreatePipeline(context.Background(), pool, proj.ID, "etl", "", tc.stored); err != nil {
					t.Fatalf("seed stored pipeline: %v", err)
				}
			}

			resp := postValidate(t, ts.URL+"/api/v1/projects/"+proj.ID.String()+"/pipelines/validate"+tc.query, tc.body, "application/yaml", cookie)
			defer resp.Body.Close()
			body, _ := readAll(resp)
			if resp.StatusCode != 200 {
				t.Fatalf("status = %d, want 200 (resource-gate result is a finding, not 403); body=%s", resp.StatusCode, body)
			}
			var fb secretsFindingsBody
			if err := json.Unmarshal([]byte(body), &fb); err != nil {
				t.Fatalf("decode: %v", err)
			}
			var found bool
			for _, f := range fb.Findings {
				if tc.wantFinding && f.Severity == "error" && strings.Contains(f.Message, "superadmin") {
					found = true
				}
				if !tc.wantFinding && strings.Contains(f.Message, "superadmin") {
					t.Fatalf("unexpected resource-gate finding for unchanged resources: %+v", fb.Findings)
				}
			}
			if tc.wantFinding && !found {
				t.Fatalf("want a resource-gate finding naming superadmin, got %+v", fb.Findings)
			}
		})
	}
}

// TestValidatePipeline_NonSuperadmin_NameMismatchFindingSurvives is a
// regression test: the non-superadmin branch re-validates against the
// policy to surface gateway-bound findings (mirroring PUT's diff-gate), and
// an earlier version of that re-validation used validate.ValidateTyped
// directly — which silently dropped the doc-name-vs-?name= mismatch finding
// that only validate.ValidatePipelineDoc folds in (ValidateTyped has no
// notion of the routing-context name at all). This proves that finding
// still survives the non-superadmin path.
func TestValidatePipeline_NonSuperadmin_NameMismatchFindingSurvives(t *testing.T) {
	ts, pool, fakeAuthz, cleanup := freshServerT6(t, t6Registry("4"), nil, stubServerAdminT6{result: false})
	defer cleanup()
	cookie, _, proj := seedProject(t, ts, pool, fakeAuthz, "lk-validate-name-mismatch", "data_admin")
	pid := proj.ID

	mismatched := `{
  "name": "wrong-name",
  "stages": [
    {
      "name": "extract",
      "components": [
        {"name": "c1", "component": "datuplet/test:latest", "outputs": {"defaultBucket": "raw", "defaultWriteMode": "APPEND"}}
      ]
    }
  ]
}`
	resp := postValidate(t, ts.URL+"/api/v1/projects/"+pid.String()+"/pipelines/validate?name=etl", []byte(mismatched), "application/yaml", cookie)
	defer resp.Body.Close()
	body, _ := readAll(resp)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, body)
	}
	var fb secretsFindingsBody
	if err := json.Unmarshal([]byte(body), &fb); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var found bool
	for _, f := range fb.Findings {
		if f.Severity == "error" && strings.Contains(f.Message, `does not match the pipeline name`) {
			found = true
		}
	}
	if !found {
		t.Fatalf("want a name-mismatch finding to survive the non-superadmin diff-gate path, got %+v", fb.Findings)
	}
}
