package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz/authztest"
	"github.com/datuplet/datuplet/pkg/pipelineapi/preview"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

const previewTestLK = "bbbbbbbb-0000-0000-0000-000000000009"

type previewTestResolver struct{}

func (previewTestResolver) UserFor(http.ResponseWriter, *http.Request) (*store.User, bool, error) {
	return testUser, true, nil
}
func (previewTestResolver) Mode() string        { return "test" }
func (previewTestResolver) SupportsLogin() bool { return false }

type previewTestProjects struct{}

func (previewTestProjects) ListForUser(context.Context, uuid.UUID) ([]ProjectView, error) {
	return nil, nil
}

func (previewTestProjects) GetByID(_ context.Context, id uuid.UUID) (*ProjectView, error) {
	return &ProjectView{ID: id, LakekeeperProjectID: previewTestLK}, nil
}

type previewTestRegistry struct{ validate.StaticRegistry }

func (previewTestRegistry) List(context.Context) ([]datupletv1.ComponentDefinition, error) {
	return nil, nil
}

type fakePreviews struct {
	created    *preview.CreateRequest
	createResp *preview.CreateResponse
	createErr  error
	listWS     string
	deleteWS   string
	wsDeleted  []uuid.UUID
	wsFailed   map[uuid.UUID]string
}

func (f *fakePreviews) Create(_ context.Context, r preview.CreateRequest) (*preview.CreateResponse, error) {
	f.created = &r
	return f.createResp, f.createErr
}

func (f *fakePreviews) Get(context.Context, uuid.UUID, uuid.UUID, bool) (*preview.Result, error) {
	return nil, store.ErrPreviewNotFound
}

func (f *fakePreviews) List(_ context.Context, _ uuid.UUID, ws string, _ int, _ string) ([]store.Preview, string, error) {
	f.listWS = ws
	return nil, "", nil
}

func (f *fakePreviews) Cancel(context.Context, uuid.UUID, uuid.UUID) error { return nil }

func (f *fakePreviews) Delete(context.Context, uuid.UUID, uuid.UUID) error { return nil }

func (f *fakePreviews) DeleteWorkspace(_ context.Context, _ uuid.UUID, ws string) ([]uuid.UUID, map[uuid.UUID]string) {
	f.deleteWS = ws
	return f.wsDeleted, f.wsFailed
}

func dataGeneratorRegistry(t *testing.T) previewTestRegistry {
	t.Helper()
	schema, err := os.ReadFile("../../../components/data-generator/schema.json")
	if err != nil {
		t.Fatalf("read data-generator schema: %v", err)
	}
	return previewTestRegistry{validate.StaticRegistry{"data-generator": datupletv1.ComponentDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "data-generator"},
		Spec: datupletv1.ComponentDefinitionSpec{
			DefaultVersion: "v1.0.0",
			Versions: []datupletv1.VersionSpec{{
				Version: "v1.0.0", Image: "datuplet/data-generator:v1.0.0", ConfigSchema: string(schema),
				Resources: &datupletv1.ComponentResources{
					Max: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
				},
			}},
		},
	}}}
}

// newPreviewServer serves the preview routes with fake as the service. The
// test user holds relation on the project; superadmin answers the diff-gate.
func newPreviewServer(t *testing.T, fake previewService, relation string, superadmin bool) *httptest.Server {
	t.Helper()
	fa := authztest.New()
	fa.Allow(authz.UserObject(testUser.ID.String()).String(), relation, authz.ProjectObject(previewTestLK))
	s := NewServer(nil).
		WithUserResolver(previewTestResolver{}).
		WithAuthorizer(fa).
		WithProjectReader(previewTestProjects{}).
		WithRegistry(dataGeneratorRegistry(t)).
		WithServerAdmin(stubServerAdmin{result: superadmin})
	s.previews = fake
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func previewDoc(generatorTable, extraComponent string) string {
	return `{"name":"etl","stages":[{"name":"extract","components":[{"name":"gen","component":"data-generator",` +
		extraComponent + `"config":{"tables":[` + generatorTable + `]},` +
		`"outputs":{"defaultBucket":"raw","defaultWriteMode":"APPEND"}}]}]}`
}

const validGeneratorTable = `{"name":"t","random":{"schema":{"id":"int"},"limit":{"rowsCount":10}}}`

func postPreview(t *testing.T, ts *httptest.Server, pid uuid.UUID, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(ts.URL+"/api/v1/projects/"+pid.String()+"/previews", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST previews: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func decodeBody(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return out
}

func TestPostPreview_ValidationFindings400(t *testing.T) {
	fake := &fakePreviews{}
	ts := newPreviewServer(t, fake, "data_admin", false)
	missingLimit := `{"name":"t","random":{"schema":{"id":"int"}}}`
	resp := postPreview(t, ts, uuid.New(), `{"doc":`+previewDoc(missingLimit, "")+`}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var body struct {
		Error    string             `json:"error"`
		Findings []validate.Finding `json:"findings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Error != "validation failed" {
		t.Errorf("error = %q, want validation failed", body.Error)
	}
	found := false
	for _, f := range body.Findings {
		if f.Severity == "error" && strings.HasSuffix(f.Path, ".random") {
			found = true
		}
	}
	if !found {
		t.Errorf("findings = %+v, want an error finding with a path ending .random", body.Findings)
	}
	if fake.created != nil {
		t.Error("Create was called for an invalid doc")
	}
}

func TestPostPreview_Accepted202(t *testing.T) {
	id := uuid.New()
	expires := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	fake := &fakePreviews{createResp: &preview.CreateResponse{
		ID: id, Workspace: "default", Phase: "Pending",
		ScratchNamespaces: []string{"pv-" + id.String()[:8] + "-raw"}, ExpiresAt: expires,
	}}
	ts := newPreviewServer(t, fake, "data_admin", false)
	pid := uuid.New()
	resp := postPreview(t, ts, pid, `{"doc":`+previewDoc(validGeneratorTable, "")+`,"row_limit":10,"until_stage":"extract"}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	body := decodeBody(t, resp)
	if body["id"] != id.String() || body["workspace"] != "default" || body["phase"] != "Pending" {
		t.Errorf("body = %v", body)
	}
	if ns, _ := body["scratch_namespaces"].([]any); len(ns) != 1 || ns[0] != "pv-"+id.String()[:8]+"-raw" {
		t.Errorf("scratch_namespaces = %v", body["scratch_namespaces"])
	}
	if body["expires_at"] != expires.Format(time.RFC3339) {
		t.Errorf("expires_at = %v", body["expires_at"])
	}
	got := fake.created
	if got == nil {
		t.Fatal("Create not called")
	}
	if got.ProjectID != pid || got.UserID != testUser.ID || got.RowLimit != 10 || got.UntilStage != "extract" {
		t.Errorf("CreateRequest = %+v", got)
	}
	if got.Doc == nil || got.Doc.Name != "etl" || len(got.Doc.Stages) != 1 {
		t.Errorf("CreateRequest.Doc = %+v", got.Doc)
	}
}

func TestPostPreview_Cap409(t *testing.T) {
	fake := &fakePreviews{createErr: &store.PreviewCapError{RunningInWorkspace: 2, RunningInProject: 2}}
	ts := newPreviewServer(t, fake, "data_admin", false)
	resp := postPreview(t, ts, uuid.New(), `{"doc":`+previewDoc(validGeneratorTable, "")+`}`)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}
	body := decodeBody(t, resp)
	if body["error"] != "too_many_previews" || body["running_in_workspace"] != 2.0 || body["running_in_project"] != 2.0 {
		t.Errorf("body = %v", body)
	}
	if _, ok := body["live_in_project"]; !ok {
		t.Errorf("body = %v, want live_in_project", body)
	}
}

func TestPostPreview_BadRequest400(t *testing.T) {
	fake := &fakePreviews{createErr: errors.Join(preview.ErrBadRequest, errors.New("row_limit out of range"))}
	ts := newPreviewServer(t, fake, "data_admin", false)
	resp := postPreview(t, ts, uuid.New(), `{"doc":`+previewDoc(validGeneratorTable, "")+`}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestPostPreview_ResourcesNonSuperadmin403(t *testing.T) {
	doc := previewDoc(validGeneratorTable, `"resources":{"cpu":"2"},`)
	for _, tc := range []struct {
		superadmin bool
		want       int
	}{{false, http.StatusForbidden}, {true, http.StatusAccepted}} {
		fake := &fakePreviews{createResp: &preview.CreateResponse{ID: uuid.New()}}
		ts := newPreviewServer(t, fake, "data_admin", tc.superadmin)
		resp := postPreview(t, ts, uuid.New(), `{"doc":`+doc+`}`)
		if resp.StatusCode != tc.want {
			b, _ := io.ReadAll(resp.Body)
			t.Errorf("superadmin=%v: status = %d, want %d: %s", tc.superadmin, resp.StatusCode, tc.want, b)
		}
	}
}

func TestListPreviews_ScopedToWorkspace(t *testing.T) {
	pid := uuid.New()
	for _, tc := range []struct{ query, want string }{{"?workspace=b", "b"}, {"", "default"}, {"?workspace=*", "*"}} {
		fake := &fakePreviews{}
		ts := newPreviewServer(t, fake, "data_admin", false)
		resp, err := http.Get(ts.URL + "/api/v1/projects/" + pid.String() + "/previews" + tc.query)
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		body := decodeBody(t, resp)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%q: status = %d, want 200", tc.query, resp.StatusCode)
		}
		if fake.listWS != tc.want {
			t.Errorf("%q: workspace = %q, want %q", tc.query, fake.listWS, tc.want)
		}
		if p, ok := body["previews"].([]any); !ok || len(p) != 0 {
			t.Errorf("%q: previews = %v, want []", tc.query, body["previews"])
		}
	}
}

func deletePreviews(t *testing.T, ts *httptest.Server, path string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodDelete, ts.URL+path, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE %s: %v", path, err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestDeleteWorkspace_RequiresParam400(t *testing.T) {
	fake := &fakePreviews{}
	ts := newPreviewServer(t, fake, "data_admin", false)
	resp := deletePreviews(t, ts, "/api/v1/projects/"+uuid.NewString()+"/previews")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if fake.deleteWS != "" {
		t.Errorf("DeleteWorkspace called with %q", fake.deleteWS)
	}
}

func TestDeleteWorkspace_Outcomes(t *testing.T) {
	ok1, bad := uuid.New(), uuid.New()
	for _, tc := range []struct {
		name    string
		deleted []uuid.UUID
		failed  map[uuid.UUID]string
		want    int
	}{
		{"partial", []uuid.UUID{ok1}, map[uuid.UUID]string{bad: "purge: boom"}, http.StatusAccepted},
		{"list failure", nil, map[uuid.UUID]string{uuid.Nil: "query preview ids: down"}, http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakePreviews{wsDeleted: tc.deleted, wsFailed: tc.failed}
			ts := newPreviewServer(t, fake, "data_admin", false)
			resp := deletePreviews(t, ts, "/api/v1/projects/"+uuid.NewString()+"/previews?workspace=w1")
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.want)
			}
			if fake.deleteWS != "w1" {
				t.Errorf("workspace = %q, want w1", fake.deleteWS)
			}
			body := decodeBody(t, resp)
			if tc.want == http.StatusInternalServerError {
				if body["error"] != "list workspace failed" {
					t.Errorf("body = %v", body)
				}
				return
			}
			if d, _ := body["deleted"].([]any); len(d) != 1 || d[0] != ok1.String() {
				t.Errorf("deleted = %v", body["deleted"])
			}
			if f, _ := body["failed"].(map[string]any); f[bad.String()] != "purge: boom" {
				t.Errorf("failed = %v", body["failed"])
			}
		})
	}
}

func TestPreviewRoutes_RequireDataAdmin403(t *testing.T) {
	fake := &fakePreviews{createResp: &preview.CreateResponse{ID: uuid.New()}}
	ts := newPreviewServer(t, fake, "describe_effective", false)
	base := "/api/v1/projects/" + uuid.NewString() + "/previews"
	id := uuid.NewString()
	for _, rt := range []struct{ method, path, body string }{
		{http.MethodPost, base, `{"doc":` + previewDoc(validGeneratorTable, "") + `}`},
		{http.MethodGet, base, ""},
		{http.MethodGet, base + "/" + id, ""},
		{http.MethodDelete, base + "/" + id, ""},
		{http.MethodDelete, base + "?workspace=w1", ""},
		{http.MethodPost, base + "/" + id + "/cancel", ""},
	} {
		req, _ := http.NewRequest(rt.method, ts.URL+rt.path, bytes.NewBufferString(rt.body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", rt.method, rt.path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s %s: status = %d, want 403", rt.method, rt.path, resp.StatusCode)
		}
	}
	if fake.created != nil || fake.listWS != "" || fake.deleteWS != "" {
		t.Error("service reached without data_admin")
	}
}

func TestPreviewRoutes_UnregisteredWithoutService(t *testing.T) {
	s := NewServer(nil).
		WithUserResolver(previewTestResolver{}).
		WithAuthorizer(authztest.New()).
		WithProjectReader(previewTestProjects{}).
		WithPreviews(nil)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/api/v1/projects/" + uuid.NewString() + "/previews")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}
