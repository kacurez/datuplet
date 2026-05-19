package lakekeeper

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// trivialMinter returns a fixed test JWT. Lakekeeper isn't running in
// tests; the manager just attaches the bearer to outgoing requests.
func trivialMinter() (string, error) { return "test-jwt", nil }

// newTestManager spins up a Manager pointed at the given httptest.Server.
func newTestManager(t *testing.T, srv *httptest.Server) *Manager {
	t.Helper()
	mgr, err := New(srv.URL, trivialMinter, 5*time.Second)
	if err != nil {
		t.Fatalf("lakekeeper.New: %v", err)
	}
	return mgr
}

// fakeListResponse returns the JSON shape Manager.listWarehousesInProject
// decodes from GET /management/v1/warehouse.
func fakeListResponse(names ...string) []byte {
	type item struct {
		Name string `json:"name"`
	}
	type wrap struct {
		Warehouses []item `json:"warehouses"`
	}
	w := wrap{}
	for _, n := range names {
		w.Warehouses = append(w.Warehouses, item{Name: n})
	}
	b, _ := json.Marshal(w)
	return b
}

// TestEnsureS3WarehouseInProjectIdempotent asserts the probe shortcut:
// if the warehouse already exists in the project, no POST follows.
func TestEnsureS3WarehouseInProjectIdempotent(t *testing.T) {
	var postCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if got := r.Header.Get("x-project-id"); got != "proj-1" {
				t.Errorf("expected x-project-id=proj-1, got %q", got)
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(fakeListResponse("wh-existing"))
		case http.MethodPost:
			postCount++
			w.WriteHeader(http.StatusCreated)
		}
	}))
	defer srv.Close()

	mgr := newTestManager(t, srv)
	err := mgr.EnsureS3WarehouseInProject(t.Context(), "proj-1", "wh-existing", S3WarehouseProfile{
		Bucket:    "b",
		Endpoint:  "http://minio",
		AccessKey: "ak",
		SecretKey: "sk",
	})
	if err != nil {
		t.Fatalf("EnsureS3WarehouseInProject: %v", err)
	}
	if postCount != 0 {
		t.Fatalf("expected zero POSTs (warehouse already exists); got %d", postCount)
	}
}

// TestEnsureS3WarehouseInProjectCreates verifies the POST happens when the
// probe returns an empty list, and that the basic shape passes through.
func TestEnsureS3WarehouseInProjectCreates(t *testing.T) {
	var sawPost bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(fakeListResponse())
		case http.MethodPost:
			sawPost = true
			body, _ := io.ReadAll(r.Body)
			if !strings.Contains(string(body), `"type":"s3"`) {
				t.Errorf("expected S3 storage-profile type in body, got: %s", string(body))
			}
			if !strings.Contains(string(body), `"bucket":"mybucket"`) {
				t.Errorf("expected bucket=mybucket in body, got: %s", string(body))
			}
			w.WriteHeader(http.StatusCreated)
		}
	}))
	defer srv.Close()

	mgr := newTestManager(t, srv)
	if err := mgr.EnsureS3WarehouseInProject(t.Context(), "proj-1", "wh-new", S3WarehouseProfile{
		Bucket:    "mybucket",
		Endpoint:  "http://minio",
		AccessKey: "ak",
		SecretKey: "sk",
	}); err != nil {
		t.Fatalf("EnsureS3WarehouseInProject: %v", err)
	}
	if !sawPost {
		t.Fatal("expected POST to be issued; none observed")
	}
}

// TestEnsureGCSWarehouseInProjectCreatesSystemIdentity asserts the system-identity
// (WIF) branch produces the gcp-system-identity credential-type and the
// expected GCS bucket field.
func TestEnsureGCSWarehouseInProjectCreatesSystemIdentity(t *testing.T) {
	var postBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(fakeListResponse())
		case http.MethodPost:
			postBody, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusCreated)
		}
	}))
	defer srv.Close()

	mgr := newTestManager(t, srv)
	if err := mgr.EnsureGCSWarehouseInProject(t.Context(), "proj-1", "wh-gcs", GCSWarehouseProfile{
		Bucket:         "datuplet-gcs",
		StsEnabled:     true,
		CredentialType: "system-identity",
	}); err != nil {
		t.Fatalf("EnsureGCSWarehouseInProject: %v", err)
	}
	if !strings.Contains(string(postBody), `"credential-type":"gcp-system-identity"`) {
		t.Errorf("expected gcp-system-identity credential-type; body: %s", string(postBody))
	}
	if !strings.Contains(string(postBody), `"bucket":"datuplet-gcs"`) {
		t.Errorf("expected bucket=datuplet-gcs; body: %s", string(postBody))
	}
	if !strings.Contains(string(postBody), `"type":"gcs"`) {
		t.Errorf("expected storage-profile type=gcs; body: %s", string(postBody))
	}
}

// TestEnsureGCSWarehouseInProjectCreatesServiceAccountKey asserts:
//   - service-account-key credential-type is set
//   - the parsed JSON object lives under "key" (NOT a raw string)
func TestEnsureGCSWarehouseInProjectCreatesServiceAccountKey(t *testing.T) {
	var postBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(fakeListResponse())
		case http.MethodPost:
			_ = json.NewDecoder(r.Body).Decode(&postBody)
			w.WriteHeader(http.StatusCreated)
		}
	}))
	defer srv.Close()

	mgr := newTestManager(t, srv)
	saJSON := `{"type":"service_account","project_id":"p","private_key":"PRIV"}`
	if err := mgr.EnsureGCSWarehouseInProject(t.Context(), "proj-1", "wh-gcs", GCSWarehouseProfile{
		Bucket:                "datuplet-gcs",
		StsEnabled:            true,
		CredentialType:        "service-account-key",
		ServiceAccountKeyJSON: saJSON,
	}); err != nil {
		t.Fatalf("EnsureGCSWarehouseInProject: %v", err)
	}

	cred, ok := postBody["storage-credential"].(map[string]any)
	if !ok {
		t.Fatalf("expected storage-credential object; got %#v", postBody["storage-credential"])
	}
	if cred["credential-type"] != "service-account-key" {
		t.Errorf("expected credential-type=service-account-key; got %v", cred["credential-type"])
	}
	keyObj, ok := cred["key"].(map[string]any)
	if !ok {
		t.Fatalf("expected key to be a JSON object (parsed), got %T: %#v", cred["key"], cred["key"])
	}
	if keyObj["type"] != "service_account" {
		t.Errorf("expected parsed key.type=service_account; got %v", keyObj["type"])
	}
}

// TestEnsureGCSWarehouseInProjectIdempotent: probe finds existing → no POST.
func TestEnsureGCSWarehouseInProjectIdempotent(t *testing.T) {
	var postCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(fakeListResponse("wh-gcs"))
		case http.MethodPost:
			postCount++
			w.WriteHeader(http.StatusCreated)
		}
	}))
	defer srv.Close()

	mgr := newTestManager(t, srv)
	if err := mgr.EnsureGCSWarehouseInProject(t.Context(), "proj-1", "wh-gcs", GCSWarehouseProfile{
		Bucket:         "datuplet-gcs",
		StsEnabled:     true,
		CredentialType: "system-identity",
	}); err != nil {
		t.Fatalf("EnsureGCSWarehouseInProject: %v", err)
	}
	if postCount != 0 {
		t.Fatalf("expected zero POSTs (warehouse already exists); got %d", postCount)
	}
}

// TestEnsureGCSWarehouseInProjectRejectsSystemIdentityWithoutSTS mirrors
// gcsSpec.Validate (cmd/pipeline-api/admin_lakekeeper.go): WIF with
// StsEnabled=false must fail upfront.
func TestEnsureGCSWarehouseInProjectRejectsSystemIdentityWithoutSTS(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// If we get here the validation failed silently — fail the test.
		t.Errorf("unexpected HTTP call to lakekeeper (%s %s); validation should reject first",
			r.Method, r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	mgr := newTestManager(t, srv)
	err := mgr.EnsureGCSWarehouseInProject(t.Context(), "proj-1", "wh-gcs", GCSWarehouseProfile{
		Bucket:         "datuplet-gcs",
		StsEnabled:     false,
		CredentialType: "system-identity",
	})
	if err == nil {
		t.Fatal("expected error for system-identity + StsEnabled=false; got nil")
	}
	if !strings.Contains(err.Error(), "sts-enabled") && !strings.Contains(err.Error(), "STS") {
		t.Errorf("expected error to mention STS requirement; got: %v", err)
	}
}

// TestEnsureGCSWarehouseInProjectRedactsInvalidKeyJSON: a malformed
// ServiceAccountKeyJSON must produce an error that does NOT echo the
// malformed JSON body (which may contain a private key).
func TestEnsureGCSWarehouseInProjectRedactsInvalidKeyJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(fakeListResponse())
		case http.MethodPost:
			t.Errorf("unexpected POST: JSON parse should have failed before this")
			w.WriteHeader(http.StatusCreated)
		}
	}))
	defer srv.Close()

	// Malformed JSON with secret-looking content. The error must NOT contain
	// any of these markers.
	const secretMarker = "TOPSECRETPRIVATEKEY_AAA123"
	mgr := newTestManager(t, srv)
	err := mgr.EnsureGCSWarehouseInProject(t.Context(), "proj-1", "wh-gcs", GCSWarehouseProfile{
		Bucket:                "datuplet-gcs",
		StsEnabled:            true,
		CredentialType:        "service-account-key",
		ServiceAccountKeyJSON: `{"private_key":"` + secretMarker + `"` /* malformed: missing brace */,
	})
	if err == nil {
		t.Fatal("expected error for malformed SA key JSON; got nil")
	}
	if strings.Contains(err.Error(), secretMarker) {
		t.Fatalf("error must not echo SA key content (security regression); got: %v", err)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "invalid") {
		t.Errorf("expected error to mention 'invalid'; got: %v", err)
	}
}
