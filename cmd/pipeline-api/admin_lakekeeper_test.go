package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBuildWarehouseBody_S3(t *testing.T) {
	body, err := buildWarehouseBody(warehouseSpec{
		WarehouseName: "datuplet",
		Type:          "s3",
		S3: &s3Spec{
			Bucket:          "datuplet",
			Region:          "us-east-1",
			Endpoint:        "http://minio:9000",
			PathStyleAccess: true,
			StsEnabled:      true,
			AccessKey:       "ak",
			SecretKey:       "sk",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if body["warehouse-name"] != "datuplet" {
		t.Errorf("warehouse-name = %v", body["warehouse-name"])
	}
	profile, ok := body["storage-profile"].(map[string]any)
	if !ok {
		t.Fatalf("storage-profile is not map[string]any: %T", body["storage-profile"])
	}
	if profile["type"] != "s3" {
		t.Errorf("type = %v, want s3", profile["type"])
	}
	if profile["bucket"] != "datuplet" {
		t.Errorf("bucket = %v", profile["bucket"])
	}
	if profile["region"] != "us-east-1" {
		t.Errorf("region = %v", profile["region"])
	}
	if profile["endpoint"] != "http://minio:9000" {
		t.Errorf("endpoint = %v", profile["endpoint"])
	}
	if profile["path-style-access"] != true {
		t.Errorf("path-style-access = %v", profile["path-style-access"])
	}
	if profile["sts-enabled"] != true {
		t.Errorf("sts-enabled = %v", profile["sts-enabled"])
	}
	cred, ok := body["storage-credential"].(map[string]any)
	if !ok {
		t.Fatalf("storage-credential is not map[string]any: %T", body["storage-credential"])
	}
	if cred["aws-access-key-id"] != "ak" {
		t.Errorf("access key = %v", cred["aws-access-key-id"])
	}
	if cred["aws-secret-access-key"] != "sk" {
		t.Errorf("secret key = %v", cred["aws-secret-access-key"])
	}
	if cred["type"] != "s3" {
		t.Errorf("credential type = %v", cred["type"])
	}
	if cred["credential-type"] != "access-key" {
		t.Errorf("credential-type = %v", cred["credential-type"])
	}
}

func TestBuildWarehouseBody_GCS(t *testing.T) {
	saKeyJSON := `{"type":"service_account","project_id":"my-proj","private_key":"X","client_email":"sa@x.iam"}`
	body, err := buildWarehouseBody(warehouseSpec{
		WarehouseName: "my-warehouse",
		Type:          "gcs",
		GCS: &gcsSpec{
			Bucket:                "my-gcs-bucket",
			KeyPrefix:             "datuplet",
			StsEnabled:            true,
			CredentialType:        "service-account-key",
			ServiceAccountKeyJSON: saKeyJSON,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	profile, ok := body["storage-profile"].(map[string]any)
	if !ok {
		t.Fatalf("storage-profile is not map[string]any: %T", body["storage-profile"])
	}
	if profile["type"] != "gcs" {
		t.Errorf("type = %v, want gcs", profile["type"])
	}
	if profile["bucket"] != "my-gcs-bucket" {
		t.Errorf("bucket = %v", profile["bucket"])
	}
	if profile["key-prefix"] != "datuplet" {
		t.Errorf("key-prefix = %v", profile["key-prefix"])
	}
	if profile["sts-enabled"] != true {
		t.Errorf("sts-enabled = %v", profile["sts-enabled"])
	}
	cred, ok := body["storage-credential"].(map[string]any)
	if !ok {
		t.Fatalf("storage-credential is not map[string]any: %T", body["storage-credential"])
	}
	if cred["credential-type"] != "service-account-key" {
		t.Errorf("credential-type = %v", cred["credential-type"])
	}
	keyObj, ok := cred["key"].(map[string]any)
	if !ok {
		t.Fatalf("key field is not map[string]any: %T", cred["key"])
	}
	if keyObj["project_id"] != "my-proj" {
		t.Errorf("project_id = %v", keyObj["project_id"])
	}
	if keyObj["client_email"] != "sa@x.iam" {
		t.Errorf("client_email = %v", keyObj["client_email"])
	}
}

func TestBuildWarehouseBody_GCS_InvalidJSON(t *testing.T) {
	// Malformed SA key JSON should produce an error rather than silently
	// embedding empty/invalid creds.
	_, err := buildWarehouseBody(warehouseSpec{
		WarehouseName: "wh",
		Type:          "gcs",
		GCS: &gcsSpec{
			Bucket:                "b",
			CredentialType:        "service-account-key",
			ServiceAccountKeyJSON: "not-valid-json",
		},
	})
	if err == nil {
		t.Fatalf("expected error for invalid SA key JSON, got nil")
	}
	// Security: the error must NOT echo the JSON content (could leak the key).
	if strings.Contains(err.Error(), "not-valid-json") {
		t.Errorf("error message must not contain the SA key JSON content: %v", err)
	}
}

func TestBuildWarehouseBody_UnknownType(t *testing.T) {
	_, err := buildWarehouseBody(warehouseSpec{
		WarehouseName: "wh",
		Type:          "azure",
	})
	if err == nil {
		t.Fatalf("expected error for unknown type, got nil")
	}
}

func TestBuildWarehouseBody_S3_MissingSpec(t *testing.T) {
	_, err := buildWarehouseBody(warehouseSpec{
		WarehouseName: "wh",
		Type:          "s3",
		S3:            nil,
	})
	if err == nil {
		t.Fatalf("expected error for s3 type with nil S3 spec, got nil")
	}
}

func TestBuildWarehouseBody_GCS_MissingSpec(t *testing.T) {
	_, err := buildWarehouseBody(warehouseSpec{
		WarehouseName: "wh",
		Type:          "gcs",
		GCS:           nil,
	})
	if err == nil {
		t.Fatalf("expected error for gcs type with nil GCS spec, got nil")
	}
}

func TestServerAdminTupleWrite(t *testing.T) {
	var writeBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/changes"):
			fmt.Fprintln(w, `{"changes":[{"tuple_key":{"object":"server:abc-123-def-456-789a-bcdef0123456"}}]}`)
		case strings.HasSuffix(r.URL.Path, "/write"):
			b, _ := io.ReadAll(r.Body)
			writeBody = string(b)
			w.WriteHeader(200)
		}
	}))
	defer srv.Close()

	err := writeServerAdminTuple(context.Background(), srv.URL, "test-key", "store-uuid")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(writeBody, `"user":"user:oidc~admin"`) ||
		!strings.Contains(writeBody, `"object":"server:abc-123-def-456-789a-bcdef0123456"`) {
		t.Fatalf("expected tuple write, got: %s", writeBody)
	}
}

// TestBuildWarehouseBodySystemIdentity and TestBuildWarehouseBodyServiceAccountKey
// verify the system-identity and service-account-key branches of buildWarehouseBody.

func TestBuildWarehouseBodySystemIdentity(t *testing.T) {
	spec := warehouseSpec{
		WarehouseName:       "test",
		LakekeeperProjectID: "00000000-0000-0000-0000-000000000000",
		Type:                "gcs",
		GCS:                 &gcsSpec{Bucket: "b", CredentialType: "system-identity"},
	}
	body, err := buildWarehouseBody(spec)
	if err != nil {
		t.Fatal(err)
	}
	cred := body["storage-credential"].(map[string]any)
	if cred["credential-type"] != "gcp-system-identity" {
		t.Fatalf("credential-type = %q, want gcp-system-identity", cred["credential-type"])
	}
	if _, hasKey := cred["key"]; hasKey {
		t.Fatal("system-identity body must NOT carry a key field")
	}
}

func TestBuildWarehouseBodyServiceAccountKey(t *testing.T) {
	spec := warehouseSpec{
		WarehouseName:       "test",
		LakekeeperProjectID: "00000000-0000-0000-0000-000000000000",
		Type:                "gcs",
		GCS: &gcsSpec{
			Bucket:                "b",
			CredentialType:        "service-account-key",
			ServiceAccountKeyJSON: `{"type":"service_account","project_id":"x"}`,
		},
	}
	body, err := buildWarehouseBody(spec)
	if err != nil {
		t.Fatal(err)
	}
	cred := body["storage-credential"].(map[string]any)
	if cred["credential-type"] != "service-account-key" {
		t.Fatalf("credential-type = %q", cred["credential-type"])
	}
	if _, hasKey := cred["key"]; !hasKey {
		t.Fatal("SA-key body must carry a key field")
	}
}

// TestGCSSpec* tests exercise gcsSpec.Validate() — mutual-exclusion and unknown
// credential type cases.

func TestGCSSpecValidateSystemIdentityDefault(t *testing.T) {
	s := &gcsSpec{Bucket: "b", CredentialType: ""}
	if err := s.Validate(); err != nil {
		t.Fatalf("default (system-identity) happy path: %v", err)
	}
}

func TestGCSSpecValidateSystemIdentityExplicit(t *testing.T) {
	s := &gcsSpec{Bucket: "b", CredentialType: "system-identity"}
	if err := s.Validate(); err != nil {
		t.Fatalf("system-identity happy path: %v", err)
	}
	s.ServiceAccountKeyJSON = "{...}"
	err := s.Validate()
	if err == nil || !strings.Contains(err.Error(), "cannot be combined with --gcs-sa-key-file") {
		t.Fatalf("expected mutual-exclusion error, got %v", err)
	}
}

func TestGCSSpecValidateServiceAccountKey(t *testing.T) {
	s := &gcsSpec{Bucket: "b", CredentialType: "service-account-key", ServiceAccountKeyJSON: ""}
	err := s.Validate()
	if err == nil || !strings.Contains(err.Error(), "--gcs-sa-key-file") {
		t.Fatalf("expected SA-key-file required error, got %v", err)
	}
	s.ServiceAccountKeyJSON = "{...}"
	if err := s.Validate(); err != nil {
		t.Fatalf("with key set: %v", err)
	}
}

func TestGCSSpecValidateUnknownType(t *testing.T) {
	s := &gcsSpec{Bucket: "b", CredentialType: "passport"}
	err := s.Validate()
	if err == nil || !strings.Contains(err.Error(), "unknown --gcs-credential-type") {
		t.Fatalf("expected unknown-type error, got %v", err)
	}
}

// TestBootstrapGrantsServiceIdentityProjectAdmin exercises
// `grantServiceIdentityProjectAdminIfMissing`, which is the helper extracted
// from `adminLakekeeperBootstrap` so we can verify (without a real Lakekeeper):
//
//  1. On a non-default project + Check=false → exactly one Write follows.
//  2. On a non-default project + Check=true (idempotent re-run) → NO Write.
//  3. On the default lakekeeper project UUID → neither Check nor Write.
//  4. When fgaURL is empty → neither Check nor Write (local-mode-without-FGA).
//
// The helper is the unit-testable seam; the env-driven OPENFGA_URL skip lives
// in adminLakekeeperBootstrap itself (case 4 below mirrors what the caller
// does in that branch — calling the helper with fgaURL="").
func TestBootstrapGrantsServiceIdentityProjectAdmin(t *testing.T) {
	const (
		nonDefaultProjectID = "deadbeef-dead-beef-dead-beefdeadbeef"
		userSub             = bootstrapServiceSubject
		storeID             = "store-uuid"
	)

	t.Run("non-default project, missing tuple => Check then Write", func(t *testing.T) {
		var checks, writes int
		var writeBody string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasSuffix(r.URL.Path, "/check"):
				checks++
				_, _ = w.Write([]byte(`{"allowed":false}`))
			case strings.HasSuffix(r.URL.Path, "/write"):
				writes++
				b, _ := io.ReadAll(r.Body)
				writeBody = string(b)
				w.WriteHeader(200)
			default:
				t.Errorf("unexpected request to %s", r.URL.Path)
				w.WriteHeader(404)
			}
		}))
		defer srv.Close()

		if err := grantServiceIdentityProjectAdminIfMissing(context.Background(),
			srv.URL, "test-key", storeID, userSub, nonDefaultProjectID); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if checks != 1 {
			t.Errorf("checks = %d, want 1", checks)
		}
		if writes != 1 {
			t.Errorf("writes = %d, want 1", writes)
		}
		// The write must target user:oidc~<sub>, relation=project_admin,
		// object=project:<id>.
		if !strings.Contains(writeBody, `"user":"user:oidc~`+userSub+`"`) ||
			!strings.Contains(writeBody, `"relation":"project_admin"`) ||
			!strings.Contains(writeBody, `"object":"project:`+nonDefaultProjectID+`"`) {
			t.Fatalf("unexpected write body: %s", writeBody)
		}
	})

	t.Run("non-default project, existing tuple => Check only, no Write", func(t *testing.T) {
		var checks, writes int
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasSuffix(r.URL.Path, "/check"):
				checks++
				_, _ = w.Write([]byte(`{"allowed":true}`))
			case strings.HasSuffix(r.URL.Path, "/write"):
				writes++
				w.WriteHeader(200)
			default:
				t.Errorf("unexpected request to %s", r.URL.Path)
				w.WriteHeader(404)
			}
		}))
		defer srv.Close()

		if err := grantServiceIdentityProjectAdminIfMissing(context.Background(),
			srv.URL, "test-key", storeID, userSub, nonDefaultProjectID); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if checks != 1 {
			t.Errorf("checks = %d, want 1", checks)
		}
		if writes != 0 {
			t.Errorf("writes = %d, want 0 (idempotent re-run)", writes)
		}
	})

	t.Run("default lakekeeper project => skip entirely", func(t *testing.T) {
		var hits int
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits++
			w.WriteHeader(500)
		}))
		defer srv.Close()

		if err := grantServiceIdentityProjectAdminIfMissing(context.Background(),
			srv.URL, "test-key", storeID, userSub, defaultLakekeeperProjectUUID); err != nil {
			t.Fatalf("unexpected error on default-project skip: %v", err)
		}
		if hits != 0 {
			t.Errorf("hits = %d, want 0 (default project should be skipped)", hits)
		}
	})

	t.Run("empty fgaURL => skip entirely", func(t *testing.T) {
		if err := grantServiceIdentityProjectAdminIfMissing(context.Background(),
			"", "test-key", storeID, userSub, nonDefaultProjectID); err != nil {
			t.Fatalf("unexpected error on empty-fgaURL skip: %v", err)
		}
		// Nothing to assert beyond "no panic, no error" — there's no server to count
		// hits against. The caller's own OPENFGA_URL check is the primary guard;
		// the helper's defensive skip is belt-and-braces.
	})

	t.Run("write 'already exists' is tolerated", func(t *testing.T) {
		var writes int
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasSuffix(r.URL.Path, "/check"):
				_, _ = w.Write([]byte(`{"allowed":false}`))
			case strings.HasSuffix(r.URL.Path, "/write"):
				writes++
				w.WriteHeader(400)
				_, _ = w.Write([]byte(`{"code":"write_failed_due_to_invalid_input","message":"cannot write a tuple which already exists"}`))
			}
		}))
		defer srv.Close()

		if err := grantServiceIdentityProjectAdminIfMissing(context.Background(),
			srv.URL, "test-key", storeID, userSub, nonDefaultProjectID); err != nil {
			t.Fatalf("expected idempotent 'already exists' write to be swallowed, got: %v", err)
		}
		if writes != 1 {
			t.Errorf("writes = %d, want 1", writes)
		}
	})
}

// TestPostJSON_WarehouseError_RedactsBody verifies that when redactBodyOnError
// is true, the response body (which lakekeeper may echo from the request, e.g.
// an SA key JSON) is not included in the returned error string.
func TestPostJSON_WarehouseError_RedactsBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Echo the request body back in a 400 response, simulating a lakekeeper
		// debug-mode handler that mirrors the payload.
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	secret := "TOPSECRET-private-key-content"
	payload := map[string]any{"private_key": secret}
	httpc := &http.Client{}

	// With redaction enabled: secret must NOT appear in the error.
	err := postJSON(httpc, srv.URL, "", payload, http.StatusCreated, true)
	if err == nil {
		t.Fatal("expected error for unexpected status, got nil")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("error must not contain sensitive body content; got: %v", err)
	}
	if !strings.Contains(err.Error(), "redacted") {
		t.Errorf("error should mention redaction; got: %v", err)
	}

	// With redaction disabled: the body IS included (existing behaviour).
	err = postJSON(httpc, srv.URL, "", payload, http.StatusCreated, false)
	if err == nil {
		t.Fatal("expected error for unexpected status, got nil")
	}
	// The echoed payload will contain the key name; body is not redacted here.
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("non-redacted error should include status code; got: %v", err)
	}
}
