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
