package lakekeeper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

// TestNewResolver_RequiresURL: a resolver without a lakekeeper URL is
// useless; constructor enforces this.
func TestNewResolver_RequiresURL(t *testing.T) {
	_, err := NewResolver("", "datuplet", "", "")
	if err == nil {
		t.Fatal("NewResolver(\"\", ...): expected error, got nil")
	}
	if !strings.Contains(err.Error(), "URL is required") {
		t.Errorf("NewResolver: unexpected error: %v", err)
	}
}

// TestNewResolver_OK: a non-empty URL succeeds. Constructor doesn't
// dial lakekeeper — opening a connection is deferred to LoadOrCreate
// / LoadTableForRead.
func TestNewResolver_OK(t *testing.T) {
	r, err := NewResolver("http://example.invalid:8181/catalog", "datuplet", "jwt-1", "11111111-2222-3333-4444-555555555555")
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	if r == nil {
		t.Fatal("NewResolver: nil resolver, no error")
	}
	if r.URL != "http://example.invalid:8181/catalog" {
		t.Errorf("URL = %q, want \"http://example.invalid:8181/catalog\"", r.URL)
	}
	if r.Warehouse != "datuplet" {
		t.Errorf("Warehouse = %q, want \"datuplet\"", r.Warehouse)
	}
	if r.Token != "jwt-1" {
		t.Errorf("Token = %q, want jwt-1", r.Token)
	}
	if r.ProjectID != "11111111-2222-3333-4444-555555555555" {
		t.Errorf("ProjectID = %q, want UUID", r.ProjectID)
	}
}

// TestBuildBackend_FileScheme: a file:// data prefix yields a
// LocalBackend and nil VendedCreds via the scheme dispatcher. Used by
// tests + local-mode dev where no S3 client is needed.
func TestBuildBackend_FileScheme(t *testing.T) {
	r := &Resolver{URL: "http://example.invalid:8181/catalog", Warehouse: "datuplet"}
	be, vc, err := r.buildBackend("file:///tmp/warehouse/raw/orders/data/", "raw", "orders")
	if err != nil {
		t.Fatalf("buildBackend file://: %v", err)
	}
	if be == nil {
		t.Fatal("buildBackend file://: nil backend")
	}
	if vc != nil {
		t.Error("buildBackend file://: VendedCreds should be nil for file:// (no STS)")
	}
}

// TestBuildBackend_UnsupportedScheme: a path that's neither s3://, gs://,
// nor file:// is rejected by the dispatcher. Catches a misconfigured
// lakekeeper warehouse returning e.g. an HTTP URL.
func TestBuildBackend_UnsupportedScheme(t *testing.T) {
	r := &Resolver{URL: "http://example.invalid:8181/catalog", Warehouse: "datuplet"}
	_, _, err := r.buildBackend("http://example.invalid/data/", "raw", "orders")
	if err == nil {
		t.Fatal("buildBackend http://: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported scheme") {
		t.Errorf("buildBackend http://: unexpected error: %v", err)
	}
}

// TestBuildS3Backend_S3SchemeMissingBucket: an s3:// URL with no
// bucket segment is rejected.
func TestBuildS3Backend_S3SchemeMissingBucket(t *testing.T) {
	r := &Resolver{URL: "http://example.invalid:8181/catalog", Warehouse: "datuplet"}
	_, _, err := r.buildS3Backend("s3:///orphan-prefix/data/", "raw", "orders")
	if err == nil {
		t.Fatal("buildS3Backend s3:/// (empty bucket): expected error, got nil")
	}
	if !strings.Contains(err.Error(), "cannot derive bucket") {
		t.Errorf("buildS3Backend s3:/// : unexpected error: %v", err)
	}
}

// TestBuildS3Backend_S3WithToken: an s3:// data prefix with a real
// bucket succeeds and VendedCreds is wired up. The token comes from
// r.Token (single per-run JWT).
func TestBuildS3Backend_S3WithToken(t *testing.T) {
	stsResp := `{
	  "config": {
	    "s3.access-key-id":     "AKIA-VENDED",
	    "s3.secret-access-key": "secret-vended",
	    "s3.session-token":     "session-vended",
	    "s3.endpoint":          "http://minio.example:9000",
	    "s3.region":            "local-01",
	    "s3.expires-at-ms":     "9999999999999"
	  }
	}`
	const warehouseUUID = "019dceed-aaaa-bbbb-cccc-111122223333"
	var seenAuth string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if seenAuth == "" {
			seenAuth = r.Header.Get("Authorization")
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/v1/config") {
			_, _ = w.Write([]byte(`{"defaults":{"prefix":"` + warehouseUUID + `"}}`))
			return
		}
		_, _ = w.Write([]byte(stsResp))
	}))
	defer stub.Close()

	r := &Resolver{
		URL:       stub.URL,
		Warehouse: "datuplet",
		Token:     "jwt-run-token",
	}
	dataPrefix := "s3://datuplet/" + warehouseUUID + "/019dceed-tbluuid/data/"
	be, vc, err := r.buildS3Backend(dataPrefix, "raw", "orders")
	if err != nil {
		t.Fatalf("buildS3Backend s3://: %v", err)
	}
	if be == nil {
		t.Fatal("buildS3Backend s3://: nil backend")
	}
	if vc == nil {
		t.Fatal("buildS3Backend s3://: nil VendedCreds, want populated")
	}
	if vc.Namespace != "raw" || vc.Table != "orders" {
		t.Errorf("VendedCreds (ns, tbl) = (%q, %q), want (raw, orders)", vc.Namespace, vc.Table)
	}
	if vc.LakekeeperURL != r.URL {
		t.Errorf("VendedCreds.LakekeeperURL = %q, want %q", vc.LakekeeperURL, r.URL)
	}
	if vc.Prefix != warehouseUUID {
		t.Errorf("VendedCreds.Prefix = %q, want %q (warehouse-uuid from dataPrefix)", vc.Prefix, warehouseUUID)
	}
	tok, terr := vc.TokenProvider(context.Background())
	if terr != nil {
		t.Fatalf("VendedCreds.TokenProvider: %v", terr)
	}
	if tok != "jwt-run-token" {
		t.Errorf("VendedCreds.TokenProvider() = %q, want \"jwt-run-token\"", tok)
	}
	if !strings.Contains(seenAuth, "jwt-run-token") {
		t.Errorf("STS Authorization header = %q, want it to contain \"jwt-run-token\"", seenAuth)
	}
}

// TestBuildBackendRejectsUnknownScheme: buildBackend must return an
// "unsupported scheme" error for paths that are neither s3://, gs://, nor
// file://. The error message must mention gs:// as a supported scheme so
// operators know GCS is available.
func TestBuildBackendRejectsUnknownScheme(t *testing.T) {
	r := &Resolver{URL: "http://example.invalid:8181/catalog", Warehouse: "datuplet"}
	_, _, err := r.buildBackend("azure://x/y", "ns", "tbl")
	if err == nil || !strings.Contains(err.Error(), "unsupported scheme") {
		t.Fatalf("expected unsupported-scheme error, got %v", err)
	}
	if !strings.Contains(err.Error(), "gs://") {
		t.Fatalf("error message should mention gs:// as a supported scheme; got %v", err)
	}
}

// TestBuildBackendDispatchesGCS: a gs:// data prefix is routed to
// buildGCSBackend via buildBackend. The returned backend must be non-nil,
// VendedCreds must be non-nil, and ExpectedCredsType must be CredsTypeGCS.
func TestBuildBackendDispatchesGCS(t *testing.T) {
	r := &Resolver{
		URL:       "http://test.example/catalog",
		Warehouse: "test-warehouse",
		ProjectID: "proj",
		Token:     "fake-token",
	}
	be, vc, err := r.buildBackend("gs://test-bucket/some/prefix", "ns", "tbl")
	if err != nil {
		t.Fatal(err)
	}
	if be == nil {
		t.Fatal("nil backend")
	}
	if vc == nil {
		t.Fatal("nil VendedCreds")
	}
	if vc.ExpectedCredsType != catalogwriter.CredsTypeGCS {
		t.Fatalf("ExpectedCredsType = %q, want gcs", vc.ExpectedCredsType)
	}
}

// TestNewClient_TokenForwarded: the per-run token in r.Token flows
// through to catalogwriter.NewClient via the resolver's per-call
// closure. Uses an httptest server that captures the Authorization
// header from iceberg-go's catalog handshake.
func TestNewClient_TokenForwarded(t *testing.T) {
	var seenAuth string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if seenAuth == "" {
			seenAuth = r.Header.Get("Authorization")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
	}))
	defer stub.Close()

	r := &Resolver{
		URL:       stub.URL,
		Warehouse: "datuplet",
		Token:     "jwt-from-run-token",
	}
	_, err := r.newClient(context.Background(), r.Token)
	if err != nil {
		t.Logf("newClient (expected, may fail handshake against minimal stub): %v", err)
	}
	if seenAuth == "" {
		t.Skip("stub never received a request — iceberg-go didn't attempt the handshake")
	}
	if !strings.Contains(seenAuth, "jwt-from-run-token") {
		t.Errorf("Authorization header = %q, want it to contain \"jwt-from-run-token\"", seenAuth)
	}
}
