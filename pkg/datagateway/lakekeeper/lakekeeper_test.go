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
// dial lakekeeper — opening a connection is deferred to LoadOrCreate.
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

// TestWriteCreds_S3FetchesWithRunToken: an s3:// writer's VendedCreds
// fetches STS credentials from lakekeeper with r.Token (single per-run JWT)
// and resolves the warehouse prefix through /v1/config.
func TestWriteCreds_S3FetchesWithRunToken(t *testing.T) {
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
	vc, err := r.writeCreds(dataPrefix, "raw", "orders")
	if err != nil || vc == nil {
		t.Fatalf("writeCreds s3:// = %v, %v; want populated VendedCreds", vc, err)
	}
	creds, err := vc.Get(context.Background())
	if err != nil {
		t.Fatalf("VendedCreds.Get: %v", err)
	}
	if s3, ok := creds.(catalogwriter.S3Creds); !ok || s3.AccessKeyID != "AKIA-VENDED" {
		t.Errorf("vended creds = %v, want the stub's S3 credentials", creds)
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

// TestWriteCreds_PerScheme: writers get a per-table VendedCreds of the
// scheme's credential type (no storage backend is built for writes), nil for
// file://, and an error for an unknown scheme.
func TestWriteCreds_PerScheme(t *testing.T) {
	r := &Resolver{URL: "http://lk.invalid/catalog", Warehouse: "wh", Token: "jwt", ProjectID: "proj"}
	for _, tc := range []struct {
		prefix string
		want   catalogwriter.CredsType
	}{
		{"s3://bucket/wh/t/data/", catalogwriter.CredsTypeS3},
		{"gs://bucket/wh/t/data/", catalogwriter.CredsTypeGCS},
	} {
		vc, err := r.writeCreds(tc.prefix, "raw", "orders")
		if err != nil || vc == nil {
			t.Fatalf("%s: writeCreds = %v, %v", tc.prefix, vc, err)
		}
		if vc.ExpectedCredsType != tc.want || vc.Namespace != "raw" || vc.Table != "orders" ||
			vc.WarehouseName != "wh" || vc.ProjectID != "proj" || vc.LakekeeperURL != r.URL {
			t.Errorf("%s: VendedCreds = %+v", tc.prefix, vc)
		}
		if tok, _ := vc.TokenProvider(context.Background()); tok != "jwt" {
			t.Errorf("%s: token provider returned %q", tc.prefix, tok)
		}
	}
	if vc, err := r.writeCreds("file:///tmp/wh/t/data/", "raw", "orders"); err != nil || vc != nil {
		t.Errorf("file://: writeCreds = %v, %v; want nil, nil", vc, err)
	}
	if _, err := r.writeCreds("http://x/data/", "raw", "orders"); err == nil || !strings.Contains(err.Error(), "unsupported scheme") {
		t.Errorf("http://: err = %v, want unsupported scheme", err)
	}
}
