package icebergjob

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// TestParseTableManifest covers the per-table manifest decoder.
// One file = one (namespace, table). Smoke-test the happy path and a
// malformed-JSON case.
func TestParseTableManifest_RoundTrip(t *testing.T) {
	t.Parallel()
	doc := FilesManifest{
		RunID:     "run-abc",
		Namespace: "raw",
		Table:     "events",
		Paths:     []string{"s3://bucket/<wh>/<tbl>/data/a.parquet", "s3://bucket/<wh>/<tbl>/data/b.parquet"},
	}
	body, err := json.Marshal(&doc)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseTableManifest(body)
	if err != nil {
		t.Fatalf("parseTableManifest: %v", err)
	}
	if parsed.RunID != doc.RunID || parsed.Namespace != doc.Namespace || parsed.Table != doc.Table {
		t.Errorf("identifiers: got %+v want %+v", parsed, doc)
	}
	if len(parsed.Paths) != 2 {
		t.Errorf("Paths len=%d want 2", len(parsed.Paths))
	}
}

func TestParseTableManifest_Malformed(t *testing.T) {
	t.Parallel()
	if _, err := parseTableManifest([]byte("{not valid json")); err == nil {
		t.Fatal("expected error on malformed JSON, got nil")
	}
}

// TestFilesManifestPath covers the per-table path derivation. The
// input is always an iceberg-go Table.Location() value — only a trailing
// slash is stripped; the `/data` suffix is NOT stripped because a table
// named "data" would otherwise have its path corrupted.
func TestFilesManifestPath(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		tableBase string
		runID     string
		want      string
	}{
		{
			name:      "table base no slash",
			tableBase: "s3://b/<wh>/<tbl>",
			runID:     "r",
			want:      "s3://b/<wh>/<tbl>/.run-state/r/files.json",
		},
		{
			name:      "table base trailing slash",
			tableBase: "s3://b/<wh>/<tbl>/",
			runID:     "r",
			want:      "s3://b/<wh>/<tbl>/.run-state/r/files.json",
		},
		{
			// Table.Location() for lakekeeper UUID paths ends in "/" — strip it
			// and place .run-state at the table root (no /data stripping).
			name:      "lakekeeper UUID path with trailing slash",
			tableBase: "s3://b/<storage-uuid>/<table-uuid>/",
			runID:     "r",
			want:      "s3://b/<storage-uuid>/<table-uuid>/.run-state/r/files.json",
		},
		{
			// SQLite catalog: table named "data" — must NOT strip /data suffix.
			name:      "SQLite catalog table named data",
			tableBase: "file:///warehouse/ns.db/data",
			runID:     "r",
			want:      "file:///warehouse/ns.db/data/.run-state/r/files.json",
		},
		{
			// SQLite catalog: normal table name — unchanged behaviour.
			name:      "SQLite catalog table named products",
			tableBase: "file:///warehouse/ns.db/products",
			runID:     "r",
			want:      "file:///warehouse/ns.db/products/.run-state/r/files.json",
		},
		{
			name:      "empty base",
			tableBase: "",
			runID:     "r",
			want:      "",
		},
		{
			name:      "empty runID",
			tableBase: "s3://b/<wh>/<tbl>/",
			runID:     "",
			want:      "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := FilesManifestPath(tc.tableBase, tc.runID); got != tc.want {
				t.Errorf("FilesManifestPath(%q, %q) = %q, want %q", tc.tableBase, tc.runID, got, tc.want)
			}
		})
	}
}

// TestIsNotFoundErr covers the substring detection used to distinguish
// "manifest absent → success-zero" from "transient I/O → fail".
func TestIsNotFoundErr(t *testing.T) {
	t.Parallel()
	yes := []string{
		"open foo: no such file or directory",
		"NoSuchKey: The specified key does not exist",
		"HTTP 404 Not Found",
		"object does not exist",
	}
	for _, m := range yes {
		if !isNotFoundErr(errors.New(m)) {
			t.Errorf("isNotFoundErr(%q) = false, want true", m)
		}
	}
	no := []string{
		"connection refused",
		"timeout",
		"forbidden",
		"",
	}
	for _, m := range no {
		err := errors.New(m)
		if m == "" {
			err = nil
		}
		if isNotFoundErr(err) {
			t.Errorf("isNotFoundErr(%q) = true, want false", m)
		}
	}
}

func TestNewValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		mut         func(c *Config)
		errContains string
	}{
		{"empty run id", func(c *Config) { c.RunID = "" }, "run_id"},
		{"empty lakekeeper url", func(c *Config) { c.LakekeeperURL = "" }, "lakekeeper_url"},
		{"missing namespace", func(c *Config) {
			c.Tables = []TableConfig{{Namespace: "", Table: "t"}}
		}, "namespace + table"},
		{"missing table", func(c *Config) {
			c.Tables = []TableConfig{{Namespace: "ns", Table: ""}}
		}, "namespace + table"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				RunID:         "run-1",
				LakekeeperURL: "http://lk:8181/catalog",
				Tables:        []TableConfig{{Namespace: "raw", Table: "t"}},
			}
			tt.mut(c)
			_, err := New(c)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("err=%v want substring %q", err, tt.errContains)
			}
		})
	}
}

// ============================================================================
// Run-token validation in Execute
// ============================================================================

// tc017GenKey generates a 2048-bit RSA keypair; fatal on error.
func tc017GenKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	return priv
}

// tc017MakeJWKS builds a minimal JWKS JSON for a single RSA public key.
func tc017MakeJWKS(kid string, pub *rsa.PublicKey) []byte {
	type jwkEntry struct {
		Kty string `json:"kty"`
		Kid string `json:"kid"`
		N   string `json:"n"`
		E   string `json:"e"`
	}
	type jwksDoc struct {
		Keys []jwkEntry `json:"keys"`
	}
	eBytes := new(big.Int).SetInt64(int64(pub.E)).Bytes()
	doc := jwksDoc{Keys: []jwkEntry{{
		Kty: "RSA",
		Kid: kid,
		N:   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
		E:   base64.RawURLEncoding.EncodeToString(eBytes),
	}}}
	b, _ := json.Marshal(doc)
	return b
}

// tc017MintToken mints a valid RS256 run-token JWT.
func tc017MintToken(t *testing.T, priv *rsa.PrivateKey, kid, runID, warehouse, projectID string) string {
	t.Helper()
	now := time.Now()
	claims := jwt.MapClaims{
		"iss":        "datuplet-api",
		"aud":        "datuplet-catalog",
		"sub":        runID,
		"run_id":     runID,
		"token_kind": "run",
		"project_id": projectID,
		"warehouse":  warehouse,
		"iat":        now.Unix(),
		"nbf":        now.Unix(),
		"exp":        now.Add(24 * time.Hour).Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = kid
	signed, err := tok.SignedString(priv)
	if err != nil {
		t.Fatalf("jwt.SignedString: %v", err)
	}
	return signed
}

// TestExecute_RunTokenPath_MissingJWKSURL verifies that Execute fails closed
// when RunTokenPath is set but PipelineAPIJWKSURL is empty.
func TestExecute_RunTokenPath_MissingJWKSURL(t *testing.T) {
	t.Parallel()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("placeholder"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	cfg := &Config{
		RunID:         "run-d1",
		LakekeeperURL: "http://lk:8181/catalog",
		RunTokenPath:  tokenFile,
		// PipelineAPIJWKSURL intentionally empty — must fail closed.
	}
	tc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = tc.Execute(context.Background())
	if err == nil {
		t.Fatal("expected error when RunTokenPath set but PipelineAPIJWKSURL empty")
	}
	if !strings.Contains(err.Error(), "PipelineAPIJWKSURL") {
		t.Errorf("error should mention PipelineAPIJWKSURL, got: %v", err)
	}
}

// TestExecute_RunTokenPath_MissingRUNID verifies that Execute fails when
// RUN_ID env is unset (required for check 8 Secret-swap defence).
func TestExecute_RunTokenPath_MissingRUNID(t *testing.T) {
	// Deliberately clear RUN_ID for this test.
	t.Setenv("RUN_ID", "")

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("placeholder"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	cfg := &Config{
		RunID:              "run-d2",
		LakekeeperURL:      "http://lk:8181/catalog",
		RunTokenPath:       tokenFile,
		PipelineAPIJWKSURL: "http://pipeline-api.datuplet.svc.cluster.local:8081/api/v1/auth/jwks.json",
	}
	tc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = tc.Execute(context.Background())
	if err == nil {
		t.Fatal("expected error when RUN_ID env unset")
	}
	if !strings.Contains(err.Error(), "RUN_ID") {
		t.Errorf("error should mention RUN_ID, got: %v", err)
	}
}

// TestExecute_RunTokenPath_ValidJWT_RoutesFromClaims verifies the happy path:
// Execute validates the JWT, sets validatedClaims, and the
// warehouseForCall / projectIDForCall helpers return claim values.
// The test does NOT call a real lakekeeper — Execute will fail when it
// tries to reach lakekeeper, but the error will be a network error
// (not a validation error), confirming validation passed.
func TestExecute_RunTokenPath_ValidJWT_RoutesFromClaims(t *testing.T) {
	const kid = "tc017-key-1"
	const runID = "run-d3-valid"
	const warehouse = "wh-from-jwt"
	const projectID = "proj-from-jwt"

	priv := tc017GenKey(t)
	jwksBody := tc017MakeJWKS(kid, &priv.PublicKey)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(jwksBody) //nolint:errcheck
	}))
	defer srv.Close()

	tokenStr := tc017MintToken(t, priv, kid, runID, warehouse, projectID)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(tokenStr+"\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	t.Setenv("RUN_ID", runID)

	cfg := &Config{
		RunID:              runID,
		LakekeeperURL:      "http://lk-unreachable.invalid:8181/catalog",
		RunTokenPath:       tokenFile,
		PipelineAPIJWKSURL: srv.URL,
	}
	tc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, execErr := tc.Execute(context.Background())

	// Execute will fail trying to reach lakekeeper (unreachable host),
	// but MUST NOT fail with a validation error — validation succeeds before
	// the network call.
	if execErr != nil {
		if strings.Contains(execErr.Error(), "run-token validation") {
			t.Errorf("Execute failed on validation, not lakekeeper: %v", execErr)
		}
		// A network/catalog error is expected — that's fine.
	}

	// validatedClaims must be populated (set by Execute during validation).
	if tc.validatedClaims == nil {
		t.Fatal("validatedClaims must be non-nil after successful validation")
	}
	if tc.warehouseForCall() != warehouse {
		t.Errorf("warehouseForCall = %q, want %q", tc.warehouseForCall(), warehouse)
	}
	if tc.projectIDForCall() != projectID {
		t.Errorf("projectIDForCall = %q, want %q", tc.projectIDForCall(), projectID)
	}
}

// TestWarehouseForCall_NoClaimsReturnsEmpty verifies that warehouseForCall /
// projectIDForCall return empty string in dev mode (no JWT validated).
// Operator-supplied warehouse/project values are not accepted; downstream
// lakekeeper will error on a missing warehouse if one is required, surfacing
// the misconfiguration clearly.
func TestWarehouseForCall_NoClaimsReturnsEmpty(t *testing.T) {
	t.Parallel()
	tc := &TableCommitter{
		config:          &Config{RunID: "r1", LakekeeperURL: "http://lk:8181/catalog"},
		validatedClaims: nil, // no JWT validated — dev mode
	}
	if got := tc.warehouseForCall(); got != "" {
		t.Errorf("warehouseForCall = %q, want empty (no config fallback)", got)
	}
	if got := tc.projectIDForCall(); got != "" {
		t.Errorf("projectIDForCall = %q, want empty (no config fallback)", got)
	}
}
