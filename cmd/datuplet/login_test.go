package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeLoginServer builds an httptest.Server that responds to
// POST /api/v1/auth/token with the supplied status + body.
func fakeLoginServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/auth/token" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}))
}

// fakeJWT is a syntactically valid 3-segment JWT-shaped token for tests.
// It is NOT a real signed JWT — tests only need the shape to be correct.
const fakeJWT = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyLTEyMyJ9.c2lnbmF0dXJl"

func happyBody(t *testing.T) string {
	t.Helper()
	resp := loginResponse{
		Token:     fakeJWT,
		ExpiresAt: "2026-05-07T12:00:00Z",
		UserID:    "user-123",
		Cluster: loginResponseCluster{
			LakekeeperURL: "http://lakekeeper:8181/catalog",
			WarehouseName: "datuplet",
		},
		Projects: []clusterMetaProject{
			{ID: "p-1", Name: "default", LakekeeperProjectID: "lk-proj-1"},
		},
	}
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// setHomeDir overrides HOME to a temp directory and returns the cleanup fn.
func setHomeDir(t *testing.T) string {
	t.Helper()
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	return tmp
}

// TestLogin_PromptsAndWritesFiles verifies the happy path end-to-end:
// both ~/.datuplet/token and ~/.datuplet/cluster.json are created with
// the correct content and mode 0600.
func TestLogin_PromptsAndWritesFiles(t *testing.T) {
	home := setHomeDir(t)
	srv := fakeLoginServer(t, http.StatusOK, happyBody(t))
	defer srv.Close()

	input := strings.NewReader("alice@example.com\nhunter2\n")
	var out strings.Builder
	args := loginArgs{
		Remote: srv.URL,
		Stdin:  input,
		Stdout: &out,
		Stderr: &out,
	}

	if err := runLogin(args); err != nil {
		t.Fatalf("runLogin: %v", err)
	}

	// token file
	tokenPath := filepath.Join(home, ".datuplet", "token")
	tokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatalf("read token file: %v", err)
	}
	if string(tokenBytes) != fakeJWT {
		t.Errorf("token file = %q, want %q", string(tokenBytes), fakeJWT)
	}
	checkMode(t, tokenPath, 0o600)

	// cluster.json
	clusterPath := filepath.Join(home, ".datuplet", "cluster.json")
	clusterBytes, err := os.ReadFile(clusterPath)
	if err != nil {
		t.Fatalf("read cluster.json: %v", err)
	}
	var meta clusterMeta
	if err := json.Unmarshal(clusterBytes, &meta); err != nil {
		t.Fatalf("unmarshal cluster.json: %v", err)
	}
	if meta.LakekeeperURL != "http://lakekeeper:8181/catalog" {
		t.Errorf("lakekeeper_url = %q", meta.LakekeeperURL)
	}
	if meta.WarehouseName != "datuplet" {
		t.Errorf("warehouse_name = %q", meta.WarehouseName)
	}
	if meta.UserID != "user-123" {
		t.Errorf("user_id = %q", meta.UserID)
	}
	if meta.PipelineAPIURL != srv.URL {
		t.Errorf("pipeline_api_url = %q, want %q", meta.PipelineAPIURL, srv.URL)
	}
	checkMode(t, clusterPath, 0o600)

	// stdout must mention the email; must NOT contain the JWT
	outStr := out.String()
	if !strings.Contains(outStr, "alice@example.com") {
		t.Errorf("stdout does not mention email: %q", outStr)
	}
	if strings.Contains(outStr, fakeJWT) {
		t.Errorf("stdout must not contain the JWT token (security)")
	}
}

// TestLogin_TokenFileIsRawJWTNotJSON pins the critical security invariant:
// the token file MUST be a raw JWT (3 dot-separated base64 segments), NEVER
// the full JSON login-response blob. The gateway sidecar reads this file as
// a bare bearer string; if it were JSON the K8s Secret path and local-CLI
// path would parse inconsistently.
func TestLogin_TokenFileIsRawJWTNotJSON(t *testing.T) {
	home := setHomeDir(t)
	srv := fakeLoginServer(t, http.StatusOK, happyBody(t))
	defer srv.Close()

	input := strings.NewReader("alice@example.com\nhunter2\n")
	args := loginArgs{
		Remote: srv.URL,
		Stdin:  input,
		Stdout: &strings.Builder{},
		Stderr: &strings.Builder{},
	}
	if err := runLogin(args); err != nil {
		t.Fatalf("runLogin: %v", err)
	}

	tokenBytes, err := os.ReadFile(filepath.Join(home, ".datuplet", "token"))
	if err != nil {
		t.Fatalf("read token file: %v", err)
	}
	content := string(tokenBytes)

	// Must NOT be a JSON blob.
	if strings.HasPrefix(content, "{") {
		t.Errorf("token file starts with '{' — it is a JSON blob, not a raw JWT. "+
			"content: %q", content)
	}

	// Must contain at least one '.' (JWT header.payload.signature separator).
	if !strings.Contains(content, ".") {
		t.Errorf("token file contains no '.' separator — does not look like a JWT. "+
			"content: %q", content)
	}

	// Must have exactly three dot-separated segments (JWT structure).
	parts := strings.Split(content, ".")
	if len(parts) != 3 {
		t.Errorf("token file has %d dot-separated segments, want 3 (JWT). content: %q",
			len(parts), content)
	}
}

// TestLogin_BadCredentialsExitsNonZero verifies that a 401 from the server
// causes runLogin to return a non-nil error.
func TestLogin_BadCredentialsExitsNonZero(t *testing.T) {
	setHomeDir(t)
	srv := fakeLoginServer(t, http.StatusUnauthorized, `{"error":"invalid credentials"}`)
	defer srv.Close()

	input := strings.NewReader("alice@example.com\nwrongpassword\n")
	args := loginArgs{
		Remote: srv.URL,
		Stdin:  input,
		Stdout: &strings.Builder{},
		Stderr: &strings.Builder{},
	}
	if err := runLogin(args); err == nil {
		t.Error("expected error for 401, got nil")
	}
}

// TestLogin_NetworkErrorExitsNonZero verifies that an unreachable server
// causes runLogin to return a non-nil error.
func TestLogin_NetworkErrorExitsNonZero(t *testing.T) {
	setHomeDir(t)
	// Use a server that is immediately closed so no connection is possible.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // close before the request

	input := strings.NewReader("alice@example.com\nhunter2\n")
	args := loginArgs{
		Remote: srv.URL,
		Stdin:  input,
		Stdout: &strings.Builder{},
		Stderr: &strings.Builder{},
	}
	if err := runLogin(args); err == nil {
		t.Error("expected error for network failure, got nil")
	}
}

// TestLogin_OverwritesExistingFiles verifies that a second login replaces
// stale token + cluster.json files left from a previous session.
func TestLogin_OverwritesExistingFiles(t *testing.T) {
	home := setHomeDir(t)
	dir := filepath.Join(home, ".datuplet")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	// Pre-seed stale files.
	if err := os.WriteFile(filepath.Join(dir, "token"), []byte("stale-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cluster.json"), []byte(`{"lakekeeper_url":"old"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	srv := fakeLoginServer(t, http.StatusOK, happyBody(t))
	defer srv.Close()

	input := strings.NewReader("alice@example.com\nhunter2\n")
	args := loginArgs{
		Remote: srv.URL,
		Stdin:  input,
		Stdout: &strings.Builder{},
		Stderr: &strings.Builder{},
	}
	if err := runLogin(args); err != nil {
		t.Fatalf("runLogin: %v", err)
	}

	tokenBytes, _ := os.ReadFile(filepath.Join(dir, "token"))
	if string(tokenBytes) != fakeJWT {
		t.Errorf("token not overwritten: got %q, want %q", string(tokenBytes), fakeJWT)
	}

	clusterBytes, _ := os.ReadFile(filepath.Join(dir, "cluster.json"))
	var meta clusterMeta
	if err := json.Unmarshal(clusterBytes, &meta); err != nil {
		t.Fatalf("unmarshal cluster.json after overwrite: %v", err)
	}
	if meta.LakekeeperURL != "http://lakekeeper:8181/catalog" {
		t.Errorf("cluster.json not overwritten: lakekeeper_url = %q", meta.LakekeeperURL)
	}
}

// checkMode asserts that the file at path has exactly the expected permission bits.
func checkMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	got := info.Mode().Perm()
	if got != want {
		t.Errorf("mode(%s) = %04o, want %04o", path, got, want)
	}
}
