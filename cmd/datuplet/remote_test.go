package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeDatupletFiles is a test helper that writes ~/.datuplet/token and
// ~/.datuplet/cluster.json into dir (which should be the fake HOME).
func writeDatupletFiles(t *testing.T, dir, token string, meta clusterMeta) {
	t.Helper()
	dotDir := filepath.Join(dir, ".datuplet")
	if err := os.MkdirAll(dotDir, 0o700); err != nil {
		t.Fatalf("mkdir .datuplet: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dotDir, "token"), []byte(token), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	b, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		t.Fatalf("marshal cluster: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dotDir, "cluster.json"), b, 0o600); err != nil {
		t.Fatalf("write cluster.json: %v", err)
	}
}

// TestLoadRemoteArgs_LoadsTokenAndCluster verifies the happy path: both files
// present, non-expired, correctly parsed.
func TestLoadRemoteArgs_LoadsTokenAndCluster(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	meta := clusterMeta{
		LakekeeperURL:  "http://lk:8181/catalog",
		WarehouseName:  "datuplet",
		ExpiresAt:      "2099-01-01T00:00:00Z",
		UserID:         "u-1",
		PipelineAPIURL: "http://api",
		Projects: []clusterMetaProject{{ID: "p-1", Name: "default", LakekeeperProjectID: "lk-proj-1"}},
	}
	writeDatupletFiles(t, tmp, fakeJWT, meta)

	args, err := loadRemoteArgs("http://api", "", "")
	if err != nil {
		t.Fatalf("loadRemoteArgs: %v", err)
	}
	if args.Token != fakeJWT {
		t.Errorf("Token = %q, want %q", args.Token, fakeJWT)
	}
	if args.LakekeeperURL != "http://lk:8181/catalog" {
		t.Errorf("LakekeeperURL = %q", args.LakekeeperURL)
	}
	if args.WarehouseName != "datuplet" {
		t.Errorf("WarehouseName = %q", args.WarehouseName)
	}
	// TokenPath must be absolute (Docker -v requirement).
	if !filepath.IsAbs(args.TokenPath) {
		t.Errorf("TokenPath = %q, want absolute path", args.TokenPath)
	}
}

// TestLoadRemoteArgs_ExpiredToken checks that a past expires_at returns an
// error that mentions `datuplet login --remote`.
func TestLoadRemoteArgs_ExpiredToken(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	past := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
	meta := clusterMeta{
		LakekeeperURL: "http://lk:8181/catalog",
		WarehouseName: "datuplet",
		ExpiresAt:     past,
	}
	writeDatupletFiles(t, tmp, fakeJWT, meta)

	_, err := loadRemoteArgs("http://api", "", "")
	if err == nil {
		t.Fatal("expected error for expired token, got nil")
	}
	if !strings.Contains(err.Error(), "datuplet login --remote") {
		t.Errorf("error does not mention 'datuplet login --remote': %v", err)
	}
	if !strings.Contains(err.Error(), "expired") {
		t.Errorf("error does not mention 'expired': %v", err)
	}
}

// TestLoadRemoteArgs_TokenFileFlag verifies that --token-file overrides the
// default ~/.datuplet/token path.
func TestLoadRemoteArgs_TokenFileFlag(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	// Write cluster.json (always read from ~/.datuplet).
	meta := clusterMeta{
		LakekeeperURL:  "http://lk:8181/catalog",
		WarehouseName:  "datuplet",
		ExpiresAt:      "2099-01-01T00:00:00Z",
		PipelineAPIURL: "http://api",
		Projects: []clusterMetaProject{{ID: "p-1", Name: "default", LakekeeperProjectID: "lk-proj-1"}},
	}
	dotDir := filepath.Join(tmp, ".datuplet")
	if err := os.MkdirAll(dotDir, 0o700); err != nil {
		t.Fatal(err)
	}
	b, _ := json.MarshalIndent(meta, "", "  ")
	if err := os.WriteFile(filepath.Join(dotDir, "cluster.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}

	// Write a custom token file in a different location.
	customToken := "custom.jwt.token"
	customPath := filepath.Join(tmp, "my-token")
	if err := os.WriteFile(customPath, []byte(customToken), 0o600); err != nil {
		t.Fatal(err)
	}

	args, err := loadRemoteArgs("http://api", customPath, "")
	if err != nil {
		t.Fatalf("loadRemoteArgs with --token-file: %v", err)
	}
	if args.Token != customToken {
		t.Errorf("Token = %q, want %q (from custom path)", args.Token, customToken)
	}
	// TokenPath must resolve to the absolute custom path.
	absCustom, _ := filepath.Abs(customPath)
	if args.TokenPath != absCustom {
		t.Errorf("TokenPath = %q, want %q", args.TokenPath, absCustom)
	}
}

// TestLoadRemoteArgs_MissingTokenFile verifies a friendly error when the
// default token file does not exist.
func TestLoadRemoteArgs_MissingTokenFile(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	// cluster.json exists but token file is absent.
	meta := clusterMeta{LakekeeperURL: "http://lk:8181/catalog", ExpiresAt: "2099-01-01T00:00:00Z"}
	dotDir := filepath.Join(tmp, ".datuplet")
	os.MkdirAll(dotDir, 0o700)
	b, _ := json.MarshalIndent(meta, "", "  ")
	os.WriteFile(filepath.Join(dotDir, "cluster.json"), b, 0o600)

	_, err := loadRemoteArgs("http://api", "", "")
	if err == nil {
		t.Fatal("expected error for missing token file, got nil")
	}
	if !strings.Contains(err.Error(), "datuplet login --remote") {
		t.Errorf("error does not mention 'datuplet login --remote': %v", err)
	}
}

// TestLoadRemoteArgs_MissingClusterJSON verifies a friendly error when
// ~/.datuplet/cluster.json does not exist.
func TestLoadRemoteArgs_MissingClusterJSON(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	// Token file exists but cluster.json is absent.
	dotDir := filepath.Join(tmp, ".datuplet")
	os.MkdirAll(dotDir, 0o700)
	os.WriteFile(filepath.Join(dotDir, "token"), []byte(fakeJWT), 0o600)

	_, err := loadRemoteArgs("http://api", "", "")
	if err == nil {
		t.Fatal("expected error for missing cluster.json, got nil")
	}
	if !strings.Contains(err.Error(), "datuplet login --remote") {
		t.Errorf("error does not mention 'datuplet login --remote': %v", err)
	}
}

// TestRunRemote_GeneratesUniqueRunID verifies that two consecutive
// loadRemoteArgs calls (which underpins the run-id generation path)
// would produce different UUIDs. We test the uuid generation directly
// since runRemote itself launches Docker containers — full orchestration
// coverage lives in slice B.6's Helm-install e2e.
func TestRunRemote_GeneratesUniqueRunID(t *testing.T) {
	// We test uuid.New() indirectly: call the helper twice and confirm
	// uniqueness. This pins the guarantee that run-ids are never reused.
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	meta := clusterMeta{
		LakekeeperURL:  "http://lk:8181/catalog",
		WarehouseName:  "datuplet",
		ExpiresAt:      "2099-01-01T00:00:00Z",
		PipelineAPIURL: "http://api",
		Projects: []clusterMetaProject{{ID: "p-1", Name: "default", LakekeeperProjectID: "lk-proj-1"}},
	}
	writeDatupletFiles(t, tmp, fakeJWT, meta)

	args1, err := loadRemoteArgs("http://api", "", "")
	if err != nil {
		t.Fatalf("first loadRemoteArgs: %v", err)
	}
	args2, err := loadRemoteArgs("http://api", "", "")
	if err != nil {
		t.Fatalf("second loadRemoteArgs: %v", err)
	}

	// loadRemoteArgs itself doesn't generate run-ids (runRemote does),
	// but we can confirm both calls return valid args, then separately
	// confirm the UUID generation is unique.
	_ = args1
	_ = args2

	// Direct test of the UUID generation: two calls must differ.
	id1 := generateRunID()
	id2 := generateRunID()
	if id1 == id2 {
		t.Errorf("expected unique run-ids, got identical: %q", id1)
	}
	if id1 == "" || id2 == "" {
		t.Errorf("run-id must be non-empty")
	}
}

// TestLoadRemoteArgs_TokenNotInError ensures the raw JWT is never leaked
// into error messages (security invariant).
func TestLoadRemoteArgs_TokenNotInError(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	// Write a valid token but an expired cluster.json so we get an error
	// after the token is loaded.
	past := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
	meta := clusterMeta{
		LakekeeperURL: "http://lk:8181/catalog",
		ExpiresAt:     past,
	}
	writeDatupletFiles(t, tmp, fakeJWT, meta)

	_, err := loadRemoteArgs("http://api", "", "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// The error must not contain the JWT.
	if strings.Contains(err.Error(), fakeJWT) {
		t.Errorf("error message leaks JWT token (security violation): %v", err)
	}
}

// TestLoadRemoteArgs_RemoteUrlMismatchErrors verifies that passing a --remote
// URL that differs from the one stored in cluster.json returns a descriptive
// error asking the user to re-login.
func TestLoadRemoteArgs_RemoteUrlMismatchErrors(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	meta := clusterMeta{
		LakekeeperURL:  "http://lk:8181/catalog",
		WarehouseName:  "datuplet",
		ExpiresAt:      "2099-01-01T00:00:00Z",
		PipelineAPIURL: "http://cluster-a:30081",
	}
	writeDatupletFiles(t, tmp, fakeJWT, meta)

	// Pass cluster-B — should error.
	_, err := loadRemoteArgs("http://cluster-b:30081", "", "")
	if err == nil {
		t.Fatal("expected error for URL mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "datuplet login --remote") {
		t.Errorf("error does not mention 'datuplet login --remote': %v", err)
	}
	if !strings.Contains(err.Error(), "cluster-b") {
		t.Errorf("error does not mention the requested URL: %v", err)
	}
}

// TestLoadRemoteArgs_RemoteUrlNormalization verifies that trailing slashes and
// scheme/host case differences are ignored when comparing --remote to the
// logged-in URL (e.g. "http://Localhost:30081/" matches "http://localhost:30081").
func TestLoadRemoteArgs_RemoteUrlNormalization(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	meta := clusterMeta{
		LakekeeperURL:  "http://lk:8181/catalog",
		WarehouseName:  "datuplet",
		ExpiresAt:      "2099-01-01T00:00:00Z",
		PipelineAPIURL: "http://localhost:30081",
		Projects: []clusterMetaProject{{ID: "p-1", Name: "default", LakekeeperProjectID: "lk-proj-1"}},
	}
	writeDatupletFiles(t, tmp, fakeJWT, meta)

	// Trailing slash + uppercase scheme/host — should succeed.
	args, err := loadRemoteArgs("HTTP://Localhost:30081/", "", "")
	if err != nil {
		t.Fatalf("expected normalization to match, got error: %v", err)
	}
	if args.LakekeeperURL != "http://lk:8181/catalog" {
		t.Errorf("LakekeeperURL = %q", args.LakekeeperURL)
	}
}

// TestLoadRemoteArgs_MalformedExpiresAt verifies that a cluster.json with a
// non-RFC3339 expires_at returns a credentials error instead of silently
// skipping the expiry check (fail-closed requirement).
func TestLoadRemoteArgs_MalformedExpiresAt(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	meta := clusterMeta{
		LakekeeperURL:  "http://lk:8181/catalog",
		WarehouseName:  "datuplet",
		ExpiresAt:      "not-a-date",
		PipelineAPIURL: "http://api",
		Projects: []clusterMetaProject{{ID: "p-1", Name: "default", LakekeeperProjectID: "lk-proj-1"}},
	}
	writeDatupletFiles(t, tmp, fakeJWT, meta)

	_, err := loadRemoteArgs("http://api", "", "")
	if err == nil {
		t.Fatal("expected error for malformed expires_at, got nil")
	}
	if !strings.Contains(err.Error(), "corrupt") {
		t.Errorf("error does not mention 'corrupt': %v", err)
	}
	if !strings.Contains(err.Error(), "datuplet login --remote") {
		t.Errorf("error does not mention 'datuplet login --remote': %v", err)
	}
}
