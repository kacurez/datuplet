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
		Projects:       []clusterMetaProject{{ID: "p-1", Name: "default", LakekeeperProjectID: "lk-proj-1"}},
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
		Projects:       []clusterMetaProject{{ID: "p-1", Name: "default", LakekeeperProjectID: "lk-proj-1"}},
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
		Projects:       []clusterMetaProject{{ID: "p-1", Name: "default", LakekeeperProjectID: "lk-proj-1"}},
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

// TestLoadRemoteArgs_APITokenPrecedence is a table-driven test of the
// api-token resolution precedence: --token-file > DATUPLET_API_TOKEN >
// ~/.datuplet/api-token. Remote resolution is pinned to the explicit
// "http://api" flag value in every case so this test isolates the
// api-token axis only.
func TestLoadRemoteArgs_APITokenPrecedence(t *testing.T) {
	cases := []struct {
		name          string
		tokenFileFlag bool   // write a --token-file and pass it
		envToken      string // DATUPLET_API_TOKEN value ("" = unset)
		defaultToken  bool   // write ~/.datuplet/api-token
		wantAPIToken  string
	}{
		{
			name:          "token-file wins over env and default file",
			tokenFileFlag: true,
			envToken:      "env-token",
			defaultToken:  true,
			wantAPIToken:  "flag-token",
		},
		{
			name:         "env wins over default file when no token-file",
			envToken:     "env-token",
			defaultToken: true,
			wantAPIToken: "env-token",
		},
		{
			name:         "default file used when no flag and no env",
			defaultToken: true,
			wantAPIToken: "default-token",
		},
		{
			name:         "empty when nothing set (soft-fail)",
			wantAPIToken: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			t.Setenv("HOME", tmp)
			if tc.envToken != "" {
				t.Setenv("DATUPLET_API_TOKEN", tc.envToken)
			}

			meta := clusterMeta{
				LakekeeperURL:  "http://lk:8181/catalog",
				ExpiresAt:      "2099-01-01T00:00:00Z",
				PipelineAPIURL: "http://api",
				Projects:       []clusterMetaProject{{ID: "p-1", Name: "default", LakekeeperProjectID: "lk-proj-1"}},
			}
			dotDir := filepath.Join(tmp, ".datuplet")
			if err := os.MkdirAll(dotDir, 0o700); err != nil {
				t.Fatal(err)
			}
			b, _ := json.MarshalIndent(meta, "", "  ")
			if err := os.WriteFile(filepath.Join(dotDir, "cluster.json"), b, 0o600); err != nil {
				t.Fatal(err)
			}
			// The lakekeeper token file must exist whenever we read it
			// (always, and additionally as the source for --token-file).
			if err := os.WriteFile(filepath.Join(dotDir, "token"), []byte(fakeJWT), 0o600); err != nil {
				t.Fatal(err)
			}
			if tc.defaultToken {
				if err := os.WriteFile(filepath.Join(dotDir, "api-token"), []byte("default-token"), 0o600); err != nil {
					t.Fatal(err)
				}
			}

			var tokenFileFlag string
			if tc.tokenFileFlag {
				customPath := filepath.Join(tmp, "custom-token")
				if err := os.WriteFile(customPath, []byte("flag-token"), 0o600); err != nil {
					t.Fatal(err)
				}
				tokenFileFlag = customPath
			}

			args, err := loadRemoteArgs("http://api", tokenFileFlag, "")
			if err != nil {
				t.Fatalf("loadRemoteArgs: %v", err)
			}
			if args.APIToken != tc.wantAPIToken {
				t.Errorf("APIToken = %q, want %q", args.APIToken, tc.wantAPIToken)
			}
		})
	}
}

// TestLoadRemoteArgs_RemotePrecedence is a table-driven test of the remote
// resolution precedence: --remote > DATUPLET_REMOTE > ~/.datuplet/cluster.json.
func TestLoadRemoteArgs_RemotePrecedence(t *testing.T) {
	cases := []struct {
		name       string
		remoteFlag string
		envRemote  string
		clusterURL string
		wantRemote string
	}{
		{
			name:       "flag wins over env and cluster.json",
			remoteFlag: "http://flag-api",
			envRemote:  "http://env-api",
			clusterURL: "http://flag-api",
			wantRemote: "http://flag-api",
		},
		{
			name:       "env wins over cluster.json when no flag",
			envRemote:  "http://env-api",
			clusterURL: "http://env-api",
			wantRemote: "http://env-api",
		},
		{
			name:       "cluster.json used when no flag and no env",
			clusterURL: "http://cluster-api",
			wantRemote: "http://cluster-api",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			t.Setenv("HOME", tmp)
			if tc.envRemote != "" {
				t.Setenv("DATUPLET_REMOTE", tc.envRemote)
			}

			meta := clusterMeta{
				LakekeeperURL:  "http://lk:8181/catalog",
				ExpiresAt:      "2099-01-01T00:00:00Z",
				PipelineAPIURL: tc.clusterURL,
				Projects:       []clusterMetaProject{{ID: "p-1", Name: "default", LakekeeperProjectID: "lk-proj-1"}},
			}
			writeDatupletFiles(t, tmp, fakeJWT, meta)

			args, err := loadRemoteArgs(tc.remoteFlag, "", "")
			if err != nil {
				t.Fatalf("loadRemoteArgs: %v", err)
			}
			if args.Remote != tc.wantRemote {
				t.Errorf("Remote = %q, want %q", args.Remote, tc.wantRemote)
			}
		})
	}
}

// TestLoadRemoteArgs_HeadlessNoDatupletFileRead verifies the core headless
// promise (RFC 027 §7): with both DATUPLET_API_TOKEN and DATUPLET_REMOTE set
// and no --token-file flag, loadRemoteArgs never touches ~/.datuplet at all
// — not even to check it exists. HOME points at an empty temp dir (no
// .datuplet subdirectory whatsoever); any attempt to read a file under it
// would surface as an error, so a nil error here is proof no such read was
// attempted.
func TestLoadRemoteArgs_HeadlessNoDatupletFileRead(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("DATUPLET_API_TOKEN", "headless-api-token")
	t.Setenv("DATUPLET_REMOTE", "http://headless-api")

	args, err := loadRemoteArgs("", "", "my-project")
	if err != nil {
		t.Fatalf("loadRemoteArgs: unexpected error in headless mode: %v", err)
	}
	if args.APIToken != "headless-api-token" {
		t.Errorf("APIToken = %q, want %q", args.APIToken, "headless-api-token")
	}
	if args.Remote != "http://headless-api" {
		t.Errorf("Remote = %q, want %q", args.Remote, "http://headless-api")
	}
	if args.ID != "my-project" {
		t.Errorf("ID = %q, want %q", args.ID, "my-project")
	}

	// Confirm ~/.datuplet was never even created (no read/write attempt).
	if _, statErr := os.Stat(filepath.Join(tmp, ".datuplet")); !os.IsNotExist(statErr) {
		t.Errorf("expected ~/.datuplet to not exist, stat err = %v", statErr)
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
		Projects:       []clusterMetaProject{{ID: "p-1", Name: "default", LakekeeperProjectID: "lk-proj-1"}},
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
