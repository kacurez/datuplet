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

// TestLoadRemoteArgs_UnusableCredentialsErrors covers the credential-state
// failures that must send the user to `datuplet login --remote`: an expired
// token (and, as a security invariant, the raw JWT is never leaked into the
// error), a malformed expires_at (fail-closed rather than silently skipping
// the expiry check), and a missing token file / cluster.json.
func TestLoadRemoteArgs_UnusableCredentialsErrors(t *testing.T) {
	past := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
	// writeAll writes token + a cluster.json with the given expires_at.
	writeAll := func(expiresAt string) func(t *testing.T, home string) {
		return func(t *testing.T, home string) {
			writeDatupletFiles(t, home, fakeJWT, clusterMeta{
				LakekeeperURL:  "http://lk:8181/catalog",
				WarehouseName:  "datuplet",
				ExpiresAt:      expiresAt,
				PipelineAPIURL: "http://api",
				Projects:       []clusterMetaProject{{ID: "p-1", Name: "default", LakekeeperProjectID: "lk-proj-1"}},
			})
		}
	}
	tests := []struct {
		name  string
		setup func(t *testing.T, home string)
		want  []string
	}{
		{"expired token", writeAll(past), []string{"datuplet login --remote", "expired"}},
		{"expired token does not leak the JWT", writeAll(past), nil},
		{"malformed expires_at", writeAll("not-a-date"), []string{"corrupt", "datuplet login --remote"}},
		{"missing token file", func(t *testing.T, home string) {
			// cluster.json exists but token file is absent.
			dotDir := filepath.Join(home, ".datuplet")
			if err := os.MkdirAll(dotDir, 0o700); err != nil {
				t.Fatal(err)
			}
			b, _ := json.MarshalIndent(clusterMeta{LakekeeperURL: "http://lk:8181/catalog", ExpiresAt: "2099-01-01T00:00:00Z"}, "", "  ")
			if err := os.WriteFile(filepath.Join(dotDir, "cluster.json"), b, 0o600); err != nil {
				t.Fatal(err)
			}
		}, []string{"datuplet login --remote"}},
		{"missing cluster.json", func(t *testing.T, home string) {
			// Token file exists but cluster.json is absent.
			dotDir := filepath.Join(home, ".datuplet")
			if err := os.MkdirAll(dotDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dotDir, "token"), []byte(fakeJWT), 0o600); err != nil {
				t.Fatal(err)
			}
		}, []string{"datuplet login --remote"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			t.Setenv("HOME", tmp)
			tc.setup(t, tmp)

			_, err := loadRemoteArgs("http://api", "", "")
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error does not mention %q: %v", want, err)
				}
			}
			if strings.Contains(err.Error(), fakeJWT) {
				t.Errorf("error message leaks JWT token (security violation): %v", err)
			}
		})
	}
}

// TestLoadRemoteArgs_RemoteUrlMatching verifies that a --remote URL that
// differs from the one stored in cluster.json returns a descriptive error
// asking the user to re-login, while trailing slashes and scheme/host case
// differences are ignored (e.g. "HTTP://Localhost:30081/" matches
// "http://localhost:30081").
func TestLoadRemoteArgs_RemoteUrlMatching(t *testing.T) {
	tests := []struct {
		name       string
		clusterURL string
		remote     string
		wantErr    []string // nil means success
	}{
		{"mismatch errors", "http://cluster-a:30081", "http://cluster-b:30081", []string{"datuplet login --remote", "cluster-b"}},
		{"trailing slash and case are normalized", "http://localhost:30081", "HTTP://Localhost:30081/", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			t.Setenv("HOME", tmp)
			writeDatupletFiles(t, tmp, fakeJWT, clusterMeta{
				LakekeeperURL:  "http://lk:8181/catalog",
				WarehouseName:  "datuplet",
				ExpiresAt:      "2099-01-01T00:00:00Z",
				PipelineAPIURL: tc.clusterURL,
				Projects:       []clusterMetaProject{{ID: "p-1", Name: "default", LakekeeperProjectID: "lk-proj-1"}},
			})

			args, err := loadRemoteArgs(tc.remote, "", "")
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("expected normalization to match, got error: %v", err)
				}
				if args.LakekeeperURL != "http://lk:8181/catalog" {
					t.Errorf("LakekeeperURL = %q", args.LakekeeperURL)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error for URL mismatch, got nil")
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error does not mention %q: %v", want, err)
				}
			}
		})
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

// TestLoadRemoteArgs_HeadlessFastPath verifies the core headless promise
// (RFC 027 §7): with both DATUPLET_API_TOKEN and DATUPLET_REMOTE set and no
// --token-file flag, loadRemoteArgs never touches ~/.datuplet at all — not
// even to check it exists. HOME points at a path that does not exist on disk
// (not merely an empty existing temp dir): any actual os.ReadFile/os.Stat
// against a path under a non-existent parent directory fails loudly, so a nil
// error plus the home dir still not existing is proof no read was attempted.
// On this path $DATUPLET_PROJECT supplies the project ID when --project is
// omitted, --project wins over it when both are set, and with neither the ID
// resolves to empty (callers that don't need project scoping are unaffected).
func TestLoadRemoteArgs_HeadlessFastPath(t *testing.T) {
	tests := []struct {
		name        string
		envProject  string
		projectFlag string
		wantID      string
	}{
		{"project flag only", "", "my-project", "my-project"},
		{"project from env", "env-project-uuid", "", "env-project-uuid"},
		{"project flag wins over env", "env-project-uuid", "flag-project-uuid", "flag-project-uuid"},
		{"no project source", "", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			home := filepath.Join(tmp, "does-not-exist")
			t.Setenv("HOME", home)
			t.Setenv("DATUPLET_API_TOKEN", "headless-api-token")
			t.Setenv("DATUPLET_REMOTE", "http://headless-api")
			if tc.envProject != "" {
				t.Setenv("DATUPLET_PROJECT", tc.envProject)
			}

			args, err := loadRemoteArgs("", "", tc.projectFlag)
			if err != nil {
				t.Fatalf("loadRemoteArgs: unexpected error in headless mode: %v", err)
			}
			if args.APIToken != "headless-api-token" {
				t.Errorf("APIToken = %q, want %q", args.APIToken, "headless-api-token")
			}
			if args.Remote != "http://headless-api" {
				t.Errorf("Remote = %q, want %q", args.Remote, "http://headless-api")
			}
			if args.ID != tc.wantID {
				t.Errorf("ID = %q, want %q", args.ID, tc.wantID)
			}

			// Confirm the home dir itself was never created (no read/write
			// attempt anywhere under it, including ~/.datuplet).
			if _, statErr := os.Stat(home); !os.IsNotExist(statErr) {
				t.Errorf("expected %s to not exist, stat err = %v", home, statErr)
			}
		})
	}
}

// TestLoadRemoteArgs_EnvAPITokenNoTokenFileClusterRemoteFallback is a
// regression test for the "partial headless" case: $DATUPLET_API_TOKEN is
// set (so the api-token axis is fully resolved from env), but neither
// --remote nor $DATUPLET_REMOTE is set, so the remote must fall back to
// cluster.json's tier-3 pipeline_api_url. loadRemoteArgs must NOT require
// ~/.datuplet/token to exist in this case — that file is never consulted
// for the api-token when envAPITokenVal is already non-empty and
// --token-file wasn't passed. cluster.json itself must still be read (it's
// needed for the remote fallback and expiry validation).
func TestLoadRemoteArgs_EnvAPITokenNoTokenFileClusterRemoteFallback(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("DATUPLET_API_TOKEN", "env-api-token")

	meta := clusterMeta{
		LakekeeperURL:  "http://lk:8181/catalog",
		WarehouseName:  "datuplet",
		ExpiresAt:      "2099-01-01T00:00:00Z",
		PipelineAPIURL: "http://cluster-api",
		Projects:       []clusterMetaProject{{ID: "p-1", Name: "default", LakekeeperProjectID: "lk-proj-1"}},
	}
	dotDir := filepath.Join(tmp, ".datuplet")
	if err := os.MkdirAll(dotDir, 0o700); err != nil {
		t.Fatal(err)
	}
	b, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dotDir, "cluster.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	// Deliberately do NOT write dotDir/"token" — the point of this test is
	// that its absence must not matter when the api-token comes from env.

	args, err := loadRemoteArgs("", "", "")
	if err != nil {
		t.Fatalf("loadRemoteArgs: unexpected error with missing ~/.datuplet/token: %v", err)
	}
	if args.APIToken != "env-api-token" {
		t.Errorf("APIToken = %q, want %q", args.APIToken, "env-api-token")
	}
	if args.Remote != "http://cluster-api" {
		t.Errorf("Remote = %q, want %q (from cluster.json)", args.Remote, "http://cluster-api")
	}
}

// TestLoadRemoteArgs_ProjectEnvDiskBackedMultiProject verifies that on the
// disk-backed (non-fast) path, $DATUPLET_PROJECT can disambiguate a
// multi-project account via resolveProject without needing --project.
func TestLoadRemoteArgs_ProjectEnvDiskBackedMultiProject(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("DATUPLET_PROJECT", "beta")

	meta := clusterMeta{
		LakekeeperURL:  "http://lk:8181/catalog",
		WarehouseName:  "datuplet",
		ExpiresAt:      "2099-01-01T00:00:00Z",
		PipelineAPIURL: "http://api",
		Projects: []clusterMetaProject{
			{ID: "p-alpha", Name: "alpha", LakekeeperProjectID: "lk-alpha"},
			{ID: "p-beta", Name: "beta", LakekeeperProjectID: "lk-beta"},
		},
	}
	writeDatupletFiles(t, tmp, fakeJWT, meta)

	// No --project flag, but $DATUPLET_PROJECT=beta must pick the beta project
	// (and not error with the multi-project ambiguity message).
	args, err := loadRemoteArgs("http://api", "", "")
	if err != nil {
		t.Fatalf("loadRemoteArgs: unexpected error: %v", err)
	}
	if args.ID != "p-beta" {
		t.Errorf("ID = %q, want %q (resolved via $DATUPLET_PROJECT)", args.ID, "p-beta")
	}
	if args.LakekeeperProjectID != "lk-beta" {
		t.Errorf("LakekeeperProjectID = %q, want %q", args.LakekeeperProjectID, "lk-beta")
	}
	if args.ProjectName != "beta" {
		t.Errorf("ProjectName = %q, want %q", args.ProjectName, "beta")
	}
}
