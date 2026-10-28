package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeDatupletDir lays out a fake ~/.datuplet with cluster.json + api-token
// and returns the dir. Files are written 0600 to match the real layout.
func writeDatupletDir(t *testing.T, clusterJSON, apiToken string) string {
	t.Helper()
	dir := t.TempDir()
	if clusterJSON != "" {
		if err := os.WriteFile(filepath.Join(dir, "cluster.json"), []byte(clusterJSON), 0o600); err != nil {
			t.Fatalf("write cluster.json: %v", err)
		}
	}
	if apiToken != "" {
		if err := os.WriteFile(filepath.Join(dir, "api-token"), []byte(apiToken), 0o600); err != nil {
			t.Fatalf("write api-token: %v", err)
		}
	}
	return dir
}

const goodCluster = `{
  "lakekeeper_url": "https://lk.example.com/catalog",
  "warehouse_name": "analytics",
  "user_id": "u-1",
  "pipeline_api_url": "https://api.example.com",
  "expires_at": "2099-01-01T00:00:00Z",
  "api_expires_at": "2099-01-01T00:00:00Z",
  "projects": [
    {"id": "proj-uuid", "name": "default", "lakekeeper_project_id": "lk-proj-uuid"}
  ]
}`

func TestLoadConfig_Success(t *testing.T) {
	dir := writeDatupletDir(t, goodCluster, "  api-jwt-token  \n")
	cfg, err := loadConfig(dir)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.LakekeeperURL != "https://lk.example.com/catalog" {
		t.Errorf("LakekeeperURL = %q", cfg.LakekeeperURL)
	}
	if cfg.PipelineAPIURL != "https://api.example.com" {
		t.Errorf("PipelineAPIURL = %q", cfg.PipelineAPIURL)
	}
	// api-token must be whitespace-trimmed (raw JWT only).
	if cfg.APIToken != "api-jwt-token" {
		t.Errorf("APIToken = %q, want trimmed", cfg.APIToken)
	}
	// Warehouse must be project-qualified "<lk-project-id>/<warehouse>".
	if cfg.QualifiedWarehouse() != "lk-proj-uuid/analytics" {
		t.Errorf("QualifiedWarehouse = %q, want lk-proj-uuid/analytics", cfg.QualifiedWarehouse())
	}
}

func TestLoadConfig_Errors(t *testing.T) {
	cases := []struct {
		name    string
		cluster string
		token   string
		// wantHint, when set, must appear in the error (the login hint).
		wantHint string
	}{
		{name: "missing cluster.json", cluster: "", token: "tok", wantHint: "datuplet login --remote"},
		{name: "missing api-token", cluster: goodCluster, token: "", wantHint: "datuplet login --remote"},
		// Empty projects: cannot qualify the warehouse.
		{name: "no projects", cluster: `{"lakekeeper_url":"u","warehouse_name":"w","pipeline_api_url":"a","projects":[]}`, token: "tok"},
		{name: "whitespace-only api-token", cluster: goodCluster, token: "   \n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := writeDatupletDir(t, tc.cluster, tc.token)
			_, err := loadConfig(dir)
			if err == nil {
				t.Fatal("expected error")
			}
			if tc.wantHint != "" && !strings.Contains(err.Error(), tc.wantHint) {
				t.Errorf("error should contain %q, got: %v", tc.wantHint, err)
			}
		})
	}
}
