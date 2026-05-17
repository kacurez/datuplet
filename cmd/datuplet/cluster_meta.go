package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// clusterMeta is the cluster-side metadata persisted to
// ~/.datuplet/cluster.json after a successful `datuplet login --remote`.
// It carries everything the `datuplet run --remote` subcommand (B.3) needs
// to talk to lakekeeper + pipeline-api without prompting again.
// NEVER include the JWT token here — it lives in ~/.datuplet/token (raw
// bearer string) so the gateway sidecar bind-mount contract is preserved.
type clusterMeta struct {
	LakekeeperURL  string `json:"lakekeeper_url"`
	WarehouseName  string `json:"warehouse_name"`
	ExpiresAt      string `json:"expires_at"`
	UserID         string `json:"user_id"`
	PipelineAPIURL string `json:"pipeline_api_url"`
	// Projects the user has been granted on. The first entry's
	// LakekeeperProjectID is forwarded as `x-project-id` on every
	// catalog/STS call by `datuplet run --remote`. When the user is on
	// multiple projects we honour the order as returned by pipeline-api
	// and use the first; multi-project selection via `--project <name>`
	// is a future-cli concern.
	Projects []clusterMetaProject `json:"projects"`
}

type clusterMetaProject struct {
	ID                  string `json:"id"`
	Name                string `json:"name"`
	LakekeeperProjectID string `json:"lakekeeper_project_id"`
}

// datupletDir returns the ~/.datuplet directory path, creating it with
// mode 0700 if it does not exist.
func datupletDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir: %w", err)
	}
	dir := filepath.Join(home, ".datuplet")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create ~/.datuplet: %w", err)
	}
	return dir, nil
}

// writeTokenFile writes the raw JWT (and nothing else) to
// ~/.datuplet/token with mode 0600.
// SECURITY INVARIANT: the file MUST contain only the raw JWT string — no
// JSON wrapper, no metadata. The gateway sidecar bind-mounts this path as
// /var/run/secrets/datuplet-runtoken/token and reads it as a bare bearer
// string. If this ever became JSON, K8s-mode and local-cli-mode tokens
// would parse inconsistently.
func writeTokenFile(dir, rawJWT string) error {
	path := filepath.Join(dir, "token")
	return os.WriteFile(path, []byte(rawJWT), 0o600)
}

// writeClusterFile marshals meta to JSON and writes it to
// ~/.datuplet/cluster.json with mode 0600.
func writeClusterFile(dir string, meta clusterMeta) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal cluster meta: %w", err)
	}
	path := filepath.Join(dir, "cluster.json")
	return os.WriteFile(path, data, 0o600)
}
