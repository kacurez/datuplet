// Package main is the datuplet-query binary: BYO-local (mode c) ad-hoc SQL
// against the warehouse, running DuckDB on the user's own machine (RFC 022
// §4). It is a SEPARATE, duckdb-tagged binary from the root duckdb-free
// `datuplet` CLI; only main.go carries the //go:build duckdb_arrow tag, so the
// config/mint/format logic here stays testable without DuckDB.
//
// SECURITY: this binary handles credentials that reach the laptop — the
// pipeline-api api-token, the freshly-minted short-lived query JWT, and the
// raw user SQL. None of these are ever logged. The query JWT is held only in
// memory; temp files are 0600 and cleaned up.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
)

// config holds everything datuplet-query needs to mint a query token and run
// a query. It is assembled by loadConfig from ~/.datuplet/{cluster.json,
// api-token}. The catalog query JWT is NOT part of this struct — it is minted
// per invocation and held only transiently (RFC 022 §5.3 option i).
type config struct {
	// LakekeeperURL is the iceberg-REST catalog base URL.
	LakekeeperURL string
	// WarehouseName is the bare lakekeeper warehouse name. Use
	// QualifiedWarehouse() to assemble the project-qualified form the
	// iceberg /v1/config handshake requires.
	WarehouseName string
	// LakekeeperProjectID is projects[0].lakekeeper_project_id — used to
	// project-qualify the warehouse (the handshake sends no x-project-id
	// header, so a bare name resolves against the nil-UUID default → 401/403).
	LakekeeperProjectID string
	// PipelineAPIURL is the base URL of pipeline-api (mint endpoint host).
	PipelineAPIURL string
	// APIToken is the raw pipeline-api bearer JWT (aud=datuplet-api). NEVER
	// log this.
	APIToken string
}

// QualifiedWarehouse returns the project-qualified warehouse string
// "<lakekeeper_project_id>/<warehouse_name>" that queryengine.Request.Warehouse
// requires (RFC 022 Spike 0.1 §1).
func (c config) QualifiedWarehouse() string {
	return c.LakekeeperProjectID + "/" + c.WarehouseName
}

// clusterMeta mirrors the on-disk ~/.datuplet/cluster.json shape (written by
// `datuplet login --remote`, defined in cmd/datuplet/cluster_meta.go). This is
// a DIFFERENT Go module, so the shape is replicated here rather than imported.
// Only the fields datuplet-query needs are declared.
type clusterMeta struct {
	LakekeeperURL  string               `json:"lakekeeper_url"`
	WarehouseName  string               `json:"warehouse_name"`
	PipelineAPIURL string               `json:"pipeline_api_url"`
	Projects       []clusterMetaProject `json:"projects"`
}

type clusterMetaProject struct {
	ID                  string `json:"id"`
	Name                string `json:"name"`
	LakekeeperProjectID string `json:"lakekeeper_project_id"`
}

// loginHint is appended to every load failure so the user knows the recovery
// action (the credential files come from `datuplet login --remote`).
const loginHint = "\n(run `datuplet login --remote <pipeline-api-url>` first)"

// defaultDatupletDir returns ~/.datuplet. It does NOT create the directory:
// datuplet-query only reads existing login state.
func defaultDatupletDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir: %w", err)
	}
	return filepath.Join(home, ".datuplet"), nil
}

// loadConfig reads cluster.json + api-token from dir and assembles a config.
// It fails with a clear, login-pointing error on any missing/empty/corrupt
// input. The api-token file is trimmed (raw JWT only — no JSON wrapper).
func loadConfig(dir string) (*config, error) {
	clusterPath := filepath.Join(dir, "cluster.json")
	clusterBytes, err := os.ReadFile(clusterPath)
	if err != nil {
		return nil, fmt.Errorf("read cluster config %s: %w%s", clusterPath, err, loginHint)
	}
	var meta clusterMeta
	if err := json.Unmarshal(clusterBytes, &meta); err != nil {
		return nil, fmt.Errorf("parse cluster.json: %w%s", err, loginHint)
	}
	if meta.PipelineAPIURL == "" {
		return nil, fmt.Errorf("cluster.json missing pipeline_api_url%s", loginHint)
	}
	// Defense in depth: the api-token is sent as a bearer to this host, so
	// reject anything that isn't an http(s) URL (a corrupt cluster.json with a
	// file://-or-other scheme must not become the mint target).
	if u, perr := url.Parse(meta.PipelineAPIURL); perr != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return nil, fmt.Errorf("cluster.json pipeline_api_url must be an http(s) URL%s", loginHint)
	}
	if meta.LakekeeperURL == "" {
		return nil, fmt.Errorf("cluster.json missing lakekeeper_url%s", loginHint)
	}
	if meta.WarehouseName == "" {
		return nil, fmt.Errorf("cluster.json missing warehouse_name%s", loginHint)
	}
	if len(meta.Projects) == 0 || meta.Projects[0].LakekeeperProjectID == "" {
		return nil, fmt.Errorf("cluster.json has no project with a lakekeeper_project_id; cannot qualify the warehouse%s", loginHint)
	}

	apiTokenPath := filepath.Join(dir, "api-token")
	apiTokBytes, err := os.ReadFile(apiTokenPath)
	if err != nil {
		return nil, fmt.Errorf("read api-token %s: %w%s", apiTokenPath, err, loginHint)
	}
	apiToken := string(bytes.TrimSpace(apiTokBytes))
	if apiToken == "" {
		return nil, fmt.Errorf("api-token file %s is empty%s", apiTokenPath, loginHint)
	}

	return &config{
		LakekeeperURL:       meta.LakekeeperURL,
		WarehouseName:       meta.WarehouseName,
		LakekeeperProjectID: meta.Projects[0].LakekeeperProjectID,
		PipelineAPIURL:      meta.PipelineAPIURL,
		APIToken:            apiToken,
	}, nil
}
