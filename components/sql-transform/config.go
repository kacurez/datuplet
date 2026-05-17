//go:build duckdb_arrow

package main

import (
	"encoding/json"
	"fmt"
)

// ComponentConfig holds sql-transform component configuration, populated from
// the pipeline YAML's `config:` section (passed through the DataGateway SDK).
type ComponentConfig struct {
	// SQL is the user-provided SQL to execute. Required.
	// The SQL should reference input tables by their logical names (e.g. FROM orders)
	// and CREATE TABLE <output-logical-name> AS SELECT ... for outputs.
	// No bucket-templating — DG mediates reads/writes so the SQL stays
	// environment-agnostic.
	SQL string `json:"sql"`
	// Threads controls DuckDB's parallelism (default: 4).
	Threads int `json:"threads,omitempty"`
	// TempDirectory is the DuckDB spill-to-disk directory for large aggregates (default: /tmp/duckdb-spill).
	TempDirectory string `json:"temp_directory,omitempty"`
}

// parseConfig parses and validates the component config from the raw JSON map
// passed via sdk.Client.Config().Raw.
func parseConfig(raw json.RawMessage) (*ComponentConfig, error) {
	var c ComponentConfig
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if c.SQL == "" {
		return nil, fmt.Errorf("config.sql is required")
	}
	if c.Threads == 0 {
		c.Threads = 4
	}
	if c.TempDirectory == "" {
		c.TempDirectory = "/tmp/duckdb-spill"
	}
	return &c, nil
}
