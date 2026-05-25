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
	// Memory caps DuckDB's in-process buffer-managed memory (default:
	// "1GB"). Accepts DuckDB memory-limit strings (e.g. "512MB",
	// "1.5GB"). DuckDB spills to `temp_directory` when an operator
	// exceeds this. Note: this is NOT a hard RSS cap — non-buffer-managed
	// allocations (parquet decode buffers, page cache, go-duckdb FFI
	// state per marcboeker/go-duckdb#255) bypass it. The default is
	// deliberately conservative for shared single-node clusters where
	// the pod competes with infra workloads for node-level memory and
	// even a brief overshoot can trigger kubelet eviction. Bump
	// per-pipeline when a workload genuinely needs more in-memory
	// hash-table room than spill can absorb (e.g. JOIN of two large
	// tables).
	Memory string `json:"memory,omitempty"`
	// MaxTempSize is a HARD internal cap on DuckDB's spill disk usage
	// (default: "6GB"). When reached, DuckDB errors the query rather
	// than continuing to write. This is the real guardrail against
	// node-killing spill — kubelet's ephemeral-storage limit is *soft*
	// (sampled ~every 10s, can be outrun by a fast spiller), while
	// DuckDB's internal cap is synchronous. Set this below the pod's
	// `ephemeral-storage` limit so DuckDB fails the query before the
	// kubelet evicts the pod, and well below the node's free boot disk
	// so a runaway spill can't crater the node.
	MaxTempSize string `json:"max_temp_size,omitempty"`
	// Threads controls DuckDB's parallelism (default: 2). Each thread
	// keeps its own per-pipeline hash-aggregate state + decode buffers,
	// so threads multiply both in-memory state and concurrent spill
	// writer pressure. 2 is a balance: enough parallelism to use the
	// pod's typical 1-2 vCPU allotment for the SQL phase, low enough
	// not to multiply per-thread arena cost dramatically. Bump to 4+
	// when the host has dedicated CPU + memory headroom and wallclock
	// matters more than peak memory.
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
	if c.Memory == "" {
		c.Memory = "1GB"
	}
	if c.MaxTempSize == "" {
		c.MaxTempSize = "6GB"
	}
	if c.Threads == 0 {
		c.Threads = 2
	}
	if c.TempDirectory == "" {
		c.TempDirectory = "/tmp/duckdb-spill"
	}
	return &c, nil
}
