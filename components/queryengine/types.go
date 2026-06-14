package queryengine

import "time"

type Request struct {
	SQL string
	// LakekeeperURL is the iceberg-REST catalog base URL. An empty string =
	// pure-compute mode: Run SKIPS attachCatalog (no catalog is wired) but the
	// lockdown posture still applies. Only unit tests and ad-hoc compute-only
	// callers run pure-compute; production frontends MUST validate this is
	// non-empty at their call site (the Phase 2 query-worker does).
	LakekeeperURL string
	// Warehouse is the ATTACH warehouse argument, passed verbatim to DuckDB.
	// It MUST be project-qualified as "<lakekeeper-project-id>/<warehouse>"
	// whenever a project id is known: the iceberg extension's /v1/config
	// handshake sends no x-project-id header, so a bare name resolves against
	// lakekeeper's nil-UUID default project → 401/403 (RFC 022 Spike 0.1 §1).
	// There is no separate ProjectID field by design — the caller assembles
	// the qualified string and attachCatalog treats it as an opaque value
	// (the repo's opaque-storage-string principle).
	Warehouse   string
	CatalogJWT  string
	Timeout     time.Duration
	MaxRows     int
	MaxBytes    int
	MemoryLimit string
	TempDir     string
	MaxTempSize string
	Threads     int
}

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Stats struct {
	DurationMs   int64 `json:"duration_ms"`
	RowsScanned  int64 `json:"rows_scanned,omitempty"`  // best-effort (RFC §3)
	BytesScanned int64 `json:"bytes_scanned,omitempty"` // best-effort
}

type Result struct {
	Schema    []Column `json:"schema"`
	Rows      [][]any  `json:"rows"`
	Truncated bool     `json:"truncated"`
	Stats     Stats    `json:"stats"`
}
