package queryengine

import "time"

type Request struct {
	SQL           string
	LakekeeperURL string
	Warehouse     string
	CatalogJWT    string
	Timeout       time.Duration
	MaxRows       int
	MaxBytes      int
	MemoryLimit   string
	TempDir       string
	MaxTempSize   string
	Threads       int
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
