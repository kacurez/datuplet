package framework

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// BackdateSnapshots rewrites the latest Iceberg metadata JSON for the given
// table, subtracting `age` from every snapshot's timestamp-ms (and the
// corresponding timestamp-ms in snapshot-log).
//
// Only timestamps are changed — snapshot IDs, parent-snapshot-ids, and
// current-snapshot-id are left intact so they remain consistent with the
// binary manifest/manifest-list Avro files on disk.
//
// Filesystem layout (LocalFile / SQLite catalog):
//
//	{warehouseDir}/{bucket}.db/{table}/metadata/vN.metadata.json
func BackdateSnapshots(warehouseDir, bucket, table string, age time.Duration) error {
	metadataDir := filepath.Join(warehouseDir, bucket+".db", table, "metadata")

	// Find the latest vN.metadata.json
	entries, err := os.ReadDir(metadataDir)
	if err != nil {
		return fmt.Errorf("read metadata dir %s: %w", metadataDir, err)
	}

	var metadataFiles []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".metadata.json") {
			metadataFiles = append(metadataFiles, e.Name())
		}
	}
	if len(metadataFiles) == 0 {
		return fmt.Errorf("no metadata files found in %s", metadataDir)
	}
	sort.Strings(metadataFiles)
	latest := metadataFiles[len(metadataFiles)-1]
	metadataPath := filepath.Join(metadataDir, latest)

	// Read and parse as generic JSON (preserves all fields)
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", metadataPath, err)
	}

	var meta map[string]any
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("parse %s: %w", metadataPath, err)
	}

	ageMs := age.Milliseconds()

	// Backdate snapshots[].timestamp-ms only (leave snapshot-id untouched)
	if snapshots, ok := meta["snapshots"].([]any); ok {
		for _, s := range snapshots {
			snap, ok := s.(map[string]any)
			if !ok {
				continue
			}
			if ts, ok := snap["timestamp-ms"].(float64); ok {
				snap["timestamp-ms"] = int64(ts) - ageMs
			}
		}
	}

	// Backdate snapshot-log[].timestamp-ms only
	if log, ok := meta["snapshot-log"].([]any); ok {
		for _, entry := range log {
			e, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			if ts, ok := e["timestamp-ms"].(float64); ok {
				e["timestamp-ms"] = int64(ts) - ageMs
			}
		}
	}

	// Write back
	out, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}
	if err := os.WriteFile(metadataPath, out, 0644); err != nil {
		return fmt.Errorf("write %s: %w", metadataPath, err)
	}

	return nil
}
