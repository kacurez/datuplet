package framework

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// QueryTarget describes how to reach Iceberg tables for assertions.
// Either WarehouseDir (filesystem, deprecated tier) or S3 config must
// be set; K8s/DockerS3 tiers populate S3* fields. Resolver, when set,
// overrides legacy path construction with a lakekeeper-vended
// Table.Location() lookup — this is the path the K8s tier takes.
type QueryTarget struct {
	// Filesystem mode (deprecated — kept for the DockerS3 tier's local
	// warehouse fallback).
	WarehouseDir string

	// S3/MinIO mode
	S3Endpoint  string // e.g., "localhost:30900"
	S3AccessKey string
	S3SecretKey string
	S3Bucket    string // e.g., "datuplet"

	// Resolver, when non-nil, looks up the concrete table location
	// from lakekeeper (Table.Location()) instead of constructing the
	// legacy URL. Required by the K8s tier — lakekeeper allocates
	// UUID-keyed paths that don't carry project identity.
	Resolver *LakekeeperVerifier
}

// IsEmpty reports whether the target carries enough info to build a
// DuckDB query. The Filesystem tier sets WarehouseDir, the S3 tiers
// set S3Endpoint, and the K8s tier sets BOTH S3Endpoint (so DuckDB
// can read the parquet) AND Resolver (so we know where the parquet
// lives).
func (q QueryTarget) IsEmpty() bool {
	return q.WarehouseDir == "" && q.S3Endpoint == ""
}

// duckdbQuery runs a SQL query against an Iceberg table using the DuckDB CLI.
// The sql parameter may contain {{TABLE}} which is replaced with iceberg_scan('path').
func duckdbQuery(target QueryTarget, bucket, table, sql string) ([]map[string]any, error) {
	var preamble string
	var icebergScan string

	if target.Resolver != nil {
		// K8s tier: lakekeeper-vended Table.Location() lookup.
		// Lakekeeper allocates UUID-keyed paths
		// (s3://<warehouse>/<storage-uuid>/<table-uuid>/...) that
		// can't be reconstructed from (bucket, table) alone.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		loc, err := target.Resolver.LocationFor(ctx, bucket, table)
		if err != nil {
			return nil, fmt.Errorf("resolve table location for %s.%s: %w", bucket, table, err)
		}
		// DuckDB's iceberg_scan accepts the table base; allow_moved_paths
		// lets it follow the metadata-pointer if the table moved.
		icebergScan = fmt.Sprintf("iceberg_scan('%s', allow_moved_paths = true)", loc)
		preamble = fmt.Sprintf(
			"INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg; "+
				"SET s3_endpoint='%s'; "+
				"SET s3_access_key_id='%s'; "+
				"SET s3_secret_access_key='%s'; "+
				"SET s3_use_ssl=false; "+
				"SET s3_url_style='path'; "+
				"SET force_download=true; "+
				"SET unsafe_enable_version_guessing=true;",
			target.S3Endpoint, target.S3AccessKey, target.S3SecretKey,
		)
	} else if target.WarehouseDir != "" {
		// LocalFile mode: iceberg-go SQL+SQLite catalog allocates tables
		// as <warehouseRoot>/<namespace>.db/<table>/... where namespace
		// == bucket (the pipeline's defaultBucket / explicit bucket name).
		tablePath := filepath.Join(target.WarehouseDir, bucket+".db", table)
		icebergScan = fmt.Sprintf("iceberg_scan('%s', allow_moved_paths = true)", tablePath)
		preamble = "INSTALL iceberg; LOAD iceberg; SET unsafe_enable_version_guessing = true;"
	} else if target.S3Endpoint != "" {
		// DockerS3 mode (legacy URL shape — kept for the Docker/Compose smoke path).
		tablePath := fmt.Sprintf("s3://%s/orgs/myorg/projects/myproject/tables/%s/%s", target.S3Bucket, bucket, table)
		icebergScan = fmt.Sprintf("iceberg_scan('%s', allow_moved_paths = true)", tablePath)
		preamble = fmt.Sprintf(
			"INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg; "+
				"SET s3_endpoint='%s'; "+
				"SET s3_access_key_id='%s'; "+
				"SET s3_secret_access_key='%s'; "+
				"SET s3_use_ssl=false; "+
				"SET s3_url_style='path'; "+
				"SET force_download=true; "+
				"SET unsafe_enable_version_guessing=true;",
			target.S3Endpoint, target.S3AccessKey, target.S3SecretKey,
		)
	} else {
		return nil, fmt.Errorf("QueryTarget has no resolver, warehouse dir, or S3 config")
	}

	resolvedSQL := strings.ReplaceAll(sql, "{{TABLE}}", icebergScan)
	fullSQL := fmt.Sprintf("%s %s", preamble, resolvedSQL)

	cmd := exec.Command("duckdb", "-json", "-c", fullSQL)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("duckdb query failed\n  SQL: %s\n  output: %s\n  error: %w", fullSQL, string(out), err)
	}

	outStr := strings.TrimSpace(string(out))
	if outStr == "" {
		return nil, nil
	}

	var rows []map[string]any
	if err := json.Unmarshal([]byte(outStr), &rows); err != nil {
		return nil, fmt.Errorf("failed to parse duckdb JSON output\n  SQL: %s\n  output: %s\n  error: %w", fullSQL, outStr, err)
	}

	return rows, nil
}

// AssertBrowseTableSucceeds verifies that the Iceberg table is browsable via
// the lakekeeper verifier (K8s tier) or a direct S3 path (DockerS3 tier).
// Semantically equivalent to AssertTableExists but signals FGA-grant-matrix
// intent: a user with browse rights can successfully read this table. The
// FGA-matrix scenarios use this assertion type alongside "row_count" to
// confirm that alice/bob can both trigger and browse.
func AssertBrowseTableSucceeds(t *testing.T, target QueryTarget, bucket, table string) {
	t.Helper()
	AssertTableExists(t, target, bucket, table)
}

// AssertTableExists verifies that the Iceberg table is readable.
func AssertTableExists(t *testing.T, target QueryTarget, bucket, table string) {
	t.Helper()
	_, err := duckdbQuery(target, bucket, table, "SELECT 1 AS ok FROM {{TABLE}} LIMIT 1")
	if err != nil {
		t.Fatalf("table %s/%s does not exist or is not readable: %v", bucket, table, err)
	}
}

// AssertRowCount verifies the table has exactly the expected number of rows.
func AssertRowCount(t *testing.T, target QueryTarget, bucket, table string, expected int) {
	t.Helper()
	rows, err := duckdbQuery(target, bucket, table, "SELECT COUNT(*) AS cnt FROM {{TABLE}}")
	if err != nil {
		t.Fatalf("row count query failed for %s/%s: %v", bucket, table, err)
	}
	if len(rows) == 0 {
		t.Fatalf("row count query returned no rows for %s/%s", bucket, table)
	}
	cnt, err := toInt(rows[0]["cnt"])
	if err != nil {
		t.Fatalf("failed to parse row count for %s/%s: %v (raw: %v)", bucket, table, err, rows[0]["cnt"])
	}
	if cnt != expected {
		t.Fatalf("row count mismatch for %s/%s: got %d, want %d", bucket, table, cnt, expected)
	}
}

// AssertMinRowCount verifies the table has at least minCount rows.
func AssertMinRowCount(t *testing.T, target QueryTarget, bucket, table string, minCount int) {
	t.Helper()
	rows, err := duckdbQuery(target, bucket, table, "SELECT COUNT(*) AS cnt FROM {{TABLE}}")
	if err != nil {
		t.Fatalf("row count query failed for %s/%s: %v", bucket, table, err)
	}
	if len(rows) == 0 {
		t.Fatalf("row count query returned no rows for %s/%s", bucket, table)
	}
	cnt, err := toInt(rows[0]["cnt"])
	if err != nil {
		t.Fatalf("failed to parse row count for %s/%s: %v (raw: %v)", bucket, table, err, rows[0]["cnt"])
	}
	if cnt < minCount {
		t.Fatalf("row count too low for %s/%s: got %d, want at least %d", bucket, table, cnt, minCount)
	}
}

// AssertSchema verifies that the expected columns exist with the expected types.
// The expectedColumns map is col_name -> expected_type. An empty type string skips the type check.
func AssertSchema(t *testing.T, target QueryTarget, bucket, table string, expectedColumns map[string]string) {
	t.Helper()
	rows, err := duckdbQuery(target, bucket, table, "DESCRIBE SELECT * FROM {{TABLE}}")
	if err != nil {
		t.Fatalf("schema query failed for %s/%s: %v", bucket, table, err)
	}

	actual := make(map[string]string) // col_name -> col_type
	for _, row := range rows {
		name, _ := row["column_name"].(string)
		colType, _ := row["column_type"].(string)
		if name != "" {
			actual[name] = colType
		}
	}

	for col, wantType := range expectedColumns {
		gotType, ok := actual[col]
		if !ok {
			t.Errorf("column %q not found in %s/%s; existing columns: %v", col, bucket, table, keys(actual))
			continue
		}
		if wantType != "" && !strings.EqualFold(gotType, wantType) {
			t.Errorf("column %q type mismatch in %s/%s: got %q, want %q", col, bucket, table, gotType, wantType)
		}
	}
}

// AssertColumnAbsent verifies that the given column does NOT exist in the table.
func AssertColumnAbsent(t *testing.T, target QueryTarget, bucket, table, column string) {
	t.Helper()
	rows, err := duckdbQuery(target, bucket, table, "DESCRIBE SELECT * FROM {{TABLE}}")
	if err != nil {
		t.Fatalf("schema query failed for %s/%s: %v", bucket, table, err)
	}

	for _, row := range rows {
		name, _ := row["column_name"].(string)
		if strings.EqualFold(name, column) {
			t.Fatalf("column %q should not exist in %s/%s but it does", column, bucket, table)
		}
	}
}

// AssertQuery runs an arbitrary SQL query and compares results.
// Values are compared via fmt.Sprintf("%v") for simplicity.
func AssertQuery(t *testing.T, target QueryTarget, bucket, table, sql string, expected []map[string]any) {
	t.Helper()
	rows, err := duckdbQuery(target, bucket, table, sql)
	if err != nil {
		t.Fatalf("query failed for %s/%s: %v", bucket, table, err)
	}

	if len(rows) != len(expected) {
		t.Fatalf("query result row count mismatch for %s/%s: got %d, want %d\n  SQL: %s\n  got: %v",
			bucket, table, len(rows), len(expected), sql, rows)
	}

	for i, wantRow := range expected {
		gotRow := rows[i]
		for k, wantVal := range wantRow {
			gotVal, ok := gotRow[k]
			if !ok {
				t.Errorf("row %d: key %q missing in result", i, k)
				continue
			}
			if fmt.Sprintf("%v", gotVal) != fmt.Sprintf("%v", wantVal) {
				t.Errorf("row %d: key %q mismatch: got %v, want %v", i, k, gotVal, wantVal)
			}
		}
	}
}

// toInt converts a value (float64, json.Number, or string) to int.
func toInt(v any) (int, error) {
	switch val := v.(type) {
	case float64:
		return int(val), nil
	case json.Number:
		n, err := val.Int64()
		if err != nil {
			return 0, fmt.Errorf("json.Number to int: %w", err)
		}
		return int(n), nil
	case string:
		n, err := strconv.Atoi(val)
		if err != nil {
			return 0, fmt.Errorf("string %q to int: %w", val, err)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("unsupported type %T for int conversion", v)
	}
}

// keys returns the sorted keys of a map, useful for error messages.
func keys(m map[string]string) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
