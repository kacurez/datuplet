package framework

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"
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
	cmd.Env = append(os.Environ(), "DUCKDB_AGENT_MODE=0") // 2.0 CLI agent mode would swap -json for Markdown; -json already prevents it, this makes it explicit.
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

// RequireDuckDBV2 fails the test unless the duckdb CLI is 2.x: reading v3
// tables and VARIANT needs DuckDB 2.0 (e2e.yml installs v2.0.0). It never
// skips, so a missing or old CLI cannot pass as green.
func RequireDuckDBV2(t *testing.T) {
	t.Helper()
	out, err := exec.Command("duckdb", "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("duckdb CLI unavailable; the v3/VARIANT assertions need DuckDB 2.x: %v\n%s", err, out)
	}
	if v := strings.TrimSpace(string(out)); !strings.HasPrefix(v, "v2.") {
		t.Fatalf("duckdb CLI is %q; the v3/VARIANT assertions need DuckDB 2.x (e2e.yml installs v2.0.0)", v)
	}
}

// ParseJSONExact decodes JSON text keeping every number exact (json.Number),
// so integers above 2^53 compare by their digits.
func ParseJSONExact(text []byte) (any, error) {
	dec := json.NewDecoder(strings.NewReader(string(text)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, fmt.Errorf("trailing data after JSON value")
	}
	return v, nil
}

// AssertVariantJSON checks that variant column col of bucket.table, read by
// the duckdb CLI as col::JSON::VARCHAR, equals want row for row (keyed by the
// table's id column), compared as parsed JSON with exact numbers.
func AssertVariantJSON(t *testing.T, target QueryTarget, bucket, table, col string, want map[int64]any) {
	t.Helper()
	rows, err := duckdbQuery(target, bucket, table,
		fmt.Sprintf("SELECT id, %s::JSON::VARCHAR AS v FROM {{TABLE}} ORDER BY id", col))
	if err != nil {
		t.Fatalf("variant query failed for %s/%s.%s: %v", bucket, table, col, err)
	}
	got := make(map[int64]any, len(rows))
	for _, row := range rows {
		id, err := toInt(row["id"])
		if err != nil {
			t.Fatalf("%s/%s: id %v: %v", bucket, table, row["id"], err)
		}
		text, ok := row["v"].(string)
		if !ok {
			t.Fatalf("%s/%s.%s id %d: got %T %v, want JSON text", bucket, table, col, id, row["v"], row["v"])
		}
		v, err := ParseJSONExact([]byte(text))
		if err != nil {
			t.Fatalf("%s/%s.%s id %d: %q is not JSON: %v", bucket, table, col, id, text, err)
		}
		got[int64(id)] = v
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s/%s.%s as JSON:\n got  %v\n want %v", bucket, table, col, got, want)
	}
}

// AssertIcebergTable checks bucket.table's format version and partition
// spec, given as "transform(source)" entries (empty for unpartitioned).
func AssertIcebergTable(t *testing.T, target QueryTarget, bucket, table string, wantVersion int, wantPartition []string) {
	t.Helper()
	if target.Resolver == nil {
		t.Fatalf("AssertIcebergTable %s/%s: query target has no lakekeeper resolver", bucket, table)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tbl, err := target.Resolver.LoadTable(ctx, bucket, table)
	if err != nil {
		t.Fatalf("load %s/%s: %v", bucket, table, err)
	}
	if v := tbl.Metadata().Version(); v != wantVersion {
		t.Errorf("%s/%s: format version %d, want %d", bucket, table, v, wantVersion)
	}
	spec := tbl.Spec()
	got := []string{}
	for _, f := range spec.Fields() {
		src, _ := tbl.Schema().FindColumnName(f.SourceID())
		got = append(got, fmt.Sprintf("%s(%s)", f.Transform, src))
	}
	if wantPartition == nil {
		wantPartition = []string{}
	}
	if !reflect.DeepEqual(got, wantPartition) {
		t.Errorf("%s/%s: partition spec %v, want %v", bucket, table, got, wantPartition)
	}
}

// AssertIcebergColumnTypes checks that bucket.table's current schema has each
// named column with exactly the given Iceberg type (e.g. iceberg.VariantType{}),
// so a variant that came back as a string column fails.
func AssertIcebergColumnTypes(t *testing.T, target QueryTarget, bucket, table string, want map[string]iceberg.Type) {
	t.Helper()
	if target.Resolver == nil {
		t.Fatalf("AssertIcebergColumnTypes %s/%s: query target has no lakekeeper resolver", bucket, table)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tbl, err := target.Resolver.LoadTable(ctx, bucket, table)
	if err != nil {
		t.Fatalf("load %s/%s: %v", bucket, table, err)
	}
	for name, wantType := range want {
		f, ok := tbl.Schema().FindFieldByName(name)
		if !ok {
			t.Errorf("%s/%s: no column %q in schema %s", bucket, table, name, tbl.Schema())
			continue
		}
		if !f.Type.Equals(wantType) {
			t.Errorf("%s/%s: column %q is %s, want %s", bucket, table, name, f.Type, wantType)
		}
	}
}

// DataFileCount returns the number of data files in bucket.table's current
// snapshot, planned through the lakekeeper catalog (K8s tier only).
func DataFileCount(t *testing.T, target QueryTarget, bucket, table string) int {
	t.Helper()
	if target.Resolver == nil {
		t.Fatalf("DataFileCount %s/%s: query target has no lakekeeper resolver", bucket, table)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h := SharedHarness()
	if h == nil {
		t.Fatalf("DataFileCount %s/%s: no shared harness", bucket, table)
	}
	// Host IO: the Resolver's vended in-cluster storage endpoint is unreachable from the host.
	load, err := hostIOLoader(ctx, h, "datuplet-e2e-filecount", bucket, table)
	if err != nil {
		t.Fatalf("%v", err)
	}
	tbl, err := load()
	if err != nil {
		t.Fatalf("load %s/%s: %v", bucket, table, err)
	}
	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		t.Fatalf("plan files %s/%s: %v", bucket, table, err)
	}
	return len(tasks)
}

// AssertDataFileCount fails when bucket.table's current snapshot holds more
// than maxFiles data files.
func AssertDataFileCount(t *testing.T, target QueryTarget, bucket, table string, maxFiles int) {
	t.Helper()
	if n := DataFileCount(t, target, bucket, table); n > maxFiles {
		t.Errorf("%s/%s: %d data files, want <= %d", bucket, table, n, maxFiles)
	}
}

// AssertLatestSnapshotReplaceWithCommitKey checks that bucket.table's current
// snapshot is a `replace` operation carrying a non-empty datuplet.commit-key.
func AssertLatestSnapshotReplaceWithCommitKey(t *testing.T, target QueryTarget, bucket, table string) {
	t.Helper()
	if target.Resolver == nil {
		t.Fatalf("AssertLatestSnapshotReplaceWithCommitKey %s/%s: query target has no lakekeeper resolver", bucket, table)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tbl, err := target.Resolver.LoadTable(ctx, bucket, table)
	if err != nil {
		t.Fatalf("load %s/%s: %v", bucket, table, err)
	}
	snap := tbl.CurrentSnapshot()
	if snap == nil || snap.Summary == nil {
		t.Fatalf("%s/%s: no current snapshot summary", bucket, table)
	}
	if snap.Summary.Operation != icebergtable.OpReplace {
		t.Errorf("%s/%s: latest snapshot operation %q, want %q", bucket, table, snap.Summary.Operation, icebergtable.OpReplace)
	}
	if snap.Summary.Properties["datuplet.commit-key"] == "" {
		t.Errorf("%s/%s: latest snapshot lacks datuplet.commit-key: %v", bucket, table, snap.Summary.Properties)
	}
}
