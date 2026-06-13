//go:build duckdb_arrow && integration

// Integration test for the datuplet-query binary (RFC 022 Task 3.1).
//
// Run:
//
//	go test -tags 'duckdb_arrow integration' -run TestDatupletQueryIntegration ./cmd/datuplet-query/...
//
// It mirrors components/queryengine/integration_test.go's host-network docker
// fixture (postgres + MinIO + lakekeeper, anonymous/allowall) and seeds a
// table via the repo's own iceberg-go write stack (pkg/catalogwriter). On top
// of the fixture it:
//   - stubs the pipeline-api /api/v1/query/token mint endpoint with httptest,
//   - writes a throwaway ~/.datuplet (cluster.json + api-token) pointing at the
//     stub + the fixture lakekeeper,
//   - drives realMain end-to-end for --sql, -f, and stdin, and the --format
//     switch + --memory-limit.
//
// Skips gracefully when docker is unavailable, matching the queryengine
// integration test's skip guard.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberg "github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	_ "github.com/datuplet/datuplet/pkg/datupleticeio"
)

const (
	minioRootUser     = "dq-it-root"
	minioRootPassword = "dq-it-secret-pw"
	minioBucket       = "dq-it-bucket"
	pgPassword        = "dq-it-pg-pw"
	lkEncryptionKey   = "dq-it-throwaway-encryption-key"
	seedNamespace     = "qe"
	seedTable         = "posts"
	seedRowCount      = 7
	// dummyCatalogJWT is a structurally valid (unsigned) JWT shape; lakekeeper
	// authn is disabled in the fixture so it is never validated. The mint stub
	// returns this so realMain's queryengine.Run attaches with a well-formed
	// token (attachCatalog's shape guard rejects malformed ones).
	dummyCatalogJWT = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJxZSJ9.c2ln"
)

func TestDatupletQueryIntegration(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skipf("docker not available (docker info failed: %v); skipping integration test", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	fx := provision(ctx, t)
	defer fx.teardown()
	seedPostsTable(ctx, t, fx)

	// Stub the pipeline-api mint endpoint: 200 → dummy query JWT.
	mint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query/token" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, fmt.Sprintf(`{"token":%q,"expires_at":"2099-01-01T00:05:00Z"}`, dummyCatalogJWT))
	}))
	defer mint.Close()

	// Write a throwaway ~/.datuplet equivalent and point HOME at it so
	// defaultDatupletDir resolves there.
	home := t.TempDir()
	t.Setenv("HOME", home)
	ddir := filepath.Join(home, ".datuplet")
	if err := os.MkdirAll(ddir, 0o700); err != nil {
		t.Fatal(err)
	}
	cluster := map[string]any{
		"lakekeeper_url":   fx.catalogURL(),
		"warehouse_name":   fx.warehouseName,
		"pipeline_api_url": mint.URL,
		"projects": []map[string]string{
			{"id": "proj", "name": "default", "lakekeeper_project_id": fx.projectID},
		},
	}
	cb, _ := json.Marshal(cluster)
	if err := os.WriteFile(filepath.Join(ddir, "cluster.json"), cb, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ddir, "api-token"), []byte("dummy-api-token"), 0o600); err != nil {
		t.Fatal(err)
	}

	countSQL := fmt.Sprintf(`SELECT count(*) AS n FROM lk."%s"."%s"`, seedNamespace, seedTable)

	// --sql, json format: assert the seeded count comes back.
	t.Run("sql-json", func(t *testing.T) {
		stdout, stderr, code := runMain(t, []string{"--sql", countSQL, "--format", "json", "--memory-limit", "1500MiB"}, "")
		if code != 0 {
			t.Fatalf("exit=%d stderr=%s", code, stderr)
		}
		var res struct {
			Rows [][]any `json:"rows"`
		}
		if err := json.Unmarshal([]byte(stdout), &res); err != nil {
			t.Fatalf("decode: %v\n%s", err, stdout)
		}
		if len(res.Rows) != 1 || fmt.Sprintf("%v", res.Rows[0][0]) != fmt.Sprint(seedRowCount) {
			t.Fatalf("rows=%v want one row [%d]", res.Rows, seedRowCount)
		}
	})

	// -f FILE path.
	t.Run("file-table", func(t *testing.T) {
		fpath := filepath.Join(t.TempDir(), "q.sql")
		if err := os.WriteFile(fpath, []byte(countSQL), 0o600); err != nil {
			t.Fatal(err)
		}
		stdout, stderr, code := runMain(t, []string{"-f", fpath, "--format", "table"}, "")
		if code != 0 {
			t.Fatalf("exit=%d stderr=%s", code, stderr)
		}
		if !strings.Contains(stdout, fmt.Sprint(seedRowCount)) {
			t.Fatalf("table output missing count: %s", stdout)
		}
	})

	// stdin path, csv format.
	t.Run("stdin-csv", func(t *testing.T) {
		stdout, stderr, code := runMain(t, []string{"--format", "csv"}, countSQL)
		if code != 0 {
			t.Fatalf("exit=%d stderr=%s", code, stderr)
		}
		lines := strings.Split(strings.TrimSpace(stdout), "\n")
		if len(lines) != 2 { // header + 1 row
			t.Fatalf("csv want 2 lines, got %d: %s", len(lines), stdout)
		}
		if !strings.Contains(lines[1], fmt.Sprint(seedRowCount)) {
			t.Fatalf("csv row missing count: %s", stdout)
		}
	})

	// Bad user SQL → exit 1.
	t.Run("bad-sql-exit1", func(t *testing.T) {
		_, _, code := runMain(t, []string{"--sql", `SELECT * FROM nonexistent_table_xyz`}, "")
		if code != exitUserFailure {
			t.Fatalf("bad SQL exit=%d, want %d", code, exitUserFailure)
		}
	})
}

// runMain drives realMain with the given argv + stdin string, capturing
// stdout/stderr via os.Pipe (realMain takes *os.File). Returns captured
// output and the exit code.
func runMain(t *testing.T, argv []string, stdin string) (stdoutStr, stderrStr string, code int) {
	t.Helper()

	inR, inW, _ := os.Pipe()
	defer inR.Close()
	go func() { _, _ = inW.WriteString(stdin); inW.Close() }()

	outR, outW, _ := os.Pipe()
	defer outR.Close()
	errR, errW, _ := os.Pipe()
	defer errR.Close()

	code = realMain(argv, inR, outW, errW)
	outW.Close()
	errW.Close()

	var ob, eb bytes.Buffer
	_, _ = io.Copy(&ob, outR)
	_, _ = io.Copy(&eb, errR)
	return ob.String(), eb.String(), code
}

// --- fixture (mirrors components/queryengine/integration_test.go) ----------

type fixture struct {
	t             *testing.T
	pgPort        int
	minioPort     int
	lkPort        int
	projectID     string
	warehouseName string
}

func (f *fixture) lakekeeperBase() string { return fmt.Sprintf("http://127.0.0.1:%d", f.lkPort) }
func (f *fixture) catalogURL() string     { return f.lakekeeperBase() + "/catalog" }
func (f *fixture) minioEndpoint() string  { return fmt.Sprintf("http://127.0.0.1:%d", f.minioPort) }

func provision(ctx context.Context, t *testing.T) *fixture {
	removeLeftovers()
	f := &fixture{
		t:             t,
		pgPort:        freePort(t),
		minioPort:     freePort(t),
		lkPort:        freePort(t),
		warehouseName: "dq-it-wh",
	}
	t.Logf("ports: pg=%d minio=%d lakekeeper=%d", f.pgPort, f.minioPort, f.lkPort)

	runDetachedEnv(t, "dq-it-pg",
		[]string{"POSTGRES_PASSWORD=" + pgPassword, "POSTGRES_DB=lakekeeper"},
		"postgres:16", "-c", fmt.Sprintf("port=%d", f.pgPort))
	waitPostgres(ctx, t, f.pgPort)

	runDetachedEnv(t, "dq-it-minio",
		[]string{"MINIO_ROOT_USER=" + minioRootUser, "MINIO_ROOT_PASSWORD=" + minioRootPassword},
		"minio/minio:RELEASE.2024-12-18T13-15-44Z",
		"server", "/data", "--address", fmt.Sprintf(":%d", f.minioPort))
	waitHTTP(ctx, t, fmt.Sprintf("%s/minio/health/live", f.minioEndpoint()))
	makeBucket(ctx, t, f)

	lkEnv := []string{
		fmt.Sprintf("LAKEKEEPER__PG_DATABASE_URL_READ=postgres://postgres:%s@127.0.0.1:%d/lakekeeper", pgPassword, f.pgPort),
		fmt.Sprintf("LAKEKEEPER__PG_DATABASE_URL_WRITE=postgres://postgres:%s@127.0.0.1:%d/lakekeeper", pgPassword, f.pgPort),
		"LAKEKEEPER__PG_ENCRYPTION_KEY=" + lkEncryptionKey,
		fmt.Sprintf("LAKEKEEPER__LISTEN_PORT=%d", f.lkPort),
	}
	runOneShot(ctx, t, "dq-it-lk-migrate", lkEnv, "quay.io/lakekeeper/catalog:v0.12.1", "migrate")
	runDetachedEnv(t, "dq-it-lk", lkEnv, "quay.io/lakekeeper/catalog:v0.12.1", "serve")
	waitHTTP(ctx, t, f.lakekeeperBase()+"/health")

	bootstrapLakekeeper(ctx, t, f)
	f.projectID = createWarehouse(ctx, t, f)
	t.Logf("provisioned: project=%s warehouse=%s", f.projectID, f.warehouseName)
	return f
}

func (f *fixture) teardown() { removeLeftovers() }

func removeLeftovers() {
	for _, name := range []string{"dq-it-lk", "dq-it-lk-migrate", "dq-it-minio", "dq-it-pg", "dq-it-mc"} {
		_ = exec.Command("docker", "rm", "-f", name).Run()
	}
}

func runDetachedEnv(t *testing.T, name string, env []string, image string, args ...string) {
	t.Helper()
	full := []string{"run", "-d", "--name", name, "--network", "host"}
	for _, e := range env {
		full = append(full, "-e", e)
	}
	full = append(full, image)
	full = append(full, args...)
	if out, err := exec.Command("docker", full...).CombinedOutput(); err != nil {
		t.Fatalf("docker run %s: %v\n%s", name, err, out)
	}
}

func runOneShot(ctx context.Context, t *testing.T, name string, env []string, image string, args ...string) {
	t.Helper()
	full := []string{"run", "--rm", "--name", name, "--network", "host"}
	for _, e := range env {
		full = append(full, "-e", e)
	}
	full = append(full, image)
	full = append(full, args...)
	if out, err := exec.CommandContext(ctx, "docker", full...).CombinedOutput(); err != nil {
		t.Fatalf("docker run (one-shot) %s: %v\n%s", name, err, out)
	}
}

func waitHTTP(ctx context.Context, t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for %s", url)
}

func waitPostgres(ctx context.Context, t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		out, err := exec.CommandContext(ctx, "docker", "exec", "dq-it-pg",
			"pg_isready", "-h", "127.0.0.1", "-p", fmt.Sprint(port)).CombinedOutput()
		if err == nil && strings.Contains(string(out), "accepting connections") {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for postgres on :%d", port)
}

func makeBucket(ctx context.Context, t *testing.T, f *fixture) {
	t.Helper()
	script := fmt.Sprintf(
		"mc alias set local %s %s %s && mc mb --ignore-existing local/%s",
		f.minioEndpoint(), minioRootUser, minioRootPassword, minioBucket)
	if out, err := exec.CommandContext(ctx, "docker", "run", "--rm", "--name", "dq-it-mc",
		"--network", "host", "--entrypoint", "sh",
		"minio/mc", "-c", script).CombinedOutput(); err != nil {
		t.Fatalf("create bucket: %v\n%s", err, out)
	}
}

func bootstrapLakekeeper(ctx context.Context, t *testing.T, f *fixture) {
	t.Helper()
	postJSONNoAuth(ctx, t, f.lakekeeperBase()+"/management/v1/bootstrap",
		map[string]any{"accept-terms-of-use": true}, http.StatusNoContent)
}

func createWarehouse(ctx context.Context, t *testing.T, f *fixture) string {
	t.Helper()
	body := map[string]any{
		"warehouse-name": f.warehouseName,
		"storage-profile": map[string]any{
			"type": "s3", "bucket": minioBucket, "region": "local-01",
			"sts-enabled": true, "flavor": "s3-compat",
			"endpoint": f.minioEndpoint(), "path-style-access": true,
		},
		"storage-credential": map[string]any{
			"type": "s3", "credential-type": "access-key",
			"aws-access-key-id": minioRootUser, "aws-secret-access-key": minioRootPassword,
		},
		"delete-profile": map[string]any{"type": "hard"},
	}
	postJSONNoAuth(ctx, t, f.lakekeeperBase()+"/management/v1/warehouse", body, http.StatusCreated)

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, f.lakekeeperBase()+"/management/v1/warehouse", nil)
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("list warehouses: %v", err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list warehouses HTTP %d: %s", resp.StatusCode, rb)
	}
	var parsed struct {
		Warehouses []struct {
			Name      string `json:"name"`
			ProjectID string `json:"project-id"`
		} `json:"warehouses"`
	}
	if err := json.Unmarshal(rb, &parsed); err != nil {
		t.Fatalf("decode warehouses: %v\n%s", err, rb)
	}
	for _, w := range parsed.Warehouses {
		if w.Name == f.warehouseName {
			if w.ProjectID == "" {
				return "00000000-0000-0000-0000-000000000000"
			}
			return w.ProjectID
		}
	}
	t.Fatalf("warehouse %q not found: %s", f.warehouseName, rb)
	return ""
}

func postJSONNoAuth(ctx context.Context, t *testing.T, url string, body any, want int) {
	t.Helper()
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		t.Fatalf("encode body for %s: %v", url, err)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, buf)
	req.Header.Set("content-type", "application/json")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != want {
		t.Fatalf("POST %s: HTTP %d (want %d): %s", url, resp.StatusCode, want, rb)
	}
}

func seedPostsTable(ctx context.Context, t *testing.T, f *fixture) {
	t.Helper()
	cl, err := catalogwriter.NewClient(ctx, catalogwriter.ClientConfig{
		Name:          "dq-it",
		URI:           f.catalogURL(),
		Warehouse:     f.projectID + "/" + f.warehouseName,
		TokenProvider: func(context.Context) (string, error) { return dummyCatalogJWT, nil },
	})
	if err != nil {
		t.Fatalf("catalogwriter.NewClient: %v", err)
	}
	if err := cl.Catalog.CreateNamespace(ctx, icebergtable.Identifier{seedNamespace}, iceberg.Properties{}); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "title", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	tbl, err := cl.Catalog.CreateTable(ctx, icebergtable.Identifier{seedNamespace, seedTable}, schema)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	rec := buildSeedRecord(t)
	defer rec.Release()
	txn := tbl.NewTransaction()
	arrowTable := array.NewTableFromRecords(rec.Schema(), []arrow.Record{rec})
	defer arrowTable.Release()
	if err := txn.AppendTable(ctx, arrowTable, int64(seedRowCount), nil); err != nil {
		t.Fatalf("AppendTable: %v", err)
	}
	if _, err := txn.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	t.Logf("seeded %d rows into %s.%s", seedRowCount, seedNamespace, seedTable)
}

func buildSeedRecord(t *testing.T) arrow.Record {
	t.Helper()
	md1 := arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"1"})
	md2 := arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"2"})
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false, Metadata: md1},
		{Name: "title", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: md2},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	idB := b.Field(0).(*array.Int64Builder)
	titleB := b.Field(1).(*array.StringBuilder)
	for i := 0; i < seedRowCount; i++ {
		idB.Append(int64(i + 1))
		titleB.Append(fmt.Sprintf("post-%d", i+1))
	}
	return b.NewRecord()
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
