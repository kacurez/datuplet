//go:build duckdb_arrow && integration

// Package queryengine integration test for attachCatalog (RFC 022 Task 1.3).
//
// Run:
//
//	go test -tags 'duckdb_arrow integration' -run TestAttachIntegration ./...
//
// This test provisions its OWN lakekeeper + MinIO + postgres on the HOST
// network via the docker CLI (no testcontainers dependency, no Kubernetes).
// Host-network containers are required because on OrbStack macOS the
// cluster-internal DNS in the existing datuplet-e2e cluster is unreachable
// from the host, and lakekeeper-vended STS creds must carry an endpoint the
// host-side DuckDB can dial (RFC 022 Spike 0.1 "execution locus" note).
//
// lakekeeper runs with NO openid/openfga config → authentication disabled
// (Actor::Anonymous) and authorization=allowall, so bootstrap, warehouse
// creation, the catalog handshake, and table writes/reads all proceed without
// a real JWT. The CatalogJWT passed to attachCatalog is therefore a dummy that
// lakekeeper never validates.
//
// The seed path reuses the repo's own iceberg-go write stack
// (pkg/catalogwriter): CreateNamespace + CreateTable + Transaction.AppendTable
// + Commit, all through lakekeeper-vended creds against the host-reachable
// MinIO. Phase 2 wires this into `make`; for now it is manual + CI-ready.
package queryengine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberg "github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/catalogwriter"

	// Blank import: registers Datuplet's iceberg-go IO scheme factories
	// (load-bearing at process init — see RFC 019 §4.5). For the S3/MinIO
	// path the gocloud `s3://` registration baked into pkg/catalogwriter is
	// what actually matters, but the queryengine binary always blank-imports
	// this package, so the test does too.
	_ "github.com/datuplet/datuplet/pkg/datupleticeio"
)

const (
	minioRootUser     = "qe-it-root"
	minioRootPassword = "qe-it-secret-pw"
	minioBucket       = "qe-it-bucket"
	pgPassword        = "qe-it-pg-pw"
	// PG_ENCRYPTION_KEY is mandatory for lakekeeper; any non-empty value works
	// for a throwaway fixture.
	lkEncryptionKey = "qe-it-throwaway-encryption-key"
	seedNamespace = "qe"
	seedTable     = "posts"
	seedRowCount  = 7
	// dummyCatalogJWT is a structurally valid (but unsigned) JWT shape: three
	// base64url segments. attachCatalog's shape guard rejects malformed tokens
	// before any SQL is built, so the fixture must use a well-formed shape.
	// lakekeeper authn is disabled here, so it is never actually validated.
	dummyCatalogJWT = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJxZSJ9.c2ln"
)

func TestAttachIntegration(t *testing.T) {
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skipf("docker not available (docker info failed: %v); skipping integration test", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	fx := provision(ctx, t)
	defer fx.teardown()

	// --- Seed: namespace "qe", table "posts", seedRowCount committed rows. ---
	seedPostsTable(ctx, t, fx)

	// --- Attach + read via the production lifecycle: open → attach → lock. ---
	e, err := openEngine(ctx, Request{TempDir: t.TempDir()})
	if err != nil {
		t.Fatalf("openEngine: %v", err)
	}
	defer e.Close()

	if err := attachCatalog(ctx, e, Request{
		LakekeeperURL: fx.catalogURL(),
		Warehouse:     fx.projectID + "/" + fx.warehouseName,
		CatalogJWT:    dummyCatalogJWT,
	}); err != nil {
		t.Fatalf("attachCatalog: %v", err)
	}

	// (a) fully-qualified lk."qe"."posts" resolves to the seeded row count.
	assertCount(ctx, t, e, `SELECT count(*) FROM lk."qe"."posts"`, seedRowCount, "fully-qualified")

	// (b) two-part "qe".posts resolves post-USE (USE lk."qe" made lk current).
	assertCount(ctx, t, e, `SELECT count(*) FROM "qe".posts`, seedRowCount, "two-part")

	// (c) bare posts resolves post-USE (current schema is lk."qe").
	assertCount(ctx, t, e, `SELECT count(*) FROM posts`, seedRowCount, "bare")

	// Regression / Spike 0.2 positive control: the count read STILL works after
	// the lockdown posture is applied.
	if err := e.lock(ctx); err != nil {
		t.Fatalf("lock: %v", err)
	}
	assertCount(ctx, t, e, `SELECT count(*) FROM lk."qe"."posts"`, seedRowCount, "post-lock")
}

func assertCount(ctx context.Context, t *testing.T, e *engine, query string, want int, label string) {
	t.Helper()
	var got int
	if err := e.conn.QueryRowContext(ctx, query).Scan(&got); err != nil {
		t.Fatalf("%s read (%s): %v", label, query, err)
	}
	if got != want {
		t.Fatalf("%s read: count = %d, want %d", label, got, want)
	}
}

// fixture holds the provisioned endpoints + identifiers needed by the test.
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

// provision spins up postgres + minio + lakekeeper on the host network, polls
// each for readiness, then bootstraps lakekeeper and creates an S3 warehouse.
// It registers teardown so containers are removed even on failure.
func provision(ctx context.Context, t *testing.T) *fixture {
	removeLeftovers()

	f := &fixture{
		t:             t,
		pgPort:        freePort(t),
		minioPort:     freePort(t),
		lkPort:        freePort(t),
		warehouseName: "qe-it-wh",
	}
	t.Logf("ports: pg=%d minio=%d lakekeeper=%d", f.pgPort, f.minioPort, f.lkPort)

	// 1. postgres (lakekeeper metadata DB) on a non-default port. Env (password
	//    + db) must be set at launch; the `-c port=` arg moves it off 5432 to
	//    avoid clashing with any developer postgres on the host network.
	runDetachedEnv(t, "qe-it-pg",
		[]string{"POSTGRES_PASSWORD=" + pgPassword, "POSTGRES_DB=lakekeeper"},
		"postgres:16",
		"-c", fmt.Sprintf("port=%d", f.pgPort),
	)
	waitPostgres(ctx, t, f.pgPort)

	// 2. MinIO + bucket.
	runDetachedEnv(t, "qe-it-minio",
		[]string{"MINIO_ROOT_USER=" + minioRootUser, "MINIO_ROOT_PASSWORD=" + minioRootPassword},
		"minio/minio:RELEASE.2024-12-18T13-15-44Z",
		"server", "/data",
		"--address", fmt.Sprintf(":%d", f.minioPort),
	)
	waitHTTP(ctx, t, fmt.Sprintf("%s/minio/health/live", f.minioEndpoint()))
	makeBucket(ctx, t, f)

	// 3. lakekeeper: migrate (one-shot) then serve. No OPENID / OPENFGA config
	//    → anonymous + allowall.
	lkEnv := []string{
		fmt.Sprintf("LAKEKEEPER__PG_DATABASE_URL_READ=postgres://postgres:%s@127.0.0.1:%d/lakekeeper", pgPassword, f.pgPort),
		fmt.Sprintf("LAKEKEEPER__PG_DATABASE_URL_WRITE=postgres://postgres:%s@127.0.0.1:%d/lakekeeper", pgPassword, f.pgPort),
		"LAKEKEEPER__PG_ENCRYPTION_KEY=" + lkEncryptionKey,
		fmt.Sprintf("LAKEKEEPER__LISTEN_PORT=%d", f.lkPort),
	}
	runOneShot(ctx, t, "qe-it-lk-migrate", lkEnv, "quay.io/lakekeeper/catalog:v0.12.1", "migrate")
	runDetachedEnv(t, "qe-it-lk", lkEnv, "quay.io/lakekeeper/catalog:v0.12.1", "serve")
	waitHTTP(ctx, t, f.lakekeeperBase()+"/health")

	// 4. Bootstrap + create the S3 warehouse (no JWT — authn disabled).
	bootstrapLakekeeper(ctx, t, f)
	f.projectID = createWarehouse(ctx, t, f)
	t.Logf("provisioned: project=%s warehouse=%s", f.projectID, f.warehouseName)

	return f
}

func (f *fixture) teardown() {
	for _, name := range []string{"qe-it-lk", "qe-it-lk-migrate", "qe-it-minio", "qe-it-pg", "qe-it-mc"} {
		_ = exec.Command("docker", "rm", "-f", name).Run()
	}
}

func removeLeftovers() {
	for _, name := range []string{"qe-it-lk", "qe-it-lk-migrate", "qe-it-minio", "qe-it-pg", "qe-it-mc"} {
		_ = exec.Command("docker", "rm", "-f", name).Run()
	}
}

// --- docker helpers ---------------------------------------------------------

// runDetachedEnv starts a host-network container in the background with the
// given env, image, and command args.
func runDetachedEnv(t *testing.T, name string, env []string, image string, args ...string) {
	t.Helper()
	full := []string{"run", "-d", "--name", name, "--network", "host"}
	for _, e := range env {
		full = append(full, "-e", e)
	}
	full = append(full, image)
	full = append(full, args...)
	out, err := exec.Command("docker", full...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run %s: %v\n%s", name, err, out)
	}
}

// runOneShot runs a container to completion (e.g. lakekeeper migrate) and fails
// the test on non-zero exit.
func runOneShot(ctx context.Context, t *testing.T, name string, env []string, image string, args ...string) {
	t.Helper()
	full := []string{"run", "--rm", "--name", name, "--network", "host"}
	for _, e := range env {
		full = append(full, "-e", e)
	}
	full = append(full, image)
	full = append(full, args...)
	out, err := exec.CommandContext(ctx, "docker", full...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run (one-shot) %s: %v\n%s", name, err, out)
	}
}

// --- readiness probes -------------------------------------------------------

func waitHTTP(ctx context.Context, t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
		if err == nil {
			resp.Body.Close()
			// Both probed endpoints (lakekeeper /health, MinIO
			// /minio/health/live) return 200 only when fully ready. A stricter
			// == 200 avoids treating a transient 4xx during startup as ready.
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
		out, err := exec.CommandContext(ctx, "docker", "exec", "qe-it-pg",
			"pg_isready", "-h", "127.0.0.1", "-p", fmt.Sprint(port)).CombinedOutput()
		if err == nil && strings.Contains(string(out), "accepting connections") {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for postgres on :%d", port)
}

// makeBucket creates the warehouse bucket via a one-shot minio/mc container.
func makeBucket(ctx context.Context, t *testing.T, f *fixture) {
	t.Helper()
	script := fmt.Sprintf(
		"mc alias set local %s %s %s && mc mb --ignore-existing local/%s",
		f.minioEndpoint(), minioRootUser, minioRootPassword, minioBucket,
	)
	out, err := exec.CommandContext(ctx, "docker", "run", "--rm", "--name", "qe-it-mc",
		"--network", "host", "--entrypoint", "sh",
		"minio/mc", "-c", script).CombinedOutput()
	if err != nil {
		t.Fatalf("create bucket: %v\n%s", err, out)
	}
}

// --- lakekeeper REST (no auth) ---------------------------------------------

func bootstrapLakekeeper(ctx context.Context, t *testing.T, f *fixture) {
	t.Helper()
	postJSONNoAuth(ctx, t, f.lakekeeperBase()+"/management/v1/bootstrap",
		map[string]any{"accept-terms-of-use": true}, http.StatusNoContent)
}

// createWarehouse POSTs an S3/MinIO warehouse mirroring buildWarehouseBody in
// cmd/pipeline-api/admin_lakekeeper.go, then reads the project-id back via the
// warehouse list. Returns the lakekeeper project id (the default nil-UUID
// project, since this fixture creates no extra projects).
func createWarehouse(ctx context.Context, t *testing.T, f *fixture) string {
	t.Helper()
	body := map[string]any{
		"warehouse-name": f.warehouseName,
		"storage-profile": map[string]any{
			"type":              "s3",
			"bucket":            minioBucket,
			"region":            "local-01",
			"sts-enabled":       true,
			"flavor":            "s3-compat",
			"endpoint":          f.minioEndpoint(),
			"path-style-access": true,
		},
		"storage-credential": map[string]any{
			"type":                  "s3",
			"credential-type":       "access-key",
			"aws-access-key-id":     minioRootUser,
			"aws-secret-access-key": minioRootPassword,
		},
		"delete-profile": map[string]any{"type": "hard"},
	}
	postJSONNoAuth(ctx, t, f.lakekeeperBase()+"/management/v1/warehouse", body, http.StatusCreated)

	// Read the project id off the warehouse list. With authn disabled the
	// warehouse lands in lakekeeper's default project (nil UUID); read it back
	// rather than hardcoding, so a future lakekeeper default change is caught.
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
	t.Fatalf("warehouse %q not found in list: %s", f.warehouseName, rb)
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

// --- seed via the repo's iceberg-go write stack ----------------------------

// seedPostsTable creates namespace "qe" + table "posts" through lakekeeper and
// commits seedRowCount rows via Transaction.AppendTable (iceberg-go writes the
// parquet to MinIO using lakekeeper-vended STS creds — same FS path the
// production writer uses). Uses a dummy bearer (authn disabled).
func seedPostsTable(ctx context.Context, t *testing.T, f *fixture) {
	t.Helper()
	cl, err := catalogwriter.NewClient(ctx, catalogwriter.ClientConfig{
		Name:          "qe-it",
		URI:           f.catalogURL(),
		Warehouse:     f.projectID + "/" + f.warehouseName,
		TokenProvider: func(context.Context) (string, error) { return dummyCatalogJWT, nil },
	})
	if err != nil {
		t.Fatalf("catalogwriter.NewClient: %v", err)
	}

	nsIdent := icebergtable.Identifier{seedNamespace}
	if err := cl.Catalog.CreateNamespace(ctx, nsIdent, iceberg.Properties{}); err != nil {
		t.Fatalf("CreateNamespace %q: %v", seedNamespace, err)
	}

	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "title", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	tbl, err := cl.Catalog.CreateTable(ctx, icebergtable.Identifier{seedNamespace, seedTable}, icebergSchema)
	if err != nil {
		t.Fatalf("CreateTable %s.%s: %v", seedNamespace, seedTable, err)
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

// buildSeedRecord builds an Arrow record with field-ids matching the iceberg
// schema (id=1, title=2). iceberg-go's parquet writer keys columns by the
// PARQUET:field_id metadata, so the Arrow schema must carry those ids.
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

// freePort asks the kernel for an unused TCP port, then closes the listener so
// the container can bind it. Small race window between close and container
// bind (a classic TOCTOU): explicitly accepted here because this is a
// manual / CI-opt-in integration fixture (behind the `integration` build tag),
// not hot production code, and it avoids hardcoded clashes (e.g. a kubectl
// port-forward on 8181).
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
