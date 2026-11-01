//go:build integration && duckdb_arrow

package duckplane

// Integration tests: the DuckDB writer against Lakekeeper + silo with real
// vended credentials, committed by the unchanged Go commit path
// (icebergjob.CommitTableFiles → iceberg-go AddFiles / ReplaceDataFiles).
//
// Run (Docker and network to extensions.duckdb.org required):
//
//	cd gateway && CGO_ENABLED=1 go test -tags 'duckdb_arrow integration' -run Integration ./duckplane/ -v -timeout 15m

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/catalog"

	"github.com/datuplet/datuplet/gateway/duck"
	"github.com/datuplet/datuplet/pkg/catalogwriter"
	"github.com/datuplet/datuplet/pkg/datagateway"
	"github.com/datuplet/datuplet/pkg/datagateway/lakekeeper"
	dgschema "github.com/datuplet/datuplet/pkg/datagateway/schema"
	_ "github.com/datuplet/datuplet/pkg/datupleticeio"
	"github.com/datuplet/datuplet/pkg/icebergjob"
)

// integrationEnv is provisioned once for all integration tests and torn down
// by TestMain.
type integrationEnv struct {
	fx     *fixture
	extDir string
}

var (
	envOnce sync.Once
	envVal  *integrationEnv
	envErr  string
)

func sharedEnv(t *testing.T) *integrationEnv {
	t.Helper()
	envOnce.Do(func() {
		if err := exec.Command("docker", "info").Run(); err != nil {
			envErr = fmt.Sprintf("BLOCKED: docker not available: %v", err)
			return
		}
		testCleanups = append(testCleanups, removeLeftovers)
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
		defer cancel()
		fx := provision(ctx, t)
		extDir := installExtensions(t)
		envVal = &integrationEnv{fx: fx, extDir: extDir}
	})
	if envVal == nil {
		if envErr == "" {
			envErr = "integration fixture failed in an earlier test"
		}
		t.Fatal(envErr)
	}
	return envVal
}

// installExtensions INSTALLs httpfs + iceberg into a fresh extension
// directory from an unlocked throwaway database (network to
// extensions.duckdb.org). It must outlive every test, so it is not a
// t.TempDir of the (first) test that creates it.
func installExtensions(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "dg-it-ext-")
	if err != nil {
		t.Fatal(err)
	}
	testCleanups = append(testCleanups, func() { os.RemoveAll(dir) })
	db, err := duck.Open(duck.Settings{ExtensionDir: dir, TempDir: dir + "/tmp"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	c, err := db.Connect()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close()
	for _, s := range []string{"INSTALL httpfs", "INSTALL iceberg"} {
		if err := c.Exec(context.Background(), s); err != nil {
			t.Fatalf("BLOCKED: %s (extensions.duckdb.org unreachable?): %v", s, err)
		}
	}
	return dir
}

func (e *integrationEnv) plane(t *testing.T) *Plane {
	t.Helper()
	p, err := New(Config{
		Settings: duck.Settings{
			Threads:        1,
			MemoryLimit:    "1GiB",
			TempDir:        t.TempDir(),
			MaxTempDirSize: "1GiB",
			ExtensionDir:   e.extDir,
		},
		Extensions: []string{"httpfs", "iceberg"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func (e *integrationEnv) resolver(t *testing.T) *lakekeeper.Resolver {
	t.Helper()
	r, err := lakekeeper.NewResolver(e.fx.catalogURL(), e.fx.warehouseName, dummyCatalogJWT, e.fx.projectID)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// addFilesRejects are golden types iceberg-go AddFiles refuses, with the
// expected error. The Parquet types are the golden ones (today's writer wrote
// the same), so this is pre-existing, not a DuckDB difference: the gateway
// creates every timestamp column as Iceberg timestamptz, which a
// non-UTC-adjusted Parquet timestamp does not match, and this iceberg-go
// rejects nanosecond timestamps outright.
var addFilesRejects = map[string]string{
	"timestamp_s":         "mismatch in fields",
	"timestamp_ms":        "mismatch in fields",
	"timestamp_us":        "mismatch in fields",
	"timestamp_ns":        "'ns' timestamp precision not supported",
	"timestamp_ns_tz_utc": "'ns' timestamp precision not supported",
}

// allTypesSchema has one optional column per accepted golden type that
// AddFiles can commit (see addFilesRejects).
func allTypesSchema(t *testing.T) (*arrow.Schema, []goldenInput) {
	t.Helper()
	golden := loadGolden(t)
	var fields []arrow.Field
	var inputs []goldenInput
	for _, in := range goldenInputs() {
		if _, rejected := addFilesRejects[in.name]; rejected || !golden[in.name].Accepted {
			continue
		}
		fields = append(fields, arrow.Field{Name: "c_" + in.name, Type: in.dt, Nullable: true})
		inputs = append(inputs, in)
	}
	return arrow.NewSchema(fields, nil), inputs
}

// allTypesBatches returns the two golden chunks as two 3-row batches.
func allTypesBatches(t *testing.T, sch *arrow.Schema, inputs []goldenInput) []arrow.RecordBatch {
	t.Helper()
	mem := memory.NewGoAllocator()
	var out []arrow.RecordBatch
	for chunk := 0; chunk < 2; chunk++ {
		cols := make([]arrow.Array, len(inputs))
		for i, in := range inputs {
			js := in.chunk1
			if chunk == 1 {
				js = in.chunk2
			}
			arr, _, err := array.FromJSON(mem, in.dt, strings.NewReader(js))
			if err != nil {
				t.Fatalf("%s: %v", in.name, err)
			}
			cols[i] = arr
		}
		out = append(out, array.NewRecordBatch(sch, cols, 3))
		for _, c := range cols {
			c.Release()
		}
	}
	return out
}

// writeRun writes the golden batches `repeat` times through a new writer and
// returns the files.
func writeRun(t *testing.T, p *Plane, target *lakekeeper.WriterTarget, filePrefix string, sch *arrow.Schema, inputs []goldenInput, repeat int, creds datagateway.CredsSource, targetBytes int64) []datagateway.WrittenFile {
	t.Helper()
	w, err := p.OpenTableWriter(context.Background(), datagateway.WriterSpec{
		DataPrefix:      target.BasePath,
		FilePrefix:      filePrefix,
		Schema:          sch,
		Creds:           creds,
		TargetFileBytes: targetBytes,
	})
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	for r := 0; r < repeat; r++ {
		for _, b := range allTypesBatches(t, sch, inputs) {
			err := w.Write(context.Background(), b)
			b.Release()
			if err != nil {
				w.Abort()
				t.Fatalf("Write: %v", err)
			}
		}
	}
	files, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, f := range files {
		if !strings.HasPrefix(f.Path, target.BasePath+filePrefix+"-") || f.Rows <= 0 || f.Bytes <= 0 {
			t.Fatalf("bad file %+v", f)
		}
	}
	return files
}

func paths(files []datagateway.WrittenFile) []string {
	var out []string
	for _, f := range files {
		out = append(out, f.Path)
	}
	return out
}

func createTable(t *testing.T, r *lakekeeper.Resolver, ns, tbl string, sch *arrow.Schema) *lakekeeper.WriterTarget {
	t.Helper()
	gw, err := dgschema.NewSchemaFromArrow(sch)
	if err != nil {
		t.Fatalf("gateway schema: %v", err)
	}
	target, err := r.LoadOrCreateForWrite(context.Background(), ns, tbl, func(context.Context) (*dgschema.Schema, error) { return gw, nil })
	if err != nil {
		t.Fatalf("LoadOrCreateForWrite: %v", err)
	}
	if target.VendedCreds == nil || !strings.HasPrefix(target.BasePath, "s3://") {
		t.Fatalf("unexpected target %q (creds %v)", target.BasePath, target.VendedCreds != nil)
	}
	return target
}

func commit(t *testing.T, r *lakekeeper.Resolver, ns, tbl string, files []datagateway.WrittenFile, mode icebergjob.WriteMode, key string) *icebergjob.CommitResult {
	t.Helper()
	cat, err := r.Catalog(context.Background())
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	res, err := icebergjob.CommitTableFiles(context.Background(), cat, catalog.ToIdentifier(ns, tbl), paths(files), mode, nil, key)
	if err != nil {
		t.Fatalf("CommitTableFiles %s: %v", mode, err)
	}
	return res
}

// icebergCount counts the table's rows through a fresh, unlocked DuckDB that
// ATTACHes Lakekeeper (no metadata cached between assertions).
func (e *integrationEnv) icebergCount(t *testing.T, ns, tbl string) int64 {
	t.Helper()
	db, err := duck.Open(duck.Settings{ExtensionDir: e.extDir, TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	for _, s := range []string{
		"LOAD httpfs",
		"LOAD iceberg",
		"CREATE OR REPLACE SECRET lk_secret (TYPE ICEBERG, TOKEN " + duck.Literal(dummyCatalogJWT) + ")",
		"ATTACH " + duck.Literal(e.fx.projectID+"/"+e.fx.warehouseName) + " AS lk (TYPE ICEBERG, URI " +
			duck.Literal(e.fx.catalogURL()) + ", SECRET lk_secret, ACCESS_DELEGATION_MODE 'vended_credentials', READ_ONLY)",
	} {
		if err := c.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", strings.Fields(s)[0], err)
		}
	}
	rows, err := c.QueryRows(ctx, "SELECT count(*) FROM lk."+duck.Ident(ns)+"."+duck.Ident(tbl))
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	return rows[0][0].(int64)
}

func currentCommitKey(t *testing.T, r *lakekeeper.Resolver, ns, tbl string) string {
	t.Helper()
	cat, err := r.Catalog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tb, err := cat.LoadTable(context.Background(), catalog.ToIdentifier(ns, tbl))
	if err != nil {
		t.Fatal(err)
	}
	s := tb.CurrentSnapshot()
	if s == nil || s.Summary == nil {
		t.Fatal("no current snapshot")
	}
	return s.Summary.Properties["datuplet.commit-key"]
}

func TestIntegrationWriteAndCommit(t *testing.T) {
	env := sharedEnv(t)
	p := env.plane(t)
	r := env.resolver(t)
	sch, inputs := allTypesSchema(t)
	const ns, tbl = "dg_it", "all_types"
	target := createTable(t, r, ns, tbl, sch)
	t.Logf("data prefix %s", target.BasePath)

	// Run 1 + run 2: APPEND.
	f1 := writeRun(t, p, target, "part-run00001-w1", sch, inputs, 1, target.VendedCreds, 0)
	if len(f1) != 1 || f1[0].Rows != 6 {
		t.Fatalf("run 1 files %+v", f1)
	}
	commit(t, r, ns, tbl, f1, icebergjob.WriteModeAppend, "key-run1")
	f2 := writeRun(t, p, target, "part-run00002-w1", sch, inputs, 2, target.VendedCreds, 0)
	commit(t, r, ns, tbl, f2, icebergjob.WriteModeAppend, "key-run2")
	if got := env.icebergCount(t, ns, tbl); got != 6+12 {
		t.Fatalf("after two APPENDs: %d rows, want 18", got)
	}
	if k := currentCommitKey(t, r, ns, tbl); k != "key-run2" {
		t.Fatalf("snapshot commit key %q", k)
	}

	// Run 3: FULL_LOAD replaces everything with its own rows.
	f3 := writeRun(t, p, target, "part-run00003-w1", sch, inputs, 1, target.VendedCreds, 0)
	res := commit(t, r, ns, tbl, f3, icebergjob.WriteModeFullLoad, "key-run3")
	if res.IdempotencyHit {
		t.Fatal("first FULL_LOAD commit reported an idempotency hit")
	}
	if got := env.icebergCount(t, ns, tbl); got != 6 {
		t.Fatalf("after FULL_LOAD: %d rows, want 6", got)
	}
	if k := currentCommitKey(t, r, ns, tbl); k != "key-run3" {
		t.Fatalf("snapshot commit key %q", k)
	}

	// The same commit key again: idempotency hit, nothing changes.
	again := commit(t, r, ns, tbl, f3, icebergjob.WriteModeFullLoad, "key-run3")
	if !again.IdempotencyHit || again.SnapshotIDAfter != res.SnapshotIDAfter {
		t.Fatalf("repeat commit = %+v, first %+v", again, res)
	}
	if got := env.icebergCount(t, ns, tbl); got != 6 {
		t.Fatalf("after repeat commit: %d rows, want 6", got)
	}

	// A writer aborted mid-COPY: no file is committed, and no writer leaves
	// its secret behind (closed or aborted).
	w, err := p.OpenTableWriter(context.Background(), datagateway.WriterSpec{
		DataPrefix: target.BasePath, FilePrefix: "part-run00004-w1", Schema: sch, Creds: target.VendedCreds,
	})
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	for _, b := range allTypesBatches(t, sch, inputs) {
		if err := w.Write(context.Background(), b); err != nil {
			t.Fatalf("Write: %v", err)
		}
		b.Release()
	}
	w.Abort()
	c, err := p.db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	rows, err := c.QueryRows(context.Background(), "SELECT count(*) FROM duckdb_secrets()")
	if err != nil || rows[0][0].(int64) != 0 {
		t.Fatalf("secrets left after every writer ended: %v, %v", rows, err)
	}
}

// invalidatingCreds forces a fresh Lakekeeper roundtrip for every file and
// records the access key each file got.
type invalidatingCreds struct {
	vc   *catalogwriter.VendedCreds
	mu   sync.Mutex
	keys []string
}

func (c *invalidatingCreds) Get(ctx context.Context) (catalogwriter.Creds, error) {
	c.vc.Invalidate()
	cr, err := c.vc.Get(ctx)
	if err != nil {
		return nil, err
	}
	if s3, ok := cr.(catalogwriter.S3Creds); ok {
		c.mu.Lock()
		c.keys = append(c.keys, s3.AccessKeyID)
		c.mu.Unlock()
	}
	return cr, nil
}

func (c *invalidatingCreds) Invalidate() { c.vc.Invalidate() }

func TestIntegrationCredsInvalidateBetweenFiles(t *testing.T) {
	env := sharedEnv(t)
	p := env.plane(t)
	r := env.resolver(t)
	sch, inputs := allTypesSchema(t)
	const ns, tbl = "dg_it", "rotating"
	target := createTable(t, r, ns, tbl, sch)
	creds := &invalidatingCreds{vc: target.VendedCreds}
	files := writeRun(t, p, target, "part-run00009-w1", sch, inputs, 3, creds, 1) // every batch ends a file
	if len(files) < 3 {
		t.Fatalf("files = %d, want >= 3", len(files))
	}
	creds.mu.Lock()
	keys := append([]string(nil), creds.keys...)
	creds.mu.Unlock()
	if len(keys) != len(files) {
		t.Fatalf("%d credential fetches for %d files", len(keys), len(files))
	}
	distinct := map[string]bool{}
	for _, k := range keys {
		distinct[k] = true
	}
	t.Logf("%d files, %d distinct vended access keys", len(files), len(distinct))
	commit(t, r, ns, tbl, files, icebergjob.WriteModeAppend, "key-rotating")
	if got := env.icebergCount(t, ns, tbl); got != 18 {
		t.Fatalf("rows = %d, want 18", got)
	}
}

// TestIntegrationAddFilesPerType commits each accepted golden type alone and
// pins which of them iceberg-go AddFiles accepts (addFilesRejects).
func TestIntegrationAddFilesPerType(t *testing.T) {
	env := sharedEnv(t)
	p := env.plane(t)
	r := env.resolver(t)
	golden := loadGolden(t)
	for _, in := range goldenInputs() {
		if !golden[in.name].Accepted {
			continue
		}
		sch := arrow.NewSchema([]arrow.Field{{Name: "v", Type: in.dt, Nullable: true}}, nil)
		target := createTable(t, r, "dg_types", in.name, sch)
		files := writeRun(t, p, target, "part-types00-w1", sch, []goldenInput{in}, 1, target.VendedCreds, 0)
		cat, err := r.Catalog(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_, err = icebergjob.CommitTableFiles(context.Background(), cat, catalog.ToIdentifier("dg_types", in.name),
			paths(files), icebergjob.WriteModeAppend, nil, "k")
		want, rejected := addFilesRejects[in.name]
		switch {
		case !rejected && err != nil:
			t.Errorf("%s: AddFiles rejected: %v", in.name, err)
		case rejected && (err == nil || !strings.Contains(err.Error(), want)):
			t.Errorf("%s: AddFiles = %v, want rejection %q", in.name, err, want)
		case !rejected:
			if got := env.icebergCount(t, "dg_types", in.name); got != 6 {
				t.Errorf("%s: %d rows committed, want 6", in.name, got)
			}
		}
	}
}
