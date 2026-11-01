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
	"errors"
	"fmt"
	"io"
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
// AddFiles can commit. The five addFilesRejects timestamp types are excluded
// because iceberg-go refuses them at commit for today's writer too (naive
// s/ms/us against the timestamptz column, any ns); maintainer ruling
// 2026-11-01, spec Write path item 6. TestIntegrationAddFilesPerType pins
// that refusal so a fix or a regression is noticed.
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
			arr, _, err := array.FromJSON(mem, in.dt, strings.NewReader(js), array.WithUseNumber())
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

// --- reads (Task 4) ----------------------------------------------------------

// readPlane is a locked plane (New applies the lockdown) with the fixture's
// catalog configured for reads.
func (e *integrationEnv) readPlane(t *testing.T) *Plane {
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
		Catalog: CatalogAttach{
			URL:       e.fx.catalogURL(),
			ProjectID: e.fx.projectID,
			Warehouse: e.fx.warehouseName,
			Token:     dummyCatalogJWT,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// writeCommitted creates ns.tbl with sch, writes the batches through a writer
// of the write plane and commits them (APPEND).
func (e *integrationEnv) writeCommitted(t *testing.T, ns, tbl string, sch *arrow.Schema, batches ...arrow.RecordBatch) {
	t.Helper()
	r := e.resolver(t)
	target := createTable(t, r, ns, tbl, sch)
	w, err := e.plane(t).OpenTableWriter(context.Background(), datagateway.WriterSpec{
		DataPrefix: target.BasePath, FilePrefix: "part-read0001-w1", Schema: sch, Creds: target.VendedCreds,
	})
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	writeBatches(t, w, batches...)
	files, err := w.Close(context.Background())
	if err != nil || len(files) == 0 {
		t.Fatalf("Close = %+v, %v", files, err)
	}
	commit(t, r, ns, tbl, files, icebergjob.WriteModeAppend, "key-"+tbl)
}

func openReader(t *testing.T, p *Plane, ns, tbl string) *reader {
	t.Helper()
	rs, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: ns, Table: tbl})
	if err != nil {
		t.Fatalf("OpenTableReader(%s.%s): %v", ns, tbl, err)
	}
	t.Cleanup(func() { rs.Close() })
	return rs.(*reader)
}

func TestIntegrationAttachAfterLock(t *testing.T) {
	env := sharedEnv(t)
	const ns, tbl = "dg_read", "after_lock"
	env.writeCommitted(t, ns, tbl, idNameSchema, idNameBatch(t, 0, 1000))

	p := env.readPlane(t)
	c := queryConn(t, p)
	if err := c.Exec(context.Background(), "SET threads = 2"); err == nil {
		t.Fatal("the plane is not locked: SET succeeded")
	}
	r := openReader(t, p, ns, tbl)
	if got := drain(t, r); got != 1000 {
		t.Fatalf("rows = %d, want 1000", got)
	}
	if n := scalar(t, c, "SELECT count(*) FROM duckdb_secrets() WHERE name = "+duck.Literal(catalogSecretName)); n != 0 {
		t.Fatalf("catalog secret left after ATTACH: %d", n)
	}
	if n := scalar(t, c, "SELECT count(*) FROM duckdb_databases() WHERE database_name = 'lk' AND readonly"); n != 1 {
		t.Fatalf("lk attached read-only: %d", n)
	}
}

func TestIntegrationReadBack(t *testing.T) {
	env := sharedEnv(t)
	const ns, tbl = "dg_read", "read_back"
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "f", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "d", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)
	rec, _, err := array.RecordFromJSON(memory.NewGoAllocator(), sch, strings.NewReader(`[
		{"id": 1, "i32": -7, "name": "alpha", "f": 1.5, "b": true, "d": "2026-10-31", "ts": "2026-10-31T12:34:56.123456Z", "bin": "AAEC"},
		{"id": 2, "i32": null, "name": null, "f": null, "b": false, "d": null, "ts": null, "bin": null}
	]`))
	if err != nil {
		t.Fatal(err)
	}
	env.writeCommitted(t, ns, tbl, sch, rec)

	p := env.readPlane(t)
	r := openReader(t, p, ns, tbl)
	got := r.Schema()
	t.Logf("read schema: %s", got)
	if got.NumFields() != sch.NumFields() {
		t.Fatalf("schema = %s", got)
	}
	for i, f := range sch.Fields() {
		if got.Field(i).Name != f.Name || !arrow.TypeEqual(got.Field(i).Type, f.Type) {
			t.Errorf("field %d = %s %s, want %s %s", i, got.Field(i).Name, got.Field(i).Type, f.Name, f.Type)
		}
	}
	var rows []arrow.RecordBatch
	for {
		b, err := r.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		rows = append(rows, b)
	}
	tbl2 := array.NewTableFromRecords(got, rows)
	defer tbl2.Release()
	for _, b := range rows {
		b.Release()
	}
	if tbl2.NumRows() != 2 {
		t.Fatalf("rows = %d, want 2", tbl2.NumRows())
	}
	want := map[int64]string{
		1: `{"b":true,"bin":"AAEC","d":"2026-10-31","f":1.5,"i32":-7,"id":1,"name":"alpha","ts":"2026-10-31T12:34:56.123456Z"}`,
		2: `{"b":false,"bin":null,"d":null,"f":null,"i32":null,"id":2,"name":null,"ts":null}`,
	}
	tr := array.NewTableReader(tbl2, -1)
	defer tr.Release()
	for tr.Next() {
		b := tr.RecordBatch()
		for i := 0; i < int(b.NumRows()); i++ {
			slice := b.NewSlice(int64(i), int64(i+1))
			js, err := slice.MarshalJSON()
			slice.Release()
			if err != nil {
				t.Fatal(err)
			}
			line := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(string(js)), "["), "]"))
			id := b.Column(0).(*array.Int64).Value(i)
			if line != want[id] {
				t.Errorf("row %d = %s\n want %s", id, line, want[id])
			}
		}
	}
}

// Review Focus 5: a table without a snapshot reads as its schema and no rows.
func TestIntegrationReadEmptyTable(t *testing.T) {
	env := sharedEnv(t)
	const ns, tbl = "dg_read", "empty"
	createTable(t, env.resolver(t), ns, tbl, idNameSchema)

	r := openReader(t, env.readPlane(t), ns, tbl)
	sch := r.Schema()
	if sch.NumFields() != 2 || sch.Field(0).Name != "id" || sch.Field(1).Name != "name" {
		t.Fatalf("schema = %s", sch)
	}
	if _, err := r.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("Next = %v, want io.EOF", err)
	}
}

// Review Focus 1: hostile table and column names are read back verbatim.
func TestIntegrationReadHostileNames(t *testing.T) {
	env := sharedEnv(t)
	const ns, tbl = "dg_read", "t\"x'y"
	names := []string{`a"b`, `x'y`, `;DROP`, "nl\nx", `ünï`}
	var fields []arrow.Field
	for _, n := range names {
		fields = append(fields, arrow.Field{Name: n, Type: arrow.BinaryTypes.String, Nullable: true})
	}
	sch := arrow.NewSchema(fields, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), sch)
	for i, n := range names {
		b.Field(i).(*array.StringBuilder).Append("v:" + n)
	}
	rec := b.NewRecordBatch()
	b.Release()
	env.writeCommitted(t, ns, tbl, sch, rec)

	r := openReader(t, env.readPlane(t), ns, tbl)
	got, err := r.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	defer got.Release()
	if got.NumRows() != 1 || int(got.NumCols()) != len(names) {
		t.Fatalf("read %d rows x %d cols", got.NumRows(), got.NumCols())
	}
	for i, n := range names {
		if got.ColumnName(i) != n || got.Column(i).(*array.String).Value(0) != "v:"+n {
			t.Errorf("column %d = %q %q, want %q", i, got.ColumnName(i), got.Column(i).(*array.String).Value(0), n)
		}
	}
	if _, err := r.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("Next = %v, want io.EOF", err)
	}
}

const bigRows = 300_000

func (e *integrationEnv) writeBig(t *testing.T, ns, tbl string) {
	t.Helper()
	var batches []arrow.RecordBatch
	for i := 0; i < bigRows; i += 50_000 {
		batches = append(batches, idNameBatch(t, i, 50_000))
	}
	e.writeCommitted(t, ns, tbl, idNameSchema, batches...)
}

func TestIntegrationReaderIsStreaming(t *testing.T) {
	env := sharedEnv(t)
	const ns, tbl = "dg_read", "streaming"
	env.writeBig(t, ns, tbl)

	r := openReader(t, env.readPlane(t), ns, tbl)
	if !r.stream.IsStreaming() {
		t.Fatal("the Iceberg scan is not a streaming result")
	}
	rows, hw := drainHoldingPrevious(t, r)
	if rows != bigRows {
		t.Fatalf("rows = %d, want %d", rows, bigRows)
	}
	if hw > 2 {
		t.Fatalf("batches alive at once = %d, want <= 2", hw)
	}
}

func TestIntegrationReaderCtxOwnership(t *testing.T) {
	env := sharedEnv(t)
	const ns, tbl = "dg_read", "ctx_owned"
	env.writeBig(t, ns, tbl)
	p := env.readPlane(t)

	ctx, cancel := context.WithCancel(context.Background())
	rs, err := p.OpenTableReader(ctx, datagateway.ReaderSpec{Namespace: ns, Table: tbl})
	cancel()
	if err != nil {
		t.Fatalf("OpenTableReader: %v", err)
	}
	defer rs.Close()
	var rows int64
	for {
		live, liveCancel := context.WithTimeout(context.Background(), time.Minute)
		b, err := rs.Next(live)
		liveCancel()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next after %d rows: %v", rows, err)
		}
		rows += b.NumRows()
		b.Release()
	}
	if rows != bigRows {
		t.Fatalf("rows = %d, want %d", rows, bigRows)
	}

	// Close mid-stream, then a new reader drains everything.
	mid := openReader(t, p, ns, tbl)
	b, err := mid.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	b.Release()
	if err := mid.Close(); err != nil {
		t.Fatalf("Close mid-stream: %v", err)
	}
	if got := drain(t, openReader(t, p, ns, tbl)); got != bigRows {
		t.Fatalf("rows after a mid-stream Close = %d", got)
	}
}
