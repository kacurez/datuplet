//go:build duckdb_arrow

package duckplane

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	bindings "github.com/duckdb/duckdb-go-bindings"

	// Keeps the duckdb-go/v2 pin in go.mod (scripts/verify-versions.sh).
	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/datuplet/datuplet/gateway/duck"
	"github.com/datuplet/datuplet/pkg/catalogwriter"
	"github.com/datuplet/datuplet/pkg/datagateway"
)

// testCleanups run after every test (shared integration fixtures).
var testCleanups []func()

// TestMain fails the run if any DuckDB handle leaked (counters exist only
// with -tags 'duckdb_arrow debug_bindings').
func TestMain(m *testing.M) {
	code := m.Run()
	for _, f := range testCleanups {
		f()
	}
	if leaks := bindings.GetAllocationCounts(); leaks != "" && code == 0 {
		fmt.Fprintln(os.Stderr, "duckdb handle leak:\n"+leaks)
		code = 1
	}
	os.Exit(code)
}

func testPlane(t *testing.T) *Plane {
	t.Helper()
	p, err := New(Config{Settings: duck.Settings{
		Threads:        1,
		MemoryLimit:    "1GiB",
		TempDir:        t.TempDir(),
		MaxTempDirSize: "1GiB",
	}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("Plane.Close: %v", err)
		}
	})
	return p
}

var idNameSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

// idNameBatch builds n rows with ids start..start+n-1.
func idNameBatch(t *testing.T, start, n int) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(memory.NewGoAllocator(), idNameSchema)
	defer b.Release()
	for i := start; i < start+n; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		b.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("row-%d", i))
	}
	return b.NewRecordBatch()
}

func localSpec(prefix string) datagateway.WriterSpec {
	return datagateway.WriterSpec{
		DataPrefix: prefix,
		FilePrefix: "part-run12345-w1",
		Schema:     idNameSchema,
	}
}

func writeBatches(t *testing.T, w datagateway.TableWriter, batches ...arrow.RecordBatch) {
	t.Helper()
	for _, b := range batches {
		if err := w.Write(context.Background(), b); err != nil {
			t.Fatalf("Write: %v", err)
		}
		b.Release()
	}
}

// queryConn is a connection of the plane's own database for read-back checks.
func queryConn(t *testing.T, p *Plane) *duck.Conn {
	t.Helper()
	c, err := p.db.Connect()
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func scalar(t *testing.T, c *duck.Conn, sql string) int64 {
	t.Helper()
	rows, err := c.QueryRows(context.Background(), sql)
	if err != nil {
		t.Fatalf("QueryRows(%q): %v", sql, err)
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("QueryRows(%q): want 1x1, got %v", sql, rows)
	}
	switch v := rows[0][0].(type) {
	case int64:
		return v
	case uint64:
		return int64(v)
	case int32:
		return int64(v)
	}
	t.Fatalf("QueryRows(%q): %T is not an integer", sql, rows[0][0])
	return 0
}

func listDir(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

func TestWriterSingleFile(t *testing.T) {
	p := testPlane(t)
	dir := t.TempDir() + "/"
	w, err := p.OpenTableWriter(context.Background(), localSpec(dir))
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	writeBatches(t, w, idNameBatch(t, 0, 100), idNameBatch(t, 100, 100), idNameBatch(t, 200, 100))
	files, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("files = %+v, want 1", files)
	}
	f := files[0]
	if want := dir + "part-run12345-w1-00000.parquet"; f.Path != want {
		t.Fatalf("path = %q, want %q", f.Path, want)
	}
	if f.Rows != 300 {
		t.Fatalf("rows = %d, want 300", f.Rows)
	}
	fi, err := os.Stat(f.Path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if f.Bytes != fi.Size() {
		t.Fatalf("bytes = %d, file size %d", f.Bytes, fi.Size())
	}
	c := queryConn(t, p)
	lit := duck.Literal(f.Path)
	if got := scalar(t, c, "SELECT num_rows FROM parquet_file_metadata("+lit+")"); got != 300 {
		t.Fatalf("parquet_file_metadata num_rows = %d", got)
	}
	if got := scalar(t, c, "SELECT sum(id)::BIGINT FROM read_parquet("+lit+")"); got != 299*300/2 {
		t.Fatalf("sum(id) = %d", got)
	}
	// Close is idempotent and returns the same result.
	again, err := w.Close(context.Background())
	if err != nil || len(again) != 1 || again[0] != f {
		t.Fatalf("second Close = %+v, %v", again, err)
	}
}

func TestWriterRotates(t *testing.T) {
	p := testPlane(t)
	dir := t.TempDir() + "/"
	spec := localSpec(dir)
	spec.TargetFileBytes = 1 // rotation threshold 4 bytes: every batch ends a file
	w, err := p.OpenTableWriter(context.Background(), spec)
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	writeBatches(t, w, idNameBatch(t, 0, 10), idNameBatch(t, 10, 20), idNameBatch(t, 30, 30), idNameBatch(t, 60, 5))
	files, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(files) < 3 {
		t.Fatalf("files = %d, want >= 3", len(files))
	}
	var rows int64
	for i, f := range files {
		if want := fmt.Sprintf("%spart-run12345-w1-%05d.parquet", dir, i); f.Path != want {
			t.Fatalf("file %d path = %q, want %q", i, f.Path, want)
		}
		rows += f.Rows
	}
	if rows != 65 {
		t.Fatalf("total rows = %d, want 65", rows)
	}
	c := queryConn(t, p)
	if got := scalar(t, c, "SELECT count(*) FROM read_parquet("+duck.Literal(dir+"*.parquet")+")"); got != 65 {
		t.Fatalf("read back %d rows", got)
	}
}

func TestWriterZeroRows(t *testing.T) {
	p := testPlane(t)
	dir := t.TempDir()
	w, err := p.OpenTableWriter(context.Background(), localSpec(dir+"/"))
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	files, err := w.Close(context.Background())
	if err != nil || len(files) != 0 {
		t.Fatalf("Close = %+v, %v; want no files", files, err)
	}
	if names := listDir(t, dir); len(names) != 0 {
		t.Fatalf("files on disk: %v", names)
	}
}

func TestWriterHostileColumnNames(t *testing.T) {
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

	p := testPlane(t)
	dir := t.TempDir() + "/"
	spec := localSpec(dir)
	spec.Schema = sch
	w, err := p.OpenTableWriter(context.Background(), spec)
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	writeBatches(t, w, rec)
	files, err := w.Close(context.Background())
	if err != nil || len(files) != 1 {
		t.Fatalf("Close = %+v, %v", files, err)
	}
	c := queryConn(t, p)
	rows, err := c.QueryRows(context.Background(), "SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet("+duck.Literal(files[0].Path)+"))")
	if err != nil {
		t.Fatalf("DESCRIBE: %v", err)
	}
	var got []string
	for _, r := range rows {
		got = append(got, r[0].(string))
	}
	if strings.Join(got, "|") != strings.Join(names, "|") {
		t.Fatalf("column names = %q, want %q", got, names)
	}
	for _, n := range names {
		q := "SELECT " + duck.Ident(n) + " FROM read_parquet(" + duck.Literal(files[0].Path) + ")"
		vals, err := c.QueryRows(context.Background(), q)
		if err != nil || len(vals) != 1 || vals[0][0] != "v:"+n {
			t.Fatalf("column %q read back %v, %v", n, vals, err)
		}
	}
}

// settleGoroutines waits for the goroutine count to drop to at most want.
func settleGoroutines(want int) int {
	var n int
	for i := 0; i < 100; i++ {
		runtime.GC()
		if n = runtime.NumGoroutine(); n <= want {
			return n
		}
		time.Sleep(20 * time.Millisecond)
	}
	return n
}

func TestWriterCopyErrorSurfaces(t *testing.T) {
	p := testPlane(t)
	before := runtime.NumGoroutine()
	dir := t.TempDir() + "/missing/sub/" // COPY cannot create the file
	w, err := p.OpenTableWriter(context.Background(), localSpec(dir))
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	start := time.Now()
	var werr error
	for i := 0; i < 20 && werr == nil; i++ {
		rec := idNameBatch(t, i*10, 10)
		werr = w.Write(context.Background(), rec)
		rec.Release()
	}
	_, cerr := w.Close(context.Background())
	if werr == nil && cerr == nil {
		t.Fatal("neither Write nor Close returned the COPY error")
	}
	if cerr == nil {
		t.Fatalf("Close after a failed COPY returned no error (Write err: %v)", werr)
	}
	t.Logf("Write error: %v; Close error: %v", werr, cerr)
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("error surfaced after %v", d)
	}
	if _, err := w.Close(context.Background()); err == nil || err.Error() != cerr.Error() {
		t.Fatalf("second Close = %v, want %v", err, cerr)
	}
	w.Abort()
	if n := settleGoroutines(before); n > before {
		t.Fatalf("goroutines: %d before, %d after", before, n)
	}
}

func TestWriterAbortIdempotent(t *testing.T) {
	p := testPlane(t)
	before := runtime.NumGoroutine()
	dir := t.TempDir() + "/"
	w, err := p.OpenTableWriter(context.Background(), localSpec(dir))
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	writeBatches(t, w, idNameBatch(t, 0, 10))
	w.Abort()
	w.Abort()
	t.Logf("after Abort mid-COPY: %v", listDir(t, dir))
	rec := idNameBatch(t, 10, 10)
	if err := w.Write(context.Background(), rec); err == nil {
		t.Fatal("Write after Abort succeeded")
	}
	rec.Release()
	if files, err := w.Close(context.Background()); err == nil || len(files) != 0 {
		t.Fatalf("Close after Abort = %+v, %v", files, err)
	}
	w.Abort()
	if n := settleGoroutines(before); n > before {
		t.Fatalf("goroutines: %d before, %d after", before, n)
	}

	// Abort after a successful Close is a no-op and keeps the file.
	w2, err := p.OpenTableWriter(context.Background(), localSpec(dir+"x-"))
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	writeBatches(t, w2, idNameBatch(t, 0, 10))
	files, err := w2.Close(context.Background())
	if err != nil || len(files) != 1 {
		t.Fatalf("Close = %+v, %v", files, err)
	}
	w2.Abort()
	if _, err := os.Stat(files[0].Path); err != nil {
		t.Fatalf("file gone after Abort-after-Close: %v", err)
	}
}

func TestWriteCtxDoesNotOwnWriter(t *testing.T) {
	p := testPlane(t)
	w, err := p.OpenTableWriter(context.Background(), localSpec(t.TempDir()+"/"))
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rec := idNameBatch(t, 0, 7)
	if err := w.Write(ctx, rec); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	rec.Release()
	cancel() // the first call's context dies; the COPY it started must not
	time.Sleep(200 * time.Millisecond)
	writeBatches(t, w, idNameBatch(t, 7, 5))
	files, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	var rows int64
	for _, f := range files {
		rows += f.Rows
	}
	if rows != 12 {
		t.Fatalf("rows = %d, want 12", rows)
	}
}

func TestWriterRejectsSchemaMismatch(t *testing.T) {
	p := testPlane(t)
	w, err := p.OpenTableWriter(context.Background(), localSpec(t.TempDir()+"/"))
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	defer w.Abort()
	other := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32}}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), other)
	b.Field(0).(*array.Int32Builder).Append(1)
	rec := b.NewRecordBatch()
	b.Release()
	defer rec.Release()
	if err := w.Write(context.Background(), rec); err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("Write with another schema = %v", err)
	}
}

// fakeCreds is a CredsSource whose Get returns next(call number).
type fakeCreds struct {
	mu          sync.Mutex
	gets        int
	invalidates int
	next        func(n int) catalogwriter.Creds
}

func (f *fakeCreds) Get(context.Context) (catalogwriter.Creds, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	return f.next(f.gets), nil
}

func (f *fakeCreds) Invalidate() {
	f.mu.Lock()
	f.invalidates++
	f.mu.Unlock()
}

func (f *fakeCreds) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gets, f.invalidates
}

func freshS3(n int) catalogwriter.Creds {
	now := time.Now()
	return catalogwriter.S3Creds{
		AccessKeyID:     fmt.Sprintf("AK%d", n),
		SecretAccessKey: fmt.Sprintf("SK'%d", n),
		SessionToken:    fmt.Sprintf("ST%d", n),
		Region:          "local-01",
		Endpoint:        "http://minio:9000",
		Issued:          now,
		Expires:         now.Add(15 * time.Minute),
	}
}

func nearExpiryS3(n int) catalogwriter.Creds {
	now := time.Now()
	c := freshS3(n).(catalogwriter.S3Creds)
	c.Issued = now.Add(-14*time.Minute - 30*time.Second)
	c.Expires = now.Add(30 * time.Second)
	return c
}

// secretRecorder replaces the secret statement executor and records the SQL.
type secretRecorder struct {
	mu   sync.Mutex
	sqls []string
}

func (r *secretRecorder) exec(_ context.Context, sql string) error {
	r.mu.Lock()
	r.sqls = append(r.sqls, sql)
	r.mu.Unlock()
	return nil
}

func (r *secretRecorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.sqls...)
}

func TestCredsRefreshPerFile(t *testing.T) {
	p := testPlane(t)
	rec := &secretRecorder{}
	p.secretExec = rec.exec
	creds := &fakeCreds{next: freshS3}
	dir := t.TempDir() + "/"
	spec := localSpec(dir)
	spec.TargetFileBytes = 1
	spec.Creds = creds
	w, err := p.OpenTableWriter(context.Background(), spec)
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	writeBatches(t, w, idNameBatch(t, 0, 10), idNameBatch(t, 10, 10), idNameBatch(t, 20, 10))
	files, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	gets, inv := creds.counts()
	if gets != len(files) || inv != 0 {
		t.Fatalf("creds Get calls = %d (invalidates %d) for %d files", gets, inv, len(files))
	}
	sqls := rec.all()
	if len(sqls) != len(files) {
		t.Fatalf("secret statements = %d for %d files", len(sqls), len(files))
	}
	for i, sql := range sqls {
		n := i + 1
		for _, want := range []string{
			`CREATE OR REPLACE SECRET "dgw_`,
			"TYPE s3",
			fmt.Sprintf("KEY_ID 'AK%d'", n),
			fmt.Sprintf("SECRET 'SK''%d'", n),
			fmt.Sprintf("SESSION_TOKEN 'ST%d'", n),
			"REGION 'local-01'",
			"ENDPOINT 'minio:9000'",
			"USE_SSL false",
			"URL_STYLE 'path'",
			"SCOPE " + duck.Literal(dir+"part-run12345-w1"),
		} {
			if !strings.Contains(sql, want) {
				t.Errorf("secret %d lacks %q", i, want)
			}
		}
	}
}

func TestCredsGCSSecret(t *testing.T) {
	p := testPlane(t)
	rec := &secretRecorder{}
	p.secretExec = rec.exec
	now := time.Now()
	creds := &fakeCreds{next: func(int) catalogwriter.Creds {
		return catalogwriter.GCSCreds{OAuthToken: "ya29.tok'en", Issued: now, Expires: now.Add(time.Hour)}
	}}
	spec := localSpec(t.TempDir() + "/")
	spec.Creds = creds
	w, err := p.OpenTableWriter(context.Background(), spec)
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	writeBatches(t, w, idNameBatch(t, 0, 3))
	if _, err := w.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	sqls := rec.all()
	if len(sqls) != 1 || !strings.Contains(sqls[0], "TYPE gcs, BEARER_TOKEN 'ya29.tok''en', SCOPE ") {
		t.Fatalf("gcs secret = %q", sqls)
	}
}

func TestCredsNearExpiryRefetched(t *testing.T) {
	p := testPlane(t)
	rec := &secretRecorder{}
	p.secretExec = rec.exec
	creds := &fakeCreds{next: func(n int) catalogwriter.Creds {
		if n == 1 {
			return nearExpiryS3(n)
		}
		return freshS3(n)
	}}
	spec := localSpec(t.TempDir() + "/")
	spec.Creds = creds
	w, err := p.OpenTableWriter(context.Background(), spec)
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	writeBatches(t, w, idNameBatch(t, 0, 10))
	files, err := w.Close(context.Background())
	if err != nil || len(files) != 1 {
		t.Fatalf("Close = %+v, %v", files, err)
	}
	if gets, inv := creds.counts(); gets != 2 || inv != 1 {
		t.Fatalf("Get calls %d, Invalidate calls %d; want 2, 1", gets, inv)
	}
	if sqls := rec.all(); len(sqls) != 1 || !strings.Contains(sqls[0], "KEY_ID 'AK2'") {
		t.Fatalf("secret used %q, want the refetched AK2", sqls)
	}
}

func TestCredsStillNearExpiryFails(t *testing.T) {
	p := testPlane(t)
	rec := &secretRecorder{}
	p.secretExec = rec.exec
	creds := &fakeCreds{next: nearExpiryS3}
	dir := t.TempDir()
	spec := localSpec(dir + "/")
	spec.Creds = creds
	w, err := p.OpenTableWriter(context.Background(), spec)
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	b := idNameBatch(t, 0, 10)
	err = w.Write(context.Background(), b)
	b.Release()
	if err == nil || !strings.Contains(err.Error(), "vended credentials expire too soon") {
		t.Fatalf("Write = %v, want expire-too-soon", err)
	}
	if gets, inv := creds.counts(); gets != 2 || inv != 1 {
		t.Fatalf("Get calls %d, Invalidate calls %d; want 2, 1", gets, inv)
	}
	if sqls := rec.all(); len(sqls) != 0 {
		t.Fatalf("secret created although no COPY may start: %d", len(sqls))
	}
	if _, err := w.Close(context.Background()); err == nil {
		t.Fatal("Close after the failed Write returned no error")
	}
	if names := listDir(t, dir); len(names) != 0 {
		t.Fatalf("files on disk: %v", names)
	}
}

func TestCredsRequiredForRemotePrefix(t *testing.T) {
	p := testPlane(t)
	spec := localSpec("s3://bucket/t/data/")
	if _, err := p.OpenTableWriter(context.Background(), spec); err == nil {
		t.Fatal("remote prefix without creds accepted")
	}
}

func TestParseS3Endpoint(t *testing.T) {
	for _, tc := range []struct {
		raw, host string
		ssl       bool
		style     string
		err       bool
	}{
		{raw: "minio:9000", host: "minio:9000", ssl: true, style: "path"},
		{raw: "http://minio:9000", host: "minio:9000", ssl: false, style: "path"},
		{raw: "https://minio:9000/", host: "minio:9000", ssl: true, style: "path"},
		{raw: "https://s3.eu-west-1.amazonaws.com", host: "s3.eu-west-1.amazonaws.com", ssl: true, style: "vhost"},
		{raw: "https://amazonaws.com", host: "amazonaws.com", ssl: true, style: "vhost"},
		{raw: "https://notamazonaws.com", host: "notamazonaws.com", ssl: true, style: "path"},
		{raw: "https://x.amazonaws.com.evil.io", host: "x.amazonaws.com.evil.io", ssl: true, style: "path"},
		{raw: "ftp://x", err: true},
		{raw: "", err: true},
		{raw: "http://", err: true},
		{raw: "http://user:pw@minio:9000", err: true},
		{raw: "http://minio:9000/path", err: true},
	} {
		host, ssl, style, err := ParseS3Endpoint(tc.raw)
		if tc.err {
			if err == nil {
				t.Errorf("%q: no error", tc.raw)
			}
			continue
		}
		if err != nil || host != tc.host || ssl != tc.ssl || style != tc.style {
			t.Errorf("%q = (%q, %v, %q, %v), want (%q, %v, %q)", tc.raw, host, ssl, style, err, tc.host, tc.ssl, tc.style)
		}
	}
}

func TestPlaneInterruptAbortsWriters(t *testing.T) {
	p := testPlane(t)
	w, err := p.OpenTableWriter(context.Background(), localSpec(t.TempDir()+"/"))
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	writeBatches(t, w, idNameBatch(t, 0, 10))
	p.Interrupt()
	rec := idNameBatch(t, 10, 10)
	defer rec.Release()
	if err := w.Write(context.Background(), rec); err == nil {
		t.Fatal("Write after Interrupt succeeded")
	}
	if _, err := w.Close(context.Background()); err == nil {
		t.Fatal("Close after Interrupt succeeded")
	}
}

func TestReadsNotImplemented(t *testing.T) {
	p := testPlane(t)
	if _, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"}); err == nil {
		t.Fatal("OpenTableReader succeeded")
	}
}

func TestSettingsFromEnv(t *testing.T) {
	dir := t.TempDir()
	memMax := filepath.Join(dir, "memory.max")
	old := cgroupMemoryMax
	cgroupMemoryMax = memMax
	defer func() { cgroupMemoryMax = old }()

	for _, k := range []string{envThreads, envMemoryLimit, envTempDir, envExtensionDir} {
		t.Setenv(k, "")
	}
	s, err := SettingsFromEnv()
	if err != nil {
		t.Fatalf("SettingsFromEnv: %v", err)
	}
	if s.Threads != 1 || s.MemoryLimit != "512MiB" || s.TempDir != "/scratch" || s.MaxTempDirSize != "1GiB" ||
		s.ExtensionDir == "" || s.SecretDir == "" || s.HomeDir == "" {
		t.Fatalf("defaults = %+v", s)
	}

	if err := os.WriteFile(memMax, []byte("1073741824\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if s, _ = SettingsFromEnv(); s.MemoryLimit != "524288KiB" {
		t.Fatalf("cgroup-derived memory limit = %q", s.MemoryLimit)
	}
	if err := os.WriteFile(memMax, []byte("max\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if s, _ = SettingsFromEnv(); s.MemoryLimit != "512MiB" {
		t.Fatalf("unlimited cgroup memory limit = %q", s.MemoryLimit)
	}

	t.Setenv(envThreads, "4")
	t.Setenv(envMemoryLimit, "2GiB")
	t.Setenv(envTempDir, "/tmp/x")
	t.Setenv(envExtensionDir, "/ext")
	if s, err = SettingsFromEnv(); err != nil || s.Threads != 4 || s.MemoryLimit != "2GiB" || s.TempDir != "/tmp/x" || s.ExtensionDir != "/ext" {
		t.Fatalf("overrides = %+v, %v", s, err)
	}
	t.Setenv(envThreads, "zero")
	if _, err := SettingsFromEnv(); err == nil {
		t.Fatal("invalid thread count accepted")
	}
}

func TestCatalogAttachRedactsToken(t *testing.T) {
	c := CatalogAttach{URL: "http://lk", ProjectID: "p", Warehouse: "w", Token: "secret-jwt"}
	for _, s := range []string{fmt.Sprint(c), fmt.Sprintf("%v", c), fmt.Sprintf("%+v", c)} {
		if strings.Contains(s, "secret-jwt") {
			t.Fatalf("token leaked: %s", s)
		}
	}
}
