package goplane

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	"github.com/datuplet/datuplet/pkg/datagateway"
)

// Unit fixture: a Hadoop (local filesystem) iceberg-go catalog per test,
// data files written by the goplane writer (or pqarrow directly where a test
// needs a file the writer never produces) and committed with AddFiles.

// readSchema is the Iceberg twin of testSchema (optional id long, name string).
var readSchema = iceberg.NewSchema(0,
	iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
	iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
)

// testTables serves the catalog's tables with fixed credentials.
type testTables struct {
	cat   *hadoop.Catalog
	creds datagateway.CredsSource
}

func (tt *testTables) LoadTable(ctx context.Context, ns, tbl string) (*table.Table, datagateway.CredsSource, error) {
	t, err := tt.cat.LoadTable(ctx, catalog.ToIdentifier(ns, tbl))
	return t, tt.creds, err
}

type tfx struct {
	t      *testing.T
	cat    *hadoop.Catalog
	ident  table.Identifier
	p      *Plane
	tables *testTables
	files  int
}

func newTable(t *testing.T, sc *iceberg.Schema, props iceberg.Properties) *tfx {
	t.Helper()
	ctx := context.Background()
	cat, err := hadoop.NewCatalog("unit", t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateNamespace(ctx, catalog.ToIdentifier("ns"), nil); err != nil {
		t.Fatal(err)
	}
	ident := catalog.ToIdentifier("ns", "tbl")
	if _, err := cat.CreateTable(ctx, ident, sc, catalog.WithProperties(props)); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	tables := &testTables{cat: cat}
	p := New(Config{Tables: tables})
	t.Cleanup(func() { _ = p.Close() })
	return &tfx{t: t, cat: cat, ident: ident, p: p, tables: tables}
}

// morProps is a format-version table with merge-on-read deletes.
func morProps(version string) iceberg.Properties {
	return iceberg.Properties{table.PropertyFormatVersion: version, table.WriteDeleteModeKey: table.WriteModeMergeOnRead}
}

// morTargetedProps is morProps("2") with full column metrics. iceberg-go
// matches an unpartitioned position-delete file to data files by its
// file_path bounds, which the default truncate(16) metrics widen to cover
// every file under one prefix (every task then carries the delete file);
// full metrics keep a delete on the file it targets.
func morTargetedProps() iceberg.Properties {
	props := morProps("2")
	props["write.metadata.metrics.default"] = "full"
	return props
}

func (f *tfx) load() *table.Table {
	f.t.Helper()
	tb, err := f.cat.LoadTable(context.Background(), f.ident)
	if err != nil {
		f.t.Fatal(err)
	}
	return tb
}

func (f *tfx) dataPrefix() string { return strings.TrimRight(f.load().Location(), "/") + "/data/" }

// writeFile writes ids [start, start+n) of testSchema as one file with the
// goplane writer.
func (f *tfx) writeFile(start, n int, rowGroupRows int64) string {
	f.t.Helper()
	f.files++
	w := openWriter(f.t, f.p, datagateway.WriterSpec{
		DataPrefix: f.dataPrefix(), FilePrefix: fmt.Sprintf("part-unit-w%d", f.files), RowGroupRows: rowGroupRows,
	})
	write(f.t, w, batch(testSchema, start, n))
	files, err := w.Close(context.Background())
	if err != nil || len(files) != 1 {
		f.t.Fatalf("writer Close = %+v, %v", files, err)
	}
	return files[0].Path
}

// writeArrow writes rec with pqarrow as-is (field ids and types preserved).
func (f *tfx) writeArrow(rec arrow.RecordBatch) string {
	f.t.Helper()
	f.files++
	path := fmt.Sprintf("%spart-raw-%d.parquet", f.dataPrefix(), f.files)
	if err := os.MkdirAll(localPath(f.dataPrefix()), 0o755); err != nil {
		f.t.Fatal(err)
	}
	out, err := os.Create(localPath(path))
	if err != nil {
		f.t.Fatal(err)
	}
	fw, err := pqarrow.NewFileWriter(rec.Schema(), out, parquet.NewWriterProperties(), pqarrow.NewArrowWriterProperties())
	if err != nil {
		f.t.Fatal(err)
	}
	if err := fw.Write(rec); err != nil {
		f.t.Fatal(err)
	}
	if err := fw.Close(); err != nil {
		f.t.Fatal(err)
	}
	return path
}

func (f *tfx) commit(fn func(*table.Transaction) error) {
	f.t.Helper()
	txn := f.load().NewTransaction()
	if err := fn(txn); err != nil {
		f.t.Fatalf("stage: %v", err)
	}
	if _, err := txn.Commit(context.Background()); err != nil {
		f.t.Fatalf("commit: %v", err)
	}
}

func (f *tfx) addFiles(paths ...string) {
	f.t.Helper()
	f.commit(func(txn *table.Transaction) error { return txn.AddFiles(context.Background(), paths, nil, false) })
}

func (f *tfx) deleteWhere(expr iceberg.BooleanExpression) {
	f.t.Helper()
	f.commit(func(txn *table.Transaction) error { return txn.Delete(context.Background(), expr, nil) })
}

func (f *tfx) plan() []table.FileScanTask {
	f.t.Helper()
	tasks, err := f.load().Scan().PlanFiles(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	return tasks
}

func (f *tfx) taskFor(path string) table.FileScanTask {
	f.t.Helper()
	for _, t := range f.plan() {
		if t.File.FilePath() == path {
			return t
		}
	}
	f.t.Fatalf("no planned task for %s", path)
	return table.FileScanTask{}
}

func (f *tfx) open(raw bool) *reader {
	f.t.Helper()
	tr, err := f.p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "ns", Table: "tbl", RawParquet: raw})
	if err != nil {
		f.t.Fatalf("OpenTableReader: %v", err)
	}
	f.t.Cleanup(func() { _ = tr.Close() })
	return tr.(*reader)
}

type partSummary struct {
	kind string // "raw" | "decoded"
	path string // raw only
	rows int64
	ids  []int64
	cols [][]any // per column, the values (decoded and raw)
}

// readParts drains r, checking that every part carries exactly r.Schema().
func readParts(t *testing.T, r datagateway.TableReader) []partSummary {
	t.Helper()
	ctx := context.Background()
	var out []partSummary
	for {
		part, err := r.NextPart(ctx)
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("NextPart: %v", err)
		}
		switch {
		case part.Raw != nil:
			rc, err := part.Raw.Open(ctx)
			if err != nil {
				t.Fatalf("raw Open: %v", err)
			}
			b, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				t.Fatal(err)
			}
			if int64(len(b)) != part.Raw.Size {
				t.Fatalf("raw %s: %d bytes, Size %d", part.Raw.Path, len(b), part.Raw.Size)
			}
			sch, recs := readParquetBytes(t, b)
			if !sameNamesAndTypes(sch, r.Schema()) {
				t.Fatalf("raw part schema %s != table schema %s", sch, r.Schema())
			}
			s := summarize(t, "raw", recs)
			s.path = part.Raw.Path
			if s.rows != part.Raw.Rows {
				t.Fatalf("raw %s: %d rows, manifest %d", s.path, s.rows, part.Raw.Rows)
			}
			out = append(out, s)
		case part.Records != nil:
			if !part.Records.Schema().Equal(r.Schema()) {
				t.Fatalf("records schema %s != %s", part.Records.Schema(), r.Schema())
			}
			var recs []arrow.RecordBatch
			for {
				rec, err := part.Records.Next(ctx)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("Records.Next: %v", err)
				}
				if !rec.Schema().Equal(r.Schema()) {
					t.Fatalf("batch schema %s != %s", rec.Schema(), r.Schema())
				}
				recs = append(recs, rec)
			}
			_ = part.Records.Close()
			out = append(out, summarize(t, "decoded", recs))
		default:
			t.Fatal("empty part")
		}
	}
}

func readParquetBytes(t *testing.T, b []byte) (*arrow.Schema, []arrow.RecordBatch) {
	t.Helper()
	pr, err := file.NewParquetReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	fr, err := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{BatchSize: 1 << 20}, memory.NewGoAllocator())
	if err != nil {
		t.Fatal(err)
	}
	rr, err := fr.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rr.Release()
	var recs []arrow.RecordBatch
	for rr.Next() {
		rec := rr.RecordBatch()
		rec.Retain()
		recs = append(recs, rec)
	}
	return rr.Schema(), recs
}

func summarize(t *testing.T, kind string, recs []arrow.RecordBatch) partSummary {
	t.Helper()
	s := partSummary{kind: kind}
	for _, rec := range recs {
		s.rows += rec.NumRows()
		if s.cols == nil {
			s.cols = make([][]any, rec.NumCols())
		}
		for c := range int(rec.NumCols()) {
			col := rec.Column(c)
			for i := range col.Len() {
				s.cols[c] = append(s.cols[c], col.GetOneForMarshal(i))
			}
		}
		if a, ok := rec.Column(0).(*array.Int64); ok {
			for i := range a.Len() {
				s.ids = append(s.ids, a.Value(i))
			}
		}
		rec.Release()
	}
	return s
}

func kinds(parts []partSummary) string {
	var k []string
	for _, p := range parts {
		k = append(k, p.kind)
	}
	return strings.Join(k, ",")
}

func allIDs(parts []partSummary) []int64 {
	var ids []int64
	for _, p := range parts {
		ids = append(ids, p.ids...)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func idRange(from, to int64, skip ...int64) []int64 {
	var out []int64
	for i := from; i < to; i++ {
		skipped := false
		for _, s := range skip {
			skipped = skipped || s == i
		}
		if !skipped {
			out = append(out, i)
		}
	}
	return out
}

func equalIDs(a, b []int64) bool {
	return fmt.Sprint(a) == fmt.Sprint(b)
}

// splitTable has one 2000-row file of 20 row groups and a split target small
// enough that PlanFiles returns it as several byte-range tasks.
func splitTable(t *testing.T) *tfx {
	t.Helper()
	props := morProps("2")
	props[table.ReadSplitTargetSizeKey] = "2048"
	f := newTable(t, readSchema, props)
	path := f.writeFile(0, 2000, 100)
	f.addFiles(path)
	tasks := f.plan()
	if len(tasks) < 2 {
		t.Fatalf("PlanFiles returned %d tasks, want >= 2 splits", len(tasks))
	}
	for _, task := range tasks {
		if task.File.FilePath() != path || wholeFileTask(task) {
			t.Fatalf("unexpected task %s [%d,+%d)", task.File.FilePath(), task.Start, task.Length)
		}
	}
	return f
}

func TestSplitTasksCoalescedAndRowsOnce(t *testing.T) {
	f := splitTable(t)
	t.Logf("%d splits", len(f.plan()))
	parts := readParts(t, f.open(true))
	if kinds(parts) != "raw" || parts[0].rows != 2000 || !equalIDs(allIDs(parts), idRange(0, 2000)) {
		t.Fatalf("parts %s rows %d", kinds(parts), parts[0].rows)
	}

	f.deleteWhere(iceberg.EqualTo(iceberg.Reference("id"), int64(1500)))
	tasks := f.plan()
	if len(tasks) < 2 || len(tasks[0].DeleteFiles) == 0 {
		t.Fatalf("after delete: %d tasks, deletes %d", len(tasks), len(tasks[0].DeleteFiles))
	}
	r := f.open(true)
	if len(r.tasks) != 1 || !wholeFileTask(r.tasks[0]) {
		t.Fatalf("splits not coalesced: %d tasks", len(r.tasks))
	}
	parts = readParts(t, r)
	if kinds(parts) != "decoded" || !equalIDs(allIDs(parts), idRange(0, 2000, 1500)) {
		t.Fatalf("parts %s, %d ids", kinds(parts), len(allIDs(parts)))
	}
}

func TestPartialTaskDecodedWithRange(t *testing.T) {
	f := splitTable(t)
	splits := f.plan()
	var all []int64
	for i, split := range splits {
		r := f.open(true)
		r.tasks, r.raw = coalesceSplits([]table.FileScanTask{split}), make([]int8, 1)
		parts := readParts(t, r)
		if kinds(parts) != "decoded" || parts[0].rows == 0 || parts[0].rows >= 2000 {
			t.Fatalf("split %d [%d,+%d): parts %s rows %d", i, split.Start, split.Length, kinds(parts), parts[0].rows)
		}
		all = append(all, parts[0].ids...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	if !equalIDs(all, idRange(0, 2000)) {
		t.Fatalf("splits read separately: %d ids, want each of 0..1999 once", len(all))
	}
}

func TestReaderMixedPartsSchemaEqual(t *testing.T) {
	f := newTable(t, readSchema, morTargetedProps())
	p1, p2, p3, p4 := f.writeFile(0, 10, 0), f.writeFile(10, 10, 0), f.writeFile(20, 10, 0), f.writeFile(30, 10, 0)
	f.addFiles(p1, p2, p3, p4)
	f.deleteWhere(iceberg.EqualTo(iceberg.Reference("id"), int64(12)))
	f.deleteWhere(iceberg.EqualTo(iceberg.Reference("id"), int64(25)))
	deleted := map[string]bool{p2: true, p3: true}

	r := f.open(true)
	var want []string
	for i, task := range r.tasks {
		k := "raw"
		if deleted[task.File.FilePath()] {
			if len(task.DeleteFiles) == 0 {
				t.Fatalf("task %s has no delete files", task.File.FilePath())
			}
			k = "decoded"
		}
		if i > 0 && k == "decoded" && want[len(want)-1] == "decoded" {
			continue // consecutive decoded tasks form one part
		}
		want = append(want, k)
	}
	parts := readParts(t, r)
	if got := kinds(parts); got != strings.Join(want, ",") {
		t.Fatalf("parts %s, want %s (task order)", got, strings.Join(want, ","))
	}
	if !equalIDs(allIDs(parts), idRange(0, 40, 12, 25)) {
		t.Fatalf("ids %v", allIDs(parts))
	}
}

func TestReaderEmptyTable(t *testing.T) {
	f := newTable(t, readSchema, morProps("2"))
	r := f.open(true)
	if r.Schema().NumFields() != 2 {
		t.Fatalf("schema %s", r.Schema())
	}
	if _, err := r.NextPart(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("NextPart = %v, want io.EOF", err)
	}
}

func TestReaderZeroFiles(t *testing.T) {
	f := newTable(t, readSchema, morProps("2"))
	f.addFiles(f.writeFile(0, 5, 0))
	f.deleteWhere(iceberg.AlwaysTrue{})
	if f.load().CurrentSnapshot() == nil || len(f.plan()) != 0 {
		t.Fatalf("want a snapshot with zero files, got %d tasks", len(f.plan()))
	}
	for _, raw := range []bool{true, false} {
		if _, err := f.open(raw).NextPart(context.Background()); !errors.Is(err, io.EOF) {
			t.Fatalf("raw=%v NextPart = %v, want io.EOF", raw, err)
		}
	}
}

// cachedCreds returns the same credentials until Invalidate moves to the
// next ones.
type cachedCreds struct {
	mu   sync.Mutex
	keys []string
	cur  int
	gets int
}

func (c *cachedCreds) Get(context.Context) (catalogwriter.Creds, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets++
	now := time.Now()
	return s3creds(c.keys[min(c.cur, len(c.keys)-1)], now, now.Add(time.Hour)), nil
}

func (c *cachedCreds) Invalidate() {
	c.mu.Lock()
	c.cur++
	c.mu.Unlock()
}

type ioCall struct{ key, location string }

// recordIO wraps the plane's IO loader and records the access key and
// location of every IO built.
func recordIO(p *Plane) func() []ioCall {
	var mu sync.Mutex
	var calls []ioCall
	p.loadIO = func(ctx context.Context, props map[string]string, location string) (iceio.IO, error) {
		mu.Lock()
		calls = append(calls, ioCall{props[iceio.S3AccessKeyID], location})
		mu.Unlock()
		return iceio.LoadFS(ctx, props, location)
	}
	return func() []ioCall {
		mu.Lock()
		defer mu.Unlock()
		return append([]ioCall(nil), calls...)
	}
}

func TestRawOpenUsesCurrentCreds(t *testing.T) {
	f := newTable(t, readSchema, morProps("2"))
	path := f.writeFile(0, 5, 0)
	f.addFiles(path)
	creds := &cachedCreds{keys: []string{"k1", "k2"}}
	f.tables.creds = creds
	calls := recordIO(f.p)
	r := f.open(true)
	part, err := r.NextPart(context.Background())
	if err != nil || part.Raw == nil {
		t.Fatalf("NextPart = %+v, %v", part, err)
	}
	creds.Invalidate()
	rc, err := part.Raw.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
	got := calls()
	last := got[len(got)-1]
	if last.location != path || last.key != "k2" {
		t.Fatalf("raw open IO = %+v, want key k2 for %s (calls %+v)", last, path, got)
	}
	for _, c := range got[:len(got)-1] {
		if c.key != "k1" {
			t.Fatalf("IO before Invalidate used %q", c.key)
		}
	}
}

func TestDecodedRunRenewsCredsPerFile(t *testing.T) {
	f := newTable(t, readSchema, morProps("2"))
	p1, p2 := f.writeFile(0, 10, 0), f.writeFile(10, 10, 0)
	f.addFiles(p1, p2)
	creds := &cachedCreds{keys: []string{"k1", "k2"}}
	f.tables.creds = creds
	calls := recordIO(f.p)
	r := f.open(false)
	location := r.location
	planned := len(calls())
	part, err := r.NextPart(context.Background())
	if err != nil || part.Records == nil {
		t.Fatalf("NextPart = %+v, %v", part, err)
	}
	defer part.Records.Close()
	ctx := context.Background()
	rec, err := part.Records.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	n := rec.NumRows()
	rec.Release()
	if n != 10 {
		t.Fatalf("first batch %d rows, want one whole file (10)", n)
	}
	creds.Invalidate()
	for {
		rec, err := part.Records.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		n += rec.NumRows()
		rec.Release()
	}
	if n != 20 {
		t.Fatalf("%d rows, want 20", n)
	}
	var reads []string
	for _, c := range calls()[planned:] {
		if c.location != location {
			t.Fatalf("unexpected IO %+v", c)
		}
		reads = append(reads, c.key)
	}
	if strings.Join(reads, ",") != "k1,k2" {
		t.Fatalf("ReadTasks IO keys %v, want one per file: k1 then k2", reads)
	}
}

func TestReaderCloseClosesParts(t *testing.T) {
	f := newTable(t, readSchema, morTargetedProps())
	clean, deleted := f.writeFile(0, 10, 0), f.writeFile(10, 10, 0)
	f.addFiles(clean, deleted)
	f.deleteWhere(iceberg.EqualTo(iceberg.Reference("id"), int64(15)))
	r := f.open(true)
	var raw *datagateway.RawFile
	var recs datagateway.RecordStream
	for raw == nil || recs == nil {
		part, err := r.NextPart(context.Background())
		if err != nil {
			t.Fatalf("NextPart: %v", err)
		}
		if part.Raw != nil {
			raw = part.Raw
		} else {
			recs = part.Records
		}
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if rec, err := recs.Next(context.Background()); err == nil {
		rec.Release()
		t.Fatal("Records.Next after reader Close returned a batch")
	}
	if rc, err := raw.Open(context.Background()); err == nil {
		rc.Close()
		t.Fatal("RawFile.Open after reader Close succeeded")
	}
	if _, err := r.NextPart(context.Background()); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("NextPart after Close = %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestReaderCtxDoesNotOwnReader(t *testing.T) {
	f := newTable(t, readSchema, morProps("2"))
	f.addFiles(f.writeFile(0, 10, 0))
	ctx, cancel := context.WithCancel(context.Background())
	tr, err := f.p.OpenTableReader(ctx, datagateway.ReaderSpec{Namespace: "ns", Table: "tbl"})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	cancel()
	parts := readParts(t, tr)
	if !equalIDs(allIDs(parts), idRange(0, 10)) {
		t.Fatalf("ids %v", allIDs(parts))
	}
}

func TestPlaneCloseAndInterruptStopReaders(t *testing.T) {
	f := newTable(t, readSchema, morProps("2"))
	f.addFiles(f.writeFile(0, 10, 0))
	r := f.open(false)
	part, err := r.NextPart(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f.p.Interrupt()
	if rec, err := part.Records.Next(context.Background()); err == nil {
		rec.Release()
		t.Fatal("read continued after Interrupt")
	}
	if err := f.p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "ns", Table: "tbl"}); err == nil {
		t.Fatal("OpenTableReader accepted after plane Close")
	}
}

func TestOpenTableReaderWithoutTables(t *testing.T) {
	p := New(Config{})
	defer p.Close()
	if _, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"}); err == nil {
		t.Fatal("reader opened without a table loader")
	}
	dp, err := Factory(context.Background(), datagateway.BootInfo{})
	if err != nil || dp == nil {
		t.Fatalf("Factory = %v, %v", dp, err)
	}
	_ = dp.Close()
	dp, err = Factory(context.Background(), datagateway.BootInfo{LakekeeperURL: "http://127.0.0.1:1/catalog", Warehouse: "w"})
	if err != nil || dp.(*Plane).cfg.Tables == nil {
		t.Fatalf("Factory with Lakekeeper = %v, %v", dp, err)
	}
	_ = dp.Close()
}

func TestCoalesceSplits(t *testing.T) {
	df := func(path string, size int64) iceberg.DataFile {
		b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData, path,
			iceberg.ParquetFile, nil, nil, nil, 1, size)
		if err != nil {
			t.Fatal(err)
		}
		return b.Build()
	}
	a, b := df("a", 100), df("b", 50)
	task := func(f iceberg.DataFile, start, length int64) table.FileScanTask {
		return table.FileScanTask{File: f, Start: start, Length: length}
	}
	got := coalesceSplits([]table.FileScanTask{
		task(a, 0, 40), task(a, 40, 60), // whole a
		task(b, 0, 50),                  // whole b
		task(a, 0, 40), task(a, 50, 50), // gap: not coalesced
		task(b, 10, 40), // partial
	})
	var desc []string
	for _, g := range got {
		desc = append(desc, fmt.Sprintf("%s[%d,+%d)", g.File.FilePath(), g.Start, g.Length))
	}
	want := "a[0,+100) b[0,+50) a[0,+40) a[50,+50) b[10,+40)"
	if strings.Join(desc, " ") != want {
		t.Fatalf("coalesced %v, want %s", desc, want)
	}
}

// countingIO wraps the plane's IO loader: it counts files opened and closed
// (a file closed twice counts twice) and records any Read that overlaps a
// Close of the same file, or starts after it.
type countingIO struct {
	mu             sync.Mutex
	opened, closed int
	violations     []string
	closes         chan struct{} // one value per file Close (non-blocking send)
}

type countingFS struct {
	iceio.IO
	c *countingIO
}

type countingFile struct {
	iceio.File
	c      *countingIO
	inRead atomic.Int32
	shut   atomic.Bool
}

func (fs countingFS) Open(name string) (iceio.File, error) {
	f, err := fs.IO.Open(name)
	if err != nil {
		return nil, err
	}
	fs.c.mu.Lock()
	fs.c.opened++
	fs.c.mu.Unlock()
	return &countingFile{File: f, c: fs.c}, nil
}

func (f *countingFile) violation(v string) {
	f.c.mu.Lock()
	f.c.violations = append(f.c.violations, v)
	f.c.mu.Unlock()
}

func (f *countingFile) Read(p []byte) (int, error) {
	if f.inRead.Add(1) != 1 {
		f.violation("concurrent Reads")
	}
	defer f.inRead.Add(-1)
	if f.shut.Load() {
		f.violation("Read after Close")
	}
	time.Sleep(20 * time.Microsecond) // widen the window a racing Close could hit
	return f.File.Read(p)
}

func (f *countingFile) Close() error {
	if f.inRead.Load() != 0 {
		f.violation("Close during Read")
	}
	f.shut.Store(true)
	err := f.File.Close()
	f.c.mu.Lock()
	f.c.closed++
	f.c.mu.Unlock()
	select {
	case f.c.closes <- struct{}{}:
	default:
	}
	return err
}

func (c *countingIO) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.opened, c.closed
}

func (c *countingIO) problems() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.violations...)
}

func countIO(p *Plane) *countingIO {
	c := &countingIO{closes: make(chan struct{}, 64)}
	p.loadIO = func(ctx context.Context, props map[string]string, location string) (iceio.IO, error) {
		fsys, err := iceio.LoadFS(ctx, props, location)
		if err != nil {
			return nil, err
		}
		return countingFS{IO: fsys, c: c}, nil
	}
	return c
}

// rawPart opens a reader over one clean file and returns its raw part.
func rawPart(t *testing.T, f *tfx) (*reader, *datagateway.RawFile) {
	t.Helper()
	r := f.open(true)
	part, err := r.NextPart(context.Background())
	if err != nil || part.Raw == nil {
		t.Fatalf("NextPart = %+v, %v", part, err)
	}
	return r, part.Raw
}

// openBody opens raw and reads a few bytes from it.
func openBody(t *testing.T, raw *datagateway.RawFile) io.ReadCloser {
	t.Helper()
	body, err := raw.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(body, make([]byte, 8)); err != nil {
		t.Fatalf("first read: %v", err)
	}
	return body
}

// assertBodyClosed checks that every file opened is closed, the body no
// longer reads, and its Close is a no-op.
func assertBodyClosed(t *testing.T, c *countingIO, body io.ReadCloser) {
	t.Helper()
	if o, cl := c.counts(); o != cl {
		t.Fatalf("files opened %d, closed %d", o, cl)
	}
	if _, err := body.Read(make([]byte, 8)); err == nil {
		t.Fatal("Read on a raw body succeeded after the reader ended")
	}
	if err := body.Close(); err != nil {
		t.Fatalf("body Close after reader end: %v", err)
	}
	if err := body.Close(); err != nil {
		t.Fatalf("second body Close: %v", err)
	}
	if o, cl := c.counts(); o != cl {
		t.Fatalf("after body Close: files opened %d, closed %d", o, cl)
	}
	if p := c.problems(); len(p) != 0 {
		t.Fatalf("Read/Close violations: %v", p)
	}
}

// Reader Close and plane Close are synchronous: when they return, every raw
// body is closed (no waiting in the test).
func TestReaderCloseClosesOpenRawBodies(t *testing.T) {
	for name, end := range map[string]func(*tfx, *reader){
		"reader Close": func(_ *tfx, r *reader) { _ = r.Close() },
		"plane Close":  func(f *tfx, _ *reader) { _ = f.p.Close() },
	} {
		t.Run(name, func(t *testing.T) {
			f := newTable(t, readSchema, morProps("2"))
			f.addFiles(f.writeFile(0, 100, 0))
			c := countIO(f.p)
			r, raw := rawPart(t, f)
			body := openBody(t, raw)
			end(f, r)
			assertBodyClosed(t, c, body)
			if _, err := raw.Open(context.Background()); err == nil {
				t.Fatal("Open after the reader ended succeeded")
			}
		})
	}
}

// Interrupt closes bodies asynchronously (it returns at once by contract);
// the test waits for the file's Close event.
func TestInterruptClosesOpenRawBodies(t *testing.T) {
	f := newTable(t, readSchema, morProps("2"))
	f.addFiles(f.writeFile(0, 100, 0))
	c := countIO(f.p)
	_, raw := rawPart(t, f)
	body := openBody(t, raw)
	for len(c.closes) > 0 { // footer reads' closes
		<-c.closes
	}
	f.p.Interrupt()
	select {
	case <-c.closes:
	case <-time.After(10 * time.Second):
		t.Fatal("Interrupt did not close the open raw body")
	}
	assertBodyClosed(t, c, body)
}

// The asynchronous close path snapshots the bodies and stalls before closing
// them; reader Close, plane Close and a body's own Close must still return
// only after the file is closed.
func TestCloseWaitsForStalledAsyncBodyClose(t *testing.T) {
	for name, end := range map[string]func(*tfx, *reader, io.ReadCloser){
		"reader Close": func(_ *tfx, r *reader, _ io.ReadCloser) { _ = r.Close() },
		"plane Close":  func(f *tfx, _ *reader, _ io.ReadCloser) { _ = f.p.Close() },
		"body Close":   func(_ *tfx, _ *reader, b io.ReadCloser) { _ = b.Close() },
	} {
		t.Run(name, func(t *testing.T) {
			f := newTable(t, readSchema, morProps("2"))
			f.addFiles(f.writeFile(0, 100, 0))
			c := countIO(f.p)
			r, raw := rawPart(t, f)
			paused, release := make(chan struct{}), make(chan struct{})
			r.asyncPause = func() {
				close(paused)
				<-release
			}
			body := openBody(t, raw)
			f.p.Interrupt() // starts the asynchronous path
			<-paused        // it holds its snapshot and has closed nothing
			end(f, r, body)
			if o, cl := c.counts(); o != cl {
				close(release)
				t.Fatalf("%s returned with a body open: files opened %d, closed %d", name, o, cl)
			}
			close(release)
			r.closeBodies(false)
			assertBodyClosed(t, c, body)
		})
	}
}

func TestRawBodyCloseUntracksAndIsIdempotent(t *testing.T) {
	f := newTable(t, readSchema, morProps("2"))
	f.addFiles(f.writeFile(0, 10, 0))
	c := countIO(f.p)
	r, raw := rawPart(t, f)
	body, err := raw.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if err := body.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	r.mu.Lock()
	tracked := len(r.bodies)
	r.mu.Unlock()
	if tracked != 0 {
		t.Fatalf("%d bodies still tracked", tracked)
	}
	_ = r.Close()
	if o, cl := c.counts(); o != cl {
		t.Fatalf("files opened %d, closed %d (a file closed twice or leaked)", o, cl)
	}
}

func TestRawOpenRacesReaderClose(t *testing.T) {
	for i := 0; i < 20; i++ {
		f := newTable(t, readSchema, morProps("2"))
		f.addFiles(f.writeFile(0, 10, 0))
		c := countIO(f.p)
		r, raw := rawPart(t, f)
		var wg sync.WaitGroup
		bodies := make(chan io.ReadCloser, 8)
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if b, err := raw.Open(context.Background()); err == nil {
					bodies <- b
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.Close()
		}()
		wg.Wait()
		close(bodies)
		buf := make([]byte, 1)
		for b := range bodies {
			if _, err := b.Read(buf); err == nil {
				t.Fatal("a body opened before Close is still readable after it")
			}
			_ = b.Close()
		}
		if o, cl := c.counts(); o != cl {
			t.Fatalf("iteration %d: files opened %d, closed %d", i, o, cl)
		}
		_ = f.p.Close()
	}
}

// Concurrent Read loops on one body overlap the reader ending; the
// underlying Read and Close of the file must never overlap.
func TestRawBodyReadsRaceShutdown(t *testing.T) {
	ends := map[string]func(*tfx, *reader, io.ReadCloser){
		"reader Close": func(_ *tfx, r *reader, _ io.ReadCloser) { _ = r.Close() },
		"Interrupt":    func(f *tfx, _ *reader, _ io.ReadCloser) { f.p.Interrupt() },
		"plane Close":  func(f *tfx, _ *reader, _ io.ReadCloser) { _ = f.p.Close() },
		"body Close":   func(_ *tfx, _ *reader, b io.ReadCloser) { _ = b.Close() },
	}
	for name, end := range ends {
		t.Run(name, func(t *testing.T) {
			for i := 0; i < 10; i++ {
				f := newTable(t, readSchema, morProps("2"))
				f.addFiles(f.writeFile(0, 2000, 0))
				c := countIO(f.p)
				r, raw := rawPart(t, f)
				body := openBody(t, raw)
				var wg sync.WaitGroup
				started := make(chan struct{}, 4)
				for range 4 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						buf := make([]byte, 16)
						started <- struct{}{}
						for {
							if _, err := body.Read(buf); errors.Is(err, errReaderClosed) {
								return
							}
						}
					}()
				}
				for range 4 {
					<-started
				}
				end(f, r, body)
				r.closeBodies(false) // Interrupt is asynchronous; this waits for it
				wg.Wait()
				if p := c.problems(); len(p) != 0 {
					t.Fatalf("iteration %d: %v", i, p)
				}
				if o, cl := c.counts(); o != cl {
					t.Fatalf("iteration %d: files opened %d, closed %d", i, o, cl)
				}
			}
		})
	}
}
