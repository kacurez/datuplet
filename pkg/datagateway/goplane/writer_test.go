package goplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"gocloud.dev/blob"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	"github.com/datuplet/datuplet/pkg/datagateway"
)

var testSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

// batch builds n rows of testSchema (or sch, which must have the same types)
// with ids starting at start.
func batch(sch *arrow.Schema, start, n int) arrow.RecordBatch {
	return batchWith(memory.NewGoAllocator(), sch, start, n)
}

func batchWith(mem memory.Allocator, sch *arrow.Schema, start, n int) arrow.RecordBatch {
	ib := array.NewInt64Builder(mem)
	sb := array.NewStringBuilder(mem)
	defer ib.Release()
	defer sb.Release()
	for i := start; i < start+n; i++ {
		ib.Append(int64(i))
		sb.Append(fmt.Sprintf("row-%d-%s", i, strings.Repeat("x", i%17)))
	}
	ia, sa := ib.NewArray(), sb.NewArray()
	defer ia.Release()
	defer sa.Release()
	return array.NewRecordBatch(sch, []arrow.Array{ia, sa}, int64(n))
}

func localPrefix(t *testing.T) string {
	t.Helper()
	return "file://" + t.TempDir() + "/data/"
}

func localPath(p string) string { return strings.TrimPrefix(p, "file://") }

func openWriter(t *testing.T, p *Plane, spec datagateway.WriterSpec) datagateway.TableWriter {
	t.Helper()
	if spec.Schema == nil {
		spec.Schema = testSchema
	}
	if spec.FilePrefix == "" {
		spec.FilePrefix = "part-00000000-w1"
	}
	w, err := p.OpenTableWriter(context.Background(), spec)
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	return w
}

func write(t *testing.T, w datagateway.TableWriter, rec arrow.RecordBatch) {
	t.Helper()
	defer rec.Release()
	if err := w.Write(context.Background(), rec); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

type footer struct {
	rows      int64
	rowGroups []int64
	codecs    []compress.Compression
	fieldIDs  []int32
	names     []string
}

func readFooter(t *testing.T, path string) footer {
	t.Helper()
	rdr, err := file.OpenParquetFile(localPath(path), false)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer rdr.Close()
	md := rdr.MetaData()
	f := footer{rows: md.NumRows}
	for i := 0; i < md.NumRowGroups(); i++ {
		rg := md.RowGroup(i)
		f.rowGroups = append(f.rowGroups, rg.NumRows())
		for c := 0; c < rg.NumColumns(); c++ {
			cc, err := rg.ColumnChunk(c)
			if err != nil {
				t.Fatal(err)
			}
			f.codecs = append(f.codecs, cc.Compression())
		}
	}
	for c := 0; c < md.Schema.NumColumns(); c++ {
		f.fieldIDs = append(f.fieldIDs, md.Schema.Column(c).SchemaNode().FieldID())
		f.names = append(f.names, md.Schema.Column(c).Name())
	}
	return f
}

func readIDs(t *testing.T, path string) []int64 {
	t.Helper()
	rdr, err := file.OpenParquetFile(localPath(path), false)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Close()
	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := fr.ReadTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Release()
	var out []int64
	for _, ch := range tbl.Column(0).Data().Chunks() {
		a := ch.(*array.Int64)
		for i := 0; i < a.Len(); i++ {
			out = append(out, a.Value(i))
		}
	}
	return out
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(localPath(path))
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

func TestWriterSingleFile(t *testing.T) {
	p := New(Config{})
	defer p.Close()
	prefix := localPrefix(t)
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: prefix})
	for i := 0; i < 3; i++ {
		write(t, w, batch(testSchema, i*100, 100))
	}
	files, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("files = %+v, want 1", files)
	}
	f := files[0]
	if f.Path != prefix+"part-00000000-w1-00000.parquet" {
		t.Errorf("path = %q", f.Path)
	}
	if ft := readFooter(t, f.Path); ft.rows != 300 || f.Rows != 300 {
		t.Errorf("rows: written %d, footer %d, want 300", f.Rows, ft.rows)
	}
	if sz := fileSize(t, f.Path); f.Bytes != sz {
		t.Errorf("bytes = %d, file size %d", f.Bytes, sz)
	}
	ids := readIDs(t, f.Path)
	for i, id := range ids {
		if id != int64(i) {
			t.Fatalf("row %d id %d", i, id)
		}
	}
	// Close is idempotent.
	again, err := w.Close(context.Background())
	if err != nil || len(again) != 1 || again[0].Path != f.Path || again[0].Rows != f.Rows {
		t.Fatalf("second Close = %+v, %v", again, err)
	}
}

func TestWriterRotates(t *testing.T) {
	p := New(Config{})
	defer p.Close()
	prefix := localPrefix(t)
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: prefix, TargetFileBytes: 1}) // every batch ends a file
	const batches, per = 5, 50
	for i := 0; i < batches; i++ {
		write(t, w, batch(testSchema, i*per, per))
	}
	files, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(files) < 3 {
		t.Fatalf("files = %d, want >= 3", len(files))
	}
	var total int64
	for i, f := range files {
		want := fmt.Sprintf("%spart-00000000-w1-%05d.parquet", prefix, i)
		if f.Path != want {
			t.Errorf("file %d path %q, want %q", i, f.Path, want)
		}
		if ft := readFooter(t, f.Path); ft.rows != f.Rows {
			t.Errorf("file %d rows %d, footer %d", i, f.Rows, ft.rows)
		}
		if sz := fileSize(t, f.Path); sz != f.Bytes {
			t.Errorf("file %d bytes %d, size %d", i, f.Bytes, sz)
		}
		total += f.Rows
	}
	if total != batches*per {
		t.Fatalf("total rows %d, want %d", total, batches*per)
	}
}

func TestWriterZeroRows(t *testing.T) {
	p := New(Config{})
	defer p.Close()
	prefix := localPrefix(t)
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: prefix})
	write(t, w, batch(testSchema, 0, 0)) // an empty batch starts no file
	files, err := w.Close(context.Background())
	if err != nil || len(files) != 0 {
		t.Fatalf("Close = %+v, %v; want no files", files, err)
	}
	if _, err := os.Stat(localPath(prefix)); !errors.Is(err, os.ErrNotExist) {
		entries, _ := os.ReadDir(localPath(prefix))
		if len(entries) != 0 {
			t.Fatalf("data dir not empty: %v", entries)
		}
	}
}

func TestWriterParquetProps(t *testing.T) {
	p := New(Config{})
	defer p.Close()
	withIDs := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id", "keep"}, []string{"1", "v"})},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"2"})},
	}, nil)
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: localPrefix(t), Schema: withIDs, RowGroupRows: 64})
	write(t, w, batch(withIDs, 0, 100))
	write(t, w, batch(withIDs, 100, 100))
	files, err := w.Close(context.Background())
	if err != nil || len(files) != 1 {
		t.Fatalf("Close = %+v, %v", files, err)
	}
	ft := readFooter(t, files[0].Path)
	if ft.rows != 200 || len(ft.rowGroups) < 4 {
		t.Errorf("rows %d, row groups %v", ft.rows, ft.rowGroups)
	}
	for i, n := range ft.rowGroups {
		if n > 64 {
			t.Errorf("row group %d has %d rows > 64", i, n)
		}
	}
	for i, c := range ft.codecs {
		if c != compress.Codecs.Zstd {
			t.Errorf("column chunk %d codec %v, want ZSTD", i, c)
		}
	}
	for i, id := range ft.fieldIDs {
		if id >= 0 {
			t.Errorf("column %d carries field id %d", i, id)
		}
	}
}

func TestWriterHostileColumnNames(t *testing.T) {
	p := New(Config{})
	defer p.Close()
	names := []string{`a"b`, `x'y`, `;DROP`, "nl\nx", "ünï"}
	var fields []arrow.Field
	for _, n := range names {
		fields = append(fields, arrow.Field{Name: n, Type: arrow.PrimitiveTypes.Int32, Nullable: true})
	}
	sch := arrow.NewSchema(fields, nil)
	mem := memory.NewGoAllocator()
	var cols []arrow.Array
	for range names {
		arr, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1, null, 3]`))
		if err != nil {
			t.Fatal(err)
		}
		cols = append(cols, arr)
	}
	rec := array.NewRecordBatch(sch, cols, 3)
	for _, c := range cols {
		c.Release()
	}
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: localPrefix(t), Schema: sch})
	write(t, w, rec)
	files, err := w.Close(context.Background())
	if err != nil || len(files) != 1 {
		t.Fatalf("Close = %+v, %v", files, err)
	}
	ft := readFooter(t, files[0].Path)
	if strings.Join(ft.names, "|") != strings.Join(names, "|") || ft.rows != 3 {
		t.Fatalf("names %q rows %d, want %q", ft.names, ft.rows, names)
	}
}

// waitGoroutines waits until the goroutine count is back to base.
func waitGoroutines(t *testing.T, base int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > base {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("goroutines %d > baseline %d\n%s", runtime.NumGoroutine(), base, buf[:n])
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWriterUploadErrorSurfaces(t *testing.T) {
	base := runtime.NumGoroutine()
	p := New(Config{})
	dir := t.TempDir()
	blocker := filepath.Join(dir, "afile")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: "file://" + blocker + "/data/"})
	start := time.Now()
	var werr error
	for i := 0; i < 50 && werr == nil; i++ {
		rec := batch(testSchema, i*10, 10)
		werr = w.Write(context.Background(), rec)
		rec.Release()
	}
	files, cerr := w.Close(context.Background())
	if werr == nil && cerr == nil {
		t.Fatalf("no error from Write or Close (files %+v)", files)
	}
	if cerr == nil {
		t.Fatalf("Close succeeded after a failed write: %+v", files)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("error took %v", time.Since(start))
	}
	w.Abort()
	p.Close()
	waitGoroutines(t, base)
}

func TestWriterAbortIdempotent(t *testing.T) {
	base := runtime.NumGoroutine()
	p := New(Config{})
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: localPrefix(t)})
	write(t, w, batch(testSchema, 0, 10))
	w.Abort()
	w.Abort()
	if _, err := w.Close(context.Background()); err == nil {
		t.Fatal("Close after Abort succeeded")
	}
	if err := w.Write(context.Background(), batch(testSchema, 0, 1)); err == nil {
		t.Fatal("Write after Abort succeeded")
	}

	// Abort after a successful Close is a no-op and keeps the files.
	w2 := openWriter(t, p, datagateway.WriterSpec{DataPrefix: localPrefix(t)})
	write(t, w2, batch(testSchema, 0, 10))
	files, err := w2.Close(context.Background())
	if err != nil || len(files) != 1 {
		t.Fatalf("Close = %+v, %v", files, err)
	}
	w2.Abort()
	if _, err := os.Stat(localPath(files[0].Path)); err != nil {
		t.Fatalf("Abort after Close removed the file: %v", err)
	}
	p.Close()
	waitGoroutines(t, base)
}

func TestAbortLeavesNoLocalFile(t *testing.T) {
	p := New(Config{})
	defer p.Close()
	prefix := localPrefix(t)
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: prefix, RowGroupRows: 10})
	write(t, w, batch(testSchema, 0, 100))
	partial := localPath(prefix + "part-00000000-w1-00000.parquet")
	deadline := time.Now().Add(5 * time.Second)
	for {
		if st, err := os.Stat(partial); err == nil && st.Size() > 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("partial file never appeared")
		}
		time.Sleep(5 * time.Millisecond)
	}
	w.Abort()
	if _, err := os.Stat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial file still exists after Abort: %v", err)
	}
}

// failingSink fails every Write after the first okWrites calls and counts Close.
type failingSink struct {
	okWrites int32
	writes   atomic.Int32
	closes   atomic.Int32
	ctx      context.Context // the upload context the sink was opened with
	// cancelledAtClose records whether ctx was already cancelled when Close
	// ran (cancel-before-close is what aborts a gocloud upload).
	cancelledAtClose atomic.Bool
}

func (s *failingSink) Write(p []byte) (int, error) {
	if s.writes.Add(1) > s.okWrites {
		return 0, errors.New("injected sink failure")
	}
	return len(p), nil
}

func (s *failingSink) Close() error {
	s.closes.Add(1)
	s.cancelledAtClose.Store(s.ctx.Err() != nil)
	return nil
}

func TestEncodeErrorClosesSink(t *testing.T) {
	p := New(Config{})
	defer p.Close()
	sink := &failingSink{okWrites: 1} // the magic header, then fail mid-row-group flush
	var opens atomic.Int32
	p.openSink = func(ctx context.Context, props map[string]string, path string, opts *blob.WriterOptions) (io.WriteCloser, error) {
		opens.Add(1)
		sink.ctx = ctx
		return sink, nil
	}
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: "file:///nonexistent/", RowGroupRows: 10})
	var werr error
	for i := 0; i < 20 && werr == nil; i++ {
		rec := batch(testSchema, i*50, 50)
		werr = w.Write(context.Background(), rec)
		rec.Release()
	}
	_, cerr := w.Close(context.Background())
	if cerr == nil || !strings.Contains(cerr.Error(), "injected sink failure") {
		t.Fatalf("Close err = %v, want the sink failure", cerr)
	}
	if opens.Load() != 1 {
		t.Fatalf("sink opened %d times", opens.Load())
	}
	if n := sink.closes.Load(); n != 1 {
		t.Fatalf("sink closed %d times, want exactly 1", n)
	}
	if !sink.cancelledAtClose.Load() {
		t.Fatal("sink closed before its upload context was cancelled")
	}
	w.Abort()
	if n := sink.closes.Load(); n != 1 {
		t.Fatalf("sink closed %d times after Abort, want exactly 1", n)
	}
}

func TestWriteCtxDoesNotOwnWriter(t *testing.T) {
	p := New(Config{})
	defer p.Close()
	openCtx, cancelOpen := context.WithCancel(context.Background())
	w, err := p.OpenTableWriter(openCtx, datagateway.WriterSpec{
		DataPrefix: localPrefix(t), FilePrefix: "part-00000000-w1", Schema: testSchema,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancelOpen()
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		rec := batch(testSchema, i*10, 10)
		err := w.Write(ctx, rec)
		rec.Release()
		cancel() // the request that carried this chunk ended
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	files, err := w.Close(context.Background())
	if err != nil || len(files) != 1 || files[0].Rows != 30 {
		t.Fatalf("Close = %+v, %v", files, err)
	}
}

// fakeCreds hands out a scripted sequence of credentials.
type fakeCreds struct {
	mu          sync.Mutex
	seq         []catalogwriter.Creds // returned in order; the last one repeats
	gets        int
	invalidates int
}

func (f *fakeCreds) Get(context.Context) (catalogwriter.Creds, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := min(f.gets, len(f.seq)-1)
	f.gets++
	return f.seq[i], nil
}

func (f *fakeCreds) Invalidate() {
	f.mu.Lock()
	f.invalidates++
	f.mu.Unlock()
}

func s3creds(key string, issued, expires time.Time) catalogwriter.S3Creds {
	return catalogwriter.S3Creds{
		AccessKeyID: key, SecretAccessKey: "secret-" + key, SessionToken: "tok-" + key,
		Endpoint: "http://127.0.0.1:9000", Issued: issued, Expires: expires,
	}
}

// recordProps wraps the default sink opener and records the props per file.
func recordProps(p *Plane) *[]map[string]string {
	var mu sync.Mutex
	var got []map[string]string
	p.openSink = func(ctx context.Context, props map[string]string, path string, opts *blob.WriterOptions) (io.WriteCloser, error) {
		mu.Lock()
		got = append(got, props)
		mu.Unlock()
		return openIOSink(ctx, props, path, opts)
	}
	return &got
}

func TestCredsPerFile(t *testing.T) {
	p := New(Config{})
	defer p.Close()
	now := time.Now()
	creds := &fakeCreds{seq: []catalogwriter.Creds{
		s3creds("k1", now, now.Add(time.Hour)),
		s3creds("k2", now, now.Add(time.Hour)),
		s3creds("k3", now, now.Add(time.Hour)),
	}}
	props := recordProps(p)
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: localPrefix(t), Creds: creds, TargetFileBytes: 1})
	for i := 0; i < 3; i++ {
		write(t, w, batch(testSchema, i*10, 10))
	}
	files, err := w.Close(context.Background())
	if err != nil || len(files) != 3 {
		t.Fatalf("Close = %+v, %v", files, err)
	}
	if creds.gets != 3 {
		t.Fatalf("Get called %d times for 3 files", creds.gets)
	}
	if len(*props) != 3 {
		t.Fatalf("%d sink opens", len(*props))
	}
	for i, pr := range *props {
		if want := fmt.Sprintf("k%d", i+1); pr["s3.access-key-id"] != want {
			t.Errorf("file %d opened with key %q, want %q", i, pr["s3.access-key-id"], want)
		}
	}
}

func TestCredsNearExpiryRefetched(t *testing.T) {
	p := New(Config{})
	defer p.Close()
	now := time.Now()
	creds := &fakeCreds{seq: []catalogwriter.Creds{
		s3creds("stale", now.Add(-59*time.Minute), now.Add(time.Minute)), // 1 min left < 2 min
		s3creds("fresh", now, now.Add(time.Hour)),
	}}
	props := recordProps(p)
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: localPrefix(t), Creds: creds})
	write(t, w, batch(testSchema, 0, 10))
	files, err := w.Close(context.Background())
	if err != nil || len(files) != 1 {
		t.Fatalf("Close = %+v, %v", files, err)
	}
	if creds.gets != 2 || creds.invalidates != 1 {
		t.Fatalf("gets %d invalidates %d, want 2 / 1", creds.gets, creds.invalidates)
	}
	if got := (*props)[0]["s3.access-key-id"]; got != "fresh" {
		t.Fatalf("file opened with %q, want fresh", got)
	}

	// 25 % of a long TTL also counts: 10 h TTL, 2 h left < 2.5 h.
	creds2 := &fakeCreds{seq: []catalogwriter.Creds{
		s3creds("stale", now.Add(-8*time.Hour), now.Add(2*time.Hour)),
		s3creds("fresh", now, now.Add(10*time.Hour)),
	}}
	w2 := openWriter(t, p, datagateway.WriterSpec{DataPrefix: localPrefix(t), Creds: creds2})
	write(t, w2, batch(testSchema, 0, 10))
	if _, err := w2.Close(context.Background()); err != nil || creds2.invalidates != 1 {
		t.Fatalf("Close err %v, invalidates %d", err, creds2.invalidates)
	}
}

func TestCredsStillNearExpiryFails(t *testing.T) {
	p := New(Config{})
	defer p.Close()
	now := time.Now()
	creds := &fakeCreds{seq: []catalogwriter.Creds{
		s3creds("stale", now.Add(-59*time.Minute), now.Add(time.Minute)),
	}}
	var opens atomic.Int32
	p.openSink = func(ctx context.Context, props map[string]string, path string, opts *blob.WriterOptions) (io.WriteCloser, error) {
		opens.Add(1)
		return openIOSink(ctx, props, path, opts)
	}
	prefix := localPrefix(t)
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: prefix, Creds: creds})
	rec := batch(testSchema, 0, 10)
	werr := w.Write(context.Background(), rec)
	rec.Release()
	_, cerr := w.Close(context.Background())
	err := cerr
	if err == nil {
		err = werr
	}
	if err == nil || !strings.Contains(err.Error(), "goplane: vended credentials expire too soon") {
		t.Fatalf("err = %v (write %v)", cerr, werr)
	}
	if opens.Load() != 0 {
		t.Fatalf("a file was started (%d opens)", opens.Load())
	}
	if creds.gets != 2 || creds.invalidates != 1 {
		t.Fatalf("gets %d invalidates %d, want 2 / 1", creds.gets, creds.invalidates)
	}
	if entries, _ := os.ReadDir(localPath(prefix)); len(entries) != 0 {
		t.Fatalf("files left: %v", entries)
	}
}

func TestOpenTableWriterRejectsRemoteWithoutCreds(t *testing.T) {
	p := New(Config{})
	defer p.Close()
	_, err := p.OpenTableWriter(context.Background(), datagateway.WriterSpec{
		DataPrefix: "s3://bucket/t/data/", FilePrefix: "part-x-w1", Schema: testSchema,
	})
	if err == nil {
		t.Fatal("remote prefix without credentials accepted")
	}
}

func TestWriteRejectsSchemaMismatch(t *testing.T) {
	p := New(Config{})
	defer p.Close()
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: localPrefix(t)})
	defer w.Abort()
	other := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	mem := memory.NewGoAllocator()
	arr, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int32, strings.NewReader(`[1]`))
	rec := array.NewRecordBatch(other, []arrow.Array{arr}, 1)
	arr.Release()
	defer rec.Release()
	if err := w.Write(context.Background(), rec); err == nil {
		t.Fatal("mismatched record accepted")
	}
}

// blockingSink blocks every Write until release is closed.
type blockingSink struct {
	release chan struct{}
	buf     strings.Builder
}

func (s *blockingSink) Write(p []byte) (int, error) {
	<-s.release
	return s.buf.Write(p)
}

func (s *blockingSink) Close() error { return nil }

func TestWriterBackpressureAndQueuedBytes(t *testing.T) {
	p := New(Config{})
	defer p.Close()
	sink := &blockingSink{release: make(chan struct{})}
	p.openSink = func(context.Context, map[string]string, string, *blob.WriterOptions) (io.WriteCloser, error) {
		return sink, nil
	}
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: "file:///unused/"})
	one := batch(testSchema, 0, 10)
	size := recordBytes(one)
	one.Release()

	// The first batch is taken by the encoder, which blocks on the sink; the
	// next channelBatches wait in the channel.
	write(t, w, batch(testSchema, 0, 10))
	deadline := time.Now().Add(5 * time.Second)
	for w.QueuedBytes() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("first batch never left the channel (queued %d)", w.QueuedBytes())
		}
		time.Sleep(5 * time.Millisecond)
	}
	for i := 0; i < channelBatches; i++ {
		write(t, w, batch(testSchema, 0, 10))
	}
	if got := w.QueuedBytes(); got != int64(channelBatches)*size {
		t.Fatalf("QueuedBytes = %d, want %d", got, int64(channelBatches)*size)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	rec := batch(testSchema, 0, 10)
	err := w.Write(ctx, rec)
	rec.Release()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Write on a full channel = %v, want the call's deadline", err)
	}
	if got := w.QueuedBytes(); got != int64(channelBatches)*size {
		t.Fatalf("QueuedBytes after the timed-out Write = %d", got)
	}
	close(sink.release)
	files, err := w.Close(context.Background())
	if err != nil || len(files) != 1 || files[0].Rows != 10*(1+channelBatches) {
		t.Fatalf("Close = %+v, %v", files, err)
	}
	if w.QueuedBytes() != 0 {
		t.Fatalf("QueuedBytes after Close = %d", w.QueuedBytes())
	}
}

// stallSink blocks every Write until release is closed or its upload context
// is cancelled (as a real streamed upload does).
type stallSink struct {
	ctx              context.Context
	release          chan struct{}
	closes           atomic.Int32
	cancelledAtClose atomic.Bool
}

func (s *stallSink) Write(p []byte) (int, error) {
	select {
	case <-s.release:
		return len(p), nil
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	}
}

func (s *stallSink) Close() error {
	s.closes.Add(1)
	s.cancelledAtClose.Store(s.ctx.Err() != nil)
	return nil
}

// TestWriterExitsOnPlaneCancel: plane Close or Interrupt alone (no writer
// Close/Abort) ends the writer goroutine, releases every batch it holds,
// aborts the in-flight upload, and later Write/Close fail without blocking.
func TestWriterExitsOnPlaneCancel(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cancel func(*Plane)
	}{
		{"Close", func(p *Plane) { _ = p.Close() }},
		{"Interrupt", func(p *Plane) { p.Interrupt() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			p := New(Config{})
			sink := &stallSink{release: make(chan struct{})}
			opened := make(chan struct{})
			p.openSink = func(ctx context.Context, _ map[string]string, _ string, _ *blob.WriterOptions) (io.WriteCloser, error) {
				sink.ctx = ctx
				close(opened)
				return sink, nil
			}
			w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: "file:///unused/"}).(*writer)
			for i := 0; i < 1+channelBatches; i++ { // one in the encoder, the rest queued
				rec := batchWith(mem, testSchema, i*10, 10)
				if err := w.Write(context.Background(), rec); err != nil {
					t.Fatalf("Write %d: %v", i, err)
				}
				rec.Release()
			}
			<-opened
			tc.cancel(p)
			select {
			case <-w.done:
			case <-time.After(5 * time.Second):
				t.Fatal("writer goroutine still running after plane cancellation")
			}
			if n := mem.CurrentAlloc(); n != 0 {
				t.Fatalf("%d bytes of batches not released", n)
			}
			if sink.closes.Load() != 1 || !sink.cancelledAtClose.Load() {
				t.Fatalf("upload not aborted: closes %d, cancelled at close %v", sink.closes.Load(), sink.cancelledAtClose.Load())
			}
			start := time.Now()
			rec := batch(testSchema, 0, 1)
			werr := w.Write(context.Background(), rec)
			rec.Release()
			_, cerr := w.Close(context.Background())
			if werr == nil || cerr == nil || time.Since(start) > time.Second {
				t.Fatalf("after plane cancel: Write %v, Close %v, took %v", werr, cerr, time.Since(start))
			}
			w.Abort()
		})
	}
}

// TestWriteCloseHonourDeadlinesUnderBackpressure: with the sink stalled and
// the queue full, a Write and a Close with short deadlines return
// DeadlineExceeded promptly while another Write is blocked, and the blocked
// sender is not counted in QueuedBytes.
func TestWriteCloseHonourDeadlinesUnderBackpressure(t *testing.T) {
	p := New(Config{})
	defer p.Close()
	sink := &blockingSink{release: make(chan struct{})}
	p.openSink = func(context.Context, map[string]string, string, *blob.WriterOptions) (io.WriteCloser, error) {
		return sink, nil
	}
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: "file:///unused/"}).(*writer)
	one := batch(testSchema, 0, 10)
	size := recordBytes(one)
	one.Release()
	write(t, w, batch(testSchema, 0, 10))
	for deadline := time.Now().Add(5 * time.Second); w.QueuedBytes() != 0; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("first batch never left the channel")
		}
	}
	for i := 0; i < channelBatches; i++ {
		write(t, w, batch(testSchema, 0, 10))
	}

	blocked := make(chan error, 1)
	go func() {
		rec := batch(testSchema, 0, 10)
		defer rec.Release()
		blocked <- w.Write(context.Background(), rec)
	}()
	time.Sleep(50 * time.Millisecond) // let the fifth sender block

	if got := w.QueuedBytes(); got != int64(channelBatches)*size {
		t.Fatalf("QueuedBytes with a blocked sender = %d, want %d", got, int64(channelBatches)*size)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	rec := batch(testSchema, 0, 10)
	err := w.Write(ctx, rec)
	rec.Release()
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("concurrent Write = %v after %v, want DeadlineExceeded promptly", err, time.Since(start))
	}

	cctx, ccancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer ccancel()
	start = time.Now()
	if _, err := w.Close(cctx); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("concurrent Close = %v after %v, want DeadlineExceeded promptly", err, time.Since(start))
	}
	// Close stopped admission: the blocked sender gives up.
	select {
	case err := <-blocked:
		if err == nil {
			t.Fatal("blocked Write succeeded after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Write still blocked after Close")
	}

	close(sink.release)
	files, err := w.Close(context.Background())
	if err != nil || len(files) != 1 || files[0].Rows != 10*(1+channelBatches) {
		t.Fatalf("Close = %+v, %v", files, err)
	}
	if w.QueuedBytes() != 0 {
		t.Fatalf("QueuedBytes after Close = %d", w.QueuedBytes())
	}
}
