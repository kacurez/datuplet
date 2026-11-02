package goplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
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
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/datagateway"
	"github.com/datuplet/datuplet/pkg/datagateway/lakekeeper"
	"github.com/datuplet/datuplet/pkg/icebergjob"
)

// Shared row-group byte budget of partitioned writers (spec decision 9).

// wideSchema: an id, the partition key, a dictionary-heavy string, a
// sequence and an incompressible blob.
var wideSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "k", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "cat", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "num", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "blob", Type: arrow.BinaryTypes.Binary, Nullable: true},
}, nil)

var wideIceberg = iceberg.NewSchema(0,
	iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
	iceberg.NestedField{ID: 2, Name: "k", Type: iceberg.PrimitiveTypes.Int64},
	iceberg.NestedField{ID: 3, Name: "cat", Type: iceberg.PrimitiveTypes.String},
	iceberg.NestedField{ID: 4, Name: "num", Type: iceberg.PrimitiveTypes.Int64},
	iceberg.NestedField{ID: 5, Name: "blob", Type: iceberg.PrimitiveTypes.Binary},
)

const blobBytes = 256

// wideBatch builds one row per entry of keys (row i has partition key
// keys[i]), ids from start, blobs from rng.
func wideBatch(rng *rand.Rand, start int, keys []int64) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.NewGoAllocator(), wideSchema)
	defer b.Release()
	blob := make([]byte, blobBytes)
	for i, k := range keys {
		id := int64(start + i)
		b.Field(0).(*array.Int64Builder).Append(id)
		b.Field(1).(*array.Int64Builder).Append(k)
		b.Field(2).(*array.StringBuilder).Append(fmt.Sprintf("category-%02d", id%16))
		b.Field(3).(*array.Int64Builder).Append(id * 7)
		rng.Read(blob)
		b.Field(4).(*array.BinaryBuilder).Append(blob)
	}
	return b.NewRecordBatch()
}

// repeatKey is n copies of k.
func repeatKey(k int64, n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = k
	}
	return out
}

// interleave is perTuple rows of each of tuples keys, interleaved.
func interleave(tuples, perTuple int) []int64 {
	out := make([]int64, 0, tuples*perTuple)
	for i := 0; i < perTuple; i++ {
		for k := 0; k < tuples; k++ {
			out = append(out, int64(k))
		}
	}
	return out
}

func wideSpec(t *testing.T) iceberg.PartitionSpec {
	t.Helper()
	id := 1000
	spec, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldByName("k", "k", iceberg.IdentityTransform{}, wideIceberg, &id))
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

// budgetLog records the hook's events (called from the writer's run
// goroutine).
type budgetLog struct {
	mu     sync.Mutex
	events []budgetEvent
}

func (l *budgetLog) hook(ev budgetEvent) {
	l.mu.Lock()
	l.events = append(l.events, ev)
	l.mu.Unlock()
}

func (l *budgetLog) all() []budgetEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]budgetEvent(nil), l.events...)
}

func (l *budgetLog) count(site string) int {
	n := 0
	for _, ev := range l.all() {
		if ev.site == site {
			n++
		}
	}
	return n
}

// rowGroupShape is the row counts of a file's non-empty row groups and the
// number of empty ones.
func rowGroupShape(t *testing.T, path string) (rows []int64, empty int) {
	t.Helper()
	for _, n := range readFooter(t, path).rowGroups {
		if n == 0 {
			empty++
		} else {
			rows = append(rows, n)
		}
	}
	return rows, empty
}

func TestPartitionBufferTriggerKeepsSumUnderBudget(t *testing.T) {
	const budget = 4 << 20
	pt := newPartTable(t, wideSchema, lakekeeper.PartitionField{SourceColumn: "k", Transform: "identity"})
	pt.p.cfg.PartitionBufferBytes = budget
	bl := &budgetLog{}
	pt.p.budgetHook = bl.hook

	rng := rand.New(rand.NewSource(1))
	var recs []arrow.RecordBatch
	next := 0
	for i := 0; i < 40; i++ {
		keys := interleave(8, 256) // 8 tuples, scattered rows
		if i == 20 {
			// One oversized sub-batch: ~7.5 MiB of blobs for tuple 3 alone.
			keys = append(keys, repeatKey(3, 30000)...)
		}
		recs = append(recs, wideBatch(rng, next, keys))
		next += len(keys)
	}
	files := pt.write(pt.spec(), recs...)

	events := bl.all()
	enforced, other := 0, 0
	for _, ev := range events {
		switch ev.site {
		case "enforced":
			enforced++
			if ev.sum > budget {
				t.Errorf("after enforcement the open files hold %d buffered page bytes, over the %d budget (%d open)", ev.sum, budget, ev.open)
			}
		case "largest":
			if ev.path != ev.writing {
				other++
			}
		}
	}
	if enforced == 0 || bl.count("share") == 0 || bl.count("largest") == 0 || other == 0 {
		t.Fatalf("enforced %d, share flushes %d, largest flushes %d (%d of another file): the budget never triggered every path",
			enforced, bl.count("share"), bl.count("largest"), other)
	}

	if len(files) != 8 {
		t.Fatalf("files = %d, want one per tuple (8)", len(files))
	}
	spec := pt.tbl.Spec()
	seen := map[int64]bool{}
	for _, f := range files {
		k := f.Partition[spec.Field(0).FieldID].(int64)
		if seen[k] {
			t.Fatalf("tuple %d has more than one file: %v", k, files)
		}
		seen[k] = true
		rows, empty := rowGroupShape(t, f.Path)
		t.Logf("tuple %d: %d rows in %d row groups (+%d empty)", k, f.Rows, len(rows), empty)
	}
	pt.commit(files, icebergjob.WriteModeAppend)
	_, ids := pt.scan(nil)
	if len(ids) != next {
		t.Fatalf("committed %d rows, want %d", len(ids), next)
	}
	for i, id := range ids {
		if id != int64(i) {
			t.Fatalf("committed ids not each of 0..%d exactly once (index %d is %d)", next-1, i, id)
		}
	}
}

// writeWide writes recs through a fresh partitioned writer on p and returns
// its files.
func writeWide(t *testing.T, p *Plane, spec datagateway.WriterSpec, recs []arrow.RecordBatch) []datagateway.WrittenFile {
	t.Helper()
	w := openWriter(t, p, spec)
	for _, rec := range recs {
		write(t, w, rec)
	}
	files, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	return files
}

func TestPartitionRowGroupsShrinkWithOpenFiles(t *testing.T) {
	const budget = 8 << 20
	spec := wideSpec(t)
	// Per tuple and batch: 1024 rows (~256 KiB of blobs); 40 batches give
	// each tuple ~10 MiB in 40 960 rows, far below the 229 376-row limit.
	run := func(tuples int) [][]int64 {
		p := New(Config{PartitionBufferBytes: budget})
		defer p.Close()
		rng := rand.New(rand.NewSource(2))
		var recs []arrow.RecordBatch
		for i := 0; i < 40; i++ {
			recs = append(recs, wideBatch(rng, i*tuples*1024, interleave(tuples, 1024)))
		}
		files := writeWide(t, p, datagateway.WriterSpec{DataPrefix: localPrefix(t), Schema: wideSchema, PartitionSpec: &spec, TableSchema: wideIceberg}, recs)
		if len(files) != tuples {
			t.Fatalf("%d tuples: files = %d, want one per tuple", tuples, len(files))
		}
		var out [][]int64
		for _, f := range files {
			if f.Rows != 40*1024 {
				t.Fatalf("file %s has %d rows, want %d", f.Path, f.Rows, 40*1024)
			}
			rows, _ := rowGroupShape(t, f.Path)
			for _, n := range rows {
				if n >= defaultRowGroupRows {
					t.Fatalf("row group of %d rows: the row limit, not the byte budget, ended it", n)
				}
			}
			out = append(out, rows)
		}
		return out
	}
	one := run(1)
	eight := run(8)
	if len(one[0]) < 2 {
		t.Fatalf("1 open file: row groups %v, want the byte budget to end at least one", one[0])
	}
	for i, rgs := range eight {
		if len(rgs) <= len(one[0]) {
			t.Errorf("8 open files: file %d has %d row groups %v, want more than the %d of 1 open file %v", i, len(rgs), rgs, len(one[0]), one[0])
		}
	}
	t.Logf("1 open file: row groups %v; 8 open files: file 0 row groups %v", one[0], eight[0])
}

// flushSink wraps a local upload sink: Write fails once armed or once the
// upload context is cancelled; an abort (Close with the context cancelled)
// waits until want aborts are in flight at once, or 2 s.
type flushSink struct {
	inner      io.WriteCloser
	ctx        context.Context
	armed      atomic.Bool
	inFlight   *atomic.Int32
	want       *atomic.Int32
	closes     atomic.Int32
	cancelled  atomic.Bool
	concurrent atomic.Bool
}

func (s *flushSink) Write(p []byte) (int, error) {
	if s.armed.Load() {
		return 0, errors.New("injected flush failure")
	}
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}
	return s.inner.Write(p)
}

func (s *flushSink) Close() error {
	s.closes.Add(1)
	if s.ctx.Err() != nil {
		s.cancelled.Store(true)
		s.inFlight.Add(1)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if s.inFlight.Load() >= s.want.Load() {
				s.concurrent.Store(true)
				break
			}
			time.Sleep(time.Millisecond)
		}
	}
	return s.inner.Close()
}

func TestPartitionBudgetFlushErrorCleansAll(t *testing.T) {
	const budget = 16 << 20
	spec := wideSpec(t)
	// Batches per scenario; tuple 0 gets ~12 MiB first.
	scenarios := map[string][][]int64{
		// Three open files; the third batch's write to tuple 0 finds it over
		// its fair share (16 MiB / 3).
		"share": {append(repeatKey(0, 48000), repeatKey(2, 100)...), repeatKey(1, 100), repeatKey(0, 100)},
		// Tuple 1's write takes the sum over budget: tuple 0, a different
		// file, holds the most.
		"largest": {append(repeatKey(0, 48000), repeatKey(2, 100)...), repeatKey(1, 32000)},
	}
	for _, site := range []string{"share", "largest"} {
		for _, mode := range []string{"sink error", "cancel"} {
			t.Run(site+"/"+mode, func(t *testing.T) {
				base := runtime.NumGoroutine()
				p := New(Config{PartitionBufferBytes: budget})
				var inFlight, want atomic.Int32
				want.Store(1 << 30)
				var mu sync.Mutex
				sinks := map[string]*flushSink{}
				p.openSink = func(ctx context.Context, props map[string]string, path string) (io.WriteCloser, error) {
					inner, err := openIOSink(ctx, props, path)
					if err != nil {
						return nil, err
					}
					s := &flushSink{inner: inner, ctx: ctx, inFlight: &inFlight, want: &want}
					mu.Lock()
					sinks[path] = s
					mu.Unlock()
					return s, nil
				}
				var triggered atomic.Bool
				p.budgetHook = func(ev budgetEvent) {
					if ev.site != site || triggered.Load() || (site == "largest" && ev.path == ev.writing) {
						return
					}
					triggered.Store(true)
					want.Store(int32(ev.open))
					if mode == "cancel" {
						p.Interrupt()
						return
					}
					mu.Lock()
					sinks[ev.path].armed.Store(true)
					mu.Unlock()
				}
				prefix := localPrefix(t)
				w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: prefix, Schema: wideSchema, PartitionSpec: &spec, TableSchema: wideIceberg})
				rng := rand.New(rand.NewSource(3))
				next := 0
				for _, keys := range scenarios[site] {
					rec := wideBatch(rng, next, keys)
					next += len(keys)
					_ = w.Write(context.Background(), rec) // may fail once the writer has failed
					rec.Release()
				}
				files, err := w.Close(context.Background())
				if !triggered.Load() {
					t.Fatalf("no %s flush happened", site)
				}
				if err == nil || files != nil {
					t.Fatalf("Close = %v, %v; want the flush failure", files, err)
				}
				if mode == "sink error" && !strings.Contains(err.Error(), "injected flush failure") {
					t.Errorf("Close error %v, want the injected failure", err)
				}
				if _, again := w.Close(context.Background()); again == nil || again.Error() != err.Error() {
					t.Errorf("second Close = %v, want the same error %v", again, err)
				}
				rec := wideBatch(rng, next, repeatKey(0, 1))
				if werr := w.Write(context.Background(), rec); werr == nil {
					t.Error("Write after the failure succeeded")
				}
				rec.Release()
				mu.Lock()
				defer mu.Unlock()
				if len(sinks) != 3 {
					t.Fatalf("sinks = %d, want 3", len(sinks))
				}
				for path, s := range sinks {
					if s.closes.Load() != 1 || !s.cancelled.Load() {
						t.Errorf("%s: closes %d, aborted %v; want aborted once", path, s.closes.Load(), s.cancelled.Load())
					}
					if !s.concurrent.Load() {
						t.Errorf("%s: abort did not overlap the others", path)
					}
				}
				if m, _ := filepath.Glob(localPath(prefix) + "k=*/*"); len(m) != 0 {
					t.Errorf("files left after the failure: %v", m)
				}
				w.Abort()
				_ = p.Close()
				waitGoroutines(t, base)
			})
		}
	}
}

func TestPartitionBudgetFlushPreservesRotationAndMetadata(t *testing.T) {
	spec := wideSpec(t)
	type fileView struct {
		name string
		rows int64
		part string
		ids  string
	}
	run := func(budget int64) ([]fileView, *budgetLog) {
		p := New(Config{PartitionBufferBytes: budget})
		defer p.Close()
		bl := &budgetLog{}
		p.budgetHook = bl.hook
		rng := rand.New(rand.NewSource(4))
		var recs []arrow.RecordBatch
		for i := 0; i < 60; i++ {
			recs = append(recs, wideBatch(rng, i*4*1024, interleave(4, 1024)))
		}
		prefix := localPrefix(t)
		// Rotation at 4 x 2 MiB fed: two files per tuple.
		files := writeWide(t, p, datagateway.WriterSpec{DataPrefix: prefix, Schema: wideSchema, TargetFileBytes: 2 << 20, PartitionSpec: &spec, TableSchema: wideIceberg}, recs)
		var out []fileView
		for _, f := range files {
			ids := readIDs(t, f.Path)
			if int64(len(ids)) != f.Rows {
				t.Fatalf("%s: %d ids, %d rows reported", f.Path, len(ids), f.Rows)
			}
			if rows, empty := rowGroupShape(t, f.Path); empty > 0 {
				t.Logf("budget %d: %s has row groups %v and %d empty", budget, strings.TrimPrefix(f.Path, prefix), rows, empty)
			}
			out = append(out, fileView{
				name: strings.TrimPrefix(f.Path, prefix),
				rows: f.Rows,
				part: fmt.Sprint(f.SpecID, f.Partition),
				ids:  fmt.Sprint(ids),
			})
		}
		return out, bl
	}
	plain, plainLog := run(maxPartitionBufferBytes)
	flushed, flushLog := run(4 << 20)
	if n := plainLog.count("share") + plainLog.count("largest"); n != 0 {
		t.Fatalf("8 GiB budget flushed %d times, want none", n)
	}
	if flushLog.count("share") == 0 || flushLog.count("largest") == 0 {
		t.Fatalf("4 MiB budget: share flushes %d, largest flushes %d; want both", flushLog.count("share"), flushLog.count("largest"))
	}
	if len(plain) <= 4 {
		t.Fatalf("files = %d, want rotation to make more than one per tuple", len(plain))
	}
	if len(flushed) != len(plain) {
		t.Fatalf("budget flushes changed the file count: %d, want %d", len(flushed), len(plain))
	}
	for i := range plain {
		if flushed[i] != plain[i] {
			f, q := flushed[i], plain[i]
			t.Errorf("file %d: with budget flushes %s rows %d %s (ids equal %v), without %s rows %d %s",
				i, f.name, f.rows, f.part, f.ids == q.ids, q.name, q.rows, q.part)
		}
	}
}

func TestUnpartitionedRowGroupsUnchanged(t *testing.T) {
	p := New(Config{PartitionBufferBytes: 1}) // any page is over budget
	defer p.Close()
	bl := &budgetLog{}
	p.budgetHook = bl.hook
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: localPrefix(t), RowGroupRows: 4000})
	for i := 0; i < 10; i++ {
		write(t, w, batch(testSchema, i*1000, 1000))
	}
	files, err := w.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("files = %d, want 1", len(files))
	}
	if got := fmt.Sprint(readFooter(t, files[0].Path).rowGroups); got != "[4000 4000 2000]" {
		t.Fatalf("row groups %s, want [4000 4000 2000] (the row limit only)", got)
	}
	if n := len(bl.all()); n != 0 {
		t.Fatalf("unpartitioned writer reached the budget hook %d times", n)
	}
}

func TestPartitionBufferBytesEnv(t *testing.T) {
	for _, tc := range []struct {
		val  string
		set  bool
		want int64
		warn bool
	}{
		{"", false, 256 << 20, false},
		{"16777216", true, 16 << 20, false},
		{"8589934592", true, 8 << 30, false},
		{" 33554432 ", true, 32 << 20, false},
		{"16777215", true, 256 << 20, true},
		{"8589934593", true, 256 << 20, true},
		{"0", true, 256 << 20, true},
		{"-1", true, 256 << 20, true},
		{"256MiB", true, 256 << 20, true},
	} {
		t.Run(tc.val, func(t *testing.T) {
			logs := captureLog(t)
			t.Setenv(partitionBufferBytesEnv, "")
			if tc.set {
				t.Setenv(partitionBufferBytesEnv, tc.val)
			}
			dp, err := Factory(context.Background(), datagateway.BootInfo{})
			if err != nil {
				t.Fatal(err)
			}
			defer dp.Close()
			if got := dp.(*Plane).cfg.PartitionBufferBytes; got != tc.want {
				t.Errorf("PartitionBufferBytes = %d, want %d", got, tc.want)
			}
			if got := strings.Contains(logs.String(), partitionBufferBytesEnv); got != tc.warn {
				t.Errorf("warning logged = %v, want %v (%q)", got, tc.warn, logs.String())
			}
		})
	}
	if got := New(Config{}).cfg.PartitionBufferBytes; got != 256<<20 {
		t.Errorf("New default PartitionBufferBytes = %d, want 256 MiB", got)
	}
}

func TestMaxOpenPartitionsDefault16(t *testing.T) {
	if got := New(Config{}).cfg.MaxOpenPartitions; got != 16 {
		t.Errorf("New default MaxOpenPartitions = %d, want 16", got)
	}
	t.Setenv(maxOpenPartitionsEnv, "")
	dp, err := Factory(context.Background(), datagateway.BootInfo{})
	if err != nil {
		t.Fatal(err)
	}
	defer dp.Close()
	if got := dp.(*Plane).cfg.MaxOpenPartitions; got != 16 {
		t.Errorf("Factory default MaxOpenPartitions = %d, want 16", got)
	}
}

// nameSpec is identity(name) over readSchema (id long, name string).
func nameSpec(t *testing.T) iceberg.PartitionSpec {
	t.Helper()
	id := 1000
	spec, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldByName("name", "name", iceberg.IdentityTransform{}, readSchema, &id))
	if err != nil {
		t.Fatal(err)
	}
	return spec
}

func TestEvictionWarningNamesRemedy(t *testing.T) {
	logs := captureLog(t)
	spec := nameSpec(t)
	p := New(Config{MaxOpenPartitions: 2})
	defer p.Close()
	w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: localPrefix(t), Table: "ns.tbl", PartitionSpec: &spec, TableSchema: readSchema})
	write(t, w, nameBatch(0, "a", "b", "c"))
	if _, err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	out := logs.String()
	if strings.Count(out, "partition files open") != 1 {
		t.Fatalf("eviction warnings: %q, want one", out)
	}
	for _, want := range []string{"cluster the input by the partition column(s)", "coarser transform", "cannot be streamed cheaply"} {
		if !strings.Contains(out, want) {
			t.Errorf("eviction warning lacks %q: %s", want, out)
		}
	}
}

func TestManySmallFilesSummaryWarning(t *testing.T) {
	spec := nameSpec(t)
	// With a cap of 2 no name is open when it comes again (it is never one of
	// the previous two), so every one-row batch opens a file. "d" is first
	// seen after the first eviction; a, b, c are revisited after theirs.
	seq := func(n int) []string {
		s := []string{"a", "b", "c", "a", "b", "d"}
		for len(s) < n {
			s = append(s, "c", "a", "b")
		}
		return s[:n]
	}
	for _, n := range []int{16, 17} { // 4 distinct tuples: warn above 16 files
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			logs := captureLog(t)
			p := New(Config{MaxOpenPartitions: 2})
			defer p.Close()
			w := openWriter(t, p, datagateway.WriterSpec{DataPrefix: localPrefix(t), Table: "ns.tbl", PartitionSpec: &spec, TableSchema: readSchema})
			for i, name := range seq(n) {
				write(t, w, nameBatch(i, name))
			}
			files, err := w.Close(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(files) != n {
				t.Fatalf("files = %d, want %d (one per batch)", len(files), n)
			}
			out := logs.String()
			warned := strings.Contains(out, "files for")
			if warned != (n > 16) {
				t.Fatalf("summary warning logged = %v, want %v: %s", warned, n > 16, out)
			}
			if warned {
				want := fmt.Sprintf("table ns.tbl: wrote %d files for 4 distinct partition tuples", n)
				if !strings.Contains(out, want) || !strings.Contains(out, "coarser transform") {
					t.Errorf("summary warning lacks %q or the remedy: %s", want, out)
				}
			}
		})
	}
}

// committedFiles returns the data files of the table's current snapshot by
// path.
func committedFiles(t *testing.T, tb *table.Table) map[string]iceberg.DataFile {
	t.Helper()
	fs, err := tb.FS(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mfs, err := tb.CurrentSnapshot().Manifests(fs)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]iceberg.DataFile{}
	for _, mf := range mfs {
		entries, err := mf.FetchEntries(fs, true)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			out[e.DataFile().FilePath()] = e.DataFile()
		}
	}
	return out
}

func boundOf(t *testing.T, typ iceberg.Type, b []byte) string {
	t.Helper()
	if b == nil {
		return "<none>"
	}
	lit, err := iceberg.LiteralFromBytes(typ, b)
	if err != nil {
		t.Fatal(err)
	}
	return lit.String()
}

// A budget flush followed by close, or by LRU eviction, ends tuple 0's file
// with a 0-row row group; its committed metrics still carry the
// non-partition columns' bounds and null counts, and a row filter on one
// prunes the file.
func TestPartitionBudgetEmptyRowGroupKeepsMetrics(t *testing.T) {
	for _, tc := range []struct {
		name    string
		maxOpen int
		batches [][]int64
	}{
		// Tuple 1's write takes the sum over 16 MiB; tuple 0, the largest,
		// ends its row group and gets no more rows before close.
		{"close", 0, [][]int64{repeatKey(0, 48000), repeatKey(1, 32000)}},
		// Same, then tuple 2 evicts tuple 0 (cap 2).
		{"eviction", 2, [][]int64{repeatKey(0, 48000), repeatKey(1, 32000), repeatKey(2, 100)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pt := newPartTable(t, wideSchema, lakekeeper.PartitionField{SourceColumn: "k", Transform: "identity"})
			pt.p.cfg.PartitionBufferBytes = 16 << 20
			if tc.maxOpen > 0 {
				pt.p.cfg.MaxOpenPartitions = tc.maxOpen
			}
			bl := &budgetLog{}
			pt.p.budgetHook = bl.hook
			rng := rand.New(rand.NewSource(5))
			var recs []arrow.RecordBatch
			next := 0
			for _, keys := range tc.batches {
				recs = append(recs, wideBatch(rng, next, keys))
				next += len(keys)
			}
			files := pt.write(pt.spec(), recs...)
			if len(files) != len(tc.batches) {
				t.Fatalf("files = %d, want %d", len(files), len(tc.batches))
			}
			spec := pt.tbl.Spec()
			var zero string
			for _, f := range files {
				if f.Partition[spec.Field(0).FieldID].(int64) == 0 {
					zero = f.Path
				}
			}
			if rows, empty := rowGroupShape(t, zero); empty == 0 || fmt.Sprint(rows) != "[48000]" {
				t.Fatalf("tuple 0's file has row groups %v and %d empty; want [48000] and a trailing empty one", rows, empty)
			}
			if bl.count("largest") == 0 {
				t.Fatal("no budget flush happened")
			}
			pt.commit(files, icebergjob.WriteModeAppend)

			tb := pt.reload()
			df := committedFiles(t, tb)[zero]
			if df == nil {
				t.Fatalf("%s not committed", zero)
			}
			sc := tb.Schema()
			want := map[string][2]string{
				"id":  {"0", "47999"},
				"cat": {"category-00", "category-15"},
				"num": {"0", fmt.Sprint(47999 * 7)},
			}
			for name, bounds := range want {
				fld, _ := sc.FindFieldByName(name)
				lo := boundOf(t, fld.Type, df.LowerBoundValues()[fld.ID])
				hi := boundOf(t, fld.Type, df.UpperBoundValues()[fld.ID])
				if lo != bounds[0] || hi != bounds[1] {
					t.Errorf("%s bounds [%s, %s], want [%s, %s]", name, lo, hi, bounds[0], bounds[1])
				}
				if n, ok := df.NullValueCounts()[fld.ID]; !ok || n != 0 {
					t.Errorf("%s null count %d (present %v), want 0", name, n, ok)
				}
			}
			blob, _ := sc.FindFieldByName("blob")
			if _, ok := df.LowerBoundValues()[blob.ID]; !ok {
				t.Error("blob has no lower bound")
			}

			planned, ids := pt.scan(iceberg.GreaterThanEqual(iceberg.Reference("id"), int64(48000)))
			if planned != len(files)-1 {
				t.Errorf("id >= 48000 planned %d files, want %d (tuple 0's pruned by its bounds)", planned, len(files)-1)
			}
			if len(ids) != next-48000 {
				t.Errorf("id >= 48000 read %d rows, want %d", len(ids), next-48000)
			}
		})
	}
}
