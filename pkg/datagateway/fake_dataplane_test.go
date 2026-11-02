package datagateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// fakeDataPlane is an in-memory DataPlane for tests of everything above the
// plane (server wiring, gRPC/HTTP handlers). It lives in package datagateway
// as a _test.go file: a separate test-helper package would import datagateway
// while datagateway's own tests import it, which is an import cycle.
//
// It honours the DataPlane lifecycle contract: after Interrupt or Close every
// writer/reader operation fails; Abort discards; an injected write failure
// sticks and fails Close; retained batches are released on writer Close/Abort
// and plane Close.
type fakeDataPlane struct {
	mu sync.Mutex

	writers    []*fakeTableWriter
	readerList []*fakeTableReader // every reader opened, in order
	tables     map[string]fakeTable

	writeErr      error
	openReaderErr error
	dead          bool // Interrupt or Close was called
	interruptedF  bool
	closed        bool
	boot          BootInfo
	factoryCalls  int
	opens         int // OpenTableWriter calls, successful or not

	// rejectLargeTypes makes OpenTableWriter refuse large_string /
	// large_binary columns, as the DuckDB plane does (duckplane selectList).
	rejectLargeTypes bool
	// openAfterInterrupt lets OpenTableWriter and OpenTableReader succeed
	// after Interrupt (not after Close); writers and readers opened then are
	// live.
	openAfterInterrupt bool

	// nextErr, when set, fails a reader's batch Next once that reader has
	// returned nextErrAfter batches (counted across its parts).
	nextErr      error
	nextErrAfter int
	nextErrOnce  bool // the injected Next failure fires once, then clears
	// rawOpenErr, when set, fails every RawFile.Open.
	rawOpenErr error
	// beforeOpenReader, when set, runs at the start of OpenTableReader
	// (before p.mu), so a test can pause an open across a cancel.
	beforeOpenReader func()
	// readerSpecs records the ReaderSpec of every OpenTableReader call.
	readerSpecs []ReaderSpec
}

type fakeTable struct {
	schema *arrow.Schema
	parts  []fakePart
}

// fakePart is one ordered piece of a fake table scan: raw Parquet bytes
// (served as a RawFile, rows is the manifest record count) or decoded batches.
type fakePart struct {
	raw     []byte
	rows    int64
	batches []arrow.RecordBatch
	size    int64                // rawFakePartFunc only
	open    func() io.ReadCloser // rawFakePartFunc only
	files   int                  // decoded part only: tasks the fake reports as started once Next ran (0 = 1)
}

func rawFakePart(b []byte, rows int64) fakePart { return fakePart{raw: b, rows: rows} }

// rawFakePartFunc is a raw part of size bytes whose body comes from open
// (wrapped so reader Close still closes it).
func rawFakePartFunc(size, rows int64, open func() io.ReadCloser) fakePart {
	return fakePart{raw: []byte{}, rows: rows, size: size, open: open}
}

func batchFakePart(batches ...arrow.RecordBatch) fakePart { return fakePart{batches: batches} }

func (fp fakePart) isRaw() bool { return fp.raw != nil }

var errFakePlaneDead = errors.New("fake: data plane interrupted or closed")

func newFakeDataPlane() *fakeDataPlane {
	return &fakeDataPlane{tables: map[string]fakeTable{}}
}

var _ DataPlane = (*fakeDataPlane)(nil)

func (p *fakeDataPlane) OpenTableWriter(_ context.Context, spec WriterSpec) (TableWriter, error) {
	// Context ownership: the writer's lifetime never derives from the Open
	// ctx (a cancelled Open ctx must not poison later Writes); it ends with
	// the plane or an explicit Abort/Close.
	p.mu.Lock()
	defer p.mu.Unlock()
	p.opens++
	if p.closed || (p.dead && !p.openAfterInterrupt) {
		return nil, errFakePlaneDead
	}
	if p.rejectLargeTypes && spec.Schema != nil {
		for _, f := range spec.Schema.Fields() {
			if id := f.Type.ID(); id == arrow.LARGE_STRING || id == arrow.LARGE_BINARY {
				return nil, fmt.Errorf("fake: column %q: unsupported Arrow type %s", f.Name, f.Type)
			}
		}
	}
	w := &fakeTableWriter{plane: p, spec: spec}
	p.writers = append(p.writers, w)
	return w, nil
}

func (p *fakeDataPlane) OpenTableReader(_ context.Context, spec ReaderSpec) (TableReader, error) {
	if p.beforeOpenReader != nil {
		p.beforeOpenReader()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.readerSpecs = append(p.readerSpecs, spec)
	if p.closed || (p.dead && !p.openAfterInterrupt) {
		return nil, errFakePlaneDead
	}
	if p.openReaderErr != nil {
		return nil, p.openReaderErr
	}
	t, ok := p.tables[spec.Namespace+"."+spec.Table]
	if !ok {
		return nil, fmt.Errorf("fake: unknown table %s.%s", spec.Namespace, spec.Table)
	}
	r := &fakeTableReader{plane: p, table: t}
	p.readerList = append(p.readerList, r)
	return r, nil
}

func (p *fakeDataPlane) Interrupt() {
	p.mu.Lock()
	p.interruptedF = true
	p.dead = true
	writers := append([]*fakeTableWriter(nil), p.writers...)
	readers := append([]*fakeTableReader(nil), p.readerList...)
	p.mu.Unlock()
	terminateAll(writers)
	interruptAll(readers)
}

// interruptAll fails every reader's later NextPart, Next and raw Open (each
// under its own lock).
func interruptAll(readers []*fakeTableReader) {
	for _, r := range readers {
		r.mu.Lock()
		r.interrupted = true
		r.mu.Unlock()
	}
}

// readers returns every reader opened so far.
func (p *fakeDataPlane) readers() []*fakeTableReader {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*fakeTableReader(nil), p.readerList...)
}

// failNext makes a reader's batch Next fail with err once that reader has
// returned after batches.
func (p *fakeDataPlane) failNext(err error, after int) {
	p.mu.Lock()
	p.nextErr, p.nextErrAfter, p.nextErrOnce = err, after, false
	p.mu.Unlock()
}

// failNextOnce is failNext for a single (transient) failure: the next Next
// after that one carries on with the following batch.
func (p *fakeDataPlane) failNextOnce(err error, after int) {
	p.mu.Lock()
	p.nextErr, p.nextErrAfter, p.nextErrOnce = err, after, true
	p.mu.Unlock()
}

// failRawOpen makes every RawFile.Open fail with err (nil clears it).
func (p *fakeDataPlane) failRawOpen(err error) {
	p.mu.Lock()
	p.rawOpenErr = err
	p.mu.Unlock()
}

// terminateAll marks each writer terminated and releases its batches, each
// under that writer's own lock (see the locking protocol on fakeTableWriter).
// Callers hold no lock; OpenTableWriter registers writers under p.mu only
// while !dead, so a snapshot taken after dead is set covers every writer.
func terminateAll(writers []*fakeTableWriter) {
	for _, w := range writers {
		w.terminate()
	}
}

// Close marks the plane dead and releases every writer's retained batches
// (idempotent).
func (p *fakeDataPlane) Close() error {
	p.mu.Lock()
	p.closed = true
	p.dead = true
	writers := append([]*fakeTableWriter(nil), p.writers...)
	readers := append([]*fakeTableReader(nil), p.readerList...)
	p.mu.Unlock()
	terminateAll(writers)
	interruptAll(readers)
	return nil
}

// setTable registers the batches a reader of ns.table replays as one decoded
// part (an empty table when there are no batches).
func (p *fakeDataPlane) setTable(ns, table string, schema *arrow.Schema, batches ...arrow.RecordBatch) {
	if len(batches) == 0 {
		p.setTableParts(ns, table, schema)
		return
	}
	p.setTableParts(ns, table, schema, batchFakePart(batches...))
}

// setTableParts registers the ordered parts a reader of ns.table yields; no
// parts is an empty table (NextPart returns io.EOF at once). The fake serves
// the parts exactly as registered, whatever ReaderSpec.RawParquet says.
func (p *fakeDataPlane) setTableParts(ns, table string, schema *arrow.Schema, parts ...fakePart) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tables[ns+"."+table] = fakeTable{schema: schema, parts: parts}
}

// readerStats reports how many readers were opened, how many of them were
// Closed, how many part streams were handed out, and how many of those were
// Closed.
func (p *fakeDataPlane) readerStats() (readers, readersClosed, streams, streamsClosed int) {
	for _, r := range p.readers() {
		r.mu.Lock()
		closed := r.closes > 0
		sts := append([]*fakeRecordStream(nil), r.streams...)
		r.mu.Unlock()
		readers++
		if closed {
			readersClosed++
		}
		for _, st := range sts {
			streams++
			if st.isClosed() {
				streamsClosed++
			}
		}
	}
	return
}

func (p *fakeDataPlane) failWrites(err error) {
	p.mu.Lock()
	p.writeErr = err
	p.mu.Unlock()
}

func (p *fakeDataPlane) failOpenReader(err error) {
	p.mu.Lock()
	p.openReaderErr = err
	p.mu.Unlock()
}

func (p *fakeDataPlane) openCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.opens
}

func (p *fakeDataPlane) interrupted() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.interruptedF
}

func (p *fakeDataPlane) currentWriteErr() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.writeErr
}

// fakeFactory returns a DataPlaneFactory that hands out p and records the
// BootInfo it was called with (see lastBoot).
func fakeFactory(p *fakeDataPlane) DataPlaneFactory {
	return func(_ context.Context, boot BootInfo) (DataPlane, error) {
		p.mu.Lock()
		p.boot = boot
		p.factoryCalls++
		p.mu.Unlock()
		return p, nil
	}
}

// lastBoot returns the BootInfo the factory was last called with.
func (p *fakeDataPlane) lastBoot() BootInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.boot
}

// fakeTableWriter locking protocol: every state change and every check that
// decides an operation's outcome happens under w.mu. Plane Interrupt/Close do
// not read a "plane is dead" flag from inside Write/Close; instead they mark
// each writer terminated, under that writer's w.mu, and release its batches
// there. So an operation either completes before termination (and its batch is
// then released by it) or observes terminated: no Write retains a batch and no
// Close returns files after termination. Lock order: w.mu -> p.mu only; the
// plane never holds p.mu while taking w.mu.
type fakeTableWriter struct {
	plane *fakeDataPlane
	spec  WriterSpec

	// Test hooks, called at the start of Write/Close before w.mu is taken,
	// to pause an operation at the point where a race used to be possible.
	beforeWriteLock func()
	beforeCloseLock func()

	mu         sync.Mutex
	batches    []arrow.RecordBatch // retained until released
	nWrites    int
	rows       int64
	bytes      int64
	failErr    error // first injected write failure; sticks, fails Close
	aborted    bool
	terminated bool // plane Interrupt/Close
	closed     bool
	files      []WrittenFile
	closeErr   error
}

func (w *fakeTableWriter) releaseLocked() {
	for _, b := range w.batches {
		b.Release()
	}
	w.batches = nil
}

// terminate is called by plane Interrupt/Close.
func (w *fakeTableWriter) terminate() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.terminated = true
	w.releaseLocked()
}

func (w *fakeTableWriter) Write(_ context.Context, rec arrow.RecordBatch) error {
	if w.beforeWriteLock != nil {
		w.beforeWriteLock()
	}
	injected := w.plane.currentWriteErr()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.terminated {
		return errFakePlaneDead
	}
	if w.aborted {
		return errors.New("fake: write on aborted writer")
	}
	if w.closed {
		return errors.New("fake: write on closed writer")
	}
	if w.failErr != nil {
		return w.failErr
	}
	if injected != nil {
		w.failErr = injected
		return injected
	}
	rec.Retain() // the caller keeps ownership of rec; the fake keeps its own ref
	w.batches = append(w.batches, rec)
	w.nWrites++
	w.rows += rec.NumRows()
	for _, c := range rec.Columns() {
		for _, buf := range c.Data().Buffers() {
			if buf != nil {
				w.bytes += int64(buf.Len())
			}
		}
	}
	return nil
}

func (w *fakeTableWriter) QueuedBytes() int64 { return 0 }

func (w *fakeTableWriter) Close(_ context.Context) ([]WrittenFile, error) {
	if w.beforeCloseLock != nil {
		w.beforeCloseLock()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return w.files, w.closeErr // idempotent: same result as the first call
	}
	if w.aborted {
		return nil, errors.New("fake: close on aborted writer")
	}
	w.closed = true
	switch {
	case w.terminated:
		w.closeErr = errFakePlaneDead
	case w.failErr != nil:
		w.closeErr = fmt.Errorf("fake: writer failed earlier: %w", w.failErr)
	case w.rows > 0:
		w.files = []WrittenFile{{
			Path:  w.spec.DataPrefix + w.spec.FilePrefix + "-00000.parquet",
			Rows:  w.rows,
			Bytes: w.bytes,
		}}
	}
	w.releaseLocked()
	return w.files, w.closeErr
}

func (w *fakeTableWriter) Abort() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		w.aborted = true
	}
	w.releaseLocked()
}

// recorded returns the batches written so far; it is empty once the writer
// has been closed, aborted or its plane closed (the batches are released).
func (w *fakeTableWriter) recorded() []arrow.RecordBatch {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]arrow.RecordBatch(nil), w.batches...)
}

// fakeTableReader replays a fakeTable part by part and records its own Close
// calls and the Close of every part stream it handed out. Lock order:
// fakeRecordStream.mu -> fakeTableReader.mu -> fakeDataPlane.mu.
type fakeTableReader struct {
	plane *fakeDataPlane
	table fakeTable

	mu          sync.Mutex
	next        int
	served      int // batches returned by this reader's part streams
	closes      int // Close calls
	interrupted bool
	streams     []*fakeRecordStream
	rawOpen     []*fakeRawBody
}

func (r *fakeTableReader) Schema() *arrow.Schema { return r.table.schema }

// liveLocked reports why the reader can no longer serve; the caller holds r.mu.
func (r *fakeTableReader) liveLocked() error {
	if r.interrupted {
		return errFakePlaneDead
	}
	if r.closes > 0 {
		return errors.New("fake: reader is closed")
	}
	return nil
}

func (r *fakeTableReader) NextPart(ctx context.Context) (ReadPart, error) {
	if err := ctx.Err(); err != nil {
		return ReadPart{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.liveLocked(); err != nil {
		return ReadPart{}, err
	}
	if r.next >= len(r.table.parts) {
		return ReadPart{}, io.EOF
	}
	fp := r.table.parts[r.next]
	r.next++
	if fp.isRaw() {
		size := int64(len(fp.raw))
		if fp.open != nil {
			size = fp.size
		}
		return ReadPart{Raw: &RawFile{
			Path: fmt.Sprintf("fake://part-%05d.parquet", r.next-1),
			Rows: fp.rows,
			Size: size,
			Open: func(ctx context.Context) (io.ReadCloser, error) { return r.openRaw(ctx, fp) },
		}}, nil
	}
	st := &fakeRecordStream{reader: r, schema: r.table.schema, batches: fp.batches, files: fp.files}
	r.streams = append(r.streams, st)
	return ReadPart{Records: st, Started: st.tasksStarted}, nil
}

// openRaw hands out the raw bytes; it fails once the reader is closed or
// interrupted, as a real raw Open would after its context is gone.
func (r *fakeTableReader) openRaw(ctx context.Context, fp fakePart) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.liveLocked(); err != nil {
		return nil, err
	}
	r.plane.mu.Lock()
	openErr := r.plane.rawOpenErr
	r.plane.mu.Unlock()
	if openErr != nil {
		return nil, openErr
	}
	var rc io.ReadCloser = io.NopCloser(bytes.NewReader(fp.raw))
	if fp.open != nil {
		rc = fp.open()
	}
	body := &fakeRawBody{rc: rc}
	r.rawOpen = append(r.rawOpen, body)
	return body, nil
}

// Close closes the reader, every part stream and every raw body still open
// (idempotent; every call is counted).
func (r *fakeTableReader) Close() error {
	r.mu.Lock()
	r.closes++
	streams := append([]*fakeRecordStream(nil), r.streams...)
	bodies := append([]*fakeRawBody(nil), r.rawOpen...)
	r.mu.Unlock()
	for _, st := range streams {
		st.Close()
	}
	for _, b := range bodies {
		b.Close()
	}
	return nil
}

// closeCount is the number of Close calls.
func (r *fakeTableReader) closeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closes
}

// fakeRawBody is a raw part body; Read fails once it is closed.
type fakeRawBody struct {
	mu     sync.Mutex
	rc     io.ReadCloser
	closed bool
}

func (b *fakeRawBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, errors.New("fake: read of a closed raw body")
	}
	return b.rc.Read(p)
}

func (b *fakeRawBody) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	return b.rc.Close()
}

func (b *fakeRawBody) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

type fakeRecordStream struct {
	reader  *fakeTableReader
	schema  *arrow.Schema
	batches []arrow.RecordBatch
	files   int // tasks the part stands for (0 = 1); one batch per task

	mu      sync.Mutex
	next    int
	closed  bool
	started atomic.Int64 // tasks started, as a plane would report
}

// tasksStarted: a task starts when its batch is produced; reaching the end
// (even with no batches) starts all of the part's tasks.
func (s *fakeRecordStream) tasksStarted() int { return int(s.started.Load()) }

func (s *fakeRecordStream) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *fakeRecordStream) Schema() *arrow.Schema { return s.schema }

func (s *fakeRecordStream) Next(ctx context.Context) (arrow.RecordBatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, p := s.reader, s.reader.plane
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.liveLocked(); err != nil {
		return nil, err
	}
	if s.closed {
		return nil, errors.New("fake: Next on closed stream")
	}
	p.mu.Lock()
	nextErr, after := p.nextErr, p.nextErrAfter
	if nextErr != nil && r.served >= after && p.nextErrOnce {
		p.nextErr = nil
	}
	p.mu.Unlock()
	if nextErr != nil && r.served >= after {
		return nil, nextErr
	}
	if s.next >= len(s.batches) {
		s.started.Store(int64(max(s.files, 1)))
		return nil, io.EOF
	}
	b := s.batches[s.next]
	s.next++
	s.started.Store(int64(min(max(s.files, 1), s.next)))
	r.served++
	b.Retain() // caller Releases
	return b, nil
}

func (s *fakeRecordStream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

// ---- self-tests ----------------------------------------------------------

func fakeTestBatchMem(t *testing.T, mem memory.Allocator, ids ...int64) (*arrow.Schema, arrow.RecordBatch) {
	t.Helper()
	sch := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	b := array.NewInt64Builder(mem)
	defer b.Release()
	b.AppendValues(ids, nil)
	arr := b.NewArray()
	defer arr.Release()
	return sch, array.NewRecordBatch(sch, []arrow.Array{arr}, int64(len(ids)))
}

func fakeTestBatch(t *testing.T, ids ...int64) (*arrow.Schema, arrow.RecordBatch) {
	return fakeTestBatchMem(t, memory.NewGoAllocator(), ids...)
}

// fakeInt32Batch is a 3-row int32 batch (narrower than fakeTestBatch's int64).
func fakeInt32Batch(t *testing.T) arrow.RecordBatch {
	t.Helper()
	sch := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	b := array.NewInt32Builder(memory.NewGoAllocator())
	defer b.Release()
	b.AppendValues([]int32{1, 2, 3}, nil)
	arr := b.NewArray()
	defer arr.Release()
	return array.NewRecordBatch(sch, []arrow.Array{arr}, 3)
}

func TestFakeDataPlane_WriteCloseFiles(t *testing.T) {
	p := newFakeDataPlane()
	defer p.Close()
	sch, rec := fakeTestBatch(t, 1, 2, 3)
	defer rec.Release()

	w, err := p.OpenTableWriter(context.Background(), WriterSpec{
		DataPrefix: "s3://b/t/data/", FilePrefix: "part-abcd1234-w1", Schema: sch,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := w.Write(context.Background(), rec); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if n := len(p.writers[0].recorded()); n != 2 {
		t.Errorf("recorded batches = %d, want 2", n)
	}
	files, err := w.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "s3://b/t/data/part-abcd1234-w1-00000.parquet" || files[0].Rows != 6 {
		t.Fatalf("files = %+v", files)
	}
	again, err := w.Close(context.Background())
	if err != nil || len(again) != 1 || again[0] != files[0] {
		t.Errorf("second Close = %+v, %v; want identical %+v", again, err, files)
	}
	w.Abort() // safe after Close
	w.Abort() // idempotent
	if again, err := w.Close(context.Background()); err != nil || len(again) != 1 {
		t.Errorf("Close after Abort-after-Close = %+v, %v; want unchanged result", again, err)
	}
}

// Bytes is the sum of the Arrow buffer lengths, so element width matters.
func TestFakeDataPlane_BytesSumBufferLengths(t *testing.T) {
	p := newFakeDataPlane()
	defer p.Close()

	sch64, rec64 := fakeTestBatch(t, 1, 2, 3)
	defer rec64.Release()
	rec32 := fakeInt32Batch(t)
	defer rec32.Release()

	bytesOf := func(sch *arrow.Schema, rec arrow.RecordBatch) int64 {
		t.Helper()
		w, _ := p.OpenTableWriter(context.Background(), WriterSpec{Schema: sch})
		for i := 0; i < 2; i++ {
			if err := w.Write(context.Background(), rec); err != nil {
				t.Fatal(err)
			}
		}
		files, err := w.Close(context.Background())
		if err != nil || len(files) != 1 {
			t.Fatalf("Close = %+v, %v", files, err)
		}
		return files[0].Bytes
	}
	var want64, want32 int64
	for _, b := range rec64.Column(0).Data().Buffers() {
		if b != nil {
			want64 += int64(b.Len())
		}
	}
	for _, b := range rec32.Column(0).Data().Buffers() {
		if b != nil {
			want32 += int64(b.Len())
		}
	}
	got64, got32 := bytesOf(sch64, rec64), bytesOf(rec32.Schema(), rec32)
	if got64 != 2*want64 || got32 != 2*want32 {
		t.Errorf("bytes int64=%d int32=%d, want %d / %d", got64, got32, 2*want64, 2*want32)
	}
	if got64 < 2*3*8 || got32 < 2*3*4 || got64 <= got32 {
		t.Errorf("bytes must reflect element width (>= rows*width, int64 > int32): int64=%d int32=%d", got64, got32)
	}
}

func TestFakeDataPlane_ReaderReplay(t *testing.T) {
	p := newFakeDataPlane()
	defer p.Close()
	sch, a := fakeTestBatch(t, 1, 2)
	_, b := fakeTestBatch(t, 3)
	defer a.Release()
	defer b.Release()
	p.setTable("raw", "orders", sch, a, b)

	tr, err := p.OpenTableReader(context.Background(), ReaderSpec{Namespace: "raw", Table: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if !tr.Schema().Equal(sch) {
		t.Errorf("schema mismatch")
	}
	part, err := tr.NextPart(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if part.Raw != nil || part.Records == nil {
		t.Fatalf("part = %+v, want decoded records only", part)
	}
	if !part.Records.Schema().Equal(sch) {
		t.Errorf("part schema mismatch")
	}
	var rows int64
	for {
		rec, err := part.Records.Next(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		rows += rec.NumRows()
		rec.Release()
	}
	if rows != 3 {
		t.Errorf("replayed rows = %d, want 3", rows)
	}
	if _, err := tr.NextPart(context.Background()); err != io.EOF {
		t.Errorf("NextPart after last = %v, want io.EOF", err)
	}
	tr.Close()
	if _, err := part.Records.Next(context.Background()); err == nil || err == io.EOF {
		t.Errorf("part Next after reader Close = %v, want failure", err)
	}
	if _, err := tr.NextPart(context.Background()); err == nil || err == io.EOF {
		t.Errorf("NextPart after reader Close = %v, want failure", err)
	}
	if _, err := p.OpenTableReader(context.Background(), ReaderSpec{Namespace: "raw", Table: "nope"}); err == nil {
		t.Error("unknown table: want error")
	}
}

func TestFakeDataPlane_ReaderPartsOrderAndEOF(t *testing.T) {
	p := newFakeDataPlane()
	defer p.Close()
	sch, a := fakeTestBatch(t, 1, 2)
	_, c := fakeTestBatch(t, 5)
	defer a.Release()
	defer c.Release()
	p.setTableParts("raw", "mixed", sch,
		batchFakePart(a),
		rawFakePart([]byte("PAR1-raw-bytes"), 7),
		batchFakePart(c),
	)

	tr, err := p.OpenTableReader(context.Background(), ReaderSpec{Namespace: "raw", Table: "mixed", RawParquet: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	var order []string
	for {
		part, err := tr.NextPart(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if (part.Raw == nil) == (part.Records == nil) {
			t.Fatalf("part %+v: exactly one of Raw and Records must be set", part)
		}
		if part.Raw != nil {
			order = append(order, "raw")
			if part.Raw.Rows != 7 || part.Raw.Size != int64(len("PAR1-raw-bytes")) {
				t.Errorf("raw Rows/Size = %d/%d", part.Raw.Rows, part.Raw.Size)
			}
			rc, err := part.Raw.Open(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			got, _ := io.ReadAll(rc)
			rc.Close()
			if string(got) != "PAR1-raw-bytes" {
				t.Errorf("raw bytes = %q", got)
			}
			continue
		}
		order = append(order, "records")
		rec, err := part.Records.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		rec.Release()
		part.Records.Close()
	}
	if got := strings.Join(order, ","); got != "records,raw,records" {
		t.Errorf("part order = %s", got)
	}
	if _, err := tr.NextPart(context.Background()); err != io.EOF {
		t.Errorf("NextPart after EOF = %v, want io.EOF again", err)
	}
}

func TestFakeDataPlane_ReaderEmptyTable(t *testing.T) {
	p := newFakeDataPlane()
	defer p.Close()
	sch, _ := fakeTestBatch(t)
	p.setTableParts("raw", "empty", sch)
	tr, err := p.OpenTableReader(context.Background(), ReaderSpec{Namespace: "raw", Table: "empty"})
	if err != nil {
		t.Fatal(err)
	}
	if !tr.Schema().Equal(sch) {
		t.Error("empty table must still carry its schema")
	}
	if _, err := tr.NextPart(context.Background()); err != io.EOF {
		t.Errorf("NextPart on empty table = %v, want io.EOF", err)
	}
	tr.Close()
}

func TestFakeDataPlane_ReaderRecordsCloses(t *testing.T) {
	p := newFakeDataPlane()
	defer p.Close()
	sch, a := fakeTestBatch(t, 1)
	defer a.Release()
	p.setTableParts("raw", "t", sch, batchFakePart(a), batchFakePart(a))

	tr, _ := p.OpenTableReader(context.Background(), ReaderSpec{Namespace: "raw", Table: "t"})
	first, err := tr.NextPart(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.NextPart(context.Background()); err != nil {
		t.Fatal(err)
	}
	first.Records.Close() // the caller closes a finished part stream itself
	rd, rdClosed, st, stClosed := p.readerStats()
	if rd != 1 || rdClosed != 0 || st != 2 || stClosed != 1 {
		t.Errorf("stats before Close = %d/%d/%d/%d, want 1/0/2/1", rd, rdClosed, st, stClosed)
	}
	// Reader Close closes the stream the caller left open.
	tr.Close()
	tr.Close() // idempotent
	rd, rdClosed, st, stClosed = p.readerStats()
	if rd != 1 || rdClosed != 1 || st != 2 || stClosed != 2 {
		t.Errorf("stats after Close = %d/%d/%d/%d, want 1/1/2/2", rd, rdClosed, st, stClosed)
	}
}

func TestFakeDataPlane_RawOpenAfterCloseFails(t *testing.T) {
	p := newFakeDataPlane()
	defer p.Close()
	sch, _ := fakeTestBatch(t)
	p.setTableParts("raw", "t", sch, rawFakePart([]byte("x"), 1))
	tr, _ := p.OpenTableReader(context.Background(), ReaderSpec{Namespace: "raw", Table: "t", RawParquet: true})
	part, err := tr.NextPart(context.Background())
	if err != nil || part.Raw == nil {
		t.Fatalf("NextPart = %+v, %v", part, err)
	}
	tr.Close()
	if rc, err := part.Raw.Open(context.Background()); err == nil {
		rc.Close()
		t.Error("raw Open after reader Close: want error")
	}
}

func TestFakeDataPlane_FailureInjectionAndInterrupt(t *testing.T) {
	p := newFakeDataPlane()
	defer p.Close()
	sch, rec := fakeTestBatch(t, 1)
	defer rec.Release()

	w, _ := p.OpenTableWriter(context.Background(), WriterSpec{Schema: sch})
	boom := errors.New("boom")
	p.failWrites(boom)
	if err := w.Write(context.Background(), rec); !errors.Is(err, boom) {
		t.Errorf("Write err = %v, want boom", err)
	}
	p.failWrites(nil)

	p.setTable("raw", "t", sch)
	p.failOpenReader(boom)
	if _, err := p.OpenTableReader(context.Background(), ReaderSpec{Namespace: "raw", Table: "t"}); !errors.Is(err, boom) {
		t.Errorf("OpenTableReader err = %v, want boom", err)
	}

	if p.interrupted() {
		t.Error("interrupted before Interrupt()")
	}
	p.Interrupt()
	if !p.interrupted() {
		t.Error("not interrupted after Interrupt()")
	}
}

// An injected write failure sticks: later Writes keep failing and Close fails
// with no files, even after the injection is cleared.
func TestFakeDataPlane_InjectedFailureFailsClose(t *testing.T) {
	p := newFakeDataPlane()
	defer p.Close()
	sch, rec := fakeTestBatch(t, 1, 2)
	defer rec.Release()

	w, _ := p.OpenTableWriter(context.Background(), WriterSpec{DataPrefix: "x/", FilePrefix: "f", Schema: sch})
	if err := w.Write(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	p.failWrites(boom)
	if err := w.Write(context.Background(), rec); !errors.Is(err, boom) {
		t.Fatalf("Write = %v, want boom", err)
	}
	p.failWrites(nil)
	if err := w.Write(context.Background(), rec); !errors.Is(err, boom) {
		t.Errorf("Write after clearing injection = %v, want the sticky failure", err)
	}
	files, err := w.Close(context.Background())
	if !errors.Is(err, boom) || len(files) != 0 {
		t.Errorf("Close = %+v, %v; want boom and no files", files, err)
	}
	if again, err2 := w.Close(context.Background()); err2 == nil || len(again) != 0 {
		t.Errorf("second Close = %+v, %v; want the same failure", again, err2)
	}
}

func TestFakeDataPlane_WriteAbortClose(t *testing.T) {
	p := newFakeDataPlane()
	defer p.Close()
	sch, rec := fakeTestBatch(t, 1)
	defer rec.Release()

	w, _ := p.OpenTableWriter(context.Background(), WriterSpec{DataPrefix: "x/", FilePrefix: "f", Schema: sch})
	if err := w.Write(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	w.Abort()
	w.Abort() // idempotent
	if err := w.Write(context.Background(), rec); err == nil {
		t.Error("Write after Abort: want error")
	}
	if files, err := w.Close(context.Background()); err == nil || len(files) != 0 {
		t.Errorf("Close after Abort = %+v, %v; want error and no files", files, err)
	}
}

func TestFakeDataPlane_InterruptAndCloseTerminate(t *testing.T) {
	p := newFakeDataPlane()
	defer p.Close()
	sch, rec := fakeTestBatch(t, 1)
	defer rec.Release()
	p.setTable("raw", "t", sch, rec)

	w, _ := p.OpenTableWriter(context.Background(), WriterSpec{DataPrefix: "x/", FilePrefix: "f", Schema: sch})
	if err := w.Write(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	rs, err := p.OpenTableReader(context.Background(), ReaderSpec{Namespace: "raw", Table: "t"})
	if err != nil {
		t.Fatal(err)
	}
	part, err := rs.NextPart(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	p.Interrupt()
	if err := w.Write(context.Background(), rec); err == nil {
		t.Error("Write after Interrupt: want error")
	}
	if files, err := w.Close(context.Background()); err == nil || len(files) != 0 {
		t.Errorf("Close after Interrupt = %+v, %v; want error and no files", files, err)
	}
	if _, err := part.Records.Next(context.Background()); err == nil || err == io.EOF {
		t.Errorf("part Next after Interrupt = %v, want failure", err)
	}
	if _, err := rs.NextPart(context.Background()); err == nil || err == io.EOF {
		t.Errorf("NextPart after Interrupt = %v, want failure", err)
	}
	if _, err := p.OpenTableWriter(context.Background(), WriterSpec{Schema: sch}); err == nil {
		t.Error("OpenTableWriter after Interrupt: want error")
	}

	// Plane Close terminates a fresh plane's writers/readers the same way.
	p2 := newFakeDataPlane()
	p2.setTable("raw", "t", sch, rec)
	w2, _ := p2.OpenTableWriter(context.Background(), WriterSpec{Schema: sch})
	rs2, _ := p2.OpenTableReader(context.Background(), ReaderSpec{Namespace: "raw", Table: "t"})
	p2.Close()
	p2.Close() // idempotent
	if err := w2.Write(context.Background(), rec); err == nil {
		t.Error("Write after plane Close: want error")
	}
	if _, err := rs2.NextPart(context.Background()); err == nil || err == io.EOF {
		t.Errorf("NextPart after plane Close = %v, want failure", err)
	}
}

// Retained batches are released by writer Close, writer Abort and plane Close
// (each idempotent): once the caller has released its own refs, nothing stays
// allocated.
func TestFakeDataPlane_ReleasesRetainedBatches(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cleanup func(p *fakeDataPlane, w TableWriter)
	}{
		{"writer Close", func(_ *fakeDataPlane, w TableWriter) {
			w.Close(context.Background())
			w.Close(context.Background())
		}},
		{"writer Abort", func(_ *fakeDataPlane, w TableWriter) { w.Abort(); w.Abort() }},
		{"plane Close", func(p *fakeDataPlane, _ TableWriter) { p.Close(); p.Close() }},
		{"plane Interrupt then writer Close", func(p *fakeDataPlane, w TableWriter) {
			p.Interrupt()
			w.Close(context.Background())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			p := newFakeDataPlane()
			defer p.Close()
			sch, rec := fakeTestBatchMem(t, mem, 1, 2, 3)
			w, _ := p.OpenTableWriter(context.Background(), WriterSpec{Schema: sch})
			for i := 0; i < 3; i++ {
				if err := w.Write(context.Background(), rec); err != nil {
					t.Fatal(err)
				}
			}
			rec.Release() // caller's own reference
			if mem.CurrentAlloc() == 0 {
				t.Fatal("writer should still retain the batches before cleanup")
			}
			tc.cleanup(p, w)
			// Assert right here: the deferred plane Close above is only a
			// fallback and must not be what releases the batches.
			mem.AssertSize(t, 0)
		})
	}
}

func TestFakeDataPlane_FactoryRecordsBootInfo(t *testing.T) {
	p := newFakeDataPlane()
	defer p.Close()
	want := BootInfo{RunID: "r1", LakekeeperURL: "http://lk", ProjectID: "p", Warehouse: "w", Token: "jwt"}
	plane, err := fakeFactory(p)(context.Background(), want)
	if err != nil || plane != DataPlane(p) {
		t.Fatalf("factory = %v, %v", plane, err)
	}
	if got := p.lastBoot(); got != want {
		t.Errorf("BootInfo = %+v, want %+v", got, want)
	}
}

// Context ownership: a writer opened with an already-cancelled ctx must still
// accept later Writes, because its lifetime is the plane's, not the Open call's.
func TestFakeDataPlane_WriterSurvivesCancelledOpenContext(t *testing.T) {
	p := newFakeDataPlane()
	defer p.Close()
	sch, rec := fakeTestBatch(t, 1)
	defer rec.Release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w, err := p.OpenTableWriter(ctx, WriterSpec{Schema: sch})
	if err != nil {
		t.Fatalf("OpenTableWriter with cancelled ctx: %v", err)
	}
	if err := w.Write(context.Background(), rec); err != nil {
		t.Errorf("Write after cancelled Open ctx: %v", err)
	}
	if _, err := w.Close(context.Background()); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// BootInfo carries the raw run JWT; no default fmt verb may print it.
func TestBootInfo_StringRedactsToken(t *testing.T) {
	const secret = "eyJhbGciOiJSUzI1NiJ9.super-secret-payload.sig"
	b := BootInfo{RunID: "run-1", LakekeeperURL: "http://lk", ProjectID: "proj", Warehouse: "wh", Token: secret}
	for _, verb := range []string{"%v", "%+v", "%s"} {
		out := fmt.Sprintf(verb, b)
		if strings.Contains(out, secret) || strings.Contains(out, "super-secret") {
			t.Errorf("%s leaks the token: %s", verb, out)
		}
		if !strings.Contains(out, "run-1") || !strings.Contains(out, "<redacted>") {
			t.Errorf("%s = %q, want run id and redaction marker", verb, out)
		}
	}
	if out := fmt.Sprintf("%v", &b); strings.Contains(out, secret) {
		t.Errorf("%%v of *BootInfo leaks the token: %s", out)
	}
}

// Plane termination racing a Write that has already passed its pre-lock point:
// the Write must fail and retain nothing (the old protocol checked plane state
// before taking the writer lock, so it could succeed after cleanup).
func TestFakeDataPlane_TerminationRacesWrite(t *testing.T) {
	for _, term := range []struct {
		name string
		fn   func(p *fakeDataPlane)
	}{
		{"Close", func(p *fakeDataPlane) { p.Close() }},
		{"Interrupt", func(p *fakeDataPlane) { p.Interrupt() }},
	} {
		t.Run(term.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			p := newFakeDataPlane()
			defer p.Close()
			sch, rec := fakeTestBatchMem(t, mem, 1, 2, 3)
			wi, _ := p.OpenTableWriter(context.Background(), WriterSpec{Schema: sch})
			w := wi.(*fakeTableWriter)

			entered, resume := make(chan struct{}), make(chan struct{})
			w.beforeWriteLock = func() { close(entered); <-resume }
			res := make(chan error, 1)
			go func() { res <- w.Write(context.Background(), rec) }()

			<-entered
			term.fn(p) // terminate while Write is paused before its critical section
			close(resume)

			if err := <-res; err == nil {
				t.Error("Write that raced plane termination succeeded")
			}
			if n := len(w.recorded()); n != 0 {
				t.Errorf("writer retains %d batches after termination", n)
			}
			rec.Release()
			mem.AssertSize(t, 0)
		})
	}
}

// Same for Close: a Close paused before its critical section must not return
// files once the plane has been terminated.
func TestFakeDataPlane_TerminationRacesClose(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	p := newFakeDataPlane()
	defer p.Close()
	sch, rec := fakeTestBatchMem(t, mem, 1, 2, 3)
	wi, _ := p.OpenTableWriter(context.Background(), WriterSpec{DataPrefix: "x/", FilePrefix: "f", Schema: sch})
	w := wi.(*fakeTableWriter)
	if err := w.Write(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	rec.Release()

	entered, resume := make(chan struct{}), make(chan struct{})
	w.beforeCloseLock = func() { close(entered); <-resume }
	type result struct {
		files []WrittenFile
		err   error
	}
	res := make(chan result, 1)
	go func() {
		f, err := w.Close(context.Background())
		res <- result{f, err}
	}()

	<-entered
	p.Close()
	close(resume)

	r := <-res
	if r.err == nil || len(r.files) != 0 {
		t.Errorf("Close that raced plane termination = %+v, %v; want error and no files", r.files, r.err)
	}
	mem.AssertSize(t, 0)
}

// Stress under -race: writers hammer Write while the plane is closed. Every
// Write that returned nil must have happened before termination, and after
// plane Close returns, nothing is retained and every further Write fails.
func TestFakeDataPlane_ConcurrentWritesAndClose(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	p := newFakeDataPlane()
	sch, rec := fakeTestBatchMem(t, mem, 1, 2, 3)

	const nWriters = 8
	var wg sync.WaitGroup
	writers := make([]TableWriter, nWriters)
	for i := range writers {
		writers[i], _ = p.OpenTableWriter(context.Background(), WriterSpec{Schema: sch})
	}
	start := make(chan struct{})
	for _, w := range writers {
		wg.Add(1)
		go func(w TableWriter) {
			defer wg.Done()
			<-start
			for i := 0; i < 2000; i++ {
				if err := w.Write(context.Background(), rec); err != nil {
					return
				}
			}
		}(w)
	}
	close(start)
	p.Close()
	// Close has returned: no writer may retain a batch or accept another.
	for i, w := range writers {
		if n := len(w.(*fakeTableWriter).recorded()); n != 0 {
			t.Errorf("writer %d retains %d batches after plane Close returned", i, n)
		}
		if err := w.Write(context.Background(), rec); err == nil {
			t.Errorf("writer %d accepted a Write after plane Close returned", i)
		}
		if files, err := w.Close(context.Background()); err == nil || len(files) != 0 {
			t.Errorf("writer %d Close after plane Close = %+v, %v", i, files, err)
		}
	}
	wg.Wait()
	rec.Release()
	mem.AssertSize(t, 0)
}
