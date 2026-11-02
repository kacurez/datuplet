package goplane

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"log"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/datuplet/datuplet/pkg/datagateway"
)

// Writer constants (spec, Write path 4–5).
const (
	defaultTargetFileBytes = 128 << 20
	defaultRowGroupRows    = 229376
	rotationFactor         = 4 // a file ends once 4 × TargetFileBytes uncompressed Arrow bytes were fed into it
	channelBatches         = 4
)

var (
	errAborted     = errors.New("goplane: writer aborted")
	errClosed      = errors.New("goplane: writer closed")
	errInterrupted = errors.New("goplane: writer interrupted")
)

// writer is one TableWriter: Write queues batches on a channel of
// channelBatches; one goroutine (run) encodes them into the current file or,
// for a partitioned table, splits each dequeued batch by partition tuple
// into up to maxOpen open files (spec decisions 8-9).
type writer struct {
	spec        datagateway.WriterSpec
	specID      int
	part        *partitioner // nil: unpartitioned, one file at a time (cur)
	maxOpen     int
	local       bool
	openSink    sinkOpener
	writeSchema *arrow.Schema // spec.Schema without PARQUET:field_id metadata
	props       *parquet.WriterProperties
	arrowProps  pqarrow.ArrowWriterProperties

	ctx    context.Context // lifetime; its cause is the writer's failure
	cancel context.CancelCauseFunc
	ch     chan arrow.RecordBatch // never closed; admission ends via closing
	done   chan struct{}
	queued atomic.Int64

	// Admission. mu is never held while blocked: senders register in
	// sending, then select on the send, closing, the lifetime and their ctx.
	// Once closing is closed no new sender registers, and run drains ch only
	// after sending reached zero, so no batch is left behind in ch.
	mu        sync.Mutex
	idle      *sync.Cond // signalled when sending drops to zero
	sending   int
	closing   chan struct{}
	isClosing bool

	// Owned by run until done is closed, then read-only.
	cur   *dataFile
	files []datagateway.WrittenFile
	err   error

	// Partitioned writers only; owned by run.
	open    map[string]*dataFile // by tuple key
	tick    uint64               // LRU clock
	nextSeq int                  // writer-wide file sequence
	budget  int64                // row-group bytes shared by the open files
	hook    func(budgetEvent)    // nil in production
	seed    maphash.Seed
	tuples  map[uint64]struct{} // hashes of every tuple key seen (lifetime)
	evicted bool
}

// budgetEvent is what the test hook sees of the row-group budget, from
// inside run: a flush about to happen ("share": the file about to be written
// holds at least its fair share; "largest": the open files hold more than
// the budget after a write) or the state after a sub-batch ("enforced").
type budgetEvent struct {
	site    string
	path    string // the file whose row group ends ("share", "largest")
	writing string // the file the sub-batch went to
	sum     int64  // buffered page bytes of all open files ("enforced")
	budget  int64
	open    int
}

func newWriter(parent context.Context, spec datagateway.WriterSpec, local bool, open sinkOpener, part *partitioner, maxOpen int, budget int64, hook func(budgetEvent)) *writer {
	ctx, cancel := context.WithCancelCause(parent)
	w := &writer{
		spec:        spec,
		part:        part,
		maxOpen:     maxOpen,
		budget:      budget,
		hook:        hook,
		local:       local,
		openSink:    open,
		writeSchema: stripFieldIDMetadata(spec.Schema),
		props: parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Zstd),
			parquet.WithDictionaryDefault(true),
			parquet.WithStats(true),
			parquet.WithMaxRowGroupLength(spec.RowGroupRows),
		),
		arrowProps: pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()),
		ctx:        ctx,
		cancel:     cancel,
		ch:         make(chan arrow.RecordBatch, channelBatches),
		done:       make(chan struct{}),
		closing:    make(chan struct{}),
	}
	w.idle = sync.NewCond(&w.mu)
	if spec.PartitionSpec != nil {
		w.specID = spec.PartitionSpec.ID()
	}
	if part != nil {
		w.open = map[string]*dataFile{}
		w.seed = maphash.MakeSeed()
		w.tuples = map[uint64]struct{}{}
	}
	go w.run()
	return w
}

// QueuedBytes is the size of the batches waiting in the channel. The count is
// added after a send lands and subtracted when run dequeues, so it can dip
// below zero for an instant; it is reported as zero then.
func (w *writer) QueuedBytes() int64 { return max(0, w.queued.Load()) }

// Write queues rec (retaining it) and blocks while the channel is full. ctx
// bounds only this call.
func (w *writer) Write(ctx context.Context, rec arrow.RecordBatch) error {
	if err := w.checkRecord(rec); err != nil {
		return err
	}
	if w.ctx.Err() != nil {
		return w.failure()
	}
	w.mu.Lock()
	if w.isClosing {
		w.mu.Unlock()
		return errClosed
	}
	w.sending++
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		if w.sending--; w.sending == 0 {
			w.idle.Broadcast()
		}
		w.mu.Unlock()
	}()

	size := recordBytes(rec)
	rec.Retain()
	select {
	case w.ch <- rec:
		w.queued.Add(size)
		return nil
	case <-w.ctx.Done():
		rec.Release()
		return w.failure()
	case <-w.closing:
		rec.Release()
		return errClosed
	case <-ctx.Done():
		rec.Release()
		return ctx.Err()
	}
}

// Close flushes the last file and returns every file written. A second call
// returns the same result. ctx bounds only the wait.
func (w *writer) Close(ctx context.Context) ([]datagateway.WrittenFile, error) {
	w.stopAdmission()
	select {
	case <-w.done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if w.err != nil {
		return nil, w.err
	}
	return w.files, nil
}

// Abort discards the writer: the in-flight upload is aborted and a partial
// local file removed. Idempotent; a no-op after a completed Close.
func (w *writer) Abort() {
	select {
	case <-w.done:
		return
	default:
	}
	w.cancel(errAborted)
	w.stopAdmission()
	<-w.done
}

// stopAdmission makes every pending and later Write fail with errClosed.
func (w *writer) stopAdmission() {
	w.mu.Lock()
	if !w.isClosing {
		w.isClosing = true
		close(w.closing)
	}
	w.mu.Unlock()
}

// drain stops admission, waits until no sender is inside Write, and then
// returns the batches still in the channel (nothing can be added after).
func (w *writer) drain() []arrow.RecordBatch {
	w.stopAdmission()
	w.mu.Lock()
	for w.sending > 0 {
		w.idle.Wait()
	}
	w.mu.Unlock()
	var out []arrow.RecordBatch
	for {
		select {
		case rec := <-w.ch:
			out = append(out, rec)
		default:
			return out
		}
	}
}

// failure is the error a cancelled lifetime context stands for.
func (w *writer) failure() error {
	cause := context.Cause(w.ctx)
	if cause == context.Canceled {
		return errInterrupted // plane Interrupt / Close
	}
	return cause
}

func (w *writer) checkRecord(rec arrow.RecordBatch) error {
	if rec == nil {
		return errors.New("goplane: nil record")
	}
	if int(rec.NumCols()) != w.writeSchema.NumFields() {
		return fmt.Errorf("goplane: record has %d columns, writer schema %d", rec.NumCols(), w.writeSchema.NumFields())
	}
	for i, f := range w.writeSchema.Fields() {
		if got := rec.Column(i).DataType(); !arrow.TypeEqual(got, f.Type) {
			return fmt.Errorf("goplane: record column %d has type %s, writer schema %s", i, got, f.Type)
		}
	}
	return nil
}

func (w *writer) run() {
	defer close(w.done)
	for {
		select {
		case rec := <-w.ch:
			w.process(rec)
		case <-w.closing: // Close or Abort
			for _, rec := range w.drain() {
				w.process(rec)
			}
			w.finishLast()
			return
		case <-w.ctx.Done(): // failure, Abort, plane Close / Interrupt
			w.fail(w.failure())
			for _, rec := range w.drain() {
				w.queued.Add(-recordBytes(rec))
				rec.Release()
			}
			return
		}
	}
}

func (w *writer) process(rec arrow.RecordBatch) {
	w.queued.Add(-recordBytes(rec)) // no longer waiting in the channel
	if w.err == nil && w.ctx.Err() != nil {
		w.fail(w.failure())
	}
	if w.err == nil {
		if err := w.writeRecord(rec); err != nil {
			w.fail(err)
		}
	}
	rec.Release()
}

// fail records the first error, cancels the lifetime context (unblocking
// Write) and aborts every in-flight file, concurrently.
func (w *writer) fail(err error) {
	if w.err == nil {
		w.err = err
	}
	w.cancel(err)
	var files []*dataFile
	if w.cur != nil {
		files = append(files, w.cur)
		w.cur = nil
	}
	for key, f := range w.open {
		files = append(files, f)
		delete(w.open, key)
	}
	abortAll(files)
}

// abortAll aborts files concurrently, so the cleanup of many open uploads
// (each S3 abort bounded on its own) fits plane Close's wait.
func abortAll(files []*dataFile) {
	if len(files) == 1 {
		files[0].abort()
		return
	}
	var wg sync.WaitGroup
	for _, f := range files {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.abort()
		}()
	}
	wg.Wait()
}

func (w *writer) finishLast() {
	if w.err == nil && w.ctx.Err() != nil {
		w.fail(w.failure())
	}
	if w.err != nil {
		return
	}
	if w.part != nil {
		w.finishOpen()
		return
	}
	if w.cur == nil {
		return
	}
	f := w.cur
	w.cur = nil
	if err := w.finish(f); err != nil {
		w.fail(err)
		return
	}
	w.cancel(errClosed) // release the lifetime context; the writer is done
}

func (w *writer) writeRecord(rec arrow.RecordBatch) error {
	if rec.NumRows() == 0 {
		return nil
	}
	if w.part != nil {
		return w.writePartitioned(rec)
	}
	if w.cur == nil {
		f, err := w.openFile(len(w.files))
		if err != nil {
			return err
		}
		w.cur = f
	}
	out := rec
	if !rec.Schema().Equal(w.writeSchema) {
		// Same columns under the stripped schema (no data copy).
		out = array.NewRecordBatch(w.writeSchema, rec.Columns(), rec.NumRows())
		defer out.Release()
	}
	if err := w.cur.fw.WriteBuffered(out); err != nil {
		return fmt.Errorf("goplane: encode %s: %w", w.cur.path, err)
	}
	w.cur.rows += rec.NumRows()
	w.cur.fed += recordBytes(rec)
	if w.cur.fed >= rotationFactor*w.spec.TargetFileBytes {
		f := w.cur
		w.cur = nil
		return w.finish(f)
	}
	return nil
}

// finishOpen finishes every open partition file, in the order they were
// opened; the first error aborts the rest.
func (w *writer) finishOpen() {
	files := make([]*dataFile, 0, len(w.open))
	for _, f := range w.open {
		files = append(files, f)
	}
	slices.SortFunc(files, func(a, b *dataFile) int { return a.seq - b.seq })
	for _, f := range files {
		delete(w.open, f.key)
		if err := w.finish(f); err != nil {
			w.fail(err)
			return
		}
	}
	if len(w.files) > 4*len(w.tuples) { // spec decision 9: more than 4 files per distinct tuple
		log.Printf("goplane: warning: table %s: wrote %d files for %d distinct partition tuples; "+
			"cluster the input by the partition column(s) or use a coarser transform",
			w.spec.Table, len(w.files), len(w.tuples))
	}
	w.cancel(errClosed) // release the lifetime context; the writer is done
}

// writePartitioned splits rec by partition tuple (first-seen order) and
// writes each sub-batch to its tuple's open file.
func (w *writer) writePartitioned(rec arrow.RecordBatch) error {
	groups, err := w.part.split(rec)
	if err != nil {
		return err
	}
	out := rec
	if !rec.Schema().Equal(w.writeSchema) {
		out = array.NewRecordBatch(w.writeSchema, rec.Columns(), rec.NumRows())
		defer out.Release()
	}
	parentBytes := recordBytes(rec)
	for _, g := range groups {
		if err := w.writeGroup(out, g, parentBytes); err != nil {
			return err
		}
	}
	return nil
}

func (w *writer) writeGroup(rec arrow.RecordBatch, g *rowGroup, parentBytes int64) error {
	sub, fed, err := subBatch(w.ctx, rec, g, parentBytes)
	if err != nil {
		return err
	}
	defer sub.Release()
	f := w.open[g.key]
	if f == nil {
		if f, err = w.openPartition(g); err != nil {
			return err
		}
	}
	if err := w.flushShare(f); err != nil {
		return err
	}
	if err := f.fw.WriteBuffered(sub); err != nil {
		return fmt.Errorf("goplane: encode %s: %w", f.path, err)
	}
	f.rows += sub.NumRows()
	f.fed += fed
	w.tick++
	f.lastUsed = w.tick
	if f.fed >= rotationFactor*w.spec.TargetFileBytes {
		delete(w.open, g.key)
		if err := w.finish(f); err != nil {
			return err
		}
	}
	return w.enforceBudget(f.path)
}

// flushShare ends f's row group before a write when f already holds its
// fair share of the budget (spec decision 9).
func (w *writer) flushShare(f *dataFile) error {
	n := f.fw.RowGroupTotalCompressedBytes()
	if n == 0 || n < w.budget/int64(len(w.open)) {
		return nil
	}
	if w.hook != nil {
		w.hook(budgetEvent{site: "share", path: f.path, writing: f.path, budget: w.budget, open: len(w.open)})
	}
	return flushRowGroup(f)
}

// enforceBudget ends the largest buffered row group while the open files
// hold more than the budget. Buffered page bytes are an approximate trigger:
// encoder state and not-yet-paged values are not counted.
func (w *writer) enforceBudget(writing string) error {
	for {
		var sum, most int64
		var largest *dataFile
		for _, f := range w.open {
			n := f.fw.RowGroupTotalCompressedBytes()
			sum += n
			if largest == nil || n > most {
				largest, most = f, n
			}
		}
		if sum <= w.budget || most == 0 {
			if w.hook != nil {
				w.hook(budgetEvent{site: "enforced", writing: writing, sum: sum, budget: w.budget, open: len(w.open)})
			}
			return nil
		}
		if w.hook != nil {
			w.hook(budgetEvent{site: "largest", path: largest.path, writing: writing, budget: w.budget, open: len(w.open)})
		}
		if err := flushRowGroup(largest); err != nil {
			return err
		}
	}
}

// flushRowGroup ends f's buffered row group, writing it to the upload
// stream. f stays open, so a failure reaches fail, which aborts it.
func flushRowGroup(f *dataFile) error {
	if err := f.fw.NewBufferedRowGroupChecked(); err != nil {
		return fmt.Errorf("goplane: flush row group of %s: %w", f.path, err)
	}
	return nil
}

// subBatch returns the rows of g as a batch and the uncompressed bytes it
// counts toward rotation: the whole batch, a zero-copy slice of a contiguous
// run (parent bytes x rows / parent rows), or a Take of scattered rows (its
// own buffers).
func subBatch(ctx context.Context, rec arrow.RecordBatch, g *rowGroup, parentBytes int64) (arrow.RecordBatch, int64, error) {
	n := len(g.rows)
	if n == int(rec.NumRows()) {
		rec.Retain()
		return rec, parentBytes, nil
	}
	if g.contiguous() {
		first := int64(g.rows[0])
		return rec.NewSlice(first, first+int64(n)), parentBytes * int64(n) / rec.NumRows(), nil
	}
	ib := array.NewInt32Builder(memory.DefaultAllocator)
	defer ib.Release()
	ib.Reserve(n)
	for _, r := range g.rows {
		ib.UnsafeAppend(int32(r))
	}
	idx := ib.NewArray()
	defer idx.Release()
	cols := make([]arrow.Array, rec.NumCols())
	defer func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}()
	for c := range cols {
		var err error
		if cols[c], err = compute.TakeArray(ctx, rec.Column(c), idx); err != nil {
			return nil, 0, fmt.Errorf("goplane: split batch by partition: %w", err)
		}
	}
	sub := array.NewRecordBatch(rec.Schema(), cols, int64(n))
	return sub, recordBytes(sub), nil
}

// openPartition opens the file of g's tuple, first finishing the least
// recently written open file when maxOpen files are open.
func (w *writer) openPartition(g *rowGroup) (*dataFile, error) {
	w.tuples[maphash.String(w.seed, g.key)] = struct{}{}
	if len(w.open) >= w.maxOpen {
		if err := w.evictLRU(); err != nil {
			return nil, err
		}
	}
	partPath := w.part.path(g.tuple)
	path := w.spec.DataPrefix + partPath + "/" + fmt.Sprintf("%s-%05d.parquet", w.spec.FilePrefix, w.nextSeq)
	if err := checkKeyLength(path, partPath); err != nil {
		return nil, err
	}
	f, err := w.openPath(path)
	if err != nil {
		return nil, err
	}
	f.seq, f.key, f.partition = w.nextSeq, g.key, g.tuple
	w.nextSeq++
	w.open[g.key] = f
	return f, nil
}

// evictLRU finishes (and keeps for commit) the least recently written open
// file; the first eviction of a writer logs one warning naming the remedy.
func (w *writer) evictLRU() error {
	var lru *dataFile
	for _, f := range w.open {
		if lru == nil || f.lastUsed < lru.lastUsed {
			lru = f
		}
	}
	if !w.evicted {
		w.evicted = true
		log.Printf("goplane: warning: table %s: %d partition files open (DATUPLET_GATEWAY_MAX_OPEN_PARTITIONS); "+
			"finishing the least recently written one after %d distinct partition tuples, so this writer produces more, smaller files; "+
			"more distinct tuples per batch than the cap cannot be streamed cheaply: cluster the input by the partition column(s) or use a coarser transform",
			w.spec.Table, w.maxOpen, len(w.tuples))
	}
	delete(w.open, lru.key)
	return w.finish(lru)
}

// openFile starts data file seq with the credentials current now.
func (w *writer) openFile(seq int) (*dataFile, error) {
	return w.openPath(fmt.Sprintf("%s%s-%05d.parquet", w.spec.DataPrefix, w.spec.FilePrefix, seq))
}

// openPath starts the data file at path with the credentials current now.
func (w *writer) openPath(path string) (*dataFile, error) {
	props := map[string]string{}
	if w.spec.Creds != nil {
		c, err := currentCreds(w.ctx, w.spec.Creds)
		if err != nil {
			return nil, err
		}
		if props, err = ioProps(c); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(w.ctx)
	sink, err := w.openSink(ctx, props, path)
	if err != nil {
		cancel()
		if w.local {
			_ = removeLocal(path)
		}
		return nil, err
	}
	f := &dataFile{path: path, local: w.local, cancel: cancel, sink: sink, out: &countingWriter{w: sink}}
	if f.fw, err = pqarrow.NewFileWriter(w.writeSchema, f.out, w.props, w.arrowProps); err != nil {
		f.abort()
		return nil, fmt.Errorf("goplane: parquet writer for %s: %w", path, err)
	}
	return f, nil
}

// finish writes the footer and completes the upload; on failure the file is
// aborted.
func (w *writer) finish(f *dataFile) error {
	if err := f.fw.Close(); err != nil {
		f.abort()
		return fmt.Errorf("goplane: finish %s: %w", f.path, err)
	}
	if err := f.closeSink(); err != nil {
		f.abort()
		return fmt.Errorf("goplane: upload %s: %w", f.path, err)
	}
	f.cancel()
	w.files = append(w.files, datagateway.WrittenFile{Path: f.path, Rows: f.rows, Bytes: f.out.n, SpecID: w.specID, Partition: f.partition})
	return nil
}

// dataFile is the file being written. The pqarrow writer writes through out,
// which has no Close: only this type closes the sink, so it is closed exactly
// once and, on failure, only after the upload context is cancelled.
type dataFile struct {
	path   string
	local  bool
	cancel context.CancelFunc // the upload context the sink was opened with
	sink   io.WriteCloser
	out    *countingWriter
	fw     *pqarrow.FileWriter
	rows   int64
	fed    int64 // uncompressed Arrow bytes fed

	// Partitioned writers only.
	seq       int
	key       string
	partition map[int]any
	lastUsed  uint64

	sinkClosed bool
}

func (f *dataFile) closeSink() error {
	if f.sinkClosed {
		return nil
	}
	f.sinkClosed = true
	return f.sink.Close()
}

// abort cancels the upload context first (a gocloud blob writer then aborts
// the upload on Close instead of completing it; see openIOSink for S3),
// closes the sink and removes a partial local file.
func (f *dataFile) abort() {
	f.cancel()
	_ = f.closeSink()
	if f.local {
		_ = removeLocal(f.path)
	}
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// recordBytes is the uncompressed size of rec's buffers.
func recordBytes(rec arrow.RecordBatch) int64 {
	var n int64
	for _, col := range rec.Columns() {
		n += dataBytes(col.Data())
	}
	return n
}

func dataBytes(d arrow.ArrayData) int64 {
	var n int64
	for _, b := range d.Buffers() {
		if b != nil {
			n += int64(b.Len())
		}
	}
	for _, c := range d.Children() {
		n += dataBytes(c)
	}
	return n
}

// stripFieldIDMetadata returns s without PARQUET:field_id field metadata
// (iceberg-go AddFiles rejects Parquet carrying field ids; tables have a name
// mapping). Mirrors main's pkg/datagateway/buffer stripFieldIDMetadata.
func stripFieldIDMetadata(s *arrow.Schema) *arrow.Schema {
	fields := s.Fields()
	out := make([]arrow.Field, len(fields))
	for i, f := range fields {
		keys, vals := f.Metadata.Keys(), f.Metadata.Values()
		newKeys := make([]string, 0, len(keys))
		newVals := make([]string, 0, len(vals))
		for j, k := range keys {
			if k == "PARQUET:field_id" {
				continue
			}
			newKeys = append(newKeys, k)
			newVals = append(newVals, vals[j])
		}
		out[i] = arrow.Field{Name: f.Name, Type: f.Type, Nullable: f.Nullable, Metadata: arrow.NewMetadata(newKeys, newVals)}
	}
	return arrow.NewSchema(out, nil)
}
