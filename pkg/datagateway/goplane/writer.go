package goplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
// channelBatches; one goroutine (run) encodes them into the current file.
type writer struct {
	spec        datagateway.WriterSpec
	local       bool
	openSink    sinkOpener
	writeSchema *arrow.Schema // spec.Schema without PARQUET:field_id metadata
	props       *parquet.WriterProperties
	arrowProps  pqarrow.ArrowWriterProperties

	ctx    context.Context // lifetime; its cause is the writer's failure
	cancel context.CancelCauseFunc
	ch     chan arrow.RecordBatch
	done   chan struct{}
	queued atomic.Int64

	mu       sync.Mutex // serializes sends on ch with closing it
	chClosed bool

	// Owned by run until done is closed, then read-only.
	cur   *dataFile
	files []datagateway.WrittenFile
	err   error
}

func newWriter(parent context.Context, spec datagateway.WriterSpec, local bool, open sinkOpener) *writer {
	ctx, cancel := context.WithCancelCause(parent)
	w := &writer{
		spec:        spec,
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
	}
	go w.run()
	return w
}

func (w *writer) QueuedBytes() int64 { return w.queued.Load() }

// Write queues rec (retaining it) and blocks while the channel is full. ctx
// bounds only this call.
func (w *writer) Write(ctx context.Context, rec arrow.RecordBatch) error {
	if err := w.checkRecord(rec); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ctx.Err() != nil {
		return w.failure()
	}
	if w.chClosed {
		return errClosed
	}
	size := recordBytes(rec)
	rec.Retain()
	w.queued.Add(size)
	select {
	case w.ch <- rec:
		return nil
	case <-w.ctx.Done():
		rec.Release()
		w.queued.Add(-size)
		return w.failure()
	case <-ctx.Done():
		rec.Release()
		w.queued.Add(-size)
		return ctx.Err()
	}
}

// Close flushes the last file and returns every file written. A second call
// returns the same result. ctx bounds only the wait.
func (w *writer) Close(ctx context.Context) ([]datagateway.WrittenFile, error) {
	w.closeChannel()
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
	w.closeChannel()
	<-w.done
}

func (w *writer) closeChannel() {
	w.mu.Lock()
	if !w.chClosed {
		w.chClosed = true
		close(w.ch)
	}
	w.mu.Unlock()
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
		case rec, ok := <-w.ch:
			if !ok {
				w.finishLast()
				return
			}
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
		case <-w.ctx.Done():
			w.fail(w.failure())
			for rec := range w.ch { // until Close / Abort closes it
				w.queued.Add(-recordBytes(rec))
				rec.Release()
			}
			return
		}
	}
}

// fail records the first error, cancels the lifetime context (unblocking
// Write) and aborts the in-flight file.
func (w *writer) fail(err error) {
	if w.err == nil {
		w.err = err
	}
	w.cancel(err)
	if w.cur != nil {
		w.cur.abort()
		w.cur = nil
	}
}

func (w *writer) finishLast() {
	if w.err == nil && w.ctx.Err() != nil {
		w.fail(w.failure())
	}
	if w.err != nil || w.cur == nil {
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

// openFile starts data file seq with the credentials current now.
func (w *writer) openFile(seq int) (*dataFile, error) {
	path := fmt.Sprintf("%s%s-%05d.parquet", w.spec.DataPrefix, w.spec.FilePrefix, seq)
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
	w.files = append(w.files, datagateway.WrittenFile{Path: f.path, Rows: f.rows, Bytes: f.out.n})
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
