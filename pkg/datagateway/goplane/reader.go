package goplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/datagateway"
)

var (
	errReaderClosed = errors.New("goplane: reader closed")
	errStreamClosed = errors.New("goplane: part stream closed")
)

// ioLoader builds the iceberg-go IO for location with props; tests replace it.
type ioLoader func(ctx context.Context, props map[string]string, location string) (iceio.IO, error)

// reader is one TableReader over a pinned snapshot (spec, Read path).
type reader struct {
	spec     datagateway.ReaderSpec
	creds    datagateway.CredsSource // nil only for local tables
	location string                  // table location (scheme for the scan IO)
	loadIO   ioLoader

	ctx    context.Context // lifetime: plane ctx child; cancelled by Close, Interrupt, plane Close
	cancel context.CancelFunc

	scan        *table.Scan
	schema      *arrow.Schema // current schema, SchemaToArrowSchema(sc, nil, false, false)
	tableSchema *iceberg.Schema
	nameMapping iceberg.NameMapping

	// Owned by the NextPart caller (parts are requested sequentially).
	tasks []table.FileScanTask // coalesced, in planning order
	raw   []int8               // eligibility memo per task: 0 unknown, 1 raw, -1 decoded
	next  int

	plane *Plane
	// asyncPause, when set (tests), runs on the asynchronous close path after
	// it took its snapshot of bodies and before it closes them.
	asyncPause func()

	mu      sync.Mutex
	closed  bool
	streams map[*taskStream]struct{}
	bodies  map[*rawBody]struct{} // raw bodies opened and not yet closed
}

func (p *Plane) openReader(ctx context.Context, spec datagateway.ReaderSpec) (*reader, error) {
	if p.cfg.Tables == nil {
		return nil, errors.New("goplane: no table loader configured for reads")
	}
	p.mu.Lock()
	if p.closed || p.ctx.Err() != nil {
		p.mu.Unlock()
		return nil, errors.New("goplane: plane closed")
	}
	lctx, cancel := context.WithCancel(p.ctx)
	r := &reader{spec: spec, loadIO: p.loadIO, ctx: lctx, cancel: cancel, streams: map[*taskStream]struct{}{},
		bodies: map[*rawBody]struct{}{}, plane: p}
	p.readers[r] = struct{}{}
	p.mu.Unlock()

	// Interrupt ends the lifetime without calling Close: open raw bodies (an
	// *os.File ignores cancellation) are closed here too. Plane Close and
	// reader Close also close them synchronously.
	context.AfterFunc(lctx, func() { r.closeBodies(true) })
	if err := r.init(ctx, p.cfg.Tables); err != nil {
		p.forget(r)
		cancel()
		if r.scan != nil {
			_ = r.scan.Close()
		}
		return nil, err
	}
	return r, nil
}

// init loads the table, rebuilds it on the reader's own IO factory, plans
// once and coalesces split tasks.
func (r *reader) init(ctx context.Context, loader TableLoader) error {
	cctx, stop := callContext(ctx, r.ctx)
	defer stop()
	t, creds, err := loader.LoadTable(cctx, r.spec.Namespace, r.spec.Table)
	if err != nil {
		return fmt.Errorf("goplane: load %s.%s: %w", r.spec.Namespace, r.spec.Table, err)
	}
	r.creds, r.location = creds, t.Location()
	if r.location == "" {
		return fmt.Errorf("goplane: table %s.%s has no location", r.spec.Namespace, r.spec.Table)
	}
	if creds == nil && !isLocalPrefix(r.location) {
		return fmt.Errorf("goplane: no credentials for remote table %s.%s", r.spec.Namespace, r.spec.Table)
	}
	// Every scan IO (manifests, each ReadTasks call) is built from the
	// credentials current at that moment (spec, Read path 6). No catalog:
	// the reader never commits, and planning stays local.
	tbl := table.New(t.Identifier(), t.Metadata(), t.MetadataLocation(),
		func(ctx context.Context) (iceio.IO, error) { return r.openIO(ctx, r.location) }, nil)
	r.tableSchema = tbl.Schema()
	r.nameMapping = tbl.NameMapping()
	if r.schema, err = table.SchemaToArrowSchema(r.tableSchema, nil, false, false); err != nil {
		return fmt.Errorf("goplane: arrow schema of %s.%s: %w", r.spec.Namespace, r.spec.Table, err)
	}
	r.scan = tbl.Scan(table.WithMaxConcurrency(1))
	planned, err := r.scan.PlanFiles(cctx)
	if err != nil {
		return fmt.Errorf("goplane: plan %s.%s: %w", r.spec.Namespace, r.spec.Table, err)
	}
	r.tasks = coalesceSplits(planned)
	r.raw = make([]int8, len(r.tasks))
	return nil
}

// openIO builds an IO for location with credentials current now. The props
// carry secrets and never reach a log or an error.
func (r *reader) openIO(ctx context.Context, location string) (iceio.IO, error) {
	props := map[string]string{}
	if r.creds != nil {
		c, err := currentCreds(ctx, r.creds)
		if err != nil {
			return nil, err
		}
		if props, err = ioProps(c); err != nil {
			return nil, err
		}
	}
	fsys, err := r.loadIO(ctx, props, location)
	if err != nil {
		return nil, fmt.Errorf("goplane: load IO for %s: %w", location, err)
	}
	return fsys, nil
}

func (r *reader) Schema() *arrow.Schema { return r.schema }

func (r *reader) alive(ctx context.Context) error {
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed || r.ctx.Err() != nil {
		return errReaderClosed
	}
	return ctx.Err()
}

// NextPart returns the next part in task order: a raw-eligible task as a
// RawFile, a maximal run of other tasks as one Records part; with
// RawParquet false the whole scan is one Records part.
func (r *reader) NextPart(ctx context.Context) (datagateway.ReadPart, error) {
	if err := r.alive(ctx); err != nil {
		return datagateway.ReadPart{}, err
	}
	if r.next >= len(r.tasks) {
		return datagateway.ReadPart{}, io.EOF
	}
	if !r.spec.RawParquet {
		s, err := r.newStream(r.tasks[r.next:])
		if err != nil {
			return datagateway.ReadPart{}, err
		}
		r.next = len(r.tasks)
		return datagateway.ReadPart{Records: s}, nil
	}
	cctx, stop := callContext(ctx, r.ctx)
	defer stop()
	raw, err := r.rawAt(cctx, r.next)
	if err != nil {
		return datagateway.ReadPart{}, err
	}
	if raw {
		t := r.tasks[r.next]
		r.next++
		path := t.File.FilePath()
		return datagateway.ReadPart{Raw: &datagateway.RawFile{
			Path: path, Rows: t.File.Count(), Size: t.File.FileSizeBytes(),
			Open: func(ctx context.Context) (io.ReadCloser, error) { return r.openRaw(ctx, path) },
		}}, nil
	}
	end := r.next + 1
	for end < len(r.tasks) {
		raw, err := r.rawAt(cctx, end)
		if err != nil {
			return datagateway.ReadPart{}, err
		}
		if raw {
			break
		}
		end++
	}
	s, err := r.newStream(r.tasks[r.next:end])
	if err != nil {
		return datagateway.ReadPart{}, err
	}
	r.next = end
	return datagateway.ReadPart{Records: s}, nil
}

func (r *reader) rawAt(ctx context.Context, i int) (bool, error) {
	if r.raw[i] == 0 {
		ok, err := r.rawEligible(ctx, r.tasks[i])
		if err != nil {
			return false, err
		}
		r.raw[i] = -1
		if ok {
			r.raw[i] = 1
		}
	}
	return r.raw[i] == 1, nil
}

// openRaw opens a data file with the credentials current now. ctx bounds the
// open; the returned body stays readable until it is closed or the reader
// closes (Close, Interrupt, plane Close).
func (r *reader) openRaw(ctx context.Context, path string) (io.ReadCloser, error) {
	if err := r.alive(ctx); err != nil {
		return nil, err
	}
	octx, cancel := context.WithCancel(r.ctx)
	stopOpen := context.AfterFunc(ctx, cancel)
	fsys, err := r.openIO(octx, path)
	if err == nil {
		var f iceio.File
		if f, err = fsys.Open(path); err == nil {
			if !stopOpen() {
				_ = f.Close()
				cancel()
				return nil, fmt.Errorf("goplane: open %s: %w", path, context.Cause(ctx))
			}
			return r.track(&rawBody{r: r, f: f, cancel: cancel})
		}
		err = fmt.Errorf("goplane: open %s: %w", path, err)
	}
	stopOpen()
	cancel()
	return nil, err
}

// track registers b unless the reader has ended (Close, Interrupt, plane
// Close), in which case b is closed and an error returned.
func (r *reader) track(b *rawBody) (io.ReadCloser, error) {
	r.mu.Lock()
	if r.closed || r.ctx.Err() != nil {
		r.mu.Unlock()
		b.shut()
		return nil, errReaderClosed
	}
	r.bodies[b] = struct{}{}
	r.mu.Unlock()
	return b, nil
}

// closeBodies closes every tracked raw body and returns once each is
// closed; later reads on them fail. Bodies stay registered until their close
// has completed, and shut is itself a barrier (it returns only after the
// file is closed, whichever caller closed it), so concurrent callers — the
// asynchronous cancel path, reader Close, plane Close, a body's own Close —
// all return only after the files are closed.
func (r *reader) closeBodies(async bool) {
	r.mu.Lock()
	bodies := make([]*rawBody, 0, len(r.bodies))
	for b := range r.bodies {
		bodies = append(bodies, b)
	}
	r.mu.Unlock()
	if async && r.asyncPause != nil {
		r.asyncPause()
	}
	for _, b := range bodies {
		_ = b.shut()
	}
	r.mu.Lock()
	for _, b := range bodies {
		delete(r.bodies, b)
	}
	r.mu.Unlock()
}

// rawBody is an open raw data file. Close is idempotent; once the body or
// its reader is closed, Read returns an error. The underlying Read and Close
// are serialized under ioMu (gocloud's blob.Reader is not safe for a Close
// concurrent with a Read).
type rawBody struct {
	r      *reader
	f      iceio.File
	cancel context.CancelFunc

	ioMu     sync.Mutex
	closed   bool // guarded by ioMu
	closeErr error
}

func (b *rawBody) Read(p []byte) (int, error) {
	b.ioMu.Lock()
	defer b.ioMu.Unlock()
	if b.closed {
		return 0, errReaderClosed
	}
	return b.f.Read(p)
}

// Close closes the file, then untracks the body.
func (b *rawBody) Close() error {
	err := b.shut()
	b.r.mu.Lock()
	delete(b.r.bodies, b)
	b.r.mu.Unlock()
	return err
}

// shut cancels the body's context first (that releases a Read blocked on
// remote storage; a local Read in progress just finishes), then takes the
// I/O lock and closes the file once. It returns only after the file is
// closed, whoever closed it.
func (b *rawBody) shut() error {
	b.cancel()
	b.ioMu.Lock()
	defer b.ioMu.Unlock()
	if !b.closed {
		b.closed = true
		b.closeErr = b.f.Close()
	}
	return b.closeErr
}

// Close stops every in-flight read of this reader, closes every raw body
// and part stream still open, and returns only once they are closed.
// Idempotent; a second call also waits for the bodies.
func (r *reader) Close() error {
	r.mu.Lock()
	first := !r.closed
	r.closed = true
	streams := make([]*taskStream, 0, len(r.streams))
	for s := range r.streams {
		streams = append(streams, s)
	}
	r.mu.Unlock()
	r.cancel()
	r.closeBodies(false)
	for _, s := range streams {
		_ = s.Close()
	}
	if !first {
		return nil
	}
	r.plane.forget(r)
	return r.scan.Close()
}

func (r *reader) newStream(tasks []table.FileScanTask) (*taskStream, error) {
	ctx, cancel := context.WithCancel(r.ctx)
	s := &taskStream{r: r, tasks: tasks, ctx: ctx, cancel: cancel}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		cancel()
		return nil, errReaderClosed
	}
	r.streams[s] = struct{}{}
	return s, nil
}

// taskStream decodes its tasks in order, one Scan.ReadTasks call per task,
// so every task's IO carries the credentials current at its start.
type taskStream struct {
	r      *reader
	tasks  []table.FileScanTask
	ctx    context.Context
	cancel context.CancelFunc

	mu   sync.Mutex
	i    int // next task to start
	pull func() (arrow.RecordBatch, error, bool)
	stop func()
	err  error // sticky: io.EOF, a failure, or errStreamClosed
}

func (s *taskStream) Schema() *arrow.Schema { return s.r.schema }

// Next returns the next decoded batch. ctx bounds this call; cancelling it
// mid-call fails the stream.
func (s *taskStream) Next(ctx context.Context) (arrow.RecordBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	defer context.AfterFunc(ctx, s.cancel)()
	for {
		if s.pull == nil {
			if s.i >= len(s.tasks) {
				s.err = io.EOF
				return nil, io.EOF
			}
			if s.ctx.Err() != nil {
				return nil, s.fail(nil, ctx)
			}
			task := s.tasks[s.i]
			s.i++
			_, seq, err := s.r.scan.ReadTasks(s.ctx, []table.FileScanTask{task})
			if err != nil {
				return nil, s.fail(fmt.Errorf("goplane: read %s: %w", task.File.FilePath(), err), ctx)
			}
			s.pull, s.stop = iter.Pull2(seq)
		}
		rec, err, ok := s.pull()
		if !ok {
			s.endTask()
			continue
		}
		if err != nil {
			if rec != nil {
				rec.Release()
			}
			return nil, s.fail(fmt.Errorf("goplane: read %s: %w", s.tasks[s.i-1].File.FilePath(), err), ctx)
		}
		out, err := conform(rec, s.r.schema)
		if err != nil {
			return nil, s.fail(err, ctx)
		}
		return out, nil
	}
}

func (s *taskStream) endTask() {
	if s.stop != nil {
		s.stop()
	}
	s.pull, s.stop = nil, nil
}

// fail makes err (or the cancellation that caused it) sticky.
func (s *taskStream) fail(err error, call context.Context) error {
	switch {
	case s.r.ctx.Err() != nil:
		err = errReaderClosed
	case call.Err() != nil:
		err = fmt.Errorf("goplane: read cancelled: %w", call.Err())
	case err == nil:
		err = errStreamClosed
	}
	s.err = err
	s.endTask()
	s.cancel()
	return err
}

// Close stops the stream's in-flight read. Idempotent.
func (s *taskStream) Close() error {
	s.cancel()
	s.mu.Lock()
	if s.err == nil || s.err == io.EOF {
		s.err = errStreamClosed
	}
	s.endTask()
	s.mu.Unlock()
	s.r.mu.Lock()
	delete(s.r.streams, s)
	s.r.mu.Unlock()
	return nil
}

// conform returns rec under schema: rec itself when the schemas are equal,
// the same columns re-labelled when names and types match, else an error.
func conform(rec arrow.RecordBatch, schema *arrow.Schema) (arrow.RecordBatch, error) {
	if rec.Schema().Equal(schema) {
		return rec, nil
	}
	if !sameNamesAndTypes(rec.Schema(), schema) {
		got := rec.Schema().String()
		rec.Release()
		return nil, fmt.Errorf("goplane: decoded batch schema %s differs from table schema %s", got, schema)
	}
	out := array.NewRecordBatch(schema, rec.Columns(), rec.NumRows())
	rec.Release()
	return out, nil
}

// coalesceSplits merges each run of consecutive tasks of the same data file
// whose byte ranges are contiguous and cover exactly [0, FileSizeBytes) back
// into one whole-file task (spec, Read path 2). Other tasks pass unchanged.
func coalesceSplits(tasks []table.FileScanTask) []table.FileScanTask {
	out := make([]table.FileScanTask, 0, len(tasks))
	for i := 0; i < len(tasks); {
		t := tasks[i]
		size := t.File.FileSizeBytes()
		if t.Start == 0 && t.Length < size {
			end, j := t.Length, i+1
			for j < len(tasks) && end < size && tasks[j].File.FilePath() == t.File.FilePath() && tasks[j].Start == end {
				end += tasks[j].Length
				j++
			}
			if end == size {
				whole := t
				whole.Length = size
				out = append(out, whole)
				i = j
				continue
			}
		}
		out = append(out, t)
		i++
	}
	return out
}

// callContext returns a context cancelled when either call or life is done.
func callContext(call, life context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(life)
	stop := context.AfterFunc(call, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}
