package datagateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// fakeDataPlane is an in-memory DataPlane for tests of everything above the
// plane (server wiring, gRPC/HTTP handlers). It lives in package datagateway
// as a _test.go file: a separate test-helper package would import datagateway
// while datagateway's own tests import it, which is an import cycle.
type fakeDataPlane struct {
	mu sync.Mutex

	writers []*fakeTableWriter
	tables  map[string]fakeTable

	writeErr      error
	openReaderErr error
	interruptedF  bool
	closed        bool
	boot          BootInfo
	factoryCalls  int
}

type fakeTable struct {
	schema  *arrow.Schema
	batches []arrow.RecordBatch
}

func newFakeDataPlane() *fakeDataPlane {
	return &fakeDataPlane{tables: map[string]fakeTable{}}
}

var _ DataPlane = (*fakeDataPlane)(nil)

func (p *fakeDataPlane) OpenTableWriter(_ context.Context, spec WriterSpec) (TableWriter, error) {
	// Context ownership: the writer's lifetime never derives from the Open ctx.
	p.mu.Lock()
	defer p.mu.Unlock()
	w := &fakeTableWriter{plane: p, spec: spec}
	p.writers = append(p.writers, w)
	return w, nil
}

func (p *fakeDataPlane) OpenTableReader(_ context.Context, spec ReaderSpec) (RecordStream, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.openReaderErr != nil {
		return nil, p.openReaderErr
	}
	t, ok := p.tables[spec.Namespace+"."+spec.Table]
	if !ok {
		return nil, fmt.Errorf("fake: unknown table %s.%s", spec.Namespace, spec.Table)
	}
	return &fakeRecordStream{table: t}, nil
}

func (p *fakeDataPlane) Interrupt() {
	p.mu.Lock()
	p.interruptedF = true
	p.mu.Unlock()
}

func (p *fakeDataPlane) Close() error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	return nil
}

// setTable registers the batches a reader of ns.table replays.
func (p *fakeDataPlane) setTable(ns, table string, schema *arrow.Schema, batches ...arrow.RecordBatch) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tables[ns+"."+table] = fakeTable{schema: schema, batches: batches}
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

type fakeTableWriter struct {
	plane *fakeDataPlane
	spec  WriterSpec

	mu      sync.Mutex
	batches []arrow.RecordBatch
	rows    int64
	bytes   int64
	closed  bool
	aborted bool
	files   []WrittenFile
}

func (w *fakeTableWriter) Write(_ context.Context, rec arrow.RecordBatch) error {
	if err := w.plane.currentWriteErr(); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.aborted {
		return errors.New("fake: write on finished writer")
	}
	rec.Retain() // the caller keeps ownership of rec; the fake keeps its own ref
	w.batches = append(w.batches, rec)
	w.rows += rec.NumRows()
	for _, c := range rec.Columns() {
		w.bytes += int64(c.Data().Len())
	}
	return nil
}

func (w *fakeTableWriter) QueuedBytes() int64 { return 0 }

func (w *fakeTableWriter) Close(_ context.Context) ([]WrittenFile, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return w.files, nil
	}
	w.closed = true
	if w.rows > 0 {
		w.files = []WrittenFile{{
			Path:  w.spec.DataPrefix + w.spec.FilePrefix + "-00000.parquet",
			Rows:  w.rows,
			Bytes: w.bytes,
		}}
	}
	return w.files, nil
}

func (w *fakeTableWriter) Abort() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.aborted = true
}

// recorded returns the batches written so far (still retained by the writer).
func (w *fakeTableWriter) recorded() []arrow.RecordBatch {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]arrow.RecordBatch(nil), w.batches...)
}

type fakeRecordStream struct {
	table fakeTable
	next  int
	done  bool
}

func (s *fakeRecordStream) Schema() *arrow.Schema { return s.table.schema }

func (s *fakeRecordStream) Next(ctx context.Context) (arrow.RecordBatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.done || s.next >= len(s.table.batches) {
		return nil, io.EOF
	}
	b := s.table.batches[s.next]
	s.next++
	b.Retain() // caller Releases
	return b, nil
}

func (s *fakeRecordStream) Close() error { s.done = true; return nil }

// ---- self-tests ----------------------------------------------------------

func fakeTestBatch(t *testing.T, ids ...int64) (*arrow.Schema, arrow.RecordBatch) {
	t.Helper()
	sch := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	b := array.NewInt64Builder(memory.NewGoAllocator())
	defer b.Release()
	b.AppendValues(ids, nil)
	arr := b.NewArray()
	defer arr.Release()
	return sch, array.NewRecordBatch(sch, []arrow.Array{arr}, int64(len(ids)))
}

func TestFakeDataPlane_WriteCloseFiles(t *testing.T) {
	p := newFakeDataPlane()
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
	files, err := w.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "s3://b/t/data/part-abcd1234-w1-00000.parquet" || files[0].Rows != 6 {
		t.Fatalf("files = %+v", files)
	}
	again, _ := w.Close(context.Background())
	if len(again) != 1 || again[0] != files[0] {
		t.Errorf("second Close = %+v, want identical %+v", again, files)
	}
	if n := len(p.writers[0].recorded()); n != 2 {
		t.Errorf("recorded batches = %d, want 2", n)
	}
	w.Abort() // safe after Close
	w.Abort() // idempotent
}

func TestFakeDataPlane_ReaderReplay(t *testing.T) {
	p := newFakeDataPlane()
	sch, a := fakeTestBatch(t, 1, 2)
	_, b := fakeTestBatch(t, 3)
	defer a.Release()
	defer b.Release()
	p.setTable("raw", "orders", sch, a, b)

	rs, err := p.OpenTableReader(context.Background(), ReaderSpec{Namespace: "raw", Table: "orders"})
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	if !rs.Schema().Equal(sch) {
		t.Errorf("schema mismatch")
	}
	var rows int64
	for {
		rec, err := rs.Next(context.Background())
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
	if _, err := p.OpenTableReader(context.Background(), ReaderSpec{Namespace: "raw", Table: "nope"}); err == nil {
		t.Error("unknown table: want error")
	}
}

func TestFakeDataPlane_FailureInjectionAndInterrupt(t *testing.T) {
	p := newFakeDataPlane()
	sch, rec := fakeTestBatch(t, 1)
	defer rec.Release()

	w, _ := p.OpenTableWriter(context.Background(), WriterSpec{Schema: sch})
	boom := errors.New("boom")
	p.failWrites(boom)
	if err := w.Write(context.Background(), rec); !errors.Is(err, boom) {
		t.Errorf("Write err = %v, want boom", err)
	}
	p.failWrites(nil)
	if err := w.Write(context.Background(), rec); err != nil {
		t.Errorf("Write after clearing failure: %v", err)
	}

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

func TestFakeDataPlane_FactoryRecordsBootInfo(t *testing.T) {
	p := newFakeDataPlane()
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
