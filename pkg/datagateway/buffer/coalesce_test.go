package buffer

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// fiveColSchema is the canonical test schema for coalesce tests — mirrors
// the membench scenario and the data-generator's typical output shape.
func fiveColSchema(t *testing.T) (*schema.Schema, *arrow.Schema) {
	t.Helper()
	cols := []schema.ColumnDef{
		{Name: "id", Type: schema.TypeInt64, Nullable: false},
		{Name: "name", Type: schema.TypeString, Nullable: true},
		{Name: "value", Type: schema.TypeFloat64, Nullable: true},
		{Name: "active", Type: schema.TypeBool, Nullable: true},
		{Name: "ts", Type: schema.TypeTimestamp, Nullable: true},
	}
	s, err := schema.NewSchema(cols)
	if err != nil {
		t.Fatalf("schema.NewSchema: %v", err)
	}
	return s, s.ArrowSchema()
}

// oneRow builds a 1-row arrow.Record matching the 5-col schema.
func oneRow(alloc memory.Allocator, sch *arrow.Schema, i int) arrow.Record {
	b := array.NewRecordBuilder(alloc, sch)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(int64(i))
	b.Field(1).(*array.StringBuilder).Append("user")
	b.Field(2).(*array.Float64Builder).Append(1.5)
	b.Field(3).(*array.BooleanBuilder).Append(true)
	b.Field(4).(*array.TimestampBuilder).Append(arrow.Timestamp(int64(i)))
	return b.NewRecord()
}

// kRows builds a k-row arrow.Record matching the 5-col schema.
func kRows(alloc memory.Allocator, sch *arrow.Schema, k int) arrow.Record {
	b := array.NewRecordBuilder(alloc, sch)
	defer b.Release()
	for i := 0; i < k; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		b.Field(1).(*array.StringBuilder).Append("user")
		b.Field(2).(*array.Float64Builder).Append(1.5)
		b.Field(3).(*array.BooleanBuilder).Append(true)
		b.Field(4).(*array.TimestampBuilder).Append(arrow.Timestamp(int64(i)))
	}
	return b.NewRecord()
}

func newTestBufferMgr(t *testing.T, s *schema.Schema) *BufferManager {
	t.Helper()
	cfg := &BufferConfig{
		BufferSize:     128 * 1024 * 1024,
		RowGroupSize:   128 * 1024 * 1024,
		TargetFileSize: 256 * 1024 * 1024,
		OutputDir:      t.TempDir(),
		FilePrefix:     "coalesce-test",
		Compression:    CompressionSnappy,
	}
	mgr, err := NewBufferManager(s, cfg, memory.NewGoAllocator(), nil)
	if err != nil {
		t.Fatalf("NewBufferManager: %v", err)
	}
	return mgr
}

// TestAdd_SmallRecordsCoalesce verifies small records do not retain
// as individual batches — they accumulate in the in-flight builder
// until a flush trigger fires.
func TestAdd_SmallRecordsCoalesce(t *testing.T) {
	s, sch := fiveColSchema(t)
	mgr := newTestBufferMgr(t, s)
	defer mgr.Close()
	alloc := memory.NewGoAllocator()

	// Add 10 single-row records. None of these should land in batches yet —
	// they should be accumulating in mgr.acc (we're below coalesceFlushRows).
	for i := 0; i < 10; i++ {
		rec := oneRow(alloc, sch, i)
		if err := mgr.Add(rec); err != nil {
			t.Fatalf("Add[%d]: %v", i, err)
		}
		rec.Release()
	}

	if got := len(mgr.batches); got != 0 {
		t.Errorf("batches after 10 small Adds: got %d, want 0 (should still be in accumulator)", got)
	}
	if mgr.accRows != 10 {
		t.Errorf("accRows after 10 Adds: got %d, want 10", mgr.accRows)
	}

	// Stats() must reflect accumulator state, not zero.
	stats := mgr.Stats()
	if stats.BufferedRecords < 1 {
		t.Errorf("Stats().BufferedRecords = %d, want >= 1 (accumulator pending)", stats.BufferedRecords)
	}
}

// TestAdd_FastPathSkipsAccumulator verifies that records ≥ coalesceFastPathRows
// bypass the accumulator and go straight to batches.
func TestAdd_FastPathSkipsAccumulator(t *testing.T) {
	s, sch := fiveColSchema(t)
	mgr := newTestBufferMgr(t, s)
	defer mgr.Close()
	alloc := memory.NewGoAllocator()

	rec := kRows(alloc, sch, coalesceFastPathRows) // exactly at threshold
	defer rec.Release()
	if err := mgr.Add(rec); err != nil {
		t.Fatalf("Add: %v", err)
	}

	if len(mgr.batches) != 1 {
		t.Errorf("batches after fast-path Add: got %d, want 1", len(mgr.batches))
	}
	if mgr.accRows != 0 {
		t.Errorf("accRows after fast-path Add: got %d, want 0", mgr.accRows)
	}
}

// TestAdd_FastPathFlushesAccumulatorFirst verifies append-order invariant:
// when a fast-path record arrives, any pending accumulator rows must
// flush first so the order matches the caller's Add sequence.
func TestAdd_FastPathFlushesAccumulatorFirst(t *testing.T) {
	s, sch := fiveColSchema(t)
	mgr := newTestBufferMgr(t, s)
	defer mgr.Close()
	alloc := memory.NewGoAllocator()

	// Three small rows accumulate.
	for i := 0; i < 3; i++ {
		rec := oneRow(alloc, sch, i)
		_ = mgr.Add(rec)
		rec.Release()
	}
	// One big record arrives — accumulator should flush first.
	big := kRows(alloc, sch, coalesceFastPathRows)
	defer big.Release()
	if err := mgr.Add(big); err != nil {
		t.Fatalf("Add big: %v", err)
	}

	// Expect 2 batches: [accumulated 3 rows, big record]
	if len(mgr.batches) != 2 {
		t.Fatalf("batches: got %d, want 2", len(mgr.batches))
	}
	if mgr.batches[0].NumRows() != 3 {
		t.Errorf("first batch rows: got %d, want 3 (accumulated)", mgr.batches[0].NumRows())
	}
	if mgr.batches[1].NumRows() != int64(coalesceFastPathRows) {
		t.Errorf("second batch rows: got %d, want %d (fast-path)", mgr.batches[1].NumRows(), coalesceFastPathRows)
	}
}

// TestAdd_RowTriggerFlushesAccumulator verifies coalesceFlushRows trigger.
func TestAdd_RowTriggerFlushesAccumulator(t *testing.T) {
	s, sch := fiveColSchema(t)
	mgr := newTestBufferMgr(t, s)
	defer mgr.Close()
	alloc := memory.NewGoAllocator()

	for i := 0; i < coalesceFlushRows; i++ {
		rec := oneRow(alloc, sch, i)
		if err := mgr.Add(rec); err != nil {
			t.Fatalf("Add[%d]: %v", i, err)
		}
		rec.Release()
	}

	// At this exact count the trigger fires inside Add — batches should
	// hold the accumulated record and acc should be empty.
	if len(mgr.batches) != 1 {
		t.Errorf("batches after %d small Adds: got %d, want 1", coalesceFlushRows, len(mgr.batches))
	}
	if mgr.accRows != 0 {
		t.Errorf("accRows after flush trigger: got %d, want 0", mgr.accRows)
	}
	if got := mgr.batches[0].NumRows(); got != int64(coalesceFlushRows) {
		t.Errorf("flushed record rows: got %d, want %d", got, coalesceFlushRows)
	}
}

// TestAdd_SchemaMismatchReturnsError verifies clean rejection of
// schema-incompatible records (instead of panicking deep in the copy).
func TestAdd_SchemaMismatchReturnsError(t *testing.T) {
	s, _ := fiveColSchema(t)
	mgr := newTestBufferMgr(t, s)
	defer mgr.Close()
	alloc := memory.NewGoAllocator()

	// Record with a totally different schema.
	otherCols := []schema.ColumnDef{
		{Name: "x", Type: schema.TypeInt32, Nullable: false},
	}
	otherSch, _ := schema.NewSchema(otherCols)
	b := array.NewRecordBuilder(alloc, otherSch.ArrowSchema())
	b.Field(0).(*array.Int32Builder).Append(1)
	rec := b.NewRecord()
	b.Release()
	defer rec.Release()

	err := mgr.Add(rec)
	if err == nil {
		t.Fatal("Add with mismatched schema should error")
	}
}

// TestFlush_DrainsAccumulator verifies explicit Flush() finalizes the
// accumulator into batches.
func TestFlush_DrainsAccumulator(t *testing.T) {
	s, sch := fiveColSchema(t)
	tmpDir := t.TempDir()
	cfg := &BufferConfig{
		BufferSize:     1024 * 1024 * 1024, // large — don't auto-flush
		RowGroupSize:   1024 * 1024 * 1024,
		TargetFileSize: 1024 * 1024 * 1024,
		OutputDir:      tmpDir,
		FilePrefix:     "flush-test",
	}
	mgr, _ := NewBufferManager(s, cfg, memory.NewGoAllocator(), nil)
	defer mgr.Close()
	alloc := memory.NewGoAllocator()

	// Five small rows pending in accumulator.
	for i := 0; i < 5; i++ {
		rec := oneRow(alloc, sch, i)
		_ = mgr.Add(rec)
		rec.Release()
	}
	if mgr.accRows != 5 {
		t.Fatalf("precondition: accRows = %d, want 5", mgr.accRows)
	}

	// Flush() should drain the accumulator AND flush the row group.
	if err := mgr.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if mgr.accRows != 0 {
		t.Errorf("accRows after Flush: %d, want 0", mgr.accRows)
	}
	// After Flush the row group is written and batches is empty
	// (TotalRowsFlushed only updates on file rotate/Close, by design).
	if len(mgr.batches) != 0 {
		t.Errorf("batches after Flush: %d, want 0", len(mgr.batches))
	}
	if mgr.currentFileRows != 5 {
		t.Errorf("currentFileRows after Flush: %d, want 5", mgr.currentFileRows)
	}
}

// TestClose_DrainsAccumulator verifies Close drains residual accumulator
// rows into the final parquet file.
func TestClose_DrainsAccumulator(t *testing.T) {
	s, sch := fiveColSchema(t)
	mgr := newTestBufferMgr(t, s)
	alloc := memory.NewGoAllocator()

	for i := 0; i < 7; i++ {
		rec := oneRow(alloc, sch, i)
		_ = mgr.Add(rec)
		rec.Release()
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	stats := mgr.Stats()
	if stats.TotalRowsFlushed != 7 {
		t.Errorf("TotalRowsFlushed after Close: %d, want 7", stats.TotalRowsFlushed)
	}
}

// TestSchemasCompatible covers the structural type comparison helper.
func TestSchemasCompatible(t *testing.T) {
	s, _ := fiveColSchema(t)
	a := s.ArrowSchema()

	t.Run("identical", func(t *testing.T) {
		if !schemasCompatible(a, a) {
			t.Error("identical schemas should be compatible")
		}
	})

	t.Run("different column count", func(t *testing.T) {
		fewerCols, _ := schema.NewSchema([]schema.ColumnDef{
			{Name: "id", Type: schema.TypeInt64},
		})
		if schemasCompatible(a, fewerCols.ArrowSchema()) {
			t.Error("schemas with different column counts should not be compatible")
		}
	})

	t.Run("different type", func(t *testing.T) {
		// Same name, different type.
		altered, _ := schema.NewSchema([]schema.ColumnDef{
			{Name: "id", Type: schema.TypeInt32}, // was Int64
			{Name: "name", Type: schema.TypeString, Nullable: true},
			{Name: "value", Type: schema.TypeFloat64, Nullable: true},
			{Name: "active", Type: schema.TypeBool, Nullable: true},
			{Name: "ts", Type: schema.TypeTimestamp, Nullable: true},
		})
		if schemasCompatible(a, altered.ArrowSchema()) {
			t.Error("schemas with mismatched type should not be compatible")
		}
	})
}
