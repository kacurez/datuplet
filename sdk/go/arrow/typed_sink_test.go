package arrow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// typedTestSchema is a typed, non-nullable, multi-type schema — proof that
// Sink is independent of StringSink's all-String path. fakeWriter, ipcRows,
// and ipcCells (from sink_test.go) are reused here; they make no assumption
// about column types beyond what ipcCells' *array.String cast requires, and
// this file never calls ipcCells.
func typedTestSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "label", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

func appendTypedRow(b *array.RecordBuilder, id int64, value float64, active bool, label string) {
	b.Field(0).(*array.Int64Builder).Append(id)
	b.Field(1).(*array.Float64Builder).Append(value)
	b.Field(2).(*array.BooleanBuilder).Append(active)
	b.Field(3).(*array.StringBuilder).Append(label)
}

type typedRow struct {
	id     int64
	value  float64
	active bool
	label  string
}

// decodeTypedPayload parses one payload as a complete standalone IPC stream
// against typedTestSchema's four columns and returns the decoded rows.
func decodeTypedPayload(t *testing.T, payload []byte) []typedRow {
	t.Helper()
	br := bytes.NewReader(payload)
	rd, err := ipc.NewReader(br)
	if err != nil {
		t.Fatalf("payload is not a valid IPC stream: %v", err)
	}
	defer rd.Release()

	var rows []typedRow
	for rd.Next() {
		rec := rd.Record()
		idCol := rec.Column(0).(*array.Int64)
		valCol := rec.Column(1).(*array.Float64)
		activeCol := rec.Column(2).(*array.Boolean)
		labelCol := rec.Column(3).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			rows = append(rows, typedRow{
				id:     idCol.Value(i),
				value:  valCol.Value(i),
				active: activeCol.Value(i),
				label:  labelCol.Value(i),
			})
		}
		rec.Release()
	}
	if rd.Err() != nil {
		t.Fatalf("ipc read: %v", rd.Err())
	}
	return rows
}

func TestSink_BatchBoundariesAndSelfContainedStreams(t *testing.T) {
	fw := &fakeWriter{}
	opened := false
	sink := NewSink(context.Background(), func() (ChunkWriter, error) {
		opened = true
		return fw, nil
	}, typedTestSchema(), WithBatchRows(4) /* tiny batch */)

	if opened {
		t.Fatal("writer must not be opened before the first flush")
	}

	for i := 0; i < 10; i++ {
		appendTypedRow(sink.Builder(), int64(i), float64(i)*1.5, i%2 == 0, fmt.Sprintf("row-%d", i))
		if err := sink.RowDone(); err != nil {
			t.Fatalf("RowDone(%d): %v", i, err)
		}
	}
	if !opened {
		t.Fatal("writer should have opened by the first flush (4 rows reached)")
	}

	rows, _, err := sink.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if rows != 10 {
		t.Fatalf("rows = %d, want 10", rows)
	}
	if len(fw.payloads) != 3 { // 4+4+2
		t.Fatalf("payloads = %d, want 3 (4+4+2)", len(fw.payloads))
	}

	var total int64
	for _, p := range fw.payloads {
		n, names, fields := ipcRows(t, p) // each independently parseable = no-concat invariant
		total += n
		if len(names) != 4 || names[0] != "id" || names[1] != "value" || names[2] != "active" || names[3] != "label" {
			t.Fatalf("schema = %v", names)
		}
		for _, f := range fields {
			if f.Nullable {
				t.Fatalf("field %s should be non-nullable", f.Name)
			}
		}
	}
	if total != 10 {
		t.Fatalf("ipc total rows = %d, want 10", total)
	}
	if !fw.closed {
		t.Fatal("writer not closed")
	}

	// Verify actual cell values round-trip correctly for every type in the
	// schema, not just row counts.
	decoded := decodeTypedPayload(t, fw.payloads[0])
	if len(decoded) != 4 {
		t.Fatalf("first payload rows = %d, want 4", len(decoded))
	}
	for i, row := range decoded {
		wantID := int64(i)
		wantValue := float64(i) * 1.5
		wantActive := i%2 == 0
		wantLabel := fmt.Sprintf("row-%d", i)
		if row.id != wantID || row.value != wantValue || row.active != wantActive || row.label != wantLabel {
			t.Fatalf("row %d = %+v, want {id:%d value:%v active:%v label:%q}", i, row, wantID, wantValue, wantActive, wantLabel)
		}
	}
}

func TestSink_ZeroRowsNeverOpensWriter(t *testing.T) {
	opened := false
	sink := NewSink(context.Background(), func() (ChunkWriter, error) {
		opened = true
		return &fakeWriter{}, nil
	}, typedTestSchema(), WithBatchRows(4))

	rows, cr, err := sink.Finish()
	if err != nil || rows != 0 || cr != nil {
		t.Fatalf("rows=%d cr=%v err=%v", rows, cr, err)
	}
	if opened {
		t.Fatal("writer must not be opened for zero rows")
	}
	if sink.Writer() != nil {
		t.Fatal("Writer() must be nil for zero rows")
	}
}

func TestSink_OpenFailureIsWriteError(t *testing.T) {
	sink := NewSink(context.Background(), func() (ChunkWriter, error) {
		return nil, errors.New("open boom")
	}, typedTestSchema(), WithBatchRows(1))

	appendTypedRow(sink.Builder(), 1, 1.0, true, "x")
	err := sink.RowDone()
	var we *WriteError
	if !errors.As(err, &we) {
		t.Fatalf("want WriteError, got %v", err)
	}
}

func TestSink_FinishIsIdempotent(t *testing.T) {
	fw := &fakeWriter{}
	sink := NewSink(context.Background(), func() (ChunkWriter, error) { return fw, nil }, typedTestSchema(), WithBatchRows(4))

	appendTypedRow(sink.Builder(), 1, 1.5, true, "a")
	if err := sink.RowDone(); err != nil {
		t.Fatal(err)
	}
	rows1, cr1, err1 := sink.Finish()
	rows2, cr2, err2 := sink.Finish()
	if err1 != nil || err2 != nil {
		t.Fatalf("Finish errors: %v, %v", err1, err2)
	}
	if rows1 != 1 || rows2 != 1 {
		t.Fatalf("rows mismatch: %d vs %d", rows1, rows2)
	}
	if cr1 != cr2 {
		t.Fatalf("closeResult mismatch")
	}
	if fw.closeCount != 1 {
		t.Fatalf("closeCount = %d, want 1 (not double-closed)", fw.closeCount)
	}
}

func TestSink_RowDoneAfterFinishErrors(t *testing.T) {
	fw := &fakeWriter{}
	sink := NewSink(context.Background(), func() (ChunkWriter, error) { return fw, nil }, typedTestSchema(), WithBatchRows(4))

	appendTypedRow(sink.Builder(), 1, 1.0, true, "a")
	if err := sink.RowDone(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := sink.RowDone(); err == nil {
		t.Fatal("RowDone after Finish should error")
	}
	if sink.Builder() != nil {
		t.Fatal("Builder() should be nil after Finish (released)")
	}
}

func TestSink_WriteFailureIsStickyAndDropsNoRowsSilently(t *testing.T) {
	fw := &fakeWriter{failWriteOn: 2} // fail on second Write
	sink := NewSink(context.Background(), func() (ChunkWriter, error) { return fw, nil }, typedTestSchema(), WithBatchRows(2))

	// Rows 0,1: first batch, flush succeeds (first Write call).
	appendTypedRow(sink.Builder(), 0, 0, true, "x")
	if err := sink.RowDone(); err != nil {
		t.Fatalf("RowDone(0) should succeed, got: %v", err)
	}
	appendTypedRow(sink.Builder(), 1, 1, true, "x")
	if err := sink.RowDone(); err != nil {
		t.Fatalf("RowDone(1) should succeed, got: %v", err)
	}

	// Row 2: batch not full yet, no flush.
	appendTypedRow(sink.Builder(), 2, 2, true, "x")
	if err := sink.RowDone(); err != nil {
		t.Fatalf("RowDone(2) should succeed (batch not full yet), got: %v", err)
	}

	// Row 3: second batch full, flush fails (second Write call).
	appendTypedRow(sink.Builder(), 3, 3, true, "x")
	err := sink.RowDone()
	var we *WriteError
	if !errors.As(err, &we) {
		t.Fatalf("RowDone(3) want WriteError, got %v", err)
	}
	failureErr := err

	// Sticky: the builder was released, so a well-behaved caller stops
	// appending; calling RowDone again (with no new append) must return the
	// exact same error rather than attempt another flush.
	if err2 := sink.RowDone(); err2 != failureErr {
		t.Fatalf("RowDone after failure should return same sticky error, got: %v", err2)
	}
	if sink.Builder() != nil {
		t.Fatal("Builder() should be nil after sticky failure (released)")
	}

	_, _, finishErr := sink.Finish()
	if finishErr != failureErr {
		t.Fatalf("Finish should return sticky error, got: %v", finishErr)
	}
	if len(fw.payloads) != 1 {
		t.Fatalf("payloads = %d, want 1 (second batch never sent after write failed)", len(fw.payloads))
	}
	// The sticky failure came from a failed writer.Write, so the writer was
	// already open; Finish makes one best-effort attempt to close it so the
	// gateway-side writer session doesn't dangle, while still returning the
	// original write error (asserted above) to the caller unchanged.
	if fw.closeCount != 1 {
		t.Fatalf("closeCount = %d, want 1 (best-effort close of the already-opened writer after a sticky failure)", fw.closeCount)
	}
}

func TestSink_BytesShippedAgreesWithWriterReceipts(t *testing.T) {
	fw := &fakeWriter{}
	sink := NewSink(context.Background(), func() (ChunkWriter, error) { return fw, nil }, typedTestSchema(), WithBatchRows(3))

	for i := 0; i < 7; i++ {
		appendTypedRow(sink.Builder(), int64(i), float64(i)*2.5, i%2 == 0, fmt.Sprintf("v%d", i))
		if err := sink.RowDone(); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := sink.Finish(); err != nil {
		t.Fatal(err)
	}

	var want int64
	for _, p := range fw.payloads {
		want += int64(len(p))
	}
	if want == 0 {
		t.Fatal("test setup produced no payloads")
	}
	if got := sink.BytesShipped(); got != want {
		t.Fatalf("BytesShipped() = %d, want %d (sum of fake writer's received payload lengths)", got, want)
	}
}

// --- appended in Fix Round 1 (close writer on sticky-failure Finish) ---

// TestSink_FinishClosesWriterOnStickyWriteFailure proves Finish's
// close-after-failure fix has teeth: the common failure path is a Write
// call failing mid-run inside flush() (not a Close failure), which means
// the writer was already opened successfully. Finish must still return the
// original write error unchanged, but must also make a best-effort attempt
// to close that already-opened writer instead of leaving it dangling.
func TestSink_FinishClosesWriterOnStickyWriteFailure(t *testing.T) {
	fw := &fakeWriter{failWriteOn: 2} // fail on the second flush's Write call
	sink := NewSink(context.Background(), func() (ChunkWriter, error) { return fw, nil }, typedTestSchema(), WithBatchRows(2))

	// First batch: flush succeeds (first Write call), opening the writer.
	appendTypedRow(sink.Builder(), 0, 0, true, "x")
	if err := sink.RowDone(); err != nil {
		t.Fatalf("RowDone(0): %v", err)
	}
	appendTypedRow(sink.Builder(), 1, 1, true, "x")
	if err := sink.RowDone(); err != nil {
		t.Fatalf("RowDone(1): %v", err)
	}

	// Second batch: flush's Write call fails -> sticky failure.
	appendTypedRow(sink.Builder(), 2, 2, true, "x")
	if err := sink.RowDone(); err != nil {
		t.Fatalf("RowDone(2) should succeed (batch not full yet), got: %v", err)
	}
	appendTypedRow(sink.Builder(), 3, 3, true, "x")
	writeErr := sink.RowDone()
	var we *WriteError
	if !errors.As(writeErr, &we) {
		t.Fatalf("RowDone(3) want WriteError, got %v", writeErr)
	}

	_, _, finishErr := sink.Finish()
	if finishErr != writeErr {
		t.Fatalf("Finish error = %v, want the original sticky write error %v unchanged", finishErr, writeErr)
	}
	if fw.closeCount != 1 {
		t.Fatalf("closeCount = %d, want 1 (writer was opened, so Finish must close it best-effort after the sticky failure)", fw.closeCount)
	}
}

// TestSink_FinishTwiceAfterFailureClosesOnlyOnce proves the close-once
// guard: the maintainer's own code calls Finish twice on the failure path
// (a deferred best-effort finish, plus an explicit one), and the gateway's
// own writer is not guaranteed idempotent under a second Close, so double
// closing on repeated Finish calls is a real hazard, not a theoretical one.
func TestSink_FinishTwiceAfterFailureClosesOnlyOnce(t *testing.T) {
	fw := &fakeWriter{failWriteOn: 1} // fail on the very first Write call
	sink := NewSink(context.Background(), func() (ChunkWriter, error) { return fw, nil }, typedTestSchema(), WithBatchRows(1))

	appendTypedRow(sink.Builder(), 0, 0, true, "x")
	writeErr := sink.RowDone()
	if writeErr == nil {
		t.Fatal("RowDone should fail on the first Write call")
	}

	_, _, err1 := sink.Finish()
	_, _, err2 := sink.Finish()
	if err1 != writeErr || err2 != writeErr {
		t.Fatalf("Finish errors = %v, %v, want both = original sticky error %v", err1, err2, writeErr)
	}
	if fw.closeCount != 1 {
		t.Fatalf("closeCount = %d, want 1 (two Finish calls after failure must not double-close)", fw.closeCount)
	}
}

// TestSink_FinishAfterOpenFailureNeverAttemptsClose proves the lazy-open
// guarantee holds even in the failure-close path: a sink that fails before
// its first flush (writer-open itself fails, so s.writer is nil) must not
// call Close on anything — there is nothing to close, and doing so would
// either panic (nil ChunkWriter) or spuriously touch an unrelated writer.
func TestSink_FinishAfterOpenFailureNeverAttemptsClose(t *testing.T) {
	fw := &fakeWriter{} // never handed to the sink; any Close on it would be a bug
	sink := NewSink(context.Background(), func() (ChunkWriter, error) {
		return nil, errors.New("open boom")
	}, typedTestSchema(), WithBatchRows(1))

	appendTypedRow(sink.Builder(), 1, 1.0, true, "x")
	if err := sink.RowDone(); err == nil {
		t.Fatal("RowDone should fail on open error")
	}
	if sink.Writer() != nil {
		t.Fatal("writer must stay nil after an open failure")
	}

	if _, _, err := sink.Finish(); err == nil {
		t.Fatal("Finish should return the sticky open error")
	}
	if fw.closeCount != 0 {
		t.Fatalf("closeCount = %d, want 0 (writer was never opened, so Finish must never attempt a close)", fw.closeCount)
	}
}

func TestSink_BytesShippedZeroForZeroRows(t *testing.T) {
	sink := NewSink(context.Background(), func() (ChunkWriter, error) { return &fakeWriter{}, nil }, typedTestSchema(), WithBatchRows(4))
	if _, _, err := sink.Finish(); err != nil {
		t.Fatal(err)
	}
	if got := sink.BytesShipped(); got != 0 {
		t.Fatalf("BytesShipped() = %d, want 0 for zero rows", got)
	}
}
