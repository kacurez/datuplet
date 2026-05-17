package storage

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// buildFixtureRecord returns a minimal 3-row record with id int64 +
// name string + ts timestamp(us). Row 1 is all present; row 2 has a
// null name; row 3 has a null ts.
func buildFixtureRecord(t *testing.T) (arrow.RecordBatch, *arrow.Schema) {
	t.Helper()
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "", "c"}, []bool{true, false, true})
	b.Field(2).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{1_735_689_600_000_000, 1_735_776_000_000_000, 0},
		[]bool{true, true, false},
	)
	return b.NewRecordBatch(), schema
}

// singleShotNext wraps a single RecordBatch as a one-call iterator.
// Retain() is called once because EncodeRecords will Release() the
// batch after it's done consuming it.
func singleShotNext(t *testing.T, rec arrow.RecordBatch) func() (arrow.RecordBatch, error) {
	t.Helper()
	called := false
	return func() (arrow.RecordBatch, error) {
		if called {
			return nil, nil
		}
		called = true
		rec.Retain()
		return rec, nil
	}
}

func TestEncodeRecords_BasicShape(t *testing.T) {
	rec, schema := buildFixtureRecord(t)
	defer rec.Release()

	resp, err := EncodeRecords(schema, singleShotNext(t, rec), 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Columns) != 3 {
		t.Fatalf("want 3 cols, got %d", len(resp.Columns))
	}
	if len(resp.Rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(resp.Rows))
	}

	// Column metadata.
	if resp.Columns[0].Name != "id" || resp.Columns[1].Name != "name" || resp.Columns[2].Name != "ts" {
		t.Errorf("column names: %+v", resp.Columns)
	}

	// Row 1: id=1, name="a".
	if resp.Rows[0][0] != int64(1) {
		t.Errorf("row0 col0: got %v (%T) want 1", resp.Rows[0][0], resp.Rows[0][0])
	}
	if resp.Rows[0][1] != "a" {
		t.Errorf("row0 col1: got %v want 'a'", resp.Rows[0][1])
	}

	// Row 2: name should be nil (null).
	if resp.Rows[1][1] != nil {
		t.Errorf("row1 col1 (null name): got %v want nil", resp.Rows[1][1])
	}

	// Row 3: ts should be nil.
	if resp.Rows[2][2] != nil {
		t.Errorf("row2 col2 (null ts): got %v want nil", resp.Rows[2][2])
	}

	// Confirm the whole thing JSON-marshals cleanly (catches any
	// non-JSON-able types leaking through).
	if _, err := json.Marshal(resp); err != nil {
		t.Errorf("json.Marshal: %v", err)
	}
	if resp.Truncated {
		t.Error("unexpected Truncated=true")
	}
}

func TestEncodeRecords_RowCap(t *testing.T) {
	rec, schema := buildFixtureRecord(t)
	defer rec.Release()

	resp, err := EncodeRecords(schema, singleShotNext(t, rec), 2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Rows) != 2 {
		t.Fatalf("want 2 rows (capped), got %d", len(resp.Rows))
	}
	if !resp.Truncated {
		t.Error("want Truncated=true when row cap hit")
	}
}

func TestEncodeRecords_BytesCap_DropsTrailingColumns(t *testing.T) {
	rec, schema := buildFixtureRecord(t)
	defer rec.Release()

	// An aggressively tight byte cap forces column-drop.
	resp, err := EncodeRecords(schema, singleShotNext(t, rec), 100, 40)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Truncated {
		t.Error("want Truncated=true under tight byte cap")
	}
	if len(resp.Columns) >= 3 {
		t.Errorf("expected trailing columns dropped, still have %d", len(resp.Columns))
	}
	// Rows array must still match column count.
	for i, row := range resp.Rows {
		if len(row) != len(resp.Columns) {
			t.Errorf("row %d len=%d want %d", i, len(row), len(resp.Columns))
		}
	}
}

func TestEncodeRecords_EmptyInput(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	next := func() (arrow.RecordBatch, error) { return nil, nil }

	resp, err := EncodeRecords(schema, next, 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Columns) != 1 {
		t.Errorf("want 1 column from schema, got %d", len(resp.Columns))
	}
	if len(resp.Rows) != 0 {
		t.Errorf("want 0 rows, got %d", len(resp.Rows))
	}
	if resp.Truncated {
		t.Error("empty input should not be Truncated")
	}
}

func TestEncodeRecords_NullsInPrimitives(t *testing.T) {
	// Covered by BasicShape (rows 2 and 3 have nulls). Keep as a
	// named test for easy targeting when expanding coverage later.
	t.Skip("covered by TestEncodeRecords_BasicShape")
}

// Iterator error during end-of-stream peek must propagate.
func TestEncodeRecords_IteratorErrorPropagates(t *testing.T) {
	rec, schema := buildFixtureRecord(t)
	defer rec.Release()
	callCount := 0
	next := func() (arrow.RecordBatch, error) {
		callCount++
		if callCount == 1 {
			rec.Retain()
			return rec, nil
		}
		return nil, fmt.Errorf("scan failure on batch 2")
	}
	resp, err := EncodeRecords(schema, next, 2, 1<<20) // row cap 2 triggers the peek
	if err == nil {
		t.Fatal("want error propagated from iterator, got nil")
	}
	if !strings.Contains(err.Error(), "scan failure") {
		t.Errorf("want scan-failure error, got %v", err)
	}
	// Rows from batch 1 should still be present even though batch 2's
	// fetch failed — callers can choose to render the partial preview
	// alongside the error rather than losing already-encoded state.
	if len(resp.Rows) == 0 {
		t.Error("want partial rows preserved on late-peek error, got empty")
	}
}

// After shrinking columns on first-row overflow, keep iterating so
// subsequent rows also land in the response with the narrow column set.
func TestEncodeRecords_ShrinkThenContinue(t *testing.T) {
	rec, schema := buildFixtureRecord(t)
	defer rec.Release()
	called := false
	next := func() (arrow.RecordBatch, error) {
		if called {
			return nil, nil
		}
		called = true
		rec.Retain()
		return rec, nil
	}
	// Tight byte cap that forces column shrink on row 1 but leaves
	// space for additional narrow rows. With the 3-column fixture
	// the full-width skeleton is ~145 B and a full row ~36 B; 105 B
	// keeps 2 cols in the skeleton (100 B) but overflows on row 1
	// (100+7>105), which triggers shrinkFirstRow to 1 col (70 B) and
	// leaves headroom for the remaining narrow rows.
	resp, err := EncodeRecords(schema, next, 100, 105)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Rows) < 2 {
		t.Errorf("want >=2 rows after shrink, got %d", len(resp.Rows))
	}
	// Rows must all match narrowed column count
	for i, r := range resp.Rows {
		if len(r) != len(resp.Columns) {
			t.Errorf("row %d len=%d, want %d", i, len(r), len(resp.Columns))
		}
	}
}
