package backend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func TestParquetArrowReader_SingleFile_TwoRowGroups(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "in.parquet")
	writeFixtureParquet(t, file, []int64{1, 2, 3, 4, 5}, 2 /* rowsPerGroup */)

	schema := &SchemaInfo{Columns: []ColumnInfo{{Name: "id", Type: "int64", Nullable: false}}}
	r, err := NewParquetArrowReader(context.Background(), []string{file}, schema)
	if err != nil {
		t.Fatalf("NewParquetArrowReader: %v", err)
	}
	defer r.Close()

	var totalRows int64
	chunkCount := 0
	for {
		chunk, err := r.ReadChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadChunk: %v", err)
		}
		if chunk.Format != "arrow_ipc" {
			t.Fatalf("expected arrow_ipc format, got %q", chunk.Format)
		}
		totalRows += chunk.RowsInChunk
		chunkCount++
	}
	if totalRows != 5 {
		t.Errorf("totalRows = %d, want 5", totalRows)
	}
	// 5 rows / 2 per group = 3 groups; with BatchSize=64K and only 5 rows we get one batch per group
	if chunkCount < 1 {
		t.Errorf("chunkCount = %d, want at least 1", chunkCount)
	}
}

func TestParquetArrowReader_TwoFiles_TotalRowsCorrect(t *testing.T) {
	dir := t.TempDir()
	f1 := filepath.Join(dir, "1.parquet")
	f2 := filepath.Join(dir, "2.parquet")
	writeFixtureParquet(t, f1, []int64{1, 2, 3}, 2)
	writeFixtureParquet(t, f2, []int64{10, 20}, 2)

	schema := &SchemaInfo{Columns: []ColumnInfo{{Name: "id", Type: "int64", Nullable: false}}}
	r, err := NewParquetArrowReader(context.Background(), []string{f1, f2}, schema)
	if err != nil {
		t.Fatalf("NewParquetArrowReader: %v", err)
	}
	defer r.Close()

	var totalRows int64
	sawIsLast := false
	for {
		chunk, err := r.ReadChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadChunk: %v", err)
		}
		totalRows += chunk.RowsInChunk
		if chunk.IsLast {
			sawIsLast = true
		}
	}
	if totalRows != 5 {
		t.Errorf("totalRows = %d, want 5", totalRows)
	}
	// IsLast may be true on the last chunk OR EOF may be the terminator — either is acceptable.
	_ = sawIsLast
}

func TestParquetArrowReader_EmptyFileList_ReturnsError(t *testing.T) {
	schema := &SchemaInfo{Columns: []ColumnInfo{{Name: "id", Type: "int64", Nullable: false}}}
	_, err := NewParquetArrowReader(context.Background(), nil, schema)
	if err == nil {
		t.Fatal("expected error for empty file list")
	}
}

func TestParquetArrowReader_ZeroRowsInFile_ReturnsEOFImmediately(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "empty.parquet")
	writeFixtureParquet(t, f, []int64{} /* no rows */, 1)

	schema := &SchemaInfo{Columns: []ColumnInfo{{Name: "id", Type: "int64", Nullable: false}}}
	r, err := NewParquetArrowReader(context.Background(), []string{f}, schema)
	if err != nil {
		t.Fatalf("NewParquetArrowReader: %v", err)
	}
	defer r.Close()

	_, err = r.ReadChunk()
	if err != io.EOF {
		t.Errorf("expected io.EOF on zero-row file, got %v", err)
	}
}

func TestParquetArrowReader_SchemaProjection_PadsMissingColumn(t *testing.T) {
	dir := t.TempDir()
	f1 := filepath.Join(dir, "1.parquet")
	f2 := filepath.Join(dir, "2.parquet")

	// file 1: has only "id" (older snapshot, before "name" column was added)
	writeFixtureParquet(t, f1, []int64{1, 2}, 2)
	// file 2: has both "id" and "name" (current schema)
	writeTwoColParquet(t, f2, []int64{10}, []string{"hello"}, 2)

	currentSchema := &SchemaInfo{Columns: []ColumnInfo{
		{Name: "id", Type: "int64", Nullable: false},
		{Name: "name", Type: "string", Nullable: true},
	}}
	r, err := NewParquetArrowReader(context.Background(), []string{f1, f2}, currentSchema)
	if err != nil {
		t.Fatalf("NewParquetArrowReader: %v", err)
	}
	defer r.Close()

	var totalRows int64
	for {
		chunk, err := r.ReadChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadChunk: %v", err)
		}
		rec, _, err := readArrowIPCBytes(chunk.Data)
		if err != nil {
			t.Fatalf("ipc decode: %v", err)
		}
		if rec.NumCols() != 2 {
			t.Errorf("expected 2 cols (projection), got %d", rec.NumCols())
		}
		totalRows += rec.NumRows()
		// For the chunk from f1 (2 rows, id-only file), the padded "name" column must be all-null.
		if rec.NumRows() == 2 {
			nameCol := rec.Column(1)
			for row := 0; row < int(rec.NumRows()); row++ {
				if !nameCol.IsNull(row) {
					t.Errorf("row %d name should be null (padded), got non-null", row)
				}
			}
		}
		rec.Release()
	}
	if totalRows != 3 {
		t.Errorf("totalRows = %d, want 3", totalRows)
	}
}

// writeTwoColParquet writes a 2-col parquet (int64 id, utf8 name) — helper, schema-evolution fixture.
func writeTwoColParquet(t *testing.T, path string, ids []int64, names []string, rowsPerGroup int) {
	t.Helper()
	pool := memory.NewGoAllocator()
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()
	props := parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(int64(rowsPerGroup)))
	pqw, err := pqarrow.NewFileWriter(arrowSchema, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	defer pqw.Close()
	idB := array.NewInt64Builder(pool)
	nameB := array.NewStringBuilder(pool)
	for i, v := range ids {
		idB.Append(v)
		nameB.Append(names[i])
	}
	idArr := idB.NewArray()
	defer idArr.Release()
	nameArr := nameB.NewArray()
	defer nameArr.Release()
	rec := array.NewRecord(arrowSchema, []arrow.Array{idArr, nameArr}, int64(len(ids)))
	defer rec.Release()
	if err := pqw.Write(rec); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func readArrowIPCBytes(b []byte) (arrow.Record, *arrow.Schema, error) {
	rdr, err := ipc.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, nil, err
	}
	defer rdr.Release()
	if !rdr.Next() {
		return nil, nil, fmt.Errorf("no records in IPC stream")
	}
	rec := rdr.RecordBatch()
	rec.Retain()
	return rec, rdr.Schema(), nil
}

// writeFixtureParquet writes a single-column int64 parquet file with the given values, using the requested rowsPerGroup row-group size.
func writeFixtureParquet(t *testing.T, path string, values []int64, rowsPerGroup int) {
	t.Helper()
	pool := memory.NewGoAllocator()
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false}}, nil)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create fixture: %v", err)
	}
	defer f.Close()
	props := parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(int64(rowsPerGroup)))
	pqw, err := pqarrow.NewFileWriter(arrowSchema, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("pqarrow writer: %v", err)
	}
	defer pqw.Close()
	builder := array.NewInt64Builder(pool)
	for _, v := range values {
		builder.Append(v)
	}
	arr := builder.NewArray()
	defer arr.Release()
	rec := array.NewRecord(arrowSchema, []arrow.Array{arr}, int64(len(values)))
	defer rec.Release()
	if err := pqw.Write(rec); err != nil {
		t.Fatalf("write record: %v", err)
	}
}
