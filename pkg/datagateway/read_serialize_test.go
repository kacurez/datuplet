package datagateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/datuplet/datuplet/pkg/datagateway/format"
)

// idNameBatch is a two-column (id int64, name string) batch with ids
// from..from+n-1 and names "n<id>"; every name of an even id is null.
func idNameBatch(t *testing.T, mem memory.Allocator, from, n int) (*arrow.Schema, arrow.RecordBatch) {
	t.Helper()
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(mem, sch)
	defer b.Release()
	for i := from; i < from+n; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		if i%2 == 0 {
			b.Field(1).AppendNull()
		} else {
			b.Field(1).(*array.StringBuilder).Append("n" + strconv.Itoa(i))
		}
	}
	return sch, b.NewRecordBatch()
}

// readParquetChunk parses a chunk as a standalone Parquet file and returns
// its Arrow table.
func readParquetChunk(t *testing.T, data []byte) arrow.Table {
	t.Helper()
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("chunk is not a Parquet file: %v", err)
	}
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	if err != nil {
		t.Fatalf("pqarrow reader: %v", err)
	}
	tbl, err := fr.ReadTable(context.Background())
	if err != nil {
		t.Fatalf("read table: %v", err)
	}
	return tbl
}

func int64Column(t *testing.T, tbl arrow.Table, name string) []int64 {
	t.Helper()
	idx := tbl.Schema().FieldIndices(name)
	if len(idx) != 1 {
		t.Fatalf("column %q not found in %s", name, tbl.Schema())
	}
	var out []int64
	for _, c := range tbl.Column(idx[0]).Data().Chunks() {
		arr := c.(*array.Int64)
		for i := 0; i < arr.Len(); i++ {
			out = append(out, arr.Value(i))
		}
	}
	return out
}

func TestEncodeChunkParquetIsOneZstdFile(t *testing.T) {
	mem := memory.NewGoAllocator()
	sch, a := idNameBatch(t, mem, 1, 3)
	_, b := idNameBatch(t, mem, 4, 2)
	defer a.Release()
	defer b.Release()

	data, err := encodeChunk(format.FormatParquet, nil, sch, nil, []arrow.RecordBatch{a, b}, mem)
	if err != nil {
		t.Fatal(err)
	}
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("not a Parquet file: %v", err)
	}
	if got := pf.NumRows(); got != 5 {
		t.Errorf("rows = %d, want 5", got)
	}
	for rg := 0; rg < pf.NumRowGroups(); rg++ {
		for c := 0; c < pf.MetaData().Schema.NumColumns(); c++ {
			cc, err := pf.MetaData().RowGroup(rg).ColumnChunk(c)
			if err != nil {
				t.Fatal(err)
			}
			if cc.Compression() != compress.Codecs.Zstd {
				t.Errorf("row group %d column %d compression = %v, want ZSTD", rg, c, cc.Compression())
			}
		}
	}
	tbl := readParquetChunk(t, data)
	defer tbl.Release()
	if got := int64Column(t, tbl, "id"); len(got) != 5 || got[0] != 1 || got[4] != 5 {
		t.Errorf("ids = %v, want 1..5", got)
	}
}

func TestEncodeChunkParquetNoBatchesKeepsSchema(t *testing.T) {
	mem := memory.NewGoAllocator()
	sch, a := idNameBatch(t, mem, 1, 1)
	a.Release()
	data, err := encodeChunk(format.FormatParquet, nil, sch, nil, nil, mem)
	if err != nil {
		t.Fatal(err)
	}
	tbl := readParquetChunk(t, data)
	defer tbl.Release()
	if tbl.NumRows() != 0 {
		t.Errorf("rows = %d, want 0", tbl.NumRows())
	}
	if got := tbl.Schema(); got.NumFields() != 2 || got.Field(0).Name != "id" || got.Field(1).Name != "name" ||
		!arrow.TypeEqual(got.Field(0).Type, arrow.PrimitiveTypes.Int64) {
		t.Errorf("schema = %s, want id int64, name string", got)
	}
}

// ipcBatches decodes an IPC chunk: its schema, batch count and total rows.
func ipcBatches(t *testing.T, data []byte) (*arrow.Schema, int, int64) {
	t.Helper()
	r, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("not an Arrow IPC stream: %v", err)
	}
	defer r.Release()
	var batches int
	var rows int64
	for r.Next() {
		batches++
		rows += r.RecordBatch().NumRows()
	}
	if r.Err() != nil {
		t.Fatal(r.Err())
	}
	return r.Schema(), batches, rows
}

// Spec Read path 8: an IPC chunk is one stream holding exactly one batch
// (the chunk's batches concatenated) in the wire schema; with no batches it
// is one zero-row batch.
func TestEncodeChunkArrowIPCHoldsEveryBatch(t *testing.T) {
	mem := memory.NewGoAllocator()
	sch, a := idNameBatch(t, mem, 1, 3)
	_, b := idNameBatch(t, mem, 4, 2)
	defer a.Release()
	defer b.Release()

	data, err := encodeChunk(format.FormatArrowIPC, nil, sch, sch, []arrow.RecordBatch{a, b}, mem)
	if err != nil {
		t.Fatal(err)
	}
	got, batches, rows := ipcBatches(t, data)
	if !got.Equal(sch) || batches != 1 || rows != 5 {
		t.Errorf("IPC: schema %s, %d batches, %d rows; want %s, 1 batch, 5 rows", got, batches, rows, sch)
	}

	empty, err := encodeChunk(format.FormatArrowIPC, nil, sch, sch, nil, mem)
	if err != nil {
		t.Fatal(err)
	}
	got, batches, rows = ipcBatches(t, empty)
	if !got.Equal(sch) || batches != 1 || rows != 0 {
		t.Errorf("empty IPC chunk: schema %s, %d batches, %d rows; want one zero-row batch", got, batches, rows)
	}

	if _, err := encodeChunk(format.FormatArrowIPC, nil, sch, nil, []arrow.RecordBatch{a}, mem); err == nil {
		t.Error("IPC chunk without a wire schema: want error")
	}
}

func TestEncodeChunkCSVJSONLAndJSON(t *testing.T) {
	mem := memory.NewGoAllocator()
	reg := format.DefaultRegistry()
	sch, a := idNameBatch(t, mem, 1, 3)
	_, b := idNameBatch(t, mem, 4, 2)
	defer a.Release()
	defer b.Release()
	batches := []arrow.RecordBatch{a, b}

	csvAdapter, _ := reg.Get(format.FormatCSV)
	data, err := encodeChunk(format.FormatCSV, csvAdapter, sch, nil, batches, mem)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(bytes.NewReader(data)).ReadAll()
	if err != nil {
		t.Fatalf("CSV: %v", err)
	}
	if len(rows) != 6 || rows[0][0] != "id" || rows[0][1] != "name" || rows[1][0] != "1" || rows[1][1] != "n1" || rows[5][0] != "5" {
		t.Errorf("CSV rows = %q, want one header and ids 1..5", rows)
	}

	jsonlAdapter, _ := reg.Get(format.FormatJSONL)
	data, err = encodeChunk(format.FormatJSONL, jsonlAdapter, sch, nil, batches, mem)
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(bytes.NewReader(data))
	var ids []float64
	for sc.Scan() {
		var obj map[string]any
		if err := json.Unmarshal(sc.Bytes(), &obj); err != nil {
			t.Fatalf("JSONL line %q: %v", sc.Text(), err)
		}
		ids = append(ids, obj["id"].(float64))
	}
	if len(ids) != 5 || ids[0] != 1 || ids[4] != 5 {
		t.Errorf("JSONL ids = %v, want 1..5", ids)
	}

	jsonAdapter, _ := reg.Get(format.FormatJSON)
	data, err = encodeChunk(format.FormatJSON, jsonAdapter, sch, nil, batches, mem)
	if err != nil {
		t.Fatal(err)
	}
	var objs []map[string]any
	if err := json.Unmarshal(data, &objs); err != nil || len(objs) != 5 {
		t.Errorf("JSON = %s (%v), want an array of 5 objects", data, err)
	}

	data, err = encodeChunk(format.FormatCSV, csvAdapter, sch, nil, nil, mem)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "id,name\n" {
		t.Errorf("empty CSV chunk = %q, want the header only", data)
	}
}

// The encoders take no ownership: every batch handed in is still the
// caller's to release, and nothing the encoder allocated is left behind.
func TestEncodeChunkReleasesItsAllocations(t *testing.T) {
	reg := format.DefaultRegistry()
	for _, f := range []format.DataFormat{format.FormatParquet, format.FormatArrowIPC, format.FormatCSV, format.FormatJSONL, format.FormatJSON} {
		t.Run(f.String(), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			sch, a := idNameBatch(t, mem, 1, 3)
			_, b := idNameBatch(t, mem, 4, 2)
			ad, _ := reg.Get(f)
			if _, err := encodeChunk(f, ad, sch, sch, []arrow.RecordBatch{a, b}, mem); err != nil {
				t.Fatal(err)
			}
			a.Release()
			b.Release()
			mem.AssertSize(t, 0)
		})
	}
}
