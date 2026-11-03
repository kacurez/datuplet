package datagateway

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// Rows of the variant read fixtures; "" is SQL NULL and "null" a variant
// JSON null (a non-null row whose value is the variant null).
var variantReadRows = []string{`{"a":1,"b":"x","n":9007199254740993}`, `[1,"two",3.5]`, `"hello"`, ``, `null`, `-9223372036854775808`}

func variantBatchOf(t *testing.T, rows []string) (*arrow.Schema, arrow.RecordBatch) {
	t.Helper()
	vt := extensions.NewDefaultVariantType()
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "v", Type: vt, Nullable: true},
	}, nil)
	ib := array.NewInt64Builder(memory.DefaultAllocator)
	vb := extensions.NewVariantBuilder(memory.DefaultAllocator, vt)
	defer ib.Release()
	defer vb.Release()
	for i, js := range rows {
		ib.Append(int64(i + 1))
		if js == "" {
			vb.AppendNull()
			continue
		}
		v, err := variant.ParseJSON(js, false)
		if err != nil {
			t.Fatal(err)
		}
		vb.Append(v)
	}
	ia, va := ib.NewArray(), vb.NewArray()
	defer ia.Release()
	defer va.Release()
	return sch, array.NewRecordBatch(sch, []arrow.Array{ia, va}, int64(len(rows)))
}

func variantReadServer(t *testing.T, chunkSize int64, parts ...fakePart) *ServerV2 {
	t.Helper()
	s, p := newWriteTestServer(t, &Config{ChunkSize: chunkSize})
	sch, a := variantBatchOf(t, variantReadRows)
	a.Release()
	p.setTableParts("raw", "events", sch, parts...)
	return s
}

func variantReadServerBatches(t *testing.T, chunkSize int64, batches ...arrow.RecordBatch) *ServerV2 {
	t.Helper()
	return variantReadServer(t, chunkSize, batchFakePart(batches...))
}

func TestReadVariantArrowIPCAsJSONText(t *testing.T) {
	_, a := variantBatchOf(t, variantReadRows[:3])
	_, b := variantBatchOf(t, variantReadRows[3:])
	defer a.Release()
	defer b.Release()
	s := variantReadServerBatches(t, 1<<30, a, b)

	resp := openTestReader(t, s, "events", pb.DataFormat_FORMAT_ARROW_IPC)
	if resp.Schema.Columns[1].Name != "v" || resp.Schema.Columns[1].Type != "string" {
		t.Fatalf("advertised schema = %v, want v as string", resp.Schema)
	}
	wire, err := sdkWireSchema(resp.Schema)
	if err != nil {
		t.Fatal(err)
	}
	chunks := drainHTTP(t, s, resp.ReaderId)
	if len(chunks) != 1 {
		t.Fatalf("%d chunks, want 1", len(chunks))
	}
	r, err := ipc.NewReader(bytes.NewReader(chunks[0].data))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	if !r.Schema().Equal(wire) {
		t.Fatalf("IPC schema %s, want the wire schema %s", r.Schema(), wire)
	}
	var got []string
	batches := 0
	for r.Next() {
		batches++
		col := r.RecordBatch().Column(1).(*array.String)
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				got = append(got, "NULL")
			} else {
				got = append(got, col.Value(i))
			}
		}
	}
	if r.Err() != nil {
		t.Fatal(r.Err())
	}
	want := []string{`{"a":1,"b":"x","n":9007199254740993}`, `[1,"two",3.5]`, `"hello"`, "NULL", `null`, `-9223372036854775808`}
	if batches != 1 || !reflect.DeepEqual(got, want) {
		t.Errorf("%d batches, v = %q; want 1 batch, %q", batches, got, want)
	}
}

func TestReadVariantJSONEmbedsValues(t *testing.T) {
	_, a := variantBatchOf(t, variantReadRows)
	defer a.Release()
	s := variantReadServerBatches(t, 1<<30, a)

	resp := openTestReader(t, s, "events", pb.DataFormat_FORMAT_JSONL)
	chunks := drainHTTP(t, s, resp.ReaderId)
	var lines []string
	sc := bufio.NewScanner(bytes.NewReader(chunks[0].data))
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	wantLines := []string{
		`{"id":1,"v":{"a":1,"b":"x","n":9007199254740993}}`,
		`{"id":2,"v":[1,"two",3.5]}`,
		`{"id":3,"v":"hello"}`,
		`{"id":4,"v":null}`,
		`{"id":5,"v":null}`,
		`{"id":6,"v":-9223372036854775808}`,
	}
	if !reflect.DeepEqual(lines, wantLines) {
		t.Errorf("JSONL lines\n got %q\nwant %q", lines, wantLines)
	}

	resp = openTestReader(t, s, "events", pb.DataFormat_FORMAT_JSON)
	chunks = drainHTTP(t, s, resp.ReaderId)
	var objs []struct {
		ID int64           `json:"id"`
		V  json.RawMessage `json:"v"`
	}
	if err := json.Unmarshal(chunks[0].data, &objs); err != nil || len(objs) != 6 {
		t.Fatalf("JSON = %s (%v)", chunks[0].data, err)
	}
	if string(objs[0].V) != `{"a":1,"b":"x","n":9007199254740993}` || string(objs[2].V) != `"hello"` || string(objs[3].V) != `null` {
		t.Errorf("JSON v values = %s %s %s", objs[0].V, objs[2].V, objs[3].V)
	}
}

func TestReadVariantCSVJSONText(t *testing.T) {
	_, a := variantBatchOf(t, variantReadRows)
	defer a.Release()
	s := variantReadServerBatches(t, 1<<30, a)

	resp := openTestReader(t, s, "events", pb.DataFormat_FORMAT_CSV)
	chunks := drainHTTP(t, s, resp.ReaderId)
	rows, err := csv.NewReader(bytes.NewReader(chunks[0].data)).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{`{"a":1,"b":"x","n":9007199254740993}`, `[1,"two",3.5]`, `"hello"`, ``, `null`, `-9223372036854775808`}
	if len(rows) != 7 || rows[0][1] != "v" {
		t.Fatalf("CSV rows = %q", rows)
	}
	for i, w := range want {
		if rows[i+1][1] != w {
			t.Errorf("CSV row %d v = %q, want %q", i+1, rows[i+1][1], w)
		}
	}
}

// SQL NULL stays null in every format; a variant JSON null is the value
// null (JSON) or the text null (CSV, Arrow IPC).
func TestReadVariantNullsStayNull(t *testing.T) {
	_, a := variantBatchOf(t, []string{``, `null`})
	defer a.Release()
	s := variantReadServerBatches(t, 1<<30, a)

	resp := openTestReader(t, s, "events", pb.DataFormat_FORMAT_CSV)
	rows, err := csv.NewReader(bytes.NewReader(drainHTTP(t, s, resp.ReaderId)[0].data)).ReadAll()
	if err != nil || rows[1][1] != "" || rows[2][1] != "null" {
		t.Errorf("CSV = %q (%v), want an empty field for SQL NULL and null for variant null", rows, err)
	}

	resp = openTestReader(t, s, "events", pb.DataFormat_FORMAT_JSONL)
	if got := string(drainHTTP(t, s, resp.ReaderId)[0].data); got != "{\"id\":1,\"v\":null}\n{\"id\":2,\"v\":null}\n" {
		t.Errorf("JSONL = %q", got)
	}

	resp = openTestReader(t, s, "events", pb.DataFormat_FORMAT_ARROW_IPC)
	r, err := ipc.NewReader(bytes.NewReader(drainHTTP(t, s, resp.ReaderId)[0].data))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	if !r.Next() {
		t.Fatal(r.Err())
	}
	col := r.RecordBatch().Column(1).(*array.String)
	if !col.IsNull(0) || col.IsNull(1) || col.Value(1) != "null" {
		t.Errorf("IPC v = %v, want SQL NULL then the text null", col)
	}

	resp = openTestReader(t, s, "events", pb.DataFormat_FORMAT_PARQUET)
	tbl := readParquetChunk(t, drainHTTP(t, s, resp.ReaderId)[0].data)
	defer tbl.Release()
	va, ok := tbl.Column(1).Data().Chunk(0).(*extensions.VariantArray)
	if !ok || !va.Storage().IsNull(0) || va.Storage().IsNull(1) {
		t.Errorf("Parquet v = %v (%T), want SQL NULL then a variant null value", tbl.Column(1).Data().Chunk(0), tbl.Column(1).Data().Chunk(0))
	}
}

// A raw Parquet part is streamed as is and a decoded part is re-encoded by
// encodeParquet; the canonical variant survives on both.
func TestReadVariantParquetRawAndDecoded(t *testing.T) {
	sch, a := variantBatchOf(t, variantReadRows)
	defer a.Release()
	raw := parquetBytes(t, sch, a)
	s := variantReadServer(t, 1<<30, rawFakePart(raw, int64(len(variantReadRows))), batchFakePart(a))

	resp := openTestReader(t, s, "events", pb.DataFormat_FORMAT_PARQUET)
	if resp.Schema.Columns[1].Type != "string" {
		t.Errorf("advertised v type = %q, want string", resp.Schema.Columns[1].Type)
	}
	chunks := drainHTTP(t, s, resp.ReaderId)
	if len(chunks) != 2 {
		t.Fatalf("%d chunks, want a raw and a decoded one", len(chunks))
	}
	want := []string{`{"a":1,"b":"x","n":9007199254740993}`, `[1,"two",3.5]`, `"hello"`, "NULL", "NULL", `-9223372036854775808`}
	for i, c := range chunks {
		tbl := readParquetChunk(t, c.data)
		if !arrow.TypeEqual(tbl.Schema().Field(1).Type, extensions.NewDefaultVariantType()) {
			t.Errorf("chunk %d v type %s, want the canonical variant", i, tbl.Schema().Field(1).Type)
		}
		if got := variantJSON(t, tbl.Column(1).Data().Chunk(0)); !reflect.DeepEqual(got, want) {
			t.Errorf("chunk %d v\n got %q\nwant %q", i, got, want)
		}
		tbl.Release()
	}
}
