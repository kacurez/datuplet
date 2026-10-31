package buffer_test

// Golden record of the Parquet types today's Data Gateway write path produces
// for each Arrow input type (RFC: DuckDB gateway data plane, Task 1). The
// DuckDB-backed plane is later compared against this file.
//
// The generator drives the real ServerV2 write path (WriteChunk -> Arrow IPC
// format adapter -> schema inference -> BufferManager + StreamingParquetWriter
// configured by processParsedRecord) against a local static backend, with no
// lakekeeper. The HTTP handler differs from the gRPC entry point only in
// calling ParseReader instead of Parse; both share parseFromReader and
// processParsedRecord. Whatever the code does is recorded as-is.
//
// Regenerate (only when the write path is intentionally changed):
//
//	go test ./pkg/datagateway/buffer/ -run TestGoldenParquetTypes -update-golden

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/datuplet/datuplet/pkg/datagateway"
	"github.com/datuplet/datuplet/pkg/datagateway/backend"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

var updateGolden = flag.Bool("update-golden", false, "regenerate gateway/duckplane/testdata/parquet-types.golden.json")

const goldenRelPath = "../../../gateway/duckplane/testdata/parquet-types.golden.json"

type goldenInput struct {
	name string
	dt   arrow.DataType
	// chunk1 / chunk2 are Arrow-JSON arrays: 3 rows each, one null,
	// boundary values (max uint64, pre-1970 timestamps, ...).
	chunk1, chunk2 string
}

func goldenInputs() []goldenInput {
	ts := func(unit arrow.TimeUnit, tz string) arrow.DataType {
		return &arrow.TimestampType{Unit: unit, TimeZone: tz}
	}
	tsCases := func(name string, unit arrow.TimeUnit, tz string, c1, c2 string) goldenInput {
		return goldenInput{name: name, dt: ts(unit, tz), chunk1: c1, chunk2: c2}
	}
	var in []goldenInput
	in = append(in,
		goldenInput{"int8", arrow.PrimitiveTypes.Int8, `[-128, null, 127]`, `[0, 1, null]`},
		goldenInput{"int16", arrow.PrimitiveTypes.Int16, `[-32768, null, 32767]`, `[0, 1, null]`},
		goldenInput{"int32", arrow.PrimitiveTypes.Int32, `[-2147483648, null, 2147483647]`, `[0, 1, null]`},
		goldenInput{"int64", arrow.PrimitiveTypes.Int64, `[-9223372036854775808, null, 9223372036854775807]`, `[0, 1, null]`},
		goldenInput{"uint8", arrow.PrimitiveTypes.Uint8, `[0, null, 255]`, `[1, 128, null]`},
		goldenInput{"uint16", arrow.PrimitiveTypes.Uint16, `[0, null, 65535]`, `[1, 32768, null]`},
		goldenInput{"uint32", arrow.PrimitiveTypes.Uint32, `[0, null, 4294967295]`, `[1, 2147483648, null]`},
		goldenInput{"uint64", arrow.PrimitiveTypes.Uint64, `[0, null, 18446744073709551615]`, `[1, 9223372036854775808, null]`},
		goldenInput{"float32", arrow.PrimitiveTypes.Float32, `[-3.4028235e38, null, 3.4028235e38]`, `[0.5, -0.25, null]`},
		goldenInput{"float64", arrow.PrimitiveTypes.Float64, `[-1.7976931348623157e308, null, 1.7976931348623157e308]`, `[0.5, -0.25, null]`},
		goldenInput{"string", arrow.BinaryTypes.String, `["héllo", null, ""]`, `["a", "", null]`},
		goldenInput{"large_string", arrow.BinaryTypes.LargeString, `["héllo", null, ""]`, `["a", "", null]`},
		goldenInput{"bool", arrow.FixedWidthTypes.Boolean, `[true, null, false]`, `[false, true, null]`},
	)
	// Timestamps are given as RFC 3339 strings (exact at every unit; JSON
	// integers would round through float64 at ns). Rows: 1900 and 1969 are
	// before 1970; sub-second digits exercise each unit's precision.
	for _, tz := range []string{"", "UTC"} {
		suffix := ""
		if tz != "" {
			suffix = "_tz_utc"
		}
		in = append(in,
			tsCases("timestamp_s"+suffix, arrow.Second, tz,
				`["1900-01-01T00:00:00Z", null, "2100-01-01T00:00:00Z"]`,
				`["1970-01-01T00:00:00Z", "1969-12-31T23:59:59Z", null]`),
			tsCases("timestamp_ms"+suffix, arrow.Millisecond, tz,
				`["1900-01-01T00:00:00Z", null, "2100-01-01T00:00:00.123Z"]`,
				`["1970-01-01T00:00:00Z", "1969-12-31T23:59:59.999Z", null]`),
			tsCases("timestamp_us"+suffix, arrow.Microsecond, tz,
				`["1900-01-01T00:00:00Z", null, "2100-01-01T00:00:00.123456Z"]`,
				`["1970-01-01T00:00:00Z", "1969-12-31T23:59:59.999999Z", null]`),
			tsCases("timestamp_ns"+suffix, arrow.Nanosecond, tz,
				`["1900-01-01T00:00:00Z", null, "2100-01-01T00:00:00.123456789Z"]`,
				`["1970-01-01T00:00:00Z", "1969-12-31T23:59:59.999999999Z", null]`),
		)
	}
	in = append(in,
		goldenInput{"date32", arrow.FixedWidthTypes.Date32, `[-1, null, 19000]`, `[0, 1, null]`},
		goldenInput{"date64", arrow.FixedWidthTypes.Date64, `[-86400000, null, 1641600000000]`, `[0, 86400000, null]`},
		// Binary values are base64 in Arrow JSON: "AP8=" = 00 ff, "" = empty.
		goldenInput{"binary", arrow.BinaryTypes.Binary, `["AP8=", null, ""]`, `["YQ==", "", null]`},
		goldenInput{"large_binary", arrow.BinaryTypes.LargeBinary, `["AP8=", null, ""]`, `["YQ==", "", null]`},
	)
	return in
}

type goldenColumn struct {
	Name          string `json:"name"`
	PhysicalType  string `json:"physical_type"`
	LogicalType   string `json:"logical_type"`
	ConvertedType string `json:"converted_type"`
	Repetition    string `json:"repetition"`
	HasFieldID    bool   `json:"field_id_present"`
	ArrowReadType string `json:"arrow_read_type"`
}

type goldenEntry struct {
	InputType string `json:"input_type"`
	Accepted  bool   `json:"accepted"`

	// Rejected inputs.
	RejectedAtChunk int    `json:"rejected_at_chunk,omitempty"`
	RejectedAt      string `json:"rejected_at,omitempty"` // "close" when the failure surfaced at CloseWriter
	Error           string `json:"error,omitempty"`

	// Accepted inputs.
	Files   int            `json:"parquet_files,omitempty"`
	Rows    int64          `json:"rows,omitempty"`
	Columns []goldenColumn `json:"columns,omitempty"`
	// Values are the read-back values of column "v" via pqarrow (using the
	// arrow schema embedded in the file), formatted with Array.ValueStr;
	// chunk 1 rows then chunk 2 rows.
	Values []string `json:"values,omitempty"`
}

type goldenFile struct {
	Description string        `json:"description"`
	Entries     []goldenEntry `json:"entries"`
}

func ipcBytes(t *testing.T, sch *arrow.Schema, arr arrow.Array) []byte {
	t.Helper()
	rec := array.NewRecordBatch(sch, []arrow.Array{arr}, int64(arr.Len()))
	defer rec.Release()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(sch))
	if err := w.Write(rec); err != nil {
		t.Fatalf("ipc write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("ipc close: %v", err)
	}
	return buf.Bytes()
}

func scrub(msg, tmp string) string {
	return strings.ReplaceAll(msg, tmp, "<tmp>")
}

func runGoldenCase(t *testing.T, in goldenInput) goldenEntry {
	t.Helper()
	entry := goldenEntry{InputType: in.name}
	tmp := t.TempDir()
	ctx := context.Background()

	// Production defaults (see generateGatewayConfig / LoadGatewayConfigFile).
	srv := datagateway.NewServerV2(&datagateway.Config{
		RunID:          "00000000-golden",
		ComponentName:  "golden",
		DefaultBucket:  "raw",
		BufferSize:     64 << 20,
		RowGroupSize:   64 << 20,
		TargetFileSize: 128 << 20,
		Backend:        backend.NewLocalBackend(backend.LocalConfig{}), // DataDir "": absolute paths pass through
	})
	defer srv.Close()

	// bucket = tmp dir so basePath "<tmp>/t/data/" is an absolute local path.
	open, err := srv.OpenWriter(ctx, &pb.OpenWriterRequest{
		Bucket:      tmp,
		Table:       "t",
		InputFormat: pb.DataFormat_FORMAT_ARROW_IPC,
	})
	if err != nil {
		t.Fatalf("%s: OpenWriter: %v", in.name, err)
	}

	sch := arrow.NewSchema([]arrow.Field{{Name: "v", Type: in.dt, Nullable: true}}, nil)
	mem := memory.NewGoAllocator()
	for i, js := range []string{in.chunk1, in.chunk2} {
		arr, _, err := array.FromJSON(mem, in.dt, strings.NewReader(js))
		if err != nil {
			t.Fatalf("%s: building chunk %d: %v", in.name, i+1, err)
		}
		payload := ipcBytes(t, sch, arr)
		arr.Release()
		if _, err := srv.WriteChunk(ctx, &pb.WriteChunkRequest{WriterId: open.WriterId, Data: payload}); err != nil {
			entry.RejectedAtChunk = i + 1
			entry.Error = scrub(err.Error(), tmp)
			return entry
		}
	}

	if _, err := srv.CloseWriter(ctx, &pb.CloseWriterRequest{WriterId: open.WriterId}); err != nil {
		entry.RejectedAt = "close"
		entry.Error = scrub(err.Error(), tmp)
		return entry
	}

	files, err := filepath.Glob(filepath.Join(tmp, "t", "data", "*.parquet"))
	if err != nil || len(files) == 0 {
		t.Fatalf("%s: no parquet files under %s (err=%v)", in.name, tmp, err)
	}
	entry.Accepted = true
	entry.Files = len(files)

	for fi, path := range files {
		rdr, err := file.OpenParquetFile(path, false)
		if err != nil {
			t.Fatalf("%s: open parquet: %v", in.name, err)
		}
		pqSchema := rdr.MetaData().Schema
		fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, mem)
		if err != nil {
			t.Fatalf("%s: pqarrow reader: %v", in.name, err)
		}
		arrowSchema, err := fr.Schema()
		if err != nil {
			t.Fatalf("%s: arrow schema: %v", in.name, err)
		}
		if fi == 0 {
			for c := 0; c < pqSchema.NumColumns(); c++ {
				col := pqSchema.Column(c)
				node := col.SchemaNode()
				gc := goldenColumn{
					Name:          col.Name(),
					PhysicalType:  col.PhysicalType().String(),
					LogicalType:   col.LogicalType().String(),
					ConvertedType: col.ConvertedType().String(),
					Repetition:    node.RepetitionType().String(),
					HasFieldID:    node.FieldID() >= 0,
				}
				if c < arrowSchema.NumFields() {
					gc.ArrowReadType = arrowSchema.Field(c).Type.String()
				}
				entry.Columns = append(entry.Columns, gc)
			}
		}
		tbl, err := fr.ReadTable(context.Background())
		if err != nil {
			t.Fatalf("%s: read table: %v", in.name, err)
		}
		entry.Rows += tbl.NumRows()
		col := tbl.Column(0).Data()
		for _, chunk := range col.Chunks() {
			for r := 0; r < chunk.Len(); r++ {
				entry.Values = append(entry.Values, chunk.ValueStr(r))
			}
		}
		tbl.Release()
		rdr.Close()
	}
	return entry
}

func generateGolden(t *testing.T) []byte {
	t.Helper()
	g := goldenFile{
		Description: "Parquet output of today's Go Data Gateway write path (ServerV2 -> Arrow IPC adapter -> " +
			"schema inference -> BufferManager/StreamingParquetWriter, local static backend) per Arrow input type. " +
			"Two 3-row IPC chunks per type (one null, boundary values). Generated by " +
			"pkg/datagateway/buffer TestGoldenParquetTypes -update-golden; the DuckDB data plane is compared against it.",
	}
	for _, in := range goldenInputs() {
		g.Entries = append(g.Entries, runGoldenCase(t, in))
	}
	out, err := json.MarshalIndent(g, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(out, '\n')
}

func TestGoldenParquetTypes(t *testing.T) {
	if *updateGolden {
		out := generateGolden(t)
		if err := os.MkdirAll(filepath.Dir(goldenRelPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenRelPath, out, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s (%d bytes)", goldenRelPath, len(out))
		return
	}

	raw, err := os.ReadFile(goldenRelPath)
	if err != nil {
		t.Fatalf("golden file missing (%v); run with -update-golden", err)
	}
	var g goldenFile
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatalf("golden file does not parse: %v", err)
	}
	have := map[string]bool{}
	for _, e := range g.Entries {
		have[e.InputType] = true
	}
	for _, in := range goldenInputs() {
		if !have[in.name] {
			t.Errorf("golden file has no entry for input type %q", in.name)
		}
	}

	// Today's write path must still reproduce the recorded behaviour.
	if got := generateGolden(t); !bytes.Equal(got, raw) {
		t.Errorf("today's write path no longer matches %s; if intentional, regenerate with -update-golden\n%s",
			goldenRelPath, firstDiff(string(raw), string(got)))
	}
}

func firstDiff(want, got string) string {
	wl, gl := strings.Split(want, "\n"), strings.Split(got, "\n")
	for i := 0; i < len(wl) && i < len(gl); i++ {
		if wl[i] != gl[i] {
			return fmt.Sprintf("first difference at line %d:\n  golden:    %s\n  generated: %s", i+1, wl[i], gl[i])
		}
	}
	return fmt.Sprintf("length differs: golden %d lines, generated %d lines", len(wl), len(gl))
}
