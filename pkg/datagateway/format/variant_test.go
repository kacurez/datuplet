package format

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

func variantSchema(t *testing.T) *schema.Schema {
	t.Helper()
	s, err := schema.NewSchema([]schema.ColumnDef{
		{Name: "id", Type: schema.TypeInt64, Nullable: true},
		{Name: "v", Type: schema.TypeVariant, Nullable: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// variantValues renders a variant column row by row: "NULL" for SQL NULL,
// otherwise the value's JSON.
func variantValues(t *testing.T, col arrow.Array) []string {
	t.Helper()
	va, ok := col.(*extensions.VariantArray)
	if !ok {
		t.Fatalf("column is %T (%s), want a variant array", col, col.DataType())
	}
	out := make([]string, va.Len())
	for i := range out {
		if va.IsNull(i) {
			out[i] = "NULL"
			continue
		}
		v, err := va.Value(i)
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		b, err := v.MarshalJSON()
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		out[i] = string(b)
	}
	return out
}

func variantAt(t *testing.T, col arrow.Array, i int) variant.Value {
	t.Helper()
	v, err := col.(*extensions.VariantArray).Value(i)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// JSON-native values are stored as they are: "42" stays a string, null is
// SQL NULL, objects and arrays become variant objects and arrays.
func TestVariantJSONNativeValues(t *testing.T) {
	input := `[{"id":1,"v":{"a":1,"b":"x"}},{"id":2,"v":[1,"two",false]},{"id":3,"v":{"o":{"l":[true,null,{"z":1}]}}},` +
		`{"id":4,"v":"hello"},{"id":5,"v":"42"},{"id":6,"v":7},{"id":7,"v":true},{"id":8,"v":null},{"id":9}]`
	rec, _, err := NewJSONAdapter(nil, nil).Parse([]byte(input), variantSchema(t))
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	if !IsCanonicalVariant(rec.Column(1).DataType()) {
		t.Fatalf("v type %s, want the canonical variant", rec.Column(1).DataType())
	}
	want := []string{`{"a":1,"b":"x"}`, `[1,"two",false]`, `{"o":{"l":[true,null,{"z":1}]}}`, `"hello"`, `"42"`, `7`, `true`, "NULL", "NULL"}
	if got := variantValues(t, rec.Column(1)); !reflect.DeepEqual(got, want) {
		t.Fatalf("values\n got %v\nwant %v", got, want)
	}
	if typ := variantAt(t, rec.Column(1), 4).Type(); typ != variant.String {
		t.Fatalf(`"42" stored as type %v, want a variant string`, typ)
	}
	if typ := variantAt(t, rec.Column(1), 5).Type(); typ != variant.Int8 && typ != variant.Int16 && typ != variant.Int32 && typ != variant.Int64 {
		t.Fatalf("7 stored as type %v, want a variant int", typ)
	}
}

// Integers stay exact through the buffered JSON and the streaming JSONL
// paths: int64 min/max as variant ints, a nested 2^53+1.
func TestVariantJSONExactIntegers(t *testing.T) {
	rows := []string{`{"id":1,"v":-9223372036854775808}`, `{"id":2,"v":9223372036854775807}`, `{"id":3,"v":{"n":9007199254740993}}`}
	want := []string{`-9223372036854775808`, `9223372036854775807`, `{"n":9007199254740993}`}
	parse := map[string]func() (arrow.Record, error){
		"json": func() (arrow.Record, error) {
			r, _, err := NewJSONAdapter(nil, nil).Parse([]byte("["+strings.Join(rows, ",")+"]"), variantSchema(t))
			return r, err
		},
		"jsonl-streaming": func() (arrow.Record, error) {
			r, _, err := NewJSONLAdapter(nil, nil).ParseReader(strings.NewReader(strings.Join(rows, "\n")), variantSchema(t))
			return r, err
		},
		"jsonl-buffered": func() (arrow.Record, error) {
			r, _, err := NewJSONLAdapter(nil, nil).Parse([]byte(strings.Join(rows, "\n")), variantSchema(t))
			return r, err
		},
	}
	for name, p := range parse {
		t.Run(name, func(t *testing.T) {
			rec, err := p()
			if err != nil {
				t.Fatal(err)
			}
			defer rec.Release()
			if got := variantValues(t, rec.Column(1)); !reflect.DeepEqual(got, want) {
				t.Fatalf("values\n got %v\nwant %v", got, want)
			}
			for i, n := range []int64{-9223372036854775808, 9223372036854775807} {
				v := variantAt(t, rec.Column(1), i)
				if v.Type() != variant.Int64 || v.Value() != n {
					t.Fatalf("row %d = type %v %v, want int64 %d", i, v.Type(), v.Value(), n)
				}
			}
			n, err := variantAt(t, rec.Column(1), 2).Value().(variant.ObjectValue).ValueByKey("n")
			if err != nil || n.Value.Value() != int64(9007199254740993) {
				t.Fatalf("nested n = %v (%v), want int64 9007199254740993", n.Value.Value(), err)
			}
		})
	}
}

// Adding UseNumber does not change inference for non-variant columns, nor
// the values stored in them: the inferred schema and the record equal those
// of the same objects decoded with json.Unmarshal (float64 numbers).
func TestScalarInferenceUnchangedByUseNumber(t *testing.T) {
	input := `[{"i":1,"f":1.0,"e":1e2,"big":9007199254740993,"neg":-5,"mix":1,"s":"2","b":true,"ts":1.5},` +
		`{"i":2,"f":2.5,"e":3e-1,"big":1,"neg":-6,"mix":2.5,"s":"x","b":false,"ts":2}]`
	var floats []map[string]any
	if err := json.Unmarshal([]byte(input), &floats); err != nil {
		t.Fatal(err)
	}
	want, err := schema.InferSchemaFromJSON(floats)
	if err != nil {
		t.Fatal(err)
	}
	for name, a := range map[string]FormatAdapter{"json": NewJSONAdapter(nil, nil), "jsonl": NewJSONLAdapter(nil, nil)} {
		t.Run(name, func(t *testing.T) {
			data := input
			if name == "jsonl" {
				var objs []json.RawMessage
				if err := json.Unmarshal([]byte(input), &objs); err != nil {
					t.Fatal(err)
				}
				var lines []string
				for _, o := range objs {
					lines = append(lines, string(o))
				}
				data = strings.Join(lines, "\n")
			}
			rec, got, err := a.Parse([]byte(data), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer rec.Release()
			if !got.Equal(want) {
				t.Fatalf("inferred %v, want %v", got.Columns(), want.Columns())
			}
			wantRec, err := NewJSONAdapter(nil, nil).buildRecord(want, floats)
			if err != nil {
				t.Fatal(err)
			}
			defer wantRec.Release()
			if !array.RecordEqual(rec, wantRec) {
				t.Fatalf("record\n got %v\nwant %v", rec, wantRec)
			}
		})
	}
}

// Text input is parsed as JSON text; a variant string needs JSON-quoted text.
func TestVariantTextInputCSVAndUTF8(t *testing.T) {
	csvIn := "id,v\n1,\"{\"\"a\"\":1}\"\n2,\"\"\"hello\"\"\"\n3,[1]\n"
	rec, _, err := NewCSVAdapter(nil, nil).Parse([]byte(csvIn), variantSchema(t))
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	if got, want := variantValues(t, rec.Column(1)), []string{`{"a":1}`, `"hello"`, `[1]`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("csv values %v, want %v", got, want)
	}
	if _, _, err := NewCSVAdapter(nil, nil).Parse([]byte("id,v\n1,1\n2,hello\n"), variantSchema(t)); err == nil ||
		!strings.Contains(err.Error(), `row 1, column "v"`) || !strings.Contains(err.Error(), "invalid JSON") {
		t.Fatalf("invalid csv cell err = %v, want an invalid JSON error naming row 1 and column v", err)
	}

	b := array.NewStringBuilder(memory.DefaultAllocator)
	b.AppendValues([]string{`{"a":1}`, `"hello"`, `42`, ``}, []bool{true, true, true, false})
	text := b.NewArray()
	defer text.Release()
	out, err := ToCanonicalVariant(text, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if got, want := variantValues(t, out), []string{`{"a":1}`, `"hello"`, `42`, "NULL"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("utf8 values %v, want %v", got, want)
	}
	b.AppendValues([]string{`1`, `hello`, `{"a":1} x`}, nil)
	bad := b.NewArray()
	defer bad.Release()
	if _, err := ToCanonicalVariant(bad, memory.DefaultAllocator); err == nil || !strings.Contains(err.Error(), "row 1") {
		t.Fatalf("invalid utf8 err = %v, want one naming row 1", err)
	}
}

// fixtureVariant returns the v column of a testdata/variant fixture.
func fixtureVariant(t *testing.T, name string) (arrow.Record, arrow.Array) {
	t.Helper()
	data, err := os.ReadFile("../testdata/variant/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var rec arrow.Record
	if strings.HasSuffix(name, ".parquet") {
		rec, _, err = NewParquetAdapter(nil, nil).Parse(data, nil)
	} else {
		rec, _, err = NewArrowIPCAdapter(nil).ParseReader(bytes.NewReader(data), nil)
	}
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return rec, rec.Column(1)
}

// Every variant layout ends up type-equal to the default; the canonical one
// is retained as is.
func TestVariantNormalizesEveryLayout(t *testing.T) {
	layouts := map[string][]string{
		"shredded_duckdb.parquet": {`{"a":1,"b":"x"}`, `{"a":2,"b":"y"}`, `{"a":9007199254740993,"b":"z","c":[1,2]}`, "NULL", `"hello"`},
		"large_binary.arrow":      {`{"a":1,"b":"x","n":9007199254740993}`, `[1,"two",3.5]`, `"hello"`, "NULL"},
		"dict_metadata.arrow":     {`{"a":1,"b":"x","n":9007199254740993}`, `[1,"two",3.5]`, `"hello"`, "NULL"},
	}
	for name, want := range layouts {
		t.Run(name, func(t *testing.T) {
			rec, col := fixtureVariant(t, name)
			defer rec.Release()
			if IsCanonicalVariant(col.DataType()) {
				t.Fatalf("fixture %s is already canonical (%s)", name, col.DataType())
			}
			if dt, err := schema.FromArrowType(col.DataType()); err != nil || dt != schema.TypeVariant {
				t.Fatalf("FromArrowType = %v, %v; want variant", dt, err)
			}
			out, err := ToCanonicalVariant(col, memory.DefaultAllocator)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Release()
			if !arrow.TypeEqual(out.DataType(), extensions.NewDefaultVariantType()) {
				t.Fatalf("normalized type %s, want the default variant", out.DataType())
			}
			if got := variantValues(t, out); !reflect.DeepEqual(got, want) {
				t.Fatalf("values\n got %v\nwant %v", got, want)
			}
		})
	}
	t.Run("canonical", func(t *testing.T) {
		rec, _, err := NewJSONAdapter(nil, nil).Parse([]byte(`[{"id":1,"v":{"a":1}}]`), variantSchema(t))
		if err != nil {
			t.Fatal(err)
		}
		defer rec.Release()
		out, err := ToCanonicalVariant(rec.Column(1), memory.DefaultAllocator)
		if err != nil {
			t.Fatal(err)
		}
		defer out.Release()
		if out != rec.Column(1) {
			t.Fatal("the canonical layout was rebuilt, want it retained as is")
		}
	})
}
