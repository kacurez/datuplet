package format

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/google/uuid"
)

func variantArrayOf(t *testing.T, vals ...variant.Value) *extensions.VariantArray {
	t.Helper()
	b := extensions.NewVariantBuilder(memory.DefaultAllocator, extensions.NewDefaultVariantType())
	defer b.Release()
	for _, v := range vals {
		b.Append(v)
	}
	return b.NewArray().(*extensions.VariantArray)
}

func renderVariant(t *testing.T, v variant.Value) (string, error) {
	t.Helper()
	arr := variantArrayOf(t, v)
	defer arr.Release()
	got, err := VariantJSON(arr, 0)
	return string(got), err
}

func TestVariantJSONRendering(t *testing.T) {
	cases := []struct{ in, want string }{
		{`[]`, `[]`},
		{`{"t":[]}`, `{"t":[]}`},
		{`[[]]`, `[[]]`},
		{`{}`, `{}`},
		{`[{},[]]`, `[{},[]]`},
		{`{"a":{"b":[1,[],{"c":[]}]},"z":[[],[[]]]}`, `{"a":{"b":[1,[],{"c":[]}]},"z":[[],[[]]]}`},
		{`"a<b&c>"`, `"a<b&c>"`},
		{`{"k<&>":"v<&>"}`, `{"k<&>":"v<&>"}`},
		{`"quote\" newline\n"`, `"quote\" newline\n"`},
		{`9007199254740993`, `9007199254740993`},
		{`-9223372036854775808`, `-9223372036854775808`},
		{`12345678901234567890.123456789`, `12345678901234567890.123456789`},
		{`1.50`, `1.50`},
		{`3.5`, `3.5`},
		{`true`, `true`},
		{`false`, `false`},
		{`[null,true,false]`, `[null,true,false]`},
		{`{"a":null}`, `{"a":null}`},
	}
	for _, c := range cases {
		v, err := variant.ParseJSON(c.in, false)
		if err != nil {
			t.Fatalf("%s: %v", c.in, err)
		}
		got, err := renderVariant(t, v)
		if err != nil {
			t.Errorf("%s: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("render %s = %s, want %s", c.in, got, c.want)
		}
	}
}

// TestVariantJSONPrimitives covers the typed primitives JSON text cannot
// produce. Zone-less temporal types render the UTC wall clock whatever the
// process zone, so time.Local is moved off UTC for the test.
func TestVariantJSONPrimitives(t *testing.T) {
	saved := time.Local
	time.Local = time.FixedZone("UTC+5", 5*3600)
	t.Cleanup(func() { time.Local = saved })

	instant := time.Date(2024, 1, 2, 3, 4, 5, 123456789, time.UTC)
	micros := arrow.Timestamp(instant.UnixMicro())
	nanos := arrow.Timestamp(instant.UnixNano())
	u := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")

	cases := []struct {
		name string
		val  func() (variant.Value, error)
		want string
	}{
		{"timestamp micros ntz", func() (variant.Value, error) { return variant.Of(micros) }, `"2024-01-02 03:04:05.123456"`},
		{"timestamp nanos ntz", func() (variant.Value, error) { return variant.Of(nanos, variant.OptTimestampNano) }, `"2024-01-02 03:04:05.123456789"`},
		{"timestamp micros tz", func() (variant.Value, error) { return variant.Of(micros, variant.OptTimestampUTC) }, `"2024-01-02 03:04:05.123456Z"`},
		{"timestamp nanos tz", func() (variant.Value, error) {
			return variant.Of(nanos, variant.OptTimestampNano|variant.OptTimestampUTC)
		}, `"2024-01-02 03:04:05.123456789Z"`},
		{"time", func() (variant.Value, error) {
			return variant.Of(arrow.Time64((3*3600+4*60+5)*1_000_000 + 123456))
		}, `"03:04:05.123456"`},
		{"date", func() (variant.Value, error) { return variant.Of(arrow.Date32FromTime(instant)) }, `"2024-01-02"`},
		{"binary", func() (variant.Value, error) { return variant.Of([]byte{0x00, 0xff, 'a'}) }, `"AP9h"`},
		{"uuid", func() (variant.Value, error) { return variant.Of(u) }, `"123e4567-e89b-12d3-a456-426614174000"`},
		{"decimal4", func() (variant.Value, error) {
			return variant.Of(variant.DecimalValue[decimal.Decimal32]{Scale: 2, Value: decimal.Decimal32(12345)})
		}, `123.45`},
		{"decimal4 negative", func() (variant.Value, error) {
			return variant.Of(variant.DecimalValue[decimal.Decimal32]{Scale: 3, Value: decimal.Decimal32(-5)})
		}, `-0.005`},
		{"decimal8", func() (variant.Value, error) {
			return variant.Of(variant.DecimalValue[decimal.Decimal64]{Scale: 4, Value: decimal.Decimal64(-1234567890123)})
		}, `-123456789.0123`},
		{"decimal16", func() (variant.Value, error) {
			n, _ := decimal128.FromString("12345678901234567890123456789012345678", 38, 0)
			return variant.Of(variant.DecimalValue[decimal.Decimal128]{Scale: 10, Value: n})
		}, `1234567890123456789012345678.9012345678`},
		{"decimal16 negative", func() (variant.Value, error) {
			return variant.Of(variant.DecimalValue[decimal.Decimal128]{Scale: 2, Value: decimal128.FromI64(-100)})
		}, `-1.00`},
	}
	for _, c := range cases {
		v, err := c.val()
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		got, err := renderVariant(t, v)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s = %s, want %s", c.name, got, c.want)
		}
	}

	nan, err := variant.Of(math.NaN())
	if err != nil {
		t.Fatal(err)
	}
	if got, err := renderVariant(t, nan); err == nil {
		t.Errorf("NaN rendered as %s, want an error", got)
	}
}

// TestVariantRenderErrorNamesRowAndColumn sends a NaN double variant through
// each text serializer: the error names the row and column once.
func TestVariantRenderErrorNamesRowAndColumn(t *testing.T) {
	ok, err := variant.Of(int64(1))
	if err != nil {
		t.Fatal(err)
	}
	nan, err := variant.Of(math.NaN())
	if err != nil {
		t.Fatal(err)
	}
	arr := variantArrayOf(t, ok, nan)
	defer arr.Release()
	sch := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arr.DataType(), Nullable: true}}, nil)
	rec := array.NewRecordBatch(sch, []arrow.Array{arr}, int64(arr.Len()))
	defer rec.Release()

	for name, a := range map[string]FormatAdapter{
		"json":  NewJSONAdapter(nil, nil),
		"jsonl": NewJSONLAdapter(nil, nil),
		"csv":   NewCSVAdapter(nil, nil),
	} {
		_, err := a.Serialize(rec)
		if err == nil {
			t.Errorf("%s: Serialize succeeded, want a render error", name)
			continue
		}
		msg := err.Error()
		if !strings.Contains(msg, "row 1") || !strings.Contains(msg, `column "v"`) || strings.Count(msg, "row 1") != 1 {
			t.Errorf("%s: error %q, want row 1 and column \"v\" named once", name, msg)
		}
	}
}
