//go:build duckdb_arrow

package duckplane

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestDuckTypeFor(t *testing.T) {
	ts := func(u arrow.TimeUnit, tz string) arrow.DataType { return &arrow.TimestampType{Unit: u, TimeZone: tz} }
	for _, tc := range []struct {
		dt   arrow.DataType
		want string
	}{
		{arrow.PrimitiveTypes.Int8, "TINYINT"},
		{arrow.PrimitiveTypes.Int16, "SMALLINT"},
		{arrow.PrimitiveTypes.Int32, "INTEGER"},
		{arrow.PrimitiveTypes.Int64, "BIGINT"},
		{arrow.PrimitiveTypes.Uint8, "UTINYINT"},
		{arrow.PrimitiveTypes.Uint16, "USMALLINT"},
		{arrow.PrimitiveTypes.Uint32, "UINTEGER"},
		{arrow.PrimitiveTypes.Uint64, "UBIGINT"},
		{arrow.PrimitiveTypes.Float32, "FLOAT"},
		{arrow.PrimitiveTypes.Float64, "DOUBLE"},
		{arrow.BinaryTypes.String, "VARCHAR"},
		{arrow.BinaryTypes.Binary, "BLOB"},
		{arrow.FixedWidthTypes.Boolean, "BOOLEAN"},
		{arrow.FixedWidthTypes.Date32, "DATE"},
		{arrow.FixedWidthTypes.Date64, "DATE"},
		{ts(arrow.Second, ""), "TIMESTAMP_MS"},
		{ts(arrow.Millisecond, ""), "TIMESTAMP_MS"},
		{ts(arrow.Microsecond, ""), "TIMESTAMP"},
		{ts(arrow.Nanosecond, ""), "TIMESTAMP_NS"},
		{ts(arrow.Second, "UTC"), "TIMESTAMPTZ"},
		{ts(arrow.Millisecond, "UTC"), "TIMESTAMPTZ"},
		{ts(arrow.Microsecond, "UTC"), "TIMESTAMPTZ"},
		{ts(arrow.Nanosecond, "UTC"), "TIMESTAMPTZ_NS"},
		{ts(arrow.Second, "Europe/Prague"), "TIMESTAMPTZ"},
		{ts(arrow.Millisecond, "+01:00"), "TIMESTAMPTZ"},
		{ts(arrow.Microsecond, "America/New_York"), "TIMESTAMPTZ"},
		{ts(arrow.Nanosecond, "Europe/Prague"), "TIMESTAMPTZ_NS"},
	} {
		got, err := duckTypeFor(tc.dt)
		if err != nil || got != tc.want {
			t.Errorf("%v: got %q, %v; want %q", tc.dt, got, err, tc.want)
		}
	}
}

func TestDuckTypeForRejects(t *testing.T) {
	for _, dt := range []arrow.DataType{
		arrow.BinaryTypes.LargeString,
		arrow.BinaryTypes.LargeBinary,
		arrow.BinaryTypes.StringView,
		arrow.BinaryTypes.BinaryView,
		arrow.Null,
		arrow.FixedWidthTypes.Float16,
		&arrow.Decimal128Type{Precision: 10, Scale: 2},
		arrow.ListOf(arrow.PrimitiveTypes.Int64),
		arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64}),
		arrow.FixedWidthTypes.Time64us,
		&arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String},
	} {
		if got, err := duckTypeFor(dt); err == nil || !strings.Contains(err.Error(), "unsupported Arrow type") {
			t.Errorf("%v: got %q, %v; want unsupported Arrow type", dt, got, err)
		}
	}
}

func TestSelectListQuotesEveryColumn(t *testing.T) {
	sch := arrow.NewSchema([]arrow.Field{
		{Name: `a"b`, Type: arrow.PrimitiveTypes.Int64},
		{Name: "x'y\n", Type: arrow.BinaryTypes.String},
	}, nil)
	got, err := selectList(sch)
	if err != nil {
		t.Fatal(err)
	}
	want := `CAST(#1 AS BIGINT) AS "a""b", CAST(#2 AS VARCHAR) AS "x'y` + "\n" + `"`
	if got != want {
		t.Fatalf("selectList = %q, want %q", got, want)
	}
	bad := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.BinaryTypes.LargeString}}, nil)
	if _, err := selectList(bad); err == nil || !strings.Contains(err.Error(), `column "v"`) {
		t.Fatalf("selectList(large_string) = %v", err)
	}
}
