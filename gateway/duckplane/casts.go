//go:build duckdb_arrow

package duckplane

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/datuplet/datuplet/gateway/duck"
)

// duckTypeFor maps an accepted Arrow input type to the DuckDB type whose
// Parquet output equals today's writer (testdata/parquet-types.golden.json).
// Types the golden file records as rejected (large_string, large_binary) and
// types it has no entry for are refused.
//
// Timestamps with any time zone are written like UTC ones. Two golden columns
// cannot be reproduced: timestamp[s|ms, <zone>] are written as UTC
// microseconds (DuckDB has no TIMESTAMPTZ in s/ms); see golden_test.go.
func duckTypeFor(dt arrow.DataType) (string, error) {
	switch dt.ID() {
	case arrow.INT8:
		return "TINYINT", nil
	case arrow.INT16:
		return "SMALLINT", nil
	case arrow.INT32:
		return "INTEGER", nil
	case arrow.INT64:
		return "BIGINT", nil
	case arrow.UINT8:
		return "UTINYINT", nil
	case arrow.UINT16:
		return "USMALLINT", nil
	case arrow.UINT32:
		return "UINTEGER", nil
	case arrow.UINT64:
		return "UBIGINT", nil
	case arrow.FLOAT32:
		return "FLOAT", nil
	case arrow.FLOAT64:
		return "DOUBLE", nil
	case arrow.STRING:
		return "VARCHAR", nil
	case arrow.BINARY:
		return "BLOB", nil
	case arrow.BOOL:
		return "BOOLEAN", nil
	case arrow.DATE32, arrow.DATE64:
		return "DATE", nil
	case arrow.TIMESTAMP:
		ts := dt.(*arrow.TimestampType)
		switch ts.TimeZone {
		case "":
			switch ts.Unit {
			case arrow.Second, arrow.Millisecond:
				// DuckDB writes TIMESTAMP_S as microseconds; today's writer
				// wrote seconds as milliseconds.
				return "TIMESTAMP_MS", nil
			case arrow.Microsecond:
				return "TIMESTAMP", nil
			case arrow.Nanosecond:
				return "TIMESTAMP_NS", nil
			}
		default:
			// Any zone: Arrow stores the instants in UTC, and today's writer
			// wrote every zone like UTC (UTC-adjusted Parquet timestamps).
			if ts.Unit == arrow.Nanosecond {
				return "TIMESTAMPTZ_NS", nil
			}
			return "TIMESTAMPTZ", nil
		}
	}
	return "", fmt.Errorf("unsupported Arrow type %s", dt)
}

// selectList is the COPY's SELECT list: every column by position (so input
// names never need to resolve), cast to its golden type, renamed to its
// verbatim Arrow name through duck.Ident.
func selectList(sch *arrow.Schema) (string, error) {
	if sch == nil || sch.NumFields() == 0 {
		return "", fmt.Errorf("duckplane: writer schema has no columns")
	}
	parts := make([]string, sch.NumFields())
	for i, f := range sch.Fields() {
		t, err := duckTypeFor(f.Type)
		if err != nil {
			return "", fmt.Errorf("duckplane: column %q: %w", f.Name, err)
		}
		if strings.IndexByte(f.Name, 0) >= 0 {
			return "", fmt.Errorf("duckplane: column %q: name contains a NUL byte", f.Name)
		}
		parts[i] = fmt.Sprintf("CAST(#%d AS %s) AS %s", i+1, t, duck.Ident(f.Name))
	}
	return strings.Join(parts, ", "), nil
}

// sameColumns reports whether rec's columns have the writer schema's names and
// types (nullability and metadata do not matter to the COPY).
func sameColumns(want, got *arrow.Schema) bool {
	if want.NumFields() != got.NumFields() {
		return false
	}
	for i := range want.Fields() {
		w, g := want.Field(i), got.Field(i)
		if w.Name != g.Name || !arrow.TypeEqual(w.Type, g.Type) {
			return false
		}
	}
	return true
}
