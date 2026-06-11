//go:build duckdb_arrow

package queryengine

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"
)

// buildResult streams the final statement's *sql.Rows into a Result, applying
// the MaxRows and MaxBytes caps (0 = unlimited for each). It is the single
// place that materializes user query output, so it owns both the row-count cap
// and the serialized-size cap.
//
// MaxBytes is measured on the FINAL SERIALIZED Result envelope (what
// json.Marshal will emit), not on raw row bytes: we seed a running byte counter
// with the JSON-encoded envelope-without-rows size (schema + stats + truncated
// + the surrounding punctuation of an empty `"rows":[]`), then add each row's
// JSON-encoded size (+1 for the inter-row comma) as we append. We stop and set
// Truncated before appending a row that would push the total over MaxBytes.
// This guarantees the post-condition json.Marshal(result) <= MaxBytes. If the
// envelope alone already exceeds MaxBytes, no row can ever fit, so we return
// ErrResultTooLarge rather than an unbounded-but-empty result.
//
// rowsScanned/bytesScanned are intentionally left at zero in Stats: DuckDB does
// not surface a per-query scanned-row / scanned-byte count through the
// database/sql interface, so these stay best-effort/0 (RFC 022 §3).
func buildResult(rows *sql.Rows, maxRows, maxBytes int) (*Result, error) {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("column types: %w", err)
	}

	schema := make([]Column, len(colTypes))
	for i, ct := range colTypes {
		schema[i] = Column{Name: ct.Name(), Type: ct.DatabaseTypeName()}
	}

	res := &Result{
		Schema:    schema,
		Rows:      [][]any{},
		Truncated: false,
	}

	// Seed the byte budget with the envelope-without-rows size. We marshal the
	// in-progress result (schema + empty rows + stats + truncated) once; that is
	// the fixed overhead every serialized envelope carries. Each row JSON we then
	// add accounts for its own bytes plus one inter-row comma. This deliberately
	// slightly OVER-counts (an empty `"rows":[]` already includes the two
	// brackets, and the first row needs no leading comma), which keeps the final
	// json.Marshal(result) strictly <= MaxBytes — never over.
	//
	// DurationMs is written by the caller (runUserSQL) AFTER buildResult returns,
	// so at seed time it is still 0 — a narrower number than the real duration. If
	// we seeded with "duration_ms":0 (1 digit) but the real value is e.g.
	// 1234 (4 digits), the final marshal would be 3 bytes wider than the budget
	// reserved, breaking the strict post-condition. To make the seed reserve the
	// MAXIMUM possible width, we marshal with a maximal-width placeholder
	// (9_999_999_999_999 — 13 digits, ~317 years in ms), then restore it to 0 so
	// the caller writes the real value. The real value's JSON width is always
	// <= the placeholder's, so json.Marshal(result) stays strictly <= MaxBytes.
	var byteBudgetUsed int
	if maxBytes > 0 {
		const maxDurationWidthPlaceholder = 9_999_999_999_999 // 13 digits, widest plausible duration_ms
		res.Stats.DurationMs = maxDurationWidthPlaceholder
		envelope, mErr := json.Marshal(res)
		res.Stats.DurationMs = 0 // restore; caller writes the real (narrower-or-equal) value
		if mErr != nil {
			return nil, fmt.Errorf("measure result envelope: %w", mErr)
		}
		byteBudgetUsed = len(envelope)
		if byteBudgetUsed > maxBytes {
			// The schema/stats/punctuation alone overflow the cap: no row can ever
			// fit. Surface a typed error rather than an unbounded-but-empty result.
			return nil, fmt.Errorf("%w: result envelope is %d bytes (cap %d)",
				ErrResultTooLarge, byteBudgetUsed, maxBytes)
		}
	}

	ncol := len(colTypes)
	for rows.Next() {
		// Row-count cap: if we have already collected maxRows rows and another
		// row exists, the result is truncated — stop without appending. Peeking
		// one extra row (this Next() succeeding) is how we know more existed.
		if maxRows > 0 && len(res.Rows) >= maxRows {
			res.Truncated = true
			break
		}

		// Pre-Scan byte-budget short-circuit: once the budget is already at/over
		// MaxBytes, no further row can fit, so stop BEFORE scanning+marshaling the
		// next one. RESIDUAL EXPOSURE (honest): the row that first pushed the
		// budget over is still scanned and marshaled once below — a single
		// over-budget row's Go-side allocation happens outside duckdb's
		// memory_limit accounting. It is bounded to ONE row (we break here on the
		// next iteration), not unbounded. Phase 5 known-limitations documents it.
		if maxBytes > 0 && byteBudgetUsed >= maxBytes {
			res.Truncated = true
			break
		}

		dest := make([]any, ncol)
		ptrs := make([]any, ncol)
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		for i := range dest {
			dest[i] = normalizeValue(dest[i])
		}

		// Byte-size cap: measure this row's JSON contribution and stop (marking
		// Truncated) if appending it would exceed MaxBytes. Measured on the
		// JSON-encoded row so the budget tracks the SERIALIZED envelope, not raw
		// Go values.
		if maxBytes > 0 {
			rowJSON, mErr := json.Marshal(dest)
			if mErr != nil {
				return nil, fmt.Errorf("measure row size: %w", mErr)
			}
			// +1 for the inter-row comma in the rows array.
			cost := len(rowJSON) + 1
			if byteBudgetUsed+cost > maxBytes {
				res.Truncated = true
				break
			}
			byteBudgetUsed += cost
		}

		res.Rows = append(res.Rows, dest)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration: %w", err)
	}

	return res, nil
}

// normalizeValue converts a value as scanned from duckdb-go's database/sql
// driver into a JSON-marshalable, faithful form. The rule is: keep what
// json.Marshal already handles correctly, and normalize ONLY what json.Marshal
// would reject or mangle, plus widen the narrow integer widths to one uniform
// numeric type:
//
//   - float32 (DuckDB FLOAT / single precision) → float64. duckdb-go scans
//     FLOAT as float32, which the %v fallback would otherwise stringify; widen to
//     float64 so it stays a plain JSON number (DOUBLE already scans as float64).
//   - Signed integers (DuckDB TINYINT/SMALLINT/INTEGER/BIGINT scan as
//     int8/int16/int32/int64) → int64. duckdb-go returns the native Go width, so
//     `SELECT 1+1` (INTEGER) scans as int32. All signed widths fit int64
//     losslessly, so we widen to one type — callers get int64 for every integer
//     column rather than having to switch on width. (json.Marshal renders all of
//     them as plain JSON numbers either way; the widening is for caller
//     ergonomics and a stable Go type, not JSON correctness.)
//   - Unsigned integers (UTINYINT…UBIGINT scan as uint8…uint64) → int64 when
//     they fit, else the decimal string. uint8/16/32 always fit int64; a uint64
//     above math.MaxInt64 would overflow, so it becomes a string to stay
//     faithful.
//   - time.Time → RFC3339 string. DuckDB DATE/TIMESTAMP land here as time.Time;
//     we pin the RFC3339(Nano) string explicitly so the wire shape is stable and
//     not dependent on time.Time's MarshalJSON.
//   - []byte → string. DuckDB VARCHAR usually scans as a Go string, but BLOB /
//     some text/decimal paths scan as []byte, which json.Marshal would
//     base64-encode. For a faithful text/decimal value we keep it as a string.
//   - *big.Int / *big.Float → decimal string. HUGEINT overflows int64 and
//     DECIMAL is not representable as a faithful float64, so the driver returns
//     *big.Int / *big.Float; json.Marshal of those produces a JSON number that
//     loses precision (big.Float) or is inconsistent — we pin the exact decimal
//     string for both.
//   - everything else (DuckDB composite types like LIST/STRUCT/MAP, intervals,
//     UUID) → fmt.Sprintf("%v") fallback string, so json.Marshal can never fail
//     on an unexpected type. This is best-effort textual rendering, not a
//     structured shape; structured composite output is out of MVP scope.
//
// nil passes through unchanged (json null).
func normalizeValue(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case bool, float64, string, int64:
		return x
	case float32:
		// DuckDB FLOAT (single precision) scans as float32; widen to float64 so it
		// marshals as a plain JSON number rather than hitting the %v string fallback.
		return float64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case uint64:
		if x <= math.MaxInt64 {
			return int64(x)
		}
		return strconv.FormatUint(x, 10)
	case uint:
		if uint64(x) <= math.MaxInt64 {
			return int64(x)
		}
		return strconv.FormatUint(uint64(x), 10)
	case time.Time:
		return x.Format(time.RFC3339Nano)
	case []byte:
		return string(x)
	case *big.Int:
		return x.String()
	case *big.Float:
		return x.Text('f', -1)
	case big.Int:
		return x.String()
	case big.Float:
		return x.Text('f', -1)
	default:
		// Faithful-enough textual fallback for composite / exotic types so the
		// Result is always JSON-marshalable. fmt's default %v rendering of the
		// driver's Go value (e.g. duckdb.Interval, a []any list) is used as-is.
		return fmt.Sprintf("%v", x)
	}
}
