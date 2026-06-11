//go:build duckdb_arrow

// RFC 022 Task 1.5 — tests for the public Run entrypoint (lifecycle, final-
// statement result, caps, cancellation). These are PURE-COMPUTE tests: no
// Request.LakekeeperURL is set, so Run skips attachCatalog and exercises only
// the open → lock → QueryContext → build-result path. The catalog-attached
// path is covered by integration_test.go.
//
// Contract implemented here is the one decided by Phase 0 Spike 0.4
// (multi-statement final-statement semantics) and Spike 0.3 (cancellation
// interrupts). See docs/tmp/spikes/spike-0.{3,4}-*.md.
package queryengine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestFinalStatementResult: a multi-statement script returns ONLY the final
// statement's result; earlier statements run as setup and share connection
// state (the TEMP TABLE is visible to the final SELECT). Spike 0.4 case (a).
func TestFinalStatementResult(t *testing.T) {
	res, err := Run(context.Background(), Request{
		SQL:     "CREATE TEMP TABLE t AS SELECT 1 a; SELECT a*2 AS b FROM t;",
		TempDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Schema) != 1 || res.Schema[0].Name != "b" {
		t.Fatalf("schema = %+v, want one column named b", res.Schema)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %v, want exactly 1 row", res.Rows)
	}
	if got := res.Rows[0][0]; got != int64(2) {
		t.Fatalf("Rows[0][0] = %v (%T), want int64(2)", got, got)
	}
	if res.Truncated {
		t.Fatal("Truncated should be false")
	}
}

// TestFinalStatementNonResult: a final non-result-producing statement (DDL)
// returns DuckDB's status result set AS-IS, not a fabricated empty grid. Spike
// 0.4 case (c): `CREATE … AS …` final → cols=[Count] rows=[[1]]. We assert what
// reality returns rather than guessing.
func TestFinalStatementNonResult(t *testing.T) {
	res, err := Run(context.Background(), Request{
		SQL:     "SELECT 1; CREATE TEMP TABLE t2 AS SELECT 41 x;",
		TempDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Empirically (duckdb-go v2.10503.0) the status result set for CREATE..AS is
	// cols=[Count] rows=[[1]]. Assert that exact shape so a driver change surfaces.
	if len(res.Schema) != 1 || res.Schema[0].Name != "Count" {
		t.Fatalf("schema = %+v, want one column named Count (DuckDB status result set)", res.Schema)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		t.Fatalf("rows = %v, want one row with one value", res.Rows)
	}
	if got := res.Rows[0][0]; got != int64(1) {
		t.Fatalf("Rows[0][0] = %v (%T), want int64(1) (one row affected)", got, got)
	}
}

// TestRowCapTruncates: MaxRows caps the returned rows and sets Truncated when
// more rows existed; with a generous cap there is no truncation.
func TestRowCapTruncates(t *testing.T) {
	t.Run("truncates", func(t *testing.T) {
		res, err := Run(context.Background(), Request{
			SQL:     "SELECT * FROM range(100)",
			MaxRows: 10,
			TempDir: t.TempDir(),
		})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if !res.Truncated {
			t.Fatal("Truncated should be true (100 rows, cap 10)")
		}
		if len(res.Rows) != 10 {
			t.Fatalf("len(Rows) = %d, want 10", len(res.Rows))
		}
	})
	t.Run("no-truncation", func(t *testing.T) {
		res, err := Run(context.Background(), Request{
			SQL:     "SELECT * FROM range(100)",
			MaxRows: 200,
			TempDir: t.TempDir(),
		})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if res.Truncated {
			t.Fatal("Truncated should be false (100 rows, cap 200)")
		}
		if len(res.Rows) != 100 {
			t.Fatalf("len(Rows) = %d, want 100", len(res.Rows))
		}
	})
}

// TestByteCapUsesJSONSize: MaxBytes is measured on the FINAL SERIALIZED Result
// envelope. After building, json.Marshal(result) must be <= MaxBytes. An
// envelope that alone exceeds MaxBytes (zero rows still too big) returns
// ErrResultTooLarge.
func TestByteCapUsesJSONSize(t *testing.T) {
	t.Run("truncates-and-fits", func(t *testing.T) {
		const maxBytes = 5000
		res, err := Run(context.Background(), Request{
			SQL:      "SELECT repeat('x',1000) AS s FROM range(50)",
			MaxBytes: maxBytes,
			TempDir:  t.TempDir(),
		})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if !res.Truncated {
			t.Fatal("Truncated should be true (50 wide rows, ~1KB each, cap 5000)")
		}
		// Post-condition with the REAL duration present: Run wrote the actual
		// DurationMs (via runUserSQL) AFTER buildResult seeded the byte budget with
		// a maximal-width placeholder. The marshaled envelope below therefore
		// carries the real "duration_ms" — it must STILL fit under MaxBytes,
		// proving the placeholder reserved enough width (fix 1).
		if res.Stats.DurationMs < 0 {
			t.Fatalf("DurationMs must be >= 0, got %d", res.Stats.DurationMs)
		}
		b, mErr := json.Marshal(res)
		if mErr != nil {
			t.Fatalf("json.Marshal(result): %v", mErr)
		}
		if len(b) > maxBytes {
			t.Fatalf("serialized result is %d bytes (incl. duration_ms=%d), must be <= MaxBytes (%d)",
				len(b), res.Stats.DurationMs, maxBytes)
		}
	})
	t.Run("envelope-alone-too-big", func(t *testing.T) {
		// A tiny MaxBytes smaller than the empty envelope itself must reject with
		// ErrResultTooLarge (the schema/stats/punctuation alone overflows the cap).
		_, err := Run(context.Background(), Request{
			SQL:      "SELECT repeat('x',1000) AS some_long_column_name FROM range(50)",
			MaxBytes: 10,
			TempDir:  t.TempDir(),
		})
		if !errors.Is(err, ErrResultTooLarge) {
			t.Fatalf("err = %v, want ErrResultTooLarge", err)
		}
	})
}

// TestTimeoutInterrupts: a long cross-join scan with a 2s Request.Timeout is
// interrupted; the error maps to ErrTimeout and the call returns well within
// the watchdog window. A fresh Run afterwards proves no leaked process state.
// Spike 0.3 (cancellation interrupts) + 0.4 step 3 (conn reusable after cancel)
// — except here each Run opens its OWN engine.
func TestTimeoutInterrupts(t *testing.T) {
	start := time.Now()
	_, err := Run(context.Background(), Request{
		SQL:     "SELECT count(*) FROM range(33000000) a(x) CROSS JOIN range(3000) b(y) WHERE (a.x*b.y)%7=0",
		Timeout: 2 * time.Second,
		TempDir: t.TempDir(),
	})
	elapsed := time.Since(start)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("err = %v, want errors.Is(err, ErrTimeout)", err)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("call took %v, want it to interrupt within a few seconds of the 2s deadline", elapsed)
	}
	t.Logf("interrupted after %v: %v", elapsed, err)

	// No leaked process/engine state: a fresh Run works normally.
	res, err := Run(context.Background(), Request{SQL: "SELECT 1 AS one", TempDir: t.TempDir()})
	if err != nil {
		t.Fatalf("fresh Run after timeout: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0] != int64(1) {
		t.Fatalf("fresh Run result = %+v, want one row [1]", res)
	}
}

// TestStatementErrorFailsWhole: an error in any statement fails the whole call
// with DuckDB's native error; the message names the offending table (line info
// preserved). Spike 0.4 case (d).
func TestStatementErrorFailsWhole(t *testing.T) {
	_, err := Run(context.Background(), Request{
		SQL:     "SELECT 1; SELECT * FROM nonexistent_t; SELECT 2;",
		TempDir: t.TempDir(),
	})
	if err == nil {
		t.Fatal("Run should fail when a statement references a missing table")
	}
	if !strings.Contains(err.Error(), "nonexistent_t") {
		t.Fatalf("error should name nonexistent_t (native duckdb error), got: %v", err)
	}
	// A genuine SQL error must NOT be mis-mapped to a timeout.
	if errors.Is(err, ErrTimeout) {
		t.Fatalf("a catalog error must not map to ErrTimeout: %v", err)
	}
}

// TestNormalizeValue exercises the value converter end-to-end through Run on a
// row of varied DuckDB types, then asserts the Result is JSON-marshalable and
// the normalized forms are the faithful, decided shapes (time.Time → RFC3339
// string, HUGEINT → string, etc.).
func TestNormalizeValue(t *testing.T) {
	res, err := Run(context.Background(), Request{
		SQL: `SELECT
			42::BIGINT                                              AS bigint_col,
			'hello'                                                 AS varchar_col,
			3.5::DOUBLE                                             AS double_col,
			3.5::FLOAT                                              AS float_col,
			true                                                    AS bool_col,
			TIMESTAMP '2024-01-02 03:04:05'                         AS ts_col,
			DATE '2024-01-02'                                       AS date_col,
			170141183460469231731687303715884105727::HUGEINT        AS hugeint_col,
			123.45::DECIMAL(10,2)                                   AS decimal_col`,
		TempDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(res.Rows))
	}
	// Whatever the normalized forms are, the Result must be JSON-marshalable —
	// that is the load-bearing contract for the value converter.
	b, mErr := json.Marshal(res)
	if mErr != nil {
		t.Fatalf("json.Marshal(result) must succeed for all normalized types: %v", mErr)
	}
	t.Logf("normalized row JSON: %s", b)

	byName := map[string]any{}
	for i, c := range res.Schema {
		byName[c.Name] = res.Rows[0][i]
	}

	if got := byName["bigint_col"]; got != int64(42) {
		t.Errorf("bigint_col = %v (%T), want int64(42)", got, got)
	}
	if got := byName["varchar_col"]; got != "hello" {
		t.Errorf("varchar_col = %v (%T), want string \"hello\"", got, got)
	}
	if got := byName["double_col"]; got != float64(3.5) {
		t.Errorf("double_col = %v (%T), want float64(3.5)", got, got)
	}
	// FLOAT (single precision) scans as float32 and must be widened to float64 —
	// 3.5 is exactly representable, so the value round-trips cleanly (fix 2).
	if got := byName["float_col"]; got != float64(3.5) {
		t.Errorf("float_col = %v (%T), want float64(3.5)", got, got)
	}
	if got := byName["bool_col"]; got != true {
		t.Errorf("bool_col = %v (%T), want true", got, got)
	}
	// time.Time must be normalized to a string (json.Marshal would otherwise
	// produce a non-faithful default encoding we don't control; we pin RFC3339).
	tsStr, ok := byName["ts_col"].(string)
	if !ok || !strings.HasPrefix(tsStr, "2024-01-02T03:04:05") {
		t.Errorf("ts_col = %v (%T), want an RFC3339 string starting 2024-01-02T03:04:05", byName["ts_col"], byName["ts_col"])
	}
	dateStr, ok := byName["date_col"].(string)
	if !ok || !strings.HasPrefix(dateStr, "2024-01-02") {
		t.Errorf("date_col = %v (%T), want a string starting 2024-01-02", byName["date_col"], byName["date_col"])
	}
	// HUGEINT (max int128) overflows int64; it must be a string to be faithful.
	hugeStr, ok := byName["hugeint_col"].(string)
	if !ok || hugeStr != "170141183460469231731687303715884105727" {
		t.Errorf("hugeint_col = %v (%T), want the exact decimal string", byName["hugeint_col"], byName["hugeint_col"])
	}
	// DECIMAL: must be JSON-faithful (a string is acceptable; a mangled float is
	// not). Assert the marshaled JSON carries the exact value.
	if !strings.Contains(string(b), "123.45") {
		t.Errorf("decimal_col 123.45 not faithfully represented in JSON: %s", b)
	}
}

// TestRunRequiresNoAttachForPureCompute documents (and proves) the pure-compute
// contract: with no LakekeeperURL, Run skips attachCatalog entirely and still
// runs user SQL against the locked posture.
func TestRunRequiresNoAttachForPureCompute(t *testing.T) {
	res, err := Run(context.Background(), Request{SQL: "SELECT 1+1 AS two", TempDir: t.TempDir()})
	if err != nil {
		t.Fatalf("Run (pure compute, no LakekeeperURL): %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0] != int64(2) {
		t.Fatalf("result = %+v, want one row [2]", res)
	}
	if res.Stats.DurationMs < 0 {
		t.Fatalf("DurationMs must be >= 0, got %d", res.Stats.DurationMs)
	}
}
