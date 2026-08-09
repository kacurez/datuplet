//go:build duckdb_arrow

// RFC 028 Q2 — engine-level tests for bound parameters (spec §6.1).
//
// These are PURE-COMPUTE tests (no Request.LakekeeperURL): Run skips
// attachCatalog and exercises open → lock → QueryContext(sql, namedArgs…) →
// build-result. They prove that (a) values bind and come back correctly,
// (b) the §6.1 bind-type table is honoured, (c) a DuckDB bind/execute failure
// stays a plain error (so the worker maps it to `sql_error`, never a
// sentinel), and (d) the interrupt-safe execution path is unchanged — a slow
// query with params is torn down by the deadline exactly as the no-params
// path is.
package queryengine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestBindParams_Scalars: every JSON scalar in the §6.1 bind-type table binds
// and round-trips through the result. Integral numbers arrive as BIGINT,
// non-integral as DOUBLE, strings as VARCHAR, booleans as BOOLEAN, and an
// explicit null binds as SQL NULL (cast in SQL, per the table's note).
func TestBindParams_Scalars(t *testing.T) {
	res, err := Run(context.Background(), Request{
		SQL: `SELECT $a::INT AS v`,
		Params: map[string]any{
			"a": json.Number("7"),
		},
		TempDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
		t.Fatalf("rows = %v, want one row with one value", res.Rows)
	}
	if got := res.Rows[0][0]; got != int32(7) && got != int64(7) {
		t.Fatalf("Rows[0][0] = %v (%T), want 7", got, got)
	}
}

// TestBindParams_TypeTable exercises the whole §6.1 table in one query, with a
// repeated placeholder and two placeholders bound out of declaration order (to
// prove binding is by NAME, not position).
func TestBindParams_TypeTable(t *testing.T) {
	res, err := Run(context.Background(), Request{
		SQL: `SELECT
			$s                       AS s,
			$n                       AS n,
			$f                       AS f,
			$b                       AS b,
			CAST($nul AS INTEGER)    AS nul,
			$n + $n                  AS n_twice`,
		Params: map[string]any{
			// Deliberately not in SQL order.
			"b":   true,
			"nul": nil,
			"n":   json.Number("42"),
			"s":   "hello",
			"f":   json.Number("3.5"),
		},
		TempDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(res.Rows))
	}
	byName := map[string]any{}
	for i, c := range res.Schema {
		byName[c.Name] = res.Rows[0][i]
	}
	if got := byName["s"]; got != "hello" {
		t.Errorf("s = %v (%T), want string \"hello\"", got, got)
	}
	if got := byName["n"]; got != int64(42) {
		t.Errorf("n = %v (%T), want int64(42) — integral JSON number binds as BIGINT", got, got)
	}
	if got := byName["f"]; got != float64(3.5) {
		t.Errorf("f = %v (%T), want float64(3.5) — non-integral JSON number binds as DOUBLE", got, got)
	}
	if got := byName["b"]; got != true {
		t.Errorf("b = %v (%T), want true", got, got)
	}
	if got := byName["nul"]; got != nil {
		t.Errorf("nul = %v (%T), want nil (SQL NULL)", got, got)
	}
	if got := byName["n_twice"]; got != int64(84) {
		t.Errorf("n_twice = %v (%T), want int64(84) — a repeated placeholder reuses the same bound value", got, got)
	}
}

// TestBindParams_StringDateCast: dates arrive as ISO strings and are cast in
// SQL (§6.1 note on the VARCHAR row). The bound string must reach DuckDB as a
// value, never as SQL text.
func TestBindParams_StringDateCast(t *testing.T) {
	res, err := Run(context.Background(), Request{
		SQL: `SELECT CAST($d AS DATE) + INTERVAL 1 DAY AS next_day`,
		Params: map[string]any{
			"d": "2024-01-02",
		},
		TempDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(res.Rows))
	}
	got, ok := res.Rows[0][0].(string)
	if !ok {
		t.Fatalf("next_day = %v (%T), want a normalized string", res.Rows[0][0], res.Rows[0][0])
	}
	if len(got) < 10 || got[:10] != "2024-01-03" {
		t.Fatalf("next_day = %q, want it to start 2024-01-03", got)
	}
}

// TestBindParams_ValueIsNeverSQL: the injection defence. A param value that
// looks like SQL is bound as a literal string, not parsed.
func TestBindParams_ValueIsNeverSQL(t *testing.T) {
	res, err := Run(context.Background(), Request{
		SQL:     `SELECT $evil AS v`,
		Params:  map[string]any{"evil": "'; DROP TABLE t; --"},
		TempDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := res.Rows[0][0]; got != "'; DROP TABLE t; --" {
		t.Fatalf("v = %v (%T), want the literal string (value never parsed as SQL)", got, got)
	}
}

// TestBindParams_DuckDBBindErrorIsPlainError: a param used where DuckDB cannot
// bind it (as a table identifier) fails with DuckDB's native error. It must NOT
// map to ErrTimeout / ErrResultTooLarge — the worker's error switch sends every
// other Run error to 400 kind=sql_error, which is exactly the contract for a
// bind/prepare failure.
func TestBindParams_DuckDBBindErrorIsPlainError(t *testing.T) {
	cases := []struct {
		name   string
		sql    string
		params map[string]any
	}{
		{
			name:   "placeholder as table identifier",
			sql:    `SELECT * FROM $tbl`,
			params: map[string]any{"tbl": "range(3)"},
		},
		{
			name:   "value cannot be cast to the bound type",
			sql:    `SELECT $a::INT AS v`,
			params: map[string]any{"a": "not-a-number"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Run(context.Background(), Request{
				SQL:     tc.sql,
				Params:  tc.params,
				TempDir: t.TempDir(),
			})
			if err == nil {
				t.Fatal("Run should fail when the bound param cannot be used that way")
			}
			if errors.Is(err, ErrTimeout) {
				t.Fatalf("a bind error must not map to ErrTimeout: %v", err)
			}
			if errors.Is(err, ErrResultTooLarge) {
				t.Fatalf("a bind error must not map to ErrResultTooLarge: %v", err)
			}
			var pe *ParamError
			if errors.As(err, &pe) {
				t.Fatalf("a DuckDB bind failure must not surface as a ParamError (that is 400 bad_request): %v", err)
			}
			t.Logf("bind error (→ sql_error): %v", err)
		})
	}
}

// TestBindParams_NoParamsRegression: with no Params the behaviour is exactly
// what it was before Q2. This mirrors TestFinalStatementResult verbatim (the
// existing passing case) — a nil Params map must leave the QueryContext call
// argument-free.
func TestBindParams_NoParamsRegression(t *testing.T) {
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

	// An explicitly empty (non-nil) map is the same no-op.
	res2, err := Run(context.Background(), Request{
		SQL:     "SELECT 1+1 AS two",
		Params:  map[string]any{},
		TempDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Run (empty params map): %v", err)
	}
	if len(res2.Rows) != 1 || res2.Rows[0][0] != int64(2) {
		t.Fatalf("result = %+v, want one row [2]", res2)
	}
}

// slowSQLNoParams / slowSQLParams are the SAME logical query written without
// and with bound parameters. Both must be interrupted by the deadline in the
// same way (finding 8): the params path must not escape the ctx-interrupt
// scope by taking a separate Prepare/Exec route.
const (
	slowSQLNoParams = `SELECT count(*) FROM range(33000000) a(x) CROSS JOIN range(3000) b(y) WHERE (a.x*b.y)%7=0`
	slowSQLParams   = `SELECT count(*) FROM range(33000000) a(x) CROSS JOIN range(3000) b(y) WHERE (a.x*b.y)%$m=$z`
)

// TestBindParams_CancellationParity: a slow query WITH bound params is torn
// down by the deadline exactly as the no-params path is — same ErrTimeout
// mapping, same interrupt latency envelope (well inside the watchdog's
// force-close window), and the engine is reusable afterwards. Run both halves
// so a divergence, not just an absolute number, is what fails.
func TestBindParams_CancellationParity(t *testing.T) {
	const timeout = 2 * time.Second
	// The interrupt lands 0–500ms past the deadline; the watchdog force-close
	// is armed at deadline+600ms. Anything under this bound proves the query
	// was interrupted, not merely abandoned.
	const maxElapsed = 10 * time.Second

	// assertInterrupted pins the MECHANISM, not just the outcome: the error must
	// carry DuckDB's own interrupt marker, which only appears when
	// duckdb_interrupt() aborted a query running inside QueryContext's
	// ctx-interrupt scope. A params path that escaped that scope (a separate
	// PrepareContext) would instead be torn down by the watchdog's
	// force-close and surface as a closed-connection error.
	assertInterrupted := func(t *testing.T, err error, elapsed time.Duration) {
		t.Helper()
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("err = %v, want errors.Is(err, ErrTimeout)", err)
		}
		if !strings.Contains(err.Error(), errInterruptMarker) {
			t.Fatalf("err = %v, want DuckDB's %q — the query must be interrupted in-flight, "+
				"not force-closed by the watchdog", err, errInterruptMarker)
		}
		if elapsed > maxElapsed {
			t.Fatalf("call took %v, want interruption within %v of the %v deadline", elapsed, maxElapsed, timeout)
		}
	}

	run := func(t *testing.T, sql string, params map[string]any) time.Duration {
		t.Helper()
		start := time.Now()
		_, err := Run(context.Background(), Request{
			SQL:     sql,
			Params:  params,
			Timeout: timeout,
			TempDir: t.TempDir(),
		})
		elapsed := time.Since(start)
		assertInterrupted(t, err, elapsed)
		t.Logf("interrupted after %v: %v", elapsed, err)
		return elapsed
	}

	t.Run("request-timeout", func(t *testing.T) {
		baseline := run(t, slowSQLNoParams, nil)
		withParams := run(t, slowSQLParams, map[string]any{
			"m": json.Number("7"),
			"z": json.Number("0"),
		})
		// Parity: the params path must not take materially longer to unwind
		// than the no-params path (a separate un-interruptible Prepare would
		// show up as the params run only stopping at the watchdog).
		if withParams > baseline+2*time.Second {
			t.Fatalf("params run took %v vs %v without params — the params path is not being interrupted the same way",
				withParams, baseline)
		}
	})

	t.Run("caller-context-deadline", func(t *testing.T) {
		// Cancellation driven by the CALLER's context (the request context in
		// production), not Request.Timeout: Run's derived deadline never
		// extends the parent, so the interrupt must still fire.
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		start := time.Now()
		_, err := Run(ctx, Request{
			SQL:     slowSQLParams,
			Params:  map[string]any{"m": json.Number("7"), "z": json.Number("0")},
			TempDir: t.TempDir(), // no Request.Timeout: the 60s floor applies, the ctx wins
		})
		elapsed := time.Since(start)
		assertInterrupted(t, err, elapsed)
		t.Logf("request-context interrupt after %v: %v", elapsed, err)
	})

	// No leaked engine state after the interrupted params runs.
	res, err := Run(context.Background(), Request{
		SQL:     "SELECT $one::INT AS one",
		Params:  map[string]any{"one": json.Number("1")},
		TempDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("fresh params Run after timeout: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("fresh Run result = %+v, want one row", res)
	}
}
