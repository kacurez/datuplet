// Package queryengine error sentinels — no build tag so callers (e.g. the
// query-worker HTTP server) can import them without the duckdb_arrow tag.
// engine.go (tagged) declares the same names; the constants here carry the
// canonical values and engine.go references them via the package-level vars.
package queryengine

import "errors"

// ErrTimeout is returned (wrapped) when a Run is aborted by its deadline. It
// fires when the call fails AND either the context cause is
// context.DeadlineExceeded OR DuckDB's native interrupt error
// ("INTERRUPT Error") is present. The duckdb-go interrupt routine fires 0–500ms
// AFTER the deadline (one interruptInterval tick), so the timeout is detected
// from the error, never from a wall-clock comparison (Spike 0.3). Callers use
// errors.Is(err, ErrTimeout); the underlying DuckDB message is preserved.
var ErrTimeout = errors.New("query timed out")

// ErrResultTooLarge is returned when the result envelope alone (schema + stats +
// punctuation, zero rows) already exceeds Request.MaxBytes, so no row could ever
// fit. See buildResult.
var ErrResultTooLarge = errors.New("result exceeds byte cap")
