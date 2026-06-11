//go:build duckdb_arrow

package queryengine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

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

// errInterruptMarker is DuckDB's own C++ engine interrupt string, surfaced
// verbatim by duckdb-go when duckdb_interrupt() aborts a running query (Spike
// 0.3: "INTERRUPT Error: Interrupted!"). Matching on this — not a wall-clock
// check — is how a deadline-driven interrupt is attributed to a timeout, since
// the interrupt lands 0–500ms past the deadline.
const errInterruptMarker = "INTERRUPT Error"

// fsDisabledMarker is the LocalFileSystem-disabled error DuckDB raises when the
// locked posture (disabled_filesystems='LocalFileSystem') blocks a filesystem
// touch. Spill is disabled by the posture, so an over-memory query can surface
// EITHER a native out-of-memory error OR this FS-disabled error from a mid-flight
// spill attempt — we map the latter to a dual-cause hint (Spike 0.2 carry-forward).
const fsDisabledMarker = "File system LocalFileSystem has been disabled"

// watchdogHeadroom is how long past the context deadline the defense-in-depth
// watchdog waits before force-closing the connection. One duckdb-go
// interruptInterval (500ms) tick of headroom plus a small margin: the normal
// duckdb_interrupt path returns within ~500ms of the deadline, so the watchdog
// only ever fires if QueryContext is blocked in non-interruptible C code (e.g. a
// httpfs socket read, which ignores duckdb_interrupt — Spike 0.3 carry-forward).
const watchdogHeadroom = 600 * time.Millisecond

// Run is the public entrypoint: it opens a fresh, single-use embedded DuckDB
// engine, optionally attaches a lakekeeper iceberg-REST catalog, applies the
// lockdown posture, runs the user's (multi-statement) SQL, and returns the FINAL
// statement's result capped at MaxRows / MaxBytes.
//
// Lifecycle (open → [attach] → lock → run), all on one pinned connection:
//  1. r.Timeout (>0) becomes a context deadline for the whole call.
//  2. openEngine applies resource limits; e.Close() tears down synchronously.
//  3. If r.LakekeeperURL != "", attachCatalog wires the catalog (pre-lock). If
//     it is "", attach is SKIPPED — pure-compute mode. The BYO-local CLI and the
//     query-worker always set LakekeeperURL; only unit tests run pure-compute.
//  4. lock() applies the security posture; user SQL never runs against an
//     unlocked or half-locked engine (precondition: e.locked).
//  5. The user SQL runs via a SINGLE conn.QueryContext — duckdb-go executes
//     statements 0..n-2 and returns the final statement's *sql.Rows (Spike 0.4).
//
// USER SQL VIA QueryContext ONLY: duckdb-go's ExecContext runs its multi-
// statement prepare phase OUTSIDE the ctx-interrupt scope (connection.go:59),
// while QueryContext wraps the whole prepare+execute INSIDE it (connection.go:86).
// Using ExecContext would leave the all-but-last statements un-interruptible on a
// deadline. So Run always uses QueryContext, even though the final statement may
// be non-result-producing (DuckDB returns its status result set, handled as-is).
func Run(ctx context.Context, r Request) (*Result, error) {
	// Apply defaults up front so the timeout floor (defaultTimeout) is in effect
	// before we derive the deadline below: Run NEVER runs unbounded, the watchdog
	// is always armed. openEngine re-applies defaults idempotently.
	applyDefaults(&r)

	// 1. Apply the per-call timeout as a context deadline. r.Timeout is now always
	//    > 0 (floored by applyDefaults), so Run always has a deadline.
	{
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.Timeout)
		defer cancel()
	}

	// 2. Open + pin a single connection; teardown is synchronous.
	e, err := openEngine(ctx, r)
	if err != nil {
		return nil, err
	}
	defer e.Close()

	// 3. Attach the catalog only when a lakekeeper URL is supplied (pre-lock).
	//    Empty URL = pure-compute mode (unit tests); production callers set it.
	if r.LakekeeperURL != "" {
		if err := attachCatalog(ctx, e, r); err != nil {
			return nil, err
		}
	}

	// 4. Apply the lockdown posture. User SQL MUST run only against a fully
	//    locked engine.
	if err := e.lock(ctx); err != nil {
		return nil, err
	}
	if !e.locked {
		// Defensive: lock() returns nil only after setting locked=true, but never
		// run untrusted SQL against an unlocked posture.
		return nil, errors.New("engine not locked after lock(); refusing to run user SQL")
	}

	// 5. Run the user SQL and stream the final statement's rows into a Result.
	res, runErr := e.runUserSQL(ctx, r)
	if runErr != nil {
		return nil, mapRunError(ctx, runErr)
	}
	return res, nil
}

// runUserSQL executes the user's multi-statement SQL via the single pinned
// connection and builds the capped Result. It arms a defense-in-depth watchdog
// (only when a deadline exists) that force-closes the connection if
// QueryContext/row-streaming has not finished by deadline+headroom — guarding
// against an extension blocking in non-interruptible C code (httpfs socket reads
// ignore duckdb_interrupt). The watchdog is disarmed (via sync.Once) on normal
// completion so it never closes a connection a later query depends on.
func (e *engine) runUserSQL(ctx context.Context, r Request) (*Result, error) {
	start := time.Now()

	// Arm the watchdog only when there is a deadline to arm it against.
	var disarm func()
	if deadline, ok := ctx.Deadline(); ok {
		stop := make(chan struct{})
		var once sync.Once
		disarm = func() { once.Do(func() { close(stop) }) }
		go func() {
			// Fire one interrupt-tick of headroom past the deadline. If the query
			// finished (disarm closed stop) we never touch the connection.
			timer := time.NewTimer(time.Until(deadline) + watchdogHeadroom)
			defer timer.Stop()
			select {
			case <-stop:
				return
			case <-timer.C:
				// QueryContext is still blocked past deadline+headroom — the normal
				// duckdb_interrupt path did not unblock it (non-interruptible C
				// code). Force-close the pinned connection to tear the query down.
				_ = e.conn.Close()
			}
		}()
		defer disarm()
	}

	// USER SQL VIA QueryContext ONLY (see Run doc): the whole multi-statement
	// prepare+execute runs inside duckdb-go's ctx-interrupt scope, so a deadline
	// interrupts every statement, not just the last.
	rows, err := e.conn.QueryContext(ctx, r.SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res, err := buildResult(rows, r.MaxRows, r.MaxBytes)
	if err != nil {
		return nil, err
	}

	// Benign post-completion race: the watchdog timer may fire in the narrow
	// window between buildResult returning here and the deferred disarm running.
	// The result is already fully built, so a late force-close cannot corrupt it;
	// the double-close only surfaces as sql.ErrConnDone from the deferred
	// rows.Close()/e.Close(), which is harmless to the caller.
	res.Stats.DurationMs = time.Since(start).Milliseconds()
	return res, nil
}

// mapRunError maps a raw user-SQL execution error onto the typed sentinels,
// preserving the underlying DuckDB message so errors.Is works AND the original
// text remains visible:
//
//   - Timeout: the context cause is context.DeadlineExceeded OR the error string
//     carries DuckDB's "INTERRUPT Error" (the interrupt fires 0–500ms after the
//     deadline — never a wall-clock check). Wrapped as ErrTimeout.
//   - Local-FS deny: the posture disables LocalFileSystem, so the primary cause
//     of this error is a plain policy denial. Because spill-to-disk is disabled,
//     an over-memory query is a secondary way to surface the SAME error. When the
//     LocalFileSystem-disabled error appears, wrap with a hint that names the
//     policy denial as the primary cause and the over-memory case as a note.
//   - Otherwise the error passes through unchanged (native DuckDB Catalog/Binder
//     error with line info).
func mapRunError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()

	// Timeout: deadline cause OR DuckDB's interrupt marker. ctx.Err()/cause may be
	// context.DeadlineExceeded even when the error string is the bare interrupt.
	if errors.Is(ctx.Err(), context.DeadlineExceeded) ||
		errors.Is(context.Cause(ctx), context.DeadlineExceeded) ||
		strings.Contains(msg, errInterruptMarker) {
		return fmt.Errorf("%w: %v", ErrTimeout, err)
	}

	// Local-FS deny (the posture disables LocalFileSystem). The PRIMARY cause is a
	// plain policy denial; an over-memory query is a SECONDARY way to surface the
	// same error because spill-to-disk is disabled. Word the hint so memory is not
	// presented as a co-equal cause. One wrapped error preserves the native error.
	if strings.Contains(msg, fsDisabledMarker) {
		return fmt.Errorf("local file access is denied by policy (disabled_filesystems). "+
			"Note: an over-memory query can surface this same error because spill-to-disk "+
			"is disabled — if you weren't reading local files, reduce the query's working set: %w", err)
	}

	return err
}
