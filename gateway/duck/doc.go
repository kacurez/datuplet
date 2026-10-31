//go:build duckdb_arrow

// Package duck is the gateway's thin layer over duckdb-go-bindings: an
// in-memory DuckDB instance (DB), connections (Conn), a streaming query result
// exposed as Arrow record batches (Stream), Arrow record readers registered as
// views (RegisterArrowView), and the two SQL quoting helpers (Ident, Literal).
//
// It uses the DuckDB C API directly instead of database/sql so that query
// results can be streamed (duckdb-go materializes every result) and so that
// cancellation maps onto duckdb_interrupt.
//
// Concurrency: a Conn is used by one goroutine at a time; only Conn.Interrupt
// may be called concurrently with a running statement. Every statement goes
// through Prepare, so each call runs exactly one SQL statement.
//
// The package is CGO-only and builds only with -tags duckdb_arrow.
package duck
