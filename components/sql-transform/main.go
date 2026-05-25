//go:build duckdb_arrow

// Package main is the sql-transform component.
//
// The component:
//   1. Opens an arrow IPC stream for each declared input via the DataGateway SDK
//      (FORMAT_ARROW_IPC). DG row-group-streams parquet → arrow internally.
//   2. Stream-writes each input's Arrow batches to a temp parquet file on local
//      ephemeral disk (bounded RAM — only one batch alive at a time) and exposes
//      it to user SQL as a DuckDB view via `read_parquet(...)`. The disk-stage
//      replaces an earlier in-memory `CREATE TABLE AS SELECT * FROM <arrow_view>`
//      that OOM'd on multi-GB inputs and required a multi-scan workaround for
//      duckdb/duckdb#19040.
//   3. Executes the user-supplied SQL inside DuckDB.
//   4. Streams each declared output back through the SDK's OpenWriter +
//      WriteChunk path so DG owns parquet emission to the iceberg target
//      prefix. The post-stage iceberg-job table-commit Job picks up the
//      per-target files.json and runs txn.AddFiles / txn.ReplaceDataFiles.
//
// The component never touches S3 — DG mediates both sides (read + write).
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	sdk "github.com/datuplet/datuplet/sdk/go"
)

// userErr wraps an error so the top-level main() exits 1 (FailedUser).
type userErr struct{ err error }

func (u *userErr) Error() string { return u.err.Error() }
func (u *userErr) Unwrap() error { return u.err }

// asUserErr classifies an error as user-caused (bad SQL, missing column,
// invalid config). main() exits 1.
func asUserErr(err error) error {
	if err == nil {
		return nil
	}
	return &userErr{err: err}
}

func main() {
	ctx := context.Background()
	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		var ue *userErr
		if errors.As(err, &ue) {
			sdk.ExitUserError(err.Error())
		}
		sdk.ExitAppError(err.Error())
	}
}

func run(ctx context.Context) error {
	// Start Pyroscope continuous profiling when DATUPLET_COMPONENT_PROFILING
	// is set (the operator injects this together with the gateway's
	// profiling env when GatewayProfilingEnabled is on). Off otherwise.
	if stopProfiling := startProfilingIfEnabled(); stopProfiling != nil {
		defer func() {
			if err := stopProfiling(); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: pyroscope stop error: %v\n", err)
			}
		}()
	}

	client, err := sdk.New(ctx)
	if err != nil {
		return fmt.Errorf("connect DataGateway: %w", err)
	}
	defer client.Close()

	cfg := client.Config()
	client.Log(ctx, "INFO", fmt.Sprintf("sql-transform started: execution=%s inputs=%d outputs=%d", //nolint:errcheck
		cfg.ExecutionID, len(cfg.InputTables), len(cfg.OutputTables)))

	compCfg, err := parseConfig(cfg.Raw)
	if err != nil {
		return asUserErr(fmt.Errorf("parse config: %w", err))
	}

	if len(cfg.InputTables) == 0 {
		return asUserErr(fmt.Errorf("at least one input table must be declared"))
	}
	if len(cfg.OutputTables) == 0 {
		return asUserErr(fmt.Errorf("at least one output table must be declared"))
	}

	// Workspace for staged input + output parquet (both registerInput and
	// writeOutput stage files locally before/after running user SQL).
	workdir, err := os.MkdirTemp("", "sql-transform-")
	if err != nil {
		return fmt.Errorf("mkdir workdir: %w", err)
	}
	defer os.RemoveAll(workdir)

	// Open DuckDB and pin a single *sql.Conn. All subsequent SQL must run
	// through `conn` (NOT `db.ExecContext`), otherwise it would deadlock
	// waiting on the held conn under the MaxOpenConns=1 contract. The
	// pinning also keeps CREATE VIEW / SELECT visibility consistent
	// across statements under DuckDB's shared in-memory DB model.
	db, err := openDuckDB(ctx, compCfg)
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close() //nolint:errcheck
		return fmt.Errorf("db.Conn: %w", err)
	}

	releases := make([]func(), 0, len(cfg.InputTables))
	// Single ordered cleanup. Deferred order matters — release inputs
	// FIRST (drops views + unlinks staged files), THEN return the
	// *sql.Conn to the pool, THEN close the pool. This is the inverse
	// of the LIFO stack you'd get from three separate defers, which is
	// why we collapse into one closure here.
	defer func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
		conn.Close() //nolint:errcheck
		db.Close()   //nolint:errcheck
	}()

	for _, in := range cfg.InputTables {
		rel, err := registerInput(ctx, conn, client, workdir, in)
		if err != nil {
			return fmt.Errorf("register input %s.%s: %w", in.Bucket, in.Table, err)
		}
		releases = append(releases, rel)
	}

	// Run the user's SQL on the held conn. DuckDB resolves `FROM <logical>`
	// against the read_parquet-backed views registered above, lazy-streaming
	// row groups from disk on each scan. CREATE TABLE <out> AS …
	// materializes the result locally inside the DuckDB instance.
	client.Log(ctx, "INFO", fmt.Sprintf("executing SQL (%d bytes)", len(compCfg.SQL))) //nolint:errcheck
	if _, err := conn.ExecContext(ctx, compCfg.SQL); err != nil {
		return asUserErr(fmt.Errorf("SQL execution: %w", err))
	}

	// Stream each declared output back through DG.
	for _, out := range cfg.OutputTables {
		rows, err := writeOutput(ctx, conn, client, workdir, out)
		if err != nil {
			return fmt.Errorf("write output %s.%s: %w", out.Bucket, out.Table, err)
		}
		client.Log(ctx, "INFO", fmt.Sprintf("output %s.%s wrote %d rows", out.Bucket, out.Table, rows)) //nolint:errcheck
	}

	// Commit (DG flushes per-target files.json; iceberg-job picks it up post-stage).
	if _, err := client.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	sdk.StatusMessage(fmt.Sprintf("sql-transform completed (%d inputs, %d outputs)",
		len(cfg.InputTables), len(cfg.OutputTables)))
	return nil
}
