//go:build duckdb_arrow

// Package main is the sql-transform component.
//
// The component:
//   1. Opens an arrow IPC stream for each declared input via the DataGateway SDK
//      (FORMAT_ARROW_IPC). DG row-group-streams parquet → arrow internally.
//   2. Registers each stream as a DuckDB view via Arrow.RegisterView (arrow_scan).
//      No parquet files are written to the component's local disk.
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

	// Workspace for staged output parquet (writeOutput stages COPY TO
	// targets locally before streaming chunks to DG). Inputs no longer
	// touch disk — they stream over Arrow IPC directly into DuckDB via
	// arrow_scan.
	workdir, err := os.MkdirTemp("", "sql-transform-")
	if err != nil {
		return fmt.Errorf("mkdir workdir: %w", err)
	}
	defer os.RemoveAll(workdir)

	// Open DuckDB and pin a single *sql.Conn via the Arrow handle. All
	// subsequent SQL must run through `conn` (NOT `db.ExecContext`),
	// otherwise it would deadlock waiting on the held conn under the
	// MaxOpenConns=1 contract.
	db, err := openDuckDB(ctx, compCfg)
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}

	arrowConn, conn, err := arrowFromDB(db)
	if err != nil {
		db.Close() //nolint:errcheck
		return fmt.Errorf("arrow handle: %w", err)
	}

	releases := make([]func(), 0, len(cfg.InputTables))
	// Single ordered cleanup. Deferred order matters — release scans
	// FIRST (so DuckDB drops its view refs / drains the C-level Arrow
	// streams), THEN return the *sql.Conn to the pool, THEN close the
	// pool. This is the inverse of the LIFO stack you'd get from three
	// separate defers, which is why we collapse into one closure here.
	defer func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
		conn.Close() //nolint:errcheck
		db.Close()   //nolint:errcheck
	}()

	for _, in := range cfg.InputTables {
		rel, err := registerInput(ctx, conn, arrowConn, client, in)
		if err != nil {
			return fmt.Errorf("register input %s.%s: %w", in.Bucket, in.Table, err)
		}
		releases = append(releases, rel)
	}

	// Run the user's SQL on the held conn. DuckDB resolves `FROM <logical>`
	// against the CTAS-materialized tables registered above (the streams are
	// drained via `CREATE TABLE … AS SELECT * FROM <hidden_stream>` to avoid
	// duckdb/duckdb#19040, where multi-scan plans against single-pass
	// arrow_scan views produce wrong results). CREATE TABLE <out> AS …
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
