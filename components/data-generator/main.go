// Package main is the data-generator component.
//
// It reads its pipeline-YAML config, generates random or literal data per
// table, writes to the data lake via the DataGateway SDK, and respects per-table
// limits (rows / bytes / time) and optional fault injection.
//
// Two modes per table (exactly one of `random` or `literal`):
//   - Random mode:  random data per configured column type; stops when any
//     limit is reached (OR semantics).
//   - Literal mode: explicit rows + matching column names; schema inferred
//     from the first non-null value per column.
//
// All tables run concurrently (one goroutine each).
package main

import (
	"context"
	"fmt"
	"sync"

	sdk "github.com/datuplet/datuplet/sdk/go"
)

func main() {
	ctx := context.Background()

	// Connect to gateway.
	client, err := sdk.New(ctx)
	if err != nil {
		sdk.ExitAppError(fmt.Sprintf("failed to connect to gateway: %v", err))
	}
	defer client.Close()

	cfg := client.Config()

	// Log SDK build info FIRST so cluster operators can verify at a
	// glance that the running data-generator binary has the SDK behavior
	// they expect (e.g., Write() batching from v0.2.4+). This is the
	// single most valuable line for "did the image get rebuilt against
	// the new SDK?" diagnostics. On by default — minimal overhead, big
	// debug payoff.
	client.Log(ctx, "INFO", sdk.BuildInfo().String()) //nolint:errcheck
	client.Log(ctx, "INFO", fmt.Sprintf("data-generator started: execution=%s", cfg.ExecutionID)) //nolint:errcheck

	// Parse component config.
	var compCfg Config
	if err := client.ParseConfig(&compCfg); err != nil {
		sdk.ExitUserError(fmt.Sprintf("failed to parse config: %v", err))
	}

	// Validate before doing any work.
	if err := ParseAndValidate(&compCfg); err != nil {
		sdk.ExitUserError(err.Error())
	}

	client.Log(ctx, "INFO", fmt.Sprintf("generating %d table(s)", len(compCfg.Tables))) //nolint:errcheck

	// Run all tables concurrently.
	type tableResult struct {
		name string
		rows int
		err  error
	}

	results := make([]tableResult, len(compCfg.Tables))
	var wg sync.WaitGroup
	errCh := make(chan error, len(compCfg.Tables))

	// Cancellable context so a failed goroutine can signal the rest.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i := range compCfg.Tables {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			t := &compCfg.Tables[idx]

			var rows int
			var err error

			if t.Literal != nil {
				rows, err = runLiteral(runCtx, client, t)
			} else {
				rows, err = runRandom(runCtx, client, &cfg, t)
			}

			results[idx] = tableResult{name: t.Name, rows: rows, err: err}
			if err != nil {
				cancel()        // Signal other goroutines to stop.
				errCh <- err    // Report error.
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	// Check for errors.
	for e := range errCh {
		sdk.ExitAppError(fmt.Sprintf("table write failed: %v", e))
	}

	// Commit all outputs.
	result, err := client.Commit(ctx)
	if err != nil {
		sdk.ExitAppError(fmt.Sprintf("commit failed: %v", err))
	}
	if !result.Success {
		sdk.ExitAppError(fmt.Sprintf("commit returned failure: %s", result.Error))
	}

	// Log per-table stats and build status message.
	totalRows := 0
	for _, r := range results {
		client.Log(ctx, "INFO", fmt.Sprintf("table %q: %d rows written", r.name, r.rows)) //nolint:errcheck
		totalRows += r.rows
	}

	for _, b := range result.Buckets {
		for _, t := range b.Tables {
			client.Log(ctx, "INFO", fmt.Sprintf("committed %s.%s: files=%d, rows=%d", t.Bucket, t.Table, t.FilesAdded, t.RowsAdded)) //nolint:errcheck
		}
	}

	sdk.StatusMessage(fmt.Sprintf("generated %d rows across %d table(s)", totalRows, len(compCfg.Tables)))
}
