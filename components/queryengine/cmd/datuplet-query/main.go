//go:build duckdb_arrow

// datuplet-query is the BYO-local (mode c) ad-hoc SQL tool (RFC 022 §4): it
// runs an embedded DuckDB engine on the user's own machine against the remote
// warehouse, reusing the queryengine.Run core (the same lockdown + resource
// posture the query-worker uses). It is a SEPARATE binary from the duckdb-free
// root `datuplet` CLI and must be built with -tags duckdb_arrow (CGO).
//
// Flow (RFC 022 §4.1 / §5.3):
//  1. Parse flags + resolve SQL (--sql | -f FILE | stdin).
//  2. Load ~/.datuplet/{cluster.json, api-token}.
//  3. POST /api/v1/query/token (api-token bearer) → fresh short-lived query
//     JWT, or surface the policy-off 403 ("client-side query disabled") clearly.
//  4. queryengine.Run with the project-qualified warehouse + minted JWT.
//  5. Render schema+rows as table | csv | json.
//
// SECURITY: the api-token, the minted query JWT, and the raw SQL are NEVER
// logged. The query JWT lives only in memory. The DuckDB temp dir is created
// 0700 and removed on exit.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/datuplet/datuplet/components/queryengine"

	// Blank-import the centralised iceberg-go IO scheme registration so this
	// binary's gs:// factory is the Datuplet override — load-bearing at process
	// init (RFC 019 §4.5). The query-worker and root datuplet do the same.
	_ "github.com/datuplet/datuplet/pkg/datupleticeio"
)

func main() {
	os.Exit(realMain(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

// realMain runs the full flow and returns a process exit code per the repo
// contract (0 / 1 / >=20). It is split from main() so the IO + argv are
// injectable (a duckdb-tagged smoke test can drive it without os.Exit).
func realMain(argv []string, stdin *os.File, stdout, stderr *os.File) int {
	opts, err := parseFlags(argv)
	if err != nil {
		// flag.ContinueOnError already printed usage to stderr for -h/parse
		// errors; add nothing that could echo secrets.
		return exitUserFailure
	}

	sql, err := resolveSQL(opts, stdin)
	if err != nil {
		fmt.Fprintf(stderr, "datuplet-query: %v\n", err)
		return exitUserFailure
	}

	dir, err := defaultDatupletDir()
	if err != nil {
		fmt.Fprintf(stderr, "datuplet-query: %v\n", err)
		return exitAppFailure
	}
	cfg, err := loadConfig(dir)
	if err != nil {
		fmt.Fprintf(stderr, "datuplet-query: %v\n", err)
		return exitAppFailure
	}

	// Mint a fresh per-invocation query JWT (RFC 022 §5.3 option i). Bound the
	// mint by --timeout (capped at 30s) so it honors the user's deadline and an
	// unresponsive pipeline-api cannot hang the CLI. A policy-off 403 surfaces a
	// clean refusal here; treat it as an application-level refusal.
	mintTimeout := opts.Timeout
	if mintTimeout <= 0 || mintTimeout > 30*time.Second {
		mintTimeout = 30 * time.Second
	}
	mintCtx, cancelMint := context.WithTimeout(context.Background(), mintTimeout)
	token, err := mintQueryToken(mintCtx, cfg.PipelineAPIURL, cfg.APIToken)
	cancelMint()
	if err != nil {
		fmt.Fprintf(stderr, "datuplet-query: %v\n", err)
		return exitAppFailure
	}

	// Per-invocation temp dir for DuckDB spill, 0700, cleaned up on exit.
	tempDir, err := os.MkdirTemp("", "datuplet-query-*")
	if err != nil {
		fmt.Fprintf(stderr, "datuplet-query: create temp dir: %v\n", err)
		return exitAppFailure
	}
	defer os.RemoveAll(tempDir)

	req := queryengine.Request{
		SQL:           sql,
		LakekeeperURL: cfg.LakekeeperURL,
		Warehouse:     cfg.QualifiedWarehouse(),
		CatalogJWT:    token,
		Timeout:       opts.Timeout,
		MaxRows:       opts.MaxRows,
		MemoryLimit:   opts.MemoryLimit,
		TempDir:       tempDir,
	}

	// queryengine.Run derives its own deadline from req.Timeout (opts.Timeout),
	// so a background ctx is correct here — the engine bounds itself.
	res, runErr := queryengine.Run(context.Background(), req)
	if runErr != nil {
		// SECURITY: queryengine errors preserve the native DuckDB message but
		// never the SQL text or token; safe to print to stderr.
		fmt.Fprintf(stderr, "datuplet-query: %v\n", runErr)
		return exitCodeFor(runErr)
	}

	if err := render(stdout, res, opts.Format); err != nil {
		fmt.Fprintf(stderr, "datuplet-query: render: %v\n", err)
		return exitAppFailure
	}

	// Truncation note goes to stderr so it never corrupts the stdout data.
	if res.Truncated {
		fmt.Fprintln(stderr, "note: result truncated by the row/byte cap (--max-rows)")
	}
	return exitSuccess
}
