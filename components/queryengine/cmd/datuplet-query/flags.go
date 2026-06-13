package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// defaultMemoryLimit is the conservative local default DuckDB memory_limit for
// BYO-local query. The laptop has no pod bounding it (RFC 022 §4), so we keep
// the default modest; the user can raise it via --memory-limit. DuckDB accepts
// human sizes like "2800MiB".
const defaultMemoryLimit = "2800MiB"

// defaultMaxRows caps the rows returned to the terminal by default. Local
// query has no multi-tenant cost concern, but an unbounded dump to a terminal
// is rarely useful; the user can raise it with --max-rows.
const defaultMaxRows = 10000

// defaultTimeout is the per-query deadline applied when --timeout is unset.
const defaultTimeout = 5 * time.Minute

// options holds the parsed datuplet-query flags.
type options struct {
	SQL         string
	File        string
	Format      string
	MemoryLimit string
	Timeout     time.Duration
	MaxRows     int
}

// parseFlags parses datuplet-query's argv (excluding the program name) into
// options, validating --format. SQL-source resolution (--sql vs -f vs stdin)
// is deferred to resolveSQL so it can read stdin lazily.
func parseFlags(argv []string) (options, error) {
	fs := flag.NewFlagSet("datuplet-query", flag.ContinueOnError)
	var o options
	fs.StringVar(&o.SQL, "sql", "", "Inline SQL to run (takes precedence over -f and stdin)")
	fs.StringVar(&o.File, "f", "", "Path to a .sql file (used when --sql is empty; else stdin)")
	fs.StringVar(&o.Format, "format", "table", "Output format: table | csv | json")
	fs.StringVar(&o.MemoryLimit, "memory-limit", defaultMemoryLimit, "DuckDB memory limit, e.g. 2800MiB")
	fs.DurationVar(&o.Timeout, "timeout", defaultTimeout, "Per-query timeout, e.g. 30s, 5m")
	fs.IntVar(&o.MaxRows, "max-rows", defaultMaxRows, "Maximum rows to return (0 = engine default)")
	if err := fs.Parse(argv); err != nil {
		return options{}, err
	}
	switch o.Format {
	case "table", "csv", "json":
	default:
		return options{}, fmt.Errorf("invalid --format %q (want table|csv|json)", o.Format)
	}
	return o, nil
}

// resolveSQL resolves the SQL from the three sources in strict precedence:
// --sql wins (when non-blank), else -f FILE, else stdin. A blank --sql falls
// through. Returns a clear error when no source yields any SQL.
func resolveSQL(o options, stdin io.Reader) (string, error) {
	if strings.TrimSpace(o.SQL) != "" {
		return o.SQL, nil
	}
	if o.File != "" {
		data, err := os.ReadFile(o.File)
		if err != nil {
			return "", fmt.Errorf("read SQL file %s: %w", o.File, err)
		}
		if strings.TrimSpace(string(data)) == "" {
			return "", fmt.Errorf("SQL file %s is empty", o.File)
		}
		return string(data), nil
	}
	data, err := io.ReadAll(stdin)
	if err != nil {
		return "", fmt.Errorf("read SQL from stdin: %w", err)
	}
	if strings.TrimSpace(string(data)) == "" {
		return "", fmt.Errorf("no SQL provided (use --sql, -f FILE, or pipe SQL on stdin)")
	}
	return string(data), nil
}
