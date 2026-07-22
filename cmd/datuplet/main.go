// Package main provides the datuplet CLI.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	// Blank-import the centralised iceberg-go IO scheme registration
	// package so this binary's `gs://` factory is the Datuplet override
	// regardless of which transitive package registers first. See
	// pkg/datupleticeio/doc.go and RFC 019 §4.5.
	_ "github.com/datuplet/datuplet/pkg/datupleticeio"
)

func main() {
	loginCmd := flag.NewFlagSet("login", flag.ExitOnError)
	loginRemote := loginCmd.String("remote", "", "pipeline-api URL (required)")
	loginEmail := loginCmd.String("email", "", "Email address (required with --password-stdin)")
	loginPasswordStdin := loginCmd.Bool("password-stdin", false, "Read the password from stdin (one line, no prompt); requires --email")

	triggerCmd := flag.NewFlagSet("trigger", flag.ExitOnError)
	triggerRemote := triggerCmd.String("remote", "", "pipeline-api URL (required)")
	triggerTokenFile := triggerCmd.String("token-file", "", "Path to JWT token file (default: ~/.datuplet/token)")
	triggerProject := triggerCmd.String("project", "", "Project name (auto-defaulted if you have exactly one)")
	triggerWait := triggerCmd.Bool("wait", false, "Block until the run reaches a terminal phase")
	triggerTimeout := triggerCmd.Duration("timeout", time.Hour, "Hard cap on --wait; cancels the run cluster-side on expiry")
	triggerJSON := triggerCmd.Bool("json", false, "Structured JSON output")

	storageCmd := flag.NewFlagSet("storage", flag.ExitOnError)
	storageRemote := storageCmd.String("remote", "", "pipeline-api URL (required)")
	storageTokenFile := storageCmd.String("token-file", "", "Path to JWT token file (default: ~/.datuplet/token)")
	storageProject := storageCmd.String("project", "", "Project name (auto-defaulted if you have exactly one)")
	storageRows := storageCmd.Int("rows", 0, "Max preview rows (sample subcommand only; 0 = server default)")

	queryCmd := flag.NewFlagSet("query", flag.ExitOnError)
	queryRemote := queryCmd.String("remote", "", "pipeline-api URL (required)")
	queryTokenFile := queryCmd.String("token-file", "", "Path to JWT token file (default: ~/.datuplet/token)")
	queryProject := queryCmd.String("project", "", "Project name (auto-defaulted if you have exactly one)")
	querySQL := queryCmd.String("sql", "", "Inline SQL to run (takes precedence over -f and stdin)")
	queryFile := queryCmd.String("f", "", "Path to a .sql file to run (used when --sql is empty)")
	queryFormat := queryCmd.String("format", "table", "Output format: table | csv | json")
	queryLocal := queryCmd.Bool("local", false, "Force local execution (requires the separate duckdb-enabled datuplet-query binary)")

	gatewayCmd := flag.NewFlagSet("gateway", flag.ExitOnError)
	gatewayLocal := gatewayCmd.Bool("local", false, "Run in local mode (filesystem backend)")
	gatewayMinio := gatewayCmd.Bool("minio", false, "Run in MinIO mode (S3-compatible backend)")
	gatewayConfig := gatewayCmd.String("config", "", "Path to config file (YAML)")
	gatewayDataDir := gatewayCmd.String("data-dir", "./data", "Data directory for local mode")
	gatewayAddr := gatewayCmd.String("addr", ":50051", "gRPC server address")
	gatewayRunTokenPath := gatewayCmd.String("run-token-path", "", "Path to the mounted run-token file (K8s typically sets /var/run/secrets/datuplet-runtoken/tokens). When set, the gateway holds the per-table JSON map of JWTs the gateway forwards to lakekeeper for catalog + STS calls. Also RUN_TOKEN_PATH env var.")
	gatewayPodAnnotationsPath := gatewayCmd.String("pod-annotations-path", "", "Path to the kubelet downward-API pod-annotations file (K8s typically sets /etc/podinfo/annotations). When set, the gateway polls the file every 5s and exits cleanly on `datuplet.io/cancel=true`. Also POD_ANNOTATIONS_PATH env var.")

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "gateway":
		gatewayCmd.Parse(os.Args[2:])
		if !*gatewayLocal && !*gatewayMinio {
			fmt.Println("Error: must specify --local or --minio mode")
			os.Exit(1)
		}
		mode := "local"
		if *gatewayMinio {
			mode = "minio"
		}
		runTokenPath := resolveRunTokenPath(*gatewayRunTokenPath, os.Getenv("RUN_TOKEN_PATH"))
		podAnnotationsPath := resolveRunTokenPath(*gatewayPodAnnotationsPath, os.Getenv("POD_ANNOTATIONS_PATH"))
		if err := runGateway(mode, *gatewayConfig, *gatewayDataDir, *gatewayAddr, runTokenPath, podAnnotationsPath); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "login":
		loginCmd.Parse(os.Args[2:])
		if *loginRemote == "" {
			fmt.Fprintln(os.Stderr, "Error: --remote is required")
			fmt.Fprintln(os.Stderr, "Usage: datuplet login --remote <pipeline-api-url>")
			os.Exit(1)
		}
		if err := runLogin(loginArgs{
			Remote:        *loginRemote,
			Email:         *loginEmail,
			PasswordStdin: *loginPasswordStdin,
			Stdin:         os.Stdin,
			Stdout:        os.Stdout,
			Stderr:        os.Stderr,
		}); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "trigger":
		triggerCmd.Parse(os.Args[2:])
		if *triggerRemote == "" && os.Getenv(envRemote) == "" {
			fmt.Fprintln(os.Stderr, "Error: --remote is required (or set $DATUPLET_REMOTE)")
			fmt.Fprintln(os.Stderr, "Usage: datuplet trigger --remote <pipeline-api-url> [--wait --timeout 1h --json --project <name>] <pipeline-name>")
			os.Exit(1)
		}
		if triggerCmd.NArg() < 1 {
			fmt.Fprintln(os.Stderr, "Error: pipeline name is required (positional, after flags)")
			os.Exit(1)
		}
		if err := runTrigger(*triggerRemote, *triggerTokenFile, *triggerProject, triggerCmd.Arg(0), *triggerWait, *triggerTimeout, *triggerJSON); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "storage":
		storageCmd.Parse(os.Args[2:])
		if *storageRemote == "" && os.Getenv(envRemote) == "" {
			fmt.Fprintln(os.Stderr, "Error: --remote is required (or set $DATUPLET_REMOTE)")
			fmt.Fprintln(os.Stderr, "Usage: datuplet storage --remote <url> [--project N] [--rows N] <tables|info|schema|sample|history> [<ns>.<table>]")
			os.Exit(1)
		}
		if storageCmd.NArg() < 1 {
			fmt.Fprintln(os.Stderr, "Error: missing storage subcommand")
			os.Exit(1)
		}
		sub := storageCmd.Arg(0)
		var err error
		switch sub {
		case "tables":
			err = runStorageTables(*storageRemote, *storageTokenFile, *storageProject)
		case "info", "schema", "history", "sample":
			if storageCmd.NArg() < 2 {
				fmt.Fprintf(os.Stderr, "Error: %s requires <ns>.<table>\n", sub)
				os.Exit(1)
			}
			ref := storageCmd.Arg(1)
			switch sub {
			case "info":
				err = runStorageInfo(*storageRemote, *storageTokenFile, *storageProject, ref)
			case "schema":
				err = runStorageSchema(*storageRemote, *storageTokenFile, *storageProject, ref)
			case "history":
				err = runStorageHistory(*storageRemote, *storageTokenFile, *storageProject, ref)
			case "sample":
				err = runStorageSample(*storageRemote, *storageTokenFile, *storageProject, ref, *storageRows)
			}
		default:
			fmt.Fprintf(os.Stderr, "Error: unknown storage subcommand %q\n", sub)
			os.Exit(1)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "query":
		queryCmd.Parse(os.Args[2:])
		// --local does not need --remote: it routes nowhere (the root binary
		// is duckdb-free and errors clearly). The server-routing path does.
		if !*queryLocal && *queryRemote == "" && os.Getenv(envRemote) == "" {
			fmt.Fprintln(os.Stderr, "Error: --remote is required (omit it only with --local, or set $DATUPLET_REMOTE)")
			fmt.Fprintln(os.Stderr, `Usage: datuplet query --remote <url> [--project N] [--format table|csv|json] (--sql "..." | -f FILE | stdin)`)
			os.Exit(1)
		}
		if err := runQuery(*queryRemote, *queryTokenFile, *queryProject, *querySQL, *queryFile, *queryFormat, *queryLocal); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "pipeline":
		if err := runPipeline(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			// `validate` uses exitCodeErr to distinguish a transport/HTTP
			// failure (>=2) from "the pipeline is invalid" (plain error,
			// default exit 1) — spec §7.
			code := 1
			var ece *exitCodeErr
			if errors.As(err, &ece) {
				code = ece.code
			}
			os.Exit(code)
		}

	case "components":
		if err := runComponents(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "runs":
		if err := runRuns(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "version":
		fmt.Println("datuplet version 0.1.0-poc")

	case "help", "-h", "--help":
		printUsage()

	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

// resolveRunTokenPath returns flagVal if non-empty, otherwise envVal. This
// precedence rule (flag > env) must not be inverted in future refactors.
func resolveRunTokenPath(flagVal, envVal string) string {
	if flagVal != "" {
		return flagVal
	}
	return envVal
}

func printUsage() {
	fmt.Println(`datuplet - Data pipeline orchestrator

Usage:
  datuplet <command> [options]

Commands:
  login                  Authenticate to a Datuplet cluster (stores token + cluster config)
  trigger                Trigger a cluster-side pipeline run (via PipelineRun CRD)
  pipeline               CRUD for pipeline specs (list, get, put, delete, validate)
  runs                   List runs (filter by pipeline/phase) and get run detail + timeline
  components             Browse the component catalog (list, get --schema)
  storage                Browse iceberg storage (tables, info, schema, sample, history)
  query                  Run ad-hoc SQL against the warehouse (routes to the server query service)
  gateway                Start the data gateway server (container entrypoint)
  version                Show version
  help                   Show this help

Options for 'login':
  -remote string         pipeline-api URL (required)
  -email string          Email address (required with --password-stdin)
  -password-stdin        Read the password from stdin (one line, no prompt); requires -email

Options for 'trigger':
  -remote string         pipeline-api URL (required; falls back to $DATUPLET_REMOTE, then ~/.datuplet/cluster.json)
  -project string        Project name (falls back to $DATUPLET_PROJECT; auto-defaulted if you have exactly one)
  -token-file string     Path to JWT/api-token file (falls back to $DATUPLET_API_TOKEN, then ~/.datuplet/api-token)
  -wait                  Block until the run reaches a terminal phase
  -timeout duration      Hard cap on --wait; cancels the run cluster-side on expiry (default 1h)
  -json                  Structured JSON output
  <pipeline-name>        Pipeline name (positional, AFTER flags - flag package stops at first non-flag)

Options for 'storage':
  -remote string         pipeline-api URL (required; falls back to $DATUPLET_REMOTE, then ~/.datuplet/cluster.json)
  -project string        Project name (falls back to $DATUPLET_PROJECT; auto-defaulted if you have exactly one)
  -token-file string     Path to JWT/api-token file (falls back to $DATUPLET_API_TOKEN, then ~/.datuplet/api-token)
  -rows int              Max preview rows for 'sample' subcommand (0 = server default)
  <subcommand>           One of: tables | info | schema | sample | history
  <ns>.<table>           Namespace.table reference (required for info/schema/sample/history)

Options for 'components':
  -remote string         pipeline-api URL (required; falls back to $DATUPLET_REMOTE, then ~/.datuplet/cluster.json)
  -token-file string     Path to JWT/api-token file (falls back to $DATUPLET_API_TOKEN, then ~/.datuplet/api-token)
  <subcommand>           One of: list | get
  list -json             Emit the catalog as JSON (table: NAME, DISPLAY, DEFAULT, IO, DEPRECATED)
  get <name>             Show one component's detail (always resolves the detail endpoint)
  get -version <v>       Resolve a specific version (default: registry defaultVersion, else highest stable)
  get -schema            Print the resolved version's configSchema verbatim (mutually exclusive with -json)
  Not project-scoped: no -project flag (spec RFC 027 §4.7 — shared catalog).

Options for 'query':
  -remote string         pipeline-api URL (required unless --local; falls back to $DATUPLET_REMOTE, then ~/.datuplet/cluster.json)
  -project string        Project name (falls back to $DATUPLET_PROJECT; auto-defaulted if you have exactly one)
  -token-file string     Path to JWT/api-token file (falls back to $DATUPLET_API_TOKEN, then ~/.datuplet/api-token)
  -sql string            Inline SQL (takes precedence over -f and stdin)
  -f string              Path to a .sql file (used when --sql is empty; else stdin)
  -format string         Output format: table | csv | json (default: table)
  -local                 Force local execution (requires the separate duckdb-enabled datuplet-query binary)

Headless / agent auth (RFC 027 §7):
  DATUPLET_API_TOKEN     pipeline-api bearer token; precedence: -token-file > $DATUPLET_API_TOKEN > ~/.datuplet/api-token
  DATUPLET_REMOTE        pipeline-api URL; precedence: -remote > $DATUPLET_REMOTE > ~/.datuplet/cluster.json
  DATUPLET_PROJECT       Project name (resolved like -project) or UUID (fully-headless fast path only); precedence: -project > $DATUPLET_PROJECT > on-disk project resolution
  With all set, no ~/.datuplet state is needed. Use 'datuplet login -email <e> -password-stdin'
  for explicit non-interactive login instead.

Options for 'gateway':
  -local                 Run in local mode (filesystem backend)
  -minio                 Run in MinIO mode (S3-compatible backend)
  -config string         Path to config file (YAML)
  -data-dir string       Data directory for local mode (default: ./data)
  -addr string           gRPC server address (default: :50051)

Examples:
  datuplet gateway --minio --config minio.yaml`)
}
