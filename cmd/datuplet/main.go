// Package main provides the datuplet CLI.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/datuplet/datuplet/pkg/icebergjob"

	// Blank-import the centralised iceberg-go IO scheme registration
	// package so this binary's `gs://` factory is the Datuplet override
	// regardless of which transitive package registers first. See
	// pkg/datupleticeio/doc.go and RFC 019 §4.5.
	_ "github.com/datuplet/datuplet/pkg/datupleticeio"
)

func main() {
	testCmd := flag.NewFlagSet("test-component", flag.ExitOnError)
	testImage := testCmd.String("image", "", "Component image to test")
	testConfig := testCmd.String("config", "{}", "Component config (JSON)")
	testEndpoint := testCmd.String("endpoint", "localhost:9000", "Data lake endpoint")
	testBucket := testCmd.String("bucket", "datuplet", "Data lake bucket")

	sampleCmd := flag.NewFlagSet("sample", flag.ExitOnError)
	sampleImage := sampleCmd.String("image", "", "Component image to sample (required)")
	sampleConfig := sampleCmd.String("config", "{}", "Component config (JSON)")
	sampleLimit := sampleCmd.Int("limit", 10, "Maximum number of rows to return")

	loginCmd := flag.NewFlagSet("login", flag.ExitOnError)
	loginRemote := loginCmd.String("remote", "", "pipeline-api URL (required)")

	runCmd := flag.NewFlagSet("run", flag.ExitOnError)
	runRemoteFlag := runCmd.String("remote", "", "pipeline-api URL of the target cluster (required for remote runs)")
	runTokenFile := runCmd.String("token-file", "", "Path to JWT token file (default: ~/.datuplet/token)")
	runProject := runCmd.String("project", "", "Project name to run under (required if you have access to >1 project; auto-defaulted if you have exactly one)")

	triggerCmd := flag.NewFlagSet("trigger", flag.ExitOnError)
	triggerRemote := triggerCmd.String("remote", "", "pipeline-api URL (required)")
	triggerTokenFile := triggerCmd.String("token-file", "", "Path to JWT token file (default: ~/.datuplet/token)")
	triggerProject := triggerCmd.String("project", "", "Project name (auto-defaulted if you have exactly one)")
	triggerWait := triggerCmd.Bool("wait", false, "Block until the run reaches a terminal phase")
	triggerTimeout := triggerCmd.Duration("timeout", time.Hour, "Hard cap on --wait; cancels the run cluster-side on expiry")
	triggerJSON := triggerCmd.Bool("json", false, "Structured JSON output")

	gatewayCmd := flag.NewFlagSet("gateway", flag.ExitOnError)
	gatewayLocal := gatewayCmd.Bool("local", false, "Run in local mode (filesystem backend)")
	gatewayMinio := gatewayCmd.Bool("minio", false, "Run in MinIO mode (S3-compatible backend)")
	gatewayConfig := gatewayCmd.String("config", "", "Path to config file (YAML)")
	gatewayDataDir := gatewayCmd.String("data-dir", "./data", "Data directory for local mode")
	gatewayAddr := gatewayCmd.String("addr", ":50051", "gRPC server address")
	gatewayRunTokenPath := gatewayCmd.String("run-token-path", "", "Path to the mounted run-token file (K8s typically sets /var/run/secrets/datuplet-runtoken/tokens). When set, the gateway holds the per-table JSON map of JWTs the gateway forwards to lakekeeper for catalog + STS calls. Also RUN_TOKEN_PATH env var.")
	gatewayPodAnnotationsPath := gatewayCmd.String("pod-annotations-path", "", "Path to the kubelet downward-API pod-annotations file (K8s typically sets /etc/podinfo/annotations). When set, the gateway polls the file every 5s and exits cleanly on `datuplet.io/cancel=true`. Also POD_ANNOTATIONS_PATH env var.")

	icebergJobCmd := flag.NewFlagSet("iceberg-job", flag.ExitOnError)
	ijMode := icebergJobCmd.String("mode", "table-commit", "iceberg-job mode: table-commit (only supported mode; workspace-* modes have been removed)")
	tcRunID := icebergJobCmd.String("run-id", "", "Run ID to commit (required for --mode=table-commit, or RUN_ID env)")
	tcBucket := icebergJobCmd.String("bucket", "", "Logical bucket identifier / Iceberg namespace (required, or BUCKET env)")
	tcTable := icebergJobCmd.String("table", "", "Table name within the bucket (required when --bucket is used directly; multi-table runs typically come through env)")
	tcWriteMode := icebergJobCmd.String("write-mode", "APPEND", "Write mode: APPEND or FULL_LOAD (or WRITE_MODE env)")
	// TableCommit talks directly to lakekeeper; S3 credential flags are no
	// longer accepted. The commit binary uses only the run-token JWT and
	// lakekeeper-vended STS credentials for all data-plane reads/writes.
	tcLakekeeperURL := icebergJobCmd.String("lakekeeper-url", "", "Lakekeeper REST catalog base URL (required, or LAKEKEEPER_URL env)")
	// S3/MinIO credential flags are dropped. The commit binary uses only the
	// run-token JWT; lakekeeper vends per-table STS credentials for all
	// data-plane reads/writes. No long-lived S3 credentials accepted.
	tcRunTokenPath := icebergJobCmd.String("run-token-path", "", "Path to the projected per-run JWT (K8s typically sets /var/run/secrets/datuplet-runtoken/token). When set, the binary attaches it as `Authorization: Bearer <jwt>` on lakekeeper requests. Also RUN_TOKEN_PATH env var.")
	// JWKS endpoint for run-token validation. Required when --run-token-path
	// is set; the binary validates the mounted JWT against this URL before using any
	// claims (8-check contract). Also PIPELINE_API_JWKS_URL env.
	tcPipelineAPIJWKSURL := icebergJobCmd.String("pipeline-api-jwks-url", "", "JWKS endpoint URL for pipeline-api (e.g. http://pipeline-api.datuplet.svc.cluster.local:8081/api/v1/auth/jwks.json). Required when --run-token-path is set; used to validate the mounted JWT. Also PIPELINE_API_JWKS_URL env.")

	// The table-gateway subcommand has been removed; lakekeeper is now the
	// catalog of record. The case below still exists so users running the old
	// subcommand see a clear error instead of a generic "unknown command".

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "test-component":
		testCmd.Parse(os.Args[2:])
		if *testImage == "" {
			fmt.Println("Error: --image is required")
			fmt.Println("Usage: datuplet test-component --image <image> [options]")
			os.Exit(1)
		}
		if err := testComponent(*testImage, *testConfig, *testEndpoint, *testBucket); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "sample":
		sampleCmd.Parse(os.Args[2:])
		if *sampleImage == "" {
			fmt.Println("Error: --image is required")
			fmt.Println("Usage: datuplet sample --image <image> [options]")
			os.Exit(1)
		}
		if err := sampleComponent(*sampleImage, *sampleConfig, *sampleLimit); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

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

	case "iceberg-job":
		icebergJobCmd.Parse(os.Args[2:])

		switch *ijMode {
		case "table-commit":
			// Determine which flags the user actually set on the command
			// line so env vars only fill in when the flag was NOT passed.
			// Without this, an explicit `--write-mode=APPEND` (the default
			// value) would silently shadow `WRITE_MODE=FULL_LOAD` from the
			// container env because the post-parse string compare can't
			// tell "user passed APPEND" from "default APPEND, never set".
			tcExplicit := map[string]bool{}
			icebergJobCmd.Visit(func(f *flag.Flag) { tcExplicit[f.Name] = true })

			// Support env vars for container mode
			runID := *tcRunID
			if runID == "" {
				runID = os.Getenv("RUN_ID")
			}
			bucket := *tcBucket
			if bucket == "" {
				bucket = os.Getenv("BUCKET")
			}
			tableName := *tcTable
			if tableName == "" {
				tableName = os.Getenv("TABLE")
			}
			writeMode := *tcWriteMode
			if !tcExplicit["write-mode"] {
				if envMode := os.Getenv("WRITE_MODE"); envMode != "" {
					writeMode = envMode
				}
			}
			lakekeeperURL := *tcLakekeeperURL
			if lakekeeperURL == "" {
				lakekeeperURL = os.Getenv("LAKEKEEPER_URL")
			}
			// S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY,
			// S3_REGION, S3_USE_SSL, S3_USE_PATH_STYLE are no longer read. The
			// commit binary uses only the run-token JWT + lakekeeper-vended creds.
			tcRunToken := resolveRunTokenPath(*tcRunTokenPath, os.Getenv("RUN_TOKEN_PATH"))
			pipelineAPIJWKSURL := *tcPipelineAPIJWKSURL
			if pipelineAPIJWKSURL == "" {
				pipelineAPIJWKSURL = os.Getenv("PIPELINE_API_JWKS_URL")
			}

			if runID == "" {
				fmt.Println("Error: --run-id is required (or RUN_ID env var)")
				fmt.Println("Usage: datuplet iceberg-job --mode=table-commit --run-id <id> --lakekeeper-url <url> [--bucket <ns> [--table <tbl>]]")
				os.Exit(1)
			}
			if lakekeeperURL == "" {
				fmt.Println("Error: --lakekeeper-url is required (or LAKEKEEPER_URL env var)")
				os.Exit(1)
			}
			// --bucket alone is an auto-discover-within-namespace filter.
			// --table without --bucket is rejected.
			if tableName != "" && bucket == "" {
				fmt.Println("Error: --table requires --bucket")
				os.Exit(1)
			}

			runArgs := tableCommitArgs{
				RunID:              runID,
				Namespace:          bucket,
				Table:              tableName,
				WriteMode:          icebergjob.WriteMode(writeMode),
				LakekeeperURL:      lakekeeperURL,
				RunTokenPath:       tcRunToken,
				PipelineAPIJWKSURL: pipelineAPIJWKSURL,
			}
			if err := runTableCommit(runArgs); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				// runTableCommit emits the DUPLET_STATUS_MESSAGE prefix
				// itself on every error path (defer at function entry), so
				// don't double-emit it here.
				os.Exit(20) // FailedApplication: TableCommit failures are always application errors
			}

		default:
			// workspace-* modes have been removed; table-commit is the only supported mode.
			fmt.Fprintf(os.Stderr, "Error: unknown iceberg-job mode %q (only valid mode: table-commit)\n", *ijMode)
			os.Exit(2)
		}

	case "run":
		runCmd.Parse(os.Args[2:])
		if *runRemoteFlag == "" {
			fmt.Fprintln(os.Stderr, "Error: --remote is required")
			fmt.Fprintln(os.Stderr, "Usage: datuplet run --remote <pipeline-api-url> <pipeline.yaml>")
			os.Exit(1)
		}
		pipelineArgs := runCmd.Args()
		if len(pipelineArgs) == 0 {
			fmt.Fprintln(os.Stderr, "Error: pipeline YAML path is required")
			fmt.Fprintln(os.Stderr, "Usage: datuplet run --remote <pipeline-api-url> <pipeline.yaml>")
			os.Exit(1)
		}
		if err := runRemote(*runRemoteFlag, *runTokenFile, *runProject, pipelineArgs[0]); err != nil {
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
			Remote: *loginRemote,
			Stdin:  os.Stdin,
			Stdout: os.Stdout,
			Stderr: os.Stderr,
		}); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "trigger":
		triggerCmd.Parse(os.Args[2:])
		if *triggerRemote == "" {
			fmt.Fprintln(os.Stderr, "Error: --remote is required")
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

	case "table-gateway":
		fmt.Fprintln(os.Stderr, "Error: the `table-gateway` subcommand has been removed. Lakekeeper now serves the catalog directly.")
		os.Exit(1)

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
// precedence rule (flag > env) is the pinned invariant tested in
// run_token_path_test.go — a future refactor must not invert it.
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
  run                    Run a pipeline against a remote Datuplet cluster
  trigger                Trigger a cluster-side pipeline run (via PipelineRun CRD)
  gateway                Start the data gateway server (container entrypoint)
  iceberg-job            Run an Iceberg job (table-commit mode only)
  table-gateway          (REMOVED) Lakekeeper now serves the catalog directly
  test-component         Test a single component
  sample                 Get sample data from a component (for AI/automation)
  version                Show version
  help                   Show this help

Options for 'login':
  -remote string         pipeline-api URL (required)

Options for 'run':
  -remote string         pipeline-api URL of the target cluster (required)
  -token-file string     Path to JWT token file (default: ~/.datuplet/token)
  <pipeline.yaml>        Path to pipeline YAML file (positional, required)

Options for 'trigger':
  -remote string         pipeline-api URL (required)
  -project string        Project name (auto-defaulted if you have exactly one)
  -token-file string     Path to JWT token file (default: ~/.datuplet/token)
  -wait                  Block until the run reaches a terminal phase
  -timeout duration      Hard cap on --wait; cancels the run cluster-side on expiry (default 1h)
  -json                  Structured JSON output
  <pipeline-name>        Pipeline name (positional, AFTER flags - flag package stops at first non-flag)

Options for 'gateway':
  -local                 Run in local mode (filesystem backend)
  -minio                 Run in MinIO mode (S3-compatible backend)
  -config string         Path to config file (YAML)
  -data-dir string       Data directory for local mode (default: ./data)
  -addr string           gRPC server address (default: :50051)

Options for 'iceberg-job':
  -mode string              Job mode: table-commit (only supported mode)
  --- table-commit mode ---
  -run-id string            Run ID to commit (required, or RUN_ID env)
  -lakekeeper-url string    Lakekeeper REST catalog URL (required, or LAKEKEEPER_URL env)
  -warehouse-name string    Lakekeeper warehouse name (or WAREHOUSE_NAME env)
  -warehouse-root string    Storage root URL (vestigial post-Slice-10b; unused — derived from Table.Location()). Also WAREHOUSE_ROOT env.
  -bucket string            Logical bucket / namespace filter; auto-discover within it when --table is omitted (or BUCKET env)
  -table string             Optional table name to commit; requires --bucket (or TABLE env)
  -write-mode string        APPEND or FULL_LOAD (default: APPEND, or WRITE_MODE env)
  -run-token-path string    Path to projected per-run JWT (or RUN_TOKEN_PATH env)
  NOTE: S3 credential flags (--endpoint, --access-key, --secret-key, etc.) are
  removed. Commit Jobs use the run-token JWT + lakekeeper-vended credentials only.

Options for 'test-component':
  -image string          Component image to test (required)
  -config string         Component config as JSON (default: {})
  -endpoint string       Data lake endpoint (default: localhost:9000)
  -bucket string         Data lake bucket (default: datuplet)

Options for 'sample':
  -image string          Component image to sample (required)
  -config string         Component config as JSON (default: {})
  -limit int             Maximum number of rows to return (default: 10)

Examples:
  datuplet gateway --minio --config minio.yaml
  datuplet iceberg-job --mode=table-commit --run-id run-abc123 --lakekeeper-url http://lakekeeper:8181/catalog --bucket raw --table products
  datuplet test-component --image datuplet/data-generator:latest --config '{"tables":[{"name":"t","random":{"schema":{"id":"int"},"limit":{"rowsCount":5}}}]}'
  datuplet sample --image datuplet/data-generator:latest --config '{"tables":[{"name":"t","random":{"schema":{"id":"int"},"limit":{"rowsCount":5}}}]}' --limit 5`)
}
