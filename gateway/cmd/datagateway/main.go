//go:build duckdb_arrow

// Command datagateway is the Data Gateway sidecar: the unchanged gRPC + HTTP
// component contract (pkg/datagateway.ServerV2) on the DuckDB data plane
// (gateway/duckplane). It accepts the operator's sidecar args.
//
// Exit codes: 0 clean shutdown, 20 boot or serve failure (exit-code
// contract: >= 20 is an application failure).
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/datuplet/datuplet/gateway/duckplane"
	"github.com/datuplet/datuplet/pkg/datagateway"
)

const exitApplication = 20

type options struct {
	configPath         string
	addr               string
	runTokenPath       string
	podAnnotationsPath string
	mode               string // "minio" / "local" when given (names the component), else "gateway"
	selfcheck          bool   // --selfcheck: open the plane offline, then exit
}

func main() {
	os.Exit(realMain(os.Args[1:], os.Getenv, os.Stderr))
}

// realMain parses args and runs the gateway until shutdown; it returns the
// process exit code.
func realMain(args []string, getenv func(string) string, stderr io.Writer) int {
	opts, err := parseFlags(args, getenv, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		return exitApplication
	}
	if opts.selfcheck {
		return selfcheck(stderr)
	}
	return run(opts, getenv)
}

// selfcheck opens the data plane with the default (env) settings, which loads
// httpfs and iceberg from the baked extension directory and locks the
// configuration, then closes it. It needs no config file and no network; the
// image's verify stage runs it with --network=none as the runtime user.
func selfcheck(stderr io.Writer) int {
	settings, err := duckplane.SettingsFromEnv()
	if err != nil {
		fmt.Fprintf(stderr, "datagateway: selfcheck: %v\n", err)
		return exitApplication
	}
	plane, err := duckplane.New(duckplane.Config{
		Settings:   settings,
		Extensions: []string{"httpfs", "iceberg"},
	})
	if err != nil {
		fmt.Fprintf(stderr, "datagateway: selfcheck: %v\n", err)
		return exitApplication
	}
	if err := plane.Close(); err != nil {
		fmt.Fprintf(stderr, "datagateway: selfcheck: close: %v\n", err)
		return exitApplication
	}
	fmt.Fprintln(stderr, "datagateway: selfcheck ok")
	return 0
}

// parseFlags parses the sidecar flags. --minio and --local are accepted and
// ignored (the operator passes --minio); --run-token-path and
// --pod-annotations-path fall back to RUN_TOKEN_PATH / POD_ANNOTATIONS_PATH.
func parseFlags(args []string, getenv func(string) string, stderr io.Writer) (options, error) {
	fs := flag.NewFlagSet("datagateway", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "Path to the gateway config file (YAML, required)")
	addr := fs.String("addr", ":50051", "gRPC listen address (the HTTP data plane listens on :50052)")
	runTokenPath := fs.String("run-token-path", "", "Path to the mounted run-token file (also RUN_TOKEN_PATH)")
	podAnnotationsPath := fs.String("pod-annotations-path", "", "Path to the downward-API pod annotations file; the gateway exits cleanly on datuplet.io/cancel=true (also POD_ANNOTATIONS_PATH)")
	minio := fs.Bool("minio", false, "Accepted and ignored (operator compatibility)")
	local := fs.Bool("local", false, "Accepted and ignored (operator compatibility)")
	selfcheck := fs.Bool("selfcheck", false, "Open the DuckDB plane (load httpfs + iceberg, lock) and exit 0; needs no config or network")
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if *selfcheck {
		return options{selfcheck: true}, nil
	}
	if *configPath == "" {
		fmt.Fprintln(stderr, "datagateway: --config is required")
		fs.Usage()
		return options{}, errors.New("--config is required")
	}
	opts := options{
		configPath:         *configPath,
		addr:               *addr,
		runTokenPath:       firstNonEmpty(*runTokenPath, getenv("RUN_TOKEN_PATH")),
		podAnnotationsPath: firstNonEmpty(*podAnnotationsPath, getenv("POD_ANNOTATIONS_PATH")),
		mode:               "gateway",
	}
	switch {
	case *minio:
		opts.mode = "minio"
	case *local:
		opts.mode = "local"
	}
	return opts, nil
}

// firstNonEmpty returns flagVal if set, else envVal (flag > env).
func firstNonEmpty(flagVal, envVal string) string {
	if flagVal != "" {
		return flagVal
	}
	return envVal
}

func run(opts options, getenv func(string) string) int {
	log.Printf("datagateway: starting %s iteration_id=%s run_id=%s",
		datagateway.GatewayBuildInfo(), orNone(getenv("DATUPLET_ITERATION_ID")), orNone(getenv("RUN_ID")))

	cfg, err := datagateway.LoadConfigFile(opts.configPath, opts.runTokenPath, opts.podAnnotationsPath)
	if err != nil {
		log.Printf("datagateway: %v", err)
		return exitApplication
	}
	if cfg.RunID == "" {
		cfg.RunID = fmt.Sprintf("%s-%d", opts.mode, time.Now().Unix())
	}
	cfg.ComponentName = opts.mode + "-component"

	settings, err := duckplane.SettingsFromEnv()
	if err != nil {
		log.Printf("datagateway: %v", err)
		return exitApplication
	}

	// Pyroscope continuous profiling when DATUPLET_GATEWAY_PROFILING is set.
	if stopProfiling := datagateway.StartProfilingIfEnabled(); stopProfiling != nil {
		defer func() {
			if err := stopProfiling(); err != nil {
				log.Printf("datagateway: pyroscope stop error: %v", err)
			}
		}()
	}

	// Registered before the data plane comes up: the kubelet may stop the
	// sidecar while NewServerV2 is still initialising, and an unhandled
	// SIGTERM there kills the process mid-init (exit 2). A signal caught here
	// is handled by the select below once the server exists.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	server, err := datagateway.NewServerV2(cfg, duckplane.Factory(duckplane.Config{
		Settings:   settings,
		Extensions: []string{"httpfs", "iceberg"},
	}))
	if err != nil {
		log.Printf("datagateway: %v", err)
		return exitApplication
	}

	// ServeWithHTTP starts both gRPC (control) and HTTP (data transfer); it
	// returns after a graceful stop (Close, or the cancel annotation).
	errCh := make(chan error, 1)
	go func() { errCh <- server.ServeWithHTTP(opts.addr) }()

	select {
	case err := <-errCh:
		closeServer(server)
		if err != nil {
			log.Printf("datagateway: serve: %v", err)
			return exitApplication
		}
		return 0
	case sig := <-sigCh:
		log.Printf("datagateway: received %s; shutting down", sig)
		closeServer(server)
		// Wait for Serve to return after the graceful stop so in-flight RPCs finish.
		<-errCh
		return 0
	}
}

func closeServer(server *datagateway.ServerV2) {
	if err := server.Close(); err != nil {
		log.Printf("datagateway: close: %v", err)
	}
}

func orNone(v string) string {
	if v == "" {
		return "(none)"
	}
	return v
}
