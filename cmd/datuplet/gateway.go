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

	"github.com/datuplet/datuplet/pkg/datagateway"
	"github.com/datuplet/datuplet/pkg/datagateway/goplane"
)

// gatewayExitApplication is the exit code for a boot or serve failure
// (exit-code contract: >= 20 is an application failure).
const gatewayExitApplication = 20

type gatewayOptions struct {
	configPath         string
	addr               string
	runTokenPath       string
	podAnnotationsPath string
	mode               string // "minio" / "local" when given (names the component), else "gateway"
}

// gatewayMain runs `datuplet gateway` (the Data Gateway sidecar) and returns
// the process exit code: 0 clean shutdown, 20 boot or serve failure, non-zero
// on a usage error.
func gatewayMain(args []string, getenv func(string) string, stderr io.Writer) int {
	opts, err := parseGatewayFlags(args, getenv, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		return 2
	}
	return runGateway(opts, getenv)
}

// parseGatewayFlags parses the sidecar flags. --minio and --local are
// accepted (the operator passes --minio) and only name the component;
// --data-dir is accepted and ignored. --run-token-path and
// --pod-annotations-path fall back to RUN_TOKEN_PATH / POD_ANNOTATIONS_PATH.
func parseGatewayFlags(args []string, getenv func(string) string, stderr io.Writer) (gatewayOptions, error) {
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	fs.SetOutput(stderr)
	local := fs.Bool("local", false, "Accepted for compatibility (names the component)")
	minio := fs.Bool("minio", false, "Accepted for compatibility; the operator passes it (names the component)")
	configPath := fs.String("config", "", "Path to the gateway config file (YAML, required)")
	fs.String("data-dir", "./data", "Accepted and ignored")
	addr := fs.String("addr", ":50051", "gRPC server address (the HTTP data plane listens on :50052)")
	runTokenPath := fs.String("run-token-path", "", "Path to the mounted run-token file (K8s typically sets /var/run/secrets/datuplet-runtoken/token). Also RUN_TOKEN_PATH env var.")
	podAnnotationsPath := fs.String("pod-annotations-path", "", "Path to the kubelet downward-API pod-annotations file (K8s typically sets /etc/podinfo/annotations). When set, the gateway polls the file every 5s and exits cleanly on `datuplet.io/cancel=true`. Also POD_ANNOTATIONS_PATH env var.")
	if err := fs.Parse(args); err != nil {
		return gatewayOptions{}, err
	}
	if *configPath == "" {
		fmt.Fprintln(stderr, "datuplet gateway: --config is required")
		fs.Usage()
		return gatewayOptions{}, errors.New("--config is required")
	}
	opts := gatewayOptions{
		configPath:         *configPath,
		addr:               *addr,
		runTokenPath:       resolveRunTokenPath(*runTokenPath, getenv("RUN_TOKEN_PATH")),
		podAnnotationsPath: resolveRunTokenPath(*podAnnotationsPath, getenv("POD_ANNOTATIONS_PATH")),
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

func runGateway(opts gatewayOptions, getenv func(string) string) int {
	log.Printf("datuplet-gateway: starting %s iteration_id=%s run_id=%s",
		datagateway.GatewayBuildInfo(), orNone(getenv("DATUPLET_ITERATION_ID")), orNone(getenv("RUN_ID")))

	cfg, err := datagateway.LoadConfigFile(opts.configPath, opts.runTokenPath, opts.podAnnotationsPath)
	if err != nil {
		log.Printf("datuplet-gateway: %v", err)
		return gatewayExitApplication
	}
	if cfg.RunID == "" {
		cfg.RunID = fmt.Sprintf("%s-%d", opts.mode, time.Now().Unix())
	}
	cfg.ComponentName = opts.mode + "-component"

	// Pyroscope continuous profiling when DATUPLET_GATEWAY_PROFILING is set.
	if stopProfiling := datagateway.StartProfilingIfEnabled(); stopProfiling != nil {
		defer func() {
			if err := stopProfiling(); err != nil {
				log.Printf("datuplet-gateway: pyroscope stop error: %v", err)
			}
		}()
	}

	server, err := datagateway.NewServerV2(cfg, goplane.Factory)
	if err != nil {
		log.Printf("datuplet-gateway: %v", err)
		return gatewayExitApplication
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	// ServeWithHTTP starts both gRPC (control) and HTTP (data transfer); it
	// returns after a graceful stop (Close, or the cancel annotation).
	errCh := make(chan error, 1)
	go func() { errCh <- server.ServeWithHTTP(opts.addr) }()

	select {
	case err := <-errCh:
		closeGatewayServer(server)
		if err != nil {
			log.Printf("datuplet-gateway: serve: %v", err)
			return gatewayExitApplication
		}
		return 0
	case sig := <-sigCh:
		log.Printf("datuplet-gateway: received %s; shutting down", sig)
		closeGatewayServer(server)
		// Wait for Serve to return after the graceful stop so in-flight RPCs finish.
		<-errCh
		return 0
	}
}

func closeGatewayServer(server *datagateway.ServerV2) {
	if err := server.Close(); err != nil {
		log.Printf("datuplet-gateway: close: %v", err)
	}
}

func orNone(v string) string {
	if v == "" {
		return "(none)"
	}
	return v
}
