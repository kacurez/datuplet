package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
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

// gatewayBootFailureHold bounds how long a gateway whose boot failed keeps
// serving the boot error before it exits.
const gatewayBootFailureHold = 10 * time.Minute

// gatewayHooks lets tests drive runGatewayWith without a real plane or
// process signals. The zero value is production.
type gatewayHooks struct {
	// newPlane replaces goplane.Factory.
	newPlane datagateway.DataPlaneFactory
	// signals replaces SIGINT/SIGTERM delivery.
	signals chan os.Signal
	// bootFailureHold replaces gatewayBootFailureHold when non-zero.
	bootFailureHold time.Duration
	// httpAddr replaces the config's HTTP listen address when non-empty.
	httpAddr string
	// newServer replaces datagateway.NewServerV2 (called with the resolved
	// plane factory).
	newServer func(*datagateway.Config, datagateway.DataPlaneFactory) (*datagateway.ServerV2, error)
	// listening is called with the gRPC address once it is bound.
	listening func(net.Addr)
}

func runGateway(opts gatewayOptions, getenv func(string) string) int {
	return runGatewayWith(opts, getenv, gatewayHooks{})
}

// runGatewayWith listens before it boots: the component container starts with
// this process and its SDK makes a single GetConfig call, so the gRPC listener
// is bound first and the HTTP one as soon as the config is loaded; both hold
// every request until boot ends. A failed boot is served as the error (gRPC
// Unavailable / HTTP 503) until SIGTERM/SIGINT or the hold, then the gateway
// exits 20: exiting at once would make the kubelet restart the sidecar and
// race the component's call again.
func runGatewayWith(opts gatewayOptions, getenv func(string) string, hooks gatewayHooks) int {
	if limit, applied := datagateway.ApplyMemoryLimit(os.Getenv, os.ReadFile); applied {
		log.Printf("datuplet-gateway: cgroup memory limit %d bytes; Go memory limit set to 70%%", limit)
	} else {
		log.Printf("datuplet-gateway: Go memory limit not applied (GOMEMLIMIT set or no cgroup limit)")
	}
	log.Printf("datuplet-gateway: starting %s iteration_id=%s run_id=%s",
		datagateway.GatewayBuildInfo(), orNone(getenv("DATUPLET_ITERATION_ID")), orNone(getenv("RUN_ID")))

	// Registered before boot: the kubelet may stop the sidecar while it is
	// still booting. A signal caught during boot is handled once boot returns.
	sigCh := hooks.signals
	if sigCh == nil {
		sigCh = make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sigCh)
	}

	grpcLis, err := net.Listen("tcp", opts.addr)
	if err != nil {
		log.Printf("datuplet-gateway: listen %s: %v", opts.addr, err)
		return gatewayExitApplication
	}
	gate := datagateway.NewBootGate()
	// Each serve goroutine sends once; buffered so neither blocks.
	serveErr := make(chan error, 2)
	go func() { serveErr <- gate.ServeGRPC(grpcLis) }()
	if hooks.listening != nil {
		hooks.listening(grpcLis.Addr())
	}

	// Pyroscope continuous profiling when DATUPLET_GATEWAY_PROFILING is set.
	if stopProfiling := datagateway.StartProfilingIfEnabled(); stopProfiling != nil {
		defer func() {
			if err := stopProfiling(); err != nil {
				log.Printf("datuplet-gateway: pyroscope stop error: %v", err)
			}
		}()
	}

	server, err := bootGateway(opts, hooks, gate, serveErr)
	if err != nil {
		err = datagateway.SanitizeBootError(err, opts.runTokenPath)
		gate.Fail(err)
		hold := gatewayBootFailureHold
		if hooks.bootFailureHold > 0 {
			hold = hooks.bootFailureHold
		}
		log.Printf("datuplet-gateway: boot failed: %v; serving the error for at most %s", err, hold)
		timer := time.NewTimer(hold)
		defer timer.Stop()
		select {
		case sig := <-sigCh:
			log.Printf("datuplet-gateway: received %s after a failed boot; shutting down", sig)
		case <-timer.C:
			log.Printf("datuplet-gateway: boot error served for %s; exiting", hold)
		case err := <-serveErr:
			if err != nil {
				log.Printf("datuplet-gateway: serve: %v", err)
			}
		}
		gate.Stop()
		return gatewayExitApplication
	}
	gate.Ready(server)

	select {
	case err := <-serveErr:
		// A graceful stop (the cancel annotation) or a serve failure.
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
		<-serveErr
		return 0
	}
}

// bootGateway loads the config, binds and serves the HTTP listener, then
// builds the server (run-token validation, secrets, commit pool, data plane).
func bootGateway(opts gatewayOptions, hooks gatewayHooks, gate *datagateway.BootGate, serveErr chan<- error) (*datagateway.ServerV2, error) {
	cfg, err := datagateway.LoadConfigFile(opts.configPath, opts.runTokenPath, opts.podAnnotationsPath)
	if err != nil {
		return nil, err
	}
	if hooks.httpAddr != "" {
		cfg.HTTPAddr = hooks.httpAddr
	}
	// Bound before boot completes, so a GetConfig that advertises the HTTP
	// endpoint never points at an unbound port.
	httpLis, err := net.Listen("tcp", cfg.HTTPListenAddr())
	if err != nil {
		return nil, fmt.Errorf("listen %s (HTTP): %w", cfg.HTTPListenAddr(), err)
	}
	go func() { serveErr <- gate.ServeDataHTTP(httpLis) }()

	if cfg.RunID == "" {
		cfg.RunID = fmt.Sprintf("%s-%d", opts.mode, time.Now().Unix())
	}
	cfg.ComponentName = opts.mode + "-component"

	newPlane := hooks.newPlane
	if newPlane == nil {
		newPlane = goplane.Factory
	}
	newServer := hooks.newServer
	if newServer == nil {
		newServer = datagateway.NewServerV2
	}
	return newServer(cfg, newPlane)
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
