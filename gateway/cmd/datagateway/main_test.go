//go:build duckdb_arrow

package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/datuplet/datuplet/gateway/duck"
	"github.com/datuplet/datuplet/pkg/datagateway"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

func noEnv(string) string { return "" }

// The operator's sidecar args parse, including the accepted-but-ignored
// --minio; flags win over the env fallbacks.
func TestParseFlagsOperatorArgs(t *testing.T) {
	var stderr bytes.Buffer
	opts, err := parseFlags([]string{
		"--minio",
		"--config", "/etc/datuplet/gateway.yaml",
		"--addr", ":6000",
		"--run-token-path", "/var/run/secrets/datuplet-runtoken/token",
		"--pod-annotations-path", "/etc/podinfo/annotations",
	}, func(k string) string { return "/from-env/" + k }, &stderr)
	if err != nil {
		t.Fatalf("parseFlags: %v (stderr %q)", err, stderr.String())
	}
	want := options{
		configPath:         "/etc/datuplet/gateway.yaml",
		addr:               ":6000",
		runTokenPath:       "/var/run/secrets/datuplet-runtoken/token",
		podAnnotationsPath: "/etc/podinfo/annotations",
		mode:               "minio",
	}
	if opts != want {
		t.Errorf("opts = %+v, want %+v", opts, want)
	}
}

func TestParseFlagsDefaultsAndEnv(t *testing.T) {
	env := map[string]string{"RUN_TOKEN_PATH": "/env/token", "POD_ANNOTATIONS_PATH": "/env/annotations"}
	opts, err := parseFlags([]string{"--local", "--config", "c.yaml"}, func(k string) string { return env[k] }, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if opts.addr != ":50051" || opts.runTokenPath != "/env/token" || opts.podAnnotationsPath != "/env/annotations" || opts.mode != "local" {
		t.Errorf("opts = %+v", opts)
	}
}

func TestMissingConfigExitsWithUsage(t *testing.T) {
	var stderr bytes.Buffer
	code := realMain([]string{"--minio"}, noEnv, &stderr)
	if code == 0 {
		t.Fatal("missing --config: exit code 0, want non-zero")
	}
	if !strings.Contains(stderr.String(), "--config is required") || !strings.Contains(stderr.String(), "Usage") {
		t.Errorf("stderr = %q, want the error and the usage text", stderr.String())
	}
}

func TestUnknownFlagExitsNonZero(t *testing.T) {
	if code := realMain([]string{"--nope"}, noEnv, &bytes.Buffer{}); code == 0 {
		t.Error("unknown flag: exit code 0, want non-zero")
	}
}

// --selfcheck needs no --config: the image build runs it offline.
func TestParseFlagsSelfcheck(t *testing.T) {
	opts, err := parseFlags([]string{"--selfcheck"}, noEnv, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if !opts.selfcheck {
		t.Errorf("opts.selfcheck = false, want true")
	}
	opts, err = parseFlags([]string{"--config", "c.yaml"}, noEnv, &bytes.Buffer{})
	if err != nil || opts.selfcheck {
		t.Errorf("without the flag: opts = %+v, err = %v", opts, err)
	}
}

// --selfcheck fails (exit 20) when httpfs/iceberg cannot be loaded. The
// success path needs the baked extensions; the image's verify stage covers it.
func TestSelfcheckFailsWithoutExtensions(t *testing.T) {
	var stderr bytes.Buffer
	// Settings come from the process env; an empty extension dir has nothing
	// to LOAD (and nothing is fetched: an explicit LOAD never installs).
	t.Setenv("DATUPLET_GATEWAY_DUCKDB_EXTENSION_DIR", t.TempDir())
	t.Setenv("DATUPLET_GATEWAY_DUCKDB_TEMP_DIR", t.TempDir())
	if code := realMain([]string{"--selfcheck"}, noEnv, &stderr); code != exitApplication {
		t.Errorf("selfcheck with no extensions: exit %d, want %d (stderr %q)", code, exitApplication, stderr.String())
	}
}

// nopPlane is a data plane that only records Close.
type nopPlane struct{ closed atomic.Bool }

func (p *nopPlane) OpenTableWriter(context.Context, datagateway.WriterSpec) (datagateway.TableWriter, error) {
	return nil, errors.New("nopPlane")
}

func (p *nopPlane) OpenTableReader(context.Context, datagateway.ReaderSpec) (datagateway.RecordStream, error) {
	return nil, errors.New("nopPlane")
}
func (p *nopPlane) Interrupt()   {}
func (p *nopPlane) Close() error { p.closed.Store(true); return nil }

// gatewayRun is run started in the background with test hooks.
type gatewayRun struct {
	signals chan os.Signal
	client  pb.DataGatewayClient
	exit    chan int
}

func startRun(t *testing.T, configPath string, newPlane func(duck.Settings) datagateway.DataPlaneFactory, hold time.Duration) *gatewayRun {
	t.Helper()
	r := &gatewayRun{signals: make(chan os.Signal, 1), exit: make(chan int, 1)}
	addrCh := make(chan net.Addr, 1)
	hooks := runHooks{
		newPlane:        newPlane,
		signals:         r.signals,
		bootFailureHold: hold,
		httpAddr:        "127.0.0.1:0",
		listening:       func(a net.Addr) { addrCh <- a },
	}
	opts := options{configPath: configPath, addr: "127.0.0.1:0", mode: "gateway"}
	go func() { r.exit <- run(opts, noEnv, hooks) }()
	var addr net.Addr
	select {
	case addr = <-addrCh:
	case code := <-r.exit:
		t.Fatalf("run exited %d before listening", code)
	}
	conn, err := grpc.NewClient(addr.String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	r.client = pb.NewDataGatewayClient(conn)
	return r
}

func (r *gatewayRun) waitExit(t *testing.T) int {
	t.Helper()
	select {
	case code := <-r.exit:
		return code
	case <-time.After(10 * time.Second):
		t.Fatal("run did not exit")
		return -1
	}
}

func writeConfig(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(p, []byte("run_id: main-test-run\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// (e) A SIGTERM that arrives while boot is blocked is handled once boot
// returns: a clean exit 0 after a successful boot (the in-flight GetConfig
// still succeeds and the plane is closed), 20 after a failed one.
func TestRunSIGTERMDuringBlockedBoot(t *testing.T) {
	for _, tc := range []struct {
		name     string
		bootErr  error
		wantExit int
	}{
		{"boot succeeds", nil, 0},
		{"boot fails", errors.New("plane init failed"), exitApplication},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			plane := &nopPlane{}
			newPlane := func(duck.Settings) datagateway.DataPlaneFactory {
				return func(context.Context, datagateway.BootInfo) (datagateway.DataPlane, error) {
					close(started)
					<-release
					if tc.bootErr != nil {
						return nil, tc.bootErr
					}
					return plane, nil
				}
			}
			// A long hold: only the signal may end a failed run here.
			r := startRun(t, writeConfig(t), newPlane, time.Hour)
			<-started

			gotCfg := make(chan error, 1)
			go func() {
				_, err := r.client.GetConfig(context.Background(), &pb.GetConfigRequest{})
				gotCfg <- err
			}()
			r.signals <- syscall.SIGTERM
			select {
			case code := <-r.exit:
				t.Fatalf("run exited %d while boot was still blocked", code)
			case err := <-gotCfg:
				t.Fatalf("GetConfig returned while boot was still blocked: %v", err)
			case <-time.After(200 * time.Millisecond):
			}

			close(release)
			if code := r.waitExit(t); code != tc.wantExit {
				t.Errorf("exit = %d, want %d", code, tc.wantExit)
			}
			err := <-gotCfg
			if tc.bootErr == nil {
				if err != nil {
					t.Errorf("in-flight GetConfig: %v, want success", err)
				}
				if !plane.closed.Load() {
					t.Error("plane not closed on shutdown")
				}
				return
			}
			if st, _ := status.FromError(err); st.Code() != codes.Unavailable || !strings.Contains(st.Message(), "gateway boot failed: data gateway: data plane: plane init failed") {
				t.Errorf("in-flight GetConfig: %v, want Unavailable with the boot error", err)
			}
		})
	}
}

// A failed boot (config or plane) keeps serving the error instead of exiting
// at once, then exits 20 when the hold expires.
func TestRunBootFailureServesErrorUntilHold(t *testing.T) {
	failingPlane := func(duck.Settings) datagateway.DataPlaneFactory {
		return func(context.Context, datagateway.BootInfo) (datagateway.DataPlane, error) {
			return nil, errors.New("plane init failed")
		}
	}
	for _, tc := range []struct {
		name       string
		configPath string
		wantMsg    string
	}{
		{"plane error", writeConfig(t), "gateway boot failed: data gateway: data plane: plane init failed"},
		{"config error", filepath.Join(t.TempDir(), "missing.yaml"), "gateway boot failed: failed to read config file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const hold = 500 * time.Millisecond
			start := time.Now()
			r := startRun(t, tc.configPath, failingPlane, hold)
			_, err := r.client.GetConfig(context.Background(), &pb.GetConfigRequest{})
			if st, _ := status.FromError(err); st.Code() != codes.Unavailable || !strings.HasPrefix(st.Message(), tc.wantMsg) {
				t.Errorf("GetConfig: %v, want Unavailable %q...", err, tc.wantMsg)
			}
			if code := r.waitExit(t); code != exitApplication {
				t.Errorf("exit = %d, want %d", code, exitApplication)
			}
			if elapsed := time.Since(start); elapsed < hold {
				t.Errorf("exited after %s, want the boot error served for the %s hold", elapsed, hold)
			}
		})
	}
}
