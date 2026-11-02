package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
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

	"github.com/datuplet/datuplet/pkg/datagateway"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// nopPlane is a data plane that only records Close.
type nopPlane struct{ closed atomic.Bool }

func (p *nopPlane) OpenTableWriter(context.Context, datagateway.WriterSpec) (datagateway.TableWriter, error) {
	return nil, errors.New("nopPlane")
}

func (p *nopPlane) OpenTableReader(context.Context, datagateway.ReaderSpec) (datagateway.TableReader, error) {
	return nil, errors.New("nopPlane")
}
func (p *nopPlane) Interrupt()   {}
func (p *nopPlane) Close() error { p.closed.Store(true); return nil }

// gatewayRun is runGatewayWith started in the background with test hooks.
type gatewayRun struct {
	signals chan os.Signal
	client  pb.DataGatewayClient
	exit    chan int
}

func startGateway(t *testing.T, configPath string, newPlane datagateway.DataPlaneFactory, hold time.Duration) *gatewayRun {
	t.Helper()
	r := &gatewayRun{signals: make(chan os.Signal, 1), exit: make(chan int, 1)}
	addrCh := make(chan net.Addr, 1)
	hooks := gatewayHooks{
		newPlane:        newPlane,
		signals:         r.signals,
		bootFailureHold: hold,
		httpAddr:        "127.0.0.1:0",
		listening:       func(a net.Addr) { addrCh <- a },
	}
	opts := gatewayOptions{configPath: configPath, addr: "127.0.0.1:0", mode: "gateway"}
	noEnv := func(string) string { return "" }
	go func() { r.exit <- runGatewayWith(opts, noEnv, hooks) }()
	var addr net.Addr
	select {
	case addr = <-addrCh:
	case code := <-r.exit:
		t.Fatalf("gateway exited %d before listening", code)
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
		t.Fatal("gateway did not exit")
		return -1
	}
}

func writeGatewayConfig(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(p, []byte("run_id: main-test-run\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// A SIGTERM that arrives while boot is blocked is handled once boot returns:
// a clean exit 0 after a successful boot (the in-flight GetConfig still
// succeeds and the plane is closed), 20 after a failed one.
func TestGatewaySIGTERMDuringBlockedBoot(t *testing.T) {
	for _, tc := range []struct {
		name     string
		bootErr  error
		wantExit int
	}{
		{"boot succeeds", nil, 0},
		{"boot fails", errors.New("plane init failed"), gatewayExitApplication},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			plane := &nopPlane{}
			newPlane := func(context.Context, datagateway.BootInfo) (datagateway.DataPlane, error) {
				close(started)
				<-release
				if tc.bootErr != nil {
					return nil, tc.bootErr
				}
				return plane, nil
			}
			// A long hold: only the signal may end a failed run here.
			r := startGateway(t, writeGatewayConfig(t), newPlane, time.Hour)
			<-started

			gotCfg := make(chan error, 1)
			go func() {
				_, err := r.client.GetConfig(context.Background(), &pb.GetConfigRequest{})
				gotCfg <- err
			}()
			r.signals <- syscall.SIGTERM
			select {
			case code := <-r.exit:
				t.Fatalf("gateway exited %d while boot was still blocked", code)
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
func TestGatewayBootFailureServesErrorUntilHold(t *testing.T) {
	failingPlane := func(context.Context, datagateway.BootInfo) (datagateway.DataPlane, error) {
		return nil, errors.New("plane init failed")
	}
	for _, tc := range []struct {
		name       string
		configPath string
		wantMsg    string
	}{
		{"plane error", writeGatewayConfig(t), "gateway boot failed: data gateway: data plane: plane init failed"},
		{"config error", filepath.Join(t.TempDir(), "missing.yaml"), "gateway boot failed: failed to read config file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const hold = 500 * time.Millisecond
			start := time.Now()
			r := startGateway(t, tc.configPath, failingPlane, hold)
			_, err := r.client.GetConfig(context.Background(), &pb.GetConfigRequest{})
			if st, _ := status.FromError(err); st.Code() != codes.Unavailable || !strings.HasPrefix(st.Message(), tc.wantMsg) {
				t.Errorf("GetConfig: %v, want Unavailable %q...", err, tc.wantMsg)
			}
			if code := r.waitExit(t); code != gatewayExitApplication {
				t.Errorf("exit = %d, want %d", code, gatewayExitApplication)
			}
			if elapsed := time.Since(start); elapsed < hold {
				t.Errorf("exited after %s, want the boot error served for the %s hold", elapsed, hold)
			}
		})
	}
}

// A successful boot answers GetConfig and shuts down cleanly on SIGTERM,
// closing the plane.
func TestGatewaySuccessfulBootAnswersGetConfig(t *testing.T) {
	plane := &nopPlane{}
	newPlane := func(context.Context, datagateway.BootInfo) (datagateway.DataPlane, error) { return plane, nil }
	r := startGateway(t, writeGatewayConfig(t), newPlane, time.Hour)
	if _, err := r.client.GetConfig(context.Background(), &pb.GetConfigRequest{}); err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	r.signals <- syscall.SIGTERM
	if code := r.waitExit(t); code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
	if !plane.closed.Load() {
		t.Error("plane not closed on shutdown")
	}
}

// A boot error that embeds the run token is served redacted over gRPC and
// HTTP, and the process still exits 20 after the hold.
func TestGatewayBootErrorOmitsRunToken(t *testing.T) {
	const sig = "c2lnbmF0dXJlLXNlY3JldC1zaWduYXR1cmU"
	const token = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJzZWNyZXQtcnVuLXRva2VuLXBheWxvYWQifQ." + sig
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpAddr := l.Addr().String()
	l.Close()

	addrCh := make(chan net.Addr, 1)
	hooks := gatewayHooks{
		signals:         make(chan os.Signal, 1),
		bootFailureHold: 1500 * time.Millisecond,
		httpAddr:        httpAddr,
		listening:       func(a net.Addr) { addrCh <- a },
		newServer: func(*datagateway.Config, datagateway.DataPlaneFactory) (*datagateway.ServerV2, error) {
			return nil, errors.New("boot failed with token " + token)
		},
	}
	opts := gatewayOptions{configPath: writeGatewayConfig(t), addr: "127.0.0.1:0", mode: "gateway", runTokenPath: tokenPath}
	exit := make(chan int, 1)
	go func() { exit <- runGatewayWith(opts, func(string) string { return "" }, hooks) }()
	addr := <-addrCh

	conn, err := grpc.NewClient(addr.String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = pb.NewDataGatewayClient(conn).GetConfig(context.Background(), &pb.GetConfigRequest{})
	if st, _ := status.FromError(err); st.Code() != codes.Unavailable {
		t.Fatalf("GetConfig: %v, want Unavailable", err)
	}
	if strings.Contains(err.Error(), token) || strings.Contains(err.Error(), sig) {
		t.Errorf("gRPC boot error leaks the token: %v", err)
	}
	if !strings.Contains(err.Error(), "boot failed with token") {
		t.Errorf("gRPC boot error lost its text: %v", err)
	}

	var resp *http.Response
	for i := 0; i < 100; i++ {
		resp, err = http.Get("http://" + httpAddr + "/data/read/x")
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("HTTP: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("HTTP status = %d, want 503", resp.StatusCode)
	}
	if strings.Contains(string(body), token) || strings.Contains(string(body), sig) {
		t.Errorf("HTTP boot error leaks the token: %s", body)
	}

	select {
	case code := <-exit:
		if code != gatewayExitApplication {
			t.Errorf("exit = %d, want %d", code, gatewayExitApplication)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("gateway did not exit after the hold")
	}
}
