package datagateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// gateHarness serves a BootGate on loopback listeners, as the binary does
// before boot.
type gateHarness struct {
	gate     *BootGate
	grpcAddr string
	httpAddr string
	client   pb.DataGatewayClient
	served   chan error
}

func newGateHarness(t *testing.T) *gateHarness {
	t.Helper()
	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := &gateHarness{
		gate:     NewBootGate(),
		grpcAddr: grpcLis.Addr().String(),
		httpAddr: httpLis.Addr().String(),
		served:   make(chan error, 2),
	}
	go func() { h.served <- h.gate.ServeGRPC(grpcLis) }()
	go func() { h.served <- h.gate.ServeDataHTTP(httpLis) }()
	conn, err := grpc.NewClient(h.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	h.client = pb.NewDataGatewayClient(conn)
	return h
}

// blockingFactory returns a factory that blocks until release is closed,
// then returns plane (or err).
func blockingFactory(started chan<- struct{}, release <-chan struct{}, plane DataPlane, err error) DataPlaneFactory {
	return func(context.Context, BootInfo) (DataPlane, error) {
		close(started)
		<-release
		return plane, err
	}
}

// bootAsync runs NewServerV2 and opens the gate the way the binary does.
func bootAsync(h *gateHarness, cfg *Config, factory DataPlaneFactory) <-chan *ServerV2 {
	out := make(chan *ServerV2, 1)
	go func() {
		s, err := NewServerV2(cfg, factory)
		if err != nil {
			h.gate.Fail(SanitizeBootError(err, cfg.RunTokenPath))
			out <- nil
			return
		}
		h.gate.Ready(s)
		out <- s
	}()
	return out
}

func decodeErrorEnvelope(t *testing.T, resp *http.Response) httpErrorResponse {
	t.Helper()
	var env httpErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	return env
}

// (a) The ports accept while the plane factory blocks; a GetConfig (and an
// HTTP request) sent then waits and succeeds once boot completes.
func TestBootGate_RequestDuringBootWaitsThenSucceeds(t *testing.T) {
	h := newGateHarness(t)
	started, release := make(chan struct{}), make(chan struct{})
	booted := bootAsync(h, &Config{RunID: "gate-run", ComponentName: "c", HTTPAddr: h.httpAddr},
		blockingFactory(started, release, newFakeDataPlane(), nil))
	<-started

	for _, addr := range []string{h.grpcAddr, h.httpAddr} {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("dial %s during boot: %v", addr, err)
		}
		c.Close()
	}

	type cfgResult struct {
		cfg *pb.ComponentConfig
		err error
	}
	gotCfg := make(chan cfgResult, 1)
	go func() {
		c, err := h.client.GetConfig(context.Background(), &pb.GetConfigRequest{})
		gotCfg <- cfgResult{c, err}
	}()
	gotHealth := make(chan int, 1)
	go func() {
		resp, err := http.Get("http://" + h.httpAddr + "/health")
		if err != nil {
			gotHealth <- -1
			return
		}
		resp.Body.Close()
		gotHealth <- resp.StatusCode
	}()

	select {
	case r := <-gotCfg:
		t.Fatalf("GetConfig returned before boot completed: %+v", r)
	case code := <-gotHealth:
		t.Fatalf("HTTP request returned before boot completed: %d", code)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	s := <-booted
	if s == nil {
		t.Fatal("boot failed")
	}
	t.Cleanup(func() { s.Close() })

	r := <-gotCfg
	if r.err != nil {
		t.Fatalf("GetConfig after boot: %v", r.err)
	}
	if r.cfg.ExecutionId != "gate-run" {
		t.Errorf("ExecutionId = %q, want gate-run", r.cfg.ExecutionId)
	}
	if code := <-gotHealth; code != http.StatusOK {
		t.Errorf("/health after boot = %d, want 200", code)
	}

	// Close stops both servers the gate serves.
	s.Close()
	for i := 0; i < 2; i++ {
		select {
		case err := <-h.served:
			if err != nil {
				t.Errorf("serve returned %v after Close, want nil", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("servers still running after Close")
		}
	}
}

// (b) A plane factory error is served as the boot error: gRPC Unavailable
// (unary and stream), HTTP 503 with the error envelope.
func TestBootGate_FactoryErrorServedAsBootError(t *testing.T) {
	h := newGateHarness(t)
	started, release := make(chan struct{}), make(chan struct{})
	booted := bootAsync(h, &Config{RunID: "gate-run", HTTPAddr: h.httpAddr},
		blockingFactory(started, release, nil, errors.New("duckdb: LOAD iceberg failed")))
	<-started

	pending := make(chan error, 1)
	go func() {
		_, err := h.client.GetConfig(context.Background(), &pb.GetConfigRequest{})
		pending <- err
	}()
	close(release)
	if s := <-booted; s != nil {
		t.Fatal("boot succeeded, want factory error")
	}

	wantMsg := "gateway boot failed: data gateway: data plane: duckdb: LOAD iceberg failed"
	checkUnavailable := func(name string, err error) {
		t.Helper()
		st, _ := status.FromError(err)
		if st.Code() != codes.Unavailable || st.Message() != wantMsg {
			t.Errorf("%s: got %v / %q, want Unavailable / %q", name, st.Code(), st.Message(), wantMsg)
		}
	}
	checkUnavailable("GetConfig in flight during boot", <-pending)
	_, err := h.client.GetConfig(context.Background(), &pb.GetConfigRequest{})
	checkUnavailable("GetConfig after boot", err)

	stream, err := h.client.ReadChunk(context.Background(), &pb.ReadChunkRequest{ReaderId: "r1"})
	if err == nil {
		_, err = stream.Recv()
	}
	checkUnavailable("ReadChunk stream", err)

	resp, err := http.Post("http://"+h.httpAddr+"/data/write/w1", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("HTTP status = %d, want 503", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if env := decodeErrorEnvelope(t, resp); env.Error != wantMsg || env.Code != "GATEWAY_BOOT_FAILED" {
		t.Errorf("envelope = %+v, want error %q code GATEWAY_BOOT_FAILED", env, wantMsg)
	}

	h.gate.Stop()
	for i := 0; i < 2; i++ {
		if err := <-h.served; err != nil {
			t.Errorf("serve after Stop: %v", err)
		}
	}
}

// (c) A boot error from a bad run token never carries the token to the
// component.
func TestBootGate_BadRunTokenErrorDoesNotLeakToken(t *testing.T) {
	const kid, runID = "test-key-1", "run-gate-token"
	legit, attacker := rfc017GenKey(t), rfc017GenKey(t)
	jwksBody := rfc017MakeJWKS(kid, &legit.PublicKey)
	jwksSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(jwksBody) //nolint:errcheck
	}))
	defer jwksSrv.Close()

	cases := map[string]struct {
		token string
		runID string // RUN_ID env
	}{
		"bad signature":   {rfc017MintToken(t, attacker, kid, runID, "wh", "proj"), runID},
		"run_id mismatch": {rfc017MintToken(t, legit, kid, runID, "wh", "proj"), "some-other-run"},
		"malformed":       {"eyJhbGciOiJSUzI1NiJ9.not-a-valid-payload-but-long-enough.c2lnbmF0dXJlLXNlY3JldA", runID},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tokenFile := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenFile, []byte(tc.token+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("RUN_ID", tc.runID)
			h := newGateHarness(t)
			booted := bootAsync(h, &Config{
				HTTPAddr:           h.httpAddr,
				RunTokenPath:       tokenFile,
				PipelineAPIJWKSURL: jwksSrv.URL,
			}, fakeFactory(newFakeDataPlane()))
			if s := <-booted; s != nil {
				t.Fatal("boot succeeded with a bad run token")
			}
			_, err := h.client.GetConfig(context.Background(), &pb.GetConfigRequest{})
			st, _ := status.FromError(err)
			if st.Code() != codes.Unavailable || !strings.Contains(st.Message(), "run-token validation failed") {
				t.Fatalf("GetConfig: %v / %q, want Unavailable run-token validation failure", st.Code(), st.Message())
			}
			assertNoToken(t, st.Message(), tc.token)

			resp, err := http.Get("http://" + h.httpAddr + "/health")
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			assertNoToken(t, string(body), tc.token)
			h.gate.Stop()
		})
	}
}

func assertNoToken(t *testing.T, msg, token string) {
	t.Helper()
	if strings.Contains(msg, token) {
		t.Fatalf("boot error contains the run token: %q", msg)
	}
	for _, seg := range strings.Split(token, ".") {
		if len(seg) >= 16 && strings.Contains(msg, seg) {
			t.Fatalf("boot error contains run-token segment %q: %q", seg, msg)
		}
	}
}

// SanitizeBootError strips the run token and its segments from an error that
// does carry them (the backstop for a future error that quotes the token).
func TestSanitizeBootError_RedactsToken(t *testing.T) {
	const token = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJydW4tMTIzIn0.c2lnbmF0dXJlLWJ5dGVzLXNlY3JldA"
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	err := SanitizeBootError(errors.New("parse "+token+" failed; sig "+parts[2]), tokenFile)
	assertNoToken(t, err.Error(), token)
	if !strings.Contains(err.Error(), "[REDACTED]") {
		t.Errorf("err = %q, want redaction marker", err)
	}

	plain := errors.New("jwks fetch failed")
	if got := SanitizeBootError(plain, tokenFile); got != plain {
		t.Errorf("an error without the token must pass through unchanged, got %v", got)
	}
	if got := SanitizeBootError(plain, ""); got != plain {
		t.Errorf("no token path: got %v", got)
	}
}

// (d) A gated call honours its own deadline while boot is still running.
func TestBootGate_RequestDeadlineHonouredWhileBooting(t *testing.T) {
	h := newGateHarness(t)
	t.Cleanup(h.gate.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := h.client.GetConfig(ctx, &pb.GetConfigRequest{})
	if code := status.Code(err); code != codes.DeadlineExceeded {
		t.Fatalf("GetConfig: %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("GetConfig took %s, want about the 150ms deadline", elapsed)
	}

	sctx, scancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer scancel()
	stream, err := h.client.ReadChunk(sctx, &pb.ReadChunkRequest{ReaderId: "r1"})
	if err == nil {
		_, err = stream.Recv()
	}
	if code := status.Code(err); code != codes.DeadlineExceeded {
		t.Errorf("ReadChunk: %v, want DeadlineExceeded", err)
	}

	hctx, hcancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer hcancel()
	req, _ := http.NewRequestWithContext(hctx, http.MethodGet, "http://"+h.httpAddr+"/health", nil)
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
		t.Errorf("HTTP request during boot returned %d before its deadline", resp.StatusCode)
	}
}

// A cancel annotation present at boot stops the servers the gate serves
// once the server is ready.
func TestBootGate_CancelAnnotationStopsServers(t *testing.T) {
	annotations := filepath.Join(t.TempDir(), "annotations")
	if err := os.WriteFile(annotations, []byte("datuplet.io/cancel=\"true\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	h := newGateHarness(t)
	s := <-bootAsync(h, &Config{
		HTTPAddr:           h.httpAddr,
		PodAnnotationsPath: annotations,
		CancelPollInterval: 20 * time.Millisecond,
	}, fakeFactory(newFakeDataPlane()))
	if s == nil {
		t.Fatal("boot failed")
	}
	t.Cleanup(func() { s.Close() })
	for i := 0; i < 2; i++ {
		select {
		case err := <-h.served:
			if err != nil {
				t.Errorf("serve after cancel: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("servers still running after the cancel annotation")
		}
	}
}

// ServeWithHTTP reports an HTTP bind failure instead of only logging it.
func TestServeWithHTTP_HTTPBindFailureReturned(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	s := mustNewServerV2(t, &Config{RunID: "r", HTTPAddr: busy.Addr().String()})
	defer s.Close()
	if err := s.ServeWithHTTP("127.0.0.1:0"); err == nil || !strings.Contains(err.Error(), "(HTTP)") {
		t.Fatalf("ServeWithHTTP with a busy HTTP port: %v, want an HTTP listen error", err)
	}
}

// The gRPC health service is NOT_SERVING until the gate is ready, SERVING
// after Ready, and stays NOT_SERVING after Fail.
func TestHealthNotServingUntilReady(t *testing.T) {
	check := func(t *testing.T, h *gateHarness) grpc_health_v1.HealthCheckResponse_ServingStatus {
		t.Helper()
		conn, err := grpc.NewClient(h.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		if err != nil {
			t.Fatalf("health Check: %v", err)
		}
		return resp.GetStatus()
	}

	t.Run("ready", func(t *testing.T) {
		h := newGateHarness(t)
		t.Cleanup(h.gate.Stop)
		if got := check(t, h); got != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
			t.Errorf("before Ready: %v, want NOT_SERVING", got)
		}
		s, err := NewServerV2(&Config{RunID: "gate-run", HTTPAddr: h.httpAddr}, func(context.Context, BootInfo) (DataPlane, error) {
			return newFakeDataPlane(), nil
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		h.gate.Ready(s)
		if got := check(t, h); got != grpc_health_v1.HealthCheckResponse_SERVING {
			t.Errorf("after Ready: %v, want SERVING", got)
		}
	})

	t.Run("fail", func(t *testing.T) {
		h := newGateHarness(t)
		t.Cleanup(h.gate.Stop)
		h.gate.Fail(errors.New("boom"))
		if got := check(t, h); got != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
			t.Errorf("after Fail: %v, want NOT_SERVING", got)
		}
	})
}

// wait returns the context error promptly when the ctx is cancelled while the
// gate is closed.
func TestGateWaitReturnsOnCancel(t *testing.T) {
	g := NewBootGate()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- g.wait(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("wait = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not return within 1s of a cancelled ctx")
	}
}

// An HTTP request whose context is cancelled while the gate is closed returns
// from the handler, and the gate stays closed.
func TestGateHTTPReturnsOnCancel(t *testing.T) {
	g := NewBootGate()
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/health", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		g.serveHTTPRequest(rec, req)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("handler returned before cancel while the gate was closed")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not return within 1s of cancel")
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	select {
	case <-g.done:
		t.Error("gate opened by a cancelled request")
	default:
	}
}
