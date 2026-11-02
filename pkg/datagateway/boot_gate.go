package datagateway

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// BootGate serves the gateway's gRPC and HTTP listeners while the server is
// still booting. The component container starts with the sidecar process and
// its SDK makes a single GetConfig call, so the gateway listens first and
// holds every request until boot ends: on Ready requests reach the ServerV2,
// on Fail they get the boot error (gRPC Unavailable, HTTP 503).
type BootGate struct {
	done chan struct{}
	once sync.Once
	err  error // set before done is closed
	impl *gatedService
	data http.Handler // the ServerV2's HTTP mux; set before done is closed

	grpcServer *grpc.Server
	httpServer *http.Server
	health     *health.Server // NOT_SERVING until Ready
}

// gatedService is what the gRPC server dispatches DataGateway RPCs to. Its
// embedded *ServerV2 is nil until Ready; the interceptors let no call reach
// it before then.
type gatedService struct{ *ServerV2 }

// NewBootGate builds the gRPC and HTTP servers with every request gated.
func NewBootGate() *BootGate {
	g := &BootGate{done: make(chan struct{}), impl: &gatedService{}}
	g.grpcServer = grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if isHealthMethod(info.FullMethod) {
				return handler(ctx, req)
			}
			if err := g.wait(ctx); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		}),
		grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if isHealthMethod(info.FullMethod) {
				return handler(srv, ss)
			}
			if err := g.wait(ss.Context()); err != nil {
				return err
			}
			return handler(srv, ss)
		}),
	)
	// RegisterService, not pb.RegisterDataGatewayServer: the latter probes
	// the embedded Unimplemented server, which is reached through the still
	// nil *ServerV2.
	g.grpcServer.RegisterService(&pb.DataGateway_ServiceDesc, g.impl)
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(g.grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	g.health = healthServer
	g.httpServer = &http.Server{Handler: http.HandlerFunc(g.serveHTTPRequest)}
	return g
}

// isHealthMethod reports whether the RPC is the standard health service,
// which answers immediately (NOT_SERVING until Ready) instead of being gated.
func isHealthMethod(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/grpc.health.v1.Health/")
}

// wait blocks until boot ends or ctx is done; it returns the gRPC status a
// gated call fails with, or nil once the server is ready.
func (g *BootGate) wait(ctx context.Context) error {
	select {
	case <-g.done:
	case <-ctx.Done():
		select {
		case <-g.done:
		default:
			return status.FromContextError(ctx.Err()).Err()
		}
	}
	if g.err != nil {
		return status.Error(codes.Unavailable, "gateway boot failed: "+g.err.Error())
	}
	return nil
}

func (g *BootGate) serveHTTPRequest(w http.ResponseWriter, r *http.Request) {
	select {
	case <-g.done:
	case <-r.Context().Done():
		writeHTTPError(w, http.StatusServiceUnavailable, "request ended while the gateway was booting", "GATEWAY_BOOTING")
		return
	}
	if g.err != nil {
		writeHTTPError(w, http.StatusServiceUnavailable, "gateway boot failed: "+g.err.Error(), "GATEWAY_BOOT_FAILED")
		return
	}
	g.data.ServeHTTP(w, r)
}

// Ready opens the gate onto s: gated and later requests are served by s, and
// s.Close (or the cancel annotation) stops both servers. Only the first of
// Ready / Fail / Stop takes effect.
func (g *BootGate) Ready(s *ServerV2) {
	g.once.Do(func() {
		g.impl.ServerV2 = s
		g.data = s.httpHandler()
		s.mu.Lock()
		s.grpcServer = g.grpcServer
		s.httpServer = g.httpServer
		s.mu.Unlock()
		s.stopServersOnCancel(g.grpcServer)
		g.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
		close(g.done)
	})
}

// Fail opens the gate with a boot error: every gated and later request fails
// with it. err must already be safe to show the component (SanitizeBootError).
func (g *BootGate) Fail(err error) {
	if err == nil {
		err = errors.New("unknown boot error")
	}
	g.once.Do(func() {
		g.err = err
		close(g.done)
	})
}

// Stop fails any still-gated request and stops both servers (bounded). Use
// it when boot failed; after Ready, ServerV2.Close stops them instead.
func (g *BootGate) Stop() {
	g.Fail(errors.New("gateway stopped before boot completed"))
	stopGRPC(g.grpcServer, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := g.httpServer.Shutdown(ctx); err != nil {
		log.Printf("data gateway: HTTP server shutdown: %v", err)
	}
}

// ServeGRPC serves the control plane on lis until a graceful stop (nil).
func (g *BootGate) ServeGRPC(lis net.Listener) error {
	log.Printf("Data Gateway v2 listening on %s (%s)", lis.Addr(), GatewayBuildInfo())
	if DebugEnabled() {
		log.Printf("DBG gateway debug logging enabled via DATUPLET_GATEWAY_DEBUG")
	}
	if err := g.grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

// ServeDataHTTP serves the HTTP data plane on lis until shutdown (nil).
func (g *BootGate) ServeDataHTTP(lis net.Listener) error {
	log.Printf("Data Gateway HTTP server listening on %s", lis.Addr())
	if err := g.httpServer.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// stopGRPC stops srv gracefully, forcing the stop after timeout.
func stopGRPC(srv *grpc.Server, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		log.Printf("Warning: gRPC graceful stop timed out, forcing stop")
		srv.Stop()
	}
}

// SanitizeBootError returns err with the run token removed, so the boot
// error can be served to the component (which must never see the token) and
// logged. None of today's boot errors embed the token; this is the backstop.
// Secret-resolution errors carry only secret names and paths, never values.
func SanitizeBootError(err error, runTokenPath string) error {
	if err == nil || runTokenPath == "" {
		return err
	}
	raw, readErr := readBoundedFile(runTokenPath)
	if readErr != nil {
		return err
	}
	token := strings.TrimSpace(string(raw))
	msg := err.Error()
	redacted := redactToken(msg, token)
	if redacted == msg {
		return err
	}
	return errors.New(redacted)
}

// redactToken removes token, and each of its dot-separated JWT segments long
// enough to be distinctive, from msg.
func redactToken(msg, token string) string {
	const minLen = 16
	if len(token) >= minLen {
		msg = strings.ReplaceAll(msg, token, "[REDACTED]")
	}
	for _, seg := range strings.Split(token, ".") {
		if len(seg) >= minLen {
			msg = strings.ReplaceAll(msg, seg, "[REDACTED]")
		}
	}
	return msg
}
