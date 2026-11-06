package sdk

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

type configOnlyGateway struct {
	pb.UnimplementedDataGatewayServer
}

func (configOnlyGateway) GetConfig(context.Context, *pb.GetConfigRequest) (*pb.ComponentConfig, error) {
	return &pb.ComponentConfig{}, nil
}

// The component container can start before its gateway sidecar listens;
// New must wait for the gateway instead of failing on connection refused.
func TestNew_WaitsForGatewayToListen(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	t.Setenv("DATUPLET_GATEWAY_ADDR", addr)

	srv := grpc.NewServer()
	pb.RegisterDataGatewayServer(srv, configOnlyGateway{})
	defer srv.Stop()
	go func() {
		time.Sleep(500 * time.Millisecond)
		l, err := net.Listen("tcp", addr)
		if err != nil {
			t.Errorf("late listen: %v", err)
			return
		}
		_ = srv.Serve(l)
	}()

	c, err := New(context.Background())
	if err != nil {
		t.Fatalf("New before the gateway listens: %v", err)
	}
	c.Close()
}
