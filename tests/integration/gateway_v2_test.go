package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/datuplet/datuplet/pkg/datagateway"
	"github.com/datuplet/datuplet/pkg/datagateway/backend"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/datuplet/datuplet/sdk/go"
	"github.com/stretchr/testify/require"
)

// TestGatewayV2BasicReadWrite tests basic read/write operations with gateway v2
func TestGatewayV2BasicReadWrite(t *testing.T) {
	// Skip if not in integration test mode
	if os.Getenv("INTEGRATION_TEST") == "" {
		t.Skip("Skipping integration test. Set INTEGRATION_TEST=1 to run.")
	}

	ctx := context.Background()

	// Create test backend configuration
	backendCfg := backend.Config{
		Type:            "minio",
		Endpoint:        getEnv("MINIO_ENDPOINT", "localhost:9000"),
		AccessKeyID:     getEnv("MINIO_ACCESS_KEY", "minioadmin"),
		SecretAccessKey: getEnv("MINIO_SECRET_KEY", "minioadmin"),
		UseSSL:          false,
		BucketName:      getEnv("MINIO_BUCKET", "datuplet-test"),
		Namespace:       "test",
	}

	// Create backend
	be, err := backend.NewStorageBackend(ctx, backendCfg)
	require.NoError(t, err)
	defer be.Close()

	// Create gateway configuration
	gatewayCfg := datagateway.Config{
		ExecutionID:   "test-execution-001",
		ComponentName: "test-component",
		Inputs:        map[string]string{},
		Outputs: map[string]datagateway.OutputConfig{
			"test_output": {Path: "test.test_output"},
		},
		ComponentCfg: []byte(`{}`),
		ChunkSize:    1024 * 1024, // 1MB
		Backend:      be,
	}

	// Start gateway v2 server
	server := datagateway.NewServerV2(gatewayCfg)

	// Start server in background
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Serve("localhost:50051")
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Ensure server stops
	defer server.Stop()

	// Connect SDK client
	os.Setenv("DATUPLET_GATEWAY_ADDR", "localhost:50051")
	client, err := sdk.New(ctx)
	require.NoError(t, err)
	defer client.Close()

	// Test CSV write
	t.Run("CSV Write", func(t *testing.T) {
		writer, err := client.OpenWriter(ctx, "test_output", sdk.WithFormat(pb.DataFormat_FORMAT_CSV))
		require.NoError(t, err)

		// Write CSV header and rows
		csvData := "id,name,price\n1,Widget,9.99\n2,Gadget,19.99\n"
		err = writer.Write(ctx, []byte(csvData))
		require.NoError(t, err)

		// Close writer
		result, err := writer.Close(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(2), result.TotalRows)
	})

	// Commit
	commitResult, err := client.Commit(ctx)
	require.NoError(t, err)
	require.True(t, commitResult.Success)
}

// TestGatewayV2FormatConversion tests format conversion capabilities
func TestGatewayV2FormatConversion(t *testing.T) {
	if os.Getenv("INTEGRATION_TEST") == "" {
		t.Skip("Skipping integration test. Set INTEGRATION_TEST=1 to run.")
	}

	ctx := context.Background()

	// Setup (similar to above test)
	backendCfg := backend.Config{
		Type:            "minio",
		Endpoint:        getEnv("MINIO_ENDPOINT", "localhost:9000"),
		AccessKeyID:     getEnv("MINIO_ACCESS_KEY", "minioadmin"),
		SecretAccessKey: getEnv("MINIO_SECRET_KEY", "minioadmin"),
		UseSSL:          false,
		BucketName:      getEnv("MINIO_BUCKET", "datuplet-test"),
		Namespace:       "test",
	}

	be, err := backend.NewStorageBackend(ctx, backendCfg)
	require.NoError(t, err)
	defer be.Close()

	gatewayCfg := datagateway.Config{
		ExecutionID:   "test-execution-002",
		ComponentName: "test-component",
		Inputs:        map[string]string{},
		Outputs: map[string]datagateway.OutputConfig{
			"test_json": {Path: "test.test_json"},
		},
		ComponentCfg: []byte(`{}`),
		ChunkSize:    1024 * 1024,
		Backend:      be,
	}

	server := datagateway.NewServerV2(gatewayCfg)
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Serve("localhost:50052")
	}()
	time.Sleep(100 * time.Millisecond)
	defer server.Stop()

	os.Setenv("DATUPLET_GATEWAY_ADDR", "localhost:50052")
	client, err := sdk.New(ctx)
	require.NoError(t, err)
	defer client.Close()

	// Test JSON write (gateway converts to Parquet internally)
	t.Run("JSON to Parquet", func(t *testing.T) {
		writer, err := client.OpenWriter(ctx, "test_json", sdk.WithFormat(pb.DataFormat_FORMAT_JSON))
		require.NoError(t, err)

		// Write JSON array
		jsonData := `[{"id":1,"name":"Widget","price":9.99},{"id":2,"name":"Gadget","price":19.99}]`
		err = writer.Write(ctx, []byte(jsonData))
		require.NoError(t, err)

		// Close writer
		result, err := writer.Close(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(2), result.TotalRows)
	})

	// Commit
	commitResult, err := client.Commit(ctx)
	require.NoError(t, err)
	require.True(t, commitResult.Success)
}

// TestGatewayV2Processors tests server-side processor operations (drop columns)
func TestGatewayV2Processors(t *testing.T) {
	if os.Getenv("INTEGRATION_TEST") == "" {
		t.Skip("Skipping integration test. Set INTEGRATION_TEST=1 to run.")
	}

	ctx := context.Background()

	// Setup
	backendCfg := backend.Config{
		Type:            "minio",
		Endpoint:        getEnv("MINIO_ENDPOINT", "localhost:9000"),
		AccessKeyID:     getEnv("MINIO_ACCESS_KEY", "minioadmin"),
		SecretAccessKey: getEnv("MINIO_SECRET_KEY", "minioadmin"),
		UseSSL:          false,
		BucketName:      getEnv("MINIO_BUCKET", "datuplet-test"),
		Namespace:       "test",
	}

	be, err := backend.NewStorageBackend(ctx, backendCfg)
	require.NoError(t, err)
	defer be.Close()

	// Create output with drop processor configured
	gatewayCfg := datagateway.Config{
		ExecutionID:   "test-execution-003",
		ComponentName: "test-component",
		Inputs:        map[string]string{},
		Outputs: map[string]datagateway.OutputConfig{
			"test_output": {
				Path: "test.test_processor_output",
				Processors: []datagateway.ProcessorConfig{
					{Type: "drop", Columns: []string{"price", "category"}},
				},
			},
		},
		ComponentCfg: []byte(`{}`),
		ChunkSize:    1024 * 1024,
		Backend:      be,
	}

	server := datagateway.NewServerV2(gatewayCfg)
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Serve("localhost:50053")
	}()
	time.Sleep(100 * time.Millisecond)
	defer server.Stop()

	os.Setenv("DATUPLET_GATEWAY_ADDR", "localhost:50053")
	client, err := sdk.New(ctx)
	require.NoError(t, err)
	defer client.Close()

	// Write data - the drop processor should remove price and category columns
	t.Run("Write With Drop Processor", func(t *testing.T) {
		writer, err := client.OpenWriter(ctx, "test_output", sdk.WithFormat(pb.DataFormat_FORMAT_CSV))
		require.NoError(t, err)

		// Input has 4 columns: id, name, price, category
		csvData := "id,name,price,category\n1,Widget,9.99,A\n2,Gadget,19.99,B\n3,Tool,29.99,A\n"
		err = writer.Write(ctx, []byte(csvData))
		require.NoError(t, err)

		result, err := writer.Close(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(3), result.TotalRows)

		// After drop processor, output should only have id and name columns
		// (price and category are dropped)
	})

	// Commit
	commitResult, err := client.Commit(ctx)
	require.NoError(t, err)
	require.True(t, commitResult.Success)
}

// getEnv returns environment variable value or default
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
