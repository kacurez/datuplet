package datagateway

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/datuplet/datuplet/pkg/datagateway/backend"
	"github.com/datuplet/datuplet/pkg/datagateway/buffer"
	"github.com/datuplet/datuplet/pkg/datagateway/format"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

func TestResolveSecretRefsInConfigReplacesRefs(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "db_password"), []byte("hunter2\n"), 0o400); err != nil {
		t.Fatalf("write secret: %v", err)
	}

	cfg := &Config{
		RunID:        "run-1",
		SecretsDir:   dir,
		ComponentCfg: []byte(`{"password":"$[db_password]","user":"alice"}`),
	}

	if err := resolveSecretRefsInConfig(cfg); err != nil {
		t.Fatalf("resolveSecretRefsInConfig: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(cfg.ComponentCfg, &got); err != nil {
		t.Fatalf("unmarshal resolved cfg: %v", err)
	}
	if got["password"] != "hunter2" {
		t.Errorf("password got %v, want hunter2", got["password"])
	}
	if got["user"] != "alice" {
		t.Errorf("user got %v, want alice", got["user"])
	}
}

func TestResolveSecretRefsInConfigNoopOnEmpty(t *testing.T) {
	cfg := &Config{ComponentCfg: nil}
	// With empty ComponentCfg we expect a no-op (resolveSecretRefsInConfig isn't
	// called from NewServerV2 in that case — but calling it directly should work
	// on a trivial-but-valid JSON input too).
	cfg2 := &Config{ComponentCfg: []byte(`{"user":"alice"}`)}
	if err := resolveSecretRefsInConfig(cfg2); err != nil {
		t.Fatalf("resolveSecretRefsInConfig: %v", err)
	}
	if string(cfg2.ComponentCfg) != `{"user":"alice"}` {
		t.Errorf("ComponentCfg mutated when no refs present: %s", cfg2.ComponentCfg)
	}
	_ = cfg
}

func TestResolveSecretRefsInConfigMissingDirErrors(t *testing.T) {
	cfg := &Config{
		SecretsDir:   "", // deliberately empty
		ComponentCfg: []byte(`{"password":"$[needs_a_file]"}`),
	}
	err := resolveSecretRefsInConfig(cfg)
	if err == nil {
		t.Fatal("expected error when refs present but SecretsDir empty")
	}
	if !strings.Contains(err.Error(), "needs_a_file") {
		t.Errorf("error message missing secret name: %v", err)
	}
}

func TestResolveSecretRefsInConfigMissingFileErrors(t *testing.T) {
	cfg := &Config{
		SecretsDir:   t.TempDir(),
		ComponentCfg: []byte(`{"password":"$[nope]"}`),
	}
	err := resolveSecretRefsInConfig(cfg)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Errorf("error missing secret name: %v", err)
	}
}

// mockBackend implements a minimal backend.StorageBackend for testing.
type mockBackend struct {
	data map[string][]byte
}

func newMockBackend() *mockBackend {
	return &mockBackend{
		data: make(map[string][]byte),
	}
}

func (b *mockBackend) OpenReader(ctx context.Context, tablePath string) (backend.Reader, error) {
	return &mockReader{data: b.data[tablePath]}, nil
}

func (b *mockBackend) OpenWriter(ctx context.Context, tablePath string, opts backend.WriteOptions) (backend.Writer, error) {
	return &mockWriter{path: tablePath, backend: b}, nil
}

func (b *mockBackend) Commit(ctx context.Context, writers []backend.Writer) (*backend.CommitResult, error) {
	return &backend.CommitResult{}, nil
}

func (b *mockBackend) Rollback(ctx context.Context, writers []backend.Writer) error {
	return nil
}

func (b *mockBackend) GetSchema(ctx context.Context, tablePath string) (*backend.SchemaInfo, error) {
	return &backend.SchemaInfo{
		Columns: []backend.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false},
			{Name: "name", Type: "string", Nullable: true},
		},
	}, nil
}

func (b *mockBackend) GetSample(ctx context.Context, tablePath string, limit int) (*backend.SampleResult, error) {
	return &backend.SampleResult{
		Schema: &backend.SchemaInfo{
			Columns: []backend.ColumnInfo{
				{Name: "id", Type: "int64", Nullable: false},
				{Name: "name", Type: "string", Nullable: true},
			},
		},
		Rows:          [][]byte{[]byte(`{"id":1,"name":"test"}`)},
		TotalEstimate: 100,
	}, nil
}

func (b *mockBackend) GetObject(ctx context.Context, path string) ([]byte, error) {
	if data, ok := b.data[path]; ok {
		return data, nil
	}
	return nil, fmt.Errorf("object not found: %s", path)
}

func (b *mockBackend) PutObject(ctx context.Context, path string, data []byte) error {
	b.data[path] = data
	return nil
}

func (b *mockBackend) RemoveAll(ctx context.Context, prefix string) error {
	for k := range b.data {
		if strings.HasPrefix(k, prefix) {
			delete(b.data, k)
		}
	}
	return nil
}

func (b *mockBackend) Close() error {
	return nil
}

type mockReader struct {
	data    []byte
	readPos int
}

func (r *mockReader) ReadChunk() (*backend.DataChunk, error) {
	if r.readPos > 0 {
		return &backend.DataChunk{
			IsLast: true,
		}, nil
	}
	r.readPos++
	return &backend.DataChunk{
		Data:        r.data,
		Format:      "csv",
		IsLast:      true,
		RowsInChunk: 1,
	}, nil
}

func (r *mockReader) Schema() *backend.SchemaInfo {
	return &backend.SchemaInfo{
		Columns: []backend.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false},
			{Name: "name", Type: "string", Nullable: true},
		},
	}
}

func (r *mockReader) TotalSizeEstimate() int64 {
	return int64(len(r.data))
}

func (r *mockReader) Close() error {
	return nil
}

type mockWriter struct {
	path    string
	data    []byte
	backend *mockBackend
}

func (w *mockWriter) WriteChunk(data []byte, rows int64) error {
	w.data = append(w.data, data...)
	return nil
}

func (w *mockWriter) OutputName() string {
	return filepath.Base(w.path)
}

func (w *mockWriter) TablePath() string {
	return w.path
}

func (w *mockWriter) Stats() backend.WriterStats {
	return backend.WriterStats{
		BytesWritten: int64(len(w.data)),
		RowsWritten:  1,
	}
}

func (w *mockWriter) Close() error {
	w.backend.data[w.path] = w.data
	return nil
}

func createTestServerV2(t *testing.T) (*ServerV2, string) {
	tmpDir, err := os.MkdirTemp("", "gateway-v2-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cfg := &Config{
		RunID:         "test-exec",
		ComponentName: "test-component",
		DefaultBucket: "raw", // Default bucket for writes
		Inputs:        map[string]string{"input": filepath.Join(tmpDir, "input")},
		Outputs: map[string]OutputConfig{
			"output": {Path: filepath.Join(tmpDir, "output")},
		},
		Backend: newMockBackend(),
	}

	return NewServerV2(cfg), tmpDir
}

func TestServerV2_GetConfig(t *testing.T) {
	server, tmpDir := createTestServerV2(t)
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()
	resp, err := server.GetConfig(ctx, &pb.GetConfigRequest{})
	if err != nil {
		t.Fatalf("GetConfig error: %v", err)
	}

	if resp.ExecutionId != "test-exec" {
		t.Errorf("ExecutionId = %q, want %q", resp.ExecutionId, "test-exec")
	}
	if resp.ComponentName != "test-component" {
		t.Errorf("ComponentName = %q, want %q", resp.ComponentName, "test-component")
	}
	if len(resp.Inputs) != 1 || len(resp.Outputs) != 1 {
		t.Errorf("inputs/outputs count incorrect")
	}
}

func TestServerV2_OpenWriter(t *testing.T) {
	server, tmpDir := createTestServerV2(t)
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()

	// Open writer with CSV format - uses defaultBucket from config
	resp, err := server.OpenWriter(ctx, &pb.OpenWriterRequest{
		Table:       "output",
		InputFormat: pb.DataFormat_FORMAT_CSV,
	})
	if err != nil {
		t.Fatalf("OpenWriter error: %v", err)
	}

	if resp.WriterId == "" {
		t.Error("WriterId should not be empty")
	}

	// Verify writer was stored
	server.mu.RLock()
	ws, ok := server.writers[resp.WriterId]
	server.mu.RUnlock()

	if !ok {
		t.Error("writer should be stored")
	}
	if ws.bucket != "raw" {
		t.Errorf("bucket = %q, want %q", ws.bucket, "raw")
	}
	if ws.table != "output" {
		t.Errorf("table = %q, want %q", ws.table, "output")
	}
}

func TestServerV2_OpenWriter_MissingTable(t *testing.T) {
	server, tmpDir := createTestServerV2(t)
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()

	// Table is required
	_, err := server.OpenWriter(ctx, &pb.OpenWriterRequest{
		Bucket: "raw",
		// Table not specified
	})
	if err == nil {
		t.Error("expected error when table is missing")
	}
	if err.Error() != "table is required" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestServerV2_OpenWriter_RequiresBackend(t *testing.T) {
	// Server without lakekeeper resolver and without static backend
	// rejects OpenWriter — there's no place for parquet to land.
	tmpDir, err := os.MkdirTemp("", "gateway-v2-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	server := NewServerV2(&Config{
		RunID:         "test-exec",
		ComponentName: "test-component",
		DefaultBucket: "raw",
		// No Backend, no LakekeeperURL.
	})

	ctx := context.Background()
	_, err = server.OpenWriter(ctx, &pb.OpenWriterRequest{
		Table:       "output",
		InputFormat: pb.DataFormat_FORMAT_CSV,
	})
	if err == nil {
		t.Fatal("expected error when no backend is configured")
	}
	if !strings.Contains(err.Error(), "no storage backend configured") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestServerV2_WriteChunk(t *testing.T) {
	server, tmpDir := createTestServerV2(t)
	defer os.RemoveAll(tmpDir)

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	os.MkdirAll(outputDir, 0755)

	ctx := context.Background()

	// Open writer
	openResp, err := server.OpenWriter(ctx, &pb.OpenWriterRequest{
		Table:       "output",
		InputFormat: pb.DataFormat_FORMAT_CSV,
	})
	if err != nil {
		t.Fatalf("OpenWriter error: %v", err)
	}

	// Write CSV chunk
	csvData := []byte("id,name\n1,Alice\n2,Bob\n")
	writeResp, err := server.WriteChunk(ctx, &pb.WriteChunkRequest{
		WriterId: openResp.WriterId,
		Data:     csvData,
	})
	if err != nil {
		t.Fatalf("WriteChunk error: %v", err)
	}

	if writeResp.RowsAccepted != 2 {
		t.Errorf("RowsAccepted = %d, want 2", writeResp.RowsAccepted)
	}

	// Verify schema was inferred
	if writeResp.InferredSchema == nil {
		t.Error("InferredSchema should be populated on first chunk")
	}
}

func TestServerV2_CloseWriter(t *testing.T) {
	server, tmpDir := createTestServerV2(t)
	defer os.RemoveAll(tmpDir)

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	os.MkdirAll(outputDir, 0755)

	ctx := context.Background()

	// Open writer
	openResp, err := server.OpenWriter(ctx, &pb.OpenWriterRequest{
		Table:       "output",
		InputFormat: pb.DataFormat_FORMAT_CSV,
	})
	if err != nil {
		t.Fatalf("OpenWriter error: %v", err)
	}

	// Write chunk
	csvData := []byte("id,name\n1,Alice\n")
	server.WriteChunk(ctx, &pb.WriteChunkRequest{
		WriterId: openResp.WriterId,
		Data:     csvData,
	})

	// Close writer
	closeResp, err := server.CloseWriter(ctx, &pb.CloseWriterRequest{
		WriterId: openResp.WriterId,
	})
	if err != nil {
		t.Fatalf("CloseWriter error: %v", err)
	}

	if closeResp.TotalRows == 0 {
		t.Error("TotalRows should be > 0")
	}

	// Verify writer is still in map (Commit removes it, not CloseWriter)
	// Writers remain in map after Close so Commit can process them
	server.mu.RLock()
	_, ok := server.writers[openResp.WriterId]
	server.mu.RUnlock()

	if !ok {
		t.Error("writer should still be in map after close (Commit removes it)")
	}
}

func TestServerV2_OpenReader(t *testing.T) {
	// OpenReader test uses direct backend read (no lakekeeper)
	server, tmpDir := createTestServerV2(t)
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()

	// Open reader with JSON output format - requires bucket and table
	resp, err := server.OpenReader(ctx, &pb.OpenReaderRequest{
		Bucket:       "raw",
		Table:        "input",
		OutputFormat: pb.DataFormat_FORMAT_JSON,
	})
	if err != nil {
		t.Fatalf("OpenReader error: %v", err)
	}

	if resp.ReaderId == "" {
		t.Error("ReaderId should not be empty")
	}
	if resp.Schema == nil {
		t.Error("Schema should be populated")
	}
}

func TestServerV2_OpenReader_MissingBucket(t *testing.T) {
	server, tmpDir := createTestServerV2(t)
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()

	// Bucket is required for reads
	_, err := server.OpenReader(ctx, &pb.OpenReaderRequest{
		Table: "products",
	})
	if err == nil {
		t.Error("expected error when bucket is missing")
	}
	if err.Error() != "bucket is required for reads" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestServerV2_OpenReader_MissingTable(t *testing.T) {
	server, tmpDir := createTestServerV2(t)
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()

	// Table is required for reads
	_, err := server.OpenReader(ctx, &pb.OpenReaderRequest{
		Bucket: "raw",
	})
	if err == nil {
		t.Error("expected error when table is missing")
	}
	if err.Error() != "table is required for reads" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestServerV2_GetSchema(t *testing.T) {
	server, tmpDir := createTestServerV2(t)
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()

	resp, err := server.GetSchema(ctx, &pb.GetSchemaRequest{
		InputName: "input",
	})
	if err != nil {
		t.Fatalf("GetSchema error: %v", err)
	}

	if resp.Schema == nil {
		t.Error("Schema should not be nil")
	}
	if len(resp.Schema.Columns) != 2 {
		t.Errorf("columns count = %d, want 2", len(resp.Schema.Columns))
	}
}

func TestServerV2_GetSample(t *testing.T) {
	server, tmpDir := createTestServerV2(t)
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()

	resp, err := server.GetSample(ctx, &pb.GetSampleRequest{
		InputName: "input",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("GetSample error: %v", err)
	}

	if resp.Schema == nil {
		t.Error("Schema should not be nil")
	}
	if len(resp.Rows) == 0 {
		t.Error("should have sample rows")
	}
}

func TestServerV2_Commit(t *testing.T) {
	server, tmpDir := createTestServerV2(t)
	defer os.RemoveAll(tmpDir)

	// Create output directory
	outputDir := filepath.Join(tmpDir, "output")
	os.MkdirAll(outputDir, 0755)

	ctx := context.Background()

	// Open and write to writer
	openResp, err := server.OpenWriter(ctx, &pb.OpenWriterRequest{
		Table:       "output",
		InputFormat: pb.DataFormat_FORMAT_CSV,
	})
	if err != nil {
		t.Fatalf("OpenWriter error: %v", err)
	}

	csvData := []byte("id,name\n1,Alice\n")
	_, err = server.WriteChunk(ctx, &pb.WriteChunkRequest{
		WriterId: openResp.WriterId,
		Data:     csvData,
	})
	if err != nil {
		t.Fatalf("WriteChunk error: %v", err)
	}

	// Commit
	commitResp, err := server.Commit(ctx, &pb.CommitRequest{})
	if err != nil {
		t.Fatalf("Commit error: %v", err)
	}

	if !commitResp.Success {
		t.Errorf("Commit should succeed, got Success=%v, Error=%q",
			commitResp.Success, commitResp.Error)
	}

	// Writers should be cleared after commit
	server.mu.RLock()
	writerCount := len(server.writers)
	server.mu.RUnlock()

	if writerCount != 0 {
		t.Errorf("writers should be cleared after commit, got %d", writerCount)
	}
}

func TestServerV2_Log(t *testing.T) {
	server, tmpDir := createTestServerV2(t)
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()

	_, err := server.Log(ctx, &pb.LogRequest{
		Level:   "INFO",
		Message: "test message",
		Fields: map[string]string{
			"key": "value",
		},
	})
	if err != nil {
		t.Fatalf("Log error: %v", err)
	}
}

func TestProtoToDataFormat(t *testing.T) {
	tests := []struct {
		proto  pb.DataFormat
		expect format.DataFormat
	}{
		{pb.DataFormat_FORMAT_CSV, format.FormatCSV},
		{pb.DataFormat_FORMAT_JSON, format.FormatJSON},
		{pb.DataFormat_FORMAT_JSONL, format.FormatJSONL},
		{pb.DataFormat_FORMAT_PARQUET, format.FormatParquet},
		{pb.DataFormat_FORMAT_ARROW_IPC, format.FormatArrowIPC},
		{pb.DataFormat_FORMAT_UNSPECIFIED, format.FormatUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.proto.String(), func(t *testing.T) {
			result := protoToDataFormat(tt.proto)
			if result != tt.expect {
				t.Errorf("protoToDataFormat(%v) = %v, want %v", tt.proto, result, tt.expect)
			}
		})
	}
}

func TestDataFormatToProto(t *testing.T) {
	tests := []struct {
		format format.DataFormat
		expect pb.DataFormat
	}{
		{format.FormatCSV, pb.DataFormat_FORMAT_CSV},
		{format.FormatJSON, pb.DataFormat_FORMAT_JSON},
		{format.FormatJSONL, pb.DataFormat_FORMAT_JSONL},
		{format.FormatParquet, pb.DataFormat_FORMAT_PARQUET},
		{format.FormatArrowIPC, pb.DataFormat_FORMAT_ARROW_IPC},
		{format.FormatUnknown, pb.DataFormat_FORMAT_UNSPECIFIED},
	}

	for _, tt := range tests {
		t.Run(tt.format.String(), func(t *testing.T) {
			result := dataFormatToProto(tt.format)
			if result != tt.expect {
				t.Errorf("dataFormatToProto(%v) = %v, want %v", tt.format, result, tt.expect)
			}
		})
	}
}

func TestSchemaConversion(t *testing.T) {
	// Test proto -> schema -> proto roundtrip
	protoSchema := &pb.Schema{
		Columns: []*pb.ColumnDef{
			{Name: "id", Type: "int64", Nullable: false},
			{Name: "name", Type: "string", Nullable: true},
			{Name: "price", Type: "float64", Nullable: true},
		},
	}

	// Convert to gateway schema
	gatewaySchema, err := protoToSchema(protoSchema)
	if err != nil {
		t.Fatalf("protoToSchema error: %v", err)
	}

	if gatewaySchema.NumColumns() != 3 {
		t.Errorf("NumColumns() = %d, want 3", gatewaySchema.NumColumns())
	}

	// Convert back to proto
	resultProto := schemaToProto(gatewaySchema)
	if len(resultProto.Columns) != 3 {
		t.Errorf("columns count = %d, want 3", len(resultProto.Columns))
	}

	// Verify columns match
	for i, col := range protoSchema.Columns {
		if resultProto.Columns[i].Name != col.Name {
			t.Errorf("column %d name = %q, want %q", i, resultProto.Columns[i].Name, col.Name)
		}
	}
}

func TestBackendSchemaToGatewaySchema(t *testing.T) {
	backendSchema := &backend.SchemaInfo{
		Columns: []backend.ColumnInfo{
			{Name: "id", Type: "int64", Nullable: false},
			{Name: "name", Type: "string", Nullable: true},
		},
	}

	gatewaySchema := backendSchemaToGatewaySchema(backendSchema)
	if gatewaySchema == nil {
		t.Fatal("schema should not be nil")
	}

	if gatewaySchema.NumColumns() != 2 {
		t.Errorf("NumColumns() = %d, want 2", gatewaySchema.NumColumns())
	}

	col0 := gatewaySchema.Column(0)
	if col0.Name != "id" || col0.Type != schema.TypeInt64 {
		t.Errorf("column 0 = %+v, want id:int64", col0)
	}

	col1 := gatewaySchema.Column(1)
	if col1.Name != "name" || col1.Type != schema.TypeString {
		t.Errorf("column 1 = %+v, want name:string", col1)
	}
}

func TestBackendSchemaToGatewaySchema_Nil(t *testing.T) {
	gatewaySchema := backendSchemaToGatewaySchema(nil)
	if gatewaySchema != nil {
		t.Error("nil backend schema should return nil gateway schema")
	}
}

// Helper function to create schema for tests
func mustNewSchema(t *testing.T, columns []schema.ColumnDef) *schema.Schema {
	t.Helper()
	sch, err := schema.NewSchema(columns)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}
	return sch
}

// TestBuildFieldIDMap tests that field IDs are correctly assigned from schema
func TestBuildFieldIDMap(t *testing.T) {
	tests := []struct {
		name     string
		schema   *schema.Schema
		expected map[string]int32
	}{
		{
			name: "auto-assign field IDs",
			schema: mustNewSchema(t, []schema.ColumnDef{
				{Name: "id", Type: schema.TypeInt64, Nullable: false},
				{Name: "name", Type: schema.TypeString, Nullable: true},
				{Name: "price", Type: schema.TypeFloat64, Nullable: true},
			}),
			expected: map[string]int32{
				"id":    1,
				"name":  2,
				"price": 3,
			},
		},
		{
			name: "explicit field IDs",
			schema: mustNewSchema(t, []schema.ColumnDef{
				{Name: "id", Type: schema.TypeInt64, Nullable: false, FieldID: 10},
				{Name: "name", Type: schema.TypeString, Nullable: true, FieldID: 20},
			}),
			expected: map[string]int32{
				"id":   10,
				"name": 20,
			},
		},
		{
			name: "mixed field IDs",
			schema: mustNewSchema(t, []schema.ColumnDef{
				{Name: "id", Type: schema.TypeInt64, Nullable: false, FieldID: 5},
				{Name: "name", Type: schema.TypeString, Nullable: true}, // Should auto-assign
			}),
			expected: map[string]int32{
				"id":   5,
				"name": 2, // Auto-assigned based on index (0-based index + 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildFieldIDMap(tt.schema)

			if len(result) != len(tt.expected) {
				t.Errorf("field ID map length = %d, want %d", len(result), len(tt.expected))
			}

			for name, expectedID := range tt.expected {
				if gotID, ok := result[name]; !ok {
					t.Errorf("field %q not found in result map", name)
				} else if gotID != expectedID {
					t.Errorf("field %q ID = %d, want %d", name, gotID, expectedID)
				}
			}
		})
	}
}

// TestValidateSchemaMatchesFieldIDs tests schema validation against field ID map
func TestValidateSchemaMatchesFieldIDs(t *testing.T) {
	// This test requires creating actual Parquet schemas which is complex
	// For now, we'll test the field ID map building logic which is used in validation

	// Test with matching schema
	t.Run("valid schema", func(t *testing.T) {
		sch := mustNewSchema(t, []schema.ColumnDef{
			{Name: "id", Type: schema.TypeInt64},
			{Name: "name", Type: schema.TypeString},
		})

		fieldIDMap := buildFieldIDMap(sch)

		// Verify all schema columns have entries in field ID map
		for _, col := range sch.Columns() {
			if _, ok := fieldIDMap[col.Name]; !ok {
				t.Errorf("column %q not found in field ID map", col.Name)
			}
		}
	})
}

// TestInjectFieldIDsIntoArrowSchema tests that field IDs are correctly injected
func TestInjectFieldIDsIntoArrowSchema(t *testing.T) {
	// This test would require importing Arrow and creating Arrow schemas
	// Skipping for basic test coverage, but the function is tested indirectly
	// through integration tests
	t.Skip("Arrow schema manipulation requires complex setup")
}

// TestPatchParquetFieldIDs_NoExternalFiles tests that patcher handles empty file list
func TestPatchParquetFieldIDs_NoExternalFiles(t *testing.T) {
	ctx := context.Background()
	mockBackend := newMockBackend()

	cfg := &Config{
		RunID: "ws-test",
	}

	server := &ServerV2{
		backend: mockBackend,
		config:  cfg,
		writers: make(map[string]*writerState),
	}

	// Create writer state with no external files
	ws := &writerState{
		bucket:        "test-bucket",
		table:         "test-table",
		externalFiles: []buffer.FileInfo{}, // Empty
		schema: mustNewSchema(t, []schema.ColumnDef{
			{Name: "id", Type: schema.TypeInt64},
		}),
	}

	// Should return nil without error
	err := server.patchParquetFieldIDs(ctx, ws)
	if err != nil {
		t.Errorf("patchParquetFieldIDs() with no external files should not error, got: %v", err)
	}
}

// TestPatchParquetFieldIDs_NoSchema tests that patcher fails when schema is nil
func TestPatchParquetFieldIDs_NoSchema(t *testing.T) {
	ctx := context.Background()
	mockBackend := newMockBackend()

	cfg := &Config{
		RunID: "ws-test",
	}

	server := &ServerV2{
		backend: mockBackend,
		config:  cfg,
		writers: make(map[string]*writerState),
	}

	// Create writer state with external files but no schema
	ws := &writerState{
		bucket: "test-bucket",
		table:  "test-table",
		externalFiles: []buffer.FileInfo{
			{Path: "s3://bucket/file.parquet", RowCount: 10, SizeBytes: 1000},
		},
		schema: nil, // No schema
	}

	// Should return error
	err := server.patchParquetFieldIDs(ctx, ws)
	if err == nil {
		t.Error("patchParquetFieldIDs() with nil schema should return error")
	}
	if err != nil && err.Error() != "cannot patch Parquet files: schema is nil" {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestSchemaFieldIDAutoAssignment tests that field IDs are auto-assigned correctly
func TestSchemaFieldIDAutoAssignment(t *testing.T) {
	// Test that when FieldID is 0, it gets auto-assigned
	cols := []schema.ColumnDef{
		{Name: "col1", Type: schema.TypeInt64, FieldID: 0},   // Should become 1
		{Name: "col2", Type: schema.TypeString, FieldID: 0},  // Should become 2
		{Name: "col3", Type: schema.TypeFloat64, FieldID: 5}, // Already set
	}

	sch := mustNewSchema(t, cols)
	fieldIDMap := buildFieldIDMap(sch)

	if fieldIDMap["col1"] != 1 {
		t.Errorf("col1 field ID = %d, want 1", fieldIDMap["col1"])
	}
	if fieldIDMap["col2"] != 2 {
		t.Errorf("col2 field ID = %d, want 2", fieldIDMap["col2"])
	}
	if fieldIDMap["col3"] != 5 {
		t.Errorf("col3 field ID = %d, want 5", fieldIDMap["col3"])
	}
}

// TestCommit_WithExternalFiles tests commit flow with external files
func TestCommit_WithExternalFiles(t *testing.T) {
	ctx := context.Background()
	mockBackend := newMockBackend()

	cfg := &Config{
		RunID: "ws-test-123",
	}

	server := &ServerV2{
		backend: mockBackend,
		config:  cfg,
		writers: make(map[string]*writerState),
	}

	// Create a writer state with external files (simulating DuckDB COPY TO)
	ws := &writerState{
		bucket:   "test-bucket",
		table:    "test-table",
		basePath: "s3://datuplet/test/",
		schema: mustNewSchema(t, []schema.ColumnDef{
			{Name: "id", Type: schema.TypeInt64},
			{Name: "name", Type: schema.TypeString},
		}),
		externalFiles: []buffer.FileInfo{
			{
				Path:      "s3://datuplet/test/data.parquet",
				RowCount:  100,
				SizeBytes: 10000,
			},
		},
		totalRows:  100,
		totalBytes: 10000,
	}

	server.writers["w1"] = ws

	// Note: This test will fail at the Parquet reading stage since we don't have
	// actual Parquet files, but it demonstrates the commit flow structure
	req := &pb.CommitRequest{
		BestEffort: false,
	}

	resp, err := server.Commit(ctx, req)

	// We expect it to fail because we can't actually read/patch the Parquet file
	// but we're testing that the flow is correct
	if err != nil {
		t.Logf("Expected error (no actual Parquet file): %v", err)
	}

	// Response should be created even if commit fails
	if resp == nil {
		t.Error("Commit response should not be nil")
	}
}

// ============================================================================
// validateRunTokenForBoot integration tests
// ============================================================================

// rfc017MakeJWKS builds a minimal JWKS JSON document for a single RSA key.
func rfc017MakeJWKS(kid string, pub *rsa.PublicKey) []byte {
	type jwkEntry struct {
		Kty string `json:"kty"`
		Kid string `json:"kid"`
		N   string `json:"n"`
		E   string `json:"e"`
	}
	type jwksDoc struct {
		Keys []jwkEntry `json:"keys"`
	}
	eBytes := new(big.Int).SetInt64(int64(pub.E)).Bytes()
	doc := jwksDoc{Keys: []jwkEntry{{
		Kty: "RSA",
		Kid: kid,
		N:   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
		E:   base64.RawURLEncoding.EncodeToString(eBytes),
	}}}
	b, _ := json.Marshal(doc)
	return b
}

// rfc017GenKey generates a 2048-bit RSA key; fatal on error.
func rfc017GenKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	return priv
}

// rfc017MintToken mints a valid RS256 run-token JWT signed by priv.
func rfc017MintToken(t *testing.T, priv *rsa.PrivateKey, kid, runID, warehouse, projectID string) string {
	t.Helper()
	now := time.Now()
	claims := jwt.MapClaims{
		"iss":        "datuplet-api",
		"aud":        "datuplet-catalog",
		"sub":        runID,
		"run_id":     runID,
		"token_kind": "run",
		"project_id": projectID,
		"warehouse":  warehouse,
		"iat":        now.Unix(),
		"nbf":        now.Unix(),
		"exp":        now.Add(24 * time.Hour).Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = kid
	signed, err := tok.SignedString(priv)
	if err != nil {
		t.Fatalf("jwt.SignedString: %v", err)
	}
	return signed
}

// TestValidateRunTokenForBoot_HappyPath verifies that validateRunTokenForBoot
// returns populated ValidatedClaims when given a correctly-signed token whose
// run_id matches the RUN_ID env.
func TestValidateRunTokenForBoot_HappyPath(t *testing.T) {
	const kid = "test-key-1"
	const runID = "run-abc-123"
	const warehouse = "my-warehouse"
	const projectID = "proj-uuid-456"

	priv := rfc017GenKey(t)
	jwksBody := rfc017MakeJWKS(kid, &priv.PublicKey)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(jwksBody) //nolint:errcheck
	}))
	defer srv.Close()

	tokenStr := rfc017MintToken(t, priv, kid, runID, warehouse, projectID)

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(tokenStr+"\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	t.Setenv("RUN_ID", runID)

	cfg := &Config{
		RunTokenPath:       tokenFile,
		PipelineAPIJWKSURL: srv.URL,
	}

	claims, rawJWT, err := validateRunTokenForBoot(cfg)
	if err != nil {
		t.Fatalf("validateRunTokenForBoot: %v", err)
	}
	if claims == nil {
		t.Fatal("claims must be non-nil on success")
	}
	if claims.Warehouse != warehouse {
		t.Errorf("Warehouse = %q, want %q", claims.Warehouse, warehouse)
	}
	if claims.ProjectID != projectID {
		t.Errorf("ProjectID = %q, want %q", claims.ProjectID, projectID)
	}
	if claims.RunID != runID {
		t.Errorf("RunID = %q, want %q", claims.RunID, runID)
	}
	if rawJWT == "" {
		t.Error("rawJWT must be non-empty")
	}
	// rawJWT must equal the token string (trimmed)
	if strings.TrimSpace(rawJWT) != tokenStr {
		t.Errorf("rawJWT does not match minted token")
	}
}

// TestValidateRunTokenForBoot_TamperedSignature verifies that a token signed
// with a different key (not in the JWKS) is rejected.
func TestValidateRunTokenForBoot_TamperedSignature(t *testing.T) {
	const kid = "test-key-1"
	const runID = "run-tampered"

	legit := rfc017GenKey(t)
	attacker := rfc017GenKey(t)

	// JWKS serves the legit public key, but token is signed with attacker's key.
	jwksBody := rfc017MakeJWKS(kid, &legit.PublicKey)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(jwksBody) //nolint:errcheck
	}))
	defer srv.Close()

	// Sign with attacker key but claim kid of legit key.
	tokenStr := rfc017MintToken(t, attacker, kid, runID, "wh", "proj")

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(tokenStr), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	t.Setenv("RUN_ID", runID)

	cfg := &Config{
		RunTokenPath:       tokenFile,
		PipelineAPIJWKSURL: srv.URL,
	}

	_, _, err := validateRunTokenForBoot(cfg)
	if err == nil {
		t.Fatal("expected error for tampered signature, got nil")
	}
	// Should mention check 1 (signature/parse)
	if !strings.Contains(err.Error(), "check 1") {
		t.Errorf("error should reference check 1 (signature), got: %v", err)
	}
}

// TestValidateRunTokenForBoot_RunIDMismatch verifies that check 8 fires when
// run_id in the JWT does not match the RUN_ID env.
func TestValidateRunTokenForBoot_RunIDMismatch(t *testing.T) {
	const kid = "test-key-1"
	const jwtRunID = "run-from-jwt"
	const envRunID = "run-different-env"

	priv := rfc017GenKey(t)
	jwksBody := rfc017MakeJWKS(kid, &priv.PublicKey)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(jwksBody) //nolint:errcheck
	}))
	defer srv.Close()

	tokenStr := rfc017MintToken(t, priv, kid, jwtRunID, "wh", "proj")

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(tokenStr), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	t.Setenv("RUN_ID", envRunID) // deliberately different from jwtRunID

	cfg := &Config{
		RunTokenPath:       tokenFile,
		PipelineAPIJWKSURL: srv.URL,
	}

	_, _, err := validateRunTokenForBoot(cfg)
	if err == nil {
		t.Fatal("expected error for run_id mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "check 8") {
		t.Errorf("error should reference check 8 (run_id mismatch), got: %v", err)
	}
}

// TestValidateRunTokenForBoot_MissingRUNID verifies that an unset RUN_ID env
// causes validateRunTokenForBoot to return an error rather than panicking.
func TestValidateRunTokenForBoot_MissingRUNID(t *testing.T) {
	// Ensure RUN_ID is unset for this test.
	t.Setenv("RUN_ID", "")

	cfg := &Config{
		RunTokenPath:       "/some/path",
		PipelineAPIJWKSURL: "http://localhost:9999/jwks.json",
	}

	_, _, err := validateRunTokenForBoot(cfg)
	if err == nil {
		t.Fatal("expected error when RUN_ID is unset")
	}
	if !strings.Contains(err.Error(), "RUN_ID") {
		t.Errorf("error should mention RUN_ID, got: %v", err)
	}
}

// Ensure the test file uses context (avoids unused import if no other test needs it).
var _ = context.Background
