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

func TestResolveSecretRefsInConfig(t *testing.T) {
	tests := []struct {
		name         string
		files        map[string]string // written under SecretsDir
		noSecretsDir bool              // leave SecretsDir empty
		cfg          string
		wantRaw      string         // exact bytes after resolve, when non-empty
		wantMap      map[string]any // parsed result, when non-nil
		wantErrName  string         // error must mention this secret name
	}{
		{
			name:    "replaces refs",
			files:   map[string]string{"db_password": "hunter2\n"},
			cfg:     `{"password":"$[db_password]","user":"alice"}`,
			wantMap: map[string]any{"password": "hunter2", "user": "alice"},
		},
		{
			name:    "no refs leaves config untouched",
			cfg:     `{"user":"alice"}`,
			wantRaw: `{"user":"alice"}`,
		},
		{
			name:         "refs present but SecretsDir empty errors",
			noSecretsDir: true,
			cfg:          `{"password":"$[needs_a_file]"}`,
			wantErrName:  "needs_a_file",
		},
		{
			name:        "missing secret file errors",
			cfg:         `{"password":"$[nope]"}`,
			wantErrName: "nope",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{RunID: "run-1", ComponentCfg: []byte(tc.cfg)}
			if !tc.noSecretsDir {
				cfg.SecretsDir = t.TempDir()
				for name, body := range tc.files {
					if err := os.WriteFile(filepath.Join(cfg.SecretsDir, name), []byte(body), 0o400); err != nil {
						t.Fatalf("write secret: %v", err)
					}
				}
			}

			err := resolveSecretRefsInConfig(cfg)
			if tc.wantErrName != "" {
				if err == nil {
					t.Fatal("expected error")
				}
				if !strings.Contains(err.Error(), tc.wantErrName) {
					t.Errorf("error missing secret name %q: %v", tc.wantErrName, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveSecretRefsInConfig: %v", err)
			}
			if tc.wantRaw != "" && string(cfg.ComponentCfg) != tc.wantRaw {
				t.Errorf("ComponentCfg mutated when no refs present: %s", cfg.ComponentCfg)
			}
			if tc.wantMap != nil {
				var got map[string]any
				if err := json.Unmarshal(cfg.ComponentCfg, &got); err != nil {
					t.Fatalf("unmarshal resolved cfg: %v", err)
				}
				for k, want := range tc.wantMap {
					if got[k] != want {
						t.Errorf("%s got %v, want %v", k, got[k], want)
					}
				}
			}
		})
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

// TestServerV2_GetConfig_OutputTableColumns proves an output table's
// explicit column mapping (config-side OutputTableConfig.Columns) survives
// the trip into the proto ComponentConfig response.
func TestServerV2_GetConfig_OutputTableColumns(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gateway-v2-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &Config{
		RunID:         "test-exec",
		ComponentName: "test-component",
		OutputTables: []OutputTableConfig{
			{
				Name:      "summary",
				Bucket:    "curated",
				WriteMode: "APPEND",
				Columns: []ColumnConfig{
					{Name: "id", Type: "int"},
					{Name: "note", Type: "string"},
				},
			},
			{
				Name:      "no_mapping",
				Bucket:    "curated",
				WriteMode: "APPEND",
			},
		},
		Backend: newMockBackend(),
	}
	server := NewServerV2(cfg)

	ctx := context.Background()
	resp, err := server.GetConfig(ctx, &pb.GetConfigRequest{})
	if err != nil {
		t.Fatalf("GetConfig error: %v", err)
	}

	if len(resp.OutputConfig.Tables) != 2 {
		t.Fatalf("want 2 output tables, got %d", len(resp.OutputConfig.Tables))
	}

	withCols := resp.OutputConfig.Tables[0]
	if len(withCols.Columns) != 2 {
		t.Fatalf("want 2 columns on %q, got %d", withCols.Name, len(withCols.Columns))
	}
	if withCols.Columns[0].Name != "id" || withCols.Columns[0].Type != "int" {
		t.Errorf("columns[0] = %+v, want {id int}", withCols.Columns[0])
	}
	if withCols.Columns[1].Name != "note" || withCols.Columns[1].Type != "string" {
		t.Errorf("columns[1] = %+v, want {note string}", withCols.Columns[1])
	}

	noMapping := resp.OutputConfig.Tables[1]
	if len(noMapping.Columns) != 0 {
		t.Errorf("want 0 columns on %q, got %d", noMapping.Name, len(noMapping.Columns))
	}
}

// OpenWriter resolves the bucket from defaultBucket, or from outputs.tables
// when the table is declared there (defaultBucket cleared so resolution can
// only come from the OutputTables lookup).
func TestServerV2_OpenWriter(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(*ServerV2)
		table      string
		wantBucket string
	}{
		{
			name:       "uses defaultBucket from config",
			table:      "output",
			wantBucket: "raw",
		},
		{
			name: "declared table resolves bucket from outputs.tables",
			setup: func(s *ServerV2) {
				s.config.DefaultBucket = ""
				s.config.OutputTables = []OutputTableConfig{{Bucket: "declared", Name: "orders"}}
			},
			table:      "orders",
			wantBucket: "declared",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, tmpDir := createTestServerV2(t)
			defer os.RemoveAll(tmpDir)
			if tc.setup != nil {
				tc.setup(server)
			}

			resp, err := server.OpenWriter(context.Background(), &pb.OpenWriterRequest{
				Table:       tc.table,
				InputFormat: pb.DataFormat_FORMAT_CSV,
			})
			if err != nil {
				t.Fatalf("OpenWriter error: %v", err)
			}
			if resp.WriterId == "" {
				t.Fatal("WriterId should not be empty")
			}

			server.mu.RLock()
			ws, ok := server.writers[resp.WriterId]
			server.mu.RUnlock()
			if !ok {
				t.Fatal("writer should be stored")
			}
			if ws.bucket != tc.wantBucket {
				t.Errorf("bucket = %q, want %q", ws.bucket, tc.wantBucket)
			}
			if ws.table != tc.table {
				t.Errorf("table = %q, want %q", ws.table, tc.table)
			}
		})
	}
}

func TestServerV2_MissingRequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		call    func(*ServerV2) error
		wantSub string
	}{
		{"OpenWriter without table", func(s *ServerV2) error {
			_, err := s.OpenWriter(context.Background(), &pb.OpenWriterRequest{Bucket: "raw"})
			return err
		}, "table is required"},
		{"OpenReader without bucket", func(s *ServerV2) error {
			_, err := s.OpenReader(context.Background(), &pb.OpenReaderRequest{Table: "products"})
			return err
		}, "bucket is required for reads"},
		{"OpenReader without table", func(s *ServerV2) error {
			_, err := s.OpenReader(context.Background(), &pb.OpenReaderRequest{Bucket: "raw"})
			return err
		}, "table is required for reads"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, tmpDir := createTestServerV2(t)
			defer os.RemoveAll(tmpDir)

			err := tc.call(server)
			if err == nil {
				t.Fatal("expected an error for the missing field")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not contain %q", err, tc.wantSub)
			}
		})
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

func TestProtoDataFormatMapping(t *testing.T) {
	tests := []struct {
		proto  pb.DataFormat
		format format.DataFormat
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
			if got := protoToDataFormat(tt.proto); got != tt.format {
				t.Errorf("protoToDataFormat(%v) = %v, want %v", tt.proto, got, tt.format)
			}
			if got := dataFormatToProto(tt.format); got != tt.proto {
				t.Errorf("dataFormatToProto(%v) = %v, want %v", tt.format, got, tt.proto)
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
	tests := []struct {
		name    string
		in      *backend.SchemaInfo
		wantNil bool
		want    []schema.ColumnDef
	}{
		{
			name: "converts columns",
			in: &backend.SchemaInfo{Columns: []backend.ColumnInfo{
				{Name: "id", Type: "int64", Nullable: false},
				{Name: "name", Type: "string", Nullable: true},
			}},
			want: []schema.ColumnDef{
				{Name: "id", Type: schema.TypeInt64},
				{Name: "name", Type: schema.TypeString},
			},
		},
		{name: "nil backend schema returns nil gateway schema", in: nil, wantNil: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := backendSchemaToGatewaySchema(tc.in)
			if tc.wantNil {
				if got != nil {
					t.Fatal("nil backend schema should return nil gateway schema")
				}
				return
			}
			if got == nil {
				t.Fatal("schema should not be nil")
			}
			if got.NumColumns() != len(tc.want) {
				t.Fatalf("NumColumns() = %d, want %d", got.NumColumns(), len(tc.want))
			}
			for i, w := range tc.want {
				if col := got.Column(i); col.Name != w.Name || col.Type != w.Type {
					t.Errorf("column %d = %+v, want %s:%v", i, col, w.Name, w.Type)
				}
			}
		})
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

func TestPatchParquetFieldIDs(t *testing.T) {
	tests := []struct {
		name    string
		ws      *writerState
		wantErr string // empty: must succeed
	}{
		{
			name: "no external files is a no-op",
			ws: &writerState{
				bucket:        "test-bucket",
				table:         "test-table",
				externalFiles: []buffer.FileInfo{},
				schema: mustNewSchema(t, []schema.ColumnDef{
					{Name: "id", Type: schema.TypeInt64},
				}),
			},
		},
		{
			name: "external files but nil schema fails",
			ws: &writerState{
				bucket: "test-bucket",
				table:  "test-table",
				externalFiles: []buffer.FileInfo{
					{Path: "s3://bucket/file.parquet", RowCount: 10, SizeBytes: 1000},
				},
			},
			wantErr: "schema is nil",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := &ServerV2{
				backend: newMockBackend(),
				config:  &Config{RunID: "ws-test"},
				writers: make(map[string]*writerState),
			}
			err := server.patchParquetFieldIDs(context.Background(), tc.ws)
			if tc.wantErr == "" {
				if err != nil {
					t.Errorf("patchParquetFieldIDs() should not error, got: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("patchParquetFieldIDs() err = %v, want it to contain %q", err, tc.wantErr)
			}
		})
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
