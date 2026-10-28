package sdk

import (
	"testing"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// TestClient_Accessors covers the Client's config accessors: Config()
// surfacing an output table's explicit column mapping (set via
// outputs.tables[].columns in the pipeline doc, so a component can look up its
// target table's mapping by name), and StorageBootstrap() surfacing the
// resolved inputs/outputs/credentials (nil when the gateway sent none).
func TestClient_Accessors(t *testing.T) {
	outputColumns := func(wantColumns []ColumnRef) func(*testing.T, *Client) {
		return func(t *testing.T, c *Client) {
			cfg := c.Config()
			if len(cfg.OutputTables) != 1 {
				t.Fatalf("OutputTables count = %d, want 1", len(cfg.OutputTables))
			}
			got := cfg.OutputTables[0].Columns
			if len(got) != len(wantColumns) {
				t.Fatalf("Columns = %+v, want %+v", got, wantColumns)
			}
			for i, want := range wantColumns {
				if got[i] != want {
					t.Errorf("Columns[%d] = %+v, want %+v", i, got[i], want)
				}
			}
		}
	}

	tests := []struct {
		name   string
		config *pb.ComponentConfig
		check  func(*testing.T, *Client)
	}{
		{
			name: "Config explicit column mapping present",
			config: &pb.ComponentConfig{
				OutputConfig: &pb.OutputConfig{
					Tables: []*pb.TableOutputConfig{
						{
							Name:      "summary",
							Bucket:    "curated",
							WriteMode: "APPEND",
							Columns: []*pb.ColumnConfig{
								{Name: "id", Type: "int"},
								{Name: "note", Type: "string"},
							},
						},
					},
				},
			},
			check: outputColumns([]ColumnRef{{Name: "id", Type: "int"}, {Name: "note", Type: "string"}}),
		},
		{
			name: "Config no explicit mapping - producer infers",
			config: &pb.ComponentConfig{
				OutputConfig: &pb.OutputConfig{
					Tables: []*pb.TableOutputConfig{
						{Name: "summary", Bucket: "curated", WriteMode: "APPEND"},
					},
				},
			},
			check: outputColumns(nil),
		},
		{
			name: "StorageBootstrap data present",
			config: &pb.ComponentConfig{
				StorageBootstrap: &pb.StorageBootstrap{
					Inputs: []*pb.ResolvedInput{
						{
							Bucket:    "raw",
							Table:     "sales",
							FilePaths: []string{"s3://bucket/file1.parquet"},
							Format:    "parquet",
						},
					},
					Outputs: []*pb.ResolvedOutput{
						{
							Bucket:   "curated",
							Table:    "agg",
							Location: "s3://bucket/writes/ws-123/agg/data/",
						},
					},
					BucketCredentials: map[string]*pb.S3Credentials{
						"raw": {
							AccessKeyId:     "test-key",
							SecretAccessKey: "test-secret",
							Endpoint:        "http://minio:9000",
							Region:          "us-east-1",
							BucketName:      "raw-bucket",
						},
					},
				},
			},
			check: func(t *testing.T, c *Client) {
				bootstrap := c.StorageBootstrap()
				if bootstrap == nil {
					t.Fatal("StorageBootstrap() = nil, want non-nil")
				}
				if len(bootstrap.Inputs) != 1 {
					t.Fatalf("StorageBootstrap().Inputs count = %d, want 1", len(bootstrap.Inputs))
				}
				input := bootstrap.Inputs[0]
				if input.Bucket != "raw" {
					t.Errorf("Input bucket = %s, want raw", input.Bucket)
				}
				if input.Table != "sales" {
					t.Errorf("Input table = %s, want sales", input.Table)
				}
				if input.Format != "parquet" {
					t.Errorf("Input format = %s, want parquet", input.Format)
				}
				if len(input.FilePaths) != 1 {
					t.Errorf("Input file paths count = %d, want 1", len(input.FilePaths))
				}
				if len(bootstrap.Outputs) != 1 {
					t.Errorf("StorageBootstrap().Outputs count = %d, want 1", len(bootstrap.Outputs))
				}
				if len(bootstrap.BucketCredentials) != 1 {
					t.Errorf("StorageBootstrap().BucketCredentials count = %d, want 1", len(bootstrap.BucketCredentials))
				}
				creds, ok := bootstrap.BucketCredentials["raw"]
				if !ok {
					t.Fatal("Expected credentials for 'raw' bucket not found")
				}
				if creds.AccessKeyId != "test-key" {
					t.Errorf("Credentials AccessKeyId = %s, want test-key", creds.AccessKeyId)
				}
				if creds.Endpoint != "http://minio:9000" {
					t.Errorf("Credentials Endpoint = %s, want http://minio:9000", creds.Endpoint)
				}
			},
		},
		{
			name:   "StorageBootstrap no bootstrap data",
			config: &pb.ComponentConfig{ExecutionId: "test-exec"},
			check: func(t *testing.T, c *Client) {
				if bootstrap := c.StorageBootstrap(); bootstrap != nil {
					t.Errorf("StorageBootstrap() = %v, want nil", bootstrap)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.check(t, &Client{config: tt.config})
		})
	}
}
