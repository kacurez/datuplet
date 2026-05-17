package sdk

import (
	"testing"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// TestStorageBootstrap verifies that StorageBootstrap accessor works correctly.
func TestStorageBootstrap(t *testing.T) {
	tests := []struct {
		name      string
		config    *pb.ComponentConfig
		wantNil   bool
		wantCount int // Number of inputs expected
	}{
		{
			name: "bootstrap data present",
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
			wantNil:   false,
			wantCount: 1,
		},
		{
			name: "no bootstrap data",
			config: &pb.ComponentConfig{
				ExecutionId: "test-exec",
			},
			wantNil:   true,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				config: tt.config,
			}

			bootstrap := client.StorageBootstrap()

			if tt.wantNil && bootstrap != nil {
				t.Errorf("StorageBootstrap() = %v, want nil", bootstrap)
			}

			if !tt.wantNil && bootstrap == nil {
				t.Fatal("StorageBootstrap() = nil, want non-nil")
			}

			if !tt.wantNil {
				if len(bootstrap.Inputs) != tt.wantCount {
					t.Errorf("StorageBootstrap().Inputs count = %d, want %d", len(bootstrap.Inputs), tt.wantCount)
				}

				if len(bootstrap.Inputs) > 0 {
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
				}

				if len(bootstrap.Outputs) != 1 {
					t.Errorf("StorageBootstrap().Outputs count = %d, want 1", len(bootstrap.Outputs))
				}

				if len(bootstrap.BucketCredentials) != 1 {
					t.Errorf("StorageBootstrap().BucketCredentials count = %d, want 1", len(bootstrap.BucketCredentials))
				}

				if creds, ok := bootstrap.BucketCredentials["raw"]; ok {
					if creds.AccessKeyId != "test-key" {
						t.Errorf("Credentials AccessKeyId = %s, want test-key", creds.AccessKeyId)
					}
					if creds.Endpoint != "http://minio:9000" {
						t.Errorf("Credentials Endpoint = %s, want http://minio:9000", creds.Endpoint)
					}
				} else {
					t.Error("Expected credentials for 'raw' bucket not found")
				}
			}
		})
	}
}
