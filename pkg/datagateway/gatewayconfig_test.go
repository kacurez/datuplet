package datagateway

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// operatorShapeYAML mirrors what the operator's generateGatewayConfig
// (pkg/k8s/controllers/pipelinerun_jobs.go) renders for a component with a
// default bucket, an explicit output table with columns + partition fields,
// a drop processor, a referenced secret and a lakekeeper catalog.
const operatorShapeYAML = `component:
  config:
    url: https://example.test/api
    token: $[api_token]
  outputs:
    processors:
      - columns:
          - secret_col
        type: drop
default_bucket: raw
default_write_mode: FULL_LOAD
gateway:
  buffer_size: 33554432
  chunk_size: 16777216
  row_group_size: 8388608
  target_file_size: 67108864
input_buckets:
  - staging
input_tables:
  - bucket: staging
    table: users
lakekeeper_url: http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog
mode: minio
output_buckets:
  - raw
  - curated
output_tables:
  - bucket: curated
    columns:
      - name: id
        type: INT64
      - name: name
        type: STRING
    logical_name: orders_out
    name: orders
    partition_fields:
      - source_column: created
        transform: day
    write_mode: APPEND
pipeline_api_jwks_url: http://pipeline-api.datuplet.svc.cluster.local:8081/api/v1/auth/jwks.json
run_id: 0b1c2d3e-aaaa-bbbb-cccc-1234567890ab
secrets_dir: /var/run/secrets/datuplet
`

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadConfigFile_OperatorShape(t *testing.T) {
	p := writeTemp(t, operatorShapeYAML)
	cfg, err := LoadConfigFile(p, "/run/token", "/etc/podinfo/annotations")
	if err != nil {
		t.Fatalf("LoadConfigFile: %v", err)
	}

	if cfg.RunID != "0b1c2d3e-aaaa-bbbb-cccc-1234567890ab" {
		t.Errorf("RunID = %q", cfg.RunID)
	}
	if want := "http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog"; cfg.LakekeeperURL != want {
		t.Errorf("LakekeeperURL = %q, want %q", cfg.LakekeeperURL, want)
	}
	if want := "http://pipeline-api.datuplet.svc.cluster.local:8081/api/v1/auth/jwks.json"; cfg.PipelineAPIJWKSURL != want {
		t.Errorf("PipelineAPIJWKSURL = %q, want %q", cfg.PipelineAPIJWKSURL, want)
	}
	if cfg.RunTokenPath != "/run/token" || cfg.PodAnnotationsPath != "/etc/podinfo/annotations" {
		t.Errorf("paths = %q / %q", cfg.RunTokenPath, cfg.PodAnnotationsPath)
	}
	if cfg.SecretsDir != "/var/run/secrets/datuplet" {
		t.Errorf("SecretsDir = %q", cfg.SecretsDir)
	}
	if cfg.DefaultBucket != "raw" || cfg.DefaultWriteMode != "FULL_LOAD" {
		t.Errorf("default bucket/mode = %q/%q", cfg.DefaultBucket, cfg.DefaultWriteMode)
	}
	if !reflect.DeepEqual(cfg.InputBuckets, []string{"staging"}) {
		t.Errorf("InputBuckets = %v", cfg.InputBuckets)
	}
	if !reflect.DeepEqual(cfg.OutputBuckets, []string{"raw", "curated"}) {
		t.Errorf("OutputBuckets = %v", cfg.OutputBuckets)
	}
	if len(cfg.InputTables) != 1 || cfg.InputTables[0].Bucket != "staging" || cfg.InputTables[0].Table != "users" {
		t.Errorf("InputTables = %+v", cfg.InputTables)
	}

	wantOut := OutputTableConfig{
		Name:            "orders",
		Bucket:          "curated",
		WriteMode:       "APPEND",
		LogicalName:     "orders_out",
		PartitionFields: []PartitionFieldConfig{{SourceColumn: "created", Transform: "day"}},
		Columns:         []ColumnConfig{{Name: "id", Type: "INT64"}, {Name: "name", Type: "STRING"}},
	}
	if len(cfg.OutputTables) != 1 || !reflect.DeepEqual(cfg.OutputTables[0], wantOut) {
		t.Errorf("OutputTables = %+v, want [%+v]", cfg.OutputTables, wantOut)
	}

	wantProc := []ProcessorConfig{{Type: "drop", Columns: []string{"secret_col"}}}
	if !reflect.DeepEqual(cfg.Processors, wantProc) {
		t.Errorf("Processors = %+v, want %+v", cfg.Processors, wantProc)
	}

	if cfg.ChunkSize != 16<<20 || cfg.BufferSize != 32<<20 || cfg.RowGroupSize != 8<<20 || cfg.TargetFileSize != 64<<20 {
		t.Errorf("sizes = chunk %d buffer %d rowgroup %d target %d",
			cfg.ChunkSize, cfg.BufferSize, cfg.RowGroupSize, cfg.TargetFileSize)
	}
	if string(cfg.ComponentCfg) != `{"token":"$[api_token]","url":"https://example.test/api"}` {
		t.Errorf("ComponentCfg = %s", cfg.ComponentCfg)
	}
	if cfg.Backend != nil {
		t.Errorf("Backend must stay nil (static backends are the caller's concern)")
	}
}

func TestLoadConfigFile_Defaults(t *testing.T) {
	p := writeTemp(t, "run_id: r1\n")
	cfg, err := LoadConfigFile(p, "", "")
	if err != nil {
		t.Fatalf("LoadConfigFile: %v", err)
	}
	if cfg.ChunkSize != 32<<20 {
		t.Errorf("ChunkSize = %d, want 32 MiB", cfg.ChunkSize)
	}
	if cfg.BufferSize != 64<<20 {
		t.Errorf("BufferSize = %d, want 64 MiB", cfg.BufferSize)
	}
	if cfg.RowGroupSize != 64<<20 {
		t.Errorf("RowGroupSize = %d, want BufferSize (64 MiB)", cfg.RowGroupSize)
	}
	if cfg.TargetFileSize != 128<<20 {
		t.Errorf("TargetFileSize = %d, want 128 MiB", cfg.TargetFileSize)
	}
}

func TestLoadConfigFile_LegacyOutputsAndErrors(t *testing.T) {
	p := writeTemp(t, "component:\n  legacy_outputs:\n    out: /data/out\n")
	cfg, err := LoadConfigFile(p, "", "")
	if err != nil {
		t.Fatalf("LoadConfigFile: %v", err)
	}
	if got := cfg.Outputs["out"].Path; got != "/data/out" {
		t.Errorf("legacy output path = %q", got)
	}

	if _, err := LoadConfigFile(filepath.Join(t.TempDir(), "missing.yaml"), "", ""); err == nil {
		t.Error("missing file: want error")
	}
	if _, err := LoadConfigFile(writeTemp(t, "run_id: [unterminated"), "", ""); err == nil {
		t.Error("bad yaml: want error")
	}
}
