package sdk

import (
	"testing"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// TestConfig_OutputTableColumns verifies Config() surfaces an output table's
// explicit column mapping (set via outputs.tables[].columns in the pipeline
// doc) so a component can look up its target table's mapping by name.
func TestConfig_OutputTableColumns(t *testing.T) {
	tests := []struct {
		name        string
		config      *pb.ComponentConfig
		wantColumns []ColumnRef // for OutputTables[0]
	}{
		{
			name: "explicit column mapping present",
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
			wantColumns: []ColumnRef{{Name: "id", Type: "int"}, {Name: "note", Type: "string"}},
		},
		{
			name: "no explicit mapping — producer infers",
			config: &pb.ComponentConfig{
				OutputConfig: &pb.OutputConfig{
					Tables: []*pb.TableOutputConfig{
						{Name: "summary", Bucket: "curated", WriteMode: "APPEND"},
					},
				},
			},
			wantColumns: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{config: tt.config}
			cfg := client.Config()

			if len(cfg.OutputTables) != 1 {
				t.Fatalf("OutputTables count = %d, want 1", len(cfg.OutputTables))
			}
			got := cfg.OutputTables[0].Columns
			if len(got) != len(tt.wantColumns) {
				t.Fatalf("Columns = %+v, want %+v", got, tt.wantColumns)
			}
			for i, want := range tt.wantColumns {
				if got[i] != want {
					t.Errorf("Columns[%d] = %+v, want %+v", i, got[i], want)
				}
			}
		})
	}
}
