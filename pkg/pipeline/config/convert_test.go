package config

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

// TestDocCRDocRoundTripLossless proves DocToCR/CRToDoc round-trip without
// data loss: CRToDoc(DocToCR(doc)) must equal doc exactly, including the
// as<->logicalName and source_column<->sourceColumn renames (spec §3).
func TestDocCRDocRoundTripLossless(t *testing.T) {
	doc := &Pipeline{
		Name: "rt", Description: "d",
		Gateway: GatewayConfig{ChunkSize: 1024},
		Stages: []Stage{{Name: "s", Components: []Component{{
			Name: "c", Component: "sql-transform", Version: "1.2.3",
			Config: map[string]any{"sql": "SELECT 1"},
			Inputs: &InputSpec{Tables: []InputTableSpec{{Bucket: "raw", Table: "t", As: "alias", Since: "3d", TimestampColumn: "ts"}}},
			Outputs: &OutputSpec{Tables: []OutputTableSpec{{Name: "o", Bucket: "cur", WriteMode: "APPEND",
				PartitionSpec: []PartitionFieldSpec{{SourceColumn: "day", Transform: "day"}}}}},
			Resources: &ResourceSpec{Memory: "1Gi", CPU: "500m"},
		}}}},
	}
	got := CRToDoc(DocToCR(doc))
	if diff := cmp.Diff(doc, got); diff != "" {
		t.Fatalf("round trip lost data (-want +got):\n%s", diff)
	}
}
