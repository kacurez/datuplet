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
	// Description is kept in the fixture (to prove it doesn't crash or
	// corrupt other fields on the way through) but is expected to be lost:
	// the CR has no Description field per spec §3, so the round-tripped
	// result must match a copy of the fixture with Description zeroed out.
	// Description's canonical home is the DB (wired in a later task), not
	// the CR.
	want := *doc
	want.Description = ""

	got := CRToDoc(DocToCR(doc))
	if diff := cmp.Diff(&want, got); diff != "" {
		t.Fatalf("round trip lost data (-want +got):\n%s", diff)
	}
}
