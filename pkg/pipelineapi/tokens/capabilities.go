package tokens

import (
	"github.com/datuplet/datuplet/pkg/pipeline/config"
)

// PipelineIntent classifies the high-level access a run needs against
// lakekeeper. The synthetic run user receives a single role at the
// lakekeeper-project level; lakekeeper's own model chains the grant
// to namespaces and tables.
//
// The mapping to FGA relations is fixed:
//
//	HasWrites=true  → write the `editor` tuple (covers reads + writes)
//	HasWrites=false → write the `viewer` tuple (read-only)
//
// The chain is upward:
// the leaf `editor` tuple we write is included in `data_admin`'s union
// (`data_admin: [user, role#assignee] or project_admin or editor`),
// `data_admin` flows into `modify` and `describe`, and lakekeeper
// further chains those through namespace.modify → table.modify →
// can_write_data on the catalog side. So a project-level `editor`
// grant is the simplest correct shape that covers every namespace +
// table the pipeline references.)
type PipelineIntent struct {
	// HasWrites is true when any component declares an output. False
	// for read-only pipelines (input-only — e.g. an export pipeline
	// that never writes back).
	HasWrites bool

	// HasReads is true when any component declares an input. Most
	// pipelines have both, but a fresh-extract pipeline may have
	// only HasWrites=true.
	HasReads bool
}

// PipelineIntentFromPipeline scans the parsed pipeline and returns
// the high-level read/write classification used to derive the FGA
// tuples for the synthetic run user. Empty pipeline (no stages) →
// zero-value (no grants — the run goroutine will start, do nothing,
// and complete successfully).
func PipelineIntentFromPipeline(p *config.Pipeline) PipelineIntent {
	if p == nil {
		return PipelineIntent{}
	}
	out := PipelineIntent{}
	for _, stage := range p.Stages {
		for _, comp := range stage.Components {
			if in := comp.Inputs; in != nil {
				if len(in.Buckets) > 0 || len(in.Tables) > 0 {
					out.HasReads = true
				}
			}
			if outSpec := comp.Outputs; outSpec != nil {
				if outSpec.DefaultBucket != "" ||
					len(outSpec.Buckets) > 0 ||
					len(outSpec.Tables) > 0 {
					out.HasWrites = true
				}
			}
		}
	}
	return out
}
