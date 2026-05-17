package tokens_test

import (
	"testing"

	"github.com/datuplet/datuplet/pkg/pipeline/config"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// TestPipelineIntentFromPipeline pins the read/write classification used
// by run-trigger to choose between an `editor` and a `viewer` FGA tuple
// for the synthetic run user.
func TestPipelineIntentFromPipeline(t *testing.T) {
	tests := []struct {
		name     string
		pipeline *config.Pipeline
		want     tokens.PipelineIntent
	}{
		{
			name:     "nil pipeline → zero",
			pipeline: nil,
			want:     tokens.PipelineIntent{},
		},
		{
			name:     "empty pipeline → zero",
			pipeline: &config.Pipeline{},
			want:     tokens.PipelineIntent{},
		},
		{
			name: "explicit input + output → both",
			pipeline: &config.Pipeline{Spec: config.Spec{Stages: []config.Stage{{
				Components: []config.Component{{
					Inputs: &config.InputSpec{Tables: []config.InputTableSpec{
						{Bucket: "raw", Table: "events"},
					}},
					Outputs: &config.OutputSpec{Tables: []config.OutputTableSpec{
						{Bucket: "curated", Name: "summary", WriteMode: "FULL_LOAD"},
					}},
				}},
			}}}},
			want: tokens.PipelineIntent{HasReads: true, HasWrites: true},
		},
		{
			name: "bucket-only inputs count as reads",
			pipeline: &config.Pipeline{Spec: config.Spec{Stages: []config.Stage{{
				Components: []config.Component{{
					Inputs: &config.InputSpec{Buckets: []string{"raw"}},
				}},
			}}}},
			want: tokens.PipelineIntent{HasReads: true},
		},
		{
			name: "defaultBucket counts as a write",
			pipeline: &config.Pipeline{Spec: config.Spec{Stages: []config.Stage{{
				Components: []config.Component{{
					Outputs: &config.OutputSpec{DefaultBucket: "raw"},
				}},
			}}}},
			want: tokens.PipelineIntent{HasWrites: true},
		},
		{
			name: "outputs.buckets[] counts as a write",
			pipeline: &config.Pipeline{Spec: config.Spec{Stages: []config.Stage{{
				Components: []config.Component{{
					Outputs: &config.OutputSpec{Buckets: []config.OutputBucketSpec{{Name: "raw"}}},
				}},
			}}}},
			want: tokens.PipelineIntent{HasWrites: true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tokens.PipelineIntentFromPipeline(tc.pipeline)
			if got != tc.want {
				t.Errorf("intent: got=%+v want=%+v", got, tc.want)
			}
		})
	}
}
