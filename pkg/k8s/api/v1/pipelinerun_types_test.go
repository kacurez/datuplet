package v1

import "testing"

func TestPipelineRunSpec_DeepCopyInto_RunTokenRef_NonNil(t *testing.T) {
	in := PipelineRunSpec{
		PipelineRef: PipelineRef{Name: "p"},
		RunTokenRef: &RunTokenRef{Name: "runtoken-abc"},
	}
	var out PipelineRunSpec
	in.DeepCopyInto(&out)

	if out.RunTokenRef == nil {
		t.Fatal("out.RunTokenRef is nil; expected deep copy")
	}
	if out.RunTokenRef == in.RunTokenRef {
		t.Fatal("out.RunTokenRef aliases in.RunTokenRef; expected distinct pointer")
	}
	if out.RunTokenRef.Name != "runtoken-abc" {
		t.Errorf("Name = %q, want %q", out.RunTokenRef.Name, "runtoken-abc")
	}

	out.RunTokenRef.Name = "mutated"
	if in.RunTokenRef.Name == "mutated" {
		t.Error("mutation on out leaked into in; deep copy is broken")
	}
}

func TestPipelineRunSpec_DeepCopyInto_RunTokenRef_Nil(t *testing.T) {
	in := PipelineRunSpec{PipelineRef: PipelineRef{Name: "p"}}
	var out PipelineRunSpec
	in.DeepCopyInto(&out)
	if out.RunTokenRef != nil {
		t.Errorf("out.RunTokenRef = %v, want nil", out.RunTokenRef)
	}
}

func TestRunTokenRef_DeepCopy_Nil(t *testing.T) {
	var in *RunTokenRef
	if got := in.DeepCopy(); got != nil {
		t.Errorf("nil.DeepCopy() = %v, want nil", got)
	}
}

func TestPipelineRunSpec_DeepCopyPreview(t *testing.T) {
	in := PipelineRunSpec{
		PipelineSpec: &PipelineSpec{
			Stages: []StageSpec{{
				Name:       "extract",
				Components: []ComponentSpec{{Name: "c1", Component: "data-generator"}},
			}},
		},
		Preview: &PreviewSpec{ID: "11111111-1111-1111-1111-111111111111", RowLimit: 1000, TimeoutSeconds: 300},
	}
	var out PipelineRunSpec
	in.DeepCopyInto(&out)

	in.PipelineSpec.Stages[0].Name = "mutated"
	in.Preview.RowLimit = 5

	if out.PipelineSpec == nil || out.PipelineSpec.Stages[0].Name != "extract" {
		t.Errorf("PipelineSpec not deep-copied: %+v", out.PipelineSpec)
	}
	if out.Preview == nil || out.Preview.RowLimit != 1000 {
		t.Errorf("Preview not deep-copied: %+v", out.Preview)
	}
	if out.Preview.ID != "11111111-1111-1111-1111-111111111111" || out.Preview.TimeoutSeconds != 300 {
		t.Errorf("Preview fields lost: %+v", out.Preview)
	}
}
