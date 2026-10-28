package http

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
)

// pipelineWithResources builds a single-stage pipeline whose one component
// instance is named comp and carries the given resources block (nil = no block).
func pipelineWithResources(comp string, rr *corev1.ResourceRequirements) *datupletv1.Pipeline {
	return &datupletv1.Pipeline{
		Spec: datupletv1.PipelineSpec{
			Stages: []datupletv1.StageSpec{{
				Name: "s0",
				Components: []datupletv1.ComponentSpec{{
					Name:      comp,
					Component: "datuplet/test:latest",
					Resources: rr,
				}},
			}},
		},
	}
}

// stageComp names a single component instance placed in a named stage.
type stageComp struct {
	stage string
	name  string
	rr    *corev1.ResourceRequirements
}

// pipelineFromStages builds a pipeline with one component instance per entry,
// each in its own StageSpec (entries sharing a stage name land in the same
// stage in order).
func pipelineFromStages(entries ...stageComp) *datupletv1.Pipeline {
	byStage := map[string]int{}
	var stages []datupletv1.StageSpec
	for _, e := range entries {
		comp := datupletv1.ComponentSpec{
			Name:      e.name,
			Component: "datuplet/test:latest",
			Resources: e.rr,
		}
		if idx, ok := byStage[e.stage]; ok {
			stages[idx].Components = append(stages[idx].Components, comp)
			continue
		}
		byStage[e.stage] = len(stages)
		stages = append(stages, datupletv1.StageSpec{
			Name:       e.stage,
			Components: []datupletv1.ComponentSpec{comp},
		})
	}
	return &datupletv1.Pipeline{Spec: datupletv1.PipelineSpec{Stages: stages}}
}

func cpuLimit(v string) *corev1.ResourceRequirements {
	return &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(v)},
	}
}

func TestResourcesModified(t *testing.T) {
	tests := []struct {
		name string
		oldP *datupletv1.Pipeline
		newP *datupletv1.Pipeline
		want bool
	}{
		{"add block where old had none", pipelineWithResources("c1", nil), pipelineWithResources("c1", cpuLimit("2")), true},
		{"alter limits.cpu", pipelineWithResources("c1", cpuLimit("1")), pipelineWithResources("c1", cpuLimit("2")), true},
		{"remove existing block", pipelineWithResources("c1", cpuLimit("1")), pipelineWithResources("c1", nil), true},
		// c1, which had resources, is gone.
		{"component with resources removed", pipelineWithResources("c1", cpuLimit("1")), pipelineWithResources("c2", nil), true},
		// "1" and "1000m" are the same quantity.
		{"semantically equal quantities", pipelineWithResources("c1", cpuLimit("1")), pipelineWithResources("c1", cpuLimit("1000m")), false},
		{"no resources on either side", pipelineWithResources("c1", nil), pipelineWithResources("c1", nil), false},
		// Guards the bypass a bare-name key allowed: old has stage1/c1 with NO
		// block and stage2/c1 WITH block R; a non-superadmin PUT copies R onto
		// stage1/c1. Keying by (stage, name) must see stage1/c1 gaining a block;
		// a bare-name key would collapse both c1 instances and miss it.
		{
			"cross-stage same name no bypass",
			pipelineFromStages(
				stageComp{stage: "stage1", name: "c1", rr: nil},
				stageComp{stage: "stage2", name: "c1", rr: cpuLimit("2")},
			),
			pipelineFromStages(
				stageComp{stage: "stage1", name: "c1", rr: cpuLimit("2")},
				stageComp{stage: "stage2", name: "c1", rr: cpuLimit("2")},
			),
			true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := resourcesModified(tc.oldP, tc.newP); got != tc.want {
				t.Errorf("resourcesModified = %v, want %v", got, tc.want)
			}
		})
	}
}
