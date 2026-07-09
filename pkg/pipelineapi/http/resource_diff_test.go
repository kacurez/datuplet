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

func cpuLimit(v string) *corev1.ResourceRequirements {
	return &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(v)},
	}
}

func TestResourcesModified_AddBlock(t *testing.T) {
	oldP := pipelineWithResources("c1", nil)
	newP := pipelineWithResources("c1", cpuLimit("2"))
	if !resourcesModified(oldP, newP) {
		t.Error("adding a resources block where old had none should count as modified")
	}
}

func TestResourcesModified_AlterBlock(t *testing.T) {
	oldP := pipelineWithResources("c1", cpuLimit("1"))
	newP := pipelineWithResources("c1", cpuLimit("2"))
	if !resourcesModified(oldP, newP) {
		t.Error("changing limits.cpu should count as modified")
	}
}

func TestResourcesModified_RemoveBlock(t *testing.T) {
	oldP := pipelineWithResources("c1", cpuLimit("1"))
	newP := pipelineWithResources("c1", nil)
	if !resourcesModified(oldP, newP) {
		t.Error("removing an existing resources block should count as modified")
	}
}

func TestResourcesModified_ComponentRemoved(t *testing.T) {
	oldP := pipelineWithResources("c1", cpuLimit("1"))
	newP := pipelineWithResources("c2", nil) // c1 (which had resources) gone
	if !resourcesModified(oldP, newP) {
		t.Error("removing a component that had resources should count as modified")
	}
}

func TestResourcesModified_SemanticEqualUnchanged(t *testing.T) {
	oldP := pipelineWithResources("c1", cpuLimit("1"))
	newP := pipelineWithResources("c1", cpuLimit("1000m"))
	if resourcesModified(oldP, newP) {
		t.Error(`"1" and "1000m" are the same quantity — must NOT count as modified`)
	}
}

func TestResourcesModified_BothNilUnchanged(t *testing.T) {
	oldP := pipelineWithResources("c1", nil)
	newP := pipelineWithResources("c1", nil)
	if resourcesModified(oldP, newP) {
		t.Error("no resources on either side must NOT count as modified")
	}
}
