package controllers

// RFC 026 P3 (Task T7): effective resources are computed ONCE at run admission
// and frozen into status.resolvedSpec; buildComponentJob consumes the snapshot
// verbatim, so a mid-run registry edit cannot change any stage's resources.

import (
	"context"
	"reflect"
	"strings"
	"testing"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// compDefWithMax builds a Valid ComponentDefinition whose single version bounds
// resources with the given Max.
func compDefWithMax(name, version, image string, max corev1.ResourceList) *datupletv1.ComponentDefinition {
	return &datupletv1.ComponentDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: datupletv1.ComponentDefinitionSpec{Versions: []datupletv1.VersionSpec{{
			Version:   version,
			Image:     image,
			Resources: &datupletv1.ComponentResources{Max: max},
		}}},
		Status: datupletv1.ComponentDefinitionStatus{Phase: "Valid"},
	}
}

// Admission: an over-Max spec is clamped into the frozen resolvedSpec and the
// run message records the clamp; the run still proceeds (Running).
func TestClampAdmission_OverMaxResourcesClampedAndNoted(t *testing.T) {
	scheme := newTestScheme(t)
	const gpu = corev1.ResourceName("nvidia.com/gpu")
	pipeline := singleStagePipeline("comp-a", "v1.0.0")
	pipeline.Spec.Stages[0].Components[0].Resources = &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4"), gpu: resource.MustParse("1")},
	}
	pr := runFor("p1")
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pipeline, pr, compDefWithMax("comp-a", "v1.0.0", "img-a:v1", corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")})).
		WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)

	reconcileOnce(t, r) // admission
	got := getRun(t, c)
	if got.Status.ResolvedSpec == nil {
		t.Fatal("ResolvedSpec nil after admission")
	}
	rr := got.Status.ResolvedSpec.Stages[0].Components[0].Resources
	if rr == nil {
		t.Fatal("frozen component Resources nil")
	}
	if cpu := rr.Limits[corev1.ResourceCPU]; cpu.Cmp(resource.MustParse("2")) != 0 {
		t.Errorf("frozen limits.cpu = %s, want 2 (clamped to Max)", cpu.String())
	}
	if _, ok := rr.Limits[gpu]; ok {
		t.Errorf("nvidia.com/gpu should be stripped from frozen resources: %v", rr.Limits)
	}
	if !strings.Contains(got.Status.Message, "resources clamped for component c1") {
		t.Errorf("status.message = %q, want it to note the clamp for component c1", got.Status.Message)
	}
	if !strings.Contains(got.Status.Message, "cpu") {
		t.Errorf("status.message = %q, want it to name a changed resource (cpu)", got.Status.Message)
	}
	if got.Status.Phase != datupletv1.PipelineRunPhaseRunning {
		t.Errorf("phase = %q, want Running (a clamped run still proceeds)", got.Status.Phase)
	}
}

// Verbatim: buildComponentJob copies the frozen snapshot's Resources onto the
// container byte-for-byte and adds no clamp of its own.
func TestClampBuildComponentJob_ConsumesSnapshotVerbatim(t *testing.T) {
	r := &PipelineRunReconciler{}
	pr := minimalPipelineRun()
	comp := &pr.Status.ResolvedSpec.Stages[0].Components[0]
	comp.Resources = &corev1.ResourceRequirements{
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
	}

	job, _, err := r.buildComponentJob(context.Background(), pr, comp)
	if err != nil {
		t.Fatalf("buildComponentJob: %v", err)
	}
	got := job.Spec.Template.Spec.Containers[0].Resources
	if !reflect.DeepEqual(got, *comp.Resources) {
		t.Errorf("container Resources = %+v, want the snapshot value %+v (build must add no clamp)", got, *comp.Resources)
	}
}

// Freeze: raising the registry Max AFTER admission must not change what the
// Job gets — later reconciles build from the admission-time clamp, never the
// live registry.
func TestClampFreeze_RegistryMaxRaisedAfterAdmission_JobKeepsAdmissionClamp(t *testing.T) {
	scheme := newTestScheme(t)
	pipeline := singleStagePipeline("comp-a", "v1.0.0")
	pipeline.Spec.Stages[0].Components[0].Resources = &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
	}
	pr := runFor("p1")
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pipeline, pr, compDefWithMax("comp-a", "v1.0.0", "img-a:v1", corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")})).
		WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)

	reconcileOnce(t, r) // admission clamps cpu 4→2 into the frozen snapshot

	// Raise the registry Max AFTER admission. A re-read at build time would
	// now leave cpu at 4 (4 < 8); the freeze must ignore this edit.
	def := &datupletv1.ComponentDefinition{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "comp-a"}, def); err != nil {
		t.Fatalf("Get comp-a: %v", err)
	}
	def.Spec.Versions[0].Resources.Max[corev1.ResourceCPU] = resource.MustParse("8")
	if err := c.Update(context.Background(), def); err != nil {
		t.Fatalf("Update comp-a: %v", err)
	}

	reconcileOnce(t, r) // startStage builds the Job from the frozen snapshot
	got := getRun(t, c)
	job := getJob(t, c, componentJobName(got, "extract", "c1"))
	cpu := job.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU]
	if cpu.Cmp(resource.MustParse("2")) != 0 {
		t.Errorf("Job limits.cpu = %s, want 2 (admission-time clamp; registry NOT re-read)", cpu.String())
	}
}
