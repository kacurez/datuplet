package controllers

// RFC 026 P2 (Task R6): component/version replace image; runs execute from a
// FROZEN status.resolvedSpec snapshotted at admission. These tests pin the
// resolve-&-freeze behaviour: cases (0) and (a)–(f) from the task brief.

import (
	"context"
	"strings"
	"testing"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// ver / preVer build VersionSpecs for a ComponentDefinition.
func ver(v, img string) datupletv1.VersionSpec  { return datupletv1.VersionSpec{Version: v, Image: img} }
func preVer(v, img string) datupletv1.VersionSpec {
	return datupletv1.VersionSpec{Version: v, Image: img, Prerelease: true}
}

// compDef builds a cluster-scoped, Valid ComponentDefinition.
func compDef(name string, versions ...datupletv1.VersionSpec) *datupletv1.ComponentDefinition {
	return &datupletv1.ComponentDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       datupletv1.ComponentDefinitionSpec{Versions: versions},
		Status:     datupletv1.ComponentDefinitionStatus{Phase: "Valid"},
	}
}

// registryReconciler wires a reconciler whose Registry adapter delegates to the
// ComponentDefinitions living in the fake client.
func registryReconciler(c client.Client, scheme *runtime.Scheme) *PipelineRunReconciler {
	return &PipelineRunReconciler{
		Client:    c,
		APIReader: c,
		Scheme:    scheme,
		Registry:  ComponentRegistry{Reader: c},
	}
}

// singleStagePipeline returns a one-stage pipeline whose component references
// the named registry component at the given (optional) version.
func singleStagePipeline(component, version string) *datupletv1.Pipeline {
	return &datupletv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "default"},
		Spec: datupletv1.PipelineSpec{
			Stages: []datupletv1.StageSpec{{
				Name: "extract",
				Components: []datupletv1.ComponentSpec{{
					Name:      "c1",
					Component: component,
					Version:   version,
					Outputs:   &datupletv1.OutputSpec{DefaultBucket: "raw"},
				}},
			}},
		},
	}
}

// twoStagePipeline returns a two-stage extract->load pipeline.
func twoStagePipeline() *datupletv1.Pipeline {
	return &datupletv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "default"},
		Spec: datupletv1.PipelineSpec{
			Stages: []datupletv1.StageSpec{
				{
					Name: "extract",
					Components: []datupletv1.ComponentSpec{{
						Name:      "c1",
						Component: "comp-a",
						Version:   "v1.0.0",
						Outputs:   &datupletv1.OutputSpec{DefaultBucket: "raw"},
					}},
				},
				{
					Name: "load",
					Components: []datupletv1.ComponentSpec{{
						Name:      "c2",
						Component: "comp-b",
						Version:   "v1.0.0",
						Inputs:    &datupletv1.InputSpec{Buckets: []string{"raw"}},
						Outputs:   &datupletv1.OutputSpec{DefaultBucket: "curated"},
					}},
				},
			},
		},
	}
}

func runFor(pipelineName string) *datupletv1.PipelineRun {
	return &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: "pr1", Namespace: "default"},
		Spec: datupletv1.PipelineRunSpec{
			PipelineRef: datupletv1.PipelineRef{Name: pipelineName},
			RunID:       "00000000-0000-0000-0000-0000000000f6",
		},
	}
}

func getRun(t *testing.T, c client.Client) *datupletv1.PipelineRun {
	t.Helper()
	got := &datupletv1.PipelineRun{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "pr1", Namespace: "default"}, got); err != nil {
		t.Fatalf("Get PipelineRun: %v", err)
	}
	return got
}

func componentContainerImage(job *batchv1.Job) (string, corev1.PullPolicy) {
	c := job.Spec.Template.Spec.Containers[0]
	return c.Image, c.ImagePullPolicy
}

// (0) status.stageStatuses is still populated at admission — one entry per
// stage — but sourced from the frozen resolvedSpec.Stages.
func TestFreeze_StageStatusesPopulatedFromFrozenSpec(t *testing.T) {
	scheme := newTestScheme(t)
	pipeline := twoStagePipeline()
	pr := runFor("p1")
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pipeline, pr, compDef("comp-a", ver("v1.0.0", "img-a:v1")), compDef("comp-b", ver("v1.0.0", "img-b:v1"))).
		WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "pr1", Namespace: "default"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := getRun(t, c)
	if got.Status.ResolvedSpec == nil {
		t.Fatal("ResolvedSpec is nil after admission")
	}
	if len(got.Status.StageStatuses) != 2 {
		t.Fatalf("StageStatuses len = %d, want 2 (one per frozen stage)", len(got.Status.StageStatuses))
	}
	for i, want := range []string{"extract", "load"} {
		if got.Status.StageStatuses[i].Name != want {
			t.Errorf("StageStatuses[%d].Name = %q, want %q", i, got.Status.StageStatuses[i].Name, want)
		}
		if got.Status.StageStatuses[i].Name != got.Status.ResolvedSpec.Stages[i].Name {
			t.Errorf("StageStatuses[%d] not sourced from resolvedSpec.Stages", i)
		}
		if got.Status.StageStatuses[i].Phase != datupletv1.StagePhasePending {
			t.Errorf("StageStatuses[%d].Phase = %q, want Pending", i, got.Status.StageStatuses[i].Phase)
		}
	}
}

// (a) valid run → resolvedSpec + components set with resolved image; the Job
// uses the resolved image.
func TestFreeze_ValidRun_ResolvedSpecAndJobImage(t *testing.T) {
	scheme := newTestScheme(t)
	pipeline := singleStagePipeline("comp-a", "v1.0.0")
	pr := runFor("p1")
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pipeline, pr, compDef("comp-a", ver("v1.0.0", "img-a:v1"))).
		WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)

	reconcileOnce(t, r) // admission
	got := getRun(t, c)
	if got.Status.ResolvedSpec == nil {
		t.Fatal("ResolvedSpec nil after admission")
	}
	if len(got.Status.Components) != 1 {
		t.Fatalf("Components len = %d, want 1", len(got.Status.Components))
	}
	rc := got.Status.Components[0]
	if rc.Name != "c1" || rc.Component != "comp-a" || rc.Version != "v1.0.0" || rc.Image != "img-a:v1" {
		t.Fatalf("resolved component = %+v, want {c1 comp-a v1.0.0 img-a:v1}", rc)
	}

	reconcileOnce(t, r) // startStage builds the Job
	job := getJob(t, c, componentJobName(got, "extract", "c1"))
	img, _ := componentContainerImage(job)
	if img != "img-a:v1" {
		t.Errorf("component container image = %q, want img-a:v1 (resolved)", img)
	}
}

// (b) omitted version → latest stable resolved & recorded.
func TestFreeze_OmittedVersion_LatestStable(t *testing.T) {
	scheme := newTestScheme(t)
	pipeline := singleStagePipeline("comp-a", "") // no version
	pr := runFor("p1")
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pipeline, pr, compDef("comp-a", ver("v0.1.0", "img-a:v0.1.0"), ver("v0.2.0", "img-a:v0.2.0"))).
		WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)

	reconcileOnce(t, r)
	got := getRun(t, c)
	if len(got.Status.Components) != 1 {
		t.Fatalf("Components len = %d, want 1", len(got.Status.Components))
	}
	rc := got.Status.Components[0]
	if rc.Version != "v0.2.0" || rc.Image != "img-a:v0.2.0" {
		t.Errorf("resolved = {%s %s}, want {v0.2.0 img-a:v0.2.0} (latest stable)", rc.Version, rc.Image)
	}
}

// (c) prerelease-resolved version → component container PullAlways; stable →
// PullIfNotPresent. The gateway sidecar policy is unaffected (runtimePullPolicy).
func TestFreeze_ComponentPullPolicy_RegistryDriven(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version string
		image   string
		def     *datupletv1.ComponentDefinition
		want    corev1.PullPolicy
	}{
		{"prerelease", "dev", "img-a:dev", compDef("comp-a", preVer("dev", "img-a:dev"), ver("v1.0.0", "img-a:v1")), corev1.PullAlways},
		{"stable", "v1.0.0", "img-a:v1", compDef("comp-a", preVer("dev", "img-a:dev"), ver("v1.0.0", "img-a:v1")), corev1.PullIfNotPresent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newTestScheme(t)
			pipeline := singleStagePipeline("comp-a", tc.version)
			pr := runFor("p1")
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(pipeline, pr, tc.def).
				WithStatusSubresource(pr).Build()
			r := registryReconciler(c, scheme)

			reconcileOnce(t, r) // admission
			reconcileOnce(t, r) // startStage
			got := getRun(t, c)
			job := getJob(t, c, componentJobName(got, "extract", "c1"))
			img, policy := componentContainerImage(job)
			if img != tc.image {
				t.Errorf("component image = %q, want %q", img, tc.image)
			}
			if policy != tc.want {
				t.Errorf("component ImagePullPolicy = %q, want %q", policy, tc.want)
			}
		})
	}
}

// (d) unknown component → run FailedUser, message names it, no Jobs, no
// resolvedSpec.
func TestFreeze_UnknownComponent_FailsUser_NoJobs(t *testing.T) {
	scheme := newTestScheme(t)
	pipeline := singleStagePipeline("ghost", "v1.0.0")
	pr := runFor("p1")
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pipeline, pr). // no ComponentDefinition for "ghost"
		WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)

	reconcileOnce(t, r)
	got := getRun(t, c)
	if got.Status.Phase != datupletv1.PipelineRunPhaseFailedUser {
		t.Fatalf("phase = %q, want FailedUser", got.Status.Phase)
	}
	if !strings.Contains(got.Status.Message, "ghost") {
		t.Errorf("message = %q, want it to name the unknown component %q", got.Status.Message, "ghost")
	}
	if got.Status.ResolvedSpec != nil {
		t.Error("ResolvedSpec must not be set on a failed admission")
	}
	jobs := &batchv1.JobList{}
	if err := c.List(context.Background(), jobs, client.InNamespace("default")); err != nil {
		t.Fatalf("List Jobs: %v", err)
	}
	if len(jobs.Items) != 0 {
		t.Errorf("expected 0 Jobs, found %d", len(jobs.Items))
	}
}

// (e) freeze: after admission, mutating the Pipeline spec AND the registry must
// not change what executes — later stages build from the frozen snapshot, and
// pipelineGeneration records the generation validated at admission.
func TestFreeze_MidRunMutation_JobsFromSnapshot(t *testing.T) {
	scheme := newTestScheme(t)
	pipeline := twoStagePipeline()
	pipeline.Generation = 3
	pr := runFor("p1")
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pipeline, pr, compDef("comp-a", ver("v1.0.0", "img-a:v1")), compDef("comp-b", ver("v1.0.0", "img-b:v1"))).
		WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)

	reconcileOnce(t, r) // admission freezes resolvedSpec at generation 3
	got := getRun(t, c)
	if got.Status.PipelineGeneration != 3 {
		t.Fatalf("PipelineGeneration = %d, want 3 (validated generation)", got.Status.PipelineGeneration)
	}

	// Mutate the registry: comp-b now points at a new image.
	defB := &datupletv1.ComponentDefinition{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "comp-b"}, defB); err != nil {
		t.Fatalf("Get comp-b: %v", err)
	}
	defB.Spec.Versions[0].Image = "img-b:v2"
	if err := c.Update(context.Background(), defB); err != nil {
		t.Fatalf("Update comp-b: %v", err)
	}
	// Mutate the live Pipeline spec.
	live := &datupletv1.Pipeline{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "p1", Namespace: "default"}, live); err != nil {
		t.Fatalf("Get pipeline: %v", err)
	}
	live.Generation = 4
	live.Spec.Stages[1].Components[0].Component = "comp-a" // would resolve differently if re-read
	if err := c.Update(context.Background(), live); err != nil {
		t.Fatalf("Update pipeline: %v", err)
	}

	// Mark stage 0 Succeeded so the next reconcile drives stage 1.
	got = getRun(t, c)
	got.Status.StageStatuses[0].Phase = datupletv1.StagePhaseSucceeded
	if err := c.Status().Update(context.Background(), got); err != nil {
		t.Fatalf("Status update: %v", err)
	}

	reconcileOnce(t, r) // handleRunning → startStage(1) from the frozen snapshot
	got = getRun(t, c)
	job := getJob(t, c, componentJobName(got, "load", "c2"))
	img, _ := componentContainerImage(job)
	if img != "img-b:v1" {
		t.Errorf("stage-1 component image = %q, want img-b:v1 (frozen; registry+spec mutation must not leak)", img)
	}
	if got.Status.PipelineGeneration != 3 {
		t.Errorf("PipelineGeneration = %d, want 3 (unchanged by live-spec bump)", got.Status.PipelineGeneration)
	}
}

// (f) mid-run reconcile never depends on the live Pipeline: deleting it after
// admission must not stop stages being built from the frozen snapshot.
func TestFreeze_LivePipelineDeleted_StillBuildsFromSnapshot(t *testing.T) {
	scheme := newTestScheme(t)
	pipeline := singleStagePipeline("comp-a", "v1.0.0")
	pr := runFor("p1")
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pipeline, pr, compDef("comp-a", ver("v1.0.0", "img-a:v1"))).
		WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)

	reconcileOnce(t, r) // admission
	got := getRun(t, c)

	// Delete the live Pipeline — a mid-run reconcile must not need it.
	if err := c.Delete(context.Background(), pipeline); err != nil {
		t.Fatalf("Delete pipeline: %v", err)
	}

	reconcileOnce(t, r) // startStage from the frozen snapshot; must not error on missing Pipeline
	job := getJob(t, c, componentJobName(got, "extract", "c1"))
	img, _ := componentContainerImage(job)
	if img != "img-a:v1" {
		t.Errorf("component image = %q, want img-a:v1 (built from frozen snapshot without the live Pipeline)", img)
	}
}

func reconcileOnce(t *testing.T, r *PipelineRunReconciler) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "pr1", Namespace: "default"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

func getJob(t *testing.T, c client.Client, name string) *batchv1.Job {
	t.Helper()
	job := &batchv1.Job{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, job); err != nil {
		t.Fatalf("Get Job %q: %v", name, err)
	}
	return job
}
