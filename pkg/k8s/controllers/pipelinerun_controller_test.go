package controllers

// RFC 021 Task 7: After all components in a stage exit 0, the stage
// transitions Running -> Succeeded directly. No commit Job is created.

import (
	"context"
	"strings"
	"testing"
	"time"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// newTestScheme returns a runtime.Scheme with the datupletv1 types and
// the standard K8s batch + core types registered.
func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := datupletv1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	if err := batchv1.AddToScheme(s); err != nil {
		t.Fatalf("batchv1.AddToScheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1.AddToScheme: %v", err)
	}
	return s
}

// TestStageRunningToSucceededDirect: after all components in a stage exit 0
// the stage transitions Running -> Succeeded directly (RFC 021).
// No table-commit Job must be created.
func TestStageRunningToSucceededDirect(t *testing.T) {
	scheme := newTestScheme(t)

	// One pipeline, one stage, one component.
	pipeline := &datupletv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p1",
			Namespace: "default",
		},
		Status: datupletv1.PipelineStatus{Phase: datupletv1.PipelinePhaseReady},
		Spec: datupletv1.PipelineSpec{
			Stages: []datupletv1.StageSpec{{
				Name: "extract",
				Components: []datupletv1.ComponentSpec{{
					Name:      "c1",
					Component: "comp-a",
					Version:   "v1.0.0",
				}},
			}},
		},
	}

	// PipelineRun already in Running/stage-Running state, component succeeded.
	// The frozen resolvedSpec is what handleRunning reads (never the live Pipeline).
	pr := &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr1",
			Namespace: "default",
		},
		Spec: datupletv1.PipelineRunSpec{
			PipelineRef: datupletv1.PipelineRef{Name: "p1"},
		},
		Status: datupletv1.PipelineRunStatus{
			Phase:        datupletv1.PipelineRunPhaseRunning,
			RunID:        "00000000-0000-0000-0000-000000000001",
			ResolvedSpec: pipeline.Spec.DeepCopy(),
			StageStatuses: []datupletv1.StageStatus{{
				Name:  "extract",
				Phase: datupletv1.StagePhaseRunning,
				ComponentStatuses: []datupletv1.ComponentStatus{{
					Name:  "c1",
					Phase: datupletv1.ComponentPhaseSucceeded,
				}},
			}},
		},
	}

	// Pre-existing component Job (already complete) so the controller can
	// read it during checkStageComponents. Labels match what
	// componentJobName generates.
	jobName := componentJobName(pr, "extract", "c1")
	compJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: "default",
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{
				Type:   batchv1.JobComplete,
				Status: corev1.ConditionTrue,
			}},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipeline, pr, compJob).
		WithStatusSubresource(pr).
		Build()

	r := &PipelineRunReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	// Reconcile once: should advance the stage to Succeeded.
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pr1", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Reload the PipelineRun.
	got := &datupletv1.PipelineRun{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "pr1", Namespace: "default"}, got); err != nil {
		t.Fatalf("Get PipelineRun: %v", err)
	}

	if len(got.Status.StageStatuses) == 0 {
		t.Fatal("StageStatuses is empty after reconcile")
	}
	stagePhase := got.Status.StageStatuses[0].Phase
	if stagePhase != datupletv1.StagePhaseSucceeded {
		t.Errorf("stage phase = %q, want %q (Running->Succeeded direct, no Committing)", stagePhase, datupletv1.StagePhaseSucceeded)
	}

	// No table-commit Jobs must exist.
	jobList := &batchv1.JobList{}
	if err := fakeClient.List(context.Background(), jobList,
		client.InNamespace("default"),
		client.MatchingLabels{"app.kubernetes.io/component": "table-commit"},
	); err != nil {
		t.Fatalf("List Jobs: %v", err)
	}
	if len(jobList.Items) != 0 {
		t.Errorf("expected 0 table-commit Jobs, found %d", len(jobList.Items))
	}
}

// TestPipelineRunReconcile_InvalidPipeline_FailsUser_NoJobs: run admission
// runs validate.ValidateTyped on the fetched Pipeline itself — it does not
// gate on pipeline.Status.Phase (which the fake Pipeline below deliberately
// leaves unset). An invalid Pipeline must fail the run FailedUser with the
// first finding in the message, before any component Job is created.
func TestPipelineRunReconcile_InvalidPipeline_FailsUser_NoJobs(t *testing.T) {
	scheme := newTestScheme(t)

	pipeline := &datupletv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p1",
			Namespace: "default",
		},
		// Status.Phase intentionally left unset ("") — the run controller
		// must not rely on the Pipeline controller having reconciled.
		Spec: datupletv1.PipelineSpec{
			Stages: []datupletv1.StageSpec{{
				Name: "extract",
				Components: []datupletv1.ComponentSpec{{
					Name:      "c1",
					Component: "comp-a",
					Outputs: &datupletv1.OutputSpec{
						DefaultBucket: "Bad_Bucket",
					},
				}},
			}},
		},
	}

	pr := &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pr1",
			Namespace: "default",
		},
		Spec: datupletv1.PipelineRunSpec{
			PipelineRef: datupletv1.PipelineRef{Name: "p1"},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipeline, pr).
		WithStatusSubresource(pr).
		Build()

	r := &PipelineRunReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pr1", Namespace: "default"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &datupletv1.PipelineRun{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "pr1", Namespace: "default"}, got); err != nil {
		t.Fatalf("Get PipelineRun: %v", err)
	}

	if got.Status.Phase != datupletv1.PipelineRunPhaseFailedUser {
		t.Fatalf("phase = %q, want %q", got.Status.Phase, datupletv1.PipelineRunPhaseFailedUser)
	}
	if !strings.Contains(got.Status.Message, "Bad_Bucket") {
		t.Errorf("message = %q, want it to contain the first validate.Finding text about %q", got.Status.Message, "Bad_Bucket")
	}

	jobList := &batchv1.JobList{}
	if err := fakeClient.List(context.Background(), jobList, client.InNamespace("default")); err != nil {
		t.Fatalf("List Jobs: %v", err)
	}
	if len(jobList.Items) != 0 {
		t.Errorf("expected 0 Jobs created, found %d", len(jobList.Items))
	}
}

const testPreviewID = "22222222-2222-2222-2222-222222222222"

func inlineSpecRun() *datupletv1.PipelineRun {
	return &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: "pr1", Namespace: "default"},
		Spec: datupletv1.PipelineRunSpec{
			RunID:        testPreviewID,
			PipelineSpec: singleStagePipeline("data-generator", "v1.0.0").Spec.DeepCopy(),
			Preview:      &datupletv1.PreviewSpec{ID: testPreviewID, RowLimit: 1000, TimeoutSeconds: 60},
		},
	}
}

func reconcilePR1(t *testing.T, r *PipelineRunReconciler) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pr1", Namespace: "default"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// An inline-spec PipelineRun is admitted without any Pipeline CR existing.
func TestHandlePending_InlineSpec(t *testing.T) {
	scheme := newTestScheme(t)
	pr := inlineSpecRun()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pr, compDef("data-generator", ver("v1.0.0", "img-dg:v1"))).
		WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)

	reconcilePR1(t, r)

	got := getRun(t, c)
	if got.Status.Phase != datupletv1.PipelineRunPhaseRunning {
		t.Fatalf("phase = %q (message %q), want Running", got.Status.Phase, got.Status.Message)
	}
	if got.Status.ResolvedSpec == nil || len(got.Status.ResolvedSpec.Stages) != 1 {
		t.Fatalf("ResolvedSpec = %+v, want one stage", got.Status.ResolvedSpec)
	}
	if got.Status.ResolvedSpec.Stages[0].Name != "extract" {
		t.Errorf("ResolvedSpec.Stages[0].Name = %q, want %q", got.Status.ResolvedSpec.Stages[0].Name, "extract")
	}
	if got.Status.PipelineGeneration != 0 {
		t.Errorf("PipelineGeneration = %d, want 0 for an inline spec", got.Status.PipelineGeneration)
	}
}

func TestHandlePending_NoRefNoSpec_FailedUser(t *testing.T) {
	scheme := newTestScheme(t)
	pr := &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: "pr1", Namespace: "default"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pr).WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)

	reconcilePR1(t, r)

	got := getRun(t, c)
	if got.Status.Phase != datupletv1.PipelineRunPhaseFailedUser {
		t.Fatalf("phase = %q, want FailedUser", got.Status.Phase)
	}
	if got.Status.Message != "spec.pipelineRef or spec.pipelineSpec is required" {
		t.Errorf("message = %q", got.Status.Message)
	}
}

// A Running preview past its deadline is cancelled: pods annotated, the
// current stage's Jobs deleted, the run FailedUser with a timeout message.
func TestHandleRunning_PreviewDeadline(t *testing.T) {
	scheme := newTestScheme(t)
	pr := inlineSpecRun()
	started := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	pr.Status.RunID = testPreviewID
	jobName := componentJobName(pr, "extract", "c1")
	pr.Status = datupletv1.PipelineRunStatus{
		Phase:        datupletv1.PipelineRunPhaseRunning,
		RunID:        testPreviewID,
		StartTime:    &started,
		ResolvedSpec: pr.Spec.PipelineSpec.DeepCopy(),
		StageStatuses: []datupletv1.StageStatus{{
			Name:  "extract",
			Phase: datupletv1.StagePhaseRunning,
			ComponentStatuses: []datupletv1.ComponentStatus{{
				Name:    "c1",
				Phase:   datupletv1.ComponentPhaseRunning,
				PodName: jobName,
			}},
		}},
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: "default"}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: jobName + "-abcde", Namespace: "default",
		Labels: map[string]string{"datuplet.io/run-id": testPreviewID},
	}}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pr, job, pod).WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)

	reconcilePR1(t, r)

	got := getRun(t, c)
	if got.Status.Phase != datupletv1.PipelineRunPhaseFailedUser {
		t.Fatalf("phase = %q, want FailedUser", got.Status.Phase)
	}
	if !strings.HasPrefix(got.Status.Message, "preview timed out after 60s during stage ") {
		t.Errorf("message = %q", got.Status.Message)
	}
	if got.Status.CompletionTime == nil {
		t.Error("CompletionTime not set")
	}
	err := c.Get(context.Background(), types.NamespacedName{Name: jobName, Namespace: "default"}, &batchv1.Job{})
	if !errors.IsNotFound(err) {
		t.Errorf("stage Job still present (err=%v), want deleted", err)
	}
	gotPod := &corev1.Pod{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: pod.Name, Namespace: "default"}, gotPod); err != nil {
		t.Fatalf("Get pod: %v", err)
	}
	if gotPod.Annotations["datuplet.io/cancel"] != "true" {
		t.Errorf("pod cancel annotation = %q, want true", gotPod.Annotations["datuplet.io/cancel"])
	}
}

// Within its deadline, a preview run keeps running.
func TestHandleRunning_PreviewWithinDeadline(t *testing.T) {
	scheme := newTestScheme(t)
	pr := inlineSpecRun()
	started := metav1.NewTime(time.Now().Add(-10 * time.Second))
	pr.Status = datupletv1.PipelineRunStatus{
		Phase:        datupletv1.PipelineRunPhaseRunning,
		RunID:        testPreviewID,
		StartTime:    &started,
		ResolvedSpec: pr.Spec.PipelineSpec.DeepCopy(),
		StageStatuses: []datupletv1.StageStatus{{
			Name:              "extract",
			Phase:             datupletv1.StagePhaseRunning,
			ComponentStatuses: []datupletv1.ComponentStatus{{Name: "c1", Phase: datupletv1.ComponentPhaseRunning}},
		}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pr).WithStatusSubresource(pr).Build()
	r := registryReconciler(c, scheme)

	reconcilePR1(t, r)

	if got := getRun(t, c); got.Status.Phase != datupletv1.PipelineRunPhaseRunning {
		t.Fatalf("phase = %q (message %q), want Running", got.Status.Phase, got.Status.Message)
	}
}
