package controllers

// RFC 021 Task 7: After all components in a stage exit 0, the stage
// transitions Running -> Succeeded directly. No commit Job is created.

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
