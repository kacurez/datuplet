package controllers

// RFC 026 Task B6b: the kubectl path must run the SAME semantic checks as
// the pipeline-api save path (validate.ValidateTyped), not a second
// hand-rolled dialect.

import (
	"context"
	"strings"
	"testing"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestPipelineReconcile_InvalidBucketName_SetsInvalidPhase reconciles a
// Pipeline CR whose only defect is a bad output bucket name and asserts the
// controller surfaces validate.ValidateTyped's finding, not a hand-rolled
// check.
func TestPipelineReconcile_InvalidBucketName_SetsInvalidPhase(t *testing.T) {
	scheme := newTestScheme(t)

	pipeline := &datupletv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p1",
			Namespace: "default",
		},
		Spec: datupletv1.PipelineSpec{
			Stages: []datupletv1.StageSpec{{
				Name: "extract",
				Components: []datupletv1.ComponentSpec{{
					Name:      "c1",
					Component: "comp-a",
					Outputs: &datupletv1.OutputSpec{
						// Uppercase + underscore: fails validate's
						// bucketNameRegex (lowercase alnum + hyphens only).
						DefaultBucket: "Bad_Bucket",
					},
				}},
			}},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipeline).
		WithStatusSubresource(pipeline).
		Build()

	r := &PipelineReconciler{Client: fakeClient, Scheme: scheme}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "p1", Namespace: "default"},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &datupletv1.Pipeline{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "p1", Namespace: "default"}, got); err != nil {
		t.Fatalf("Get Pipeline: %v", err)
	}

	if got.Status.Phase != datupletv1.PipelinePhaseInvalid {
		t.Fatalf("phase = %q, want %q", got.Status.Phase, datupletv1.PipelinePhaseInvalid)
	}
	if !strings.Contains(got.Status.Message, "Bad_Bucket") {
		t.Errorf("message = %q, want it to contain the validate.Finding text about %q", got.Status.Message, "Bad_Bucket")
	}
}
