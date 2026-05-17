package k8s_test

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
)

func TestReapOnce_DeletesOldPipelineRuns(t *testing.T) {
	old := &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "old", Namespace: "ns1",
			Labels: map[string]string{"datuplet.io/run-id": "old"},
		},
		Spec: datupletv1.PipelineRunSpec{PipelineRef: datupletv1.PipelineRef{Name: "p"}},
		Status: datupletv1.PipelineRunStatus{
			StartTime: &metav1.Time{Time: time.Now().Add(-25 * time.Hour)},
		},
	}
	young := &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "young", Namespace: "ns1",
			Labels: map[string]string{"datuplet.io/run-id": "young"},
		},
		Spec: datupletv1.PipelineRunSpec{PipelineRef: datupletv1.PipelineRef{Name: "p"}},
		Status: datupletv1.PipelineRunStatus{
			StartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Hour)},
		},
	}
	// A third, unlabeled run (e.g., hand-crafted via kubectl) must NOT be
	// reaped even if old — pipeline-api only manages its own runs.
	foreign := &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "ns1"},
		Spec:       datupletv1.PipelineRunSpec{PipelineRef: datupletv1.PipelineRef{Name: "p"}},
		Status: datupletv1.PipelineRunStatus{
			StartTime: &metav1.Time{Time: time.Now().Add(-30 * time.Hour)},
		},
	}
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).WithObjects(old, young, foreign).Build()

	if err := pkg8s.ReapOnce(context.Background(), c, 24*time.Hour, nil); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}

	list := &datupletv1.PipelineRunList{}
	_ = c.List(context.Background(), list)
	names := map[string]bool{}
	for _, pr := range list.Items {
		names[pr.Name] = true
	}
	if names["old"] {
		t.Errorf("old labeled run should have been reaped; names=%v", names)
	}
	if !names["young"] {
		t.Errorf("young labeled run should have been kept")
	}
	if !names["foreign"] {
		t.Errorf("unlabeled foreign run should have been kept (pipeline-api must not reap it)")
	}
}

func TestReapOnce_IgnoresPipelineRunsWithoutStartTime(t *testing.T) {
	// StartTime nil, phase Pending — not yet started. Reaper must not
	// delete these even if they're old (slow bootstrap is legitimate).
	pr := &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pending", Namespace: "ns1",
			Labels:            map[string]string{"datuplet.io/run-id": "pending"},
			CreationTimestamp: metav1.Time{Time: time.Now().Add(-30 * time.Hour)},
		},
		Spec: datupletv1.PipelineRunSpec{PipelineRef: datupletv1.PipelineRef{Name: "p"}},
		// Phase unset / not terminal.
	}
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).WithObjects(pr).Build()
	if err := pkg8s.ReapOnce(context.Background(), c, 24*time.Hour, nil); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	list := &datupletv1.PipelineRunList{}
	_ = c.List(context.Background(), list)
	if len(list.Items) != 1 {
		t.Errorf("pending PipelineRun was deleted by reaper")
	}
}

func TestReapOnce_DeletesTerminalRunsEvenWithoutStartTime(t *testing.T) {
	// Validation-failed run: FailedUser, no StartTime, creationTimestamp > maxAge.
	// Must be reaped so its owner-referenced Secret gets GC'd.
	pr := &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "invalid", Namespace: "ns1",
			Labels:            map[string]string{"datuplet.io/run-id": "invalid"},
			CreationTimestamp: metav1.Time{Time: time.Now().Add(-25 * time.Hour)},
		},
		Spec: datupletv1.PipelineRunSpec{PipelineRef: datupletv1.PipelineRef{Name: "p"}},
		Status: datupletv1.PipelineRunStatus{
			Phase: datupletv1.PipelineRunPhaseFailedUser,
		},
	}
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).WithObjects(pr).Build()
	if err := pkg8s.ReapOnce(context.Background(), c, 24*time.Hour, nil); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	list := &datupletv1.PipelineRunList{}
	_ = c.List(context.Background(), list)
	if len(list.Items) != 0 {
		t.Errorf("terminal run without startTime should be reaped; remaining=%v", list.Items)
	}
}

func TestReapOnce_PrefersCompletionTimeForTerminalRuns(t *testing.T) {
	// A run that finished 10 minutes ago but started 25h ago should be
	// kept — age is measured from completionTime, not startTime.
	pr := &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "recent-finish", Namespace: "ns1",
			Labels: map[string]string{"datuplet.io/run-id": "recent-finish"},
		},
		Spec: datupletv1.PipelineRunSpec{PipelineRef: datupletv1.PipelineRef{Name: "p"}},
		Status: datupletv1.PipelineRunStatus{
			Phase:          datupletv1.PipelineRunPhaseSucceeded,
			StartTime:      &metav1.Time{Time: time.Now().Add(-25 * time.Hour)},
			CompletionTime: &metav1.Time{Time: time.Now().Add(-10 * time.Minute)},
		},
	}
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).WithObjects(pr).Build()
	if err := pkg8s.ReapOnce(context.Background(), c, 24*time.Hour, nil); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	list := &datupletv1.PipelineRunList{}
	_ = c.List(context.Background(), list)
	if len(list.Items) != 1 {
		t.Errorf("terminal run completed 10m ago should NOT be reaped (startTime is 25h ago but completion was fresh)")
	}
}
