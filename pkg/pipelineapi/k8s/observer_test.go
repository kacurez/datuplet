package k8s_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
)

// mockUpdater captures calls and lets the test control the applied bool.
type mockUpdater struct {
	calls   []pkg8s.RunStatus
	applied bool
	err     error
}

func (m *mockUpdater) Update(_ context.Context, s pkg8s.RunStatus) (bool, error) {
	m.calls = append(m.calls, s)
	return m.applied, m.err
}

func reconcileFor(t *testing.T, c client.Client, u pkg8s.RunStatusUpdater, ns, name string) {
	t.Helper()
	if _, err := pkg8s.ReconcileOneForTest(context.Background(), c, reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: ns, Name: name},
	}, u); err != nil {
		t.Fatalf("reconcileOne: %v", err)
	}
}

func TestReconcileOne_MirrorsStatusAndParsesRV(t *testing.T) {
	pid := uuid.New()
	ns := pkg8s.NamespaceForProject(pid)
	runID := uuid.New()
	pr := &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "etl-" + runID.String()[:8], Namespace: ns,
			Labels:          map[string]string{"datuplet.io/run-id": runID.String()},
			ResourceVersion: "42",
		},
		Status: datupletv1.PipelineRunStatus{Phase: "Running", CurrentStage: "extract"},
	}
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).WithObjects(pr).Build()
	mu := &mockUpdater{applied: true}
	reconcileFor(t, c, mu, ns, pr.Name)
	if len(mu.calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(mu.calls))
	}
	got := mu.calls[0]
	if got.RunID != runID || got.Phase != "Running" || got.ResourceVersion != 42 {
		t.Errorf("got %+v", got)
	}
}

func TestReconcileOne_ReturnsErrorOnUpdateFailureSoWorkqueueRetries(t *testing.T) {
	// Regression for codex P1: under the informer path there's no
	// guaranteed next event, so a swallowed Update error would leave
	// runs.phase stale forever. Returning the error triggers
	// controller-runtime's workqueue exponential-backoff retry.
	runID := uuid.New()
	pr := &datupletv1.PipelineRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "x", Namespace: "ns",
			Labels:          map[string]string{"datuplet.io/run-id": runID.String()},
			ResourceVersion: "7",
		},
		Status: datupletv1.PipelineRunStatus{Phase: "Succeeded"},
	}
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).WithObjects(pr).Build()
	boom := errors.New("boom")
	mu := &mockUpdater{err: boom}
	_, err := pkg8s.ReconcileOneForTest(context.Background(), c, reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "ns", Name: "x"},
	}, mu)
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want boom (return error so workqueue retries)", err)
	}
}

// TestReconcileOne_Skips: objects the observer must never mirror result in no
// Update call.
func TestReconcileOne_Skips(t *testing.T) {
	now := metav1.Now()
	runLabel := func() map[string]string { return map[string]string{"datuplet.io/run-id": uuid.New().String()} }
	cases := []struct {
		name string
		pr   *datupletv1.PipelineRun // nil = object does not exist
	}{
		{"empty_phase", &datupletv1.PipelineRun{
			ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "ns", Labels: runLabel(), ResourceVersion: "1"},
		}},
		{"deletion_timestamp", &datupletv1.PipelineRun{
			ObjectMeta: metav1.ObjectMeta{
				Name: "x", Namespace: "ns",
				DeletionTimestamp: &now,
				// DeletionTimestamp requires at least one finalizer in the
				// fake client — otherwise the object would be deleted.
				Finalizers:      []string{"datuplet.io/keep"},
				Labels:          runLabel(),
				ResourceVersion: "1",
			},
			Status: datupletv1.PipelineRunStatus{Phase: "Running"},
		}},
		{"missing_run_id_label", &datupletv1.PipelineRun{
			ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "ns", ResourceVersion: "1"},
			Status:     datupletv1.PipelineRunStatus{Phase: "Running"},
		}},
		// Object doesn't exist — NotFound path. reconcile returns cleanly
		// without calling Update; coalesce.Forget is wired separately via the
		// DeleteFunc handler.
		{"no_object", nil},
		// metadata.resourceVersion is opaque per K8s spec. On non-decimal
		// values we fail closed rather than writing with rv=0 (which would
		// disable the SQL guard).
		{"non_numeric_resource_version", &datupletv1.PipelineRun{
			ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "ns", Labels: runLabel(), ResourceVersion: "not-a-number"},
			Status:     datupletv1.PipelineRunStatus{Phase: "Running"},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := fake.NewClientBuilder().WithScheme(pkg8s.Scheme())
			if tc.pr != nil {
				b = b.WithObjects(tc.pr)
			}
			mu := &mockUpdater{applied: true}
			reconcileFor(t, b.Build(), mu, "ns", "x")
			if len(mu.calls) != 0 {
				t.Errorf("should skip; got %d calls", len(mu.calls))
			}
		})
	}
}
