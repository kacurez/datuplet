package controllers

// RFC 026 P2 (Task R7): explicit controller error taxonomy at run admission
// (spec §7):
//
//   - spec/schema/policy validation failure, unresolvable component/version,
//     reference to an Invalid definition -> FailedUser (exit-code contract 1).
//   - registry informer not synced / K8s API errors (TRANSIENT, not
//     NotFound) -> requeue with backoff, run NOT failed; escalating to
//     FailedApplication (exit-code contract >=20) after
//     admissionTransientRetryBudget CONSECUTIVE occurrences.
//
// These tests pin both halves of the taxonomy and the boundary between them.

import (
	"context"
	stderrors "errors"
	"fmt"
	"testing"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// transientRegistryStub is a validate.RegistryView test double that always
// reports a TRANSIENT resolution failure (mirroring what ComponentRegistry.
// Resolve returns for a non-NotFound client.Reader.Get error), never a real
// definition. It never touches the fake client — admission tests exercising
// it don't need a ComponentDefinition object at all.
type transientRegistryStub struct {
	err error
}

func (s transientRegistryStub) Resolve(component, _ string) (*validate.ResolvedComponent, []validate.Finding) {
	return nil, []validate.Finding{{
		Path:     "component",
		Message:  fmt.Sprintf("component %q: %v", component, s.err),
		Severity: severityTransient,
	}}
}

func reconcileRun(t *testing.T, r *PipelineRunReconciler, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nnDefault(name)})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return res
}

func nnDefault(name string) types.NamespacedName {
	return types.NamespacedName{Name: name, Namespace: "default"}
}

// TestAdmission_TransientRegistryError_RequeuesWithoutFailingRun covers R7
// (b) first half: a registry Get failing with a transient (non-NotFound)
// error must requeue with backoff and must NOT fail the run.
func TestAdmission_TransientRegistryError_RequeuesWithoutFailingRun(t *testing.T) {
	scheme := newTestScheme(t)
	pipeline := singleStagePipeline("comp-a", "v1.0.0")
	pr := runFor("p1")
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pipeline, pr).
		WithStatusSubresource(pr).Build()
	r := &PipelineRunReconciler{
		Client:    c,
		APIReader: c,
		Scheme:    scheme,
		Registry:  transientRegistryStub{err: stderrors.New("etcd timeout")},
	}

	res := reconcileRun(t, r, "pr1")
	if !res.Requeue && res.RequeueAfter <= 0 {
		t.Fatalf("result = %+v, want Requeue or RequeueAfter set (backoff)", res)
	}

	got := getRun(t, c)
	if got.Status.Phase == datupletv1.PipelineRunPhaseFailedUser || got.Status.Phase == datupletv1.PipelineRunPhaseFailedApplication {
		t.Fatalf("phase = %q, want run NOT failed on a single transient error", got.Status.Phase)
	}
}

// TestAdmission_TransientRegistryError_EscalatesAfterConsecutiveBudget covers
// R7 (b) second half: after admissionTransientRetryBudget CONSECUTIVE
// transient failures, the run is failed FailedApplication (exit-code
// contract >=20) — a wedged registry must not requeue forever.
func TestAdmission_TransientRegistryError_EscalatesAfterConsecutiveBudget(t *testing.T) {
	scheme := newTestScheme(t)
	pipeline := singleStagePipeline("comp-a", "v1.0.0")
	pr := runFor("p1")
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pipeline, pr).
		WithStatusSubresource(pr).Build()
	r := &PipelineRunReconciler{
		Client:    c,
		APIReader: c,
		Scheme:    scheme,
		Registry:  transientRegistryStub{err: stderrors.New("etcd timeout")},
	}

	for i := 1; i < admissionTransientRetryBudget; i++ {
		res := reconcileRun(t, r, "pr1")
		if !res.Requeue && res.RequeueAfter <= 0 {
			t.Fatalf("attempt %d: result = %+v, want requeue (budget not yet exhausted)", i, res)
		}
		got := getRun(t, c)
		if got.Status.Phase == datupletv1.PipelineRunPhaseFailedApplication {
			t.Fatalf("attempt %d: run escalated to FailedApplication before the %d-attempt budget was exhausted", i, admissionTransientRetryBudget)
		}
	}

	// The admissionTransientRetryBudget-th CONSECUTIVE failure exhausts the
	// budget.
	reconcileRun(t, r, "pr1")
	got := getRun(t, c)
	if got.Status.Phase != datupletv1.PipelineRunPhaseFailedApplication {
		t.Fatalf("phase = %q, want FailedApplication after %d consecutive transient errors", got.Status.Phase, admissionTransientRetryBudget)
	}
}

// TestAdmission_ErrorTaxonomyBoundary_ValidationVsTransient asserts the R7
// classification boundary explicitly, side by side: an ordinary validation
// finding (unknown component) fails the run FailedUser; a transient registry
// error exhausted past the retry budget fails the run FailedApplication.
// Neither must ever produce the other's phase.
func TestAdmission_ErrorTaxonomyBoundary_ValidationVsTransient(t *testing.T) {
	t.Run("validation finding -> FailedUser", func(t *testing.T) {
		scheme := newTestScheme(t)
		pipeline := singleStagePipeline("ghost", "v1.0.0") // unresolvable: no ComponentDefinition
		pr := runFor("p1")
		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(pipeline, pr).
			WithStatusSubresource(pr).Build()
		r := registryReconciler(c, scheme)

		reconcileRun(t, r, "pr1")
		got := getRun(t, c)
		if got.Status.Phase != datupletv1.PipelineRunPhaseFailedUser {
			t.Fatalf("phase = %q, want FailedUser for an unresolvable-component validation finding", got.Status.Phase)
		}
	})

	t.Run("transient-exhausted -> FailedApplication", func(t *testing.T) {
		scheme := newTestScheme(t)
		pipeline := singleStagePipeline("comp-a", "v1.0.0")
		pr := runFor("p1")
		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(pipeline, pr).
			WithStatusSubresource(pr).Build()
		r := &PipelineRunReconciler{
			Client:    c,
			APIReader: c,
			Scheme:    scheme,
			Registry:  transientRegistryStub{err: stderrors.New("api server 503")},
		}

		for i := 1; i < admissionTransientRetryBudget; i++ {
			reconcileRun(t, r, "pr1")
		}
		reconcileRun(t, r, "pr1")
		got := getRun(t, c)
		if got.Status.Phase != datupletv1.PipelineRunPhaseFailedApplication {
			t.Fatalf("phase = %q, want FailedApplication for a transient-exhausted registry error", got.Status.Phase)
		}
		if got.Status.Phase == datupletv1.PipelineRunPhaseFailedUser {
			t.Fatal("a transient-exhausted registry error must never classify as FailedUser")
		}
	})
}
