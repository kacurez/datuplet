package k8s_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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

// projectNS builds a project-labelled Namespace (the marker the reaper
// iterates on now that Secret listing is per-namespace, not cluster-wide).
func projectNS(name string) *corev1.Namespace {
	return &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{"datuplet.io/project-id": "p"},
		},
	}
}

func runSecret(name, ns string, age time.Duration, owned bool) *corev1.Secret {
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: ns,
			Labels:            map[string]string{"datuplet.io/run-id": name},
			CreationTimestamp: metav1.Time{Time: time.Now().Add(-age)},
		},
	}
	if owned {
		s.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "datuplet.io/v1", Kind: "PipelineRun", Name: "owner", UID: "u",
		}}
	}
	return s
}

// TestReapOnce_SweepsOrphanSecretsPerNamespace pins the RFC 026 P1.5 change:
// the reaper finds run-token/snapshot Secrets by iterating project-labelled
// namespaces and listing per-namespace (authorized by the datuplet-secrets
// Role), NOT by a cluster-wide Secret list. The orphan/owner/cutoff logic is
// unchanged.
func TestReapOnce_SweepsOrphanSecretsPerNamespace(t *testing.T) {
	oldOrphan := runSecret("old-orphan", "datuplet-proj", 25*time.Hour, false)  // reaped
	youngOrphan := runSecret("young-orphan", "datuplet-proj", time.Hour, false) // kept (fresh)
	ownedOld := runSecret("owned-old", "datuplet-proj", 25*time.Hour, true)     // kept (GC owns it)
	// A Secret in a namespace WITHOUT the project-id label must never be
	// visited — this is exactly what the cluster-wide→per-namespace switch
	// guarantees.
	foreignOrphan := runSecret("foreign-orphan", "unmanaged-ns", 25*time.Hour, false) // kept

	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).WithObjects(
		projectNS("datuplet-proj"),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "unmanaged-ns"}},
		oldOrphan, youngOrphan, ownedOld, foreignOrphan,
	).Build()

	if err := pkg8s.ReapOnce(context.Background(), c, 24*time.Hour, nil); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}

	exists := func(name, ns string) bool {
		err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: ns}, &corev1.Secret{})
		return !apierrors.IsNotFound(err)
	}
	if exists("old-orphan", "datuplet-proj") {
		t.Error("old orphan Secret in project namespace should have been reaped")
	}
	if !exists("young-orphan", "datuplet-proj") {
		t.Error("young orphan Secret should be kept (under cutoff)")
	}
	if !exists("owned-old", "datuplet-proj") {
		t.Error("owner-referenced Secret should be kept (GC handles it)")
	}
	if !exists("foreign-orphan", "unmanaged-ns") {
		t.Error("orphan Secret in a non-project namespace must NOT be reaped")
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
