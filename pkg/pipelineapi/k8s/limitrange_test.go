package k8s_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
)

func TestEnsureProjectLimitRange_CreatesIfMissing(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	projectID := uuid.New()

	if err := pkg8s.EnsureProjectLimitRange(context.Background(), c, projectID); err != nil {
		t.Fatalf("EnsureProjectLimitRange: %v", err)
	}

	ns := pkg8s.NamespaceForProject(projectID)
	lr := &corev1.LimitRange{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: pkg8s.ProjectLimitRangeName, Namespace: ns}, lr); err != nil {
		t.Fatalf("Get LimitRange: %v", err)
	}
	if len(lr.Spec.Limits) != 1 {
		t.Fatalf("Limits = %d, want 1", len(lr.Spec.Limits))
	}
	item := lr.Spec.Limits[0]
	if item.Type != corev1.LimitTypeContainer {
		t.Errorf("Type = %q, want Container", item.Type)
	}
	if got := item.Default[corev1.ResourceCPU]; got.Cmp(resource.MustParse("1")) != 0 {
		t.Errorf("default cpu = %s, want 1", got.String())
	}
	if got := item.Default[corev1.ResourceMemory]; got.Cmp(resource.MustParse("512Mi")) != 0 {
		t.Errorf("default memory = %s, want 512Mi", got.String())
	}
	if got := item.DefaultRequest[corev1.ResourceCPU]; got.Cmp(resource.MustParse("100m")) != 0 {
		t.Errorf("defaultRequest cpu = %s, want 100m", got.String())
	}
	if got := item.DefaultRequest[corev1.ResourceMemory]; got.Cmp(resource.MustParse("128Mi")) != 0 {
		t.Errorf("defaultRequest memory = %s, want 128Mi", got.String())
	}
	// Belt-and-braces: NO max (would fight the registry-driven controller clamp).
	if len(item.Max) != 0 {
		t.Errorf("Max = %v, want empty (no max on the belt-and-braces LimitRange)", item.Max)
	}
}

func TestEnsureProjectLimitRange_Idempotent(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	projectID := uuid.New()

	if err := pkg8s.EnsureProjectLimitRange(context.Background(), c, projectID); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Second call must be a no-op even though the LimitRange now exists.
	if err := pkg8s.EnsureProjectLimitRange(context.Background(), c, projectID); err != nil {
		t.Fatalf("second: %v", err)
	}
}

func TestEnsureProjectLimitRange_LeavesExistingUntouched(t *testing.T) {
	projectID := uuid.New()
	ns := pkg8s.NamespaceForProject(projectID)
	preexisting := &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Name: pkg8s.ProjectLimitRangeName, Namespace: ns},
		Spec: corev1.LimitRangeSpec{
			Limits: []corev1.LimitRangeItem{{
				Type: corev1.LimitTypeContainer,
				Default: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("4"),
				},
			}},
		},
	}
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).WithObjects(preexisting).Build()

	if err := pkg8s.EnsureProjectLimitRange(context.Background(), c, projectID); err != nil {
		t.Fatalf("EnsureProjectLimitRange: %v", err)
	}

	lr := &corev1.LimitRange{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: pkg8s.ProjectLimitRangeName, Namespace: ns}, lr); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := lr.Spec.Limits[0].Default[corev1.ResourceCPU]; got.Cmp(resource.MustParse("4")) != 0 {
		t.Errorf("pre-existing LimitRange was overwritten: default cpu = %s, want 4 (untouched)", got.String())
	}
}

// EnsureProjectNamespace must provision the LimitRange too, so every caller
// (run trigger + admin create-project --with-namespace) gets it automatically.
// It must also still provision the Phase-1.5 secret RBAC (regression guard).
func TestEnsureProjectNamespace_CreatesLimitRange(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	projectID := uuid.New()

	if err := pkg8s.EnsureProjectNamespace(context.Background(), c, projectID); err != nil {
		t.Fatalf("EnsureProjectNamespace: %v", err)
	}

	ns := pkg8s.NamespaceForProject(projectID)
	lr := &corev1.LimitRange{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: pkg8s.ProjectLimitRangeName, Namespace: ns}, lr); err != nil {
		t.Fatalf("LimitRange not provisioned by EnsureProjectNamespace: %v", err)
	}
}
