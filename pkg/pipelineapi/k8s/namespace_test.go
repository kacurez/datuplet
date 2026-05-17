package k8s_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
)

func TestEnsureProjectNamespace_CreatesIfMissing(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()

	projectID := uuid.New()
	if err := pkg8s.EnsureProjectNamespace(context.Background(), c, projectID); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	want := "datuplet-" + projectID.String()
	ns := &corev1.Namespace{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: want}, ns); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := ns.Labels["datuplet.io/project-id"]; got != projectID.String() {
		t.Errorf("label = %q, want %s", got, projectID)
	}
}

func TestEnsureProjectNamespace_Idempotent(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	projectID := uuid.New()
	if err := pkg8s.EnsureProjectNamespace(context.Background(), c, projectID); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Second call must not error even though the namespace now exists.
	if err := pkg8s.EnsureProjectNamespace(context.Background(), c, projectID); err != nil {
		t.Fatalf("second: %v", err)
	}
}
