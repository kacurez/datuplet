package k8s_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

// getRole / getRoleBinding fetch a namespaced RBAC object for assertions.
func getRole(t *testing.T, c client.Client, ns, name string) *rbacv1.Role {
	t.Helper()
	r := &rbacv1.Role{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: ns}, r); err != nil {
		t.Fatalf("get Role %s/%s: %v", ns, name, err)
	}
	return r
}

func getRoleBinding(t *testing.T, c client.Client, ns, name string) *rbacv1.RoleBinding {
	t.Helper()
	rb := &rbacv1.RoleBinding{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: ns}, rb); err != nil {
		t.Fatalf("get RoleBinding %s/%s: %v", ns, name, err)
	}
	return rb
}

func TestEnsureProjectNamespace_CreatesSecretsRBAC(t *testing.T) {
	// POD_NAMESPACE pins the install namespace so the RoleBinding subject
	// namespace is deterministic regardless of where the test runs.
	t.Setenv("POD_NAMESPACE", "datuplet-install")
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()

	projectID := uuid.New()
	if err := pkg8s.EnsureProjectNamespace(context.Background(), c, projectID); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	ns := "datuplet-" + projectID.String()

	// pipeline-api Role: full lifecycle on secrets.
	apiRole := getRole(t, c, ns, "datuplet-secrets")
	if len(apiRole.Rules) != 1 {
		t.Fatalf("datuplet-secrets: want 1 rule, got %d", len(apiRole.Rules))
	}
	if got := apiRole.Rules[0].Resources; !reflect.DeepEqual(got, []string{"secrets"}) {
		t.Errorf("datuplet-secrets resources = %v, want [secrets]", got)
	}
	wantAPI := []string{"get", "list", "create", "update", "patch", "delete"}
	if got := apiRole.Rules[0].Verbs; !reflect.DeepEqual(got, wantAPI) {
		t.Errorf("datuplet-secrets verbs = %v, want %v", got, wantAPI)
	}

	// operator Role: get + create only (least privilege).
	opRole := getRole(t, c, ns, "datuplet-secrets-operator")
	wantOp := []string{"get", "create"}
	if got := opRole.Rules[0].Verbs; !reflect.DeepEqual(got, wantOp) {
		t.Errorf("datuplet-secrets-operator verbs = %v, want %v", got, wantOp)
	}

	// RoleBinding: pipeline-api SA -> datuplet-secrets.
	apiRB := getRoleBinding(t, c, ns, "datuplet-secrets")
	if apiRB.RoleRef.Name != "datuplet-secrets" || apiRB.RoleRef.Kind != "Role" {
		t.Errorf("datuplet-secrets binding roleRef = %+v, want Role/datuplet-secrets", apiRB.RoleRef)
	}
	if len(apiRB.Subjects) != 1 ||
		apiRB.Subjects[0].Kind != "ServiceAccount" ||
		apiRB.Subjects[0].Name != "pipeline-api" ||
		apiRB.Subjects[0].Namespace != "datuplet-install" {
		t.Errorf("datuplet-secrets binding subject = %+v, want ServiceAccount pipeline-api/datuplet-install", apiRB.Subjects)
	}

	// RoleBinding: pipeline-operator SA -> datuplet-secrets-operator.
	opRB := getRoleBinding(t, c, ns, "datuplet-secrets-operator")
	if opRB.RoleRef.Name != "datuplet-secrets-operator" {
		t.Errorf("operator binding roleRef = %+v, want datuplet-secrets-operator", opRB.RoleRef)
	}
	if len(opRB.Subjects) != 1 ||
		opRB.Subjects[0].Name != "pipeline-operator" ||
		opRB.Subjects[0].Namespace != "datuplet-install" {
		t.Errorf("operator binding subject = %+v, want ServiceAccount pipeline-operator/datuplet-install", opRB.Subjects)
	}
}

func TestEnsureProjectNamespace_SecretsRBACIdempotent(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "datuplet-install")
	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	projectID := uuid.New()

	// Two Ensures must both succeed and leave exactly one of each RBAC object.
	for i := 0; i < 2; i++ {
		if err := pkg8s.EnsureProjectNamespace(context.Background(), c, projectID); err != nil {
			t.Fatalf("Ensure #%d: %v", i+1, err)
		}
	}
	ns := "datuplet-" + projectID.String()

	roles := &rbacv1.RoleList{}
	if err := c.List(context.Background(), roles, client.InNamespace(ns)); err != nil {
		t.Fatalf("list Roles: %v", err)
	}
	if len(roles.Items) != 2 {
		t.Errorf("want 2 Roles after two Ensures, got %d", len(roles.Items))
	}
	bindings := &rbacv1.RoleBindingList{}
	if err := c.List(context.Background(), bindings, client.InNamespace(ns)); err != nil {
		t.Fatalf("list RoleBindings: %v", err)
	}
	if len(bindings.Items) != 2 {
		t.Errorf("want 2 RoleBindings after two Ensures, got %d", len(bindings.Items))
	}
}
