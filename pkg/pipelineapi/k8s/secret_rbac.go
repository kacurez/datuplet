package k8s

import (
	"context"
	"fmt"
	"os"
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RFC 026 P1.5 — Secret verbs live in per-project-namespace Roles, not on
// the cluster-wide ClusterRoles. project provisioning (EnsureProjectNamespace)
// creates these idempotently in each project namespace so the two SAs get the
// minimum they need scoped to that namespace only.
const (
	// SecretsRoleName grants pipeline-api full lifecycle on Secrets in a
	// project namespace: the managed project Secret, per-run run-token
	// Secrets, and per-run snapshot Secrets it (and the reaper) manages.
	SecretsRoleName = "datuplet-secrets"
	// SecretsOperatorRoleName grants the operator the minimum it needs:
	// `get` the managed project Secret and `create` the per-run snapshot.
	// A separate Role (not a second binding to SecretsRoleName) so the
	// operator never gains list/update/delete on Secrets.
	SecretsOperatorRoleName = "datuplet-secrets-operator"

	// pipelineAPIServiceAccount / pipelineOperatorServiceAccount are the SA
	// names the charts create in the install namespace; the RoleBindings
	// reference them as subjects.
	pipelineAPIServiceAccount      = "pipeline-api"
	pipelineOperatorServiceAccount = "pipeline-operator"

	// DefaultInstallNamespace is the fallback install namespace used only
	// when POD_NAMESPACE is unset AND the in-cluster ServiceAccount
	// namespace file is absent (e.g. an out-of-cluster admin CLI).
	DefaultInstallNamespace = "datuplet"

	saNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
)

// secretVerbsPipelineAPI / secretVerbsOperator are the exact verb sets each
// SA receives on `secrets` in a project namespace (RFC 026 P1.5).
var (
	secretVerbsPipelineAPI = []string{"get", "list", "create", "update", "patch", "delete"}
	secretVerbsOperator    = []string{"get", "create"}
)

// installNamespace resolves the namespace the pipeline-api / operator
// ServiceAccounts live in — needed for the RoleBinding subjects. In-cluster
// the ServiceAccount token (and its namespace file) is always mounted, so the
// file read is the authoritative source; POD_NAMESPACE overrides it for
// tests/flexibility, and DefaultInstallNamespace is the last resort.
func installNamespace() string {
	if ns := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); ns != "" {
		return ns
	}
	if b, err := os.ReadFile(saNamespaceFile); err == nil {
		if ns := strings.TrimSpace(string(b)); ns != "" {
			return ns
		}
	}
	return DefaultInstallNamespace
}

// ensureSecretsRBAC creates the two per-namespace secret Roles + their
// RoleBindings in namespace. Each object is Get-before-Create (idempotent);
// existing objects are left untouched. Called by EnsureProjectNamespace so
// namespace + secret RBAC are always ensured together on every ensure path.
func ensureSecretsRBAC(ctx context.Context, c client.Client, namespace string) error {
	saNS := installNamespace()

	if err := ensureSecretRole(ctx, c, namespace, SecretsRoleName, secretVerbsPipelineAPI); err != nil {
		return err
	}
	if err := ensureSecretRole(ctx, c, namespace, SecretsOperatorRoleName, secretVerbsOperator); err != nil {
		return err
	}
	if err := ensureRoleBinding(ctx, c, namespace, SecretsRoleName, pipelineAPIServiceAccount, saNS); err != nil {
		return err
	}
	return ensureRoleBinding(ctx, c, namespace, SecretsOperatorRoleName, pipelineOperatorServiceAccount, saNS)
}

// ensureSecretRole creates a Role granting verbs on `secrets` in namespace.
func ensureSecretRole(ctx context.Context, c client.Client, namespace, name string, verbs []string) error {
	existing := &rbacv1.Role{}
	err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get role %s/%s: %w", namespace, name, err)
	}
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""},
			Resources: []string{"secrets"},
			Verbs:     verbs,
		}},
	}
	if err := c.Create(ctx, role); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create role %s/%s: %w", namespace, name, err)
	}
	return nil
}

// ensureRoleBinding binds roleName to the ServiceAccount saName (in saNS)
// within namespace. The RoleBinding takes roleName as its own name — one
// binding per Role, so the two names never collide.
func ensureRoleBinding(ctx context.Context, c client.Client, namespace, roleName, saName, saNS string) error {
	existing := &rbacv1.RoleBinding{}
	err := c.Get(ctx, types.NamespacedName{Name: roleName, Namespace: namespace}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get rolebinding %s/%s: %w", namespace, roleName, err)
	}
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: roleName, Namespace: namespace},
		Subjects: []rbacv1.Subject{{
			Kind:      rbacv1.ServiceAccountKind,
			Name:      saName,
			Namespace: saNS,
		}},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     roleName,
		},
	}
	if err := c.Create(ctx, rb); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create rolebinding %s/%s: %w", namespace, roleName, err)
	}
	return nil
}
