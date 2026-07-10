package k8s

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ProjectNamespaceLabel is the label on a Namespace that identifies the
// Datuplet project it belongs to. Operators validate this label against
// the project_id claim on incoming run tokens.
const ProjectNamespaceLabel = "datuplet.io/project-id"

// NamespaceForProject returns the K8s namespace name Datuplet uses for a
// given project UUID. All callers should use this helper rather than
// hand-rolling the prefix — the convention is enforced by the DB CHECK
// constraint on projects.k8s_namespace.
func NamespaceForProject(projectID uuid.UUID) string {
	return "datuplet-" + projectID.String()
}

// EnsureProjectNamespace creates the Namespace for projectID if it does not
// exist, plus the per-project-namespace secret Roles + RoleBindings (RFC 026
// P1.5) and a belt-and-braces LimitRange (RFC 026 P3 §4.4). Idempotent: treats
// AlreadyExists as success. Does NOT update labels
// on an existing Namespace (the operator will reject runs whose token
// disagrees with the existing label, which is the right fail-safe).
//
// This is the single project-provisioning path: run trigger
// (storeProjectNS.Ensure), the write-only secrets API, and the admin
// bootstrap CLI all route through here, so namespace + secret RBAC are always
// ensured together. The RBAC step runs UNCONDITIONALLY — not gated on the
// namespace having just been created — so a namespace pre-provisioned before
// P1.5 still gets its secret Roles backfilled on the next ensure.
func EnsureProjectNamespace(ctx context.Context, c client.Client, projectID uuid.UUID) error {
	name := NamespaceForProject(projectID)
	if err := ensureNamespace(ctx, c, name, projectID); err != nil {
		return err
	}
	if err := ensureSecretsRBAC(ctx, c, name); err != nil {
		return err
	}
	// Belt-and-braces LimitRange (RFC 026 §4.4). Unconditional, like the RBAC
	// step above, so a pre-provisioned namespace is backfilled on the next
	// ensure. The real resource ceiling is the registry Max + controller clamp;
	// this is only a default for pods created outside Datuplet's own path.
	return EnsureProjectLimitRange(ctx, c, projectID)
}

// ensureNamespace creates the project Namespace if absent.
//
// Get-before-create so deployments where namespaces are pre-provisioned and
// pipeline-api lacks cluster-wide `namespaces.create` RBAC still work. If the
// namespace already exists, we skip the Create entirely — otherwise K8s
// returns Forbidden (not AlreadyExists) and the run trigger fails.
func ensureNamespace(ctx context.Context, c client.Client, name string, projectID uuid.UUID) error {
	existing := &corev1.Namespace{}
	err := c.Get(ctx, types.NamespacedName{Name: name}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get namespace %s: %w", name, err)
	}
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				ProjectNamespaceLabel: projectID.String(),
			},
		},
	}
	if err := c.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create namespace %s: %w", name, err)
	}
	return nil
}
