package k8s

import (
	"context"
	"fmt"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ProjectSecretsName is the name of the single managed Secret pipeline-api
// stores project-scoped user secrets in (RFC 026 P1.5) — one per project
// namespace, keyed by the caller-supplied secret name under Data.
//
// Aliased to the canonical constant in the leaf CRD-types package so the
// operator controllers and pipeline-api share one source of truth without the
// controllers having to import this (db/authz/store-heavy) package.
const ProjectSecretsName = datupletv1.ProjectSecretsName

// EnsureProjectSecret creates the empty managed project-secrets Secret in
// namespace if it does not already exist. Idempotent: treats AlreadyExists
// as success.
//
// Get-before-create, mirroring EnsureProjectNamespace: callers on a
// deployment without cluster-wide `secrets.create` RBAC still work once the
// Secret has been provisioned once. Callers must ensure namespace itself
// exists first (EnsureProjectNamespace) — Secret creation fails if the
// namespace is absent.
func EnsureProjectSecret(ctx context.Context, c client.Client, namespace string) error {
	existing := &corev1.Secret{}
	err := c.Get(ctx, types.NamespacedName{Name: ProjectSecretsName, Namespace: namespace}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get secret %s/%s: %w", namespace, ProjectSecretsName, err)
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ProjectSecretsName,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeOpaque,
	}
	if err := c.Create(ctx, sec); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create secret %s/%s: %w", namespace, ProjectSecretsName, err)
	}
	return nil
}
