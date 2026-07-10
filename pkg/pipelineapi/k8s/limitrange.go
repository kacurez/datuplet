package k8s

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ProjectLimitRangeName is the LimitRange every project namespace carries.
const ProjectLimitRangeName = "datuplet-defaults"

// EnsureProjectLimitRange creates a belt-and-braces LimitRange in the
// project namespace if absent (idempotent, get-before-create). It sets only
// default container requests/limits — a safety net for any pod created
// outside Datuplet's own path. It deliberately sets NO max: the real ceiling
// is the ComponentDefinition resources.max enforced by the controller clamp
// (RFC 026 §4.4). Belt-and-braces, not the primary boundary.
func EnsureProjectLimitRange(ctx context.Context, c client.Client, projectID uuid.UUID) error {
	ns := NamespaceForProject(projectID)
	existing := &corev1.LimitRange{}
	err := c.Get(ctx, types.NamespacedName{Name: ProjectLimitRangeName, Namespace: ns}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get limitrange %s/%s: %w", ns, ProjectLimitRangeName, err)
	}
	lr := &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Name: ProjectLimitRangeName, Namespace: ns},
		Spec: corev1.LimitRangeSpec{
			Limits: []corev1.LimitRangeItem{{
				Type: corev1.LimitTypeContainer,
				Default: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("512Mi"),
				},
				DefaultRequest: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("128Mi"),
				},
			}},
		},
	}
	if err := c.Create(ctx, lr); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create limitrange %s/%s: %w", ns, ProjectLimitRangeName, err)
	}
	return nil
}
