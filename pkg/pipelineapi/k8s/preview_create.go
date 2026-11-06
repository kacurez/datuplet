package k8s

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
)

// PreviewPipelineRunName is the PipelineRun name of a preview.
func PreviewPipelineRunName(id uuid.UUID) string { return "preview-" + id.String() }

// CreatePreviewOpts is the input to CreatePreviewResources.
type CreatePreviewOpts struct {
	Namespace string
	PreviewID uuid.UUID
	Token     string
	Spec      datupletv1.PipelineSpec
	Preview   datupletv1.PreviewSpec
}

// CreatePreviewResources mirrors CreateRunResources for a preview: run-token
// Secret, PipelineRun with an inline spec, then the Secret's owner reference.
// The PipelineRun carries only the preview-id label; the operator stamps
// datuplet.io/run-id=<preview id> on the Jobs and Pods from Spec.RunID.
func CreatePreviewResources(ctx context.Context, c client.Client, o CreatePreviewOpts) error {
	if o.Namespace == "" || o.PreviewID == uuid.Nil {
		return fmt.Errorf("namespace and preview id are required")
	}
	id := o.PreviewID.String()
	secretName := CreateRunOpts{RunID: o.PreviewID}.SecretName()
	prName := PreviewPipelineRunName(o.PreviewID)
	labels := func() map[string]string { return map[string]string{"datuplet.io/preview-id": id} }

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: o.Namespace, Labels: labels()},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{RunTokenSecretKey: []byte(o.Token)},
	}
	if err := c.Create(ctx, sec); err != nil {
		return fmt.Errorf("create Secret: %w", err)
	}

	spec := o.Spec.DeepCopy()
	pv := o.Preview
	pr := &datupletv1.PipelineRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: "datuplet.io/v1", Kind: "PipelineRun"},
		ObjectMeta: metav1.ObjectMeta{Name: prName, Namespace: o.Namespace, Labels: labels()},
		Spec: datupletv1.PipelineRunSpec{
			RunID:        id,
			RunTokenRef:  &datupletv1.RunTokenRef{Name: secretName},
			PipelineSpec: spec,
			Preview:      &pv,
		},
	}
	if err := c.Create(ctx, pr); err != nil {
		return fmt.Errorf("create PipelineRun: %w", err)
	}

	created := &datupletv1.PipelineRun{}
	if err := c.Get(ctx, types.NamespacedName{Name: prName, Namespace: o.Namespace}, created); err != nil {
		return fmt.Errorf("get created PipelineRun: %w", err)
	}
	sec.OwnerReferences = []metav1.OwnerReference{{
		APIVersion:         created.APIVersion,
		Kind:               created.Kind,
		Name:               created.Name,
		UID:                created.UID,
		Controller:         ptr.To(false),
		BlockOwnerDeletion: ptr.To(false),
	}}
	if err := c.Update(ctx, sec); err != nil {
		log.Printf("pipeline-api: patch Secret %s/%s owner reference (non-fatal, preview is started): %v",
			o.Namespace, secretName, err)
	}
	return nil
}
