package k8s

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/config"
)

// ApplyPipelineCRD renders doc into a datupletv1.Pipeline via config.DocToCR,
// forces the namespace, and Creates or Updates the object in K8s.
// Uses a Get+Create/Update rather than server-side-apply because the
// latter requires a FieldManager string and interacts awkwardly with
// the fake client in tests — for MVP, Get+Create/Update is equivalent.
func ApplyPipelineCRD(ctx context.Context, c client.Client, namespace string, doc *config.Pipeline) error {
	if doc == nil {
		return errors.New("pipeline doc is nil")
	}
	pl := config.DocToCR(doc) // sets TypeMeta (datuplet.io/v1, Pipeline) and ObjectMeta.Name.
	if pl.Name == "" {
		return errors.New("pipeline doc has no name")
	}
	pl.Namespace = namespace
	pl.ResourceVersion = "" // Client.Create will reject a set RV.

	existing := &datupletv1.Pipeline{}
	err := c.Get(ctx, types.NamespacedName{Name: pl.Name, Namespace: namespace}, existing)
	if apierrors.IsNotFound(err) {
		// Attempt create; on AlreadyExists (a concurrent trigger won the
		// race) fall through to update so the helper stays idempotent.
		createErr := c.Create(ctx, pl)
		if createErr == nil {
			return nil
		}
		if !apierrors.IsAlreadyExists(createErr) {
			return fmt.Errorf("create Pipeline CRD: %w", createErr)
		}
		// Race — re-fetch and continue into update.
		if err := c.Get(ctx, types.NamespacedName{Name: pl.Name, Namespace: namespace}, existing); err != nil {
			return fmt.Errorf("get Pipeline CRD after AlreadyExists: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("get Pipeline CRD: %w", err)
	}
	// Replace spec + labels, preserve resourceVersion so the update succeeds.
	pl.ResourceVersion = existing.ResourceVersion
	if err := c.Update(ctx, pl); err != nil {
		return fmt.Errorf("update Pipeline CRD: %w", err)
	}
	return nil
}
