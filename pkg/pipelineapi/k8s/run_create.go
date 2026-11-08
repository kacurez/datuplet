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

// RunTokenSecretKey is the Secret data key under which pipeline-api
// stores the single per-run JWT. Value is the raw JWT — no JSON wrapping.
// DG and TableCommit read this singular key at the well-known path
// /var/run/secrets/datuplet-runtoken/token.
const RunTokenSecretKey = "token"

// CreateRunOpts is the input to CreateRunResources.
type CreateRunOpts struct {
	Namespace    string    // K8s namespace (typically NamespaceForProject)
	PipelineName string    // matches pipelines.name and Pipeline CRD name
	RunID        uuid.UUID // same UUID the API stores in runs.id

	// Token is the single per-run JWT. Stored as the raw string under
	// Secret.data["token"]. DG + TableCommit read this file; lakekeeper
	// sees the run user's synthetic identity (`sub: user:oidc~<run-uuid>`)
	// and consults FGA for per-table authz.
	Token string
}

// SecretName returns the conventional name for the run-token Secret. The
// full 36-char RunID is embedded so names are unique across the project's
// entire run history — an 8-char prefix would birthday-collide around 77k
// runs.
func (o CreateRunOpts) SecretName() string {
	return "runtoken-" + o.RunID.String()
}

// PipelineRunName is the PipelineRun name of a run. Derived from the run ID
// alone so it is always a valid DNS-1123 label, whatever the pipeline is named.
func PipelineRunName(runID uuid.UUID) string { return "run-" + runID.String() }

// CreateRunResources creates the run-token Secret, the PipelineRun CRD, and
// patches the Secret with an ownerReference to the PipelineRun so both are
// GC'd together when the run completes and is reaped.
//
// Order of operations:
//  1. Create Secret (no owner ref yet — PipelineRun doesn't exist yet).
//  2. Create PipelineRun pointing at the Secret via spec.runTokenRef.
//  3. Get the created PipelineRun to pick up its UID.
//  4. Patch the Secret with OwnerReferences[PipelineRun].
//
// If step 4 fails the orphaned Secret remains; a future reaper will clean it.
// Callers should only treat step 2 failure as "run did not start" — by then
// the Secret may already exist and will be harmless clutter.
func CreateRunResources(ctx context.Context, c client.Client, opts CreateRunOpts) error {
	if opts.Namespace == "" || opts.PipelineName == "" {
		return fmt.Errorf("namespace and pipeline name are required")
	}

	secretName := opts.SecretName()
	prName := PipelineRunName(opts.RunID)

	// 1. Create Secret with the single per-run token. DG + TableCommit
	// read the singular `token` key at the well-known mount path.
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: opts.Namespace,
			Labels: map[string]string{
				"datuplet.io/run-id": opts.RunID.String(),
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			RunTokenSecretKey: []byte(opts.Token),
		},
	}
	if err := c.Create(ctx, sec); err != nil {
		return fmt.Errorf("create Secret: %w", err)
	}

	// 2. Create PipelineRun.
	prLabels := map[string]string{
		"datuplet.io/run-id": opts.RunID.String(),
	}
	pr := &datupletv1.PipelineRun{
		TypeMeta: metav1.TypeMeta{APIVersion: "datuplet.io/v1", Kind: "PipelineRun"},
		ObjectMeta: metav1.ObjectMeta{
			Name: prName, Namespace: opts.Namespace, Labels: prLabels,
			Annotations: map[string]string{"datuplet.io/pipeline-name": opts.PipelineName},
		},
		Spec: datupletv1.PipelineRunSpec{
			PipelineRef: datupletv1.PipelineRef{Name: opts.PipelineName},
			RunID:       opts.RunID.String(),
			RunTokenRef: &datupletv1.RunTokenRef{Name: secretName},
		},
	}
	if err := c.Create(ctx, pr); err != nil {
		return fmt.Errorf("create PipelineRun: %w", err)
	}

	// 3. Get PipelineRun to fetch UID for the owner reference.
	created := &datupletv1.PipelineRun{}
	if err := c.Get(ctx, types.NamespacedName{Name: prName, Namespace: opts.Namespace}, created); err != nil {
		return fmt.Errorf("get created PipelineRun: %w", err)
	}

	// 4. Patch Secret with owner reference.
	//
	// The run is already running in K8s at this point. A failure here
	// leaves the Secret without an ownerReference — it won't be GC'd when
	// the PipelineRun is deleted. The reaper will clean it up by age
	// regardless. We log and succeed, because returning an error would
	// make the caller mark an actually-running run as FailedApplication.
	sec.OwnerReferences = []metav1.OwnerReference{{
		APIVersion:         created.APIVersion,
		Kind:               created.Kind,
		Name:               created.Name,
		UID:                created.UID,
		Controller:         ptr.To(false),
		BlockOwnerDeletion: ptr.To(false),
	}}
	if err := c.Update(ctx, sec); err != nil {
		log.Printf("pipeline-api: patch Secret %s/%s owner reference (non-fatal, run is started): %v",
			opts.Namespace, secretName, err)
	}
	return nil
}
