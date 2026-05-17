package k8s

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"strings"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
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

// PipelineRunName returns the conventional name for the PipelineRun object.
// Full RunID for the same collision-safety reason as SecretName, but if the
// pipeline name is long enough that "<pipeline>-<uuid>" would exceed the
// 253-char K8s limit, the pipeline portion is truncated and an 8-char SHA-256
// fragment is appended so names remain collision-free.
func (o CreateRunOpts) PipelineRunName() string {
	name := o.PipelineName + "-" + o.RunID.String()
	// Use the concatenated name only if it satisfies every DNS-1123
	// subdomain rule — not just the 253-char total, but also the 63-char
	// per-label limit. A valid pipeline name with a 40-char trailing
	// label becomes a 77-char trailing label after "-<uuid>" is added,
	// which K8s rejects. Fall through to the single-label form in that
	// case.
	if len(validation.IsDNS1123Subdomain(name)) == 0 {
		return name
	}
	// Fallback: emit a single-label name that is bounded by 63 chars and
	// contains no '.', so both DNS-1123 rules hold unconditionally:
	// `<debug-prefix>-<hash8>-<uuid>`.
	//   uuid (36) + '-' (1) + hash (8) + '-' (1) = 46 reserved.
	// That leaves 63 - 46 = 17 chars for a human-readable prefix derived
	// from the pipeline name. '.' is replaced with '-' and trailing '-'
	// is trimmed so the prefix ends on an alphanumeric.
	const dnsLabelMax = 63
	const reserved = 36 + 1 + 1 + 8
	h := sha256.Sum256([]byte(o.PipelineName))
	prefix := o.PipelineName
	if len(prefix) > dnsLabelMax-reserved {
		prefix = prefix[:dnsLabelMax-reserved]
	}
	prefix = strings.ReplaceAll(prefix, ".", "-")
	prefix = strings.TrimRight(prefix, "-")
	if prefix == "" {
		prefix = "pl"
	}
	return prefix + "-" + hex.EncodeToString(h[:4]) + "-" + o.RunID.String()
}

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
	prName := opts.PipelineRunName()

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
		TypeMeta:   metav1.TypeMeta{APIVersion: "datuplet.io/v1", Kind: "PipelineRun"},
		ObjectMeta: metav1.ObjectMeta{Name: prName, Namespace: opts.Namespace, Labels: prLabels},
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
