package controllers

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/lib/status"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"
	"github.com/google/uuid"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// DefaultGatewayImage is the default image for the data gateway sidecar.
	PipelineRunDefaultGatewayImage = "datuplet/gateway:latest"

	// GatewayPort is the gRPC port the gateway listens on.
	PipelineRunGatewayPort = 50051

	// JobTTLSecondsAfterFinished is the TTL for completed component jobs.
	JobTTLSecondsAfterFinished = int32(3600)

	// PipelineRunRequeueInterval is the requeue interval when execution is in progress.
	PipelineRunRequeueInterval = 5 * time.Second

	// admissionTransientRetryBudget caps CONSECUTIVE transient registry
	// errors at run admission (see the taxonomy comment on
	// ComponentRegistry.Resolve). Exhausting the budget escalates the run to
	// FailedApplication — a wedged registry must not requeue forever.
	admissionTransientRetryBudget = 5

	// admissionTransientRetryInterval is the requeue backoff applied while
	// the transient retry budget has not yet been exhausted.
	admissionTransientRetryInterval = 2 * time.Second
)

// severityTransient marks a validate.Finding returned by
// ComponentRegistry.Resolve as a TRANSIENT infrastructure error (K8s API
// timeout/5xx, registry informer not yet synced) rather than a terminal,
// user-facing verdict. It is a controller-local extension of the shared
// Finding.Severity string — recognized only by handlePending's admission
// path below, never by pipeline-api (which never wires a ComponentRegistry).
//
// Controller error taxonomy (spec §7 — explicit, because pre-R7 code
// blanket-classified every registry/job-build error as FailedApplication):
//
//   - spec/schema/policy validation failure, unresolvable component or
//     version, reference to an Invalid definition
//     -> FailedUser (exit-code contract 1), first finding in the status
//     message.
//   - registry informer not synced, K8s API errors, image-pull
//     infrastructure failures
//     -> transient requeue with backoff, escalating to FailedApplication
//     (exit-code contract >=20) after admissionTransientRetryBudget
//     CONSECUTIVE occurrences — never FailedUser.
const severityTransient = "transient"

// shortID returns the first 8 chars of id, or the whole string if shorter.
// Used to build K8s resource names; avoids panicking on unexpectedly short IDs.
func shortID(id string) string {
	if len(id) < 8 {
		return id
	}
	return id[:8]
}

// PipelineRunReconciler reconciles a PipelineRun object
type PipelineRunReconciler struct {
	client.Client

	// APIReader is an UNCACHED reader (mgr.GetAPIReader) used to read the
	// managed project Secret at admission. Reading via the cached client
	// would spin up a cluster-wide Secret informer; a direct Get needs only
	// per-namespace `secrets:get` RBAC. Never used for the run's own objects.
	APIReader client.Reader

	Scheme       *runtime.Scheme
	GatewayImage string

	// LakekeeperURL is the catalog REST base URL injected into the
	// Data Gateway sidecar configMap (e.g.
	// http://lakekeeper.lakekeeper.svc.cluster.local:8181/catalog).
	// Required in production K8s; empty in tests.
	LakekeeperURL string
	// PipelineAPIURL is the base URL of pipeline-api. The operator derives the
	// JWKS URL (PipelineAPIURL + "/api/v1/auth/jwks.json") and injects it into
	// every DG sidecar configMap. Empty disables the injection (dev paths).
	PipelineAPIURL string

	Clientset kubernetes.Interface

	// RuntimeTolerations are injected onto every per-run Pod spec (component
	// Jobs) spawned by this operator. Populated from
	// DATUPLET_RUN_TOLERATIONS_JSON at startup. Nil means no injection.
	RuntimeTolerations []corev1.Toleration

	// GatewayDebug, when true, injects DATUPLET_GATEWAY_DEBUG=true into
	// the gateway sidecar's env on every PipelineRun. Operators flip this
	// via the operator Deployment's own env (set from helm values).
	// Default off — chatty DBG logs are opt-in.
	GatewayDebug bool

	// GatewayProfilingEnabled controls whether DATUPLET_GATEWAY_PROFILING=true
	// + PYROSCOPE_SERVER_ADDRESS are injected. The gateway's
	// StartProfilingIfEnabled boots pyroscope-go only when both are set;
	// see pkg/datagateway/profiling.go.
	GatewayProfilingEnabled bool

	// GatewayProfilingServerAddress is the Grafana Cloud Profiles endpoint
	// (or in-cluster Pyroscope receiver URL). Required when
	// GatewayProfilingEnabled is true; ignored otherwise.
	GatewayProfilingServerAddress string

	// Pyroscope Basic Auth, resolved from the operator's own env
	// (PYROSCOPE_USERNAME / PYROSCOPE_PASSWORD) and passed plain to
	// each gateway sidecar. Plain (not secretKeyRef) because per-run
	// namespaces are dynamic. Both empty = unauthenticated.
	GatewayProfilingUsername string
	GatewayProfilingPassword string

	// RuntimePullPolicy is applied to every container the operator
	// builds at runtime (gateway sidecar, component container). Sourced from
	// DATUPLET_RUNTIME_PULL_POLICY env
	// which the chart wires from .Values.image.pullPolicy.
	//
	// Production iteration-loop deploys want PullAlways so re-pushed
	// ttl.sh images are picked up; kind/e2e runs that pre-load images
	// via `kind load docker-image` need IfNotPresent so K8s does not
	// try to pull non-existent `datuplet/*` repositories from Docker
	// Hub. Empty defaults to PullAlways (chart-iteration default).
	RuntimePullPolicy corev1.PullPolicy

	// Registry resolves component/version references at run admission. In
	// production it is a ComponentRegistry over the manager's cached client;
	// tests inject a fake-client-backed one. Never re-read after admission —
	// the resolution is frozen into status.resolvedSpec.
	Registry validate.RegistryView

	// transientRetries tracks CONSECUTIVE transient registry-Get failures at
	// admission, keyed by the PipelineRun's NamespacedName (see
	// requeueOnTransientAdmissionError). Deliberately in-memory, not CRD
	// status: a purely operational retry budget. An operator restart resets
	// it, which is the safe direction — it never prematurely fails a run
	// that would otherwise have succeeded.
	transientRetries   map[types.NamespacedName]int
	transientRetriesMu sync.Mutex
}

// ComponentRegistry adapts a cached, cluster-scoped client.Reader into a
// validate.RegistryView. Resolve Gets the ComponentDefinition named by the
// component reference and delegates to a validate.StaticRegistry built from it,
// so the resolved component retains the full config-schema (secret-ref)
// validation semantics that only StaticRegistry populates. An Invalid-phase
// definition is refused by StaticRegistry.Resolve.
type ComponentRegistry struct {
	Reader client.Reader
}

// Resolve implements validate.RegistryView.
func (cr ComponentRegistry) Resolve(component, version string) (*validate.ResolvedComponent, []validate.Finding) {
	def := &datupletv1.ComponentDefinition{}
	if err := cr.Reader.Get(context.Background(), types.NamespacedName{Name: component}, def); err != nil {
		if errors.IsNotFound(err) {
			return nil, []validate.Finding{{
				Path:     "component",
				Message:  fmt.Sprintf("unknown component %q", component),
				Severity: "error",
			}}
		}
		// Any other Get error (API server timeout/5xx, informer not yet
		// synced) is TRANSIENT infrastructure noise, not a statement about
		// the component reference itself. Tagged severityTransient so
		// handlePending requeues instead of failing the run FailedUser —
		// see the taxonomy comment above.
		return nil, []validate.Finding{{
			Path:     "component",
			Message:  fmt.Sprintf("component %q: %v", component, err),
			Severity: severityTransient,
		}}
	}
	return validate.StaticRegistry{component: *def}.Resolve(component, version)
}

// componentPullPolicy governs the COMPONENT container's image pull policy from
// the frozen resolved version: stable (semver) versions are immutable →
// IfNotPresent; prerelease versions use mutable tags → Always so a re-pushed
// tag is picked up. The gateway sidecar keeps runtimePullPolicy separately.
func componentPullPolicy(version string) corev1.PullPolicy {
	if datupletv1.IsStableVersion(version) {
		return corev1.PullIfNotPresent
	}
	return corev1.PullAlways
}

// componentImagePullPolicy resolves the COMPONENT container's pull policy. The
// operator-wide RuntimePullPolicy override wins when set: it is documented to
// apply to every container the operator builds (gateway sidecar AND component
// container), and e2e/kind rely on it so pre-loaded local images are not
// re-pulled from a registry that does not have them. Otherwise the policy is
// registry-driven by the frozen version (componentPullPolicy).
func (r *PipelineRunReconciler) componentImagePullPolicy(version string) corev1.PullPolicy {
	if r.RuntimePullPolicy != "" {
		return r.RuntimePullPolicy
	}
	return componentPullPolicy(version)
}

// firstErrorFinding returns the first error-severity finding, if any. Warning
// findings (e.g. deprecation) do not fail admission.
func firstErrorFinding(findings []validate.Finding) (validate.Finding, bool) {
	for _, f := range findings {
		if f.Severity == "error" {
			return f, true
		}
	}
	return validate.Finding{}, false
}

// firstTransientFinding returns the first severityTransient finding, if any.
// Checked BEFORE firstErrorFinding at every admission call site: a transient
// registry error must never be classified FailedUser, even when it happens
// to be bundled alongside genuine validation findings for other components.
func firstTransientFinding(findings []validate.Finding) (validate.Finding, bool) {
	for _, f := range findings {
		if f.Severity == severityTransient {
			return f, true
		}
	}
	return validate.Finding{}, false
}

// runtimePullPolicy returns r.RuntimePullPolicy when set, else PullAlways
// (the iteration-loop default). Centralised so the three runtime
// pod-builder sites stay in sync.
func (r *PipelineRunReconciler) runtimePullPolicy() corev1.PullPolicy {
	if r.RuntimePullPolicy != "" {
		return r.RuntimePullPolicy
	}
	return corev1.PullAlways
}

// +kubebuilder:rbac:groups=datuplet.io,resources=pipelineruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=datuplet.io,resources=pipelineruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=datuplet.io,resources=pipelineruns/finalizers,verbs=update
// +kubebuilder:rbac:groups=datuplet.io,resources=pipelines,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch
//
// Secrets: intentionally NO cluster-wide +kubebuilder:rbac marker (RFC 026
// P1.5). Secret access is per-project-namespace only: the operator gets `get`
// (managed project Secret) + `create` (per-run snapshot Secret) via the
// `datuplet-secrets-operator` Role that pipeline-api binds to this SA in each
// project namespace (EnsureProjectNamespace) — never a cluster-wide grant.
// NB: this repo hand-maintains RBAC (charts/datuplet-app/templates/ +
// utils/deploy/k8s/rbac/); there is no controller-gen `make manifests` target,
// so these markers are documentation only and do not generate any manifest.

// Reconcile handles PipelineRun reconciliation.
func (r *PipelineRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the PipelineRun instance
	pipelineRun := &datupletv1.PipelineRun{}
	if err := r.Get(ctx, req.NamespacedName, pipelineRun); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get PipelineRun")
		return ctrl.Result{}, err
	}

	// Update observed generation
	if pipelineRun.Status.ObservedGeneration != pipelineRun.Generation {
		pipelineRun.Status.ObservedGeneration = pipelineRun.Generation
	}

	// Cancel propagation: when the PipelineRun itself carries
	// `datuplet.io/cancel=true` (admin-driven cancel via `kubectl annotate`),
	// propagate the annotation to every owned component pod so DG's cancel
	// watcher observes it. The pipeline-api cancel path patches pods directly
	// before deleting the CRD, but admin/kubectl-driven cancels never touch
	// pipeline-api — without this, those cancels would only stop new pods
	// being created, not drain in-flight ones.
	if pipelineRun.Annotations["datuplet.io/cancel"] == "true" {
		r.propagateCancelToPods(ctx, pipelineRun)
	}

	// Handle based on current phase
	switch pipelineRun.Status.Phase {
	case "":
		// New PipelineRun - initialize
		return r.handlePending(ctx, pipelineRun)
	case datupletv1.PipelineRunPhasePending:
		// Start execution
		return r.handlePending(ctx, pipelineRun)
	case datupletv1.PipelineRunPhaseRunning:
		// Continue execution
		return r.handleRunning(ctx, pipelineRun)
	case datupletv1.PipelineRunPhaseSucceeded, datupletv1.PipelineRunPhaseFailedUser, datupletv1.PipelineRunPhaseFailedApplication:
		// Terminal state - nothing to do
		return ctrl.Result{}, nil
	default:
		logger.Info("Unknown phase", "phase", pipelineRun.Status.Phase)
		return ctrl.Result{}, nil
	}
}

// handlePending initializes the PipelineRun and starts execution.
func (r *PipelineRunReconciler) handlePending(ctx context.Context, pr *datupletv1.PipelineRun) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Pipeline
	pipeline := &datupletv1.Pipeline{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      pr.Spec.PipelineRef.Name,
		Namespace: pr.Namespace,
	}, pipeline); err != nil {
		if errors.IsNotFound(err) {
			pr.Status.Phase = datupletv1.PipelineRunPhaseFailedUser
			pr.Status.Message = fmt.Sprintf("Pipeline '%s' not found", pr.Spec.PipelineRef.Name)
			if err := r.Status().Update(ctx, pr); err != nil {
				logger.Error(err, "Failed to update PipelineRun status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get Pipeline")
		return ctrl.Result{}, err
	}

	// Determine run ID: use spec value if provided, otherwise generate
	runID := pr.Spec.RunID
	if runID == "" {
		runID = uuid.New().String()
	} else {
		// Validate user-provided RunID is a valid UUID
		if _, err := uuid.Parse(runID); err != nil {
			pr.Status.Phase = datupletv1.PipelineRunPhaseFailedUser
			pr.Status.Message = fmt.Sprintf("invalid spec.runId: must be a valid UUID, got %q", runID)
			if err := r.Status().Update(ctx, pr); err != nil {
				logger.Error(err, "Failed to update PipelineRun status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	}
	// RunID must be set before snapshotRunSecrets: the per-run snapshot name
	// (runSecretsName) is derived from it.
	pr.Status.RunID = runID

	// Snapshot the referenced project secrets into a per-run Secret at
	// admission — before any Job is built. A missing key fails the run
	// FailedUser here, so no snapshot and no Jobs are ever created for it.
	missing, err := r.snapshotRunSecrets(ctx, pr, pipeline)
	if err != nil {
		logger.Error(err, "Failed to snapshot run secrets")
		return ctrl.Result{}, err
	}
	if len(missing) > 0 {
		pr.Status.Phase = datupletv1.PipelineRunPhaseFailedUser
		pr.Status.Message = fmt.Sprintf("%s referenced secret(s) not found in project secrets: %s",
			status.StatusMessagePrefix, strings.Join(missing, ", "))
		meta.SetStatusCondition(&pr.Status.Conditions, metav1.Condition{
			Type:    datupletv1.PipelineRunSecretsResolved,
			Status:  metav1.ConditionFalse,
			Reason:  datupletv1.PipelineRunReasonSnapshotMissing,
			Message: pr.Status.Message,
		})
		if err := r.Status().Update(ctx, pr); err != nil {
			logger.Error(err, "Failed to update PipelineRun status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Run admission: validate the fetched Pipeline with the same semantic
	// checks the pipeline-api save path runs, now resolving every component
	// against the registry (r.Registry). This does NOT gate on
	// pipeline.Status.Phase, so a run is never admitted against an invalid
	// Pipeline regardless of whether the Pipeline controller reconciled it.
	// A registry resolution error (unknown component, invalid definition,
	// unresolvable version) surfaces as a finding here, before any Job. A
	// TRANSIENT registry error (checked first) never fails the run — see the
	// taxonomy comment on ComponentRegistry.Resolve.
	admissionFindings := validate.ValidateTyped(pipeline, r.Registry, nil)
	if tf, ok := firstTransientFinding(admissionFindings); ok {
		return r.requeueOnTransientAdmissionError(ctx, pr, tf.Message)
	}
	if f, ok := firstErrorFinding(admissionFindings); ok {
		pr.Status.Phase = datupletv1.PipelineRunPhaseFailedUser
		pr.Status.Message = f.Message
		if err := r.Status().Update(ctx, pr); err != nil {
			logger.Error(err, "Failed to update PipelineRun status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Resolve every component and FREEZE the result into status: the run
	// executes exclusively from this snapshot, so later registry changes or
	// mid-run edits to the Pipeline spec cannot change what runs (RFC 026
	// §4.3). resolvedSpec is a DeepCopy of the validated spec; components[]
	// records the resolved component/version/image per instance.
	resolvedSpec := pipeline.Spec.DeepCopy()
	var resolvedComponents []datupletv1.ResolvedComponentStatus
	for i := range resolvedSpec.Stages {
		stage := &resolvedSpec.Stages[i]
		for j := range stage.Components {
			comp := &stage.Components[j]
			rc, findings := r.Registry.Resolve(comp.Component, comp.Version)
			if rc == nil {
				if tf, ok := firstTransientFinding(findings); ok {
					return r.requeueOnTransientAdmissionError(ctx, pr, tf.Message)
				}
				f, _ := firstErrorFinding(findings)
				pr.Status.Phase = datupletv1.PipelineRunPhaseFailedUser
				pr.Status.Message = f.Message
				if err := r.Status().Update(ctx, pr); err != nil {
					logger.Error(err, "Failed to update PipelineRun status")
					return ctrl.Result{}, err
				}
				return ctrl.Result{}, nil
			}
			resolvedComponents = append(resolvedComponents, datupletv1.ResolvedComponentStatus{
				Name:      comp.Name,
				Component: rc.Component,
				Version:   rc.Version,
				Image:     rc.Image,
			})
		}
	}
	pr.Status.ResolvedSpec = resolvedSpec
	pr.Status.PipelineGeneration = pipeline.Generation
	pr.Status.Components = resolvedComponents

	// Admission succeeded past every transient check — clear any
	// accumulated retry count so a later, unrelated admission (e.g. this
	// PipelineRun name reused after delete/recreate) starts with a fresh
	// budget.
	r.clearTransientRetries(types.NamespacedName{Namespace: pr.Namespace, Name: pr.Name})

	// Initialize status
	pr.Status.Phase = datupletv1.PipelineRunPhaseRunning
	now := metav1.Now()
	pr.Status.StartTime = &now

	// Initialize stage statuses from the FROZEN spec (not the live Pipeline).
	pr.Status.StageStatuses = make([]datupletv1.StageStatus, len(resolvedSpec.Stages))
	for i, stage := range resolvedSpec.Stages {
		pr.Status.StageStatuses[i] = datupletv1.StageStatus{
			Name:  stage.Name,
			Phase: datupletv1.StagePhasePending,
		}
	}

	// The snapshot exists (or the pipeline references no secrets); reflect that
	// on the SecretsResolved condition. Absent when there are no references.
	r.updateSecretsResolvedCondition(pr, pipeline)

	if err := r.Status().Update(ctx, pr); err != nil {
		logger.Error(err, "Failed to update PipelineRun status")
		return ctrl.Result{}, err
	}

	logger.Info("Initialized PipelineRun", "runID", runID)
	return ctrl.Result{RequeueAfter: PipelineRunRequeueInterval}, nil
}

// requeueOnTransientAdmissionError implements the transient half of the
// admission error taxonomy (see the comment on severityTransient /
// ComponentRegistry.Resolve): a registry Get failing with anything other than
// NotFound is infrastructure noise, not a verdict on the pipeline, so the run
// is NOT failed here. It requeues with backoff for up to
// admissionTransientRetryBudget CONSECUTIVE occurrences (tracked in-memory,
// per PipelineRun); once the budget is exhausted the run is failed
// FailedApplication (exit-code contract >=20).
func (r *PipelineRunReconciler) requeueOnTransientAdmissionError(ctx context.Context, pr *datupletv1.PipelineRun, msg string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	key := types.NamespacedName{Namespace: pr.Namespace, Name: pr.Name}
	attempt := r.noteTransientRetry(key)

	if attempt < admissionTransientRetryBudget {
		logger.Info("Transient registry error at admission, requeueing",
			"attempt", attempt, "budget", admissionTransientRetryBudget, "error", msg)
		return ctrl.Result{RequeueAfter: admissionTransientRetryInterval}, nil
	}

	logger.Info("Transient registry error retry budget exhausted, failing run FailedApplication",
		"attempts", attempt, "error", msg)
	r.clearTransientRetries(key)
	pr.Status.Phase = datupletv1.PipelineRunPhaseFailedApplication
	pr.Status.Message = fmt.Sprintf("registry unavailable after %d consecutive attempts: %s", attempt, msg)
	now := metav1.Now()
	pr.Status.CompletionTime = &now
	if err := r.Status().Update(ctx, pr); err != nil {
		logger.Error(err, "Failed to update PipelineRun status")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// noteTransientRetry increments and returns the CONSECUTIVE transient-error
// count for key.
func (r *PipelineRunReconciler) noteTransientRetry(key types.NamespacedName) int {
	r.transientRetriesMu.Lock()
	defer r.transientRetriesMu.Unlock()
	if r.transientRetries == nil {
		r.transientRetries = make(map[types.NamespacedName]int)
	}
	r.transientRetries[key]++
	return r.transientRetries[key]
}

// clearTransientRetries resets the CONSECUTIVE transient-error count for key,
// called once admission proceeds past every transient check (success or a
// terminal failure) so a stale count never leaks into an unrelated future
// admission of the same name.
func (r *PipelineRunReconciler) clearTransientRetries(key types.NamespacedName) {
	r.transientRetriesMu.Lock()
	defer r.transientRetriesMu.Unlock()
	delete(r.transientRetries, key)
}

// snapshotRunSecrets copies the exact set of $[name] keys the pipeline
// references from the managed project Secret into a per-run snapshot Secret
// owned by the run. It is the admission gate for secret resolution:
//
//   - returns (nil, nil) when the pipeline references no secrets (skip), the
//     snapshot already exists (run already admitted), or the snapshot was just
//     created;
//   - returns (missing, nil) — a sorted list of referenced keys absent from the
//     project Secret (or the whole ref set when the project Secret itself is
//     missing) — signalling a user error the caller surfaces as FailedUser;
//   - returns (nil, err) on a transient API error the caller requeues on.
//
// The snapshot's EXISTENCE is authoritative. It is checked FIRST, before the
// project Secret is touched: once the snapshot exists the run has already been
// admitted for secrets, so a re-reconcile (e.g. after a partial admission where
// the snapshot was created but the status Update that flips the run to Running
// failed) must NOT re-read the project Secret and must NOT re-run the
// missing-key check. Otherwise a project-Secret rotation / key removal between
// the two reconciles could flip a run with a valid, immutable snapshot to
// FailedUser. Reading the project Secret only when the snapshot is absent keeps
// the snapshot immutable for the run's life — rotation-exact isolation.
//
// Both Gets use the UNCACHED APIReader — the project Secret and the operator's
// own run-snapshot Secret alike. Using the cached client for the snapshot would
// spin up a cluster-wide Secret informer (the very thing APIReader avoids); a
// direct read needs only per-namespace secrets:get RBAC and is strongly
// consistent, which is exactly what an authoritative existence check wants.
func (r *PipelineRunReconciler) snapshotRunSecrets(ctx context.Context, pr *datupletv1.PipelineRun, pipeline *datupletv1.Pipeline) ([]string, error) {
	refs := validate.ReferencedSecrets(pipeline)
	if len(refs) == 0 {
		return nil, nil
	}

	// Authoritative: an existing snapshot means the run is already admitted.
	// Do not read the project Secret, re-run the missing-key check, or rewrite.
	snapName := runSecretsName(pr)
	existing := &corev1.Secret{}
	if err := r.APIReader.Get(ctx, types.NamespacedName{Name: snapName, Namespace: pr.Namespace}, existing); err == nil {
		return nil, nil
	} else if !errors.IsNotFound(err) {
		return nil, err
	}

	// Snapshot absent → first admission: read the project Secret and run the
	// missing-key check before creating the snapshot.
	project := &corev1.Secret{}
	err := r.APIReader.Get(ctx, types.NamespacedName{
		Name:      datupletv1.ProjectSecretsName,
		Namespace: pr.Namespace,
	}, project)
	if err != nil && !errors.IsNotFound(err) {
		return nil, err
	}
	// On NotFound, project.Data is nil and every referenced key is missing.

	data := make(map[string][]byte, len(refs))
	var missing []string
	for _, name := range refs {
		v, ok := project.Data[name]
		if !ok {
			missing = append(missing, name)
			continue
		}
		data[name] = v
	}
	if len(missing) > 0 {
		return missing, nil
	}

	snap := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapName,
			Namespace: pr.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/component": "run-secrets",
				"app.kubernetes.io/part-of":   "datuplet",
				"datuplet.io/pipelinerun":     pr.Name,
				"datuplet.io/run-id":          pr.Status.RunID,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}
	// OwnerRef -> PipelineRun so the snapshot is garbage-collected with the run.
	if err := ctrl.SetControllerReference(pr, snap, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, snap); err != nil && !errors.IsAlreadyExists(err) {
		return nil, err
	}
	return nil, nil
}

// handleRunning continues the PipelineRun execution. It reads exclusively from
// the FROZEN status.resolvedSpec — never the live Pipeline — so mid-run edits
// and registry changes cannot alter what executes.
func (r *PipelineRunReconciler) handleRunning(ctx context.Context, pr *datupletv1.PipelineRun) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if pr.Status.ResolvedSpec == nil {
		// A Running run must carry the frozen spec written at admission.
		pr.Status.Phase = datupletv1.PipelineRunPhaseFailedApplication
		pr.Status.Message = "internal: resolvedSpec missing on a Running PipelineRun"
		now := metav1.Now()
		pr.Status.CompletionTime = &now
		if err := r.Status().Update(ctx, pr); err != nil {
			logger.Error(err, "Failed to update PipelineRun status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
	resolvedSpec := pr.Status.ResolvedSpec

	// Find the current stage to execute
	currentStageIdx := -1
	for i, stageStatus := range pr.Status.StageStatuses {
		if stageStatus.Phase != datupletv1.StagePhaseSucceeded {
			currentStageIdx = i
			break
		}
	}

	// All stages completed
	if currentStageIdx == -1 {
		pr.Status.Phase = datupletv1.PipelineRunPhaseSucceeded
		pr.Status.Message = "All stages completed successfully"
		now := metav1.Now()
		pr.Status.CompletionTime = &now
		if err := r.Status().Update(ctx, pr); err != nil {
			logger.Error(err, "Failed to update PipelineRun status")
			return ctrl.Result{}, err
		}
		logger.Info("PipelineRun completed successfully")
		return ctrl.Result{}, nil
	}

	stage := &resolvedSpec.Stages[currentStageIdx]
	stageStatus := &pr.Status.StageStatuses[currentStageIdx]
	pr.Status.CurrentStage = stage.Name

	// Handle stage based on its phase
	switch stageStatus.Phase {
	case datupletv1.StagePhasePending:
		// Start the stage
		return r.startStage(ctx, pr, currentStageIdx)
	case datupletv1.StagePhaseRunning:
		// Check component status
		return r.checkStageComponents(ctx, pr, currentStageIdx)
	case datupletv1.StagePhaseFailedUser:
		// Stage failed due to user error - fail the entire run
		pr.Status.Phase = datupletv1.PipelineRunPhaseFailedUser
		pr.Status.Message = fmt.Sprintf("Stage '%s' failed: %s", stage.Name, stageStatus.Message)
		now := metav1.Now()
		pr.Status.CompletionTime = &now
		if err := r.Status().Update(ctx, pr); err != nil {
			logger.Error(err, "Failed to update PipelineRun status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	case datupletv1.StagePhaseFailedApplication:
		// Stage failed due to application error - fail the entire run
		pr.Status.Phase = datupletv1.PipelineRunPhaseFailedApplication
		pr.Status.Message = fmt.Sprintf("Stage '%s' failed: %s", stage.Name, stageStatus.Message)
		now := metav1.Now()
		pr.Status.CompletionTime = &now
		if err := r.Status().Update(ctx, pr); err != nil {
			logger.Error(err, "Failed to update PipelineRun status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	return ctrl.Result{RequeueAfter: PipelineRunRequeueInterval}, nil
}

// startStage creates Jobs for all components in the stage, reading from the
// FROZEN status.resolvedSpec.
func (r *PipelineRunReconciler) startStage(ctx context.Context, pr *datupletv1.PipelineRun, stageIdx int) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	stage := &pr.Status.ResolvedSpec.Stages[stageIdx]
	stageStatus := &pr.Status.StageStatuses[stageIdx]

	logger.Info("Starting stage", "stage", stage.Name)

	// Initialize component statuses
	stageStatus.ComponentStatuses = make([]datupletv1.ComponentStatus, len(stage.Components))
	for i, comp := range stage.Components {
		stageStatus.ComponentStatuses[i] = datupletv1.ComponentStatus{
			Name:  comp.Name,
			Phase: datupletv1.ComponentPhasePending,
		}
	}

	// Create Jobs for each component
	for i := range stage.Components {
		comp := &stage.Components[i]
		compStatus := &stageStatus.ComponentStatuses[i]

		job, configMap, err := r.buildComponentJob(ctx, pr, comp)
		if err != nil {
			logger.Error(err, "Failed to build component job", "component", comp.Name)
			compStatus.Phase = datupletv1.ComponentPhaseFailedApplication
			compStatus.Message = err.Error()
			stageStatus.Phase = datupletv1.StagePhaseFailedApplication
			stageStatus.Message = fmt.Sprintf("Failed to build job for component '%s': %v", comp.Name, err)
			if err := r.Status().Update(ctx, pr); err != nil {
				logger.Error(err, "Failed to update PipelineRun status")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}

		// Set owner reference
		if err := ctrl.SetControllerReference(pr, job, r.Scheme); err != nil {
			logger.Error(err, "Failed to set controller reference for job")
			return ctrl.Result{}, err
		}
		if err := ctrl.SetControllerReference(pr, configMap, r.Scheme); err != nil {
			logger.Error(err, "Failed to set controller reference for configmap")
			return ctrl.Result{}, err
		}

		// Create ConfigMap
		if err := r.Create(ctx, configMap); err != nil && !errors.IsAlreadyExists(err) {
			logger.Error(err, "Failed to create gateway config map", "component", comp.Name)
			return ctrl.Result{}, err
		}

		// Create Job
		if err := r.Create(ctx, job); err != nil && !errors.IsAlreadyExists(err) {
			logger.Error(err, "Failed to create job", "component", comp.Name)
			return ctrl.Result{}, err
		}

		compStatus.Phase = datupletv1.ComponentPhaseRunning
		compStatus.PodName = job.Name
		now := metav1.Now()
		compStatus.StartTime = &now

		logger.Info("Created Job for component", "component", comp.Name, "job", job.Name)
	}

	stageStatus.Phase = datupletv1.StagePhaseRunning
	now := metav1.Now()
	stageStatus.StartTime = &now

	if err := r.Status().Update(ctx, pr); err != nil {
		logger.Error(err, "Failed to update PipelineRun status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: PipelineRunRequeueInterval}, nil
}

// checkStageComponents checks the status of all component Jobs in a stage,
// reading from the FROZEN status.resolvedSpec.
func (r *PipelineRunReconciler) checkStageComponents(ctx context.Context, pr *datupletv1.PipelineRun, stageIdx int) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	stage := &pr.Status.ResolvedSpec.Stages[stageIdx]
	stageStatus := &pr.Status.StageStatuses[stageIdx]

	allSucceeded := true
	anyFailedUser := false
	anyFailedApp := false
	var failedComponent string
	var failedMessage string

	for i := range stage.Components {
		compStatus := &stageStatus.ComponentStatuses[i]

		if compStatus.Phase == datupletv1.ComponentPhaseSucceeded {
			continue
		}
		if compStatus.Phase == datupletv1.ComponentPhaseFailedUser {
			anyFailedUser = true
			failedComponent = compStatus.Name
			failedMessage = compStatus.Message
			continue
		}
		if compStatus.Phase == datupletv1.ComponentPhaseFailedApplication {
			anyFailedApp = true
			failedComponent = compStatus.Name
			failedMessage = compStatus.Message
			continue
		}

		allSucceeded = false

		// Check Job status. Use componentJobName so this stays in sync
		// with buildComponentJob — changing the scheme in one place
		// without the other makes the poller look for the wrong object
		// and leaves components "running" forever.
		jobName := componentJobName(pr, stage.Name, compStatus.Name)

		job := &batchv1.Job{}
		if err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: pr.Namespace}, job); err != nil {
			if errors.IsNotFound(err) {
				logger.Info("Job not found, waiting...", "job", jobName)
				continue
			}
			logger.Error(err, "Failed to get Job", "job", jobName)
			return ctrl.Result{}, err
		}

		// Try to extract exit code from pod
		exitCode := r.extractExitCodeFromJob(ctx, job)

		// Observe the pulled image digest (RFC 026 §4.3). Captured from
		// whichever reconcile first sees it on the pod's containerStatuses
		// — while the component is still Running, or on the terminal
		// reconcile below — and never overwritten afterward.
		recordComponentImageID(pr, compStatus.Name, r.extractImageIDFromJob(ctx, job))

		// Check Job conditions
		for _, condition := range job.Status.Conditions {
			if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
				compStatus.Phase = datupletv1.ComponentPhaseSucceeded
				// Job completed successfully - exit code is 0 by definition
				zero := int32(0)
				compStatus.ExitCode = &zero
				now := metav1.Now()
				compStatus.CompletionTime = &now

				// Extract status message from pod logs
				if msg := r.extractComponentStatusMessage(ctx, job, 0); msg != "" {
					compStatus.Message = msg
				}

				logger.Info("Component succeeded", "component", compStatus.Name, "message", compStatus.Message)
			}
			if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
				compStatus.ExitCode = exitCode

				// Classify failure type based on exit code
				if exitCode != nil {
					failureType := status.ClassifyExitCode(int(*exitCode))
					if failureType == status.FailureTypeUser {
						compStatus.Phase = datupletv1.ComponentPhaseFailedUser
						anyFailedUser = true
					} else {
						compStatus.Phase = datupletv1.ComponentPhaseFailedApplication
						anyFailedApp = true
					}

					// Try to extract status message from pod logs
					msg := r.extractComponentStatusMessage(ctx, job, int(*exitCode))
					if msg != "" {
						compStatus.Message = msg
					} else {
						compStatus.Message = condition.Message
					}
				} else {
					// No exit code available - treat as application error
					compStatus.Phase = datupletv1.ComponentPhaseFailedApplication
					compStatus.Message = condition.Message
					anyFailedApp = true
				}

				failedComponent = compStatus.Name
				failedMessage = compStatus.Message
				now := metav1.Now()
				compStatus.CompletionTime = &now
				logger.Info("Component failed", "component", compStatus.Name,
					"phase", compStatus.Phase, "exitCode", exitCode, "message", compStatus.Message)
			}
		}
	}

	// Check final state - first failed component determines stage failure type
	if anyFailedUser || anyFailedApp {
		if anyFailedUser {
			stageStatus.Phase = datupletv1.StagePhaseFailedUser
		} else {
			stageStatus.Phase = datupletv1.StagePhaseFailedApplication
		}
		stageStatus.Message = fmt.Sprintf("Component '%s' failed: %s", failedComponent, failedMessage)
		now := metav1.Now()
		stageStatus.CompletionTime = &now
		if err := r.Status().Update(ctx, pr); err != nil {
			logger.Error(err, "Failed to update PipelineRun status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if allSucceeded {
		// All components done - DG already owns commit; transition directly to Succeeded.
		stageStatus.Phase = datupletv1.StagePhaseSucceeded
		now := metav1.Now()
		stageStatus.CompletionTime = &now
		logger.Info("Stage completed successfully", "stage", stage.Name)
		if err := r.Status().Update(ctx, pr); err != nil {
			logger.Error(err, "Failed to update PipelineRun status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: PipelineRunRequeueInterval}, nil
	}

	// Still running - keep requeue cadence; log Update failures (next tick will retry).
	if err := r.Status().Update(ctx, pr); err != nil {
		logger.Error(err, "Failed to update PipelineRun status")
	}
	return ctrl.Result{RequeueAfter: PipelineRunRequeueInterval}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PipelineRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&datupletv1.PipelineRun{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}

// propagateCancelToPods patches every pod labelled
// `datuplet.io/run-id=<runID>` in the PipelineRun's namespace with
// annotation `datuplet.io/cancel=true`. DG's cancel watcher polls the
// kubelet downward-API projection of pod annotations and triggers a
// graceful shutdown when it sees the annotation flip.
//
// Best-effort: a patch failure on any individual pod is logged and we
// keep going. The reconcile loop itself never errors on a cancel
// propagation failure — the cancel pathway is informational, not
// load-bearing for correctness (a future delete of the PipelineRun
// would force-kill the pod regardless).
//
// Mirrors pkg/pipelineapi/runbackend.K8sBackend.annotateRunPodsCancelled
// — kept here so admin/kubectl-driven cancels (which never touch
// pipeline-api) still drain in-flight pods cleanly.
func (r *PipelineRunReconciler) propagateCancelToPods(ctx context.Context, pr *datupletv1.PipelineRun) {
	logger := log.FromContext(ctx)
	if pr.Status.RunID == "" {
		// Nothing to look up by yet — pods are labelled with the run-id,
		// which the reconciler stamps onto Status during handlePending.
		// A cancel arriving before handlePending fires will be picked
		// up on the next reconcile pass once the run-id exists.
		return
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(pr.Namespace),
		client.MatchingLabels{"datuplet.io/run-id": pr.Status.RunID},
	); err != nil {
		logger.V(1).Info("cancel: list pods failed (best-effort)", "err", err.Error())
		return
	}
	if len(pods.Items) == 0 {
		return
	}
	patchBytes := []byte(`{"metadata":{"annotations":{"datuplet.io/cancel":"true"}}}`)
	for i := range pods.Items {
		pod := &pods.Items[i]
		// Skip pods that already carry the annotation — avoids burning
		// API server budget on every requeue.
		if pod.Annotations["datuplet.io/cancel"] == "true" {
			continue
		}
		if err := r.Patch(ctx, pod, client.RawPatch(types.MergePatchType, patchBytes)); err != nil {
			logger.V(1).Info("cancel: patch pod failed (best-effort)",
				"pod", pod.Name, "err", err.Error())
			continue
		}
	}
}
