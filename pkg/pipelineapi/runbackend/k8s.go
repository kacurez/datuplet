package runbackend

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/config"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// RunTokenLifetime is the fixed 24h expiry for run tokens, matching the
// session-cookie TTL so a run cannot outlive the issuing session.
const RunTokenLifetime = 24 * time.Hour

// terminalPhases are the run phases that must not be overwritten by a
// late cancel. Matches the set in the old handler.
var terminalPhases = map[string]bool{
	"Succeeded":         true,
	"FailedUser":        true,
	"FailedApplication": true,
	"Cancelled":         true,
	// Expired is a terminal outcome written by the reaper when a run
	// was GC'd while still non-terminal. Treat it as terminal here so
	// a late cancel click on an old run doesn't rewrite the real final
	// state.
	"Expired": true,
}

// --- Seam interfaces (internal for testability) ---

// RunInserter abstracts inserting the runs row and marking it
// FailedApplication when a downstream step fails. The production adapter
// is storeInserter; tests inject a stub.
type RunInserter interface {
	Insert(ctx context.Context, opts store.CreateRunOpts) (*store.Run, error)
	// MarkFailed flips a previously-inserted run to FailedApplication
	// using a detached context so a client disconnect between Insert and
	// the failure doesn't also cancel the UPDATE and leave a ghost
	// Pending row. Best-effort.
	MarkFailed(ctx context.Context, runID uuid.UUID, reason error)
}

// ProjectNS abstracts "make sure a namespace exists for this project and
// give me its name".
type ProjectNS interface {
	Ensure(ctx context.Context, projectID uuid.UUID) (string, error)
}

// Minter mints a single per-run JWT. One token per run, audience
// `datuplet-catalog`, sub `user:oidc~<run-uuid>`, actor derived from
// ctx via tokens.subjectFromCtx. The run user carries a project-level
// FGA grant; the FGA model resolves per-table modify/select through
// inheritance.
type Minter interface {
	MintRun(ctx context.Context, spec tokens.RunSpec) (string, error)
}

// --- Production adapters ---

// NewStoreInserter returns a RunInserter backed by the Postgres pool.
func NewStoreInserter(pool *pgxpool.Pool) RunInserter { return &storeInserter{pool: pool} }

type storeInserter struct{ pool *pgxpool.Pool }

func (s *storeInserter) Insert(ctx context.Context, opts store.CreateRunOpts) (*store.Run, error) {
	return store.CreateRun(ctx, s.pool, opts)
}

// MarkFailed uses a detached context with a short timeout so a client
// disconnect between CreateRun and the failure doesn't cancel the
// UPDATE too and leave a ghost Pending row.
func (s *storeInserter) MarkFailed(ctx context.Context, runID uuid.UUID, reason error) {
	bgCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, _ = store.UpdateRunPhase(bgCtx, s.pool, runID, store.UpdateRunPhaseOpts{
		Phase:   "FailedApplication",
		Message: reason.Error(),
	})
}

// NewStoreProjectNS returns a ProjectNS that reads the canonical
// namespace name from Postgres and ensures the K8s Namespace exists.
func NewStoreProjectNS(pool *pgxpool.Pool, c client.Client) ProjectNS {
	return &storeProjectNS{pool: pool, client: c}
}

type storeProjectNS struct {
	pool   *pgxpool.Pool
	client client.Client
}

func (s *storeProjectNS) Ensure(ctx context.Context, projectID uuid.UUID) (string, error) {
	proj, err := store.GetProjectByID(ctx, s.pool, projectID)
	if err != nil {
		return "", fmt.Errorf("get project: %w", err)
	}
	if err := pkg8s.EnsureProjectNamespace(ctx, s.client, proj.ID); err != nil {
		return "", fmt.Errorf("ensure namespace: %w", err)
	}
	return proj.K8sNamespace, nil
}

// NewTokenMinter returns a Minter that wraps tokens.MintRunToken.
func NewTokenMinter(signer *tokens.Signer) Minter { return &tokenMinter{signer: signer} }

type tokenMinter struct{ signer *tokens.Signer }

func (m *tokenMinter) MintRun(ctx context.Context, spec tokens.RunSpec) (string, error) {
	return tokens.MintRunToken(ctx, m.signer, spec)
}

// --- K8sBackend ---

// K8sOpts bundles K8sBackend's dependencies. Authorizer + DB are required
// for the FGA-tuple compensating writes.
type K8sOpts struct {
	Client      client.Client
	RunInserter RunInserter
	ProjectNS   ProjectNS
	Minter      Minter
	Audience    string
	DB          *pgxpool.Pool // run_tuples + cancel; trigger uses RunInserter

	// Authorizer is the FGA backend the trigger flow writes the synthetic
	// run user's tuple(s) to. Required for run lifecycle correctness;
	// nil disables FGA writes (soft-degrade so test harnesses can still
	// exercise the trigger path without OpenFGA).
	Authorizer authz.Authorizer

	// WarehouseResolver resolves the lakekeeper warehouse name for a given
	// lakekeeper project ID at trigger time. The resolved name is baked
	// into the run-token JWT as the "warehouse" claim so DG and TableCommit
	// can self-route without operator-supplied env vars.
	//
	// nil soft-degrades: TriggerRun mints with an empty warehouse claim.
	// DG and TableCommit fail closed on an empty claim.
	WarehouseResolver func(ctx context.Context, lakekeeperProjectID string) (string, error)
}

// K8sBackend owns the cluster-mode lifecycle: Namespace + Pipeline CRD
// apply, runs-row insert, run_tuples + FGA writes, single-token mint,
// PipelineRun + Secret create; and on cancel the terminal-phase guard +
// CRD delete + DB update + FGA tuple cleanup.
type K8sBackend struct {
	client            client.Client
	runInserter       RunInserter
	projectNS         ProjectNS
	minter            Minter
	audience          string
	db                *pgxpool.Pool
	authzr            authz.Authorizer
	warehouseResolver func(ctx context.Context, lakekeeperProjectID string) (string, error)
}

// NewK8sBackend constructs a K8sBackend from opts. A zero Audience falls
// back to tokens.TableTokenAudience ("datuplet-catalog").
func NewK8sBackend(opts K8sOpts) *K8sBackend {
	aud := opts.Audience
	if aud == "" {
		aud = tokens.TableTokenAudience
	}
	return &K8sBackend{
		client:            opts.Client,
		runInserter:       opts.RunInserter,
		projectNS:         opts.ProjectNS,
		minter:            opts.Minter,
		audience:          aud,
		db:                opts.DB,
		authzr:            opts.Authorizer,
		warehouseResolver: opts.WarehouseResolver,
	}
}

// TriggerRun drives the four-step compensating-write ordering for the
// FGA tuple + runs row + K8s resources.
//
// Compensating sequence (NOT Postgres-ACID — FGA is HTTP, not a Postgres
// participant):
//
//  1. INSERT run_tuples row (committed=false) — recovery breadcrumb.
//  2. authzr.WriteTuples — synthetic user → relation → lakekeeper project.
//  3. INSERT runs row + create Secret + PipelineRun CR.
//  4. UPDATE run_tuples.committed=true.
//
// Crash recovery (reaper, k8s/reap_run_tuples.go):
//   - 1 then crash: row exists committed=false, no FGA tuples → reaper deletes the orphan row at age >30 min.
//   - 2 then crash: FGA tuples exist + row committed=false → reaper deletes both at age >30 min.
//   - 3 then crash: everything live, just committed=false → reaper LEAVES IT ALONE (Case 2 — non-terminal run).
//     The committed flag is never flipped by the reaper; the run completes through the
//     normal informer path and CompleteRun deletes the breadcrumb. The committed=true
//     UPDATE in step 4 is best-effort observability, not a correctness gate.
//
// Plus the existing flow:
//   - Ensure project namespace.
//   - Apply Pipeline CRD (lazy).
func (b *K8sBackend) TriggerRun(ctx context.Context, req TriggerRequest) (TriggerResponse, error) {
	// Pre-trigger: ensure namespace + apply Pipeline CRD (these are idempotent
	// and don't need rollback).
	ns, err := b.projectNS.Ensure(ctx, req.ProjectID)
	if err != nil {
		return TriggerResponse{}, err
	}
	if err := pkg8s.ApplyPipelineCRD(ctx, b.client, ns, string(req.PipelineYAML)); err != nil {
		return TriggerResponse{}, fmt.Errorf("apply pipeline: %w", err)
	}

	runID := uuid.New()
	syntheticSub := runID.String()

	// Resolve lakekeeper project ID once for tuple writes. A project that
	// hasn't yet been provisioned in lakekeeper has empty
	// LakekeeperProjectID — we soft-degrade by skipping FGA writes in
	// that case.
	var lakekeeperProjectID string
	if b.db != nil {
		proj, perr := store.GetProjectByID(ctx, b.db, req.ProjectID)
		if perr != nil {
			return TriggerResponse{}, fmt.Errorf("get project: %w", perr)
		}
		lakekeeperProjectID = proj.LakekeeperProjectID
	}

	// Resolve the warehouse for this project. The lakekeeperProjectID
	// comes from the same project row read above — single read scope
	// avoids a TOCTOU window on concurrent warehouse reassignment.
	// Nil resolver soft-degrades to an empty warehouse claim.
	var warehouse string
	if b.warehouseResolver != nil {
		wh, werr := b.warehouseResolver(ctx, lakekeeperProjectID)
		if werr != nil {
			return TriggerResponse{}, fmt.Errorf("resolve warehouse: %w", werr)
		}
		warehouse = wh
	}

	// Derive FGA tuples for the synthetic run user. One project-level
	// grant; the lakekeeper FGA model chains it to namespaces + tables.
	tuplesToWrite := buildRunTuples(syntheticSub, lakekeeperProjectID, req.Parsed)

	// --- Step 1: run_tuples row (intent recorded). ---
	if b.db != nil && len(tuplesToWrite) > 0 {
		if err := store.RecordRunTuples(ctx, b.db, runID, tuplesToWrite); err != nil {
			return TriggerResponse{}, fmt.Errorf("record run_tuples: %w", err)
		}
	}

	// --- Step 2: WriteTuples to FGA. ---
	if b.authzr != nil && len(tuplesToWrite) > 0 {
		if err := b.authzr.WriteTuples(ctx, tuplesToWrite); err != nil {
			// Best-effort cleanup of the recovery row so a retry isn't
			// blocked by a primary-key conflict. Detached ctx so a client
			// disconnect doesn't strand the row.
			cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			_ = store.DeleteRunTuples(cleanupCtx, b.db, runID)
			cancelCleanup()
			return TriggerResponse{}, fmt.Errorf("write FGA tuples: %w", err)
		}
	}

	// --- Step 3: insert runs row + create K8s resources. ---
	if _, err := b.runInserter.Insert(ctx, store.CreateRunOpts{
		ID:          runID,
		ProjectID:   req.ProjectID,
		PipelineID:  req.PipelineID,
		TriggeredBy: req.UserID,
	}); err != nil {
		// FGA tuples + run_tuples row remain; the reaper will sweep
		// them on the committed=false predicate.
		return TriggerResponse{}, fmt.Errorf("insert run: %w", err)
	}

	// The `project_id` claim carries the LAKEKEEPER project UUID, not the
	// Datuplet project UUID. DG forwards it as `x-project-id` on every
	// lakekeeper REST call, and the synthetic run user's FGA tuple was
	// written against the lakekeeper project. An empty `lakekeeperProjectID`
	// (single-project lakekeeper deploys with ENABLE_DEFAULT_PROJECT) is
	// fine; DG drops the header in that case.
	tok, err := b.minter.MintRun(ctx, tokens.RunSpec{
		RunID:        runID.String(),
		ProjectID:    lakekeeperProjectID,
		PipelineName: req.PipelineName,
		Warehouse:    warehouse,
		Audience:     b.audience,
		Lifetime:     RunTokenLifetime,
	})
	if err != nil {
		wrapped := fmt.Errorf("mint run token: %w", err)
		b.runInserter.MarkFailed(ctx, runID, wrapped)
		return TriggerResponse{}, wrapped
	}

	if err := pkg8s.CreateRunResources(ctx, b.client, pkg8s.CreateRunOpts{
		Namespace:    ns,
		PipelineName: req.PipelineName,
		RunID:        runID,
		Token:        tok,
	}); err != nil {
		wrapped := fmt.Errorf("create K8s resources: %w", err)
		b.runInserter.MarkFailed(ctx, runID, wrapped)
		return TriggerResponse{}, wrapped
	}

	// --- Step 4: mark run_tuples committed. ---
	if b.db != nil && len(tuplesToWrite) > 0 {
		if err := store.MarkRunTuplesCommitted(ctx, b.db, runID); err != nil {
			// Non-fatal — the reaper's self-heal observes the live runs row
			// and bumps committed=true on its sweep. Log and proceed.
			log.Printf("pipeline-api: mark run_tuples committed (run=%s): %v (non-fatal — reaper self-heals)", runID, err)
		}
	}

	return TriggerResponse{RunID: runID, Namespace: ns}, nil
}

// CancelRun cancels an active run. FGA tuple cleanup is hoisted to the
// front so the run user loses lakekeeper access within the STS-credential
// renewal window. Operation ordering on cancel:
//
//  1. Pre-checks (run exists, project match).
//  2. authzr.DeleteTuples for the run's FGA tuples + DELETE run_tuples row.
//     This is the AUTHORITATIVE step: DG/TC's next lakekeeper RPC fails
//     with PermissionDenied within ~5s (the STS-credential renewal cycle).
//     Errors here log and continue — the reaper picks up stragglers.
//  3. Terminal-already short-circuit (the CRD's own status wins).
//  4. Annotate pods + delete PipelineRun CRD (in-band cancel signal +
//     resource cleanup; runs in parallel with step 2 from DG's POV).
//  5. UPDATE runs row to Cancelled (under detached ctx).
//
// The reorder is load-bearing: deleting the K8s resources first wastes
// the seconds DG would otherwise need to discover its STS creds are
// dead. Deleting the FGA tuple first means the next /v1/credentials
// vended-creds rotate (or any lakekeeper REST call) returns 403
// immediately.
func (b *K8sBackend) CancelRun(ctx context.Context, req CancelRequest) error {
	run, err := store.GetRunByID(ctx, b.db, req.RunID)
	if err != nil {
		return err
	}
	if run.ProjectID != req.ProjectID {
		return store.ErrRunNotFound
	}

	alreadyTerminal := terminalPhases[run.Phase]

	// --- Step 2: FGA tuple delete FIRST. ---
	// We do this even when the runs row is "already terminal" — the
	// reaper-saves-us metric counts terminal-phase rows whose FGA
	// tuples didn't get cleaned up by the primary path. Repeating the
	// cleanup here is idempotent (no-op via isMissingTupleErr inside
	// authz.DeleteProjectTuples / cleanupRunTuples).
	{
		bgCtx, bgCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		b.cleanupRunTuples(bgCtx, req.RunID)
		bgCancel()
	}

	proj, err := store.GetProjectByID(ctx, b.db, req.ProjectID)
	if err != nil {
		return fmt.Errorf("get project: %w", err)
	}
	pipeName, err := store.GetPipelineNameByID(ctx, b.db, run.PipelineID)
	if err != nil {
		return fmt.Errorf("get pipeline: %w", err)
	}
	prName := pkg8s.CreateRunOpts{PipelineName: pipeName, RunID: req.RunID}.PipelineRunName()

	pr := &datupletv1.PipelineRun{}
	err = b.client.Get(ctx, types.NamespacedName{Name: prName, Namespace: proj.K8sNamespace}, pr)
	switch {
	case apierrors.IsNotFound(err):
		// CRD already gone — accept cancel as a no-op.
	case err != nil:
		return fmt.Errorf("get PipelineRun: %w", err)
	default:
		// CRD exists — if its live phase is terminal, trust the CRD.
		if terminalPhases[string(pr.Status.Phase)] {
			return nil
		}
		b.annotateRunPodsCancelled(ctx, proj.K8sNamespace, req.RunID.String())
		if err := b.client.Delete(ctx, pr); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete PipelineRun: %w", err)
		}
	}

	if !alreadyTerminal {
		bgCtx, bgCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer bgCancel()
		cancelNow := time.Now().UTC()
		if _, err := store.UpdateRunPhase(bgCtx, b.db, req.RunID, store.UpdateRunPhaseOpts{
			Phase:       "Cancelled",
			Message:     "cancelled by user",
			CompletedAt: &cancelNow,
		}); err != nil {
			return fmt.Errorf("update run phase: %w", err)
		}
	}
	return nil
}

// CompleteRun is invoked when a run reaches a terminal phase via the
// normal informer path (Succeeded / Failed / Expired). Drops the run's
// FGA tuples + the run_tuples row; failure logs and continues.
// CompleteRun is idempotent.
func (b *K8sBackend) CompleteRun(ctx context.Context, runID uuid.UUID) {
	bgCtx, bgCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer bgCancel()
	b.cleanupRunTuples(bgCtx, runID)
}

// cleanupRunTuples reads the run_tuples recovery row, asks FGA to delete
// the recorded tuples, and drops the row. Best-effort: any failure logs
// and continues (the reaper sweeps stragglers on its own schedule).
func (b *K8sBackend) cleanupRunTuples(ctx context.Context, runID uuid.UUID) {
	if b.db == nil {
		return
	}
	rec, err := store.GetRunTuples(ctx, b.db, runID)
	if err != nil {
		log.Printf("pipeline-api: cleanup get run_tuples (run=%s): %v", runID, err)
		return
	}
	if rec == nil {
		return
	}
	if b.authzr != nil && len(rec.Tuples) > 0 {
		if err := b.authzr.DeleteTuples(ctx, rec.Tuples); err != nil {
			log.Printf("pipeline-api: cleanup FGA delete (run=%s): %v (non-fatal — reaper retries)", runID, err)
			// Don't drop the row when the delete failed — the reaper's
			// next sweep will retry.
			return
		}
	}
	if err := store.DeleteRunTuples(ctx, b.db, runID); err != nil {
		log.Printf("pipeline-api: cleanup DELETE run_tuples (run=%s): %v", runID, err)
	}
}

// annotateRunPodsCancelled patches every Pod labelled with
// `datuplet.io/run-id=<runID>` in `namespace` to carry
// `datuplet.io/cancel=true`. Best-effort — pods pick up the annotation
// from the downward API within one poll cycle.
func (b *K8sBackend) annotateRunPodsCancelled(ctx context.Context, namespace, runID string) {
	pods := &corev1.PodList{}
	if err := b.client.List(ctx, pods,
		client.InNamespace(namespace),
		client.MatchingLabels{"datuplet.io/run-id": runID},
	); err != nil {
		log.Printf("pipeline-api: cancel annotate list pods (run=%s ns=%s): %v (best-effort, proceeding)", runID, namespace, err)
		return
	}
	if len(pods.Items) == 0 {
		return
	}
	patchBytes := []byte(`{"metadata":{"annotations":{"datuplet.io/cancel":"true"}}}`)
	for i := range pods.Items {
		pod := &pods.Items[i]
		if err := b.client.Patch(ctx, pod, client.RawPatch(types.MergePatchType, patchBytes)); err != nil {
			log.Printf("pipeline-api: cancel annotate patch pod %s/%s: %v (best-effort)", pod.Namespace, pod.Name, err)
			continue
		}
	}
}

// buildRunTuples derives the FGA tuples for the synthetic run user. One
// project-level grant; lakekeeper's FGA model chains it through
// warehouse → namespace → table.
//
// `editor` for any pipeline that writes; `viewer` for read-only. Both
// relations are Datuplet extensions on lakekeeper's `project` type; see
// pkg/pipelineapi/authz/fga_model.fga.
//
// Asymmetry — leaf write, canonical check: we WRITE the leaf relation
// (`editor` / `viewer`), but pipeline-api handlers and lakekeeper CHECK
// canonical relations (`data_admin` / `describe`) that resolve UPWARD
// through the FGA model union (`data_admin: ... or editor`). The leaf
// shape keeps tuples auditable as Datuplet roles; the upward resolution
// is what lets a Datuplet `editor` grant satisfy lakekeeper's
// `can_list_warehouses → can_get_metadata → describe → data_admin`
// chain on tables. Inverting either side breaks Bug C all over again.
//
// Returns nil when the lakekeeper project ID is empty (project not yet
// provisioned in lakekeeper — soft-degrade) or when the pipeline has no
// declared inputs/outputs.
//
// The synthetic user identity is `user:oidc~<run-uuid>` — produced by
// authz.UserObject from the run UUID. The same identity is what
// lakekeeper sees in the JWT's `sub` claim (MintRunToken sets
// `sub: user:oidc~<run-uuid>`).
func buildRunTuples(runUUID, lakekeeperProjectID string, parsed *config.Pipeline) []authz.Tuple {
	if lakekeeperProjectID == "" {
		return nil
	}
	intent := tokens.PipelineIntentFromPipeline(parsed)
	if !intent.HasReads && !intent.HasWrites {
		return nil
	}
	relation := authz.RelationViewer
	if intent.HasWrites {
		relation = authz.RelationEditor
	}
	return []authz.Tuple{{
		User:     authz.UserObject(runUUID).String(),
		Relation: relation,
		Object:   authz.ProjectObject(lakekeeperProjectID),
	}}
}
