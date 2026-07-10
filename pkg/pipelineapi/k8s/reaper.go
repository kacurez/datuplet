package k8s

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
)

// terminalPhases are phases that mean the run is done; the reaper is free to
// delete these as soon as they're older than maxAge even if startTime was
// never set (validation-failed runs can go straight to FailedUser).
var terminalPhases = map[datupletv1.PipelineRunPhase]bool{
	datupletv1.PipelineRunPhaseSucceeded:         true,
	datupletv1.PipelineRunPhaseFailedUser:        true,
	datupletv1.PipelineRunPhaseFailedApplication: true,
}

// reapAge returns the effective age timestamp for a PipelineRun:
//   - For non-terminal runs that have started: startTime.
//   - For non-terminal runs that haven't started yet: zero (skip reaping —
//     they might just be slow).
//   - For terminal runs: completionTime if present, else startTime, else
//     the object's creationTimestamp (always set by the API server).
//
// Returns the zero Time value to signal "do not reap".
func reapAge(pr *datupletv1.PipelineRun) time.Time {
	terminal := terminalPhases[pr.Status.Phase]

	if !terminal {
		if pr.Status.StartTime != nil {
			return pr.Status.StartTime.Time
		}
		return time.Time{} // Pending with no start — leave alone.
	}

	// Terminal: prefer completion, then start, then creation.
	if pr.Status.CompletionTime != nil {
		return pr.Status.CompletionTime.Time
	}
	if pr.Status.StartTime != nil {
		return pr.Status.StartTime.Time
	}
	return pr.CreationTimestamp.Time
}

// ReapOnceOpts bundles the optional dependencies for the run_tuples sweep.
// Callers wire these through ReapOnceWith; ReapOnce keeps its narrow
// signature for the K8s-only sweep (reaper_test.go pins this shape).
type ReapOnceOpts struct {
	// Pool is the Postgres connection pool. Required for the
	// run_tuples sweep; nil disables that step (test/dev posture).
	Pool *pgxpool.Pool

	// Authorizer is the FGA backend used to DELETE tuples for
	// terminal-phase + orphan run_tuples rows. Nil disables FGA
	// writes; the row-level cleanup still runs so the breadcrumb
	// table doesn't grow unbounded.
	Authorizer authz.Authorizer
}

// ReapOnce deletes every PipelineRun whose reapAge is older than maxAge.
// Pending runs with no startTime are left alone so a slow pipeline isn't
// killed mid-boot. Terminal runs (Succeeded / FailedUser / FailedApplication)
// are reaped even if they never set startTime.
//
// Scoped to PipelineRuns carrying the `datuplet.io/run-id` label —
// pipeline-api stamps that label on every object it creates. An admin
// kubeconfig that can see hand-crafted PipelineRuns (e.g., written via
// kubectl apply) will NOT reap them.
//
// When `u` is non-nil and we're about to delete a still-non-terminal run,
// we first update its DB row to phase "Expired" so GET /runs doesn't
// perpetually show a Pending/Running ghost long after the CRD is gone.
// Pass nil when running in a test or cleanup-only context.
//
// Refuses to run with a non-positive maxAge: REAPER_MAX_AGE=0 would reap
// every started run on the next sweep and destroy live work. Safer to
// error out than to silently apply a destructive default.
//
// ReapOnce stays focused on the K8s + Secret sweep. The run_tuples sweep
// is bolted on by ReapOnceWith — callers that want both invoke that
// wrapper. The standalone shape is kept so reaper_test.go (which doesn't
// have a Postgres pool) still compiles.
func ReapOnce(ctx context.Context, c client.Client, maxAge time.Duration, u RunStatusUpdater) error {
	if maxAge <= 0 {
		return fmt.Errorf("reaper: maxAge must be positive (got %s); refusing to run", maxAge)
	}
	// labels.Everything().Add(...) would be equivalent; using the Exists
	// selector directly is cleaner for a single-key check.
	sel, err := labels.Parse("datuplet.io/run-id")
	if err != nil {
		return fmt.Errorf("build label selector: %w", err)
	}
	list := &datupletv1.PipelineRunList{}
	if err := c.List(ctx, list, &client.ListOptions{LabelSelector: sel}); err != nil {
		return fmt.Errorf("list PipelineRuns: %w", err)
	}
	cutoff := time.Now().Add(-maxAge)
	for i := range list.Items {
		pr := &list.Items[i]
		age := reapAge(pr)
		if age.IsZero() {
			continue
		}
		if !age.Before(cutoff) {
			continue
		}
		wasNonTerminal := !terminalPhases[pr.Status.Phase]
		err := c.Delete(ctx, pr)
		if err != nil && !apierrors.IsNotFound(err) {
			// Log and continue — operators can retry; the next sweep
			// will pick this up. Crucially we skip the DB Expired
			// update on delete failure so the mirrored phase doesn't
			// go terminal while the CRD is still live (e.g. transient
			// RBAC/API-server error). A structured logger would be
			// nicer here.
			log.Printf("pipeline-api reaper: delete %s/%s: %v", pr.Namespace, pr.Name, err)
			continue
		}
		// We successfully deleted iff err == nil. If NotFound, another
		// actor (e.g. a cancel handler) already removed the CRD, and
		// they're authoritative for the DB phase — don't overwrite a
		// fresher "Cancelled" with "Expired" using our stale list data.
		deleted := err == nil
		if u != nil && wasNonTerminal && deleted {
			if rid, parseErr := uuid.Parse(pr.Labels["datuplet.io/run-id"]); parseErr == nil {
				expiredAt := time.Now().UTC()
				if _, err := u.Update(ctx, RunStatus{
					RunID:           rid,
					Namespace:       pr.Namespace,
					PipelineRunName: pr.Name,
					Phase:           "Expired",
					Message:         fmt.Sprintf("reaped after %s", maxAge),
					CompletedAt:     &expiredAt,
				}); err != nil {
					log.Printf("pipeline-api reaper: expired mark run=%s: %v", rid, err)
				}
			}
		}
	}

	// Also reap orphan run-token Secrets — they would normally be GC'd by
	// OwnerReference when their PipelineRun is deleted, but CreateRunResources
	// step 4 (patch ownerReference) is best-effort and can leave a Secret
	// without an owner. Filter by the same label we stamp on creation and
	// age criterion: runtime metadata says CreationTimestamp > maxAge.
	if err := reapOrphanSecrets(ctx, c, cutoff); err != nil {
		log.Printf("pipeline-api reaper: orphan secrets: %v", err)
	}
	return nil
}

// reapOrphanSecrets sweeps orphan run-token / snapshot Secrets. RFC 026 P1.5
// moved Secret verbs off pipeline-api's ClusterRole into per-project-namespace
// Roles, so the reaper can no longer list Secrets cluster-wide. Instead it
// iterates the project namespaces (found via the cluster-wide
// `datuplet.io/project-id` label — the one namespace-scope verb still on the
// ClusterRole) and lists Secrets within each, authorized by the
// `datuplet-secrets` Role bound to the shared pipeline-api SA. The orphan
// detection (ownerReference skip, zero/after-cutoff skip) is unchanged.
func reapOrphanSecrets(ctx context.Context, c client.Client, cutoff time.Time) error {
	nsSel, err := labels.Parse(ProjectNamespaceLabel)
	if err != nil {
		return fmt.Errorf("build namespace selector: %w", err)
	}
	namespaces := &corev1.NamespaceList{}
	if err := c.List(ctx, namespaces, &client.ListOptions{LabelSelector: nsSel}); err != nil {
		return fmt.Errorf("list project namespaces: %w", err)
	}

	secretSel, err := labels.Parse("datuplet.io/run-id")
	if err != nil {
		return fmt.Errorf("build label selector: %w", err)
	}
	for i := range namespaces.Items {
		ns := namespaces.Items[i].Name
		secrets := &corev1.SecretList{}
		if err := c.List(ctx, secrets, &client.ListOptions{Namespace: ns, LabelSelector: secretSel}); err != nil {
			// Log + continue so one namespace's transient error (e.g. RBAC
			// still propagating) doesn't abort the whole sweep.
			log.Printf("pipeline-api reaper: list secrets in ns %s: %v", ns, err)
			continue
		}
		for j := range secrets.Items {
			sec := &secrets.Items[j]
			// Skip if an ownerReference still ties this Secret to a PipelineRun —
			// GC will handle cleanup when the owner is deleted.
			if len(sec.OwnerReferences) > 0 {
				continue
			}
			if sec.CreationTimestamp.Time.IsZero() {
				continue
			}
			if !sec.CreationTimestamp.Time.Before(cutoff) {
				continue
			}
			if err := c.Delete(ctx, sec); err != nil && !apierrors.IsNotFound(err) {
				log.Printf("pipeline-api reaper: delete Secret %s/%s: %v", sec.Namespace, sec.Name, err)
			}
		}
	}
	return nil
}

// ReapOnceWith runs ReapOnce + the run_tuples sweep in one pass.
// K8s-side errors are returned (the caller may want to fail
// the CronJob); run_tuples errors log + soldier on so a transient FGA
// outage doesn't leave PipelineRuns un-reaped.
//
// Pass opts.Pool=nil to skip the run_tuples step entirely (the binary's
// "OpenFGA unreachable" soft-degrade); pass opts.Authorizer=nil to skip
// only the FGA DeleteTuples call while still GC'ing the row.
func ReapOnceWith(ctx context.Context, c client.Client, maxAge time.Duration, u RunStatusUpdater, opts ReapOnceOpts) error {
	if err := ReapOnce(ctx, c, maxAge, u); err != nil {
		return err
	}
	if opts.Pool != nil {
		if err := ReapRunTuples(ctx, opts.Pool, c, opts.Authorizer); err != nil {
			log.Printf("pipeline-api reaper: run_tuples sweep: %v (non-fatal — retrying next cycle)", err)
		}
	}
	return nil
}
