package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/metrics"
)

// orphanRunTuplesAge is the breadcrumb-staleness threshold for the
// orphan sweep: a run_tuples row whose created_at is older than this
// AND has no live runs row in non-terminal state AND no live PipelineRun
// is treated as a crashed-trigger leftover and purged. 30 minutes is the
// 30 minutes: long enough that a slow trigger (FGA write + K8s create +
// DB insert) won't be misclassified as a crash, short enough that the
// crash-recovery cancel SLO of ≤5min still gets hit on the next sweep.
const orphanRunTuplesAge = 30 * time.Minute

// terminalDBPhases mirrors runbackend.terminalPhases but lives here so
// the reaper doesn't import runbackend (would cycle: runbackend already
// imports pkg8s for K8s helpers). The set must stay in sync.
var terminalDBPhases = map[string]bool{
	"Succeeded":         true,
	"FailedUser":        true,
	"FailedApplication": true,
	"Cancelled":         true,
	"Expired":           true,
}

// runTupleSweepRow is the join row returned by the reaper's scan over
// run_tuples LEFT JOIN runs. The reaper interprets it via three
// disjoint cases:
//
//  1. runs row exists, terminal phase → DeleteTuples + DELETE row
//     (and bump terminal_with_tuples metric — should be 0 in steady
//     state; non-zero indicates a primary-cancel-path leak).
//  2. runs row exists, NON-terminal phase → leave alone (run still in
//     flight, primary path will clean up at completion).
//  3. runs row missing AND created_at < now() - 30m → orphan; check
//     for a live PipelineRun by run-id label, and if absent
//     DeleteTuples + DELETE row (bump orphan metric).
type runTupleSweepRow struct {
	RunID     uuid.UUID
	Tuples    []authz.Tuple
	HasRun    bool
	RunPhase  string
	CreatedAt time.Time
}

// ReapRunTuples iterates run_tuples and reconciles against the runs table
// + live PipelineRuns. Idempotent —
// re-running on the same data is a no-op.
//
// authzr=nil disables FGA writes (soft-degrade for environments where
// the OpenFGA deploy isn't reachable); the row-level cleanup still runs
// for terminal rows so the breadcrumb table doesn't grow without bound.
//
// k8sClient may be nil when running outside the cluster (e.g. in unit
// tests); the orphan-PipelineRun probe is then skipped and orphans are
// determined purely by the runs-row + age predicate.
func ReapRunTuples(ctx context.Context, pool *pgxpool.Pool, k8sClient client.Client, authzr authz.Authorizer) error {
	if pool == nil {
		return fmt.Errorf("ReapRunTuples: pool is required")
	}

	rows, err := pool.Query(ctx, `
		SELECT rt.run_id, rt.tuples, rt.created_at,
		       (r.id IS NOT NULL) AS has_run,
		       COALESCE(r.phase, '') AS run_phase
		  FROM run_tuples rt
		  LEFT JOIN runs r ON r.id = rt.run_id`)
	if err != nil {
		return fmt.Errorf("ReapRunTuples: select: %w", err)
	}
	defer rows.Close()

	cutoff := time.Now().Add(-orphanRunTuplesAge)

	var sweepRows []runTupleSweepRow
	for rows.Next() {
		var (
			id        uuid.UUID
			raw       []byte
			createdAt time.Time
			hasRun    bool
			runPhase  string
		)
		if err := rows.Scan(&id, &raw, &createdAt, &hasRun, &runPhase); err != nil {
			return fmt.Errorf("ReapRunTuples: scan: %w", err)
		}
		tuples, err := decodeRunTuples(raw)
		if err != nil {
			log.Printf("pipeline-api reaper: skip run=%s — decode tuples: %v", id, err)
			continue
		}
		sweepRows = append(sweepRows, runTupleSweepRow{
			RunID: id, Tuples: tuples,
			HasRun: hasRun, RunPhase: runPhase, CreatedAt: createdAt,
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("ReapRunTuples: iterate: %w", err)
	}

	for _, r := range sweepRows {
		switch {
		case r.HasRun && terminalDBPhases[r.RunPhase]:
			// Case 1: terminal-phase row left behind by a cancel/complete
			// path that crashed before deleting the FGA tuples. Reaper
			// finishes the cleanup and bumps the "saved us" metric.
			if len(r.Tuples) > 0 {
				metrics.ReaperRunTuplesTerminalWithTuplesTotal.Inc()
			}
			cleanupRunTuplesRow(ctx, pool, authzr, r)

		case r.HasRun:
			// Case 2: non-terminal — primary path owns this. Leave alone.
			continue

		case !r.HasRun && r.CreatedAt.Before(cutoff):
			// Case 3: orphan candidate — runs row never landed (or was
			// reaped at 24h while the run_tuples row escaped CASCADE
			// because it's keyed on the runs FK and the runs row got
			// archived rather than deleted). Probe the K8s API to make
			// sure no PipelineRun by this run-id is still live.
			if k8sClient != nil {
				live, perr := pipelineRunIsLive(ctx, k8sClient, r.RunID)
				if perr != nil {
					log.Printf("pipeline-api reaper: probe PipelineRun run=%s: %v (skip — retry next sweep)", r.RunID, perr)
					continue
				}
				if live {
					continue
				}
			}
			cleanupRunTuplesRow(ctx, pool, authzr, r)
			metrics.ReaperRunTuplesOrphanedTotal.Inc()

		default:
			// !HasRun and not yet past the orphan deadline — too young
			// to GC. Leave alone; next sweep will pick it up if still
			// stuck.
			continue
		}
	}
	return nil
}

// cleanupRunTuplesRow asks FGA to delete the recorded tuples (idempotent
// — missing tuples are ignored) and then drops the run_tuples row.
// Errors log + return; the row stays so the next sweep retries.
func cleanupRunTuplesRow(ctx context.Context, pool *pgxpool.Pool, authzr authz.Authorizer, r runTupleSweepRow) {
	if authzr != nil && len(r.Tuples) > 0 {
		if err := authzr.DeleteTuples(ctx, r.Tuples); err != nil {
			if !isMissingTupleErr(err) {
				log.Printf("pipeline-api reaper: DeleteTuples run=%s: %v (retry next sweep)", r.RunID, err)
				return
			}
			// missing-tuple is benign — proceed to drop the row.
		}
	}
	if _, err := pool.Exec(ctx, `DELETE FROM run_tuples WHERE run_id = $1`, r.RunID); err != nil {
		log.Printf("pipeline-api reaper: DELETE run_tuples run=%s: %v", r.RunID, err)
	}
}

// pipelineRunIsLive reports whether a PipelineRun labelled with
// run-id == runID exists in any namespace. Used by the orphan path to
// avoid deleting tuples for a run whose runs row failed to insert but
// whose K8s resources are still scheduled.
//
// Fail-open policy: on a transient API error (apiserver hiccup, cache
// stall) the caller logs + continues to the next row, leaving the
// breadcrumb in place for the NEXT sweep. Better to keep a stale tuple
// alive an extra 5 minutes than to delete a legitimate in-flight run's
// access grant on a transient failure. Steady-state operators reading
// the metrics will see the orphan-counter stay flat across the noisy
// sweep + recover the next tick.
func pipelineRunIsLive(ctx context.Context, c client.Client, runID uuid.UUID) (bool, error) {
	sel, err := labels.Parse("datuplet.io/run-id=" + runID.String())
	if err != nil {
		return false, fmt.Errorf("build label selector: %w", err)
	}
	list := &datupletv1.PipelineRunList{}
	if err := c.List(ctx, list, &client.ListOptions{LabelSelector: sel}); err != nil {
		return false, fmt.Errorf("list PipelineRuns: %w", err)
	}
	return len(list.Items) > 0, nil
}

// decodeRunTuples mirrors store.decodeAuthzTuples but lives here so the
// reaper doesn't have to import the store package (small duplication
// kept narrow on purpose). Same on-wire shape as
// pkg/pipelineapi/store/run_tuples.go.
func decodeRunTuples(raw []byte) ([]authz.Tuple, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	type tupleJSON struct {
		User     string `json:"user"`
		Relation string `json:"relation"`
		Object   string `json:"object"`
	}
	var arr []tupleJSON
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, err
	}
	out := make([]authz.Tuple, 0, len(arr))
	for _, t := range arr {
		obj, err := authz.ParseObject(t.Object)
		if err != nil {
			return nil, fmt.Errorf("parse object %q: %w", t.Object, err)
		}
		out = append(out, authz.Tuple{
			User:     t.User,
			Relation: t.Relation,
			Object:   obj,
		})
	}
	return out, nil
}

// isMissingTupleErr mirrors authz.isMissingTupleErr (which is unexported
// in that package) — a duplicate-tuple delete is a no-op semantically,
// but the reaper has to recognize the wrapped wire-shape OpenFGA
// returns. Keep the strings aligned with authz/reconciler.go.
func isMissingTupleErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "cannot delete a tuple which does not exist")
}

