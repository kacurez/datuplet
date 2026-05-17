package k8s

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// DBRunUpdater is the production RunStatusUpdater. It validates the
// PipelineRun's identity against the runs+projects+pipelines join
// before applying the phase write — otherwise any actor with write
// access to a project namespace could create a decoy PipelineRun with
// a victim's run-id label and hijack its mirrored status.
type DBRunUpdater struct {
	pool *pgxpool.Pool
}

// NewDBRunUpdater returns a DBRunUpdater over the given pool. Used by
// pipeline-api's serve path (via the Observer + coalesce chain) and by
// the reap-once subcommand (directly, for Expired marks before delete).
func NewDBRunUpdater(pool *pgxpool.Pool) *DBRunUpdater {
	return &DBRunUpdater{pool: pool}
}

// Update implements RunStatusUpdater. Returns (applied, err) where
// applied=true means the DB row was updated, applied=false with nil
// err means the observed_rv guard filtered the write or identity
// validation failed (benign drop), and (false, err) is a real DB
// error the caller should surface.
func (d *DBRunUpdater) Update(ctx context.Context, s RunStatus) (bool, error) {
	var expectedNS, pipelineName string
	err := d.pool.QueryRow(ctx, `
		SELECT p.k8s_namespace, pl.name FROM runs r
		  JOIN projects p  ON p.id  = r.project_id
		  JOIN pipelines pl ON pl.id = r.pipeline_id
		 WHERE r.id = $1
	`, s.RunID).Scan(&expectedNS, &pipelineName)
	if errors.Is(err, pgx.ErrNoRows) {
		// No matching run row — could be a reaped-but-visible
		// PipelineRun or an unauthorized copy. Benign drop.
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lookup run namespace: %w", err)
	}
	if s.Namespace != expectedNS {
		// Cross-namespace hijack attempt — drop silently.
		return false, nil
	}
	expectedName := CreateRunOpts{PipelineName: pipelineName, RunID: s.RunID}.PipelineRunName()
	if s.PipelineRunName != expectedName {
		// Decoy PipelineRun with the right label, wrong name.
		return false, nil
	}
	applied, err := store.UpdateRunPhase(ctx, d.pool, s.RunID, store.UpdateRunPhaseOpts{
		Phase:        s.Phase,
		CurrentStage: s.CurrentStage,
		Message:      s.Message,
		StartedAt:    s.StartedAt,
		CompletedAt:  s.CompletedAt,
		ObservedRV:   s.ResourceVersion,
	})
	if errors.Is(err, store.ErrRunNotFound) {
		return false, nil
	}
	return applied, err
}
