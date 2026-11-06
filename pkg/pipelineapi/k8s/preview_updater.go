package k8s

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// RoutingUpdater sends preview PipelineRun statuses to Previews and
// everything else to Runs.
type RoutingUpdater struct {
	Runs, Previews RunStatusUpdater
}

func (r *RoutingUpdater) Update(ctx context.Context, s RunStatus) (bool, error) {
	if s.PreviewID != uuid.Nil {
		return r.Previews.Update(ctx, s)
	}
	return r.Runs.Update(ctx, s)
}

// DBPreviewUpdater mirrors preview PipelineRun status into the previews table,
// dropping decoys whose namespace or name do not match the stored preview.
type DBPreviewUpdater struct {
	pool *pgxpool.Pool
}

func NewDBPreviewUpdater(pool *pgxpool.Pool) *DBPreviewUpdater {
	return &DBPreviewUpdater{pool: pool}
}

func (d *DBPreviewUpdater) Update(ctx context.Context, s RunStatus) (bool, error) {
	p, err := store.GetPreviewByID(ctx, d.pool, s.PreviewID)
	if errors.Is(err, store.ErrPreviewNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lookup preview: %w", err)
	}
	if s.Namespace != p.K8sNamespace || s.PipelineRunName != "preview-"+p.ID.String() {
		return false, nil
	}
	return store.UpdatePreviewPhase(ctx, d.pool, p.ID, store.UpdatePreviewPhaseOpts{
		Phase:         s.Phase,
		CurrentStage:  s.CurrentStage,
		Message:       s.Message,
		StartedAt:     s.StartedAt,
		CompletedAt:   s.CompletedAt,
		ObservedRV:    s.ResourceVersion,
		StageStatuses: s.StageStatuses,
		GuardTerminal: true,
	})
}
