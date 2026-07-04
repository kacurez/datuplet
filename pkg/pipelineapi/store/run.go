package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Run is the in-memory view of a runs row.
type Run struct {
	ID           uuid.UUID
	ProjectID    uuid.UUID
	PipelineID   uuid.UUID
	Phase        string
	CurrentStage string
	Message      string
	// PipelineName is populated only when a query joins the pipelines table
	// (ListRunsPage, GetRunByID). Empty otherwise.
	PipelineName string
	// StageStatuses is the raw JSON snapshot of PipelineRun.Status.StageStatuses,
	// or nil when no timeline has been recorded. Populated by GetRunByID only.
	StageStatuses []byte
	StartedAt     *time.Time
	CompletedAt   *time.Time
	TriggeredBy   uuid.UUID // zero-value if system-triggered
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// ErrRunNotFound is returned when no run matches the lookup.
var ErrRunNotFound = errors.New("run not found")

// CreateRunOpts is the input to CreateRun.
type CreateRunOpts struct {
	ID          uuid.UUID // caller-generated; also used as PipelineRun runId claim
	ProjectID   uuid.UUID
	PipelineID  uuid.UUID
	TriggeredBy uuid.UUID // uuid.Nil if no user in context (system trigger)
}

// CreateRun inserts a new runs row in phase "Pending".
func CreateRun(ctx context.Context, pool *pgxpool.Pool, opts CreateRunOpts) (*Run, error) {
	var triggeredBy *uuid.UUID
	if opts.TriggeredBy != uuid.Nil {
		triggeredBy = &opts.TriggeredBy
	}
	r := &Run{
		ID: opts.ID, ProjectID: opts.ProjectID, PipelineID: opts.PipelineID,
		Phase: "Pending",
	}
	err := pool.QueryRow(ctx,
		`INSERT INTO runs(id, project_id, pipeline_id, triggered_by)
		 VALUES ($1, $2, $3, $4)
		 RETURNING created_at, updated_at`,
		opts.ID, opts.ProjectID, opts.PipelineID, triggeredBy,
	).Scan(&r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("insert run: %w", err)
	}
	if triggeredBy != nil {
		r.TriggeredBy = *triggeredBy
	}
	return r, nil
}

// GetRunByID returns a run or ErrRunNotFound.
func GetRunByID(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) (*Run, error) {
	r := &Run{}
	var triggeredBy *uuid.UUID
	err := pool.QueryRow(ctx,
		`SELECT r.id, r.project_id, r.pipeline_id, pl.name, r.phase, r.current_stage,
		        r.message, r.started_at, r.completed_at, r.triggered_by,
		        r.created_at, r.updated_at, r.stage_statuses
		   FROM runs r
		   JOIN pipelines pl ON pl.id = r.pipeline_id
		  WHERE r.id = $1`, id,
	).Scan(&r.ID, &r.ProjectID, &r.PipelineID, &r.PipelineName, &r.Phase, &r.CurrentStage,
		&r.Message, &r.StartedAt, &r.CompletedAt, &triggeredBy,
		&r.CreatedAt, &r.UpdatedAt, &r.StageStatuses)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRunNotFound
		}
		return nil, fmt.Errorf("select run: %w", err)
	}
	if triggeredBy != nil {
		r.TriggeredBy = *triggeredBy
	}
	return r, nil
}

// ListRunsForProject returns the most-recent `limit` runs in the project,
// newest first.
func ListRunsForProject(ctx context.Context, pool *pgxpool.Pool, projectID uuid.UUID, limit int) ([]*Run, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := pool.Query(ctx,
		`SELECT id, project_id, pipeline_id, phase, current_stage, message,
		        started_at, completed_at, triggered_by, created_at, updated_at
		   FROM runs WHERE project_id = $1
		  ORDER BY created_at DESC
		  LIMIT $2`, projectID, limit)
	if err != nil {
		return nil, fmt.Errorf("query runs: %w", err)
	}
	defer rows.Close()
	var out []*Run
	for rows.Next() {
		r := &Run{}
		var triggeredBy *uuid.UUID
		if err := rows.Scan(&r.ID, &r.ProjectID, &r.PipelineID, &r.Phase, &r.CurrentStage, &r.Message,
			&r.StartedAt, &r.CompletedAt, &triggeredBy, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		if triggeredBy != nil {
			r.TriggeredBy = *triggeredBy
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// UpdateRunPhaseOpts is the input to UpdateRunPhase. ObservedRV activates
// the monotonic-rv guard: when > 0, the UPDATE is predicated on
// `($rv = 0 OR $rv > observed_rv)` so out-of-order or replayed events
// from the observer are silently dropped. Callers (cancel, reaper,
// admin) that don't have an rv leave ObservedRV at 0 and retain
// unconditional-write semantics — GREATEST(observed_rv, $rv) keeps
// observed_rv untouched when rv=0.
type UpdateRunPhaseOpts struct {
	Phase        string
	CurrentStage string
	Message      string
	StartedAt    *time.Time
	CompletedAt  *time.Time
	ObservedRV   int64
	// StageStatuses is the raw JSON timeline snapshot. nil means "preserve the
	// existing value" (COALESCE) — terminal writers (cancel/reaper) pass nil so
	// they never null a snapshot the observer already captured.
	StageStatuses []byte
	// GuardTerminal, when true, refuses to overwrite a row already in an
	// out-of-band terminal phase (Cancelled/Expired). Set by the observer's
	// DBRunUpdater so a stale Running reconcile cannot resurrect a cancelled run.
	GuardTerminal bool
}

// UpdateRunPhase writes the phase transition. Returns:
//   - (true,  nil)                   row updated
//   - (false, nil)                   ObservedRV > 0 and the guard filtered
//     the write (stale event from informer)
//   - (false, ErrRunNotFound)        no matching row, ObservedRV == 0
//   - (false, err)                   other DB error
//
// For ObservedRV > 0 we treat "no matching row" the same as stale-rv: the
// coalesce decorator doesn't need to distinguish them, and a reaped run
// shouldn't be re-inserted into the coalesce cache. Cancel/admin callers
// (ObservedRV == 0) still get ErrRunNotFound so the HTTP handler can
// surface a 404.
func UpdateRunPhase(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, opts UpdateRunPhaseOpts) (bool, error) {
	guard := ""
	if opts.GuardTerminal {
		guard = ` AND phase <> ALL('{Cancelled,Expired}')`
	}
	tag, err := pool.Exec(ctx,
		`UPDATE runs SET
		    phase = $2,
		    current_stage = $3,
		    message = $4,
		    started_at = COALESCE($5, started_at),
		    completed_at = COALESCE($6, completed_at),
		    stage_statuses = COALESCE($8, stage_statuses),
		    observed_rv = GREATEST(observed_rv, $7::bigint),
		    updated_at = now()
		  WHERE id = $1
		    AND ($7::bigint = 0 OR $7::bigint > observed_rv)`+guard,
		id, opts.Phase, opts.CurrentStage, opts.Message, opts.StartedAt, opts.CompletedAt,
		opts.ObservedRV, opts.StageStatuses,
	)
	if err != nil {
		return false, fmt.Errorf("update run: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if opts.ObservedRV == 0 && !opts.GuardTerminal {
			return false, ErrRunNotFound
		}
		return false, nil
	}
	return true, nil
}
