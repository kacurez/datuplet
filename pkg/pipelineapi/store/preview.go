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

// Preview is the in-memory view of a previews row.
type Preview struct {
	ID, ProjectID       uuid.UUID
	TriggeredBy         uuid.UUID // zero-value if no user
	Workspace           string
	Doc                 []byte
	RowLimit            int
	TimeoutSeconds      int
	TTLSeconds          int
	UntilStage          string
	ScratchNamespaces   []string
	LakekeeperProjectID string
	Warehouse           string
	K8sNamespace        string
	Phase               string
	CurrentStage        string
	Message             string
	StageStatuses       []byte
	Result              []byte
	FGATupleDeletedAt   *time.Time
	StartedAt           *time.Time
	CompletedAt         *time.Time
	CleanedAt           *time.Time
	CreatedAt           time.Time
	// ExpiresAt, when zero on insert, defaults to created_at + timeout + ttl.
	ExpiresAt time.Time
}

var ErrPreviewNotFound = errors.New("preview not found")

// ErrPreviewCap is matched (errors.Is) by *PreviewCapError.
var ErrPreviewCap = errors.New("preview cap reached")

// PreviewCaps bounds concurrent and retained previews. A value <= 0 disables
// that cap.
type PreviewCaps struct {
	RunningPerWorkspace, RunningPerProject, MaxPerProject int
}

// PreviewCapError reports which counts tripped InsertPreview's caps.
type PreviewCapError struct {
	RunningInWorkspace, RunningInProject, LiveInProject int
}

func (e *PreviewCapError) Error() string {
	return fmt.Sprintf("preview cap reached: %d running in workspace, %d running in project, %d live in project",
		e.RunningInWorkspace, e.RunningInProject, e.LiveInProject)
}

func (e *PreviewCapError) Is(target error) bool { return target == ErrPreviewCap }

const previewCols = `id, project_id, triggered_by, workspace, doc, row_limit, timeout_seconds,
	ttl_seconds, until_stage, scratch_namespaces, lakekeeper_project_id, warehouse,
	k8s_namespace, phase, current_stage, message, stage_statuses, result,
	fga_tuple_deleted_at, started_at, completed_at, cleaned_at, created_at, expires_at`

func scanPreview(row pgx.Row) (*Preview, error) {
	p := &Preview{}
	var triggeredBy *uuid.UUID
	var untilStage *string
	if err := row.Scan(&p.ID, &p.ProjectID, &triggeredBy, &p.Workspace, &p.Doc, &p.RowLimit,
		&p.TimeoutSeconds, &p.TTLSeconds, &untilStage, &p.ScratchNamespaces,
		&p.LakekeeperProjectID, &p.Warehouse, &p.K8sNamespace, &p.Phase, &p.CurrentStage,
		&p.Message, &p.StageStatuses, &p.Result, &p.FGATupleDeletedAt, &p.StartedAt,
		&p.CompletedAt, &p.CleanedAt, &p.CreatedAt, &p.ExpiresAt); err != nil {
		return nil, err
	}
	if triggeredBy != nil {
		p.TriggeredBy = *triggeredBy
	}
	if untilStage != nil {
		p.UntilStage = *untilStage
	}
	return p, nil
}

// InsertPreview enforces the caps and inserts p under a per-project advisory
// lock so concurrent creates cannot overshoot them. Cleaned rows whose FGA
// tuple is gone are evicted oldest-first to stay within MaxPerProject.
func InsertPreview(ctx context.Context, pool *pgxpool.Pool, p *Preview, caps PreviewCaps) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('previews:' || $1::text))`, p.ProjectID); err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	if caps.MaxPerProject > 0 {
		if _, err := tx.Exec(ctx, `
			DELETE FROM previews WHERE id IN (
			  SELECT id FROM previews
			   WHERE project_id = $1 AND cleaned_at IS NOT NULL AND fga_tuple_deleted_at IS NOT NULL
			   ORDER BY created_at ASC, id ASC
			   LIMIT GREATEST(0, (SELECT count(*) FROM previews WHERE project_id = $1) - $2 + 1))`,
			p.ProjectID, caps.MaxPerProject); err != nil {
			return fmt.Errorf("evict cleaned previews: %w", err)
		}
	}
	var capErr PreviewCapError
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE phase IN ('Pending','Running') AND cleaned_at IS NULL AND workspace = $2),
		       count(*) FILTER (WHERE phase IN ('Pending','Running') AND cleaned_at IS NULL),
		       count(*) FILTER (WHERE cleaned_at IS NULL)
		  FROM previews WHERE project_id = $1`, p.ProjectID, p.Workspace,
	).Scan(&capErr.RunningInWorkspace, &capErr.RunningInProject, &capErr.LiveInProject); err != nil {
		return fmt.Errorf("count previews: %w", err)
	}
	if (caps.RunningPerWorkspace > 0 && capErr.RunningInWorkspace >= caps.RunningPerWorkspace) ||
		(caps.RunningPerProject > 0 && capErr.RunningInProject >= caps.RunningPerProject) ||
		(caps.MaxPerProject > 0 && capErr.LiveInProject >= caps.MaxPerProject) {
		return &capErr
	}

	var triggeredBy *uuid.UUID
	if p.TriggeredBy != uuid.Nil {
		triggeredBy = &p.TriggeredBy
	}
	var untilStage *string
	if p.UntilStage != "" {
		untilStage = &p.UntilStage
	}
	ns := p.ScratchNamespaces
	if ns == nil {
		ns = []string{}
	}
	var expiresAt *time.Time
	if !p.ExpiresAt.IsZero() {
		expiresAt = &p.ExpiresAt
	}
	phase := p.Phase
	if phase == "" {
		phase = "Pending"
	}
	if err := tx.QueryRow(ctx, `
		INSERT INTO previews(id, project_id, triggered_by, workspace, doc, row_limit,
		       timeout_seconds, ttl_seconds, until_stage, scratch_namespaces,
		       lakekeeper_project_id, warehouse, k8s_namespace, phase, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
		        COALESCE($15, now() + make_interval(secs => $7::int + $8::int)))
		RETURNING created_at, expires_at`,
		p.ID, p.ProjectID, triggeredBy, p.Workspace, p.Doc, p.RowLimit, p.TimeoutSeconds,
		p.TTLSeconds, untilStage, ns, p.LakekeeperProjectID, p.Warehouse, p.K8sNamespace,
		phase, expiresAt,
	).Scan(&p.CreatedAt, &p.ExpiresAt); err != nil {
		return fmt.Errorf("insert preview: %w", err)
	}
	p.Phase = phase
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// GetPreview returns a preview scoped to its project, or ErrPreviewNotFound.
func GetPreview(ctx context.Context, pool *pgxpool.Pool, projectID, id uuid.UUID) (*Preview, error) {
	p, err := scanPreview(pool.QueryRow(ctx,
		`SELECT `+previewCols+` FROM previews WHERE id = $1 AND project_id = $2`, id, projectID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrPreviewNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select preview: %w", err)
	}
	return p, nil
}

// GetPreviewByID returns a preview without project scoping (observer, sweeper).
func GetPreviewByID(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) (*Preview, error) {
	p, err := scanPreview(pool.QueryRow(ctx,
		`SELECT `+previewCols+` FROM previews WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrPreviewNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select preview: %w", err)
	}
	return p, nil
}

func queryPreviews(ctx context.Context, pool *pgxpool.Pool, q string, args ...any) ([]Preview, error) {
	rows, err := pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query previews: %w", err)
	}
	defer rows.Close()
	var out []Preview
	for rows.Next() {
		p, err := scanPreview(rows)
		if err != nil {
			return nil, fmt.Errorf("scan preview: %w", err)
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

// ListPreviews returns one keyset page (newest first) of a project's previews.
// workspace "*" lists every workspace. The returned cursor is "" on the last page.
func ListPreviews(ctx context.Context, pool *pgxpool.Pool, projectID uuid.UUID, workspace string, limit int, cursor string) ([]Preview, string, error) {
	if limit <= 0 {
		limit = 50
	} else if limit > 200 {
		limit = 200
	}
	args := []any{projectID}
	q := `SELECT ` + previewCols + ` FROM previews WHERE project_id = $1`
	if workspace != "*" {
		args = append(args, workspace)
		q += fmt.Sprintf(` AND workspace = $%d`, len(args))
	}
	if cursor != "" {
		ct, cid, err := decodeCursor(cursor)
		if err != nil {
			return nil, "", err
		}
		args = append(args, ct, cid)
		q += fmt.Sprintf(` AND (created_at, id) < ($%d, $%d)`, len(args)-1, len(args))
	}
	args = append(args, limit+1)
	q += fmt.Sprintf(` ORDER BY created_at DESC, id DESC LIMIT $%d`, len(args))
	out, err := queryPreviews(ctx, pool, q, args...)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > limit {
		last := out[limit-1]
		next = encodeCursor(last.CreatedAt, last.ID)
		out = out[:limit]
	}
	return out, next, nil
}

// UpdatePreviewPhaseOpts mirrors UpdateRunPhaseOpts for previews.
type UpdatePreviewPhaseOpts struct {
	Phase, CurrentStage, Message string
	StartedAt, CompletedAt       *time.Time
	ObservedRV                   int64
	StageStatuses                []byte
	GuardTerminal                bool
}

// UpdatePreviewPhase writes a phase transition with the same observed_rv and
// terminal guards as UpdateRunPhase. A terminal phase resets expires_at to
// completed_at (or now) + ttl_seconds. Returns false when the guards (or a
// missing row) filtered the write.
func UpdatePreviewPhase(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, o UpdatePreviewPhaseOpts) (bool, error) {
	guard := ""
	if o.GuardTerminal {
		guard = ` AND phase <> ALL('{Cancelled,Expired}')`
	}
	tag, err := pool.Exec(ctx, `
		UPDATE previews SET
		    phase = $2,
		    current_stage = $3,
		    message = $4,
		    started_at = COALESCE($5, started_at),
		    completed_at = COALESCE($6, completed_at),
		    stage_statuses = COALESCE($8, stage_statuses),
		    observed_rv = GREATEST(observed_rv, $7::bigint),
		    expires_at = CASE
		        WHEN $2 IN ('Succeeded','FailedUser','FailedApplication','Cancelled','Expired')
		        THEN COALESCE($6, completed_at, now()) + make_interval(secs => ttl_seconds)
		        ELSE expires_at END
		  WHERE id = $1
		    AND ($7::bigint = 0 OR $7::bigint > observed_rv)`+guard,
		id, o.Phase, o.CurrentStage, o.Message, o.StartedAt, o.CompletedAt, o.ObservedRV, o.StageStatuses)
	if err != nil {
		return false, fmt.Errorf("update preview: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// SetPreviewResult caches the result document once; later calls are no-ops.
func SetPreviewResult(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, result []byte) error {
	if _, err := pool.Exec(ctx,
		`UPDATE previews SET result = $2 WHERE id = $1 AND result IS NULL`, id, result); err != nil {
		return fmt.Errorf("set preview result: %w", err)
	}
	return nil
}

// OverwritePreviewResult replaces a cached result document.
func OverwritePreviewResult(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, result []byte) error {
	if _, err := pool.Exec(ctx, `UPDATE previews SET result = $2 WHERE id = $1`, id, result); err != nil {
		return fmt.Errorf("overwrite preview result: %w", err)
	}
	return nil
}

func MarkPreviewTupleDeleted(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, at time.Time) error {
	if _, err := pool.Exec(ctx,
		`UPDATE previews SET fga_tuple_deleted_at = $2 WHERE id = $1`, id, at); err != nil {
		return fmt.Errorf("mark preview tuple deleted: %w", err)
	}
	return nil
}

// ExpirePreviewNow pulls expires_at in to now (never later) so the sweeper
// retries a purge that fails.
func ExpirePreviewNow(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, now time.Time) error {
	if _, err := pool.Exec(ctx,
		`UPDATE previews SET expires_at = LEAST(expires_at, $2) WHERE id = $1`, id, now); err != nil {
		return fmt.Errorf("expire preview: %w", err)
	}
	return nil
}

// MarkPreviewCleaned stamps cleaned_at. With expired=true a non-terminal
// preview also moves to phase Expired.
func MarkPreviewCleaned(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, at time.Time, expired bool) error {
	if _, err := pool.Exec(ctx, `
		UPDATE previews SET
		    cleaned_at = $2,
		    phase = CASE WHEN $3 AND phase IN ('Pending','Running') THEN 'Expired' ELSE phase END
		  WHERE id = $1`, id, at, expired); err != nil {
		return fmt.Errorf("mark preview cleaned: %w", err)
	}
	return nil
}

// ListPreviewsNeedingTupleDelete returns terminal previews whose run identity
// tuples have not been deleted yet.
func ListPreviewsNeedingTupleDelete(ctx context.Context, pool *pgxpool.Pool, limit int) ([]Preview, error) {
	return queryPreviews(ctx, pool, `SELECT `+previewCols+` FROM previews
		WHERE phase IN ('Succeeded','FailedUser','FailedApplication','Cancelled','Expired')
		  AND fga_tuple_deleted_at IS NULL
		ORDER BY created_at ASC LIMIT $1`, limit)
}

// ListExpiredPreviews returns uncleaned previews past expires_at.
func ListExpiredPreviews(ctx context.Context, pool *pgxpool.Pool, now time.Time, limit int) ([]Preview, error) {
	return queryPreviews(ctx, pool, `SELECT `+previewCols+` FROM previews
		WHERE expires_at < $1 AND cleaned_at IS NULL
		ORDER BY expires_at ASC LIMIT $2`, now, limit)
}

// ListPreviewIDsInWorkspace returns the uncleaned preview ids of a workspace.
func ListPreviewIDsInWorkspace(ctx context.Context, pool *pgxpool.Pool, projectID uuid.UUID, workspace string) ([]uuid.UUID, error) {
	rows, err := pool.Query(ctx, `SELECT id FROM previews
		WHERE project_id = $1 AND workspace = $2 AND cleaned_at IS NULL`, projectID, workspace)
	if err != nil {
		return nil, fmt.Errorf("query preview ids: %w", err)
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
