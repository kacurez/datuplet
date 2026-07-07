package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pipeline is the in-memory view of a pipelines row.
type Pipeline struct {
	ID        uuid.UUID
	ProjectID uuid.UUID
	Name      string
	YAML      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ErrPipelineAlreadyExists is returned by CreatePipeline on unique-constraint
// violation on (project_id, name).
var ErrPipelineAlreadyExists = errors.New("pipeline already exists")

// ErrPipelineNotFound is returned when no pipeline matches the lookup.
var ErrPipelineNotFound = errors.New("pipeline not found")

// CreatePipeline inserts a new pipeline. The yaml string must already have
// been validated by the caller (pipeline-api handlers run it through
// pkg/pipeline/config.Parse before calling this).
func CreatePipeline(ctx context.Context, pool *pgxpool.Pool, projectID uuid.UUID, name, yaml string) (*Pipeline, error) {
	p := &Pipeline{ProjectID: projectID, Name: name, YAML: yaml}
	err := pool.QueryRow(ctx,
		`INSERT INTO pipelines(project_id, name, yaml) VALUES ($1, $2, $3)
		 RETURNING id, created_at, updated_at`,
		projectID, name, yaml,
	).Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		if strings.Contains(err.Error(), "pipelines_project_id_name_key") {
			return nil, ErrPipelineAlreadyExists
		}
		return nil, fmt.Errorf("insert pipeline: %w", err)
	}
	return p, nil
}

// GetPipelineByName returns the pipeline with the given name in the project,
// or ErrPipelineNotFound.
func GetPipelineByName(ctx context.Context, pool *pgxpool.Pool, projectID uuid.UUID, name string) (*Pipeline, error) {
	p := &Pipeline{}
	err := pool.QueryRow(ctx,
		`SELECT id, project_id, name, yaml, created_at, updated_at
		   FROM pipelines WHERE project_id = $1 AND name = $2`,
		projectID, name,
	).Scan(&p.ID, &p.ProjectID, &p.Name, &p.YAML, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrPipelineNotFound
		}
		return nil, fmt.Errorf("select pipeline: %w", err)
	}
	return p, nil
}

// GetPipelineNameByID returns a pipeline's name by id, or ErrPipelineNotFound.
// Used by cancel-path code that already has the pipeline_id from runs.pipeline_id
// and doesn't need the full row.
func GetPipelineNameByID(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) (string, error) {
	var name string
	err := pool.QueryRow(ctx, `SELECT name FROM pipelines WHERE id = $1`, id).Scan(&name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrPipelineNotFound
		}
		return "", fmt.Errorf("select pipeline name: %w", err)
	}
	return name, nil
}

// GetPipelineYAMLByID returns the stored YAML for a pipeline, or ErrPipelineNotFound.
func GetPipelineYAMLByID(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) (string, error) {
	var yaml string
	err := pool.QueryRow(ctx, `SELECT yaml FROM pipelines WHERE id = $1`, id).Scan(&yaml)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrPipelineNotFound
	}
	if err != nil {
		return "", fmt.Errorf("select pipeline yaml: %w", err)
	}
	return yaml, nil
}

// ListPipelines returns every pipeline in the project sorted by name.
func ListPipelines(ctx context.Context, pool *pgxpool.Pool, projectID uuid.UUID) ([]*Pipeline, error) {
	rows, err := pool.Query(ctx,
		`SELECT id, project_id, name, yaml, created_at, updated_at
		   FROM pipelines WHERE project_id = $1 ORDER BY name`, projectID)
	if err != nil {
		return nil, fmt.Errorf("query pipelines: %w", err)
	}
	defer rows.Close()
	var out []*Pipeline
	for rows.Next() {
		p := &Pipeline{}
		if err := rows.Scan(&p.ID, &p.ProjectID, &p.Name, &p.YAML, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// UpdatePipelineYAML replaces the YAML of an existing pipeline.
func UpdatePipelineYAML(ctx context.Context, pool *pgxpool.Pool, projectID uuid.UUID, name, yaml string) error {
	tag, err := pool.Exec(ctx,
		`UPDATE pipelines SET yaml = $3, updated_at = now()
		  WHERE project_id = $1 AND name = $2`, projectID, name, yaml)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrPipelineNotFound
	}
	return nil
}

// DeletePipeline removes a pipeline row. The CASCADE on runs.pipeline_id
// takes care of in-flight run rows; the corresponding Pipeline CRD in
// K8s is NOT deleted by this call (operators can kubectl delete manually
// once Plan C3 makes this a routine operation).
func DeletePipeline(ctx context.Context, pool *pgxpool.Pool, projectID uuid.UUID, name string) error {
	tag, err := pool.Exec(ctx,
		`DELETE FROM pipelines WHERE project_id = $1 AND name = $2`, projectID, name)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrPipelineNotFound
	}
	return nil
}
