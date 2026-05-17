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

// Project is the in-memory view of a projects row.
//
// LakekeeperProjectID is populated after the post-INSERT round-trip to
// lakekeeper's POST /management/v1/project. Empty string
// is the "not yet provisioned" sentinel — the create-project flow writes
// it immediately on success; the ensure-project-authz reconciler back-fills
// rows that lost their companion lakekeeper Project (e.g. crashed mid-create).
type Project struct {
	ID                  uuid.UUID
	Name                string
	K8sNamespace        string
	LakekeeperProjectID string
	CreatedAt           time.Time
}

// ErrProjectAlreadyExists is returned by CreateProject on a unique-constraint
// violation on name.
var ErrProjectAlreadyExists = errors.New("project already exists")

// ErrProjectNotFound is returned when no project matches the lookup.
var ErrProjectNotFound = errors.New("project not found")

// CreateProject inserts a new project. We compute the k8s_namespace in Go so
// the CHECK constraint (k8s_namespace = 'datuplet-' || id::text) passes.
//
// lakekeeper_project_id is left empty here — the caller (admin
// create-project) fills it in via SetLakekeeperProjectID after the POST
// /management/v1/project round-trip succeeds. Splitting the two writes
// keeps the rollback story simple: if the lakekeeper call fails, just
// DELETE the row.
func CreateProject(ctx context.Context, pool *pgxpool.Pool, name string) (*Project, error) {
	id := uuid.New()
	ns := "datuplet-" + id.String()
	p := &Project{ID: id, Name: name, K8sNamespace: ns}
	err := pool.QueryRow(ctx,
		`INSERT INTO projects (id, name, k8s_namespace) VALUES ($1, $2, $3) RETURNING created_at`,
		id, name, ns,
	).Scan(&p.CreatedAt)
	if err != nil {
		if strings.Contains(err.Error(), "projects_name_key") {
			return nil, ErrProjectAlreadyExists
		}
		return nil, fmt.Errorf("insert project: %w", err)
	}
	return p, nil
}

// SetLakekeeperProjectID UPDATEs lakekeeper_project_id for an existing row.
// Idempotent — re-setting to the same value is a no-op at the SQL level.
// Returns ErrProjectNotFound if the row doesn't exist.
func SetLakekeeperProjectID(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID, lakekeeperProjectID string) error {
	tag, err := pool.Exec(ctx,
		`UPDATE projects SET lakekeeper_project_id = $1 WHERE id = $2`,
		lakekeeperProjectID, id,
	)
	if err != nil {
		return fmt.Errorf("update lakekeeper_project_id: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrProjectNotFound
	}
	return nil
}

// DeleteProject removes the row by ID. ON DELETE CASCADE on the
// project_memberships table takes care of cleanup. Returns ErrProjectNotFound
// when no
// row matched. Idempotent at the caller's discretion: a second delete of
// the same id returns ErrProjectNotFound.
func DeleteProject(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) error {
	tag, err := pool.Exec(ctx, `DELETE FROM projects WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete project: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrProjectNotFound
	}
	return nil
}

// GetProjectByID returns the project or ErrProjectNotFound.
func GetProjectByID(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) (*Project, error) {
	p := &Project{}
	err := pool.QueryRow(ctx,
		`SELECT id, name, k8s_namespace, lakekeeper_project_id, created_at FROM projects WHERE id = $1`,
		id,
	).Scan(&p.ID, &p.Name, &p.K8sNamespace, &p.LakekeeperProjectID, &p.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrProjectNotFound
		}
		return nil, fmt.Errorf("select project: %w", err)
	}
	return p, nil
}

// GetProjectByName looks up a project by its display name.
// Used by admin delete-project and ensure-project-authz to resolve a
// human-typed name to a row.
func GetProjectByName(ctx context.Context, pool *pgxpool.Pool, name string) (*Project, error) {
	p := &Project{}
	err := pool.QueryRow(ctx,
		`SELECT id, name, k8s_namespace, lakekeeper_project_id, created_at FROM projects WHERE name = $1`,
		name,
	).Scan(&p.ID, &p.Name, &p.K8sNamespace, &p.LakekeeperProjectID, &p.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrProjectNotFound
		}
		return nil, fmt.Errorf("select project by name: %w", err)
	}
	return p, nil
}

// ListProjectsByLakekeeperIDs returns every project whose
// lakekeeper_project_id is in the given set, ordered by name. Used by
// pgxProjectReader.ListForUser to reverse-resolve FGA ListObjects results
// back to Datuplet project rows.
// An empty ids slice returns nil, nil.
func ListProjectsByLakekeeperIDs(ctx context.Context, pool *pgxpool.Pool, ids []string) ([]*Project, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT id, name, k8s_namespace, lakekeeper_project_id, created_at
		  FROM projects
		 WHERE lakekeeper_project_id = ANY($1)
		 ORDER BY name
	`, ids)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var out []*Project
	for rows.Next() {
		p := &Project{}
		if err := rows.Scan(&p.ID, &p.Name, &p.K8sNamespace, &p.LakekeeperProjectID, &p.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ListAllProjects returns every projects row, ordered by name. Used by the
// ensure-project-authz reconciler to sweep across projects and back-fill
// lakekeeper Project + FGA tuples.
func ListAllProjects(ctx context.Context, pool *pgxpool.Pool) ([]*Project, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, name, k8s_namespace, lakekeeper_project_id, created_at
		  FROM projects
		 ORDER BY name
	`)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var out []*Project
	for rows.Next() {
		p := &Project{}
		if err := rows.Scan(&p.ID, &p.Name, &p.K8sNamespace, &p.LakekeeperProjectID, &p.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

