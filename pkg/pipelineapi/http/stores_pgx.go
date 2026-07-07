package http

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// --- Projects ---

// pgxProjectReader adapts the pgx store's project functions to the
// handler-layer ProjectReader interface. authzr is required for
// ListForUser — FGA-backed project enumeration replaces the legacy
// project_memberships JOIN.
type pgxProjectReader struct {
	pool   *pgxpool.Pool
	authzr authz.Authorizer
}

// NewPgxProjectReader is the cluster-mode adapter used by runServeCluster.
// authzr must be non-nil; ListForUser calls ListObjects to enumerate
// projects the user has any relation on before resolving them via Postgres.
func NewPgxProjectReader(pool *pgxpool.Pool, authzr authz.Authorizer) ProjectReader {
	return &pgxProjectReader{pool: pool, authzr: authzr}
}

// ListForUser returns every Datuplet project the user has at least one
// FGA relation on. It calls ListObjects(user, "datuplet_member",
// TypeProject) to get the set of lakekeeper Project UUIDs, then does a
// single reverse-lookup SELECT to turn those UUIDs into full project rows.
//
// "datuplet_member" is satisfied by viewer, editor, and project_admin via
// union — so any user with any access sees their project(s) here.
//
// Local mode: the localProjectReader always returns the single hard-coded
// project; it does NOT call this path.
func (r *pgxProjectReader) ListForUser(ctx context.Context, userID uuid.UUID) ([]ProjectView, error) {
	objects, err := r.authzr.ListObjects(ctx,
		authz.UserObject(userID.String()).String(),
		"datuplet_member",
		authz.TypeProject)
	if err != nil {
		return nil, fmt.Errorf("ListForUser: FGA ListObjects: %w", err)
	}
	if len(objects) == 0 {
		return nil, nil
	}

	// Extract lakekeeper UUIDs from the FGA object list.
	lkIDs := make([]string, 0, len(objects))
	for _, o := range objects {
		lkIDs = append(lkIDs, o.ID())
	}

	// Reverse-lookup: project rows whose lakekeeper_project_id is in the set.
	ps, err := store.ListProjectsByLakekeeperIDs(ctx, r.pool, lkIDs)
	if err != nil {
		return nil, fmt.Errorf("ListForUser: reverse-lookup: %w", err)
	}
	out := make([]ProjectView, 0, len(ps))
	for _, p := range ps {
		out = append(out, ProjectView{
			ID:                  p.ID,
			Name:                p.Name,
			K8sNamespace:        p.K8sNamespace,
			LakekeeperProjectID: p.LakekeeperProjectID,
		})
	}
	return out, nil
}

func (r *pgxProjectReader) GetByID(ctx context.Context, id uuid.UUID) (*ProjectView, error) {
	p, err := store.GetProjectByID(ctx, r.pool, id)
	if errors.Is(err, store.ErrProjectNotFound) {
		return nil, errStoreNotFound
	}
	if err != nil {
		return nil, err
	}
	return &ProjectView{
		ID:                  p.ID,
		Name:                p.Name,
		K8sNamespace:        p.K8sNamespace,
		LakekeeperProjectID: p.LakekeeperProjectID,
	}, nil
}

// --- Pipelines ---

// pgxPipelineStore adapts the pgx store's pipeline functions. Put is an
// upsert: try GetByName first, UPDATE on hit, INSERT on miss; on a
// concurrent-PUT conflict (losing the insert race), retry as UPDATE.
type pgxPipelineStore struct {
	pool *pgxpool.Pool
}

// NewPgxPipelineStore is the cluster-mode adapter used by runServeCluster.
func NewPgxPipelineStore(pool *pgxpool.Pool) PipelineStore {
	return &pgxPipelineStore{pool: pool}
}

func (s *pgxPipelineStore) List(ctx context.Context, projectID uuid.UUID) ([]PipelineRef, error) {
	ps, err := store.ListPipelines(ctx, s.pool, projectID)
	if err != nil {
		return nil, err
	}
	out := make([]PipelineRef, 0, len(ps))
	for _, p := range ps {
		out = append(out, PipelineRef{
			Name:      p.Name,
			CreatedAt: p.CreatedAt,
			UpdatedAt: p.UpdatedAt,
		})
	}
	return out, nil
}

func (s *pgxPipelineStore) GetByName(ctx context.Context, projectID uuid.UUID, name string) (*PipelineDetail, error) {
	p, err := store.GetPipelineByName(ctx, s.pool, projectID, name)
	if errors.Is(err, store.ErrPipelineNotFound) {
		return nil, errStoreNotFound
	}
	if err != nil {
		return nil, err
	}
	return &PipelineDetail{
		ID:        p.ID,
		ProjectID: p.ProjectID,
		Name:      p.Name,
		YAML:      p.YAML,
		CreatedAt: p.CreatedAt,
		UpdatedAt: p.UpdatedAt,
	}, nil
}

func (s *pgxPipelineStore) GetYAMLByID(ctx context.Context, pipelineID uuid.UUID) (string, error) {
	return store.GetPipelineYAMLByID(ctx, s.pool, pipelineID)
}

func (s *pgxPipelineStore) Put(ctx context.Context, projectID uuid.UUID, name string, yaml []byte) error {
	yamlStr := string(yaml)
	// Upsert: update if exists, insert otherwise. Concurrent PUTs can
	// race between the GetByName miss and the Create; on a
	// unique-conflict fall through to Update rather than surfacing 500.
	if _, err := store.GetPipelineByName(ctx, s.pool, projectID, name); err == nil {
		if err := store.UpdatePipelineYAML(ctx, s.pool, projectID, name, yamlStr); err != nil {
			return fmt.Errorf("update pipeline: %w", err)
		}
		return nil
	} else if !errors.Is(err, store.ErrPipelineNotFound) {
		return fmt.Errorf("lookup pipeline: %w", err)
	}
	if _, err := store.CreatePipeline(ctx, s.pool, projectID, name, yamlStr); err != nil {
		if errors.Is(err, store.ErrPipelineAlreadyExists) {
			// Lost the race against a concurrent PUT — retry as update.
			if err := store.UpdatePipelineYAML(ctx, s.pool, projectID, name, yamlStr); err != nil {
				return fmt.Errorf("update pipeline after conflict: %w", err)
			}
			return nil
		}
		return fmt.Errorf("create pipeline: %w", err)
	}
	return nil
}

func (s *pgxPipelineStore) Delete(ctx context.Context, projectID uuid.UUID, name string) error {
	if err := store.DeletePipeline(ctx, s.pool, projectID, name); err != nil {
		if errors.Is(err, store.ErrPipelineNotFound) {
			return errStoreNotFound
		}
		return err
	}
	return nil
}

// --- Runs ---

// pgxRunReader adapts the pgx store's run functions. Cross-project
// lookups are filtered here (matching the old handler's "cross-project
// ID guessing -> uniform 404" rule).
type pgxRunReader struct {
	pool *pgxpool.Pool
}

// NewPgxRunReader is the cluster-mode adapter used by runServeCluster.
func NewPgxRunReader(pool *pgxpool.Pool) RunReader {
	return &pgxRunReader{pool: pool}
}

func (r *pgxRunReader) ListPage(ctx context.Context, projectID uuid.UUID, opts store.RunListOpts) (store.RunPage, error) {
	return store.ListRunsPage(ctx, r.pool, projectID, opts)
}

func (r *pgxRunReader) GetByID(ctx context.Context, projectID, runID uuid.UUID) (store.RunView, error) {
	run, err := store.GetRunByID(ctx, r.pool, runID)
	if errors.Is(err, store.ErrRunNotFound) {
		return store.RunView{}, errStoreNotFound
	}
	if err != nil {
		return store.RunView{}, err
	}
	// Cross-project ID guessing -> uniform 404.
	if run.ProjectID != projectID {
		return store.RunView{}, errStoreNotFound
	}
	return store.ToRunView(run), nil
}

// pgxEmailLookup is the storage.EmailLookup adapter that resolves a
// snapshot's actor UUID to the user's CURRENT email via the users table.
// Used only for display in the UI snapshot history pane; the canonical
// actor identifier in iceberg metadata stays the UUID.
// Misses (deleted user, DB error) collapse to "" so the UI falls back
// to the truncated UUID.
type pgxEmailLookup struct {
	pool *pgxpool.Pool
}

func (e pgxEmailLookup) EmailByID(ctx context.Context, id uuid.UUID) string {
	if e.pool == nil {
		return ""
	}
	u, err := store.GetUserByID(ctx, e.pool, id)
	if err != nil || u == nil {
		return ""
	}
	return u.Email
}
