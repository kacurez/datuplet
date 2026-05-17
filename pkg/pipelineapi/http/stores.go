package http

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// errStoreNotFound is the mode-agnostic "no such row" signal surfaced by
// the store interfaces below. Handlers translate it into 404. It lives
// here (not in store/) because it's the handler-layer abstraction —
// pgx impls re-wrap store.ErrProjectNotFound/ErrPipelineNotFound/
// ErrRunNotFound as this; local impls return it directly.
var errStoreNotFound = errors.New("not found")

// errPipelineInUse is the mode-agnostic "delete rejected because a run is
// active" signal. Handlers translate it into 409. Only the local
// DirPipelineStore with an ActiveRunCheck wired currently emits this;
// the pgx path CASCADEs deletes and never pushes back.
var errPipelineInUse = errors.New("pipeline in use")

// ProjectReader serves the project endpoints that exist under
// /api/v1/projects. Local mode returns a single hard-coded project;
// cluster mode delegates to the pgx store's project functions.
type ProjectReader interface {
	ListForUser(ctx context.Context, userID uuid.UUID) ([]ProjectView, error)
	GetByID(ctx context.Context, projectID uuid.UUID) (*ProjectView, error)
}

// ProjectView is the handler-layer DTO. Mirrors the subset of
// store.Project that the project endpoints actually serialize.
type ProjectView struct {
	ID                  uuid.UUID
	Name                string
	K8sNamespace        string // empty in local mode
	LakekeeperProjectID string // used as the FGA `project:<uuid>` Check object
}

// PipelineStore serves the pipeline endpoints. Local mode is backed by
// localstore.DirPipelineStore (reads/writes YAML files). Cluster mode
// wraps the pgx store.Pipeline* functions.
//
// Put semantics: upsert. GetByName + errStoreNotFound signals "insert";
// any other error bubbles. Delete returns errStoreNotFound when the
// pipeline doesn't exist.
type PipelineStore interface {
	List(ctx context.Context, projectID uuid.UUID) ([]PipelineRef, error)
	GetByName(ctx context.Context, projectID uuid.UUID, name string) (*PipelineDetail, error)
	Put(ctx context.Context, projectID uuid.UUID, name string, yaml []byte) error
	Delete(ctx context.Context, projectID uuid.UUID, name string) error
}

// PipelineRef is the summary row returned by List.
type PipelineRef struct {
	Name      string
	CreatedAt time.Time // empty in local mode (filesystem stat not surfaced in list)
	UpdatedAt time.Time
}

// PipelineDetail is the full row returned by GetByName.
type PipelineDetail struct {
	ID        uuid.UUID // zero in local mode
	ProjectID uuid.UUID // zero in local mode
	Name      string
	YAML      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// RunReader serves read-only run endpoints. Trigger/Cancel go through
// runbackend.Backend (already wired).
type RunReader interface {
	ListForProject(ctx context.Context, projectID uuid.UUID, limit int) ([]store.RunView, error)
	GetByID(ctx context.Context, projectID, runID uuid.UUID) (store.RunView, error)
}
