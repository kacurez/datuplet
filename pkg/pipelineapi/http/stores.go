package http

import (
	"context"
	"encoding/json"
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

// PipelineStore serves the pipeline endpoints. Cluster mode wraps the pgx
// store.Pipeline* functions; the doc is stored canonically as JSON
// (RFC 027 §5.1) — YAML request bodies are canonicalized to JSON by the
// handler before Put is called.
//
// Put semantics: upsert. Get + errStoreNotFound signals "insert"; any
// other error bubbles. Delete returns errStoreNotFound when the pipeline
// doesn't exist.
type PipelineStore interface {
	List(ctx context.Context, projectID uuid.UUID) ([]PipelineRef, error)
	Get(ctx context.Context, projectID uuid.UUID, name string) (*PipelineDetail, error)
	GetDocByID(ctx context.Context, id string) ([]byte, error)
	Put(ctx context.Context, projectID uuid.UUID, name string, doc []byte, description string) error
	Delete(ctx context.Context, projectID uuid.UUID, name string) error
}

// PipelineRef is the summary row returned by List. Description is carried
// on the struct (RFC 027 §5.1) and serialized in the List response JSON (S6).
type PipelineRef struct {
	Name        string
	Description string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// PipelineDetail is the full row returned by Get. ID/CreatedAt/UpdatedAt
// are pre-formatted strings so the handler layer can serialize them
// directly without reaching back into store types.
type PipelineDetail struct {
	ID        string
	Name      string
	Doc       json.RawMessage
	CreatedAt string
	UpdatedAt string
}

// RunReader serves read-only run endpoints. Trigger/Cancel go through
// runbackend.Backend (already wired).
type RunReader interface {
	ListPage(ctx context.Context, projectID uuid.UUID, opts store.RunListOpts) (store.RunPage, error)
	GetByID(ctx context.Context, projectID, runID uuid.UUID) (store.RunView, error)
}
