package http

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/apps"
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

// appsProjectLookup adapts ProjectReader to apps.ProjectLookup (RFC 028):
// the app author routes need only the lakekeeper project id for their FGA
// check object. errStoreNotFound is translated to apps.ErrNotFound so the
// handler's errors.Is check maps it to 404; an empty LakekeeperProjectID
// (project not yet provisioned) is passed through as ("", nil), which the
// handler soft-degrades to 503 exactly as mustHaveRelation does.
type appsProjectLookup struct {
	projects ProjectReader
}

// NewAppsProjectLookup exposes that adapter to the binary, which needs the
// same seam to construct the concrete apps.IdentityManager (RFC 028 P4): the
// identity manager resolves the lakekeeper project id itself, from the
// Datuplet project UUID its callers pass. Wiring it through the same adapter
// the routes use guarantees main.go and Handler() agree on what a project id
// means.
func NewAppsProjectLookup(projects ProjectReader) apps.ProjectLookup {
	return appsProjectLookup{projects: projects}
}

func (a appsProjectLookup) LakekeeperProjectID(ctx context.Context, projectID uuid.UUID) (string, error) {
	proj, err := a.projects.GetByID(ctx, projectID)
	if errors.Is(err, errStoreNotFound) {
		return "", apps.ErrNotFound
	}
	if err != nil {
		return "", err
	}
	return proj.LakekeeperProjectID, nil
}

// RunReader serves read-only run endpoints. Trigger/Cancel go through
// runbackend.Backend (already wired).
type RunReader interface {
	ListPage(ctx context.Context, projectID uuid.UUID, opts store.RunListOpts) (store.RunPage, error)
	GetByID(ctx context.Context, projectID, runID uuid.UUID) (store.RunView, error)
}
