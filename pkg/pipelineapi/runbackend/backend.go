// Package runbackend defines the lifecycle controller for pipeline runs.
// K8sBackend is the sole implementation; it owns DB insert, K8s resource
// creation, token minting, and FGA tuple management.
package runbackend

import (
	"context"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipeline/config"
)

// Backend is the lifecycle controller for pipeline runs. K8sBackend is
// the sole implementation. The interface is kept so HTTP handlers remain
// testable with stub implementations.
type Backend interface {
	TriggerRun(ctx context.Context, req TriggerRequest) (TriggerResponse, error)
	CancelRun(ctx context.Context, req CancelRequest) error
}

// TriggerRequest carries the inputs a handler has gathered for a trigger
// call. The handler is responsible for auth, project membership, pipeline
// lookup, and YAML parsing; the backend owns everything from DB insert
// onward.
type TriggerRequest struct {
	ProjectID    uuid.UUID        // zero for system triggers
	UserID       uuid.UUID        // zero for system triggers
	PipelineName string           // matches pipelines.name and Pipeline CRD name
	PipelineID   uuid.UUID        // pipelines.id from GetPipelineByName
	PipelineYAML []byte           // raw YAML, needed by K8sBackend for CRD apply
	Parsed       *config.Pipeline // parsed once, reused for capability derivation
}

// TriggerResponse is what the backend returns to the handler.
type TriggerResponse struct {
	RunID     uuid.UUID
	Namespace string // K8s namespace the PipelineRun was created in
}

// CancelRequest carries the inputs to cancel a run. The backend is
// responsible for the terminal-phase guard, CRD deletion (K8s mode),
// DB phase update, and token revocation.
type CancelRequest struct {
	ProjectID uuid.UUID
	RunID     uuid.UUID
}
