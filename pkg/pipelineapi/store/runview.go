package store

import (
	"time"

	"github.com/google/uuid"
)

// RunView is the read-model returned by RunStore.Get/.List and handed to
// HTTP handlers. Fields that exist in K8s mode but not local mode use
// pointer types so the local backend can set them to nil.
//
// PipelineName (vs pgx store's PipelineID) carries through because local
// mode has no pipelines table — pipelines are filesystem entries keyed by
// name. The pgx converter populates it from the joined pipelines row when
// callers have it available; otherwise left empty.
type RunView struct {
	ID           uuid.UUID
	PipelineID   uuid.UUID // zero in local mode
	PipelineName string    // zero if not joined
	Phase        string
	CurrentStage string
	Message      string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	StartedAt    *time.Time
	CompletedAt  *time.Time
	ProjectID    *uuid.UUID // nil in local mode
	TriggeredBy  *uuid.UUID // nil if system-triggered or local mode
	ObservedRV   int64      // 0 in local mode
}

// ToRunView converts a pgx-loaded Run into the shared DTO. A nil-uuid
// TriggeredBy is remapped to a nil pointer so JSON serialization omits it.
func ToRunView(r *Run) RunView {
	v := RunView{
		ID: r.ID, PipelineID: r.PipelineID, Phase: r.Phase,
		CurrentStage: r.CurrentStage, Message: r.Message,
		CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
		StartedAt: r.StartedAt, CompletedAt: r.CompletedAt,
	}
	if r.ProjectID != uuid.Nil {
		pid := r.ProjectID
		v.ProjectID = &pid
	}
	if r.TriggeredBy != uuid.Nil {
		tb := r.TriggeredBy
		v.TriggeredBy = &tb
	}
	return v
}
