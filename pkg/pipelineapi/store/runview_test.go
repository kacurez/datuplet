package store_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

func TestToRunView_Pointers(t *testing.T) {
	started := time.Now()
	completed := started.Add(5 * time.Minute)
	projectID := uuid.New()
	triggeredBy := uuid.New()

	cases := []struct {
		name string
		run  *store.Run
		// check asserts the pointer fields of the view.
		check func(t *testing.T, r *store.Run, v store.RunView)
	}{
		{
			"populated_from_non_zero_values",
			&store.Run{
				ID:           uuid.New(),
				ProjectID:    projectID,
				PipelineID:   uuid.New(),
				Phase:        "Succeeded",
				CurrentStage: "extract",
				Message:      "ok",
				StartedAt:    &started,
				CompletedAt:  &completed,
				TriggeredBy:  triggeredBy,
				CreatedAt:    started,
				UpdatedAt:    completed,
			},
			func(t *testing.T, r *store.Run, v store.RunView) {
				if v.ID != r.ID {
					t.Errorf("ID mismatch: %v vs %v", v.ID, r.ID)
				}
				if v.ProjectID == nil || *v.ProjectID != projectID {
					t.Errorf("ProjectID = %v, want &%v", v.ProjectID, projectID)
				}
				if v.TriggeredBy == nil || *v.TriggeredBy != triggeredBy {
					t.Errorf("TriggeredBy = %v, want &%v", v.TriggeredBy, triggeredBy)
				}
				if v.StartedAt == nil || !v.StartedAt.Equal(started) {
					t.Errorf("StartedAt = %v, want &%v", v.StartedAt, started)
				}
			},
		},
		{
			"uuid_nil_becomes_nil_pointer",
			&store.Run{ID: uuid.New(), Phase: "Pending", TriggeredBy: uuid.Nil},
			func(t *testing.T, _ *store.Run, v store.RunView) {
				if v.TriggeredBy != nil {
					t.Errorf("TriggeredBy = %v, want nil for uuid.Nil input", v.TriggeredBy)
				}
				if v.ProjectID != nil {
					t.Errorf("ProjectID = %v, want nil for uuid.Nil input", v.ProjectID)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.check(t, tc.run, store.ToRunView(tc.run))
		})
	}
}
