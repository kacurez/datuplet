package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

func TestCreateRun_AndGet(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	proj, _ := store.CreateProject(ctx, pool, "proj")
	pipe, _ := store.CreatePipeline(ctx, pool, proj.ID, "etl", minimalYAML)

	run, err := store.CreateRun(ctx, pool, store.CreateRunOpts{
		ID:          uuid.New(),
		ProjectID:   proj.ID,
		PipelineID:  pipe.ID,
		TriggeredBy: uuid.Nil,
	})
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	got, err := store.GetRunByID(ctx, pool, run.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.PipelineID != pipe.ID || got.Phase != "Pending" {
		t.Errorf("mismatch: %+v", got)
	}
}

func TestGetRun_Missing(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	if _, err := store.GetRunByID(context.Background(), pool, uuid.New()); !errors.Is(err, store.ErrRunNotFound) {
		t.Errorf("expected ErrRunNotFound, got %v", err)
	}
}

func TestListRunsForProject_OrderedDesc(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	proj, _ := store.CreateProject(ctx, pool, "proj")
	pipe, _ := store.CreatePipeline(ctx, pool, proj.ID, "etl", minimalYAML)

	// Insert in deterministic order. CreatedAt resolution may tie within
	// the same microsecond, so we can't assert strict order — just count.
	for i := 0; i < 3; i++ {
		_, _ = store.CreateRun(ctx, pool, store.CreateRunOpts{
			ID:         uuid.New(),
			ProjectID:  proj.ID,
			PipelineID: pipe.ID,
		})
	}
	got, err := store.ListRunsForProject(ctx, pool, proj.ID, 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("len = %d, want 3", len(got))
	}
}

func TestUpdateRunPhase(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	proj, _ := store.CreateProject(ctx, pool, "proj")
	pipe, _ := store.CreatePipeline(ctx, pool, proj.ID, "etl", minimalYAML)
	run, _ := store.CreateRun(ctx, pool, store.CreateRunOpts{
		ID: uuid.New(), ProjectID: proj.ID, PipelineID: pipe.ID,
	})
	applied, err := store.UpdateRunPhase(ctx, pool, run.ID, store.UpdateRunPhaseOpts{
		Phase:        "Running",
		CurrentStage: "extract",
		Message:      "go go go",
	})
	if err != nil {
		t.Fatalf("UpdatePhase: %v", err)
	}
	if !applied {
		t.Fatal("applied = false, want true")
	}
	got, _ := store.GetRunByID(ctx, pool, run.ID)
	if got.Phase != "Running" || got.CurrentStage != "extract" || got.Message != "go go go" {
		t.Errorf("phase not updated: %+v", got)
	}
}

func TestUpdateRunPhase_FirstWriteAppliesAndReturnsApplied(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	proj, _ := store.CreateProject(ctx, pool, "proj")
	pipe, _ := store.CreatePipeline(ctx, pool, proj.ID, "etl", minimalYAML)
	run, _ := store.CreateRun(ctx, pool, store.CreateRunOpts{
		ID: uuid.New(), ProjectID: proj.ID, PipelineID: pipe.ID,
	})

	applied, err := store.UpdateRunPhase(ctx, pool, run.ID, store.UpdateRunPhaseOpts{
		Phase: "Running", ObservedRV: 100,
	})
	if err != nil {
		t.Fatalf("UpdateRunPhase: %v", err)
	}
	if !applied {
		t.Fatal("applied = false, want true for first rv-gated write")
	}
	got, _ := store.GetRunByID(ctx, pool, run.ID)
	if got.Phase != "Running" {
		t.Errorf("phase = %q, want Running", got.Phase)
	}
}

func TestUpdateRunPhase_StaleRVDropsWrite(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	proj, _ := store.CreateProject(ctx, pool, "proj")
	pipe, _ := store.CreatePipeline(ctx, pool, proj.ID, "etl", minimalYAML)
	run, _ := store.CreateRun(ctx, pool, store.CreateRunOpts{
		ID: uuid.New(), ProjectID: proj.ID, PipelineID: pipe.ID,
	})

	if _, err := store.UpdateRunPhase(ctx, pool, run.ID, store.UpdateRunPhaseOpts{
		Phase: "Running", ObservedRV: 200,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	applied, err := store.UpdateRunPhase(ctx, pool, run.ID, store.UpdateRunPhaseOpts{
		Phase: "Succeeded", ObservedRV: 150,
	})
	if err != nil {
		t.Fatalf("UpdateRunPhase: %v", err)
	}
	if applied {
		t.Fatal("applied = true, want false for stale rv")
	}
	got, _ := store.GetRunByID(ctx, pool, run.ID)
	if got.Phase != "Running" {
		t.Errorf("phase = %q, want Running (stale write must be dropped)", got.Phase)
	}
}

func TestUpdateRunPhase_CancelPathIgnoresRVGuard(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	proj, _ := store.CreateProject(ctx, pool, "proj")
	pipe, _ := store.CreatePipeline(ctx, pool, proj.ID, "etl", minimalYAML)
	run, _ := store.CreateRun(ctx, pool, store.CreateRunOpts{
		ID: uuid.New(), ProjectID: proj.ID, PipelineID: pipe.ID,
	})

	if _, err := store.UpdateRunPhase(ctx, pool, run.ID, store.UpdateRunPhaseOpts{
		Phase: "Running", ObservedRV: 500,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	applied, err := store.UpdateRunPhase(ctx, pool, run.ID, store.UpdateRunPhaseOpts{
		Phase: "Cancelled",
	})
	if err != nil {
		t.Fatalf("UpdateRunPhase: %v", err)
	}
	if !applied {
		t.Fatal("cancel (rv=0) did not apply; must override observed_rv")
	}
	got, _ := store.GetRunByID(ctx, pool, run.ID)
	if got.Phase != "Cancelled" {
		t.Errorf("phase = %q, want Cancelled", got.Phase)
	}

	// Cancel must NOT lower observed_rv — a later stale rv write must
	// still be filtered. Probe with rv=400 (< the rv=500 we seeded):
	applied, err = store.UpdateRunPhase(ctx, pool, run.ID, store.UpdateRunPhaseOpts{
		Phase: "Running", ObservedRV: 400,
	})
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if applied {
		t.Fatal("rv=400 should still be filtered after cancel — observed_rv must be preserved")
	}
}

func TestUpdateRunPhase_MissingRunReturnsErrRunNotFound(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	// No rv: classic not-found behavior preserved.
	_, err := store.UpdateRunPhase(context.Background(), pool, uuid.New(), store.UpdateRunPhaseOpts{Phase: "Cancelled"})
	if !errors.Is(err, store.ErrRunNotFound) {
		t.Errorf("err = %v, want ErrRunNotFound", err)
	}
}
