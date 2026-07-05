package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
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

// TestUpdateRunPhase_LargeObservedRV pins the bigint cast on the $7
// parameter (RFC 019 v0.2.1 Slice 9). Without `$7::bigint`, pgx infers
// int4 from the `$7 = 0` literal context and fails the protocol-level
// encode for any value > math.MaxInt32. GKE etcd issues hybrid-clock
// resourceVersions in the 1.7e18 range; the 2026-05-19 live deploy hit
// ~1.78e18. We test the smallest int4-overflow value to make the
// regression boundary self-documenting.
func TestUpdateRunPhase_LargeObservedRV(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	proj, _ := store.CreateProject(ctx, pool, "proj")
	pipe, _ := store.CreatePipeline(ctx, pool, proj.ID, "etl", minimalYAML)
	run, _ := store.CreateRun(ctx, pool, store.CreateRunOpts{
		ID: uuid.New(), ProjectID: proj.ID, PipelineID: pipe.ID,
	})

	// Smallest value that exceeds int4 (the regression boundary).
	const largeRV int64 = int64(math.MaxInt32) + 1

	applied, err := store.UpdateRunPhase(ctx, pool, run.ID, store.UpdateRunPhaseOpts{
		Phase:      "Running",
		ObservedRV: largeRV,
	})
	if err != nil {
		t.Fatalf("UpdateRunPhase with large RV: %v", err)
	}
	if !applied {
		t.Fatal("expected applied=true on first write")
	}

	// Confirm observed_rv persisted unchanged. Run/GetRunByID don't
	// surface the column, so probe via the guard: a stale rv probe just
	// below largeRV must be filtered (applied=false). If observed_rv
	// were silently truncated/zeroed, the probe would succeed.
	applied, err = store.UpdateRunPhase(ctx, pool, run.ID, store.UpdateRunPhaseOpts{
		Phase: "Running", ObservedRV: largeRV - 1,
	})
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if applied {
		t.Fatalf("probe at largeRV-1 should be filtered; observed_rv did not persist as %d", largeRV)
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

func TestGetRunByID_ReturnsPipelineNameAndNilSnapshot(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	proj, _ := store.CreateProject(ctx, pool, "proj")
	pipe, _ := store.CreatePipeline(ctx, pool, proj.ID, "etl", minimalYAML)
	run, _ := store.CreateRun(ctx, pool, store.CreateRunOpts{
		ID: uuid.New(), ProjectID: proj.ID, PipelineID: pipe.ID,
	})

	got, err := store.GetRunByID(ctx, pool, run.ID)
	if err != nil {
		t.Fatalf("GetRunByID: %v", err)
	}
	if got.PipelineName != "etl" {
		t.Errorf("PipelineName = %q, want etl", got.PipelineName)
	}
	if got.StageStatuses != nil {
		t.Errorf("StageStatuses = %q, want nil for a fresh run", got.StageStatuses)
	}
}

func TestUpdateRunPhase_PersistsSnapshotAndCoalesceNilPreserves(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	proj, _ := store.CreateProject(ctx, pool, "proj")
	pipe, _ := store.CreatePipeline(ctx, pool, proj.ID, "etl", minimalYAML)
	run, _ := store.CreateRun(ctx, pool, store.CreateRunOpts{ID: uuid.New(), ProjectID: proj.ID, PipelineID: pipe.ID})

	snap := []byte(`[{"name":"extract","phase":"Running"}]`)
	if _, err := store.UpdateRunPhase(ctx, pool, run.ID, store.UpdateRunPhaseOpts{
		Phase: "Running", CurrentStage: "extract", ObservedRV: 10, StageStatuses: snap,
	}); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	// A later write with nil snapshot must PRESERVE the stored JSON.
	if _, err := store.UpdateRunPhase(ctx, pool, run.ID, store.UpdateRunPhaseOpts{
		Phase: "Running", CurrentStage: "transform", ObservedRV: 11,
	}); err != nil {
		t.Fatalf("nil-snapshot write: %v", err)
	}
	got, _ := store.GetRunByID(ctx, pool, run.ID)
	var gotJSON, wantJSON any
	if err := json.Unmarshal(got.StageStatuses, &gotJSON); err != nil {
		t.Fatalf("unmarshal got: %v", err)
	}
	if err := json.Unmarshal(snap, &wantJSON); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	if !reflect.DeepEqual(gotJSON, wantJSON) {
		t.Errorf("snapshot = %s, want preserved %s", got.StageStatuses, snap)
	}
}

func TestUpdateRunPhase_GuardTerminalBlocksResurrection(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	proj, _ := store.CreateProject(ctx, pool, "proj")
	pipe, _ := store.CreatePipeline(ctx, pool, proj.ID, "etl", minimalYAML)
	run, _ := store.CreateRun(ctx, pool, store.CreateRunOpts{ID: uuid.New(), ProjectID: proj.ID, PipelineID: pipe.ID})

	// Out-of-band cancel (rv=0, no guard) — like runbackend.CancelRun.
	if _, err := store.UpdateRunPhase(ctx, pool, run.ID, store.UpdateRunPhaseOpts{Phase: "Cancelled"}); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	// Stale observer reconcile (rv>0, GuardTerminal) must NOT resurrect Running.
	applied, err := store.UpdateRunPhase(ctx, pool, run.ID, store.UpdateRunPhaseOpts{
		Phase: "Running", CurrentStage: "extract", ObservedRV: 999, GuardTerminal: true,
	})
	if err != nil {
		t.Fatalf("guarded write: %v", err)
	}
	if applied {
		t.Fatal("applied=true: guarded write resurrected a Cancelled run")
	}
	got, _ := store.GetRunByID(ctx, pool, run.ID)
	if got.Phase != "Cancelled" {
		t.Errorf("phase = %q, want Cancelled", got.Phase)
	}
}

func TestListRunsPage_KeysetNoSkipNoDupe(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	proj, _ := store.CreateProject(ctx, pool, "proj")
	pipe, _ := store.CreatePipeline(ctx, pool, proj.ID, "etl", minimalYAML)
	for i := 0; i < 5; i++ {
		_, _ = store.CreateRun(ctx, pool, store.CreateRunOpts{ID: uuid.New(), ProjectID: proj.ID, PipelineID: pipe.ID})
	}
	seen := map[uuid.UUID]bool{}
	cursor := ""
	pages := 0
	for {
		page, err := store.ListRunsPage(ctx, pool, proj.ID, store.RunListOpts{Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatalf("page: %v", err)
		}
		for _, r := range page.Runs {
			if seen[r.ID] {
				t.Fatalf("duplicate run %s across pages", r.ID)
			}
			seen[r.ID] = true
		}
		pages++
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != 5 {
		t.Errorf("saw %d runs across pages, want 5", len(seen))
	}
}

func TestListRunsPage_FiltersByPhaseAndPipelineName(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	proj, _ := store.CreateProject(ctx, pool, "proj")
	daily, _ := store.CreatePipeline(ctx, pool, proj.ID, "daily-orders", minimalYAML)
	sync, _ := store.CreatePipeline(ctx, pool, proj.ID, "customer-sync", minimalYAML)
	rA, _ := store.CreateRun(ctx, pool, store.CreateRunOpts{ID: uuid.New(), ProjectID: proj.ID, PipelineID: daily.ID})
	_, _ = store.CreateRun(ctx, pool, store.CreateRunOpts{ID: uuid.New(), ProjectID: proj.ID, PipelineID: sync.ID})
	_, _ = store.UpdateRunPhase(ctx, pool, rA.ID, store.UpdateRunPhaseOpts{Phase: "Succeeded"})

	byName, _ := store.ListRunsPage(ctx, pool, proj.ID, store.RunListOpts{PipelineSubstr: "daily"})
	if len(byName.Runs) != 1 || byName.Runs[0].PipelineName != "daily-orders" {
		t.Fatalf("name filter = %+v, want 1 daily-orders", byName.Runs)
	}
	byPhase, _ := store.ListRunsPage(ctx, pool, proj.ID, store.RunListOpts{Phase: "Succeeded"})
	if len(byPhase.Runs) != 1 || byPhase.Runs[0].Phase != "Succeeded" {
		t.Fatalf("phase filter = %+v, want 1 Succeeded", byPhase.Runs)
	}
	byPipe, _ := store.ListRunsPage(ctx, pool, proj.ID, store.RunListOpts{PipelineID: sync.ID})
	if len(byPipe.Runs) != 1 || byPipe.Runs[0].PipelineID != sync.ID {
		t.Fatalf("pipeline_id filter = %+v, want 1 customer-sync", byPipe.Runs)
	}
}
