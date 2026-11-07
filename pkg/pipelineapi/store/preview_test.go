package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

func newPreview(projectID uuid.UUID, ws string) *store.Preview {
	return &store.Preview{
		ID: uuid.New(), ProjectID: projectID, Workspace: ws, Doc: []byte(`{"name":"x"}`),
		RowLimit: 10, TimeoutSeconds: 60, TTLSeconds: 60, K8sNamespace: "ns",
	}
}

func seedProject(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	proj, err := store.CreateProject(context.Background(), pool, "proj")
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	return proj.ID
}

func TestInsertPreview_RunningCapPerWorkspace(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	pid := seedProject(t, pool)
	caps := store.PreviewCaps{RunningPerWorkspace: 2, RunningPerProject: 8, MaxPerProject: 50}

	for i := 0; i < 2; i++ {
		if err := store.InsertPreview(ctx, pool, newPreview(pid, "a"), caps); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	err := store.InsertPreview(ctx, pool, newPreview(pid, "a"), caps)
	if !errors.Is(err, store.ErrPreviewCap) {
		t.Fatalf("want ErrPreviewCap, got %v", err)
	}
	var capErr *store.PreviewCapError
	if !errors.As(err, &capErr) || capErr.RunningInWorkspace != 2 {
		t.Fatalf("want RunningInWorkspace=2, got %+v", capErr)
	}
	if err := store.InsertPreview(ctx, pool, newPreview(pid, "b"), caps); err != nil {
		t.Fatalf("workspace b insert: %v", err)
	}
}

func TestInsertPreview_EvictsOldestCleaned(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	pid := seedProject(t, pool)
	caps := store.PreviewCaps{RunningPerWorkspace: 5, RunningPerProject: 5, MaxPerProject: 2}

	oldest, newer := newPreview(pid, "a"), newPreview(pid, "a")
	for _, p := range []*store.Preview{oldest, newer} {
		if err := store.InsertPreview(ctx, pool, p, caps); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if _, err := store.UpdatePreviewPhase(ctx, pool, p.ID, store.UpdatePreviewPhaseOpts{Phase: "Succeeded"}); err != nil {
			t.Fatalf("terminal: %v", err)
		}
		if err := store.MarkPreviewCleaned(ctx, pool, p.ID, time.Now(), false); err != nil {
			t.Fatalf("clean: %v", err)
		}
		if err := store.MarkPreviewTupleDeleted(ctx, pool, p.ID, time.Now()); err != nil {
			t.Fatalf("tuple deleted: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := store.InsertPreview(ctx, pool, newPreview(pid, "a"), caps); err != nil {
		t.Fatalf("insert over cap: %v", err)
	}
	if _, err := store.GetPreviewByID(ctx, pool, oldest.ID); !errors.Is(err, store.ErrPreviewNotFound) {
		t.Errorf("oldest cleaned row should be evicted, got %v", err)
	}
	if _, err := store.GetPreviewByID(ctx, pool, newer.ID); err != nil {
		t.Errorf("newer cleaned row should survive: %v", err)
	}
}

func TestInsertPreview_CleanedPendingNotRunning(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	pid := seedProject(t, pool)
	caps := store.PreviewCaps{RunningPerWorkspace: 2, RunningPerProject: 2, MaxPerProject: 50}

	for i := 0; i < 2; i++ {
		p := newPreview(pid, "a")
		if err := store.InsertPreview(ctx, pool, p, caps); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
		if err := store.MarkPreviewCleaned(ctx, pool, p.ID, time.Now(), false); err != nil {
			t.Fatalf("clean: %v", err)
		}
	}
	if err := store.InsertPreview(ctx, pool, newPreview(pid, "a"), caps); err != nil {
		t.Fatalf("cleaned Pending rows must not count as running: %v", err)
	}
}

func TestInsertPreview_KeepsCleanedRowWithLiveTuple(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	pid := seedProject(t, pool)
	caps := store.PreviewCaps{RunningPerWorkspace: 5, RunningPerProject: 5, MaxPerProject: 1}

	old := newPreview(pid, "a")
	if err := store.InsertPreview(ctx, pool, old, caps); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpdatePreviewPhase(ctx, pool, old.ID, store.UpdatePreviewPhaseOpts{Phase: "Succeeded"}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPreviewCleaned(ctx, pool, old.ID, time.Now(), false); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertPreview(ctx, pool, newPreview(pid, "a"), caps); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := store.GetPreviewByID(ctx, pool, old.ID); err != nil {
		t.Errorf("cleaned row whose tuple is not deleted must not be evicted: %v", err)
	}
}

func TestUpdatePreviewPhase_TerminalResetsExpiry(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	pid := seedProject(t, pool)
	p := newPreview(pid, "a")
	if err := store.InsertPreview(ctx, pool, p, store.PreviewCaps{}); err != nil {
		t.Fatal(err)
	}
	if want := p.CreatedAt.Add(120 * time.Second); p.ExpiresAt.Sub(want).Abs() > time.Second {
		t.Errorf("initial expires_at = %v, want ~%v", p.ExpiresAt, want)
	}

	done := time.Now().UTC().Truncate(time.Microsecond)
	applied, err := store.UpdatePreviewPhase(ctx, pool, p.ID, store.UpdatePreviewPhaseOpts{
		Phase: "Succeeded", CompletedAt: &done, ObservedRV: 10})
	if err != nil || !applied {
		t.Fatalf("update: applied=%v err=%v", applied, err)
	}
	got, _ := store.GetPreviewByID(ctx, pool, p.ID)
	if !got.ExpiresAt.Equal(done.Add(60 * time.Second)) {
		t.Errorf("expires_at = %v, want %v", got.ExpiresAt, done.Add(60*time.Second))
	}

	applied, err = store.UpdatePreviewPhase(ctx, pool, p.ID, store.UpdatePreviewPhaseOpts{
		Phase: "Running", ObservedRV: 5})
	if err != nil || applied {
		t.Errorf("stale rv: applied=%v err=%v, want false,nil", applied, err)
	}
	got, _ = store.GetPreviewByID(ctx, pool, p.ID)
	if got.Phase != "Succeeded" {
		t.Errorf("phase = %q after stale update", got.Phase)
	}
}

func TestListPreviews_Workspace(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	pid := seedProject(t, pool)
	for _, ws := range []string{"a", "a", "b"} {
		if err := store.InsertPreview(ctx, pool, newPreview(pid, ws), store.PreviewCaps{}); err != nil {
			t.Fatal(err)
		}
	}
	a, _, err := store.ListPreviews(ctx, pool, pid, "a", 10, "")
	if err != nil || len(a) != 2 {
		t.Fatalf("ws a: len=%d err=%v", len(a), err)
	}
	all, _, err := store.ListPreviews(ctx, pool, pid, "*", 10, "")
	if err != nil || len(all) != 3 {
		t.Fatalf("all: len=%d err=%v", len(all), err)
	}
	page, next, err := store.ListPreviews(ctx, pool, pid, "*", 2, "")
	if err != nil || len(page) != 2 || next == "" {
		t.Fatalf("page1: len=%d next=%q err=%v", len(page), next, err)
	}
	rest, next, err := store.ListPreviews(ctx, pool, pid, "*", 2, next)
	if err != nil || len(rest) != 1 || next != "" {
		t.Fatalf("page2: len=%d next=%q err=%v", len(rest), next, err)
	}
}

func TestDeferPreviewExpiry_SkipsCleanedRow(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	pid := seedProject(t, pool)
	live, cleaned := newPreview(pid, "a"), newPreview(pid, "b")
	for _, p := range []*store.Preview{live, cleaned} {
		if err := store.InsertPreview(ctx, pool, p, store.PreviewCaps{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.MarkPreviewCleaned(ctx, pool, cleaned.ID, time.Now().UTC(), false); err != nil {
		t.Fatal(err)
	}
	before, _ := store.GetPreviewByID(ctx, pool, cleaned.ID)

	until := time.Now().UTC().Add(5 * time.Minute).Truncate(time.Microsecond)
	for _, id := range []uuid.UUID{live.ID, cleaned.ID} {
		if err := store.DeferPreviewExpiry(ctx, pool, id, until); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := store.GetPreviewByID(ctx, pool, live.ID); !got.ExpiresAt.Equal(until) {
		t.Errorf("live expires_at = %v, want %v", got.ExpiresAt, until)
	}
	if got, _ := store.GetPreviewByID(ctx, pool, cleaned.ID); !got.ExpiresAt.Equal(before.ExpiresAt) {
		t.Errorf("cleaned expires_at moved: %v -> %v", before.ExpiresAt, got.ExpiresAt)
	}
}
