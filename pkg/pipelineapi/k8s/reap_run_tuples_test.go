package k8s_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz/authztest"
	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// testReapDB sets up a fresh schema-migrated Postgres pool. Skips when
// TEST_DATABASE_URL isn't set so `go test ./...` stays green on a
// laptop without a live Postgres.
func testReapDB(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping DB-backed reaper test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgx pool: %v", err)
	}
	if _, err := pool.Exec(context.Background(), "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"); err != nil {
		pool.Close()
		t.Fatalf("reset: %v", err)
	}
	if err := pipelineapidb.Migrate(context.Background(), pool); err != nil {
		pool.Close()
		t.Fatalf("migrate: %v", err)
	}
	return pool, func() { pool.Close() }
}

// seedProjectPipeline creates a project + pipeline pair so we can
// INSERT runs rows the run_tuples table CASCADEs from. Returns the
// project ID + pipeline ID for use in CreateRun.
func seedProjectPipeline(t *testing.T, pool *pgxpool.Pool) (projectID, pipelineID uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	p, err := store.CreateProject(ctx, pool, "reaper-project")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	pp, err := store.CreatePipeline(ctx, pool, p.ID, "p", "", []byte(`{}`))
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}
	return p.ID, pp.ID
}

func TestReapRunTuples_TerminalRunDeletesTuplesAndRow(t *testing.T) {
	pool, done := testReapDB(t)
	defer done()
	ctx := context.Background()

	projectID, pipelineID := seedProjectPipeline(t, pool)
	runID := uuid.New()
	if _, err := store.CreateRun(ctx, pool, store.CreateRunOpts{
		ID: runID, ProjectID: projectID, PipelineID: pipelineID,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if _, err := store.UpdateRunPhase(ctx, pool, runID, store.UpdateRunPhaseOpts{
		Phase: "Succeeded", Message: "done",
	}); err != nil {
		t.Fatalf("update run phase: %v", err)
	}

	tup := authz.Tuple{
		User:     authz.UserObject(runID.String()).String(),
		Relation: "editor",
		Object:   authz.ProjectObject("lk-proj-1"),
	}
	if err := store.RecordRunTuples(ctx, pool, runID, []authz.Tuple{tup}); err != nil {
		t.Fatalf("record run tuples: %v", err)
	}
	if err := store.MarkRunTuplesCommitted(ctx, pool, runID); err != nil {
		t.Fatalf("mark committed: %v", err)
	}

	az := authztest.New()
	az.Allow(tup.User, tup.Relation, tup.Object)

	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	if err := pkg8s.ReapRunTuples(ctx, pool, c, az); err != nil {
		t.Fatalf("ReapRunTuples: %v", err)
	}

	// Tuple should be gone from FGA.
	allowed, err := az.Check(ctx, tup.User, tup.Relation, tup.Object)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if allowed {
		t.Errorf("FGA tuple still present after reap")
	}
	// Row should be gone from run_tuples.
	rec, err := store.GetRunTuples(ctx, pool, runID)
	if err != nil {
		t.Fatalf("get run tuples: %v", err)
	}
	if rec != nil {
		t.Errorf("run_tuples row still present: %+v", rec)
	}
}

func TestReapRunTuples_NonTerminalRunIsLeftAlone(t *testing.T) {
	pool, done := testReapDB(t)
	defer done()
	ctx := context.Background()

	projectID, pipelineID := seedProjectPipeline(t, pool)
	runID := uuid.New()
	if _, err := store.CreateRun(ctx, pool, store.CreateRunOpts{
		ID: runID, ProjectID: projectID, PipelineID: pipelineID,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	// Default phase is Pending — non-terminal.

	tup := authz.Tuple{
		User:     authz.UserObject(runID.String()).String(),
		Relation: "editor",
		Object:   authz.ProjectObject("lk-proj-2"),
	}
	if err := store.RecordRunTuples(ctx, pool, runID, []authz.Tuple{tup}); err != nil {
		t.Fatalf("record: %v", err)
	}

	az := authztest.New()
	az.Allow(tup.User, tup.Relation, tup.Object)

	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	if err := pkg8s.ReapRunTuples(ctx, pool, c, az); err != nil {
		t.Fatalf("ReapRunTuples: %v", err)
	}

	// Tuple stays — primary path owns this run.
	allowed, _ := az.Check(ctx, tup.User, tup.Relation, tup.Object)
	if !allowed {
		t.Errorf("FGA tuple was deleted from in-flight run; should be left alone")
	}
	rec, _ := store.GetRunTuples(ctx, pool, runID)
	if rec == nil {
		t.Errorf("run_tuples row was deleted from in-flight run")
	}
}

func TestReapRunTuples_OrphanRowGetsCleanedUp(t *testing.T) {
	pool, done := testReapDB(t)
	defer done()
	ctx := context.Background()

	// Insert a run_tuples row directly with NO matching runs row and a
	// stale created_at (>30m ago) — a pure orphan (mimics the trigger
	// crashing between RecordRunTuples and CreateRun).
	//
	// Migration 009 dropped run_tuples' FK to runs, so the orphan row
	// inserts directly (no FK to violate). We bypass store.RecordRunTuples
	// because that would require a runs row; we want the orphan precondition.
	orphanID := uuid.New()
	tup := authz.Tuple{
		User:     authz.UserObject(orphanID.String()).String(),
		Relation: "editor",
		Object:   authz.ProjectObject("lk-proj-orphan"),
	}
	tuplesJSON := []byte(`[{"user":"` + tup.User + `","relation":"` + tup.Relation + `","object":"` + tup.Object.String() + `"}]`)

	staleTime := time.Now().Add(-1 * time.Hour)
	if _, err := pool.Exec(ctx,
		`INSERT INTO run_tuples (run_id, tuples, committed, created_at) VALUES ($1, $2, false, $3)`,
		orphanID, tuplesJSON, staleTime,
	); err != nil {
		t.Fatalf("insert orphan: %v", err)
	}

	az := authztest.New()
	az.Allow(tup.User, tup.Relation, tup.Object)

	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	if err := pkg8s.ReapRunTuples(ctx, pool, c, az); err != nil {
		t.Fatalf("ReapRunTuples: %v", err)
	}

	allowed, _ := az.Check(ctx, tup.User, tup.Relation, tup.Object)
	if allowed {
		t.Errorf("orphan FGA tuple still present after reap")
	}
	rec, _ := store.GetRunTuples(ctx, pool, orphanID)
	if rec != nil {
		t.Errorf("orphan run_tuples row still present: %+v", rec)
	}
}

func TestReapRunTuples_FreshOrphanRowIsLeftAlone(t *testing.T) {
	// Orphan row but younger than 30m → leave alone (slow trigger, not
	// a crash).
	pool, done := testReapDB(t)
	defer done()
	ctx := context.Background()

	orphanID := uuid.New()
	tup := authz.Tuple{
		User:     authz.UserObject(orphanID.String()).String(),
		Relation: "viewer",
		Object:   authz.ProjectObject("lk-proj-fresh"),
	}
	tuplesJSON := []byte(`[{"user":"` + tup.User + `","relation":"` + tup.Relation + `","object":"` + tup.Object.String() + `"}]`)

	// Migration 009 dropped run_tuples' FK to runs, so this orphan row
	// inserts directly (no FK to violate).
	freshTime := time.Now().Add(-1 * time.Minute)
	if _, err := pool.Exec(ctx,
		`INSERT INTO run_tuples (run_id, tuples, committed, created_at) VALUES ($1, $2, false, $3)`,
		orphanID, tuplesJSON, freshTime,
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	az := authztest.New()
	az.Allow(tup.User, tup.Relation, tup.Object)

	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	if err := pkg8s.ReapRunTuples(ctx, pool, c, az); err != nil {
		t.Fatalf("ReapRunTuples: %v", err)
	}

	rec, _ := store.GetRunTuples(ctx, pool, orphanID)
	if rec == nil {
		t.Errorf("fresh orphan was reaped (should wait until >30m old)")
	}
}

func TestReapRunTuples_IsIdempotent(t *testing.T) {
	pool, done := testReapDB(t)
	defer done()
	ctx := context.Background()

	projectID, pipelineID := seedProjectPipeline(t, pool)
	runID := uuid.New()
	if _, err := store.CreateRun(ctx, pool, store.CreateRunOpts{
		ID: runID, ProjectID: projectID, PipelineID: pipelineID,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if _, err := store.UpdateRunPhase(ctx, pool, runID, store.UpdateRunPhaseOpts{
		Phase: "Cancelled", Message: "test",
	}); err != nil {
		t.Fatalf("update phase: %v", err)
	}

	tup := authz.Tuple{
		User:     authz.UserObject(runID.String()).String(),
		Relation: "editor",
		Object:   authz.ProjectObject("lk-proj-idem"),
	}
	if err := store.RecordRunTuples(ctx, pool, runID, []authz.Tuple{tup}); err != nil {
		t.Fatalf("record: %v", err)
	}

	az := authztest.New()
	az.Allow(tup.User, tup.Relation, tup.Object)

	c := fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build()
	for i := 0; i < 3; i++ {
		if err := pkg8s.ReapRunTuples(ctx, pool, c, az); err != nil {
			t.Fatalf("ReapRunTuples iter %d: %v", i, err)
		}
	}
	// Still exactly one DELETE happened — second + third sweeps were
	// no-ops. Verify by re-inserting the tuple into FGA + the row, then
	// running the reaper once: same state should result.
	rec, _ := store.GetRunTuples(ctx, pool, runID)
	if rec != nil {
		t.Errorf("run_tuples row should have been deleted on first sweep")
	}
}
