package preview

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
)

func TestSweep_DeletesTupleAtTerminal(t *testing.T) {
	e := newEnv(t, roomyCaps)
	resp := e.create("a")
	e.seedTables(resp)
	e.finish(resp.ID, time.Now())
	e.rec.reset()

	if err := e.svc.SweepOnce(e.ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	p := e.get(resp.ID)
	if p.FGATupleDeletedAt == nil || p.CleanedAt != nil {
		t.Errorf("row = %+v", p)
	}
	if e.az.has(resp.ID.String()) {
		t.Error("tuple still present")
	}
	if e.cat.total() != len(resp.ScratchNamespaces) {
		t.Errorf("scratch tables touched: %d left", e.cat.total())
	}
	for _, ev := range e.rec.list() {
		if ev == "open" || strings.HasPrefix(ev, "purge ") {
			t.Errorf("unexpected catalog call %q", ev)
		}
	}
}

func TestPurge_PurgesAndDropsNamespaces(t *testing.T) {
	e := newEnv(t, roomyCaps)
	resp := e.create("a")
	for _, ns := range resp.ScratchNamespaces {
		e.cat.seed(ns, "t1", "t2")
	}
	e.finish(resp.ID, time.Now())
	e.rec.reset()

	if err := e.svc.Purge(e.ctx, e.get(resp.ID)); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	p := e.get(resp.ID)
	if p.CleanedAt == nil || p.FGATupleDeletedAt == nil || p.Phase != "Succeeded" {
		t.Errorf("row = %+v", p)
	}
	if e.cat.total() != 0 || len(e.cat.tables) != 0 {
		t.Errorf("catalog not emptied: %v", e.cat.tables)
	}
	if e.pipelineRunExists(resp.ID) {
		t.Error("PipelineRun still present")
	}

	ev := e.rec.list()
	sweeper := authz.UserObject("preview-sweeper-" + resp.ID.String()).String()
	own := authz.UserObject(resp.ID.String()).String()
	isPurge := func(s string) bool { return strings.HasPrefix(s, "purge ") }
	write := indexOf(ev, func(s string) bool { return s == "write "+sweeper }, false)
	del := indexOf(ev, func(s string) bool { return s == "delete "+sweeper }, false)
	first, last := indexOf(ev, isPurge, false), indexOf(ev, isPurge, true)
	if write < 0 || first < 0 || write > first || del < last {
		t.Errorf("sweeper tuple must bracket the purges: %v", ev)
	}
	for _, s := range ev {
		if s == "write "+own {
			t.Errorf("preview's own identity re-granted: %v", ev)
		}
	}
	if indexOf(ev, func(s string) bool { return s == "delete "+own }, false) < 0 {
		t.Errorf("preview's own tuple not deleted: %v", ev)
	}
	if e.az.has("preview-sweeper-" + resp.ID.String()) {
		t.Error("sweeper tuple left behind")
	}
	for _, ns := range resp.ScratchNamespaces {
		if indexOf(ev, func(s string) bool { return s == "drop "+ns }, false) < 0 {
			t.Errorf("namespace %s not dropped: %v", ns, ev)
		}
	}
}

func TestPurge_SweeperTupleRemovedOnError(t *testing.T) {
	e := newEnv(t, roomyCaps)
	resp := e.create("a")
	e.seedTables(resp)
	e.finish(resp.ID, time.Now())
	e.cat.purgeErr = func(string, string) error { return errors.New("lakekeeper down") }
	e.rec.reset()

	if err := e.svc.Purge(e.ctx, e.get(resp.ID)); err == nil {
		t.Fatal("Purge should fail")
	}
	sw := "preview-sweeper-" + resp.ID.String()
	if e.az.has(sw) {
		t.Error("sweeper tuple left behind after a failed purge")
	}
	if indexOf(e.rec.list(), func(s string) bool { return s == "delete "+authz.UserObject(sw).String() }, false) < 0 {
		t.Errorf("sweeper tuple never deleted: %v", e.rec.list())
	}
	if p := e.get(resp.ID); p.CleanedAt != nil {
		t.Error("cleaned_at set after a failed purge")
	}
}

func TestPurge_FailedSweeperTupleDeleteKeepsRowUncleaned(t *testing.T) {
	e := newEnv(t, roomyCaps)
	resp := e.create("a")
	e.seedTables(resp)
	e.finish(resp.ID, time.Now())
	sweeper := authz.UserObject("preview-sweeper-" + resp.ID.String()).String()
	e.az.deleteErr = func(user string) error {
		if user == sweeper {
			return errors.New("openfga down")
		}
		return nil
	}

	if err := e.svc.Purge(e.ctx, e.get(resp.ID)); err == nil || !strings.Contains(err.Error(), "sweeper tuple") {
		t.Fatalf("Purge should report the leaked sweeper tuple, got %v", err)
	}
	if p := e.get(resp.ID); p.CleanedAt != nil {
		t.Fatal("cleaned_at set while the sweeper tuple is still granted")
	}

	e.az.deleteErr = nil
	if err := e.svc.Purge(e.ctx, e.get(resp.ID)); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if p := e.get(resp.ID); p.CleanedAt == nil || e.az.has("preview-sweeper-"+resp.ID.String()) {
		t.Errorf("retry did not clean up: cleaned_at=%v sweeper tuple=%v", p.CleanedAt, e.az.has("preview-sweeper-"+resp.ID.String()))
	}
}

func TestPurge_RetriesAfterPartialFailure(t *testing.T) {
	e := newEnv(t, roomyCaps)
	resp := e.create("a")
	e.seedTables(resp)
	e.finish(resp.ID, time.Now().Add(-2*time.Hour)) // ttl 1h → expired
	calls := 0
	e.cat.purgeErr = func(string, string) error {
		calls++
		if calls == 2 {
			return errors.New("transient")
		}
		return nil
	}

	if err := e.svc.SweepOnce(e.ctx); err == nil {
		t.Fatal("first sweep should report the failed purge")
	}
	if p := e.get(resp.ID); p.CleanedAt != nil {
		t.Fatal("cleaned_at set after a partial purge")
	}
	if err := e.svc.SweepOnce(e.ctx); err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if p := e.get(resp.ID); p.CleanedAt == nil || p.Phase != "Succeeded" {
		t.Errorf("row after retry = %+v", p)
	}
	if e.cat.total() != 0 {
		t.Errorf("tables left: %v", e.cat.tables)
	}
}

func TestPurge_WaitsForPodsGone(t *testing.T) {
	e := newEnv(t, roomyCaps)
	resp := e.create("a")
	e.seedTables(resp)
	e.finish(resp.ID, time.Now())
	e.addPod(resp.ID)
	e.rec.reset()

	err := e.svc.Purge(e.ctx, e.get(resp.ID))
	if err == nil || !strings.Contains(err.Error(), "pods") {
		t.Fatalf("Purge should fail while pods remain, got %v", err)
	}
	if e.pipelineRunExists(resp.ID) {
		t.Error("PipelineRun should be deleted before waiting")
	}
	for _, ev := range e.rec.list() {
		if ev == "open" || strings.HasPrefix(ev, "list ") || strings.HasPrefix(ev, "purge ") {
			t.Errorf("catalog called while pods remain: %q", ev)
		}
	}
	if p := e.get(resp.ID); p.CleanedAt != nil {
		t.Error("cleaned_at set")
	}
}

func TestSweep_ExpiresRunningPreview(t *testing.T) {
	e := newEnv(t, roomyCaps)
	resp := e.create("a")
	e.seedTables(resp)
	if _, err := e.pool.Exec(e.ctx, `UPDATE previews SET phase = 'Running', expires_at = now() - interval '1 minute' WHERE id = $1`, resp.ID); err != nil {
		t.Fatal(err)
	}

	if err := e.svc.SweepOnce(e.ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	p := e.get(resp.ID)
	if p.Phase != "Expired" || p.CleanedAt == nil || p.FGATupleDeletedAt == nil {
		t.Errorf("row = %+v", p)
	}
	if e.pipelineRunExists(resp.ID) || e.az.count() != 0 || e.cat.total() != 0 {
		t.Errorf("leftovers: pr=%v tuples=%d tables=%d", e.pipelineRunExists(resp.ID), e.az.count(), e.cat.total())
	}
}

func TestSweepIfLeader_SkipsWhenLockHeld(t *testing.T) {
	e := newEnv(t, roomyCaps)
	conn, err := e.pool.Acquire(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(e.ctx, `SELECT pg_advisory_lock($1)`, sweepLockKey); err != nil {
		t.Fatal(err)
	}
	if got, err := e.svc.sweepIfLeader(e.ctx); got || err != nil {
		t.Fatalf("lock held elsewhere: got=%v err=%v", got, err)
	}
	if _, err := conn.Exec(e.ctx, `SELECT pg_advisory_unlock($1)`, sweepLockKey); err != nil {
		t.Fatal(err)
	}
	if got, err := e.svc.sweepIfLeader(e.ctx); !got || err != nil {
		t.Fatalf("lock free: got=%v err=%v", got, err)
	}
	var relocked bool
	if err := conn.QueryRow(e.ctx, `SELECT pg_try_advisory_lock($1)`, sweepLockKey).Scan(&relocked); err != nil || !relocked {
		t.Fatalf("sweep lock not released after a sweep: got=%v err=%v", relocked, err)
	}
	if _, err := conn.Exec(e.ctx, `SELECT pg_advisory_unlock($1)`, sweepLockKey); err != nil {
		t.Fatal(err)
	}
}

// TestRESTCatalog_NotFoundIsSuccess pins the Catalog contract on the real
// lakekeeper adapter: a missing namespace lists no tables, and purging or
// dropping something already gone succeeds.
func TestRESTCatalog_NotFoundIsSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/config" {
			_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
			return
		}
		typ := "NoSuchNamespaceException"
		if strings.Contains(r.URL.Path, "/tables/") {
			typ = "NoSuchTableException"
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"message":"gone","type":"` + typ + `","code":404}}`))
	}))
	defer srv.Close()

	cat, err := NewRESTCatalogFactory(srv.URL)(context.Background(),
		func(context.Context) (string, error) { return "tok", nil }, "lk", "wh")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	if tables, err := cat.ListTables(ctx, "pv-x-raw"); err != nil || tables != nil {
		t.Errorf("ListTables = %v, %v", tables, err)
	}
	if err := cat.PurgeTable(ctx, "pv-x-raw", "t"); err != nil {
		t.Errorf("PurgeTable: %v", err)
	}
	if err := cat.DropNamespace(ctx, "pv-x-raw"); err != nil {
		t.Errorf("DropNamespace: %v", err)
	}
}
