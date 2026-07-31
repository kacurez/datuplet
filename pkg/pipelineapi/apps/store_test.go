package apps_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi/apps"
	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// testStore mirrors the per-package test-Postgres harness used throughout
// pipelineapi (e.g. pkg/pipelineapi/store/user_test.go) — this package is
// not exempt from the "duplicated, not shared" convention documented in
// task-P0-report.md §A7.
func testStore(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping DB-backed test")
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

// testStoreConcurrent is testStore with the pool's max connections bumped
// to at least minConns. The concurrency regression tests below need every
// goroutine to be able to hold its OWN connection simultaneously — pgxpool's
// default max is small (max(4, NumCPU())), which would otherwise serialize
// most "concurrent" calls on connection acquisition alone and mask the
// races under test (a goroutine that has to wait for a freed connection
// sees the FIRST transaction's already-committed write, which looks like
// correct CAS behavior for the wrong reason).
func testStoreConcurrent(t *testing.T, minConns int32) (*pgxpool.Pool, func()) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping DB-backed test")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if cfg.MaxConns < minConns {
		cfg.MaxConns = minConns
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
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

// testProject inserts a project row via the sibling store package (it
// alone knows how to satisfy the k8s_namespace-matches-id CHECK).
func testProject(t *testing.T, pool *pgxpool.Pool, name string) uuid.UUID {
	t.Helper()
	p, err := store.CreateProject(context.Background(), pool, name)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	return p.ID
}

func hexHash(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestCreate_DuplicateName(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	s := apps.NewStore(pool)
	projectID := testProject(t, pool, "proj-a")

	if _, err := s.Create(ctx, projectID, "dash1"); err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, err := s.Create(ctx, projectID, "dash1")
	if !errors.Is(err, apps.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestPutVersion_SetsDraftAndHash(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	s := apps.NewStore(pool)
	projectID := testProject(t, pool, "proj-a")
	app, err := s.Create(ctx, projectID, "dash1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	bundle := []byte("export async function render(ctx) { return {}; }")
	v, err := s.PutVersion(ctx, app.ID, bundle)
	if err != nil {
		t.Fatalf("PutVersion: %v", err)
	}
	if len(v.Hash) != 64 {
		t.Fatalf("hash length = %d, want 64", len(v.Hash))
	}
	if want := hexHash(bundle); v.Hash != want {
		t.Fatalf("hash = %q, want %q (hex sha256 of raw bundle)", v.Hash, want)
	}

	resolved, err := s.Resolve(ctx, projectID, "dash1", "draft")
	if err != nil {
		t.Fatalf("Resolve(draft): %v", err)
	}
	if resolved.VersionHash != v.Hash || resolved.VersionID != v.ID || resolved.AppID != app.ID {
		t.Fatalf("Resolve(draft) = %+v, want it to point at %+v", resolved, v)
	}
}

func TestPutVersion_GzipRoundTrip(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	s := apps.NewStore(pool)
	projectID := testProject(t, pool, "proj-a")
	app, err := s.Create(ctx, projectID, "dash1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	bundle := make([]byte, 64*1024)
	for i := range bundle {
		bundle[i] = byte(i % 251)
	}
	v, err := s.PutVersion(ctx, app.ID, bundle)
	if err != nil {
		t.Fatalf("PutVersion: %v", err)
	}

	// The stored bytes must actually be gzip-compressed, not the raw
	// bundle — check the on-disk column directly (gzip magic 0x1f 0x8b)
	// and that size_bytes recorded the RAW size, not the compressed one.
	var stored []byte
	var sizeBytes int64
	if err := pool.QueryRow(ctx, `SELECT bundle, size_bytes FROM app_versions WHERE id = $1`, uuid.MustParse(v.ID)).
		Scan(&stored, &sizeBytes); err != nil {
		t.Fatalf("query stored bundle: %v", err)
	}
	if len(stored) < 2 || stored[0] != 0x1f || stored[1] != 0x8b {
		t.Fatalf("stored bundle does not look gzip-compressed (first bytes %v)", stored[:min(2, len(stored))])
	} // min is the Go 1.21+ builtin
	if sizeBytes != int64(len(bundle)) {
		t.Fatalf("size_bytes = %d, want %d (raw size)", sizeBytes, len(bundle))
	}

	got, err := s.GetBundle(ctx, v.Hash)
	if err != nil {
		t.Fatalf("GetBundle: %v", err)
	}
	if string(got) != string(bundle) {
		t.Fatalf("GetBundle round-trip mismatch: got %d bytes, want %d bytes", len(got), len(bundle))
	}
}

func TestPutVersion_IdempotentSameBytes(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	s := apps.NewStore(pool)
	projectID := testProject(t, pool, "proj-a")
	app, err := s.Create(ctx, projectID, "dash1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	bundle := []byte("same content twice")
	v1, err := s.PutVersion(ctx, app.ID, bundle)
	if err != nil {
		t.Fatalf("PutVersion #1: %v", err)
	}
	v2, err := s.PutVersion(ctx, app.ID, bundle)
	if err != nil {
		t.Fatalf("PutVersion #2: %v", err)
	}
	if v1.ID != v2.ID {
		t.Fatalf("re-put of identical bytes produced a new version id: %s != %s", v1.ID, v2.ID)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM app_versions WHERE app_id = $1`, uuid.MustParse(app.ID)).Scan(&count); err != nil {
		t.Fatalf("count versions: %v", err)
	}
	if count != 1 {
		t.Fatalf("app_versions row count = %d, want 1 (idempotent)", count)
	}
}

func TestPutVersion_TooLarge(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	s := apps.NewStore(pool)
	projectID := testProject(t, pool, "proj-a")
	app, err := s.Create(ctx, projectID, "dash1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	oversized := make([]byte, apps.MaxBundleBytes+1)
	_, err = s.PutVersion(ctx, app.ID, oversized)
	if !errors.Is(err, apps.ErrBundleTooLarge) {
		t.Fatalf("expected ErrBundleTooLarge, got %v", err)
	}
}

func TestPutVersion_ProjectQuota(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	s := apps.NewStore(pool)
	projectID := testProject(t, pool, "proj-a")

	// Quota is accounted across every version of every app in the project,
	// but version GC (MaxUnreferencedVersions) only ever evicts a single
	// app's own OLD, unreferenced versions. Spreading the 200 MB across 40
	// one-version apps means every version stays referenced (its app's
	// only channel pointer), so none of it is GC-eligible — this isolates
	// the project-quota behavior from the per-app version-GC behavior
	// (covered separately by TestVersionGC_KeepsNewest20Unreferenced).
	// Highly compressible (mostly-zero) content keeps this test's actual
	// Postgres bytea traffic tiny even though the logical raw sizes sum to
	// the full 200 MB quota — size_bytes accounting is on the RAW length,
	// not the stored compressed length.
	const chunks = apps.MaxProjectBytes / apps.MaxBundleBytes // exactly 40
	for i := 0; i < chunks; i++ {
		app, err := s.Create(ctx, projectID, fmt.Sprintf("dash-%d", i))
		if err != nil {
			t.Fatalf("Create app %d: %v", i, err)
		}
		b := make([]byte, apps.MaxBundleBytes)
		b[0] = byte(i)
		b[1] = byte(i >> 8)
		if _, err := s.PutVersion(ctx, app.ID, b); err != nil {
			t.Fatalf("PutVersion app %d: %v", i, err)
		}
	}

	lastApp, err := s.Create(ctx, projectID, "dash-over")
	if err != nil {
		t.Fatalf("Create over-quota app: %v", err)
	}
	over := make([]byte, 1024)
	over[0] = 0xFF
	over[1] = 0xFF
	_, err = s.PutVersion(ctx, lastApp.ID, over)
	if !errors.Is(err, apps.ErrProjectQuota) {
		t.Fatalf("expected ErrProjectQuota once the project's stored bundles hit 200 MB, got %v", err)
	}
}

func TestPromote_HappyPathAndCAS(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	s := apps.NewStore(pool)
	projectID := testProject(t, pool, "proj-a")
	app, err := s.Create(ctx, projectID, "dash1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	v1, err := s.PutVersion(ctx, app.ID, []byte("v1"))
	if err != nil {
		t.Fatalf("PutVersion v1: %v", err)
	}

	// Happy path: first promote, no production yet -> expectedProduction="".
	if err := s.Promote(ctx, app.ID, v1.Hash, ""); err != nil {
		t.Fatalf("Promote v1: %v", err)
	}
	resolved, err := s.Resolve(ctx, projectID, "dash1", "production")
	if err != nil {
		t.Fatalf("Resolve(production): %v", err)
	}
	if resolved.VersionHash != v1.Hash {
		t.Fatalf("production hash = %q, want %q", resolved.VersionHash, v1.Hash)
	}

	v2, err := s.PutVersion(ctx, app.ID, []byte("v2"))
	if err != nil {
		t.Fatalf("PutVersion v2: %v", err)
	}

	// CAS mismatch: wrong expectedProduction.
	err = s.Promote(ctx, app.ID, v2.Hash, "not-the-current-production-hash")
	if !errors.Is(err, apps.ErrCASMismatch) {
		t.Fatalf("expected ErrCASMismatch, got %v", err)
	}
	// Production must be unchanged after the failed CAS.
	resolved, err = s.Resolve(ctx, projectID, "dash1", "production")
	if err != nil {
		t.Fatalf("Resolve(production) after failed CAS: %v", err)
	}
	if resolved.VersionHash != v1.Hash {
		t.Fatalf("production hash after failed CAS = %q, want unchanged %q", resolved.VersionHash, v1.Hash)
	}

	// Correct CAS succeeds.
	if err := s.Promote(ctx, app.ID, v2.Hash, v1.Hash); err != nil {
		t.Fatalf("Promote v2 with correct CAS: %v", err)
	}
	resolved, err = s.Resolve(ctx, projectID, "dash1", "production")
	if err != nil {
		t.Fatalf("Resolve(production) after promote: %v", err)
	}
	if resolved.VersionHash != v2.Hash {
		t.Fatalf("production hash = %q, want %q", resolved.VersionHash, v2.Hash)
	}
}

func TestPromote_UnknownHash(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	s := apps.NewStore(pool)
	projectID := testProject(t, pool, "proj-a")
	app, err := s.Create(ctx, projectID, "dash1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	err = s.Promote(ctx, app.ID, fmt.Sprintf("%064x", 1), "")
	if err == nil {
		t.Fatal("expected an error promoting an unknown version hash")
	}
	if !errors.Is(err, apps.ErrNotFound) {
		t.Fatalf("expected error to wrap ErrNotFound, got %v", err)
	}
}

func TestResolve_ProductionUnset(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	s := apps.NewStore(pool)
	projectID := testProject(t, pool, "proj-a")
	if _, err := s.Create(ctx, projectID, "dash1"); err != nil {
		t.Fatalf("Create: %v", err)
	}

	_, err := s.Resolve(ctx, projectID, "dash1", "production")
	if !errors.Is(err, apps.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for unset production channel, got %v", err)
	}
}

func TestMintToken_ComposableParts(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	s := apps.NewStore(pool)
	projectID := testProject(t, pool, "proj-a")
	app, err := s.Create(ctx, projectID, "dash1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	tokenID, secret, err := s.MintToken(ctx, app.ID)
	if err != nil {
		t.Fatalf("MintToken: %v", err)
	}
	if _, err := uuid.Parse(tokenID); err != nil {
		t.Fatalf("tokenID %q is not a uuid: %v", tokenID, err)
	}
	secretBytes, err := base64.RawURLEncoding.DecodeString(secret)
	if err != nil {
		t.Fatalf("secret %q is not base64url: %v", secret, err)
	}
	if len(secretBytes) != 32 {
		t.Fatalf("decoded secret length = %d, want 32 bytes", len(secretBytes))
	}

	// The external form is composed by handlers.go (P2) as
	// "vw_<tokenID>.<secret>" — verify MintToken's return values are
	// directly composable into that shape and round-trip apart again.
	external := "vw_" + tokenID + "." + secret
	rest, ok := strings.CutPrefix(external, "vw_")
	if !ok {
		t.Fatalf("external token form %q missing vw_ prefix", external)
	}
	gotID, gotSecret, ok := strings.Cut(rest, ".")
	if !ok || gotID != tokenID || gotSecret != secret {
		t.Fatalf("external token form %q did not round-trip to (id=%q, secret=%q)", external, tokenID, secret)
	}

	verified, err := s.VerifyToken(ctx, app.ID, tokenID, secret)
	if err != nil {
		t.Fatalf("VerifyToken: %v", err)
	}
	if !verified {
		t.Fatal("VerifyToken(mint-produced tokenID+secret) = false, want true")
	}
}

func TestVerifyToken(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	s := apps.NewStore(pool)
	projectID := testProject(t, pool, "proj-a")
	app, err := s.Create(ctx, projectID, "dash1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	tokenID, secret, err := s.MintToken(ctx, app.ID)
	if err != nil {
		t.Fatalf("MintToken: %v", err)
	}

	if ok, err := s.VerifyToken(ctx, app.ID, tokenID, secret); err != nil || !ok {
		t.Fatalf("VerifyToken(correct) = %v, %v; want true, nil", ok, err)
	}
	if ok, err := s.VerifyToken(ctx, app.ID, tokenID, secret+"x"); err != nil || ok {
		t.Fatalf("VerifyToken(wrong secret) = %v, %v; want false, nil", ok, err)
	}
	if ok, err := s.VerifyToken(ctx, app.ID, uuid.NewString(), secret); err != nil || ok {
		t.Fatalf("VerifyToken(unknown token id) = %v, %v; want false, nil", ok, err)
	}

	if err := s.RevokeToken(ctx, app.ID, tokenID); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}
	if ok, err := s.VerifyToken(ctx, app.ID, tokenID, secret); err != nil || ok {
		t.Fatalf("VerifyToken(after revoke) = %v, %v; want false, nil", ok, err)
	}
}

func TestAppendRenderLog_TrimByAge(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	base := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	now := base
	s := apps.NewStore(pool,
		apps.WithClock(func() time.Time { return now }),
		apps.WithRenderLogRetention(48*time.Hour),
	)
	projectID := testProject(t, pool, "proj-a")
	app, err := s.Create(ctx, projectID, "dash1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	rec := func(id string, startedAt time.Time) apps.RenderLogRecord {
		return apps.RenderLogRecord{
			RequestID:     id,
			AppID:         app.ID,
			VersionHash:   fmt.Sprintf("%064x", 1),
			Channel:       "production",
			PrincipalKind: "viewer_token",
			PrincipalID:   uuid.NewString(),
			StartedAt:     startedAt,
			DurationMS:    5,
			Outcome:       "ok",
			LogText:       "hello",
		}
	}

	idA, idB := uuid.NewString(), uuid.NewString()
	if err := s.AppendRenderLog(ctx, rec(idA, now)); err != nil {
		t.Fatalf("AppendRenderLog A: %v", err)
	}
	if err := s.AppendRenderLog(ctx, rec(idB, now)); err != nil {
		t.Fatalf("AppendRenderLog B: %v", err)
	}

	// Both survive: within the 48h retention window relative to "now".
	if logs, err := s.GetRenderLogs(ctx, app.ID, "", 10); err != nil || len(logs) != 2 {
		t.Fatalf("GetRenderLogs after A,B = %d, %v; want 2 logs, nil err", len(logs), err)
	}

	// Advance the clock past the retention window and append a third
	// record — the trim that runs as part of THIS insert should now drop
	// A and B (their started_at is more than 48h before the new "now").
	now = base.Add(72 * time.Hour)
	idC := uuid.NewString()
	if err := s.AppendRenderLog(ctx, rec(idC, now)); err != nil {
		t.Fatalf("AppendRenderLog C: %v", err)
	}

	logs, err := s.GetRenderLogs(ctx, app.ID, "", 10)
	if err != nil {
		t.Fatalf("GetRenderLogs after C: %v", err)
	}
	if len(logs) != 1 || logs[0].RequestID != idC {
		t.Fatalf("GetRenderLogs after age-trim = %+v, want exactly [C]", logs)
	}

	if _, err := s.GetRenderLogs(ctx, app.ID, idA, 0); !errors.Is(err, apps.ErrNotFound) {
		t.Fatalf("GetRenderLogs(trimmed request id A) error = %v, want ErrNotFound", err)
	}
}

func TestAppendRenderLog_TrimByCount(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()

	base := time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)
	s := apps.NewStore(pool, apps.WithClock(func() time.Time { return base.Add(24 * time.Hour) }))
	projectID := testProject(t, pool, "proj-a")
	app, err := s.Create(ctx, projectID, "dash1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	const total = apps.MaxRenderLogsPerApp + 1
	var firstID, lastID string
	for i := 0; i < total; i++ {
		id := uuid.NewString()
		if i == 0 {
			firstID = id
		}
		lastID = id
		err := s.AppendRenderLog(ctx, apps.RenderLogRecord{
			RequestID:     id,
			AppID:         app.ID,
			VersionHash:   fmt.Sprintf("%064x", 1),
			Channel:       "production",
			PrincipalKind: "viewer_token",
			PrincipalID:   uuid.NewString(),
			StartedAt:     base.Add(time.Duration(i) * time.Second), // strictly increasing
			DurationMS:    1,
			Outcome:       "ok",
		})
		if err != nil {
			t.Fatalf("AppendRenderLog #%d: %v", i, err)
		}
	}

	logs, err := s.GetRenderLogs(ctx, app.ID, "", apps.MaxRenderLogsPerApp+10)
	if err != nil {
		t.Fatalf("GetRenderLogs: %v", err)
	}
	if len(logs) != apps.MaxRenderLogsPerApp {
		t.Fatalf("render log count = %d, want %d (ring buffer cap)", len(logs), apps.MaxRenderLogsPerApp)
	}
	if _, err := s.GetRenderLogs(ctx, app.ID, firstID, 0); !errors.Is(err, apps.ErrNotFound) {
		t.Fatalf("GetRenderLogs(oldest, should be trimmed) error = %v, want ErrNotFound", err)
	}
	if _, err := s.GetRenderLogs(ctx, app.ID, lastID, 0); err != nil {
		t.Fatalf("GetRenderLogs(newest) error = %v, want nil", err)
	}
}

func TestGetRenderLogs_UnknownRequestID(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	s := apps.NewStore(pool)
	projectID := testProject(t, pool, "proj-a")
	app, err := s.Create(ctx, projectID, "dash1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	_, err = s.GetRenderLogs(ctx, app.ID, uuid.NewString(), 0)
	if !errors.Is(err, apps.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for an unknown request_id, got %v", err)
	}
}

func TestVersionGC_KeepsNewest20Unreferenced(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	s := apps.NewStore(pool)
	projectID := testProject(t, pool, "proj-a")
	app, err := s.Create(ctx, projectID, "dash1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	const puts = apps.MaxUnreferencedVersions + 2 // 22: leaves 21 unreferenced before the 22nd's GC pass
	hashes := make([]string, puts)
	for i := 0; i < puts; i++ {
		v, err := s.PutVersion(ctx, app.ID, []byte(fmt.Sprintf("bundle-%d", i)))
		if err != nil {
			t.Fatalf("PutVersion #%d: %v", i, err)
		}
		hashes[i] = v.Hash
	}

	// The very first (oldest, unreferenced) version must have been
	// collected once the unreferenced count exceeded 20.
	if _, err := s.GetBundle(ctx, hashes[0]); !errors.Is(err, apps.ErrNotFound) {
		t.Fatalf("GetBundle(oldest unreferenced version) error = %v, want ErrNotFound (should be GC'd)", err)
	}
	// The newest (currently draft-referenced) version must still exist.
	if _, err := s.GetBundle(ctx, hashes[puts-1]); err != nil {
		t.Fatalf("GetBundle(newest/draft version): %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM app_versions WHERE app_id = $1`, uuid.MustParse(app.ID)).Scan(&count); err != nil {
		t.Fatalf("count versions: %v", err)
	}
	// 20 retained unreferenced + 1 draft-referenced.
	if want := apps.MaxUnreferencedVersions + 1; count != want {
		t.Fatalf("app_versions row count = %d, want %d", count, want)
	}
}

// --- Concurrency regression tests (RFC 028 P1 fix round) ---
//
// These launch real concurrent goroutines against the same *apps.Store /
// *pgxpool.Pool and assert an invariant that a racy read-then-write
// implementation can violate even though every individual call returns a
// plausible-looking result. A start barrier (close(start)) maximizes the
// chance every goroutine's transaction is in flight at the same time,
// which is what actually exercises the race.

// TestPromote_ConcurrentCAS_ExactlyOneWinner pins the CAS contract under
// real concurrency: N candidate versions are promoted concurrently with
// the SAME expectedProduction. Exactly one must succeed; the rest must
// observe ErrCASMismatch (not silently overwrite each other); the final
// resolved production version must be the winner's.
// runConcurrentPromotes fires one concurrent Promote per entry in targets
// (all sharing expectedProduction, all against appID), releasing every
// goroutine from the same start barrier to maximize the chance their
// transactions overlap, and returns each call's error in target order.
func runConcurrentPromotes(ctx context.Context, s *apps.Store, appID, expectedProduction string, targets []*apps.Version) []error {
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]error, len(targets))
	for i, target := range targets {
		wg.Add(1)
		go func(i int, target *apps.Version) {
			defer wg.Done()
			<-start
			results[i] = s.Promote(ctx, appID, target.Hash, expectedProduction)
		}(i, target)
	}
	close(start)
	wg.Wait()
	return results
}

// TestPromote_ConcurrentCAS_ExactlyOneWinner pins the CAS contract under
// real concurrency, against an ALREADY-SET production channel: N Promote
// calls carrying the same (correct) expectedProduction race for the same
// app. Exactly one must succeed; the rest must observe ErrCASMismatch
// (never silently overwrite each other); the final resolved production
// version must be the winner's.
//
// Run as several independent rounds (fresh app each round) rather than
// one giant N, because whether a single round's goroutines' reads
// actually overlap before any of them writes is itself timing-dependent
// — a racy implementation doesn't fail on every attempt, only when the
// interleaving is unlucky enough. Repeating rounds gives many independent
// chances to observe a violation without needing an artificially huge N.
func TestPromote_ConcurrentCAS_ExactlyOneWinner(t *testing.T) {
	const (
		rounds             = 8
		n                  = 24
		distinctCandidates = 3 // see TestVersionGC comment below on why not n
	)
	pool, cleanup := testStoreConcurrent(t, n+4)
	defer cleanup()
	ctx := context.Background()
	s := apps.NewStore(pool)
	projectID := testProject(t, pool, "proj-a")

	for round := 0; round < rounds; round++ {
		appName := fmt.Sprintf("dash-%d", round)
		app, err := s.Create(ctx, projectID, appName)
		if err != nil {
			t.Fatalf("round %d: Create: %v", round, err)
		}

		baseline, err := s.PutVersion(ctx, app.ID, []byte("baseline"))
		if err != nil {
			t.Fatalf("round %d: PutVersion baseline: %v", round, err)
		}
		if err := s.Promote(ctx, app.ID, baseline.Hash, ""); err != nil {
			t.Fatalf("round %d: Promote baseline: %v", round, err)
		}

		// Only a handful of distinct non-baseline versions (cycled across
		// the n concurrent calls below), NOT one per call: version GC
		// (MaxUnreferencedVersions=20) would otherwise collect the oldest
		// unreferenced candidates as soon as this app accumulates more
		// than 20 of them via sequential PutVersion calls, before the
		// race even starts.
		candidates := make([]*apps.Version, distinctCandidates)
		for i := range candidates {
			v, err := s.PutVersion(ctx, app.ID, []byte(fmt.Sprintf("round-%d-candidate-%d", round, i)))
			if err != nil {
				t.Fatalf("round %d: PutVersion candidate %d: %v", round, i, err)
			}
			candidates[i] = v
		}
		targets := make([]*apps.Version, n)
		for i := range targets {
			targets[i] = candidates[i%distinctCandidates]
		}

		results := runConcurrentPromotes(ctx, s, app.ID, baseline.Hash, targets)

		successes := 0
		var winner *apps.Version
		for i, rerr := range results {
			switch {
			case rerr == nil:
				successes++
				winner = targets[i]
			case errors.Is(rerr, apps.ErrCASMismatch):
				// expected for every loser
			default:
				t.Fatalf("round %d: promote %d: unexpected error %v", round, i, rerr)
			}
		}
		if successes != 1 {
			t.Fatalf("round %d: concurrent CAS promotes: %d succeeded, want exactly 1 (results=%v)", round, successes, results)
		}

		resolved, err := s.Resolve(ctx, projectID, appName, "production")
		if err != nil {
			t.Fatalf("round %d: Resolve(production): %v", round, err)
		}
		if resolved.VersionHash != winner.Hash {
			t.Fatalf("round %d: final production hash = %q, want the winner's %q", round, resolved.VersionHash, winner.Hash)
		}
	}
}

// TestPromote_ConcurrentFirstPromote_ExactlyOneWinner covers the other CAS
// branch: expectedProduction == "" (no production set yet). Only one of N
// concurrent "first promotes" may win; the rest must get ErrCASMismatch
// rather than silently clobbering the winner. See
// TestPromote_ConcurrentCAS_ExactlyOneWinner for why this runs several
// rounds instead of one large N.
func TestPromote_ConcurrentFirstPromote_ExactlyOneWinner(t *testing.T) {
	const (
		rounds             = 8
		n                  = 24
		distinctCandidates = 3
	)
	pool, cleanup := testStoreConcurrent(t, n+4)
	defer cleanup()
	ctx := context.Background()
	s := apps.NewStore(pool)
	projectID := testProject(t, pool, "proj-a")

	for round := 0; round < rounds; round++ {
		appName := fmt.Sprintf("dash-%d", round)
		app, err := s.Create(ctx, projectID, appName)
		if err != nil {
			t.Fatalf("round %d: Create: %v", round, err)
		}

		candidates := make([]*apps.Version, distinctCandidates)
		for i := range candidates {
			v, err := s.PutVersion(ctx, app.ID, []byte(fmt.Sprintf("round-%d-candidate-%d", round, i)))
			if err != nil {
				t.Fatalf("round %d: PutVersion candidate %d: %v", round, i, err)
			}
			candidates[i] = v
		}
		targets := make([]*apps.Version, n)
		for i := range targets {
			targets[i] = candidates[i%distinctCandidates]
		}

		results := runConcurrentPromotes(ctx, s, app.ID, "", targets)

		successes := 0
		var winner *apps.Version
		for i, rerr := range results {
			switch {
			case rerr == nil:
				successes++
				winner = targets[i]
			case errors.Is(rerr, apps.ErrCASMismatch):
				// expected for every loser
			default:
				t.Fatalf("round %d: promote %d: unexpected error %v", round, i, rerr)
			}
		}
		if successes != 1 {
			t.Fatalf("round %d: concurrent first-promotes: %d succeeded, want exactly 1 (results=%v)", round, successes, results)
		}

		resolved, err := s.Resolve(ctx, projectID, appName, "production")
		if err != nil {
			t.Fatalf("round %d: Resolve(production): %v", round, err)
		}
		if resolved.VersionHash != winner.Hash {
			t.Fatalf("round %d: final production hash = %q, want the winner's %q", round, resolved.VersionHash, winner.Hash)
		}
	}
}

// TestPutVersion_ConcurrentQuota_NeverExceedsCap fires enough concurrent
// PutVersions, each within the single-bundle cap but collectively over the
// project's 200 MB quota, to prove the quota check can't be beaten by a
// read-then-write race: the total stored across the project must never
// exceed MaxProjectBytes, no matter how the goroutines interleave. Each
// goroutine gets its own single-version app, so per-app version GC (a
// different mechanism, covered by TestVersionGC_KeepsNewest20Unreferenced)
// can't confound this test by evicting anything.
func TestPutVersion_ConcurrentQuota_NeverExceedsCap(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	s := apps.NewStore(pool)
	projectID := testProject(t, pool, "proj-a")

	// One MaxBundleBytes upload per goroutine; capacity/MaxBundleBytes = 40,
	// so n=41 guarantees at least one must be rejected once the cap holds.
	const n = apps.MaxProjectBytes/apps.MaxBundleBytes + 1
	appIDs := make([]string, n)
	for i := 0; i < n; i++ {
		app, err := s.Create(ctx, projectID, fmt.Sprintf("dash-%d", i))
		if err != nil {
			t.Fatalf("Create app %d: %v", i, err)
		}
		appIDs[i] = app.ID
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b := make([]byte, apps.MaxBundleBytes)
			b[0] = byte(i)
			b[1] = byte(i >> 8)
			<-start
			_, errs[i] = s.PutVersion(ctx, appIDs[i], b)
		}(i)
	}
	close(start)
	wg.Wait()

	successes := 0
	for i, err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, apps.ErrProjectQuota):
			// expected once the cap is hit
		default:
			t.Fatalf("PutVersion %d: unexpected error %v", i, err)
		}
	}
	if successes > apps.MaxProjectBytes/apps.MaxBundleBytes {
		t.Fatalf("successes = %d, want at most %d (200 MB / 5 MB) — quota was exceeded under concurrency",
			successes, apps.MaxProjectBytes/apps.MaxBundleBytes)
	}

	var total int64
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(av.size_bytes), 0)
		  FROM app_versions av
		  JOIN apps a ON a.id = av.app_id
		 WHERE a.project_id = $1`, projectID).Scan(&total); err != nil {
		t.Fatalf("sum stored bytes: %v", err)
	}
	if total > apps.MaxProjectBytes {
		t.Fatalf("total stored bytes for project = %d, want <= %d (MaxProjectBytes) — quota was exceeded under concurrency",
			total, apps.MaxProjectBytes)
	}
}

// TestPutVersion_ConcurrentIdempotent_SameBytes fires N concurrent
// PutVersions of byte-identical content at the same app. All must
// succeed, all must return the same version id, and exactly one
// app_versions row must exist — a racy SELECT-then-INSERT pair would
// instead let more than one goroutine take the "new version" branch and
// have all but one fail on the UNIQUE(app_id, hash) constraint.
func TestPutVersion_ConcurrentIdempotent_SameBytes(t *testing.T) {
	pool, cleanup := testStore(t)
	defer cleanup()
	ctx := context.Background()
	s := apps.NewStore(pool)
	projectID := testProject(t, pool, "proj-a")
	app, err := s.Create(ctx, projectID, "dash1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	bundle := []byte("identical content for concurrent idempotent put")

	const n = 12
	start := make(chan struct{})
	var wg sync.WaitGroup
	versions := make([]*apps.Version, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			versions[i], errs[i] = s.PutVersion(ctx, app.ID, bundle)
		}(i)
	}
	close(start)
	wg.Wait()

	var firstID string
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("PutVersion goroutine %d: %v", i, errs[i])
		}
		if firstID == "" {
			firstID = versions[i].ID
		} else if versions[i].ID != firstID {
			t.Fatalf("goroutine %d returned version id %q, want %q (all concurrent puts of identical bytes must agree)", i, versions[i].ID, firstID)
		}
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM app_versions WHERE app_id = $1`, uuid.MustParse(app.ID)).Scan(&count); err != nil {
		t.Fatalf("count versions: %v", err)
	}
	if count != 1 {
		t.Fatalf("app_versions row count = %d, want 1 (idempotent even under concurrency)", count)
	}
}
