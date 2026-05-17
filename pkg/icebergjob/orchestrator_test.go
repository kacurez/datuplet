package icebergjob

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergtable "github.com/apache/iceberg-go/table"
)

// stubCatalog implements the narrow `catalogOps` surface the
// orchestrator uses. It records call counts so tests can assert the
// expected sequence (LoadTable miss → CreateTable → commit fn) without
// standing up a real lakekeeper.
type stubCatalog struct {
	checkNS      atomic.Int32
	createNS     atomic.Int32
	loadCalls    atomic.Int32
	createCalls  atomic.Int32
	nsExists     bool
	tableExists  atomic.Bool
	loadErr      error
	createErr    error
	createTblErr error
}

func (s *stubCatalog) CheckNamespaceExists(ctx context.Context, ns icebergtable.Identifier) (bool, error) {
	s.checkNS.Add(1)
	return s.nsExists, nil
}

func (s *stubCatalog) CreateNamespace(ctx context.Context, ns icebergtable.Identifier, props iceberg.Properties) error {
	s.createNS.Add(1)
	if s.createErr != nil {
		return s.createErr
	}
	s.nsExists = true
	return nil
}

func (s *stubCatalog) LoadTable(ctx context.Context, id icebergtable.Identifier) (*icebergtable.Table, error) {
	s.loadCalls.Add(1)
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	if !s.tableExists.Load() {
		return nil, catalog.ErrNoSuchTable
	}
	// Return nil-table is enough for the orchestrator: production code
	// passes the table directly into the commit fn (which the tests
	// substitute), so the *table.Table itself is never dereferenced
	// off the LoadTable return path.
	return nil, nil
}

func (s *stubCatalog) CreateTable(ctx context.Context, id icebergtable.Identifier, sc *iceberg.Schema, opts ...catalog.CreateTableOpt) (*icebergtable.Table, error) {
	s.createCalls.Add(1)
	if s.createTblErr != nil {
		return nil, s.createTblErr
	}
	s.tableExists.Store(true)
	return nil, nil
}

// TestEnsureNamespace_Idempotent covers the namespace-create idempotence
// contract: an already-existing namespace must not error.
func TestEnsureNamespace_Idempotent(t *testing.T) {
	t.Parallel()
	s := &stubCatalog{nsExists: true}
	if err := ensureNamespace(context.Background(), s, "raw"); err != nil {
		t.Fatalf("ensureNamespace on existing ns: %v", err)
	}
	if got := s.createNS.Load(); got != 0 {
		t.Errorf("CreateNamespace called %d times when ns already existed (want 0)", got)
	}
}

// TestEnsureNamespace_RaceFallthrough exercises the alreadyExists race
// path: another worker created the ns between our Check + Create.
func TestEnsureNamespace_RaceFallthrough(t *testing.T) {
	t.Parallel()
	s := &stubCatalog{nsExists: false, createErr: catalog.ErrNamespaceAlreadyExists}
	if err := ensureNamespace(context.Background(), s, "raw"); err != nil {
		t.Fatalf("ensureNamespace race path: %v", err)
	}
}

// TestCommitOne_LoadHits uses an existing-table stub: the orchestrator
// must NOT call CreateTable, just hand off to the commit fn.
func TestCommitOne_LoadHits(t *testing.T) {
	t.Parallel()
	s := &stubCatalog{nsExists: true}
	s.tableExists.Store(true)

	var commitCalls atomic.Int32
	commit := func(ctx context.Context, cat catalogOps, ns, tbl string, paths []string) error {
		commitCalls.Add(1)
		return nil
	}

	committer := &TableCommitter{config: &Config{
		RunID:         "r1",
		LakekeeperURL: "http://lk:8181/catalog",
	}}
	r := committer.commitOne(context.Background(), s, commit, TableConfig{Namespace: "raw", Table: "events"}, []string{"s3://b/wh/raw/events/data/a.parquet"})
	if !r.Success {
		t.Fatalf("commitOne failed: %v", r.Error)
	}
	if r.FilesAdded != 1 {
		t.Errorf("FilesAdded=%d want 1", r.FilesAdded)
	}
	if got := s.createCalls.Load(); got != 0 {
		t.Errorf("CreateTable called %d times when LoadTable hit (want 0)", got)
	}
	if got := commitCalls.Load(); got != 1 {
		t.Errorf("commit fn called %d times (want 1)", got)
	}
}

// TestCommitOne_LoadMiss_FailsFast verifies that when LoadTable returns
// ErrNoSuchTable (cold-start table that DG has not yet created), the commit
// fails loudly with an actionable error message.
// Schema inference is not supported — CreateTable is never called from here.
func TestCommitOne_LoadMiss_FailsFast(t *testing.T) {
	t.Parallel()
	s := &stubCatalog{nsExists: true}
	// Default tableExists=false → LoadTable returns ErrNoSuchTable.

	var commitCalls atomic.Int32
	commit := func(ctx context.Context, cat catalogOps, ns, tbl string, paths []string) error {
		commitCalls.Add(1)
		return nil
	}

	committer := &TableCommitter{config: &Config{
		RunID:         "r1",
		LakekeeperURL: "http://lk:8181/catalog",
	}}
	r := committer.commitOne(context.Background(), s, commit, TableConfig{Namespace: "raw", Table: "events"}, []string{"s3://b/wh/raw/events/data/a.parquet"})

	// Must fail — cold-start tables are not supported without DG CreateTable.
	if r.Success {
		t.Fatal("commitOne must fail on cold-start table (schema inference not supported)")
	}
	// Error message must be actionable.
	if !strings.Contains(r.Error, "not found in catalog") {
		t.Errorf("error message should reference table not found, got: %q", r.Error)
	}
	// CreateTable must NOT be called — operator does not infer schema.
	if got := s.createCalls.Load(); got != 0 {
		t.Errorf("CreateTable called %d times (want 0 — schema inference not supported)", got)
	}
	// Commit fn must NOT be called.
	if got := commitCalls.Load(); got != 0 {
		t.Errorf("commit fn called %d times (want 0)", got)
	}
}

// TestCommitOne_RetriesOn409 verifies the 409→retry contract end-to-end
// at the orchestrator boundary: a commit fn that fails twice with
// rest.ErrCommitFailed and succeeds on the third attempt produces a
// successful TableResult and the retry was actually exercised.
func TestCommitOne_RetriesOn409(t *testing.T) {
	t.Parallel()
	s := &stubCatalog{nsExists: true}
	s.tableExists.Store(true) // Skip the create-table branch.

	var commitCalls atomic.Int32
	commit := func(ctx context.Context, cat catalogOps, ns, tbl string, paths []string) error {
		n := commitCalls.Add(1)
		if n < 3 {
			// Wrap to confirm errors.Is matches through wrapping.
			return fmt.Errorf("simulated 409: %w", rest.ErrCommitFailed)
		}
		return nil
	}

	committer := &TableCommitter{config: &Config{
		RunID:         "r1",
		LakekeeperURL: "http://lk:8181/catalog",
	}}
	r := committer.commitOne(context.Background(), s, commit, TableConfig{Namespace: "raw", Table: "events"}, []string{"s3://b/wh/raw/events/data/a.parquet"})
	if !r.Success {
		t.Fatalf("commitOne failed: %v", r.Error)
	}
	if got := commitCalls.Load(); got != 3 {
		t.Errorf("commit fn called %d times (want 3 — 2 conflicts + success)", got)
	}
}

// TestCommitOne_RetriesOn409_LoadsFreshPerAttempt mirrors the production
// commit shape in defaultCommitFiles (LoadTable → NewTransaction →
// AddFiles → Commit) for the load-bearing assertion the operator
// relies on: a 409 retry must re-load the table so a concurrent
// commit landing between attempts gets observed.
//
// We inline a commitFunc that does the LoadTable step explicitly,
// bumps the stub's load counter, then synthesises 409s on early
// attempts. This exercises the same load-per-attempt behaviour
// defaultCommitFiles encodes without needing a real *icebergtable.Table
// (the iceberg-go stack panics on nil-table NewTransaction calls).
func TestCommitOne_RetriesOn409_LoadsFreshPerAttempt(t *testing.T) {
	t.Parallel()
	s := &stubCatalog{nsExists: true}
	s.tableExists.Store(true) // Skip the create-table branch in commitOne.

	var commitCalls atomic.Int32
	loadAtCommit := atomic.Int32{} // LoadTable invocations from inside the retry loop only.
	loadStart := s.loadCalls.Load()

	// Commit fn shaped like defaultCommitFiles: re-LoadTable on every
	// attempt before exercising the synthetic 409 path. We cannot reach
	// fresh.NewTransaction without a real *icebergtable.Table, but the
	// LoadTable call itself is the load-bearing observable.
	commit := func(ctx context.Context, cat catalogOps, ns, tbl string, paths []string) error {
		if _, err := cat.LoadTable(ctx, []string{ns, tbl}); err != nil {
			return err
		}
		loadAtCommit.Add(1)
		n := commitCalls.Add(1)
		if n < 3 {
			return fmt.Errorf("simulated 409: %w", rest.ErrCommitFailed)
		}
		return nil
	}

	committer := &TableCommitter{config: &Config{
		RunID:         "r1",
		LakekeeperURL: "http://lk:8181/catalog",
	}}
	r := committer.commitOne(context.Background(), s, commit, TableConfig{Namespace: "raw", Table: "events"}, []string{"s3://b/wh/raw/events/data/a.parquet"})
	if !r.Success {
		t.Fatalf("commitOne failed: %v", r.Error)
	}
	if got := commitCalls.Load(); got != 3 {
		t.Errorf("commit fn called %d times (want 3 — 2 conflicts + success)", got)
	}
	// Per-attempt LoadTable inside the retry loop: 3 attempts → 3
	// loads from inside commit. The orchestrator's commitOne also
	// performs one LoadTable up front via loadOrCreateTable, so the
	// stub-level total is 3+1=4. We assert both the in-loop count
	// (the load-bearing semantic) and the absolute total to make a
	// future regression that drops one of the two surfaces obvious.
	if got := loadAtCommit.Load(); got != 3 {
		t.Errorf("LoadTable inside commit fn = %d, want 3 (one per retry attempt)", got)
	}
	if got := s.loadCalls.Load() - loadStart; got != 4 {
		t.Errorf("total LoadTable calls = %d, want 4 (1 from loadOrCreateTable + 3 retries)", got)
	}
}

// TestCommitTable_MissingTokenInMap covers the fail-loud path: an auth
// token is expected but missing. Surfacing this as a clear error beats
// letting catalogwriter ship a bare `Authorization: Bearer ` header that
// lakekeeper rejects with a confusing 401. The check fires before any
// catalog client is opened, so we don't need to stand up a stub server.
// (The per-table token map shape is retired; every catalog call uses the
// single per-run JWT.)

// TestCommitOne_PropagatesNonConflict ensures non-409 errors short-
// circuit to TableResult.Error rather than burning the retry budget.
func TestCommitOne_PropagatesNonConflict(t *testing.T) {
	t.Parallel()
	s := &stubCatalog{nsExists: true}
	s.tableExists.Store(true)

	authErr := errors.New("auth failed")
	var commitCalls atomic.Int32
	commit := func(ctx context.Context, cat catalogOps, ns, tbl string, paths []string) error {
		commitCalls.Add(1)
		return authErr
	}

	committer := &TableCommitter{config: &Config{
		RunID:         "r1",
		LakekeeperURL: "http://lk:8181/catalog",
	}}
	r := committer.commitOne(context.Background(), s, commit, TableConfig{Namespace: "raw", Table: "events"}, []string{"s3://b/wh/raw/events/data/a.parquet"})
	if r.Success {
		t.Fatalf("expected failure, got success")
	}
	if got := commitCalls.Load(); got != 1 {
		t.Errorf("commit fn called %d times (want 1 — non-409 must short-circuit)", got)
	}
}
