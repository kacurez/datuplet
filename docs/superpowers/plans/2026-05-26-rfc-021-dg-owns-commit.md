# RFC 021 — DataGateway owns the Iceberg commit — Implementation Plan (v2, post-review)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move iceberg `AddFiles + Commit` out of the per-stage `iceberg-job` Kubernetes Job and into the Data Gateway sidecar, executed in a bounded goroutine pool dispatched from `CloseWriter`, with the session-level `Commit` RPC as the barrier. Delete the operator's commit-Job state machine. Keep the `iceberg-job` library for future maintenance subcommands.

**Architecture:** DG already holds the per-run JWT, the Lakekeeper REST client (re-opened per call), and writes parquet via vended creds. `CloseWriter` already closes the buffer/router and flushes parquet. We add a library function `icebergjob.CommitTableFiles` (in-memory paths + idempotency key), a race-free commit pool in DG, a `Resolver.Catalog(ctx)` accessor, then refactor the writer-finalize path so per-table commit dispatch happens in `CloseWriter` and `Commit` is the drain barrier. The operator's commit-Job machinery is deleted; stages transition `Running → Succeeded` directly.

**Tech Stack:** Go 1.22+, iceberg-go, Lakekeeper REST (`pkg/catalogwriter`), K8s controller-runtime, gRPC, prometheus/client_golang.

**POC posture:** No migration story, no feature flag. The DG-commit activation (Task 4) and the operator deletion (Task 7) are a **coupled cutover** — there is no safe intermediate state where both DG and the TableCommit Job commit the same files (that would double-commit). They must land together. The safety net against silent regression is the barrier's per-writer reconciliation (Task 4 Step 6), not task ordering.

**RFC reference:** [docs/tmp/rfc/021-dg-owns-iceberg-commit.md](../../tmp/rfc/021-dg-owns-iceberg-commit.md).

---

## Verified ground-truth facts (read before implementing)

These were checked against the current code on `main`. The first plan got several wrong; do not re-derive from memory.

1. **`CloseWriter` already closes the buffer/router and flushes parquet** — `pkg/datagateway/server_v2_writing.go:277-293` (`ws.partitionRouter.Close()` / `ws.bufferMgr.Close()`). After `CloseWriter` returns, `FilesWritten()` is populated. External-files flow (DuckDB/sql-transform) sets `ws.externalFiles` in `CloseWriter` (lines 244-268) and returns early.
2. **`Commit()` currently ALSO closes the buffer/router AGAIN, collects paths, writes `files.json`, writes schema/manifest, and patches Parquet field IDs** — `pkg/datagateway/server_v2_commit.go:56-151`. Path sources: `ws.partitionRouter.FilesWritten()` (each `pf.FileInfo.Path`), `ws.bufferMgr.FilesWritten()` (each `f.Path`), or `ws.externalFiles` (each `f.Path`).
3. **`writerState` (`server_v2.go:104-158`) has NO `writeMode` and NO `dataPaths` field.** It has: `writerID, outputName, bucket, table, basePath, bufferMgr, partitionRouter, writerBackend, externalFiles, schema, outputSchema, totalRows, totalBytes, tableExists, initMu`.
4. **Proto `TableCommitResult` enum is `STATUS_COMMITTED` / `STATUS_FAILED` / `STATUS_SKIPPED`**, error field is `Error` (NOT `ErrorMessage`), plus `FilesAdded int32`, `RowsAdded int64`, `BytesAdded int64`. The response is `CommitResponse{Success bool, Buckets []*BucketCommitResult}`, and `BucketCommitResult{Bucket, Status, Tables []*TableCommitResult}`.
5. **`Resolver` (`pkg/datagateway/lakekeeper/lakekeeper.go:68-92`) holds NO long-lived catalog.** It re-opens a `*catalogwriter.Client` per call via `r.newClient(ctx, token)`; the iceberg catalog is `cli.Catalog`. `Resolver.Close()` is a no-op. Therefore there is no shared mutable catalog to tear down — each commit can build its own short-lived client. The original plan's `r.cat` field does not exist.
6. **`catalogwriter.RetryOnConflict` ALREADY has exponential backoff (100ms base, ×2) with ±10% jitter and conflict-gating** — `pkg/catalogwriter/retry.go:66-107`. Line 97: `if !IsCommitConflict(err) { return err }` — non-conflict errors return immediately without retry. **Do NOT rewrite the backoff.** A non-conflict sentinel error (e.g. our idempotency-hit sentinel) is returned immediately, so wrapping it in the retry is safe.
7. **WriteMode constants** (`WriteModeAppend`, `WriteModeFullLoad`) currently live in `pkg/icebergjob/commit.go` (the orchestrator we delete in Task 8). They MUST be moved to `commit_shared.go` first (Task 1 Step 1).

---

## File structure

### Created
- `pkg/datagateway/commit_pool.go` — race-free worker pool, dispatch, barrier, idempotency-key compute.
- `pkg/datagateway/commit_pool_test.go` — pool unit tests.
- `pkg/datagateway/commit_metrics.go` — prometheus counters/histograms + typed error classifier.

### Modified
- `pkg/icebergjob/commit_shared.go` — move `WriteMode` consts here; add `CommitTableFiles` + idempotency-key check; refactor `CommitTable` to delegate.
- `pkg/icebergjob/commit_shared_test.go` — coverage for new behavior.
- `pkg/datagateway/lakekeeper/lakekeeper.go` — add `Catalog(ctx) (catalog.Catalog, error)` accessor.
- `pkg/datagateway/server_v2.go` — `writerState.committed` guard; `commitPool` field; init in constructor; cancel-then-nil in `Close`.
- `pkg/datagateway/server_v2_writing.go` — `CloseWriter` finalizes + dispatches; new `finalizeAndDispatch` helper.
- `pkg/datagateway/server_v2_commit.go` — `Commit` becomes defensive-sweep + barrier + reconciliation.
- `pkg/datagateway/cancel_watch.go` — cancel watcher cancels the pool.
- `pkg/k8s/controllers/pipelinerun_controller.go` — delete `startStageCommits`, `checkStageCommits`; stage `Running → Succeeded` directly.
- `pkg/k8s/controllers/pipelinerun_controller_test.go` — update transitions.
- `cmd/datuplet/main.go` — strip `iceberg-job --mode=table-commit` body.

### Deleted
- `pkg/k8s/controllers/pipelinerun_commit_jobs.go` + `_test.go`
- `pkg/icebergjob/commit.go` (orchestrator) + `commit_test.go` + `orchestrator_test.go`
- `cmd/datuplet/table_commit.go` + `cmd/datuplet/run_token_path_test.go`

---

## Task index (subagent-driven; one fresh subagent per task)

| # | Task | Model | Reason |
|---|------|-------|--------|
| 1 | Library: move `WriteMode`, add `CommitTableFiles` + idempotency-key | **sonnet** | iceberg-go semantics + retry-correctness. |
| 2 | Resolver: `Catalog(ctx)` accessor | **haiku** | Small, isolated. |
| 3 | DG commit pool (race-free; isolated, no integration) | **sonnet** | Concurrency correctness is load-bearing. |
| 4 | Writer-finalize refactor + dispatch + barrier (**riskiest**) | **sonnet** | Relocates 4 steps across CloseWriter/Commit; must read buffer/router Close semantics first. |
| 5 | DG cancel propagation + real test | **haiku** | Focused wire-up. |
| 6 | DG metrics + typed error classifier | **haiku** | Mechanical, but classifier must use typed checks. |
| 7 | Operator: delete commit-Job state machine (**coupled cutover with Task 4**) | **sonnet** | State-machine surgery. |
| 8 | Delete orchestrator + CLI subcommand | **haiku** | Mechanical deletes. |
| 9 | E2E verification on OrbStack | **opus** | Real-cluster debugging if it fails. |

Dispatch each as a fresh subagent. Between tasks the orchestrator runs `go build ./... && go test ./pkg/...` and reads the commit. **Tasks 4 and 7 must be reviewed together before either is considered done** (coupled cutover). Run Task 9 (E2E) immediately after Task 7.

---

## Task 1: Library — move `WriteMode`, add `CommitTableFiles` + idempotency-key

**Recommended model:** sonnet

**Files:**
- Modify: `pkg/icebergjob/commit_shared.go`
- Modify: `pkg/icebergjob/commit_shared_test.go`

**Goal:** Add `CommitTableFiles` (in-memory paths variant of `CommitTable`) with an idempotency-key check inside the existing `RetryOnConflict` envelope. Do NOT touch `RetryOnConflict` — it already has jitter + conflict-gating.

- [ ] **Step 1: Move `WriteMode` consts into `commit_shared.go`**

The consts currently live in `commit.go` (deleted in Task 8). Read `pkg/icebergjob/commit.go` to copy the exact definitions, then add to the top of `commit_shared.go` (after the imports):

```go
// WriteMode specifies how data is written to the table. Single source
// of truth here (moved from the deleted orchestrator in commit.go under
// RFC 021).
type WriteMode string

const (
    WriteModeAppend   WriteMode = "APPEND"
    WriteModeFullLoad WriteMode = "FULL_LOAD"
)
```

(If `commit.go` also defines a `WriteModeUpsert` placeholder, copy it too. Match exactly.)

- [ ] **Step 2: Write failing test for `CommitTableFiles`**

Read the top of `commit_shared_test.go` to learn the existing fake-catalog fixture (look for how `CommitTable` tests construct a `catalog.Catalog` and assert snapshot state). Reuse that fixture verbatim. Append:

```go
func TestCommitTableFiles_SucceedsWithInMemoryPaths(t *testing.T) {
    ctx := context.Background()
    cat := newFakeCatalog(t) // reuse the EXACT helper the existing tests use
    ident := icebergtable.Identifier{"ns1", "tbl1"}

    res, err := CommitTableFiles(ctx, cat, ident,
        []string{"s3://b/data/a.parquet", "s3://b/data/b.parquet"},
        WriteModeAppend, nil, "")
    if err != nil {
        t.Fatalf("CommitTableFiles: %v", err)
    }
    if res.DataFilesAdded != 2 {
        t.Errorf("DataFilesAdded = %d, want 2", res.DataFilesAdded)
    }
}
```

If the existing tests use a different fixture constructor name, use that. If there is NO reusable fake catalog, read `commit_shared_test.go` fully and replicate its setup pattern — do not invent a new mock framework.

- [ ] **Step 3: Run — expect compile failure**

Run: `go test ./pkg/icebergjob/ -run TestCommitTableFiles_ -v`
Expected: FAIL — `CommitTableFiles` undefined.

- [ ] **Step 4: Implement `CommitTableFiles` + refactor `CommitTable` to delegate**

In `commit_shared.go`, add:

```go
// errIdempotencyHit is an internal sentinel that short-circuits the
// RetryOnConflict envelope when a prior attempt's snapshot is found via
// commit-key. It is NOT a commit conflict, so RetryOnConflict
// (pkg/catalogwriter/retry.go:97 — `if !IsCommitConflict(err) { return
// err }`) returns it immediately without retrying. Do not change this
// coupling without updating the retry predicate.
var errIdempotencyHit = errors.New("icebergjob: idempotency hit")

// CommitTableFiles is the in-memory-paths variant of CommitTable: the
// caller supplies dataPaths directly instead of via a files.json URL.
// Used by Data Gateway's inline commit path (RFC 021).
//
// idempotencyKey, when non-empty, is (a) checked against existing
// snapshot summaries (key "datuplet.commit-key") on every attempt — a
// match short-circuits to success-zero (protects against
// committed-server-side-but-client-missed-the-response), and (b)
// written into the new snapshot summary. Callers must NOT also place
// "datuplet.commit-key" in snapshotProps.
func CommitTableFiles(
    ctx context.Context,
    cat catalog.Catalog,
    ident icebergtable.Identifier,
    dataPaths []string,
    mode WriteMode,
    snapshotProps iceberg.Properties,
    idempotencyKey string,
) (*CommitResult, error) {
    if len(dataPaths) == 0 {
        return &CommitResult{WriteMode: mode}, nil // success-zero
    }
    switch mode {
    case WriteModeAppend, WriteModeFullLoad:
    default:
        return nil, fmt.Errorf("CommitTableFiles: unsupported write mode %q", mode)
    }

    props := iceberg.Properties{}
    for k, v := range snapshotProps {
        if k == "datuplet.commit-key" {
            return nil, fmt.Errorf("CommitTableFiles: caller must not set snapshotProps[datuplet.commit-key]; pass idempotencyKey")
        }
        props[k] = v
    }
    if idempotencyKey != "" {
        props["datuplet.commit-key"] = idempotencyKey
    }

    tbl, err := cat.LoadTable(ctx, ident)
    if err != nil {
        return nil, fmt.Errorf("CommitTableFiles: load table %v: %w", ident, err)
    }
    result := CommitResult{WriteMode: mode, SnapshotIDBefore: snapshotIDOrEmpty(tbl), DataFilesAdded: len(dataPaths)}

    runErr := catalogwriter.RetryOnConflict(ctx, catalogwriter.RetryOpts{}, func(ctx context.Context) error {
        fresh, err := cat.LoadTable(ctx, ident)
        if err != nil {
            return err
        }
        if idempotencyKey != "" {
            if sid := findSnapshotByCommitKey(fresh, idempotencyKey); sid != "" {
                result.SnapshotIDAfter = sid
                return errIdempotencyHit
            }
        }
        txn := fresh.NewTransaction()
        switch mode {
        case WriteModeAppend:
            if err := txn.AddFiles(ctx, dataPaths, props, false); err != nil {
                return err
            }
        case WriteModeFullLoad:
            oldPaths, err := listCurrentSnapshotFilePaths(ctx, fresh)
            if err != nil {
                return fmt.Errorf("list current snapshot files: %w", err)
            }
            if err := txn.ReplaceDataFiles(ctx, oldPaths, dataPaths, props); err != nil {
                return err
            }
        }
        committed, err := txn.Commit(ctx)
        if err != nil {
            return err
        }
        result.SnapshotIDAfter = snapshotIDOrEmpty(committed)
        return nil
    })
    if runErr != nil {
        if errors.Is(runErr, errIdempotencyHit) {
            return &result, nil // success-zero, SnapshotIDAfter populated
        }
        return nil, runErr
    }
    return &result, nil
}

// findSnapshotByCommitKey scans ALL snapshots newest-to-oldest for a
// matching "datuplet.commit-key" summary value. Returns the snapshot ID
// (decimal string) or "". Full scan — no windowing. POC tables have few
// snapshots; if this ever becomes hot, bound it then.
func findSnapshotByCommitKey(tbl *icebergtable.Table, key string) string {
    snaps := tbl.Metadata().Snapshots()
    for i := len(snaps) - 1; i >= 0; i-- {
        if snaps[i].Summary != nil && snaps[i].Summary["datuplet.commit-key"] == key {
            return fmt.Sprintf("%d", snaps[i].SnapshotID)
        }
    }
    return ""
}
```

Then replace `CommitTable`'s body so it reads the manifest and delegates:

```go
func CommitTable(
    ctx context.Context,
    cat catalog.Catalog,
    ident icebergtable.Identifier,
    manifestPath string,
    mode WriteMode,
    snapshotProps iceberg.Properties,
) (*CommitResult, error) {
    tbl, err := cat.LoadTable(ctx, ident)
    if err != nil {
        return nil, fmt.Errorf("CommitTable: load table %v: %w", ident, err)
    }
    m, err := readManifestFromTableFS(ctx, tbl, manifestPath)
    if errors.Is(err, ErrManifestMissing) || errors.Is(err, ErrManifestEmpty) {
        return &CommitResult{WriteMode: mode}, nil
    }
    if err != nil {
        return nil, fmt.Errorf("CommitTable: read manifest %s: %w", manifestPath, err)
    }
    return CommitTableFiles(ctx, cat, ident, m.DataPaths, mode, snapshotProps, "")
}
```

Verify `tbl.Metadata().Snapshots()`, `snap.Summary`, and `snap.SnapshotID` are the correct iceberg-go accessors by reading how the existing code reads snapshots (grep `Metadata().` and `Snapshots()` in `pkg/icebergjob/` and `pkg/catalogwriter/`). Adjust the accessor names to match the iceberg-go version in `go.mod`.

- [ ] **Step 5: Run the success test**

Run: `go test ./pkg/icebergjob/ -run TestCommitTableFiles_Succeeds -v`
Expected: PASS.

- [ ] **Step 6: Add idempotency-hit test**

Append to `commit_shared_test.go`:

```go
func TestCommitTableFiles_IdempotencyHit(t *testing.T) {
    ctx := context.Background()
    cat := newFakeCatalog(t)
    ident := icebergtable.Identifier{"ns1", "tbl1"}
    key := "test-key-abc"

    if _, err := CommitTableFiles(ctx, cat, ident,
        []string{"s3://b/data/a.parquet"}, WriteModeAppend, nil, key); err != nil {
        t.Fatalf("first commit: %v", err)
    }
    res, err := CommitTableFiles(ctx, cat, ident,
        []string{"s3://b/data/SHOULD_NOT_BE_ADDED.parquet"}, WriteModeAppend, nil, key)
    if err != nil {
        t.Fatalf("second commit: %v", err)
    }
    if res.SnapshotIDAfter == "" {
        t.Error("expected SnapshotIDAfter populated on idempotency hit")
    }

    tbl, err := cat.LoadTable(ctx, ident)
    if err != nil {
        t.Fatal(err)
    }
    files, err := listCurrentSnapshotFilePaths(ctx, tbl)
    if err != nil {
        t.Fatal(err)
    }
    if len(files) != 1 {
        t.Errorf("table has %d files, want 1 — second commit must be skipped", len(files))
    }
}
```

If the fake catalog cannot record/read snapshot summaries, extend it minimally (add a `Summary map[string]string` to the fake snapshot and have `Commit` persist `snapshotProps` into it). Keep the extension as small as possible and comment it.

Run: `go test ./pkg/icebergjob/ -run TestCommitTableFiles_IdempotencyHit -v`
Expected: PASS.

- [ ] **Step 7: Full library suite + vet + commit**

```
go build ./...
go vet ./pkg/icebergjob/
go test ./pkg/icebergjob/
```
Expected: green.

```bash
git add pkg/icebergjob/commit_shared.go pkg/icebergjob/commit_shared_test.go
git commit -m "icebergjob: move WriteMode + add CommitTableFiles with idempotency key (RFC 021)"
```

---

## Task 2: Resolver — `Catalog(ctx)` accessor

**Recommended model:** haiku

**Files:**
- Modify: `pkg/datagateway/lakekeeper/lakekeeper.go`

**Goal:** Expose a way for the commit pool to obtain an iceberg catalog using the run token. Mirrors the per-call client pattern the resolver already uses.

- [ ] **Step 1: Read `newClient` + `LoadTableForRead` to confirm the pattern**

Run: `grep -n "func (r \*Resolver) newClient\|cli.Catalog\|LoadTableForRead" pkg/datagateway/lakekeeper/lakekeeper.go`
Confirm `newClient(ctx, token) (*catalogwriter.Client, error)` and that `cli.Catalog` is a `catalog.Catalog`.

- [ ] **Step 2: Add the accessor**

Append to `lakekeeper.go`:

```go
// Catalog returns an iceberg catalog handle bound to the resolver's run
// token. A fresh *catalogwriter.Client is opened per call (same pattern
// as LoadTableForRead / LoadOrCreateForWrite) — there is no shared
// catalog state, so no lifecycle coupling with concurrent callers.
// Used by the inline commit pool (RFC 021).
func (r *Resolver) Catalog(ctx context.Context) (catalog.Catalog, error) {
    cli, err := r.newClient(ctx, r.Token)
    if err != nil {
        return nil, fmt.Errorf("lakekeeper: open catalog client: %w", err)
    }
    return cli.Catalog, nil
}
```

Add `"fmt"` to imports if not already present. Confirm `catalog` is imported (it is — used at line 42).

- [ ] **Step 3: Build + commit**

```
go build ./...
go vet ./pkg/datagateway/lakekeeper/
```
Expected: clean.

```bash
git add pkg/datagateway/lakekeeper/lakekeeper.go
git commit -m "lakekeeper: add Resolver.Catalog accessor for inline commit (RFC 021)"
```

---

## Task 3: DG commit pool (race-free, isolated)

**Recommended model:** sonnet

**Files:**
- Create: `pkg/datagateway/commit_pool.go`
- Create: `pkg/datagateway/commit_pool_test.go`

**Goal:** A bounded pool with **no channel-close barrier** (that pattern panics on concurrent dispatch / double-wait). Concurrency is limited by a semaphore; the barrier is a `sync.WaitGroup`; the queue bound is an integer counter under a mutex. Contract: **`Dispatch` must not be called concurrently with `Wait`** (the SDK guarantees this — all `CloseWriter`s precede the session `Commit`).

- [ ] **Step 1: Write the failing tests**

Create `pkg/datagateway/commit_pool_test.go`:

```go
package datagateway

import (
    "context"
    "errors"
    "fmt"
    "sync/atomic"
    "testing"

    "github.com/apache/iceberg-go/catalog"
    icebergtable "github.com/apache/iceberg-go/table"

    "github.com/datuplet/datuplet/pkg/icebergjob"
)

func okCatalogFn(context.Context) (catalog.Catalog, error) { return nil, nil }

func TestCommitPool_DispatchAndBarrier(t *testing.T) {
    var n atomic.Int32
    pool := NewCommitPool(CommitPoolConfig{
        Workers: 4, MaxQueueSize: 16, CatalogFn: okCatalogFn,
        CommitFn: func(_ context.Context, _ catalog.Catalog, _ icebergtable.Identifier,
            paths []string, mode icebergjob.WriteMode, _ string) (*icebergjob.CommitResult, error) {
            n.Add(1)
            return &icebergjob.CommitResult{DataFilesAdded: len(paths), WriteMode: mode}, nil
        },
    })
    defer pool.Cancel()
    for i := 0; i < 5; i++ {
        if err := pool.Dispatch(context.Background(), CommitJob{
            WriterID: fmt.Sprintf("w%d", i), Namespace: "ns", Table: fmt.Sprintf("t%d", i),
            DataPaths: []string{"p"}, Mode: icebergjob.WriteModeAppend, RunID: "r",
        }); err != nil {
            t.Fatalf("dispatch %d: %v", i, err)
        }
    }
    res := pool.Wait(context.Background())
    if len(res) != 5 || n.Load() != 5 {
        t.Fatalf("results=%d commits=%d, want 5/5", len(res), n.Load())
    }
}

func TestCommitPool_QueueOverflow(t *testing.T) {
    block := make(chan struct{})
    pool := NewCommitPool(CommitPoolConfig{
        Workers: 1, MaxQueueSize: 2, CatalogFn: okCatalogFn,
        CommitFn: func(context.Context, catalog.Catalog, icebergtable.Identifier,
            []string, icebergjob.WriteMode, string) (*icebergjob.CommitResult, error) {
            <-block
            return &icebergjob.CommitResult{}, nil
        },
    })
    defer func() { close(block); pool.Cancel() }()
    for i := 0; i < 2; i++ {
        if err := pool.Dispatch(context.Background(), CommitJob{
            WriterID: fmt.Sprintf("w%d", i), Namespace: "ns", Table: "t",
            DataPaths: []string{"p"}, Mode: icebergjob.WriteModeAppend, RunID: "r",
        }); err != nil {
            t.Fatalf("dispatch %d: %v", i, err)
        }
    }
    err := pool.Dispatch(context.Background(), CommitJob{
        WriterID: "of", Namespace: "ns", Table: "t",
        DataPaths: []string{"p"}, Mode: icebergjob.WriteModeAppend, RunID: "r",
    })
    if !errors.Is(err, ErrCommitQueueFull) {
        t.Errorf("want ErrCommitQueueFull, got %v", err)
    }
}

func TestCommitPool_PartialFailureAggregated(t *testing.T) {
    pool := NewCommitPool(CommitPoolConfig{
        Workers: 2, MaxQueueSize: 8, CatalogFn: okCatalogFn,
        CommitFn: func(_ context.Context, _ catalog.Catalog, ident icebergtable.Identifier,
            _ []string, _ icebergjob.WriteMode, _ string) (*icebergjob.CommitResult, error) {
            if ident[1] == "fail" {
                return nil, errors.New("synthetic")
            }
            return &icebergjob.CommitResult{}, nil
        },
    })
    defer pool.Cancel()
    _ = pool.Dispatch(context.Background(), CommitJob{WriterID: "ok", Namespace: "ns", Table: "ok", DataPaths: []string{"p"}, Mode: icebergjob.WriteModeAppend, RunID: "r"})
    _ = pool.Dispatch(context.Background(), CommitJob{WriterID: "bad", Namespace: "ns", Table: "fail", DataPaths: []string{"p"}, Mode: icebergjob.WriteModeAppend, RunID: "r"})
    var ok, fail int
    for _, r := range pool.Wait(context.Background()) {
        if r.Err != nil { fail++ } else { ok++ }
    }
    if ok != 1 || fail != 1 {
        t.Errorf("ok=%d fail=%d, want 1/1", ok, fail)
    }
}

func TestCommitPool_CancelUnblocksInFlight(t *testing.T) {
    started := make(chan struct{}, 2)
    pool := NewCommitPool(CommitPoolConfig{
        Workers: 2, MaxQueueSize: 4, CatalogFn: okCatalogFn,
        CommitFn: func(ctx context.Context, _ catalog.Catalog, _ icebergtable.Identifier,
            _ []string, _ icebergjob.WriteMode, _ string) (*icebergjob.CommitResult, error) {
            started <- struct{}{}
            <-ctx.Done()
            return nil, ctx.Err()
        },
    })
    for i := 0; i < 2; i++ {
        _ = pool.Dispatch(context.Background(), CommitJob{WriterID: fmt.Sprintf("w%d", i), Namespace: "ns", Table: "t", DataPaths: []string{"p"}, Mode: icebergjob.WriteModeAppend, RunID: "r"})
    }
    <-started; <-started
    pool.Cancel()
    for _, r := range pool.Wait(context.Background()) {
        if r.Err == nil {
            t.Errorf("cancelled job %s reported no error", r.WriterID)
        }
    }
}

func TestCommitPool_DispatchAfterCancel(t *testing.T) {
    pool := NewCommitPool(CommitPoolConfig{Workers: 1, MaxQueueSize: 4, CatalogFn: okCatalogFn,
        CommitFn: func(context.Context, catalog.Catalog, icebergtable.Identifier, []string, icebergjob.WriteMode, string) (*icebergjob.CommitResult, error) {
            return &icebergjob.CommitResult{}, nil
        }})
    pool.Cancel()
    if err := pool.Dispatch(context.Background(), CommitJob{WriterID: "x", Namespace: "ns", Table: "t", DataPaths: []string{"p"}, Mode: icebergjob.WriteModeAppend, RunID: "r"}); err == nil {
        t.Error("Dispatch after Cancel must error")
    }
}

func TestComputeIdempotencyKey_Stable(t *testing.T) {
    k1 := ComputeIdempotencyKey("r", "ns", "t", []string{"s3://b/b.parquet", "s3://b/a.parquet"})
    k2 := ComputeIdempotencyKey("r", "ns", "t", []string{"s3://b/a.parquet", "s3://b/b.parquet"})
    if k1 != k2 {
        t.Errorf("not order-stable: %q vs %q", k1, k2)
    }
    if len(k1) != 64 {
        t.Errorf("len=%d want 64", len(k1))
    }
    if k1 == ComputeIdempotencyKey("r2", "ns", "t", []string{"s3://b/a.parquet", "s3://b/b.parquet"}) {
        t.Error("collision across run IDs")
    }
    // Separator-injection guard: these must NOT collide.
    ka := ComputeIdempotencyKey("r", "ns", "t", []string{"a", "b|c"})
    kb := ComputeIdempotencyKey("r", "ns", "t", []string{"a|b", "c"})
    if ka == kb {
        t.Error("path separator ambiguity: distinct path sets collided")
    }
}
```

Run: `go test ./pkg/datagateway/ -run "TestCommitPool_|TestComputeIdempotencyKey_" -v`
Expected: compile FAIL.

- [ ] **Step 2: Implement the pool**

Create `pkg/datagateway/commit_pool.go`:

```go
// Package datagateway — inline iceberg-commit worker pool (RFC 021).
//
// Concurrency model: a buffered semaphore caps simultaneous commits at
// Workers; a WaitGroup is the session barrier; an int counter under mu
// bounds queued+in-flight work at MaxQueueSize. There is NO shared
// channel closed as a barrier (that pattern panics on concurrent
// Dispatch / double Wait). CONTRACT: Dispatch must not run concurrently
// with Wait — the SDK calls all CloseWriters before the session Commit.
package datagateway

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "errors"
    "fmt"
    "sort"
    "strings"
    "sync"
    "time"

    "github.com/apache/iceberg-go/catalog"
    icebergtable "github.com/apache/iceberg-go/table"

    "github.com/datuplet/datuplet/pkg/icebergjob"
)

// ErrCommitQueueFull is returned by Dispatch when queued+in-flight work
// is at MaxQueueSize. CloseWriter must surface this to the SDK.
var ErrCommitQueueFull = errors.New("commit pool: queue full")

type CommitJob struct {
    WriterID, Namespace, Table, RunID string
    DataPaths                         []string
    Mode                              icebergjob.WriteMode
}

type CommitResult struct {
    WriterID, Namespace, Table       string
    Err                              error
    SnapshotIDBefore, SnapshotIDAfter string
    DataFilesAdded                   int
    IdempotencyKey                   string
    Duration                         time.Duration
}

// CommitFunc is the test seam (prod adapter calls icebergjob.CommitTableFiles).
type CommitFunc func(ctx context.Context, cat catalog.Catalog,
    ident icebergtable.Identifier, paths []string,
    mode icebergjob.WriteMode, idempotencyKey string) (*icebergjob.CommitResult, error)

type CommitPoolConfig struct {
    Workers      int
    MaxQueueSize int
    CatalogFn    func(ctx context.Context) (catalog.Catalog, error)
    CommitFn     CommitFunc
}

type CommitPool struct {
    cfg       CommitPoolConfig
    parentCtx context.Context
    cancel    context.CancelFunc
    sem       chan struct{}
    wg        sync.WaitGroup
    mu        sync.Mutex
    inflight  int
    closed    bool
    resultsMu sync.Mutex
    results   []CommitResult
}

func NewCommitPool(cfg CommitPoolConfig) *CommitPool {
    if cfg.Workers <= 0 {
        cfg.Workers = 4
    }
    if cfg.MaxQueueSize <= 0 {
        cfg.MaxQueueSize = 256
    }
    ctx, cancel := context.WithCancel(context.Background())
    return &CommitPool{
        cfg: cfg, parentCtx: ctx, cancel: cancel,
        sem: make(chan struct{}, cfg.Workers),
    }
}

// Dispatch enqueues a job. Non-blocking; returns ErrCommitQueueFull at
// capacity, or the parent ctx error after Cancel.
func (p *CommitPool) Dispatch(ctx context.Context, j CommitJob) error {
    p.mu.Lock()
    if p.closed {
        p.mu.Unlock()
        return fmt.Errorf("commit pool: closed: %w", p.parentCtx.Err())
    }
    if p.inflight >= p.cfg.MaxQueueSize {
        p.mu.Unlock()
        return fmt.Errorf("%w (max %d)", ErrCommitQueueFull, p.cfg.MaxQueueSize)
    }
    p.inflight++
    p.wg.Add(1)
    p.mu.Unlock()

    go func() {
        defer func() {
            p.mu.Lock()
            p.inflight--
            p.mu.Unlock()
            p.wg.Done()
        }()
        select {
        case p.sem <- struct{}{}:
            defer func() { <-p.sem }()
        case <-p.parentCtx.Done():
            p.record(CommitResult{WriterID: j.WriterID, Namespace: j.Namespace, Table: j.Table, Err: p.parentCtx.Err()})
            return
        }
        p.run(j)
    }()
    return nil
}

func (p *CommitPool) run(j CommitJob) {
    start := time.Now()
    key := ComputeIdempotencyKey(j.RunID, j.Namespace, j.Table, j.DataPaths)
    cr := CommitResult{WriterID: j.WriterID, Namespace: j.Namespace, Table: j.Table, IdempotencyKey: key}

    cat, err := p.cfg.CatalogFn(p.parentCtx)
    if err != nil {
        cr.Err = err
        cr.Duration = time.Since(start)
        p.record(cr)
        return
    }
    res, err := p.cfg.CommitFn(p.parentCtx, cat, icebergtable.Identifier{j.Namespace, j.Table}, j.DataPaths, j.Mode, key)
    cr.Duration = time.Since(start)
    if err != nil {
        cr.Err = err
    } else if res != nil {
        cr.SnapshotIDBefore = res.SnapshotIDBefore
        cr.SnapshotIDAfter = res.SnapshotIDAfter
        cr.DataFilesAdded = res.DataFilesAdded
    }
    p.record(cr)
}

func (p *CommitPool) record(cr CommitResult) {
    p.resultsMu.Lock()
    p.results = append(p.results, cr)
    p.resultsMu.Unlock()
}

// Wait blocks until all dispatched jobs finish OR ctx is cancelled,
// then returns and clears the result list. Reusable across sessions.
// Safe to call twice. Must not run concurrently with Dispatch.
//
// Honoring ctx matters for the bounded shutdown drain in
// ServerV2.Close() (N2): a commit wedged despite Cancel must not hang
// process teardown forever. On ctx timeout, Wait returns whatever
// results were recorded so far (in-flight jobs keep running in the
// background but no longer block the caller).
func (p *CommitPool) Wait(ctx context.Context) []CommitResult {
    done := make(chan struct{})
    go func() { p.wg.Wait(); close(done) }()
    select {
    case <-done:
    case <-ctx.Done():
    }
    p.resultsMu.Lock()
    out := p.results
    p.results = nil
    p.resultsMu.Unlock()
    return out
}

// Cancel terminates the pool: in-flight jobs see ctx cancellation, new
// Dispatch errors. Terminal — used at shutdown and on cancel-annotation.
func (p *CommitPool) Cancel() {
    p.mu.Lock()
    p.closed = true
    p.mu.Unlock()
    p.cancel()
}

// ComputeIdempotencyKey: hex-sha256 of (runID, ns.table, sorted-paths),
// NUL-separated so distinct path sets cannot collide via separator
// ambiguity.
func ComputeIdempotencyKey(runID, namespace, table string, paths []string) string {
    sorted := append([]string(nil), paths...)
    sort.Strings(sorted)
    h := sha256.New()
    h.Write([]byte(runID))
    h.Write([]byte{0})
    h.Write([]byte(namespace))
    h.Write([]byte{0})
    h.Write([]byte(table))
    h.Write([]byte{0})
    for _, p := range sorted {
        h.Write([]byte(p))
        h.Write([]byte{0})
    }
    return hex.EncodeToString(h.Sum(nil))
}

var _ = strings.Join // retained import guard if a future helper needs it
```

(If `strings` ends up unused, drop the import and the guard line.)

- [ ] **Step 3: Run pool tests (with the race detector)**

Run: `go test -race ./pkg/datagateway/ -run "TestCommitPool_|TestComputeIdempotencyKey_" -v`
Expected: all PASS, no race warnings.

- [ ] **Step 4: Commit**

```bash
git add pkg/datagateway/commit_pool.go pkg/datagateway/commit_pool_test.go
git commit -m "datagateway: race-free inline commit worker pool (RFC 021)"
```

---

## Task 4: Writer-finalize refactor + dispatch + barrier (riskiest — coupled with Task 7)

**Recommended model:** sonnet

**Files:**
- Modify: `pkg/datagateway/server_v2.go` (add `committed bool` to `writerState`; add `commitPool` field; init + teardown)
- Modify: `pkg/datagateway/server_v2_writing.go` (`finalizeAndDispatch` helper; call from `CloseWriter`)
- Modify: `pkg/datagateway/server_v2_commit.go` (`Commit` = defensive sweep + barrier + reconciliation)

**Goal:** Relocate path-collection / `files.json` / schema-manifest / Parquet field-ID patching so per-table iceberg commit is dispatched right after a writer's buffer is closed, and `Commit` becomes the drain barrier. Eliminate the current double-close.

**Before writing code — read these to learn exact semantics:**
- `pkg/datagateway/server_v2_commit.go:40-151` — the existing per-writer finalize loop (path collection, `writeSchemaAndManifest`, `patchParquetFieldIDs`, status assignment).
- `pkg/datagateway/server_v2_writing.go:277-299` — `CloseWriter`'s existing buffer close.
- Whether `bufferMgr.Close()` / `partitionRouter.Close()` are idempotent. If NOT, the `committed` guard below is what prevents the double-close.

- [ ] **Step 1: Add `committed` guard + `commitPool` field**

In `server_v2.go`, add to `writerState`:

```go
// committed is set once finalizeAndDispatch has CLAIMED this writer, so
// a later defensive sweep in Commit() does not double-process it. Set
// under s.mu.
committed bool
// closed is set the first time this writer's buffer/router is closed
// (by CloseWriter). The Commit defensive sweep checks it to avoid a
// second Close() call (buffer/router Close is not guaranteed
// idempotent). Set under s.mu.
closed bool
```

Add to `ServerV2` struct:

```go
commitPool *CommitPool
```

In the constructor (`grep -n "func New" server_v2.go` — find the one returning `*ServerV2`), after `s.lakekeeperResolver` is assigned:

```go
if s.lakekeeperResolver != nil {
    s.commitPool = NewCommitPool(CommitPoolConfig{
        Workers:      4,   // RFC 021 open-question 10.1: make env-configurable later
        MaxQueueSize: 256,
        CatalogFn:    s.lakekeeperResolver.Catalog,
        CommitFn: func(ctx context.Context, cat catalog.Catalog,
            ident icebergtable.Identifier, paths []string,
            mode icebergjob.WriteMode, key string) (*icebergjob.CommitResult, error) {
            return icebergjob.CommitTableFiles(ctx, cat, ident, paths, mode, nil, key)
        },
    })
}
```

In `Close()` — **cancel the pool BEFORE any resolver teardown**, and wait for drain with a bounded deadline so a wedged commit can't hang shutdown forever (N2). `Cancel()` already flips the pool's `closed` flag so no new `Dispatch` is accepted; ensure the gRPC server has stopped accepting calls before `Close()` runs (it does — `Close()` is the teardown path):

```go
if s.commitPool != nil {
    s.commitPool.Cancel()
    drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    s.commitPool.Wait(drainCtx) // bounded drain; Wait honors the deadline (see note)
    cancel()
}
```

Note: `CommitPool.Wait` in Task 3 currently ignores its `ctx`. Extend it here to honor a deadline: race `wg.Wait()` (via a `done` channel closed by a helper goroutine) against `<-ctx.Done()`, returning whatever results are recorded so far on timeout. Add a one-line test `TestCommitPool_WaitRespectsDeadline` in `commit_pool_test.go` (dispatch a job whose `CommitFn` blocks on a never-closed channel; assert `Wait` returns within ~the deadline rather than blocking forever).

Add imports: `"github.com/apache/iceberg-go/catalog"`, `icebergtable "github.com/apache/iceberg-go/table"`, `"github.com/datuplet/datuplet/pkg/icebergjob"`.

- [ ] **Step 2: Write `finalizeAndDispatch` helper**

In `server_v2_writing.go`, add a helper that takes a writer already past buffer-close and does: collect paths, patch field IDs (external only), write schema/manifest + `files.json` breadcrumb, dispatch the commit.

**Lock discipline (N1 — do NOT hold `s.mu` across object storage I/O).** The helper takes `s.mu` ONLY briefly to claim the writer (`committed`), then releases it before any backend write or dispatch. Holding the server-wide mutex across GCS/S3 writes would serialize every concurrent gRPC call (`OpenWriter`/`WriteChunk`/`CloseWriter` for *other* writers) for the duration of a network write. **Callers must NOT hold `s.mu` when calling this helper.** Safety: once `committed` is set under the lock, no other goroutine processes this writer (CloseWriter for a given writerID is single-shot; the Commit sweep skips committed writers), so the post-claim I/O on `ws` is race-free.

```go
// finalizeAndDispatch collects a closed writer's parquet paths, writes
// the schema/manifest + files.json breadcrumb, then dispatches the
// iceberg commit to the pool. The writer's buffer/router must already
// be closed (CloseWriter does this). Claims the writer under s.mu (sets
// ws.committed) then releases the lock for all I/O.
//
// MUST be called WITHOUT s.mu held. No-op (returns nil) when the writer
// is already claimed. When s.commitPool is nil (test mode without a
// resolver) the writer is still claimed + metadata written, but no
// commit is dispatched.
func (s *ServerV2) finalizeAndDispatch(ctx context.Context, ws *writerState, runID string) error {
    // Claim under the lock so the Commit defensive sweep cannot double-
    // process this writer. Everything after the Unlock is lock-free I/O.
    s.mu.Lock()
    if ws.committed {
        s.mu.Unlock()
        return nil
    }
    ws.committed = true
    s.mu.Unlock()

    // Collect paths from the same three sources the old Commit() loop used.
    var paths []string
    switch {
    case ws.partitionRouter != nil:
        for _, pf := range ws.partitionRouter.FilesWritten() {
            paths = append(paths, pf.FileInfo.Path)
        }
    case ws.bufferMgr != nil:
        for _, f := range ws.bufferMgr.FilesWritten() {
            paths = append(paths, f.Path)
        }
    case len(ws.externalFiles) > 0:
        for _, f := range ws.externalFiles {
            paths = append(paths, f.Path)
        }
    }

    // External files need Iceberg field-ID patching before AddFiles.
    if len(ws.externalFiles) > 0 {
        if ws.schema == nil {
            return fmt.Errorf("external files but no schema for %s.%s", ws.bucket, ws.table)
        }
        if err := s.patchParquetFieldIDs(ctx, ws); err != nil {
            return fmt.Errorf("patch parquet field IDs %s.%s: %w", ws.bucket, ws.table, err)
        }
    }

    // Schema/manifest + files.json breadcrumb (debug only; not read at
    // runtime anymore). Best-effort for buffered flow, fatal for external
    // (matches prior behavior).
    if ws.schema != nil {
        if err := s.writeSchemaAndManifest(ctx, ws, runID); err != nil {
            if len(ws.externalFiles) > 0 {
                return fmt.Errorf("write manifest %s.%s: %w", ws.bucket, ws.table, err)
            }
            log.Printf("Warning: schema/manifest write failed for %s.%s: %v", ws.bucket, ws.table, err)
        }
    }
    if ws.writerBackend != nil {
        // files.json breadcrumb via the existing per-table persist path.
        for _, p := range paths {
            s.filesManifest.Append(ws.bucket, ws.table, p)
        }
        if err := s.persistTableManifest(ctx, ws.writerBackend, ws.basePath, ws.bucket, ws.table, runID); err != nil {
            log.Printf("Warning: files.json breadcrumb write failed for %s.%s: %v", ws.bucket, ws.table, err)
        }
    }

    if s.commitPool == nil {
        return nil // test mode — no commit
    }
    if len(paths) == 0 {
        return nil // nothing to commit (SKIPPED)
    }
    mode := icebergjob.WriteModeAppend
    if m := s.writeModeForTable(ws.bucket, ws.table); m != "" {
        mode = m
    }
    if err := s.commitPool.Dispatch(ctx, CommitJob{
        WriterID:  ws.writerID,
        Namespace: ws.bucket,
        Table:     ws.table,
        DataPaths: paths,
        Mode:      mode,
        RunID:     runID,
    }); err != nil {
        return fmt.Errorf("dispatch commit %s.%s: %w", ws.bucket, ws.table, err)
    }
    log.Printf("dispatched inline commit: writer=%s table=%s.%s files=%d mode=%s",
        ws.writerID, ws.bucket, ws.table, len(paths), mode)
    return nil
}
```

**Write-mode source:** the old commit Job derived mode per bucket from the Pipeline CRD. DG has the output config in `s.config.OutputTables` (used in `OpenWriter`). Add a small helper `writeModeForTable(bucket, table) icebergjob.WriteMode` that maps a matching entry's write-mode string (`"FULL_LOAD"` → `WriteModeFullLoad`, else `WriteModeAppend`). Read the `OutputConfig` / `TableOutputConfig` proto fields (`api/proto/gateway/v2/gateway.proto:111-141`) for exact names. **N6 — match on BOTH `bucket` AND `table`, never table name alone.** `FULL_LOAD` triggers destructive `ReplaceDataFiles`; matching by table name only would let a same-named table in another bucket wrongly select `FULL_LOAD` and overwrite the wrong table. If the config carries no matching `(bucket, table)` entry or no per-entry mode, default APPEND. Add a unit test `TestWriteModeForTable_BucketScoped` asserting that two tables with the same name in different buckets resolve to their own modes (one APPEND, one FULL_LOAD).

- [ ] **Step 3: Call from `CloseWriter`**

In `CloseWriter`, mark the writer closed once its buffer/router close succeeds, then call the helper. The helper manages its own short lock for the claim (N1), so do NOT wrap it in `s.mu` here.

First, in the standard branch right after a successful `ws.partitionRouter.Close()` / `ws.bufferMgr.Close()`, set the `closed` flag under the lock:

```go
s.mu.Lock()
ws.closed = true
s.mu.Unlock()
```

(The external-files branch writes no buffer; leave `closed` false — the sweep's close-guard keys off it but external writers have no buffer to close, so the sweep skips closing them anyway via the `partitionRouter == nil && bufferMgr == nil` shape.)

Then, before each branch's `return` (both the external-files early return and the standard return), dispatch:

```go
// RFC 021: finalize + dispatch the per-table iceberg commit now that
// the buffer is flushed (or external files are registered). The helper
// claims the writer under s.mu internally, then does I/O lock-free.
if err := s.finalizeAndDispatch(ctx, ws, s.config.GetRunID()); err != nil {
    return nil, err
}
```

Place the dispatch call in BOTH branches (before their respective `return`). Do not double-call — each branch returns independently.

- [ ] **Step 4: Rewrite `Commit` as defensive-sweep + barrier + reconciliation**

Replace the `Commit` body in `server_v2_commit.go`. The new flow:

```go
func (s *ServerV2) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
    runID := s.config.GetRunID()

    // Snapshot the writer set under the lock, then do all close + finalize
    // I/O lock-free (N1: never hold s.mu across storage writes;
    // finalizeAndDispatch self-locks only for its claim — calling it while
    // holding s.mu would DEADLOCK).
    s.mu.Lock()
    expected := make(map[string]*writerState, len(s.writers))
    sweepList := make([]*writerState, 0, len(s.writers))
    for id, ws := range s.writers {
        expected[id] = ws
        if !ws.committed {
            sweepList = append(sweepList, ws)
        }
    }
    s.mu.Unlock()

    // Defensive sweep: any writer opened but never CloseWriter'd (SDK
    // called Commit without per-writer Close). Close its buffer first
    // (CloseWriter didn't), guarded by ws.closed so we never double-close
    // (buffer/router Close is not guaranteed idempotent — N4). These
    // writers have no concurrent accessor, so lock-free close is safe.
    var sweepErr error
    for _, ws := range sweepList {
        s.mu.Lock()
        needClose := !ws.closed && (ws.partitionRouter != nil || ws.bufferMgr != nil)
        if needClose {
            ws.closed = true // claim the close
        }
        s.mu.Unlock()
        if needClose {
            var cerr error
            if ws.partitionRouter != nil {
                cerr = ws.partitionRouter.Close()
            } else if ws.bufferMgr != nil {
                cerr = ws.bufferMgr.Close()
            }
            if cerr != nil {
                if sweepErr == nil {
                    sweepErr = fmt.Errorf("sweep close %s.%s: %w", ws.bucket, ws.table, cerr)
                }
                continue // don't dispatch a writer whose flush failed
            }
        }
        if err := s.finalizeAndDispatch(ctx, ws, runID); err != nil && sweepErr == nil {
            sweepErr = err
        }
    }

    // Barrier: drain the pool.
    var poolResults []CommitResult
    if s.commitPool != nil {
        poolResults = s.commitPool.Wait(ctx)
    }

    // Fold pool results into per-bucket table results.
    bucketTables := make(map[string][]*pb.TableCommitResult)
    seen := make(map[string]bool)
    allSuccess := sweepErr == nil
    for _, r := range poolResults {
        seen[r.WriterID] = true
        tcr := &pb.TableCommitResult{Table: r.Table, Bucket: r.Namespace}
        if r.Err != nil {
            tcr.Status = pb.TableCommitResult_STATUS_FAILED
            tcr.Error = r.Err.Error()
            allSuccess = false
        } else if r.DataFilesAdded == 0 && r.SnapshotIDAfter == "" {
            tcr.Status = pb.TableCommitResult_STATUS_SKIPPED
        } else {
            tcr.Status = pb.TableCommitResult_STATUS_COMMITTED
            tcr.FilesAdded = int32(r.DataFilesAdded)
        }
        bucketTables[r.Namespace] = append(bucketTables[r.Namespace], tcr)
    }

    // Reconciliation (the L-fix): every expected writer that produced
    // paths must have a pool result. A writer with no result + no
    // SKIPPED entry means a commit was silently dropped — fail loudly
    // rather than report a false success.
    //
    // N3: in test mode (s.commitPool == nil) NO commit is ever
    // dispatched, so a produced-files writer legitimately has no pool
    // result. Report it COMMITTED in that mode rather than FAILED — the
    // reconciliation-failure path applies only when a pool exists.
    for id, ws := range expected {
        if seen[id] {
            continue
        }
        switch {
        case !writerProducedFiles(ws):
            // Genuine no-data writer.
            bucketTables[ws.bucket] = append(bucketTables[ws.bucket], &pb.TableCommitResult{
                Table: ws.table, Bucket: ws.bucket, Status: pb.TableCommitResult_STATUS_SKIPPED,
            })
        case s.commitPool == nil:
            // Test mode: produced files, no pool, no dispatch by design.
            bucketTables[ws.bucket] = append(bucketTables[ws.bucket], &pb.TableCommitResult{
                Table: ws.table, Bucket: ws.bucket, Status: pb.TableCommitResult_STATUS_COMMITTED,
            })
        default:
            // Pool exists but produced-files writer has no result → a
            // commit was silently dropped. Fail loudly.
            bucketTables[ws.bucket] = append(bucketTables[ws.bucket], &pb.TableCommitResult{
                Table: ws.table, Bucket: ws.bucket,
                Status: pb.TableCommitResult_STATUS_FAILED,
                Error:  "commit not dispatched (reconciliation failure)",
            })
            allSuccess = false
        }
    }

    // Clear the writer map now that the session is done.
    // N5 (documented assumption, not guarded): DG is one-session-per-
    // sidecar — there is no concurrent second session calling OpenWriter
    // between the barrier and this clear. If that lifecycle ever changes
    // (writer reuse across sessions), this blanket clear must become a
    // targeted delete of the just-committed writer IDs.
    s.mu.Lock()
    s.writers = make(map[string]*writerState)
    s.mu.Unlock()

    // Build buckets.
    buckets := make([]*pb.BucketCommitResult, 0, len(bucketTables))
    for bucket, tables := range bucketTables {
        st := pb.BucketCommitResult_STATUS_COMMITTED
        for _, t := range tables {
            if t.Status == pb.TableCommitResult_STATUS_FAILED {
                st = pb.BucketCommitResult_STATUS_FAILED
                break
            }
        }
        buckets = append(buckets, &pb.BucketCommitResult{Bucket: bucket, Status: st, Tables: tables})
    }
    if sweepErr != nil {
        log.Printf("ERROR: commit sweep error: %v", sweepErr)
    }
    return &pb.CommitResponse{Success: allSuccess, Buckets: buckets}, nil
}

// writerProducedFiles reports whether the writer wrote any parquet.
func writerProducedFiles(ws *writerState) bool {
    switch {
    case ws.partitionRouter != nil:
        return len(ws.partitionRouter.FilesWritten()) > 0
    case ws.bufferMgr != nil:
        return len(ws.bufferMgr.FilesWritten()) > 0
    default:
        return len(ws.externalFiles) > 0
    }
}
```

Remove the now-dead helpers/imports left over from the old loop (e.g. `perTableManifest`, `tableManifestTarget`) if nothing else uses them. Keep `persistTableManifest`, `writeSchemaAndManifest`, `patchParquetFieldIDs` — they're now called from `finalizeAndDispatch`.

- [ ] **Step 5: Update / add DG tests**

Existing `server_v2_*_test.go` tests that drove `Commit` without a resolver (no pool) should still pass: `finalizeAndDispatch` claims + writes metadata but dispatches nothing (nil pool); `Commit`'s reconciliation then reports each writer COMMITTED (produced files) or SKIPPED (no files) per the N3 nil-pool branch. Run them. Fix any that asserted the old double-close behavior or expected a `Committing`-style intermediate.

Add a focused test that with a fake pool (inject via a test-only setter or by constructing `ServerV2` with a `commitPool` whose `CommitFn` records calls), a `CloseWriter` + `Commit` sequence dispatches exactly one commit per writer and the barrier returns COMMITTED. If `ServerV2`'s constructor makes pool injection hard, add an unexported test helper `func (s *ServerV2) setCommitPoolForTest(p *CommitPool) { s.commitPool = p }` in a `_test.go` file.

- [ ] **Step 6: Build, race-test, commit**

```
go build ./...
go vet ./pkg/datagateway/...
go test -race ./pkg/datagateway/...
```
Expected: green.

```bash
git add pkg/datagateway/server_v2.go pkg/datagateway/server_v2_writing.go pkg/datagateway/server_v2_commit.go
git commit -m "datagateway: inline per-table commit on CloseWriter, Commit as barrier (RFC 021)"
```

---

## Task 5: DG cancel propagation + real test

**Recommended model:** haiku

**Files:**
- Modify: `pkg/datagateway/cancel_watch.go`
- Modify: `pkg/datagateway/cancel_watch_test.go`

- [ ] **Step 1: Find the cancel hook**

Run: `grep -n "func\|Cancel\|s \*ServerV2\|server" pkg/datagateway/cancel_watch.go | head -20`
Identify where the watcher fires on `datuplet.io/cancel=true` and whether it has a `*ServerV2`.

- [ ] **Step 2: Cancel the pool**

In the fire path, add:

```go
if s.commitPool != nil {
    log.Printf("cancel: cancelling commit pool")
    s.commitPool.Cancel()
}
```

If the watcher lacks a `*ServerV2` reference, plumb one in following the file's existing wiring.

- [ ] **Step 3: Write a real test (not a stub)**

```go
func TestCancelWatch_CancelsCommitPool(t *testing.T) {
    // Build a ServerV2 with a commit pool whose CommitFn blocks on
    // ctx.Done. Dispatch one job, simulate the annotation flip the way
    // the existing cancel tests do, then assert pool.Wait returns a
    // result with a non-nil (context-cancelled) Err.
    // Reuse the annotation-flip harness from the existing cancel test.
}
```

Fill in using the existing cancel-test harness in the same file. The assertion: after the flip, the in-flight commit's result carries `context.Canceled`.

- [ ] **Step 4: Run + commit**

```
go test -race ./pkg/datagateway/ -run TestCancel -v
```
```bash
git add pkg/datagateway/cancel_watch.go pkg/datagateway/cancel_watch_test.go
git commit -m "datagateway: cancel watcher cancels commit pool (RFC 021)"
```

---

## Task 6: DG metrics + typed error classifier

**Recommended model:** haiku

**Files:**
- Create: `pkg/datagateway/commit_metrics.go`
- Modify: `pkg/datagateway/commit_pool.go` (emit metrics in `run`/`Dispatch`)

- [ ] **Step 1: Match existing metrics convention**

Run: `grep -rn "promauto\|prometheus.New\|MustRegister" pkg/datagateway/ pkg/ | head`
Use the same registry pattern. If the project uses `promauto` with the default registry, follow it.

- [ ] **Step 2: Create `commit_metrics.go`**

```go
package datagateway

import (
    "context"
    "errors"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"

    "github.com/datuplet/datuplet/pkg/catalogwriter"
)

// Metrics are registered ONCE here at package-init via promauto against
// the default registry. Do NOT move registration into NewCommitPool —
// constructing two pools in one process would then panic with
// "duplicate metrics collector registration".
//
// N7 (documented assumption): production constructs exactly one
// CommitPool per process, so package-level registration is safe. Tests
// that need multiple pools must NOT trigger this var block twice — the
// pool tests in commit_pool_test.go inject a fake CommitFn and never
// touch these metrics, so they are unaffected. If a future test needs
// per-pool metrics, switch to an injected *prometheus.Registry.
var (
    commitDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
        Name:    "datuplet_dg_commit_duration_seconds",
        Help:    "Inline iceberg commit duration per table.",
        Buckets: []float64{0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300},
    }, []string{"mode", "result"})
    commitQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "datuplet_dg_commit_queue_depth",
        Help: "Queued+in-flight inline commits.",
    })
    commitIdempotencySkips = promauto.NewCounter(prometheus.CounterOpts{
        Name: "datuplet_dg_commit_idempotency_skips_total",
        Help: "Inline commits skipped via matching commit-key.",
    })
    commitErrors = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "datuplet_dg_commit_errors_total",
        Help: "Inline commit errors by class.",
    }, []string{"class"})
)

// classifyCommitError maps an error to a stable metric label using
// TYPED checks (no substring scanning of message text).
func classifyCommitError(err error) string {
    switch {
    case err == nil:
        return ""
    case errors.Is(err, context.Canceled):
        return "cancelled"
    case errors.Is(err, context.DeadlineExceeded):
        return "timeout"
    case catalogwriter.IsCommitConflict(err):
        return "conflict"
    default:
        return "other"
    }
}
```

(If `catalogwriter` exposes more typed errors — e.g. an auth or not-found sentinel — add cases. Check `pkg/catalogwriter/*.go` for exported `Err*` or `Is*` helpers.)

- [ ] **Step 3: Emit metrics from the pool**

In `commit_pool.go` `Dispatch`, after `p.inflight++`:

```go
commitQueueDepth.Set(float64(p.inflight))
```
and after `p.inflight--` in the deferred cleanup:
```go
commitQueueDepth.Set(float64(p.inflight))
```
(Both are already under `p.mu`.)

In `run`, after computing the result:

```go
mode := string(j.Mode)
switch {
case cr.Err != nil:
    commitErrors.WithLabelValues(classifyCommitError(cr.Err)).Inc()
    commitDuration.WithLabelValues(mode, "err").Observe(cr.Duration.Seconds())
case cr.DataFilesAdded == 0 && cr.SnapshotIDAfter != "":
    commitIdempotencySkips.Inc()
    commitDuration.WithLabelValues(mode, "idempotent").Observe(cr.Duration.Seconds())
default:
    commitDuration.WithLabelValues(mode, "ok").Observe(cr.Duration.Seconds())
}
```

- [ ] **Step 4: Build, test, commit**

```
go build ./...
go test -race ./pkg/datagateway/ -run TestCommitPool_ -v
```
```bash
git add pkg/datagateway/commit_metrics.go pkg/datagateway/commit_pool.go
git commit -m "datagateway: prometheus metrics + typed error classifier for commit pool (RFC 021)"
```

---

## Task 7: Operator — delete commit-Job state machine (coupled cutover with Task 4)

**Recommended model:** sonnet

> **Coupled cutover:** this task and Task 4 together flip the system from "operator commits via Job" to "DG commits inline." There is NO safe state where both are active (double-commit). Land Tasks 1-6, then this task, then Task 9 (E2E) immediately. The reconciliation in Task 4 Step 4 is the loud-failure guard if DG commit silently regresses.

**Files:**
- Delete: `pkg/k8s/controllers/pipelinerun_commit_jobs.go` + `_test.go`
- Modify: `pkg/k8s/controllers/pipelinerun_controller.go`
- Modify: `pkg/k8s/controllers/pipelinerun_controller_test.go`

- [ ] **Step 1: Delete the commit-job files**

```bash
git rm pkg/k8s/controllers/pipelinerun_commit_jobs.go pkg/k8s/controllers/pipelinerun_commit_jobs_test.go
```

- [ ] **Step 2: Remove call sites in `pipelinerun_controller.go`**

- The `checkStageCommits` branch (~line 321): delete the `if`-branch that routed to commit-job polling.
- The `startStageCommits` call (~line 558): replace with the direct success transition:

```go
stageStatus.Phase = datupletv1.StagePhaseSucceeded
now := metav1.Now()
stageStatus.CompletionTime = &now
logger.Info("Stage completed successfully", "stage", stage.Name)
if err := r.Status().Update(ctx, pr); err != nil {
    logger.Error(err, "Failed to update PipelineRun status")
    return ctrl.Result{}, err
}
return ctrl.Result{RequeueAfter: PipelineRunRequeueInterval}, nil
```

Delete the whole `startStageCommits` and `checkStageCommits` functions. Remove now-unused imports (`batchv1`, `types`, etc. if only those functions used them — let the compiler tell you).

- [ ] **Step 3: Keep `StagePhaseCommitting` + `TableCommits` in the CRD types**

Do NOT remove them from `pkg/k8s/api/v1/pipelinerun_types.go` — kept for unmarshal compat. They're simply never written now.

- [ ] **Step 4: Fix tests**

Replace any test asserting a `Committing` transition with:

```go
func TestStageTransition_RunningToSucceededDirect(t *testing.T) {
    // Reuse the existing reconcile fixture. One stage, one component,
    // component exits 0. Reconcile to steady state. Assert
    // pr.Status.StageStatuses[0].Phase == datupletv1.StagePhaseSucceeded
    // and that no batchv1.Job with label
    // app.kubernetes.io/component=table-commit was created (list via the
    // fake client; expect zero).
}
```

- [ ] **Step 5: Build + test + commit**

```
go build ./...
go test ./pkg/k8s/controllers/...
```
```bash
git add -u pkg/k8s/controllers/
git commit -m "operator: delete commit-Job state machine; stage Running->Succeeded direct (RFC 021)"
```

---

## Task 8: Delete orchestrator + CLI subcommand

**Recommended model:** haiku

**Files:**
- Delete: `pkg/icebergjob/commit.go`, `pkg/icebergjob/commit_test.go`, `pkg/icebergjob/orchestrator_test.go`
- Delete: `cmd/datuplet/table_commit.go`, `cmd/datuplet/run_token_path_test.go`
- Modify: `cmd/datuplet/main.go`
- Modify: `pkg/icebergjob/router.go` (collapse if it only routed table-commit mode)

- [ ] **Step 1: Confirm no external importers of the orchestrator**

Run: `grep -rn "icebergjob.TableCommitter\|icebergjob.Execute\|icebergjob.New(\|icebergjob.TableConfig\|icebergjob.Config{" --include="*.go" .`
Expected: matches only in the files being deleted + `cmd/datuplet/main.go`. If anything else references them, halt and report.

Also confirm `WriteMode` consts were moved to `commit_shared.go` in Task 1 (grep `WriteModeAppend` — must resolve in `commit_shared.go`, not `commit.go`).

- [ ] **Step 2: Delete files**

```bash
git rm pkg/icebergjob/commit.go pkg/icebergjob/commit_test.go pkg/icebergjob/orchestrator_test.go \
       cmd/datuplet/table_commit.go cmd/datuplet/run_token_path_test.go
```

- [ ] **Step 3: Replace the `iceberg-job` subcommand in `main.go`**

Find `case "iceberg-job":` (~line 132). Replace its body with:

```go
case "iceberg-job":
    fmt.Fprintln(os.Stderr, "Error: `datuplet iceberg-job --mode=table-commit` is removed in RFC 021.")
    fmt.Fprintln(os.Stderr, "Inline commit now lives in the data gateway sidecar. This binary will")
    fmt.Fprintln(os.Stderr, "grow --mode=compact / expire-snapshots / remove-orphans in a future RFC.")
    os.Exit(2)
```

Remove the old subcommand's flag declarations + usage-banner lines (`grep -n "iceberg-job\|ijMode\|icebergJobCmd" cmd/datuplet/main.go`).

- [ ] **Step 4: Collapse `router.go` if now empty**

If `pkg/icebergjob/router.go` only dispatched modes that no longer exist, reduce to:

```go
// Package icebergjob currently exports library functions only
// (CommitTableFiles / CommitTable). Maintenance subcommands land later.
package icebergjob
```

- [ ] **Step 5: Build everything + commit**

```
go build ./...
go vet ./...
go test ./pkg/icebergjob/ ./cmd/datuplet/
```
```bash
git add -u
git commit -m "icebergjob: delete table-commit orchestrator + CLI subcommand (RFC 021)"
```

---

## Task 9: E2E verification

**Recommended model:** opus (sonnet if everything passes first try)

**Files:** none (verification only).

- [ ] **Step 1: Full build + images**

```
go build ./...
make build
make build-components
```

- [ ] **Step 2: Unit + race suite**

```
go test -race ./...
```
Expected: green.

- [ ] **Step 3: e2e**

```
make e2e-k8s
```

- [ ] **Step 4: Original bug repro**

Submit `demo2-distribution` (two stages, both writing bucket `demo2`). After completion:

```bash
./bin/datuplet storage info demo2.user_tx_counts
./bin/datuplet storage info demo2.users
./bin/datuplet storage info demo2.transactions
```
Expected: each has `current_snapshot_id != 0`; row counts 10 / 10 / 500000.

- [ ] **Step 5: No commit Jobs scheduled**

```bash
kubectl get jobs -A -l app.kubernetes.io/component=table-commit
```
Expected: no jobs from the new run.

- [ ] **Step 6: DG logs show inline commits**

```bash
kubectl logs -A -l datuplet.io/run-id=<run-id> -c gateway | grep "dispatched inline commit"
```
Expected: one line per produced output table.

- [ ] **Step 7: Cancel mid-run**

Submit a longer pipeline; once Running:
```bash
./bin/datuplet pipelines cancel <run-id>
```
Expected DG log: `cancel: cancelling commit pool`. Lakekeeper: no snapshots from the cancelled run.

- [ ] **Step 8: Reconciliation sanity (the L-fix)**

Confirm that a deliberately-failing commit (e.g. point DG at an unreachable lakekeeper for one table via a fault-injection config, if the e2e harness supports it) makes the component exit non-zero and the stage `FailedApplication` — NOT a false `Succeeded`. If no fault-injection hook exists, document this as a manual check deferred to the first real failure.

---

## Self-review checklist

- [x] **Spec coverage:** RFC §1 Goals + §3 approach + accepted review amendments (idempotency check inside retry, queue bound + `ErrCommitQueueFull`, no plaintext run-id — only the hashed key in summary, typed error taxonomy, five metrics, sentinel-safety doc, NUL-separated key, catalog-lifecycle ordering, reconciliation L-fix) all mapped to tasks. Jitter rewrite dropped (already in `RetryOnConflict`).
- [x] **Verified facts:** proto enum `STATUS_COMMITTED`/field `Error`, path sources, `writerState` fields, resolver per-call client, `RetryOnConflict` existing jitter — all checked against `main` and recorded in the "ground-truth facts" section.
- [x] **Placeholder scan:** no "TBD"/"add error handling"/"similar to Task N". Two intentional reads-before-edit instructions (Task 4 buffer-Close idempotency; Task 6 extra typed errors) are explicit verification steps, not placeholders.
- [x] **Type consistency:** `CommitJob`, `CommitResult`, `CommitPool`, `CommitFunc`, `CommitPoolConfig`, `CommitTableFiles`, `finalizeAndDispatch`, `writerProducedFiles`, `writeModeForTable`, `committed` field — consistent across Tasks 1-6.
- [x] **Coupled-cutover risk (review finding L):** flagged at the top + on Tasks 4 and 7; reconciliation in Task 4 Step 4 is the loud-failure guard; E2E Step 8 verifies it.
- [x] **Race-free pool (finding A):** semaphore + WaitGroup, no channel-close barrier; `-race` runs in Tasks 3, 4, 5, 9.
- [x] **v2 re-review amendments folded in:** N1 — `finalizeAndDispatch` holds `s.mu` only for the claim, never across storage I/O; the `Commit` sweep snapshots writers under the lock then closes + finalizes lock-free (also avoids the self-lock deadlock). N2 — `CommitPool.Wait` honors a ctx deadline; `Close()` drains with a 30s bound. N3 — reconciliation reports COMMITTED (not FAILED) for produced-files writers in nil-pool test mode. N4 — `ws.closed` guard + surfaced close errors in the sweep. N6 — `writeModeForTable` matches on `(bucket, table)` pair with a bucket-scoped test. N5 (writer-map clear race) and N7 (promauto single-pool) documented as assumptions in-code.
- [x] **Deferred per agreement:** orphan cleanup, forgeable key (multi-tenant), SDK panic paths, migration, rollback, cross-DG same-table — not in scope.

---

## Execution handoff

Plan saved to `docs/superpowers/plans/2026-05-26-rfc-021-dg-owns-commit.md`.

1. **Subagent-Driven (recommended)** — fresh subagent per task with the model recommendation in the index; orchestrator reviews each commit, runs `go build ./... && go test -race ./pkg/...` between tasks, and reviews Tasks 4 + 7 together before E2E.
2. **Inline Execution** — superpowers:executing-plans, batched with checkpoints.
