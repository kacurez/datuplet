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
	"log"
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
	WriterID, Namespace, Table        string
	Err                               error
	SnapshotIDBefore, SnapshotIDAfter string
	DataFilesAdded                    int
	IdempotencyKey                    string
	IdempotencyHit                    bool
	Duration                          time.Duration
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
	sessionID uint64
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
func (p *CommitPool) Dispatch(_ context.Context, j CommitJob) error {
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
	commitQueueDepth.Set(float64(p.inflight))
	p.wg.Add(1)
	p.mu.Unlock()

	p.resultsMu.Lock()
	sid := p.sessionID
	p.resultsMu.Unlock()

	go func() {
		defer func() {
			p.mu.Lock()
			p.inflight--
			commitQueueDepth.Set(float64(p.inflight))
			p.mu.Unlock()
			p.wg.Done()
		}()
		select {
		case p.sem <- struct{}{}:
			defer func() { <-p.sem }()
		case <-p.parentCtx.Done():
			p.record(sid, CommitResult{WriterID: j.WriterID, Namespace: j.Namespace, Table: j.Table, Err: p.parentCtx.Err()})
			return
		}
		p.run(sid, j)
	}()
	return nil
}

func (p *CommitPool) run(sid uint64, j CommitJob) {
	start := time.Now()
	key := ComputeIdempotencyKey(j.RunID, j.Namespace, j.Table, j.DataPaths)
	cr := CommitResult{WriterID: j.WriterID, Namespace: j.Namespace, Table: j.Table, IdempotencyKey: key}

	cat, err := p.cfg.CatalogFn(p.parentCtx)
	if err != nil {
		cr.Err = err
		cr.Duration = time.Since(start)
		commitErrors.WithLabelValues(classifyCommitError(cr.Err)).Inc()
		commitDuration.WithLabelValues(string(j.Mode), "err").Observe(cr.Duration.Seconds())
		p.record(sid, cr)
		return
	}
	res, err := p.cfg.CommitFn(p.parentCtx, cat, icebergtable.Identifier{j.Namespace, j.Table}, j.DataPaths, j.Mode, key)
	cr.Duration = time.Since(start)
	if err != nil {
		cr.Err = err
		logCatalogSchemaOnMismatch(p.parentCtx, cat, j.Namespace, j.Table, err)
	} else if res != nil {
		cr.SnapshotIDBefore = res.SnapshotIDBefore
		cr.SnapshotIDAfter = res.SnapshotIDAfter
		cr.DataFilesAdded = res.DataFilesAdded
		cr.IdempotencyHit = res.IdempotencyHit
	}
	mode := string(j.Mode)
	switch {
	case cr.Err != nil:
		commitErrors.WithLabelValues(classifyCommitError(cr.Err)).Inc()
		commitDuration.WithLabelValues(mode, "err").Observe(cr.Duration.Seconds())
	case cr.IdempotencyHit:
		commitIdempotencySkips.Inc()
		commitDuration.WithLabelValues(mode, "idempotent").Observe(cr.Duration.Seconds())
	default:
		commitDuration.WithLabelValues(mode, "ok").Observe(cr.Duration.Seconds())
	}
	p.record(sid, cr)
}

// looksLikeSchemaMismatch reports whether a commit error is plausibly an
// Iceberg schema-compatibility rejection, so the extra catalog round-trip in
// logCatalogSchemaOnMismatch is only paid when it can actually help. Kept a
// text heuristic deliberately: iceberg-go returns these as plain errors with
// no distinguishable type, and being wrong here costs one logged line, not
// correctness.
func looksLikeSchemaMismatch(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, needle := range []string{"schema", "field", "incompatible", "type mismatch"} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

// logCatalogSchemaOnMismatch logs the table's CURRENT catalog schema when a
// commit fails in a way that looks like a schema conflict. Paired with the
// gateway's "writer schema:" line, this puts the emitted schema and the
// existing one in the same log stream — the two facts needed to diagnose the
// mismatch, which previously required pulling iceberg metadata out of object
// storage by hand.
//
// Strictly best-effort: every failure to fetch is swallowed, and the original
// commit error is never modified.
func logCatalogSchemaOnMismatch(ctx context.Context, cat catalog.Catalog, ns, table string, commitErr error) {
	if cat == nil || !looksLikeSchemaMismatch(commitErr) {
		return
	}
	// Bounded: this runs on a failure path, possibly during shutdown.
	lookupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	tbl, err := cat.LoadTable(lookupCtx, icebergtable.Identifier{ns, table})
	if err != nil {
		log.Printf("commit failed for %s.%s and the existing schema could not be read for comparison: %v", ns, table, err)
		return
	}
	sch := tbl.Schema()
	if sch == nil {
		return
	}
	fields := sch.Fields()
	shown := fields
	if len(shown) > maxLoggedSchemaColumns {
		shown = shown[:maxLoggedSchemaColumns]
	}
	parts := make([]string, 0, len(shown))
	for _, f := range shown {
		null := "?"
		if f.Required {
			null = ""
		}
		parts = append(parts, fmt.Sprintf("%s:%s%s", f.Name, f.Type, null))
	}
	truncated := ""
	if len(fields) > len(shown) {
		truncated = fmt.Sprintf(", +%d more", len(fields)-len(shown))
	}
	log.Printf("existing catalog schema for %s.%s: columns=%d [%s%s] "+
		"(compare with the \"writer schema:\" line above; a type or required/nullable difference fails the commit)",
		ns, table, len(fields), strings.Join(parts, ", "), truncated)
}

func (p *CommitPool) record(sid uint64, cr CommitResult) {
	p.resultsMu.Lock()
	if p.sessionID == sid {
		p.results = append(p.results, cr)
	}
	p.resultsMu.Unlock()
}

// Wait blocks until all dispatched jobs finish OR ctx is cancelled, then
// returns and clears the result list. Reusable across sessions. Safe to
// call twice. Must not run concurrently with Dispatch. On ctx timeout it
// returns the results recorded so far (in-flight jobs keep running in the
// background but no longer block the caller) — used by the bounded
// shutdown drain in ServerV2.Close().
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
	p.sessionID++
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
