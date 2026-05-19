package datagateway

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datuplet/datuplet/pkg/datagateway/backend"
	"github.com/datuplet/datuplet/pkg/datagateway/format"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

const membenchEnvVar = "DUPLET_MEMBENCH_ROWS"

// TestMemoryFootprint_EndToEnd_JSONL exercises the full processWriteChunk
// path (parse JSONL -> Arrow -> BufferManager.Add) with N single-row writes.
// Mirrors what data-generator does in production. Gated by DUPLET_MEMBENCH_ROWS.
func TestMemoryFootprint_EndToEnd_JSONL(t *testing.T) {
	n := membenchRowsOrSkip(t)
	runEndToEndMembench(t, "EndToEnd_JSONL", n, pb.DataFormat_FORMAT_JSONL, jsonlRowGen)
}

// TestMemoryFootprint_EndToEnd_CSV exercises CSV row-at-a-time. The default
// CSVAdapter has HasHeader=true which treats every call's first row as the
// header — fine when the caller batches but useless row-at-a-time. We swap
// the writerState's adapter post-OpenWriter to HasHeader=false so each
// WriteChunk call is one data row. This is a test-only adjustment; production
// CSV writers do not currently call row-at-a-time, but the underlying
// NewRecordBuilder + NewRecord per-call pattern is identical to JSONL, so
// any caller that did would hit the same memory wall.
func TestMemoryFootprint_EndToEnd_CSV(t *testing.T) {
	n := membenchRowsOrSkip(t)
	runEndToEndMembench(t, "EndToEnd_CSV", n, pb.DataFormat_FORMAT_CSV, csvRowGen)
}

type rowGenFn func(i int) []byte

func jsonlRowGen(i int) []byte {
	// ~95 bytes/row payload; matches data-generator output shape.
	return []byte(fmt.Sprintf(`{"id":%d,"name":"user_%05d","value":%f,"active":%t,"ts":"2026-05-19T10:00:00Z"}`+"\n",
		i, i, float64(i)*1.5, i%2 == 0))
}

func csvRowGen(i int) []byte {
	// ~50 bytes/row payload; no header line.
	return []byte(fmt.Sprintf("%d,user_%05d,%f,%t,2026-05-19T10:00:00\n",
		i, i, float64(i)*1.5, i%2 == 0))
}

func runEndToEndMembench(t *testing.T, scenarioName string, n int, fmtPb pb.DataFormat, gen rowGenFn) {
	t.Helper()

	// Discard backend: PutObject is a no-op so flushed parquet bytes
	// don't contaminate heap measurements. We're measuring buffering
	// memory, not stored-file memory.
	be := newDiscardBackend()

	cfg := &Config{
		RunID:         "membench-run",
		ComponentName: "membench",
		DefaultBucket: "raw",
		Backend:       be,
	}
	srv := NewServerV2(cfg)

	ctx := context.Background()
	resp, err := srv.OpenWriter(ctx, &pb.OpenWriterRequest{
		Table:       "events",
		InputFormat: fmtPb,
	})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	srv.mu.RLock()
	ws, ok := srv.writers[resp.WriterId]
	srv.mu.RUnlock()
	if !ok {
		t.Fatalf("writerState %q not found", resp.WriterId)
	}

	// CSV row-at-a-time hack: replace the adapter with one that doesn't
	// expect a header row. See test docstring for rationale.
	if fmtPb == pb.DataFormat_FORMAT_CSV {
		opts := format.DefaultParseOptions()
		opts.HasHeader = false
		ws.adapter = format.NewCSVAdapter(srv.allocator, opts)
	}

	runtime.GC()
	runtime.GC()
	baseline := readMemStats()
	sampler := startMemSampler(10 * time.Millisecond)

	var payloadBytes int64
	start := time.Now()
	for i := 0; i < n; i++ {
		row := gen(i)
		payloadBytes += int64(len(row))
		if _, err := srv.processWriteChunk(ctx, ws, row); err != nil {
			t.Fatalf("processWriteChunk[%d]: %v", i, err)
		}
	}
	elapsed := time.Since(start)

	postLoop := readMemStats()
	peak := sampler.stop()
	peak.HeapInuse = maxU64(peak.HeapInuse, postLoop.HeapInuse)
	peak.HeapAlloc = maxU64(peak.HeapAlloc, postLoop.HeapAlloc)
	peak.Sys = maxU64(peak.Sys, postLoop.Sys)

	runtime.GC()
	runtime.GC()
	liveAfter := readMemStats()
	// Defeat Go's precise-liveness GC: srv (and its writerState/bufferMgr/
	// retained records) would otherwise be collected before we observe.
	runtime.KeepAlive(srv)
	runtime.KeepAlive(ws)

	reportScenario(t, scenarioName, n, payloadBytes, baseline, peak, liveAfter, elapsed)
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// discardBackend implements backend.StorageBackend; PutObject is a no-op so
// flushed parquet bytes don't pollute heap measurements. The other methods
// return errors / empties — only PutObject is exercised by the buffer flush path.
type discardBackend struct{}

func newDiscardBackend() *discardBackend { return &discardBackend{} }

func (b *discardBackend) OpenReader(ctx context.Context, tablePath string) (backend.Reader, error) {
	return nil, fmt.Errorf("discardBackend: OpenReader not supported")
}
func (b *discardBackend) OpenReaderForFiles(ctx context.Context, paths []string) (backend.Reader, error) {
	return nil, fmt.Errorf("discardBackend: OpenReaderForFiles not supported")
}
func (b *discardBackend) OpenStreamingArrowReader(ctx context.Context, paths []string, sch *backend.SchemaInfo) (backend.Reader, error) {
	return nil, fmt.Errorf("discardBackend: OpenStreamingArrowReader not supported")
}
func (b *discardBackend) OpenWriter(ctx context.Context, tablePath string, opts backend.WriteOptions) (backend.Writer, error) {
	return nil, fmt.Errorf("discardBackend: OpenWriter not supported (membench uses BufferManager.factory path)")
}
func (b *discardBackend) Commit(ctx context.Context, writers []backend.Writer) (*backend.CommitResult, error) {
	return &backend.CommitResult{}, nil
}
func (b *discardBackend) Rollback(ctx context.Context, writers []backend.Writer) error { return nil }
func (b *discardBackend) GetSchema(ctx context.Context, tablePath string) (*backend.SchemaInfo, error) {
	return nil, fmt.Errorf("discardBackend: GetSchema not supported")
}
func (b *discardBackend) GetSample(ctx context.Context, tablePath string, limit int) (*backend.SampleResult, error) {
	return nil, fmt.Errorf("discardBackend: GetSample not supported")
}
func (b *discardBackend) GetObject(ctx context.Context, path string) ([]byte, error) {
	return nil, fmt.Errorf("discardBackend: GetObject not supported")
}
func (b *discardBackend) PutObject(ctx context.Context, path string, data []byte) error { return nil }
func (b *discardBackend) RemoveAll(ctx context.Context, prefix string) error             { return nil }
func (b *discardBackend) Close() error                                                   { return nil }

// --- helpers (duplicated with pkg/datagateway/buffer/membench_test.go because
// _test.go identifiers are not importable across packages) -------------------

type memStatsSnapshot struct {
	HeapInuse  uint64
	HeapAlloc  uint64
	Sys        uint64
	StackInuse uint64
	NumGC      uint32
}

func readMemStats() memStatsSnapshot {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return memStatsSnapshot{
		HeapInuse:  ms.HeapInuse,
		HeapAlloc:  ms.HeapAlloc,
		Sys:        ms.Sys,
		StackInuse: ms.StackInuse,
		NumGC:      ms.NumGC,
	}
}

type memSampler struct {
	stopCh         chan struct{}
	done           chan struct{}
	peakHeapInuse  atomic.Uint64
	peakHeapAlloc  atomic.Uint64
	peakSys        atomic.Uint64
	peakStackInuse atomic.Uint64
	once           sync.Once
}

func startMemSampler(interval time.Duration) *memSampler {
	s := &memSampler{
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
	go func() {
		defer close(s.done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-s.stopCh:
				return
			case <-t.C:
				snap := readMemStats()
				updateMax(&s.peakHeapInuse, snap.HeapInuse)
				updateMax(&s.peakHeapAlloc, snap.HeapAlloc)
				updateMax(&s.peakSys, snap.Sys)
				updateMax(&s.peakStackInuse, snap.StackInuse)
			}
		}
	}()
	return s
}

func updateMax(dst *atomic.Uint64, v uint64) {
	for {
		cur := dst.Load()
		if v <= cur {
			return
		}
		if dst.CompareAndSwap(cur, v) {
			return
		}
	}
}

func (s *memSampler) stop() memStatsSnapshot {
	s.once.Do(func() {
		close(s.stopCh)
		<-s.done
	})
	return memStatsSnapshot{
		HeapInuse:  s.peakHeapInuse.Load(),
		HeapAlloc:  s.peakHeapAlloc.Load(),
		Sys:        s.peakSys.Load(),
		StackInuse: s.peakStackInuse.Load(),
	}
}

func reportScenario(t *testing.T, name string, rows int, payloadBytes int64, baseline, peak, liveAfter memStatsSnapshot, elapsed time.Duration) {
	t.Helper()
	deltaHeapInuse := safeSub(peak.HeapInuse, baseline.HeapInuse)
	deltaHeapAlloc := safeSub(peak.HeapAlloc, baseline.HeapAlloc)
	deltaSys := safeSub(peak.Sys, baseline.Sys)
	liveDeltaAlloc := safeSub(liveAfter.HeapAlloc, baseline.HeapAlloc)

	ratioPeak := float64(deltaHeapInuse) / float64(rows)
	ratioLive := float64(liveDeltaAlloc) / float64(rows)
	overheadPeak := "n/a"
	overheadLive := "n/a"
	if payloadBytes > 0 {
		overheadPeak = fmt.Sprintf("%.2fx", float64(deltaHeapInuse)/float64(payloadBytes))
		overheadLive = fmt.Sprintf("%.2fx", float64(liveDeltaAlloc)/float64(payloadBytes))
	}

	t.Logf("\n=== membench scenario=%s rows=%d payload=%s elapsed=%s ===", name, rows, fmtBytes(payloadBytes), elapsed.Round(time.Millisecond))
	t.Logf("  baseline  HeapInuse=%s HeapAlloc=%s Sys=%s", fmtBytes(int64(baseline.HeapInuse)), fmtBytes(int64(baseline.HeapAlloc)), fmtBytes(int64(baseline.Sys)))
	t.Logf("  peak      HeapInuse=%s HeapAlloc=%s Sys=%s", fmtBytes(int64(peak.HeapInuse)), fmtBytes(int64(peak.HeapAlloc)), fmtBytes(int64(peak.Sys)))
	t.Logf("  live      HeapInuse=%s HeapAlloc=%s Sys=%s (after 2x runtime.GC)", fmtBytes(int64(liveAfter.HeapInuse)), fmtBytes(int64(liveAfter.HeapAlloc)), fmtBytes(int64(liveAfter.Sys)))
	t.Logf("  delta     ΔHeapInuse=%s ΔHeapAlloc=%s ΔSys=%s ΔLiveAlloc=%s", fmtBytes(int64(deltaHeapInuse)), fmtBytes(int64(deltaHeapAlloc)), fmtBytes(int64(deltaSys)), fmtBytes(int64(liveDeltaAlloc)))
	t.Logf("  per-row   peak=%.1f B/row live=%.1f B/row", ratioPeak, ratioLive)
	t.Logf("  overhead  peak=%s of payload, live=%s of payload", overheadPeak, overheadLive)
}

func safeSub(a, b uint64) uint64 {
	if a < b {
		return 0
	}
	return a - b
}

func fmtBytes(n int64) string {
	const (
		KiB = 1024
		MiB = 1024 * KiB
		GiB = 1024 * MiB
	)
	switch {
	case n >= GiB:
		return fmt.Sprintf("%.2f GiB", float64(n)/float64(GiB))
	case n >= MiB:
		return fmt.Sprintf("%.2f MiB", float64(n)/float64(MiB))
	case n >= KiB:
		return fmt.Sprintf("%.2f KiB", float64(n)/float64(KiB))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

func membenchRowsOrSkip(t *testing.T) int {
	t.Helper()
	raw := os.Getenv(membenchEnvVar)
	if raw == "" {
		t.Skipf("set %s=N to run (e.g., 100000, 1000000, 5000000)", membenchEnvVar)
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		t.Fatalf("invalid %s=%q: %v", membenchEnvVar, raw, err)
	}
	return n
}
