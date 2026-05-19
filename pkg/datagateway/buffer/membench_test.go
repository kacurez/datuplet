package buffer

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

const (
	membenchEnvVar         = "DUPLET_MEMBENCH_ROWS"
	membenchCoalesceEnvVar = "DUPLET_MEMBENCH_COALESCE_K"
)

// TestMemoryFootprint_BufferManagerDirect drives BufferManager.Add with N
// single-row Arrow records and reports peak heap usage. Format-agnostic:
// isolates per-Record Arrow scaffolding cost + estimateRecordSize undercount
// from any format-adapter overhead.
//
// Gated by env DUPLET_MEMBENCH_ROWS. Skipped in CI; run locally with:
//
//	DUPLET_MEMBENCH_ROWS=100000  go test -run TestMemoryFootprint_BufferManagerDirect -count=1 -v ./pkg/datagateway/buffer
//	DUPLET_MEMBENCH_ROWS=1000000 go test -run TestMemoryFootprint_BufferManagerDirect -count=1 -v ./pkg/datagateway/buffer
//	DUPLET_MEMBENCH_ROWS=5000000 go test -run TestMemoryFootprint_BufferManagerDirect -count=1 -v ./pkg/datagateway/buffer
func TestMemoryFootprint_BufferManagerDirect(t *testing.T) {
	n := membenchRowsOrSkip(t)
	tmpDir := t.TempDir()

	cols := []schema.ColumnDef{
		{Name: "id", Type: schema.TypeInt64, Nullable: false},
		{Name: "name", Type: schema.TypeString, Nullable: true},
		{Name: "value", Type: schema.TypeFloat64, Nullable: true},
		{Name: "active", Type: schema.TypeBool, Nullable: true},
		{Name: "ts", Type: schema.TypeTimestamp, Nullable: true},
	}
	s, err := schema.NewSchema(cols)
	if err != nil {
		t.Fatalf("NewSchema: %v", err)
	}

	cfg := &BufferConfig{
		BufferSize:     64 * 1024 * 1024,
		RowGroupSize:   64 * 1024 * 1024,
		TargetFileSize: 128 * 1024 * 1024,
		OutputDir:      tmpDir,
		FilePrefix:     "membench",
		Compression:    CompressionSnappy,
	}

	alloc := memory.NewGoAllocator()
	mgr, err := NewBufferManager(s, cfg, alloc, nil)
	if err != nil {
		t.Fatalf("NewBufferManager: %v", err)
	}
	defer mgr.Close()

	arrowSchema := s.ArrowSchema()

	// Warm GC, capture baseline.
	runtime.GC()
	runtime.GC()
	baseline := readMemStats()

	sampler := startMemSampler(10 * time.Millisecond)

	approxPayloadBytesPerRow := int64(8 + 10 + 8 + 1 + 8) // int64 + ~10B name + float64 + bool + ts8
	start := time.Now()
	for i := 0; i < n; i++ {
		rec := buildOneRow(alloc, arrowSchema, i)
		if err := mgr.Add(rec); err != nil {
			t.Fatalf("Add[%d]: %v", i, err)
		}
		rec.Release()
	}
	elapsed := time.Since(start)

	// Take a synchronous post-loop snapshot before stopping the sampler,
	// so fast runs (<10ms) still have at least one observation at the peak.
	postLoop := readMemStats()
	peak := sampler.stop()
	peak.HeapInuse = maxU64(peak.HeapInuse, postLoop.HeapInuse)
	peak.HeapAlloc = maxU64(peak.HeapAlloc, postLoop.HeapAlloc)
	peak.Sys = maxU64(peak.Sys, postLoop.Sys)

	runtime.GC()
	runtime.GC()
	liveAfter := readMemStats()
	// Defeat Go's precise-liveness GC: without this, the runtime may
	// collect mgr (and its retained records) before we observe heap state.
	runtime.KeepAlive(mgr)

	reportScenario(t, "BufferManagerDirect", n, int64(n)*approxPayloadBytesPerRow, baseline, peak, liveAfter, elapsed)
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// buildOneRow constructs a single-row Arrow Record. Mirrors what JSONL/CSV
// adapters do under the hood: NewRecordBuilder -> per-field Append -> NewRecord.
// The caller owns the returned record and must Release it.
func buildOneRow(alloc memory.Allocator, sch *arrow.Schema, i int) arrow.Record {
	b := array.NewRecordBuilder(alloc, sch)
	defer b.Release()
	for f := 0; f < sch.NumFields(); f++ {
		field := sch.Field(f)
		switch field.Type.ID() {
		case arrow.INT64:
			b.Field(f).(*array.Int64Builder).Append(int64(i))
		case arrow.FLOAT64:
			b.Field(f).(*array.Float64Builder).Append(float64(i) * 1.5)
		case arrow.STRING:
			b.Field(f).(*array.StringBuilder).Append(fmt.Sprintf("user_%05d", i))
		case arrow.BOOL:
			b.Field(f).(*array.BooleanBuilder).Append(i%2 == 0)
		case arrow.TIMESTAMP:
			b.Field(f).(*array.TimestampBuilder).Append(arrow.Timestamp(int64(i) * 1_000_000))
		default:
			b.Field(f).AppendNull()
		}
	}
	return b.NewRecord()
}

// memSampler polls runtime.ReadMemStats on a ticker and tracks the
// high-water mark for HeapInuse, HeapAlloc, and Sys.
type memSampler struct {
	stopCh         chan struct{}
	done           chan struct{}
	peakHeapInuse  atomic.Uint64
	peakHeapAlloc  atomic.Uint64
	peakSys        atomic.Uint64
	peakStackInuse atomic.Uint64
	once           sync.Once
}

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

// TestMemoryFootprint_Coalesced proves the fix concept: pre-coalesce every K
// rows into a single Arrow Record before BufferManager.Add. The current
// row-at-a-time code path is K=1; raising K to 100 / 1000 / 10000 should
// drop per-row live-heap roughly linearly until per-record overhead becomes
// negligible vs per-row data cost. Gated by DUPLET_MEMBENCH_ROWS; K via
// DUPLET_MEMBENCH_COALESCE_K (default 1, i.e. same as the un-coalesced run).
//
// Example sweep:
//
//	for k in 1 10 100 1000 10000; do
//	  DUPLET_MEMBENCH_ROWS=100000 DUPLET_MEMBENCH_COALESCE_K=$k \
//	    go test -run TestMemoryFootprint_Coalesced -count=1 -v ./pkg/datagateway/buffer
//	done
func TestMemoryFootprint_Coalesced(t *testing.T) {
	n := membenchRowsOrSkip(t)
	k := 1
	if raw := os.Getenv(membenchCoalesceEnvVar); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 {
			t.Fatalf("invalid %s=%q: %v", membenchCoalesceEnvVar, raw, err)
		}
		k = v
	}
	if n%k != 0 {
		t.Fatalf("rows (%d) must be divisible by coalesce K (%d)", n, k)
	}

	tmpDir := t.TempDir()
	cols := []schema.ColumnDef{
		{Name: "id", Type: schema.TypeInt64, Nullable: false},
		{Name: "name", Type: schema.TypeString, Nullable: true},
		{Name: "value", Type: schema.TypeFloat64, Nullable: true},
		{Name: "active", Type: schema.TypeBool, Nullable: true},
		{Name: "ts", Type: schema.TypeTimestamp, Nullable: true},
	}
	s, err := schema.NewSchema(cols)
	if err != nil {
		t.Fatalf("NewSchema: %v", err)
	}
	cfg := &BufferConfig{
		BufferSize:     64 * 1024 * 1024,
		RowGroupSize:   64 * 1024 * 1024,
		TargetFileSize: 128 * 1024 * 1024,
		OutputDir:      tmpDir,
		FilePrefix:     "membench-coalesce",
		Compression:    CompressionSnappy,
	}
	alloc := memory.NewGoAllocator()
	mgr, err := NewBufferManager(s, cfg, alloc, nil)
	if err != nil {
		t.Fatalf("NewBufferManager: %v", err)
	}
	defer mgr.Close()
	arrowSchema := s.ArrowSchema()

	runtime.GC()
	runtime.GC()
	baseline := readMemStats()
	sampler := startMemSampler(10 * time.Millisecond)

	approxPayloadBytesPerRow := int64(8 + 10 + 8 + 1 + 8)
	start := time.Now()
	for batch := 0; batch < n/k; batch++ {
		rec := buildKRows(alloc, arrowSchema, batch*k, k)
		if err := mgr.Add(rec); err != nil {
			t.Fatalf("Add[batch=%d]: %v", batch, err)
		}
		rec.Release()
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
	runtime.KeepAlive(mgr)

	reportScenario(t, fmt.Sprintf("Coalesced(K=%d)", k), n, int64(n)*approxPayloadBytesPerRow, baseline, peak, liveAfter, elapsed)
}

// buildKRows constructs one Arrow Record carrying k rows. Mirrors buildOneRow's
// data shape so per-row payload is identical across K sweeps.
func buildKRows(alloc memory.Allocator, sch *arrow.Schema, startIdx, k int) arrow.Record {
	b := array.NewRecordBuilder(alloc, sch)
	defer b.Release()
	for r := 0; r < k; r++ {
		i := startIdx + r
		for f := 0; f < sch.NumFields(); f++ {
			field := sch.Field(f)
			switch field.Type.ID() {
			case arrow.INT64:
				b.Field(f).(*array.Int64Builder).Append(int64(i))
			case arrow.FLOAT64:
				b.Field(f).(*array.Float64Builder).Append(float64(i) * 1.5)
			case arrow.STRING:
				b.Field(f).(*array.StringBuilder).Append(fmt.Sprintf("user_%05d", i))
			case arrow.BOOL:
				b.Field(f).(*array.BooleanBuilder).Append(i%2 == 0)
			case arrow.TIMESTAMP:
				b.Field(f).(*array.TimestampBuilder).Append(arrow.Timestamp(int64(i) * 1_000_000))
			default:
				b.Field(f).AppendNull()
			}
		}
	}
	return b.NewRecord()
}

// TestEstimatorAccuracy quantifies how much estimateRecordSize undercounts
// the real heap cost of retaining single-row Arrow Records. For each Add, we
// compare the estimator's return value to the actual HeapAlloc delta. Run with
// DUPLET_MEMBENCH_ROWS (capped at 10K for this test to keep it fast).
func TestEstimatorAccuracy(t *testing.T) {
	n := membenchRowsOrSkip(t)
	if n > 10000 {
		n = 10000 // estimator-accuracy test only needs a representative sample
	}
	tmpDir := t.TempDir()
	cols := []schema.ColumnDef{
		{Name: "id", Type: schema.TypeInt64, Nullable: false},
		{Name: "name", Type: schema.TypeString, Nullable: true},
		{Name: "value", Type: schema.TypeFloat64, Nullable: true},
		{Name: "active", Type: schema.TypeBool, Nullable: true},
		{Name: "ts", Type: schema.TypeTimestamp, Nullable: true},
	}
	s, _ := schema.NewSchema(cols)
	cfg := &BufferConfig{
		BufferSize:     1024 * 1024 * 1024, // 1 GiB — large enough that we never auto-flush during this test
		RowGroupSize:   1024 * 1024 * 1024,
		TargetFileSize: 1024 * 1024 * 1024,
		OutputDir:      tmpDir,
		FilePrefix:     "membench-estimator",
		Compression:    CompressionSnappy,
	}
	alloc := memory.NewGoAllocator()
	mgr, err := NewBufferManager(s, cfg, alloc, nil)
	if err != nil {
		t.Fatalf("NewBufferManager: %v", err)
	}
	defer mgr.Close()
	arrowSchema := s.ArrowSchema()

	runtime.GC()
	runtime.GC()
	pre := readMemStats()

	var totalEstimated int64
	for i := 0; i < n; i++ {
		rec := buildOneRow(alloc, arrowSchema, i)
		totalEstimated += estimateRecordSize(rec)
		if err := mgr.Add(rec); err != nil {
			t.Fatalf("Add[%d]: %v", i, err)
		}
		rec.Release()
	}
	// HeapAlloc grows roughly monotonically here (no flush, GC happens only
	// for unreachable transients; the retained records stay live).
	post := readMemStats()
	runtime.KeepAlive(mgr)

	actualDelta := safeSub(post.HeapAlloc, pre.HeapAlloc)
	undercount := float64(actualDelta) / float64(totalEstimated)
	t.Logf("\n=== estimator accuracy rows=%d ===", n)
	t.Logf("  estimateRecordSize sum  = %s (%d B)", fmtBytes(totalEstimated), totalEstimated)
	t.Logf("  actual HeapAlloc delta  = %s (%d B)", fmtBytes(int64(actualDelta)), actualDelta)
	t.Logf("  undercount factor       = %.2fx (real heap / estimator)", undercount)
	t.Logf("  per-row estimator       = %.1f B/row", float64(totalEstimated)/float64(n))
	t.Logf("  per-row real            = %.1f B/row", float64(actualDelta)/float64(n))
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
