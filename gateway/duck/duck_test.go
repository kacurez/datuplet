//go:build duckdb_arrow

package duck

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	bindings "github.com/duckdb/duckdb-go-bindings"

	// Keeps the duckdb-go/v2 pin in go.mod: scripts/verify-versions.sh checks
	// it for parity with the other DuckDB modules.
	_ "github.com/duckdb/duckdb-go/v2"
)

const streamQuery = "SELECT range AS i, repeat('x', 100) AS pad FROM range(3_000_000)"

const streamRows = 3_000_000

func testSettings(t testing.TB) Settings {
	return Settings{
		Threads:        1,
		MemoryLimit:    "4GiB",
		TempDir:        t.TempDir(),
		MaxTempDirSize: "1GiB",
	}
}

func openConn(t testing.TB) (*DB, *Conn) {
	t.Helper()
	db, err := Open(testSettings(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	c, err := db.Connect()
	if err != nil {
		db.Close()
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Conn.Close: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Errorf("DB.Close: %v", err)
		}
	})
	return db, c
}

func scalarInt(t *testing.T, c *Conn, sql string) int64 {
	t.Helper()
	rows, err := c.QueryRows(context.Background(), sql)
	if err != nil {
		t.Fatalf("QueryRows(%q): %v", sql, err)
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("QueryRows(%q): want 1x1, got %v", sql, rows)
	}
	switch v := rows[0][0].(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	default:
		t.Fatalf("QueryRows(%q): value %T(%v) is not an integer", sql, v, v)
		return 0
	}
}

func TestStreamingQueryIsStreaming(t *testing.T) {
	_, c := openConn(t)
	s, err := c.QueryStream(context.Background(), streamQuery)
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	defer s.Release()
	if !s.IsStreaming() {
		t.Fatal("IsStreaming() = false, want a streaming result")
	}
	var rows int64
	for {
		rec, err := s.NextCtx(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("NextCtx: %v", err)
		}
		rows += rec.NumRows()
	}
	if rows != streamRows {
		t.Fatalf("rows = %d, want %d", rows, streamRows)
	}
	if hw := s.maxHeldBatches(); hw > 2 {
		t.Fatalf("Go-side retained batches high-water = %d, want <= 2", hw)
	}
}

// TestHelperStreamDrain is not a test on its own: TestStreamingQueryRSSBounded
// re-executes the test binary with DUCK_RSS_HELPER set so each drain runs in a
// fresh process whose peak RSS is measurable.
func TestHelperStreamDrain(t *testing.T) {
	mode := os.Getenv("DUCK_RSS_HELPER")
	if mode == "" {
		t.Skip("helper process for TestStreamingQueryRSSBounded")
	}
	_, c := openConn(t)
	var rows int64
	var err error
	switch mode {
	case "stream":
		rows, err = drainStreaming(c)
	case "materialized":
		rows, err = drainMaterialized(c)
	default:
		t.Fatalf("unknown DUCK_RSS_HELPER mode %q", mode)
	}
	if err != nil {
		t.Fatalf("%s drain: %v", mode, err)
	}
	if rows != streamRows {
		t.Fatalf("%s drain: rows = %d, want %d", mode, rows, streamRows)
	}
}

func drainStreaming(c *Conn) (int64, error) {
	s, err := c.QueryStream(context.Background(), streamQuery)
	if err != nil {
		return 0, err
	}
	defer s.Release()
	var rows int64
	for {
		rec, err := s.NextCtx(context.Background())
		if errors.Is(err, io.EOF) {
			return rows, nil
		}
		if err != nil {
			return rows, err
		}
		rows += rec.NumRows()
	}
}

// drainMaterialized is the comparison baseline: the same C calls with
// PendingPrepared (materialized result) instead of PendingPreparedStreaming.
func drainMaterialized(c *Conn) (int64, error) {
	var ps bindings.PreparedStatement
	if bindings.Prepare(c.conn, streamQuery, &ps) == bindings.StateError {
		defer bindings.DestroyPrepare(&ps)
		return 0, errors.New(bindings.PrepareError(ps))
	}
	defer bindings.DestroyPrepare(&ps)
	var pend bindings.PendingResult
	if bindings.PendingPrepared(ps, &pend) == bindings.StateError {
		defer bindings.DestroyPending(&pend)
		return 0, errors.New(bindings.PendingError(pend))
	}
	var res bindings.Result
	state := bindings.ExecutePending(pend, &res)
	bindings.DestroyPending(&pend)
	defer bindings.DestroyResult(&res)
	if state == bindings.StateError {
		return 0, errors.New(bindings.ResultError(&res))
	}
	opts := bindings.ResultGetArrowOptions(&res)
	defer bindings.DestroyArrowOptions(&opts)
	schema, err := resultArrowSchema(&res, opts)
	if err != nil {
		return 0, err
	}
	var rows int64
	for {
		ch := bindings.FetchChunk(res)
		if ch.Ptr == nil {
			return rows, nil
		}
		n := int64(bindings.DataChunkGetSize(ch))
		if n == 0 {
			bindings.DestroyDataChunk(&ch)
			return rows, nil
		}
		rec, ed := bindings.DataChunkToArrowArray(opts, schema, ch)
		bindings.DestroyDataChunk(&ch)
		if err := errorDataErr(ed); err != nil {
			return rows, err
		}
		rows += rec.NumRows()
		rec.Release()
	}
}

func childPeakRSS(t *testing.T, mode string) int64 {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperStreamDrain$", "-test.count=1")
	cmd.Env = append(os.Environ(), "DUCK_RSS_HELPER="+mode)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s helper: %v\n%s", mode, err, out)
	}
	if strings.Contains(string(out), "SKIP") || !strings.Contains(string(out), "PASS") {
		t.Fatalf("%s helper did not run the drain:\n%s", mode, out)
	}
	ru, ok := cmd.ProcessState.SysUsage().(*syscall.Rusage)
	if !ok {
		t.Fatalf("SysUsage is %T, want *syscall.Rusage", cmd.ProcessState.SysUsage())
	}
	maxrss := int64(ru.Maxrss)
	if runtime.GOOS == "linux" {
		maxrss *= 1024 // KiB on linux, bytes on darwin
	}
	return maxrss
}

func TestStreamingQueryRSSBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns two helper processes")
	}
	streaming := childPeakRSS(t, "stream")
	materialized := childPeakRSS(t, "materialized")
	t.Logf("peak RSS: streaming=%d MiB materialized=%d MiB", streaming>>20, materialized>>20)
	if float64(streaming) >= 0.5*float64(materialized) {
		t.Fatalf("streaming peak RSS %d MiB is not < 0.5 x materialized %d MiB", streaming>>20, materialized>>20)
	}
}

func TestStreamEarlyRelease(t *testing.T) {
	_, c := openConn(t)
	s, err := c.QueryStream(context.Background(), streamQuery)
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := s.NextCtx(context.Background()); err != nil {
			t.Fatalf("NextCtx #%d: %v", i, err)
		}
	}
	s.Release()
	s.Release() // idempotent
	if _, err := s.NextCtx(context.Background()); err == nil {
		t.Fatal("NextCtx after Release: want error")
	}
	if got := scalarInt(t, c, "SELECT 42"); got != 42 {
		t.Fatalf("query after early release = %d, want 42", got)
	}
}

func TestStreamCancelMidFetch(t *testing.T) {
	_, c := openConn(t)
	s, err := c.QueryStream(context.Background(), streamQuery)
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < 2; i++ {
		if _, err := s.NextCtx(ctx); err != nil {
			t.Fatalf("NextCtx #%d: %v", i, err)
		}
	}
	cancel()
	start := time.Now()
	_, err = s.NextCtx(ctx)
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("NextCtx after cancel = %v, want a non-EOF error", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("NextCtx after cancel = %v, want it to wrap context.Canceled", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("NextCtx after cancel took %v, want <= 2s", d)
	}
	if _, err2 := s.NextCtx(context.Background()); err2 == nil {
		t.Fatal("NextCtx after a failed fetch: want the sticky error")
	}
	s.Release()
	if got := scalarInt(t, c, "SELECT 7"); got != 7 {
		t.Fatalf("query after cancelled stream = %d, want 7", got)
	}
}

func TestStreamCancelDuringFetch(t *testing.T) {
	_, c := openConn(t)
	// A blocking operator: the first chunk is only produced after the full
	// aggregate, so the cancel lands while FetchChunk is inside DuckDB.
	s, err := c.QueryStream(context.Background(),
		"SELECT sum(hash(range)) FROM range(20_000_000_000)")
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	defer s.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = s.NextCtx(ctx)
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("NextCtx under deadline = %v, want a non-EOF error", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("NextCtx under deadline took %v, want <= 2s", d)
	}
	s.Release()
	if got := scalarInt(t, c, "SELECT 9"); got != 9 {
		t.Fatalf("query after interrupted fetch = %d, want 9", got)
	}
}

func TestStreamReleaseDuringFetch(t *testing.T) {
	_, c := openConn(t)
	s, err := c.QueryStream(context.Background(),
		"SELECT sum(hash(range)) FROM range(20_000_000_000)")
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	fetchErr := make(chan error, 1)
	go func() {
		_, err := s.NextCtx(context.Background())
		fetchErr <- err
	}()
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	s.Release()
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Release during fetch took %v, want <= 2s", d)
	}
	if err := <-fetchErr; err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("in-flight NextCtx = %v, want a non-EOF error", err)
	}
	if got := scalarInt(t, c, "SELECT 11"); got != 11 {
		t.Fatalf("query after release during fetch = %d, want 11", got)
	}
}

func TestStreamBlocksOtherStatements(t *testing.T) {
	_, c := openConn(t)
	s, err := c.QueryStream(context.Background(), streamQuery)
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	if err := c.Exec(context.Background(), "SELECT 1"); !errors.Is(err, ErrStreamOpen) {
		t.Fatalf("Exec with an open stream = %v, want ErrStreamOpen", err)
	}
	if _, err := c.QueryStream(context.Background(), "SELECT 1"); !errors.Is(err, ErrStreamOpen) {
		t.Fatalf("second QueryStream = %v, want ErrStreamOpen", err)
	}
	s.Release()
	if err := c.Exec(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("Exec after Release: %v", err)
	}
}

func threeBatchReader(t *testing.T, n int) array.RecordReader {
	t.Helper()
	return threeBatchReaderAlloc(t, n, memory.DefaultAllocator)
}

func threeBatchReaderAlloc(t *testing.T, n int, mem memory.Allocator) array.RecordReader {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	var recs []arrow.RecordBatch
	for b := 0; b < 3; b++ {
		bld := array.NewRecordBuilder(mem, schema)
		for i := 0; i < n; i++ {
			bld.Field(0).(*array.Int64Builder).Append(int64(b*n + i))
			bld.Field(1).(*array.StringBuilder).Append("row-" + strconv.Itoa(b*n+i))
		}
		recs = append(recs, bld.NewRecordBatch())
		bld.Release()
	}
	rr, err := array.NewRecordReader(schema, recs)
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	for _, r := range recs {
		r.Release()
	}
	return rr
}

func TestRegisterArrowViewCopyToParquet(t *testing.T) {
	_, c := openConn(t)
	const n = 1000
	rr := threeBatchReader(t, n)
	defer rr.Release()
	release, err := c.RegisterArrowView("v", rr)
	if err != nil {
		t.Fatalf("RegisterArrowView: %v", err)
	}
	defer release()
	out := filepath.Join(t.TempDir(), "x.parquet")
	rows, err := c.QueryRows(context.Background(), fmt.Sprintf(
		"COPY (SELECT * FROM v) TO %s (FORMAT parquet, RETURN_STATS true)", Literal(out)))
	if err != nil {
		t.Fatalf("COPY: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("COPY stats rows = %d, want 1: %v", len(rows), rows)
	}
	// RETURN_STATS columns: filename, count, file_size_bytes, ...
	if got, ok := rows[0][1].(uint64); !ok || got != 3*n {
		t.Fatalf("COPY stats count = %T(%v), want uint64(%d)", rows[0][1], rows[0][1], 3*n)
	}
	if fi, err := os.Stat(out); err != nil || fi.Size() == 0 {
		t.Fatalf("parquet file: %v (%v)", fi, err)
	}
	if got := scalarInt(t, c, "SELECT count(*) FROM read_parquet("+Literal(out)+")"); got != 3*n {
		t.Fatalf("read back %d rows, want %d", got, 3*n)
	}
}

func TestInterruptCancelsQuery(t *testing.T) {
	_, c := openConn(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := c.Exec(ctx, "SELECT sum(hash(range)) FROM range(20_000_000_000)")
	if err == nil {
		t.Fatal("long query under a 300ms deadline: want error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want it to wrap context.DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("cancel took %v, want <= 2s", d)
	}
	if got := scalarInt(t, c, "SELECT 3"); got != 3 {
		t.Fatalf("query after interrupt = %d, want 3", got)
	}
}

func TestInterruptFromOtherGoroutine(t *testing.T) {
	_, c := openConn(t)
	go func() {
		time.Sleep(300 * time.Millisecond)
		c.Interrupt()
	}()
	start := time.Now()
	if err := c.Exec(context.Background(), "SELECT sum(hash(range)) FROM range(20_000_000_000)"); err == nil {
		t.Fatal("interrupted query: want error")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Interrupt took %v, want <= 2s", d)
	}
	if got := scalarInt(t, c, "SELECT 5"); got != 5 {
		t.Fatalf("query after Interrupt = %d, want 5", got)
	}
}

func TestLockBlocksSet(t *testing.T) {
	db, c := openConn(t)
	if err := c.Exec(context.Background(), "SET threads=2"); err != nil {
		t.Fatalf("SET before Lock: %v", err)
	}
	if err := db.Lock(c); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := c.Exec(context.Background(), "SET threads=4"); err == nil {
		t.Fatal("SET threads=4 after Lock: want error")
	}
	if err := c.Exec(context.Background(), "SET autoload_known_extensions=true"); err == nil {
		t.Fatal("SET autoload_known_extensions after Lock: want error")
	}
	if got := scalarInt(t, c, "SELECT 1"); got != 1 {
		t.Fatalf("query after Lock = %d, want 1", got)
	}
}

func TestLockFailureLeavesDBUnusable(t *testing.T) {
	db, c := openConn(t)
	if err := db.Lock(c); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	// Configuration is locked now, so a second lockdown fails on its first SET.
	if err := db.Lock(c); err == nil {
		t.Fatal("second Lock: want error")
	}
	if err := c.Exec(context.Background(), "SELECT 1"); !errors.Is(err, ErrUnusable) {
		t.Fatalf("Exec after failed Lock = %v, want ErrUnusable", err)
	}
	if _, err := db.Connect(); !errors.Is(err, ErrUnusable) {
		t.Fatalf("Connect after failed Lock = %v, want ErrUnusable", err)
	}
}

func TestOpenAppliesSettings(t *testing.T) {
	_, c := openConn(t)
	rows, err := c.QueryRows(context.Background(),
		"SELECT current_setting('threads'), current_setting('preserve_insertion_order'), "+
			"current_setting('enable_external_file_cache')")
	if err != nil {
		t.Fatalf("QueryRows: %v", err)
	}
	got := fmt.Sprint(rows[0]...)
	if want := fmt.Sprint(int64(1), false, false); got != want {
		t.Fatalf("settings = %q, want %q", got, want)
	}
}

func TestExecRedactedHidesSQL(t *testing.T) {
	_, c := openConn(t)
	err := c.ExecRedacted(context.Background(),
		"CREATE SECRET s (TYPE S3, KEY_ID 'SECRET-VALUE', SECRET 'SECRET-VALUE', BOGUS_OPTION 'SECRET-VALUE')",
		"create secret")
	if err == nil {
		t.Fatal("ExecRedacted on a failing statement: want error")
	}
	if strings.Contains(err.Error(), "SECRET-VALUE") {
		t.Fatalf("redacted error leaks the statement: %q", err)
	}
	if err.Error() != "duck: create secret failed" {
		t.Fatalf("redacted error = %q, want %q", err, "duck: create secret failed")
	}
	// Parser errors echo statement fragments; they must be redacted too.
	err = c.ExecRedacted(context.Background(), "SELEC 'SECRET-VALUE'", "probe")
	if err == nil || strings.Contains(err.Error(), "SECRET-VALUE") {
		t.Fatalf("redacted parser error = %v", err)
	}
}

func TestExecRejectsMultipleStatements(t *testing.T) {
	_, c := openConn(t)
	if err := c.Exec(context.Background(), "SELECT 1; SET threads=8"); err == nil {
		t.Fatal("Exec with two statements: want error")
	}
	if got := scalarInt(t, c, "SELECT current_setting('threads')"); got != 1 {
		t.Fatalf("threads = %d after a rejected multi-statement, want 1", got)
	}
}

func TestQueryRowsCopiesValues(t *testing.T) {
	_, c := openConn(t)
	rows, err := c.QueryRows(context.Background(),
		"SELECT 'plain-string-value' AS s, {'k': 'nested-string-value', 'b': 'nested-blob'::BLOB} AS st, NULL AS n")
	if err != nil {
		t.Fatalf("QueryRows: %v", err)
	}
	// Churn C and Go memory so dangling references into freed batches show.
	for i := 0; i < 20; i++ {
		if _, err := c.QueryRows(context.Background(), "SELECT repeat('z', 1000) FROM range(2000)"); err != nil {
			t.Fatalf("churn: %v", err)
		}
	}
	if got := rows[0][0]; got != "plain-string-value" {
		t.Fatalf("string value = %q", got)
	}
	st, ok := rows[0][1].(map[string]any)
	if !ok {
		t.Fatalf("struct value = %T(%v)", rows[0][1], rows[0][1])
	}
	if got := st["k"]; got != "nested-string-value" {
		t.Fatalf("nested string = %q", got)
	}
	if got, _ := st["b"].([]byte); string(got) != "nested-blob" {
		t.Fatalf("nested blob = %q", got)
	}
	if rows[0][2] != nil {
		t.Fatalf("NULL = %v, want nil", rows[0][2])
	}
}

// TestMain fails the run if any DuckDB handle (result, prepared statement,
// chunk, connection, ...) leaked. The counters are only maintained when built
// with -tags duckdb_arrow,debug_bindings; otherwise this check is a no-op.
func TestMain(m *testing.M) {
	code := m.Run()
	if leaks := bindings.GetAllocationCounts(); leaks != "" && code == 0 {
		fmt.Fprintln(os.Stderr, "duckdb handle leak:\n"+leaks)
		code = 1
	}
	os.Exit(code)
}

// TestStreamReleaseWhileFetchEntering pins the interleaving where NextCtx
// holds the stream lock but has not started the fetch when Release begins:
// Release must still make that fetch stop instead of waiting for the whole
// query.
func TestStreamReleaseWhileFetchEntering(t *testing.T) {
	_, c := openConn(t)
	s, err := c.QueryStream(context.Background(),
		"SELECT sum(hash(range)) FROM range(20_000_000_000)")
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	entered := make(chan struct{})
	proceed := make(chan struct{})
	testHookBeforeFetch = func() {
		close(entered)
		<-proceed
	}
	t.Cleanup(func() { testHookBeforeFetch = nil })

	fetchErr := make(chan error, 1)
	go func() {
		_, err := s.NextCtx(context.Background())
		fetchErr <- err
	}()
	<-entered
	released := make(chan struct{})
	go func() {
		s.Release()
		close(released)
	}()
	<-s.closing.Done() // Release has published closing and waits for the lock
	close(proceed)

	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("Release did not return within 2s of an entering fetch")
	}
	if err := <-fetchErr; !errors.Is(err, ErrStreamReleased) {
		t.Fatalf("entering NextCtx = %v, want ErrStreamReleased", err)
	}
	testHookBeforeFetch = nil
	if got := scalarInt(t, c, "SELECT 13"); got != 13 {
		t.Fatalf("query after release = %d, want 13", got)
	}
}

func pendingDropCount(c *Conn) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pendingDrops)
}

func liveViewCount(c *Conn) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.views)
}

func copyView(t *testing.T, c *Conn, view string) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "v.parquet")
	if _, err := c.QueryRows(context.Background(), fmt.Sprintf(
		"COPY (SELECT * FROM %s) TO %s (FORMAT parquet)", Ident(view), Literal(out))); err != nil {
		t.Fatalf("COPY: %v", err)
	}
}

// scanReleased scans a view that survived its release (DROP failed or rolled
// back). It must fail cleanly, never read freed memory.
func scanReleased(t *testing.T, c *Conn, view string) {
	t.Helper()
	_, err := c.QueryRows(context.Background(), "SELECT count(*) FROM "+Ident(view))
	if err == nil {
		t.Fatalf("scan of released view %s: want error", view)
	}
}

func TestArrowViewReleaseFreesReader(t *testing.T) {
	_, c := openConn(t)
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	rr := threeBatchReaderAlloc(t, 100, mem)
	release, err := c.RegisterArrowView("v", rr)
	if err != nil {
		t.Fatalf("RegisterArrowView: %v", err)
	}
	rr.Release() // the view holds its own reference
	copyView(t, c, "v")
	if mem.CurrentAlloc() == 0 {
		t.Fatal("reader freed while the view still references it")
	}
	release()
	// The C stream struct stays until Close; the reader and batches do not.
	if n := liveViewCount(c); n != 1 {
		t.Fatalf("live views = %d, want 1 (C stream kept until Close)", n)
	}
	mem.AssertSize(t, 0)
}

func TestArrowViewReleaseWhileStreamOpen(t *testing.T) {
	_, c := openConn(t)
	rr := threeBatchReader(t, 100)
	defer rr.Release()
	release, err := c.RegisterArrowView("v", rr)
	if err != nil {
		t.Fatalf("RegisterArrowView: %v", err)
	}
	copyView(t, c, "v")
	s, err := c.QueryStream(context.Background(), "SELECT * FROM range(10)")
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	release() // DROP cannot run while s is open
	if n := pendingDropCount(c); n != 1 {
		t.Fatalf("pending drops = %d, want 1", n)
	}
	s.Release()
	// The next statement drops the view first: it is gone, never dangling.
	_, err = c.QueryRows(context.Background(), "SELECT count(*) FROM v")
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("query of a released view = %v, want a does-not-exist error", err)
	}
	if n := pendingDropCount(c); n != 0 {
		t.Fatalf("pending drops = %d after the next statement, want 0", n)
	}
}

func TestArrowViewDropFailureKeepsStream(t *testing.T) {
	_, c := openConn(t)
	rr := threeBatchReader(t, 100)
	defer rr.Release()
	release, err := c.RegisterArrowView("v", rr)
	if err != nil {
		t.Fatalf("RegisterArrowView: %v", err)
	}
	copyView(t, c, "v")
	testHookDropView = func(string) error { return errors.New("injected drop failure") }
	t.Cleanup(func() { testHookDropView = nil })
	release()
	if n := pendingDropCount(c); n != 1 {
		t.Fatalf("pending drops after a failed DROP = %d, want 1", n)
	}
	scanReleased(t, c, "v") // the view survived; its stream is still allocated
	if n := pendingDropCount(c); n != 1 {
		t.Fatalf("pending drops after a second failed DROP = %d, want 1", n)
	}
	testHookDropView = nil
	_, err = c.QueryRows(context.Background(), "SELECT count(*) FROM v")
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("query after the retried DROP = %v, want a does-not-exist error", err)
	}
	if n := pendingDropCount(c); n != 0 {
		t.Fatalf("pending drops after the retried DROP = %d, want 0", n)
	}
}

func TestArrowViewDropFailureInAbortedTransaction(t *testing.T) {
	_, c := openConn(t)
	rr := threeBatchReader(t, 100)
	defer rr.Release()
	release, err := c.RegisterArrowView("v", rr)
	if err != nil {
		t.Fatalf("RegisterArrowView: %v", err)
	}
	copyView(t, c, "v")
	if err := c.Exec(context.Background(), "BEGIN TRANSACTION"); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	if err := c.Exec(context.Background(), "SELECT error('abort the transaction')"); err == nil {
		t.Fatal("error(): want error")
	}
	release() // DROP fails: the transaction is aborted
	if n := pendingDropCount(c); n != 1 {
		t.Fatalf("pending drops in an aborted transaction = %d, want 1", n)
	}
	if err := c.Exec(context.Background(), "ROLLBACK"); err != nil {
		t.Fatalf("ROLLBACK: %v", err)
	}
	if _, err := c.QueryRows(context.Background(), "SELECT count(*) FROM v"); err == nil {
		t.Fatal("view still reachable after ROLLBACK and the retried DROP")
	}
	if n := pendingDropCount(c); n != 0 {
		t.Fatalf("pending drops = %d, want 0", n)
	}
}

// TestArrowViewReleaseInsideRolledBackTransaction is the review-2 round-1
// scenario: the DROP runs inside the caller's transaction, ROLLBACK restores
// the view, and the restored view must not point at freed memory.
func TestArrowViewReleaseInsideRolledBackTransaction(t *testing.T) {
	_, c := openConn(t)
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	rr := threeBatchReaderAlloc(t, 100, mem)
	release, err := c.RegisterArrowView("v", rr)
	if err != nil {
		t.Fatalf("RegisterArrowView: %v", err)
	}
	rr.Release()
	if err := c.Exec(context.Background(), "BEGIN TRANSACTION"); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	s, err := c.QueryStream(context.Background(), "SELECT * FROM range(10)")
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	release() // DROP deferred: the stream is open
	s.Release()
	// The pre-statement cleanup DROPs the view inside the transaction.
	if got := scalarInt(t, c, "SELECT 1"); got != 1 {
		t.Fatalf("SELECT 1 = %d", got)
	}
	if n := pendingDropCount(c); n != 0 {
		t.Fatalf("pending drops after the in-transaction DROP = %d, want 0", n)
	}
	if err := c.Exec(context.Background(), "ROLLBACK"); err != nil {
		t.Fatalf("ROLLBACK: %v", err)
	}
	// ROLLBACK restored the view. Its C stream is still allocated (until
	// Close) and detached from the reader: the scan fails cleanly.
	if n := liveViewCount(c); n != 1 {
		t.Fatalf("live views = %d, want 1", n)
	}
	_, err = c.QueryRows(context.Background(), "SELECT count(*) FROM v")
	if err == nil || !strings.Contains(err.Error(), errViewReleased.Error()) {
		t.Fatalf("scan of the restored view = %v, want %q", err, errViewReleased)
	}
	mem.AssertSize(t, 0) // the reader was released at release(), not at Close
	if got := scalarInt(t, c, "SELECT 2"); got != 2 {
		t.Fatalf("query after the failed scan = %d, want 2", got)
	}
}

func TestArrowViewFreedAtClose(t *testing.T) {
	db, err := Open(testSettings(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	c, err := db.Connect()
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	rr := threeBatchReaderAlloc(t, 10, mem)
	r1, err := c.RegisterArrowView("v1", rr)
	if err != nil {
		t.Fatalf("RegisterArrowView v1: %v", err)
	}
	r2, err := c.RegisterArrowView("v2", rr)
	if err != nil {
		t.Fatalf("RegisterArrowView v2: %v", err)
	}
	rr.Release()
	testHookDropView = func(string) error { return errors.New("injected drop failure") }
	t.Cleanup(func() { testHookDropView = nil })
	r1()
	if n := liveViewCount(c); n != 2 {
		t.Fatalf("live views = %d, want 2", n)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n, d := liveViewCount(c), pendingDropCount(c); n != 0 || d != 0 {
		t.Fatalf("after Close: live views = %d, pending drops = %d, want 0, 0", n, d)
	}
	mem.AssertSize(t, 0) // v2 was never released by its caller; Close freed it
	r2()                 // after Close: a no-op
}
