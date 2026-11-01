//go:build duckdb_arrow

package duckplane

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/datuplet/datuplet/gateway/duck"
	"github.com/datuplet/datuplet/pkg/datagateway"
)

// testJWT is shape-valid (three base64url segments); never sent anywhere.
const testJWT = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ0In0.c2ln"

// memRows is the row count of the in-memory test table: many DuckDB chunks.
const memRows = 100_000

// memCatalogPlane is a locked plane whose catalog ATTACH is replaced by an
// in-memory DuckDB database named lk holding ns.tbl (id BIGINT, name VARCHAR)
// with memRows rows and a view ns.slow whose first fetch blocks for minutes. The JWT shape check still runs.
func memCatalogPlane(t *testing.T, ns, tbl string) (*Plane, *atomic.Int32) {
	t.Helper()
	p := testPlane(t)
	p.cfg.Catalog = CatalogAttach{URL: "http://127.0.0.1:1/catalog", ProjectID: "p", Warehouse: "w", Token: testJWT}
	var attaches atomic.Int32
	p.attachHook = func(ctx context.Context, c *duck.Conn) error {
		attaches.Add(1)
		for _, s := range []string{
			"ATTACH ':memory:' AS lk",
			"CREATE SCHEMA lk." + duck.Ident(ns),
			"CREATE TABLE lk." + duck.Ident(ns) + "." + duck.Ident(tbl) +
				" AS SELECT range AS id, 'row-' || range AS name FROM range(" + strconv.Itoa(memRows) + ")",
			// A blocking aggregate: its first fetch stays inside DuckDB.
			"CREATE VIEW lk." + duck.Ident(ns) + ".slow AS SELECT sum(hash(range)) AS s FROM range(20_000_000_000)",
		} {
			if err := c.Exec(ctx, s); err != nil {
				return err
			}
		}
		return nil
	}
	return p, &attaches
}

// drain reads r to the end and returns the row count.
func drain(t *testing.T, r datagateway.RecordStream) int64 {
	t.Helper()
	var rows int64
	for {
		rec, err := r.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return rows
		}
		if err != nil {
			t.Fatalf("Next after %d rows: %v", rows, err)
		}
		rows += rec.NumRows()
		rec.Release()
	}
}

// Zoned timestamps are read as tz=UTC whatever the host's zone.
func TestReaderTimestampZoneIsUTC(t *testing.T) {
	t.Setenv("TZ", "Europe/Prague")
	p := testPlane(t)
	c := queryConn(t, p)
	s, err := c.QueryStream(context.Background(), "SELECT TIMESTAMPTZ '2026-10-31 12:00:00+00' AS ts")
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	defer s.Release()
	want := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	if got := s.Schema().Field(0).Type; !arrow.TypeEqual(got, want) {
		t.Fatalf("type = %s, want %s", got, want)
	}
}

func TestReaderRequiresCatalog(t *testing.T) {
	p := testPlane(t)
	_, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"})
	if err == nil || !strings.Contains(err.Error(), "reads require a catalog") {
		t.Fatalf("OpenTableReader = %v, want 'reads require a catalog'", err)
	}
}

func TestReaderRejectsMalformedToken(t *testing.T) {
	p, attaches := memCatalogPlane(t, "n", "t")
	const bad = "not-a-jwt'); DROP TABLE x; --"
	p.cfg.Catalog.Token = bad
	_, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"})
	if err == nil || strings.Contains(err.Error(), bad) || strings.Contains(err.Error(), "DROP") {
		t.Fatalf("OpenTableReader = %v, want a fixed error without the token", err)
	}
	if n := attaches.Load(); n != 0 {
		t.Fatalf("attach ran %d times for a malformed token", n)
	}
}

// Without the iceberg extension CREATE SECRET (TYPE ICEBERG …) fails: the
// error is fixed (no token, no SQL) and cached for the plane's lifetime.
func TestReaderAttachErrorRedactedAndCached(t *testing.T) {
	p := testPlane(t)
	p.cfg.Catalog = CatalogAttach{URL: "http://127.0.0.1:1/catalog", ProjectID: "p", Warehouse: "w", Token: testJWT}
	_, err1 := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"})
	if err1 == nil {
		t.Fatal("OpenTableReader succeeded without the iceberg extension")
	}
	for _, leak := range []string{testJWT, "eyJ", "SECRET", "TOKEN"} {
		if strings.Contains(err1.Error(), leak) {
			t.Fatalf("error %q contains %q", err1, leak)
		}
	}
	_, err2 := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"})
	if err2 == nil || err2.Error() != err1.Error() {
		t.Fatalf("second OpenTableReader = %v, want cached %v", err2, err1)
	}
	c := queryConn(t, p)
	if n := scalar(t, c, "SELECT count(*) FROM duckdb_secrets()"); n != 0 {
		t.Fatalf("%d secrets left after a failed attach", n)
	}
}

// A cancelled call ctx does not cache an attach failure: the next call
// attaches.
func TestReaderAttachCancelledNotCached(t *testing.T) {
	p, attaches := memCatalogPlane(t, "n", "t")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.OpenTableReader(ctx, datagateway.ReaderSpec{Namespace: "n", Table: "t"}); err == nil {
		t.Fatal("OpenTableReader with a cancelled ctx succeeded")
	}
	r, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"})
	if err != nil {
		t.Fatalf("OpenTableReader after a cancelled attempt: %v", err)
	}
	defer r.Close()
	if got := drain(t, r); got != memRows {
		t.Fatalf("rows = %d", got)
	}
	n := attaches.Load()
	r2, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"})
	if err != nil {
		t.Fatalf("third OpenTableReader: %v", err)
	}
	r2.Close()
	if attaches.Load() != n {
		t.Fatal("a successful attach was not cached")
	}
}

func TestReaderStreamsTable(t *testing.T) {
	p, attaches := memCatalogPlane(t, "n", "t")
	r, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"})
	if err != nil {
		t.Fatalf("OpenTableReader: %v", err)
	}
	defer r.Close()
	sch := r.Schema()
	if sch.NumFields() != 2 || sch.Field(0).Name != "id" || sch.Field(1).Name != "name" ||
		!arrow.TypeEqual(sch.Field(0).Type, arrow.PrimitiveTypes.Int64) ||
		!arrow.TypeEqual(sch.Field(1).Type, arrow.BinaryTypes.String) {
		t.Fatalf("schema = %s", sch)
	}
	if !r.(*reader).stream.IsStreaming() {
		t.Fatal("reader result is not streaming")
	}
	if got := drain(t, r); got != memRows {
		t.Fatalf("rows = %d, want %d", got, memRows)
	}
	if _, err := r.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after EOF = %v", err)
	}
	// A second reader reuses the attach.
	r2, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"})
	if err != nil {
		t.Fatalf("second OpenTableReader: %v", err)
	}
	if got := drain(t, r2); got != memRows {
		t.Fatalf("second reader rows = %d", got)
	}
	if err := r2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := attaches.Load(); n != 1 {
		t.Fatalf("attach ran %d times, want 1", n)
	}
}

// drainHoldingPrevious drains r keeping the previous batch while fetching
// the next (the worst case of a chunker) and returns rows and the high-water
// count of live handed-out batches.
func drainHoldingPrevious(t *testing.T, r *reader) (int64, int64) {
	t.Helper()
	var rows int64
	var prev arrow.RecordBatch
	for {
		rec, err := r.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		rows += rec.NumRows()
		if prev != nil {
			prev.Release()
		}
		prev = rec
	}
	if prev != nil {
		prev.Release()
	}
	return rows, r.liveHighWater()
}

// A caller keeping the previous batch while fetching the next has at most two
// alive. Handed-out batches own their buffers: one kept across the whole
// stream and Close is still readable.
func TestReaderBatchOwnership(t *testing.T) {
	p, _ := memCatalogPlane(t, "n", "t")
	rs, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"})
	if err != nil {
		t.Fatalf("OpenTableReader: %v", err)
	}
	r := rs.(*reader)
	first, err := r.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	rows, hw := drainHoldingPrevious(t, r)
	if rows+first.NumRows() != memRows {
		t.Fatalf("rows = %d", rows+first.NumRows())
	}
	if hw > 3 { // first + previous + current
		t.Fatalf("batches alive at once = %d, want <= 3", hw)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ids := first.Column(0).(*array.Int64)
	names := first.Column(1).(*array.String)
	for i := 0; i < ids.Len(); i++ {
		if names.Value(i) != "row-"+strconv.FormatInt(ids.Value(i), 10) {
			t.Fatalf("row %d: %d %q after Close", i, ids.Value(i), names.Value(i))
		}
	}
	first.Release()
	if n := r.live.Load(); n != 0 {
		t.Fatalf("%d batches still counted alive", n)
	}

	rs2, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"})
	if err != nil {
		t.Fatalf("OpenTableReader: %v", err)
	}
	defer rs2.Close()
	rows, hw = drainHoldingPrevious(t, rs2.(*reader))
	if rows != memRows {
		t.Fatalf("rows = %d", rows)
	}
	if hw > 2 {
		t.Fatalf("batches alive at once = %d, want <= 2", hw)
	}
}

// Review Focus 1: hostile namespace and table names reach SQL only through
// Ident.
func TestReaderHostileNames(t *testing.T) {
	ns, tbl := "ns\"x';\n--", "t\"x'y; DROP TABLE z"
	p, _ := memCatalogPlane(t, ns, tbl)
	r, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: ns, Table: tbl})
	if err != nil {
		t.Fatalf("OpenTableReader: %v", err)
	}
	defer r.Close()
	if got := drain(t, r); got != memRows {
		t.Fatalf("rows = %d", got)
	}
	if _, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: ns, Table: "missing"}); err == nil {
		t.Fatal("reading a missing table succeeded")
	}
	if _, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: ns, Table: "a\x00b"}); err == nil {
		t.Fatal("a NUL in the table name was accepted")
	}
	if _, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "", Table: tbl}); err == nil {
		t.Fatal("an empty namespace was accepted")
	}
}

// The ctx of OpenTableReader and Next only bounds that call.
func TestReaderCtxOwnership(t *testing.T) {
	p, _ := memCatalogPlane(t, "n", "t")
	ctx, cancel := context.WithCancel(context.Background())
	r, err := p.OpenTableReader(ctx, datagateway.ReaderSpec{Namespace: "n", Table: "t"})
	cancel()
	if err != nil {
		t.Fatalf("OpenTableReader: %v", err)
	}
	defer r.Close()
	// A Next whose ctx is already done fails without touching the stream.
	if _, err := r.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next(cancelled) = %v", err)
	}
	if got := drain(t, r); got != memRows {
		t.Fatalf("rows = %d", got)
	}
}

func TestReaderCloseMidStream(t *testing.T) {
	p, _ := memCatalogPlane(t, "n", "t")
	r, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"})
	if err != nil {
		t.Fatalf("OpenTableReader: %v", err)
	}
	rec, err := r.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	rec.Release()
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := r.Next(context.Background()); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("Next after Close = %v", err)
	}
	r2, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"})
	if err != nil {
		t.Fatalf("OpenTableReader after Close: %v", err)
	}
	defer r2.Close()
	if got := drain(t, r2); got != memRows {
		t.Fatalf("rows = %d", got)
	}
	if n := len(p.liveReaders()); n != 1 {
		t.Fatalf("%d readers registered, want 1", n)
	}
}

// blockedNext starts a Next on the slow view and returns its result channel
// once the fetch is inside DuckDB.
func blockedNext(t *testing.T, p *Plane) (datagateway.RecordStream, <-chan error) {
	t.Helper()
	r, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "slow"})
	if err != nil {
		t.Fatalf("OpenTableReader: %v", err)
	}
	errc := make(chan error, 1)
	go func() {
		rec, err := r.Next(context.Background())
		if rec != nil {
			rec.Release()
		}
		errc <- err
	}()
	time.Sleep(300 * time.Millisecond)
	return r, errc
}

func waitErr(t *testing.T, errc <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-errc:
		if err == nil || errors.Is(err, io.EOF) {
			t.Fatalf("%s: Next = %v, want an error", what, err)
		}
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: Next did not return", what)
	}
	return nil
}

// Close from another goroutine while Next is blocked in DuckDB.
func TestReaderCloseDuringNext(t *testing.T) {
	p, _ := memCatalogPlane(t, "n", "t")
	r, errc := blockedNext(t, p)
	start := time.Now()
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := waitErr(t, errc, "Close"); !errors.Is(err, errReaderClosed) {
		t.Fatalf("Next = %v, want errReaderClosed", err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("Close took %v", d)
	}
}

// A Next ctx deadline ends that call (and, DuckDB's query being interrupted,
// the reader).
func TestReaderNextDeadline(t *testing.T) {
	p, _ := memCatalogPlane(t, "n", "t")
	r, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "slow"})
	if err != nil {
		t.Fatalf("OpenTableReader: %v", err)
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := r.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Next = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("Next took %v", d)
	}
}

func TestPlaneInterruptDuringNext(t *testing.T) {
	p, _ := memCatalogPlane(t, "n", "t")
	r, errc := blockedNext(t, p)
	defer r.Close()
	p.Interrupt()
	if err := waitErr(t, errc, "Interrupt"); !errors.Is(err, errInterrupted) {
		t.Fatalf("Next = %v, want errInterrupted", err)
	}
}

func TestPlaneCloseDuringNext(t *testing.T) {
	p, _ := memCatalogPlane(t, "n", "t")
	r, errc := blockedNext(t, p)
	if err := p.Close(); err != nil {
		t.Fatalf("Plane.Close: %v", err)
	}
	if err := waitErr(t, errc, "plane Close"); !errors.Is(err, errPlaneClosed) {
		t.Fatalf("Next = %v, want errPlaneClosed", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("reader Close: %v", err)
	}
}

func TestPlaneInterruptFailsReaders(t *testing.T) {
	p, _ := memCatalogPlane(t, "n", "t")
	r, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"})
	if err != nil {
		t.Fatalf("OpenTableReader: %v", err)
	}
	defer r.Close()
	rec, err := r.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	rec.Release()
	p.Interrupt()
	if _, err := r.Next(context.Background()); !errors.Is(err, errInterrupted) {
		t.Fatalf("Next after Interrupt = %v", err)
	}
	// The plane stays usable: a new reader works.
	r2, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"})
	if err != nil {
		t.Fatalf("OpenTableReader after Interrupt: %v", err)
	}
	defer r2.Close()
	if got := drain(t, r2); got != memRows {
		t.Fatalf("rows = %d", got)
	}
}

func TestPlaneCloseClosesReaders(t *testing.T) {
	p, _ := memCatalogPlane(t, "n", "t")
	r, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"})
	if err != nil {
		t.Fatalf("OpenTableReader: %v", err)
	}
	rec, err := r.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Plane.Close: %v", err)
	}
	// The handed-out batch outlives the plane.
	if rec.NumRows() == 0 || rec.Column(1).(*array.String).Value(0) == "" {
		t.Fatal("batch unreadable after plane Close")
	}
	rec.Release()
	if _, err := r.Next(context.Background()); !errors.Is(err, errPlaneClosed) {
		t.Fatalf("Next after plane Close = %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("reader Close after plane Close: %v", err)
	}
	if _, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"}); !errors.Is(err, errPlaneClosed) {
		t.Fatalf("OpenTableReader after plane Close = %v", err)
	}
}

// Interrupt cuts a running attach short; the failure is not cached.
func TestPlaneInterruptDuringAttach(t *testing.T) {
	p, _ := memCatalogPlane(t, "n", "t")
	real := p.attachHook
	entered := make(chan struct{})
	var calls atomic.Int32
	p.attachHook = func(ctx context.Context, c *duck.Conn) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}
		return real(ctx, c)
	}
	errc := make(chan error, 1)
	go func() {
		_, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"})
		errc <- err
	}()
	<-entered
	p.Interrupt()
	select {
	case err := <-errc:
		if !errors.Is(err, errInterrupted) {
			t.Fatalf("OpenTableReader = %v, want errInterrupted", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("attach not interrupted")
	}
	r, err := p.OpenTableReader(context.Background(), datagateway.ReaderSpec{Namespace: "n", Table: "t"})
	if err != nil {
		t.Fatalf("OpenTableReader after an interrupted attach: %v", err)
	}
	defer r.Close()
	if got := drain(t, r); got != memRows {
		t.Fatalf("rows = %d", got)
	}
}
