//go:build duckdb_arrow

package main

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

// TestRegisterView_CtxCancelMidScan: arrange a slow RecordReader, kick off a
// query, cancel ctx, assert the program doesn't panic and the goroutine
// finishes. This is the SQL-mid-error path for context cancellation
// during a scan.
func TestRegisterView_CtxCancelMidScan(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	arrowConn, conn, err := arrowFromDB(db)
	if err != nil {
		t.Fatalf("arrowFromDB: %v", err)
	}
	defer conn.Close()

	pool := memory.NewGoAllocator()
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
	slowReader := newSlowReader(t, pool, sch, 200*time.Millisecond /* per Next() call */, 1000 /* rows total, 10-row batches */)
	rel, err := arrowConn.RegisterView(slowReader, "slow_v")
	if err != nil {
		t.Fatalf("RegisterView: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	queryDone := make(chan error, 1)
	go func() {
		var n int64
		err := conn.QueryRowContext(ctx, "SELECT count(*) FROM slow_v").Scan(&n)
		queryDone <- err
	}()
	time.Sleep(150 * time.Millisecond) // let DuckDB start scanning the second batch
	cancel()

	select {
	case err := <-queryDone:
		if err == nil {
			t.Logf("query completed before cancel landed (acceptable on a fast machine)")
		} else if errors.Is(err, context.Canceled) {
			t.Logf("query returned context.Canceled — expected")
		} else {
			// duckdb-mapped error wrapper is also acceptable.
			t.Logf("query returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("query did not finish after ctx cancel")
	}

	// Now release — must not panic.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic on release after ctx cancel: %v", r)
		}
	}()
	rel()
	slowReader.Release()
}

// newSlowReader returns an array.RecordReader that emits 10-row batches with a
// configurable delay between Next() calls.
func newSlowReader(t *testing.T, pool memory.Allocator, sch *arrow.Schema, delay time.Duration, totalRows int) *slowRecordReader {
	t.Helper()
	const batchSize = 10
	nBatches := (totalRows + batchSize - 1) / batchSize
	records := make([]arrow.Record, 0, nBatches)
	for b := 0; b < nBatches; b++ {
		start := int64(b * batchSize)
		end := start + int64(batchSize)
		if end > int64(totalRows) {
			end = int64(totalRows)
		}
		builder := array.NewInt64Builder(pool)
		for v := start; v < end; v++ {
			builder.Append(v)
		}
		arr := builder.NewArray()
		rec := array.NewRecord(sch, []arrow.Array{arr}, end-start)
		arr.Release()
		builder.Release()
		records = append(records, rec)
	}
	base, err := array.NewRecordReader(sch, records)
	if err != nil {
		for _, r := range records {
			r.Release()
		}
		t.Fatalf("NewRecordReader: %v", err)
	}
	// Release the records once the base reader has retained them.
	for _, r := range records {
		r.Release()
	}
	return &slowRecordReader{base: base, delay: delay, refCnt: 1}
}

type slowRecordReader struct {
	base   array.RecordReader
	delay  time.Duration
	refCnt int
}

func (s *slowRecordReader) Schema() *arrow.Schema { return s.base.Schema() }
func (s *slowRecordReader) Next() bool {
	time.Sleep(s.delay)
	return s.base.Next()
}
func (s *slowRecordReader) RecordBatch() arrow.RecordBatch { return s.base.RecordBatch() }
func (s *slowRecordReader) Record() arrow.RecordBatch      { return s.base.RecordBatch() }
func (s *slowRecordReader) Err() error                     { return s.base.Err() }
func (s *slowRecordReader) Retain()                        { s.refCnt++ }
func (s *slowRecordReader) Release() {
	s.refCnt--
	if s.refCnt > 0 {
		return
	}
	s.base.Release()
}

// Verify that slowRecordReader satisfies the array.RecordReader interface.
var _ array.RecordReader = (*slowRecordReader)(nil)

// Verify that the Arrow handle type used in production is what we test with.
var _ *duckdb.Arrow = (*duckdb.Arrow)(nil)
