package datagateway

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

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
		if r.Err != nil {
			fail++
		} else {
			ok++
		}
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
	<-started
	<-started
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

func TestCommitPool_WaitRespectsDeadline(t *testing.T) {
	never := make(chan struct{})
	pool := NewCommitPool(CommitPoolConfig{Workers: 1, MaxQueueSize: 4, CatalogFn: okCatalogFn,
		CommitFn: func(context.Context, catalog.Catalog, icebergtable.Identifier, []string, icebergjob.WriteMode, string) (*icebergjob.CommitResult, error) {
			<-never // block forever, ignore cancellation
			return &icebergjob.CommitResult{}, nil
		}})
	defer func() { close(never); pool.Cancel() }()
	_ = pool.Dispatch(context.Background(), CommitJob{WriterID: "w", Namespace: "ns", Table: "t", DataPaths: []string{"p"}, Mode: icebergjob.WriteModeAppend, RunID: "r"})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	pool.Wait(ctx)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Wait ignored deadline: blocked %v", elapsed)
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
	ka := ComputeIdempotencyKey("r", "ns", "t", []string{"a", "b|c"})
	kb := ComputeIdempotencyKey("r", "ns", "t", []string{"a|b", "c"})
	if ka == kb {
		t.Error("path separator ambiguity: distinct path sets collided")
	}
}
