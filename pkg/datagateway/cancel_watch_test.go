package datagateway

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/icebergjob"
)

// TestWatchCancelAnnotation_FiresOnTrue exercises the happy path:
// once the file contains the cancel marker, the watcher returns nil.
func TestWatchCancelAnnotation_FiresOnTrue(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "annotations")

	// Pre-populate with non-cancel content first.
	if err := os.WriteFile(p, []byte("other.io/foo=\"bar\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- WatchCancelAnnotation(ctx, p, 50*time.Millisecond)
	}()

	// Flip the file to the cancel shape after a short delay.
	time.Sleep(100 * time.Millisecond)
	body := "other.io/foo=\"bar\"\ndatuplet.io/cancel=\"true\"\n"
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("watcher returned error %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watcher did not return after cancel marker was written")
	}
}

// TestWatchCancelAnnotation_HonoursContext makes sure ctx cancel
// propagates so the gateway shutdown path can stop the watcher.
func TestWatchCancelAnnotation_HonoursContext(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "annotations")
	if err := os.WriteFile(p, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- WatchCancelAnnotation(ctx, p, 50*time.Millisecond)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("watcher returned nil; expected ctx.Err()")
		}
	case <-time.After(time.Second):
		t.Fatal("watcher did not honour ctx cancellation")
	}
}

// TestWatchCancelAnnotation_EmptyPathBlocks confirms the no-K8s case:
// when path is "" the watcher waits indefinitely for ctx and returns
// ctx.Err() rather than firing immediately.
func TestWatchCancelAnnotation_EmptyPathBlocks(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := WatchCancelAnnotation(ctx, "", 0)
	if err != context.DeadlineExceeded {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

// TestReadCancelAnnotation_MultilineFormat verifies we tolerate the
// kubelet's actual projection format: one annotation per line, value
// in double quotes, possibly with other keys interleaved.
func TestReadCancelAnnotation_MultilineFormat(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "annotations")
	body := "kubectl.kubernetes.io/last-applied-configuration=\"...\"\n" +
		"datuplet.io/run-id=\"abc-123\"\n" +
		"datuplet.io/cancel=\"true\"\n"
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := readCancelAnnotation(p)
	if err != nil {
		t.Fatal(err)
	}
	if !got {
		t.Fatal("readCancelAnnotation should fire on multiline format")
	}

	// "false" should NOT trigger.
	body = "datuplet.io/cancel=\"false\"\n"
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err = readCancelAnnotation(p)
	if err != nil {
		t.Fatal(err)
	}
	if got {
		t.Fatal("readCancelAnnotation should ignore false")
	}
}

// TestCancelAnnotation_CancelsCommitPool verifies that when the pod-annotations
// cancel marker fires, the commit pool is cancelled so in-flight commit
// goroutines are unblocked and return a non-nil error.
//
// This test MUST fail if the wiring in server_v2.go (the `s.commitPool.Cancel()`
// call inside the cancel-watcher goroutine) is removed.
func TestCancelAnnotation_CancelsCommitPool(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	annotationsPath := filepath.Join(dir, "annotations")

	// Start with non-cancel content so the watcher doesn't fire prematurely.
	if err := os.WriteFile(annotationsPath, []byte("other.io/foo=\"bar\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := &Config{
		RunID:              "cancel-pool-test-run",
		ComponentName:      "test",
		DefaultBucket:      "raw",
		Backend:            newMockBackend(),
		PodAnnotationsPath: annotationsPath,
		CancelPollInterval: 50 * time.Millisecond,
	}
	server := NewServerV2(cfg)

	// blockCh lets the test control when the CommitFn unblocks.
	// The CommitFn blocks until its context is cancelled.
	// Buffered so the send never races with the test's receive.
	commitStarted := make(chan struct{}, 1)
	pool := NewCommitPool(CommitPoolConfig{
		Workers:      2,
		MaxQueueSize: 16,
		CatalogFn:    func(context.Context) (catalog.Catalog, error) { return nil, nil },
		CommitFn: func(ctx context.Context, _ catalog.Catalog, _ icebergtable.Identifier,
			_ []string, _ icebergjob.WriteMode, _ string) (*icebergjob.CommitResult, error) {
			// Signal that the commit goroutine is running, then block until
			// the pool's context is cancelled.
			commitStarted <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	server.setCommitPoolForTest(pool)

	// Dispatch one job so the blocking CommitFn is in flight.
	err := pool.Dispatch(context.Background(), CommitJob{
		WriterID:  "w1",
		Namespace: "ns",
		Table:     "orders",
		RunID:     "cancel-pool-test-run",
		DataPaths: []string{"s3://b/f.parquet"},
		Mode:      icebergjob.WriteModeAppend,
	})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	// Wait until the commit goroutine is actually blocked inside CommitFn.
	select {
	case <-commitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("commit goroutine did not start within 5s")
	}

	// Now flip the annotations file to trigger the cancel watcher.
	body := "other.io/foo=\"bar\"\ndatuplet.io/cancel=\"true\"\n"
	if err := os.WriteFile(annotationsPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	// pool.Wait must return once the pool is cancelled. Use a short-poll
	// goroutine so the test can impose its own deadline.
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()

	results := pool.Wait(waitCtx)

	if waitCtx.Err() != nil {
		t.Fatal("pool.Wait timed out — commit pool was not cancelled by the cancel watcher (wiring missing?)")
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 commit result, got %d", len(results))
	}
	if results[0].Err == nil {
		t.Error("expected in-flight commit to have a non-nil error after pool cancel, got nil")
	}
}
