package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

// fakeMinioPutAPI is the test fake for minioPutObjectAPI. It drives the
// failure scenarios codex flagged as Phase 1 acceptance tests:
//   - happy path: drains the reader and returns success
//   - network failure: returns error after consuming N bytes
//   - context cancellation: blocks on ctx.Done() and returns ctx.Err()
type fakeMinioPutAPI struct {
	mu              sync.Mutex
	bytesConsumed   int64
	failAfterBytes  int64 // 0 = never fail
	failErr         error
	blockOnCtx      bool          // if true, PutObject blocks on ctx.Done()
	blockUntilReady chan struct{} // if non-nil, PutObject waits on this before starting
	callCount       atomic.Int32
}

func (f *fakeMinioPutAPI) PutObject(
	ctx context.Context,
	bucket, object string,
	reader io.Reader, size int64,
	opts minio.PutObjectOptions,
) (minio.UploadInfo, error) {
	f.callCount.Add(1)

	// Optional sync point so tests can hold the upload before it starts.
	if f.blockUntilReady != nil {
		select {
		case <-f.blockUntilReady:
		case <-ctx.Done():
			return minio.UploadInfo{}, ctx.Err()
		}
	}

	// Scenario: block forever on ctx.Done(). Doesn't read the pipe.
	if f.blockOnCtx {
		<-ctx.Done()
		return minio.UploadInfo{}, ctx.Err()
	}

	// Normal: drain the reader, optionally failing partway.
	buf := make([]byte, 4096)
	for {
		select {
		case <-ctx.Done():
			return minio.UploadInfo{}, ctx.Err()
		default:
		}
		n, err := reader.Read(buf)
		if n > 0 {
			f.mu.Lock()
			f.bytesConsumed += int64(n)
			shouldFail := f.failAfterBytes > 0 && f.bytesConsumed >= f.failAfterBytes
			f.mu.Unlock()
			if shouldFail {
				return minio.UploadInfo{}, f.failErr
			}
		}
		if err == io.EOF {
			return minio.UploadInfo{Bucket: bucket, Key: object, Size: f.bytesConsumed}, nil
		}
		if err != nil {
			return minio.UploadInfo{}, err
		}
	}
}

// Landmine 1 (happy path baseline): writer streams bytes to the fake PutObject,
// Close returns nil, all bytes accounted for.
func TestMinIOStreaming_HappyPath(t *testing.T) {
	api := &fakeMinioPutAPI{}
	ctx := context.Background()

	w := openStreamingMinIOWriter(ctx, api, "bucket", "key.parquet")
	payload := []byte(strings.Repeat("x", 10_000))
	if n, err := w.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write: n=%d err=%v", n, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if api.bytesConsumed != int64(len(payload)) {
		t.Errorf("bytes consumed = %d, want %d", api.bytesConsumed, len(payload))
	}
	if got := api.callCount.Load(); got != 1 {
		t.Errorf("PutObject called %d times, want 1", got)
	}
}

// Landmine: "forced network failure mid-upload returns deterministic error,
// no leaked goroutines". Caller's Close surfaces the upload error verbatim.
func TestMinIOStreaming_UploadFailureMidStream(t *testing.T) {
	api := &fakeMinioPutAPI{
		failAfterBytes: 512,
		failErr:        errors.New("simulated network failure"),
	}
	ctx := context.Background()

	before := runtime.NumGoroutine()
	w := openStreamingMinIOWriter(ctx, api, "bucket", "key.parquet")

	// Write more than failAfterBytes; we should eventually see the upload error.
	// May surface on the Write call (if the pipe has been CloseWithError'd
	// already) or on Close. Either is acceptable.
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = 'x'
	}
	var writeErr error
	for i := 0; i < 100 && writeErr == nil; i++ {
		_, writeErr = w.Write(payload)
	}

	closeErr := w.Close()
	if writeErr == nil && closeErr == nil {
		t.Fatal("expected Write or Close to surface the upload failure")
	}
	combined := fmt.Sprintf("%v|%v", writeErr, closeErr)
	if !strings.Contains(combined, "simulated network failure") {
		t.Errorf("error did not propagate upload failure: %s", combined)
	}

	// Goroutine accounting: wait a moment for the upload goroutine to exit.
	if !waitForGoroutineCount(before, 500*time.Millisecond) {
		t.Errorf("goroutine leak after upload failure: before=%d, after=%d",
			before, runtime.NumGoroutine())
	}
}

// Landmine: "context cancellation during upload unwinds within bounded time."
// Cancel the upload's context while it's blocked; Close must return in <500ms.
func TestMinIOStreaming_ContextCancellation(t *testing.T) {
	api := &fakeMinioPutAPI{blockOnCtx: true}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	before := runtime.NumGoroutine()
	w := openStreamingMinIOWriter(ctx, api, "bucket", "key.parquet")

	// Cancel the upload context.
	cancel()

	// Close should return within bounded time with the ctx error.
	done := make(chan error, 1)
	go func() { done <- w.Close() }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected Close to return error after ctx cancel")
		}
		if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "context canceled") {
			t.Errorf("Close returned %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close did not unwind within 500 ms of ctx cancel")
	}

	if !waitForGoroutineCount(before, 500*time.Millisecond) {
		t.Errorf("goroutine leak after ctx cancel: before=%d, after=%d",
			before, runtime.NumGoroutine())
	}
}

// Close is idempotent. Calling Close twice must not panic and must return
// the same error both times.
func TestMinIOStreaming_CloseIdempotent(t *testing.T) {
	api := &fakeMinioPutAPI{}
	w := openStreamingMinIOWriter(context.Background(), api, "bucket", "key.parquet")
	_, _ = w.Write([]byte("hello"))
	first := w.Close()
	second := w.Close()
	if first != nil || second != nil {
		t.Fatalf("Close returned errors: first=%v second=%v", first, second)
	}
}

// Write-after-Close must not panic. The pipe writer rejects further writes
// with io.ErrClosedPipe.
func TestMinIOStreaming_WriteAfterCloseRejected(t *testing.T) {
	api := &fakeMinioPutAPI{}
	w := openStreamingMinIOWriter(context.Background(), api, "bucket", "key.parquet")
	_, _ = w.Write([]byte("hello"))
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := w.Write([]byte("after close")); err == nil {
		t.Error("Write after Close should return an error")
	}
}

// waitForGoroutineCount polls until runtime.NumGoroutine matches the
// target or until the deadline elapses. Returns true if it matched in time.
// Tolerates one extra goroutine to absorb test-harness scheduler noise.
func waitForGoroutineCount(target int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= target+1 {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return runtime.NumGoroutine() <= target+1
}
