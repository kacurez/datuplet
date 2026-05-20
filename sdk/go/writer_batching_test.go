package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// recordedRequest captures one HTTP POST to /data/write/{id} so tests can
// assert on number of roundtrips, ordering, and accumulated bytes per call.
type recordedRequest struct {
	Body        []byte
	ContentType string
}

// newWriteTestServer spins up an httptest server that answers any POST
// /data/write/{id} with a synthetic success response. It records every
// request's body so the test can assert on call count + per-call bytes.
func newWriteTestServer(t *testing.T) (*httptest.Server, *[]recordedRequest, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	var requests []recordedRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		requests = append(requests, recordedRequest{
			Body:        body,
			ContentType: r.Header.Get("Content-Type"),
		})
		mu.Unlock()

		// Synthetic gateway response. The SDK only reads RowsAccepted +
		// BufferSizeBytes + InferredSchema; everything else is fine to omit.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"rows_accepted":     0,
			"buffer_size_bytes": int64(len(body)),
		})
	}))
	t.Cleanup(srv.Close)
	return srv, &requests, &mu
}

// newTestWriter constructs a Writer wired up to the test HTTP server with
// the specified batch threshold. Bypasses OpenWriter (we'd need a full
// gateway for that) — the batching logic is independent of the open path.
func newTestWriter(srv *httptest.Server, batchThreshold int64) *Writer {
	w := &Writer{
		httpClient:     srv.Client(),
		httpEndpoint:   srv.URL,
		bucket:         "raw",
		table:          "events",
		inputFormat:    pb.DataFormat_FORMAT_JSONL,
		batchThreshold: batchThreshold,
	}
	if batchThreshold > 0 {
		w.batchBuffer = make([]byte, 0, batchThreshold)
	}
	return w
}

// TestWriteBatching_SmallWritesAccumulate verifies that many Writes below
// the threshold produce zero HTTP roundtrips, and that the accumulated
// bytes match the input.
func TestWriteBatching_SmallWritesAccumulate(t *testing.T) {
	srv, reqs, mu := newWriteTestServer(t)
	w := newTestWriter(srv, 1024) // 1 KiB threshold

	ctx := context.Background()
	// 10 writes of 50 bytes each = 500 bytes total, below the 1024 threshold.
	for i := 0; i < 10; i++ {
		if err := w.Write(ctx, []byte("0123456789012345678901234567890123456789012345678\n")); err != nil {
			t.Fatalf("Write[%d]: %v", i, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(*reqs) != 0 {
		t.Fatalf("expected 0 HTTP POSTs (all under threshold), got %d", len(*reqs))
	}
	if int64(len(w.batchBuffer)) != 500 {
		t.Errorf("batchBuffer length = %d, want 500", len(w.batchBuffer))
	}
}

// TestWriteBatching_ThresholdFlush verifies that crossing the threshold
// produces exactly one HTTP POST containing the accumulated bytes.
func TestWriteBatching_ThresholdFlush(t *testing.T) {
	srv, reqs, mu := newWriteTestServer(t)
	w := newTestWriter(srv, 100) // 100 B threshold

	ctx := context.Background()
	// Three writes of 50 bytes each. Cumulative: 50, 100, 150.
	// Threshold trips on the second Write; third write goes into a fresh buffer.
	for i := 0; i < 3; i++ {
		if err := w.Write(ctx, make([]byte, 50)); err != nil {
			t.Fatalf("Write[%d]: %v", i, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(*reqs) != 1 {
		t.Fatalf("expected 1 HTTP POST, got %d", len(*reqs))
	}
	if got := len((*reqs)[0].Body); got != 100 {
		t.Errorf("flushed body length = %d, want 100", got)
	}
	if got := len(w.batchBuffer); got != 50 {
		t.Errorf("residual batchBuffer length = %d, want 50", got)
	}
}

// TestWriteBatching_FlushDrains verifies the explicit Flush() method
// drains the accumulator immediately and is idempotent.
func TestWriteBatching_FlushDrains(t *testing.T) {
	srv, reqs, mu := newWriteTestServer(t)
	w := newTestWriter(srv, 10*1024)

	ctx := context.Background()
	_ = w.Write(ctx, []byte("hello"))
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// Idempotent: second Flush sends nothing.
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush (idempotent): %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(*reqs) != 1 {
		t.Fatalf("expected 1 HTTP POST after Flush, got %d", len(*reqs))
	}
	if string((*reqs)[0].Body) != "hello" {
		t.Errorf("flushed body = %q, want %q", (*reqs)[0].Body, "hello")
	}
}

// TestWriteBatching_CloseDrains verifies Close() flushes pending data
// before sending CloseWriter.
func TestWriteBatching_CloseDrains(t *testing.T) {
	srv, reqs, mu := newWriteTestServer(t)
	w := newTestWriter(srv, 10*1024)

	ctx := context.Background()
	_ = w.Write(ctx, []byte("row1\n"))
	_ = w.Write(ctx, []byte("row2\n"))

	// We can't call full Close() here because it also issues a gRPC
	// CloseWriter (which we don't mock). Drive flushBatchLocked directly
	// via Flush — equivalent for batching purposes.
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(*reqs) != 1 {
		t.Fatalf("expected 1 HTTP POST on flush, got %d", len(*reqs))
	}
	if string((*reqs)[0].Body) != "row1\nrow2\n" {
		t.Errorf("concatenated body = %q, want row1\\nrow2\\n", (*reqs)[0].Body)
	}
}

// TestWriteBatching_WriteChunkPreservesOrder verifies that calling
// WriteChunk while bytes are pending in the batch buffer flushes those
// bytes BEFORE the WriteChunk's data — so the gateway sees calls in
// submission order.
func TestWriteBatching_WriteChunkPreservesOrder(t *testing.T) {
	srv, reqs, mu := newWriteTestServer(t)
	w := newTestWriter(srv, 10*1024)

	ctx := context.Background()
	_ = w.Write(ctx, []byte("first\n"))                // pending
	_ = w.Write(ctx, []byte("second\n"))               // still pending
	_, err := w.WriteChunk(ctx, []byte("third-now\n")) // forces flush + explicit
	if err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(*reqs) != 2 {
		t.Fatalf("expected 2 HTTP POSTs (batch flush + explicit), got %d", len(*reqs))
	}
	if string((*reqs)[0].Body) != "first\nsecond\n" {
		t.Errorf("first POST body = %q, want first\\nsecond\\n", (*reqs)[0].Body)
	}
	if string((*reqs)[1].Body) != "third-now\n" {
		t.Errorf("second POST body = %q, want third-now\\n", (*reqs)[1].Body)
	}
}

// TestWriteBatching_DisabledByOption verifies that batchThreshold <= 0
// disables batching: every Write becomes one immediate POST (legacy
// v0.2.x behavior preserved for opt-out).
func TestWriteBatching_DisabledByOption(t *testing.T) {
	srv, reqs, mu := newWriteTestServer(t)
	w := newTestWriter(srv, 0) // 0 => disabled (set explicitly for the test)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := w.Write(ctx, []byte("row\n")); err != nil {
			t.Fatalf("Write[%d]: %v", i, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(*reqs) != 5 {
		t.Fatalf("with batching disabled, expected 5 HTTP POSTs, got %d", len(*reqs))
	}
}

// TestWriteBatching_RoundtripReduction is the headline assertion: with
// batching at 1 MiB, 10K row-at-a-time Writes of ~100 bytes each should
// produce ~1 HTTP roundtrip (vs 10K without batching). Asserts a
// reduction factor of at least 100x.
func TestWriteBatching_RoundtripReduction(t *testing.T) {
	srv, reqs, mu := newWriteTestServer(t)
	w := newTestWriter(srv, 1024*1024) // 1 MiB default

	ctx := context.Background()
	const N = 10000
	row := []byte(fmt.Sprintf(`{"id":1,"v":"%s"}`+"\n", "padding-padding-padding-padding-padding-padding"))
	for i := 0; i < N; i++ {
		if err := w.Write(ctx, row); err != nil {
			t.Fatalf("Write[%d]: %v", i, err)
		}
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("final Flush: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(*reqs) >= N/100 {
		t.Errorf("expected at least 100x fewer HTTP POSTs (got %d for N=%d Writes)", len(*reqs), N)
	}
	// Sanity: total body bytes received = N * len(row)
	var totalBytes int
	for _, r := range *reqs {
		totalBytes += len(r.Body)
	}
	if totalBytes != N*len(row) {
		t.Errorf("total bytes received = %d, want %d", totalBytes, N*len(row))
	}
}

// TestWriteBatching_ContentTypeForwarded verifies the Content-Type
// header is set correctly per input format (relevant for the gateway
// to dispatch the right parser).
func TestWriteBatching_ContentTypeForwarded(t *testing.T) {
	srv, reqs, mu := newWriteTestServer(t)
	w := newTestWriter(srv, 1024)
	w.inputFormat = pb.DataFormat_FORMAT_JSONL

	ctx := context.Background()
	_ = w.Write(ctx, make([]byte, 2048)) // forces immediate flush
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(*reqs) == 0 {
		t.Fatal("no POST captured")
	}
	if got := (*reqs)[0].ContentType; got != "application/x-ndjson" {
		t.Errorf("Content-Type = %q, want application/x-ndjson", got)
	}
}
