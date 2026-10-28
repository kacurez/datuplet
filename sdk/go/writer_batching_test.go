package sdk

import (
	"context"
	"encoding/json"
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

// TestWriteBatching_Threshold verifies the batch threshold decides when Write
// hits the wire: writes below it accumulate with zero HTTP roundtrips, crossing
// it produces exactly one POST holding the accumulated bytes (the remainder
// starts a fresh buffer), and threshold <= 0 disables batching so every Write
// is one immediate POST (legacy v0.2.x behavior preserved for opt-out).
func TestWriteBatching_Threshold(t *testing.T) {
	cases := []struct {
		name         string
		threshold    int64
		writes       int
		writeSize    int
		wantPosts    int
		wantFirstLen int // length of the first POST body; ignored when wantPosts == 0
		wantResidual int // bytes left in the batch buffer
	}{
		{"small writes accumulate", 1024, 10, 50, 0, 0, 500},
		// Cumulative 50, 100, 150: the threshold trips on the second Write and
		// the third goes into a fresh buffer.
		{"threshold flush", 100, 3, 50, 1, 100, 50},
		{"disabled by option", 0, 5, 4, 5, 4, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, reqs, mu := newWriteTestServer(t)
			w := newTestWriter(srv, tc.threshold)

			ctx := context.Background()
			for i := 0; i < tc.writes; i++ {
				if err := w.Write(ctx, make([]byte, tc.writeSize)); err != nil {
					t.Fatalf("Write[%d]: %v", i, err)
				}
			}

			mu.Lock()
			defer mu.Unlock()
			if len(*reqs) != tc.wantPosts {
				t.Fatalf("expected %d HTTP POSTs, got %d", tc.wantPosts, len(*reqs))
			}
			if tc.wantPosts > 0 {
				if got := len((*reqs)[0].Body); got != tc.wantFirstLen {
					t.Errorf("first POST body length = %d, want %d", got, tc.wantFirstLen)
				}
			}
			if got := len(w.batchBuffer); got != tc.wantResidual {
				t.Errorf("batchBuffer length = %d, want %d", got, tc.wantResidual)
			}
		})
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
