//go:build integration

package backend

// Integration test for the GCS range-read adapter against a real
// fake-gcs-server container (the same harness used by
// gcs_integration_test.go).
//
// Run with:
//   go test -v -tags=integration ./pkg/datagateway/backend/... -run TestGCSRangeReader
//
// Requires Docker. Excluded from the default `go test ./...` run by the
// `integration` build tag.

import (
	"context"
	"errors"
	"io"
	"testing"
)

// TestGCSRangeReaderFooterFirst exercises the Parquet footer-first read
// pattern against a real fake-gcs-server. Write a 1 MiB blob, then read
// the last 8 bytes via the *gcsBackend.NewRangeReader path. EOF on the
// tail read is expected and acceptable; what matters is that the bytes
// match.
func TestGCSRangeReaderFooterFirst(t *testing.T) {
	bucket := startFakeGCS(t, "datuplet-rangeread")
	be, err := NewGCSBackend(GCSConfig{Bucket: bucket})
	if err != nil {
		t.Fatalf("NewGCSBackend: %v", err)
	}
	defer be.Close()

	ctx := context.Background()
	blob := make([]byte, 1<<20)
	for i := range blob {
		blob[i] = byte(i % 256)
	}
	if err := be.PutObject(ctx, "big.bin", blob); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	ra, err := be.NewRangeReader(ctx, "big.bin")
	if err != nil {
		t.Fatalf("NewRangeReader: %v", err)
	}
	if got := ra.Size(); got != int64(len(blob)) {
		t.Errorf("Size() = %d, want %d", got, len(blob))
	}

	tail := make([]byte, 8)
	n, err := ra.ReadAt(tail, int64(len(blob))-8)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 8 {
		t.Fatalf("ReadAt = %d, want 8", n)
	}
	for i := 0; i < 8; i++ {
		want := byte((len(blob) - 8 + i) % 256)
		if tail[i] != want {
			t.Fatalf("tail[%d] = %d, want %d", i, tail[i], want)
		}
	}
}
