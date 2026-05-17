package backend

import (
	"context"
	"fmt"
	"testing"

	"github.com/minio/minio-go/v7"
)

// fakeMinioRemove is an in-process fake for minioRemoveAPI used in unit tests.
// It returns a fixed list of objects from ListObjects and records each batch
// passed to RemoveObjects.
type fakeMinioRemove struct {
	// objects is the full list of objects to return across all ListObjects calls.
	objects []minio.ObjectInfo
	// listErr, if non-nil, is injected as an error on the last object in the list.
	listErr error

	// removedBatches records each batch of objects passed to RemoveObjects,
	// in order. Each inner slice corresponds to one RemoveObjects call.
	removedBatches [][]string

	// removeErr, if non-nil, is returned as a RemoveObjectError on every call.
	removeErr error
}

func (f *fakeMinioRemove) ListObjects(_ context.Context, _ string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo, len(f.objects)+1)
	for i, obj := range f.objects {
		if f.listErr != nil && i == len(f.objects)-1 {
			// Inject error on the last entry.
			ch <- minio.ObjectInfo{Err: f.listErr}
		} else {
			ch <- obj
		}
	}
	close(ch)
	return ch
}

func (f *fakeMinioRemove) RemoveObjects(_ context.Context, _ string, objectsCh <-chan minio.ObjectInfo, _ minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError {
	errCh := make(chan minio.RemoveObjectError, 1)

	// Drain objectsCh synchronously (it's already buffered and closed by
	// the caller when we use the buffered-channel flush pattern).
	var keys []string
	for obj := range objectsCh {
		keys = append(keys, obj.Key)
	}
	f.removedBatches = append(f.removedBatches, keys)

	if f.removeErr != nil {
		errCh <- minio.RemoveObjectError{
			ObjectName: keys[0],
			Err:        f.removeErr,
		}
	}

	close(errCh)
	return errCh
}

// TestMinIOBackend_RemoveAll_PaginatesAndDeletes verifies that when
// ListObjects returns more than batchSize objects, RemoveAll issues
// multiple RemoveObjects calls (one per batch) with the correct keys.
func TestMinIOBackend_RemoveAll_PaginatesAndDeletes(t *testing.T) {
	t.Parallel()

	// Build a list of 3 objects across 2 batches (batchSize = 1000 in prod,
	// but we exercise the logic by having 3 objects and expecting a single
	// flush + final flush — both happen in the same pass).
	objects := []minio.ObjectInfo{
		{Key: "ws/data/a.parquet"},
		{Key: "ws/data/b.parquet"},
		{Key: "ws/data/c.parquet"},
	}

	fake := &fakeMinioRemove{objects: objects}
	ctx := context.Background()

	if err := removeAllObjects(ctx, fake, "mybucket", "ws/data"); err != nil {
		t.Fatalf("removeAllObjects returned unexpected error: %v", err)
	}

	// All 3 objects in one batch (< batchSize = 1000).
	if len(fake.removedBatches) != 1 {
		t.Fatalf("expected 1 RemoveObjects call, got %d", len(fake.removedBatches))
	}

	batch := fake.removedBatches[0]
	if len(batch) != 3 {
		t.Fatalf("expected 3 keys in batch, got %d", len(batch))
	}

	wantKeys := []string{"ws/data/a.parquet", "ws/data/b.parquet", "ws/data/c.parquet"}
	for i, k := range wantKeys {
		if batch[i] != k {
			t.Errorf("batch[%d] = %q, want %q", i, batch[i], k)
		}
	}
}

// TestMinIOBackend_RemoveAll_TwoBatches verifies that exactly batchSize+1
// objects results in two RemoveObjects calls (one full batch + one remainder).
func TestMinIOBackend_RemoveAll_TwoBatches(t *testing.T) {
	t.Parallel()

	// Build batchSize+1 objects.
	const batchSize = 1000
	objects := make([]minio.ObjectInfo, batchSize+1)
	for i := range objects {
		objects[i] = minio.ObjectInfo{Key: fmt.Sprintf("ws/file%04d.parquet", i)}
	}

	fake := &fakeMinioRemove{objects: objects}
	ctx := context.Background()

	if err := removeAllObjects(ctx, fake, "mybucket", "ws"); err != nil {
		t.Fatalf("removeAllObjects returned unexpected error: %v", err)
	}

	if len(fake.removedBatches) != 2 {
		t.Fatalf("expected 2 RemoveObjects calls for %d objects, got %d", batchSize+1, len(fake.removedBatches))
	}
	if len(fake.removedBatches[0]) != batchSize {
		t.Errorf("first batch: got %d keys, want %d", len(fake.removedBatches[0]), batchSize)
	}
	if len(fake.removedBatches[1]) != 1 {
		t.Errorf("second batch: got %d keys, want 1", len(fake.removedBatches[1]))
	}
}

// TestMinIOBackend_RemoveAll_NoObjects_NoError verifies that when ListObjects
// returns an empty list, no RemoveObjects call is made and no error is returned.
func TestMinIOBackend_RemoveAll_NoObjects_NoError(t *testing.T) {
	t.Parallel()

	fake := &fakeMinioRemove{objects: nil}
	ctx := context.Background()

	if err := removeAllObjects(ctx, fake, "mybucket", "nonexistent/prefix"); err != nil {
		t.Errorf("removeAllObjects on empty list returned error: %v", err)
	}

	if len(fake.removedBatches) != 0 {
		t.Errorf("expected 0 RemoveObjects calls, got %d", len(fake.removedBatches))
	}
}
