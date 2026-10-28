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

// TestMinIOBackend_RemoveAll_Batches verifies how removeAllObjects batches
// deletes (batchSize = 1000): no objects means no RemoveObjects call and no
// error; fewer than batchSize is one batch; batchSize+1 is one full batch plus
// a remainder. Keys must be passed through in order.
func TestMinIOBackend_RemoveAll_Batches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		objects     int
		wantBatches []int
	}{
		{"no objects, no RemoveObjects call", 0, nil},
		{"3 objects fit in one batch", 3, []int{3}},
		{"batchSize+1 objects: full batch plus remainder", 1001, []int{1000, 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var wantKeys []string
			objects := make([]minio.ObjectInfo, tc.objects)
			for i := range objects {
				objects[i] = minio.ObjectInfo{Key: fmt.Sprintf("ws/data/file%04d.parquet", i)}
				wantKeys = append(wantKeys, objects[i].Key)
			}

			fake := &fakeMinioRemove{objects: objects}
			if err := removeAllObjects(context.Background(), fake, "mybucket", "ws/data"); err != nil {
				t.Fatalf("removeAllObjects returned unexpected error: %v", err)
			}

			if len(fake.removedBatches) != len(tc.wantBatches) {
				t.Fatalf("expected %d RemoveObjects calls, got %d", len(tc.wantBatches), len(fake.removedBatches))
			}
			var gotKeys []string
			for i, batch := range fake.removedBatches {
				if len(batch) != tc.wantBatches[i] {
					t.Errorf("batch %d: got %d keys, want %d", i, len(batch), tc.wantBatches[i])
				}
				gotKeys = append(gotKeys, batch...)
			}
			for i, k := range wantKeys {
				if i >= len(gotKeys) || gotKeys[i] != k {
					t.Fatalf("key %d mismatch: got %v", i, gotKeys)
				}
			}
		})
	}
}
