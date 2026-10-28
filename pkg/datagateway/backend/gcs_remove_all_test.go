package backend

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/storage"
)

// fakeGCSListDelete is an in-process fake for the gcsObjectLister /
// gcsObjectDeleter pair used in unit tests. It returns a fixed list of
// object names from listObjects and records each call to deleteObject.
type fakeGCSListDelete struct {
	// names is the full set of object keys the list iterator returns.
	names []string
	// listErr, if non-nil, is returned from the iterator's Next on the
	// last entry (mirrors how ListObjects propagates errors mid-stream).
	listErr error

	// deletedKeys records every key passed to deleteObject, in order.
	deletedKeys []string
	// deleteErr, if non-nil, is returned from every deleteObject call.
	deleteErr error
}

// fakeGCSIter is the iterator returned by fakeGCSListDelete.listObjects.
type fakeGCSIter struct {
	names   []string
	listErr error
	idx     int
}

func (it *fakeGCSIter) Next() (*storage.ObjectAttrs, error) {
	if it.idx >= len(it.names) {
		return nil, iteratorDone
	}
	if it.listErr != nil && it.idx == len(it.names)-1 {
		// Inject error on the last entry.
		it.idx++
		return nil, it.listErr
	}
	attrs := &storage.ObjectAttrs{Name: it.names[it.idx]}
	it.idx++
	return attrs, nil
}

func (f *fakeGCSListDelete) listObjects(_ context.Context, _ string) gcsObjectIter {
	return &fakeGCSIter{names: f.names, listErr: f.listErr}
}

func (f *fakeGCSListDelete) deleteObject(_ context.Context, key string) error {
	f.deletedKeys = append(f.deletedKeys, key)
	return f.deleteErr
}

// TestGCSBackend_RemoveAll_DeletesEach verifies that RemoveAll invokes
// deleteObject once per listed object with the correct key.
func TestGCSBackend_RemoveAll_DeletesEach(t *testing.T) {
	t.Parallel()

	fake := &fakeGCSListDelete{
		names: []string{
			"ws/data/a.parquet",
			"ws/data/b.parquet",
			"ws/data/c.parquet",
		},
	}

	if err := removeAllGCSObjects(context.Background(), fake, fake, "ws/data"); err != nil {
		t.Fatalf("removeAllGCSObjects: %v", err)
	}

	if len(fake.deletedKeys) != 3 {
		t.Fatalf("expected 3 deletes, got %d", len(fake.deletedKeys))
	}
	for i, want := range []string{
		"ws/data/a.parquet",
		"ws/data/b.parquet",
		"ws/data/c.parquet",
	} {
		if fake.deletedKeys[i] != want {
			t.Errorf("deletedKeys[%d] = %q, want %q", i, fake.deletedKeys[i], want)
		}
	}
}

// TestGCSBackend_RemoveAll_Edges covers the idempotent no-op path (empty
// listing returns nil and never calls deleteObject) and the prefix
// normalisation (a leading slash is stripped before listing; matches MinIO
// RemoveAll).
func TestGCSBackend_RemoveAll_Edges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		names       []string
		prefix      string
		wantDeletes int
	}{
		{"no objects is a no-op", nil, "nonexistent/prefix", 0},
		{"leading slash prefix is stripped", []string{"/leading/slash/file"}, "/leading/slash", 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeGCSListDelete{names: tc.names}
			if err := removeAllGCSObjects(context.Background(), fake, fake, tc.prefix); err != nil {
				t.Fatalf("removeAllGCSObjects: %v", err)
			}
			if len(fake.deletedKeys) != tc.wantDeletes {
				t.Errorf("expected %d deletes, got %d", tc.wantDeletes, len(fake.deletedKeys))
			}
		})
	}
}

// TestGCSBackend_RemoveAll_DeleteErrorPropagates verifies that a per-object
// delete error stops the loop and is returned with prefix context.
func TestGCSBackend_RemoveAll_DeleteErrorPropagates(t *testing.T) {
	t.Parallel()

	want := errors.New("delete failed")
	fake := &fakeGCSListDelete{
		names:     []string{"x/y/file1"},
		deleteErr: want,
	}

	err := removeAllGCSObjects(context.Background(), fake, fake, "x/y")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, want) {
		t.Errorf("error = %v, want wraps %v", err, want)
	}
}
