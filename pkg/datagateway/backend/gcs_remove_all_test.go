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

// TestGCSBackend_RemoveAll_NoObjects_NoError verifies the idempotent
// no-op path: empty listing returns nil and never calls deleteObject.
func TestGCSBackend_RemoveAll_NoObjects_NoError(t *testing.T) {
	t.Parallel()

	fake := &fakeGCSListDelete{}

	if err := removeAllGCSObjects(context.Background(), fake, fake, "nonexistent/prefix"); err != nil {
		t.Errorf("removeAllGCSObjects on empty: %v", err)
	}
	if len(fake.deletedKeys) != 0 {
		t.Errorf("expected 0 deletes, got %d", len(fake.deletedKeys))
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

// TestGCSBackend_RemoveAll_StripsLeadingSlash verifies the prefix
// normalisation behaviour (matches MinIO RemoveAll).
func TestGCSBackend_RemoveAll_StripsLeadingSlash(t *testing.T) {
	t.Parallel()

	fake := &fakeGCSListDelete{
		names: []string{"/leading/slash/file"},
	}

	// Pass a leading-slash prefix; we expect the call to succeed and
	// the single object to be deleted (the prefix is stripped before
	// listing).
	if err := removeAllGCSObjects(context.Background(), fake, fake, "/leading/slash"); err != nil {
		t.Fatalf("removeAllGCSObjects: %v", err)
	}
	if len(fake.deletedKeys) != 1 {
		t.Fatalf("expected 1 delete, got %d", len(fake.deletedKeys))
	}
}
