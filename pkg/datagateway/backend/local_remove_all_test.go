package backend

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestLocalBackend_RemoveAll_HappyPath creates a directory tree under the
// backend root, calls RemoveAll on it, and asserts everything is gone.
func TestLocalBackend_RemoveAll_HappyPath(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	b := NewLocalBackend(LocalConfig{DataDir: root})

	// Create workspace/subdir/file.csv inside the root.
	subdir := filepath.Join(root, "workspace", "subdir")
	if err := os.MkdirAll(subdir, 0755); err != nil {
		t.Fatalf("setup: MkdirAll: %v", err)
	}
	filePath := filepath.Join(subdir, "file.csv")
	if err := os.WriteFile(filePath, []byte("a,b\n1,2\n"), 0644); err != nil {
		t.Fatalf("setup: WriteFile: %v", err)
	}

	ctx := context.Background()
	if err := b.RemoveAll(ctx, "workspace"); err != nil {
		t.Fatalf("RemoveAll returned unexpected error: %v", err)
	}

	// The file and its parent directories under root must be gone.
	if _, err := os.Stat(filepath.Join(root, "workspace")); !os.IsNotExist(err) {
		t.Errorf("expected workspace to be deleted, stat err = %v", err)
	}
}

// TestLocalBackend_RemoveAll_Idempotent verifies that calling RemoveAll on a
// path that does not exist returns no error.
func TestLocalBackend_RemoveAll_Idempotent(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	b := NewLocalBackend(LocalConfig{DataDir: root})

	ctx := context.Background()
	// "nonexistent" directory has never been created.
	if err := b.RemoveAll(ctx, "nonexistent/path"); err != nil {
		t.Errorf("RemoveAll on missing path returned error: %v", err)
	}
}

// TestLocalBackend_RemoveAll_RejectsEscape verifies that a prefix that
// would escape the backend root (e.g. "../../etc") is rejected with an error
// and no filesystem mutation occurs.
func TestLocalBackend_RemoveAll_RejectsEscape(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	b := NewLocalBackend(LocalConfig{DataDir: root})

	ctx := context.Background()
	err := b.RemoveAll(ctx, "../../etc")
	if err == nil {
		t.Fatal("expected error for escape prefix, got nil")
	}
}
