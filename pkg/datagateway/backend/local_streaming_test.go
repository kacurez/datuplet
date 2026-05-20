package backend

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLocalBackend_OpenObjectWriter verifies the streaming-write path
// for the local backend: bytes flow directly to disk without an
// in-process []byte buffer (the streaming-upload optimization).
func TestLocalBackend_OpenObjectWriter(t *testing.T) {
	tmp := t.TempDir()
	be := NewLocalBackend(LocalConfig{DataDir: tmp})

	wc, err := be.OpenObjectWriter(context.Background(), "sub/dir/file.bin")
	if err != nil {
		t.Fatalf("OpenObjectWriter: %v", err)
	}
	payload := []byte(strings.Repeat("x", 4096))
	if n, err := wc.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write: n=%d err=%v", n, err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(tmp, "sub", "dir", "file.bin"))
	if err != nil {
		t.Fatalf("read written file: %v", err)
	}
	if len(got) != len(payload) {
		t.Errorf("file size = %d, want %d", len(got), len(payload))
	}
	if string(got[:8]) != "xxxxxxxx" {
		t.Errorf("file content mismatch: %q", got[:16])
	}
}

// TestLocalBackend_OpenObjectWriter_CreatesParentDirs verifies the
// implementation creates intermediate directories on demand. The
// parquet writer typically targets paths like "raw/orders/data/part-*.parquet"
// where parent dirs may not yet exist.
func TestLocalBackend_OpenObjectWriter_CreatesParentDirs(t *testing.T) {
	tmp := t.TempDir()
	be := NewLocalBackend(LocalConfig{DataDir: tmp})

	wc, err := be.OpenObjectWriter(context.Background(), "a/b/c/d/file.bin")
	if err != nil {
		t.Fatalf("OpenObjectWriter: %v", err)
	}
	if _, err := wc.Write([]byte("hi")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := os.Stat(filepath.Join(tmp, "a", "b", "c", "d", "file.bin")); err != nil {
		t.Fatalf("nested file not created: %v", err)
	}
}

// TestLocalBackend_OpenObjectWriter_S3URLPath verifies that storage paths
// arriving as `s3://bucket/key` URLs are mapped to local files correctly
// (mirrors PutObject's behavior — toLocalPath strips the scheme).
func TestLocalBackend_OpenObjectWriter_S3URLPath(t *testing.T) {
	tmp := t.TempDir()
	be := NewLocalBackend(LocalConfig{DataDir: tmp})

	wc, err := be.OpenObjectWriter(context.Background(), "s3://my-bucket/data/file.parquet")
	if err != nil {
		t.Fatalf("OpenObjectWriter: %v", err)
	}
	if _, err := wc.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tmp, "data", "file.parquet")); err != nil {
		t.Fatalf("expected local file at data/file.parquet: %v", err)
	}
}
