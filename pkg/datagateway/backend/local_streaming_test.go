package backend

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLocalBackend_OpenObjectWriter verifies the streaming-write path for the
// local backend: bytes flow directly to disk without an in-process []byte
// buffer (the streaming-upload optimization). Intermediate directories are
// created on demand (the parquet writer typically targets paths like
// "raw/orders/data/part-*.parquet" whose parents may not exist yet), and
// storage paths arriving as `s3://bucket/key` URLs map to local files
// (mirrors PutObject's behavior — toLocalPath strips the scheme).
func TestLocalBackend_OpenObjectWriter(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		payload []byte
		wantRel string
	}{
		{"relative path", "sub/dir/file.bin", []byte(strings.Repeat("x", 4096)), "sub/dir/file.bin"},
		{"creates parent dirs", "a/b/c/d/file.bin", []byte("hi"), "a/b/c/d/file.bin"},
		{"s3 URL path", "s3://my-bucket/data/file.parquet", []byte("payload"), "data/file.parquet"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			be := NewLocalBackend(LocalConfig{DataDir: tmp})

			wc, err := be.OpenObjectWriter(context.Background(), tc.path)
			if err != nil {
				t.Fatalf("OpenObjectWriter: %v", err)
			}
			if n, err := wc.Write(tc.payload); err != nil || n != len(tc.payload) {
				t.Fatalf("Write: n=%d err=%v", n, err)
			}
			if err := wc.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			got, err := os.ReadFile(filepath.Join(tmp, filepath.FromSlash(tc.wantRel)))
			if err != nil {
				t.Fatalf("expected local file at %s: %v", tc.wantRel, err)
			}
			if string(got) != string(tc.payload) {
				t.Errorf("file content mismatch: got %d bytes, want %d", len(got), len(tc.payload))
			}
		})
	}
}
