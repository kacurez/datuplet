package buffer

import (
	"io"
	"os"
	"path/filepath"
)

// createLocalFile creates a local file for writing, creating parent directories if needed.
func createLocalFile(path string) (io.WriteCloser, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return os.Create(path)
}

// countingWriter wraps a writer and counts bytes written.
type countingWriter struct {
	w     io.Writer
	count int64
}

func newCountingWriter(w io.Writer) *countingWriter {
	return &countingWriter{w: w}
}

func (c *countingWriter) Write(p []byte) (n int, err error) {
	n, err = c.w.Write(p)
	c.count += int64(n)
	return
}

func (c *countingWriter) BytesWritten() int64 {
	return c.count
}
