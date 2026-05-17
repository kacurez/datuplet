package datalake

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// FilesystemDataLake implements DataLake interface for local filesystem operations.
// Used by TableCommit when storage type is "filesystem".
type FilesystemDataLake struct {
	root string // Base directory (e.g., "/data/warehouse")
}

// Compile-time interface check.
var _ DataLake = (*FilesystemDataLake)(nil)

// NewFilesystemDataLake creates a new filesystem-backed data lake.
// root is the base directory for all storage operations.
func NewFilesystemDataLake(root string) *FilesystemDataLake {
	return &FilesystemDataLake{root: root}
}

// toLocalPath converts a storage path to a local filesystem path.
//   - "file:///data/warehouse/orgs/.../file" → "/data/warehouse/orgs/.../file" (strip file://)
//   - "/absolute/path" → "/absolute/path" (pass through)
//   - "orgs/myorg/.../file" → "/data/warehouse/orgs/myorg/.../file" (join with root)
func (f *FilesystemDataLake) toLocalPath(storagePath string) string {
	if path, ok := strings.CutPrefix(storagePath, "file://"); ok {
		return path
	}
	if filepath.IsAbs(storagePath) {
		return storagePath
	}
	return filepath.Join(f.root, storagePath)
}

// Write writes data to the specified path.
func (f *FilesystemDataLake) Write(ctx context.Context, path string, reader io.Reader, size int64) error {
	localPath := f.toLocalPath(path)

	// Create parent directories
	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", localPath, err)
	}
	defer file.Close()

	if _, err := io.Copy(file, reader); err != nil {
		return fmt.Errorf("failed to write file %s: %w", localPath, err)
	}

	return nil
}

// Read reads data from the specified path.
// If offset >= 0 and length > 0, performs a range read.
// Use offset=-1 and length=-1 to read the entire object.
func (f *FilesystemDataLake) Read(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	localPath := f.toLocalPath(path)

	file, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", localPath, err)
	}

	// Range read
	if offset >= 0 && length > 0 {
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to seek in file %s: %w", localPath, err)
		}
		return &limitedReadCloser{
			Reader: io.LimitReader(file, length),
			Closer: file,
		}, nil
	}

	return file, nil
}

// limitedReadCloser wraps a LimitReader with a Closer.
type limitedReadCloser struct {
	io.Reader
	Closer io.Closer
}

func (l *limitedReadCloser) Close() error {
	return l.Closer.Close()
}

// List returns objects under the given prefix.
// Returns ObjectInfo with paths relative to root, consistent with MinIODataLake behavior.
func (f *FilesystemDataLake) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	localPath := f.toLocalPath(prefix)

	// Check if the path exists
	info, err := os.Stat(localPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No objects under this prefix
		}
		return nil, fmt.Errorf("failed to stat %s: %w", localPath, err)
	}

	var objects []ObjectInfo

	if !info.IsDir() {
		// Single file
		relPath, err := filepath.Rel(f.root, localPath)
		if err != nil {
			relPath = localPath
		}
		objects = append(objects, ObjectInfo{
			Path:         relPath,
			Size:         info.Size(),
			LastModified: info.ModTime(),
			IsDir:        false,
		})
		return objects, nil
	}

	// Walk directory recursively
	err = filepath.WalkDir(localPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil // Skip directories, only return files
		}

		fileInfo, err := d.Info()
		if err != nil {
			return err
		}

		// Return path relative to root (matching MinIO DataLake behavior)
		relPath, err := filepath.Rel(f.root, path)
		if err != nil {
			relPath = path
		}

		objects = append(objects, ObjectInfo{
			Path:         relPath,
			Size:         fileInfo.Size(),
			LastModified: fileInfo.ModTime(),
			IsDir:        false,
		})

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to walk directory %s: %w", localPath, err)
	}

	return objects, nil
}

// Exists checks if an object exists at the specified path.
func (f *FilesystemDataLake) Exists(ctx context.Context, path string) (bool, error) {
	localPath := f.toLocalPath(path)

	_, err := os.Stat(localPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to stat %s: %w", localPath, err)
	}
	return true, nil
}

// EnsureRoot creates the root directory if it doesn't exist.
func (f *FilesystemDataLake) EnsureRoot() error {
	return os.MkdirAll(f.root, 0755)
}

