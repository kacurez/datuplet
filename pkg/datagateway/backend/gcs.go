package backend

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

// iteratorDone aliases the storage object-iterator sentinel returned by
// `*storage.ObjectIterator.Next()` after the last result. Hoisted to a
// file-scope identifier so call sites read naturally (`err == iteratorDone`)
// rather than the verbose package-qualified form.
var iteratorDone = iterator.Done

// GCSConfig is the static-key path used by tests and by the local-mode
// dev flow. Production callers use NewGCSBackendWithProvider — the
// VendedCreds path with lakekeeper-vended OAuth tokens.
type GCSConfig struct {
	Bucket            string
	ServiceAccountKey []byte // optional; falls back to ADC when nil
	ChunkSize         int64  // read/write chunk size; defaults to 10 MiB.
}

// GCSProviderConfig is the production path. VendedCreds must have
// ExpectedCredsType set to CredsTypeGCS.
type GCSProviderConfig struct {
	Bucket      string
	VendedCreds *catalogwriter.VendedCreds
	ChunkSize   int64 // read/write chunk size; defaults to 10 MiB.
}

// gcsBackend implements the StorageBackend interface using GCS as the
// object-storage backend.
type gcsBackend struct {
	bucket    string
	client    *storage.Client
	bkt       *storage.BucketHandle
	chunkSize int64
}

// defaultGCSChunkSize matches MinIO's 10 MiB read/write chunking default.
const defaultGCSChunkSize = 10 * 1024 * 1024

// NewGCSBackend constructs a backend using static credentials.
func NewGCSBackend(cfg GCSConfig) (*gcsBackend, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("gcs: bucket required")
	}
	ctx := context.Background()
	opts := []option.ClientOption{option.WithUserAgent("datuplet-datagateway")}
	if len(cfg.ServiceAccountKey) > 0 {
		opts = append(opts, option.WithCredentialsJSON(cfg.ServiceAccountKey))
	} else if os.Getenv("STORAGE_EMULATOR_HOST") != "" {
		// Emulator mode (fake-gcs-server): the SDK detects the env var
		// for endpoint routing, but if no credentials are provided and
		// ADC fails (the common dev/CI case), some download endpoints
		// silently route through real storage.googleapis.com — producing
		// a confusing "object doesn't exist" right after a "successful"
		// upload. Explicit anonymous mode aligns all endpoints.
		opts = append(opts, option.WithoutAuthentication())
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	chunk := cfg.ChunkSize
	if chunk <= 0 {
		chunk = defaultGCSChunkSize
	}
	return &gcsBackend{
		bucket:    cfg.Bucket,
		client:    client,
		bkt:       client.Bucket(cfg.Bucket),
		chunkSize: chunk,
	}, nil
}

// NewGCSBackendWithProvider constructs a backend using lakekeeper-vended
// OAuth tokens. The TokenSource refreshes via VendedCreds.Get() — see RFC
// 019 §4.3.
func NewGCSBackendWithProvider(cfg GCSProviderConfig) (*gcsBackend, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("gcs: bucket required")
	}
	if cfg.VendedCreds == nil {
		return nil, fmt.Errorf("gcs: VendedCreds required")
	}
	if cfg.VendedCreds.ExpectedCredsType != catalogwriter.CredsTypeGCS {
		return nil, fmt.Errorf("gcs: VendedCreds.ExpectedCredsType must be CredsTypeGCS, got %q", cfg.VendedCreds.ExpectedCredsType)
	}
	ctx := context.Background()
	ts := &vendedTokenSource{vc: cfg.VendedCreds}
	client, err := storage.NewClient(ctx,
		option.WithTokenSource(ts),
		option.WithUserAgent("datuplet-datagateway"),
	)
	if err != nil {
		return nil, err
	}
	chunk := cfg.ChunkSize
	if chunk <= 0 {
		chunk = defaultGCSChunkSize
	}
	return &gcsBackend{
		bucket:    cfg.Bucket,
		client:    client,
		bkt:       client.Bucket(cfg.Bucket),
		chunkSize: chunk,
	}, nil
}

// credsFetcher is the abstraction vendedTokenSource depends on.
// Production: *catalogwriter.VendedCreds. Tests: in-package fake.
// Keeping this tiny (single method) is intentional — it isolates the
// token-source's lifecycle from VendedCreds's caching machinery and
// makes Token() unit-testable without spinning up an HTTP server.
type credsFetcher interface {
	Get(ctx context.Context) (catalogwriter.Creds, error)
}

// vendedTokenSource adapts VendedCreds to oauth2.TokenSource. This is the
// ONE place GCSCreds.OAuthToken is read out of the sealed-interface value;
// the bearer flows from here into the *storage.Client and MUST NOT be
// logged or formatted anywhere downstream (see RFC 019 §4.10).
type vendedTokenSource struct {
	vc credsFetcher
}

// Compile-time assertion that vendedTokenSource satisfies oauth2.TokenSource.
var _ oauth2.TokenSource = (*vendedTokenSource)(nil)

// String returns a placeholder; vendedTokenSource holds a credsFetcher
// whose Get() yields the live bearer, so any %v / %+v formatter that hits
// this method MUST NOT recurse into the underlying value. RFC 019 §4.10.
func (t *vendedTokenSource) String() string { return "<vendedTokenSource>" }

// Token fetches the current vended creds, asserts they're GCSCreds, and
// returns an *oauth2.Token suitable for the storage client. The
// type-assertion is the load-bearing safety check: even though
// NewGCSBackendWithProvider validates ExpectedCredsType up front, this
// runtime check defends against a misconfigured/poisoned VendedCreds that
// somehow returns the wrong family — fail closed, never hand a non-GCS
// secret to the GCS client.
//
// Redaction: the wrong-type error formats %T (type-only); never %v / %+v
// — formatting the Creds value would leak the bearer. See RFC 019 §4.10.
func (t *vendedTokenSource) Token() (*oauth2.Token, error) {
	// oauth2.TokenSource.Token() carries no context; context.Background() is
	// the only option here. VendedCreds enforces a 30s per-request timeout
	// via defaultHTTPTimeout in catalogwriter so a hung lakekeeper can't
	// deadlock the storage client.
	c, err := t.vc.Get(context.Background())
	if err != nil {
		return nil, err
	}
	gc, ok := c.(catalogwriter.GCSCreds)
	if !ok {
		return nil, fmt.Errorf("vendedTokenSource: expected GCSCreds, got %T", c)
	}
	return &oauth2.Token{
		AccessToken: gc.OAuthToken,
		Expiry:      gc.ExpiresAt(),
		TokenType:   "Bearer",
	}, nil
}

// toObjectKey converts a storage path that may be a full GCS URL
// ("gs://bucket/path") into the bucket-relative object key ("path").
// Mirrors MinIOBackend.toObjectKey with the gs:// scheme.
//
// Examples:
//   - "gs://mybucket/path/to/file" → "path/to/file"
//   - "gs://otherbucket/path/to/file" → "path/to/file" (extracts path regardless of bucket)
//   - "path/to/file" → "path/to/file" (already relative)
func (g *gcsBackend) toObjectKey(storagePath string) string {
	const scheme = "gs://"
	if len(storagePath) >= len(scheme) && storagePath[:len(scheme)] == scheme {
		withoutScheme := storagePath[len(scheme):]
		for i := 0; i < len(withoutScheme); i++ {
			if withoutScheme[i] == '/' {
				return withoutScheme[i+1:]
			}
		}
		// Edge case: gs://bucket with no path
		return ""
	}
	return storagePath
}

// PutObject uploads raw bytes to the given path. The path may be a full
// "gs://bucket/key" URL or a bucket-relative key; toObjectKey normalises it.
func (g *gcsBackend) PutObject(ctx context.Context, storagePath string, data []byte) error {
	objectKey := g.toObjectKey(storagePath)
	w := g.bkt.Object(objectKey).NewWriter(ctx)
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return fmt.Errorf("gcs: put %q: %w", objectKey, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("gcs: put %q: close: %w", objectKey, err)
	}
	return nil
}

// OpenObjectWriter opens a streaming writer that uploads chunks to GCS as
// the caller writes. Peak memory inside the storage.Writer is bounded by
// ChunkSize (4 MiB here) rather than the entire object — this is the
// streaming-upload optimization. Detected via the optional
// objectStreamingBackend interface in pkg/datagateway/buffer.
//
// Setting ChunkSize=0 would disable resumable uploads (single-shot,
// no retry); we deliberately keep ChunkSize > 0 so transient failures
// retry per-chunk rather than re-uploading the whole file.
func (g *gcsBackend) OpenObjectWriter(ctx context.Context, storagePath string) (io.WriteCloser, error) {
	objectKey := g.toObjectKey(storagePath)
	w := g.bkt.Object(objectKey).NewWriter(ctx)
	w.ChunkSize = 4 * 1024 * 1024 // 4 MiB resumable-upload chunks
	return w, nil
}

// Close releases the underlying storage client. Idempotent.
func (g *gcsBackend) Close() error {
	if g.client == nil {
		return nil
	}
	return g.client.Close()
}

// GetObject downloads raw bytes from the given path. The path may be a full
// "gs://bucket/key" URL or a bucket-relative key.
func (g *gcsBackend) GetObject(ctx context.Context, storagePath string) ([]byte, error) {
	objectKey := g.toObjectKey(storagePath)
	r, err := g.bkt.Object(objectKey).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("gcs: get %q: %w", objectKey, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("gcs: read %q: %w", objectKey, err)
	}
	return data, nil
}

// findDataFiles lists data files under prefix. Mirrors MinIOBackend.findDataFiles:
// returns CSV / Parquet / JSON / JSONL files and skips metadata (`_*`,
// `*.metadata.json`, anything under a `/metadata/` segment).
func (g *gcsBackend) findDataFiles(ctx context.Context, prefix string) ([]fileInfo, error) {
	var files []fileInfo
	extensions := []string{".csv", ".parquet", ".json", ".jsonl"}

	it := g.bkt.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if err == iteratorDone {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("gcs: list %q: %w", prefix, err)
		}

		// Skip directory placeholders.
		if strings.HasSuffix(attrs.Name, "/") {
			continue
		}

		// Skip metadata files (Iceberg + write-session metadata).
		baseName := path.Base(attrs.Name)
		if strings.HasPrefix(baseName, "_") ||
			strings.HasSuffix(attrs.Name, ".metadata.json") ||
			strings.Contains(attrs.Name, "/metadata/") {
			continue
		}

		// Data file?
		lower := strings.ToLower(attrs.Name)
		for _, ext := range extensions {
			if strings.HasSuffix(lower, ext) {
				files = append(files, fileInfo{path: attrs.Name, size: attrs.Size})
				break
			}
		}
	}

	return files, nil
}

// OpenReader opens a reader over the data files under tablePath.
// Mirrors MinIOBackend.OpenReader: prefers tablePath/data/ (Iceberg layout),
// falls back to tablePath (legacy layout).
func (g *gcsBackend) OpenReader(ctx context.Context, tablePath string) (Reader, error) {
	icebergDataPath := path.Join(tablePath, "data") + "/"
	files, err := g.findDataFiles(ctx, icebergDataPath)
	if err != nil {
		return nil, fmt.Errorf("gcs: find data files in %s: %w", tablePath, err)
	}

	if len(files) == 0 {
		searchPath := tablePath
		if !strings.HasSuffix(searchPath, "/") {
			searchPath = searchPath + "/"
		}
		files, err = g.findDataFiles(ctx, searchPath)
		if err != nil {
			return nil, fmt.Errorf("gcs: find data files in %s: %w", tablePath, err)
		}
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("gcs: no data files found in %s or %s", tablePath, icebergDataPath)
	}

	format := detectFormat(files[0].path)

	return &gcsReader{
		bkt:       g.bkt,
		files:     files,
		format:    format,
		chunkSize: g.chunkSize,
	}, nil
}

// OpenReaderForFiles creates a reader for a specific list of data files.
// This is used when lakekeeper provides an explicit list of files from an
// Iceberg snapshot. File paths should be absolute GCS paths
// ("gs://bucket/path") or bucket-relative keys; toObjectKey normalises them.
// Mirrors MinIOBackend.OpenReaderForFiles one-to-one.
func (g *gcsBackend) OpenReaderForFiles(ctx context.Context, filePaths []string) (Reader, error) {
	if len(filePaths) == 0 {
		return nil, fmt.Errorf("no files provided")
	}

	// Convert file paths to fileInfo structs.
	files := make([]fileInfo, 0, len(filePaths))
	for _, fp := range filePaths {
		objectKey := g.toObjectKey(fp)

		// Get file size from GCS.
		attrs, err := g.bkt.Object(objectKey).Attrs(ctx)
		if err != nil {
			log.Printf("WARNING: Could not stat file %s: %v", objectKey, err)
			// Still add the file but with unknown size (mirrors MinIO behaviour).
			files = append(files, fileInfo{path: objectKey, size: 0})
		} else {
			files = append(files, fileInfo{path: objectKey, size: attrs.Size})
		}
	}

	// Detect format from first file.
	format := detectFormat(files[0].path)

	return &gcsReader{
		bkt:       g.bkt,
		files:     files,
		format:    format,
		chunkSize: g.chunkSize,
	}, nil
}

// Commit assembles per-writer stats into a CommitResult.
// Mirrors MinIOBackend.Commit: this is the buffered-write commit, not the
// Iceberg snapshot commit. Iceberg-level commits go through pkg/tablecommit
// after the buffered files are stamped in object storage. (When the
// pkg/datupleticeio gs:// factory lands in Slice D, TableCommit gains an
// end-to-end path against GCS without changes here.)
func (g *gcsBackend) Commit(ctx context.Context, writers []Writer) (*CommitResult, error) {
	results := make([]TableCommitResult, len(writers))
	for i, w := range writers {
		gw, ok := w.(*gcsWriter)
		if !ok {
			continue
		}

		stats := gw.Stats()
		results[i] = TableCommitResult{
			OutputName: gw.OutputName(),
			TablePath:  gw.TablePath(),
			Status:     CommitStatusCommitted,
			SnapshotID: 0,
			FilesAdded: stats.PartsWritten,
			RowsAdded:  stats.RowsWritten,
		}
	}
	return &CommitResult{Tables: results}, nil
}

// Rollback deletes all part files written by the given writers.
// Errors from individual deletes are swallowed (same as MinIOBackend.Rollback)
// so a single missing file does not abort the cleanup of the remaining
// staged parts.
func (g *gcsBackend) Rollback(ctx context.Context, writers []Writer) error {
	for _, w := range writers {
		gw, ok := w.(*gcsWriter)
		if !ok {
			continue
		}
		for _, filePath := range gw.filePaths {
			_ = g.bkt.Object(filePath).Delete(ctx)
		}
	}
	return nil
}

// gcsObjectLister + gcsObjectDeleter are the minimal storage surfaces
// RemoveAll depends on. Production code passes the real BucketHandle (via
// a thin adapter); tests inject a fake. Splitting list-vs-delete is
// deliberate so the test can record deletes independently of listing.
type gcsObjectLister interface {
	listObjects(ctx context.Context, prefix string) gcsObjectIter
}

type gcsObjectDeleter interface {
	deleteObject(ctx context.Context, key string) error
}

// gcsObjectIter mirrors *storage.ObjectIterator's Next() contract:
// returns (attrs, nil) on each result and (nil, iteratorDone) when done.
type gcsObjectIter interface {
	Next() (*storage.ObjectAttrs, error)
}

// gcsListDeleteAdapter wraps *storage.BucketHandle into the two narrow
// interfaces. The only place we touch the real bucket from RemoveAll.
type gcsListDeleteAdapter struct {
	bkt *storage.BucketHandle
}

func (a *gcsListDeleteAdapter) listObjects(ctx context.Context, prefix string) gcsObjectIter {
	return a.bkt.Objects(ctx, &storage.Query{Prefix: prefix})
}

func (a *gcsListDeleteAdapter) deleteObject(ctx context.Context, key string) error {
	return a.bkt.Object(key).Delete(ctx)
}

// RemoveAll deletes every object whose key starts with prefix in the
// backend's bucket. Idempotent: empty listing → no error. Context
// cancellation is honoured between objects. Mirrors MinIOBackend.RemoveAll.
func (g *gcsBackend) RemoveAll(ctx context.Context, prefix string) error {
	adapter := &gcsListDeleteAdapter{bkt: g.bkt}
	return removeAllGCSObjects(ctx, adapter, adapter, prefix)
}

// removeAllGCSObjects is the testable core of RemoveAll. The listing-side
// interface and the delete-side interface are separate so tests can record
// each delete call individually without coupling them to a paginated list.
//
// GCS has no native bulk-delete in cloud.google.com/go/storage (the JSON
// API supports it but the SDK doesn't expose a batch surface), so we
// issue one Delete per object. This is acceptable for the workspace-cleanup
// use case — same blast radius as minio's per-batch RemoveObjects.
func removeAllGCSObjects(ctx context.Context, lister gcsObjectLister, deleter gcsObjectDeleter, prefix string) error {
	prefix = strings.TrimPrefix(prefix, "/")

	it := lister.listObjects(ctx, prefix)
	for {
		attrs, err := it.Next()
		if err == iteratorDone {
			break
		}
		if err != nil {
			return fmt.Errorf("RemoveAll %q: list: %w", prefix, err)
		}

		// Honour cancellation between objects so long listings can be cancelled.
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("RemoveAll %q: %w", prefix, err)
		}

		if err := deleter.deleteObject(ctx, attrs.Name); err != nil {
			return fmt.Errorf("RemoveAll %q: delete %q: %w", prefix, attrs.Name, err)
		}
	}

	return nil
}

// OpenWriter opens a writer for tablePath. The writer buffers all writes
// and uploads on Close (mirroring minioWriter).
func (g *gcsBackend) OpenWriter(ctx context.Context, tablePath string, opts WriteOptions) (Writer, error) {
	format := opts.Format
	if format == "" {
		format = "csv"
	}

	return &gcsWriter{
		bkt:        g.bkt,
		tablePath:  tablePath,
		outputName: opts.OutputName,
		format:     format,
		compress:   opts.Compress,
	}, nil
}

// gcsReader implements Reader for GCS objects. Mirrors minioReader: opens
// each fileInfo in sequence, switches on format, and reuses the same
// CSV chunking strategy.
type gcsReader struct {
	bkt         *storage.BucketHandle
	files       []fileInfo
	format      string
	chunkSize   int64
	currentFile int
	object      *storage.Reader
	csvReader   *csv.Reader
	headers     []string
	schema      *SchemaInfo
	totalSize   int64
	initialized bool
	parquetDone bool // tracks if all Parquet files have been read
}

func (r *gcsReader) ReadChunk() (*DataChunk, error) {
	if !r.initialized {
		if err := r.init(); err != nil {
			return nil, err
		}
	}

	switch r.format {
	case "csv":
		return r.readCSVChunk()
	default:
		return r.readRawChunk()
	}
}

func (r *gcsReader) init() error {
	for _, f := range r.files {
		r.totalSize += f.size
	}
	if err := r.openNextFile(); err != nil {
		return err
	}
	r.initialized = true
	return nil
}

func (r *gcsReader) openNextFile() error {
	if r.object != nil {
		r.object.Close()
		r.object = nil
	}

	if r.currentFile >= len(r.files) {
		return io.EOF
	}

	ctx := context.Background()
	obj, err := r.bkt.Object(r.files[r.currentFile].path).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("gcs: open %q: %w", r.files[r.currentFile].path, err)
	}
	r.object = obj
	r.currentFile++

	if r.format == "csv" {
		r.csvReader = csv.NewReader(obj)
		headers, err := r.csvReader.Read()
		if err != nil {
			return err
		}
		r.headers = headers

		columns := make([]ColumnInfo, len(headers))
		for i, h := range headers {
			columns[i] = ColumnInfo{Name: h, Type: "string", Nullable: true}
		}
		r.schema = &SchemaInfo{Columns: columns}
	}

	return nil
}

func (r *gcsReader) readCSVChunk() (*DataChunk, error) {
	var buf strings.Builder
	var rowCount int64

	writer := csv.NewWriter(&buf)
	writer.Write(r.headers)

	for buf.Len() < int(r.chunkSize) {
		record, err := r.csvReader.Read()
		if err == io.EOF {
			if err := r.openNextFile(); err == io.EOF {
				if rowCount == 0 {
					return nil, io.EOF
				}
				writer.Flush()
				return &DataChunk{
					Data:        []byte(buf.String()),
					Format:      "csv",
					IsLast:      true,
					RowsInChunk: rowCount,
				}, nil
			} else if err != nil {
				return nil, err
			}
			continue
		}
		if err != nil {
			return nil, err
		}

		writer.Write(record)
		rowCount++
	}

	writer.Flush()
	return &DataChunk{
		Data:        []byte(buf.String()),
		Format:      "csv",
		IsLast:      false,
		RowsInChunk: rowCount,
	}, nil
}

func (r *gcsReader) readRawChunk() (*DataChunk, error) {
	// Parquet must be read whole — see minioReader.readParquetFile.
	if r.format == "parquet" {
		return r.readParquetFile()
	}

	buf := make([]byte, r.chunkSize)
	n, err := r.object.Read(buf)

	// Go io.Reader spec: always process the n > 0 bytes returned before
	// considering the error err.
	if n > 0 {
		isLast := err == io.EOF
		if isLast {
			if r.currentFile < len(r.files) {
				isLast = false
			}
		}
		return &DataChunk{
			Data:   buf[:n],
			Format: r.format,
			IsLast: isLast,
		}, nil
	}

	if err == io.EOF {
		if err := r.openNextFile(); err == io.EOF {
			return nil, io.EOF
		}
		return r.readRawChunk()
	}
	if err != nil {
		return nil, err
	}

	return &DataChunk{
		Data:   buf[:n],
		Format: r.format,
		IsLast: false,
	}, nil
}

// readParquetFile reads an entire Parquet file into memory.
// Parquet files require reading the footer to parse, so they can't be
// streamed in chunks. Mirrors minioReader.readParquetFile.
func (r *gcsReader) readParquetFile() (*DataChunk, error) {
	if r.parquetDone {
		return nil, io.EOF
	}
	if r.currentFile > len(r.files) {
		r.parquetDone = true
		return nil, io.EOF
	}

	var buf bytes.Buffer
	_, err := io.Copy(&buf, r.object)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("gcs: read parquet: %w", err)
	}

	isLast := r.currentFile >= len(r.files)
	if !isLast {
		if err := r.openNextFile(); err == io.EOF {
			isLast = true
		}
	}
	if isLast {
		r.parquetDone = true
	}

	return &DataChunk{
		Data:   buf.Bytes(),
		Format: r.format,
		IsLast: isLast,
	}, nil
}

func (r *gcsReader) Schema() *SchemaInfo {
	return r.schema
}

func (r *gcsReader) TotalSizeEstimate() int64 {
	return r.totalSize
}

func (r *gcsReader) Close() error {
	if r.object != nil {
		return r.object.Close()
	}
	return nil
}

// gcsWriter implements Writer for GCS objects. Buffers chunks in memory
// (matching minioWriter) and uploads to gs://<bucket>/<tablePath>/part-NNNNN.<ext>
// on Close.
type gcsWriter struct {
	bkt        *storage.BucketHandle
	tablePath  string
	outputName string
	format     string
	compress   bool

	partNum      int32
	bytesWritten int64
	rowsWritten  int64
	filePaths    []string
	headerSet    bool
	buffer       bytes.Buffer
	csvWriter    *csv.Writer
}

func (w *gcsWriter) WriteChunk(data []byte, rows int64) error {
	switch w.format {
	case "csv":
		return w.writeCSVChunk(data, rows)
	default:
		return w.writeRawChunk(data, rows)
	}
}

func (w *gcsWriter) writeCSVChunk(data []byte, rows int64) error {
	reader := csv.NewReader(strings.NewReader(string(data)))

	records, err := reader.ReadAll()
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}

	if w.csvWriter == nil {
		w.csvWriter = csv.NewWriter(&w.buffer)
	}

	startIdx := 0
	if !w.headerSet && len(records) > 0 {
		if err := w.csvWriter.Write(records[0]); err != nil {
			return err
		}
		w.headerSet = true
		startIdx = 1
	}

	for i := startIdx; i < len(records); i++ {
		if err := w.csvWriter.Write(records[i]); err != nil {
			return err
		}
		w.rowsWritten++
	}

	w.csvWriter.Flush()
	w.bytesWritten += int64(len(data))

	return nil
}

func (w *gcsWriter) writeRawChunk(data []byte, rows int64) error {
	w.buffer.Write(data)
	w.bytesWritten += int64(len(data))
	w.rowsWritten += rows
	return nil
}

func (w *gcsWriter) OutputName() string { return w.outputName }
func (w *gcsWriter) TablePath() string  { return w.tablePath }

func (w *gcsWriter) Stats() WriterStats {
	return WriterStats{
		BytesWritten: w.bytesWritten,
		RowsWritten:  w.rowsWritten,
		PartsWritten: w.partNum,
		FilePaths:    w.filePaths,
	}
}

func (w *gcsWriter) Close() error {
	if w.csvWriter != nil {
		w.csvWriter.Flush()
	}

	if w.buffer.Len() > 0 {
		ext := w.format
		if w.compress {
			ext += ".gz"
		}

		filename := fmt.Sprintf("part-%05d.%s", w.partNum, ext)
		objectPath := path.Join(w.tablePath, filename)

		ctx := context.Background()
		ow := w.bkt.Object(objectPath).NewWriter(ctx)
		if _, err := io.Copy(ow, &w.buffer); err != nil {
			_ = ow.Close()
			return fmt.Errorf("gcs: upload %q: %w", objectPath, err)
		}
		if err := ow.Close(); err != nil {
			return fmt.Errorf("gcs: upload %q: close: %w", objectPath, err)
		}

		w.filePaths = append(w.filePaths, objectPath)
		w.partNum++
	}

	return nil
}

// GetSchema returns the schema of the first data file under tablePath.
// CSV is the only supported format here; Iceberg-aware schema resolution
// is the job of the pipeline-api storage catalog (lakekeeper) and is not
// part of the storage backend. Mirrors MinIOBackend.GetSchema.
func (g *gcsBackend) GetSchema(ctx context.Context, tablePath string) (*SchemaInfo, error) {
	files, err := g.findDataFiles(ctx, tablePath)
	if err != nil || len(files) == 0 {
		return nil, fmt.Errorf("gcs: no data files found in %s", tablePath)
	}

	format := detectFormat(files[0].path)
	switch format {
	case "csv":
		return g.getCSVSchema(ctx, files[0].path)
	default:
		return nil, fmt.Errorf("gcs: schema detection not supported for format: %s", format)
	}
}

// GetSample returns a sample of rows from the first data file under tablePath.
// Mirrors MinIOBackend.GetSample (CSV-only).
func (g *gcsBackend) GetSample(ctx context.Context, tablePath string, limit int) (*SampleResult, error) {
	files, err := g.findDataFiles(ctx, tablePath)
	if err != nil || len(files) == 0 {
		return nil, fmt.Errorf("gcs: no data files found in %s", tablePath)
	}

	format := detectFormat(files[0].path)
	switch format {
	case "csv":
		return g.getCSVSample(ctx, files[0].path, limit)
	default:
		return nil, fmt.Errorf("gcs: sampling not supported for format: %s", format)
	}
}

func (g *gcsBackend) getCSVSchema(ctx context.Context, objectKey string) (*SchemaInfo, error) {
	obj, err := g.bkt.Object(objectKey).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("gcs: open %q: %w", objectKey, err)
	}
	defer obj.Close()

	reader := csv.NewReader(obj)
	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	columns := make([]ColumnInfo, len(headers))
	for i, h := range headers {
		columns[i] = ColumnInfo{Name: h, Type: "string", Nullable: true}
	}
	return &SchemaInfo{Columns: columns}, nil
}

func (g *gcsBackend) getCSVSample(ctx context.Context, objectKey string, limit int) (*SampleResult, error) {
	obj, err := g.bkt.Object(objectKey).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("gcs: open %q: %w", objectKey, err)
	}
	defer obj.Close()

	reader := csv.NewReader(obj)
	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	columns := make([]ColumnInfo, len(headers))
	for i, h := range headers {
		columns[i] = ColumnInfo{Name: h, Type: "string", Nullable: true}
	}

	var rows [][]byte
	for i := 0; i < limit; i++ {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		row := make(map[string]string)
		for j, h := range headers {
			if j < len(record) {
				row[h] = record[j]
			}
		}
		jsonRow, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}
		rows = append(rows, jsonRow)
	}

	return &SampleResult{
		Schema:        &SchemaInfo{Columns: columns},
		Rows:          rows,
		TotalEstimate: -1,
	}, nil
}

// Compile-time assertion that *gcsBackend satisfies StorageBackend.
// If a future change to the StorageBackend interface breaks gcsBackend,
// this line fails the build before runtime — the same guard
// MinIOBackend uses.
var _ StorageBackend = (*gcsBackend)(nil)
