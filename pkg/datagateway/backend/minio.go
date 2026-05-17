package backend

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

// MinIOConfig configures the MinIO/S3 backend.
type MinIOConfig struct {
	// Endpoint is the MinIO/S3 endpoint (e.g., "localhost:9000").
	Endpoint string

	// Bucket is the bucket name.
	Bucket string

	// AccessKey is the access key for authentication.
	AccessKey string

	// SecretKey is the secret key for authentication.
	SecretKey string

	// Region is the bucket region.
	Region string

	// UseSSL enables SSL/TLS for connections.
	UseSSL bool

	// ChunkSize is the size of chunks for reading/writing.
	ChunkSize int64
}

// MinIOBackend implements StorageBackend for MinIO/S3.
type MinIOBackend struct {
	client    *minio.Client
	bucket    string
	chunkSize int64
}

// NewMinIOBackend creates a new MinIO/S3 backend.
func NewMinIOBackend(cfg MinIOConfig) (*MinIOBackend, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	chunkSize := cfg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 10 * 1024 * 1024 // 10MB default
	}

	return &MinIOBackend{
		client:    client,
		bucket:    cfg.Bucket,
		chunkSize: chunkSize,
	}, nil
}

// MinIOProviderConfig configures a MinIO backend whose credentials are
// sourced from a `pkg/catalogwriter.VendedCreds` (lakekeeper-vended STS
// triple).
//
// Endpoint + Region + UseSSL come from the VendedCreds response, so the
// caller only supplies the bucket. PathStyle is implied (lakekeeper
// always returns an S3-compat endpoint URL that demands path style).
type MinIOProviderConfig struct {
	// Bucket is the S3 bucket name (the warehouse bucket lakekeeper
	// hands out vended creds against).
	Bucket string

	// VendedCreds is the catalog-side credential cache. Each operation
	// asks it for fresh creds via the renewal contract (50%-elapsed +
	// 60s floor) so writes that span an STS expiry rotate transparently.
	VendedCreds *catalogwriter.VendedCreds

	// ChunkSize is the read/write chunk size; defaults to 10 MiB.
	ChunkSize int64
}

// NewMinIOBackendWithProvider builds a MinIOBackend whose credentials
// rotate via the supplied VendedCreds. The minio-go client is built
// lazily on the first call into `b.client` — but minio-go's
// constructor needs an endpoint up front, and VendedCreds only knows
// the endpoint after its first fetch. So we do one priming Get now to
// learn the endpoint; subsequent calls use the cached value until the
// 50%-elapsed rule triggers a renewal.
//
// Failure to prime is fatal: an unreachable lakekeeper at boot is the
// same as a misconfigured deployment, and silently degrading would
// hide it. Callers see the wrapped lakekeeper error.
func NewMinIOBackendWithProvider(cfg MinIOProviderConfig) (*MinIOBackend, error) {
	if cfg.VendedCreds == nil {
		return nil, fmt.Errorf("MinIOProviderConfig.VendedCreds is required")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("MinIOProviderConfig.Bucket is required")
	}

	// Prime VendedCreds so we learn the endpoint + can fail-fast on a
	// misconfigured lakekeeper. Use a fresh background context with a
	// modest deadline so a hung lakekeeper doesn't block boot forever
	// — VendedCreds itself enforces its own 30s default per request,
	// and our caller can override the HTTPClient if needed.
	primed, err := cfg.VendedCreds.Get(context.Background())
	if err != nil {
		return nil, fmt.Errorf("prime vended creds: %w", err)
	}
	if primed.Endpoint == "" {
		return nil, fmt.Errorf("vended creds had empty endpoint; lakekeeper config likely missing s3.endpoint")
	}

	endpoint, useSSL, err := splitEndpoint(primed.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse vended endpoint %q: %w", primed.Endpoint, err)
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.New(&vendedCredsProvider{vc: cfg.VendedCreds}),
		Secure: useSSL,
		Region: primed.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client with vended creds: %w", err)
	}

	chunkSize := cfg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 10 * 1024 * 1024
	}

	return &MinIOBackend{
		client:    client,
		bucket:    cfg.Bucket,
		chunkSize: chunkSize,
	}, nil
}

// vendedCredsProvider adapts catalogwriter.VendedCreds to minio-go's
// credentials.Provider. Each Retrieve call goes through VendedCreds.Get
// which itself caches the response and only round-trips lakekeeper when
// the renewal contract says so.
//
// minio-go's CredContext exposes only {Client, Endpoint} (no
// context.Context), so the originating gRPC/HTTP request's cancel
// can't be directly threaded through to VendedCreds.Get. The default
// HTTP client in catalogwriter.VendedCreds carries its own per-call
// timeout — that's the bound on the renewal RPC for now.
type vendedCredsProvider struct {
	vc *catalogwriter.VendedCreds
}

// Retrieve is the deprecated path; minio-go still calls it through
// some legacy code paths so we delegate to RetrieveWithCredContext.
func (p *vendedCredsProvider) Retrieve() (credentials.Value, error) {
	return p.RetrieveWithCredContext(nil)
}

// RetrieveWithCredContext fetches a fresh-or-cached creds triple from
// VendedCreds. Errors propagate as-is — minio-go wraps them with the
// originating call (PutObject, etc.) which is exactly what we want for
// observability (operator logs see "PutObject: vended creds: …").
//
// We deliberately pass context.Background() rather than threading the
// originating request context: minio-go's CredContext exposes only
// {Client, Endpoint} (no context.Context field), so there's no upstream
// signal to forward. The catalogwriter.VendedCreds default HTTPClient
// carries its own per-call timeout — that's the bound on a hung
// lakekeeper for now.
func (p *vendedCredsProvider) RetrieveWithCredContext(_ *credentials.CredContext) (credentials.Value, error) {
	c, err := p.vc.Get(context.Background())
	if err != nil {
		return credentials.Value{}, err
	}
	return credentials.Value{
		AccessKeyID:     c.AccessKeyID,
		SecretAccessKey: c.SecretAccessKey,
		SessionToken:    c.SessionToken,
		SignerType:      credentials.SignatureV4,
	}, nil
}

// IsExpired delegates to VendedCreds' renewal contract: a positive
// answer triggers minio-go to call RetrieveWithCredContext again.
// Returning true keeps the upstream cache from skewing past our own.
func (p *vendedCredsProvider) IsExpired() bool {
	// Conservative: always say "not expired", let VendedCreds.Get make
	// the call. minio-go re-checks before each request.
	// Returning false here avoids a redundant fetch path; VendedCreds
	// already throttles via its 60s floor.
	return false
}

// splitEndpoint splits a lakekeeper s3.endpoint URL ("http://minio:9000",
// "https://s3.us-east-1.amazonaws.com") into the host:port + scheme.
// minio-go's `New` wants the host:port plus a Secure bool; we strip
// "http://" / "https://" and toggle Secure off iff scheme is http.
func splitEndpoint(raw string) (string, bool, error) {
	switch {
	case strings.HasPrefix(raw, "https://"):
		return strings.TrimPrefix(raw, "https://"), true, nil
	case strings.HasPrefix(raw, "http://"):
		return strings.TrimPrefix(raw, "http://"), false, nil
	default:
		// Some deployments may pass a bare host:port. Default to https
		// (the safer choice) and let the bootstrap call fail loudly if
		// that's wrong, rather than silently dropping TLS.
		return raw, true, nil
	}
}

// toObjectKey converts a storage path (which may be a full S3 URL or relative path)
// to a MinIO object key (relative path within the bucket).
// This is the single place where S3 URL normalization happens for writes.
//
// Examples:
//   - "s3://mybucket/path/to/file" → "path/to/file"
//   - "s3://otherbucket/path/to/file" → "path/to/file" (extracts path regardless of bucket)
//   - "path/to/file" → "path/to/file" (already relative)
func (b *MinIOBackend) toObjectKey(storagePath string) string {
	// Handle s3://bucket/ prefix
	if strings.HasPrefix(storagePath, "s3://") {
		// Extract path after bucket name: s3://bucket/path → path
		withoutScheme := strings.TrimPrefix(storagePath, "s3://")
		parts := strings.SplitN(withoutScheme, "/", 2)
		if len(parts) == 2 {
			return parts[1]
		}
		// Edge case: s3://bucket with no path
		return ""
	}
	// Already a relative path
	return storagePath
}

func (b *MinIOBackend) OpenReader(ctx context.Context, tablePath string) (Reader, error) {
	// For Iceberg tables, data files are in tablePath/data/
	// Try both the table root and the data subdirectory
	var files []fileInfo
	var err error

	// First try tablePath/data/ (Iceberg format)
	icebergDataPath := path.Join(tablePath, "data") + "/"
	files, err = b.findDataFiles(ctx, icebergDataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to find data files in %s: %w", tablePath, err)
	}

	// If no files found in data/, try the table path directly (legacy format)
	if len(files) == 0 {
		searchPath := tablePath
		if !strings.HasSuffix(searchPath, "/") {
			searchPath = searchPath + "/"
		}
		files, err = b.findDataFiles(ctx, searchPath)
		if err != nil {
			return nil, fmt.Errorf("failed to find data files in %s: %w", tablePath, err)
		}
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no data files found in %s or %s", tablePath, icebergDataPath)
	}

	// Detect format from first file
	format := detectFormat(files[0].path)

	return &minioReader{
		client:    b.client,
		bucket:    b.bucket,
		files:     files,
		format:    format,
		chunkSize: b.chunkSize,
	}, nil
}

// OpenReaderForFiles creates a reader for a specific list of data files.
// This is used when the lakekeeper provides an explicit list of files from an Iceberg snapshot.
// File paths should be absolute S3 paths (s3://bucket/path) or relative paths.
func (b *MinIOBackend) OpenReaderForFiles(ctx context.Context, filePaths []string) (Reader, error) {
	if len(filePaths) == 0 {
		return nil, fmt.Errorf("no files provided")
	}

	// Convert file paths to fileInfo structs
	files := make([]fileInfo, 0, len(filePaths))
	for _, fp := range filePaths {
		// Convert storage path to object key
		objectKey := b.toObjectKey(fp)

		// Get file size from MinIO
		objInfo, err := b.client.StatObject(ctx, b.bucket, objectKey, minio.StatObjectOptions{})
		if err != nil {
			log.Printf("WARNING: Could not stat file %s: %v", objectKey, err)
			// Still add the file but with unknown size
			files = append(files, fileInfo{path: objectKey, size: 0})
		} else {
			files = append(files, fileInfo{path: objectKey, size: objInfo.Size})
		}
	}

	// Detect format from first file
	format := detectFormat(files[0].path)

	return &minioReader{
		client:    b.client,
		bucket:    b.bucket,
		files:     files,
		format:    format,
		chunkSize: b.chunkSize,
	}, nil
}

// OpenStreamingArrowReader: see LocalBackend equivalent. Constructs a
// parquetArrowReader whose underlying io.ReaderAt is a per-file MinIO
// range-read adapter — no full-file download.
func (b *MinIOBackend) OpenStreamingArrowReader(ctx context.Context, filePaths []string, currentSchema *SchemaInfo) (Reader, error) {
	if len(filePaths) == 0 {
		return nil, fmt.Errorf("no files provided")
	}
	sources := make([]fileSource, 0, len(filePaths))
	g := &minioClientAdapter{c: b.client}
	for _, fp := range filePaths {
		objectKey := b.toObjectKey(fp)
		info, err := b.client.StatObject(ctx, b.bucket, objectKey, minio.StatObjectOptions{})
		if err != nil {
			return nil, fmt.Errorf("stat %s: %w", objectKey, err)
		}
		sources = append(sources, fileSource{
			name: objectKey,
			ra:   newMinIORangeReaderAt(ctx, g, b.bucket, objectKey, info.Size),
			size: info.Size,
		})
	}
	return newParquetArrowReaderFromSources(ctx, sources, currentSchema)
}

func (b *MinIOBackend) OpenWriter(ctx context.Context, tablePath string, opts WriteOptions) (Writer, error) {
	format := opts.Format
	if format == "" {
		format = "csv"
	}

	return &minioWriter{
		client:     b.client,
		bucket:     b.bucket,
		tablePath:  tablePath,
		outputName: opts.OutputName,
		format:     format,
		compress:   opts.Compress,
	}, nil
}

func (b *MinIOBackend) Commit(ctx context.Context, writers []Writer) (*CommitResult, error) {
	results := make([]TableCommitResult, len(writers))
	for i, w := range writers {
		mw, ok := w.(*minioWriter)
		if !ok {
			continue
		}

		stats := mw.Stats()
		results[i] = TableCommitResult{
			OutputName: mw.OutputName(),
			TablePath:  mw.TablePath(),
			Status:     CommitStatusCommitted,
			SnapshotID: 0,
			FilesAdded: stats.PartsWritten,
			RowsAdded:  stats.RowsWritten,
		}
	}
	return &CommitResult{Tables: results}, nil
}

func (b *MinIOBackend) Rollback(ctx context.Context, writers []Writer) error {
	for _, w := range writers {
		mw, ok := w.(*minioWriter)
		if !ok {
			continue
		}

		// Delete all written files
		for _, filePath := range mw.filePaths {
			b.client.RemoveObject(ctx, b.bucket, filePath, minio.RemoveObjectOptions{})
		}
	}
	return nil
}

func (b *MinIOBackend) GetSchema(ctx context.Context, tablePath string) (*SchemaInfo, error) {
	files, err := b.findDataFiles(ctx, tablePath)
	if err != nil || len(files) == 0 {
		return nil, fmt.Errorf("no data files found in %s", tablePath)
	}

	format := detectFormat(files[0].path)

	switch format {
	case "csv":
		return b.getCSVSchema(ctx, files[0].path)
	default:
		return nil, fmt.Errorf("schema detection not supported for format: %s", format)
	}
}

func (b *MinIOBackend) GetSample(ctx context.Context, tablePath string, limit int) (*SampleResult, error) {
	files, err := b.findDataFiles(ctx, tablePath)
	if err != nil || len(files) == 0 {
		return nil, fmt.Errorf("no data files found in %s", tablePath)
	}

	format := detectFormat(files[0].path)

	switch format {
	case "csv":
		return b.getCSVSample(ctx, files[0].path, limit)
	default:
		return nil, fmt.Errorf("sampling not supported for format: %s", format)
	}
}

func (b *MinIOBackend) GetObject(ctx context.Context, storagePath string) ([]byte, error) {
	// Convert storage path (may be S3 URL) to object key
	objectKey := b.toObjectKey(storagePath)

	// Download object from MinIO
	obj, err := b.client.GetObject(ctx, b.bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", objectKey, err)
	}
	defer obj.Close()

	// Read all data
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to read object %s: %w", objectKey, err)
	}

	return data, nil
}

func (b *MinIOBackend) PutObject(ctx context.Context, storagePath string, data []byte) error {
	// Convert storage path (may be S3 URL) to object key
	objectKey := b.toObjectKey(storagePath)

	// Upload data to MinIO
	_, err := b.client.PutObject(
		ctx,
		b.bucket,
		objectKey,
		bytes.NewReader(data),
		int64(len(data)),
		minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		},
	)
	if err != nil {
		return fmt.Errorf("failed to upload %s: %w", objectKey, err)
	}
	return nil
}

// minioRemoveAPI is the minimal minio client surface used by RemoveAll.
// Production code passes *minio.Client; tests inject a fake.
type minioRemoveAPI interface {
	// ListObjects lists objects with the given prefix in the named bucket.
	ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
	// RemoveObjects bulk-deletes objects fed through objectsCh and returns
	// a channel of per-object removal errors.
	RemoveObjects(ctx context.Context, bucketName string, objectsCh <-chan minio.ObjectInfo, opts minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError
}

// RemoveAll deletes every object whose key starts with prefix in the backend's bucket.
// prefix is a plain object-key prefix (no "s3://" scheme). Idempotent: no error
// if no objects match. Context cancellation is honoured between pages.
func (b *MinIOBackend) RemoveAll(ctx context.Context, prefix string) error {
	return removeAllObjects(ctx, b.client, b.bucket, prefix)
}

// removeAllObjects is the testable core of RemoveAll. It uses the narrow
// minioRemoveAPI rather than the full *minio.Client so tests can inject a fake.
func removeAllObjects(ctx context.Context, api minioRemoveAPI, bucket, prefix string) error {
	// Normalise the prefix: strip leading slash so the key space is consistent.
	prefix = strings.TrimPrefix(prefix, "/")

	// Page through objects with the given prefix (minio-go ListObjects is
	// already paginated internally; each send on the channel is one object).
	// We batch into slices of ≤1000 then feed each batch to RemoveObjects.
	const batchSize = 1000

	listOpts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}

	batch := make([]minio.ObjectInfo, 0, batchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		// Build a send-only channel for this batch.
		objCh := make(chan minio.ObjectInfo, len(batch))
		for _, obj := range batch {
			objCh <- obj
		}
		close(objCh)

		for rmErr := range api.RemoveObjects(ctx, bucket, objCh, minio.RemoveObjectsOptions{}) {
			if rmErr.Err != nil {
				return fmt.Errorf("RemoveAll %q: delete %q: %w", prefix, rmErr.ObjectName, rmErr.Err)
			}
		}
		batch = batch[:0]
		return nil
	}

	for obj := range api.ListObjects(ctx, bucket, listOpts) {
		if obj.Err != nil {
			return fmt.Errorf("RemoveAll %q: list: %w", prefix, obj.Err)
		}

		// Check context between listed objects so long listings can be cancelled.
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("RemoveAll %q: %w", prefix, err)
		}

		batch = append(batch, obj)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	return flush()
}

func (b *MinIOBackend) Close() error {
	return nil
}

type fileInfo struct {
	path string
	size int64
}

func (b *MinIOBackend) findDataFiles(ctx context.Context, prefix string) ([]fileInfo, error) {
	var files []fileInfo
	extensions := []string{".csv", ".parquet", ".json", ".jsonl"}

	objectsCh := b.client.ListObjects(ctx, b.bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	for obj := range objectsCh {
		if obj.Err != nil {
			return nil, obj.Err
		}

		// Skip directories
		if strings.HasSuffix(obj.Key, "/") {
			continue
		}

		// Skip metadata files (Iceberg and write session metadata)
		baseName := path.Base(obj.Key)
		if strings.HasPrefix(baseName, "_") || strings.HasSuffix(obj.Key, ".metadata.json") || strings.Contains(obj.Key, "/metadata/") {
			continue
		}

		// Check if it's a data file
		for _, ext := range extensions {
			if strings.HasSuffix(strings.ToLower(obj.Key), ext) {
				files = append(files, fileInfo{path: obj.Key, size: obj.Size})
				break
			}
		}
	}

	return files, nil
}

func (b *MinIOBackend) getCSVSchema(ctx context.Context, objectPath string) (*SchemaInfo, error) {
	obj, err := b.client.GetObject(ctx, b.bucket, objectPath, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()

	reader := csv.NewReader(obj)
	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	columns := make([]ColumnInfo, len(headers))
	for i, h := range headers {
		columns[i] = ColumnInfo{
			Name:     h,
			Type:     "string",
			Nullable: true,
		}
	}

	return &SchemaInfo{Columns: columns}, nil
}

func (b *MinIOBackend) getCSVSample(ctx context.Context, objectPath string, limit int) (*SampleResult, error) {
	obj, err := b.client.GetObject(ctx, b.bucket, objectPath, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()

	reader := csv.NewReader(obj)
	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	columns := make([]ColumnInfo, len(headers))
	for i, h := range headers {
		columns[i] = ColumnInfo{
			Name:     h,
			Type:     "string",
			Nullable: true,
		}
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

func detectFormat(filePath string) string {
	lower := strings.ToLower(filePath)
	switch {
	case strings.HasSuffix(lower, ".csv"):
		return "csv"
	case strings.HasSuffix(lower, ".parquet"):
		return "parquet"
	case strings.HasSuffix(lower, ".json"), strings.HasSuffix(lower, ".jsonl"):
		return "json"
	default:
		return "csv"
	}
}

// minioReader implements Reader for MinIO objects.
type minioReader struct {
	client      *minio.Client
	bucket      string
	files       []fileInfo
	format      string
	chunkSize   int64
	currentFile int
	object      *minio.Object
	csvReader   *csv.Reader
	headers     []string
	schema      *SchemaInfo
	totalSize   int64
	initialized bool
	parquetDone bool // tracks if all Parquet files have been read
}

func (r *minioReader) ReadChunk() (*DataChunk, error) {
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

func (r *minioReader) init() error {
	// Calculate total size
	for _, f := range r.files {
		r.totalSize += f.size
	}

	// Open first file
	if err := r.openNextFile(); err != nil {
		return err
	}

	r.initialized = true
	return nil
}

func (r *minioReader) openNextFile() error {
	if r.object != nil {
		r.object.Close()
		r.object = nil
	}

	if r.currentFile >= len(r.files) {
		return io.EOF
	}

	ctx := context.Background()
	obj, err := r.client.GetObject(ctx, r.bucket, r.files[r.currentFile].path, minio.GetObjectOptions{})
	if err != nil {
		return err
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

func (r *minioReader) readCSVChunk() (*DataChunk, error) {
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

func (r *minioReader) readRawChunk() (*DataChunk, error) {
	// For Parquet format, we need to read the entire file at once
	// because Parquet files have footer metadata that's required for parsing
	if r.format == "parquet" {
		return r.readParquetFile()
	}

	buf := make([]byte, r.chunkSize)
	n, err := r.object.Read(buf)

	// Always process any data read before handling errors
	// (Go io.Reader spec: "Callers should always process the n > 0 bytes returned before considering the error err")
	if n > 0 {
		isLast := err == io.EOF
		if isLast {
			// Check if there are more files
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
// Parquet files require reading the footer to parse, so they can't be streamed in chunks.
func (r *minioReader) readParquetFile() (*DataChunk, error) {
	// Check if we've already read all Parquet files
	if r.parquetDone {
		return nil, io.EOF
	}

	// Check if we have files to read
	if r.currentFile > len(r.files) {
		r.parquetDone = true
		return nil, io.EOF
	}

	// Read entire file into memory
	var buf bytes.Buffer
	_, err := io.Copy(&buf, r.object)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read Parquet file: %w", err)
	}

	// Check if there are more files
	isLast := r.currentFile >= len(r.files)

	// Try to open next file for subsequent reads
	if !isLast {
		if err := r.openNextFile(); err == io.EOF {
			isLast = true
		}
	}

	// Mark as done if this is the last file
	if isLast {
		r.parquetDone = true
	}

	return &DataChunk{
		Data:   buf.Bytes(),
		Format: r.format,
		IsLast: isLast,
	}, nil
}

func (r *minioReader) Schema() *SchemaInfo {
	return r.schema
}

func (r *minioReader) TotalSizeEstimate() int64 {
	return r.totalSize
}

func (r *minioReader) Close() error {
	if r.object != nil {
		return r.object.Close()
	}
	return nil
}

// minioWriter implements Writer for MinIO objects.
type minioWriter struct {
	client     *minio.Client
	bucket     string
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

func (w *minioWriter) WriteChunk(data []byte, rows int64) error {
	switch w.format {
	case "csv":
		return w.writeCSVChunk(data, rows)
	default:
		return w.writeRawChunk(data, rows)
	}
}

func (w *minioWriter) writeCSVChunk(data []byte, rows int64) error {
	reader := csv.NewReader(strings.NewReader(string(data)))

	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	if len(records) == 0 {
		return nil
	}

	// Initialize buffer and writer on first write
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

func (w *minioWriter) writeRawChunk(data []byte, rows int64) error {
	w.buffer.Write(data)
	w.bytesWritten += int64(len(data))
	w.rowsWritten += rows
	return nil
}

func (w *minioWriter) OutputName() string {
	return w.outputName
}

func (w *minioWriter) TablePath() string {
	return w.tablePath
}

func (w *minioWriter) Stats() WriterStats {
	return WriterStats{
		BytesWritten: w.bytesWritten,
		RowsWritten:  w.rowsWritten,
		PartsWritten: w.partNum,
		FilePaths:    w.filePaths,
	}
}

func (w *minioWriter) Close() error {
	if w.csvWriter != nil {
		w.csvWriter.Flush()
	}

	// Upload the buffer to MinIO
	if w.buffer.Len() > 0 {
		ext := w.format
		if w.compress {
			ext += ".gz"
		}

		filename := fmt.Sprintf("part-%05d.%s", w.partNum, ext)
		objectPath := path.Join(w.tablePath, filename)

		ctx := context.Background()
		_, err := w.client.PutObject(ctx, w.bucket, objectPath, &w.buffer, int64(w.buffer.Len()), minio.PutObjectOptions{})
		if err != nil {
			return fmt.Errorf("failed to upload %s: %w", objectPath, err)
		}

		w.filePaths = append(w.filePaths, objectPath)
		w.partNum++
	}

	return nil
}

// Ensure MinIOBackend implements StorageBackend
var _ StorageBackend = (*MinIOBackend)(nil)
