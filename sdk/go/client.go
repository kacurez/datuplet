// Package sdk provides a thin SDK for Datuplet components.
// It communicates with the Data Gateway sidecar via gRPC using v2 protocol.
package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// maxCallRecvMsgSize is the gRPC receive cap used for both production and
// test clients. Sized so a 32-MiB IPC-encoded row group fits with headroom.
const maxCallRecvMsgSize = 64 << 20

// Client connects to the Data Gateway and provides access to inputs/outputs.
type Client struct {
	conn       *grpc.ClientConn
	client     pb.DataGatewayClient
	config     *pb.ComponentConfig
	httpClient *http.Client
	gatewayHost  string // Hostname of gateway (for HTTP endpoint rewriting)
}

// Config holds the component configuration.
type Config struct {
	ExecutionID   string
	DefaultBucket string          // Default bucket for writes (if configured)
	InputBuckets  []string        // Buckets available for reading
	OutputBuckets []string        // Buckets available for writing
	InputTables   []TableRef      // Specific input tables
	OutputTables  []OutputTableRef // Specific output tables (declared in CRD)
	Raw           json.RawMessage // Component-specific configuration

	// Legacy fields (deprecated)
	Inputs  []string
	Outputs []string
}

// TableRef identifies a table by bucket and name. LogicalName is the
// SDK-facing identifier (set via the `as` field in the input table CRD
// entry); empty when the user did not override it. Components that need
// a single name to register the input under should fall back to Table.
type TableRef struct {
	Bucket      string
	Table       string
	LogicalName string
}

// OutputTableRef identifies an output table declared in the component's
// `outputs.tables[]` block. `Name` is the SDK-facing identifier (the CRD
// `logicalName` when set, otherwise the physical table name); `Bucket` /
// `Table` is the iceberg target.
type OutputTableRef struct {
	Name      string
	Bucket    string
	Table     string
	WriteMode string
}

// New creates a new Datuplet client. It connects to the gateway using
// DATUPLET_GATEWAY_ADDR environment variable (default: localhost:50051).
func New(ctx context.Context) (*Client, error) {
	addr := os.Getenv("DATUPLET_GATEWAY_ADDR")
	if addr == "" {
		addr = "localhost:50051"
	}

	// Extract host from address (for HTTP endpoint rewriting)
	// Address format: "host:port" or just "host"
	gatewayHost := addr
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		gatewayHost = addr[:idx]
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxCallRecvMsgSize)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gateway: %w", err)
	}

	client := pb.NewDataGatewayClient(conn)

	// Fetch config immediately
	config, err := client.GetConfig(ctx, &pb.GetConfigRequest{})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to get config: %w", err)
	}

	return &Client{
		conn:       conn,
		client:     client,
		config:     config,
		httpClient: &http.Client{},
		gatewayHost:  gatewayHost,
	}, nil
}

// Config returns the component configuration.
func (c *Client) Config() Config {
	// Legacy inputs/outputs for backward compatibility
	inputs := make([]string, 0, len(c.config.Inputs))
	for name := range c.config.Inputs {
		inputs = append(inputs, name)
	}
	outputs := make([]string, 0, len(c.config.Outputs))
	for name := range c.config.Outputs {
		outputs = append(outputs, name)
	}

	// Build input tables from proto
	inputTables := make([]TableRef, 0, len(c.config.InputTables))
	for _, t := range c.config.InputTables {
		inputTables = append(inputTables, TableRef{
			Bucket:      t.Bucket,
			Table:       t.Table,
			LogicalName: t.LogicalName,
		})
	}

	// Get default bucket + explicit output tables from output config
	var defaultBucket string
	var outputTables []OutputTableRef
	if c.config.OutputConfig != nil {
		defaultBucket = c.config.OutputConfig.DefaultBucket
		outputTables = make([]OutputTableRef, 0, len(c.config.OutputConfig.Tables))
		for _, t := range c.config.OutputConfig.Tables {
			sdkName := t.LogicalName
			if sdkName == "" {
				sdkName = t.Name
			}
			outputTables = append(outputTables, OutputTableRef{
				Name:      sdkName, // SDK identifier — LogicalName when set, otherwise physical Name
				Bucket:    t.Bucket,
				Table:     t.Name, // Iceberg target table
				WriteMode: t.WriteMode,
			})
		}
	}

	return Config{
		ExecutionID:   c.config.ExecutionId,
		DefaultBucket: defaultBucket,
		InputBuckets:  c.config.InputBuckets,
		OutputBuckets: c.config.OutputBuckets,
		InputTables:   inputTables,
		OutputTables:  outputTables,
		Raw:           c.config.Config,
		// Legacy fields
		Inputs:  inputs,
		Outputs: outputs,
	}
}

// ParseConfig unmarshals the component config into the provided struct.
func (c *Client) ParseConfig(v any) error {
	return json.Unmarshal(c.config.Config, v)
}

// StorageBootstrap returns the storage bootstrap data for components with native S3 access.
// Returns nil if no bootstrap data is provided (component uses standard SDK streaming).
func (c *Client) StorageBootstrap() *pb.StorageBootstrap {
	return c.config.StorageBootstrap
}

// RawConfig returns the raw proto ComponentConfig for advanced use cases.
// Most components should use Config() instead.
func (c *Client) RawConfig() *pb.ComponentConfig {
	return c.config
}

// fixHTTPEndpoint rewrites the HTTP endpoint to use the correct gateway host.
// The gateway returns "http://localhost:50052/..." but in Docker the component
// needs to connect to the gateway container's hostname.
func (c *Client) fixHTTPEndpoint(endpoint string) string {
	if endpoint == "" {
		return ""
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return endpoint // Return as-is if parsing fails
	}

	// Replace host with gateway host, keeping the port
	port := u.Port()
	if port == "" {
		port = "50052" // Default HTTP port
	}
	u.Host = c.gatewayHost + ":" + port

	return u.String()
}

// =============================================================================
// Writer Options
// =============================================================================

// WriterOption configures a Writer.
type WriterOption func(*writerOptions)

type writerOptions struct {
	format     pb.DataFormat
	schema     *pb.Schema
	transforms *pb.TransformSpec
	batchSize  int64 // 0 = use defaultBatchSize; <0 = disable batching
}

// defaultBatchSize is the Write-call accumulator threshold. Sized so a
// row-at-a-time caller emitting ~100 B JSONL rows triggers a flush every
// ~10K rows — bounds HTTP roundtrips while keeping each flush's parse
// cost on the gateway side modest.
const defaultBatchSize = 1024 * 1024 // 1 MiB

// WithFormat sets the input format for the writer.
// This tells the gateway what format the data chunks will be in.
func WithFormat(f pb.DataFormat) WriterOption {
	return func(o *writerOptions) {
		o.format = f
	}
}

// WithSchema sets an explicit schema for the writer.
// If not provided, schema is inferred from the first chunk.
func WithSchema(s *pb.Schema) WriterOption {
	return func(o *writerOptions) {
		o.schema = s
	}
}

// WithTransforms sets write-time transforms.
func WithTransforms(spec *pb.TransformSpec) WriterOption {
	return func(o *writerOptions) {
		o.transforms = spec
	}
}

// WithBatchSize controls the Write() accumulator's flush threshold (in bytes).
//
// Row-at-a-time callers (e.g. data-generator) issue one Write() per row.
// Without batching that's one HTTP roundtrip per row — transport-bound,
// minutes of wall clock for millions of rows. With batching, Write() appends
// the bytes to an internal per-writer buffer and only flushes to the gateway
// when the buffer reaches `n` bytes, on the next WriteChunk() (to preserve
// call order), on Flush(), or on Close().
//
//   - n > 0: use exactly `n` bytes as the threshold.
//   - n == 0: use the default (1 MiB).
//   - n  < 0: disable batching entirely; every Write becomes one immediate
//     WriteChunk (legacy v0.2.x behavior).
//
// Has no effect on WriteChunk() — that path always sends immediately and
// returns a result.
func WithBatchSize(n int64) WriterOption {
	return func(o *writerOptions) {
		o.batchSize = n
	}
}

// =============================================================================
// Writer
// =============================================================================

// Writer provides chunked writing to an output.
type Writer struct {
	client         pb.DataGatewayClient
	httpClient     *http.Client
	writerID       string
	httpEndpoint   string // HTTP endpoint for data transfer (empty = use gRPC)
	bucket         string // Resolved bucket name
	table          string // Table name
	inputFormat    pb.DataFormat
	inferredSchema *pb.Schema
	totalRows      int64

	// Write-call batching state. See WithBatchSize for semantics.
	// `batchThreshold == 0` means batching is disabled (legacy behavior:
	// every Write becomes one immediate WriteChunk).
	batchBuffer    []byte
	batchThreshold int64

	// Per-writer accounting for debug reporting. Surfaced via WriterStats
	// so components can log a one-liner at Close confirming batching was
	// active and at what amplification factor.
	statsWriteCalls      int64 // user-visible Write() calls
	statsWriteChunkCalls int64 // user-visible WriteChunk() calls
	statsUnderlyingPosts int64 // actual HTTP/gRPC roundtrips dispatched
	statsBytesIn         int64 // total bytes received via Write/WriteChunk
	statsBytesOutOnFlush int64 // total bytes shipped via underlying writes
}

// WriterStats is a snapshot of SDK-side activity for one Writer. Cheap
// to obtain at any time; intended for end-of-run logging in components.
// The writes/underlying_posts ratio is the headline tell for whether
// batching activated (writes >> posts when threshold > 0).
type WriterStats struct {
	WriteCalls        int64 // number of Write() calls
	WriteChunkCalls   int64 // number of WriteChunk() calls
	UnderlyingPosts   int64 // actual HTTP/gRPC roundtrips dispatched
	BytesAccepted     int64 // total bytes the caller submitted
	BytesShipped      int64 // total bytes shipped to the gateway
	BatchThreshold    int64 // configured batch flush threshold (0 = disabled)
	PendingBatchBytes int64 // bytes still buffered, not yet shipped
}

// Stats returns a snapshot of this writer's per-call accounting. Safe to
// call concurrently with single-goroutine usage of the writer; the SDK
// does not add a mutex for these counters (matches the existing
// no-mutex convention).
func (w *Writer) Stats() WriterStats {
	return WriterStats{
		WriteCalls:        w.statsWriteCalls,
		WriteChunkCalls:   w.statsWriteChunkCalls,
		UnderlyingPosts:   w.statsUnderlyingPosts,
		BytesAccepted:     w.statsBytesIn,
		BytesShipped:      w.statsBytesOutOnFlush,
		BatchThreshold:    w.batchThreshold,
		PendingBatchBytes: int64(len(w.batchBuffer)),
	}
}

// OpenWriter opens a writer for a table.
// If bucket is empty, uses the defaultBucket from config.
// table is required.
func (c *Client) OpenWriter(ctx context.Context, table string, opts ...WriterOption) (*Writer, error) {
	return c.OpenWriterToBucket(ctx, "", table, opts...)
}

// OpenWriterToBucket opens a writer for a specific bucket and table.
// Both bucket and table are required (bucket can be empty if defaultBucket is configured).
func (c *Client) OpenWriterToBucket(ctx context.Context, bucket, table string, opts ...WriterOption) (*Writer, error) {
	// Apply options
	options := &writerOptions{
		format: pb.DataFormat_FORMAT_CSV, // Default to CSV
	}
	for _, opt := range opts {
		opt(options)
	}

	// Resolve the batch threshold. See WithBatchSize for the encoding.
	var batchThreshold int64
	switch {
	case options.batchSize == 0:
		batchThreshold = defaultBatchSize // batching ON, default size
	case options.batchSize > 0:
		batchThreshold = options.batchSize // batching ON, custom size
	default:
		batchThreshold = 0 // batching OFF (legacy behavior)
	}
	// Hard override for self-framing formats: Arrow IPC chunks each
	// carry a schema header + record + EOS marker. Concatenating two
	// IPC streams into one HTTP body produces invalid IPC — the
	// gateway's ArrowIPCAdapter.Parse reads the first stream's records,
	// stops at the first EOS, and silently discards everything after.
	// On the gen-big-pipeline write benchmark this dropped exactly half
	// the rows (5M generated → 2,501,440 committed) because the
	// 1 MiB default batchSize fit ~2 IPC chunks per POST.
	//
	// Other formats are append-safe (JSONL: newline-separated; CSV:
	// row-separated text; raw parquet bytes get one-chunk-per-Write
	// from typical callers). For Arrow IPC, force batching off
	// regardless of WithBatchSize so callers can't accidentally
	// re-enable it.
	if options.format == pb.DataFormat_FORMAT_ARROW_IPC {
		batchThreshold = 0
	}

	req := &pb.OpenWriterRequest{
		Bucket:      bucket,
		Table:       table,
		InputFormat: options.format,
		Schema:      options.schema,
		Transforms:  options.transforms,
	}

	resp, err := c.client.OpenWriter(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to open writer: %w", err)
	}

	w := &Writer{
		client:         c.client,
		httpClient:     c.httpClient,
		writerID:       resp.WriterId,
		httpEndpoint:   c.fixHTTPEndpoint(resp.HttpEndpoint),
		bucket:         resp.Bucket,
		table:          resp.Table,
		inputFormat:    options.format,
		inferredSchema: resp.InferredSchema,
		batchThreshold: batchThreshold,
	}
	if batchThreshold > 0 {
		// Pre-size the accumulator at the threshold so we don't pay
		// growth-by-doubling overhead during normal operation.
		w.batchBuffer = make([]byte, 0, batchThreshold)
	}
	return w, nil
}

// Bucket returns the resolved bucket name.
func (w *Writer) Bucket() string {
	return w.bucket
}

// Table returns the table name.
func (w *Writer) Table() string {
	return w.table
}

// HTTPEndpoint returns the writer's HTTP write endpoint, or empty string
// when only gRPC is available (no HTTP server attached to this gateway
// instance, e.g. unit tests). Components that can use HTTP directly
// (e.g. DuckDB COPY TO) prefer the HTTP endpoint over the gRPC chunked
// path.
func (w *Writer) HTTPEndpoint() string {
	return w.httpEndpoint
}

// WriteResult holds the result of a write operation.
type WriteResult struct {
	RowsAccepted   int64
	BufferSize     int64
	InferredSchema *pb.Schema
}

// WriteChunk writes a chunk of data immediately and returns the gateway's
// per-call result (rows accepted, buffer size, inferred schema).
//
// If a Write() batch is pending, it is flushed first so the gateway sees
// chunks in the order they were submitted. The result returned describes
// only THIS WriteChunk's data, not the pending-batch flush.
//
// If HTTP endpoint is available, uses HTTP for data transfer (no size limit).
// Falls back to gRPC if HTTP endpoint is empty.
func (w *Writer) WriteChunk(ctx context.Context, data []byte) (*WriteResult, error) {
	w.statsWriteChunkCalls++
	w.statsBytesIn += int64(len(data))
	// Preserve call order: any data the caller already gave us via
	// Write() must reach the gateway before this explicit chunk.
	if err := w.flushBatchLocked(ctx); err != nil {
		return nil, fmt.Errorf("flush pending Write batch: %w", err)
	}
	return w.writeChunkImmediate(ctx, data)
}

// writeChunkImmediate sends data without consulting the batch buffer.
// Used by WriteChunk (after flushing) and by flushBatchLocked itself.
func (w *Writer) writeChunkImmediate(ctx context.Context, data []byte) (*WriteResult, error) {
	w.statsUnderlyingPosts++
	w.statsBytesOutOnFlush += int64(len(data))
	if w.httpEndpoint != "" {
		return w.writeChunkHTTP(ctx, data)
	}
	return w.writeChunkGRPC(ctx, data)
}

// writeChunkHTTP writes data via HTTP POST (no size limit).
func (w *Writer) writeChunkHTTP(ctx context.Context, data []byte) (*WriteResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.httpEndpoint, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set Content-Type based on input format
	req.Header.Set("Content-Type", dataFormatToContentType(w.inputFormat))

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP write failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP write failed: %s - %s", resp.Status, string(body))
	}

	// Parse JSON response
	var httpResp struct {
		RowsAccepted    int64      `json:"rows_accepted"`
		BufferSizeBytes int64      `json:"buffer_size_bytes"`
		InferredSchema  *pb.Schema `json:"inferred_schema,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&httpResp); err != nil {
		return nil, fmt.Errorf("failed to parse HTTP response: %w", err)
	}

	w.totalRows += httpResp.RowsAccepted

	// Update inferred schema if provided
	if httpResp.InferredSchema != nil && w.inferredSchema == nil {
		w.inferredSchema = httpResp.InferredSchema
	}

	return &WriteResult{
		RowsAccepted:   httpResp.RowsAccepted,
		BufferSize:     httpResp.BufferSizeBytes,
		InferredSchema: httpResp.InferredSchema,
	}, nil
}

// writeChunkGRPC writes data via gRPC (4MB limit).
func (w *Writer) writeChunkGRPC(ctx context.Context, data []byte) (*WriteResult, error) {
	resp, err := w.client.WriteChunk(ctx, &pb.WriteChunkRequest{
		WriterId: w.writerID,
		Data:     data,
	})
	if err != nil {
		return nil, fmt.Errorf("write chunk failed: %w", err)
	}

	w.totalRows += resp.RowsAccepted

	// Update inferred schema if provided
	if resp.InferredSchema != nil && w.inferredSchema == nil {
		w.inferredSchema = resp.InferredSchema
	}

	return &WriteResult{
		RowsAccepted:   resp.RowsAccepted,
		BufferSize:     resp.BufferSizeBytes,
		InferredSchema: resp.InferredSchema,
	}, nil
}

// dataFormatToContentType converts DataFormat to HTTP Content-Type.
func dataFormatToContentType(f pb.DataFormat) string {
	switch f {
	case pb.DataFormat_FORMAT_CSV:
		return "text/csv"
	case pb.DataFormat_FORMAT_JSON:
		return "application/json"
	case pb.DataFormat_FORMAT_JSONL:
		return "application/x-ndjson"
	case pb.DataFormat_FORMAT_PARQUET:
		return "application/vnd.apache.parquet"
	case pb.DataFormat_FORMAT_ARROW_IPC:
		return "application/vnd.apache.arrow.stream"
	default:
		return "application/octet-stream"
	}
}

// Write is the row-at-a-time / streaming convenience method. It does not
// return per-call results; callers that need them should use WriteChunk.
//
// By default Write batches: bytes are appended to a per-writer accumulator
// and the gateway only sees a WriteChunk when the accumulator reaches the
// batch threshold (1 MiB by default; see WithBatchSize). This collapses
// row-at-a-time HTTP traffic by ~1000x for the common case of one row per
// Write call. To opt out, open the writer with `WithBatchSize(-1)` — every
// Write becomes one immediate WriteChunk (the legacy v0.2.x behavior).
//
// The accumulator is drained on every:
//   - threshold cross (this method)
//   - WriteChunk call (to preserve call order)
//   - Flush call (explicit)
//   - Close call (final flush before commit)
func (w *Writer) Write(ctx context.Context, data []byte) error {
	w.statsWriteCalls++
	w.statsBytesIn += int64(len(data))
	if w.batchThreshold <= 0 {
		// Batching disabled — legacy semantics.
		_, err := w.writeChunkImmediate(ctx, data)
		return err
	}

	w.batchBuffer = append(w.batchBuffer, data...)
	if int64(len(w.batchBuffer)) >= w.batchThreshold {
		return w.flushBatchLocked(ctx)
	}
	return nil
}

// Flush forces any pending Write-batched data to the gateway immediately.
// No-op when batching is disabled or the accumulator is empty. Useful for
// callers that want to checkpoint progress without closing the writer.
func (w *Writer) Flush(ctx context.Context) error {
	return w.flushBatchLocked(ctx)
}

// flushBatchLocked sends the currently-buffered Write payload as a single
// WriteChunk. Name reflects the intent that the writer is single-threaded —
// the SDK does not add a mutex (matches the existing convention). If a
// component shares a Writer across goroutines it must serialize access
// externally (same requirement as before this PR).
func (w *Writer) flushBatchLocked(ctx context.Context) error {
	if len(w.batchBuffer) == 0 {
		return nil
	}
	// Hand the underlying slice to writeChunkImmediate; the HTTP/gRPC
	// path makes a copy (gRPC marshaler does, http.NewRequest's bytes.Reader
	// holds a reference until response). We zero-length the slice header
	// after the call so the next Write reuses the same allocation.
	if _, err := w.writeChunkImmediate(ctx, w.batchBuffer); err != nil {
		return err
	}
	w.batchBuffer = w.batchBuffer[:0]
	return nil
}

// CloseResult holds the result of closing a writer.
type CloseResult struct {
	TotalRows    int64
	TotalBytes   int64
	FilesWritten int32
}

// Close closes the writer and finalizes the output.
func (w *Writer) Close(ctx context.Context) (*CloseResult, error) {
	return w.CloseWithExternalFiles(ctx, nil)
}

// ExternalFile describes a data file written directly to storage by the component.
type ExternalFile struct {
	// Path is either:
	//   - a relative filename (e.g. "data.parquet") — DataGateway joins it with the
	//     production table's basePath to form the full storage URL, or
	//   - an absolute storage URL (e.g. "s3://bucket/prefix/data.parquet",
	//     "file:///path/to/data.parquet") — DataGateway uses it verbatim. Use this
	//     when the file lives at a different location than the production prefix
	//     (e.g. a workspace scratch prefix for sql-transform).
	Path      string
	RowCount  int64 // Number of rows in this file
	SizeBytes int64 // File size in bytes (0 if unknown)
}

// CloseWithExternalFiles closes the writer and provides metadata for files written directly to storage.
// This is used when components bypass the DataGateway's buffering and write files directly (e.g., DuckDB).
func (w *Writer) CloseWithExternalFiles(ctx context.Context, externalFiles []ExternalFile) (*CloseResult, error) {
	// Drain any pending Write-batched data before closing. Otherwise the
	// gateway would commit without seeing the tail of the stream.
	if err := w.flushBatchLocked(ctx); err != nil {
		return nil, fmt.Errorf("flush pending Write batch on close: %w", err)
	}

	req := &pb.CloseWriterRequest{
		WriterId: w.writerID,
	}

	// Convert external files to proto format
	if len(externalFiles) > 0 {
		req.ExternalFiles = make([]*pb.ExternalDataFile, len(externalFiles))
		for i, ef := range externalFiles {
			req.ExternalFiles[i] = &pb.ExternalDataFile{
				Path:      ef.Path,
				RowCount:  ef.RowCount,
				SizeBytes: ef.SizeBytes,
			}
		}
	}

	resp, err := w.client.CloseWriter(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("close writer failed: %w", err)
	}

	return &CloseResult{
		TotalRows:    resp.TotalRows,
		TotalBytes:   resp.TotalBytes,
		FilesWritten: resp.FilesWritten,
	}, nil
}

// Schema returns the schema (inferred or provided).
func (w *Writer) Schema() *pb.Schema {
	return w.inferredSchema
}

// =============================================================================
// Reader Options
// =============================================================================

// ReaderOption configures a Reader.
type ReaderOption func(*readerOptions)

type readerOptions struct {
	format      pb.DataFormat
	chunkSize   int64
	transforms  *pb.TransformSpec
	incremental *pb.IncrementalReadSpec
}

// WithOutputFormat sets the output format for the reader.
// This tells the gateway what format to convert the data to.
func WithOutputFormat(f pb.DataFormat) ReaderOption {
	return func(o *readerOptions) {
		o.format = f
	}
}

// WithChunkSize sets the target chunk size in bytes.
func WithChunkSize(bytes int64) ReaderOption {
	return func(o *readerOptions) {
		o.chunkSize = bytes
	}
}

// WithReadTransforms sets read-time transforms.
func WithReadTransforms(spec *pb.TransformSpec) ReaderOption {
	return func(o *readerOptions) {
		o.transforms = spec
	}
}

// WithIncrementalSince configures the reader to only return data added
// after the given snapshot ID (incremental/delta read).
func WithIncrementalSince(snapshotID int64) ReaderOption {
	return func(o *readerOptions) {
		o.incremental = &pb.IncrementalReadSpec{
			BaseSelector: &pb.IncrementalReadSpec_FromSnapshotId{FromSnapshotId: snapshotID},
		}
	}
}

// WithIncrementalSinceTime configures the reader to only return data added
// after the given timestamp in milliseconds (incremental/delta read).
func WithIncrementalSinceTime(timestampMs int64) ReaderOption {
	return func(o *readerOptions) {
		o.incremental = &pb.IncrementalReadSpec{
			BaseSelector: &pb.IncrementalReadSpec_FromTimestampMs{FromTimestampMs: timestampMs},
		}
	}
}

// =============================================================================
// Reader
// =============================================================================

// Reader provides chunked reading from an input.
type Reader struct {
	client       pb.DataGatewayClient
	httpClient   *http.Client
	readerID     string
	httpEndpoint string // HTTP endpoint for data transfer (empty = use gRPC)
	bucket       string // Bucket name
	table        string // Table name
	stream       pb.DataGateway_ReadChunkClient
	schema       *pb.Schema
	deltaInfo    *pb.DeltaInfo // Populated for incremental reads
	isLast       bool          // Track if last chunk received via HTTP
}

// OpenReader opens a reader for a table.
// Both bucket and table are required for reads.
func (c *Client) OpenReader(ctx context.Context, bucket, table string, opts ...ReaderOption) (*Reader, error) {
	// Apply options
	options := &readerOptions{
		format: pb.DataFormat_FORMAT_CSV, // Default to CSV
	}
	for _, opt := range opts {
		opt(options)
	}

	req := &pb.OpenReaderRequest{
		Bucket:         bucket,
		Table:          table,
		OutputFormat:   options.format,
		ChunkSizeBytes: options.chunkSize,
		Transforms:     options.transforms,
		Incremental:    options.incremental,
	}

	resp, err := c.client.OpenReader(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to open reader: %w", err)
	}

	httpEndpoint := c.fixHTTPEndpoint(resp.HttpEndpoint)
	reader := &Reader{
		client:       c.client,
		httpClient:   c.httpClient,
		readerID:     resp.ReaderId,
		httpEndpoint: httpEndpoint,
		bucket:       resp.Bucket,
		table:        resp.Table,
		schema:       resp.Schema,
		deltaInfo:    resp.DeltaInfo,
	}

	// Only start gRPC stream if no HTTP endpoint available
	if httpEndpoint == "" {
		stream, err := c.client.ReadChunk(ctx, &pb.ReadChunkRequest{ReaderId: resp.ReaderId})
		if err != nil {
			return nil, fmt.Errorf("failed to start reading: %w", err)
		}
		reader.stream = stream
	}

	return reader, nil
}

// Bucket returns the bucket name.
func (r *Reader) Bucket() string {
	return r.bucket
}

// Table returns the table name.
func (r *Reader) Table() string {
	return r.table
}

// HTTPEndpoint returns the reader's HTTP read endpoint, or empty string
// when only gRPC is available. Note: each GET to the endpoint returns
// ONE chunk (the gateway's reader-id is server-stateful), so callers
// cannot point engines like DuckDB's httpfs at the endpoint as if it
// were a single static parquet URL — use NextChunk() instead.
func (r *Reader) HTTPEndpoint() string {
	return r.httpEndpoint
}

// Schema returns the data schema.
func (r *Reader) Schema() *pb.Schema {
	return r.schema
}

// DeltaInfo returns metadata about the incremental read, or nil for non-incremental reads.
func (r *Reader) DeltaInfo() *pb.DeltaInfo {
	return r.deltaInfo
}

// ColumnNames returns the column names for convenience.
func (r *Reader) ColumnNames() []string {
	if r.schema == nil {
		return nil
	}
	names := make([]string, len(r.schema.Columns))
	for i, col := range r.schema.Columns {
		names[i] = col.Name
	}
	return names
}

// Chunk represents a chunk of data.
type Chunk struct {
	Data   []byte
	Format pb.DataFormat
	Rows   int64
	IsLast bool
}

// NextChunk reads the next chunk of data. Returns io.EOF when done.
func (r *Reader) NextChunk() (*Chunk, error) {
	// Use HTTP if endpoint is available
	if r.httpEndpoint != "" {
		return r.nextChunkHTTP()
	}

	// Fall back to gRPC
	return r.nextChunkGRPC()
}

// nextChunkHTTP reads next chunk via HTTP GET.
func (r *Reader) nextChunkHTTP() (*Chunk, error) {
	// Check if already received last chunk
	if r.isLast {
		return nil, io.EOF
	}

	req, err := http.NewRequest(http.MethodGet, r.httpEndpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP read failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP read failed: %s - %s", resp.Status, string(body))
	}

	// Parse response headers
	isLast := resp.Header.Get("X-Datuplet-Is-Last") == "true"
	var rows int64
	fmt.Sscanf(resp.Header.Get("X-Datuplet-Rows"), "%d", &rows)

	// Read body
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read HTTP response body: %w", err)
	}

	// Mark as last for subsequent calls
	if isLast {
		r.isLast = true
	}

	// If no data and isLast, return EOF
	if len(data) == 0 && isLast {
		return nil, io.EOF
	}

	return &Chunk{
		Data:   data,
		Rows:   rows,
		IsLast: isLast,
	}, nil
}

// nextChunkGRPC reads next chunk via gRPC stream.
func (r *Reader) nextChunkGRPC() (*Chunk, error) {
	chunk, err := r.stream.Recv()
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("read error: %w", err)
	}

	return &Chunk{
		Data:   chunk.Data,
		Format: chunk.Format,
		Rows:   chunk.RowsInChunk,
		IsLast: chunk.IsLast,
	}, nil
}

// Close closes the reader.
func (r *Reader) Close(ctx context.Context) error {
	_, err := r.client.CloseReader(ctx, &pb.CloseReaderRequest{ReaderId: r.readerID})
	return err
}

// =============================================================================
// Commit
// =============================================================================

// CommitResult holds the result of a commit operation.
type CommitResult struct {
	Success bool
	Error   string
	Buckets []BucketResult
}

// BucketResult holds the result for a single bucket.
type BucketResult struct {
	Bucket  string
	Success bool
	Tables  []TableResult
	Error   string
}

// TableResult holds the result for a single table.
type TableResult struct {
	Bucket     string
	Table      string
	Success    bool
	SnapshotID int64
	FilesAdded int32
	RowsAdded  int64
	BytesAdded int64
	Error      string
}

// CommitOption configures a commit operation.
type CommitOption func(*commitOptions)

type commitOptions struct {
	bestEffort bool
}

// WithBestEffort continues commit even if individual buckets fail.
func WithBestEffort() CommitOption {
	return func(o *commitOptions) {
		o.bestEffort = true
	}
}

// Commit commits all outputs atomically (per-bucket).
func (c *Client) Commit(ctx context.Context, opts ...CommitOption) (*CommitResult, error) {
	options := &commitOptions{}
	for _, opt := range opts {
		opt(options)
	}

	resp, err := c.client.Commit(ctx, &pb.CommitRequest{
		BestEffort: options.bestEffort,
	})
	if err != nil {
		return nil, fmt.Errorf("commit failed: %w", err)
	}

	result := &CommitResult{
		Success: resp.Success,
		Error:   resp.Error,
	}

	for _, b := range resp.Buckets {
		bucketResult := BucketResult{
			Bucket:  b.Bucket,
			Success: b.Status == pb.BucketCommitResult_STATUS_COMMITTED,
			Error:   b.Error,
		}
		for _, t := range b.Tables {
			bucketResult.Tables = append(bucketResult.Tables, TableResult{
				Bucket:     t.Bucket,
				Table:      t.Table,
				Success:    t.Status == pb.TableCommitResult_STATUS_COMMITTED,
				SnapshotID: t.SnapshotId,
				FilesAdded: t.FilesAdded,
				RowsAdded:  t.RowsAdded,
				BytesAdded: t.BytesAdded,
				Error:      t.Error,
			})
		}
		result.Buckets = append(result.Buckets, bucketResult)
	}
	return result, nil
}

// =============================================================================
// Schema & Sample
// =============================================================================

// GetSchema returns the schema for a table.
func (c *Client) GetSchema(ctx context.Context, bucket, table string) (*pb.Schema, error) {
	resp, err := c.client.GetSchema(ctx, &pb.GetSchemaRequest{
		Bucket: bucket,
		Table:  table,
	})
	if err != nil {
		return nil, fmt.Errorf("get schema failed: %w", err)
	}
	return resp.Schema, nil
}

// SampleResult holds sample data from a table.
type SampleResult struct {
	Schema        *pb.Schema
	Rows          [][]byte
	TotalEstimate int64
}

// GetSample returns sample rows from a table.
func (c *Client) GetSample(ctx context.Context, bucket, table string, limit int, format pb.DataFormat) (*SampleResult, error) {
	resp, err := c.client.GetSample(ctx, &pb.GetSampleRequest{
		Bucket: bucket,
		Table:  table,
		Limit:  int32(limit),
		Format: format,
	})
	if err != nil {
		return nil, fmt.Errorf("get sample failed: %w", err)
	}
	return &SampleResult{
		Schema:        resp.Schema,
		Rows:          resp.Rows,
		TotalEstimate: resp.TotalEstimate,
	}, nil
}

// =============================================================================
// Logging
// =============================================================================

// Log sends a log message to the gateway.
func (c *Client) Log(ctx context.Context, level, message string) error {
	_, err := c.client.Log(ctx, &pb.LogRequest{
		Level:   level,
		Message: message,
	})
	return err
}

// LogFields sends a log message with structured fields to the gateway.
func (c *Client) LogFields(ctx context.Context, level, message string, fields map[string]string) error {
	_, err := c.client.Log(ctx, &pb.LogRequest{
		Level:   level,
		Message: message,
		Fields:  fields,
	})
	return err
}

// Convenience log methods

// Debug logs a debug message.
func (c *Client) Debug(ctx context.Context, message string) error {
	return c.Log(ctx, "DEBUG", message)
}

// Info logs an info message.
func (c *Client) Info(ctx context.Context, message string) error {
	return c.Log(ctx, "INFO", message)
}

// Warn logs a warning message.
func (c *Client) Warn(ctx context.Context, message string) error {
	return c.Log(ctx, "WARN", message)
}

// Error logs an error message.
func (c *Client) Error(ctx context.Context, message string) error {
	return c.Log(ctx, "ERROR", message)
}

// =============================================================================
// Lifecycle
// =============================================================================

// Close closes the client connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// =============================================================================
// Testing helpers
// =============================================================================

// TestingT is a minimal subset of *testing.T used by NewWithDialer so the
// SDK can stay test-framework-agnostic. *testing.T satisfies it.
type TestingT interface {
	Fatalf(format string, args ...interface{})
}

// NewWithDialer is like New but uses the provided dialer (typically a
// bufconn listener) and skips the GetConfig fetch. Production callers
// should use New() instead — this is intended for in-process tests of
// SDK plumbing (the sdk/go/arrow sub-module wraps a bufconn-backed
// gateway in tests, for example).
func NewWithDialer(t TestingT, dialer func(ctx context.Context, addr string) (net.Conn, error)) *Client {
	// grpc.NewClient is stricter about target syntax than the legacy
	// grpc.Dial — bufconn-style targets must include an explicit
	// resolver scheme, hence "passthrough:///bufnet".
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxCallRecvMsgSize)),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	return &Client{
		conn:       conn,
		client:     pb.NewDataGatewayClient(conn),
		httpClient: &http.Client{},
	}
}

// OpenGRPCReadChunk opens a gRPC server-streaming ReadChunk. Used by the
// sdk/go/arrow sub-module. Always uses gRPC (ignoring any HTTP endpoint),
// because the arrow IPC stream isn't compatible with the HTTP one-chunk-per-GET
// shape. If OpenReader already pre-opened a stream (httpEndpoint == ""),
// hand that one over rather than opening a redundant second stream.
func (r *Reader) OpenGRPCReadChunk(ctx context.Context) (pb.DataGateway_ReadChunkClient, error) {
	if r.stream != nil {
		s := r.stream
		r.stream = nil
		return s, nil
	}
	return r.client.ReadChunk(ctx, &pb.ReadChunkRequest{ReaderId: r.readerID})
}

// =============================================================================
// Exit Helpers
// =============================================================================

const statusMessagePrefix = "DUPLET_STATUS_MESSAGE:"

// StatusMessage prints a status message to stdout using the DUPLET_STATUS_MESSAGE protocol.
// The K8s controller extracts this message and stores it in the CRD status.
// Call this before exiting to report a summary (e.g., "extracted 100 rows from data.csv").
func StatusMessage(message string) {
	fmt.Printf("%s%s\n", statusMessagePrefix, message)
}

// ExitUserError prints a status message and exits with code 1 (FailedUser).
// Use this for user-caused errors: bad config, invalid input, schema mismatch.
func ExitUserError(message string) {
	StatusMessage(message)
	os.Exit(1)
}

// ExitAppError prints a status message and exits with code 20 (FailedApplication).
// Use this for infrastructure/application errors: connection failures, OOM, internal bugs.
func ExitAppError(message string) {
	StatusMessage(message)
	os.Exit(20)
}
