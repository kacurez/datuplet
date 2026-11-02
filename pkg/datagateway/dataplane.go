package datagateway

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	dglakekeeper "github.com/datuplet/datuplet/pkg/datagateway/lakekeeper"
)

// ErrInvalidPartition marks a partitioning the gateway cannot write; it is
// returned as gRPC InvalidArgument (HTTP 400).
var ErrInvalidPartition = dglakekeeper.ErrInvalidPartition

// BootInfo is what the data plane needs from boot validation; it is built by
// NewServerV2 after validateRunTokenForBoot.
type BootInfo struct {
	RunID         string
	LakekeeperURL string // cfg.LakekeeperURL ("" in tests / no catalog)
	ProjectID     string // validated claim
	Warehouse     string // validated claim (bare name)
	Token         string // raw run JWT; never logged
}

// String redacts Token. Called by every fmt verb that defaults to the Stringer
// (%v, %+v, %s). %#v still exposes fields; reviewers must catch that one
// (same convention as catalogwriter.S3Creds / GCSCreds, RFC 019 §4.10).
func (b BootInfo) String() string {
	return fmt.Sprintf("BootInfo{run_id=%q lakekeeper_url=%q project_id=%q warehouse=%q token=<redacted>}",
		b.RunID, b.LakekeeperURL, b.ProjectID, b.Warehouse)
}

// DataPlaneFactory builds the DataPlane once boot validation has passed.
type DataPlaneFactory func(ctx context.Context, boot BootInfo) (DataPlane, error)

// CredsSource is satisfied by *catalogwriter.VendedCreds (Get + Invalidate).
type CredsSource interface {
	Get(ctx context.Context) (catalogwriter.Creds, error)
	Invalidate()
}

// WriterSpec describes one table writer.
type WriterSpec struct {
	DataPrefix      string        // opaque "<table-location>/data/" incl. trailing slash
	FilePrefix      string        // "part-<run8>-<writerID>"
	Schema          *arrow.Schema // post-drop schema; every Write must match it
	Creds           CredsSource   // nil only for local-path prefixes (tests)
	TargetFileBytes int64         // <=0 → 128 MiB
	RowGroupRows    int64         // <=0 → 229376

	// Table ("ns.table") names the table in logs.
	Table string
	// PartitionSpec and TableSchema are the table's current spec and schema
	// (as the catalog returned them). A nil spec or one without fields
	// writes unpartitioned files; otherwise every file holds one partition
	// tuple of the spec.
	PartitionSpec *iceberg.PartitionSpec
	TableSchema   *iceberg.Schema
}

// WrittenFile is one Parquet file a TableWriter produced.
type WrittenFile struct {
	Path  string
	Rows  int64
	Bytes int64
	// SpecID is the partition spec the file was written under. Partition is
	// its tuple (partition field id → value, nil for a null, an entry for
	// every spec field); nil for a file of an unpartitioned spec.
	SpecID    int
	Partition map[int]any
}

// TableWriter accepts Arrow batches and produces Parquet files under
// WriterSpec.DataPrefix.
type TableWriter interface {
	// Write blocks for backpressure; it does not take ownership of rec
	// (the implementation retains it internally if it keeps it).
	Write(ctx context.Context, rec arrow.RecordBatch) error
	QueuedBytes() int64
	// Close flushes; a second call is idempotent and returns the same result.
	Close(ctx context.Context) ([]WrittenFile, error)
	// Abort discards everything; idempotent and safe after Close.
	Abort()
}

// ReaderSpec identifies the table a TableReader scans.
type ReaderSpec struct {
	Namespace, Table string
	RawParquet       bool // request format is FORMAT_PARQUET: raw-eligible data files may come back as RawFile parts
}

// RecordStream yields the Arrow batches of one table (or one part of it).
type RecordStream interface {
	Schema() *arrow.Schema
	// Next returns io.EOF at the end; the caller Releases the batch.
	Next(ctx context.Context) (arrow.RecordBatch, error)
	Close() error
}

// ReadPart is one ordered piece of a table scan; exactly one of Raw and Records is set.
type ReadPart struct {
	Raw     *RawFile
	Records RecordStream // decoded batches with TableReader.Schema(); the caller closes it
	// Started, for a Records part, reports how many scan tasks the part has
	// started decoding so far (summary log only); safe to call concurrently.
	Started func() int
}

// RawFile is a data file that can be served byte-for-byte.
type RawFile struct {
	Path string
	Rows int64                                            // record count from the manifest entry
	Size int64                                            // bytes
	Open func(ctx context.Context) (io.ReadCloser, error) // the file's bytes, credentials current at call time
}

// TableReader yields the ordered parts of one table scan.
type TableReader interface {
	Schema() *arrow.Schema                          // the table's current schema; every part carries exactly it
	NextPart(ctx context.Context) (ReadPart, error) // io.EOF after the last part (immediately for an empty table)
	Close() error                                   // closes the reader and any part stream still open
}

// DataPlane is the storage-I/O engine behind the Data Gateway.
//
// Context ownership (binding for every implementation): OpenTableWriter and
// OpenTableReader derive the writer's/reader's lifetime context from the
// plane's own context, never from the ctx argument (an RPC/HTTP request
// context is cancelled when that request returns). The ctx of Open…, Write,
// Next and Close only bounds that call's blocking. The lifetime context is
// cancelled by Abort, reader Close, Interrupt, plane Close, or an internal
// failure.
type DataPlane interface {
	OpenTableWriter(ctx context.Context, spec WriterSpec) (TableWriter, error)
	OpenTableReader(ctx context.Context, spec ReaderSpec) (TableReader, error)
	// Interrupt aborts every in-flight statement (cancel annotation).
	Interrupt()
	Close() error
}
