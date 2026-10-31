package datagateway

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

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
}

// WrittenFile is one Parquet file a TableWriter produced.
type WrittenFile struct {
	Path  string
	Rows  int64
	Bytes int64
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

// ReaderSpec identifies the table a RecordStream reads.
type ReaderSpec struct{ Namespace, Table string }

// RecordStream yields the Arrow batches of one table.
type RecordStream interface {
	Schema() *arrow.Schema
	// Next returns io.EOF at the end; the caller Releases the batch.
	Next(ctx context.Context) (arrow.RecordBatch, error)
	Close() error
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
	OpenTableReader(ctx context.Context, spec ReaderSpec) (RecordStream, error)
	// Interrupt aborts every in-flight statement (cancel annotation).
	Interrupt()
	Close() error
}
