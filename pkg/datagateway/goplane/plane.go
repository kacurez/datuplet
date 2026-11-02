// Package goplane is the all-Go datagateway.DataPlane: Arrow batches are
// encoded with arrow-go pqarrow (ZSTD) and streamed to the table's data prefix
// through iceberg-go IO with Lakekeeper-vended credentials (spec
// docs/superpowers/specs/2026-11-01-all-go-gateway-design.md, Write path 4–7).
package goplane

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/datagateway"
	"github.com/datuplet/datuplet/pkg/datagateway/lakekeeper"
	// Registers the iceberg-go IO schemes, with Datuplet's gs:// factory that
	// consumes the vended gcs.oauth2.token (RFC 019 §4.5).
	_ "github.com/datuplet/datuplet/pkg/datupleticeio"
)

// TableLoader loads a table for reading with the credentials its files are
// read with.
type TableLoader interface {
	LoadTable(ctx context.Context, ns, tbl string) (*table.Table, datagateway.CredsSource, error)
}

// Config configures a Plane.
type Config struct {
	Tables TableLoader // nil: OpenTableReader returns an error
	// MaxOpenPartitions caps the open files of one partitioned table writer
	// (spec decision 9); <=0 means defaultMaxOpenPartitions.
	MaxOpenPartitions int
	// PartitionBufferBytes is the row-group byte budget shared by the open
	// files of one partitioned table writer (spec decision 9); <=0 means
	// defaultPartitionBufferBytes.
	PartitionBufferBytes int64
}

// Open partition files per table writer: default and the bounds of
// DATUPLET_GATEWAY_MAX_OPEN_PARTITIONS.
const (
	defaultMaxOpenPartitions = 16
	maxMaxOpenPartitions     = 1024
	maxOpenPartitionsEnv     = "DATUPLET_GATEWAY_MAX_OPEN_PARTITIONS"
)

// Partitioned-writer row-group budget: default and the bounds of
// DATUPLET_GATEWAY_PARTITION_BUFFER_BYTES.
const (
	defaultPartitionBufferBytes int64 = 256 << 20
	minPartitionBufferBytes     int64 = 16 << 20
	maxPartitionBufferBytes     int64 = 8 << 30
	partitionBufferBytesEnv           = "DATUPLET_GATEWAY_PARTITION_BUFFER_BYTES"
)

// maxOpenPartitionsFromEnv reads DATUPLET_GATEWAY_MAX_OPEN_PARTITIONS:
// 1-1024; unset means the default, anything else the default and a warning.
func maxOpenPartitionsFromEnv() int {
	v, ok := os.LookupEnv(maxOpenPartitionsEnv)
	if !ok || v == "" {
		return defaultMaxOpenPartitions
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || n < 1 || n > maxMaxOpenPartitions {
		log.Printf("goplane: warning: %s=%q is not an integer in 1-%d; using %d", maxOpenPartitionsEnv, v, maxMaxOpenPartitions, defaultMaxOpenPartitions)
		return defaultMaxOpenPartitions
	}
	return n
}

// partitionBufferBytesFromEnv reads DATUPLET_GATEWAY_PARTITION_BUFFER_BYTES:
// a byte count in 16 MiB-8 GiB; unset means the default, anything else the
// default and a warning.
func partitionBufferBytesFromEnv() int64 {
	v, ok := os.LookupEnv(partitionBufferBytesEnv)
	if !ok || v == "" {
		return defaultPartitionBufferBytes
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil || n < minPartitionBufferBytes || n > maxPartitionBufferBytes {
		log.Printf("goplane: warning: %s=%q is not a byte count in %d-%d; using %d",
			partitionBufferBytesEnv, v, minPartitionBufferBytes, maxPartitionBufferBytes, defaultPartitionBufferBytes)
		return defaultPartitionBufferBytes
	}
	return n
}

// Plane implements datagateway.DataPlane.
type Plane struct {
	cfg    Config
	ctx    context.Context // lifetime of every writer/reader; cancelled by Interrupt and Close
	cancel context.CancelFunc

	// openSink opens the upload sink of one data file; tests replace it.
	openSink sinkOpener
	// loadIO builds the read-side IO; tests replace it.
	loadIO ioLoader
	// closeWait bounds how long Close waits for open writers' cleanup;
	// tests shorten it.
	closeWait time.Duration
	// budgetHook, nil in production, sees each partitioned writer's
	// row-group budget decisions from inside its run goroutine; tests set it.
	budgetHook func(budgetEvent)
	// admitHook, nil in production, runs in OpenTableWriter right after a
	// writer is admitted (counted in Close's wait), still under mu; tests
	// set it.
	admitHook func(*writer)

	mu      sync.Mutex
	closed  bool           // set by Close: no new writers
	writers sync.WaitGroup // one per writer goroutine not yet exited
	readers map[*reader]struct{}
}

// defaultCloseWait bounds Close's wait for writer upload cleanup (sink
// close, AbortMultipartUpload) so shutdown cannot hang on a stuck upload.
const defaultCloseWait = 30 * time.Second

var _ datagateway.DataPlane = (*Plane)(nil)

// New returns a Plane.
func New(cfg Config) *Plane {
	if cfg.MaxOpenPartitions <= 0 {
		cfg.MaxOpenPartitions = defaultMaxOpenPartitions
	}
	if cfg.PartitionBufferBytes <= 0 {
		cfg.PartitionBufferBytes = defaultPartitionBufferBytes
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Plane{cfg: cfg, ctx: ctx, cancel: cancel, openSink: openIOSink, loadIO: iceio.LoadFS, readers: map[*reader]struct{}{}, closeWait: defaultCloseWait}
}

// Factory builds the plane once boot validation has passed. With a
// Lakekeeper URL, reads load tables through a lakekeeper.Resolver.
func Factory(_ context.Context, boot datagateway.BootInfo) (datagateway.DataPlane, error) {
	cfg := Config{MaxOpenPartitions: maxOpenPartitionsFromEnv(), PartitionBufferBytes: partitionBufferBytesFromEnv()}
	if boot.LakekeeperURL != "" {
		r, err := lakekeeper.NewResolver(boot.LakekeeperURL, boot.Warehouse, boot.Token, boot.ProjectID)
		if err != nil {
			return nil, fmt.Errorf("goplane: lakekeeper resolver: %w", err)
		}
		cfg.Tables = ResolverTables{Resolver: r}
	}
	return New(cfg), nil
}

// ResolverTables adapts a lakekeeper.Resolver to TableLoader.
type ResolverTables struct{ Resolver *lakekeeper.Resolver }

// LoadTable loads (ns, tbl) and its vended credentials (nil for file://).
func (rt ResolverTables) LoadTable(ctx context.Context, ns, tbl string) (*table.Table, datagateway.CredsSource, error) {
	t, vc, err := rt.Resolver.LoadTableForScan(ctx, ns, tbl)
	if err != nil {
		return nil, nil, err
	}
	if vc == nil {
		return t, nil, nil // no typed-nil CredsSource
	}
	return t, vc, nil
}

// OpenTableWriter starts a writer whose lifetime context derives from the
// plane's, never from ctx.
func (p *Plane) OpenTableWriter(_ context.Context, spec datagateway.WriterSpec) (datagateway.TableWriter, error) {
	if spec.Schema == nil {
		return nil, errors.New("goplane: writer schema is required")
	}
	if spec.DataPrefix == "" || spec.FilePrefix == "" {
		return nil, errors.New("goplane: writer data prefix and file prefix are required")
	}
	local := isLocalPrefix(spec.DataPrefix)
	if spec.Creds == nil && !local {
		return nil, fmt.Errorf("goplane: no credentials for remote data prefix %q", spec.DataPrefix)
	}
	if spec.TargetFileBytes <= 0 {
		spec.TargetFileBytes = defaultTargetFileBytes
	}
	if spec.RowGroupRows <= 0 {
		spec.RowGroupRows = defaultRowGroupRows
	}
	var part *partitioner
	if spec.PartitionSpec != nil && spec.PartitionSpec.NumFields() > 0 {
		var err error
		if part, err = newPartitioner(*spec.PartitionSpec, spec.TableSchema, spec.Schema); err != nil {
			return nil, fmt.Errorf("goplane: table %s: %w", spec.Table, err)
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("goplane: plane closed")
	}
	if err := p.ctx.Err(); err != nil {
		return nil, fmt.Errorf("goplane: plane closed: %w", err)
	}
	w := newWriter(p.ctx, spec, local, p.openSink, part, p.cfg.MaxOpenPartitions, p.cfg.PartitionBufferBytes, p.budgetHook)
	p.writers.Add(1)
	go func() {
		<-w.done
		p.writers.Done()
	}()
	if p.admitHook != nil {
		p.admitHook(w)
	}
	return w, nil
}

// OpenTableReader loads the table, pins its current snapshot and plans the
// scan once. The reader's lifetime context derives from the plane's, never
// from ctx; Close, Interrupt and plane Close stop its in-flight reads.
func (p *Plane) OpenTableReader(ctx context.Context, spec datagateway.ReaderSpec) (datagateway.TableReader, error) {
	return p.openReader(ctx, spec)
}

// Interrupt aborts every in-flight writer and read and returns at once;
// writer upload cleanup runs in the background (Close, or each writer's
// Abort, waits for it).
func (p *Plane) Interrupt() { p.cancel() }

// Close stops admission, aborts every in-flight writer and read, closes
// every open raw body of every reader (returning only once they are closed),
// and waits until every writer goroutine has finished its upload cleanup
// (sink close, multipart abort), bounded by closeWait; on timeout it logs a
// warning and returns.
// Safe to call more than once.
func (p *Plane) Close() error {
	p.mu.Lock()
	p.closed = true
	readers := make([]*reader, 0, len(p.readers))
	for r := range p.readers {
		readers = append(readers, r)
	}
	p.mu.Unlock()
	p.cancel()
	// Raw bodies are closed before Close returns (cancellation has already
	// released any read blocked on remote storage).
	for _, r := range readers {
		r.closeBodies(false)
	}

	done := make(chan struct{})
	go func() {
		p.writers.Wait()
		close(done)
	}()
	t := time.NewTimer(p.closeWait)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		log.Printf("goplane: warning: writer upload cleanup still running after %s; closing anyway", p.closeWait)
	}
	return nil
}

// forget drops a closed (or never opened) reader from plane Close's list.
func (p *Plane) forget(r *reader) {
	p.mu.Lock()
	delete(p.readers, r)
	p.mu.Unlock()
}

// isLocalPrefix reports whether a data prefix is a local path (tests): it
// needs no credentials.
func isLocalPrefix(prefix string) bool {
	return !strings.Contains(prefix, "://") || strings.HasPrefix(prefix, "file://")
}
