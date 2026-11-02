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
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/datagateway"
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
}

// Plane implements datagateway.DataPlane.
type Plane struct {
	cfg    Config
	ctx    context.Context // lifetime of every writer/reader; cancelled by Interrupt and Close
	cancel context.CancelFunc

	// openSink opens the upload sink of one data file; tests replace it.
	openSink sinkOpener
	// closeWait bounds how long Close waits for open writers' cleanup;
	// tests shorten it.
	closeWait time.Duration

	mu      sync.Mutex
	closed  bool           // set by Close: no new writers
	writers sync.WaitGroup // one per writer goroutine not yet exited
}

// defaultCloseWait bounds Close's wait for writer upload cleanup (sink
// close, AbortMultipartUpload) so shutdown cannot hang on a stuck upload.
const defaultCloseWait = 30 * time.Second

var _ datagateway.DataPlane = (*Plane)(nil)

// New returns a Plane.
func New(cfg Config) *Plane {
	ctx, cancel := context.WithCancel(context.Background())
	return &Plane{cfg: cfg, ctx: ctx, cancel: cancel, openSink: openIOSink, closeWait: defaultCloseWait}
}

// Factory builds the plane once boot validation has passed.
func Factory(_ context.Context, _ datagateway.BootInfo) (datagateway.DataPlane, error) {
	return New(Config{}), nil
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
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("goplane: plane closed")
	}
	if err := p.ctx.Err(); err != nil {
		return nil, fmt.Errorf("goplane: plane closed: %w", err)
	}
	w := newWriter(p.ctx, spec, local, p.openSink)
	p.writers.Add(1)
	go func() {
		<-w.done
		p.writers.Done()
	}()
	return w, nil
}

// OpenTableReader is not implemented yet.
func (p *Plane) OpenTableReader(context.Context, datagateway.ReaderSpec) (datagateway.TableReader, error) {
	return nil, errors.New("goplane: reads not implemented")
}

// Interrupt aborts every in-flight writer and returns at once; their upload
// cleanup runs in the background (Close, or each writer's Abort, waits for it).
func (p *Plane) Interrupt() { p.cancel() }

// Close stops admission, aborts every in-flight writer and waits until every
// writer goroutine has finished its upload cleanup (sink close, multipart
// abort), bounded by closeWait; on timeout it logs a warning and returns.
// Safe to call more than once.
func (p *Plane) Close() error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.cancel()

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

// isLocalPrefix reports whether a data prefix is a local path (tests): it
// needs no credentials.
func isLocalPrefix(prefix string) bool {
	return !strings.Contains(prefix, "://") || strings.HasPrefix(prefix, "file://")
}
