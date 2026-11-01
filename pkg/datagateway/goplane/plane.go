// Package goplane is the all-Go datagateway.DataPlane: Arrow batches are
// encoded with arrow-go pqarrow (ZSTD) and streamed to the table's data prefix
// through iceberg-go IO with Lakekeeper-vended credentials (spec
// docs/superpowers/specs/2026-11-01-all-go-gateway-design.md, Write path 4–7).
package goplane

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
}

var _ datagateway.DataPlane = (*Plane)(nil)

// New returns a Plane.
func New(cfg Config) *Plane {
	ctx, cancel := context.WithCancel(context.Background())
	return &Plane{cfg: cfg, ctx: ctx, cancel: cancel, openSink: openIOSink}
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
	if err := p.ctx.Err(); err != nil {
		return nil, fmt.Errorf("goplane: plane closed: %w", err)
	}
	if spec.TargetFileBytes <= 0 {
		spec.TargetFileBytes = defaultTargetFileBytes
	}
	if spec.RowGroupRows <= 0 {
		spec.RowGroupRows = defaultRowGroupRows
	}
	return newWriter(p.ctx, spec, local, p.openSink), nil
}

// OpenTableReader is not implemented yet.
func (p *Plane) OpenTableReader(context.Context, datagateway.ReaderSpec) (datagateway.TableReader, error) {
	return nil, errors.New("goplane: reads not implemented")
}

// Interrupt aborts every in-flight writer.
func (p *Plane) Interrupt() { p.cancel() }

// Close aborts every in-flight writer.
func (p *Plane) Close() error {
	p.cancel()
	return nil
}

// isLocalPrefix reports whether a data prefix is a local path (tests): it
// needs no credentials.
func isLocalPrefix(prefix string) bool {
	return !strings.Contains(prefix, "://") || strings.HasPrefix(prefix, "file://")
}
