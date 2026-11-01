//go:build duckdb_arrow

// Package duckplane is the DuckDB implementation of datagateway.DataPlane:
// one in-memory, locked-down DuckDB database per sidecar. Writers COPY Arrow
// batches to Parquet files under the table's data prefix (the Go commit pool
// commits them); reads arrive in a later task.
package duckplane

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/datuplet/datuplet/gateway/duck"
	"github.com/datuplet/datuplet/pkg/datagateway"
)

const (
	defaultTargetFileBytes = 128 << 20
	defaultRowGroupRows    = 229376
	// rotationFactor: a file ends once the uncompressed Arrow bytes fed into
	// it reach rotationFactor × TargetFileBytes.
	rotationFactor = 4
)

var (
	errPlaneClosed  = errors.New("duckplane: data plane is closed")
	errInterrupted  = errors.New("duckplane: interrupted")
	errReadsMissing = errors.New("duckplane: reads not implemented")
)

// Config configures a Plane.
type Config struct {
	Settings   duck.Settings // from SettingsFromEnv()
	Extensions []string      // LOADed before Lock; the binary passes {"httpfs","iceberg"}; unit tests pass none
	Catalog    CatalogAttach // used by reads; zero value = reads unsupported
}

// CatalogAttach is what a reader needs to ATTACH Lakekeeper.
type CatalogAttach struct{ URL, ProjectID, Warehouse, Token string }

// String redacts Token (%v, %+v, %s; %#v still exposes it).
func (c CatalogAttach) String() string {
	return fmt.Sprintf("CatalogAttach{url=%q project_id=%q warehouse=%q token=<redacted>}", c.URL, c.ProjectID, c.Warehouse)
}

// Plane is the DuckDB data plane. It implements datagateway.DataPlane.
type Plane struct {
	cfg    Config
	db     *duck.DB
	ctx    context.Context // lifetime of every writer
	cancel context.CancelCauseFunc
	nextID atomic.Uint64

	mu      sync.Mutex
	closed  bool
	writers map[*writer]struct{}

	// secretExec, when set by a test, replaces the secret statement and
	// runs it for local prefixes too (no S3/GCS secret type without httpfs).
	secretExec func(ctx context.Context, sql string) error
	// readerHook, when set by a test, runs in every writer view's Next before
	// it receives a batch (a test can stall the COPY to build backpressure).
	readerHook func()
}

var _ datagateway.DataPlane = (*Plane)(nil)

// New opens the database with cfg.Settings, LOADs cfg.Extensions, initialises
// the secret manager (it cannot start once the configuration is locked) and
// applies the lockdown.
func New(cfg Config) (*Plane, error) {
	db, err := duck.Open(cfg.Settings)
	if err != nil {
		return nil, fmt.Errorf("duckplane: %w", err)
	}
	if err := setup(db, cfg.Extensions); err != nil {
		db.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	return &Plane{cfg: cfg, db: db, ctx: ctx, cancel: cancel, writers: map[*writer]struct{}{}}, nil
}

func setup(db *duck.DB, extensions []string) error {
	c, err := db.Connect()
	if err != nil {
		return fmt.Errorf("duckplane: %w", err)
	}
	defer c.Close()
	ctx := context.Background()
	for _, ext := range extensions {
		if ext == "" || strings.ContainsAny(ext, "/\\.") {
			return fmt.Errorf("duckplane: invalid extension name %q", ext)
		}
		if err := c.Exec(ctx, "LOAD "+duck.Ident(ext)); err != nil {
			return fmt.Errorf("duckplane: load extension %s: %w", ext, err)
		}
	}
	if _, err := c.QueryRows(ctx, "SELECT count(*) FROM duckdb_secrets()"); err != nil {
		return fmt.Errorf("duckplane: secret manager: %w", err)
	}
	if err := db.Lock(c); err != nil {
		return fmt.Errorf("duckplane: %w", err)
	}
	return nil
}

// Factory returns a DataPlaneFactory that fills base.Catalog from the
// validated boot claims and calls New.
func Factory(base Config) datagateway.DataPlaneFactory {
	return func(_ context.Context, boot datagateway.BootInfo) (datagateway.DataPlane, error) {
		cfg := base
		cfg.Catalog = CatalogAttach{
			URL:       boot.LakekeeperURL,
			ProjectID: boot.ProjectID,
			Warehouse: boot.Warehouse,
			Token:     boot.Token,
		}
		return New(cfg)
	}
}

// OpenTableWriter validates spec and returns a writer on its own connection.
// ctx bounds only this call; the writer lives until Close, Abort, Interrupt
// or plane Close.
func (p *Plane) OpenTableWriter(_ context.Context, spec datagateway.WriterSpec) (datagateway.TableWriter, error) {
	if spec.DataPrefix == "" || spec.FilePrefix == "" {
		return nil, errors.New("duckplane: writer needs a data prefix and a file prefix")
	}
	sel, err := selectList(spec.Schema)
	if err != nil {
		return nil, err
	}
	remote := !isLocalPrefix(spec.DataPrefix)
	if remote && spec.Creds == nil {
		return nil, errors.New("duckplane: a remote data prefix needs vended credentials")
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
		return nil, errPlaneClosed
	}
	conn, err := p.db.Connect()
	if err != nil {
		return nil, fmt.Errorf("duckplane: %w", err)
	}
	id := p.nextID.Add(1)
	wctx, cancel := context.WithCancelCause(p.ctx)
	w := &writer{
		p:          p,
		id:         id,
		spec:       spec,
		sel:        sel,
		remote:     remote,
		rotateAt:   rotationFactor * spec.TargetFileBytes,
		secretName: fmt.Sprintf("dgw_%d", id),
		conn:       conn,
		ctx:        wctx,
		cancel:     cancel,
		op:         make(chan struct{}, 1),
	}
	p.writers[w] = struct{}{}
	return w, nil
}

// OpenTableReader is not implemented yet.
func (p *Plane) OpenTableReader(context.Context, datagateway.ReaderSpec) (datagateway.RecordStream, error) {
	return nil, errReadsMissing
}

func (p *Plane) liveWriters() []*writer {
	p.mu.Lock()
	defer p.mu.Unlock()
	ws := make([]*writer, 0, len(p.writers))
	for w := range p.writers {
		ws = append(ws, w)
	}
	return ws
}

func (p *Plane) unregister(w *writer) {
	p.mu.Lock()
	delete(p.writers, w)
	p.mu.Unlock()
}

// Interrupt aborts every in-flight statement: each open writer fails.
func (p *Plane) Interrupt() {
	for _, w := range p.liveWriters() {
		w.interrupt(errInterrupted)
	}
}

// Close aborts every open writer and closes the database. Idempotent.
func (p *Plane) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()
	p.cancel(errPlaneClosed)
	for _, w := range p.liveWriters() {
		w.Abort()
	}
	return p.db.Close()
}
