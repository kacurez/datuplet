// Package datagateway provides the data gateway gRPC server.
//
// Path Handling Design:
// All storage paths flowing through DataGateway are treated as opaque URLs.
// Any parsing or normalization must happen in the data plane.
// Paths come from lakekeeper (per-table data prefix returned by
// LoadTable / CreateTable) and are passed verbatim to the DataPlane.
package datagateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/datuplet/datuplet/pkg/icebergjob"
	"github.com/datuplet/datuplet/pkg/lib/secrets"
	"google.golang.org/grpc"

	"github.com/datuplet/datuplet/pkg/datagateway/format"
	"github.com/datuplet/datuplet/pkg/datagateway/jwks"
	dglakekeeper "github.com/datuplet/datuplet/pkg/datagateway/lakekeeper"
	"github.com/datuplet/datuplet/pkg/datagateway/processor"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	runtokenpkg "github.com/datuplet/datuplet/pkg/datagateway/runtoken"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
	"github.com/datuplet/datuplet/pkg/datupleticeio"
)

// ServerV2 implements the DataGateway v2 gRPC service.
// It supports format conversion, transforms, and optimized Parquet output.
type ServerV2 struct {
	pb.UnimplementedDataGatewayServer

	config    *Config
	allocator memory.Allocator
	registry  *format.Registry

	// plane writes and reads table data. Built once by the DataPlaneFactory
	// in NewServerV2; nil only in hand-built test servers.
	plane DataPlane

	writers map[string]*writerState
	readers map[string]*readerState

	writerCounter int
	readerCounter int

	mu sync.RWMutex

	// gRPC and HTTP servers (published under mu by BootGate.Ready; used by
	// Close for graceful stop).
	grpcServer *grpc.Server
	httpServer *http.Server
	httpAddr   string // HTTP server address (e.g., "localhost:50052")

	// runToken is the single per-run JWT projected by pipeline-api. The
	// lakekeeper resolver attaches it as `Authorization: Bearer …` on
	// every catalog/STS call; lakekeeper's FGA Check uses the synthetic
	// identity carried in `sub` to authz each operation.
	// Empty when the mount is absent (dev/Docker).
	runToken string

	// cancelCtx + cancelStop drive the in-band cancellation watcher.
	// On boot, ServerV2 starts a goroutine that polls
	// Config.PodAnnotationsPath; on `datuplet.io/cancel=true` it cancels
	// cancelCtx, which Server.Close listens for and initiates a graceful
	// shutdown. nil when no PodAnnotationsPath is configured.
	cancelCtx  context.Context
	cancelStop context.CancelFunc

	// cancelled is set (under mu) by the cancel watcher before it snapshots
	// the open writers and readers; one published afterwards is aborted or
	// closed instead.
	cancelled bool

	// lakekeeperResolver is constructed at boot when Config.LakekeeperURL
	// is set. It owns the catalog-side LoadOrCreate plus the per-table
	// vended credentials.
	// nil when DG runs without lakekeeper (tests).
	lakekeeperResolver tableResolver

	// commitPool dispatches per-table iceberg commits inline after
	// CloseWriter. nil when lakekeeperResolver is nil (test mode). In that
	// case CloseWriter still writes metadata but skips the commit dispatch.
	commitPool *CommitPool

	// validatedClaims holds the validated run-token JWT claims set during
	// boot. Non-nil only when the run-token was successfully validated
	// against pipeline-api's JWKS. Used to wire lakekeeper routing
	// (warehouse + project_id) from the JWT payload rather than from
	// operator-supplied config fields.
	validatedClaims *runtokenpkg.ValidatedClaims
}

// tableResolver is the catalog surface the server uses;
// *lakekeeper.Resolver implements it (an interface so tests can observe
// table creates).
type tableResolver interface {
	LoadOrCreateForWrite(ctx context.Context, ns, tbl string, sp dglakekeeper.SchemaProvider) (*dglakekeeper.WriterTarget, error)
	Catalog(ctx context.Context) (catalog.Catalog, error)
	Close() error
}

// writerState holds the state for an active writer.
type writerState struct {
	writerID   string // Writer ID (e.g., "w1") - used in run-scoped file prefixes
	outputName string
	bucket     string // Logical bucket (e.g., "raw") - used for logging/metadata only
	table      string // Logical table (e.g., "orders") - used for logging/metadata only
	basePath   string // Data prefix from lakekeeper (opaque, passed as-is to the plane); "" until the table exists
	creds      CredsSource

	// Input format for Content-Type validation
	inputFormat format.DataFormat

	// Format adapter for parsing input
	adapter format.FormatAdapter

	// Transform pipeline (optional)
	pipeline *processor.Pipeline

	// schema is the input (pre-drop) schema records are parsed with:
	// component-supplied, or inferred from the first chunk.
	schema *schema.Schema
	// outputSchema is schema after the drop processors: the table schema,
	// the plane writer's schema, and what every record must match.
	outputSchema   *schema.Schema
	schemaInferred bool

	// initMu serializes the first-chunk path (deferred create + plane
	// writer open) and the writer Close, so two parallel chunk requests for
	// the same writerID cannot open two plane writers.
	initMu sync.Mutex

	// tw is the plane writer, opened on the first chunk. Set under initMu
	// and s.mu.
	tw TableWriter

	// files are the Parquet files tw produced, set once it closed
	// successfully. Set under s.mu.
	files []WrittenFile

	// committed is set once finalizeAndDispatch has CLAIMED this writer,
	// so a later defensive sweep in Commit() does not double-process it.
	// Set under s.mu.
	committed bool
}

// joinStoragePath joins a base path with a filename in a URL-safe way.
// It handles both URL schemes (s3://, file://) and plain relative paths.
// This avoids creating double slashes when basePath has a trailing slash.
func joinStoragePath(basePath, filename string) string {
	// Ensure basePath ends with exactly one /
	basePath = strings.TrimSuffix(basePath, "/") + "/"
	return basePath + filename
}

// NewServerV2 creates a new v2 gateway server: it resolves secret refs,
// validates the run token, builds the lakekeeper resolver and commit pool,
// then calls newPlane exactly once and only then starts the cancel watcher.
// Boot failures are returned (the binary exits with an application code).
func NewServerV2(cfg *Config, newPlane DataPlaneFactory) (*ServerV2, error) {
	if newPlane == nil {
		return nil, errors.New("data gateway: no data plane factory")
	}
	// Resolve $[name] references in the component config. Fail fast — if any
	// reference can't be resolved, the gateway refuses to start and the Pod
	// surfaces the error via its status.
	if len(cfg.ComponentCfg) > 0 {
		if err := resolveSecretRefsInConfig(cfg); err != nil {
			return nil, fmt.Errorf("secret resolution failed: %w", err)
		}
	}

	// A local data dir would bypass the per-table vended-creds contract
	// that lakekeeper routing requires.
	if cfg.LakekeeperURL != "" && cfg.LocalDataDir != "" {
		return nil, errors.New("data gateway: a local data dir must not be set when LakekeeperURL is configured")
	}

	httpAddr := cfg.HTTPListenAddr()
	// Validate the mounted run-token JWT against pipeline-api's JWKS.
	// Soft-degrade preserved for tests without a mounted token — caller
	// checks path emptiness before invoking the validator.
	//
	// In production both RunTokenPath and PipelineAPIJWKSURL are set by the
	// operator. If RunTokenPath is set but JWKS URL is not (misconfiguration),
	// fail-closed at boot to prevent running without JWT validation.
	var runToken string
	var validatedClaims *runtokenpkg.ValidatedClaims
	if cfg.RunTokenPath != "" {
		if cfg.PipelineAPIJWKSURL == "" {
			return nil, errors.New("data gateway: RunTokenPath is set but PipelineAPIJWKSURL is empty — refusing to boot without JWT validation")
		}
		var err error
		validatedClaims, runToken, err = validateRunTokenForBoot(cfg)
		if err != nil {
			return nil, fmt.Errorf("data gateway: run-token validation failed: %w", err)
		}
		log.Printf("data gateway: run-token validated; routing to warehouse=%q project_id=%q", validatedClaims.Warehouse, validatedClaims.ProjectID)
	}
	s := &ServerV2{
		config:          cfg,
		allocator:       memory.NewGoAllocator(),
		registry:        format.DefaultRegistry(),
		writers:         make(map[string]*writerState),
		readers:         make(map[string]*readerState),
		httpAddr:        httpAddr,
		runToken:        runToken,
		validatedClaims: validatedClaims,
	}

	// When LakekeeperURL is configured, construct the catalog resolver.
	// DG's per-write path then asks lakekeeper for vended STS creds + a
	// per-table data prefix. When unset (tests), the field stays nil and
	// writes go to LocalDataDir. Warehouse + project_id come from the
	// validated JWT claims.
	if cfg.LakekeeperURL != "" {
		if s.validatedClaims == nil {
			return nil, errors.New("data gateway: LakekeeperURL is set but no validated run-token claims — refusing to boot")
		}
		res, lkErr := dglakekeeper.NewResolver(cfg.LakekeeperURL, s.validatedClaims.Warehouse, runToken, s.validatedClaims.ProjectID)
		if lkErr != nil {
			return nil, fmt.Errorf("lakekeeper resolver: %w", lkErr)
		}
		s.lakekeeperResolver = res

		// Audit summary stamped on every inline-commit snapshot: actor /
		// run-id / run-mode / pipeline-api, parsed from the run-token JWT
		// claims (same shape the deleted TableCommit Job wrote via
		// BuildSnapshotSummary). Computed once — the token is fixed for the
		// gateway's lifetime. nil when there's no run-token (dev/test); then
		// snapshots carry only the commit-key, as before. CommitTableFiles
		// merges these with the idempotency key it adds.
		auditProps := icebergjob.BuildSnapshotSummary(runToken, cfg.GetRunID())

		// Inline-commit pool: dispatches per-table iceberg commits after
		// CloseWriter. Workers/queue are small fixed defaults — RFC 021
		// open-question: make them env-configurable in a later slice.
		s.commitPool = NewCommitPool(CommitPoolConfig{
			Workers:      4,
			MaxQueueSize: 256,
			CatalogFn:    res.Catalog,
			CommitFn: func(ctx context.Context, cat catalog.Catalog, ident icebergtable.Identifier, paths []string, mode icebergjob.WriteMode, key string) (*icebergjob.CommitResult, error) {
				return icebergjob.CommitTableFiles(ctx, cat, ident, paths, mode, auditProps, key)
			},
		})

		// Wire the per-gateway run-token into datupleticeio's
		// loadTable-refresh path so iceberg-go's `gs://` IO factory
		// can re-fetch fresh GCS vended creds when the initial token
		// nears expiry (or arrives already-stale from lakekeeper's
		// own credential cache). Without this hook the refresh path
		// errors out and reads against multi-table inputs fail with
		// HTTP 401 the moment the first token TTL elapses.
		runTokenCapture := runToken
		datupleticeio.SetTokenProvider(func(context.Context) (string, error) {
			return runTokenCapture, nil
		})
	}

	// The plane is built after token validation (a reader's catalog ATTACH
	// needs the validated claims) and published BEFORE the cancel watcher
	// starts, so a cancel annotation already present at boot interrupts it.
	boot := BootInfo{RunID: cfg.GetRunID(), LakekeeperURL: cfg.LakekeeperURL, Token: runToken}
	if validatedClaims != nil {
		boot.ProjectID = validatedClaims.ProjectID
		boot.Warehouse = validatedClaims.Warehouse
	}
	plane, err := newPlane(context.Background(), boot)
	if err == nil && plane == nil {
		err = errors.New("factory returned no data plane")
	}
	if err != nil {
		if s.commitPool != nil {
			s.commitPool.Cancel()
		}
		return nil, fmt.Errorf("data gateway: data plane: %w", err)
	}
	s.plane = plane

	// Kick off the in-band cancel watcher. WatchCancelAnnotation only
	// waits for ctx when PodAnnotationsPath is empty (non-K8s).
	cancelCtx, cancelStop := context.WithCancel(context.Background())
	s.cancelCtx = cancelCtx
	s.cancelStop = cancelStop
	go s.watchCancel(cancelCtx, cancelStop)

	return s, nil
}

// errRunCancelled fails writes that would open a plane writer after the cancel
// annotation was observed.
var errRunCancelled = errors.New("data gateway: run cancelled")

func (s *ServerV2) isCancelled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cancelled
}

// watchCancel waits for the cancel annotation; on it, it cancels the commit
// pool, interrupts the data plane, aborts every open writer, closes every
// open reader and cancels ctx (Serve watches it and stops the servers).
func (s *ServerV2) watchCancel(ctx context.Context, stop context.CancelFunc) {
	if err := WatchCancelAnnotation(ctx, s.config.PodAnnotationsPath, s.config.CancelPollInterval); err != nil {
		return // ctx done: server closing
	}
	log.Printf("datuplet.io/cancel=true observed on pod annotations; initiating graceful shutdown")
	if s.commitPool != nil {
		log.Printf("cancel: cancelling commit pool")
		s.commitPool.Cancel()
	}
	s.mu.Lock()
	s.cancelled = true
	open := make([]TableWriter, 0, len(s.writers))
	for _, ws := range s.writers {
		if ws.tw != nil {
			open = append(open, ws.tw)
		}
	}
	readers := s.openReadersLocked()
	s.mu.Unlock()
	s.plane.Interrupt()
	for _, tw := range open {
		tw.Abort()
	}
	for _, rs := range readers {
		rs.close()
	}
	stop()
}

// openReadersLocked returns the open readers; the caller holds s.mu.
func (s *ServerV2) openReadersLocked() []*readerState {
	out := make([]*readerState, 0, len(s.readers))
	for _, rs := range s.readers {
		out = append(out, rs)
	}
	return out
}

// validateRunTokenForBoot performs the JWT validation + raw-token read for DG
// startup. It is extracted from NewServerV2 so tests can call it directly
// and assert on the returned error without triggering log.Fatalf.
//
// Returns (validatedClaims, rawJWT, error). rawJWT is the trimmed JWT string
// needed for outbound Bearer on catalog/STS calls. Both are empty/nil on error.
func validateRunTokenForBoot(cfg *Config) (*runtokenpkg.ValidatedClaims, string, error) {
	jwksClient := jwks.NewClient(cfg.PipelineAPIJWKSURL, nil)
	expectedRunID := os.Getenv("RUN_ID")
	if expectedRunID == "" {
		return nil, "", fmt.Errorf("RUN_ID env not set — required for run-token validation (Secret-swap defence)")
	}
	claims, err := runtokenpkg.LoadAndValidateRunToken(context.Background(), cfg.RunTokenPath, jwksClient, expectedRunID)
	if err != nil {
		return nil, "", err
	}
	b, readErr := readBoundedFile(cfg.RunTokenPath)
	if readErr != nil {
		return nil, "", fmt.Errorf("re-read run-token for Bearer: %w", readErr)
	}
	return claims, strings.TrimSpace(string(b)), nil
}

// resolveSecretRefsInConfig decodes ComponentCfg as JSON, walks the tree via
// pkg/lib/secrets.Resolve using a FileProvider rooted at cfg.SecretsDir, and
// re-encodes. No-op if the tree contains no $[name] refs.
func resolveSecretRefsInConfig(cfg *Config) error {
	var tree any
	if err := json.Unmarshal(cfg.ComponentCfg, &tree); err != nil {
		return fmt.Errorf("ComponentCfg is not valid JSON: %w", err)
	}

	// Validate first to detect refs without file I/O (and surface syntax errors
	// with the same wording the pipeline parser uses).
	refs, err := secrets.Validate(tree)
	if err != nil {
		return err
	}
	if len(refs) == 0 {
		return nil
	}
	if cfg.SecretsDir == "" {
		return fmt.Errorf("ComponentCfg references %d secret(s) %v but SecretsDir is empty", len(refs), refs)
	}

	provider := secrets.NewFileProvider(cfg.SecretsDir)
	resolved, names, resolveErr := secrets.Resolve(tree, provider)
	if resolveErr != nil {
		return resolveErr
	}

	out, err := json.Marshal(resolved)
	if err != nil {
		return fmt.Errorf("re-marshal resolved config: %w", err)
	}
	cfg.ComponentCfg = out
	log.Printf("resolved secret refs: %v", names)
	return nil
}

// stopServersOnCancel reacts to in-band cancellation: when the cancel
// watcher fires, the cancelCtx becomes Done and we initiate GracefulStop on
// the gRPC server AND shut down the HTTP data plane, so a cancel that arrives
// while serving still causes a clean exit without requiring the orchestrator
// to call Close explicitly.
//
// Shutting down the HTTP listener after the gRPC stop is load-bearing for the
// cancel-via-annotation contract: otherwise an in-flight `/data/write/...`
// upload could keep streaming bytes for the ≤15-min STS leak window even
// though the run has been cancelled.
func (s *ServerV2) stopServersOnCancel(grpcServer *grpc.Server) {
	if s.cancelCtx == nil {
		return
	}
	go func() {
		<-s.cancelCtx.Done()
		log.Printf("data gateway: cancel context fired; stopping gRPC server")
		grpcServer.GracefulStop()
		s.mu.Lock()
		httpSrv := s.httpServer
		s.mu.Unlock()
		if httpSrv != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := httpSrv.Shutdown(shutdownCtx); err != nil {
				log.Printf("data gateway: HTTP server shutdown after cancel: %v", err)
			}
		}
	}()
}

// ServeWithHTTP binds the HTTP data plane (Config.HTTPAddr) and gRPC
// (grpcAddr) listeners, then serves both; it returns after a graceful stop
// (Close, or the cancel annotation). A bind failure is returned. The
// gateway binary instead binds before boot and serves through a BootGate.
func (s *ServerV2) ServeWithHTTP(grpcAddr string) error {
	httpLis, err := net.Listen("tcp", s.httpAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s (HTTP): %w", s.httpAddr, err)
	}
	grpcLis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		httpLis.Close()
		return fmt.Errorf("failed to listen on %s: %w", grpcAddr, err)
	}
	gate := NewBootGate()
	gate.Ready(s)
	go func() {
		if err := gate.ServeDataHTTP(httpLis); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()
	return gate.ServeGRPC(grpcLis)
}

// httpHandler is the HTTP data plane's request router.
func (s *ServerV2) httpHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/data/write/", s.handleWrite)
	mux.HandleFunc("/data/read/", s.handleRead)
	mux.HandleFunc("/health", s.handleHealth)
	return mux
}

// handleHealth handles HTTP health check requests.
func (s *ServerV2) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// ============================================================================
// Lifecycle RPCs
// ============================================================================

func (s *ServerV2) GetConfig(ctx context.Context, req *pb.GetConfigRequest) (*pb.ComponentConfig, error) {
	// Convert outputs to simple path map for proto (legacy)
	outputPaths := make(map[string]string)
	for name, out := range s.config.Outputs {
		outputPaths[name] = out.Path
	}

	// Convert input tables to proto
	inputTables := make([]*pb.InputTableConfig, 0, len(s.config.InputTables))
	for _, t := range s.config.InputTables {
		inputTables = append(inputTables, &pb.InputTableConfig{
			Bucket:      t.Bucket,
			Table:       t.Table,
			LogicalName: t.As,
		})
	}

	// Build OutputConfig from server config
	outputConfig := &pb.OutputConfig{
		DefaultBucket:    s.config.DefaultBucket,
		DefaultWriteMode: s.config.DefaultWriteMode,
	}

	// Add explicit output tables
	for _, t := range s.config.OutputTables {
		var columns []*pb.ColumnConfig
		for _, c := range t.Columns {
			columns = append(columns, &pb.ColumnConfig{
				Name: c.Name,
				Type: c.Type,
			})
		}
		outputConfig.Tables = append(outputConfig.Tables, &pb.TableOutputConfig{
			Name:        t.Name,
			Bucket:      t.Bucket,
			WriteMode:   t.WriteMode,
			LogicalName: t.LogicalName,
			Columns:     columns,
		})
	}

	// Add processors
	for _, p := range s.config.Processors {
		outputConfig.Processors = append(outputConfig.Processors, &pb.ProcessorConfig{
			Type:    p.Type,
			Columns: p.Columns,
		})
	}

	// StorageBootstrap (direct storage access for native components) is no
	// longer built: all component data flows through the gateway.
	return &pb.ComponentConfig{
		ExecutionId:   s.config.RunID,
		ComponentName: s.config.ComponentName,
		InputBuckets:  s.config.InputBuckets,
		OutputBuckets: s.config.OutputBuckets,
		InputTables:   inputTables,
		OutputConfig:  outputConfig,
		Config:        s.config.ComponentCfg,
		ChunkSize:     s.config.ChunkSize,
		// Legacy fields
		Inputs:  s.config.Inputs,
		Outputs: outputPaths,
	}, nil
}

// Shutdown is a no-op RPC retained for protocol compatibility.
// Actual server teardown is driven by the process lifecycle (ctx cancel /
// signal) via Close, not by this RPC.
func (s *ServerV2) Shutdown(ctx context.Context, req *pb.ShutdownRequest) (*pb.ShutdownResponse, error) {
	return &pb.ShutdownResponse{}, nil
}

// Close cleans up server resources.
// Safe to call concurrently with Serve; gracefully stops the gRPC server
// and shuts down the HTTP data plane within a bounded timeout.
func (s *ServerV2) Close() error {
	// Stop the in-band cancel watcher goroutine.
	if s.cancelStop != nil {
		s.cancelStop()
	}

	// Gracefully stop gRPC server if running; force-stop if drain exceeds the timeout.
	s.mu.RLock()
	grpcServer, httpServer := s.grpcServer, s.httpServer
	s.mu.RUnlock()
	if grpcServer != nil {
		stopGRPC(grpcServer, 10*time.Second)
	}

	// Shut down HTTP data plane with a short deadline.
	if httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(ctx); err != nil {
			log.Printf("Warning: HTTP server shutdown error: %v", err)
		}
	}

	// Drain the inline-commit pool before releasing the lakekeeper
	// resolver. Cancel first so new Dispatches are rejected; then Wait
	// with a bounded timeout so in-flight commits can finish cleanly
	// before the catalog connection goes away.
	if s.commitPool != nil {
		s.commitPool.Cancel()
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
		s.commitPool.Wait(drainCtx)
		drainCancel()
	}

	// Close the data plane once no commit can still be running; open
	// writers and readers end with it.
	s.mu.Lock()
	readers := s.openReadersLocked()
	s.mu.Unlock()
	for _, rs := range readers {
		rs.close()
	}
	if s.plane != nil {
		if err := s.plane.Close(); err != nil {
			log.Printf("Warning: data plane close error: %v", err)
		}
	}

	// Release the lakekeeper resolver's resources. For SQLite-filesystem
	// mode this closes the underlying *sql.DB pool that the lazy-init
	// shim opened; for REST mode it's a no-op.
	if s.lakekeeperResolver != nil {
		if err := s.lakekeeperResolver.Close(); err != nil {
			log.Printf("Warning: lakekeeper resolver close error: %v", err)
		}
	}

	return nil
}

// ============================================================================
// Logging RPC
// ============================================================================

func (s *ServerV2) Log(ctx context.Context, req *pb.LogRequest) (*pb.LogResponse, error) {
	fields := ""
	if len(req.Fields) > 0 {
		for k, v := range req.Fields {
			fields += fmt.Sprintf(" %s=%s", k, v)
		}
	}

	log.Printf("[%s] [%s] %s%s", s.config.ComponentName, req.Level, req.Message, fields)

	return &pb.LogResponse{}, nil
}

// ============================================================================
// Helper Functions
// ============================================================================

// protoToDataFormat converts proto DataFormat to format.DataFormat.
func protoToDataFormat(f pb.DataFormat) format.DataFormat {
	switch f {
	case pb.DataFormat_FORMAT_CSV:
		return format.FormatCSV
	case pb.DataFormat_FORMAT_JSON:
		return format.FormatJSON
	case pb.DataFormat_FORMAT_JSONL:
		return format.FormatJSONL
	case pb.DataFormat_FORMAT_PARQUET:
		return format.FormatParquet
	case pb.DataFormat_FORMAT_ARROW_IPC:
		return format.FormatArrowIPC
	default:
		return format.FormatUnknown
	}
}

// dataFormatToProto converts format.DataFormat to proto DataFormat.
func dataFormatToProto(f format.DataFormat) pb.DataFormat {
	switch f {
	case format.FormatCSV:
		return pb.DataFormat_FORMAT_CSV
	case format.FormatJSON:
		return pb.DataFormat_FORMAT_JSON
	case format.FormatJSONL:
		return pb.DataFormat_FORMAT_JSONL
	case format.FormatParquet:
		return pb.DataFormat_FORMAT_PARQUET
	case format.FormatArrowIPC:
		return pb.DataFormat_FORMAT_ARROW_IPC
	default:
		return pb.DataFormat_FORMAT_UNSPECIFIED
	}
}

// protoToSchema converts proto Schema to schema.Schema.
func protoToSchema(pbSchema *pb.Schema) (*schema.Schema, error) {
	if pbSchema == nil {
		return nil, nil
	}

	columns := make([]schema.ColumnDef, len(pbSchema.Columns))
	for i, col := range pbSchema.Columns {
		columns[i] = schema.ColumnDef{
			Name:     col.Name,
			Type:     schema.ParseDataType(col.Type),
			Nullable: col.Nullable,
			FieldID:  col.FieldId, // Preserve Iceberg field ID from proto
		}
	}

	return schema.NewSchema(columns)
}

// schemaToProto converts schema.Schema to proto Schema.
func schemaToProto(s *schema.Schema) *pb.Schema {
	if s == nil {
		return nil
	}

	columns := make([]*pb.ColumnDef, s.NumColumns())
	for i := 0; i < s.NumColumns(); i++ {
		col := s.Column(i)
		fieldID := col.FieldID
		// Auto-assign field IDs if not set (starting from 1)
		if fieldID == 0 {
			fieldID = int32(i + 1)
		}
		columns[i] = &pb.ColumnDef{
			Name:     col.Name,
			Type:     col.Type.String(),
			Nullable: col.Nullable,
			FieldId:  fieldID, // Include Iceberg field ID
		}
	}

	return &pb.Schema{Columns: columns}
}

// buildPipelineFromProcessors builds a transform pipeline from config processors.
func buildPipelineFromProcessors(processors []ProcessorConfig) *processor.Pipeline {
	if len(processors) == 0 {
		return nil
	}

	p := processor.NewPipeline()
	for _, proc := range processors {
		switch proc.Type {
		case "drop":
			p.Add(processor.NewDropOp(proc.Columns))
			// Add more processor types here as needed
		}
	}

	if len(p.Operations) == 0 {
		return nil
	}
	return p
}
