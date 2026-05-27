// Package datagateway provides the data gateway gRPC server.
//
// Path Handling Design:
// All storage paths flowing through DataGateway are treated as opaque URLs.
// Any parsing or normalization must happen in the backend layer.
// Paths come from lakekeeper (per-table data prefix returned by
// LoadTable / CreateTable) and are passed verbatim to BufferManager and Backend.
package datagateway

import (
	"context"
	"encoding/json"
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
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/datuplet/datuplet/pkg/datagateway/backend"
	"github.com/datuplet/datuplet/pkg/datagateway/buffer"
	"github.com/datuplet/datuplet/pkg/datagateway/format"
	"github.com/datuplet/datuplet/pkg/datagateway/jwks"
	dglakekeeper "github.com/datuplet/datuplet/pkg/datagateway/lakekeeper"
	"github.com/datuplet/datuplet/pkg/datagateway/manifest"
	"github.com/datuplet/datuplet/pkg/datupleticeio"
	"github.com/datuplet/datuplet/pkg/datagateway/partition"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/datuplet/datuplet/pkg/datagateway/processor"
	runtokenpkg "github.com/datuplet/datuplet/pkg/datagateway/runtoken"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// ServerV2 implements the DataGateway v2 gRPC service.
// It supports format conversion, transforms, and optimized Parquet output.
type ServerV2 struct {
	pb.UnimplementedDataGatewayServer

	config    *Config
	backend   backend.StorageBackend
	allocator memory.Allocator
	registry  *format.Registry
	ctx       context.Context // Background context for long-lived operations

	writers map[string]*writerState
	readers map[string]*readerState

	writerCounter int
	readerCounter int

	mu sync.RWMutex

	// gRPC server (set by Serve; used by Close for graceful stop).
	grpcServer *grpc.Server

	// HTTP data plane
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

	// filesManifest accumulates the parquet paths written during the
	// run, grouped by (namespace, table). Persisted at end-of-stream
	// to `<table-base>/.run-state/<run-id>/files.json` as a
	// recovery/observability breadcrumb (RFC 021 — DG commits inline;
	// no external TableCommit Job consumes this anymore).
	filesManifest *FilesManifest

	// lakekeeperResolver is constructed at boot when Config.LakekeeperURL
	// is set. It owns the catalog-side LoadOrCreate plus vended-creds
	// backend construction for both writes and reads.
	// nil when DG runs without lakekeeper (tests, legacy Docker).
	lakekeeperResolver *dglakekeeper.Resolver

	// commitPool dispatches per-table iceberg commits inline after
	// CloseWriter. nil when lakekeeperResolver is nil (test/static-backend
	// mode). In that case CloseWriter still writes metadata but skips the
	// commit dispatch.
	commitPool *CommitPool

	// validatedClaims holds the validated run-token JWT claims set during
	// boot. Non-nil only when the run-token was successfully validated
	// against pipeline-api's JWKS. Used to wire lakekeeper routing
	// (warehouse + project_id) from the JWT payload rather than from
	// operator-supplied config fields.
	validatedClaims *runtokenpkg.ValidatedClaims
}

// writerState holds the state for an active writer.
type writerState struct {
	writerID   string // Writer ID (e.g., "w1") - used in run-scoped file prefixes
	outputName string
	bucket     string // Logical bucket (e.g., "raw") - used for logging/metadata only
	table      string // Logical table (e.g., "orders") - used for logging/metadata only
	basePath   string // Resolved base path from lakekeeper (opaque, passed as-is to backend)

	// Input format for Content-Type validation
	inputFormat format.DataFormat

	// Format adapter for parsing input
	adapter format.FormatAdapter

	// Transform pipeline (optional)
	pipeline *processor.Pipeline

	// Buffer manager for Parquet output
	bufferMgr *buffer.BufferManager

	// Per-writer backend (vended-creds-backed minio backend, or static
	// backend in test mode).
	writerBackend backend.StorageBackend

	// Schema (may be inferred from first chunk)
	schema         *schema.Schema
	outputSchema   *schema.Schema // After transforms (same as schema when no transforms)
	schemaInferred bool

	// Statistics
	totalRows  int64
	totalBytes int64

	// External files (for components that write directly to storage)
	// If set, these are used instead of bufferMgr.FilesWritten()
	externalFiles []buffer.FileInfo

	// Partition fields (resolved from config or control file)
	partitionFields   []PartitionFieldConfig
	partitionFieldDefs []manifest.PartitionFieldDef

	// Partition router (non-nil only for partitioned tables)
	partitionRouter *partition.Router

	// Whether table already exists in the catalog at OpenWriter time
	// (set by the lakekeeper resolver: true on LoadTable success, false
	// when CreateTable was deferred to first-chunk schema inference).
	tableExists bool

	// initMu serializes the deferred-create + buffer-manager construction
	// path in processWriteChunk. Two parallel WriteChunk / handleHTTPWrite
	// requests for the same writerID could otherwise both observe
	// basePath == "" (or schema == nil) and race on building duplicate
	// BufferManager / partitionRouter / per-writer backend instances.
	initMu sync.Mutex

	// committed is set once finalizeAndDispatch has CLAIMED this writer,
	// so a later defensive sweep in Commit() does not double-process it.
	// Set under s.mu.
	committed bool

	// closed is set the first time this writer's buffer/router is closed
	// (by CloseWriter). The Commit sweep checks it to avoid a second
	// Close() call (buffer/router Close is not guaranteed idempotent).
	// Set under s.mu.
	closed bool
}

// readerState holds the state for an active reader.
type readerState struct {
	inputName string
	tablePath string

	// Backend reader
	backendReader backend.Reader

	// Output format adapter
	adapter format.FormatAdapter

	// Transform pipeline (optional)
	pipeline *processor.Pipeline

	// Schema
	schema *schema.Schema

	// Per-reader backend (vended-creds-backed minio backend, or static
	// backend in test mode).
	readerBackend backend.StorageBackend
}

// joinStoragePath joins a base path with a filename in a URL-safe way.
// It handles both URL schemes (s3://, file://) and plain relative paths.
// This avoids creating double slashes when basePath has a trailing slash.
func joinStoragePath(basePath, filename string) string {
	// Ensure basePath ends with exactly one /
	basePath = strings.TrimSuffix(basePath, "/") + "/"
	return basePath + filename
}

// NewServerV2 creates a new v2 gateway server.
func NewServerV2(cfg *Config) *ServerV2 {
	// Resolve $[name] references in the component config. Fail fast — if any
	// reference can't be resolved, the gateway refuses to start and the Pod
	// surfaces the error via its status.
	if len(cfg.ComponentCfg) > 0 {
		if err := resolveSecretRefsInConfig(cfg); err != nil {
			log.Fatalf("secret resolution failed: %v", err)
		}
	}

	httpAddr := cfg.HTTPAddr
	if httpAddr == "" {
		httpAddr = ":50052" // Default HTTP port
	}
	// Validate the mounted run-token JWT against pipeline-api's JWKS.
	// Soft-degrade preserved for tests/Docker without a mounted token —
	// caller checks path emptiness before invoking the validator.
	//
	// In production both RunTokenPath and PipelineAPIJWKSURL are set by the
	// operator. If RunTokenPath is set but JWKS URL is not (misconfiguration),
	// fail-closed at boot to prevent running without JWT validation.
	var runToken string
	var validatedClaims *runtokenpkg.ValidatedClaims
	if cfg.RunTokenPath != "" {
		if cfg.PipelineAPIJWKSURL == "" {
			log.Fatalf("data gateway: RunTokenPath is set but PipelineAPIJWKSURL is empty — refusing to boot without JWT validation")
		}
		var err error
		validatedClaims, runToken, err = validateRunTokenForBoot(cfg)
		if err != nil {
			log.Fatalf("data gateway: run-token validation failed: %v", err)
		}
		log.Printf("data gateway: run-token validated; routing to warehouse=%q project_id=%q", validatedClaims.Warehouse, validatedClaims.ProjectID)
	}
	cancelCtx, cancelStop := context.WithCancel(context.Background())
	s := &ServerV2{
		config:          cfg,
		backend:         cfg.Backend,
		allocator:       memory.NewGoAllocator(),
		registry:        format.DefaultRegistry(),
		ctx:             context.Background(), // Long-lived context for background operations
		writers:         make(map[string]*writerState),
		readers:         make(map[string]*readerState),
		httpAddr:        httpAddr,
		runToken:        runToken,
		cancelCtx:       cancelCtx,
		cancelStop:      cancelStop,
		filesManifest:   NewFilesManifest(cfg.GetRunID()),
		validatedClaims: validatedClaims,
	}

	// When LakekeeperURL is configured, construct the catalog resolver.
	// DG's per-write path then asks lakekeeper for vended STS creds + a
	// per-table data prefix. When unset (tests, pure-static-backend dev),
	// the field stays nil and writes fall back to the static backend.
	// Warehouse + project_id come from the validated JWT claims.
	if cfg.LakekeeperURL != "" {
		if s.validatedClaims == nil {
			log.Fatalf("data gateway: LakekeeperURL is set but no validated run-token claims — refusing to boot")
		}
		res, lkErr := dglakekeeper.NewResolver(cfg.LakekeeperURL, s.validatedClaims.Warehouse, runToken, s.validatedClaims.ProjectID)
		if lkErr != nil {
			log.Fatalf("lakekeeper resolver: %v", lkErr)
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
			CatalogFn:    s.lakekeeperResolver.Catalog,
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

	// Kick off the in-band cancel watcher. WatchCancelAnnotation is a
	// no-op when PodAnnotationsPath is empty (Docker / non-K8s).
	go func() {
		if err := WatchCancelAnnotation(cancelCtx, cfg.PodAnnotationsPath, cfg.CancelPollInterval); err == nil {
			// Returning nil means cancellation was requested. Signal
			// the rest of the server by cancelling cancelCtx; the
			// gateway's main loop watches it and exits gracefully.
			//
			// TODO: also abort in-flight S3 uploads here so we exit faster
			// than the gRPC graceful-stop timeout (future improvement).
			log.Printf("datuplet.io/cancel=true observed on pod annotations; initiating graceful shutdown")
			if s.commitPool != nil {
				log.Printf("cancel: cancelling commit pool")
				s.commitPool.Cancel()
			}
			cancelStop()
		}
	}()

	// Enforce: static backend must not coexist with the lakekeeper
	// resolver. A static backend would bypass the per-table vended-creds
	// contract that lakekeeper routing requires.
	if s.lakekeeperResolver != nil && s.backend != nil {
		log.Fatalf("FATAL: static backend must be nil when LakekeeperURL is configured (got %T)", s.backend)
	}

	return s
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

// Serve starts the gRPC server on the given address.
func (s *ServerV2) Serve(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterDataGatewayServer(grpcServer, s)

	// Enable gRPC health checking for K8s probes
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	// Publish grpcServer so Close() can stop it gracefully.
	s.mu.Lock()
	s.grpcServer = grpcServer
	s.mu.Unlock()

	// React to in-band cancellation: when the cancel
	// watcher fires, the cancelCtx becomes Done and we initiate
	// GracefulStop on the gRPC server AND shut down the HTTP data
	// plane. Tied here (rather than in Close) so a cancel that
	// arrives while Serve is blocked still causes a clean exit
	// without requiring the orchestrator to call Close explicitly.
	//
	// Shutting down the HTTP listener after the gRPC stop is load-bearing
	// for the cancel-via-annotation contract: otherwise an in-flight
	// `/data/write/...` upload could keep streaming bytes for the ≤15-min
	// STS leak window even though the run has been cancelled.
	if s.cancelCtx != nil {
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

	log.Printf("Data Gateway v2 listening on %s (%s)", addr, GatewayBuildInfo())
	if DebugEnabled() {
		log.Printf("DBG gateway debug logging enabled via DATUPLET_GATEWAY_DEBUG")
	}

	return grpcServer.Serve(lis)
}

// ServeWithHTTP starts both gRPC and HTTP servers.
func (s *ServerV2) ServeWithHTTP(grpcAddr string) error {
	// Start HTTP server in background
	go func() {
		if err := s.serveHTTP(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Start gRPC server (blocking)
	return s.Serve(grpcAddr)
}

// serveHTTP starts the HTTP data plane server.
func (s *ServerV2) serveHTTP() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/data/write/", s.handleWrite)
	mux.HandleFunc("/data/read/", s.handleRead)
	mux.HandleFunc("/health", s.handleHealth)

	s.httpServer = &http.Server{
		Addr:    s.httpAddr,
		Handler: mux,
	}

	log.Printf("Data Gateway HTTP server listening on %s", s.httpAddr)
	return s.httpServer.ListenAndServe()
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
		outputConfig.Tables = append(outputConfig.Tables, &pb.TableOutputConfig{
			Name:        t.Name,
			Bucket:      t.Bucket,
			WriteMode:   t.WriteMode,
			LogicalName: t.LogicalName,
		})
	}

	// Add processors
	for _, p := range s.config.Processors {
		outputConfig.Processors = append(outputConfig.Processors, &pb.ProcessorConfig{
			Type:    p.Type,
			Columns: p.Columns,
		})
	}

	// Build storage bootstrap for native S3 components (if configured)
	var storageBootstrap *pb.StorageBootstrap
	if len(s.config.InputTables) > 0 || len(s.config.OutputTables) > 0 {
		var err error
		storageBootstrap, err = s.buildStorageBootstrap(ctx)
		if err != nil {
			log.Printf("Warning: failed to build storage bootstrap: %v", err)
			// Don't fail the whole request - component may not need it
		}
	}

	return &pb.ComponentConfig{
		ExecutionId:      s.config.RunID,
		ComponentName:    s.config.ComponentName,
		InputBuckets:     s.config.InputBuckets,
		OutputBuckets:    s.config.OutputBuckets,
		InputTables:      inputTables,
		OutputConfig:     outputConfig,
		Config:           s.config.ComponentCfg,
		ChunkSize:        s.config.ChunkSize,
		StorageBootstrap: storageBootstrap,
		// Legacy fields
		Inputs:  s.config.Inputs,
		Outputs: outputPaths,
	}, nil
}

// buildStorageBootstrap builds the StorageBootstrap data for native S3
// components. Inputs/outputs are resolved via the lakekeeper resolver
// when configured; bucket credentials come from the static
// `s.config.S3*` fields. Native S3 components are a legacy escape hatch
// — the canonical write path goes through DataGateway's gRPC/HTTP
// surface where vended-creds rotation is handled per-call.
func (s *ServerV2) buildStorageBootstrap(ctx context.Context) (*pb.StorageBootstrap, error) {
	bootstrap := &pb.StorageBootstrap{
		BucketCredentials: make(map[string]*pb.S3Credentials),
	}

	// Build input table data (file paths from lakekeeper).
	for _, input := range s.config.InputTables {
		var filePaths []string
		if s.lakekeeperResolver != nil {
			rt, err := s.lakekeeperResolver.LoadTableForRead(ctx, input.Bucket, input.Table)
			if err != nil {
				log.Printf("Warning: failed to resolve table %s.%s via lakekeeper: %v", input.Bucket, input.Table, err)
			} else {
				filePaths = rt.DataFiles
			}
		}

		// Resolve logical name (default to table name if not specified)
		logicalName := input.As
		if logicalName == "" {
			logicalName = input.Table
		}

		bootstrap.Inputs = append(bootstrap.Inputs, &pb.ResolvedInput{
			Bucket:      input.Bucket,
			Table:       input.Table,
			FilePaths:   filePaths,
			Format:      "parquet",
			LogicalName: logicalName,
		})
	}

	// Build output table data (S3 write locations from lakekeeper).
	for _, output := range s.config.OutputTables {
		var location string
		if s.lakekeeperResolver != nil {
			wt, err := s.lakekeeperResolver.LoadOrCreateForWrite(ctx, output.Bucket, output.Name, nil)
			if err != nil {
				if s.config.S3BucketName != "" {
					// Schema-deferred path: just synthesize a hint so
					// native components have something to log; real
					// writes go through DG's per-table create.
					location = fmt.Sprintf("s3://%s/%s/%s/data/",
						s.config.S3BucketName, output.Bucket, output.Name)
				} else {
					return nil, fmt.Errorf("failed to resolve write path for %s.%s: %w", output.Bucket, output.Name, err)
				}
			} else {
				location = wt.BasePath
			}
		} else if s.config.S3BucketName != "" {
			location = fmt.Sprintf("s3://%s/%s/%s/data/",
				s.config.S3BucketName, output.Bucket, output.Name)
		} else {
			return nil, fmt.Errorf("no lakekeeper resolver and no S3BucketName for output %s.%s", output.Bucket, output.Name)
		}

		bootstrap.Outputs = append(bootstrap.Outputs, &pb.ResolvedOutput{
			Bucket:   output.Bucket,
			Table:    output.Name,
			Location: location,
		})
	}

	// Bucket credentials. Native S3 components fall back to static
	// config-supplied MinIO credentials (same as the Docker path).
	// Vended-creds for native components is future work.
	if (s.config.S3Endpoint != "" && s.config.S3AccessKey != "") &&
		(len(bootstrap.Inputs) > 0 || len(bootstrap.Outputs) > 0) {
		bucketSet := make(map[string]bool)
		for _, input := range bootstrap.Inputs {
			bucketSet[input.Bucket] = true
		}
		for _, output := range bootstrap.Outputs {
			bucketSet[output.Bucket] = true
		}
		for bucket := range bucketSet {
			bootstrap.BucketCredentials[bucket] = &pb.S3Credentials{
				AccessKeyId:     s.config.S3AccessKey,
				SecretAccessKey: s.config.S3SecretKey,
				Endpoint:        s.config.S3Endpoint,
				Region:          s.config.S3Region,
				BucketName:      s.config.S3BucketName,
				UseSsl:          false, // local MinIO default
				UsePathStyle:    true,  // required for MinIO
			}
		}
	} else if hasFilesystemPaths(bootstrap) {
		log.Printf("Filesystem mode detected — bootstrap has file:// paths, no S3 credentials needed")
	}

	return bootstrap, nil
}

// hasFilesystemPaths checks if any bootstrap input file paths or output locations use file:// scheme.
func hasFilesystemPaths(bootstrap *pb.StorageBootstrap) bool {
	for _, input := range bootstrap.Inputs {
		for _, fp := range input.FilePaths {
			if strings.HasPrefix(fp, "file://") {
				return true
			}
		}
	}
	for _, output := range bootstrap.Outputs {
		if strings.HasPrefix(output.Location, "file://") {
			return true
		}
	}
	return false
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
	grpcServer := s.grpcServer
	s.mu.RUnlock()
	if grpcServer != nil {
		done := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			log.Printf("Warning: gRPC graceful stop timed out, forcing stop")
			grpcServer.Stop()
		}
	}

	// Shut down HTTP data plane with a short deadline.
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(ctx); err != nil {
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
// Schema/Sampling RPCs
// ============================================================================

func (s *ServerV2) GetSchema(ctx context.Context, req *pb.GetSchemaRequest) (*pb.SchemaResponse, error) {
	tablePath, ok := s.config.Inputs[req.InputName]
	if !ok {
		return nil, fmt.Errorf("unknown input: %s", req.InputName)
	}

	backendSchema, err := s.backend.GetSchema(ctx, tablePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for %s: %w", req.InputName, err)
	}

	sch := backendSchemaToGatewaySchema(backendSchema)
	return &pb.SchemaResponse{
		Schema: schemaToProto(sch),
	}, nil
}

func (s *ServerV2) GetSample(ctx context.Context, req *pb.GetSampleRequest) (*pb.SampleResponse, error) {
	tablePath, ok := s.config.Inputs[req.InputName]
	if !ok {
		return nil, fmt.Errorf("unknown input: %s", req.InputName)
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 10
	}

	sample, err := s.backend.GetSample(ctx, tablePath, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get sample for %s: %w", req.InputName, err)
	}

	sch := backendSchemaToGatewaySchema(sample.Schema)
	return &pb.SampleResponse{
		Schema:        schemaToProto(sch),
		Rows:          sample.Rows,
		TotalEstimate: sample.TotalEstimate,
	}, nil
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

// gatewaySchemaToBackendSchema converts schema.Schema to backend.SchemaInfo.
// Returns nil if input is nil.
func gatewaySchemaToBackendSchema(sch *schema.Schema) *backend.SchemaInfo {
	if sch == nil {
		return nil
	}
	cols := make([]backend.ColumnInfo, len(sch.Columns()))
	for i, c := range sch.Columns() {
		cols[i] = backend.ColumnInfo{
			Name:     c.Name,
			Type:     c.Type.String(),
			Nullable: c.Nullable,
		}
	}
	return &backend.SchemaInfo{Columns: cols}
}

// backendSchemaToGatewaySchema converts backend.SchemaInfo to schema.Schema.
func backendSchemaToGatewaySchema(backendSchema *backend.SchemaInfo) *schema.Schema {
	if backendSchema == nil {
		return nil
	}

	columns := make([]schema.ColumnDef, len(backendSchema.Columns))
	for i, col := range backendSchema.Columns {
		columns[i] = schema.ColumnDef{
			Name:     col.Name,
			Type:     schema.ParseDataType(col.Type),
			Nullable: col.Nullable,
		}
	}

	s, _ := schema.NewSchema(columns)
	return s
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
