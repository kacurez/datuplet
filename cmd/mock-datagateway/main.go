// Command mock-datagateway is a local development stand-in for the Data
// Gateway sidecar: it serves the handful of gRPC RPCs + the HTTP write
// endpoint the component SDK uses, validates and counts the Arrow IPC
// batches it receives, and discards the data. It lets the real
// http-json-extractor binary run locally (DATUPLET_GATEWAY_ADDR=localhost:50051)
// against a live URL with no cluster, no Lakekeeper, no object storage.
//
// If the default ports (50051/50052) are already bound by something else on
// the local machine, override with -grpc-addr/-http-addr (the
// `make extractor-local` target exposes these as GRPC_ADDR/HTTP_ADDR).
//
// Dev/test tool only — NOT a deployment surface.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/grpc"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

type writerState struct {
	bucket, table string
	rows          int64
	batches       int64
	bytes         int64
	schema        string   // "name:type, ..." label of the first payload (logging)
	schemaNames   []string // first payload's column names — later payloads must match
}

type mockGateway struct {
	pb.UnimplementedDataGatewayServer

	mu         sync.Mutex
	cfgBytes   []byte
	bucket     string
	writeMode  string
	httpBase   string
	nextID     int
	writers    map[string]*writerState
	expectCols []string // from the config's fields[].name; empty = names not enforced
}

func (g *mockGateway) GetConfig(ctx context.Context, _ *pb.GetConfigRequest) (*pb.ComponentConfig, error) {
	return &pb.ComponentConfig{
		ExecutionId:   "local-mock",
		ComponentName: "http-json-extractor",
		Config:        g.cfgBytes,
		ChunkSize:     32 * 1024 * 1024,
		OutputConfig: &pb.OutputConfig{
			DefaultBucket:    g.bucket,
			DefaultWriteMode: g.writeMode,
		},
	}, nil
}

func (g *mockGateway) OpenWriter(ctx context.Context, req *pb.OpenWriterRequest) (*pb.OpenWriterResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.nextID++
	id := fmt.Sprintf("w%d", g.nextID)
	bucket := req.Bucket
	if bucket == "" {
		bucket = g.bucket
	}
	g.writers[id] = &writerState{bucket: bucket, table: req.Table}
	log.Printf("mock-dg: OpenWriter id=%s table=%s.%s format=%s", id, bucket, req.Table, req.InputFormat)
	return &pb.OpenWriterResponse{
		WriterId:     id,
		HttpEndpoint: g.httpBase + "/data/write/" + id,
		Bucket:       bucket,
		Table:        req.Table,
	}, nil
}

func (g *mockGateway) WriteChunk(ctx context.Context, req *pb.WriteChunkRequest) (*pb.WriteChunkResponse, error) {
	rows, err := g.ingest(req.WriterId, req.Data)
	if err != nil {
		return nil, err
	}
	return &pb.WriteChunkResponse{RowsAccepted: rows}, nil
}

func (g *mockGateway) CloseWriter(ctx context.Context, req *pb.CloseWriterRequest) (*pb.CloseWriterResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	ws, ok := g.writers[req.WriterId]
	if !ok {
		return nil, fmt.Errorf("unknown writer: %s", req.WriterId)
	}
	log.Printf("mock-dg: CloseWriter id=%s table=%s.%s rows=%d batches=%d bytes=%d",
		req.WriterId, ws.bucket, ws.table, ws.rows, ws.batches, ws.bytes)
	return &pb.CloseWriterResponse{TotalRows: ws.rows, TotalBytes: ws.bytes}, nil
}

func (g *mockGateway) Commit(ctx context.Context, _ *pb.CommitRequest) (*pb.CommitResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	byBucket := map[string]*pb.BucketCommitResult{}
	for _, ws := range g.writers {
		b, ok := byBucket[ws.bucket]
		if !ok {
			b = &pb.BucketCommitResult{Bucket: ws.bucket, Status: pb.BucketCommitResult_STATUS_COMMITTED}
			byBucket[ws.bucket] = b
		}
		b.Tables = append(b.Tables, &pb.TableCommitResult{
			Table:      ws.table,
			Bucket:     ws.bucket,
			Status:     pb.TableCommitResult_STATUS_COMMITTED,
			RowsAdded:  ws.rows,
			FilesAdded: int32(ws.batches),
		})
		log.Printf("mock-dg: COMMIT %s.%s rows=%d batches=%d bytes=%d schema=[%s]",
			ws.bucket, ws.table, ws.rows, ws.batches, ws.bytes, ws.schema)
	}
	resp := &pb.CommitResponse{Success: true}
	for _, b := range byBucket {
		resp.Buckets = append(resp.Buckets, b)
	}
	return resp, nil
}

func (g *mockGateway) Log(ctx context.Context, req *pb.LogRequest) (*pb.LogResponse, error) {
	fmt.Printf("[component] [%s] %s\n", req.Level, req.Message)
	return &pb.LogResponse{}, nil
}

// validateSchema enforces the extractor's output contract on every payload:
// all columns Arrow String (utf8), and — when the component config declares a
// fields projection — exactly the declared column names, in order. A
// violation fails the write (HTTP 500 / gRPC error), so the local proof
// cannot silently pass with wrong output.
func (g *mockGateway) validateSchema(sch *arrow.Schema) error {
	for _, f := range sch.Fields() {
		if f.Type.ID() != arrow.STRING {
			return fmt.Errorf("schema violation: column %q is %s, want utf8 (all-String contract)", f.Name, f.Type)
		}
	}
	if len(g.expectCols) > 0 {
		if sch.NumFields() != len(g.expectCols) {
			return fmt.Errorf("schema violation: %d columns, want %d %v", sch.NumFields(), len(g.expectCols), g.expectCols)
		}
		for i, want := range g.expectCols {
			if sch.Field(i).Name != want {
				return fmt.Errorf("schema violation: column %d is %q, want %q", i, sch.Field(i).Name, want)
			}
		}
	}
	return nil
}

// ingest parses one payload as a complete Arrow IPC stream, validates its
// schema against the contract, counts its rows, and records the schema on
// first sight.
func (g *mockGateway) ingest(writerID string, data []byte) (int64, error) {
	rd, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return 0, fmt.Errorf("payload is not a valid Arrow IPC stream: %w", err)
	}
	defer rd.Release()
	if err := g.validateSchema(rd.Schema()); err != nil {
		return 0, err
	}
	var rows int64
	for rd.Next() {
		rows += rd.Record().NumRows()
	}
	if rd.Err() != nil {
		return 0, fmt.Errorf("ipc read: %w", rd.Err())
	}

	names := make([]string, 0, rd.Schema().NumFields())
	labeled := make([]string, 0, rd.Schema().NumFields())
	for _, f := range rd.Schema().Fields() {
		names = append(names, f.Name)
		labeled = append(labeled, f.Name+":"+f.Type.String())
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	ws, ok := g.writers[writerID]
	if !ok {
		return 0, fmt.Errorf("unknown writer: %s", writerID)
	}
	if ws.schemaNames == nil {
		// First payload for this writer: lock its schema. Later payloads
		// must match exactly — catches cross-batch schema drift even for
		// no-`fields` configs (where expectCols is empty).
		ws.schemaNames = names
		ws.schema = strings.Join(labeled, ", ")
		log.Printf("mock-dg: writer %s schema: [%s]", writerID, ws.schema)
	} else if !slices.Equal(ws.schemaNames, names) {
		return 0, fmt.Errorf("schema violation: writer %s column drift: first %v, now %v", writerID, ws.schemaNames, names)
	}
	ws.rows += rows
	ws.batches++
	ws.bytes += int64(len(data))
	return rows, nil
}

func (g *mockGateway) handleHTTPWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/data/write/"), "/")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	rows, err := g.ingest(id, body)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"rows_accepted": rows, "buffer_size_bytes": 0})
}

func main() {
	grpcAddr := flag.String("grpc-addr", "localhost:50051", "gRPC listen address")
	httpAddr := flag.String("http-addr", "localhost:50052", "HTTP data-plane listen address")
	configPath := flag.String("config", "", "path to the component config JSON (required)")
	bucket := flag.String("bucket", "raw", "default output bucket name")
	writeMode := flag.String("write-mode", "FULL_LOAD", "default write mode")
	flag.Parse()

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "usage: mock-datagateway -config <component-config.json> [-bucket raw]")
		os.Exit(2)
	}
	cfgBytes, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("read config: %v", err)
	}

	// When the config declares a fields projection, enforce exactly those
	// column names (in order) on every received batch.
	var cfgFields struct {
		Fields []struct {
			Name string `json:"name"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(cfgBytes, &cfgFields); err != nil {
		log.Fatalf("parse config: %v", err)
	}
	expectCols := make([]string, 0, len(cfgFields.Fields))
	for _, f := range cfgFields.Fields {
		expectCols = append(expectCols, f.Name)
	}

	g := &mockGateway{
		cfgBytes:   cfgBytes,
		bucket:     *bucket,
		writeMode:  *writeMode,
		httpBase:   "http://" + *httpAddr,
		writers:    map[string]*writerState{},
		expectCols: expectCols,
	}
	if len(expectCols) > 0 {
		log.Printf("mock-dg: enforcing projected columns %v (all utf8)", expectCols)
	} else {
		log.Printf("mock-dg: enforcing all-utf8 columns (no fields projection in config)")
	}

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("grpc listen: %v", err)
	}
	gs := grpc.NewServer()
	pb.RegisterDataGatewayServer(gs, g)
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()
	log.Printf("mock-dg: gRPC on %s, HTTP data plane on %s (config=%s bucket=%s)", *grpcAddr, *httpAddr, *configPath, *bucket)

	http.HandleFunc("/data/write/", g.handleHTTPWrite)
	if err := http.ListenAndServe(*httpAddr, nil); err != nil {
		log.Fatalf("http serve: %v", err)
	}
}
