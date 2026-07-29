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
// The component config file (-config) may optionally carry a top-level
// "output_columns": [{"name":..., "type":...}] array — a mock-only
// convenience for exercising the extractor's DECLARED output-column-mapping
// mode locally. When present, GetConfig serves it back as a single
// OutputConfig.Tables entry (mirroring outputs.tables[].columns from a real
// pipeline doc) for the table the mock resolves the same way the component
// does: table_name > array_path > "data" — AND every payload written to
// that table is validated against it (validateAgainstMapping): names,
// types, and order must match exactly, so a broken extractor emitting the
// wrong schema fails the local run instead of silently passing.
//
// Dev/test tool only — NOT a deployment surface.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	schema        string   // "name:type[?], ..." label of the first payload (logging)
	schemaFields  []string // first payload's per-column "name:type[?]" signature (name+type+nullability) — later payloads must match exactly
}

type mockGateway struct {
	pb.UnimplementedDataGatewayServer

	mu           sync.Mutex
	cfgBytes     []byte
	bucket       string
	writeMode    string
	httpBase     string
	nextID       int
	writers      map[string]*writerState
	expectCols   []string                // from the config's fields[].name; empty = names not enforced
	outputTables []*pb.TableOutputConfig // from the config's optional output_columns; empty = no declared mapping served (GetConfig omits OutputConfig.Tables)
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
			Tables:           g.outputTables,
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

// mockArrowTypeFor mirrors sdk/go/arrow.ArrowTypeFor's canonical
// config-type-string -> Arrow-type vocabulary (int/long -> Int64,
// float/double -> Float64, boolean -> Boolean, string -> String), so
// validateAgainstMapping can check a declared output_columns mapping's
// types against what a real writer actually emits. Duplicated rather than
// imported: this mock deliberately depends on neither sdk/go nor
// sdk/go/arrow (same reasoning as resolveOutputTable's existing
// duplication in main(), below — the mock and the SDK/component are
// separate Go modules). An unrecognized string returns (nil, false).
func mockArrowTypeFor(t string) (arrow.DataType, bool) {
	switch t {
	case "int", "long":
		return arrow.PrimitiveTypes.Int64, true
	case "float", "double":
		return arrow.PrimitiveTypes.Float64, true
	case "boolean":
		return arrow.FixedWidthTypes.Boolean, true
	case "string":
		return arrow.BinaryTypes.String, true
	default:
		return nil, false
	}
}

// declaredColumnsFor returns g.outputTables' Columns for table, or nil when
// no declared output_columns mapping was configured for it. g.outputTables
// is fixed once at startup and never mutated afterward, so no lock is
// needed for this read.
func (g *mockGateway) declaredColumnsFor(table string) []*pb.ColumnConfig {
	for _, t := range g.outputTables {
		if t.Name == table {
			return t.Columns
		}
	}
	return nil
}

// validateAgainstMapping enforces a declared output_columns mapping
// EXACTLY: the same number of columns, the same names IN ORDER, and the
// same Arrow type (via mockArrowTypeFor) at each position. A positional
// mismatch report doubles as the order check — a column swapped out of
// place is caught as "column i is X, want Y" at the first position where
// declared and emitted names diverge, without a separate order pass. Every
// failure names the position/column and what was actually declared, so the
// message is actionable on its own.
func validateAgainstMapping(sch *arrow.Schema, mapping []*pb.ColumnConfig) error {
	if sch.NumFields() != len(mapping) {
		names := make([]string, len(mapping))
		for i, c := range mapping {
			names[i] = c.Name
		}
		return fmt.Errorf("schema violation: declared-columns mismatch: %d columns, want %d %v", sch.NumFields(), len(mapping), names)
	}
	for i, want := range mapping {
		got := sch.Field(i)
		if got.Name != want.Name {
			return fmt.Errorf("schema violation: declared-columns mismatch: column %d is %q, want %q (declared order)", i, got.Name, want.Name)
		}
		wantType, ok := mockArrowTypeFor(want.Type)
		if !ok {
			// A bug in the fixture's own output_columns, not the component's
			// output — still fail loudly rather than silently accept anything.
			return fmt.Errorf("schema violation: declared column %q has unrecognized config type %q (want one of int, long, float, double, boolean, string)", want.Name, want.Type)
		}
		if got.Type.ID() != wantType.ID() {
			return fmt.Errorf("schema violation: declared-columns mismatch: column %q is %s, want %s (declared type %q)", want.Name, got.Type, wantType, want.Type)
		}
	}
	return nil
}

// validateSchema enforces the extractor's output contract on every payload:
// every column must be one of the sink type vocabulary's four Arrow types
// (utf8, int64, float64, bool — dgarrow.ArrowTypeFor's canonical mapping;
// StringSink emits utf8 only, InferringSink emits any mix of the four) and
// Nullable:true always (both extractor sinks guarantee every field
// nullable, typed or not). A violation fails the write (HTTP 500 / gRPC
// error), so the local proof cannot silently pass with wrong output.
//
// Column-SET enforcement has two sources, and they are mutually exclusive
// per table:
//
//   - A declared output_columns mapping for table (g.declaredColumnsFor)
//     wins outright when present: the emitted schema must match it EXACTLY
//     (validateAgainstMapping) — names, types, AND order — regardless of
//     whether the config also has a `fields` projection. This mirrors the
//     real component: once a declared outputs.tables[].columns mapping
//     applies to a table, `fields` (when also set) only reshapes the
//     record BEFORE the declared (WithTypedColumns) sink types it — the
//     wire schema is always exactly the mapping's names/types/order, never
//     `fields`' own output names. So g.expectCols (sourced from
//     `fields[].name`) is irrelevant whenever a mapping matches this table
//     and is deliberately NOT consulted in that case (see
//     TestIngest_DeclaredColumnsMapping_OverridesExpectCols).
//   - Otherwise (no declared mapping for table), g.expectCols — when set —
//     is enforced: same names, in order. Type is NOT checked here: the
//     whole point of inference mode (no mapping) is that the type is
//     inferred from the data, not declared up front.
func (g *mockGateway) validateSchema(sch *arrow.Schema, table string) error {
	for _, f := range sch.Fields() {
		switch f.Type.ID() {
		case arrow.STRING, arrow.INT64, arrow.FLOAT64, arrow.BOOL:
			// allowed — the full sink type vocabulary.
		default:
			return fmt.Errorf("schema violation: column %q is %s, want one of utf8/int64/float64/bool (the sink type vocabulary)", f.Name, f.Type)
		}
		if !f.Nullable {
			return fmt.Errorf("schema violation: column %q is not nullable, want Nullable:true (every sink column is nullable)", f.Name)
		}
	}

	if mapping := g.declaredColumnsFor(table); len(mapping) > 0 {
		return validateAgainstMapping(sch, mapping)
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

// ingest parses one payload as a complete, standalone Arrow IPC stream —
// exactly one schema message + record batch(es) + EOS, with nothing after
// it — validates its schema against the contract, counts its rows, and
// records the schema on first sight.
//
// Two defects this must catch (both silent data loss in the real gateway if
// unnoticed): (1) a payload that is a valid IPC stream followed by MORE
// bytes (a second concatenated stream, or garbage) — the ipc reader treats
// its own EOS marker as a clean end-of-input and never looks past it, so
// this can only be caught by explicitly checking the underlying reader for
// leftover bytes; (2) a payload that parses fine but carries zero rows —
// the sink is designed to never flush an empty batch (flush() returns early
// at inBatch == 0), so a zero-row POST always indicates a caller bug, never
// legitimate output.
//
// The writer lookup happens FIRST, before any IPC parsing, for two reasons:
// there is no point validating a payload for a writer ID that does not
// exist, and validateSchema needs ws.table to look up a declared
// output_columns mapping (declaredColumnsFor). g.writers entries are never
// removed once added (no delete call anywhere in this file), so the *ws
// pointer obtained here stays valid for the rest of this call without
// re-fetching it under the later lock.
func (g *mockGateway) ingest(writerID string, data []byte) (int64, error) {
	g.mu.Lock()
	ws, ok := g.writers[writerID]
	g.mu.Unlock()
	if !ok {
		return 0, fmt.Errorf("unknown writer: %s", writerID)
	}

	br := bytes.NewReader(data)
	rd, err := ipc.NewReader(br)
	if err != nil {
		return 0, fmt.Errorf("payload is not a valid Arrow IPC stream: %w", err)
	}
	defer rd.Release()
	if err := g.validateSchema(rd.Schema(), ws.table); err != nil {
		return 0, err
	}
	var rows int64
	for rd.Next() {
		rows += rd.Record().NumRows()
	}
	if rd.Err() != nil {
		return 0, fmt.Errorf("ipc read: %w", rd.Err())
	}
	// The ipc reader stops exactly at the stream's EOS marker; anything br
	// still has unread past that point is trailing data — a second
	// concatenated IPC stream or garbage — which must fail the write rather
	// than silently counting only the first stream's rows (this is the
	// exact concatenated-stream defect the mock exists to catch; see
	// sdk/go/client.go's OpenWriterToBucket comment on the gen-big-pipeline
	// regression, and the equivalent check in sdk/go/arrow/sink_test.go's
	// ipcRows helper).
	if _, err := br.ReadByte(); err == nil {
		return 0, fmt.Errorf("payload is not a single self-contained Arrow IPC stream: %d trailing byte(s) after EOS (concatenated streams are invalid — exactly one IPC stream per POST)", br.Len()+1)
	} else if !errors.Is(err, io.EOF) {
		return 0, fmt.Errorf("checking payload for trailing bytes: %w", err)
	}
	if rows == 0 {
		return 0, fmt.Errorf("payload carries zero rows (empty IPC stream or zero-row record batch) — the sink never flushes an empty batch, so this indicates a component bug")
	}

	// labeled is this payload's full per-column signature — name, type, and
	// nullability — used both for the first-sight schema log line and as
	// the per-writer drift lock (below): later payloads on the same writer
	// must match it exactly, not just column names.
	labeled := make([]string, 0, rd.Schema().NumFields())
	for _, f := range rd.Schema().Fields() {
		label := f.Name + ":" + f.Type.String()
		if f.Nullable {
			label += "?"
		}
		labeled = append(labeled, label)
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if ws.schemaFields == nil {
		// First payload for this writer: lock its schema. Later payloads
		// must match exactly (name, type, AND nullability) — catches
		// cross-batch schema drift even for no-`fields` configs (where
		// expectCols is empty).
		ws.schemaFields = labeled
		ws.schema = strings.Join(labeled, ", ")
		log.Printf("mock-dg: writer %s schema: [%s]", writerID, ws.schema)
	} else if !slices.Equal(ws.schemaFields, labeled) {
		return 0, fmt.Errorf("schema violation: writer %s column drift: first [%s], now [%s]", writerID, strings.Join(ws.schemaFields, ", "), strings.Join(labeled, ", "))
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
	// column names (in order) on every received batch. table_name/array_path
	// are read so a declared output_columns mapping (mock-only convenience;
	// see the package doc) can be served for the SAME table the component
	// itself resolves to — table_name > array_path > "data".
	var cfgFields struct {
		TableName string `json:"table_name"`
		ArrayPath string `json:"array_path"`
		Fields    []struct {
			Name string `json:"name"`
		} `json:"fields"`
		OutputColumns []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"output_columns"`
	}
	if err := json.Unmarshal(cfgBytes, &cfgFields); err != nil {
		log.Fatalf("parse config: %v", err)
	}
	expectCols := make([]string, 0, len(cfgFields.Fields))
	for _, f := range cfgFields.Fields {
		expectCols = append(expectCols, f.Name)
	}

	// resolveOutputTable mirrors the component's own precedence
	// (components/http-json-extractor/main.go's resolveOutputTable) so a
	// declared output_columns mapping is served under the same table name
	// the component resolves and opens a writer for.
	outputTable := cfgFields.TableName
	if outputTable == "" {
		outputTable = cfgFields.ArrayPath
	}
	if outputTable == "" {
		outputTable = "data"
	}

	var outputTables []*pb.TableOutputConfig
	if len(cfgFields.OutputColumns) > 0 {
		cols := make([]*pb.ColumnConfig, len(cfgFields.OutputColumns))
		for i, c := range cfgFields.OutputColumns {
			cols[i] = &pb.ColumnConfig{Name: c.Name, Type: c.Type}
		}
		outputTables = []*pb.TableOutputConfig{
			{Name: outputTable, Bucket: *bucket, WriteMode: *writeMode, Columns: cols},
		}
	}

	g := &mockGateway{
		cfgBytes:     cfgBytes,
		bucket:       *bucket,
		writeMode:    *writeMode,
		httpBase:     "http://" + *httpAddr,
		writers:      map[string]*writerState{},
		expectCols:   expectCols,
		outputTables: outputTables,
	}
	if len(outputTables) > 0 {
		log.Printf("mock-dg: serving declared output_columns mapping for table %q: %+v", outputTable, cfgFields.OutputColumns)
	}
	if len(expectCols) > 0 {
		log.Printf("mock-dg: enforcing projected columns %v", expectCols)
	} else {
		log.Printf("mock-dg: no fields projection in config — column set not enforced by name")
	}
	log.Printf("mock-dg: accepting sink type vocabulary (utf8/int64/float64/bool, all Nullable:true)")

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
