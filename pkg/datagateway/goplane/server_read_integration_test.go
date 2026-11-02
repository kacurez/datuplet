//go:build integration

package goplane

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/datuplet/datuplet/pkg/datagateway"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

const (
	serverReadPerChunk = 10000
	serverReadChunks   = 5
	serverReadDeleted  = int64(5) // the one id removed by a merge-on-read delete
)

var serverReadSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
}, nil)

// startReadServer boots a real ServerV2 on the Go plane (run token validated
// against a JWKS) serving gRPC and HTTP; it returns the server and its gRPC
// address.
func startReadServer(t *testing.T, fx *fixture, runID, ns string) (*datagateway.ServerV2, string) {
	t.Helper()
	iss := newRunTokenIssuer(t)
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte(iss.mint(t, runID, fx.projectID, fx.warehouseName)), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RUN_ID", runID)

	httpAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	grpcAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	srv, err := datagateway.NewServerV2(&datagateway.Config{
		RunID:              runID,
		ComponentName:      "gw-it",
		DefaultBucket:      ns,
		DefaultWriteMode:   "APPEND",
		LakekeeperURL:      fx.catalogURL(),
		PipelineAPIJWKSURL: iss.jwks.URL,
		RunTokenPath:       tokenPath,
		HTTPAddr:           httpAddr,
		TargetFileSize:     64 << 10, // rotation at 256 KiB fed → several files
	}, Factory)
	if err != nil {
		t.Fatalf("NewServerV2: %v", err)
	}
	served := make(chan error, 1)
	go func() { served <- srv.ServeWithHTTP(grpcAddr) }()
	t.Cleanup(func() {
		srv.Close()
		<-served
	})
	waitHTTP(context.Background(), t, "http://"+httpAddr+"/health")
	return srv, grpcAddr
}

// writeServerReadTable creates ns.tbl as a merge-on-read v2 table with full
// metrics, writes serverReadChunks×serverReadPerChunk rows through the
// server into several files, commits, deletes one row of the first file and
// checks that exactly one planned task carries delete files. It returns the
// number of rows a read must see.
func writeServerReadTable(t *testing.T, fx *fixture, srv *datagateway.ServerV2, ns, tbl string) int64 {
	t.Helper()
	ctx := context.Background()
	r := resolver(t, fx)
	createReadTableWith(t, r, ns, tbl, serverReadSchema)

	open, err := srv.OpenWriter(ctx, &pb.OpenWriterRequest{Bucket: ns, Table: tbl, InputFormat: pb.DataFormat_FORMAT_ARROW_IPC})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	hc := &http.Client{Timeout: 2 * time.Minute}
	for i := 0; i < serverReadChunks; i++ {
		rec := serverTestBatch(t, serverReadSchema, i*serverReadPerChunk, serverReadPerChunk)
		body := serverIPCBytes(t, rec)
		rec.Release()
		resp, err := hc.Post(open.HttpEndpoint, "application/vnd.apache.arrow.stream", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST chunk %d: %v", i, err)
		}
		msg, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("POST chunk %d: HTTP %d: %s", i, resp.StatusCode, msg)
		}
	}
	closed, err := srv.CloseWriter(ctx, &pb.CloseWriterRequest{WriterId: open.WriterId})
	if err != nil || closed.FilesWritten < 2 {
		t.Fatalf("CloseWriter = %+v, %v; want several files", closed, err)
	}
	if commit, err := srv.Commit(ctx, &pb.CommitRequest{}); err != nil || !commit.Success {
		t.Fatalf("Commit = %+v, %v", commit, err)
	}

	lkDelete(t, r, ns, tbl, iceberg.EqualTo(iceberg.Reference("id"), serverReadDeleted))
	tasks, err := loadLK(t, r, ns, tbl).Scan().PlanFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	withDeletes := 0
	for _, task := range tasks {
		if hasDeletes(task) {
			withDeletes++
		}
	}
	if len(tasks) != int(closed.FilesWritten) || withDeletes != 1 {
		t.Fatalf("planned %d tasks (%d with deletes); want %d tasks, exactly one with deletes", len(tasks), withDeletes, closed.FilesWritten)
	}
	t.Logf("table %s.%s: %d files, one with a position delete", ns, tbl, len(tasks))
	return serverReadChunks*serverReadPerChunk - 1
}

// checkIDs fails unless ids holds every written id but the deleted one,
// each once.
func checkIDs(t *testing.T, ids map[int64]int, want int64) {
	t.Helper()
	if int64(len(ids)) != want {
		t.Errorf("distinct ids = %d, want %d", len(ids), want)
	}
	for id := int64(0); id < serverReadChunks*serverReadPerChunk; id++ {
		n := ids[id]
		if id == serverReadDeleted && n != 0 {
			t.Errorf("deleted id %d read back", id)
		}
		if id != serverReadDeleted && n != 1 {
			t.Fatalf("id %d read %d times, want once", id, n)
		}
	}
}

// Review Focus 1/2 end to end: a real server reads a table with clean and
// delete-carrying files as FORMAT_PARQUET over HTTP. Every chunk (raw or
// decoded) is a standalone Parquet file with the same Arrow schema, and the
// rows are the written rows minus the deleted one.
func TestIntegrationServerReadParquetEndToEnd(t *testing.T) {
	fx := sharedFixture(t)
	const ns, tbl = "gw_server_read", "parquet"
	srv, _ := startReadServer(t, fx, "0123456789abcdef-server-read-pq", ns)
	want := writeServerReadTable(t, fx, srv, ns, tbl)

	ctx := context.Background()
	open, err := srv.OpenReader(ctx, &pb.OpenReaderRequest{Bucket: ns, Table: tbl, OutputFormat: pb.DataFormat_FORMAT_PARQUET})
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer srv.CloseReader(ctx, &pb.CloseReaderRequest{ReaderId: open.ReaderId})

	hc := &http.Client{Timeout: 2 * time.Minute}
	dir := t.TempDir()
	var first *arrow.Schema
	var rows int64
	ids := map[int64]int{}
	for i := 0; ; i++ {
		if i > 1000 {
			t.Fatal("no last chunk after 1000 GETs")
		}
		resp, err := hc.Get(open.HttpEndpoint)
		if err != nil {
			t.Fatalf("GET chunk %d: %v", i, err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("GET chunk %d: HTTP %d, %v: %s", i, resp.StatusCode, err, body)
		}
		if cl := resp.Header.Get("Content-Length"); cl != strconv.Itoa(len(body)) {
			t.Errorf("chunk %d: Content-Length %q, body %d bytes", i, cl, len(body))
		}
		hdrRows, _ := strconv.ParseInt(resp.Header.Get("X-Datuplet-Rows"), 10, 64)
		last := resp.Header.Get("X-Datuplet-Is-Last") == "true"

		path := filepath.Join(dir, fmt.Sprintf("chunk-%04d.parquet", i))
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
		sch, n := readStagedChunk(t, path, ids)
		if n != hdrRows {
			t.Errorf("chunk %d: file rows %d, X-Datuplet-Rows %d", i, n, hdrRows)
		}
		if first == nil {
			first = sch
		} else if !sameNamesAndTypes(sch, first) {
			t.Errorf("chunk %d schema %s differs from chunk 0's %s", i, sch, first)
		}
		rows += n
		if last {
			t.Logf("read %d chunks", i+1)
			break
		}
	}
	if rows != want {
		t.Errorf("rows read = %d, want %d (written − deleted)", rows, want)
	}
	checkIDs(t, ids, want)
}

// readStagedChunk reads a staged chunk file and counts its ids into ids.
func readStagedChunk(t *testing.T, path string, ids map[int64]int) (*arrow.Schema, int64) {
	t.Helper()
	pf, err := file.OpenParquetFile(path, false)
	if err != nil {
		t.Fatalf("%s is not a Parquet file: %v", path, err)
	}
	defer pf.Close()
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := fr.ReadTable(context.Background())
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	defer tbl.Release()
	for _, c := range tbl.Column(0).Data().Chunks() {
		arr := c.(*array.Int64)
		for i := 0; i < arr.Len(); i++ {
			ids[arr.Value(i)]++
		}
	}
	return tbl.Schema(), tbl.NumRows()
}

// expectedSDKWire maps an advertised proto schema the way the frozen SDK's
// protoSchemaToArrow (sdk/go/arrow/reader.go:132) does, for the types this
// table has; datagateway's sdkWireSchema is pinned to the SDK by
// TestSDKWireSchemaMatchesSDK.
func expectedSDKWire(t *testing.T, s *pb.Schema) *arrow.Schema {
	t.Helper()
	fields := make([]arrow.Field, len(s.Columns))
	for i, c := range s.Columns {
		var typ arrow.DataType
		switch c.Type {
		case "int64":
			typ = arrow.PrimitiveTypes.Int64
		case "string":
			typ = arrow.BinaryTypes.String
		case "timestamp":
			typ = arrow.FixedWidthTypes.Timestamp_us
		default:
			t.Fatalf("column %q: unexpected advertised type %q", c.Name, c.Type)
		}
		fields[i] = arrow.Field{Name: c.Name, Type: typ, Nullable: c.Nullable}
	}
	return arrow.NewSchema(fields, nil)
}

// Review Focus 2 end to end: the same kind of table read over real gRPC as
// FORMAT_ARROW_IPC; every chunk decodes to exactly one batch whose schema is
// the SDK wire schema of the advertised proto schema.
func TestIntegrationServerReadArrowIPCWithSDKWire(t *testing.T) {
	fx := sharedFixture(t)
	const ns, tbl = "gw_server_read", "ipc"
	srv, grpcAddr := startReadServer(t, fx, "0123456789abcdef-server-read-ipc", ns)
	want := writeServerReadTable(t, fx, srv, ns, tbl)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(256<<20)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewDataGatewayClient(conn)

	open, err := client.OpenReader(ctx, &pb.OpenReaderRequest{Bucket: ns, Table: tbl, OutputFormat: pb.DataFormat_FORMAT_ARROW_IPC})
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer client.CloseReader(context.Background(), &pb.CloseReaderRequest{ReaderId: open.ReaderId})
	wire := expectedSDKWire(t, open.Schema)

	stream, err := client.ReadChunk(ctx, &pb.ReadChunkRequest{ReaderId: open.ReaderId})
	if err != nil {
		t.Fatal(err)
	}
	var rows int64
	var chunks int
	sawLast := false
	ids := map[int64]int{}
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv chunk %d: %v", chunks, err)
		}
		if sawLast {
			t.Fatal("chunk after the last chunk")
		}
		sawLast = chunk.IsLast
		r, err := ipc.NewReader(bytes.NewReader(chunk.Data))
		if err != nil {
			t.Fatalf("chunk %d is not an IPC stream: %v", chunks, err)
		}
		if !r.Schema().Equal(wire) {
			t.Errorf("chunk %d schema %s, want the SDK wire schema %s", chunks, r.Schema(), wire)
		}
		batches := 0
		for r.Next() {
			batches++
			rec := r.RecordBatch()
			if rec.NumRows() != chunk.RowsInChunk {
				t.Errorf("chunk %d: batch rows %d, RowsInChunk %d", chunks, rec.NumRows(), chunk.RowsInChunk)
			}
			idCol := rec.Column(0).(*array.Int64)
			for i := 0; i < idCol.Len(); i++ {
				ids[idCol.Value(i)]++
			}
			rows += rec.NumRows()
		}
		if r.Err() != nil {
			t.Fatal(r.Err())
		}
		r.Release()
		if batches != 1 {
			t.Errorf("chunk %d holds %d batches, want exactly 1", chunks, batches)
		}
		chunks++
	}
	if !sawLast {
		t.Error("no chunk marked last")
	}
	t.Logf("read %d IPC chunks", chunks)
	if rows != want {
		t.Errorf("rows read = %d, want %d (written − deleted)", rows, want)
	}
	checkIDs(t, ids, want)
}
