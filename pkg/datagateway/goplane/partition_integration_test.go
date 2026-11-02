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
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/datuplet/datuplet/pkg/datagateway"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// partitionEvent is one row of eventSchema for the server: name "" is null.
type partitionEvent struct {
	id   int64
	name string
	day  int // days after 2026-01-01
}

func partitionBatch(rows []partitionEvent) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.NewGoAllocator(), eventSchema)
	defer b.Release()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	for _, r := range rows {
		b.Field(0).(*array.Int64Builder).Append(r.id)
		if r.name == "" {
			b.Field(1).AppendNull()
		} else {
			b.Field(1).(*array.StringBuilder).Append(r.name)
		}
		b.Field(2).(*array.TimestampBuilder).Append(arrow.Timestamp(base + int64(r.day)*dayMicros + r.id))
	}
	return b.NewRecordBatch()
}

// startPartitionServer boots a real ServerV2 on the Go plane whose output
// table ns.tbl is configured with partition fields.
func startPartitionServer(t *testing.T, fx *fixture, runID, ns, tbl string, fields []datagateway.PartitionFieldConfig) *datagateway.ServerV2 {
	t.Helper()
	iss := newRunTokenIssuer(t)
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte(iss.mint(t, runID, fx.projectID, fx.warehouseName)), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RUN_ID", runID)
	httpAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	srv, err := datagateway.NewServerV2(&datagateway.Config{
		RunID:              runID,
		ComponentName:      "gw-it",
		DefaultBucket:      ns,
		DefaultWriteMode:   "APPEND",
		OutputTables:       []datagateway.OutputTableConfig{{Name: tbl, Bucket: ns, WriteMode: "APPEND", PartitionFields: fields}},
		LakekeeperURL:      fx.catalogURL(),
		PipelineAPIJWKSURL: iss.jwks.URL,
		RunTokenPath:       tokenPath,
		HTTPAddr:           httpAddr,
	}, Factory)
	if err != nil {
		t.Fatalf("NewServerV2: %v", err)
	}
	served := make(chan error, 1)
	go func() { served <- srv.ServeWithHTTP(fmt.Sprintf("127.0.0.1:%d", freePort(t))) }()
	t.Cleanup(func() {
		srv.Close()
		<-served
	})
	waitHTTP(context.Background(), t, "http://"+httpAddr+"/health")
	return srv
}

// serverAppend writes batches through one server writer and commits.
func serverAppend(t *testing.T, srv *datagateway.ServerV2, ns, tbl string, batches ...[]partitionEvent) int32 {
	t.Helper()
	ctx := context.Background()
	open, err := srv.OpenWriter(ctx, &pb.OpenWriterRequest{Bucket: ns, Table: tbl, InputFormat: pb.DataFormat_FORMAT_ARROW_IPC})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	hc := &http.Client{Timeout: 2 * time.Minute}
	for i, rows := range batches {
		rec := partitionBatch(rows)
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
	if err != nil {
		t.Fatalf("CloseWriter: %v", err)
	}
	if commit, err := srv.Commit(ctx, &pb.CommitRequest{}); err != nil || !commit.Success {
		t.Fatalf("Commit = %+v, %v", commit, err)
	}
	return closed.FilesWritten
}

// lkScan plans and reads ns.tbl with filter (nil: all rows).
func lkScan(t *testing.T, tb *table.Table, filter iceberg.BooleanExpression) (int, []int64) {
	t.Helper()
	opts := []table.ScanOption{}
	if filter != nil {
		opts = append(opts, table.WithRowFilter(filter))
	}
	sc := tb.Scan(opts...)
	tasks, err := sc.PlanFiles(context.Background())
	if err != nil {
		t.Fatalf("PlanFiles: %v", err)
	}
	arrTbl, err := sc.ToArrowTable(context.Background())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer arrTbl.Release()
	var ids []int64
	for _, ch := range arrTbl.Column(0).Data().Chunks() {
		a := ch.(*array.Int64)
		for i := 0; i < a.Len(); i++ {
			ids = append(ids, a.Value(i))
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return len(tasks), ids
}

// TestIntegrationServerPartitionedWrite: a real server creates a new
// identity(name) + day(ts) table in Lakekeeper, writes and commits it twice
// (APPEND), an iceberg-go row-filter scan prunes files and returns only the
// matching rows, a null-partition row round-trips, and the gateway's own
// read returns every row.
func TestIntegrationServerPartitionedWrite(t *testing.T) {
	fx := sharedFixture(t)
	const ns, tbl = "gw_part", "events"
	srv := startPartitionServer(t, fx, "0123456789abcdef-server-part", ns, tbl, []datagateway.PartitionFieldConfig{
		{SourceColumn: "name", Transform: "identity"},
		{SourceColumn: "ts", Transform: "day"},
	})

	first := [][]partitionEvent{
		{{0, "a", 0}, {1, "b", 0}, {2, "a", 1}, {3, "", 0}},
		{{4, "b", 1}, {5, "a", 0}, {6, "c", 2}},
	}
	files := serverAppend(t, srv, ns, tbl, first...)
	if files != 6 { // (a,0) (b,0) (a,1) (null,0) (b,1) (c,2)
		t.Fatalf("first run wrote %d files, want 6 (one per tuple)", files)
	}
	serverAppend(t, srv, ns, tbl, []partitionEvent{{7, "a", 0}, {8, "", 2}})

	cat, err := resolver(t, fx).Catalog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tb, err := cat.LoadTable(context.Background(), catalog.ToIdentifier(ns, tbl))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	spec := tb.Spec()
	if spec.NumFields() != 2 || spec.Field(0).Name != "name" || spec.Field(1).Name != "ts_day" {
		t.Fatalf("table spec = %v, want identity(name), day(ts)", spec)
	}
	if n := len(tb.Metadata().Snapshots()); n != 2 {
		t.Errorf("snapshots = %d, want 2 (two APPENDs)", n)
	}

	all, allIDs := lkScan(t, tb, nil)
	if fmt.Sprint(allIDs) != "[0 1 2 3 4 5 6 7 8]" {
		t.Fatalf("all ids %v", allIDs)
	}
	n, ids := lkScan(t, tb, iceberg.EqualTo(iceberg.Reference("name"), "a"))
	if n >= all || fmt.Sprint(ids) != "[0 2 5 7]" {
		t.Fatalf("name=a planned %d of %d files, ids %v; want fewer files and [0 2 5 7]", n, all, ids)
	}
	t.Logf("row-filter scan planned %d of %d files", n, all)
	n, ids = lkScan(t, tb, iceberg.IsNull(iceberg.Reference("name")))
	if fmt.Sprint(ids) != "[3 8]" || n != 2 {
		t.Fatalf("IsNull(name) planned %d files, ids %v; want 2 files and [3 8]", n, ids)
	}
	tasks, err := tb.Scan().PlanFiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		if p := task.File.FilePath(); !strings.Contains(p, "/data/name=") || !strings.Contains(p, "/ts_day=2026-01-0") {
			t.Errorf("data file %s is not under its partition directory", p)
		}
	}

	// The gateway's read returns every row (D12: reads unchanged).
	ctx := context.Background()
	open, err := srv.OpenReader(ctx, &pb.OpenReaderRequest{Bucket: ns, Table: tbl, OutputFormat: pb.DataFormat_FORMAT_PARQUET})
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer srv.CloseReader(ctx, &pb.CloseReaderRequest{ReaderId: open.ReaderId})
	hc := &http.Client{Timeout: 2 * time.Minute}
	dir := t.TempDir()
	read := map[int64]int{}
	for i := 0; ; i++ {
		if i > 100 {
			t.Fatal("no last chunk after 100 GETs")
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
		if len(body) > 0 {
			path := filepath.Join(dir, fmt.Sprintf("chunk-%04d.parquet", i))
			if err := os.WriteFile(path, body, 0o600); err != nil {
				t.Fatal(err)
			}
			readStagedChunk(t, path, read)
		}
		if resp.Header.Get("X-Datuplet-Is-Last") == "true" {
			break
		}
	}
	for id := int64(0); id <= 8; id++ {
		if read[id] != 1 {
			t.Fatalf("gateway read: id %d read %d times, want once (%v)", id, read[id], read)
		}
	}
}

// TestIntegrationServerPartitionTypePairError: an existing long column
// partitioned by identity, written as a string column, fails the first chunk
// with gRPC InvalidArgument and HTTP 400 (D6 at the RPC layer); nothing is
// committed.
func TestIntegrationServerPartitionTypePairError(t *testing.T) {
	fx := sharedFixture(t)
	const ns, tbl = "gw_part", "typed"
	ctx := context.Background()
	cat, err := resolver(t, fx).Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateNamespace(ctx, catalog.ToIdentifier(ns), nil); err != nil && !errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
		t.Fatal(err)
	}
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String})
	fid := iceberg.PartitionDataIDStart
	spec, err := iceberg.NewPartitionSpecOpts(iceberg.AddPartitionFieldByName("id", "id", iceberg.IdentityTransform{}, sc, &fid))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.CreateTable(ctx, catalog.ToIdentifier(ns, tbl), sc, catalog.WithPartitionSpec(&spec)); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	srv := startPartitionServer(t, fx, "0123456789abcdef-server-pair", ns, tbl, nil)

	open, err := srv.OpenWriter(ctx, &pb.OpenWriterRequest{Bucket: ns, Table: tbl, InputFormat: pb.DataFormat_FORMAT_CSV,
		Schema: &pb.Schema{Columns: []*pb.ColumnDef{{Name: "id", Type: "string", Nullable: true}, {Name: "name", Type: "string", Nullable: true}}}})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	_, err = srv.WriteChunk(ctx, &pb.WriteChunkRequest{WriterId: open.WriterId, Data: []byte("id,name\nx,a\n")})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "cannot be converted to the table type long") {
		t.Fatalf("gRPC WriteChunk err = %v (code %s), want InvalidArgument naming the type pair", err, status.Code(err))
	}
	resp, err := http.Post(open.HttpEndpoint, "text/csv", strings.NewReader("id,name\nx,a\n"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "INVALID_ARGUMENT") {
		t.Fatalf("HTTP = %d %s, want 400 INVALID_ARGUMENT", resp.StatusCode, body)
	}
	tb, err := cat.LoadTable(ctx, catalog.ToIdentifier(ns, tbl))
	if err != nil || tb.CurrentSnapshot() != nil {
		t.Fatalf("table after the rejected writes: snapshot %v, err %v; want none", tb.CurrentSnapshot(), err)
	}
}
