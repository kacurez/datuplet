package datagateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"

	dglakekeeper "github.com/datuplet/datuplet/pkg/datagateway/lakekeeper"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
	"github.com/datuplet/datuplet/pkg/icebergjob"
)

// newWriteTestServer builds a ServerV2 on a fresh fake data plane. Without a
// LakekeeperURL, writers get local data prefixes under LocalDataDir.
func newWriteTestServer(t *testing.T, cfg *Config) (*ServerV2, *fakeDataPlane) {
	t.Helper()
	if cfg.RunID == "" {
		cfg.RunID = "0123456789abcdef"
	}
	if cfg.LakekeeperURL == "" && cfg.LocalDataDir == "" {
		cfg.LocalDataDir = t.TempDir()
	}
	p := newFakeDataPlane()
	s, err := NewServerV2(cfg, fakeFactory(p))
	if err != nil {
		t.Fatalf("NewServerV2: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, p
}

// fakeResolver records the schema every table create is called with.
type fakeResolver struct {
	mu      sync.Mutex
	creates []*schema.Schema // one per LoadOrCreateForWrite that created
	calls   int

	// When set, a create (schema provider given) closes entered and then
	// blocks until release is closed.
	entered, release chan struct{}
}

func (r *fakeResolver) LoadOrCreateForWrite(ctx context.Context, ns, tbl string, sp dglakekeeper.SchemaProvider, _ []dglakekeeper.PartitionField) (*dglakekeeper.WriterTarget, error) {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	if sp == nil {
		return nil, fmt.Errorf("lakekeeper: table %s.%s missing and no schema available to create it", ns, tbl)
	}
	if r.release != nil {
		close(r.entered)
		<-r.release
	}
	sch, err := sp(ctx)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.creates = append(r.creates, sch)
	r.mu.Unlock()
	return &dglakekeeper.WriterTarget{BasePath: "s3://warehouse/" + ns + "/" + tbl + "/data/"}, nil
}

func (r *fakeResolver) Catalog(context.Context) (catalog.Catalog, error) { return nil, nil }
func (r *fakeResolver) Close() error                                     { return nil }

func (r *fakeResolver) createdColumns() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out [][]string
	for _, s := range r.creates {
		out = append(out, columnNames(s.ArrowSchema()))
	}
	return out
}

func columnNames(s *arrow.Schema) []string {
	names := make([]string, s.NumFields())
	for i, f := range s.Fields() {
		names[i] = f.Name
	}
	return names
}

// ipcPayload serialises one record batch as an Arrow IPC stream.
func ipcPayload(t *testing.T, rec arrow.RecordBatch) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	if err := w.Write(rec); err != nil {
		t.Fatalf("ipc write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("ipc close: %v", err)
	}
	return buf.Bytes()
}

// httpWrite posts body to the HTTP write endpoint of writerID.
func httpWrite(s *ServerV2, writerID string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/data/write/"+writerID, bytes.NewReader(body))
	rec := httptest.NewRecorder()
	s.handleWrite(rec, req)
	return rec
}

func decodeHTTPError(t *testing.T, rec *httptest.ResponseRecorder) httpErrorResponse {
	t.Helper()
	var e httpErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatalf("decode error body %q: %v", rec.Body.String(), err)
	}
	return e
}

// twoColumnBatch builds id:int64 plus a second column of the given type.
func twoColumnBatch(t *testing.T, second arrow.DataType, secondJSON string) arrow.RecordBatch {
	t.Helper()
	mem := memory.NewGoAllocator()
	ids, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int64, strings.NewReader(`[1, 2, 3]`))
	if err != nil {
		t.Fatal(err)
	}
	defer ids.Release()
	other, _, err := array.FromJSON(mem, second, strings.NewReader(secondJSON))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Release()
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "discard", Type: second, Nullable: true},
	}, nil)
	return array.NewRecordBatch(sch, []arrow.Array{ids, other}, 3)
}

func openIPCWriter(t *testing.T, s *ServerV2, table string) string {
	t.Helper()
	resp, err := s.OpenWriter(context.Background(), &pb.OpenWriterRequest{
		Bucket: "raw", Table: table, InputFormat: pb.DataFormat_FORMAT_ARROW_IPC,
	})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	return resp.WriterId
}

// A type the plane would refuse (int8 is not the canonical gateway type of
// its column once a drop pipeline rebuilds the schema) in a DROPPED column is
// accepted, and the plane receives only the retained column. The same type in
// a retained column is rejected with today's BufferManager error text, before
// the plane sees the record.
func TestNonCanonicalTypeInDroppedColumnAccepted(t *testing.T) {
	s, p := newWriteTestServer(t, &Config{
		DefaultBucket: "raw",
		Processors:    []ProcessorConfig{{Type: "drop", Columns: []string{"discard"}}},
	})
	rec := twoColumnBatch(t, arrow.PrimitiveTypes.Int8, `[1, 2, 3]`)
	defer rec.Release()

	id := openIPCWriter(t, s, "t")
	if resp := httpWrite(s, id, ipcPayload(t, rec)); resp.Code != http.StatusOK {
		t.Fatalf("write = %d %s, want 200", resp.Code, resp.Body.String())
	}
	got := p.writers[0].recorded()
	if len(got) != 1 {
		t.Fatalf("plane received %d batches, want 1", len(got))
	}
	if names := columnNames(got[0].Schema()); len(names) != 1 || names[0] != "id" {
		t.Errorf("plane batch columns = %v, want [id]", names)
	}
	if !arrow.TypeEqual(got[0].Schema().Field(0).Type, arrow.PrimitiveTypes.Int64) || got[0].NumRows() != 3 {
		t.Errorf("plane batch = %s rows=%d", got[0].Schema(), got[0].NumRows())
	}
	if names := columnNames(p.writers[0].spec.Schema); len(names) != 1 || names[0] != "id" {
		t.Errorf("writer schema columns = %v, want [id]", names)
	}

	// Retained int8 next to a drop: the post-drop schema is canonical (int32),
	// so the record is rejected as today and the plane gets nothing.
	s2, p2 := newWriteTestServer(t, &Config{
		DefaultBucket: "raw",
		Processors:    []ProcessorConfig{{Type: "drop", Columns: []string{"id"}}},
	})
	id2 := openIPCWriter(t, s2, "t")
	resp := httpWrite(s2, id2, ipcPayload(t, rec))
	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("retained int8 write = %d, want 500", resp.Code)
	}
	if e := decodeHTTPError(t, resp); !strings.Contains(e.Error, "buffer error: record schema does not match buffer manager schema") {
		t.Errorf("error = %q, want today's schema-mismatch text", e.Error)
	}
	for _, w := range p2.writers {
		if n := len(w.recorded()); n != 0 {
			t.Errorf("plane received %d batches for a rejected record", n)
		}
	}
}

// The deferred table create (no component schema) uses the post-drop schema.
func TestWritePathUsesPostDropSchemaForCreate(t *testing.T) {
	s, p := newWriteTestServer(t, &Config{
		DefaultBucket: "raw",
		Processors:    []ProcessorConfig{{Type: "drop", Columns: []string{"secret"}}},
		LocalDataDir:  "unused-with-resolver",
	})
	fr := &fakeResolver{}
	s.lakekeeperResolver = fr

	resp, err := s.OpenWriter(context.Background(), &pb.OpenWriterRequest{
		Bucket: "raw", Table: "orders", InputFormat: pb.DataFormat_FORMAT_CSV,
	})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if got := fr.createdColumns(); len(got) != 0 {
		t.Fatalf("table created before the first chunk: %v", got)
	}
	if _, err := s.WriteChunk(context.Background(), &pb.WriteChunkRequest{
		WriterId: resp.WriterId, Data: []byte("id,secret\n1,x\n2,y\n"),
	}); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	created := fr.createdColumns()
	if len(created) != 1 || strings.Join(created[0], ",") != "id" {
		t.Fatalf("created table columns = %v, want [[id]]", created)
	}
	w := p.writers[0]
	if w.spec.DataPrefix != "s3://warehouse/raw/orders/data/" {
		t.Errorf("DataPrefix = %q", w.spec.DataPrefix)
	}
	if names := columnNames(w.spec.Schema); strings.Join(names, ",") != "id" {
		t.Errorf("writer schema = %v, want [id]", names)
	}
}

// A component-supplied schema no longer suppresses the data.
func TestSuppliedSchemaStillWritesData(t *testing.T) {
	s, p := newWriteTestServer(t, &Config{DefaultBucket: "raw"})
	ctx := context.Background()
	open, err := s.OpenWriter(ctx, &pb.OpenWriterRequest{
		Bucket: "raw", Table: "orders", InputFormat: pb.DataFormat_FORMAT_CSV,
		Schema: &pb.Schema{Columns: []*pb.ColumnDef{
			{Name: "id", Type: "int64", Nullable: true},
			{Name: "name", Type: "string", Nullable: true},
		}},
	})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	wr, err := s.WriteChunk(ctx, &pb.WriteChunkRequest{WriterId: open.WriterId, Data: []byte("id,name\n1,a\n2,b\n")})
	if err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	if wr.RowsAccepted != 2 || wr.InferredSchema != nil {
		t.Errorf("WriteChunk = %+v, want 2 rows and no inferred schema", wr)
	}
	if len(p.writers) != 1 {
		t.Fatalf("plane writers = %d, want 1", len(p.writers))
	}
	var rows int64
	for _, b := range p.writers[0].recorded() {
		rows += b.NumRows()
	}
	if rows != 2 {
		t.Errorf("plane received %d rows, want 2", rows)
	}
	cl, err := s.CloseWriter(ctx, &pb.CloseWriterRequest{WriterId: open.WriterId})
	if err != nil {
		t.Fatalf("CloseWriter: %v", err)
	}
	if cl.TotalRows != 2 || cl.FilesWritten != 1 {
		t.Errorf("CloseWriter = %+v, want 2 rows in 1 file", cl)
	}
}

// Supplied schema + drop: the eager create at OpenWriter gets the post-drop
// schema, and the batches the plane receives lack the dropped column too.
func TestSuppliedSchemaWithDropCreatesPostDropTable(t *testing.T) {
	s, p := newWriteTestServer(t, &Config{
		DefaultBucket: "raw",
		Processors:    []ProcessorConfig{{Type: "drop", Columns: []string{"secret"}}},
	})
	fr := &fakeResolver{}
	s.lakekeeperResolver = fr
	ctx := context.Background()

	open, err := s.OpenWriter(ctx, &pb.OpenWriterRequest{
		Bucket: "raw", Table: "orders", InputFormat: pb.DataFormat_FORMAT_CSV,
		Schema: &pb.Schema{Columns: []*pb.ColumnDef{
			{Name: "id", Type: "int64", Nullable: true},
			{Name: "secret", Type: "string", Nullable: true},
		}},
	})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	created := fr.createdColumns()
	if len(created) != 1 || strings.Join(created[0], ",") != "id" {
		t.Fatalf("eager create columns = %v, want [[id]]", created)
	}
	if _, err := s.WriteChunk(ctx, &pb.WriteChunkRequest{WriterId: open.WriterId, Data: []byte("id,secret\n1,x\n")}); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	if fr.calls != 1 {
		t.Errorf("resolver calls = %d, want 1 (no second create on the first chunk)", fr.calls)
	}
	got := p.writers[0].recorded()
	if len(got) != 1 || strings.Join(columnNames(got[0].Schema()), ",") != "id" {
		t.Fatalf("plane batches = %v, want one batch with [id]", got)
	}
	if p.writers[0].spec.DataPrefix != "s3://warehouse/raw/orders/data/" {
		t.Errorf("DataPrefix = %q", p.writers[0].spec.DataPrefix)
	}
}

// The factory is called once, after run-token validation, with the
// validated claims and the raw token.
func TestFactoryReceivesValidatedClaims(t *testing.T) {
	const kid, runID, warehouse, projectID = "k1", "run-claims-1", "wh-claims", "proj-claims"
	priv := rfc017GenKey(t)
	jwksBody := rfc017MakeJWKS(kid, &priv.PublicKey)
	jwksSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(jwksBody) //nolint:errcheck
	}))
	defer jwksSrv.Close()
	token := rfc017MintToken(t, priv, kid, runID, warehouse, projectID)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RUN_ID", runID)

	s, p := newWriteTestServer(t, &Config{
		RunID:              runID,
		LakekeeperURL:      "http://lakekeeper.invalid/catalog",
		RunTokenPath:       tokenFile,
		PipelineAPIJWKSURL: jwksSrv.URL,
	})
	if s.commitPool == nil {
		t.Error("commit pool not built with a LakekeeperURL")
	}
	p.mu.Lock()
	calls := p.factoryCalls
	p.mu.Unlock()
	if calls != 1 {
		t.Errorf("factory calls = %d, want 1", calls)
	}
	want := BootInfo{RunID: runID, LakekeeperURL: "http://lakekeeper.invalid/catalog", ProjectID: projectID, Warehouse: warehouse, Token: token}
	if got := p.lastBoot(); got != want {
		t.Errorf("BootInfo = %v (token match %v), want %v", got, got.Token == token, want)
	}
}

// A cancel annotation already present at boot is seen by the watcher's first
// poll, which only starts once the factory has returned and the plane is
// published: the plane is interrupted, not missed.
func TestFactoryBeforeCancelWatcher(t *testing.T) {
	annotations := filepath.Join(t.TempDir(), "annotations")
	if err := os.WriteFile(annotations, []byte(CancelAnnotationKey+"=\"true\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	p := newFakeDataPlane()
	entered, release := make(chan struct{}), make(chan struct{})
	factory := func(context.Context, BootInfo) (DataPlane, error) {
		close(entered)
		<-release
		return p, nil
	}
	type result struct {
		s   *ServerV2
		err error
	}
	done := make(chan result, 1)
	go func() {
		s, err := NewServerV2(&Config{
			RunID: "r", LocalDataDir: t.TempDir(),
			PodAnnotationsPath: annotations, CancelPollInterval: 10 * time.Millisecond,
		}, factory)
		done <- result{s, err}
	}()
	<-entered
	time.Sleep(100 * time.Millisecond) // many poll intervals while the factory blocks
	select {
	case <-done:
		t.Fatal("NewServerV2 returned while the factory was still running")
	default:
	}
	close(release)
	r := <-done
	if r.err != nil {
		t.Fatalf("NewServerV2: %v", r.err)
	}
	defer r.s.Close()
	deadline := time.Now().Add(5 * time.Second)
	for !p.interrupted() {
		if time.Now().After(deadline) {
			t.Fatal("cancel annotation present at boot never interrupted the plane")
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case <-r.s.cancelCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("cancel context not cancelled")
	}
}

// A factory error fails NewServerV2 and starts no cancel watcher.
func TestFactoryFailureStartsNoWatcher(t *testing.T) {
	annotations := filepath.Join(t.TempDir(), "annotations")
	if err := os.WriteFile(annotations, []byte("other=\"x\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom: no engine")
	before := runtime.NumGoroutine()
	s, err := NewServerV2(&Config{
		RunID: "r", LocalDataDir: t.TempDir(),
		PodAnnotationsPath: annotations, CancelPollInterval: time.Millisecond,
	}, func(context.Context, BootInfo) (DataPlane, error) { return nil, boom })
	if err == nil || !errors.Is(err, boom) || s != nil {
		t.Fatalf("NewServerV2 = %v, %v; want nil and the factory error", s, err)
	}
	time.Sleep(50 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before {
		t.Errorf("goroutines %d -> %d after a failed NewServerV2 (watcher leaked?)", before, after)
	}

	if _, err := NewServerV2(&Config{RunID: "r", LocalDataDir: t.TempDir()}, nil); err == nil {
		t.Error("NewServerV2 with a nil factory: want error")
	}
}

func TestExternalFilesRejected(t *testing.T) {
	s, _ := newWriteTestServer(t, &Config{DefaultBucket: "raw"})
	ctx := context.Background()
	open, err := s.OpenWriter(ctx, &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_CSV})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.CloseWriter(ctx, &pb.CloseWriterRequest{
		WriterId:      open.WriterId,
		ExternalFiles: []*pb.ExternalDataFile{{Path: "s3://b/f.parquet", RowCount: 1, SizeBytes: 10}},
	})
	if err == nil || !strings.Contains(err.Error(), "external data files are not supported") {
		t.Fatalf("CloseWriter with external files = %v, want 'external data files are not supported'", err)
	}
}

// The cancel annotation interrupts the plane and aborts every open writer.
func TestCancelInterruptsPlaneAndAbortsWriters(t *testing.T) {
	annotations := filepath.Join(t.TempDir(), "annotations")
	if err := os.WriteFile(annotations, []byte("other=\"x\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, p := newWriteTestServer(t, &Config{
		DefaultBucket: "raw", PodAnnotationsPath: annotations, CancelPollInterval: 10 * time.Millisecond,
	})
	ctx := context.Background()
	open, err := s.OpenWriter(ctx, &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_CSV})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteChunk(ctx, &pb.WriteChunkRequest{WriterId: open.WriterId, Data: []byte("id\n1\n")}); err != nil {
		t.Fatal(err)
	}
	w := p.writers[0]
	if err := os.WriteFile(annotations, []byte(CancelAnnotationKey+"=\"true\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	aborted := func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.aborted
	}
	deadline := time.Now().Add(5 * time.Second)
	for !(p.interrupted() && aborted()) {
		if time.Now().After(deadline) {
			t.Fatalf("after cancel: interrupted=%v writer aborted=%v", p.interrupted(), aborted())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := s.CloseWriter(ctx, &pb.CloseWriterRequest{WriterId: open.WriterId}); err == nil {
		t.Error("CloseWriter after cancel: want error")
	}
}

// CloseWriter closes the plane writer, reports totals from its files and
// dispatches exactly those paths; a writer with no data dispatches nothing.
func TestCloseWriterDispatchesWrittenFiles(t *testing.T) {
	dir := t.TempDir()
	s, p := newWriteTestServer(t, &Config{
		RunID: "0123456789abcdef", DefaultBucket: "raw", LocalDataDir: dir,
	})
	var mu sync.Mutex
	var dispatched [][]string
	s.setCommitPoolForTest(NewCommitPool(CommitPoolConfig{
		Workers: 1, MaxQueueSize: 8,
		CatalogFn: func(context.Context) (catalog.Catalog, error) { return nil, nil },
		CommitFn: func(_ context.Context, _ catalog.Catalog, _ icebergtable.Identifier,
			paths []string, _ *icebergjob.PartitionedFiles, _ icebergjob.WriteMode, _ string) (*icebergjob.CommitResult, error) {
			mu.Lock()
			dispatched = append(dispatched, paths)
			mu.Unlock()
			return &icebergjob.CommitResult{DataFilesAdded: len(paths), SnapshotIDAfter: "1"}, nil
		},
	}))
	ctx := context.Background()

	open, err := s.OpenWriter(ctx, &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_CSV})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteChunk(ctx, &pb.WriteChunkRequest{WriterId: open.WriterId, Data: []byte("id\n1\n2\n")}); err != nil {
		t.Fatal(err)
	}
	empty, err := s.OpenWriter(ctx, &pb.OpenWriterRequest{Table: "nothing", InputFormat: pb.DataFormat_FORMAT_CSV})
	if err != nil {
		t.Fatal(err)
	}

	cl, err := s.CloseWriter(ctx, &pb.CloseWriterRequest{WriterId: open.WriterId})
	if err != nil {
		t.Fatalf("CloseWriter: %v", err)
	}
	wantPath := dir + "/raw/orders/data/part-01234567-" + open.WriterId + "-00000.parquet"
	w := p.writers[0]
	if w.spec.FilePrefix != "part-01234567-"+open.WriterId || w.spec.DataPrefix != dir+"/raw/orders/data/" {
		t.Errorf("spec prefixes = %q / %q", w.spec.DataPrefix, w.spec.FilePrefix)
	}
	w.mu.Lock()
	fakeFiles := append([]WrittenFile(nil), w.files...)
	w.mu.Unlock()
	if len(fakeFiles) != 1 || fakeFiles[0].Path != wantPath {
		t.Fatalf("fake files = %+v, want %s", fakeFiles, wantPath)
	}
	if cl.TotalRows != fakeFiles[0].Rows || cl.TotalBytes != fakeFiles[0].Bytes || cl.FilesWritten != 1 || cl.TotalRows != 2 {
		t.Errorf("CloseWriter = %+v, want totals of %+v", cl, fakeFiles[0])
	}
	if _, err := s.CloseWriter(ctx, &pb.CloseWriterRequest{WriterId: empty.WriterId}); err != nil {
		t.Fatalf("CloseWriter (no data): %v", err)
	}

	resp, err := s.Commit(ctx, &pb.CommitRequest{})
	if err != nil || !resp.Success {
		t.Fatalf("Commit = %+v, %v", resp, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dispatched) != 1 || len(dispatched[0]) != 1 || dispatched[0][0] != wantPath {
		t.Errorf("dispatched = %v, want [[%s]]", dispatched, wantPath)
	}
	statuses := map[string]pb.TableCommitResult_Status{}
	for _, b := range resp.Buckets {
		for _, tbl := range b.Tables {
			statuses[tbl.Table] = tbl.Status
		}
	}
	if statuses["orders"] != pb.TableCommitResult_STATUS_COMMITTED || statuses["nothing"] != pb.TableCommitResult_STATUS_SKIPPED {
		t.Errorf("statuses = %v, want orders COMMITTED, nothing SKIPPED", statuses)
	}
}

func TestWriteErrorFromPlaneIs500(t *testing.T) {
	s, p := newWriteTestServer(t, &Config{DefaultBucket: "raw"})
	p.failWrites(errors.New("copy failed"))
	open, err := s.OpenWriter(context.Background(), &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_CSV})
	if err != nil {
		t.Fatal(err)
	}
	resp := httpWrite(s, open.WriterId, []byte("id\n1\n"))
	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.Code)
	}
	if e := decodeHTTPError(t, resp); e.Code != "PROCESS_ERROR" || !strings.Contains(e.Error, "copy failed") {
		t.Errorf("error envelope = %+v", e)
	}
	if _, err := s.CloseWriter(context.Background(), &pb.CloseWriterRequest{WriterId: open.WriterId}); err == nil {
		t.Error("CloseWriter after a failed write: want error")
	}
}

// A first chunk of a type the plane cannot write fails with today's "buffer
// error" class BEFORE any table create or plane writer open (the DuckDB plane
// would otherwise fail OpenTableWriter with its own error).
func TestFirstChunkTypeRejectedBeforeWriterOpen(t *testing.T) {
	for _, dt := range []arrow.DataType{arrow.BinaryTypes.LargeString, arrow.BinaryTypes.LargeBinary} {
		t.Run(dt.String(), func(t *testing.T) {
			s, p := newWriteTestServer(t, &Config{DefaultBucket: "raw"})
			p.rejectLargeTypes = true
			fr := &fakeResolver{}
			s.lakekeeperResolver = fr

			js := `["a", null]`
			if dt.ID() == arrow.LARGE_BINARY {
				js = `["YQ==", null]`
			}
			arr, _, err := array.FromJSON(memory.NewGoAllocator(), dt, strings.NewReader(js))
			if err != nil {
				t.Fatal(err)
			}
			sch := arrow.NewSchema([]arrow.Field{{Name: "v", Type: dt, Nullable: true}}, nil)
			rec := array.NewRecordBatch(sch, []arrow.Array{arr}, 2)
			arr.Release()
			defer rec.Release()

			id := openIPCWriter(t, s, "t")
			resp := httpWrite(s, id, ipcPayload(t, rec))
			if resp.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500", resp.Code)
			}
			if e := decodeHTTPError(t, resp); errorClass(e.Error) != "buffer error" || !strings.Contains(e.Error, "unsupported Arrow type") {
				t.Errorf("error = %q, want the buffer error class naming the unsupported type", e.Error)
			}
			if n := p.openCalls(); n != 0 {
				t.Errorf("OpenTableWriter called %d times for a rejected first chunk", n)
			}
			if got := fr.createdColumns(); len(got) != 0 {
				t.Errorf("table created for a rejected first chunk: %v", got)
			}
		})
	}
}

// A cancel that lands while the first chunk is blocked in the deferred table
// create must not let that chunk open a live writer afterwards: the write
// fails and no plane writer survives un-aborted (the DuckDB plane accepts new
// writers after Interrupt, which the fake mimics here).
func TestCancelDuringDeferredCreateOpensNoLiveWriter(t *testing.T) {
	annotations := filepath.Join(t.TempDir(), "annotations")
	if err := os.WriteFile(annotations, []byte("other=\"x\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, p := newWriteTestServer(t, &Config{
		DefaultBucket: "raw", PodAnnotationsPath: annotations, CancelPollInterval: 5 * time.Millisecond,
	})
	p.openAfterInterrupt = true
	fr := &fakeResolver{entered: make(chan struct{}), release: make(chan struct{})}
	s.lakekeeperResolver = fr
	ctx := context.Background()

	open, err := s.OpenWriter(ctx, &pb.OpenWriterRequest{Table: "orders", InputFormat: pb.DataFormat_FORMAT_CSV})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	writeErr := make(chan error, 1)
	go func() {
		_, err := s.WriteChunk(ctx, &pb.WriteChunkRequest{WriterId: open.WriterId, Data: []byte("id\n1\n")})
		writeErr <- err
	}()
	<-fr.entered
	if err := os.WriteFile(annotations, []byte(CancelAnnotationKey+"=\"true\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.cancelCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("cancel not observed")
	}
	if !p.interrupted() {
		t.Fatal("plane not interrupted by the cancel")
	}
	close(fr.release)

	if err := <-writeErr; err == nil {
		t.Fatal("write that raced the cancel succeeded")
	}
	p.mu.Lock()
	writers := append([]*fakeTableWriter(nil), p.writers...)
	p.mu.Unlock()
	for i, w := range writers {
		w.mu.Lock()
		live := !w.aborted && !w.terminated && !w.closed
		w.mu.Unlock()
		if live {
			t.Errorf("plane writer %d opened across the cancel is still live", i)
		}
	}
}
