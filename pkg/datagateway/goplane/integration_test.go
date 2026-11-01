//go:build integration

package goplane

// Integration tests: the Go writer against Lakekeeper + silo with real vended
// credentials, committed by the unchanged commit path
// (icebergjob.CommitTableFiles → iceberg-go AddFiles / ReplaceDataFiles) and
// read back with an iceberg-go scan.
//
// Run (Docker required; unavailable Docker is BLOCKED, not a pass):
//
//	go test -count=1 -tags=integration -run Integration ./pkg/datagateway/goplane/ -v -timeout 15m

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/catalog"
	"github.com/minio/minio-go/v7"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	"github.com/datuplet/datuplet/pkg/datagateway"
	"github.com/datuplet/datuplet/pkg/datagateway/lakekeeper"
	dgschema "github.com/datuplet/datuplet/pkg/datagateway/schema"
	"github.com/datuplet/datuplet/pkg/icebergjob"
)

var (
	envOnce sync.Once
	envFx   *fixture
	envErr  string
)

func TestMain(m *testing.M) {
	code := m.Run()
	if envFx != nil {
		removeContainers()
	}
	os.Exit(code)
}

func sharedFixture(t *testing.T) *fixture {
	t.Helper()
	envOnce.Do(func() {
		if out, err := exec.Command("docker", "info").CombinedOutput(); err != nil {
			envErr = fmt.Sprintf("BLOCKED: docker not available: %v\n%s", err, out)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
		defer cancel()
		envFx = provision(ctx, t)
	})
	if envFx == nil {
		if envErr == "" {
			envErr = "integration fixture failed in an earlier test"
		}
		t.Fatal(envErr)
	}
	return envFx
}

func resolver(t *testing.T, f *fixture) *lakekeeper.Resolver {
	t.Helper()
	r, err := lakekeeper.NewResolver(f.catalogURL(), f.warehouseName, dummyCatalogJWT, f.projectID)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// addFilesRejects are accepted golden types iceberg-go v0.7.0 AddFiles
// refuses, with the expected error (pinned by TestIntegrationAddFilesPerType).
// The Parquet types equal main's golden, so this is not a writer difference:
// the gateway creates every timestamp column as Iceberg timestamptz, which a
// non-UTC-adjusted or nanosecond Parquet timestamp does not match.
var addFilesRejects = map[string]string{
	"timestamp_s":         "mismatch in fields",
	"timestamp_ms":        "mismatch in fields",
	"timestamp_us":        "mismatch in fields",
	"timestamp_ns":        "mismatch in fields",
	"timestamp_ns_tz_utc": "mismatch in fields",
}

// scanSafeChunks replaces golden chunks an iceberg-go scan cannot read back:
// uint64 maps to Iceberg long, and the scan rejects values above MaxInt64
// ("integer value 18446744073709551615 not in range"). The writer and
// AddFiles accept the golden values; only the read-back assertion needs this.
var scanSafeChunks = map[string][2]string{
	"uint64": {`[0, null, 9223372036854775807]`, `[1, 4611686018427387904, null]`},
}

// allTypesSchema has one optional column per accepted golden type that
// AddFiles commits.
func allTypesSchema(t *testing.T) (*arrow.Schema, []goldenInput) {
	t.Helper()
	golden := loadGolden(t)
	var fields []arrow.Field
	var inputs []goldenInput
	for _, in := range goldenInputs() {
		if _, rejected := addFilesRejects[in.name]; rejected || !golden[in.name].Accepted {
			continue
		}
		fields = append(fields, arrow.Field{Name: "c_" + in.name, Type: in.dt, Nullable: true})
		inputs = append(inputs, in)
	}
	return arrow.NewSchema(fields, nil), inputs
}

// allTypesBatches returns the two golden chunks as two 3-row batches.
func allTypesBatches(t *testing.T, sch *arrow.Schema, inputs []goldenInput) []arrow.RecordBatch {
	t.Helper()
	mem := memory.NewGoAllocator()
	var out []arrow.RecordBatch
	for chunk := 0; chunk < 2; chunk++ {
		cols := make([]arrow.Array, len(inputs))
		for i, in := range inputs {
			c1, c2 := in.chunk1, in.chunk2
			if safe, ok := scanSafeChunks[in.name]; ok {
				c1, c2 = safe[0], safe[1]
			}
			js := c1
			if chunk == 1 {
				js = c2
			}
			arr, _, err := array.FromJSON(mem, in.dt, strings.NewReader(js), array.WithUseNumber())
			if err != nil {
				t.Fatalf("%s: %v", in.name, err)
			}
			cols[i] = arr
		}
		out = append(out, array.NewRecordBatch(sch, cols, 3))
		for _, c := range cols {
			c.Release()
		}
	}
	return out
}

func createTable(t *testing.T, r *lakekeeper.Resolver, ns, tbl string, sch *arrow.Schema) *lakekeeper.WriterTarget {
	t.Helper()
	gw, err := dgschema.NewSchemaFromArrow(sch)
	if err != nil {
		t.Fatalf("gateway schema: %v", err)
	}
	target, err := r.LoadOrCreateForWrite(context.Background(), ns, tbl,
		func(context.Context) (*dgschema.Schema, error) { return gw, nil })
	if err != nil {
		t.Fatalf("LoadOrCreateForWrite: %v", err)
	}
	if target.VendedCreds == nil || !strings.HasPrefix(target.BasePath, "s3://") {
		t.Fatalf("unexpected target %q (creds %v)", target.BasePath, target.VendedCreds != nil)
	}
	return target
}

// writeRun writes the golden batches repeat times through a new writer.
func writeRun(t *testing.T, p *Plane, target *lakekeeper.WriterTarget, filePrefix string, sch *arrow.Schema,
	inputs []goldenInput, repeat int, creds datagateway.CredsSource, targetBytes int64) []datagateway.WrittenFile {
	t.Helper()
	w, err := p.OpenTableWriter(context.Background(), datagateway.WriterSpec{
		DataPrefix: target.BasePath, FilePrefix: filePrefix, Schema: sch, Creds: creds, TargetFileBytes: targetBytes,
	})
	if err != nil {
		t.Fatalf("OpenTableWriter: %v", err)
	}
	for r := 0; r < repeat; r++ {
		for _, b := range allTypesBatches(t, sch, inputs) {
			err := w.Write(context.Background(), b)
			b.Release()
			if err != nil {
				w.Abort()
				t.Fatalf("Write: %v", err)
			}
		}
	}
	files, err := w.Close(context.Background())
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, f := range files {
		if !strings.HasPrefix(f.Path, target.BasePath+filePrefix+"-") || f.Rows <= 0 || f.Bytes <= 0 {
			t.Fatalf("bad file %+v", f)
		}
	}
	return files
}

func paths(files []datagateway.WrittenFile) []string {
	var out []string
	for _, f := range files {
		out = append(out, f.Path)
	}
	return out
}

func commit(t *testing.T, r *lakekeeper.Resolver, ns, tbl string, files []datagateway.WrittenFile, mode icebergjob.WriteMode, key string) *icebergjob.CommitResult {
	t.Helper()
	cat, err := r.Catalog(context.Background())
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	res, err := icebergjob.CommitTableFiles(context.Background(), cat, catalog.ToIdentifier(ns, tbl), paths(files), mode, nil, key)
	if err != nil {
		t.Fatalf("CommitTableFiles %s: %v", mode, err)
	}
	return res
}

// scanRows reads the table's current snapshot with a fresh iceberg-go load
// and scan, returning the row count and the column names.
func scanRows(t *testing.T, r *lakekeeper.Resolver, ns, tbl string) (int64, []string) {
	t.Helper()
	ctx := context.Background()
	cat, err := r.Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tb, err := cat.LoadTable(ctx, catalog.ToIdentifier(ns, tbl))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	sch, recs, err := tb.Scan().ToArrowRecords(ctx)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	var n int64
	for rec, err := range recs {
		if err != nil {
			t.Fatalf("scan batch: %v", err)
		}
		n += rec.NumRows()
		rec.Release()
	}
	var names []string
	for _, f := range sch.Fields() {
		names = append(names, f.Name)
	}
	return n, names
}

func currentCommitKey(t *testing.T, r *lakekeeper.Resolver, ns, tbl string) string {
	t.Helper()
	cat, err := r.Catalog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tb, err := cat.LoadTable(context.Background(), catalog.ToIdentifier(ns, tbl))
	if err != nil {
		t.Fatal(err)
	}
	s := tb.CurrentSnapshot()
	if s == nil || s.Summary == nil {
		t.Fatal("no current snapshot")
	}
	return s.Summary.Properties["datuplet.commit-key"]
}

func TestIntegrationWriteAndCommit(t *testing.T) {
	fx := sharedFixture(t)
	p := New(Config{})
	defer p.Close()
	r := resolver(t, fx)
	sch, inputs := allTypesSchema(t)
	const ns, tbl = "gw_it", "all_types"
	target := createTable(t, r, ns, tbl, sch)
	t.Logf("data prefix %s, %d columns", target.BasePath, sch.NumFields())

	f1 := writeRun(t, p, target, "part-run00001-w1", sch, inputs, 1, target.VendedCreds, 0)
	if len(f1) != 1 || f1[0].Rows != 6 {
		t.Fatalf("run 1 files %+v", f1)
	}
	commit(t, r, ns, tbl, f1, icebergjob.WriteModeAppend, "key-run1")
	f2 := writeRun(t, p, target, "part-run00002-w1", sch, inputs, 2, target.VendedCreds, 0)
	commit(t, r, ns, tbl, f2, icebergjob.WriteModeAppend, "key-run2")
	n, names := scanRows(t, r, ns, tbl)
	if n != 6+12 {
		t.Fatalf("after two APPENDs: %d rows, want 18", n)
	}
	if len(names) != sch.NumFields() || names[0] != sch.Field(0).Name {
		t.Fatalf("scanned columns %q", names)
	}
	if k := currentCommitKey(t, r, ns, tbl); k != "key-run2" {
		t.Fatalf("snapshot commit key %q", k)
	}

	f3 := writeRun(t, p, target, "part-run00003-w1", sch, inputs, 1, target.VendedCreds, 0)
	res := commit(t, r, ns, tbl, f3, icebergjob.WriteModeFullLoad, "key-run3")
	if res.IdempotencyHit {
		t.Fatal("first FULL_LOAD commit reported an idempotency hit")
	}
	if n, _ := scanRows(t, r, ns, tbl); n != 6 {
		t.Fatalf("after FULL_LOAD: %d rows, want 6", n)
	}
	if k := currentCommitKey(t, r, ns, tbl); k != "key-run3" {
		t.Fatalf("snapshot commit key %q", k)
	}

	again := commit(t, r, ns, tbl, f3, icebergjob.WriteModeFullLoad, "key-run3")
	if !again.IdempotencyHit || again.SnapshotIDAfter != res.SnapshotIDAfter {
		t.Fatalf("repeat commit = %+v, first %+v", again, res)
	}
	if n, _ := scanRows(t, r, ns, tbl); n != 6 {
		t.Fatalf("after repeat commit: %d rows, want 6", n)
	}
}

// invalidatingCreds forces a fresh Lakekeeper round trip per Get and records
// the access key each file got.
type invalidatingCreds struct {
	vc   *catalogwriter.VendedCreds
	mu   sync.Mutex
	keys []string
}

func (c *invalidatingCreds) Get(ctx context.Context) (catalogwriter.Creds, error) {
	c.vc.Invalidate()
	cr, err := c.vc.Get(ctx)
	if err != nil {
		return nil, err
	}
	if s3, ok := cr.(catalogwriter.S3Creds); ok {
		c.mu.Lock()
		c.keys = append(c.keys, s3.AccessKeyID)
		c.mu.Unlock()
	}
	return cr, nil
}

func (c *invalidatingCreds) Invalidate() { c.vc.Invalidate() }

func TestIntegrationCredsInvalidateBetweenFiles(t *testing.T) {
	fx := sharedFixture(t)
	p := New(Config{})
	defer p.Close()
	r := resolver(t, fx)
	sch, inputs := allTypesSchema(t)
	const ns, tbl = "gw_it", "rotating"
	target := createTable(t, r, ns, tbl, sch)
	creds := &invalidatingCreds{vc: target.VendedCreds}
	files := writeRun(t, p, target, "part-run00009-w1", sch, inputs, 3, creds, 1) // every batch ends a file
	if len(files) != 6 {
		t.Fatalf("files = %d, want 6", len(files))
	}
	creds.mu.Lock()
	keys := append([]string(nil), creds.keys...)
	creds.mu.Unlock()
	if len(keys) != len(files) {
		t.Fatalf("%d credential fetches for %d files", len(keys), len(files))
	}
	distinct := map[string]bool{}
	for _, k := range keys {
		distinct[k] = true
	}
	t.Logf("%d files, %d distinct vended access keys", len(files), len(distinct))
	commit(t, r, ns, tbl, files, icebergjob.WriteModeAppend, "key-rotating")
	if n, _ := scanRows(t, r, ns, tbl); n != 18 {
		t.Fatalf("rows = %d, want 18", n)
	}
}

// TestIntegrationAddFilesPerType commits each accepted golden type alone and
// pins which of them iceberg-go AddFiles accepts (addFilesRejects).
func TestIntegrationAddFilesPerType(t *testing.T) {
	fx := sharedFixture(t)
	p := New(Config{})
	defer p.Close()
	r := resolver(t, fx)
	golden := loadGolden(t)
	for _, in := range goldenInputs() {
		if !golden[in.name].Accepted {
			continue
		}
		sch := arrow.NewSchema([]arrow.Field{{Name: "v", Type: in.dt, Nullable: true}}, nil)
		target := createTable(t, r, "gw_types", in.name, sch)
		files := writeRun(t, p, target, "part-types00-w1", sch, []goldenInput{in}, 1, target.VendedCreds, 0)
		cat, err := r.Catalog(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_, err = icebergjob.CommitTableFiles(context.Background(), cat, catalog.ToIdentifier("gw_types", in.name),
			paths(files), icebergjob.WriteModeAppend, nil, "k")
		want, rejected := addFilesRejects[in.name]
		switch {
		case !rejected && err != nil:
			t.Errorf("%s: AddFiles rejected: %v", in.name, err)
		case rejected && (err == nil || !strings.Contains(err.Error(), want)):
			t.Errorf("%s: AddFiles = %v, want rejection %q", in.name, err, want)
		case !rejected:
			if n, _ := scanRows(t, r, "gw_types", in.name); n != 6 {
				t.Errorf("%s: %d rows committed, want 6", in.name, n)
			}
		}
	}
}

// TestIntegrationAbortLeavesNoS3Object writes ~40 MiB of incompressible data
// so the streamed upload has sent at least two multipart parts (gocloud's S3
// writer uses 8 MiB parts; the iceberg-go IO exposes no part-size option),
// then aborts: the object must not exist and no multipart upload may stay
// open for its key.
func TestIntegrationAbortLeavesNoS3Object(t *testing.T) {
	fx := sharedFixture(t)
	p := New(Config{})
	defer p.Close()
	r := resolver(t, fx)
	sch := arrow.NewSchema([]arrow.Field{{Name: "blob", Type: arrow.BinaryTypes.Binary, Nullable: true}}, nil)
	const ns, tbl = "gw_it", "aborted"
	target := createTable(t, r, ns, tbl, sch)
	const filePrefix = "part-abort000-w1"
	path := target.BasePath + filePrefix + "-00000.parquet"
	key := strings.TrimPrefix(path, "s3://"+siloBucket+"/")
	if key == path {
		t.Fatalf("data path %q is not in bucket %s", path, siloBucket)
	}

	w, err := p.OpenTableWriter(context.Background(), datagateway.WriterSpec{
		DataPrefix: target.BasePath, FilePrefix: filePrefix, Schema: sch, Creds: target.VendedCreds,
		RowGroupRows: 1024, // 4 MiB row groups, flushed to the upload as they fill
	})
	if err != nil {
		t.Fatal(err)
	}
	mem := memory.NewGoAllocator()
	val := make([]byte, 4096)
	for b := 0; b < 10; b++ {
		bb := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		for i := 0; i < 1024; i++ {
			_, _ = rand.Read(val)
			bb.Append(val)
		}
		arr := bb.NewArray()
		bb.Release()
		rec := array.NewRecordBatch(sch, []arrow.Array{arr}, int64(arr.Len()))
		arr.Release()
		err := w.Write(context.Background(), rec)
		rec.Release()
		if err != nil {
			w.Abort()
			t.Fatalf("Write: %v", err)
		}
	}

	core := fx.siloCore(t)
	ctx := context.Background()
	uploadID, parts := "", 0
	deadline := time.Now().Add(60 * time.Second)
	for parts < 2 {
		if time.Now().After(deadline) {
			w.Abort()
			t.Fatalf("upload never reached 2 parts (upload %q, %d parts)", uploadID, parts)
		}
		ups, err := core.ListMultipartUploads(ctx, siloBucket, key, "", "", "", 100)
		if err != nil {
			t.Fatalf("ListMultipartUploads: %v", err)
		}
		for _, u := range ups.Uploads {
			if u.Key == key {
				uploadID = u.UploadID
			}
		}
		if uploadID != "" {
			lp, err := core.ListObjectParts(ctx, siloBucket, key, uploadID, 0, 1000)
			if err != nil {
				t.Fatalf("ListObjectParts: %v", err)
			}
			parts = len(lp.ObjectParts)
		}
		if parts < 2 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	t.Logf("multipart upload %s has %d parts before Abort", uploadID, parts)

	w.Abort()
	if _, err := w.Close(context.Background()); err == nil {
		t.Fatal("Close after Abort succeeded")
	}

	_, err = core.StatObject(ctx, siloBucket, key, minio.StatObjectOptions{})
	var resp minio.ErrorResponse
	if err == nil || !errors.As(err, &resp) || resp.Code != "NoSuchKey" {
		t.Fatalf("object after Abort: StatObject err = %v, want NoSuchKey", err)
	}
	ups, err := core.ListMultipartUploads(ctx, siloBucket, key, "", "", "", 100)
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	for _, u := range ups.Uploads {
		if u.Key == key {
			t.Fatalf("multipart upload %s still open for %s after Abort", u.UploadID, key)
		}
	}
	if _, err := core.ListObjectParts(ctx, siloBucket, key, uploadID, 0, 1000); err == nil {
		t.Fatalf("upload %s still lists parts after Abort", uploadID)
	}
}
