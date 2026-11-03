//go:build integration

package goplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	pqschema "github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go"

	"github.com/datuplet/datuplet/pkg/datagateway"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// variantRows are the JSON texts a frozen-SDK component sends for a
// variant column (Arrow utf8); "" is null.
var variantRows = []string{`{"a":1,"b":[true,"x"]}`, `"hello"`, `-9223372036854775808`, `{"n":9007199254740993}`, ``, `"42"`}

// TestIntegrationWriteVariantCommitAndRead: a real server writes a column
// declared variant in the output columns from Arrow utf8 JSON text, commits
// it into a v3 table, and the data file has the Variant-annotated group with
// metadata/value binary leaves and no child field ids; the commit carries
// the file's metrics; an iceberg-go scan (and the DuckDB 2.0 CLI, when
// installed) reads back the same values.
func TestIntegrationWriteVariantCommitAndRead(t *testing.T) {
	fx := sharedFixture(t)
	const runID = "0123456789abcdef-server-variant"
	const ns, tbl = "gw_variant", "events"
	iss := newRunTokenIssuer(t)
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte(iss.mint(t, runID, fx.projectID, fx.warehouseName)), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RUN_ID", runID)
	httpAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	srv, err := datagateway.NewServerV2(&datagateway.Config{
		RunID:            runID,
		ComponentName:    "gw-it",
		DefaultBucket:    ns,
		DefaultWriteMode: "APPEND",
		OutputTables: []datagateway.OutputTableConfig{{Name: tbl, Bucket: ns, WriteMode: "APPEND",
			Columns: []datagateway.ColumnConfig{{Name: "id", Type: "long"}, {Name: "v", Type: "variant"}}}},
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
	ctx := context.Background()
	waitHTTP(ctx, t, "http://"+httpAddr+"/health")

	open, err := srv.OpenWriter(ctx, &pb.OpenWriterRequest{Bucket: ns, Table: tbl, InputFormat: pb.DataFormat_FORMAT_ARROW_IPC})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), sch)
	for i, s := range variantRows {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		if s == "" {
			b.Field(1).AppendNull()
		} else {
			b.Field(1).(*array.StringBuilder).Append(s)
		}
	}
	rec := b.NewRecordBatch()
	b.Release()
	body := serverIPCBytes(t, rec)
	rec.Release()
	resp, err := (&http.Client{Timeout: 2 * time.Minute}).Post(open.HttpEndpoint, "application/vnd.apache.arrow.stream", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	msg, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST: HTTP %d: %s", resp.StatusCode, msg)
	}
	if _, err := srv.CloseWriter(ctx, &pb.CloseWriterRequest{WriterId: open.WriterId}); err != nil {
		t.Fatalf("CloseWriter: %v", err)
	}
	if commit, err := srv.Commit(ctx, &pb.CommitRequest{}); err != nil || !commit.Success {
		t.Fatalf("Commit = %+v, %v", commit, err)
	}

	tb := loadLK(t, resolver(t, fx), ns, tbl)
	if v := tb.Metadata().Version(); v != 3 {
		t.Fatalf("format version %d, want 3", v)
	}
	vField, ok := tb.Schema().FindFieldByName("v")
	if _, isVariant := vField.Type.(iceberg.VariantType); !ok || !isVariant {
		t.Fatalf("table column v = %v, want variant", vField.Type)
	}

	tasks, err := tb.Scan().PlanFiles(ctx)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("PlanFiles = %d tasks, %v; want 1", len(tasks), err)
	}
	// The commit carries the file's metrics; iceberg-go's footer stats skip
	// variant sub-columns, so v itself has none.
	df := tasks[0].File
	idField, _ := tb.Schema().FindFieldByName("id")
	if df.Count() != int64(len(variantRows)) || df.FileSizeBytes() <= 0 || df.ValueCounts()[idField.ID] != int64(len(variantRows)) {
		t.Fatalf("data file metrics: count %d size %d value counts %v; want %d rows, a size and id counts",
			df.Count(), df.FileSizeBytes(), df.ValueCounts(), len(variantRows))
	}
	if snap := tb.CurrentSnapshot(); snap == nil || snap.Summary == nil || snap.Summary.Properties["added-records"] != fmt.Sprint(len(variantRows)) {
		t.Fatalf("snapshot summary %+v, want added-records %d", snap, len(variantRows))
	}

	data := objectBytes(t, fx, df.FilePath())
	checkVariantLayout(t, data)

	want := map[int64]string{}
	for i, s := range variantRows {
		if s == "" {
			s = "NULL"
		}
		want[int64(i)] = s
	}
	_, recs, err := tb.Scan().ToArrowRecords(ctx)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	got := map[int64]string{}
	for r, err := range recs {
		if err != nil {
			t.Fatalf("scan batch: %v", err)
		}
		ids := r.Column(0).(*array.Int64)
		va := r.Column(1).(*extensions.VariantArray)
		for i := 0; i < int(r.NumRows()); i++ {
			if va.IsNull(i) {
				got[ids.Value(i)] = "NULL"
				continue
			}
			v, err := va.Value(i)
			if err != nil {
				t.Fatal(err)
			}
			js, err := v.MarshalJSON()
			if err != nil {
				t.Fatal(err)
			}
			got[ids.Value(i)] = string(js)
		}
		r.Release()
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scan values\n got %v\nwant %v", got, want)
	}

	checkDuckDBReadsVariant(t, data, want)
}

// checkVariantLayout checks the Parquet layout of column v: a
// Variant-annotated group whose metadata and value leaves are binary and
// carry no field ids.
func checkVariantLayout(t *testing.T, data []byte) {
	t.Helper()
	rdr, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Close()
	root := rdr.MetaData().Schema.Root()
	idx := root.FieldIndexByName("v")
	if idx < 0 {
		t.Fatal("no column v in the data file")
	}
	g, ok := root.Field(idx).(*pqschema.GroupNode)
	if !ok {
		t.Fatalf("v is %T, want a group", root.Field(idx))
	}
	if _, ok := g.LogicalType().(pqschema.VariantLogicalType); !ok {
		t.Fatalf("v logical type %v, want Variant", g.LogicalType())
	}
	if g.NumFields() != 2 {
		t.Fatalf("v has %d children, want metadata and value", g.NumFields())
	}
	for i, name := range []string{"metadata", "value"} {
		leaf, ok := g.Field(i).(*pqschema.PrimitiveNode)
		if !ok || leaf.Name() != name || leaf.PhysicalType() != parquet.Types.ByteArray {
			t.Fatalf("v child %d = %v, want binary %s", i, g.Field(i), name)
		}
		if leaf.FieldID() >= 0 {
			t.Fatalf("v.%s carries field id %d, want none", name, leaf.FieldID())
		}
	}
}

// checkDuckDBReadsVariant reads the data file with the duckdb CLI when it
// is DuckDB 2.x; an older or missing CLI skips (only) this check.
func checkDuckDBReadsVariant(t *testing.T, data []byte, want map[int64]string) {
	t.Helper()
	out, err := exec.Command("duckdb", "--version").Output()
	if err != nil || !strings.HasPrefix(strings.TrimSpace(string(out)), "v2.") {
		t.Logf("skipping the DuckDB CLI check: need DuckDB 2.x, have %q (%v)", strings.TrimSpace(string(out)), err)
		return
	}
	path := filepath.Join(t.TempDir(), "variant.parquet")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	q := fmt.Sprintf("SELECT id, v::JSON::VARCHAR AS v FROM read_parquet('%s') ORDER BY id", path)
	res, err := exec.Command("duckdb", "-json", "-c", q).Output()
	if err != nil {
		t.Fatalf("duckdb: %v", err)
	}
	var rows []struct {
		ID int64   `json:"id"`
		V  *string `json:"v"`
	}
	if err := json.Unmarshal(res, &rows); err != nil {
		t.Fatalf("duckdb output %s: %v", res, err)
	}
	if len(rows) != len(want) {
		t.Fatalf("duckdb read %d rows, want %d", len(rows), len(want))
	}
	for _, r := range rows {
		if r.V == nil {
			if want[r.ID] != "NULL" {
				t.Fatalf("duckdb id %d: NULL, want %s", r.ID, want[r.ID])
			}
			continue
		}
		if !sameJSON(t, *r.V, want[r.ID]) {
			t.Fatalf("duckdb id %d: %s, want %s", r.ID, *r.V, want[r.ID])
		}
	}
}

// sameJSON compares two JSON texts as values, numbers exactly.
func sameJSON(t *testing.T, a, b string) bool {
	t.Helper()
	decode := func(s string) any {
		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			t.Fatalf("decode %s: %v", s, err)
		}
		return v
	}
	return reflect.DeepEqual(decode(a), decode(b))
}
