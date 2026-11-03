//go:build integration

package goplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/iceberg-go"

	"github.com/datuplet/datuplet/pkg/datagateway"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// TestIntegrationHTTPJSONExtractorVariant runs the real, frozen
// http-json-extractor binary against a real ServerV2 on the Go plane: the
// output columns declare payload as variant, the gateway advertises it to
// the component as string, the extractor sends its nested objects as JSON
// text, and the committed v3 table holds them as variants, partitioned on
// category and id.
func TestIntegrationHTTPJSONExtractorVariant(t *testing.T) {
	fx := sharedFixture(t)
	const runID = "0123456789abcdef-extractor-variant"
	const ns, tbl = "gw_extractor_variant", "events"

	payloads := map[int64]map[string]any{
		1: {"user": map[string]any{"name": "ann", "tags": []any{"a", "b"}}, "n": float64(1)},
		2: {"user": map[string]any{"name": "bob", "tags": []any{"c"}}, "ok": true},
		3: {"deep": map[string]any{"x": map[string]any{"y": "z"}}, "s": "42"},
	}
	var fixture []map[string]any
	for _, id := range []int64{1, 2, 3} {
		fixture = append(fixture, map[string]any{"category": fmt.Sprintf("cat-%d", id%2), "id": id, "payload": payloads[id]})
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(fixture)
	}))
	defer api.Close()

	scratch := t.TempDir()
	bin := filepath.Join(scratch, "http-json-extractor")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = "../../../components/http-json-extractor"
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build http-json-extractor: %v\n%s", err, out)
	}

	iss := newRunTokenIssuer(t)
	tokenPath := filepath.Join(scratch, "token")
	if err := os.WriteFile(tokenPath, []byte(iss.mint(t, runID, fx.projectID, fx.warehouseName)), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RUN_ID", runID)
	compCfg, _ := json.Marshal(map[string]any{"url": api.URL, "table_name": tbl})
	httpAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	grpcAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	srv, err := datagateway.NewServerV2(&datagateway.Config{
		RunID:            runID,
		ComponentName:    "http-json-extractor",
		DefaultBucket:    ns,
		DefaultWriteMode: "APPEND",
		ComponentCfg:     compCfg,
		OutputTables: []datagateway.OutputTableConfig{{
			Name: tbl, Bucket: ns, WriteMode: "APPEND",
			PartitionFields: []datagateway.PartitionFieldConfig{
				{SourceColumn: "category", Transform: "identity"},
				{SourceColumn: "id", Transform: "bucket[4]"},
			},
			Columns: []datagateway.ColumnConfig{
				{Name: "category", Type: "string"},
				{Name: "id", Type: "long"},
				{Name: "payload", Type: "variant"},
			},
		}},
		LakekeeperURL:      fx.catalogURL(),
		PipelineAPIJWKSURL: iss.jwks.URL,
		RunTokenPath:       tokenPath,
		HTTPAddr:           httpAddr,
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
	ctx := context.Background()
	waitHTTP(ctx, t, "http://"+httpAddr+"/health")

	cfg, err := srv.GetConfig(ctx, &pb.GetConfigRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.OutputConfig.Tables[0].Columns[2]; got.Name != "payload" || got.Type != "string" {
		t.Fatalf("component sees payload as %+v, want string", got)
	}

	runCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(runCtx, bin)
	cmd.Env = append(os.Environ(), "DATUPLET_GATEWAY_ADDR="+grpcAddr)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("http-json-extractor: %v\n%s", err, out.String())
	}
	if commit, err := srv.Commit(ctx, &pb.CommitRequest{}); err != nil || !commit.Success {
		t.Fatalf("Commit = %+v, %v\nextractor output:\n%s", commit, err, out.String())
	}

	tb := loadLK(t, resolver(t, fx), ns, tbl)
	if v := tb.Metadata().Version(); v != 3 {
		t.Fatalf("format version %d, want 3", v)
	}
	if f, ok := tb.Schema().FindFieldByName("payload"); !ok {
		t.Fatal("no payload column")
	} else if _, isVariant := f.Type.(iceberg.VariantType); !isVariant {
		t.Fatalf("payload = %v, want variant", f.Type)
	}
	spec := tb.Spec()
	if n := spec.NumFields(); n != 2 {
		t.Fatalf("partition spec has %d fields, want 2", n)
	}

	_, recs, err := tb.Scan().ToArrowRecords(ctx)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	got := map[int64]any{}
	for r, err := range recs {
		if err != nil {
			t.Fatal(err)
		}
		idx := r.Schema().FieldIndices("id")[0]
		pidx := r.Schema().FieldIndices("payload")[0]
		ids := r.Column(idx).(*array.Int64)
		va := r.Column(pidx).(*extensions.VariantArray)
		for i := 0; i < int(r.NumRows()); i++ {
			v, err := va.Value(i)
			if err != nil {
				t.Fatal(err)
			}
			js, err := v.MarshalJSON()
			if err != nil {
				t.Fatal(err)
			}
			var decoded any
			if err := json.Unmarshal(js, &decoded); err != nil {
				t.Fatal(err)
			}
			got[ids.Value(i)] = decoded
		}
		r.Release()
	}
	want := map[int64]any{}
	for id, p := range payloads {
		want[id] = p
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("payload variants\n got %v\nwant %v\nextractor output:\n%s", got, want, out.String())
	}
}
