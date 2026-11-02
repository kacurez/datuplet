//go:build integration

package goplane

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/catalog"

	"github.com/datuplet/datuplet/pkg/datagateway"
	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

// runTokenIssuer signs RS256 run tokens and serves its JWKS, standing in for
// pipeline-api so a real ServerV2 can validate a run token at boot.
type runTokenIssuer struct {
	key  *rsa.PrivateKey
	kid  string
	jwks *httptest.Server
}

func newRunTokenIssuer(t *testing.T) *runTokenIssuer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	iss := &runTokenIssuer{key: key, kid: "gw-it-key"}
	doc, _ := json.Marshal(map[string]any{"keys": []map[string]string{{
		"kty": "RSA",
		"kid": iss.kid,
		"n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}}})
	iss.jwks = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(doc)
	}))
	t.Cleanup(iss.jwks.Close)
	return iss
}

// mint returns an RS256 JWT with exactly the claims the gateway's run-token
// validator requires.
func (iss *runTokenIssuer) mint(t *testing.T, runID, projectID, warehouse string) string {
	t.Helper()
	now := time.Now()
	header, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": iss.kid})
	claims, _ := json.Marshal(map[string]any{
		"iss":        "datuplet-api",
		"aud":        "datuplet-catalog",
		"token_kind": "run",
		"sub":        runID,
		"run_id":     runID,
		"project_id": projectID,
		"warehouse":  warehouse,
		"iat":        now.Unix(),
		"nbf":        now.Unix(),
		"exp":        now.Add(time.Hour).Unix(),
	})
	signing := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, iss.key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// serverTestBatch returns n rows (id, name, ts) starting at id start; every
// value is in range for an iceberg-go scan.
func serverTestBatch(t *testing.T, sch *arrow.Schema, start, n int) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(memory.NewGoAllocator(), sch)
	defer b.Release()
	ids := b.Field(0).(*array.Int64Builder)
	names := b.Field(1).(*array.StringBuilder)
	tss := b.Field(2).(*array.TimestampBuilder)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	for i := start; i < start+n; i++ {
		ids.Append(int64(i))
		names.Append(fmt.Sprintf("name-%d", i))
		tss.Append(arrow.Timestamp(base + int64(i)))
	}
	return b.NewRecordBatch()
}

func serverIPCBytes(t *testing.T, rec arrow.RecordBatch) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestIntegrationServerWriteEndToEnd drives a real ServerV2 on the Go plane:
// a run token validated against a JWKS, Arrow IPC chunks written over HTTP
// (a small target file size forces rotation), Commit through the commit pool
// into Lakekeeper, then the rows read back with an iceberg-go scan and the
// snapshot's audit summary checked.
func TestIntegrationServerWriteEndToEnd(t *testing.T) {
	fx := sharedFixture(t)
	const runID = "0123456789abcdef-server-e2e"
	const ns, tbl = "gw_server", "orders"

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
	go func() { served <- srv.ServeWithHTTP(fmt.Sprintf("127.0.0.1:%d", freePort(t))) }()
	t.Cleanup(func() {
		srv.Close()
		<-served
	})
	ctx := context.Background()
	waitHTTP(ctx, t, "http://"+httpAddr+"/health")

	sch := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
	}, nil)

	open, err := srv.OpenWriter(ctx, &pb.OpenWriterRequest{Table: tbl, InputFormat: pb.DataFormat_FORMAT_ARROW_IPC})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	hc := &http.Client{Timeout: 2 * time.Minute}
	const perChunk, chunks = 10000, 5
	for i := 0; i < chunks; i++ {
		rec := serverTestBatch(t, sch, i*perChunk, perChunk)
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
	if closed.TotalRows != perChunk*chunks || closed.FilesWritten < 2 || closed.TotalBytes <= 0 {
		t.Fatalf("CloseWriter = rows %d files %d bytes %d; want %d rows in several files",
			closed.TotalRows, closed.FilesWritten, closed.TotalBytes, perChunk*chunks)
	}
	t.Logf("CloseWriter: %d rows in %d files, %d bytes", closed.TotalRows, closed.FilesWritten, closed.TotalBytes)
	commit, err := srv.Commit(ctx, &pb.CommitRequest{})
	if err != nil || !commit.Success {
		t.Fatalf("Commit = %+v, %v", commit, err)
	}

	cat, err := resolver(t, fx).Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	table, err := cat.LoadTable(ctx, catalog.ToIdentifier(ns, tbl))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	snap := table.CurrentSnapshot()
	if snap == nil || snap.Summary == nil {
		t.Fatal("no current snapshot after Commit")
	}
	if got := snap.Summary.Properties["datuplet.run-id"]; got != runID {
		t.Errorf("snapshot datuplet.run-id = %q, want %q", got, runID)
	}
	if snap.Summary.Properties["datuplet.commit-key"] == "" {
		t.Error("snapshot has no datuplet.commit-key")
	}

	tasks, err := table.Scan().PlanFiles(ctx)
	if err != nil {
		t.Fatalf("PlanFiles: %v", err)
	}
	if len(tasks) != int(closed.FilesWritten) {
		t.Errorf("committed data files = %d, want %d", len(tasks), closed.FilesWritten)
	}
	for _, task := range tasks {
		if p := task.File.FilePath(); !strings.Contains(p, "/part-01234567-") || !strings.HasSuffix(p, ".parquet") {
			t.Errorf("data file %q: want part-<run8>-<writer>-NNNNN.parquet", p)
		}
	}

	scanSchema, recs, err := table.Scan().ToArrowRecords(ctx)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	var gotNames []string
	for _, f := range scanSchema.Fields() {
		gotNames = append(gotNames, f.Name)
	}
	if strings.Join(gotNames, ",") != "id,name,ts" {
		t.Errorf("scan columns = %v, want [id name ts]", gotNames)
	}
	seen := make(map[int64]bool, perChunk*chunks)
	var rows int64
	for rec, err := range recs {
		if err != nil {
			t.Fatalf("scan batch: %v", err)
		}
		ids := rec.Column(0).(*array.Int64)
		names := rec.Column(1).(*array.String)
		for i := 0; i < ids.Len(); i++ {
			id := ids.Value(i)
			if names.Value(i) != fmt.Sprintf("name-%d", id) {
				t.Fatalf("row id %d: name %q", id, names.Value(i))
			}
			seen[id] = true
		}
		rows += rec.NumRows()
		rec.Release()
	}
	if rows != perChunk*chunks || len(seen) != perChunk*chunks {
		t.Errorf("scan rows = %d (distinct ids %d), want %d", rows, len(seen), perChunk*chunks)
	}
	for id := int64(0); id < perChunk*chunks; id++ {
		if !seen[id] {
			t.Fatalf("id %d missing from the scan", id)
		}
	}
}
