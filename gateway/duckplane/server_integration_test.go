//go:build integration && duckdb_arrow

package duckplane

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
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/iceberg-go/catalog"

	"github.com/datuplet/datuplet/gateway/duck"
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
	iss := &runTokenIssuer{key: key, kid: "dg-it-key"}
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

// TestIntegrationServerReadParquetEndToEnd drives a real ServerV2 on the
// DuckDB plane: a run token validated against a JWKS, an Arrow IPC write over
// HTTP, Commit through the commit pool into Lakekeeper, and a FORMAT_PARQUET
// read over HTTP staged chunk by chunk to files, as sql-transform does.
func TestIntegrationServerReadParquetEndToEnd(t *testing.T) {
	env := sharedEnv(t)
	const runID = "0123456789abcdef-e2e"
	const ns, tbl = "dg_server", "orders"

	iss := newRunTokenIssuer(t)
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte(iss.mint(t, runID, env.fx.projectID, env.fx.warehouseName)), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RUN_ID", runID)

	httpAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	srv, err := datagateway.NewServerV2(&datagateway.Config{
		RunID:              runID,
		ComponentName:      "dg-it",
		DefaultBucket:      ns,
		DefaultWriteMode:   "APPEND",
		LakekeeperURL:      env.fx.catalogURL(),
		PipelineAPIJWKSURL: iss.jwks.URL,
		RunTokenPath:       tokenPath,
		HTTPAddr:           httpAddr,
		ChunkSize:          64 << 10, // several read chunks
	}, Factory(Config{
		Settings: duck.Settings{
			Threads:        1,
			MemoryLimit:    "1GiB",
			TempDir:        t.TempDir(),
			MaxTempDirSize: "1GiB",
			ExtensionDir:   env.extDir,
		},
		Extensions: []string{"httpfs", "iceberg"},
	}))
	if err != nil {
		t.Fatalf("NewServerV2: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	go srv.ServeWithHTTP(fmt.Sprintf("127.0.0.1:%d", freePort(t)))
	waitHTTP(context.Background(), t, "http://"+httpAddr+"/health")

	ctx := context.Background()
	hc := &http.Client{Timeout: 2 * time.Minute}

	// Write 50 000 rows in five Arrow IPC chunks over HTTP.
	open, err := srv.OpenWriter(ctx, &pb.OpenWriterRequest{Table: tbl, InputFormat: pb.DataFormat_FORMAT_ARROW_IPC})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	const perChunk, chunks = 10000, 5
	for i := 0; i < chunks; i++ {
		rec := idNameBatch(t, i*perChunk, perChunk)
		body := ipcBytes(t, rec)
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
	if err != nil || closed.TotalRows != perChunk*chunks {
		t.Fatalf("CloseWriter = %+v, %v; want %d rows", closed, err, perChunk*chunks)
	}
	commit, err := srv.Commit(ctx, &pb.CommitRequest{})
	if err != nil || !commit.Success {
		t.Fatalf("Commit = %+v, %v", commit, err)
	}

	// The committed snapshot carries the run's audit summary.
	cat, err := env.resolver(t).Catalog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	table, err := cat.LoadTable(ctx, catalog.ToIdentifier(ns, tbl))
	if err != nil {
		t.Fatal(err)
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

	// Read it back as FORMAT_PARQUET over HTTP, one staged file per chunk.
	rd, err := srv.OpenReader(ctx, &pb.OpenReaderRequest{Bucket: ns, Table: tbl, OutputFormat: pb.DataFormat_FORMAT_PARQUET})
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	stage := t.TempDir()
	var files []string
	var headerRows int64
	for i := 0; ; i++ {
		if i > 10000 {
			t.Fatal("no last chunk")
		}
		resp, err := hc.Get(rd.HttpEndpoint)
		if err != nil {
			t.Fatalf("GET chunk %d: %v", i, err)
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("GET chunk %d: HTTP %d %v: %s", i, resp.StatusCode, err, data)
		}
		n, _ := strconv.ParseInt(resp.Header.Get("X-Datuplet-Rows"), 10, 64)
		headerRows += n
		if len(data) > 0 {
			path := filepath.Join(stage, fmt.Sprintf("in_%04d.parquet", i))
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			files = append(files, path)
		}
		if resp.Header.Get("X-Datuplet-Is-Last") == "true" {
			break
		}
	}
	if _, err := srv.CloseReader(ctx, &pb.CloseReaderRequest{ReaderId: rd.ReaderId}); err != nil {
		t.Fatalf("CloseReader: %v", err)
	}
	t.Logf("read %d rows in %d Parquet chunks", headerRows, len(files))
	if len(files) < 2 {
		t.Errorf("chunks = %d, want several at a 64 KiB chunk size", len(files))
	}
	if headerRows != perChunk*chunks {
		t.Errorf("X-Datuplet-Rows sum = %d, want %d", headerRows, perChunk*chunks)
	}

	db, err := duck.Open(duck.Settings{TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	list := ""
	for i, f := range files {
		if i > 0 {
			list += ", "
		}
		list += duck.Literal(f)
	}
	q := "SELECT count(*), count(DISTINCT id), min(id), max(id) FROM read_parquet([" + list + "])"
	rows, err := c.QueryRows(ctx, q)
	if err != nil {
		t.Fatalf("read_parquet: %v", err)
	}
	want := []int64{perChunk * chunks, perChunk * chunks, 0, perChunk*chunks - 1}
	for i, w := range want {
		if got := asInt64(rows[0][i]); got != w {
			t.Errorf("read_parquet column %d = %v, want %d", i, rows[0][i], w)
		}
	}
}

func ipcBytes(t *testing.T, rec arrow.RecordBatch) []byte {
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

func asInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case uint64:
		return int64(n)
	case int32:
		return int64(n)
	default:
		return -1
	}
}
