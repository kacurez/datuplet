//go:build integration

package backend

// Integration tests for GCSBackend against a real fake-gcs-server container.
//
// Run with:
//   go test -v -tags=integration ./pkg/datagateway/backend/... -run TestGCSBackend
//
// These tests require Docker. They are excluded from the default
// `go test ./...` run by the `integration` build tag.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"
	mobynet "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	fakeGCSImage = "fsouza/fake-gcs-server:1.49"
	fakeGCSPort  = "4443/tcp"
	// fakeGCSHostPort is the fixed host-side port the container is bound to.
	// fake-gcs hardcodes its internal bind port (4443) into the selfLink /
	// mediaLink URLs it emits in JSON responses, and the cloud.google.com/go
	// storage SDK follows those URLs for download endpoints. With a random
	// testcontainers-mapped port the emitted URLs would be unreachable from
	// the test process; with a fixed host-port + `-public-host` matching it,
	// the URLs are reachable. Tests run sequentially (no t.Parallel), so a
	// fixed host port doesn't conflict.
	fakeGCSHostPort = "14443"
)

// startFakeGCS starts a fake-gcs-server container, creates the requested
// bucket via the JSON-API, sets the STORAGE_EMULATOR_HOST env var so the
// cloud.google.com/go/storage client routes through it, and registers
// cleanup on t.Cleanup. Returns the bucket name.
//
// The fake-gcs-server image is the same one verified against in Slice A0
// probe 3 (RFC 019).
func startFakeGCS(t *testing.T, bucket string) string {
	t.Helper()
	ctx := context.Background()

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        fakeGCSImage,
			ExposedPorts: []string{fakeGCSPort},
			// -scheme http: serve plain HTTP so the storage client can hit it
			//   without TLS plumbing.
			// -public-host: tells fake-gcs to emit URLs (selfLink, mediaLink)
			//   pointing at the FIXED host port we bind below. Required because
			//   the SDK follows mediaLink for downloads — a hardcoded internal
			//   port would be unreachable through testcontainers' default
			//   random port mapping.
			Cmd: []string{"-scheme", "http", "-public-host", "127.0.0.1:" + fakeGCSHostPort},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.PortBindings = mobynet.PortMap{
					mobynet.MustParsePort(fakeGCSPort): []mobynet.PortBinding{
						{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: fakeGCSHostPort},
					},
				}
			},
			WaitingFor: wait.ForHTTP("/storage/v1/b").WithPort(fakeGCSPort),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start fake-gcs-server container: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Terminate(ctx); err != nil {
			t.Logf("warn: terminate fake-gcs container: %v", err)
		}
	})

	// The container is bound to a fixed host port (fakeGCSHostPort) per
	// the PortBindings above, so we don't need to consult c.MappedPort.
	// We still verify the container started before constructing the
	// endpoint, by checking c.Host returns without error.
	if _, err := c.Host(ctx); err != nil {
		t.Fatalf("get container host: %v", err)
	}
	endpoint := "127.0.0.1:" + fakeGCSHostPort

	// Create the bucket via the JSON API.
	createURL := fmt.Sprintf("http://%s/storage/v1/b?project=datuplet-test", endpoint)
	body := strings.NewReader(fmt.Sprintf(`{"name":%q}`, bucket))
	resp, err := http.Post(createURL, "application/json", body)
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusConflict {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("create bucket: status %d, body: %s", resp.StatusCode, string(b))
	}

	// Point the storage client at the emulator. The SDK reads this env var
	// on storage.NewClient and routes all calls through it. The scheme
	// prefix is REQUIRED — without it the SDK silently falls back to
	// real storage.googleapis.com routing for some endpoints (e.g. range
	// reads), which produces a confusing "object doesn't exist" right
	// after a "successful" PutObject. We set with t.Setenv so the
	// original value is restored at test end.
	t.Setenv("STORAGE_EMULATOR_HOST", "http://"+endpoint)
	return bucket
}

func TestGCSBackendPutGetObjectRoundTrip(t *testing.T) {
	bucket := startFakeGCS(t, "datuplet-roundtrip")

	be, err := NewGCSBackend(GCSConfig{Bucket: bucket})
	if err != nil {
		t.Fatalf("NewGCSBackend: %v", err)
	}
	defer be.Close()

	ctx := context.Background()
	payload := []byte("hello from gcs round-trip")
	if err := be.PutObject(ctx, "round-trip.txt", payload); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	got, err := be.GetObject(ctx, "round-trip.txt")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("got %q, want %q", got, payload)
	}
}

func TestGCSBackendPutGetObjectGSURL(t *testing.T) {
	bucket := startFakeGCS(t, "datuplet-gsurl")
	be, err := NewGCSBackend(GCSConfig{Bucket: bucket})
	if err != nil {
		t.Fatalf("NewGCSBackend: %v", err)
	}
	defer be.Close()

	ctx := context.Background()
	payload := []byte("hello gs:// URL")
	gsURL := fmt.Sprintf("gs://%s/nested/path/data.txt", bucket)
	if err := be.PutObject(ctx, gsURL, payload); err != nil {
		t.Fatalf("PutObject (gs://): %v", err)
	}
	got, err := be.GetObject(ctx, gsURL)
	if err != nil {
		t.Fatalf("GetObject (gs://): %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("got %q, want %q", got, payload)
	}
}

// TestGCSBackendRollbackDeletesStagedFiles verifies that Rollback deletes
// the part files a writer accumulated in filePaths. We populate the
// bucket directly (via PutObject) to avoid coupling to OpenWriter's
// internals, hand the writer's filePaths to a fresh writer, then call
// Rollback and confirm the objects are gone.
func TestGCSBackendRollbackDeletesStagedFiles(t *testing.T) {
	bucket := startFakeGCS(t, "datuplet-rollback")
	be, err := NewGCSBackend(GCSConfig{Bucket: bucket})
	if err != nil {
		t.Fatalf("NewGCSBackend: %v", err)
	}
	defer be.Close()
	ctx := context.Background()

	// Stage two parts directly.
	paths := []string{"tables/x/part-00000.csv", "tables/x/part-00001.csv"}
	for _, p := range paths {
		if err := be.PutObject(ctx, p, []byte("hello")); err != nil {
			t.Fatalf("seed PutObject(%q): %v", p, err)
		}
	}

	// Confirm they exist.
	for _, p := range paths {
		if _, err := be.GetObject(ctx, p); err != nil {
			t.Fatalf("pre-rollback get %q: %v", p, err)
		}
	}

	// Rollback the equivalent gcsWriter. NewGCSBackend currently returns
	// *gcsBackend (Slice B staged-landing concession); after Slice D the
	// constructors flip to StorageBackend and this no longer needs the
	// concrete type.
	gw := &gcsWriter{filePaths: paths}
	if err := be.Rollback(ctx, []Writer{gw}); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Confirm they're gone.
	for _, p := range paths {
		if _, err := be.GetObject(ctx, p); err == nil {
			t.Errorf("post-rollback get %q: object still exists", p)
		}
	}
}

func TestGCSBackendGetSchemaCSV(t *testing.T) {
	bucket := startFakeGCS(t, "datuplet-schema")
	be, err := NewGCSBackend(GCSConfig{Bucket: bucket})
	if err != nil {
		t.Fatalf("NewGCSBackend: %v", err)
	}
	defer be.Close()

	ctx := context.Background()
	if err := be.PutObject(ctx, "t/data.csv", []byte("name,age,city\n")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	schema, err := be.GetSchema(ctx, "t")
	if err != nil {
		t.Fatalf("GetSchema: %v", err)
	}
	if len(schema.Columns) != 3 {
		t.Fatalf("columns = %d, want 3", len(schema.Columns))
	}
	wantNames := []string{"name", "age", "city"}
	for i, want := range wantNames {
		if schema.Columns[i].Name != want {
			t.Errorf("Columns[%d].Name = %q, want %q", i, schema.Columns[i].Name, want)
		}
		if schema.Columns[i].Type != "string" {
			t.Errorf("Columns[%d].Type = %q, want string", i, schema.Columns[i].Type)
		}
	}
}

func TestGCSBackendGetSampleCSV(t *testing.T) {
	bucket := startFakeGCS(t, "datuplet-sample")
	be, err := NewGCSBackend(GCSConfig{Bucket: bucket})
	if err != nil {
		t.Fatalf("NewGCSBackend: %v", err)
	}
	defer be.Close()

	ctx := context.Background()
	csvBody := "name,age\nAlice,30\nBob,25\nCarol,40\n"
	if err := be.PutObject(ctx, "t/data.csv", []byte(csvBody)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	sample, err := be.GetSample(ctx, "t", 2)
	if err != nil {
		t.Fatalf("GetSample: %v", err)
	}
	if len(sample.Rows) != 2 {
		t.Fatalf("Rows = %d, want 2 (limit honoured)", len(sample.Rows))
	}
	// Verify each row is JSON with the expected keys.
	for i, row := range sample.Rows {
		s := string(row)
		if !strings.Contains(s, `"name"`) || !strings.Contains(s, `"age"`) {
			t.Errorf("Rows[%d] = %q, missing expected keys", i, s)
		}
	}
}

func TestGCSBackendOpenWriterOpenReaderCSV(t *testing.T) {
	bucket := startFakeGCS(t, "datuplet-writer-reader")
	be, err := NewGCSBackend(GCSConfig{Bucket: bucket})
	if err != nil {
		t.Fatalf("NewGCSBackend: %v", err)
	}
	defer be.Close()

	ctx := context.Background()
	tablePath := "tables/people"

	// OpenWriter -> WriteChunk(CSV with header + 2 rows) -> Close.
	w, err := be.OpenWriter(ctx, tablePath, WriteOptions{
		OutputName: "people",
		Format:     "csv",
	})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	csvBytes := []byte("name,age\nAlice,30\nBob,25\n")
	if err := w.WriteChunk(csvBytes, 2); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer.Close: %v", err)
	}

	stats := w.Stats()
	if stats.RowsWritten != 2 {
		t.Errorf("RowsWritten = %d, want 2", stats.RowsWritten)
	}
	if len(stats.FilePaths) != 1 {
		t.Errorf("FilePaths = %v, want 1 entry", stats.FilePaths)
	}

	// OpenReader -> ReadChunk -> verify content.
	r, err := be.OpenReader(ctx, tablePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	chunk, err := r.ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if chunk.Format != "csv" {
		t.Errorf("chunk.Format = %q, want csv", chunk.Format)
	}
	if chunk.RowsInChunk != 2 {
		t.Errorf("RowsInChunk = %d, want 2", chunk.RowsInChunk)
	}
	if !strings.Contains(string(chunk.Data), "Alice,30") || !strings.Contains(string(chunk.Data), "Bob,25") {
		t.Errorf("chunk.Data = %q, missing expected rows", chunk.Data)
	}
	if !chunk.IsLast {
		t.Errorf("expected IsLast=true for single-file table")
	}
}
