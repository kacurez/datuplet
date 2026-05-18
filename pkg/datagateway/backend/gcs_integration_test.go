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
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	fakeGCSImage = "fsouza/fake-gcs-server:1.49"
	fakeGCSPort  = "4443/tcp"
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
			// -public-host: tell fake-gcs to emit URLs that point back at the
			//   mapped host:port the storage client sees.
			Cmd:        []string{"-scheme", "http", "-public-host", "127.0.0.1"},
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

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("get container host: %v", err)
	}
	port, err := c.MappedPort(ctx, fakeGCSPort)
	if err != nil {
		t.Fatalf("get mapped port: %v", err)
	}
	endpoint := fmt.Sprintf("%s:%s", host, port.Port())

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
	// on storage.NewClient and routes all calls through it. We set with
	// t.Setenv so the original value is restored at test end.
	t.Setenv("STORAGE_EMULATOR_HOST", endpoint)
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
