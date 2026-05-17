package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFS_Local_ReadFile(t *testing.T) {
	dir := t.TempDir()
	data := []byte(`{"x":1}`)
	if err := os.WriteFile(filepath.Join(dir, "metadata.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	fsys, err := LoadFS(context.Background(), "file://"+dir, nil)
	if err != nil {
		t.Fatalf("LoadFS: %v", err)
	}
	// iceberg-go's LocalFS.Open strips any leading "file://" prefix and then
	// calls os.Open, so either form works. Exercise the scheme-prefixed form
	// so the test catches regressions if that ever changes.
	f, err := fsys.Open("file://" + filepath.Join(dir, "metadata.json"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Errorf("got %q, want %q", string(got), string(data))
	}
}

func TestLoadFS_Local_PlainPath(t *testing.T) {
	dir := t.TempDir()
	data := []byte("hello")
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	fsys, err := LoadFS(context.Background(), "file://"+dir, nil)
	if err != nil {
		t.Fatalf("LoadFS: %v", err)
	}
	// Also verify Open works without the file:// scheme prefix, since the
	// walker in A1-6 will construct paths from the table's metadata URI +
	// relative manifest paths.
	f, err := fsys.Open(filepath.Join(dir, "a.txt"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Errorf("got %q, want %q", string(got), string(data))
	}
}

func TestLoadFS_Scheme_Unsupported(t *testing.T) {
	_, err := LoadFS(context.Background(), "ftp://example.com/x", nil)
	if err == nil {
		t.Fatal("LoadFS on ftp:// should error, got nil")
	}
}

func TestS3Props_Keys(t *testing.T) {
	got := S3Props("minio:9000", "warehouse", "ak", "sk", "", true)
	want := map[string]string{
		"s3.endpoint":                 "http://minio:9000",
		"s3.access-key-id":            "ak",
		"s3.secret-access-key":        "sk",
		"s3.region":                   "us-east-1",
		"s3.force-virtual-addressing": "false", // pathStyle=true -> force-virtual=false
	}
	if len(got) != len(want) {
		t.Fatalf("S3Props map size: got %d, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("S3Props[%q]: got %q, want %q", k, got[k], v)
		}
	}
}

func TestS3Props_EndpointScheme(t *testing.T) {
	// Endpoint already has a scheme -> left as-is.
	got := S3Props("https://s3.eu-central-1.amazonaws.com", "b", "ak", "sk", "eu-central-1", false)
	if got["s3.endpoint"] != "https://s3.eu-central-1.amazonaws.com" {
		t.Errorf("s3.endpoint: got %q, want https://... unchanged", got["s3.endpoint"])
	}
	// pathStyle=false -> force virtual-hosted addressing (AWS default).
	if got["s3.force-virtual-addressing"] != "true" {
		t.Errorf("s3.force-virtual-addressing: got %q, want true", got["s3.force-virtual-addressing"])
	}
}

func TestS3Props_RegionDefault(t *testing.T) {
	p := S3Props("", "b", "k", "s", "", false)
	if p["s3.region"] != "us-east-1" {
		t.Errorf("empty region should default to us-east-1, got %q", p["s3.region"])
	}
}

func TestS3Props_RegionExplicit(t *testing.T) {
	p := S3Props("", "b", "k", "s", "eu-central-1", false)
	if p["s3.region"] != "eu-central-1" {
		t.Errorf("want eu-central-1, got %q", p["s3.region"])
	}
}

func TestS3Props_EmptyEndpointOmitted(t *testing.T) {
	p := S3Props("", "b", "k", "s", "us-west-2", false)
	if _, ok := p["s3.endpoint"]; ok {
		t.Errorf("empty endpoint should not emit s3.endpoint, got %q", p["s3.endpoint"])
	}
}

func TestS3Props_NonEmptyEndpointKept(t *testing.T) {
	p := S3Props("minio:9000", "b", "k", "s", "", true)
	if p["s3.endpoint"] != "http://minio:9000" {
		t.Errorf("want http://minio:9000, got %q", p["s3.endpoint"])
	}
}
