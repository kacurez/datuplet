package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestGCSPropsShape(t *testing.T) {
	exp := time.Date(2026, 5, 18, 14, 0, 0, 0, time.UTC)
	props := GCSProps("ya29.abc", exp)
	if props["gcs.oauth2.token"] != "ya29.abc" {
		t.Fatalf("token = %q", props["gcs.oauth2.token"])
	}
	if props["gcs.oauth2.token-expires-at"] != strconv.FormatInt(exp.UnixMilli(), 10) {
		t.Fatalf("expires-at = %q, want %q",
			props["gcs.oauth2.token-expires-at"],
			strconv.FormatInt(exp.UnixMilli(), 10))
	}
}

func TestLoadFS_Local_Open(t *testing.T) {
	cases := []struct {
		name string
		data string
		// iceberg-go's LocalFS.Open strips any leading "file://" prefix and
		// then calls os.Open, so either form works. Exercise both so the
		// test catches regressions if that ever changes; the plain form is
		// what the walker builds from a table's metadata URI + relative
		// manifest paths.
		prefix string
	}{
		{"scheme_prefixed", `{"x":1}`, "file://"},
		{"plain_path", "hello", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "f.txt"), []byte(tc.data), 0o644); err != nil {
				t.Fatal(err)
			}
			fsys, err := LoadFS(context.Background(), "file://"+dir, nil)
			if err != nil {
				t.Fatalf("LoadFS: %v", err)
			}
			f, err := fsys.Open(tc.prefix + filepath.Join(dir, "f.txt"))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer f.Close()
			got, err := io.ReadAll(f)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.data {
				t.Errorf("got %q, want %q", string(got), tc.data)
			}
		})
	}
}

// TestLoadFS_Scheme covers LoadFS scheme dispatch. ftp:// must be rejected.
// gs:// must be dispatched to the datupleticeio factory instead of rejected:
// the factory itself may fail at credential-resolution or bucket-open time
// when there is no real GCS backend, so the row asserts only that the error
// does NOT say "unsupported URI scheme" (the dispatch itself broken) or
// "not yet supported" (the Slice D.4 placeholder). The full happy path is
// exercised in the gcs-pipeline-k8s e2e scenario.
func TestLoadFS_Scheme(t *testing.T) {
	cases := []struct {
		name     string
		uri      string
		props    map[string]string
		wantErr  bool // error is mandatory
		rejected bool // error must not be a scheme rejection
	}{
		{"unsupported_ftp", "ftp://example.com/x", nil, true, false},
		{"gs_dispatched", "gs://test-bucket/some/prefix", GCSProps("ya29.test", time.Now().Add(15*time.Minute)), false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadFS(context.Background(), tc.uri, tc.props)
			if tc.wantErr && err == nil {
				t.Fatalf("LoadFS(%s) should error, got nil", tc.uri)
			}
			if tc.rejected && err != nil {
				if strings.Contains(err.Error(), "unsupported URI scheme") {
					t.Fatalf("scheme was rejected by LoadFS instead of dispatched to factory: %v", err)
				}
				if strings.Contains(err.Error(), "not yet supported") {
					t.Fatalf("LoadFS still has the Slice D.4 placeholder rejection: %v", err)
				}
			}
		})
	}
}

func TestS3Props(t *testing.T) {
	cases := []struct {
		name      string
		endpoint  string
		region    string
		pathStyle bool
		// exact, when non-nil, requires the full prop map to equal it.
		exact map[string]string
		// want requires these keys/values to be present.
		want map[string]string
		// absent requires these keys to be missing.
		absent []string
	}{
		{
			name: "keys", endpoint: "minio:9000", region: "", pathStyle: true,
			exact: map[string]string{
				"s3.endpoint":                 "http://minio:9000",
				"s3.access-key-id":            "ak",
				"s3.secret-access-key":        "sk",
				"s3.region":                   "us-east-1",
				"s3.force-virtual-addressing": "false", // pathStyle=true -> force-virtual=false
			},
		},
		{
			// Endpoint already has a scheme -> left as-is; pathStyle=false ->
			// force virtual-hosted addressing (AWS default).
			name: "endpoint_scheme_kept", endpoint: "https://s3.eu-central-1.amazonaws.com", region: "eu-central-1", pathStyle: false,
			want: map[string]string{
				"s3.endpoint":                 "https://s3.eu-central-1.amazonaws.com",
				"s3.force-virtual-addressing": "true",
			},
		},
		{name: "region_default", endpoint: "", region: "", want: map[string]string{"s3.region": "us-east-1"}},
		{name: "region_explicit", endpoint: "", region: "eu-central-1", want: map[string]string{"s3.region": "eu-central-1"}},
		{name: "empty_endpoint_omitted", endpoint: "", region: "us-west-2", absent: []string{"s3.endpoint"}},
		{name: "nonempty_endpoint_kept", endpoint: "minio:9000", region: "", pathStyle: true, want: map[string]string{"s3.endpoint": "http://minio:9000"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := S3Props(tc.endpoint, "warehouse", "ak", "sk", tc.region, tc.pathStyle)
			if tc.exact != nil {
				if len(got) != len(tc.exact) {
					t.Fatalf("S3Props map size: got %d, want %d", len(got), len(tc.exact))
				}
				for k, v := range tc.exact {
					if got[k] != v {
						t.Errorf("S3Props[%q]: got %q, want %q", k, got[k], v)
					}
				}
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("S3Props[%q]: got %q, want %q", k, got[k], v)
				}
			}
			for _, k := range tc.absent {
				if v, ok := got[k]; ok {
					t.Errorf("S3Props should not emit %q, got %q", k, v)
				}
			}
		})
	}
}
