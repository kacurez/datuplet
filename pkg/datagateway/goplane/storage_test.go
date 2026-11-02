package goplane

import (
	"context"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

// TestAbortWithoutCancel: with the middleware, AbortMultipartUpload reaches
// the server although its context is already cancelled (as it is when the
// transfer manager aborts an upload the writer cancelled); other operations
// still honour cancellation.
func TestAbortWithoutCancel(t *testing.T) {
	var aborts, others atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && r.URL.Query().Has("uploadId") {
			aborts.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		others.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("k", "s", ""),
		APIOptions:   []func(*middleware.Stack) error{abortWithoutCancel},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket: aws.String("b"), Key: aws.String("k"), UploadId: aws.String("u1"),
	}); err != nil {
		t.Fatalf("AbortMultipartUpload on a cancelled context: %v", err)
	}
	if aborts.Load() != 1 {
		t.Fatalf("server saw %d aborts", aborts.Load())
	}
	if _, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("b"), Key: aws.String("k")}); err == nil {
		t.Fatal("HeadObject ignored the cancelled context")
	}
	if others.Load() != 0 {
		t.Fatalf("server saw %d other requests", others.Load())
	}
}

func TestIOProps(t *testing.T) {
	exp := time.UnixMilli(1_900_000_000_000)
	s3 := func(endpoint, region string) catalogwriter.S3Creds {
		return catalogwriter.S3Creds{
			AccessKeyID: "AKIA-test", SecretAccessKey: "SECRET-VALUE", SessionToken: "SESSION-VALUE",
			Region: region, Endpoint: endpoint, Issued: exp.Add(-time.Hour), Expires: exp,
		}
	}
	s3Base := func(region string) map[string]string {
		return map[string]string{
			"s3.access-key-id":     "AKIA-test",
			"s3.secret-access-key": "SECRET-VALUE",
			"s3.session-token":     "SESSION-VALUE",
			"s3.region":            region,
		}
	}
	with := func(m map[string]string, kv ...string) map[string]string {
		out := maps.Clone(m)
		for i := 0; i < len(kv); i += 2 {
			out[kv[i]] = kv[i+1]
		}
		return out
	}
	cases := []struct {
		name  string
		creds catalogwriter.Creds
		want  map[string]string
	}{
		{"minio endpoint path style", s3("http://minio.datuplet.svc:9000", "local-01"),
			with(s3Base("local-01"), "s3.endpoint", "http://minio.datuplet.svc:9000", "s3.force-virtual-addressing", "false")},
		{"aws regional endpoint virtual", s3("https://s3.eu-west-1.amazonaws.com", "eu-west-1"),
			with(s3Base("eu-west-1"), "s3.endpoint", "https://s3.eu-west-1.amazonaws.com", "s3.force-virtual-addressing", "true")},
		{"bare amazonaws.com virtual", s3("https://amazonaws.com", "eu-west-1"),
			with(s3Base("eu-west-1"), "s3.endpoint", "https://amazonaws.com", "s3.force-virtual-addressing", "true")},
		{"amazonaws look-alike path style", s3("https://x.amazonaws.com.evil.io", "eu-west-1"),
			with(s3Base("eu-west-1"), "s3.endpoint", "https://x.amazonaws.com.evil.io", "s3.force-virtual-addressing", "false")},
		{"empty region defaults", s3("http://127.0.0.1:9000", ""),
			with(s3Base("us-east-1"), "s3.endpoint", "http://127.0.0.1:9000", "s3.force-virtual-addressing", "false")},
		{"no endpoint is AWS default virtual", s3("", "eu-central-1"),
			with(s3Base("eu-central-1"), "s3.force-virtual-addressing", "true")},
		{"gcs token", catalogwriter.GCSCreds{OAuthToken: "GCS-TOKEN-VALUE", GCPProjectID: "proj-1", Expires: exp},
			map[string]string{
				"gcs.oauth2.token":            "GCS-TOKEN-VALUE",
				"gcs.oauth2.token-expires-at": "1900000000000",
				"gcs.project-id":              "proj-1",
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ioProps(tc.creds)
			if err != nil {
				t.Fatalf("ioProps: %v", err)
			}
			if !maps.Equal(got, tc.want) {
				t.Fatalf("props mismatch:\n got  %v\n want %v", keysOnly(got), keysOnly(tc.want))
			}
		})
	}

	secrets := []string{"SECRET-VALUE", "SESSION-VALUE", "GCS-TOKEN-VALUE"}
	bad := []struct {
		name  string
		creds catalogwriter.Creds
	}{
		{"nil creds", nil},
		{"malformed endpoint", s3("http://[::1", "r")},
		{"endpoint without host", s3("http://", "r")},
		{"s3 without key", catalogwriter.S3Creds{SecretAccessKey: "SECRET-VALUE", SessionToken: "SESSION-VALUE"}},
		{"gcs without token", catalogwriter.GCSCreds{GCPProjectID: "p"}},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ioProps(tc.creds)
			if err == nil {
				t.Fatalf("ioProps accepted %s: %v", tc.name, keysOnly(got))
			}
			for _, s := range secrets {
				if strings.Contains(err.Error(), s) {
					t.Fatalf("error leaks a secret: %v", err)
				}
			}
		})
	}
}

// keysOnly renders props without secret values for failure messages.
func keysOnly(m map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		switch k {
		case "s3.secret-access-key", "s3.session-token", "gcs.oauth2.token":
			v = "<redacted>"
		}
		out[k] = v
	}
	return out
}

func TestCredsValidity(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name            string
		issued, expires time.Time
		want            bool
	}{
		{"no expiry passes", time.Time{}, time.Time{}, true},
		{"1h ttl 59m left", now.Add(-time.Minute), now.Add(59 * time.Minute), true},
		{"1h ttl 1m left", now.Add(-59 * time.Minute), now.Add(time.Minute), false},
		{"10h ttl 3h left", now.Add(-7 * time.Hour), now.Add(3 * time.Hour), true},
		{"10h ttl 2h left", now.Add(-8 * time.Hour), now.Add(2 * time.Hour), false},
		{"no issued, 3m left", time.Time{}, now.Add(3 * time.Minute), true},
		{"expired", now.Add(-2 * time.Hour), now.Add(-time.Hour), false},
	}
	for _, tc := range cases {
		c := catalogwriter.S3Creds{Issued: tc.issued, Expires: tc.expires}
		if got := validFor(c, now); got != tc.want {
			t.Errorf("%s: validFor = %v, want %v", tc.name, got, tc.want)
		}
	}
}
