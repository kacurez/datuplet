// Spike harness for RFC 019 Slice A0 acceptance criteria.
// Throwaway — deleted in Slice I. Findings recorded in
// docs/tmp/spikes/2026-05-18-iceberg-gcs-factory.md.
//
// Usage:
//
//	go run ./_spike/iceberg_gcs_factory \
//	  -criterion=1|2|3|4|5|all \
//	  -gcs-bucket=<bucket> -gcs-key-file=<path-to-sa-key.json> \
//	  -lakekeeper-url=<url> -warehouse=<name>
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	iceio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud" // registers the default gs:// factory

	"cloud.google.com/go/storage"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

func main() {
	criterion := flag.String("criterion", "all", "which criterion to verify (1|2|3|4|5|all)")
	bucket := flag.String("gcs-bucket", "", "GCS bucket (must already exist)")
	keyFile := flag.String("gcs-key-file", "", "Path to a GCP SA key JSON (used to mint OAuth bearer)")
	lakekeeperURL := flag.String("lakekeeper-url", "", "Lakekeeper REST URL")
	warehouse := flag.String("warehouse", "", "Warehouse name in Lakekeeper")
	flag.Parse()
	if *bucket == "" {
		log.Fatalf("--gcs-bucket required")
	}
	ctx := context.Background()

	results := map[string]string{}
	run := func(name string, fn func(context.Context) error) {
		if *criterion != "all" && *criterion != name {
			return
		}
		if err := fn(ctx); err != nil {
			results[name] = fmt.Sprintf("FAIL: %v", err)
		} else {
			results[name] = "PASS"
		}
	}

	run("1", func(c context.Context) error { return probeRegistrationOverride(c, *bucket, *keyFile) })
	run("2", func(c context.Context) error { return probeRefreshingTokenSource(c, *bucket, *keyFile) })
	run("3", func(c context.Context) error { return probeFakeGCSServerBearer(c) })
	run("4", func(c context.Context) error { return probeLakekeeperRefreshEndpoint(c, *lakekeeperURL, *warehouse) })
	run("5", func(c context.Context) error { return probeAuditAttribution(c, *bucket, *keyFile) })

	for k, v := range results {
		fmt.Printf("criterion %s: %s\n", k, v)
	}
	for _, v := range results {
		if v != "PASS" && v != "" {
			os.Exit(1)
		}
	}
}

// nopIO is the minimal iceio.IO implementation needed by the probe.
// The real IO interface uses no context on Open/Remove.
type nopIO struct{}

func (nopIO) Open(_ string) (iceio.File, error) { return nil, nil }
func (nopIO) Remove(_ string) error             { return nil }

func probeRegistrationOverride(ctx context.Context, bucket, keyFile string) error {
	// RFC §4.5.4 criterion 1: iceio.Unregister("gs") + iceio.Register("gs", ...)
	// must succeed without panic and the new factory must be the one consulted
	// by subsequent iceio.LoadFS calls.

	// Step 1a: load a static OAuth token so we can pass it as a prop and
	// verify the factory receives it correctly.
	tok, err := loadStaticOAuthToken(ctx, keyFile)
	if err != nil {
		return fmt.Errorf("load static oauth from key: %w", err)
	}

	uri := fmt.Sprintf("gs://%s", bucket)
	props := map[string]string{"gcs.oauth2.token": tok}

	// Step 1b: try to register "gs" again — it was already registered by the
	// blank import above. Expect a panic.
	panicked := func() (p bool) {
		defer func() { p = recover() != nil }()
		iceio.Register("gs", func(_ context.Context, _ *url.URL, _ map[string]string) (iceio.IO, error) {
			return nil, nil
		})
		return
	}()
	if !panicked {
		return fmt.Errorf("Register did NOT panic on duplicate — RFC §4.5.1 premise is wrong; review upstream change-log")
	}

	// Step 1c: Unregister, then Register the probe's own factory. This is
	// exactly what Slice D's production override will do.
	iceio.Unregister("gs")
	called := false
	iceio.Register("gs", func(_ context.Context, parsed *url.URL, gotProps map[string]string) (iceio.IO, error) {
		called = true
		if gotProps["gcs.oauth2.token"] != tok {
			return nil, fmt.Errorf("factory received props missing gcs.oauth2.token")
		}
		if parsed.Host != bucket {
			return nil, fmt.Errorf("factory received wrong bucket in URL: host=%q want %q", parsed.Host, bucket)
		}
		return &nopIO{}, nil
	})

	if _, err := iceio.LoadFS(ctx, props, uri); err != nil {
		return fmt.Errorf("LoadFS after override returned error: %w", err)
	}
	if !called {
		return fmt.Errorf("LoadFS did NOT invoke the new factory — registry override failed silently")
	}
	return nil
}

// loadStaticOAuthToken mints a short-lived access token from a GCP SA key file.
func loadStaticOAuthToken(ctx context.Context, keyFile string) (string, error) {
	if keyFile == "" {
		return "", fmt.Errorf("--gcs-key-file required for criteria 1/2/5")
	}
	keyBytes, err := os.ReadFile(keyFile)
	if err != nil {
		return "", err
	}
	cfg, err := google.JWTConfigFromJSON(keyBytes, storage.ScopeReadWrite)
	if err != nil {
		return "", err
	}
	src := cfg.TokenSource(ctx)
	t, err := src.Token()
	if err != nil {
		return "", err
	}
	return t.AccessToken, nil
}

// ---- Probe 2: refreshing TokenSource lifecycle ----

// countingTokenSource returns tokens with a 10s expiry and increments *calls
// on every Token() invocation.
type countingTokenSource struct {
	inner oauth2.TokenSource // real credentials
	calls *int
}

func (c *countingTokenSource) Token() (*oauth2.Token, error) {
	*c.calls++
	t, err := c.inner.Token()
	if err != nil {
		return nil, err
	}
	// Override the expiry to 10s so the reuseSource refreshes after each sleep.
	t.Expiry = time.Now().Add(10 * time.Second)
	return t, nil
}

// gcsFile satisfies iceio.File (fs.File + io.ReadSeekCloser + io.ReaderAt)
// by eagerly reading the full object content into memory. This is fine for
// the probe since we only write tiny test payloads.
type gcsFile struct {
	data []byte // full object content
	off  int    // current read position
	name string
	size int64
}

func (f *gcsFile) Read(p []byte) (int, error) {
	if f.off >= len(f.data) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.off:])
	f.off += n
	return n, nil
}

func (f *gcsFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *gcsFile) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = int64(f.off) + offset
	case io.SeekEnd:
		abs = f.size + offset
	default:
		return 0, fmt.Errorf("gcsFile: invalid whence %d", whence)
	}
	if abs < 0 {
		return 0, fmt.Errorf("gcsFile: negative seek position")
	}
	f.off = int(abs)
	return abs, nil
}

func (f *gcsFile) Close() error { return nil }

func (f *gcsFile) Stat() (fs.FileInfo, error) {
	return &gcsFileInfo{name: f.name, size: f.size}, nil
}

type gcsFileInfo struct {
	name string
	size int64
}

func (fi *gcsFileInfo) Name() string      { return fi.name }
func (fi *gcsFileInfo) Size() int64       { return fi.size }
func (fi *gcsFileInfo) Mode() fs.FileMode { return 0444 }
func (fi *gcsFileInfo) ModTime() time.Time { return time.Time{} }
func (fi *gcsFileInfo) IsDir() bool       { return false }
func (fi *gcsFileInfo) Sys() any          { return nil }

// gcsIO implements iceio.IO backed by a gocloud *blob.Bucket.
// It also exposes Write for the probe's putAndGet helper.
type gcsIO struct {
	ctx    context.Context
	bucket *blob.Bucket
	prefix string // bucket name — used to strip from full URIs
}

func (g *gcsIO) Open(name string) (iceio.File, error) {
	key, err := gcsKeyFromURI(name, g.prefix)
	if err != nil {
		return nil, err
	}
	r, err := g.bucket.NewReader(g.ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return &gcsFile{data: data, name: name, size: int64(len(data))}, nil
}

func (g *gcsIO) Remove(name string) error {
	key, err := gcsKeyFromURI(name, g.prefix)
	if err != nil {
		return err
	}
	return g.bucket.Delete(g.ctx, key)
}

// Write writes data to the given full URI.
func (g *gcsIO) Write(ctx context.Context, uri string, data []byte) error {
	key, err := gcsKeyFromURI(uri, g.prefix)
	if err != nil {
		return err
	}
	return g.bucket.WriteAll(ctx, key, data, nil)
}

// gcsKeyFromURI strips "gs://bucket/" from a full URI.
func gcsKeyFromURI(uri, bucket string) (string, error) {
	prefix := "gs://" + bucket + "/"
	if !strings.HasPrefix(uri, prefix) {
		// Maybe it's already a bare key
		return uri, nil
	}
	key := strings.TrimPrefix(uri, prefix)
	if key == "" {
		return "", fmt.Errorf("gcsKeyFromURI: empty key in %q", uri)
	}
	return key, nil
}

// openGCSWithTokenSource builds a gcsIO backed by the given oauth2.TokenSource.
func openGCSWithTokenSource(ctx context.Context, ts oauth2.TokenSource, bucket string) (*gcsIO, error) {
	gcpClient, err := gcp.NewHTTPClient(gcp.DefaultTransport(), gcp.TokenSource(ts))
	if err != nil {
		return nil, fmt.Errorf("gcp.NewHTTPClient: %w", err)
	}
	b, err := gcsblob.OpenBucket(ctx, gcpClient, bucket, nil)
	if err != nil {
		return nil, fmt.Errorf("gcsblob.OpenBucket: %w", err)
	}
	return &gcsIO{ctx: ctx, bucket: b, prefix: bucket}, nil
}

// putAndGet writes payload to uri via gcsIO.Write, then reads it back via
// iceio.IO.Open and verifies the content matches.
func putAndGet(ctx context.Context, iofs iceio.IO, uri, payload string) error {
	g, ok := iofs.(*gcsIO)
	if !ok {
		return fmt.Errorf("putAndGet: expected *gcsIO, got %T", iofs)
	}
	if err := g.Write(ctx, uri, []byte(payload)); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	f, err := iofs.Open(uri)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer f.Close()
	got, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if !bytes.Equal(got, []byte(payload)) {
		return fmt.Errorf("content mismatch: got %q want %q", got, payload)
	}
	return nil
}

func probeRefreshingTokenSource(ctx context.Context, bucket, keyFile string) error {
	// RFC §4.5.4 criterion 2: a TokenSource whose tokens expire every 10s must
	// be re-called by iceberg-go's underlying HTTP transport on every refresh
	// cycle. We write+read 3 times with 12s gaps so the token ages past its
	// window between iterations. After the loop, Token() must have been called
	// ≥3 times — otherwise the client is caching the first token.

	keyBytes, err := os.ReadFile(keyFile)
	if err != nil {
		return fmt.Errorf("read key file: %w", err)
	}
	cfg, err := google.JWTConfigFromJSON(keyBytes, storage.ScopeReadWrite)
	if err != nil {
		return fmt.Errorf("JWTConfigFromJSON: %w", err)
	}
	innerSrc := cfg.TokenSource(ctx)

	calls := 0
	inner := &countingTokenSource{inner: innerSrc, calls: &calls}
	// ReuseTokenSourceWithExpiry ensures Token() is only called when the
	// cached token is within the expiry window (10s here — matches what
	// countingTokenSource stamps on each returned token).
	ts := oauth2.ReuseTokenSourceWithExpiry(nil, inner, 10*time.Second)

	// Override gs:// with our factory so every iceio.LoadFS call instantiates
	// a client bound to our counting TokenSource.
	iceio.Unregister("gs")
	iceio.Register("gs", func(c context.Context, parsed *url.URL, props map[string]string) (iceio.IO, error) {
		return openGCSWithTokenSource(c, ts, parsed.Host)
	})

	uri := fmt.Sprintf("gs://%s/_spike_probe/file.txt", bucket)
	iofs, err := iceio.LoadFS(ctx, nil, fmt.Sprintf("gs://%s", bucket))
	if err != nil {
		return fmt.Errorf("iceio.LoadFS: %w", err)
	}

	defer func() {
		// Best-effort cleanup of the spike test object.
		_ = iofs.Remove(uri)
	}()

	log.Printf("probe 2: starting 3-iteration write/read loop (12s sleep between iterations)…")
	for i := 0; i < 3; i++ {
		log.Printf("probe 2: iteration %d — Token() calls so far: %d", i, calls)
		if err := putAndGet(ctx, iofs, uri, fmt.Sprintf("hello %d", i)); err != nil {
			return fmt.Errorf("iter %d: %w", i, err)
		}
		if i < 2 {
			log.Printf("probe 2: sleeping 12s so the 10s token window expires…")
			time.Sleep(12 * time.Second)
		}
	}

	log.Printf("probe 2: loop done — Token() was called %d time(s)", calls)
	if calls < 3 {
		return fmt.Errorf("TokenSource.Token() called only %d time(s); iceberg-go / gocloud is caching the token beyond the window", calls)
	}
	return nil
}

func probeFakeGCSServerBearer(ctx context.Context) error {
	// RFC §4.5.4 criterion 3: does fake-gcs-server accept (or fake-validate)
	// Authorization: Bearer <tok> headers?
	// We test two GET requests (with and without bearer) and then a bucket
	// create + object PUT with a bearer, to ensure writes also work.
	const endpoint = "http://localhost:4443"

	// Part A: GET /storage/v1/b with-bearer and no-bearer both must return <500.
	for _, label := range []string{"with-bearer", "no-bearer"} {
		req, _ := http.NewRequestWithContext(ctx, "GET", endpoint+"/storage/v1/b", nil)
		if label == "with-bearer" {
			req.Header.Set("Authorization", "Bearer dummy")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		resp.Body.Close()
		if resp.StatusCode >= 500 {
			return fmt.Errorf("%s: status %d", label, resp.StatusCode)
		}
		log.Printf("probe 3: GET listing %s → %d", label, resp.StatusCode)
	}

	// Part B: create a bucket and PUT an object with a bearer header.
	bucket := "spike-probe-bucket"
	createReq, _ := http.NewRequestWithContext(ctx, "POST",
		endpoint+"/storage/v1/b?project=spike",
		strings.NewReader(`{"name":"`+bucket+`"}`))
	createReq.Header.Set("Content-Type", "application/json")
	createReq.Header.Set("Authorization", "Bearer dummy")
	cr, err := http.DefaultClient.Do(createReq)
	if err != nil {
		return fmt.Errorf("create-bucket: %w", err)
	}
	cr.Body.Close()
	// 200 (created) and 409 (already exists) are both acceptable.
	if cr.StatusCode >= 500 {
		return fmt.Errorf("create-bucket: status %d", cr.StatusCode)
	}
	log.Printf("probe 3: create-bucket → %d", cr.StatusCode)

	putReq, _ := http.NewRequestWithContext(ctx, "POST",
		endpoint+"/upload/storage/v1/b/"+bucket+"/o?uploadType=media&name=probe.txt",
		strings.NewReader("hello"))
	putReq.Header.Set("Content-Type", "text/plain")
	putReq.Header.Set("Authorization", "Bearer dummy")
	pr, err := http.DefaultClient.Do(putReq)
	if err != nil {
		return fmt.Errorf("put: %w", err)
	}
	pr.Body.Close()
	if pr.StatusCode >= 400 {
		return fmt.Errorf("put: status %d", pr.StatusCode)
	}
	log.Printf("probe 3: PUT object → %d", pr.StatusCode)

	// Part C: verify the object is retrievable.
	getObjReq, _ := http.NewRequestWithContext(ctx, "GET",
		endpoint+"/storage/v1/b/"+bucket+"/o/probe.txt?alt=media", nil)
	getObjReq.Header.Set("Authorization", "Bearer dummy")
	gr, err := http.DefaultClient.Do(getObjReq)
	if err != nil {
		return fmt.Errorf("get-object: %w", err)
	}
	defer gr.Body.Close()
	if gr.StatusCode >= 400 {
		return fmt.Errorf("get-object: status %d", gr.StatusCode)
	}
	body, err := io.ReadAll(gr.Body)
	if err != nil {
		return fmt.Errorf("get-object read: %w", err)
	}
	if string(body) != "hello" {
		return fmt.Errorf("get-object content mismatch: got %q want %q", body, "hello")
	}
	log.Printf("probe 3: GET object → %d body=%q", gr.StatusCode, body)
	return nil
}

func probeLakekeeperRefreshEndpoint(ctx context.Context, lkURL, warehouse string) error {
	// RFC §4.5.4 criterion 4: capture a Lakekeeper loadTable response against
	// the SA-key-bootstrapped warehouse; check whether it emits
	// gcs.oauth2.refresh-credentials-endpoint and (if so) whether that endpoint
	// responds to a POST with the current bearer.
	if lkURL == "" || warehouse == "" {
		return fmt.Errorf("--lakekeeper-url and --warehouse required")
	}
	// Issue a loadTable call against an existing test table.
	// (The spike author bootstraps a throwaway warehouse + creates one
	// table first; documented in the README.)
	configURL := fmt.Sprintf("%s/catalog/v1/namespaces/%s/tables/%s",
		lkURL, "spike_ns", "spike_tbl")
	req, _ := http.NewRequestWithContext(ctx, "GET", configURL, nil)
	// Use the same JWT signing as production (signing-key-file) — but the
	// spike author mints a short-lived one out-of-band.
	req.Header.Set("Authorization", "Bearer "+os.Getenv("SPIKE_JWT"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("loadTable returned %d: %s", resp.StatusCode, body)
	}

	var parsed map[string]any
	json.Unmarshal(body, &parsed)
	cfg, _ := parsed["config"].(map[string]any)

	ep, _ := cfg["gcs.oauth2.refresh-credentials-endpoint"].(string)
	if ep == "" {
		fmt.Println("    NOTE: Lakekeeper did NOT emit gcs.oauth2.refresh-credentials-endpoint")
		return nil // not a failure; Slice D falls back to loadTable refresh
	}

	// Try a POST to the refresh endpoint with the current token.
	postReq, _ := http.NewRequestWithContext(ctx, "POST", ep, nil)
	postReq.Header.Set("Authorization", "Bearer "+os.Getenv("SPIKE_JWT"))
	r2, err := http.DefaultClient.Do(postReq)
	if err != nil {
		return fmt.Errorf("refresh POST: %w", err)
	}
	defer r2.Body.Close()
	if r2.StatusCode != 200 {
		b, _ := io.ReadAll(r2.Body)
		return fmt.Errorf("refresh endpoint returned %d: %s", r2.StatusCode, b)
	}
	return nil
}

func probeAuditAttribution(ctx context.Context, bucket, keyFile string) error {
	// RFC §4.5.4 criterion 5: PUT one object with x-goog-custom-audit-* headers
	// AND with Object.Metadata["datuplet-run-id"]=spike-<n>. Wait 2 min. Read
	// the GCS audit log for the bucket. Report which signal(s) surfaced.
	return fmt.Errorf("TODO: implement in Task A0.6")
}
