package datupleticeio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	iceio "github.com/apache/iceberg-go/io"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2"
)

// refreshHTTPClient is the shared transport used by loadTableRefresh.
// 30 s timeout matches catalogwriter.defaultHTTPTimeout so both refresh
// paths fail in similar windows under a hung lakekeeper.
var refreshHTTPClient = &http.Client{Timeout: 30 * time.Second}

// maxRefreshResponseBytes caps the body we read from the refresh
// endpoint. Same posture as catalogwriter.maxResponseBytes — a vended
// creds response is ~1 KiB; the 1 MiB cap guards against runaway
// responses without affecting legitimate traffic.
const maxRefreshResponseBytes = 1 << 20

// refreshFunc returns a fresh *oauth2.Token. Called when the cached
// token is within 1 minute of Expiry. On error, refreshingTokenSource
// returns the error (never the stale token) per RFC 019 §4.5.3.
type refreshFunc func(ctx context.Context) (*oauth2.Token, error)

type refreshingTokenSource struct {
	mu      sync.Mutex
	cur     *oauth2.Token
	refresh refreshFunc
}

func newRefreshingTokenSource(initial *oauth2.Token, fn refreshFunc) *refreshingTokenSource {
	return &refreshingTokenSource{cur: initial, refresh: fn}
}

// String returns a placeholder; the wrapped *oauth2.Token carries the
// live bearer and must never be expanded into log output. RFC 019 §4.10.
func (r *refreshingTokenSource) String() string { return "<refreshingTokenSource>" }

// Token implements oauth2.TokenSource. If the cached token has more than
// one minute remaining before Expiry, it's returned as-is. Otherwise
// refresh is invoked; on error the error is returned and the stale
// token is NOT returned. Per RFC 019 §4.5.3.
func (r *refreshingTokenSource) Token() (*oauth2.Token, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cur != nil && time.Until(r.cur.Expiry) > time.Minute {
		return r.cur, nil
	}
	tok, err := r.refresh(context.Background())
	if err != nil {
		// Hard error — do NOT return stale token. RFC §4.5.3.
		return nil, fmt.Errorf("refresh: %w", err)
	}
	r.cur = tok
	return tok, nil
}

func datupletGCSFactory(ctx context.Context, parsed *url.URL, props map[string]string) (iceio.IO, error) {
	tok := props["gcs.oauth2.token"]
	if tok == "" {
		return nil, fmt.Errorf("datupletGCSFactory: missing gcs.oauth2.token in props for %q", parsed.String())
	}
	expires := parseExpiresAt(props["gcs.oauth2.token-expires-at"])
	bucket := parsed.Host
	if bucket == "" {
		return nil, fmt.Errorf("datupletGCSFactory: cannot derive bucket from %q", parsed.String())
	}

	refresh, endpointSelected := pickRefresh(props)
	if endpointSelected {
		return nil, fmt.Errorf("datupletGCSFactory: gcs.oauth2.refresh-credentials-endpoint is present in props but endpointRefresh is not yet validated end-to-end in v0.2; set gcs.oauth2.refresh-credentials-enabled=false to suppress and use the loadTable-based fallback")
	}
	ts := newRefreshingTokenSource(&oauth2.Token{
		AccessToken: tok,
		Expiry:      expires,
		TokenType:   "Bearer",
	}, refresh)

	// Wire the TokenSource into a gocloud GCP HTTP client so every storage
	// request picks up a freshly-issued bearer when the cached one nears
	// expiry. Slice A0 probe 2 confirmed this contract is honored by
	// iceberg-go's GCS client (Token() is invoked per-request once the
	// cached token enters the refresh-ahead window).
	client, err := gcp.NewHTTPClient(
		gcp.DefaultTransport(),
		oauth2.ReuseTokenSource(nil, ts),
	)
	if err != nil {
		return nil, fmt.Errorf("datupletGCSFactory: new HTTP client: %w", err)
	}
	bkt, err := gcsblob.OpenBucket(ctx, client, bucket, nil)
	if err != nil {
		return nil, fmt.Errorf("datupletGCSFactory: open bucket %q: %w", bucket, err)
	}
	return &gcsIO{bucket: bkt, bucketName: bucket, ctx: ctx}, nil
}

// pickRefresh chooses between endpoint-based refresh and the loadTable-based
// fallback (the default path). It returns the chosen refreshFunc and a boolean
// indicating whether the endpoint path was selected. Callers must reject the
// endpoint path until it is validated end-to-end (see datupletGCSFactory).
//
// Endpoint-based refresh is OPT-IN: gcs.oauth2.refresh-credentials-enabled
// must be explicitly set to "true". Lakekeeper's standard loadTable response
// includes the gcs.oauth2.refresh-credentials-endpoint key by default; the
// prior `enabled != "false"` condition would have fail-fasted every WIF
// TableCommit (the endpoint prop is always present). The opt-in guard ensures
// the factory construction succeeds when Lakekeeper emits the endpoint without
// the caller explicitly enabling it, which is the default deployment shape.
func pickRefresh(props map[string]string) (refreshFunc, bool) {
	ep := props["gcs.oauth2.refresh-credentials-endpoint"]
	enabled := props["gcs.oauth2.refresh-credentials-enabled"]
	// Endpoint-based refresh is OPT-IN: caller must set enabled=true.
	// Lakekeeper's standard loadTable response includes the endpoint key
	// by default; falling through to loadTableRefresh ensures factory
	// construction succeeds when Lakekeeper emits the endpoint without
	// explicit client opt-in (which is the default deployment shape).
	if ep != "" && enabled == "true" {
		return endpointRefresh(ep, props), true
	}
	return loadTableRefresh(props), false
}

// endpointRefresh is the latent path. Validated in a follow-on once a
// Lakekeeper instance is reachable from the dev environment. Until then,
// this returns a clear error so any accidental activation is obvious.
//
// Only reachable when gcs.oauth2.refresh-credentials-enabled=true is
// explicitly set (opt-in). Datuplet callers should not set this unless
// the endpoint path has been validated end-to-end.
func endpointRefresh(ep string, props map[string]string) refreshFunc {
	_ = ep
	_ = props
	return func(ctx context.Context) (*oauth2.Token, error) {
		return nil, fmt.Errorf("endpointRefresh: not yet validated end-to-end in v0.2; rely on loadTable-based refresh instead (set gcs.oauth2.refresh-credentials-enabled=false to suppress)")
	}
}

// loadTableRefresh is the default refresh path. It GETs
// `gcs.oauth2.refresh-credentials-endpoint` (always present in
// lakekeeper's standard loadTable response) with the package-level
// bearer installed via SetTokenProvider, parses the same shape
// lakekeeper returns on loadTable, and extracts the fresh
// `gcs.oauth2.token` + `gcs.oauth2.token-expires-at` claims.
//
// Returns a hard error (NOT a stale token) when:
//   - the endpoint URL is missing from props,
//   - no BearerTokenProvider has been installed at the package level,
//   - the provider returns an error,
//   - the HTTP call fails or returns non-2xx,
//   - the response body is malformed or missing the token claim.
//
// Per RFC 019 §4.5.3 the refreshingTokenSource propagates the error
// without falling back to the cached (expired) token — every request
// that needed a refresh fails fast rather than silently re-using a bad
// credential.
func loadTableRefresh(props map[string]string) refreshFunc {
	ep := props["gcs.oauth2.refresh-credentials-endpoint"]
	return func(ctx context.Context) (*oauth2.Token, error) {
		if ep == "" {
			return nil, errors.New("loadTableRefresh: gcs.oauth2.refresh-credentials-endpoint missing from props")
		}
		provider := getTokenProvider()
		if provider == nil {
			return nil, errors.New("loadTableRefresh: no BearerTokenProvider installed (call datupleticeio.SetTokenProvider at startup)")
		}
		bearer, err := provider(ctx)
		if err != nil {
			return nil, fmt.Errorf("loadTableRefresh: bearer provider: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ep, nil)
		if err != nil {
			return nil, fmt.Errorf("loadTableRefresh: build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+bearer)
		req.Header.Set("Accept", "application/json")
		// Standard Iceberg REST opt-in to vended creds (some servers
		// gate the credential block behind this header even on the
		// refresh URL). Lakekeeper accepts it as a no-op when the
		// warehouse is already configured for credential vending, so
		// it's safe to always send.
		req.Header.Set("X-Iceberg-Access-Delegation", "vended-credentials")

		resp, err := refreshHTTPClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("loadTableRefresh: GET %s: %w", ep, err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(io.LimitReader(resp.Body, maxRefreshResponseBytes))
		if err != nil {
			return nil, fmt.Errorf("loadTableRefresh: read body: %w", err)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("loadTableRefresh: GET %s: status %d (body len=%d)", ep, resp.StatusCode, len(body))
		}

		// Both the loadTable response and the dedicated credentials
		// endpoint shape are `{ "config": { "gcs.oauth2.token": ... } }`
		// per the Iceberg REST spec. Parse loosely so we tolerate
		// either side of the wire.
		var parsed struct {
			Config map[string]any `json:"config"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			return nil, fmt.Errorf("loadTableRefresh: unmarshal: %w", err)
		}
		if parsed.Config == nil {
			return nil, errors.New("loadTableRefresh: response has no `config` block")
		}
		tok, _ := parsed.Config["gcs.oauth2.token"].(string)
		if tok == "" {
			return nil, errors.New("loadTableRefresh: response missing gcs.oauth2.token")
		}
		expires := parseConfigExpiresAt(parsed.Config["gcs.oauth2.token-expires-at"])
		return &oauth2.Token{
			AccessToken: tok,
			Expiry:      expires,
			TokenType:   "Bearer",
		}, nil
	}
}

// parseConfigExpiresAt parses the `gcs.oauth2.token-expires-at` claim
// from the refresh response's config block. The value may arrive as a
// JSON string ("1234567890000") or a JSON number (1234567890000) per
// the spec — handle both. Empty / malformed values default to 15
// minutes from now, matching parseExpiresAt for the initial token.
func parseConfigExpiresAt(v any) time.Time {
	switch x := v.(type) {
	case string:
		return parseExpiresAt(x)
	case float64:
		return time.UnixMilli(int64(x))
	case int64:
		return time.UnixMilli(x)
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return time.UnixMilli(n)
		}
	}
	return time.Now().Add(15 * time.Minute)
}

// parseExpiresAt parses a millisecond-epoch string. Empty / malformed
// values default to 15 minutes from now (a sane backstop that keeps
// short-lived tokens within their typical lakekeeper-vended TTL).
func parseExpiresAt(s string) time.Time {
	if s == "" {
		return time.Now().Add(15 * time.Minute)
	}
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Now().Add(15 * time.Minute)
	}
	return time.UnixMilli(ms)
}

// gcsIO satisfies iceio.IO by delegating to a gocloud blob.Bucket.
//
// iceio.IO is the {Open(string), Remove(string)} surface (verified
// against pinned iceberg-go/io/io.go: Open(name string) (File, error),
// Remove(name string) error — NO context.Context argument).
type gcsIO struct {
	bucket     *blob.Bucket
	bucketName string
	ctx        context.Context
}

// preprocess strips the gs://<bucket>/ prefix so the key passed to
// blob.Bucket is just the object name. Mirrors upstream
// gocloud.defaultKeyExtractor.
func (g *gcsIO) preprocess(path string) (string, error) {
	_, after, found := strings.Cut(path, "://")
	if found {
		path = after
	}
	key := strings.TrimPrefix(path, g.bucketName+"/")
	if key == "" {
		return "", fmt.Errorf("URI path is empty: %s", path)
	}
	return key, nil
}

// Open opens the named object. iceio.IO requires Open(name string) (File, error).
func (g *gcsIO) Open(name string) (iceio.File, error) {
	key, err := g.preprocess(name)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	if !fs.ValidPath(key) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	r, err := g.bucket.NewReader(g.ctx, key, nil)
	if err != nil {
		return nil, fmt.Errorf("gcsIO open %q: %w", name, err)
	}
	return &gcsFile{
		Reader: r,
		bucket: g.bucket,
		key:    key,
		name:   filepath.Base(key),
		ctx:    g.ctx,
	}, nil
}

// Remove deletes the named object.
func (g *gcsIO) Remove(name string) error {
	key, err := g.preprocess(name)
	if err != nil {
		return &fs.PathError{Op: "remove", Path: name, Err: err}
	}
	if !fs.ValidPath(key) {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrInvalid}
	}
	return g.bucket.Delete(g.ctx, key)
}

// Close releases the underlying gocloud blob.Bucket. iceio.IO doesn't
// declare Close, but Datuplet callers that hold a non-iceio reference
// to *gcsIO should call this defensively. Today gcsblob.Bucket.Close
// is a no-op, but it is the correct cleanup point — future gcsblob
// versions may close the storage.Client there.
//
// TODO(rfc-019): wire defer io.Close() in pkg/icebergjob/commit.go and
// pkg/datagateway/lakekeeper/lakekeeper.go once the iceio.IO interface
// is annotated to accept a Closer, or the call sites obtain *gcsIO
// directly via a type assertion.
func (g *gcsIO) Close() error {
	return g.bucket.Close()
}

// Create satisfies iceio.WriteFileIO.Create. iceberg-go calls this for
// snapshot manifest avro files and table metadata.json on every
// TableCommit AddFiles → updateSnapshot path. Without this method,
// iceberg-go's type-assert to iceio.WriteFileIO panics at write time
// (v0.2.0 regression fixed in v0.2.1).
func (g *gcsIO) Create(name string) (iceio.FileWriter, error) {
	key, err := g.preprocess(name)
	if err != nil {
		return nil, &fs.PathError{Op: "create", Path: name, Err: err}
	}
	if !fs.ValidPath(key) {
		return nil, &fs.PathError{Op: "create", Path: name, Err: fs.ErrInvalid}
	}
	w, err := g.bucket.NewWriter(g.ctx, key, nil)
	if err != nil {
		return nil, fmt.Errorf("gcsIO create %q: %w", name, err)
	}
	return &gcsWriteFile{w: w}, nil
}

// WriteFile is the byte-slice convenience variant of Create. Same path
// followed by a single Write + Close. If Write fails, we still call
// Close (which finalises/aborts the multipart upload at the GCS level)
// and join both errors — the Close error carries authoritative
// "upload aborted" context the Write error doesn't have.
func (g *gcsIO) WriteFile(name string, p []byte) error {
	f, err := g.Create(name)
	if err != nil {
		return err
	}
	if _, writeErr := f.Write(p); writeErr != nil {
		return errors.Join(writeErr, f.Close())
	}
	return f.Close()
}

// gcsWriteFile satisfies iceio.FileWriter (io.WriteCloser + io.ReaderFrom)
// on top of gocloud.dev/blob.Writer. blob.Writer provides Write+Close;
// we add ReadFrom via io.Copy so streaming writes (e.g. iceberg-go
// piping avro manifest output through a Reader) don't have to buffer.
type gcsWriteFile struct {
	w *blob.Writer
}

func (f *gcsWriteFile) Write(p []byte) (int, error)        { return f.w.Write(p) }
func (f *gcsWriteFile) Close() error                       { return f.w.Close() }
func (f *gcsWriteFile) ReadFrom(r io.Reader) (int64, error) { return io.Copy(f.w, r) }

// Verify gcsIO satisfies io.Closer at compile time.
var _ io.Closer = (*gcsIO)(nil)

// Compile-time: gcsIO satisfies iceberg-go's full write interface.
// If iceberg-go's transaction.go assertion fails again, the panic
// would surface here at build time instead — which is the whole point.
var _ iceio.WriteFileIO = (*gcsIO)(nil)

// gcsFile satisfies iceio.File = fs.File + io.ReadSeekCloser + io.ReaderAt.
//
// blob.Reader natively provides Read, Seek (verified at
// gocloud.dev/blob/blob.go:158), Close, and Size. We embed it and add
// Stat (via the FileInfo methods on this struct) and ReadAt (via a
// per-call range reader, mirroring upstream's blobOpenFile.ReadAt).
type gcsFile struct {
	*blob.Reader

	bucket *blob.Bucket
	key    string
	name   string
	ctx    context.Context
}

// ReadAt does a range read for each call. Mirrors the upstream
// io/gocloud blobOpenFile.ReadAt — Parquet's footer-first reader uses
// this heavily, so the per-call allocation is acceptable.
func (f *gcsFile) ReadAt(p []byte, off int64) (n int, err error) {
	rdr, err := f.bucket.NewRangeReader(f.ctx, f.key, off, int64(len(p)), nil)
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, rdr.Close()) }()
	return io.ReadFull(rdr, p)
}

// fs.FileInfo methods (used by Stat()):

func (f *gcsFile) Name() string               { return f.name }
func (f *gcsFile) Mode() fs.FileMode          { return fs.ModeIrregular }
func (f *gcsFile) Sys() any                   { return f }
func (f *gcsFile) IsDir() bool                { return false }
func (f *gcsFile) Stat() (fs.FileInfo, error) { return f, nil }

// Size and ModTime come from *blob.Reader's existing methods, but we
// keep an explicit Size delegation here so it's clear gcsFile satisfies
// fs.FileInfo's Size() int64 even though Read returns 0 once the body
// is drained. Reader.Size() returns the content-length set at open
// time (the object's size, not the bytes-remaining).
func (f *gcsFile) Size() int64 { return f.Reader.Size() }

// ModTime, Read, Seek, and Close are all provided by *blob.Reader and are
// automatically promoted by embedding — no override needed.

// Compile-time interface assertions.
var (
	_ iceio.IO   = (*gcsIO)(nil)
	_ iceio.File = (*gcsFile)(nil)
)
