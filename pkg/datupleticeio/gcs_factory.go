package datupleticeio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
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

	refresh := pickRefresh(props)
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

// pickRefresh chooses between endpoint-based refresh (latent in v0.2 — not
// end-to-end validated; Slice A0 probe 4 was deferred because Lakekeeper
// wasn't reachable from the dev cluster) and the loadTable-based fallback
// (the default path).
//
// TODO(rfc-019): the endpoint-based path is shipped as a latent code path —
// it activates only if Lakekeeper emits gcs.oauth2.refresh-credentials-endpoint
// AND the operator hasn't explicitly set gcs.oauth2.refresh-credentials-enabled
// to "false". A follow-on slice will validate it end-to-end against a real
// Lakekeeper instance and lift the explicit "not yet validated" error.
func pickRefresh(props map[string]string) refreshFunc {
	ep := props["gcs.oauth2.refresh-credentials-endpoint"]
	enabled := props["gcs.oauth2.refresh-credentials-enabled"]
	if ep != "" && enabled != "false" {
		// Latent path — code present but NOT end-to-end validated.
		return endpointRefresh(ep, props)
	}
	return loadTableRefresh(props)
}

// endpointRefresh is the latent path. Validated in a follow-on once a
// Lakekeeper instance is reachable from the dev environment. Until then,
// this returns a clear error so any accidental activation is obvious.
//
// Suppress by setting gcs.oauth2.refresh-credentials-enabled=false.
func endpointRefresh(ep string, props map[string]string) refreshFunc {
	_ = ep
	_ = props
	return func(ctx context.Context) (*oauth2.Token, error) {
		return nil, fmt.Errorf("endpointRefresh: not yet validated end-to-end in v0.2; rely on loadTable-based refresh instead (set gcs.oauth2.refresh-credentials-enabled=false to suppress)")
	}
}

// loadTableRefresh is the default fallback. The factory's caller (DG,
// TableCommit, storage browser) wires a closure that re-issues loadTable
// against the catalog client for the bound table. Without that closure
// (which today no caller provides — wiring is a follow-on slice), refresh
// is a hard error.
//
// Today's transactions complete within the initial token TTL, so refresh
// is never invoked in practice; this surface exists so when a long
// transaction (e.g. a multi-hour ReplaceDataFiles on a very large table)
// does exceed the TTL, the failure is loud rather than silent.
func loadTableRefresh(props map[string]string) refreshFunc {
	_ = props
	return func(ctx context.Context) (*oauth2.Token, error) {
		return nil, fmt.Errorf("loadTableRefresh: caller did not inject a refresh closure (today's transactions complete within the initial token TTL; refresh wiring lands in a follow-on slice)")
	}
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
	return g.bucket.Delete(g.ctx, key)
}

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

// ModTime is provided by *blob.Reader and is automatically promoted by
// embedding — no override needed. Likewise Read/Seek/Close.

// Compile-time interface assertions.
var (
	_ iceio.IO   = (*gcsIO)(nil)
	_ iceio.File = (*gcsFile)(nil)
)
