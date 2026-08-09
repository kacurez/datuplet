package appworker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Test helpers: fake clock, recording fake pipeline-api
// ---------------------------------------------------------------------------

// fakeClock lets TTL tests advance time deterministically instead of
// sleeping real wall-clock seconds (contract: "TTL behavior driven by a
// fake clock").
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)} }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// recordedRequest captures everything a test needs to assert about one
// inbound call to the fake pipeline-api.
type recordedRequest struct {
	method  string
	path    string
	query   string
	headers http.Header
	body    []byte
}

// requestLog is a concurrency-safe recorder shared by every handler
// registered on a fake server.
type requestLog struct {
	mu   sync.Mutex
	reqs []recordedRequest
}

func (l *requestLog) record(r *http.Request) recordedRequest {
	body, _ := io.ReadAll(r.Body)
	rr := recordedRequest{
		method:  r.Method,
		path:    r.URL.Path,
		query:   r.URL.RawQuery,
		headers: r.Header.Clone(),
		body:    body,
	}
	l.mu.Lock()
	l.reqs = append(l.reqs, rr)
	l.mu.Unlock()
	return rr
}

func (l *requestLog) count(path string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, r := range l.reqs {
		if r.path == path {
			n++
		}
	}
	return n
}

func (l *requestLog) last(path string) (recordedRequest, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := len(l.reqs) - 1; i >= 0; i-- {
		if l.reqs[i].path == path {
			return l.reqs[i], true
		}
	}
	return recordedRequest{}, false
}

func writeJSONResp(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

const testServiceToken = "s3rvice-cred-abcdefghijklmnop"

// ---------------------------------------------------------------------------
// Resolve
// ---------------------------------------------------------------------------

func TestResolve_HappyPathAndHeader(t *testing.T) {
	log := &requestLog{}
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/apps/proj-1/dash/resolve", func(w http.ResponseWriter, r *http.Request) {
		log.record(r)
		if got := r.URL.Query().Get("channel"); got != "production" {
			t.Errorf("channel query = %q, want production", got)
		}
		writeJSONResp(w, http.StatusOK, map[string]string{"app_id": "app-1", "version_hash": strings.Repeat("a", 64)})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	got, err := c.Resolve(context.Background(), "proj-1", "dash", "production")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	want := Resolved{AppID: "app-1", VersionHash: strings.Repeat("a", 64)}
	if got != want {
		t.Errorf("Resolve = %+v, want %+v", got, want)
	}

	req, ok := log.last("/internal/v1/apps/proj-1/dash/resolve")
	if !ok {
		t.Fatal("no request recorded")
	}
	if req.method != http.MethodGet {
		t.Errorf("method = %s, want GET", req.method)
	}
	if got := req.headers.Get("Authorization"); got != "Bearer "+testServiceToken {
		t.Errorf("Authorization = %q, want service token", got)
	}
}

func TestResolve_EmptyChannelDefaultsToProduction(t *testing.T) {
	log := &requestLog{}
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/apps/p/n/resolve", func(w http.ResponseWriter, r *http.Request) {
		log.record(r)
		writeJSONResp(w, http.StatusOK, map[string]string{"app_id": "a", "version_hash": strings.Repeat("b", 64)})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	if _, err := c.Resolve(context.Background(), "p", "n", ""); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	req, _ := log.last("/internal/v1/apps/p/n/resolve")
	if got := req.query; !strings.Contains(got, "channel=production") {
		t.Errorf("query = %q, want channel=production", got)
	}
}

func TestResolve_CachesWithin15sFakeClock(t *testing.T) {
	log := &requestLog{}
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/apps/p/n/resolve", func(w http.ResponseWriter, r *http.Request) {
		log.record(r)
		writeJSONResp(w, http.StatusOK, map[string]string{"app_id": "a", "version_hash": strings.Repeat("c", 64)})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	clock := newFakeClock()
	c := NewAPIClient(srv.URL, testServiceToken, WithClock(clock.Now))
	defer c.Close()

	ctx := context.Background()
	if _, err := c.Resolve(ctx, "p", "n", "production"); err != nil {
		t.Fatalf("Resolve #1: %v", err)
	}
	clock.Advance(14 * time.Second)
	if _, err := c.Resolve(ctx, "p", "n", "production"); err != nil {
		t.Fatalf("Resolve #2: %v", err)
	}
	if n := log.count("/internal/v1/apps/p/n/resolve"); n != 1 {
		t.Errorf("upstream calls = %d, want 1 (cache hit at 14s)", n)
	}

	// Cross the 15s boundary: must re-fetch.
	clock.Advance(2 * time.Second)
	if _, err := c.Resolve(ctx, "p", "n", "production"); err != nil {
		t.Fatalf("Resolve #3: %v", err)
	}
	if n := log.count("/internal/v1/apps/p/n/resolve"); n != 2 {
		t.Errorf("upstream calls after 16s = %d, want 2 (TTL expired)", n)
	}
}

func TestResolve_DoesNotStampedeConcurrentCallers(t *testing.T) {
	var calls int32
	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/apps/p/n/resolve", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		<-release
		writeJSONResp(w, http.StatusOK, map[string]string{"app_id": "a", "version_hash": strings.Repeat("d", 64)})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, errs[i] = c.Resolve(context.Background(), "p", "n", "production")
		}(i)
	}
	// Give every goroutine a chance to reach the handler before releasing it.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: %v", i, err)
		}
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("upstream calls = %d, want exactly 1 (singleflight should coalesce)", got)
	}
}

func TestResolve_NotFoundIsNotCachedAndCarriesKind(t *testing.T) {
	var calls int32
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/apps/p/n/resolve", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		writeJSONResp(w, http.StatusNotFound, map[string]string{"error": "no such app", "kind": "app_not_found"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	ctx := context.Background()
	_, err := c.Resolve(ctx, "p", "n", "production")
	var apiErr *APIError
	if !asAPIError(err, &apiErr) {
		t.Fatalf("err = %v, want *APIError", err)
	}
	if apiErr.StatusCode != http.StatusNotFound || apiErr.Kind != "app_not_found" {
		t.Errorf("apiErr = %+v, want 404/app_not_found", apiErr)
	}

	// A second call must re-fetch: a miss is never cached.
	if _, err := c.Resolve(ctx, "p", "n", "production"); err == nil {
		t.Fatal("expected error again")
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("upstream calls = %d, want 2 (errors are not cached)", got)
	}
}

// ---------------------------------------------------------------------------
// Bundle
// ---------------------------------------------------------------------------

func mustSHA256Hex(b []byte) string {
	return sha256Hex(b)
}

func TestBundle_HappyPathVerifiesAndCaches(t *testing.T) {
	data := []byte("bundle-bytes-for-testing")
	hash := mustSHA256Hex(data)
	var calls int32
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/bundles/"+hash, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		if got := r.Header.Get("Authorization"); got != "Bearer "+testServiceToken {
			t.Errorf("Authorization = %q, want service token", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	got, err := c.Bundle(context.Background(), hash)
	if err != nil {
		t.Fatalf("Bundle: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("Bundle bytes mismatch")
	}
	if _, err := c.Bundle(context.Background(), hash); err != nil {
		t.Fatalf("Bundle (cached): %v", err)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Errorf("upstream calls = %d, want 1 (second call should be a cache hit)", n)
	}
}

func TestBundle_HashMismatchIsErrorAndNeverCached(t *testing.T) {
	real := []byte("real bytes")
	wrongHash := mustSHA256Hex([]byte("something else"))
	var calls int32
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/bundles/"+wrongHash, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(real) // body does NOT hash to wrongHash
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	if _, err := c.Bundle(context.Background(), wrongHash); err == nil {
		t.Fatal("expected a hash-mismatch error")
	}
	if _, err := c.Bundle(context.Background(), wrongHash); err == nil {
		t.Fatal("expected a hash-mismatch error again (must not have been cached)")
	}
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Errorf("upstream calls = %d, want 2 (a mismatch must never be served from cache)", n)
	}
}

func TestBundle_LRUEvictsOldestUnderByteBudget(t *testing.T) {
	mkBundle := func(tag byte, n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = tag
		}
		return b
	}
	b1 := mkBundle('1', 100)
	b2 := mkBundle('2', 100)
	b3 := mkBundle('3', 100)
	h1, h2, h3 := mustSHA256Hex(b1), mustSHA256Hex(b2), mustSHA256Hex(b3)

	bodies := map[string][]byte{h1: b1, h2: b2, h3: b3}
	var calls int32
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/bundles/", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		hash := strings.TrimPrefix(r.URL.Path, "/internal/v1/bundles/")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bodies[hash])
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Budget fits ~2 bundles of 100 bytes.
	c := NewAPIClient(srv.URL, testServiceToken, WithBundleCacheBytes(250))
	defer c.Close()

	ctx := context.Background()
	// Recency order after each step (front = most recently used):
	if _, err := c.Bundle(ctx, h1); err != nil { // [h1]
		t.Fatal(err)
	}
	if _, err := c.Bundle(ctx, h2); err != nil { // [h2, h1]
		t.Fatal(err)
	}
	// Inserting h3 evicts the LRU entry (h1): [h3, h2].
	if _, err := c.Bundle(ctx, h3); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&calls); n != 3 {
		t.Fatalf("setup calls = %d, want 3", n)
	}
	// Touch h2 to make it the most recently used, leaving h3 as the next
	// eviction candidate: [h2, h3].
	if _, err := c.Bundle(ctx, h2); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&calls); n != 3 {
		t.Fatalf("touching h2 caused an upstream call = %d, want still 3 (cache hit)", n)
	}

	// Inserting h1 again must evict h3 (now LRU), not h2: [h1, h2].
	if _, err := c.Bundle(ctx, h1); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&calls); n != 4 {
		t.Errorf("upstream calls = %d, want 4 (h1 was evicted earlier and must be re-fetched)", n)
	}
	// h2 must still be cached (recently touched)...
	if _, err := c.Bundle(ctx, h2); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&calls); n != 4 {
		t.Errorf("upstream calls = %d, want still 4 (h2 should be a cache hit)", n)
	}
	// ...but h3 must have been evicted and require a re-fetch.
	if _, err := c.Bundle(ctx, h3); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&calls); n != 5 {
		t.Errorf("upstream calls = %d, want 5 (h3 should have been evicted)", n)
	}
}

func TestBundle_DoesNotStampedeConcurrentCallers(t *testing.T) {
	data := []byte("concurrent-bundle")
	hash := mustSHA256Hex(data)
	var calls int32
	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/bundles/"+hash, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		<-release
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, _ = c.Bundle(context.Background(), hash)
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("upstream calls = %d, want exactly 1", got)
	}
}

// TestBundle_CallerCannotCorruptCachedBytesByMutatingReturnedSlice is the
// Finding-3 fix: Bundle must return a private copy, never the cache's own
// backing array — otherwise a caller that reuses/mutates the returned slice
// would corrupt what every other renderer subsequently reads for the same
// content hash, defeating the entire point of a content-addressed,
// supposedly-immutable cache.
func TestBundle_CallerCannotCorruptCachedBytesByMutatingReturnedSlice(t *testing.T) {
	data := []byte("original-bundle-bytes")
	hash := mustSHA256Hex(data)
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/bundles/"+hash, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	ctx := context.Background()
	// First call is a cache MISS (fetches and populates the cache). The
	// aliasing risk is on a HIT's return path, so mutate the result of the
	// SECOND call instead — that is the one that comes straight out of
	// bundleLRU.get.
	if _, err := c.Bundle(ctx, hash); err != nil {
		t.Fatal(err)
	}
	got, err := c.Bundle(ctx, hash) // cache HIT
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt the CALLER's copy in place.
	for i := range got {
		got[i] = 'X'
	}

	again, err := c.Bundle(ctx, hash) // another cache HIT
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(data) {
		t.Errorf("third Bundle() call = %q, want the untouched original %q "+
			"(mutating a hit's returned slice must not have reached the cache)", again, data)
	}
}

// TestBundle_PutDoesNotAliasCallerBuffer is the OTHER half of Finding 3:
// even though Bundle's only current caller passes a fresh io.ReadAll
// result, the LRU itself must not assume that — mutating the buffer AFTER
// it was (hypothetically) handed to a cache must not be observable through
// a later cache hit. Exercised directly against bundleLRU (the actual
// aliasing boundary) rather than through Bundle, since Bundle's HTTP path
// has no seam to inject a reused buffer.
func TestBundle_PutDoesNotAliasCallerBuffer(t *testing.T) {
	lru := newBundleLRU(1024)
	buf := []byte("bundle-data-owned-by-caller")
	original := string(buf)
	lru.put("some-hash", buf)

	// Mutate the caller's buffer AFTER put returns.
	for i := range buf {
		buf[i] = 'Z'
	}

	got, ok := lru.get("some-hash")
	if !ok {
		t.Fatal("expected a cache hit")
	}
	if string(got) != original {
		t.Errorf("cached value = %q, want %q (put must copy, not alias, the caller's slice)", got, original)
	}
}

// TestBundle_RejectsBodyExceedingStructuralCap is the Finding-4 fix:
// io.ReadAll on the bundle response must be bounded, so a buggy or hostile
// internal endpoint cannot force an oversized allocation before the hash is
// even checked.
func TestBundle_RejectsBodyExceedingStructuralCap(t *testing.T) {
	oversized := make([]byte, 200)
	for i := range oversized {
		oversized[i] = byte(i)
	}
	hash := mustSHA256Hex(oversized) // irrelevant: rejected on SIZE first
	var calls int32
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/bundles/"+hash, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(oversized)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Cap well below the fixture's 200 bytes.
	c := NewAPIClient(srv.URL, testServiceToken, WithMaxBundleBytes(100))
	defer c.Close()

	if _, err := c.Bundle(context.Background(), hash); err == nil {
		t.Fatal("expected an error for a body exceeding the structural cap")
	}
	// A second call must not be served from cache either (the oversized
	// read was never a valid entry to begin with).
	if _, err := c.Bundle(context.Background(), hash); err == nil {
		t.Fatal("expected the error again on a second call")
	}
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Errorf("upstream calls = %d, want 2 (an oversized body must never be cached)", n)
	}
}

func TestBundle_BodyAtExactCapIsAccepted(t *testing.T) {
	data := make([]byte, 100)
	for i := range data {
		data[i] = byte(i)
	}
	hash := mustSHA256Hex(data)
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/bundles/"+hash, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken, WithMaxBundleBytes(100))
	defer c.Close()

	got, err := c.Bundle(context.Background(), hash)
	if err != nil {
		t.Fatalf("body exactly AT the cap should be accepted: %v", err)
	}
	if len(got) != 100 {
		t.Errorf("len = %d, want 100", len(got))
	}
}

// ---------------------------------------------------------------------------
// VerifyToken
// ---------------------------------------------------------------------------

// registerVerifyTokenFakeRoutes wires both /viewer-tokens/verify and
// /viewer-tokens/active on mux. Since fix round 2, VerifyToken calls BOTH
// endpoints on any verify failure (to classify "wrong secret against an
// active token" vs "token itself is inactive"), so every test below that
// exercises a false result needs a working /active handler — an
// unregistered one 404s, which VerifyToken treats as "unclassifiable,
// cache nothing" (safe, but defeats the very caching behavior most of
// these tests assert). active is constant for the lifetime of one test;
// call counts are tracked separately per endpoint.
func registerVerifyTokenFakeRoutes(mux *http.ServeMux, correctSecret string, active bool, verifyCalls, activeCalls *int32) {
	mux.HandleFunc("/internal/v1/viewer-tokens/verify", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(verifyCalls, 1)
		var req verifyTokenRequestWire
		_ = json.NewDecoder(r.Body).Decode(&req)
		writeJSONResp(w, http.StatusOK, map[string]bool{"ok": req.Secret == correctSecret})
	})
	mux.HandleFunc("/internal/v1/viewer-tokens/active", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(activeCalls, 1)
		writeJSONResp(w, http.StatusOK, map[string]bool{"active": active})
	})
}

func TestVerifyToken_PositiveCache15s(t *testing.T) {
	var calls int32
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/viewer-tokens/verify", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		writeJSONResp(w, http.StatusOK, map[string]bool{"ok": true})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	clock := newFakeClock()
	c := NewAPIClient(srv.URL, testServiceToken, WithClock(clock.Now))
	defer c.Close()

	ctx := context.Background()
	ok, err := c.VerifyToken(ctx, "app-1", "tok-1", "secret-1")
	if err != nil || !ok {
		t.Fatalf("VerifyToken = %v, %v; want true, nil", ok, err)
	}
	clock.Advance(14 * time.Second)
	if ok, err := c.VerifyToken(ctx, "app-1", "tok-1", "secret-1"); err != nil || !ok {
		t.Fatalf("VerifyToken (cached) = %v, %v", ok, err)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Errorf("calls = %d, want 1 (14s < 15s positive TTL)", n)
	}

	clock.Advance(2 * time.Second) // total 16s
	if _, err := c.VerifyToken(ctx, "app-1", "tok-1", "secret-1"); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Errorf("calls = %d, want 2 (positive TTL expired at 16s)", n)
	}
}

// TestVerifyToken_InactiveTokenNegativeCache60s: the (app_id, token_id)-keyed
// 60s negative cache applies ONLY when the token itself is inactive
// (unknown/revoked/app-mismatched) — never for a wrong-secret guess against
// a live token (that would be Finding 2's fix-round-2 defect: see
// TestVerifyToken_CorrectSecretSucceedsAfterAPriorWrongGuess below). An
// inactive token has no correct secret and thus no legitimate holder to
// lock out, so sharing the 60s window across every secret is safe here.
func TestVerifyToken_InactiveTokenNegativeCache60s(t *testing.T) {
	var verifyCalls, activeCalls int32
	mux := http.NewServeMux()
	registerVerifyTokenFakeRoutes(mux, "unused-correct-secret", false /* inactive */, &verifyCalls, &activeCalls)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	clock := newFakeClock()
	c := NewAPIClient(srv.URL, testServiceToken, WithClock(clock.Now))
	defer c.Close()

	ctx := context.Background()
	if ok, err := c.VerifyToken(ctx, "app-1", "tok-1", "wrong-secret"); err != nil || ok {
		t.Fatalf("VerifyToken = %v, %v; want false, nil", ok, err)
	}

	// Still within the 60s negative TTL at 30s.
	clock.Advance(30 * time.Second)
	if ok, err := c.VerifyToken(ctx, "app-1", "tok-1", "wrong-secret"); err != nil || ok {
		t.Fatalf("VerifyToken (cached negative) = %v, %v", ok, err)
	}
	if n := atomic.LoadInt32(&verifyCalls); n != 1 {
		t.Errorf("verify calls = %d, want 1 (30s < 60s negative TTL)", n)
	}

	clock.Advance(35 * time.Second) // total 65s, past the 60s negative TTL
	if _, err := c.VerifyToken(ctx, "app-1", "tok-1", "wrong-secret"); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&verifyCalls); n != 2 {
		t.Errorf("verify calls = %d, want 2 (negative TTL expired at 65s)", n)
	}
}

// TestVerifyToken_InactiveTokenNegativeCacheSharedAcrossDifferentSecrets is
// the fix-round-2 replacement for the fix-round-1 test of the same shape:
// for an INACTIVE token, the negative cache keys on (app_id, token_id)
// ALONE, not the secret, so an attacker varying the secret on every attempt
// against a token that is unknown/revoked/app-mismatched cannot bypass it.
// This is safe specifically BECAUSE the token is inactive — see
// TestVerifyToken_VaryingSecretsAgainstActiveTokenReachesUpstreamEachTime
// for the deliberately different behavior against a LIVE token.
func TestVerifyToken_InactiveTokenNegativeCacheSharedAcrossDifferentSecrets(t *testing.T) {
	var verifyCalls, activeCalls int32
	mux := http.NewServeMux()
	registerVerifyTokenFakeRoutes(mux, "unused-correct-secret", false /* inactive */, &verifyCalls, &activeCalls)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	ctx := context.Background()
	const attempts = 10
	for i := 0; i < attempts; i++ {
		secret := fmt.Sprintf("guess-%d", i)
		if ok, err := c.VerifyToken(ctx, "app-1", "tok-1", secret); err != nil || ok {
			t.Fatalf("attempt %d (secret %q): %v, %v; want false, nil", i, secret, ok, err)
		}
	}
	if n := atomic.LoadInt32(&verifyCalls); n != 1 {
		t.Errorf("verify calls for %d different secrets against an inactive token = %d, want exactly 1", attempts, n)
	}
	if n := atomic.LoadInt32(&activeCalls); n != 1 {
		t.Errorf("active-check calls = %d, want exactly 1 (its own 15s cache absorbs the rest)", n)
	}
}

// TestVerifyToken_CorrectSecretSucceedsAfterAPriorWrongGuess is the
// fix-round-2 regression test: a prior wrong-secret attempt against a LIVE
// token must NEVER cause the legitimate holder's subsequent correct-secret
// attempt to be served a cached `false`. This is exactly the Major the
// fix-round-1 pair-keyed negative cache introduced — a single wrong guess
// (needing only a known token_id, no secret) could deny the real viewer for
// up to 60s.
func TestVerifyToken_CorrectSecretSucceedsAfterAPriorWrongGuess(t *testing.T) {
	var verifyCalls, activeCalls int32
	mux := http.NewServeMux()
	registerVerifyTokenFakeRoutes(mux, "correct-secret", true /* ACTIVE */, &verifyCalls, &activeCalls)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	ctx := context.Background()
	if ok, err := c.VerifyToken(ctx, "app-1", "tok-1", "an-attackers-guess"); err != nil || ok {
		t.Fatalf("wrong guess: %v, %v; want false, nil", ok, err)
	}
	// The LEGITIMATE holder, presenting the correct secret right after,
	// must succeed — not be served the wrong guess's cached false.
	ok, err := c.VerifyToken(ctx, "app-1", "tok-1", "correct-secret")
	if err != nil {
		t.Fatalf("correct secret after a prior wrong guess: unexpected error %v", err)
	}
	if !ok {
		t.Fatal("correct secret after a prior wrong guess = false, want true — " +
			"a wrong guess must never lock out the legitimate holder")
	}
}

// TestVerifyToken_VaryingSecretsAgainstActiveTokenReachesUpstreamEachTime
// documents and pins the accepted tradeoff, stated plainly per the
// fix-round-2 resolution: against an ACTIVE token, this cache does NOT
// rate-limit repeated wrong-secret guesses — every distinct wrong secret
// reaches pipeline-api's /viewer-tokens/verify. Anti-hammering for that case
// is app-worker's own per-(client IP, app) rate limiter's job (spec §5.3,
// W4), not this cache's — duplicating it here at the (app_id, token_id)
// granularity is exactly the mistake fix round 1 made.
func TestVerifyToken_VaryingSecretsAgainstActiveTokenReachesUpstreamEachTime(t *testing.T) {
	var verifyCalls, activeCalls int32
	mux := http.NewServeMux()
	registerVerifyTokenFakeRoutes(mux, "correct-secret", true /* ACTIVE */, &verifyCalls, &activeCalls)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	ctx := context.Background()
	const attempts = 10
	for i := 0; i < attempts; i++ {
		secret := fmt.Sprintf("guess-%d", i)
		if ok, err := c.VerifyToken(ctx, "app-1", "tok-1", secret); err != nil || ok {
			t.Fatalf("attempt %d (secret %q): %v, %v; want false, nil", i, secret, ok, err)
		}
	}
	if n := atomic.LoadInt32(&verifyCalls); n != attempts {
		t.Errorf("verify calls for %d different wrong secrets against an ACTIVE token = %d, want %d "+
			"(varying the secret must reach upstream every time — rate-limiting this is W4's job, not this cache's)",
			attempts, n, attempts)
	}
	// The active-check itself is still cheap: its own 15s cache absorbs
	// every classification after the first.
	if n := atomic.LoadInt32(&activeCalls); n != 1 {
		t.Errorf("active-check calls = %d, want exactly 1 (costs at most one extra call per token per 15s)", n)
	}
}

// TestVerifyToken_PositiveEntryNeverKeyedWithoutSecret guards the OTHER
// half of the asymmetry: only the inactive-token cache may drop the secret
// from its key. A positive result must still require the exact secret that
// earned it.
func TestVerifyToken_PositiveEntryNeverKeyedWithoutSecret(t *testing.T) {
	var verifyCalls, activeCalls int32
	mux := http.NewServeMux()
	registerVerifyTokenFakeRoutes(mux, "correct-secret", true /* ACTIVE */, &verifyCalls, &activeCalls)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	ctx := context.Background()
	if ok, err := c.VerifyToken(ctx, "app-1", "tok-1", "correct-secret"); err != nil || !ok {
		t.Fatalf("correct secret: %v, %v", ok, err)
	}
	// A DIFFERENT wrong secret for the same (app_id, token_id), tried right
	// after the correct one populated the positive cache, must still be
	// independently verified (and rejected) — never served "true" from the
	// positive entry the correct secret earned.
	if ok, err := c.VerifyToken(ctx, "app-1", "tok-1", "another-wrong-secret"); err != nil || ok {
		t.Fatalf("wrong secret after a cached correct one = %v, %v; want false, nil", ok, err)
	}
}

func TestVerifyToken_DifferentSecretsAreNotConflated(t *testing.T) {
	// The server answers ok=true only for "correct-secret".
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/viewer-tokens/verify", func(w http.ResponseWriter, r *http.Request) {
		var req verifyTokenRequestWire
		_ = json.NewDecoder(r.Body).Decode(&req)
		writeJSONResp(w, http.StatusOK, map[string]bool{"ok": req.Secret == "correct-secret"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	ctx := context.Background()
	if ok, err := c.VerifyToken(ctx, "app-1", "tok-1", "correct-secret"); err != nil || !ok {
		t.Fatalf("correct secret: %v, %v", ok, err)
	}
	// A wrong secret for the SAME (app_id, token_id), presented right
	// after a correct one was cached, must still be verified for REAL
	// (and answer false) — not silently served "true" from a cache keyed
	// only on (app_id, token_id).
	if ok, err := c.VerifyToken(ctx, "app-1", "tok-1", "wrong-secret"); err != nil || ok {
		t.Fatalf("wrong secret = %v, %v; want false, nil", ok, err)
	}
}

func TestVerifyToken_RequestShape(t *testing.T) {
	log := &requestLog{}
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/viewer-tokens/verify", func(w http.ResponseWriter, r *http.Request) {
		log.record(r)
		writeJSONResp(w, http.StatusOK, map[string]bool{"ok": true})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	if _, err := c.VerifyToken(context.Background(), "app-1", "tok-1", "sekrit"); err != nil {
		t.Fatal(err)
	}
	req, ok := log.last("/internal/v1/viewer-tokens/verify")
	if !ok {
		t.Fatal("no request recorded")
	}
	if req.method != http.MethodPost {
		t.Errorf("method = %s, want POST", req.method)
	}
	if got := req.headers.Get("Authorization"); got != "Bearer "+testServiceToken {
		t.Errorf("Authorization = %q, want service token", got)
	}
	var body verifyTokenRequestWire
	if err := json.Unmarshal(req.body, &body); err != nil {
		t.Fatal(err)
	}
	want := verifyTokenRequestWire{AppID: "app-1", TokenID: "tok-1", Secret: "sekrit"}
	if body != want {
		t.Errorf("body = %+v, want %+v", body, want)
	}
}

// ---------------------------------------------------------------------------
// CheckTokenActive — the secret-less revocation recheck (W3 fix, Blocker 1)
// ---------------------------------------------------------------------------

func TestCheckTokenActive_RequestShapeAndServiceCredential(t *testing.T) {
	log := &requestLog{}
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/viewer-tokens/active", func(w http.ResponseWriter, r *http.Request) {
		log.record(r)
		writeJSONResp(w, http.StatusOK, map[string]bool{"active": true})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	active, err := c.CheckTokenActive(context.Background(), "app-1", "tok-1")
	if err != nil {
		t.Fatalf("CheckTokenActive: %v", err)
	}
	if !active {
		t.Errorf("active = false, want true")
	}

	req, ok := log.last("/internal/v1/viewer-tokens/active")
	if !ok {
		t.Fatal("no request recorded")
	}
	if req.method != http.MethodPost {
		t.Errorf("method = %s, want POST", req.method)
	}
	// The SERVICE credential authenticates this call — never an app JWT
	// (this endpoint sits behind the same requireServiceToken gate as
	// every other /internal/v1/* worker-facing route, unlike Query's
	// deliberately different /internal/v1/projects/{pid}/query).
	if got := req.headers.Get("Authorization"); got != "Bearer "+testServiceToken {
		t.Errorf("Authorization = %q, want the service token", got)
	}
	var body tokenActiveRequestWire
	if err := json.Unmarshal(req.body, &body); err != nil {
		t.Fatal(err)
	}
	want := tokenActiveRequestWire{AppID: "app-1", TokenID: "tok-1"}
	if body != want {
		t.Errorf("body = %+v, want %+v (no secret field at all)", body, want)
	}
}

func TestCheckTokenActive_CachesWithin15sFakeClock(t *testing.T) {
	var calls int32
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/viewer-tokens/active", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		writeJSONResp(w, http.StatusOK, map[string]bool{"active": true})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	clock := newFakeClock()
	c := NewAPIClient(srv.URL, testServiceToken, WithClock(clock.Now))
	defer c.Close()

	ctx := context.Background()
	if _, err := c.CheckTokenActive(ctx, "app-1", "tok-1"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(14 * time.Second)
	if _, err := c.CheckTokenActive(ctx, "app-1", "tok-1"); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Errorf("calls = %d, want 1 (14s < 15s TTL)", n)
	}

	clock.Advance(2 * time.Second) // total 16s
	if _, err := c.CheckTokenActive(ctx, "app-1", "tok-1"); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Errorf("calls = %d, want 2 (TTL expired at 16s)", n)
	}
}

func TestCheckTokenActive_FalseForRevokedUnknownOrMismatched(t *testing.T) {
	// The fake server mirrors pipeline-api's actual "false, indistinguishably"
	// contract: only exactly (app-1, tok-1) is active.
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/viewer-tokens/active", func(w http.ResponseWriter, r *http.Request) {
		var req tokenActiveRequestWire
		_ = json.NewDecoder(r.Body).Decode(&req)
		writeJSONResp(w, http.StatusOK, map[string]bool{
			"active": req.AppID == "app-1" && req.TokenID == "tok-1",
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	ctx := context.Background()
	if active, err := c.CheckTokenActive(ctx, "app-1", "tok-1"); err != nil || !active {
		t.Fatalf("live token: %v, %v; want true, nil", active, err)
	}
	if active, err := c.CheckTokenActive(ctx, "app-2", "tok-1"); err != nil || active {
		t.Fatalf("app mismatch: %v, %v; want false, nil", active, err)
	}
	if active, err := c.CheckTokenActive(ctx, "app-1", "unknown-tok"); err != nil || active {
		t.Fatalf("unknown token: %v, %v; want false, nil", active, err)
	}
}

// ---------------------------------------------------------------------------
// VerifySession — the header hand-off
// ---------------------------------------------------------------------------

func TestVerifySession_ForwardsBearerInSeparateHeader(t *testing.T) {
	log := &requestLog{}
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/sessions/verify", func(w http.ResponseWriter, r *http.Request) {
		log.record(r)
		writeJSONResp(w, http.StatusOK, map[string]any{"user_id": "user-1", "project_member": true})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	info, err := c.VerifySession(context.Background(), "proj-1", "", "Bearer caller-cli-jwt")
	if err != nil {
		t.Fatalf("VerifySession: %v", err)
	}
	if info != (SessionInfo{UserID: "user-1", ProjectMember: true}) {
		t.Errorf("info = %+v", info)
	}

	req, _ := log.last("/internal/v1/sessions/verify")
	// The SERVICE credential must occupy Authorization...
	if got := req.headers.Get("Authorization"); got != "Bearer "+testServiceToken {
		t.Errorf("Authorization = %q, want service token (never the caller's)", got)
	}
	// ...and the CALLER's credential must ride in the forwarding header,
	// verbatim, including the "Bearer " scheme prefix.
	if got := req.headers.Get(forwardedAuthorizationHeader); got != "Bearer caller-cli-jwt" {
		t.Errorf("%s = %q, want the caller's bearer verbatim", forwardedAuthorizationHeader, got)
	}
}

func TestVerifySession_ForwardsCookieAndOmitsForwardingHeaderWhenNoAuthz(t *testing.T) {
	log := &requestLog{}
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/sessions/verify", func(w http.ResponseWriter, r *http.Request) {
		log.record(r)
		writeJSONResp(w, http.StatusOK, map[string]any{"user_id": "user-2", "project_member": false})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	if _, err := c.VerifySession(context.Background(), "proj-1", "pipeline_api_session=abc123", ""); err != nil {
		t.Fatalf("VerifySession: %v", err)
	}
	req, _ := log.last("/internal/v1/sessions/verify")
	if got := req.headers.Get("Cookie"); got != "pipeline_api_session=abc123" {
		t.Errorf("Cookie = %q", got)
	}
	// Must NOT synthesize a forwarding header when the caller carried none.
	if _, present := req.headers[forwardedAuthorizationHeader]; present {
		t.Errorf("%s must be absent when authz is empty, got present", forwardedAuthorizationHeader)
	}
	// The service credential must still be the only thing in Authorization.
	if got := req.headers.Get("Authorization"); got != "Bearer "+testServiceToken {
		t.Errorf("Authorization = %q, want service token", got)
	}
}

func TestVerifySession_NeverPutsServiceTokenInForwardingHeader(t *testing.T) {
	// Regression guard for the exact P3 defect: calling VerifySession with
	// no caller credential at all must never leak the service token into
	// the forwarding header either.
	log := &requestLog{}
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/sessions/verify", func(w http.ResponseWriter, r *http.Request) {
		log.record(r)
		writeJSONResp(w, http.StatusOK, map[string]any{"user_id": "", "project_member": false})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	if _, err := c.VerifySession(context.Background(), "proj-1", "", ""); err != nil {
		t.Fatal(err)
	}
	req, _ := log.last("/internal/v1/sessions/verify")
	if _, present := req.headers[forwardedAuthorizationHeader]; present {
		t.Errorf("forwarding header must be absent, got %q", req.headers.Get(forwardedAuthorizationHeader))
	}
	if got := req.headers.Get("Authorization"); got != "Bearer "+testServiceToken {
		t.Errorf("Authorization = %q, want ONLY the service token", got)
	}
}

func TestVerifySession_CachesPerSessionWithin15s(t *testing.T) {
	var calls int32
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/sessions/verify", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		writeJSONResp(w, http.StatusOK, map[string]any{"user_id": "u1", "project_member": true})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	clock := newFakeClock()
	c := NewAPIClient(srv.URL, testServiceToken, WithClock(clock.Now))
	defer c.Close()

	ctx := context.Background()
	if _, err := c.VerifySession(ctx, "proj-1", "session=abc", ""); err != nil {
		t.Fatal(err)
	}
	clock.Advance(14 * time.Second)
	if _, err := c.VerifySession(ctx, "proj-1", "session=abc", ""); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Errorf("calls = %d, want 1 (cached at 14s)", n)
	}

	// A DIFFERENT session (different cookie) must NOT hit the same cache
	// entry.
	if _, err := c.VerifySession(ctx, "proj-1", "session=xyz", ""); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Errorf("calls = %d, want 2 (different session must not share the cache entry)", n)
	}

	clock.Advance(2 * time.Second) // first session now at 16s
	if _, err := c.VerifySession(ctx, "proj-1", "session=abc", ""); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&calls); n != 3 {
		t.Errorf("calls = %d, want 3 (first session's TTL expired)", n)
	}
}

func TestVerifySession_NoSessionIsNotAnError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/sessions/verify", func(w http.ResponseWriter, r *http.Request) {
		writeJSONResp(w, http.StatusOK, map[string]any{"user_id": "", "project_member": false})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	info, err := c.VerifySession(context.Background(), "proj-1", "", "")
	if err != nil {
		t.Fatalf("VerifySession: %v", err)
	}
	if info.UserID != "" || info.ProjectMember {
		t.Errorf("info = %+v, want zero value", info)
	}
}

// ---------------------------------------------------------------------------
// Impersonate — never cached
// ---------------------------------------------------------------------------

func TestImpersonate_NeverCached(t *testing.T) {
	var calls int32
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/impersonate", func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if got := r.Header.Get("Authorization"); got != "Bearer "+testServiceToken {
			t.Errorf("Authorization = %q, want service token", got)
		}
		var req impersonateRequestWire
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.AppID != "app-1" {
			t.Errorf("app_id = %q, want app-1", req.AppID)
		}
		writeJSONResp(w, http.StatusOK, map[string]string{"token": fmt.Sprintf("tok-%d", n)})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	ctx := context.Background()
	tok1, err := c.Impersonate(ctx, "app-1")
	if err != nil {
		t.Fatal(err)
	}
	tok2, err := c.Impersonate(ctx, "app-1")
	if err != nil {
		t.Fatal(err)
	}
	if tok1 == tok2 {
		t.Fatalf("both calls returned %q; Impersonate must mint fresh every time (spec §5.4)", tok1)
	}
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Errorf("upstream calls = %d, want 2 (never cached)", n)
	}
}

// ---------------------------------------------------------------------------
// AppendLog — async, drop-on-full, never blocks
// ---------------------------------------------------------------------------

func TestAppendLog_PostsRecordShape(t *testing.T) {
	log := &requestLog{}
	done := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/apps/app-1/logs", func(w http.ResponseWriter, r *http.Request) {
		log.record(r)
		w.WriteHeader(http.StatusNoContent)
		close(done)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken, WithLogPostTimeout(2*time.Second))
	defer c.Close()

	started := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	rec := RenderLogRecord{
		RequestID:     "req-1",
		AppID:         "app-1",
		VersionHash:   strings.Repeat("f", 64),
		Channel:       "production",
		PrincipalKind: "viewer_token",
		PrincipalID:   "tok-1",
		StartedAt:     started,
		DurationMS:    42,
		Outcome:       "ok",
		LogText:       "hello",
	}
	if err := c.AppendLog(context.Background(), rec); err != nil {
		t.Fatalf("AppendLog: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for background POST")
	}

	req, ok := log.last("/internal/v1/apps/app-1/logs")
	if !ok {
		t.Fatal("no request recorded")
	}
	if got := req.headers.Get("Authorization"); got != "Bearer "+testServiceToken {
		t.Errorf("Authorization = %q", got)
	}
	var wire renderLogRequestWire
	if err := json.Unmarshal(req.body, &wire); err != nil {
		t.Fatal(err)
	}
	if wire.RequestID != "req-1" || wire.VersionHash != rec.VersionHash || wire.Outcome != "ok" || wire.DurationMS != 42 {
		t.Errorf("wire = %+v", wire)
	}
	if wire.StartedAt != started.Format(time.RFC3339Nano) {
		t.Errorf("StartedAt = %q, want RFC3339Nano of %v", wire.StartedAt, started)
	}
}

func TestAppendLog_DropsOnFullQueueWithoutBlocking(t *testing.T) {
	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/apps/app-1/logs", func(w http.ResponseWriter, r *http.Request) {
		<-release // hold every request open so the queue backs up
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Queue size 1: the first AppendLog is picked up by the (now-blocked)
	// consumer goroutine, the second fills the buffered channel, and the
	// third must be dropped.
	c := NewAPIClient(srv.URL, testServiceToken, WithLogQueueSize(1))
	defer func() {
		close(release)
		c.Close()
	}()

	mkRec := func(id string) RenderLogRecord {
		return RenderLogRecord{
			RequestID: id, AppID: "app-1", VersionHash: strings.Repeat("a", 64),
			Channel: "production", PrincipalKind: "viewer_token", PrincipalID: "t",
			Outcome: "ok",
		}
	}

	if err := c.AppendLog(context.Background(), mkRec("1")); err != nil {
		t.Fatalf("AppendLog #1 (consumer picks this up immediately): %v", err)
	}
	// Give the background goroutine time to dequeue #1 into the blocked
	// HTTP call, freeing the buffer slot for exactly one more.
	time.Sleep(50 * time.Millisecond)

	if err := c.AppendLog(context.Background(), mkRec("2")); err != nil {
		t.Fatalf("AppendLog #2 (should fit the buffer): %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- c.AppendLog(context.Background(), mkRec("3"))
	}()
	select {
	case err := <-done:
		if err != ErrLogQueueFull {
			t.Errorf("AppendLog #3 = %v, want ErrLogQueueFull", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("AppendLog blocked instead of dropping — must never block a render")
	}
}

// ---------------------------------------------------------------------------
// Query — the route/credential hand-off
// ---------------------------------------------------------------------------

func TestQuery_HitsInternalProjectsRouteWithAppJWTOnly(t *testing.T) {
	log := &requestLog{}
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/projects/proj-1/query", func(w http.ResponseWriter, r *http.Request) {
		log.record(r)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"rows":[]}`))
	})
	// Also register the browser route to prove Query does NOT call it.
	var browserCalls int32
	mux.HandleFunc("/api/v1/projects/proj-1/query", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&browserCalls, 1)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	body := []byte(`{"sql":"select 1"}`)
	resp, err := c.Query(context.Background(), "proj-1", "app-impersonation-jwt", body)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d", resp.StatusCode)
	}
	if string(respBody) != `{"rows":[]}` {
		t.Errorf("body = %q", respBody)
	}

	req, ok := log.last("/internal/v1/projects/proj-1/query")
	if !ok {
		t.Fatal("Query did not call /internal/v1/projects/{pid}/query")
	}
	if req.method != http.MethodPost {
		t.Errorf("method = %s, want POST", req.method)
	}
	if string(req.body) != string(body) {
		t.Errorf("body forwarded = %q, want %q", req.body, body)
	}
	// The app's impersonation JWT is the ENTIRE credential: no service
	// token anywhere on this call.
	if got := req.headers.Get("Authorization"); got != "Bearer app-impersonation-jwt" {
		t.Errorf("Authorization = %q, want the app JWT alone", got)
	}
	if atomic.LoadInt32(&browserCalls) != 0 {
		t.Error("Query must never hit the browser-facing /api/v1/projects/{pid}/query route")
	}
}

func TestQuery_PropagatesContextCancellation(t *testing.T) {
	// The handler blocks until EITHER the request context is cancelled (the
	// behavior under test) OR a generous fallback fires — the fallback
	// exists purely so the handler always returns and httptest.Server.Close
	// never hangs; it is not part of the assertion (mirrors
	// task-P5-report.md's documented fix for this exact flaky pattern:
	// asserting the SERVER observed cancellation is unreliable on
	// localhost socket teardown, so the deterministic assertion is "the
	// CLIENT returns fast", not "the server's ctx fires").
	const fallback = 3 * time.Second
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/projects/proj-1/query", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(fallback):
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAPIClient(srv.URL, testServiceToken)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := c.Query(ctx, "proj-1", "jwt", []byte(`{}`))
	if err == nil {
		t.Fatal("expected an error from a cancelled request")
	}
	if elapsed := time.Since(start); elapsed > fallback/2 {
		t.Errorf("Query took %v to return after cancellation, want well under the %v fallback", elapsed, fallback)
	}
}

// ---------------------------------------------------------------------------
// small helpers used only by tests
// ---------------------------------------------------------------------------

// asAPIError is a tiny errors.As wrapper kept local to avoid importing
// "errors" into the production file just for this test helper's sake.
func asAPIError(err error, target **APIError) bool {
	ae, ok := err.(*APIError)
	if !ok {
		return false
	}
	*target = ae
	return true
}
