package backend

import (
	"context"
	"fmt"
	"io"
	"sync"

	"cloud.google.com/go/storage"
)

// parquetPrefetchConcurrency caps the number of source parquet files
// held in gateway memory at any moment — equivalently, the number of
// in-flight GCS fetches plus the number of fully-downloaded but
// not-yet-emitted files.
//
// Token-based bound (see run + Next): the prefetcher pre-fills N
// "memory tokens". A worker must acquire a token before fetching;
// the token is only released by Next when the emitter has drained the
// corresponding result and the bytes are no longer needed gateway-side.
// Worst-case in-flight memory therefore = N * largest_file_size.
//
// Tuning:
//   - Higher (8-16): more GCS connections in parallel, more memory.
//   - Lower (1-2): essentially sequential, defeats the optimisation.
//   - 4 is empirically a good fit for GKE's e2-standard / t2a-standard
//     node networking + GCS's per-connection throughput. For typical
//     iceberg snapshot files of ~16 MiB this caps gateway prefetch
//     memory at ~64 MiB; for 250 MiB files (wide-string heavy
//     iceberg tables) it caps at ~1 GiB.
const parquetPrefetchConcurrency = 4

// parquetPrefetcher downloads source parquet files from GCS in bounded
// parallel and emits the bytes back to the caller in original order
// via Next(). Memory is hard-capped at parquetPrefetchConcurrency
// in-memory files at any moment.
//
// Why parallel: gcsReader.readParquetFile previously did one file at a
// time, blocking on GCS per-object roundtrip + body transfer. The
// 5M-row staging benchmark spent ~5 min in this serial loop. With
// 4-way prefetch the same workload completes in ~51 s.
//
// Ordering matters because iceberg-go's data-file list defines the
// per-row-group iteration order; we emit files in that order so
// downstream consumers (sql-transform component, format adapter
// passthrough path) see a deterministic stream.
type parquetPrefetcher struct {
	bkt    *storage.BucketHandle
	files  []fileInfo
	slots  []chan prefetchedFile // one buffered slot per file, in order
	tokens chan struct{}         // memory budget: see parquetPrefetchConcurrency
	cancel context.CancelFunc
	emit   int  // next index to emit from slots
	done   bool // true after EOF or first hard error
}

// prefetchedFile carries one source parquet file's downloaded bytes
// (or the error that prevented the download).
type prefetchedFile struct {
	bytes []byte
	err   error
}

// newParquetPrefetcher kicks off the dispatcher goroutine that fetches
// the given files in bounded parallel. Results land in per-file slots
// in order; callers read with Next(). The prefetcher derives its own
// context from `parent`; calling Close cancels all in-flight workers.
func newParquetPrefetcher(parent context.Context, bkt *storage.BucketHandle, files []fileInfo) *parquetPrefetcher {
	ctx, cancel := context.WithCancel(parent)
	p := &parquetPrefetcher{
		bkt:    bkt,
		files:  files,
		slots:  make([]chan prefetchedFile, len(files)),
		tokens: make(chan struct{}, parquetPrefetchConcurrency),
		cancel: cancel,
	}
	for i := range p.slots {
		// Buffered to 1 so a worker can deposit its result and exit
		// without waiting on the emitter.
		p.slots[i] = make(chan prefetchedFile, 1)
	}
	// Pre-fill the token bucket: N tokens initially available, so the
	// first N fetches start immediately.
	for i := 0; i < parquetPrefetchConcurrency; i++ {
		p.tokens <- struct{}{}
	}
	go p.run(ctx)
	return p
}

// run dispatches fetches respecting the memory-token cap. The earlier
// version used a worker-side semaphore that was released as soon as
// the worker deposited to its slot — that bounded *in-flight* fetches
// but NOT the number of fully-downloaded-but-not-yet-emitted files.
// Under a slow emitter, all len(files) results could pile up in
// memory (we observed ~2 GiB gateway peak on a 16-file × 250 MiB
// workload).
//
// The fix: workers acquire a token from p.tokens before fetching, and
// the tokens are NOT released here — Next() releases one after the
// emitter has drained the corresponding slot. So a fetched-but-unemitted
// file still occupies its token, blocking further fetches until the
// emitter catches up. Total in-memory file bytes is therefore bounded
// by parquetPrefetchConcurrency × largest_file_size, independent of
// emitter rate or len(files).
func (p *parquetPrefetcher) run(ctx context.Context) {
	var wg sync.WaitGroup
	for idx, f := range p.files {
		// Acquire memory token before launching the fetch. Blocks if
		// the emitter is behind — exactly the backpressure we want.
		select {
		case <-ctx.Done():
			// Ctx cancelled before all jobs queued — fill remaining
			// slots with a cancel error so Next() doesn't deadlock.
			for ; idx < len(p.files); idx++ {
				select {
				case p.slots[idx] <- prefetchedFile{err: ctx.Err()}:
				case <-ctx.Done():
					// ctx already done; the receiver will
					// detect via its own ctx and exit.
				}
			}
			wg.Wait()
			return
		case <-p.tokens:
		}
		wg.Add(1)
		go func(idx int, fi fileInfo) {
			defer wg.Done()
			// Send to the slot. Buffered cap 1 means this is non-blocking
			// even if Next hasn't been called yet; the goroutine can exit
			// and free its locals (including the just-fetched bytes
			// reference) while the channel still holds the value.
			p.slots[idx] <- p.fetch(ctx, fi)
			// Token is NOT released here. Next releases it after the
			// emitter has drained the slot — that's what keeps total
			// in-memory file bytes bounded.
		}(idx, f)
	}
	wg.Wait()
}

// fetch downloads one file's full bytes. We use io.ReadAll because the
// downstream protocol emits each source file as one self-contained
// DataChunk — partial reads don't compose at the protocol level. The
// GCS SDK already does HTTP-level chunking internally, so io.ReadAll
// is just the final allocation.
func (p *parquetPrefetcher) fetch(ctx context.Context, fi fileInfo) prefetchedFile {
	obj, err := p.bkt.Object(fi.path).NewReader(ctx)
	if err != nil {
		return prefetchedFile{err: fmt.Errorf("gcs: open %q: %w", fi.path, err)}
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return prefetchedFile{err: fmt.Errorf("gcs: read %q: %w", fi.path, err)}
	}
	return prefetchedFile{bytes: data}
}

// Next returns the next file's bytes, blocking until that file's
// download completes. Returns io.EOF after the last file has been
// emitted. Returns the worker error verbatim for transient failures
// — caller decides whether to retry or propagate.
//
// Releases one memory token after the slot is drained, unblocking the
// next pending fetch in run().
func (p *parquetPrefetcher) Next(ctx context.Context) ([]byte, bool, error) {
	if p.done {
		return nil, false, io.EOF
	}
	if p.emit >= len(p.slots) {
		p.done = true
		return nil, false, io.EOF
	}
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case pf := <-p.slots[p.emit]:
		p.emit++
		isLast := p.emit >= len(p.slots)
		if isLast {
			p.done = true
		}
		// Return the token so run() can launch another fetch.
		// Non-blocking send: if run() has already exited (ctx
		// cancelled or all files queued), no one is waiting on
		// p.tokens and a blocking send would deadlock. The token
		// bucket size matches the initial count, so a stale send
		// would silently grow the buffer above the cap — also bad.
		// Hence default-case drop, which is the right behaviour
		// when there's no more work to schedule.
		select {
		case p.tokens <- struct{}{}:
		default:
		}
		if pf.err != nil {
			return nil, isLast, pf.err
		}
		return pf.bytes, isLast, nil
	}
}

// Close cancels any in-flight workers and abandons un-emitted slots.
// Idempotent. Safe to call concurrently with Next from the same
// goroutine (which is the only supported access pattern).
func (p *parquetPrefetcher) Close() {
	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
	p.done = true
}
