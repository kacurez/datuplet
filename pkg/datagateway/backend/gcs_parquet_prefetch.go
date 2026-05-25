package backend

import (
	"context"
	"fmt"
	"io"
	"sync"

	"cloud.google.com/go/storage"
)

// parquetPrefetchConcurrency caps the number of source parquet files
// downloaded from GCS in parallel. Tuning notes:
//
//   - Higher (8-16) = more network parallelism but multiplies in-flight
//     memory (each in-flight file's bytes are held until the emitter
//     drains it). At 4, with typical iceberg snapshot files of ~16 MiB,
//     peak gateway prefetch memory is bounded at ~64 MiB.
//   - Lower (1-2) = essentially sequential, defeats the optimisation.
//   - 4 is empirically a good fit for GKE's e2-standard / t2a-standard
//     node networking + GCS's per-connection throughput.
//
// Each slot holds one downloaded file's bytes until the emitter goroutine
// drains it; the worker goroutines also each hold one in-flight file
// while reading from GCS. The total in-flight ceiling is therefore
// 2*parquetPrefetchConcurrency files worth of memory.
const parquetPrefetchConcurrency = 4

// parquetPrefetcher downloads source parquet files from GCS in bounded
// parallel and emits the bytes back to the caller in original order.
// The caller drains by calling Next() until io.EOF.
//
// Why parallel: gcsReader.readParquetFile previously did one file at a
// time, blocking on GCS per-object roundtrip + body transfer. The
// 5M-row staging benchmark spent ~5 min in this serial loop while
// gRPC stream consumption on the downstream side was network-bound.
// With 4-way prefetch the gateway saturates its egress while GCS
// pulls overlap, shaving the staging phase wallclock.
//
// Ordering matters because iceberg-go's data-file list defines the
// per-row-group iteration order; we emit files in that order so
// downstream consumers (sql-transform component, format adapter
// passthrough path) see a deterministic stream.
type parquetPrefetcher struct {
	bkt    *storage.BucketHandle
	files  []fileInfo
	slots  []chan prefetchedFile // one buffered slot per file, in order
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

// newParquetPrefetcher kicks off N worker goroutines that fetch the
// given files in bounded parallel. Results land in per-file slots in
// order; callers read with Next(). The prefetcher derives its own
// context from `parent`; calling Close cancels all in-flight workers.
func newParquetPrefetcher(parent context.Context, bkt *storage.BucketHandle, files []fileInfo) *parquetPrefetcher {
	ctx, cancel := context.WithCancel(parent)
	p := &parquetPrefetcher{
		bkt:    bkt,
		files:  files,
		slots:  make([]chan prefetchedFile, len(files)),
		cancel: cancel,
	}
	for i := range p.slots {
		// Buffered to 1 so a worker can deposit its result and exit
		// without waiting on the emitter.
		p.slots[i] = make(chan prefetchedFile, 1)
	}
	// Bounded concurrency via a semaphore + per-file goroutine. The
	// semaphore caps in-flight memory; goroutines themselves are
	// cheap so spawning one per file is fine.
	go p.run(ctx)
	return p
}

// run is the dispatcher: spawns at most parquetPrefetchConcurrency
// concurrent fetches, then waits for all to settle (or ctx cancel).
func (p *parquetPrefetcher) run(ctx context.Context) {
	sem := make(chan struct{}, parquetPrefetchConcurrency)
	var wg sync.WaitGroup
	for idx, f := range p.files {
		select {
		case <-ctx.Done():
			// Ctx cancelled before all jobs queued — fill remaining
			// slots with a cancel error so Next() doesn't deadlock.
			for ; idx < len(p.files); idx++ {
				p.slots[idx] <- prefetchedFile{err: ctx.Err()}
			}
			wg.Wait()
			return
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(idx int, fi fileInfo) {
			defer wg.Done()
			defer func() { <-sem }()
			p.slots[idx] <- p.fetch(ctx, fi)
		}(idx, f)
	}
	wg.Wait()
}

// fetch downloads one file's full bytes. We use io.ReadAll because the
// downstream protocol emits each source file as one self-contained
// DataChunk — partial reads don't compose. The GCS SDK already does
// HTTP-level chunking internally, so io.ReadAll is just one allocation
// at the end.
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
