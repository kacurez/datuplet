package datagateway

import (
	"log"
	"sync"
	"time"
)

// readStats counts what one reader served and logs one summary line when the
// read ends. It has its own lock so fail and close can finish it while
// holding the reader's mu. Table identity and counts only: no credentials,
// storage paths or tokens.
type readStats struct {
	namespace, table, format string
	start                    time.Time

	mu           sync.Mutex
	rawFiles     int
	decodedFiles int
	rows         int64
	bytes        int64
	chunks       int
	finished     bool
}

func newReadStats(namespace, table, format string) *readStats {
	return &readStats{namespace: namespace, table: table, format: format, start: time.Now()}
}

func (st *readStats) addRawFile() {
	st.mu.Lock()
	st.rawFiles++
	st.mu.Unlock()
}

func (st *readStats) addDecodedFiles(n int) {
	if n <= 0 {
		n = 1
	}
	st.mu.Lock()
	st.decodedFiles += n
	st.mu.Unlock()
}

// addChunk counts a produced chunk of bytes bytes and rows rows.
func (st *readStats) addChunk(bytes, rows int64) {
	st.mu.Lock()
	st.chunks++
	st.bytes += bytes
	st.rows += rows
	st.mu.Unlock()
}

// finish logs the summary once; later calls do nothing. outcome is ok, error
// or cancelled.
func (st *readStats) finish(outcome string) {
	st.mu.Lock()
	if st.finished {
		st.mu.Unlock()
		return
	}
	st.finished = true
	raw, dec, rows, bytes, chunks := st.rawFiles, st.decodedFiles, st.rows, st.bytes, st.chunks
	st.mu.Unlock()
	log.Printf("read summary: table=%s.%s format=%s raw_files=%d decoded_files=%d rows=%d bytes=%d chunks=%d outcome=%s duration=%s",
		st.namespace, st.table, st.format, raw, dec, rows, bytes, chunks, outcome, time.Since(st.start).Round(time.Millisecond))
}
