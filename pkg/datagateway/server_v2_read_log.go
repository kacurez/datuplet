package datagateway

import (
	"log"
	"time"
)

// readStats counts what one reader produced and logs one summary line when
// the read ends. It is guarded by the owning readerState's mu: counters are
// updated where chunks are produced and the summary is finalized under the
// same lock, so a summary is always internally consistent. Table identity and
// counts only: no credentials, storage paths or tokens.
type readStats struct {
	namespace, table, format string
	start                    time.Time

	rawFiles int
	// decodedStarts has one counter per decoded part, each reporting how many
	// of the part's scan tasks the plane has started decoding.
	decodedStarts []func() int
	rows          int64
	bytes         int64
	chunks        int
	finished      bool
}

func newReadStats(namespace, table, format string) *readStats {
	return &readStats{namespace: namespace, table: table, format: format, start: time.Now()}
}

// addChunk counts a produced chunk of bytes bytes and rows rows.
func (st *readStats) addChunk(bytes, rows int64) {
	st.chunks++
	st.bytes += bytes
	st.rows += rows
}

// finishLocked logs the summary once; later calls do nothing. outcome is ok,
// error or cancelled. The caller holds the reader's mu.
func (st *readStats) finishLocked(outcome string) {
	if st.finished {
		return
	}
	st.finished = true
	decoded := 0
	for _, started := range st.decodedStarts {
		decoded += started()
	}
	log.Printf("read summary: table=%s.%s format=%s raw_files=%d decoded_files=%d rows=%d bytes=%d chunks=%d outcome=%s duration=%s",
		st.namespace, st.table, st.format, st.rawFiles, decoded, st.rows, st.bytes, st.chunks, outcome, time.Since(st.start).Round(time.Millisecond))
}
