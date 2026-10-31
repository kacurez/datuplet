//go:build duckdb_arrow

package duck

/*
#include <stdlib.h>
#include <stdint.h>

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;
  void (*release)(struct ArrowSchema*);
  void* private_data;
};

struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;
  void (*release)(struct ArrowArray*);
  void* private_data;
};

struct ArrowArrayStream {
  void (*get_schema)(struct ArrowArrayStream*);
  void (*get_next)(struct ArrowArrayStream*);
  void (*get_last_error)(struct ArrowArrayStream*);
  void (*release)(struct ArrowArrayStream*);
  void* private_data;
};

#endif  // ARROW_C_DATA_INTERFACE
*/
import "C"

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	bindings "github.com/duckdb/duckdb-go-bindings"
)

var (
	errArrowScan    = errors.New("duck: arrow scan registration failed")
	errViewReleased = errors.New("duck: arrow view was released")
)

// Lifetime of a registered view's C stream: freed only at Conn.Close, after
// Disconnect has destroyed the connection's temporary views.
//
// Dropping the view does not make freeing safe. A DROP that runs inside the
// caller's transaction is undone by ROLLBACK, which restores a view pointing
// at the stream. The C API exposes no autocommit flag, and probing with BEGIN
// is not side-effect free (a nested BEGIN aborts the caller's transaction), so
// the package cannot tell when a DROP is durable. Instead, release detaches
// the caller's RecordReader from the exported stream (viewReader.detach): the
// reader and its batches are released at once, and a pending view pins only
// the ArrowArrayStream struct and an empty viewReader. A view that survives
// (DROP failed or rolled back) fails its scan with errViewReleased.

// viewReader is the RecordReader actually exported to DuckDB. It forwards to
// the caller's reader until detach, then reports errViewReleased.
type viewReader struct {
	schema *arrow.Schema
	refs   atomic.Int64

	mu sync.Mutex
	r  array.RecordReader
}

func newViewReader(r array.RecordReader) *viewReader {
	r.Retain()
	v := &viewReader{schema: r.Schema(), r: r}
	v.refs.Store(1)
	return v
}

func (v *viewReader) Retain() { v.refs.Add(1) }

func (v *viewReader) Release() {
	if v.refs.Add(-1) == 0 {
		v.detach()
	}
}

func (v *viewReader) Schema() *arrow.Schema { return v.schema }

func (v *viewReader) Next() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.r != nil && v.r.Next()
}

func (v *viewReader) RecordBatch() arrow.RecordBatch {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.r == nil {
		return nil
	}
	return v.r.RecordBatch()
}

// Record returns RecordBatch().
//
// Deprecated: use RecordBatch.
func (v *viewReader) Record() arrow.RecordBatch { return v.RecordBatch() }

func (v *viewReader) Err() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.r == nil {
		return errViewReleased
	}
	return v.r.Err()
}

// detach releases the caller's reader; later scans see errViewReleased.
func (v *viewReader) detach() {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.r != nil {
		v.r.Release()
		v.r = nil
	}
}

// arrowView is a registered view: the C stream DuckDB's arrow_scan holds a
// pointer to, and the viewReader exported into it.
type arrowView struct {
	name   string
	reader *viewReader
	stream unsafe.Pointer // C-allocated ArrowArrayStream
}

// free releases the exported stream and its C allocation. Only safe once no
// view can reach it: after Disconnect, or when registration failed.
func (v *arrowView) free() {
	cdata.ReleaseCArrowArrayStream((*cdata.CArrowArrayStream)(v.stream))
	C.free(v.stream)
	v.stream = nil
}

// registerArrowView mirrors duckdb-go/v2 Arrow.RegisterView (arrow.go): the
// reader is exported into a C-allocated ArrowArrayStream that DuckDB's
// arrow_scan reads through a temporary view.
func registerArrowView(c *Conn, name string, r array.RecordReader) (func(), error) {
	v := &arrowView{name: name, reader: newViewReader(r), stream: C.calloc(1, C.sizeof_struct_ArrowArrayStream)}
	cdata.ExportRecordReader(v.reader, (*cdata.CArrowArrayStream)(v.stream))
	if bindings.ArrowScan(c.conn, name, bindings.ArrowStream{Ptr: v.stream}) == bindings.StateError {
		v.free()
		v.reader.Release()
		return nil, errArrowScan
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		v.free()
		v.reader.Release()
		return nil, ErrClosed
	}
	c.views = append(c.views, v)
	c.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { c.releaseView(v) }) }, nil
}
