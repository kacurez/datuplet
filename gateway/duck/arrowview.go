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
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	bindings "github.com/duckdb/duckdb-go-bindings"
)

var errArrowScan = errors.New("duck: arrow scan registration failed")

// arrowView is a registered view and the C stream behind it. The stream is
// freed only once the view is dropped or the connection is disconnected, so
// no statement can ever scan freed memory.
type arrowView struct {
	name   string
	stream unsafe.Pointer // C-allocated ArrowArrayStream
}

func (v *arrowView) free() {
	cdata.ReleaseCArrowArrayStream((*cdata.CArrowArrayStream)(v.stream))
	C.free(v.stream)
	v.stream = nil
}

// registerArrowView mirrors duckdb-go/v2 Arrow.RegisterView (arrow.go): the
// reader is exported into a C-allocated ArrowArrayStream that DuckDB's
// arrow_scan reads through a temporary view.
func registerArrowView(c *Conn, name string, r array.RecordReader) (func(), error) {
	v := &arrowView{name: name, stream: C.calloc(1, C.sizeof_struct_ArrowArrayStream)}
	cdata.ExportRecordReader(r, (*cdata.CArrowArrayStream)(v.stream))
	if bindings.ArrowScan(c.conn, name, bindings.ArrowStream{Ptr: v.stream}) == bindings.StateError {
		v.free()
		return nil, errArrowScan
	}
	var once sync.Once
	return func() { once.Do(func() { c.releaseView(v) }) }, nil
}
