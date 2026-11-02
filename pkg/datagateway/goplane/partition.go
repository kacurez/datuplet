package goplane

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"

	"github.com/datuplet/datuplet/pkg/datagateway"
)

// maxObjectKeyBytes is the S3 object key limit a data file path must fit
// (spec decision 10).
const maxObjectKeyBytes = 1024

// partitioner computes the partition tuple of each row a writer receives
// (spec decisions 6–7, 10).
type partitioner struct {
	spec   iceberg.PartitionSpec
	schema *iceberg.Schema // the table schema
	fields []partField     // in spec order
}

type partField struct {
	id   int
	name string
	col  int // writer column; -1: null for every row (void, or a source column the writer does not have)
	// conv reads row i of the source column as the Go type of the table
	// column's Iceberg type, without copying strings or bytes.
	conv  func(a arrow.Array, i int) (any, error)
	apply func(v any) any // the transform; nil in, nil out
}

func invalidPartition(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{datagateway.ErrInvalidPartition}, args...)...)
}

// newPartitioner resolves spec (the table's current spec) against the
// table schema and the writer's Arrow schema. Every (Arrow type, table
// type) pair of a non-void source field is checked here once; an
// unsupported pair or transform is an ErrInvalidPartition.
func newPartitioner(spec iceberg.PartitionSpec, sc *iceberg.Schema, writeSchema *arrow.Schema) (*partitioner, error) {
	if sc == nil {
		return nil, invalidPartition("partitioned writer without a table schema")
	}
	p := &partitioner{spec: spec, schema: sc}
	for _, f := range spec.Fields() {
		pf := partField{id: f.FieldID, name: f.Name, col: -1}
		if _, isVoid := f.Transform.(iceberg.VoidTransform); !isVoid {
			src, ok := topLevelByID(sc, f.SourceID())
			if !ok {
				name, _ := sc.FindColumnName(f.SourceID())
				return nil, invalidPartition("partition field %s: source field id %d (%q) is not a top-level column of the table schema", f.Name, f.SourceID(), name)
			}
			if !f.Transform.CanTransform(src.Type) {
				return nil, invalidPartition("partition field %s: transform %s cannot be applied to column %q of type %s", f.Name, f.Transform, src.Name, src.Type)
			}
			apply, err := transformFunc(f.Transform, src.Type)
			if err != nil {
				return nil, invalidPartition("partition field %s: %v", f.Name, err)
			}
			if idx := writeSchema.FieldIndices(src.Name); len(idx) > 0 {
				at := writeSchema.Field(idx[0]).Type
				conv := converter(at, src.Type)
				if conv == nil {
					return nil, invalidPartition("partition field %s: column %q has Arrow type %s, which cannot be converted to the table type %s", f.Name, src.Name, at, src.Type)
				}
				pf.col, pf.conv, pf.apply = idx[0], conv, apply
			}
		}
		p.fields = append(p.fields, pf)
	}
	return p, nil
}

// topLevelByID returns the top-level column of sc with field id; a nested
// field is never bound to a same-named top-level writer column.
func topLevelByID(sc *iceberg.Schema, id int) (iceberg.NestedField, bool) {
	for _, f := range sc.Fields() {
		if f.ID == id {
			return f, true
		}
	}
	return iceberg.NestedField{}, false
}

// transformFunc returns tr applied to a Go value of src's Iceberg type, with
// the result typed per tr.ResultType (iceberg.Date for day).
func transformFunc(tr iceberg.Transform, src iceberg.Type) (func(any) any, error) {
	type timeTransform interface {
		Transformer(iceberg.Type) (func(any) iceberg.Optional[int32], error)
	}
	optional := func(fn func(any) iceberg.Optional[int32], wrap func(int32) any) func(any) any {
		return func(v any) any {
			if v == nil {
				return nil
			}
			o := fn(v)
			if !o.Valid {
				return nil
			}
			return wrap(o.Val)
		}
	}
	asInt32 := func(v int32) any { return v }
	switch t := tr.(type) {
	case iceberg.IdentityTransform:
		return func(v any) any { return v }, nil
	case iceberg.BucketTransform:
		return optional(t.Transformer(src), asInt32), nil
	case iceberg.TruncateTransform:
		fn, err := t.Transformer(src)
		if err != nil {
			return nil, err
		}
		return func(v any) any {
			if v == nil {
				return nil
			}
			return fn(v)
		}, nil
	case iceberg.DayTransform:
		fn, err := t.Transformer(src)
		if err != nil {
			return nil, err
		}
		return optional(fn, func(d int32) any { return iceberg.Date(d) }), nil
	case iceberg.YearTransform, iceberg.MonthTransform, iceberg.HourTransform:
		fn, err := t.(timeTransform).Transformer(src)
		if err != nil {
			return nil, err
		}
		return optional(fn, asInt32), nil
	}
	return nil, fmt.Errorf("transform %s is not supported for writing", tr)
}

// converter returns the reader of an Arrow column of type at as the Go type
// of the Iceberg type it, or nil when the pair has no conversion (spec
// decision 6).
func converter(at arrow.DataType, it iceberg.Type) func(arrow.Array, int) (any, error) {
	switch it.(type) {
	case iceberg.Int32Type:
		switch at.ID() {
		case arrow.INT8, arrow.INT16, arrow.INT32, arrow.UINT8, arrow.UINT16:
			get := intReader(at)
			return func(a arrow.Array, i int) (any, error) { return int32(get(a, i)), nil }
		}
	case iceberg.Int64Type:
		switch at.ID() {
		case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32:
			get := intReader(at)
			return func(a arrow.Array, i int) (any, error) { return get(a, i), nil }
		case arrow.UINT64:
			return func(a arrow.Array, i int) (any, error) {
				v := a.(*array.Uint64).Value(i)
				if v > math.MaxInt64 {
					return nil, fmt.Errorf("value %d does not fit the table's long column", v)
				}
				return int64(v), nil
			}
		}
	case iceberg.Float32Type:
		if at.ID() == arrow.FLOAT32 {
			return func(a arrow.Array, i int) (any, error) { return a.(*array.Float32).Value(i), nil }
		}
	case iceberg.Float64Type:
		switch at.ID() {
		case arrow.FLOAT32:
			return func(a arrow.Array, i int) (any, error) { return float64(a.(*array.Float32).Value(i)), nil }
		case arrow.FLOAT64:
			return func(a arrow.Array, i int) (any, error) { return a.(*array.Float64).Value(i), nil }
		}
	case iceberg.BooleanType:
		if at.ID() == arrow.BOOL {
			return func(a arrow.Array, i int) (any, error) { return a.(*array.Boolean).Value(i), nil }
		}
	case iceberg.StringType:
		if at.ID() == arrow.STRING {
			return func(a arrow.Array, i int) (any, error) { return a.(*array.String).Value(i), nil }
		}
	case iceberg.BinaryType, iceberg.FixedType:
		if at.ID() == arrow.BINARY {
			return func(a arrow.Array, i int) (any, error) { return a.(*array.Binary).Value(i), nil }
		}
	case iceberg.DateType:
		switch at.ID() {
		case arrow.DATE32:
			return func(a arrow.Array, i int) (any, error) { return iceberg.Date(a.(*array.Date32).Value(i)), nil }
		case arrow.DATE64:
			return func(a arrow.Array, i int) (any, error) {
				return iceberg.Date(floorDiv(int64(a.(*array.Date64).Value(i)), 86_400_000)), nil
			}
		}
	case iceberg.TimestampType, iceberg.TimestampTzType:
		if ts, ok := at.(*arrow.TimestampType); ok {
			var mul, div int64 = 1, 1
			switch ts.Unit {
			case arrow.Second:
				mul = 1_000_000
			case arrow.Millisecond:
				mul = 1_000
			case arrow.Nanosecond:
				div = 1_000
			}
			return func(a arrow.Array, i int) (any, error) {
				v := int64(a.(*array.Timestamp).Value(i))
				return iceberg.Timestamp(floorDiv(v*mul, div)), nil
			}
		}
	}
	return nil
}

// intReader reads row i of an integer column of type at as int64.
func intReader(at arrow.DataType) func(arrow.Array, int) int64 {
	switch at.ID() {
	case arrow.INT8:
		return func(a arrow.Array, i int) int64 { return int64(a.(*array.Int8).Value(i)) }
	case arrow.INT16:
		return func(a arrow.Array, i int) int64 { return int64(a.(*array.Int16).Value(i)) }
	case arrow.INT32:
		return func(a arrow.Array, i int) int64 { return int64(a.(*array.Int32).Value(i)) }
	case arrow.INT64:
		return func(a arrow.Array, i int) int64 { return a.(*array.Int64).Value(i) }
	case arrow.UINT8:
		return func(a arrow.Array, i int) int64 { return int64(a.(*array.Uint8).Value(i)) }
	case arrow.UINT16:
		return func(a arrow.Array, i int) int64 { return int64(a.(*array.Uint16).Value(i)) }
	case arrow.UINT32:
		return func(a arrow.Array, i int) int64 { return int64(a.(*array.Uint32).Value(i)) }
	}
	return nil
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// rowGroup is the rows of one batch that share a partition tuple.
type rowGroup struct {
	key   string
	tuple map[int]any // an entry for every spec field id; nil for a null
	rows  []int       // ascending row indices
}

// contiguous reports whether the group's rows are one run.
func (g *rowGroup) contiguous() bool { return g.rows[len(g.rows)-1]-g.rows[0]+1 == len(g.rows) }

// split groups the rows of rec by partition tuple, in first-seen order.
func (p *partitioner) split(rec arrow.RecordBatch) ([]*rowGroup, error) {
	cols := make([]arrow.Array, len(p.fields))
	for j, f := range p.fields {
		if f.col >= 0 {
			cols[j] = rec.Column(f.col)
		}
	}
	vals := make([]any, len(p.fields))
	byKey := map[string]*rowGroup{}
	var groups []*rowGroup
	var key []byte
	for i := 0; i < int(rec.NumRows()); i++ {
		key = key[:0]
		for j, f := range p.fields {
			var v any
			if f.col >= 0 && cols[j].IsValid(i) {
				sv, err := f.conv(cols[j], i)
				if err != nil {
					return nil, fmt.Errorf("goplane: partition field %s: %w", f.name, err)
				}
				v = f.apply(sv)
			}
			vals[j] = v
			key = appendKey(key, v)
		}
		g := byKey[string(key)]
		if g == nil {
			g = &rowGroup{key: string(key), tuple: make(map[int]any, len(p.fields))}
			for j, f := range p.fields {
				g.tuple[f.id] = cloneValue(vals[j])
			}
			byKey[g.key] = g
			groups = append(groups, g)
		}
		g.rows = append(g.rows, i)
	}
	return groups, nil
}

// cloneValue copies a string or []byte that may point into an Arrow buffer.
func cloneValue(v any) any {
	switch v := v.(type) {
	case string:
		return strings.Clone(v)
	case []byte:
		return bytes.Clone(v)
	}
	return v
}

// Key type tags (spec decision 7).
const (
	keyNull byte = iota
	keyInt32
	keyInt64
	keyFloat32
	keyFloat64
	keyBool
	keyString
	keyBytes
	keyDate
	keyTimestamp
	keyOther
)

// appendKey appends the canonical encoding of one tuple value: a type tag
// and its bytes; strings and []byte by content (length-prefixed), floats by
// IEEE bits with every NaN encoded alike, nil as its own tag.
func appendKey(b []byte, v any) []byte {
	switch v := v.(type) {
	case nil:
		return append(b, keyNull)
	case int32:
		return binary.LittleEndian.AppendUint32(append(b, keyInt32), uint32(v))
	case int64:
		return binary.LittleEndian.AppendUint64(append(b, keyInt64), uint64(v))
	case float32:
		bits := math.Float32bits(v)
		if v != v {
			bits = 0x7fc00000
		}
		return binary.LittleEndian.AppendUint32(append(b, keyFloat32), bits)
	case float64:
		bits := math.Float64bits(v)
		if v != v {
			bits = 0x7ff8000000000000
		}
		return binary.LittleEndian.AppendUint64(append(b, keyFloat64), bits)
	case bool:
		if v {
			return append(b, keyBool, 1)
		}
		return append(b, keyBool, 0)
	case string:
		return append(binary.AppendUvarint(append(b, keyString), uint64(len(v))), v...)
	case []byte:
		return append(binary.AppendUvarint(append(b, keyBytes), uint64(len(v))), v...)
	case iceberg.Date:
		return binary.LittleEndian.AppendUint32(append(b, keyDate), uint32(v))
	case iceberg.Timestamp:
		return binary.LittleEndian.AppendUint64(append(b, keyTimestamp), uint64(v))
	}
	s := fmt.Sprintf("%T:%v", v, v)
	return append(binary.AppendUvarint(append(b, keyOther), uint64(len(s))), s...)
}

// tupleRow presents a tuple in spec order to PartitionSpec.PartitionToPath.
type tupleRow []any

func (r tupleRow) Size() int          { return len(r) }
func (r tupleRow) Get(i int) any      { return r[i] }
func (r tupleRow) Set(i int, val any) { r[i] = val }

// path returns the partition directory of tuple, "name=value/..." without a
// trailing slash (iceberg's PartitionToPath).
func (p *partitioner) path(tuple map[int]any) string {
	row := make(tupleRow, len(p.fields))
	for j, f := range p.fields {
		row[j] = tuple[f.id]
	}
	return p.spec.PartitionToPath(row, p.schema)
}

// checkKeyLength fails a data file path whose object key exceeds the S3
// limit, naming the partition field with the longest path segment.
func checkKeyLength(path, partPath string) error {
	key := path
	if i := strings.Index(path, "://"); i >= 0 {
		rest := path[i+3:]
		if j := strings.IndexByte(rest, '/'); j >= 0 {
			key = rest[j+1:]
		}
	}
	if len(key) <= maxObjectKeyBytes {
		return nil
	}
	longest := ""
	for _, seg := range strings.Split(partPath, "/") {
		if len(seg) > len(longest) {
			longest = seg
		}
	}
	name, _, _ := strings.Cut(longest, "=")
	return fmt.Errorf("goplane: data file object key is %d bytes, over the %d-byte limit, because of partition field %s (%d-byte path segment)",
		len(key), maxObjectKeyBytes, name, len(longest))
}
