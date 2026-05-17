// Package partition provides partition-aware routing for DataGateway writes.
// It splits incoming Arrow records by partition values and routes each partition's
// rows to a dedicated BufferManager.
package partition

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/datuplet/datuplet/pkg/datagateway/buffer"
	"github.com/datuplet/datuplet/pkg/datagateway/manifest"
	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

const (
	// MaxPartitions is the hard cap on active partitions.
	MaxPartitions = 64
	// HiveDefaultPartition is the value used for null partition values.
	HiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__"
)

// FieldSpec describes a partition field.
type FieldSpec struct {
	SourceColumn string // Column name in source data
	Transform    string // identity, day, month, year, hour
	FieldName    string // Directory name (e.g., "country" or "event_time_day")
}

// partitionWriter holds state for a single partition.
type partitionWriter struct {
	key             string            // Partition key string (for map lookup)
	partitionValues map[string]string // field_name → canonical string value
	partitionPath   string            // "country=DE/event_time_day=2026-02-01"
	manager         *buffer.BufferManager
}

// Router routes Arrow records to per-partition BufferManagers.
type Router struct {
	fields        []FieldSpec
	schema        *schema.Schema
	arrowSchema   *arrow.Schema
	basePath      string
	bufferConfig  *buffer.BufferConfig
	allocator     memory.Allocator
	factory       buffer.WriterFactory
	writers       map[string]*partitionWriter
	maxPartitions int

	// Column indices for partition fields (resolved on first Add)
	colIndices []int
	resolved   bool
}

// NewRouter creates a new partition router.
func NewRouter(
	fields []FieldSpec,
	basePath string,
	bufferConfig *buffer.BufferConfig,
	allocator memory.Allocator,
	factory buffer.WriterFactory,
) *Router {
	return &Router{
		fields:        fields,
		basePath:      basePath,
		bufferConfig:  bufferConfig,
		allocator:     allocator,
		factory:       factory,
		writers:       make(map[string]*partitionWriter),
		maxPartitions: MaxPartitions,
	}
}

// resolveColumns resolves partition column names to Arrow schema indices.
// Also validates identity-transform column types (Phase 1 restriction).
func (r *Router) resolveColumns(arrowSchema *arrow.Schema, gatewaySchema *schema.Schema) error {
	r.arrowSchema = arrowSchema
	r.schema = gatewaySchema
	r.colIndices = make([]int, len(r.fields))

	for i, f := range r.fields {
		idx := -1
		for j, field := range arrowSchema.Fields() {
			if field.Name == f.SourceColumn {
				idx = j
				break
			}
		}
		if idx < 0 {
			return fmt.Errorf("partition source_column %q not found in schema", f.SourceColumn)
		}
		r.colIndices[i] = idx

		// Validate column type for identity transform
		col := gatewaySchema.ColumnByName(f.SourceColumn)
		if col == nil {
			return fmt.Errorf("partition source_column %q not found in gateway schema", f.SourceColumn)
		}
		if err := validateColumnType(f.Transform, col.Type); err != nil {
			return fmt.Errorf("partition field %q: %w", f.SourceColumn, err)
		}
	}

	r.resolved = true
	return nil
}

// validateColumnType validates that the column type is valid for the given transform.
func validateColumnType(transform string, dt schema.DataType) error {
	switch transform {
	case "identity":
		switch dt {
		case schema.TypeString, schema.TypeInt32, schema.TypeInt64, schema.TypeBool:
			return nil
		default:
			return fmt.Errorf("identity transform not supported for type %s (allowed: string, int32, int64, bool)", dt)
		}
	case "day", "month", "year", "hour":
		switch dt {
		case schema.TypeTimestamp, schema.TypeDate:
			return nil
		default:
			return fmt.Errorf("%s transform requires timestamp or date column (got %s)", transform, dt)
		}
	default:
		return fmt.Errorf("unsupported transform: %s", transform)
	}
}

// Add routes an Arrow record to the appropriate partition BufferManagers.
// On the first call, it resolves column indices and validates types.
func (r *Router) Add(record arrow.Record, gatewaySchema *schema.Schema) error {
	if !r.resolved {
		if err := r.resolveColumns(record.Schema(), gatewaySchema); err != nil {
			return err
		}
	}

	numRows := int(record.NumRows())
	numCols := int(record.NumCols())

	// For each row, compute partition key and collect row indices per partition
	type partitionBatch struct {
		key    string
		values map[string]string
		path   string
		rows   []int
	}
	partitions := make(map[string]*partitionBatch)

	for row := 0; row < numRows; row++ {
		values := make(map[string]string, len(r.fields))
		var keyParts []string

		for i, f := range r.fields {
			col := record.Column(r.colIndices[i])
			val := extractPartitionValue(col, row, f.Transform)
			values[f.FieldName] = val
			keyParts = append(keyParts, f.FieldName+"="+val)
		}

		key := strings.Join(keyParts, "/")
		pb, ok := partitions[key]
		if !ok {
			pb = &partitionBatch{
				key:    key,
				values: values,
				path:   key, // Same as key: field=val/field=val
				rows:   nil,
			}
			partitions[key] = pb
		}
		pb.rows = append(pb.rows, row)
	}

	// For each partition, build a sliced record and route to its BufferManager
	for _, pb := range partitions {
		// Build per-partition record using Arrow array slicing
		subRecord, err := buildSubRecord(record, pb.rows, numCols, r.allocator)
		if err != nil {
			return fmt.Errorf("failed to build partition record for %s: %w", pb.key, err)
		}

		pw, err := r.getOrCreateWriter(pb.key, pb.values, pb.path)
		if err != nil {
			subRecord.Release()
			return err
		}

		if err := pw.manager.Add(subRecord); err != nil {
			subRecord.Release()
			return fmt.Errorf("failed to add to partition %s: %w", pb.key, err)
		}
		subRecord.Release()
	}

	return nil
}

// buildSubRecord creates a new Arrow record containing only the specified rows.
// Uses per-column builders to construct new arrays (no Arrow Take()).
func buildSubRecord(record arrow.Record, rows []int, numCols int, alloc memory.Allocator) (arrow.Record, error) {
	cols := make([]arrow.Array, numCols)
	for c := 0; c < numCols; c++ {
		srcCol := record.Column(c)
		builder := array.NewBuilder(alloc, srcCol.DataType())
		for _, row := range rows {
			if srcCol.IsNull(row) {
				builder.AppendNull()
			} else {
				if err := appendValue(builder, srcCol, row); err != nil {
					builder.Release()
					// Release already-built columns
					for j := 0; j < c; j++ {
						cols[j].Release()
					}
					return nil, err
				}
			}
		}
		cols[c] = builder.NewArray()
		builder.Release()
	}

	rec := array.NewRecord(record.Schema(), cols, int64(len(rows)))
	// NewRecord retains each array; release our references to avoid leaking
	for _, col := range cols {
		col.Release()
	}
	return rec, nil
}

// appendValue appends a single value from src[row] to the builder.
func appendValue(builder array.Builder, src arrow.Array, row int) error {
	switch b := builder.(type) {
	case *array.Int8Builder:
		b.Append(src.(*array.Int8).Value(row))
	case *array.Int16Builder:
		b.Append(src.(*array.Int16).Value(row))
	case *array.Int32Builder:
		b.Append(src.(*array.Int32).Value(row))
	case *array.Int64Builder:
		b.Append(src.(*array.Int64).Value(row))
	case *array.Float32Builder:
		b.Append(src.(*array.Float32).Value(row))
	case *array.Float64Builder:
		b.Append(src.(*array.Float64).Value(row))
	case *array.StringBuilder:
		b.Append(src.(*array.String).Value(row))
	case *array.BooleanBuilder:
		b.Append(src.(*array.Boolean).Value(row))
	case *array.TimestampBuilder:
		b.Append(src.(*array.Timestamp).Value(row))
	case *array.Date32Builder:
		b.Append(src.(*array.Date32).Value(row))
	case *array.BinaryBuilder:
		b.Append(src.(*array.Binary).Value(row))
	default:
		return fmt.Errorf("unsupported arrow type for partitioning: %T", builder)
	}
	return nil
}

// extractPartitionValue extracts the canonical partition value string for a row.
func extractPartitionValue(col arrow.Array, row int, transform string) string {
	if col.IsNull(row) {
		return HiveDefaultPartition
	}

	switch transform {
	case "identity":
		return extractIdentityValue(col, row)
	case "day":
		return extractTimeValue(col, row, "2006-01-02")
	case "month":
		return extractTimeValue(col, row, "2006-01")
	case "year":
		return extractTimeValue(col, row, "2006")
	case "hour":
		return extractTimeValue(col, row, "2006-01-02-15")
	default:
		return HiveDefaultPartition
	}
}

// extractIdentityValue extracts a string representation for identity transform.
func extractIdentityValue(col arrow.Array, row int) string {
	switch c := col.(type) {
	case *array.String:
		return c.Value(row)
	case *array.Int32:
		return strconv.FormatInt(int64(c.Value(row)), 10)
	case *array.Int64:
		return strconv.FormatInt(c.Value(row), 10)
	case *array.Boolean:
		if c.Value(row) {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", col)
	}
}

// extractTimeValue extracts a formatted time string from a timestamp/date column.
func extractTimeValue(col arrow.Array, row int, layout string) string {
	switch c := col.(type) {
	case *array.Timestamp:
		// Arrow timestamps are in microseconds since epoch
		us := int64(c.Value(row))
		t := time.Unix(0, us*1000).UTC() // microseconds → nanoseconds
		return t.Format(layout)
	case *array.Date32:
		// Arrow Date32 is days since epoch
		days := int32(c.Value(row))
		t := time.Unix(int64(days)*86400, 0).UTC()
		return t.Format(layout)
	default:
		return HiveDefaultPartition
	}
}

// getOrCreateWriter returns the partition writer for the given key, creating if needed.
func (r *Router) getOrCreateWriter(key string, values map[string]string, partPath string) (*partitionWriter, error) {
	pw, ok := r.writers[key]
	if ok {
		return pw, nil
	}

	if len(r.writers) >= r.maxPartitions {
		return nil, fmt.Errorf("partition limit exceeded: more than %d active partitions", r.maxPartitions)
	}

	// Create BufferManager for this partition
	cfg := *r.bufferConfig // copy
	// Partition path: basePath + partitionPath/ (e.g., "s3://bucket/.../data/country=DE/")
	cfg.OutputDir = joinStoragePath(r.basePath, partPath)

	mgr, err := buffer.NewBufferManager(
		r.schema,
		&cfg,
		r.allocator,
		r.factory,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create buffer for partition %s: %w", key, err)
	}

	pw = &partitionWriter{
		key:             key,
		partitionValues: values,
		partitionPath:   partPath,
		manager:         mgr,
	}
	r.writers[key] = pw
	return pw, nil
}

// Close closes all partition writers and returns aggregate stats.
func (r *Router) Close() error {
	var firstErr error
	for _, pw := range r.writers {
		if err := pw.manager.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// FilesWritten returns all files written across all partitions,
// with partition values attached to each file entry.
func (r *Router) FilesWritten() []FileWithPartition {
	var result []FileWithPartition
	for _, pw := range r.writers {
		for _, f := range pw.manager.FilesWritten() {
			result = append(result, FileWithPartition{
				FileInfo:        f,
				PartitionValues: pw.partitionValues,
			})
		}
	}
	return result
}

// FileWithPartition extends FileInfo with partition values.
type FileWithPartition struct {
	buffer.FileInfo
	PartitionValues map[string]string // field_name → canonical value
}

// Stats returns aggregate statistics across all partitions.
func (r *Router) Stats() buffer.BufferStats {
	var total buffer.BufferStats
	for _, pw := range r.writers {
		s := pw.manager.Stats()
		total.TotalFiles += s.TotalFiles
		total.TotalRowsFlushed += s.TotalRowsFlushed
		total.TotalBytesFlushed += s.TotalBytesFlushed
	}
	return total
}

// PartitionFieldDefs returns the manifest-ready partition field definitions.
func (r *Router) PartitionFieldDefs() []manifest.PartitionFieldDef {
	defs := make([]manifest.PartitionFieldDef, len(r.fields))
	for i, f := range r.fields {
		defs[i] = manifest.PartitionFieldDef{
			SourceColumn: f.SourceColumn,
			Transform:    f.Transform,
			FieldName:    f.FieldName,
		}
	}
	return defs
}


// joinStoragePath joins a base path with a filename in a URL-safe way.
func joinStoragePath(basePath, filename string) string {
	if !strings.HasSuffix(basePath, "/") {
		basePath += "/"
	}
	return basePath + filename
}
