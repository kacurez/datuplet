package format

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// String to typed value conversion functions for Arrow builders.
// These are shared between CSV and JSON adapters.

func appendInt64FromString(b *array.Int64Builder, value string) error {
	i, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("cannot convert %q to int64: %w", value, err)
	}
	b.Append(i)
	return nil
}

func appendInt32FromString(b *array.Int32Builder, value string) error {
	i, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return fmt.Errorf("cannot convert %q to int32: %w", value, err)
	}
	b.Append(int32(i))
	return nil
}

func appendFloat64FromString(b *array.Float64Builder, value string) error {
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fmt.Errorf("cannot convert %q to float64: %w", value, err)
	}
	b.Append(f)
	return nil
}

func appendFloat32FromString(b *array.Float32Builder, value string) error {
	f, err := strconv.ParseFloat(value, 32)
	if err != nil {
		return fmt.Errorf("cannot convert %q to float32: %w", value, err)
	}
	b.Append(float32(f))
	return nil
}

func appendBoolFromString(b *array.BooleanBuilder, value string) error {
	lower := strings.ToLower(strings.TrimSpace(value))
	switch lower {
	case "true", "1", "yes", "y":
		b.Append(true)
	case "false", "0", "no", "n":
		b.Append(false)
	default:
		return fmt.Errorf("cannot convert %q to bool", value)
	}
	return nil
}

// Common timestamp formats to try.
var timestampFormats = []string{
	time.RFC3339,
	time.RFC3339Nano,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05.000",
	"2006-01-02 15:04:05.000",
	"2006-01-02T15:04:05.000000",
	"2006-01-02 15:04:05.000000",
	"01/02/2006 15:04:05",
	"02/01/2006 15:04:05",
}

func appendTimestampFromString(b *array.TimestampBuilder, value string) error {
	value = strings.TrimSpace(value)
	for _, format := range timestampFormats {
		if t, err := time.Parse(format, value); err == nil {
			b.Append(arrow.Timestamp(t.UnixMicro()))
			return nil
		}
	}
	return fmt.Errorf("cannot parse timestamp %q", value)
}

// Common date formats to try.
var dateFormats = []string{
	"2006-01-02",
	"01/02/2006",
	"02/01/2006",
	"2006/01/02",
	"2006-1-2",
	"1/2/2006",
}

func appendDateFromString(b *array.Date32Builder, value string) error {
	value = strings.TrimSpace(value)
	for _, format := range dateFormats {
		if t, err := time.Parse(format, value); err == nil {
			days := int32(t.Unix() / 86400)
			b.Append(arrow.Date32(days))
			return nil
		}
	}
	return fmt.Errorf("cannot parse date %q", value)
}

// appendValueFromInterface appends a Go interface{} value to an Arrow builder.
// Used by JSON adapter where values are already typed from JSON parsing.
func appendValueFromInterface(builder array.Builder, value any) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	case *array.Int64Builder:
		return appendInt64FromInterface(b, value)
	case *array.Int32Builder:
		return appendInt32FromInterface(b, value)
	case *array.Float64Builder:
		return appendFloat64FromInterface(b, value)
	case *array.Float32Builder:
		return appendFloat32FromInterface(b, value)
	case *array.StringBuilder:
		return appendStringFromInterface(b, value)
	case *array.BooleanBuilder:
		return appendBoolFromInterface(b, value)
	case *array.TimestampBuilder:
		return appendTimestampFromInterface(b, value)
	case *array.Date32Builder:
		return appendDateFromInterface(b, value)
	case *array.BinaryBuilder:
		return appendBinaryFromInterface(b, value)
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}
}

func appendInt64FromInterface(b *array.Int64Builder, value any) error {
	switch v := value.(type) {
	case int64:
		b.Append(v)
	case int:
		b.Append(int64(v))
	case int32:
		b.Append(int64(v))
	case float64:
		b.Append(int64(v))
	case float32:
		b.Append(int64(v))
	case string:
		return appendInt64FromString(b, v)
	default:
		return fmt.Errorf("cannot convert %T to int64", value)
	}
	return nil
}

func appendInt32FromInterface(b *array.Int32Builder, value any) error {
	switch v := value.(type) {
	case int32:
		b.Append(v)
	case int:
		b.Append(int32(v))
	case int64:
		b.Append(int32(v))
	case float64:
		b.Append(int32(v))
	case float32:
		b.Append(int32(v))
	case string:
		return appendInt32FromString(b, v)
	default:
		return fmt.Errorf("cannot convert %T to int32", value)
	}
	return nil
}

func appendFloat64FromInterface(b *array.Float64Builder, value any) error {
	switch v := value.(type) {
	case float64:
		b.Append(v)
	case float32:
		b.Append(float64(v))
	case int64:
		b.Append(float64(v))
	case int:
		b.Append(float64(v))
	case int32:
		b.Append(float64(v))
	case string:
		return appendFloat64FromString(b, v)
	default:
		return fmt.Errorf("cannot convert %T to float64", value)
	}
	return nil
}

func appendFloat32FromInterface(b *array.Float32Builder, value any) error {
	switch v := value.(type) {
	case float32:
		b.Append(v)
	case float64:
		b.Append(float32(v))
	case int:
		b.Append(float32(v))
	case int32:
		b.Append(float32(v))
	case int64:
		b.Append(float32(v))
	case string:
		return appendFloat32FromString(b, v)
	default:
		return fmt.Errorf("cannot convert %T to float32", value)
	}
	return nil
}

func appendStringFromInterface(b *array.StringBuilder, value any) error {
	switch v := value.(type) {
	case string:
		b.Append(v)
	case []byte:
		b.Append(string(v))
	case map[string]any, []any, []map[string]any:
		// The gateway schema has no native struct/list type, so nested JSON
		// values are preserved as re-parseable JSON text rather than Go's
		// "map[k:v]" / "[a b]" fmt representation.
		data, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("cannot JSON-encode %T for string column: %w", value, err)
		}
		b.Append(string(data))
	default:
		b.Append(fmt.Sprintf("%v", value))
	}
	return nil
}

func appendBoolFromInterface(b *array.BooleanBuilder, value any) error {
	switch v := value.(type) {
	case bool:
		b.Append(v)
	case string:
		return appendBoolFromString(b, v)
	case int:
		b.Append(v != 0)
	case int64:
		b.Append(v != 0)
	case float64:
		b.Append(v != 0)
	default:
		return fmt.Errorf("cannot convert %T to bool", value)
	}
	return nil
}

func appendTimestampFromInterface(b *array.TimestampBuilder, value any) error {
	switch v := value.(type) {
	case time.Time:
		b.Append(arrow.Timestamp(v.UnixMicro()))
	case string:
		return appendTimestampFromString(b, v)
	case int64:
		// Assume microseconds since epoch
		b.Append(arrow.Timestamp(v))
	case float64:
		// Assume seconds since epoch
		b.Append(arrow.Timestamp(int64(v * 1e6)))
	default:
		return fmt.Errorf("cannot convert %T to timestamp", value)
	}
	return nil
}

func appendDateFromInterface(b *array.Date32Builder, value any) error {
	switch v := value.(type) {
	case time.Time:
		days := int32(v.Unix() / 86400)
		b.Append(arrow.Date32(days))
	case string:
		return appendDateFromString(b, v)
	case int32:
		// Assume days since epoch
		b.Append(arrow.Date32(v))
	case int:
		b.Append(arrow.Date32(v))
	case int64:
		b.Append(arrow.Date32(v))
	default:
		return fmt.Errorf("cannot convert %T to date", value)
	}
	return nil
}

func appendBinaryFromInterface(b *array.BinaryBuilder, value any) error {
	switch v := value.(type) {
	case []byte:
		b.Append(v)
	case string:
		b.Append([]byte(v))
	default:
		return fmt.Errorf("cannot convert %T to binary", value)
	}
	return nil
}
