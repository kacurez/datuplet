package format

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
)

// IsCanonicalVariant reports whether dt is exactly the canonical variant
// layout, extensions.NewDefaultVariantType().
func IsCanonicalVariant(dt arrow.DataType) bool {
	return arrow.TypeEqual(dt, extensions.NewDefaultVariantType())
}

// CanConvertToVariant reports whether ToCanonicalVariant accepts a column of
// type dt.
func CanConvertToVariant(dt arrow.DataType) bool {
	if _, ok := dt.(*extensions.VariantType); ok {
		return true
	}
	switch dt.ID() {
	case arrow.STRING, arrow.LARGE_STRING:
		return true
	}
	return false
}

// ToCanonicalVariant converts arr into the canonical variant layout:
//   - the canonical layout is retained as is;
//   - any other variant layout (shredded, large-binary or dictionary
//     storage) is rebuilt value by value; physically null rows stay null;
//   - text (utf8, large_utf8) is parsed as JSON text; JSON null is SQL NULL,
//     and invalid JSON is an error naming the row.
//
// The caller releases the result.
func ToCanonicalVariant(arr arrow.Array, mem memory.Allocator) (arrow.Array, error) {
	if va, ok := arr.(*extensions.VariantArray); ok && IsCanonicalVariant(va.DataType()) {
		arr.Retain()
		return arr, nil
	}
	b := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer b.Release()
	b.Reserve(arr.Len())
	switch a := arr.(type) {
	case *extensions.VariantArray:
		storage := a.Storage()
		for i := 0; i < a.Len(); i++ {
			if storage.IsNull(i) {
				b.AppendNull()
				continue
			}
			v, err := a.Value(i)
			if err != nil {
				return nil, fmt.Errorf("row %d: %w", i, err)
			}
			b.Append(v)
		}
	case array.StringLike:
		for i := 0; i < a.Len(); i++ {
			if a.IsNull(i) {
				b.AppendNull()
				continue
			}
			if err := appendVariantJSONText(b, []byte(a.Value(i))); err != nil {
				return nil, fmt.Errorf("row %d: %w", i, err)
			}
		}
	default:
		return nil, fmt.Errorf("cannot convert %s to variant", arr.DataType())
	}
	return b.NewArray(), nil
}

// appendVariantJSONText parses text as exactly one JSON value (numbers kept
// exact) and appends it; JSON null appends SQL NULL.
func appendVariantJSONText(b *extensions.VariantBuilder, text []byte) error {
	var raw json.RawMessage
	if err := json.Unmarshal(text, &raw); err != nil {
		return fmt.Errorf("invalid JSON for variant: %w", err)
	}
	if err := checkVariantNumbers(raw); err != nil {
		return err
	}
	v, err := parseVariantJSON(raw)
	if err != nil {
		return fmt.Errorf("invalid JSON for variant: %w", err)
	}
	if v.Type() == variant.Null {
		b.AppendNull()
		return nil
	}
	b.Append(v)
	return nil
}

// maxVariantDecimalDigits is the precision of the variant decimal16 type.
const maxVariantDecimalDigits = 38

// checkVariantNumbers rejects an integer or decimal literal anywhere in the
// JSON value raw whose significant digits (leading zeros not counted) or
// fractional digits exceed 38: arrow-go v18.8.0's ParseJSONBytes panics on
// some of them (decimal128 FitsInPrecision) and turns others into doubles.
// Exponent literals are left to ParseJSONBytes (they become doubles).
func checkVariantNumbers(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("invalid JSON for variant: %w", err)
		}
		n, ok := tok.(json.Number)
		if !ok || strings.ContainsAny(string(n), "eE") {
			continue
		}
		intPart, frac, _ := strings.Cut(strings.TrimPrefix(string(n), "-"), ".")
		digits := strings.TrimLeft(intPart+frac, "0")
		if len(digits) > maxVariantDecimalDigits || len(frac) > maxVariantDecimalDigits {
			return fmt.Errorf("invalid JSON for variant: number %s exceeds %d digits of precision", n, maxVariantDecimalDigits)
		}
	}
}

// parseVariantJSON is variant.ParseJSONBytes with a panic turned into an
// error, so no input can crash the gateway.
func parseVariantJSON(raw []byte) (v variant.Value, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("variant parse failed: %v", r)
		}
	}()
	return variant.ParseJSONBytes(raw, false)
}

// appendVariantFromInterface appends a JSON-native value (decoded with
// json.Decoder.UseNumber) as is: strings stay variant strings.
func appendVariantFromInterface(b *extensions.VariantBuilder, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("cannot JSON-encode %T for variant column: %w", value, err)
	}
	return appendVariantJSONText(b, data)
}

// VariantJSON renders row i of a variant array as JSON text; the value's
// numbers are written exactly. A SQL NULL row is the caller's to detect
// (arr.Storage().IsNull(i)); a variant JSON null renders as null.
func VariantJSON(arr *extensions.VariantArray, i int) ([]byte, error) {
	if arr.IsNull(i) {
		return []byte("null"), nil
	}
	v, err := arr.Value(i)
	if err != nil {
		return nil, fmt.Errorf("row %d: %w", i, err)
	}
	var buf bytes.Buffer
	if err := writeVariantJSON(&buf, v); err != nil {
		return nil, fmt.Errorf("row %d: %w", i, err)
	}
	return buf.Bytes(), nil
}

// writeVariantJSON walks v by type. It does not use variant.Value.MarshalJSON
// for containers: arrow-go renders an empty array as null. Strings are not
// HTML-escaped. Primitives other than strings, and the zone-less temporal
// types, delegate to arrow-go's rendering (exact ints and decimals, base64
// binary, UUID text, UTC timestamps).
func writeVariantJSON(buf *bytes.Buffer, v variant.Value) error {
	switch v.Type() {
	case variant.Object:
		obj := v.Value().(variant.ObjectValue)
		buf.WriteByte('{')
		for i := uint32(0); i < obj.NumElements(); i++ {
			f, err := obj.FieldAt(i)
			if err != nil {
				return err
			}
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeJSONString(buf, f.Key); err != nil {
				return err
			}
			buf.WriteByte(':')
			if err := writeVariantJSON(buf, f.Value); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
		return nil
	case variant.Array:
		arr := v.Value().(variant.ArrayValue)
		buf.WriteByte('[')
		for i := uint32(0); i < arr.Len(); i++ {
			el, err := arr.Value(i)
			if err != nil {
				return err
			}
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeVariantJSON(buf, el); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
		return nil
	case variant.String:
		return writeJSONString(buf, v.Value().(string))
	case variant.TimestampMicrosNTZ:
		return writeJSONString(buf, v.Value().(arrow.Timestamp).ToTime(arrow.Microsecond).Format("2006-01-02 15:04:05.999999"))
	case variant.TimestampNanosNTZ:
		return writeJSONString(buf, v.Value().(arrow.Timestamp).ToTime(arrow.Nanosecond).Format("2006-01-02 15:04:05.999999999"))
	case variant.Time:
		return writeJSONString(buf, v.Value().(arrow.Time64).ToTime(arrow.Microsecond).Format("15:04:05.999999"))
	default:
		b, err := v.MarshalJSON()
		if err != nil {
			return err
		}
		buf.Write(b)
		return nil
	}
}

func writeJSONString(buf *bytes.Buffer, s string) error {
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		return err
	}
	buf.Truncate(buf.Len() - 1) // Encode appends a newline
	return nil
}

// IsSQLNullVariant reports whether row i of arr is SQL NULL, as opposed to a
// variant JSON null.
func IsSQLNullVariant(arr *extensions.VariantArray, i int) bool {
	return arr.Storage().IsNull(i)
}
