package storage

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// PreviewResponse is the JSON body returned by the preview handler.
// One entry per column in Columns; one slice per row in Rows. If the
// serializer had to drop trailing columns or rows to fit under the
// byte cap, Truncated is true (rows stay complete but have fewer
// cells when columns were dropped).
type PreviewResponse struct {
	Columns   []ColumnInfo `json:"columns"`
	Rows      [][]any      `json:"rows"`
	Truncated bool         `json:"truncated"`
}

// ColumnInfo describes one column in a PreviewResponse. Type is the
// Arrow type string (e.g. "int64", "utf8", "timestamp[us, tz=UTC]").
type ColumnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// EncodeRecords consumes a stream of arrow.RecordBatch values (the
// shape iceberg-go's (*Scan).ToArrowRecords returns) up to maxRows
// rows and a maxBytes cap on the final JSON body size.
//
// Behaviour:
//   - At most maxRows rows are emitted; excess is discarded.
//   - After each row is appended, we estimate body size. When adding
//     the next row would exceed maxBytes, stop and mark
//     Truncated=true.
//   - If the column header alone, or the first row on top of it,
//     already exceeds maxBytes, drop trailing columns until it fits
//     (keeping at least one column). Truncated=true in that case.
//
// next returns (nil, nil) to signal end-of-stream. Any returned batch
// must be Retain()'d by the caller; EncodeRecords will Release() it
// after consuming.
func EncodeRecords(schema *arrow.Schema, next func() (arrow.RecordBatch, error), maxRows, maxBytes int) (PreviewResponse, error) {
	if schema == nil {
		return PreviewResponse{}, fmt.Errorf("encode records: schema is nil")
	}
	if maxRows < 0 {
		maxRows = 0
	}
	if maxBytes <= 0 {
		maxBytes = 1 << 20 // 1 MiB default sanity floor.
	}

	resp := PreviewResponse{
		Columns: make([]ColumnInfo, schema.NumFields()),
		Rows:    [][]any{},
	}
	for i, f := range schema.Fields() {
		resp.Columns[i] = ColumnInfo{Name: f.Name, Type: f.Type.String()}
	}

	// Precompute skeleton bytes. If it already exceeds the cap, drop
	// trailing columns until it fits (keep >=1 column).
	skeletonBytes, err := skeletonSize(resp.Columns)
	if err != nil {
		return resp, err
	}
	for skeletonBytes > maxBytes && len(resp.Columns) > 1 {
		resp.Columns = resp.Columns[:len(resp.Columns)-1]
		resp.Truncated = true
		if skeletonBytes, err = skeletonSize(resp.Columns); err != nil {
			return resp, err
		}
	}
	estBytes := skeletonBytes

	if maxRows == 0 {
		return resp, nil
	}

	appended := 0
	stop := false
	for !stop {
		batch, err := next()
		if err != nil {
			return resp, fmt.Errorf("encode records: iterator: %w", err)
		}
		if batch == nil {
			break
		}

		rows := int(batch.NumRows())
		for i := 0; i < rows; i++ {
			if appended >= maxRows {
				resp.Truncated = true
				stop = true
				break
			}
			row := extractRow(batch, i)
			// If we've already trimmed the column set, trim this row.
			if len(row) > len(resp.Columns) {
				row = row[:len(resp.Columns)]
			}
			rowJSON, err := json.Marshal(row)
			if err != nil {
				batch.Release()
				return resp, fmt.Errorf("encode records: marshal row: %w", err)
			}
			// +1 for the comma between rows. First row slips in for
			// free because of the skeleton's empty "[]".
			add := len(rowJSON)
			if appended > 0 {
				add++
			}
			if estBytes+add > maxBytes {
				// Row pushes us over the cap. For the very first row,
				// attempt a column-drop shrink to fit. Otherwise, stop
				// here and mark truncated.
				resp.Truncated = true
				if appended == 0 {
					trimmedRow, trimmedCols, total, ok := shrinkFirstRow(row, resp.Columns, maxBytes)
					if ok {
						resp.Columns = trimmedCols
						resp.Rows = append(resp.Rows, trimmedRow)
						estBytes = total
						appended++
						// Keep iterating with the narrower column set —
						// more rows may still fit within maxBytes.
						continue
					}
				}
				stop = true
				break
			}
			resp.Rows = append(resp.Rows, row)
			estBytes += add
			appended++
		}
		batch.Release()
	}

	// Peek one more batch. Two responsibilities:
	//   1. If we stopped due to caps but data is still in the stream,
	//      flip Truncated=true.
	//   2. Surface any late-stage iterator error instead of silently
	//      masking it as Truncated=true / "complete".
	rest, err := next()
	if err != nil {
		return resp, fmt.Errorf("encode records: iterator: %w", err)
	}
	if rest != nil {
		resp.Truncated = true
		rest.Release()
	}

	return resp, nil
}

// extractRow pulls one JSON-friendly row out of a batch at index i.
// arrow-go's Array.GetOneForMarshal already handles nulls (returns
// nil), returns native Go numeric/bool/string for primitives, []byte
// for binary (which json.Marshal encodes as base64), formatted
// strings for timestamp/date/time, and recursively-constructed
// map/slice values for nested types.
func extractRow(batch arrow.RecordBatch, i int) []any {
	cols := batch.Columns()
	row := make([]any, len(cols))
	for c, col := range cols {
		row[c] = col.GetOneForMarshal(i)
	}
	return row
}

// skeletonSize marshals an empty PreviewResponse with the given
// column list and returns its byte length.
func skeletonSize(columns []ColumnInfo) (int, error) {
	b, err := json.Marshal(PreviewResponse{Columns: columns, Rows: [][]any{}})
	if err != nil {
		return 0, fmt.Errorf("encode records: marshal skeleton: %w", err)
	}
	return len(b), nil
}

// shrinkFirstRow drops trailing columns from both row and columns
// until the overall response (skeleton + single row) fits into
// maxBytes. Keeps at least one column. Returns the trimmed row +
// columns, the total body size after the shrink, and ok=true if a
// fit was achieved.
func shrinkFirstRow(row []any, columns []ColumnInfo, maxBytes int) ([]any, []ColumnInfo, int, bool) {
	for k := len(columns); k >= 1; k-- {
		trimmedCols := columns[:k]
		trimmedRow := row
		if len(trimmedRow) > k {
			trimmedRow = trimmedRow[:k]
		}
		skelBytes, err := skeletonSize(trimmedCols)
		if err != nil {
			continue
		}
		rowJSON, err := json.Marshal(trimmedRow)
		if err != nil {
			continue
		}
		// First row — skeleton has "[]" with room for one row, so no
		// separator comma. Total body = skeleton bytes + row JSON.
		total := skelBytes + len(rowJSON)
		if total <= maxBytes {
			return trimmedRow, trimmedCols, total, true
		}
	}
	return row, columns, 0, false
}
