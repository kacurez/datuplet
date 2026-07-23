package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// positionalScanWindow bounds how many leading top-level objects are buffered
// (as decoded records) while looking for a positional nested record array (the
// World Bank [ {meta}, [records] ] shape). Real positional APIs put the array
// at index 0-1; past this window we commit to bare-array mode so memory stays
// bounded to this many records.
const positionalScanWindow = 8192

// fetchStream issues the GET and returns the response body for streaming.
// Caller must Close it.
func fetchStream(ctx context.Context, url string, headers map[string]string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP error: %s", resp.Status)
	}
	return resp.Body, nil
}

// decodeRecords stream-decodes the JSON document on r and invokes fn once per
// record object, in document order. It handles the three shapes parseJSON
// supports (bare array, positional [meta,[records]], object-wrapped), skips
// non-object array elements, and treats an empty array as zero records.
// Numbers are decoded with UseNumber so values keep their exact source text.
// Returns the number of records delivered to fn; fn errors propagate verbatim.
func decodeRecords(r io.Reader, arrayPath string, fn func(map[string]any) error) (int, error) {
	dec := json.NewDecoder(r)
	dec.UseNumber()

	tok, err := dec.Token()
	if err != nil {
		return 0, fmt.Errorf("failed to parse JSON: %w", err)
	}
	switch d := tok.(type) {
	case json.Delim:
		switch d {
		case '[':
			return decodeTopLevelArray(dec, fn)
		case '{':
			return decodeWrappedObject(dec, arrayPath, fn)
		}
	}
	return 0, fmt.Errorf("failed to parse JSON: unexpected top-level token %v", tok)
}

// decodeTopLevelArray handles bare arrays and the positional shape, fully
// token-streaming (no element is ever materialized as raw bytes). Leading
// object elements are buffered as decoded records (up to
// positionalScanWindow); if an array element appears first, the buffer was
// metadata and the nested array's objects stream straight to fn. Otherwise
// the buffered + remaining objects are the records. Scalar and (in bare mode)
// array elements are skipped, matching recordsFromSlice semantics.
func decodeTopLevelArray(dec *json.Decoder, fn func(map[string]any) error) (int, error) {
	count := 0
	deliver := func(rec map[string]any) error {
		if err := fn(rec); err != nil {
			return err
		}
		count++
		return nil
	}

	var pending []map[string]any
	committedBare := false
	flushPending := func() error {
		for _, p := range pending {
			if err := deliver(p); err != nil {
				return err
			}
		}
		pending = nil
		return nil
	}

	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return count, fmt.Errorf("failed to parse JSON: %w", err)
		}
		d, isDelim := tok.(json.Delim)
		switch {
		case isDelim && d == '{':
			rec, err := decodeObjectBody(dec)
			if err != nil {
				return count, err
			}
			if committedBare {
				if err := deliver(rec); err != nil {
					return count, err
				}
				continue
			}
			pending = append(pending, rec)
			if len(pending) >= positionalScanWindow {
				committedBare = true
				if err := flushPending(); err != nil {
					return count, err
				}
			}
		case isDelim && d == '[':
			if committedBare {
				// Non-object element in bare mode: skip it wholesale.
				if err := skipBalanced(dec); err != nil {
					return count, fmt.Errorf("failed to parse JSON: %w", err)
				}
				continue
			}
			// Positional shape: pending elements were metadata; the records
			// stream directly out of this nested array, one object at a
			// time. Elements after it are ignored (parseJSON's
			// first-array-element behavior).
			pending = nil
			for dec.More() {
				elTok, err := dec.Token()
				if err != nil {
					return count, fmt.Errorf("failed to parse JSON: %w", err)
				}
				ed, eIsDelim := elTok.(json.Delim)
				switch {
				case eIsDelim && ed == '{':
					rec, err := decodeObjectBody(dec)
					if err != nil {
						return count, err
					}
					if err := deliver(rec); err != nil {
						return count, err
					}
				case eIsDelim && ed == '[':
					if err := skipBalanced(dec); err != nil {
						return count, fmt.Errorf("failed to parse JSON: %w", err)
					}
				default:
					// scalar element: fully consumed by Token(); skip
				}
			}
			if _, err := dec.Token(); err != nil { // consume the inner ']'
				return count, fmt.Errorf("failed to parse JSON: %w", err)
			}
			// Validate the remainder (trailing elements, outer ']', EOF) so
			// a malformed document still fails, as parseJSON's whole-body
			// Unmarshal did.
			if err := drainToEOF(dec); err != nil {
				return count, err
			}
			return count, nil
		default:
			// scalar element: fully consumed by Token(); skip
		}
	}
	// Validate the remainder FIRST (closing ']', EOF): for bodies within the
	// scan window nothing has been delivered yet, so a malformed document
	// errors before any record reaches fn — exactly parseJSON's semantics.
	if err := drainToEOF(dec); err != nil {
		return count, err
	}
	// End of array with no nested record array: pending are the records.
	if err := flushPending(); err != nil {
		return count, err
	}
	return count, nil
}

// decodeObjectBody reads the remainder of a JSON object whose opening '{' has
// already been consumed, returning it as a record map. Values decode through
// the same decoder, so UseNumber applies to every nested numeric value.
func decodeObjectBody(dec *json.Decoder) (map[string]any, error) {
	rec := make(map[string]any)
	for dec.More() {
		kTok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("failed to parse JSON: %w", err)
		}
		k, ok := kTok.(string)
		if !ok {
			return nil, fmt.Errorf("failed to parse JSON: unexpected object key token %v", kTok)
		}
		var v any
		if err := dec.Decode(&v); err != nil {
			return nil, fmt.Errorf("failed to parse JSON: %w", err)
		}
		rec[k] = v
	}
	if _, err := dec.Token(); err != nil { // consume '}'
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}
	return rec, nil
}

// decodeWrappedObject handles { "key": [ ... ] }: with arrayPath it selects
// that key; otherwise the first array-valued key in document order. Skipped
// values are token-walked (O(1) memory), never buffered. Error-text parity
// with parseJSON: a set arrayPath that is missing OR holds a non-array both
// yield "field '<k>' is not an array"; the generic "no array found" message
// is auto-detect-only.
func decodeWrappedObject(dec *json.Decoder, arrayPath string, fn func(map[string]any) error) (int, error) {
	count := 0
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return count, fmt.Errorf("failed to parse JSON: %w", err)
		}
		key, _ := keyTok.(string)

		// Consume the value's first token to learn its kind.
		valTok, err := dec.Token()
		if err != nil {
			return count, fmt.Errorf("failed to parse JSON: %w", err)
		}
		delim, isDelim := valTok.(json.Delim)
		isTarget := arrayPath != "" && key == arrayPath

		if isDelim && delim == '[' && (isTarget || arrayPath == "") {
			// Found the record array (explicit key, or first array-valued
			// key in document order): stream its elements one at a time.
			for dec.More() {
				elTok, err := dec.Token()
				if err != nil {
					return count, fmt.Errorf("failed to parse JSON: %w", err)
				}
				ed, eIsDelim := elTok.(json.Delim)
				switch {
				case eIsDelim && ed == '{':
					rec, err := decodeObjectBody(dec)
					if err != nil {
						return count, err
					}
					if err := fn(rec); err != nil {
						return count, err
					}
					count++
				case eIsDelim && ed == '[':
					if err := skipBalanced(dec); err != nil {
						return count, fmt.Errorf("failed to parse JSON: %w", err)
					}
				default:
					// scalar element: fully consumed by Token(); skip
				}
			}
			if _, err := dec.Token(); err != nil { // consume ']'
				return count, fmt.Errorf("failed to parse JSON: %w", err)
			}
			// Validate the remainder (later wrapper fields, closing '}',
			// EOF) so a malformed document still fails, as parseJSON's
			// whole-body Unmarshal did.
			if err := drainToEOF(dec); err != nil {
				return count, err
			}
			return count, nil
		}

		if isTarget {
			// Explicit array_path key holds a non-array value.
			return count, fmt.Errorf("field '%s' is not an array", arrayPath)
		}

		// Not our value: token-skip its remainder (composites); scalars are
		// already fully consumed by Token().
		if isDelim && (delim == '{' || delim == '[') {
			if err := skipBalanced(dec); err != nil {
				return count, fmt.Errorf("failed to parse JSON: %w", err)
			}
		}
	}
	if arrayPath != "" {
		// Key absent entirely: same error text parseJSON produces today.
		return count, fmt.Errorf("field '%s' is not an array", arrayPath)
	}
	return count, fmt.Errorf("no array found in JSON response, specify array_path in config")
}

// skipBalanced consumes tokens until the object/array opened by the
// just-consumed delimiter is closed.
func skipBalanced(dec *json.Decoder) error {
	depth := 1
	for depth > 0 {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		if d, ok := tok.(json.Delim); ok {
			switch d {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
	}
	return nil
}

// drainToEOF walks the decoder's remaining tokens to the end of input,
// validating the rest of the document — closing delimiters, trailing wrapper
// fields, ignored trailing elements — in O(1) memory. parseJSON unmarshalled
// the WHOLE body and rejected malformed documents even when the record array
// itself was fine (e.g. a truncated `{"results":[...],`); this preserves that
// strictness on the streaming path. Note the inherent streaming caveat: for
// bodies larger than the scan window, records are delivered to fn before a
// corrupt tail is discovered — the run still fails, and nothing is committed
// (Commit is the barrier), so the end state matches the old behavior.
func drainToEOF(dec *json.Decoder) error {
	for {
		if _, err := dec.Token(); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("failed to parse JSON: %w", err)
		}
	}
}
