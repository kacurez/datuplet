# http-json-extractor Streaming Arrow-IPC + Mock-DG Local Rig Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite `http-json-extractor`'s write path to stream-decode JSON and emit all-String Arrow-IPC batches (bounding component memory and removing the gateway's JSON-parse load), and add a standalone mock Data Gateway so the real extractor binary can be run and profiled locally.

**Architecture:** The component gains two new focused files: `stream.go` (token-streaming JSON decode of the three supported response shapes, one record at a time) and `arrow_sink.go` (all-String Arrow `RecordBuilder` that flushes a self-contained IPC stream per 8192 rows via one `writer.Write` call each — the SDK force-disables batching for `FORMAT_ARROW_IPC`, so each `Write` is one POST). `main.go` wires both paths (single-request + paginated) through the sink and drops the now-dead JSONL/projection code. A new root-module binary `cmd/mock-datagateway/` implements the handful of DataGateway RPCs + the HTTP write endpoint the SDK uses, validating and counting the received Arrow batches.

**Tech Stack:** Go 1.25, `github.com/apache/arrow-go/v18 v18.6.0` (already used by root + data-generator), DataGateway Go SDK (`sdk/go`), gRPC (`pkg/datagateway/proto/v2` generated stubs).

**Spec:** `docs/superpowers/specs/2026-07-23-http-json-extractor-arrow-streaming-design.md`

## Global Constraints

- Component module is its own Go module: run Go commands with `-C components/http-json-extractor`. The mock lives in the ROOT module (`cmd/mock-datagateway/`), covered by plain `go build ./...` at repo root.
- **One IPC stream per `writer.Write` call — never pre-concatenate batches.** The gateway reads exactly one IPC stream per POST (`pkg/datagateway/format/arrow_ipc.go:74`); the SDK guarantees one POST per `Write` for Arrow because `OpenWriter` force-disables batching for `FORMAT_ARROW_IPC` (`sdk/go/client.go:378-380`, immediate send at `:576-579`).
- Batch size: `const arrowBatchRows = 8192` (matches data-generator).
- All emitted columns are `arrow.BinaryTypes.String`, `Nullable: true`.
- The streaming decoder MUST use `dec.UseNumber()` so numeric JSON arrives as `json.Number` and stringifies to its exact source text (no float64 round-trip like `5.9e+09`).
- Preserve current decode semantics: three shapes (bare array / positional `[meta,[records]]` / object-wrapped), non-object array elements skipped, empty array = 0 records (not an error), `array_path` selects the wrapper key, missing-array error text `"no array found in JSON response, specify array_path in config"`, non-array key error text `"field '%s' is not an array"`.
- No-`fields` column set = **sorted union of top-level keys across the first batch** (up to 8192 records), fixed for the run. `fields` set = declared order, values via `getValueRaw` dot-paths.
- **Never delete** `fetchJSON`, `parseJSON`, `recordsFromSlice`, `getValueRaw`, `getColumns`, `collectColumns`, `inferJSONSchema`, `inferColumnTypeFromJSON`, `ColumnSchema`, `SampleOutput`, `runSampleMode` — sample mode (`DATUPLET_MODE=sample`) still uses them.
- **Delete after rewiring (Task 4):** `encodeJSONL`, `writeJSONL`, `projectRecords` and their tests — dead once both paths write Arrow.
- Exit-code contract preserved: config/fetch/decode errors → `ExitUserError` (1); writer/commit/Arrow-build errors → `ExitAppError` (>=20); paginated errors bubble to `main` which calls `ExitUserError` (pre-existing behavior).
- Do NOT touch the gateway, the SDK, other components, CRDs, or controllers.
- `make tidy` (never bare `go mod tidy`) after go.mod changes; version bump `0.12.0` via `make bump-version` (behavior change: all-String output).
- Never push `main`, never tag; feature branch `feat/http-json-extractor-arrow-streaming` + draft PR.

## Execution Protocol (models + review gates)

Execute via superpowers:subagent-driven-development. **Every review is a
Codex review** (maintainer's standing rule) — no Claude reviewer subagents.
Implementer model per dispatch (an omitted model silently inherits the most
expensive one — always pass it explicitly):

| Task | Implementer | Task reviewer | Rationale |
|---|---|---|---|
| 1 `stream.go` | haiku | **Codex** | complete code in plan → transcription + run tests |
| 2 stringify + plan | haiku | **Codex** | complete code, small, pure helpers |
| 3 `arrowSink` | haiku | **Codex** | batching/lazy-open is load-bearing |
| 4 wire `main()` | **sonnet** | **Codex** | multi-block surgery, line-drift risk, behavior parity |
| 5 mock-DG + Makefile | **sonnet** | **Codex** | new binary, Makefile, live smoke may need debugging |
| 6 docs | sonnet | **Codex** | prose accuracy vs code behavior |
| 7 proof + bump + PR | controller (no subagent) | — (final gate below) | explicit staging rules (never `git add -A`), push/PR mechanics |
| 8 post-release 5M proof | controller (operator loop) | — | cluster workflow, not a code task |

**How Codex reviews are dispatched:** directly in-session via the
codex-companion `task` command (`--background` + a log-mtime stall watcher;
cancel + re-dispatch on stall) — NOT through the `codex:codex-rescue`
subagent. Each per-task review gets the task's brief, the implementer's
report, and the task diff (`scripts/review-package BASE HEAD` file), and
returns findings by severity plus a spec-compliance + quality verdict.

**Finding protocol (maintainer's standing rule):** on any Critical/Important
finding, STOP and present it to the maintainer — no fix is dispatched without
explicit approval; after an approved fix, Codex re-reviews the task. A clean
review needs no check-in — proceed straight to the next task. Minor findings
are recorded in the ledger and rolled up to the final gate.

**Final gate — Codex whole-branch review (MANDATORY, blocks push/PR):** after
Task 7's bump commit and BEFORE `git push`/`gh pr create`, run ONE Codex
review of the whole branch diff (same dispatch mechanics). On findings: STOP
and present with severities; push and open the draft PR only after this gate
(plus any approved fixes and their re-review) passes.

---

### Task 1: Streaming JSON decoder (`stream.go`)

**Files:**
- Create: `components/http-json-extractor/stream.go`
- Test: `components/http-json-extractor/stream_test.go` (create)

**Interfaces:**
- Consumes: nothing new (stdlib only).
- Produces (used by Tasks 3–4):
  - `func decodeRecords(r io.Reader, arrayPath string, fn func(map[string]any) error) (int, error)` — streams record objects to `fn` in document order; returns the count of records delivered (objects only). Errors from `fn` propagate verbatim.
  - `func fetchStream(ctx context.Context, url string, headers map[string]string) (io.ReadCloser, error)` — HTTP GET returning the response body for streaming; non-200 → error `"HTTP error: <status>"`.
  - `const positionalScanWindow = 8192` — array-element detection bound (documented divergence: a positional nested array is only detected within the first 8192 top-level elements; real positional APIs put it at index 0–1).

- [ ] **Step 1: Write the failing tests**

```go
package main

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func collect(t *testing.T, body, arrayPath string) ([]map[string]any, int, error) {
	t.Helper()
	var got []map[string]any
	n, err := decodeRecords(strings.NewReader(body), arrayPath, func(rec map[string]any) error {
		got = append(got, rec)
		return nil
	})
	return got, n, err
}

func TestDecodeRecords_Shapes(t *testing.T) {
	cases := []struct {
		name      string
		body      string
		arrayPath string
		wantLen   int
		wantKey   string
	}{
		{"bare_array", `[{"id":1},{"id":2}]`, "", 2, "id"},
		{"worldbank_positional", `[{"page":1,"pages":53},[{"countryiso3code":"SVK","value":5},{"countryiso3code":"CZE","value":10}]]`, "", 2, "countryiso3code"},
		{"object_arraypath", `{"offset":0,"results":[{"key":1},{"key":2},{"key":3}]}`, "results", 3, "key"},
		{"object_autodetect", `{"offset":0,"results":[{"key":1},{"key":2}]}`, "", 2, "key"},
		{"empty_bare_array", `[]`, "", 0, ""},
		{"empty_wrapped_array", `{"results":[]}`, "results", 0, ""},
		{"skips_non_objects", `[{"id":1},"noise",42,{"id":2}]`, "", 2, "id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, n, err := collect(t, tc.body, tc.arrayPath)
			if err != nil {
				t.Fatalf("decodeRecords error: %v", err)
			}
			if n != tc.wantLen || len(got) != tc.wantLen {
				t.Fatalf("got n=%d len=%d, want %d", n, len(got), tc.wantLen)
			}
			if tc.wantLen > 0 {
				if _, ok := got[0][tc.wantKey]; !ok {
					t.Fatalf("first record missing key %q: %v", tc.wantKey, got[0])
				}
			}
		})
	}
}

func TestDecodeRecords_UseNumber(t *testing.T) {
	got, _, err := collect(t, `[{"big":5938028332,"f":1.50}]`, "")
	if err != nil {
		t.Fatal(err)
	}
	n, ok := got[0]["big"].(json.Number)
	if !ok || n.String() != "5938028332" {
		t.Fatalf("big = %#v, want json.Number 5938028332", got[0]["big"])
	}
	f, ok := got[0]["f"].(json.Number)
	if !ok || f.String() != "1.50" {
		t.Fatalf("f = %#v, want json.Number 1.50 (exact source text)", got[0]["f"])
	}
}

func TestDecodeRecords_Errors(t *testing.T) {
	if _, _, err := collect(t, `not json`, ""); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

// Exact error-text compatibility with parseJSON (main.go): a set array_path
// that is missing or non-array must say "field '<k>' is not an array"; the
// generic message is reserved for auto-detect finding nothing.
func TestDecodeRecords_ErrorText(t *testing.T) {
	_, _, err := collect(t, `{"results":"not-an-array"}`, "results")
	if err == nil || err.Error() != "field 'results' is not an array" {
		t.Fatalf("non-array array_path: got %v", err)
	}
	_, _, err = collect(t, `{"other":[{"x":1}]}`, "results") // key absent entirely
	if err == nil || err.Error() != "field 'results' is not an array" {
		t.Fatalf("absent array_path key: got %v", err)
	}
	_, _, err = collect(t, `{"no":"array here"}`, "")
	if err == nil || err.Error() != "no array found in JSON response, specify array_path in config" {
		t.Fatalf("auto-detect no array: got %v", err)
	}
}

// parseJSON unmarshalled the whole body, so a document that turns malformed
// AFTER the record array must still fail on the streaming path (drainToEOF).
func TestDecodeRecords_MalformedRemainder(t *testing.T) {
	cases := []struct {
		name      string
		body      string
		arrayPath string
	}{
		{"wrapped_truncated_after_array", `{"results":[{"x":1}],`, "results"},
		{"positional_unclosed_outer", `[{"meta":1},[{"x":1}]`, ""},
		{"bare_unclosed", `[{"x":1}`, ""},
		{"bare_trailing_garbage", `[{"x":1}] garbage`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := collect(t, tc.body, tc.arrayPath); err == nil {
				t.Fatal("expected parse error for malformed document remainder")
			}
		})
	}
}

func TestDecodeRecords_FnErrorPropagates(t *testing.T) {
	sentinel := errors.New("boom")
	_, err := decodeRecords(strings.NewReader(`[{"id":1},{"id":2}]`), "", func(map[string]any) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("fn error not propagated: %v", err)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test -C components/http-json-extractor -run TestDecodeRecords -v`
Expected: FAIL — `undefined: decodeRecords`.

- [ ] **Step 3: Implement `stream.go`**

```go
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

```

- [ ] **Step 4: Run to verify pass**

Run: `go test -C components/http-json-extractor -run TestDecodeRecords -v`
Expected: PASS (all subtests).

- [ ] **Step 5: Build + full package test + commit**

Run: `go build -C components/http-json-extractor ./... && go vet -C components/http-json-extractor ./... && go test -C components/http-json-extractor ./...`
Expected: clean + PASS.

```bash
git add components/http-json-extractor/stream.go components/http-json-extractor/stream_test.go
git commit -m "feat(http-json-extractor): streaming JSON record decoder"
```

---

### Task 2: Stringify + column plan (`arrow_sink.go`, part 1)

**Files:**
- Create: `components/http-json-extractor/arrow_sink.go` (this task adds the pure helpers; Task 3 adds the sink)
- Test: `components/http-json-extractor/arrow_sink_test.go` (create)

**Interfaces:**
- Consumes: `FieldMapping`, `getValueRaw` (existing in main.go).
- Produces (used by Task 3):
  - `func stringifyValue(v any) (string, bool)` — `(cell, isNull)`.
  - `type columnPlan struct { names []string; extract func(rec map[string]any, i int) any }`
  - `func planFromFields(fields []FieldMapping) *columnPlan`
  - `func planFromBatch(batch []map[string]any) *columnPlan`

- [ ] **Step 1: Write the failing tests**

```go
package main

import (
	"encoding/json"
	"testing"
)

func TestStringifyValue(t *testing.T) {
	cases := []struct {
		name     string
		in       any
		want     string
		wantNull bool
	}{
		{"nil", nil, "", true},
		{"string", "hello", "hello", false},
		{"json_number_int", json.Number("5938028332"), "5938028332", false},
		{"json_number_frac", json.Number("1.50"), "1.50", false},
		{"bool", true, "true", false},
		{"float64_defensive", float64(2.5), "2.5", false},
		{"nested_object", map[string]any{"a": json.Number("1")}, `{"a":1}`, false},
		{"nested_array", []any{json.Number("1"), "x"}, `[1,"x"]`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, null := stringifyValue(tc.in)
			if null != tc.wantNull || got != tc.want {
				t.Fatalf("stringifyValue(%#v) = (%q,%v), want (%q,%v)", tc.in, got, null, tc.want, tc.wantNull)
			}
		})
	}
}

func TestPlanFromFields(t *testing.T) {
	p := planFromFields([]FieldMapping{
		{Path: "country.value", Name: "entity"},
		{Path: "iso", Name: "iso3"},
	})
	if len(p.names) != 2 || p.names[0] != "entity" || p.names[1] != "iso3" {
		t.Fatalf("names = %v (declared order required)", p.names)
	}
	rec := map[string]any{"country": map[string]any{"value": "Africa"}, "iso": "AFE"}
	if v := p.extract(rec, 0); v != "Africa" {
		t.Fatalf("extract nested = %v", v)
	}
	if v := p.extract(rec, 1); v != "AFE" {
		t.Fatalf("extract flat = %v", v)
	}
	if v := p.extract(map[string]any{}, 0); v != nil {
		t.Fatalf("missing path should be nil, got %v", v)
	}
}

func TestPlanFromBatch(t *testing.T) {
	batch := []map[string]any{
		{"b": 1, "a": 2},
		{"c": 3}, // later-only key must still become a column (union)
	}
	p := planFromBatch(batch)
	if len(p.names) != 3 || p.names[0] != "a" || p.names[1] != "b" || p.names[2] != "c" {
		t.Fatalf("names = %v, want sorted union [a b c]", p.names)
	}
	if v := p.extract(batch[1], 2); v != 3 {
		t.Fatalf("extract c = %v", v)
	}
	if v := p.extract(batch[1], 0); v != nil {
		t.Fatalf("absent key should be nil, got %v", v)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test -C components/http-json-extractor -run 'TestStringifyValue|TestPlanFrom' -v`
Expected: FAIL — `undefined: stringifyValue` / `planFromFields` / `planFromBatch`.

- [ ] **Step 3: Implement the helpers in `arrow_sink.go`**

```go
package main

import (
	"encoding/json"
	"sort"
	"strconv"
)

// stringifyValue renders one decoded JSON value as an Arrow String cell.
// Returns (cell, isNull). json.Number keeps its exact source text; nested
// objects/arrays become compact JSON.
func stringifyValue(v any) (string, bool) {
	switch x := v.(type) {
	case nil:
		return "", true
	case string:
		return x, false
	case json.Number:
		return x.String(), false
	case bool:
		return strconv.FormatBool(x), false
	case float64: // defensive: non-UseNumber callers
		return strconv.FormatFloat(x, 'g', -1, 64), false
	default: // map[string]any, []any
		b, err := json.Marshal(x)
		if err != nil {
			return "", true
		}
		return string(b), false
	}
}

// columnPlan fixes the output column names and how to pull each column's
// value out of a decoded record.
type columnPlan struct {
	names   []string
	extract func(rec map[string]any, i int) any
}

// planFromFields builds the plan for an explicit `fields` projection:
// column names in declared order, values resolved via dot-path.
func planFromFields(fields []FieldMapping) *columnPlan {
	names := make([]string, len(fields))
	paths := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
		paths[i] = f.Path
	}
	return &columnPlan{
		names: names,
		extract: func(rec map[string]any, i int) any {
			return getValueRaw(rec, paths[i])
		},
	}
}

// planFromBatch derives the plan when `fields` is unset: the sorted union of
// top-level keys across the first batch (matching the JSONL path's gateway
// inference, which collected field names from all objects in the first chunk).
func planFromBatch(batch []map[string]any) *columnPlan {
	set := make(map[string]bool)
	for _, rec := range batch {
		for k := range rec {
			set[k] = true
		}
	}
	names := make([]string, 0, len(set))
	for k := range set {
		names = append(names, k)
	}
	sort.Strings(names)
	return &columnPlan{
		names: names,
		extract: func(rec map[string]any, i int) any {
			return rec[names[i]]
		},
	}
}
```

- [ ] **Step 4: Run to verify pass, then commit**

Run: `go test -C components/http-json-extractor -run 'TestStringifyValue|TestPlanFrom' -v`
Expected: PASS.

```bash
git add components/http-json-extractor/arrow_sink.go components/http-json-extractor/arrow_sink_test.go
git commit -m "feat(http-json-extractor): stringify + column plan for all-String Arrow output"
```

---

### Task 3: `arrowSink` — batched all-String IPC writer with lazy open

**Files:**
- Modify: `components/http-json-extractor/arrow_sink.go` (append)
- Modify: `components/http-json-extractor/go.mod` (arrow dep, via tidy)
- Test: `components/http-json-extractor/arrow_sink_test.go` (append)

**Interfaces:**
- Consumes: `columnPlan`, `stringifyValue` (Task 2); `sdk.CloseResult`.
- Produces (used by Task 4):
  - `const arrowBatchRows = 8192`
  - `type ipcChunkWriter interface { Write(ctx context.Context, data []byte) error; Close(ctx context.Context) (*sdk.CloseResult, error); Bucket() string; Table() string }` — satisfied by `*sdk.Writer`.
  - `func newArrowSink(ctx context.Context, open func() (ipcChunkWriter, error), fields []FieldMapping, batchRows int) *arrowSink`
  - `(s *arrowSink) Add(rec map[string]any) error`
  - `(s *arrowSink) Finish() (rows int64, close *sdk.CloseResult, err error)` — flushes the partial batch, closes the writer if one was opened; `(0, nil, nil)` when no records were ever added (writer never opened).
  - `(s *arrowSink) Writer() ipcChunkWriter` — the opened writer (nil when no rows), for `Bucket()`/`Table()` logging.
  - `type sinkWriteError struct{ Err error }` with `Error()`/`Unwrap()` — wraps writer-open/Write failures so `main` can map them to `ExitAppError` while decode errors stay `ExitUserError`.

- [ ] **Step 1: Write the failing tests (append to arrow_sink_test.go)**

First extend the test file's import block with: `"bytes"`, `"context"`, `"errors"`, `"github.com/apache/arrow-go/v18/arrow/ipc"`, and `sdk "github.com/datuplet/datuplet/sdk/go"`. Then append:

```go
// --- appended in Task 3 ---

type fakeWriter struct {
	payloads [][]byte
	closed   bool
	rows     int64
}

func (f *fakeWriter) Write(_ context.Context, data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	f.payloads = append(f.payloads, cp)
	return nil
}
func (f *fakeWriter) Close(context.Context) (*sdk.CloseResult, error) {
	f.closed = true
	return &sdk.CloseResult{TotalRows: f.rows}, nil
}
func (f *fakeWriter) Bucket() string { return "raw" }
func (f *fakeWriter) Table() string  { return "t" }

// ipcRows parses one payload as a complete standalone IPC stream and returns
// (rows, columnNames). Fails the test if the payload is not self-contained.
func ipcRows(t *testing.T, payload []byte) (int64, []string) {
	t.Helper()
	rd, err := ipc.NewReader(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("payload is not a valid IPC stream: %v", err)
	}
	defer rd.Release()
	var rows int64
	for rd.Next() {
		rows += rd.Record().NumRows()
	}
	if rd.Err() != nil {
		t.Fatalf("ipc read: %v", rd.Err())
	}
	names := make([]string, 0)
	for _, f := range rd.Schema().Fields() {
		names = append(names, f.Name)
	}
	return rows, names
}

func TestArrowSink_BatchBoundariesAndSelfContainedStreams(t *testing.T) {
	fw := &fakeWriter{}
	sink := newArrowSink(context.Background(), func() (ipcChunkWriter, error) { return fw, nil },
		[]FieldMapping{{Path: "id", Name: "id"}}, 4 /* tiny batch */)
	for i := 0; i < 10; i++ {
		if err := sink.Add(map[string]any{"id": json.Number("1")}); err != nil {
			t.Fatal(err)
		}
	}
	rows, _, err := sink.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if rows != 10 {
		t.Fatalf("rows = %d, want 10", rows)
	}
	if len(fw.payloads) != 3 { // 4+4+2
		t.Fatalf("payloads = %d, want 3 (4+4+2)", len(fw.payloads))
	}
	var total int64
	for _, p := range fw.payloads {
		n, names := ipcRows(t, p) // each independently parseable = no-concat invariant
		total += n
		if len(names) != 1 || names[0] != "id" {
			t.Fatalf("schema = %v", names)
		}
	}
	if total != 10 {
		t.Fatalf("ipc total rows = %d, want 10", total)
	}
	if !fw.closed {
		t.Fatal("writer not closed")
	}
}

func TestArrowSink_NoFieldsDerivesPlanFromFirstBatch(t *testing.T) {
	fw := &fakeWriter{}
	sink := newArrowSink(context.Background(), func() (ipcChunkWriter, error) { return fw, nil }, nil, 3)
	// key "c" appears only in the 2nd record — union must include it.
	recs := []map[string]any{
		{"b": json.Number("1")},
		{"a": "x", "c": true},
		{"a": "y"},
		{"a": "z"}, // second batch
	}
	for _, r := range recs {
		if err := sink.Add(r); err != nil {
			t.Fatal(err)
		}
	}
	rows, _, err := sink.Finish()
	if err != nil || rows != 4 {
		t.Fatalf("rows=%d err=%v", rows, err)
	}
	_, names := ipcRows(t, fw.payloads[0])
	want := []string{"a", "b", "c"}
	if len(names) != 3 || names[0] != want[0] || names[1] != want[1] || names[2] != want[2] {
		t.Fatalf("schema = %v, want %v (sorted union of first batch)", names, want)
	}
}

func TestArrowSink_ZeroRecordsNeverOpensWriter(t *testing.T) {
	opened := false
	sink := newArrowSink(context.Background(), func() (ipcChunkWriter, error) {
		opened = true
		return &fakeWriter{}, nil
	}, nil, 4)
	rows, cr, err := sink.Finish()
	if err != nil || rows != 0 || cr != nil {
		t.Fatalf("rows=%d cr=%v err=%v", rows, cr, err)
	}
	if opened {
		t.Fatal("writer must not be opened for zero records")
	}
	if sink.Writer() != nil {
		t.Fatal("Writer() must be nil for zero records")
	}
}

func TestArrowSink_WriteErrorIsSinkWriteError(t *testing.T) {
	sink := newArrowSink(context.Background(), func() (ipcChunkWriter, error) {
		return nil, errors.New("open boom")
	}, []FieldMapping{{Path: "id", Name: "id"}}, 1)
	err := sink.Add(map[string]any{"id": "1"})
	var swe *sinkWriteError
	if !errors.As(err, &swe) {
		t.Fatalf("want sinkWriteError, got %v", err)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test -C components/http-json-extractor -run TestArrowSink -v`
Expected: FAIL — `undefined: newArrowSink` (and the arrow import will need `go mod tidy`, done in Step 3).

- [ ] **Step 3: Implement the sink (append to arrow_sink.go) + tidy**

Add to the import block: `"context"`, `"fmt"`, `"github.com/apache/arrow-go/v18/arrow"`, `"github.com/apache/arrow-go/v18/arrow/array"`, `"github.com/apache/arrow-go/v18/arrow/ipc"`, `"github.com/apache/arrow-go/v18/arrow/memory"`, `"bytes"`, `sdk "github.com/datuplet/datuplet/sdk/go"`.

```go
// arrowBatchRows is the per-batch row count accumulated before flushing a
// self-contained Arrow IPC stream. Same empirical sweet spot data-generator
// uses (components/data-generator/arrow_writer.go).
const arrowBatchRows = 8192

// ipcChunkWriter is the slice of *sdk.Writer the sink needs; narrowed for
// testability with a fake.
type ipcChunkWriter interface {
	Write(ctx context.Context, data []byte) error
	Close(ctx context.Context) (*sdk.CloseResult, error)
	Bucket() string
	Table() string
}

// sinkWriteError marks writer-open/Write/Close failures (infrastructure) so
// main can map them to ExitAppError while decode errors stay ExitUserError.
type sinkWriteError struct{ Err error }

func (e *sinkWriteError) Error() string { return e.Err.Error() }
func (e *sinkWriteError) Unwrap() error { return e.Err }

// arrowSink accumulates records into an all-String Arrow RecordBuilder and
// flushes a self-contained IPC stream per batch via ONE writer.Write call
// each (the SDK sends Arrow-IPC writes immediately — one POST per call — so
// the gateway never sees concatenated IPC streams).
//
// The writer is opened lazily on the first flush: zero records = no writer,
// letting callers preserve the commit-empty path.
type arrowSink struct {
	ctx       context.Context
	open      func() (ipcChunkWriter, error)
	writer    ipcChunkWriter
	fields    []FieldMapping
	plan      *columnPlan
	pending   []map[string]any // buffered records while plan is unknown
	alloc     memory.Allocator
	builder   *array.RecordBuilder
	batchRows int
	inBatch   int
	rows      int64
}

func newArrowSink(ctx context.Context, open func() (ipcChunkWriter, error), fields []FieldMapping, batchRows int) *arrowSink {
	if batchRows <= 0 {
		batchRows = arrowBatchRows
	}
	s := &arrowSink{ctx: ctx, open: open, fields: fields, alloc: memory.NewGoAllocator(), batchRows: batchRows}
	if len(fields) > 0 {
		s.adoptPlan(planFromFields(fields))
	}
	return s
}

// adoptPlan fixes the schema and creates the builder.
func (s *arrowSink) adoptPlan(p *columnPlan) {
	s.plan = p
	fieldsArr := make([]arrow.Field, len(p.names))
	for i, n := range p.names {
		fieldsArr[i] = arrow.Field{Name: n, Type: arrow.BinaryTypes.String, Nullable: true}
	}
	s.builder = array.NewRecordBuilder(s.alloc, arrow.NewSchema(fieldsArr, nil))
}

// Add appends one record, flushing a batch when full.
func (s *arrowSink) Add(rec map[string]any) error {
	if s.plan == nil {
		s.pending = append(s.pending, rec)
		if len(s.pending) < s.batchRows {
			return nil
		}
		s.adoptPlan(planFromBatch(s.pending))
		for _, p := range s.pending {
			s.appendRow(p)
		}
		s.pending = nil
		return s.flush()
	}
	s.appendRow(rec)
	if s.inBatch >= s.batchRows {
		return s.flush()
	}
	return nil
}

func (s *arrowSink) appendRow(rec map[string]any) {
	for i := range s.plan.names {
		cell, isNull := stringifyValue(s.plan.extract(rec, i))
		b := s.builder.Field(i).(*array.StringBuilder)
		if isNull {
			b.AppendNull()
		} else {
			b.Append(cell)
		}
	}
	s.inBatch++
	s.rows++
}

// flush serializes the current batch as a complete IPC stream (schema +
// record + EOS) and ships it with one Write call.
func (s *arrowSink) flush() error {
	if s.inBatch == 0 {
		return nil
	}
	rec := s.builder.NewRecord() // builds + resets internal builders
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()), ipc.WithAllocator(s.alloc))
	if err := w.Write(rec); err != nil {
		w.Close() //nolint:errcheck
		return &sinkWriteError{Err: fmt.Errorf("ipc write record: %w", err)}
	}
	if err := w.Close(); err != nil {
		return &sinkWriteError{Err: fmt.Errorf("ipc close writer: %w", err)}
	}

	if s.writer == nil {
		wr, err := s.open()
		if err != nil {
			return &sinkWriteError{Err: fmt.Errorf("failed to open writer: %w", err)}
		}
		s.writer = wr
	}
	if err := s.writer.Write(s.ctx, buf.Bytes()); err != nil {
		return &sinkWriteError{Err: fmt.Errorf("failed to write IPC batch: %w", err)}
	}
	s.inBatch = 0
	return nil
}

// Finish flushes the partial batch (deriving the plan first if it is still
// pending) and closes the writer when one was opened. Zero records: no
// writer was opened and (0, nil, nil) is returned.
func (s *arrowSink) Finish() (int64, *sdk.CloseResult, error) {
	if s.plan == nil && len(s.pending) > 0 {
		s.adoptPlan(planFromBatch(s.pending))
		for _, p := range s.pending {
			s.appendRow(p)
		}
		s.pending = nil
	}
	if err := s.flush(); err != nil {
		return s.rows, nil, err
	}
	if s.builder != nil {
		s.builder.Release()
		s.builder = nil
	}
	if s.writer == nil {
		return 0, nil, nil
	}
	cr, err := s.writer.Close(s.ctx)
	if err != nil {
		return s.rows, nil, &sinkWriteError{Err: fmt.Errorf("failed to close writer: %w", err)}
	}
	return s.rows, cr, nil
}

// Writer exposes the opened writer (nil when no rows were written).
func (s *arrowSink) Writer() ipcChunkWriter { return s.writer }
```

Then run `make tidy` (the repo rule — never bare `go mod tidy`; `-C` after the
subcommand is not even valid Go flag syntax). It tidies every module and pulls
`github.com/apache/arrow-go/v18 v18.6.0` into the extractor module — same
version as root/data-generator.

- [ ] **Step 4: Run to verify pass**

Run: `go test -C components/http-json-extractor -run TestArrowSink -v`
Expected: PASS (all four).

- [ ] **Step 5: Full package test + commit**

Run: `go build -C components/http-json-extractor ./... && go vet -C components/http-json-extractor ./... && go test -C components/http-json-extractor ./...`
Expected: clean + PASS.

```bash
git add components/http-json-extractor/arrow_sink.go components/http-json-extractor/arrow_sink_test.go components/http-json-extractor/go.mod components/http-json-extractor/go.sum
git commit -m "feat(http-json-extractor): all-String Arrow-IPC sink with per-batch streams"
```

---

### Task 4: Wire both paths through the sink; delete dead JSONL/projection code

**Files:**
- Modify: `components/http-json-extractor/main.go`
- Modify: `components/http-json-extractor/transform_test.go` (remove dead-code tests)

**Interfaces:**
- Consumes: `fetchStream`, `decodeRecords` (Task 1); `newArrowSink`, `sinkWriteError`, `arrowBatchRows` (Task 3).
- Produces: no new symbols; `main()`/`runPaginatedExtraction` behavior per spec.

- [ ] **Step 1: Replace the single-request block in `main()` and fix the paginated dispatch's exit-code classification**

First, replace the paginated dispatch (main.go:136-141) so infrastructure
failures exit as FailedApplication instead of FailedUser (the exit-code
contract in Global Constraints; today every paginated error is wrapped in
`ExitUserError`):

```go
	// Paginated mode - stream data incrementally, page by page.
	if compCfg.Pagination != nil && compCfg.Pagination.Type != "" {
		if err := runPaginatedExtraction(ctx, client, &compCfg, outputTable); err != nil {
			var swe *sinkWriteError
			if errors.As(err, &swe) {
				sdk.ExitAppError(fmt.Sprintf("paginated extraction failed: %v", err))
			}
			sdk.ExitUserError(fmt.Sprintf("paginated extraction failed: %v", err))
		}
		return
	}
```

Then replace the block from `// Single-request mode.` (main.go:143) through the end of `main()` (main.go:179, the closing `}`) with:

```go
	// Single-request mode.
	client.Log(ctx, "INFO", fmt.Sprintf("Fetching JSON from: %s", compCfg.URL))

	sink := newArrowSink(ctx, func() (ipcChunkWriter, error) {
		return client.OpenWriter(ctx, outputTable, sdk.WithFormat(pb.DataFormat_FORMAT_ARROW_IPC))
	}, compCfg.Fields, arrowBatchRows)

	body, err := fetchStream(ctx, compCfg.URL, compCfg.Headers)
	if err != nil {
		sdk.ExitUserError(fmt.Sprintf("failed to fetch JSON: %v", err))
	}
	n, err := decodeRecords(body, compCfg.ArrayPath, sink.Add)
	body.Close()
	if err != nil {
		var swe *sinkWriteError
		if errors.As(err, &swe) {
			sdk.ExitAppError(err.Error())
		}
		sdk.ExitUserError(fmt.Sprintf("failed to fetch JSON: %v", err))
	}
	client.Log(ctx, "INFO", fmt.Sprintf("Fetched %d records", n))

	rows, closeResult, err := sink.Finish()
	if err != nil {
		sdk.ExitAppError(err.Error())
	}
	if rows == 0 {
		client.Log(ctx, "WARN", "No records found")
		if _, err := client.Commit(ctx); err != nil {
			sdk.ExitAppError(fmt.Sprintf("commit failed: %v", err))
		}
		sdk.StatusMessage("extracted 0 records (empty response)")
		return
	}
	client.Log(ctx, "INFO", fmt.Sprintf("Completed output %s.%s: %d rows", sink.Writer().Bucket(), sink.Writer().Table(), closeResult.TotalRows))

	if err := commitAndStatus(ctx, client, compCfg.URL); err != nil {
		sdk.ExitAppError(err.Error())
	}
	client.Log(ctx, "INFO", "HTTP JSON extraction completed successfully")
}
```

Add `"errors"` to main.go's import block.

- [ ] **Step 2: Rewrite `runPaginatedExtraction`'s writer/loop plumbing**

(a) Replace the writer-open block (main.go:206-210) with a sink:

```go
	// One sink across all pages: the schema is fixed after the first batch
	// and every batch ships as its own IPC stream/POST.
	sink := newArrowSink(ctx, func() (ipcChunkWriter, error) {
		return client.OpenWriter(ctx, outputTable, sdk.WithFormat(pb.DataFormat_FORMAT_ARROW_IPC))
	}, cfg.Fields, arrowBatchRows)
```

(b) Replace the per-page fetch/truncate/write block (main.go:233-265, from `// Fetch page` through the `client.Log(... "Page %d: fetched ...")` line) with:

```go
		// Fetch + stream-decode the page, feeding the sink. Every decoded
		// object counts toward pageObjects (for empty/partial-page detection)
		// but only records under the max_records cap are written.
		body, err := fetchStream(ctx, pageURL, cfg.Headers)
		if err != nil {
			return fmt.Errorf("failed to fetch page %d: %w", pageCount+1, err)
		}
		truncated := false
		pageObjects, err := decodeRecords(body, cfg.ArrayPath, func(rec map[string]any) error {
			if pagination.MaxRecords > 0 && totalRecords >= pagination.MaxRecords {
				truncated = true
				return nil // keep counting the page; stop writing
			}
			if err := sink.Add(rec); err != nil {
				return err
			}
			totalRecords++
			return nil
		})
		body.Close()
		if err != nil {
			return fmt.Errorf("failed to fetch page %d: %w", pageCount+1, err)
		}
		if truncated {
			client.Log(ctx, "INFO", fmt.Sprintf("Truncating to max records limit: %d", pagination.MaxRecords))
		}

		// Check if we should stop
		if pageObjects == 0 {
			if pagination.StopWhenEmpty {
				client.Log(ctx, "INFO", "Received empty page, stopping pagination")
				break
			}
		}

		pageCount++
		client.Log(ctx, "INFO", fmt.Sprintf("Page %d: fetched %d records (total: %d)", pageCount, pageObjects, totalRecords))
```

Then update the two remaining stop conditions in the loop to use `pageObjects`
instead of `len(records)`:

```go
		// Check if we've hit max records
		if pagination.MaxRecords > 0 && totalRecords >= pagination.MaxRecords {
			client.Log(ctx, "INFO", fmt.Sprintf("Reached max records limit: %d", pagination.MaxRecords))
			break
		}

		// Stop if this page had fewer records than page_size (likely last page)
		if pagination.PageSize > 0 && pageObjects < pagination.PageSize {
			client.Log(ctx, "INFO", "Received partial page, stopping pagination")
			break
		}
```

(c) Replace the tail close block (main.go:283-288, `// Close writer` through the `Completed output` log) with:

```go
	// Flush + close (no-op writer when zero records; commit still runs).
	rows, closeResult, err := sink.Finish()
	if err != nil {
		return err // already a *sinkWriteError → main exits FailedApplication
	}
	if rows > 0 {
		client.Log(ctx, "INFO", fmt.Sprintf("Completed output %s.%s: %d rows", sink.Writer().Bucket(), sink.Writer().Table(), closeResult.TotalRows))
	}
```

(c2) In the same tail, wrap the existing `commitAndStatus` call's error so a
commit failure (infrastructure) also classifies as FailedApplication:

```go
	if err := commitAndStatus(ctx, client, cfg.URL); err != nil {
		return &sinkWriteError{Err: err}
	}
```

(Note: `totalRecords`/`pageCount` keep their existing declarations; the old
`records`/`recordsToWrite` locals disappear.)

- [ ] **Step 3: Delete dead code + its tests**

- Delete from `main.go`: `encodeJSONL` (436-446), `writeJSONL` (451-460), `projectRecords` (663-676). Keep everything else (notably `fetchJSON`/`parseJSON`/`recordsFromSlice`/`getValueRaw` — sample mode).
- Delete from `transform_test.go`: `TestEncodeJSONL_LinesAndConcatSafety`, `TestEncodeJSONL_NoHTMLEscape`, `TestProjectRecords`, `TestProjectRecords_Identity`. Keep `TestResolveOutputTable`. Remove imports that become unused in that test file (`bufio`, `bytes`, `encoding/json` — the compiler is authoritative).
- In `main.go`, remove imports that became unused (`bytes`, `io` if only `fetchJSON` still uses them — `fetchJSON` uses `io.ReadAll`, so `io` stays; the compiler is authoritative).

- [ ] **Step 4: Build, vet, full test**

Run: `go build -C components/http-json-extractor ./... && go vet -C components/http-json-extractor ./... && go test -C components/http-json-extractor ./...`
Expected: clean + PASS (parse/stream/sink/config/resolve tests all green).

- [ ] **Step 5: Commit**

```bash
git add components/http-json-extractor/main.go components/http-json-extractor/transform_test.go
git commit -m "feat(http-json-extractor): stream all-String Arrow IPC in both paths"
```

---

### Task 5: `cmd/mock-datagateway/` + `make extractor-local`

**Files:**
- Create: `cmd/mock-datagateway/main.go` (root module — no go.mod changes; root already has arrow-go v18.6.0 + grpc)
- Create: `cmd/mock-datagateway/example-nyc311.json`
- Modify: `Makefile` (new target)

**Interfaces:**
- Consumes: `pkg/datagateway/proto/v2` generated stubs (`pb.UnimplementedDataGatewayServer`, `pb.RegisterDataGatewayServer`, message types), arrow `ipc`.
- Produces: `bin/mock-datagateway` binary; `make extractor-local CONFIG=<file> [BUCKET=raw]`.
- Wire contracts the mock MUST honor (verified against source):
  - HTTP write response JSON: `{"rows_accepted":N,"buffer_size_bytes":0}` (SDK parse at `sdk/go/client.go:493-499`).
  - `CommitResponse{Success, Buckets: []*BucketCommitResult{{Bucket, Status: STATUS_COMMITTED, Tables: []*TableCommitResult{{Table, Bucket, Status: STATUS_COMMITTED, RowsAdded, FilesAdded}}}}}`.
  - `OpenWriterResponse{WriterId, HttpEndpoint, Bucket, Table}`; `CloseWriterResponse{TotalRows}`; `ComponentConfig{ExecutionId, ComponentName, Config, OutputConfig:{DefaultBucket, DefaultWriteMode}, ChunkSize}`.

- [ ] **Step 1: Write `cmd/mock-datagateway/main.go`**

```go
// Command mock-datagateway is a local development stand-in for the Data
// Gateway sidecar: it serves the handful of gRPC RPCs + the HTTP write
// endpoint the component SDK uses, validates and counts the Arrow IPC
// batches it receives, and discards the data. It lets the real
// http-json-extractor binary run locally (DATUPLET_GATEWAY_ADDR=localhost:50051)
// against a live URL with no cluster, no Lakekeeper, no object storage.
//
// Dev/test tool only — NOT a deployment surface.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/grpc"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

type writerState struct {
	bucket, table string
	rows          int64
	batches       int64
	bytes         int64
	schema        string   // "name:type, ..." label of the first payload (logging)
	schemaNames   []string // first payload's column names — later payloads must match
}

type mockGateway struct {
	pb.UnimplementedDataGatewayServer

	mu         sync.Mutex
	cfgBytes   []byte
	bucket     string
	writeMode  string
	httpBase   string
	nextID     int
	writers    map[string]*writerState
	expectCols []string // from the config's fields[].name; empty = names not enforced
}

func (g *mockGateway) GetConfig(ctx context.Context, _ *pb.GetConfigRequest) (*pb.ComponentConfig, error) {
	return &pb.ComponentConfig{
		ExecutionId:   "local-mock",
		ComponentName: "http-json-extractor",
		Config:        g.cfgBytes,
		ChunkSize:     32 * 1024 * 1024,
		OutputConfig: &pb.OutputConfig{
			DefaultBucket:    g.bucket,
			DefaultWriteMode: g.writeMode,
		},
	}, nil
}

func (g *mockGateway) OpenWriter(ctx context.Context, req *pb.OpenWriterRequest) (*pb.OpenWriterResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.nextID++
	id := fmt.Sprintf("w%d", g.nextID)
	bucket := req.Bucket
	if bucket == "" {
		bucket = g.bucket
	}
	g.writers[id] = &writerState{bucket: bucket, table: req.Table}
	log.Printf("mock-dg: OpenWriter id=%s table=%s.%s format=%s", id, bucket, req.Table, req.InputFormat)
	return &pb.OpenWriterResponse{
		WriterId:     id,
		HttpEndpoint: g.httpBase + "/data/write/" + id,
		Bucket:       bucket,
		Table:        req.Table,
	}, nil
}

func (g *mockGateway) WriteChunk(ctx context.Context, req *pb.WriteChunkRequest) (*pb.WriteChunkResponse, error) {
	rows, err := g.ingest(req.WriterId, req.Data)
	if err != nil {
		return nil, err
	}
	return &pb.WriteChunkResponse{RowsAccepted: rows}, nil
}

func (g *mockGateway) CloseWriter(ctx context.Context, req *pb.CloseWriterRequest) (*pb.CloseWriterResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	ws, ok := g.writers[req.WriterId]
	if !ok {
		return nil, fmt.Errorf("unknown writer: %s", req.WriterId)
	}
	log.Printf("mock-dg: CloseWriter id=%s table=%s.%s rows=%d batches=%d bytes=%d",
		req.WriterId, ws.bucket, ws.table, ws.rows, ws.batches, ws.bytes)
	return &pb.CloseWriterResponse{TotalRows: ws.rows, TotalBytes: ws.bytes}, nil
}

func (g *mockGateway) Commit(ctx context.Context, _ *pb.CommitRequest) (*pb.CommitResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	byBucket := map[string]*pb.BucketCommitResult{}
	for _, ws := range g.writers {
		b, ok := byBucket[ws.bucket]
		if !ok {
			b = &pb.BucketCommitResult{Bucket: ws.bucket, Status: pb.BucketCommitResult_STATUS_COMMITTED}
			byBucket[ws.bucket] = b
		}
		b.Tables = append(b.Tables, &pb.TableCommitResult{
			Table:      ws.table,
			Bucket:     ws.bucket,
			Status:     pb.TableCommitResult_STATUS_COMMITTED,
			RowsAdded:  ws.rows,
			FilesAdded: int32(ws.batches),
		})
		log.Printf("mock-dg: COMMIT %s.%s rows=%d batches=%d bytes=%d schema=[%s]",
			ws.bucket, ws.table, ws.rows, ws.batches, ws.bytes, ws.schema)
	}
	resp := &pb.CommitResponse{Success: true}
	for _, b := range byBucket {
		resp.Buckets = append(resp.Buckets, b)
	}
	return resp, nil
}

func (g *mockGateway) Log(ctx context.Context, req *pb.LogRequest) (*pb.LogResponse, error) {
	fmt.Printf("[component] [%s] %s\n", req.Level, req.Message)
	return &pb.LogResponse{}, nil
}

// validateSchema enforces the extractor's output contract on every payload:
// all columns Arrow String (utf8), and — when the component config declares a
// fields projection — exactly the declared column names, in order. A
// violation fails the write (HTTP 500 / gRPC error), so the local proof
// cannot silently pass with wrong output.
func (g *mockGateway) validateSchema(sch *arrow.Schema) error {
	for _, f := range sch.Fields() {
		if f.Type.ID() != arrow.STRING {
			return fmt.Errorf("schema violation: column %q is %s, want utf8 (all-String contract)", f.Name, f.Type)
		}
	}
	if len(g.expectCols) > 0 {
		if sch.NumFields() != len(g.expectCols) {
			return fmt.Errorf("schema violation: %d columns, want %d %v", sch.NumFields(), len(g.expectCols), g.expectCols)
		}
		for i, want := range g.expectCols {
			if sch.Field(i).Name != want {
				return fmt.Errorf("schema violation: column %d is %q, want %q", i, sch.Field(i).Name, want)
			}
		}
	}
	return nil
}

// ingest parses one payload as a complete Arrow IPC stream, validates its
// schema against the contract, counts its rows, and records the schema on
// first sight.
func (g *mockGateway) ingest(writerID string, data []byte) (int64, error) {
	rd, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return 0, fmt.Errorf("payload is not a valid Arrow IPC stream: %w", err)
	}
	defer rd.Release()
	if err := g.validateSchema(rd.Schema()); err != nil {
		return 0, err
	}
	var rows int64
	for rd.Next() {
		rows += rd.Record().NumRows()
	}
	if rd.Err() != nil {
		return 0, fmt.Errorf("ipc read: %w", rd.Err())
	}

	names := make([]string, 0, rd.Schema().NumFields())
	labeled := make([]string, 0, rd.Schema().NumFields())
	for _, f := range rd.Schema().Fields() {
		names = append(names, f.Name)
		labeled = append(labeled, f.Name+":"+f.Type.String())
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	ws, ok := g.writers[writerID]
	if !ok {
		return 0, fmt.Errorf("unknown writer: %s", writerID)
	}
	if ws.schemaNames == nil {
		// First payload for this writer: lock its schema. Later payloads
		// must match exactly — catches cross-batch schema drift even for
		// no-`fields` configs (where expectCols is empty).
		ws.schemaNames = names
		ws.schema = strings.Join(labeled, ", ")
		log.Printf("mock-dg: writer %s schema: [%s]", writerID, ws.schema)
	} else if !slices.Equal(ws.schemaNames, names) {
		return 0, fmt.Errorf("schema violation: writer %s column drift: first %v, now %v", writerID, ws.schemaNames, names)
	}
	ws.rows += rows
	ws.batches++
	ws.bytes += int64(len(data))
	return rows, nil
}

func (g *mockGateway) handleHTTPWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/data/write/"), "/")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	rows, err := g.ingest(id, body)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"rows_accepted": rows, "buffer_size_bytes": 0})
}

func main() {
	grpcAddr := flag.String("grpc-addr", "localhost:50051", "gRPC listen address")
	httpAddr := flag.String("http-addr", "localhost:50052", "HTTP data-plane listen address")
	configPath := flag.String("config", "", "path to the component config JSON (required)")
	bucket := flag.String("bucket", "raw", "default output bucket name")
	writeMode := flag.String("write-mode", "FULL_LOAD", "default write mode")
	flag.Parse()

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "usage: mock-datagateway -config <component-config.json> [-bucket raw]")
		os.Exit(2)
	}
	cfgBytes, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("read config: %v", err)
	}

	// When the config declares a fields projection, enforce exactly those
	// column names (in order) on every received batch.
	var cfgFields struct {
		Fields []struct {
			Name string `json:"name"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(cfgBytes, &cfgFields); err != nil {
		log.Fatalf("parse config: %v", err)
	}
	expectCols := make([]string, 0, len(cfgFields.Fields))
	for _, f := range cfgFields.Fields {
		expectCols = append(expectCols, f.Name)
	}

	g := &mockGateway{
		cfgBytes:   cfgBytes,
		bucket:     *bucket,
		writeMode:  *writeMode,
		httpBase:   "http://" + *httpAddr,
		writers:    map[string]*writerState{},
		expectCols: expectCols,
	}
	if len(expectCols) > 0 {
		log.Printf("mock-dg: enforcing projected columns %v (all utf8)", expectCols)
	} else {
		log.Printf("mock-dg: enforcing all-utf8 columns (no fields projection in config)")
	}

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("grpc listen: %v", err)
	}
	gs := grpc.NewServer()
	pb.RegisterDataGatewayServer(gs, g)
	go func() {
		if err := gs.Serve(lis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()
	log.Printf("mock-dg: gRPC on %s, HTTP data plane on %s (config=%s bucket=%s)", *grpcAddr, *httpAddr, *configPath, *bucket)

	http.HandleFunc("/data/write/", g.handleHTTPWrite)
	if err := http.ListenAndServe(*httpAddr, nil); err != nil {
		log.Fatalf("http serve: %v", err)
	}
}
```

- [ ] **Step 2: Create the example config**

`cmd/mock-datagateway/example-nyc311.json` — a bounded 200k-row NYC 311 pull exercising projection + offset pagination:

```json
{
  "url": "https://data.cityofnewyork.us/resource/erm2-nwe9.json?$order=:id",
  "table_name": "nyc_311_requests",
  "fields": [
    {"path": "unique_key", "name": "request_id"},
    {"path": "created_date", "name": "created_date"},
    {"path": "agency", "name": "agency"},
    {"path": "complaint_type", "name": "complaint_type"},
    {"path": "borough", "name": "borough"},
    {"path": "status", "name": "status"},
    {"path": "latitude", "name": "latitude"},
    {"path": "longitude", "name": "longitude"}
  ],
  "pagination": {
    "type": "offset",
    "param": "$offset",
    "size_param": "$limit",
    "page_size": 50000,
    "max_records": 200000
  }
}
```

- [ ] **Step 3: Add the Makefile target**

Add `extractor-local` to the `.PHONY` list at the top of the Makefile (the
list is explicit; a missing entry breaks if a file of that name ever exists).
Then add the target next to the other developer-loop targets (e.g. after
`build-components-local`):

```make
extractor-local: ## Run http-json-extractor locally against the mock gateway (CONFIG=<component-config.json> [BUCKET=raw]); reports rows + peak RSS (macOS /usr/bin/time -l; on Linux use `command time -v`)
ifndef CONFIG
	$(error CONFIG is required, e.g. make extractor-local CONFIG=cmd/mock-datagateway/example-nyc311.json)
endif
	go build -o bin/mock-datagateway ./cmd/mock-datagateway
	go build -C components/http-json-extractor -o $(CURDIR)/bin/http-json-extractor-local .
	@./bin/mock-datagateway -config $(CONFIG) -bucket $(or $(BUCKET),raw) & \
	MOCK_PID=$$!; \
	trap "kill $$MOCK_PID 2>/dev/null" EXIT; \
	sleep 1; \
	DATUPLET_GATEWAY_ADDR=localhost:50051 /usr/bin/time -l ./bin/http-json-extractor-local
```

- [ ] **Step 4: Build + smoke locally**

Run: `go build ./cmd/mock-datagateway && go vet ./cmd/mock-datagateway`
Expected: clean.

Smoke (bounded, small — 500 rows to keep it quick): create a scratch config that copies example-nyc311.json with `"max_records": 500, "page_size": 500`, then:

```bash
make extractor-local CONFIG=/tmp/nyc311-500.json
```

Expected: mock startup prints `enforcing projected columns [request_id ...] (all utf8)`; component logs stream through the mock (`[component] [INFO] ...`); mock prints `schema: [request_id:utf8, ...]` (validation would 500 the write on any non-utf8 or wrong/missing column, failing the run visibly); per-writer `COMMIT ... rows=500`; extractor exits 0; `/usr/bin/time -l` prints peak RSS. Record the observed rows + peak RSS in the report.

- [ ] **Step 5: Commit**

```bash
git add cmd/mock-datagateway/main.go cmd/mock-datagateway/example-nyc311.json Makefile
git commit -m "feat(dev): mock Data Gateway + make extractor-local for local component runs"
```

---

### Task 6: Docs — all-String Arrow output + local debugging

**Files:**
- Modify: `docs/components.md`

- [ ] **Step 1: Update the http-json-extractor section**

- Replace the JSONL + 64 KiB-first-chunk paragraph (added in v0.11.0): the component now writes **Arrow IPC** to the gateway; the 64 KiB JSONL line limit **no longer applies** to this component.
- Document the output typing: **every emitted column is a string** (nested objects/arrays become compact JSON text; numbers keep their exact source text). Downstream `sql-transform` should `CAST` for numeric work.
- Document the schema rule: with `fields` — the projected names in declared order; without — the sorted union of top-level keys across the first 8192 records.
- Add the compatibility caveat: writing into a **pre-existing table with typed (non-string) columns** fails iceberg's schema check — for both APPEND and FULL_LOAD (`ReplaceDataFiles` does not replace the catalog schema). Use a fresh table name or drop the old table.
- Add a short **Local debugging** note: `make extractor-local CONFIG=cmd/mock-datagateway/example-nyc311.json` runs the real binary against the mock gateway and reports rows + peak RSS.

- [ ] **Step 2: Commit**

```bash
git add docs/components.md
git commit -m "docs(components): http-json-extractor Arrow-IPC all-String output + local debug rig"
```

---

### Task 7: Local 200k proof, tidy, bump 0.12.0, push, draft PR

**Files:**
- Modify: charts `Chart.yaml` ×4, `charts/datuplet-app/values.yaml`, `Makefile` (via `make bump-version`); possibly `go.mod`/`go.sum` (via `make tidy`).

- [ ] **Step 1: The bounded live proof (200k rows)**

```bash
make extractor-local CONFIG=cmd/mock-datagateway/example-nyc311.json
```

Expected: 4 pages × 50k, mock `COMMIT ... rows=200000`, all-`utf8` schema, exit 0. Record **peak RSS** from `/usr/bin/time -l` — must be well under 512 MiB (the per-batch bound; expect low hundreds of MB dominated by Go runtime + HTTP buffers). If RSS scales with page_size, the streaming path has a buffering bug — stop and investigate before proceeding.

- [ ] **Step 2: Monorepo hygiene**

Run: `make tidy`
Expected: completes; only expected `go.mod`/`go.sum` churn (the extractor module gained arrow-go in Task 3; other modules unchanged).

- [ ] **Step 3: Bump version**

Run: `make bump-version VERSION=0.12.0`
Expected output lists all four charts at `0.12.0`, `values.yaml tag: v0.12.0`, `COMPONENT_TAG ?= v0.12.0`.

- [ ] **Step 4: Full sanity**

Run: `go build ./... && go vet ./cmd/mock-datagateway && go test -C components/http-json-extractor ./...`
Expected: clean + PASS.

- [ ] **Step 5: Commit the bump; run the final Codex gate; then push + draft PR**

```bash
git add charts/datuplet-app/Chart.yaml charts/datuplet-infra/Chart.yaml charts/datuplet-lakekeeper/Chart.yaml charts/datuplet-operators/Chart.yaml charts/datuplet-app/values.yaml Makefile
git commit -m "chore(release): bump version to 0.12.0"
```

**GATE — do not push yet.** Run the final whole-branch Codex review per the
Execution Protocol section (in-session codex-companion `task`, diff = branch
base → the bump commit; stop-and-ask on findings). Only after it passes:

```bash
git push -u public feat/http-json-extractor-arrow-streaming
gh pr create --draft --repo kacurez/datuplet --base main \
  --title "feat(http-json-extractor): streaming Arrow-IPC output + mock-DG local rig — release v0.12.0" \
  --body "See docs/superpowers/specs/2026-07-23-http-json-extractor-arrow-streaming-design.md. Stream-decodes JSON and writes all-String Arrow-IPC batches (bounds component memory, removes gateway JSON-parse load); adds cmd/mock-datagateway + make extractor-local. BEHAVIOR CHANGE: extractor output columns are all strings now (CAST downstream); fresh tables only for pre-existing typed tables."
```

**Do NOT stage** `docs/superpowers/specs/2026-07-22-rfc-028-user-apps-wasm-workers-design.md` (unrelated untracked file) — stage files explicitly, never `git add -A`.

- [ ] **Step 6: e2e verification note (CI)**

The PR's CI e2e (fail-closed since RFC 024) exercises ~13 extractor fixtures. Audit says they're string-safe: assertions are COUNT/JOIN/GROUP-BY/row-count based; the only `AssertSchema` call (`tests/e2e/scenarios_agent_loop_test.go:292`) is presence-only and on a data-generator table. Watch the PR's e2e run; if a scenario trips on typing (e.g. an ORDER-BY-sensitive assertion), fix the fixture SQL with an explicit `CAST` — do not weaken assertions.

---

### Task 8 (post-release — operator workflow, not part of the PR)

- [ ] After the maintainer merges + tags `v0.12.0` and deploys: re-run the `nyc-311-requests` pipeline with `max_records: 5000000` (the saved pipeline from the operator session; component `resources.memory` can drop back to default). Expect `Succeeded`, then verify `SELECT count(*)` ≈ 5,000,000 and spot-check the 9 projected columns.
- [ ] Re-run `gbif-species-sk` and `worldbank-population` to confirm no regression (note: their raw tables were created with typed columns by v0.11.0 — FULL_LOAD onto them may hit the typed-table mismatch; if so, this is the documented caveat — recreate the tables or rename outputs, and record the outcome).

---

## Self-Review

**Spec coverage:**
- Streaming decode, 3 shapes + skip-non-objects + empty-ok + UseNumber (§1) → Task 1. ✓
- All-String schema; fields order / first-batch sorted union (§1) → Tasks 2–3. ✓
- 8192-row batches, self-contained IPC stream, one stream per Write/POST (§2) → Task 3 (+ invariant test). ✓
- Both paths wired; lazy writer; commit-empty preserved; exit codes (§1, Error handling) → Task 4. ✓
- Mock-DG (RPC set incl. gRPC WriteChunk fallback, no Shutdown needed; HTTP write endpoint; counts+validates; wire shapes verified) (§3) → Task 5. ✓
- `make extractor-local` + peak-RSS observation (§3) → Tasks 5, 7. ✓
- All-String behavior change + typed-table caveat + docs (§4) → Task 6. ✓
- e2e re-verify (Testing) → Task 7 Step 6 (CI, with audit + fix guidance). ✓
- Local rig 200k proof (Testing) → Task 7 Step 1. ✓
- 5M cluster proof (Testing/DoD) → Task 8 (post-release). ✓
- data-generator finding: closed as not-a-bug (#35) — nothing to do. ✓

**Placeholder scan:** no TBD/TODO; every code step carries complete code. The `--dump` flag from the spec was dropped (YAGNI — the mock's schema log + row counts cover the debugging need; noted here as a conscious cut). ✓

**Type consistency:** `decodeRecords(io.Reader, string, func(map[string]any) error) (int, error)`; `fetchStream(ctx, string, map[string]string) (io.ReadCloser, error)`; `stringifyValue(any) (string, bool)`; `columnPlan{names, extract}`; `newArrowSink(ctx, func() (ipcChunkWriter, error), []FieldMapping, int)`; `Add(map[string]any) error`; `Finish() (int64, *sdk.CloseResult, error)`; `Writer() ipcChunkWriter`; `sinkWriteError{Err}` — used identically in Tasks 1–5. `*sdk.Writer` satisfies `ipcChunkWriter` (`Write(ctx,[]byte) error` / `Close(ctx) (*sdk.CloseResult, error)` / `Bucket()` / `Table()` all exist, `sdk/go/client.go`). ✓

**Post-Codex-review revisions (round 1):**
- Blocker → Task 1 decoder rewritten to full token-walking: records are built
  key-by-key via `decodeObjectBody` (Token + Decode-per-value, UseNumber
  preserved), the positional inner array streams record-at-a-time after its
  `[` token, and skipped wrapper values are token-skipped via `skipBalanced` —
  no `json.RawMessage` buffering anywhere (`unmarshalRecord`/`firstByte`
  dropped; `bytes` import dropped).
- Major (exit codes) → Task 4 Step 1 classifies `sinkWriteError` in the
  paginated dispatch (`ExitAppError`); Step 2(c2) wraps the paginated
  `commitAndStatus` error as `sinkWriteError`.
- Major (tidy syntax) → Task 3 uses `make tidy` (repo rule), not
  `go mod tidy -C`.
- Major (mock validation) → the mock derives `expectCols` from the config's
  `fields[].name` and `validateSchema` enforces all-utf8 + exact names/order on
  every payload (violation → write fails visibly).
- Minor (error-text parity) → decoder returns `field '<k>' is not an array`
  for missing OR non-array `array_path`; `TestDecodeRecords_ErrorText` asserts
  exact strings.
- Minor (`io` import) → folded into the mock's import block.
- Nit → `extractor-local` added to `.PHONY`. ✓

**Post-Codex-review revisions (round 2):**
- Major (early-return token consumption) → new `drainToEOF` helper walks the
  document remainder to EOF (O(1) memory) on every success path: positional
  early-return, bare-array end (drain BEFORE delivering pending — parseJSON
  parity for sub-window bodies), and wrapped-object return. Malformed
  remainders (`{"results":[...],`, unclosed outer array, trailing garbage) now
  fail, matching parseJSON's whole-body strictness;
  `TestDecodeRecords_MalformedRemainder` covers all four cases. The inherent
  streaming caveat (large-body records reach fn before a corrupt tail is
  found; run still fails, Commit never runs) is documented on the helper.
- Minor (mock no-`fields` drift) → `writerState.schemaNames` locks the first
  payload's column names per writer; every later payload must match exactly
  (`slices.Equal`), independent of `expectCols`. ✓
