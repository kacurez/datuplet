# http-json-extractor Robustness + Field Mapping Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `http-json-extractor` write concatenation-safe JSONL (fixing the paginated small-page batch failure), add an optional `fields` projection for clean flat columns, and harden the component without breaking `DATUPLET_MODE=sample`.

**Architecture:** Keep the component single-file (`main.go`) per its existing convention. Both the single-request and paginated paths open the writer with `FORMAT_JSONL` and write records through shared helpers (`resolveOutputTable`, `projectRecords`, `encodeJSONL`/`writeJSONL`, `commitAndStatus`). Config parsing moves to a named `Config` + `ParseAndValidate`. Only the confirmed-dead CSV emitters are deleted; the sample-mode inference code stays.

**Tech Stack:** Go 1.25, DataGateway Go SDK (`sdk/go`), standard library (`encoding/json`, `bytes`, `regexp`).

**Spec:** `docs/superpowers/specs/2026-07-22-http-json-extractor-robustness-design.md`

## Global Constraints

- Component module is its own Go module: run all Go commands with
  `-C components/http-json-extractor` (or `cd` into it). Its module path is
  `github.com/datuplet/datuplet/components/http-json-extractor`.
- Package is `package main`; tests are `package main` (white-box) so they can
  call unexported helpers.
- **Never delete** `getColumns`, `collectColumns`, `inferJSONSchema`,
  `inferColumnTypeFromJSON`, `ColumnSchema`, `SampleOutput`, `getValueRaw`,
  `runSampleMode`, `parseJSON`, `recordsFromSlice` — they are live (sample mode
  and/or reused by projection).
- Output-column-name rule (projected `name`s): `^[A-Za-z_][A-Za-z0-9_]{0,127}$`,
  unique across `fields`.
- JSONL first-chunk records must stay < 64 KiB (documented gateway limit; not
  fixed here).
- `writeJSONL` must emit only whole, `\n`-terminated JSON records per SDK
  `Write()` call (the batch-split-safety invariant).
- Backward compatibility: with `fields` omitted, every field is emitted as
  today (same objects → same inferred schema, modulo first-chunk coalescing).
- Do not touch the gateway, SDK, CRD types, or controllers.
- Version bump is `v0.11.0` (minor; backward-compatible feature). Maintainer
  cuts the tag.

---

### Task 1: Regression tests for `parseJSON` (lock current behavior before refactor)

**Files:**
- Test: `components/http-json-extractor/parse_test.go` (create)

**Interfaces:**
- Consumes: `parseJSON(body []byte, arrayPath string) ([]map[string]any, error)` and `recordsFromSlice` (existing, `main.go:359` / `main.go:414`).
- Produces: nothing (pure regression net).

- [ ] **Step 1: Write the tests**

```go
package main

import "testing"

func TestParseJSON_Shapes(t *testing.T) {
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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recs, err := parseJSON([]byte(tc.body), tc.arrayPath)
			if err != nil {
				t.Fatalf("parseJSON error: %v", err)
			}
			if len(recs) != tc.wantLen {
				t.Fatalf("got %d records, want %d", len(recs), tc.wantLen)
			}
			if _, ok := recs[0][tc.wantKey]; !ok {
				t.Fatalf("first record missing key %q: %v", tc.wantKey, recs[0])
			}
		})
	}
}

func TestParseJSON_Invalid(t *testing.T) {
	if _, err := parseJSON([]byte(`{"no":"array here"}`), ""); err == nil {
		t.Fatal("expected error for object with no array field")
	}
}
```

- [ ] **Step 2: Run and verify PASS**

Run: `go test -C components/http-json-extractor -run TestParseJSON -v`
Expected: PASS (this locks in existing behavior).

- [ ] **Step 3: Commit**

```bash
git add components/http-json-extractor/parse_test.go
git commit -m "test(http-json-extractor): lock parseJSON shape behavior"
```

---

### Task 2: `Config` + `FieldMapping` + `ParseAndValidate`

**Files:**
- Modify: `components/http-json-extractor/main.go`
- Test: `components/http-json-extractor/config_test.go` (create)

**Interfaces:**
- Consumes: existing `PaginationConfig` (`main.go:23`).
- Produces:
  - `type FieldMapping struct { Path string `json:"path"`; Name string `json:"name"` }`
  - `type Config struct { URL string `json:"url"`; ArrayPath string `json:"array_path"`; TableName string `json:"table_name"`; Headers map[string]string `json:"headers"`; Pagination *PaginationConfig `json:"pagination"`; Fields []FieldMapping `json:"fields"` }`
  - `func ParseAndValidate(cfg *Config) error`

- [ ] **Step 1: Write the failing test**

```go
package main

import "testing"

func TestParseAndValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"ok_no_fields", Config{URL: "https://x"}, false},
		{"ok_fields", Config{URL: "https://x", Fields: []FieldMapping{{Path: "a.b", Name: "ab"}, {Path: "c", Name: "c_1"}}}, false},
		{"empty_url", Config{}, true},
		{"empty_path", Config{URL: "https://x", Fields: []FieldMapping{{Path: "", Name: "n"}}}, true},
		{"bad_name_dot", Config{URL: "https://x", Fields: []FieldMapping{{Path: "a", Name: "a.b"}}}, true},
		{"bad_name_space", Config{URL: "https://x", Fields: []FieldMapping{{Path: "a", Name: "a b"}}}, true},
		{"bad_name_empty", Config{URL: "https://x", Fields: []FieldMapping{{Path: "a", Name: ""}}}, true},
		{"dup_name", Config{URL: "https://x", Fields: []FieldMapping{{Path: "a", Name: "n"}, {Path: "b", Name: "n"}}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ParseAndValidate(&tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseAndValidate() err=%v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -C components/http-json-extractor -run TestParseAndValidate -v`
Expected: FAIL — `undefined: Config` / `undefined: ParseAndValidate`.

- [ ] **Step 3: Add types + validator to `main.go`**

Add `"regexp"` to the import block. Add these declarations just below the
`PaginationConfig` struct (after `main.go:33`):

```go
// FieldMapping selects a source value (by dot-path) and renames it to an
// output column. Used by the optional `fields` projection.
type FieldMapping struct {
	Path string `json:"path"`
	Name string `json:"name"`
}

// Config is the http-json-extractor component config.
type Config struct {
	URL        string            `json:"url"`
	ArrayPath  string            `json:"array_path"`
	TableName  string            `json:"table_name"`
	Headers    map[string]string `json:"headers"`
	Pagination *PaginationConfig `json:"pagination"`
	Fields     []FieldMapping    `json:"fields"`
}

// columnNameRe validates author-controlled projected output-column names.
var columnNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

// ParseAndValidate checks the config before any network or writer work.
func ParseAndValidate(cfg *Config) error {
	if cfg.URL == "" {
		return fmt.Errorf("config.url is required")
	}
	seen := make(map[string]bool, len(cfg.Fields))
	for i, f := range cfg.Fields {
		if f.Path == "" {
			return fmt.Errorf("fields[%d].path is required", i)
		}
		if !columnNameRe.MatchString(f.Name) {
			return fmt.Errorf("fields[%d].name %q must match ^[A-Za-z_][A-Za-z0-9_]{0,127}$", i, f.Name)
		}
		if seen[f.Name] {
			return fmt.Errorf("fields[%d].name %q is duplicated", i, f.Name)
		}
		seen[f.Name] = true
	}
	return nil
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test -C components/http-json-extractor -run TestParseAndValidate -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add components/http-json-extractor/main.go components/http-json-extractor/config_test.go
git commit -m "feat(http-json-extractor): Config type + ParseAndValidate with field mapping"
```

---

### Task 3: `encodeJSONL` / `writeJSONL` + `resolveOutputTable`

**Files:**
- Modify: `components/http-json-extractor/main.go`
- Test: `components/http-json-extractor/transform_test.go` (create)

**Interfaces:**
- Consumes: `sdk.Writer` (`w.Write(ctx, []byte) error`).
- Produces:
  - `func encodeJSONL(records []map[string]any) ([]byte, error)`
  - `func writeJSONL(ctx context.Context, w *sdk.Writer, records []map[string]any) error`
  - `func resolveOutputTable(tableName, arrayPath string) string`

- [ ] **Step 1: Write the failing test**

```go
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"testing"
)

func TestResolveOutputTable(t *testing.T) {
	cases := []struct{ table, array, want string }{
		{"t", "a", "t"},
		{"", "a", "a"},
		{"", "", "data"},
	}
	for _, c := range cases {
		if got := resolveOutputTable(c.table, c.array); got != c.want {
			t.Fatalf("resolveOutputTable(%q,%q)=%q want %q", c.table, c.array, got, c.want)
		}
	}
}

func TestEncodeJSONL_LinesAndConcatSafety(t *testing.T) {
	a := []map[string]any{{"id": 1}, {"id": 2}}
	b := []map[string]any{{"id": 3}}

	ba, err := encodeJSONL(a)
	if err != nil {
		t.Fatal(err)
	}
	bb, err := encodeJSONL(b)
	if err != nil {
		t.Fatal(err)
	}

	// Concatenating two encodeJSONL outputs (simulating coalesced pages in one
	// gateway POST) must still be valid, line-by-line JSONL: 3 parseable lines.
	joined := append(append([]byte{}, ba...), bb...)
	sc := bufio.NewScanner(bytes.NewReader(joined))
	n := 0
	for sc.Scan() {
		var obj map[string]any
		if err := json.Unmarshal(sc.Bytes(), &obj); err != nil {
			t.Fatalf("line %d not valid JSON: %v", n, err)
		}
		n++
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("got %d JSONL lines, want 3", n)
	}
}

func TestEncodeJSONL_NoHTMLEscape(t *testing.T) {
	out, err := encodeJSONL([]map[string]any{{"u": "a&b<c>"}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out, []byte("a&b<c>")) {
		t.Fatalf("value was HTML-escaped: %s", out)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -C components/http-json-extractor -run 'TestResolveOutputTable|TestEncodeJSONL' -v`
Expected: FAIL — `undefined: resolveOutputTable` / `undefined: encodeJSONL`.

- [ ] **Step 3: Add helpers to `main.go`**

Add `"bytes"` to the import block. Add these functions (near `recordsFromSlice`):

```go
// resolveOutputTable applies the table_name > array_path > "data" precedence.
func resolveOutputTable(tableName, arrayPath string) string {
	switch {
	case tableName != "":
		return tableName
	case arrayPath != "":
		return arrayPath
	default:
		return "data"
	}
}

// encodeJSONL serializes records as newline-delimited JSON (one object per
// line). Concatenation-safe: appending two encodeJSONL outputs is still valid
// JSONL, so coalesced gateway batches parse correctly. HTML escaping is off so
// values like URLs with & are preserved verbatim.
func encodeJSONL(records []map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for _, rec := range records {
		if err := enc.Encode(rec); err != nil { // Encode appends a trailing '\n'
			return nil, fmt.Errorf("marshal record: %w", err)
		}
	}
	return buf.Bytes(), nil
}

// writeJSONL writes records to the gateway as one JSONL blob. Each Write()
// payload contains only whole, newline-terminated records (the SDK never
// splits a payload mid-record), keeping per-POST JSONL parsing safe.
func writeJSONL(ctx context.Context, w *sdk.Writer, records []map[string]any) error {
	if len(records) == 0 {
		return nil
	}
	data, err := encodeJSONL(records)
	if err != nil {
		return err
	}
	return w.Write(ctx, data)
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test -C components/http-json-extractor -run 'TestResolveOutputTable|TestEncodeJSONL' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add components/http-json-extractor/main.go components/http-json-extractor/transform_test.go
git commit -m "feat(http-json-extractor): JSONL encode/write + resolveOutputTable helpers"
```

---

### Task 4: `projectRecords`

**Files:**
- Modify: `components/http-json-extractor/main.go`
- Test: `components/http-json-extractor/transform_test.go` (append)

**Interfaces:**
- Consumes: `getValueRaw(record map[string]any, path string) any` (existing, `main.go:664`); `FieldMapping` (Task 2).
- Produces: `func projectRecords(records []map[string]any, fields []FieldMapping) []map[string]any`

- [ ] **Step 1: Write the failing test (append to transform_test.go)**

```go
func TestProjectRecords(t *testing.T) {
	recs := []map[string]any{
		{"country": map[string]any{"id": "ZH", "value": "Africa"}, "iso": "AFE", "value": 5.0},
		{"country": "not-an-object", "iso": "XXX"}, // intermediate not object, missing value
	}
	fields := []FieldMapping{
		{Path: "country.value", Name: "entity"},
		{Path: "iso", Name: "iso3"},
		{Path: "value", Name: "population"},
	}
	out := projectRecords(recs, fields)
	if len(out) != 2 {
		t.Fatalf("got %d rows, want 2", len(out))
	}
	// row 0: all resolved
	if out[0]["entity"] != "Africa" || out[0]["iso3"] != "AFE" || out[0]["population"] != 5.0 {
		t.Fatalf("row0 wrong: %v", out[0])
	}
	// only projected keys present
	if len(out[0]) != 3 {
		t.Fatalf("row0 should have exactly 3 keys, got %v", out[0])
	}
	// row 1: non-object intermediate -> nil; missing -> nil
	if out[1]["entity"] != nil || out[1]["population"] != nil || out[1]["iso3"] != "XXX" {
		t.Fatalf("row1 wrong: %v", out[1])
	}
}

func TestProjectRecords_Identity(t *testing.T) {
	recs := []map[string]any{{"a": 1, "b": 2}}
	// nil / empty fields -> unchanged slice
	if got := projectRecords(recs, nil); len(got) != 1 || len(got[0]) != 2 {
		t.Fatalf("identity failed: %v", got)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -C components/http-json-extractor -run TestProjectRecords -v`
Expected: FAIL — `undefined: projectRecords`.

- [ ] **Step 3: Add `projectRecords` to `main.go`**

```go
// projectRecords reshapes each record into a flat object containing only the
// mapped fields (renamed). An unresolved path (missing key, or an intermediate
// segment that is a scalar/array/null) yields nil for that field. With no
// fields, records are returned unchanged.
func projectRecords(records []map[string]any, fields []FieldMapping) []map[string]any {
	if len(fields) == 0 {
		return records
	}
	out := make([]map[string]any, len(records))
	for i, rec := range records {
		projected := make(map[string]any, len(fields))
		for _, f := range fields {
			projected[f.Name] = getValueRaw(rec, f.Path)
		}
		out[i] = projected
	}
	return out
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test -C components/http-json-extractor -run TestProjectRecords -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add components/http-json-extractor/main.go components/http-json-extractor/transform_test.go
git commit -m "feat(http-json-extractor): projectRecords dot-path projection"
```

---

### Task 5: Wire helpers into `main()` — JSONL format, both paths, dedup

**Files:**
- Modify: `components/http-json-extractor/main.go` (`main()` and `runPaginatedExtraction`)

**Interfaces:**
- Consumes: everything from Tasks 2–4 + `fetchJSON`, `sdk.BuildInfo`, `sdk.Client`.
- Produces: `func commitAndStatus(ctx context.Context, client *sdk.Client, sourceURL string) error`

- [ ] **Step 1: Add `commitAndStatus` helper to `main.go`**

```go
// commitAndStatus commits all outputs, logs per-table results, and emits the
// status message. Iterates result.Buckets (no [0] indexing).
func commitAndStatus(ctx context.Context, client *sdk.Client, sourceURL string) error {
	result, err := client.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}
	if !result.Success {
		return fmt.Errorf("commit returned failure")
	}
	var rows int64
	for _, b := range result.Buckets {
		for _, t := range b.Tables {
			client.Log(ctx, "INFO", fmt.Sprintf("Committed %s.%s: files=%d, rows=%d", t.Bucket, t.Table, t.FilesAdded, t.RowsAdded))
			rows += t.RowsAdded
		}
	}
	sdk.StatusMessage(fmt.Sprintf("extracted %d records from %s", rows, sourceURL))
	return nil
}
```

- [ ] **Step 2: Replace the body of `main()` from `// Get config` down**

**Naming note (from review):** `main()` already declares `cfg := client.Config()`
(the SDK exec config, used for `cfg.ExecutionID`) at `main.go:55`. Do NOT reuse
`cfg` for the component config — that redeclares it and won't compile. The
component config is named **`compCfg`** (matching data-generator).

Replace the current block from `// Get config` (`main.go:54`) through the end of
`main()` (the closing `}` at `main.go:158`) with the following. This makes
`sdk.BuildInfo()` the first log line, keeps `cfg := client.Config()` for the
existing "started" log, and uses `compCfg` for the component config:

```go
	// Log SDK build info first (rebuild diagnostics), then the started line.
	client.Log(ctx, "INFO", sdk.BuildInfo().String())

	cfg := client.Config()
	client.Log(ctx, "INFO", fmt.Sprintf("HTTP JSON Extractor started: execution=%s", cfg.ExecutionID))

	// Parse + validate component config before any network or writer work.
	var compCfg Config
	if err := client.ParseConfig(&compCfg); err != nil {
		sdk.ExitUserError(fmt.Sprintf("failed to parse config: %v", err))
	}
	if err := ParseAndValidate(&compCfg); err != nil {
		sdk.ExitUserError(err.Error())
	}

	outputTable := resolveOutputTable(compCfg.TableName, compCfg.ArrayPath)

	// Paginated mode - stream data incrementally, page by page.
	if compCfg.Pagination != nil && compCfg.Pagination.Type != "" {
		if err := runPaginatedExtraction(ctx, client, &compCfg, outputTable); err != nil {
			sdk.ExitUserError(fmt.Sprintf("paginated extraction failed: %v", err))
		}
		return
	}

	// Single-request mode.
	client.Log(ctx, "INFO", fmt.Sprintf("Fetching JSON from: %s", compCfg.URL))
	records, err := fetchJSON(ctx, compCfg.URL, compCfg.ArrayPath, compCfg.Headers)
	if err != nil {
		sdk.ExitUserError(fmt.Sprintf("failed to fetch JSON: %v", err))
	}
	client.Log(ctx, "INFO", fmt.Sprintf("Fetched %d records", len(records)))

	if len(records) == 0 {
		client.Log(ctx, "WARN", "No records found")
		if _, err := client.Commit(ctx); err != nil {
			sdk.ExitAppError(fmt.Sprintf("commit failed: %v", err))
		}
		sdk.StatusMessage("extracted 0 records (empty response)")
		return
	}

	records = projectRecords(records, compCfg.Fields)

	writer, err := client.OpenWriter(ctx, outputTable, sdk.WithFormat(pb.DataFormat_FORMAT_JSONL))
	if err != nil {
		sdk.ExitAppError(fmt.Sprintf("failed to open writer: %v", err))
	}
	if err := writeJSONL(ctx, writer, records); err != nil {
		sdk.ExitAppError(fmt.Sprintf("failed to write JSONL: %v", err))
	}
	closeResult, err := writer.Close(ctx)
	if err != nil {
		sdk.ExitAppError(fmt.Sprintf("failed to close writer: %v", err))
	}
	client.Log(ctx, "INFO", fmt.Sprintf("Completed output %s.%s: %d rows", writer.Bucket(), writer.Table(), closeResult.TotalRows))

	if err := commitAndStatus(ctx, client, compCfg.URL); err != nil {
		sdk.ExitAppError(err.Error())
	}
	client.Log(ctx, "INFO", "HTTP JSON extraction completed successfully")
}
```

Note: the removed `cfg.DefaultBucket` log line (old `main.go:108`) is
intentionally dropped — the writer resolves the default bucket internally; it
was only ever logged.

- [ ] **Step 3: Change `runPaginatedExtraction` signature + body to use `cfg`, JSONL, projection, and `commitAndStatus`**

Replace the function signature (`main.go:161`):

```go
func runPaginatedExtraction(ctx context.Context, client *sdk.Client, cfg *Config, outputTable string) error {
	pagination := cfg.Pagination
```

Then inside the function: (a) delete the old local `outputTable` resolution
block (the `outputTable := "data" ... else if arrayPath != ""` lines, formerly
`main.go:184-190`) since it is now passed in; (b) change `OpenWriter` to JSONL:

```go
	writer, err := client.OpenWriter(ctx, outputTable, sdk.WithFormat(pb.DataFormat_FORMAT_JSONL))
```

(c) replace the per-page write block (formerly `main.go:242-251`) with a
project-then-JSONL write:

```go
		if len(recordsToWrite) > 0 {
			if err := writeJSONL(ctx, writer, projectRecords(recordsToWrite, cfg.Fields)); err != nil {
				return fmt.Errorf("failed to write: %w", err)
			}
		}
```

(d) replace the tail commit block (formerly `main.go:281-297`, the
`client.Commit` + `result.Success` + bucket-iteration) with:

```go
	if err := commitAndStatus(ctx, client, cfg.URL); err != nil {
		return err
	}
	client.Log(ctx, "INFO", fmt.Sprintf("Paginated extraction completed: %d pages, %d total records", pageCount, totalRecords))
	return nil
```

Update the internal references that used the old params: `pagination` is now
`cfg.Pagination` (aliased on the first line), `arrayPath` in the `fetchJSON`
call becomes `cfg.ArrayPath`, and `headers` becomes `cfg.Headers`. The
`fetchJSON` line becomes:

```go
		records, err := fetchJSON(ctx, pageURL, cfg.ArrayPath, cfg.Headers)
```

- [ ] **Step 4: Build + vet**

Run: `go build -C components/http-json-extractor ./... && go vet -C components/http-json-extractor ./...`
Expected: no output (clean). If `strconv`/`sort`/`strings` are reported unused, that means a later task's deletion was pulled forward — they must still be used here (buildPaginatedURL/getValueRaw/sample); do not remove them in this task.

- [ ] **Step 5: Run full package tests**

Run: `go test -C components/http-json-extractor ./...`
Expected: PASS (Tasks 1–4 tests still green).

- [ ] **Step 6: Commit**

```bash
git add components/http-json-extractor/main.go
git commit -m "feat(http-json-extractor): write JSONL in both paths, apply projection, dedup commit"
```

---

### Task 6: Delete the confirmed-dead CSV emitters

**Files:**
- Modify: `components/http-json-extractor/main.go`

**Interfaces:**
- Removes: `recordToCSV` (`main.go:462`), `getValue` (`main.go:472`), `formatValue` (`main.go:488`), `escapeCSV` (`main.go:512`). Nothing consumes these after Task 5.

- [ ] **Step 1: Verify no *live* path references them**

These four form a closed cluster: `recordToCSV` calls `getValue` + `escapeCSV`;
`getValue` calls `formatValue`. So raw `grep -c` counts are >1 (doc comment +
def + mutual calls) — that is expected, not a red flag. What matters is that
**nothing outside the cluster** calls them. Inspect every call site:

```bash
cd components/http-json-extractor
grep -n 'recordToCSV\|escapeCSV\|formatValue\|[^R]getValue(' main.go
```
Expected: the only references are (a) the four `func`/comment lines and (b)
`getValue`/`escapeCSV` called inside `recordToCSV`, and `formatValue` called
inside `getValue`. Confirm NO reference appears inside `main`,
`runPaginatedExtraction`, `runSampleMode`, `inferJSONSchema`,
`inferColumnTypeFromJSON`, or any other retained function. (`getValueRaw` is a
*different* function and stays — the `[^R]getValue(` pattern excludes it.) The
authoritative safety net is Step 3: after deleting all four together,
`go build` fails if any live reference remained.

- [ ] **Step 2: Delete the four functions**

Remove the `recordToCSV`, `getValue`, `formatValue`, and `escapeCSV` function
definitions (and their doc comments) from `main.go`. Leave `getValueRaw`,
`getColumns`, `collectColumns`, `inferJSONSchema`, `inferColumnTypeFromJSON`,
`ColumnSchema`, `SampleOutput`, and `runSampleMode` untouched.

- [ ] **Step 3: Build + vet (catches now-unused imports)**

Run: `go build -C components/http-json-extractor ./... && go vet -C components/http-json-extractor ./...`
Expected: clean. `strings` is still used (`buildPaginatedURL`, `getValueRaw`),
`sort` by `getColumns`, `strconv` by `buildPaginatedURL`/`runSampleMode`, `json`
throughout — so no import removal is expected. If the compiler reports an
unused import, remove exactly that import.

- [ ] **Step 4: Confirm sample mode still compiles/works**

Run: `go test -C components/http-json-extractor ./...`
Expected: PASS. (Sample mode itself is exercised post-release; the build proves
`inferJSONSchema`→`getColumns`→`collectColumns` still resolve.)

- [ ] **Step 5: Commit**

```bash
git add components/http-json-extractor/main.go
git commit -m "refactor(http-json-extractor): remove dead CSV emitter functions"
```

---

### Task 7: Add `fields` to `schema.json` + sync + lint

**Files:**
- Modify: `components/http-json-extractor/schema.json`
- Modify (generated): `charts/datuplet-app/files/component-schemas/http-json-extractor.json`

**Interfaces:** none (config contract only).

- [ ] **Step 1: Add the `fields` property to `schema.json`**

Inside the top-level `"properties"` object of
`components/http-json-extractor/schema.json`, add:

```json
"fields": {
  "type": "array",
  "description": "Optional projection: select and rename fields (including nested ones via dot-path). When omitted, all fields are emitted.",
  "x-datuplet-advanced": true,
  "items": {
    "type": "object",
    "additionalProperties": false,
    "required": ["path", "name"],
    "properties": {
      "path": {
        "type": "string",
        "description": "Dot-path to the source value, e.g. country.value for a nested object. Unresolved paths yield null."
      },
      "name": {
        "type": "string",
        "description": "Output column name. Must match ^[A-Za-z_][A-Za-z0-9_]{0,127}$ and be unique."
      }
    }
  }
}
```

- [ ] **Step 2: Lint the schema (Form Subset)**

Run: `go test ./pkg/pipeline/schemalint/...`
Expected: PASS. (If it fails, re-read the finding — the linter recurses into
`items`; `x-datuplet-advanced` is a known annotation; every property has a
`description`.)

- [ ] **Step 3: Sync into the chart**

Run: `make sync-component-schemas`
Then confirm no drift: `git status --short charts/datuplet-app/files/component-schemas/http-json-extractor.json` should show it modified.

- [ ] **Step 4: Commit**

```bash
git add components/http-json-extractor/schema.json charts/datuplet-app/files/component-schemas/http-json-extractor.json
git commit -m "feat(http-json-extractor): add fields projection to config schema"
```

---

### Task 8: Update `docs/components.md`

**Files:**
- Modify: `docs/components.md`

**Interfaces:** none.

- [ ] **Step 1: Update the http-json-extractor section**

In the `http-json-extractor` section of `docs/components.md`, add documentation
that:
- describes the new `fields` config (array of `{path, name}`; dot-path into
  nested objects; projection = only listed fields emitted, renamed; omit to
  emit everything), with a short YAML example;
- notes the component writes rows as **JSONL** to the gateway;
- states the constraint: **a single record's JSON must be < 64 KiB** (gateway
  JSONL first-chunk schema-inference uses a 64 KiB line limit).

- [ ] **Step 2: Refresh the hardcoded component tag for the release**

Note (from review): `docs/components.md` currently says **`v0.9.1`** everywhere
(line ~19 shared "release version", line ~175 the http-json-extractor image) —
the v0.10.2 release never updated it, so it is already stale. Find the real
occurrences:
```bash
grep -n "v0.9.1" docs/components.md
```
For THIS PR, update to `v0.11.0`:
- the **http-json-extractor** image line (`ghcr.io/kacurez/http-json-extractor:v0.9.1` → `:v0.11.0`);
- the shared release-version line (~line 19, "…the release version; `v0.9.1` in this release").

Leave the other components' image tags (`data-generator`, `finnhub-extractor`,
`sql-transform`, `pandas-transform`, `stdout-writer`) as-is: they are stale
independently of this change (pre-existing drift from the v0.10.2 release). Do
NOT bulk-rewrite them here — flag that drift in the PR description as a
maintainer follow-up rather than expanding this PR's scope.

- [ ] **Step 3: Commit**

```bash
git add docs/components.md
git commit -m "docs(components): document http-json-extractor fields + JSONL limit"
```

---

### Task 9: Monorepo tidy + version bump to v0.11.0

**Files:**
- Modify: `charts/*/Chart.yaml`, `charts/datuplet-app/values.yaml`, `Makefile` (via `make bump-version`); possibly `go.sum`/`go.mod` across modules (via `make tidy`).

**Interfaces:** none.

- [ ] **Step 1: Tidy all modules**

Run: `make tidy`
Expected: completes; `git status` shows only expected `go.mod`/`go.sum` churn (often none).

- [ ] **Step 2: Bump version**

Run: `make bump-version VERSION=0.11.0`
Expected output lists all four charts at `version: 0.11.0` / `appVersion: "0.11.0"`,
`charts/datuplet-app/values.yaml` `tag: v0.11.0`, and `COMPONENT_TAG ?= v0.11.0`.

- [ ] **Step 3: Sanity build/vet/test once more**

Run: `go build -C components/http-json-extractor ./... && go vet -C components/http-json-extractor ./... && go test -C components/http-json-extractor ./...`
Expected: clean + PASS.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore(release): bump version to 0.11.0"
```

- [ ] **Step 5: Push branch + open draft PR**

```bash
git push -u public feat/http-json-extractor-robustness
gh pr create --draft --repo kacurez/datuplet --base main \
  --title "feat(http-json-extractor): JSONL writes + field mapping — release v0.11.0" \
  --body "See docs/superpowers/specs/2026-07-22-http-json-extractor-robustness-design.md. Fixes the paginated small-page concatenated-array failure by writing JSONL, adds an optional \`fields\` projection, hardens config + dedups the two paths, and removes dead CSV emitters. Codex-reviewed spec + plan."
```

Note: whether the bump rides in this PR (Task 9) or is split into a separate
`bump-0-11-0` PR is a maintainer decision — confirm before this step. The
maintainer cuts the `v0.11.0` tag.

---

### Task 10 (post-release verification — not a code change)

**Files:** none (operator workflow, run after the maintainer publishes v0.11.0).

- [ ] **Step 1: Rewrite the World Bank pipeline to use `fields`** (drops the `json_extract_string` SQL):

```yaml
      - name: worldbank-extractor
        component: http-json-extractor
        config:
          url: "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json&date=2022&per_page=300"
          table_name: worldbank_population_2022
          fields:
            - path: country.value
              name: entity
            - path: countryiso3code
              name: iso3
            - path: value
              name: population
```

- [ ] **Step 2: Validate → put → trigger → verify** (per datuplet-operator loop); expect `Succeeded` with a clean `entity, iso3, population` schema.

- [ ] **Step 3: Re-run the GBIF pipeline** — confirm no regression (still `Succeeded`, 1500 rows).

---

## Self-Review

**Spec coverage:**
- JSONL switch (§1) → Task 3 (helpers) + Task 5 (both paths). ✓
- 64 KiB limit documented (§1) → Task 8. ✓
- Write-payload invariant (§1) → Task 3 concat-safety test. ✓
- Field mapping projection (§2) → Task 2 (config), Task 4 (projectRecords), Task 7 (schema). ✓
- Name-validation rule (§2) → Task 2. ✓
- Dot-path non-object → null (§2) → Task 4 test. ✓
- De-dup the two paths (§3) → Task 5 (shared `resolveOutputTable`/`projectRecords`/`writeJSONL`/`commitAndStatus`, no `[0]` index). ✓
- Delete dead CSV, keep sample mode (§4) → Task 6 (narrowed set) + Global Constraints. ✓
- Config + ParseAndValidate + BuildInfo (§5) → Task 2 + Task 5. ✓
- Schema + sync + docs (§6) → Task 7, Task 8. ✓
- Compatibility caveat (schema from first chunk) → documented in spec; no code needed. ✓
- Versioning v0.11.0 (§7) → Task 9. ✓
- Testing (parseJSON, projectRecords, writeJSONL, ParseAndValidate, invariant) → Tasks 1–4. ✓
- E2E proof → Task 10. ✓

**Placeholder scan:** No TBD/TODO; every code step has complete code. ✓

**Type consistency:** `Config`, `FieldMapping`, `ParseAndValidate`, `resolveOutputTable`, `encodeJSONL`, `writeJSONL`, `projectRecords`, `commitAndStatus`, and the `runPaginatedExtraction(ctx, client, *Config, outputTable)` signature are used identically across Tasks 2–9. `getValueRaw` reused by `projectRecords` matches its existing signature. The component config is named `compCfg` in `main()` to avoid colliding with the existing `cfg := client.Config()` (SDK exec config); `runPaginatedExtraction`'s parameter is `cfg *Config` (separate scope, no collision). ✓

**Post-Codex-review revisions:** Blocker (cfg redeclaration) → Task 5 renames to `compCfg` and reorders BuildInfo first. Major (docs tag) → Task 8 targets the real `v0.9.1` strings. Major (dead-code ref check) → Task 6 checks live callers, not raw counts, with `go build` as the net. ✓
