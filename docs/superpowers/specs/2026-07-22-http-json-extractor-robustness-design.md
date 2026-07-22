# http-json-extractor robustness + field mapping — design

- **Date:** 2026-07-22
- **Component:** `components/http-json-extractor/`
- **Status:** approved design, pre-implementation
- **Baseline:** `v0.10.2` (post PR #33, which fixed the paginated post-commit
  panic and added positional-array parsing)

## Problem

Two problems surfaced while running paginated extracts against real APIs:

1. **Concatenated-array write bug (correctness).** The extractor opens its
   writer with `FORMAT_JSON` and writes **one JSON array per page**
   (`writer.Write(json.Marshal(pageRecords))`). The SDK writer batches
   `Write()` calls into a buffer that only flushes to the gateway on a size
   threshold (default 1 MiB), on `Flush()`, or on `Close()`. The gateway's
   `JSONAdapter.Parse` (`pkg/datagateway/format/json.go`) does
   `json.Unmarshal(data, &[]map[string]any{})` — it requires each flushed
   batch to be **exactly one** JSON array. When several small pages coalesce
   into one batch, the gateway receives `[...][...][...]` and fails with
   `invalid character '[' after top-level value`.
   - Observed: GBIF (300 large records/page) crossed the 1 MiB threshold and
     flushed one array per POST, so it succeeded by luck. World Bank (100 small
     records/page) buffered three pages into one POST and failed.
   - This is batch-size-dependent and latent for **any** paginated JSON source
     with small records.

2. **Nested objects are unusable without SQL gymnastics (ergonomics).**
   Records are shipped to the gateway as raw JSON. The gateway serializes a
   nested object field (e.g. World Bank's `country: {id, value}`) into a single
   `VARCHAR` column holding JSON text. To get the country name a downstream
   `sql-transform` must call `json_extract_string(country, '$.value')`. There is
   no way to say "give me `country.value` as a column" at extract time.

Secondary issues found in the same file:

3. **Duplicated single-request vs paginated paths.** The two modes duplicate
   output-table resolution, writing, and commit logic. This duplication is what
   allowed the two paths to drift and produced the PR #33 panic (the paginated
   branch had no `return`, so it fell through into the single-request commit).

4. **Dead CSV code.** `getColumns`, `collectColumns`, `recordToCSV`,
   `getValue`, `formatValue`, `escapeCSV`, `inferJSONSchema`,
   `inferColumnTypeFromJSON`, and the `ColumnSchema` type are never reached —
   records go to the gateway as JSON, never CSV. They are confusing surface.

## Goals

- Make paginated JSON extraction correct regardless of page size or batch
  timing.
- Let a pipeline author project and rename fields (including nested ones) at
  extract time, producing clean flat columns.
- Reduce the file's surface and the risk of the two code paths diverging again.
- Preserve backward compatibility: existing pipelines that set no new config
  keep working and produce the same columns.

## Non-goals

- Type coercion of mapped fields (rely on the gateway's inference; revisit if a
  real source returns numbers-as-strings).
- Pass-through of unmapped fields when `fields` is set (projection is
  all-or-nothing by design; omit `fields` to emit everything).
- Array indexing in dot-paths (`items[0].x`) — object nesting only for now.
- Any change to the gateway, the SDK, or other components.

## Design

### 1. Write JSONL instead of a JSON array

Open the writer with `sdk.WithFormat(pb.DataFormat_FORMAT_JSONL)` in both
paths. Serialize records as newline-delimited JSON objects — one object per
line — via a shared `writeJSONL(ctx, writer, records)` helper.

JSONL is concatenation-safe: two flushed batches of JSONL are still valid
JSONL, so the gateway's streaming `JSONLAdapter` (already registered,
`bufio`-based) parses them correctly no matter how the SDK batches them. This
removes all dependence on flush timing and fixes problem (1) in both modes.

This mirrors the proven pattern in `components/data-generator/literal.go`,
which already writes with `FORMAT_JSONL`.

**Known limitation introduced by this switch (accepted).** JSONL is *not*
strictly more robust than the old `FORMAT_JSON` path. The gateway's JSONL
first-chunk / schema-inference path (`pkg/datagateway/format/json.go`, the
`s == nil` branch) uses `bufio.NewScanner` with the **default 64 KiB
line-token limit**; only the later known-schema fast path raises it to 16 MiB
(`scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)`). The old
`json.Unmarshal(data, &objects)` had no per-line limit. Consequence: a single
record whose JSON serialization exceeds 64 KiB, landing in the first flushed
chunk, will fail schema inference. This is acceptable for our sources (World
Bank, GBIF, and typical REST/JSON records are far under 64 KiB) and we take it
knowingly rather than change the gateway. It **must** be called out in
`docs/components.md` as a constraint of the component. (Fixing the gateway
slow-path buffer would remove the limit but was explicitly out of scope for
this PR.)

**Invariant the component must uphold.** Each `writer.Write()` payload must
contain only complete, newline-terminated JSON records. The SDK writer appends
a whole `Write()` payload to its batch buffer before deciding to flush
(`sdk/go/client.go`), so it never splits a payload mid-record across POSTs —
JSONL parsing per POST body is therefore safe *as long as* `writeJSONL` emits
whole records terminated by `\n`. This invariant is tested with a deliberately
tiny batch threshold (see Testing).

### 2. Field mapping (projection)

New optional config key `fields`, an array of `{path, name}` objects:

```yaml
config:
  url: "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json&date=2022&per_page=300"
  table_name: worldbank_population_2022
  fields:
    - path: country.value   # dot-path into nested objects
      name: entity          # output column name
    - path: countryiso3code
      name: iso3
    - path: value
      name: population
```

Semantics:

- **Projection:** when `fields` is set, each record is reshaped into a **new
  flat object** containing only the listed fields, keyed by `name`. Nothing
  else is emitted.
- **Dot-path resolution:** `path` is split on `.`; each segment indexes into a
  nested `map[string]any`. `country.value` reads `record["country"]["value"]`.
  Reuse the existing live helper `getValueRaw` (`main.go:664`), which already
  does dot-path traversal returning `any`.
- **Non-resolving paths yield `null` (lenient), covering every non-hit case
  explicitly:** the key is absent; an intermediate segment is a scalar, array,
  or `null` (not a `map[string]any`) so traversal cannot continue; or the final
  value is `null`. In all of these the projected field is JSON `null` for that
  record and the run does not fail. This keeps one sparse or oddly-shaped field
  from killing an otherwise-good extract.
- **Backward compatible:** when `fields` is omitted or empty, every field of
  each record is emitted verbatim (today's behavior).
- **Output-column-name rule:** each `name` must match
  `^[A-Za-z_][A-Za-z0-9_]{0,127}$` and be unique across `fields` (the same rule
  data-generator enforces on column names, `components/data-generator/config.go`).
  This rejects spaces, dots, empty/whitespace, and SQL-hostile names in
  author-controlled output columns. Note this rule applies only to *projected*
  `name`s; when `fields` is omitted, raw JSON keys pass through unchanged as
  today (we do not retroactively police source-defined columns).

Implemented as a pure helper `projectRecords(records, fields) []map[string]any`
applied in both modes immediately before `writeJSONL`.

### 3. De-duplicate the two paths

Extract shared helpers so single-request and paginated modes share one
implementation of the common steps:

- `resolveOutputTable(tableName, arrayPath) string` — the
  `table_name > array_path > "data"` precedence, defined once.
- `projectRecords(records, fields) []map[string]any` — §2.
- `writeJSONL(ctx, writer, records) error` — §1.

`main()` keeps the mode branch (`pagination.Type != ""`) but both arms call the
same helpers, and the paginated arm still `return`s (kept from PR #33). The
final commit + status-message block iterates `result.Buckets` (no `[0]`
indexing) and lives in one place.

### 4. Delete only the genuinely-dead CSV emitters

**Correction (from review):** most of the "CSV" code is NOT dead — it is used
by `DATUPLET_MODE=sample` (`runSampleMode` → `inferJSONSchema` → `getColumns` →
`collectColumns` → `inferColumnTypeFromJSON`; `SampleOutput` carries
`[]ColumnSchema`). Those all stay.

Reference-checked truly-dead set (safe to remove): `recordToCSV` (`main.go:462`,
never called) and its exclusive helpers `getValue` (`main.go:472`), `escapeCSV`,
and `formatValue` — all reachable only from `recordToCSV`. Verify zero
references again at implementation time before deleting, and drop only imports
that become unused as a result. **Do not** delete `getColumns`,
`collectColumns`, `inferJSONSchema`, `inferColumnTypeFromJSON`, `ColumnSchema`,
or `getValueRaw` (the latter is reused by §2 projection).

### 5. Config cleanup adopting data-generator patterns

- Replace the anonymous inline config struct in `main()` with a named `Config`
  type and a `ParseAndValidate(*Config) error` that runs before any network or
  writer work. Validation: `url` non-empty; each `fields[i]` has non-empty
  `path` and `name`; `name`s are unique.
- Log `sdk.BuildInfo().String()` as the first log line (rebuild diagnostics,
  matching data-generator).

### 6. Schema + docs

Add `fields` to `components/http-json-extractor/schema.json` within the Form
Subset:

```jsonc
"fields": {
  "type": "array",
  "description": "Optional projection: select and rename fields (including nested ones via dot-path). When omitted, all fields are emitted.",
  "x-datuplet-advanced": true,
  "items": {
    "type": "object",
    "additionalProperties": false,
    "required": ["path", "name"],
    "properties": {
      "path": { "type": "string", "description": "Dot-path to the source value, e.g. country.value for a nested object." },
      "name": { "type": "string", "description": "Output column name." }
    }
  }
}
```

No forbidden keywords (`oneOf`/`anyOf`/`allOf`/`not`/`$ref`/`$defs`/`if`/`then`/
`else`/`patternProperties`/`const`), every property described. Run
`make sync-component-schemas` to copy it into
`charts/datuplet-app/files/component-schemas/http-json-extractor.json` (CI
enforces no drift). Update the `http-json-extractor` section of
`docs/components.md` to document `fields` and note the JSONL write format.

### 7. Versioning

Backward-compatible feature addition → **minor bump to `v0.11.0`** via
`make bump-version VERSION=0.11.0`. Whether the bump rides in this PR or a
separate one is a release-mechanics decision confirmed at plan time; the tag is
cut by the maintainer.

## Data flow

```
fetchJSON(pageURL) --> parseJSON (bare | positional | object-wrapped)
    --> []map[string]any records
    --> projectRecords(records, fields)   [no-op when fields empty]
    --> writeJSONL(writer<FORMAT_JSONL>)   [one object per line]
        (paginated: once per page; single: once)
    --> client.Commit()  --> iterate result.Buckets for status
```

## Compatibility & schema inference (caveat from review)

"Omitting `fields` yields the same columns as today" holds for the data and
column set inferred from the objects, but is **not unconditional** across the
JSON→JSONL switch. The gateway infers a table's schema from the objects in the
**first flushed chunk** (`schema.InferSchemaFromJSONWithConfig`). Under
batching, the first chunk of a paginated extract may now contain *several
coalesced pages* rather than just page 1, so a column that only appears on
later pages could be inferred into the schema differently than in the old
large-page behavior (where each page tended to flush on its own). This is a
behavioral nuance, not a regression for our sources (World Bank / GBIF pages
are homogeneous), but the spec records it: schema is inferred from whatever
records are in the first flushed chunk, and that set depends on batch timing.
Projection (`fields`) makes the output schema deterministic regardless of
batching, which is the recommended path when a stable schema matters.

## Error handling

- Config invalid (empty `url`, malformed `fields`) → `ExitUserError` (exit 1)
  before any work.
- Fetch / HTTP / JSON-parse failure → `ExitUserError` (bad source/response).
- Writer open/write/close or commit failure → `ExitAppError` (>=20).
- Unresolved `fields[i].path` → `null` for that field; not an error.
- Zero records fetched → commit an empty result and emit a
  `"extracted 0 records"` status (existing behavior preserved).

## Testing

Unit tests in the component package (matches data-generator's convention;
committed, not throwaway):

- `parseJSON`: bare array, positional `[meta,[records]]`, object-wrapped with
  and without `array_path` (locks in the PR #33 behavior).
- `projectRecords`: top-level select+rename, nested dot-path, missing path →
  null, projection drops unlisted fields, empty `fields` → identity.
- `writeJSONL`: N records → N newline-delimited JSON objects, each a valid
  standalone object; concatenating two calls' output is still valid JSONL.
- **Batching invariant:** with a deliberately tiny batch threshold
  (`WithBatchSize`), a multi-page paginated write still parses — proving pages
  coalesced into one POST are valid JSONL and no record is split mid-line.
- `ParseAndValidate`: empty url rejected; `fields[i]` with empty `path`,
  empty/whitespace `name`, a `name` violating `^[A-Za-z_][A-Za-z0-9_]{0,127}$`,
  or duplicate `name`s all rejected.
- `projectRecords` non-object intermediate: `path` whose intermediate segment
  is a scalar/array/`null` → projected field is `null` (matches the dot-path
  semantics above).

End-to-end proof after release: rewrite the World Bank pipeline to use `fields`
(dropping the `json_extract_string` SQL) and confirm `Succeeded` with a clean
`entity, iso3, population` schema; re-run GBIF to confirm no regression.

## Definition of done

- [ ] Both paths write `FORMAT_JSONL`; the concatenated-array failure cannot
      recur.
- [ ] `fields` projection works (nested, rename, missing-null, projection);
      omitting it yields the same columns and data as today (the JSON→JSONL
      change is internal wire format; the gateway infers identical schemas from
      the same objects).
- [ ] Single-request and paginated paths share `resolveOutputTable`,
      `projectRecords`, `writeJSONL`; no `result.Buckets[0]` indexing.
- [ ] Only the confirmed-dead CSV emitters removed (`recordToCSV`, `getValue`,
      `escapeCSV`, `formatValue`); sample-mode code untouched; `DATUPLET_MODE=sample`
      still works.
- [ ] `schema.json` passes Form-Subset lint
      (`go test ./pkg/pipeline/schemalint/...`); `make sync-component-schemas`
      run, no CI drift.
- [ ] `docs/components.md` updated: `fields` documented, JSONL write format and
      the 64 KiB first-chunk record limit noted, and the hardcoded component
      tag / shared release-version text refreshed for `v0.11.0`.
- [ ] `go build ./...` + `go vet ./...` clean for the component module; unit
      tests pass; `make tidy` run (monorepo module parity — CI enforces it).
- [ ] `make bump-version VERSION=0.11.0` applied (updates all four charts,
      `charts/datuplet-app/values.yaml` `components.tag`, and `COMPONENT_TAG`);
      PR-vs-separate-bump decided with the maintainer.
- [ ] World Bank pipeline rewritten to use `fields` (no `json_extract_string`)
      and GBIF re-run — both proven green post-release.
