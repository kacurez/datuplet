# Component schemas

RFC 027 made each built-in component's config shape a first-class, versioned
artifact instead of a shape hand-copied into the Helm chart. This page
documents the authoring rules; for the config shape of a specific component,
read its `schema.json` directly (pointers below) or fetch it live:

```bash
datuplet components get <name> --schema
```

Datuplet ships six built-in components — five with published images, plus
`pandas-transform`, which exists as a ComponentDefinition template but has no
published image yet (see its section below). Each runs as an ordinary
container alongside the Data Gateway sidecar; the component communicates with
the sidecar via gRPC and HTTP — it never touches S3 directly.

Image registry: `ghcr.io/kacurez/<name>:<components.tag>` — the tag tracks the
chart's `components.tag` (the release version; `v0.12.0` in this release).

---

## Source of truth + sync

- `components/<name>/schema.json` — JSON Schema draft 2020-12, one per
  component, next to the code that parses the config. Schema and parser are
  authored and updated together, in the same commit — a plain file works
  identically for Go components and for `pandas-transform` (Python).
- `make sync-component-schemas` copies each into
  `charts/datuplet-app/files/component-schemas/<name>.json`; the
  ComponentDefinition templates read them via `.Files.Get` rather than
  embedding an inline blob (Helm can only read files inside the chart
  directory, hence the synced copy). CI runs the sync target and
  `git diff --exit-code charts/` to catch drift.

## The Form Subset (normative)

The browser UI's pipeline builder renders component config as a **form**,
recursively, at any depth — no separate "advanced mode" needed for a
schema that stays within this subset:

| Construct | Control |
|---|---|
| `object` + `properties`/`required` | field group (nested groups collapsible) |
| `array` of objects | repeater cards with add/remove |
| `array` of scalars | list editor |
| `additionalProperties: {type: string [enum: …]}` | key→value map rows (e.g. data-generator's `random.schema: column→type`) |
| scalar `string`/`integer`/`number` | input (numeric where typed) |
| `boolean` | checkbox; tri-state select when `default` is present (unset / true / false) |
| `enum` | select |
| `x-datuplet-secret: true` | secret picker (`$[key]` reference, RFC 026 semantics unchanged) |
| `x-datuplet-multiline: "<lang>"` | code textarea (e.g. `"sql"`) |
| `x-datuplet-advanced: true` | property grouped into one collapsed **Advanced** section |
| `x-datuplet-produces` (schema root only) | dynamic-output table-name resolution for the inputs picker |
| `title`, `description`, `default`, `examples` | display metadata (below) |
| `minimum`/`maximum`/`minLength`/`minItems`/`pattern` | advisory client-side checks; server + component stay authoritative |

**Out of subset:** `oneOf`, `anyOf`, `allOf`, `not`, `$ref`, `$defs`, `if`,
`then`, `else`, `patternProperties`, `const`. A node using one of these
renders as a JSON sub-editor for that node only — never a whole-form
fallback. This fallback path exists for **third-party / operator-registered**
component schemas only; built-ins must lint clean and never exercise it.

### Schema lint

`pkg/pipeline/schemalint` walks every `components/*/schema.json` (enforced in
CI via `pkg/pipeline/schemalint/schemalint_test.go`) and fails on:

1. **Invalid or out-of-subset schema** — every node must be valid JSON with a
   `type`, and must not use any of the forbidden keywords above.
2. **Missing description** — every property must have a non-empty
   `description`.
3. **`required` + `default` contradiction** — a property cannot be both
   required and carry a `default` (see below for why).
4. **Unknown or misplaced annotation** — every `x-datuplet-*` key must be one
   of the five below, and `x-datuplet-produces` is only allowed at the schema
   root.

### The five `x-datuplet-*` annotations

| Annotation | Effect |
|---|---|
| `x-datuplet-secret: true` | Property renders as a `$[key]` secret picker instead of a plain text input. A plaintext secret can never be entered through the form. |
| `x-datuplet-multiline: "<lang>"` | Property renders as a code textarea (e.g. `"sql"` for `sql-transform`'s `sql` field). |
| `x-datuplet-advanced: true` | Property is grouped into one collapsed **Advanced** section instead of appearing inline. |
| `x-datuplet-doc` | Optional long-form help, shown on hover. Ships to agents alongside `description` via `datuplet components get --schema`, since both live in the schema itself. |
| `x-datuplet-produces` | **Schema root only.** A dot/wildcard path into the component's own config that yields its runtime output table names (e.g. data-generator's `"tables[*].name"`). Lets the pipeline builder's inputs picker resolve "produced upstream" tables automatically. UI-only sugar — the server never resolves it; a dynamic-bucket output whose names can't be resolved this way shows as `<bucket> (dynamic)` and isn't selectable. |

### Descriptions and defaults

- `description` is the always-visible one-line hint and is lint-mandatory.
- `default` is **display metadata only** — rendered as placeholder text or an
  empty-option label (e.g. `— default (FULL_LOAD) —`). An untouched field
  stores nothing in the saved doc; the component applies its own default at
  runtime. This is also why `required` + `default` on the same property is a
  lint error: a required field can never be left unfilled, so a display-only
  default on it is a contradiction. Dynamic defaults ("all CPUs granted to
  the container") belong in `description` or `x-datuplet-doc`, never in
  `default`.

## IO capability

Each component definition declares whether it consumes and/or produces
tables, via `spec.io` on the `ComponentDefinition` CRD
(`pkg/k8s/api/v1/component_types.go`):

```go
IO *ComponentIO `json:"io,omitempty"`

type ComponentIO struct {
    Inputs  string `json:"inputs,omitempty"`  // none | optional | required (default optional)
    Outputs string `json:"outputs,omitempty"` // none | optional | required (default optional)
}
```

- `inputs`/`outputs`, each one of `none`, `optional`, or `required` (empty
  string / omitted defaults to `optional`).
- The catalog API (`GET /api/v1/components(/{name})`) always returns a
  resolved, never-empty `io: {inputs, outputs}` object per component.
- UI gating: `inputs: none` hides the Inputs section entirely (extractors,
  data-generator); `outputs: none` hides the Outputs section (stdout-writer).
- Validation: declaring `inputs` on an `inputs: none` component is an error;
  omitting inputs on an `inputs: required` component is an error; symmetric
  for outputs.
- `io` lives at the **definition** level, not per version — it declares what
  kind of component this is (extractor / transform / writer), which is
  identity, not per-version behavior.
- Built-in IO declarations: data-generator / http-json-extractor /
  finnhub-extractor are `{none, required}`; sql-transform / pandas-transform
  are `{required, required}`; stdout-writer is `{required, none}`.

## What schema validation cannot catch

The Form Subset deliberately excludes `oneOf`/`if`-`then`-`else`, so
component-internal semantic invariants — e.g. data-generator's "exactly one
of `random`/`literal`, and `random.limit` must be non-empty" — cannot be
expressed in `schema.json` and are **not** caught by `validate` or `PUT`.
These stay component-enforced: the component fails fast at start with exit 1
(`FailedUser`) and a `DUPLET_STATUS_MESSAGE:`-prefixed explanation. See
[docs/known-limitations.md](known-limitations.md).

---

## data-generator

Generates random or literal rows inline from pipeline YAML — useful for
testing pipelines without an external data source.

**Image:** `ghcr.io/kacurez/data-generator:v0.12.0` · **Registry name:**
`data-generator` · **IO:** `{inputs: none, outputs: required}`

**Config schema:** [`components/data-generator/schema.json`](../components/data-generator/schema.json)
(`x-datuplet-produces: "tables[*].name"`). Two mutually-exclusive generation
modes per table — `random` (column-to-type schema + stop limit) or `literal`
(explicit columns + rows) — enforced by the component at start, not by the
schema (see "What schema validation cannot catch" above).

**Supported column types (random mode):** `string`, `int`, `long`, `float`,
`double`, `boolean`, `date`, `timestamp`, `now`, `uuid`.

**Fault injection:** `random.userErrorMessage` simulates a user-error exit at
a random row — a first-class platform-test tool for exercising pipeline
failure paths.

**Reproducibility:** Same `run_id` + `table_name` → same row sequence (PCG
seeded from SHA-256 of the pair).

---

## http-json-extractor

Fetches JSON from an HTTP endpoint and writes it as an Iceberg table. Supports
single-request and paginated modes.

**Image:** `ghcr.io/kacurez/http-json-extractor:v0.12.0` · **Registry name:**
`http-json-extractor` · **IO:** `{inputs: none, outputs: required}`

**Config schema:** [`components/http-json-extractor/schema.json`](../components/http-json-extractor/schema.json)
(`x-datuplet-produces: "table_name"`).

For API keys, use `$[name]` in the `headers` map and set the value in the
project's managed secrets. See [docs/secrets.md](secrets.md).

**Field projection:** the optional `fields` config is an array of
`{path, name}` objects. `path` is a dot-path into the (possibly nested) source
record, e.g. `country.value`; `name` is the output column name. When `fields`
is set, only the listed fields are emitted, renamed to `name` — anything not
listed is dropped. Omit `fields` entirely to emit every field of the source
record unchanged.

```yaml
config:
  url: https://api.example.com/users
  array_path: results
  fields:
    - path: id
      name: user_id
    - path: address.country.value
      name: country
```

**Output format:** the component streams **Arrow IPC** to the Data Gateway —
not JSONL. The 64 KiB JSONL per-line limit from earlier releases no longer
applies to this component.

**Column typing — typed inference by default.** Each output column gets its
own Arrow type: joining the kind (integer / float / boolean / string /
nested) of every value sampled across the first 8192 records, per column,
resolves it to Int64, Float64, Boolean, or String — an all-integer column
infers Int64, mixing in a float widens the column to Float64, and any other
mismatch (or a nested object/array) degrades it to String. A `string`
column — declared or inferred — keeps a value's **exact source text**
rather than re-parsing it (`5938028332` stays `5938028332`, `1.50` stays
`1.50` — no float round-tripping); downstream consumers that need numeric
operations on a string column (e.g. `sql-transform`) must `CAST` it
explicitly. Set `schema_inference: strings` to make every column
unconditionally a string — the escape hatch for a feed too irregular for
typed inference to fit reliably.

> `strings` mode is **not** a compatibility mode for tables created before
> 0.12.0. It reproduces what the *component* used to put on the wire, not the
> table schema that used to result: pre-0.12.0 the component emitted untyped
> JSONL and the **gateway** inferred the schema, coercing strings into
> `timestamptz`, `long`, and `double` and marking most columns `required`.
> Nothing the component can emit today reproduces that. See the compatibility
> caveat below.

**Declared output columns.** A pipeline doc can declare, for this
component's output table, an explicit `{name, type}` column mapping under
`outputs.tables[].columns` (`int`/`long`/`float`/`double`/`boolean`/`string`
— see [docs/pipeline-api.md](pipeline-api.md#output-column-mapping-outputstablescolumns)). When
present it overrides `schema_inference` and column inference entirely: only
the mapped columns are written, with the mapped types, from every record —
no sampling, no inference for that table. `fields` still composes with a
declared mapping: `fields` selects/renames from the source record first, and
the mapping then types the resulting (already-renamed) columns by name.

**Schema:** with a declared `outputs.tables[].columns` mapping, the output
columns are exactly the mapped names, in mapped order and type — regardless
of `fields`/`schema_inference`. Otherwise, with `fields` set, the output
columns are the projected names, in declared order (typed per
`schema_inference`). Without either, the schema is the sorted union of
top-level keys seen across the first 8192 records, fixed for the rest of the
run (also typed per `schema_inference`).

**Late-column limitation.** Without `fields` or a declared column mapping,
because the schema is fixed after the first 8192 records, a field that first
appears later in the response is **not written** — the old buffer-everything
path did include it. This is the deliberate cost of bounded memory: the
component never buffers the whole response, only up to one batch's worth of
records while it infers columns. Mitigation: the run logs one `WARN` naming
the dropped field(s) (capped at 64 distinct names — the affected-record
count itself is always exact) and the number of affected records. If the
response's shape is not uniform within its first 8192 records, set `fields`
explicitly (or declare `outputs.tables[].columns`) so every column you need
is guaranteed from record one. See
[docs/known-limitations.md](known-limitations.md).

**Late-type-violation limitation (typed inference and declared columns
only).** A later value that no longer fits its column's already-fixed Arrow
type — e.g. a column of `1, 2, 3` followed later by `"n/a"` — fails the run
(exit code 1) naming the offending column, rather than being coerced or
silently dropped. Mitigation: declare that column as `string` in
`outputs.tables[].columns`, or set `schema_inference: strings` to sidestep
type-fit failures entirely. See [docs/known-limitations.md](known-limitations.md).

**Compatibility caveat.** Writing into a **pre-existing table whose emitted
schema does not match it** — a different column set, or the same column
name now inferred/declared with a different Arrow type than the table
already has — fails Iceberg's schema check, for both `APPEND` and
`FULL_LOAD` — `ReplaceDataFiles` (used by `FULL_LOAD`) replaces the table's
data files, not its catalog schema. Use a fresh table name, declare
`outputs.tables[].columns` to pin a stable schema across runs, or drop the
old table first.

**Tables created before 0.12.0 cannot be written by this component at all.**
Pre-0.12.0, schema decisions were made gateway-side from untyped JSONL, and
that path did things the component-side path deliberately does not:

| pre-0.12.0 (gateway inference) | 0.12.0+ (component/SDK) |
|---|---|
| parsed date-like **strings** into `timestamptz` / `date` | a JSON string stays `string` |
| numeric-looking **strings** became `long` / `double` | a JSON string stays `string` |
| integers could land as `double` | integral values are always `Int64` |
| most columns `required` (non-nullable) | every column is nullable |

Because the declared-column vocabulary has no timestamp/date type and no way
to mark a column `required`, **no `columns` mapping or `schema_inference`
setting reproduces a pre-0.12.0 schema.** The only options for such a table
are to write to a new table name or drop and recreate it. A run that hits
this fails at commit with an Iceberg schema error naming the table (visible
in the component's status message and the gateway sidecar log).

**Local debugging.** `make extractor-local
CONFIG=cmd/mock-datagateway/example-nyc311.json` builds and runs the real
binary against a mock Data Gateway, reporting rows written and peak RSS. The
target's default ports (`GRPC_ADDR=localhost:50051`, `HTTP_ADDR=localhost:50052`)
can already be bound by something else — on macOS, Canonical Multipass commonly
holds port 50051 via launchd. The target detects this and aborts fast (rather
than hanging) with a suggested override; pass your own, e.g.:

```bash
make extractor-local CONFIG=cmd/mock-datagateway/example-nyc311.json \
  GRPC_ADDR=localhost:51051 HTTP_ADDR=localhost:51052
```

Real measured example: 200,000 rows streamed in 25 batches at roughly 94 MB
peak RSS.

---

## finnhub-extractor

Fetches market data from the [Finnhub](https://finnhub.io/) API. Requires a
Finnhub API key.

**Image:** `ghcr.io/kacurez/finnhub-extractor:v0.12.0` · **Registry name:**
`finnhub-extractor` · **IO:** `{inputs: none, outputs: required}`

**Config schema:** [`components/finnhub-extractor/schema.json`](../components/finnhub-extractor/schema.json).
`mode` selects the extraction (`quote`, `news`, `company-news`,
`basic-financials`, `earnings`, `recommendations`, `insider-transactions`);
each mode writes to a fixed, component-internal output table name (documented
in the schema's `x-datuplet-doc` on `mode` — these names are not resolvable
via `x-datuplet-produces`, so reference them directly when wiring a
downstream stage's input).

Set the key in the project's managed secrets (UI: Settings → Secrets, or the
API):

```bash
curl -s -X PUT -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"value":"<your-api-key>"}' \
  https://<host>/api/v1/projects/$PROJECT_ID/secrets/finnhub_key
```

See [docs/secrets.md](secrets.md) for the full secret resolution flow.

---

## sql-transform

Runs user-supplied SQL inside an embedded DuckDB engine. Inputs stream from the
Data Gateway via Arrow IPC and are materialized into DuckDB tables before the SQL
runs. Outputs are written back through the Data Gateway; no S3 credentials touch
the component.

**Image:** `ghcr.io/kacurez/sql-transform:v0.12.0` · **Registry name:**
`sql-transform` · **IO:** `{inputs: required, outputs: required}`

**Config schema:** [`components/sql-transform/schema.json`](../components/sql-transform/schema.json)
(`sql` is `x-datuplet-multiline: "sql"`; `threads`/`memory`/`max_temp_size`/
`temp_directory` are `x-datuplet-advanced`).

**Key points:**

- Inputs stream from the Data Gateway via Arrow IPC and are materialized into
  DuckDB in-memory tables before the user SQL runs. This is load-bearing: DuckDB's
  `arrow_scan` extension does not support multi-pass reads on a streaming source.
- The SQL `CREATE TABLE <name>` must match the `outputs.tables[].name` (or its
  `logicalName` if set); `logicalName` on inputs overrides the view name used in SQL.
- `writeMode: FULL_LOAD` replaces the entire Iceberg table; `APPEND` adds rows.
- The component is credentials-clean: it holds no S3 or Lakekeeper credentials.

**Limitations:**

- No UPSERT (delete support deferred).
- `sinceDays` time-filter is wired through the CRD but the column-predicate push
  is not yet honoured by the Lakekeeper read path; full-snapshot reads are used.
- Schema evolution: if your SQL creates an output table with a different schema than
  the existing Iceberg target, the commit fails with `FailedUser`.

---

## pandas-transform

> **Not yet shipped.** No image is published for this component (it's absent
> from `_release-components.yml` and `docker-build-k8s`), so its
> ComponentDefinition template is disabled by default
> (`components.enablePandasTransform: false`, RFC 024 T6.3) until an image
> exists.

Applies a sequence of pandas operations to input data. Reads the input table as
CSV from the Data Gateway, applies the operations in order, and writes the
result back as CSV — no S3 or Lakekeeper credentials touch the component.

**Image:** `ghcr.io/kacurez/pandas-transform:v0.12.0` · **Registry name:**
`pandas-transform` · **IO:** `{inputs: required, outputs: required}`

**Config schema:** [`components/pandas-transform/schema.json`](../components/pandas-transform/schema.json)
(`x-datuplet-produces: "output_table"`).

**Supported operations (applied in order):** `filter`, `select`, `sort`,
`rename`, `drop`, `fillna` — see the schema's `operations[].type` enum and
per-type `x-datuplet-doc` for fields.

Only one input table is read (the first entry under `inputs.tables`). Unknown
operation types and references to missing columns are logged as warnings and
skipped rather than failing the run.

---

## stdout-writer

Reads input tables and prints them to stdout. For debugging only — no Iceberg
output.

**Image:** `ghcr.io/kacurez/stdout-writer:v0.12.0` · **Registry name:**
`stdout-writer` · **IO:** `{inputs: required, outputs: none}`

**Config schema:** [`components/stdout-writer/schema.json`](../components/stdout-writer/schema.json)
(`format`: `csv` (default) or `json`).

**Example pipeline** (PipelineDoc format, RFC 027 — see
[docs/pipeline-api.md](pipeline-api.md)):

```yaml
name: debug-pipeline
stages:
  - name: generate
    components:
      - name: gen
        component: data-generator
        config:
          tables:
            - name: events
              random:
                schema: { id: int, value: double }
                limit: { rowsCount: 10 }
        outputs:
          defaultBucket: raw
          defaultWriteMode: APPEND
  - name: print
    components:
      - name: printer
        component: stdout-writer
        inputs:
          tables:
            - bucket: raw
              table: events
        config:
          format: json
```

View output in component logs:

```bash
kubectl logs -n datuplet <pod-name> -c component
```

---

## Writing your own component

Use the Go SDK (`sdk/go/`) or Python SDK (`sdk/python/`) to build a component.
Both SDKs are ~200–300 LOC and expose three operations: `OpenWriter`,
`WriteChunk` / `Write`, and `Close`. The SDKs handle gRPC connection, config
resolution, and secret delivery from the Data Gateway sidecar.

Go components can opt into writer-side Arrow output via the `sdk/go/arrow`
module: `StringSink` batches records into self-contained IPC streams as an
all-String `RecordBuilder`, with optional first-batch schema inference when
you don't supply explicit columns. `InferringSink` is the typed sibling —
it infers one Arrow type per column from the first batch, or writes an
exact declared column set (name + type, via the module's `ArrowTypeFor`
vocabulary) with `WithTypedColumns`. Either sink's `Add`/`Finish` can return
a `TypeViolationError` for a value that no longer fits its column's type —
a user-error condition (map it to `sdk.ExitUserError`, not `ExitAppError`).
Both are the writer-side counterpart of that module's existing Arrow reader
(used by `sql-transform` to stream Arrow-IPC inputs); base `sdk/go` stays
Arrow-free.

**Always check `Commit`'s result, not just its error.** A per-table commit
failure (an Iceberg schema conflict, a 409, a credential problem) comes back
as a *successful* RPC carrying `Success: false` — so `if _, err :=
client.Commit(ctx); err != nil` alone reports the component as having
succeeded while its outputs were never committed. Check `result.Success`, and
on failure use `result.FailureDetail()` for the message: the gateway populates
only the **per-table** `Error`, so `result.Error` on its own is empty and the
real reason is lost. Commit failures are `ExitAppError` (≥20) per the exit-code
contract.

Write a `schema.json` next to your parser, following the Form Subset rules
above, and register it as a `ComponentDefinition` with the appropriate `io`
capability — see any built-in's chart template under
`charts/datuplet-app/templates/components/` for the shape.

See [`sdk/go/client.go`](../sdk/go/client.go) and
[`sdk/python/client.py`](../sdk/python/client.py) for the entry points.
