# RFC 027 — Schema-First Pipeline Configuration

**Status:** Draft v4 — external design review round 3 (Codex gpt-5.5, resumed
thread, maintainer-requested): all 3 round-2 fixes verified; 8
implementation-readiness findings folded in — run-timeline store consumer
(§5.1), operator's CRD-shaped `ValidateTyped` entry retained (§5.4), doc
`name` context rule (§5.4), components catalog gains `io` (§5.2/§4.3), hidden
`resources` preservation in form mode (§6), CLI `put` name extraction +
`--schema` version resolution + headless remote context (§7). Round-2 history:
all 20 round-1 findings verified resolved; 3 v2 wording defects fixed
(validate status-code contract in §5.2, trigger "unchanged" row scoped to the
HTTP contract, `TriggerRequest` naming in §5.3). Next gate: implementation
plan + maintainer review. Draft v2 history: brainstormed + UX-validated with
the maintainer against an interactive mockup
([2026-07-17-rfc-027-pipeline-builder-ux-mockup.html](2026-07-17-rfc-027-pipeline-builder-ux-mockup.html),
the companion design artifact — open it in a browser and use the built-in
walkthrough). External design review round 1 (Codex gpt-5.5) folded in: 16
findings — validator re-pointing (§5.4), doc↔CRD field-name authority (§3),
runbackend/trigger contract changes (§5.3), store/API/CLI interface renames
named explicitly (§5.1, §5.2, §7), runs-history CASCADE blast radius (§5.1),
validate-endpoint resource-gate semantics (§5.2), upstream-input check demoted
to warning for pre-existing tables (§5.4), IO-capability placement rationale
(§4.3), built-in-only fallback scoping (§4.2), backup escape-hatch conversion
note (§9). **POC greenfield posture — no data migration, no back-compat
obligations** (maintainer decision: "think of it as if we were making it from
scratch"). No code written yet.

**Scope:** Make the structured pipeline document the primary authoring surface
across UI, API, and CLI. Form-first editing generated from component config
schemas; raw YAML demoted to an explicit mode; agents get the same first-class
surface as humans (discovery, validation, JSON in/out, headless auth).

**Builds on:** RFC 026 (component registry, uniform `config:`, secrets,
resource gating) — none of its safety contracts change. This RFC replaces its
§4.7 one-way form-to-YAML posture with a two-way structured document.

---

## 1. Problems (verified against code)

### P1 — Users author Kubernetes CRs, not pipelines

The stored pipeline body is a full K8s manifest: `apiVersion: datuplet.io/v1`,
`kind: Pipeline`, `metadata:`, `spec:`
([types.go:5-24](../../../pkg/pipeline/config/types.go)). The envelope is pure
noise at authoring time — pipeline-api parses the YAML and applies the CR
itself at trigger ([runbackend/k8s.go:208-210](../../../pkg/pipelineapi/runbackend/k8s.go)).
Every user-facing surface (UI starter template, examples, CLI `put`) drags the
boilerplate along.

### P2 — The schema form is shallow and one-way

RFC 026's builder renders **top-level properties only**; any nested object
falls back to a JSON textarea
([schema-form.js:1-10](../../../ui/product/lib/schema-form.js)). The form is a
one-way snippet emitter into the YAML textarea — it never parses back
([pipeline-detail.js:190-196](../../../ui/product/pages/pipeline-detail.js)).
Consequence: `data-generator` — whose config is an array of objects with
nested `random`/`literal` specs
([data-generator.yaml:28-120](../../../charts/datuplet-app/templates/components/data-generator.yaml))
— gets effectively **zero** form support. The textarea is the real editor.

### P3 — Config schemas are hand-copied into the chart

`configSchema` blobs are authored inline in chart templates
(`charts/datuplet-app/templates/components/*.yaml`), separately from each
component's actual config parser (`components/data-generator/config.go`).
Nothing ties them together; drift is silent. Schemas are also
under-documented: no descriptions on most properties, no defaults surfaced.

### P4 — Agents get a worse surface than humans

CLI CRUD exists (`datuplet pipeline list|get|put|delete`,
[cmd/datuplet/pipeline.go](../../../cmd/datuplet/pipeline.go)) but is
YAML-file-shaped with the K8s envelope required. There is no way to discover a
component's config schema from the CLI, no validate-without-save anywhere
(validation happens only as a side effect of `PUT`), and no documented
headless auth path (login prompts for email+password interactively,
[login.go:52-53](../../../cmd/datuplet/login.go)).

---

## 2. Decisions

All maintainer-confirmed during the 2026-07-17 brainstorm:

- **D1 — Envelope-free PipelineDoc, hard break.** One structured document
  (§3), JSON-canonical, YAML-rendered for humans. Bodies containing
  `apiVersion`/`kind`/`metadata` are **rejected** with a pointed finding. No
  data migration: the pipelines table is recreated destructively (§5.1).
- **D2 — Structured store, raw as a view.** Postgres stores the doc as JSONB.
  Raw YAML mode serializes the doc; saving raw parses it back. Form ⇄ raw
  switching works both ways; YAML comments do not survive (accepted).
- **D3 — Schemas live with components.** `components/<name>/schema.json` is
  the single source of truth; a make target syncs copies into the chart; CI
  fails on drift (§4.1).
- **D4 — Form Subset with lint-enforced quality.** The UI renders a declared
  JSON-Schema subset recursively (§4.2). CI lints every built-in schema:
  in-subset (so **100% of properties render as real controls** — no JSON
  fallback for built-ins), `description` mandatory on every property, no
  `required`+`default` contradictions.
- **D5 — Agents are first-class.** Component discovery via CLI,
  validate-without-save endpoint + CLI command, JSON accepted/emitted
  everywhere, headless auth via env var (§6, §7).
- **D6 — Capability-gated IO.** `ComponentDefinition` declares whether the
  component consumes/produces tables; the UI hides inapplicable sections and
  validation enforces the declaration (§4.3).

### Non-goals (explicitly out of scope)

Custom per-component UI plugins; rendering `oneOf`/`anyOf`/`$ref` (out of
subset → per-node JSON fallback); `outputs.buckets[]` in the form (raw-only);
a per-component PATCH API; long-lived service tokens; an MCP server; any
guided walkthrough in the product UI (the mockup's tour is a design aid only).

---

## 3. The PipelineDoc

One document, canonical as JSON, rendered as YAML for humans. The validated
example from the mockup:

```yaml
name: events-etl          # optional in body; must equal the URL/CLI name when present
description: Generate demo events and aggregate revenue per country.  # optional
gateway:                  # optional, GatewayConfig unchanged (chunkSize, bufferSize, …)
  chunkSize: 33554432
stages:                   # sequential; components within a stage run in parallel
  - name: generate
    components:
      - name: gen                     # instance name, unique across the pipeline
        component: data-generator     # registry reference
        version: 0.9.1                # optional → registry default resolution
        config:                       # validated against the version's configSchema
          tables:
            - name: events
              rowInsertSpeed: 1000
              random:
                schema: { id: uuid, created: timestamp, amount: double, country: string }
                limit: { rowsCount: 100000 }
        outputs:
          defaultBucket: raw          # dynamic mode: component names tables at runtime
          defaultWriteMode: APPEND
  - name: transform
    components:
      - name: daily-summary
        component: sql-transform
        inputs:
          tables:
            - { bucket: raw, table: events }
        config:
          sql: |
            CREATE TABLE daily_summary AS
            SELECT country, count(*) AS events, sum(amount) AS revenue
            FROM events GROUP BY country;
        outputs:
          tables:                     # explicit mode: one mapping per table
            - { name: daily_summary, bucket: curated, writeMode: FULL_LOAD }
```

Semantics:

- Everything under today's `spec:` is hoisted to the top level;
  `metadata.name` becomes `name`; `apiVersion`/`kind`/`metadata`/`labels` are
  gone. `description` is new (shown in lists; useful to agents).
- `stages`, `inputs` (`InputSpec`), `outputs` (`OutputSpec`), `resources`
  (superadmin diff-gate, RFC 026 §4.4) keep their existing shapes and
  semantics unchanged. `outputs.buckets[]` remains valid in the doc but has no
  form UI.
- Parsing is **strict**: unknown fields are findings (error), and a top-level
  `apiVersion`, `kind`, or `metadata` key yields a single pointed finding
  (“legacy Kubernetes CR format — Datuplet pipelines are envelope-free now;
  see docs/pipeline-api.md”).
- The internal Go model (`pkg/pipeline/config`) slims to match:
  `Pipeline{Name, Description, Gateway, Stages}`. `convert.go` gains the
  envelope at the K8s boundary — the `Pipeline`/`PipelineRun` CRDs and the
  operator contract are **unchanged**; the CR simply stops being a user-facing
  format (examples and docs stop teaching `kubectl apply` for pipelines).

**Field-name authority.** The PipelineDoc's field names are, normatively, the
`yaml:` tags of `pkg/pipeline/config/types.go` — not the CRD JSON tags. The
two shapes currently diverge in two places, and the doc keeps the runtime
names; `convert.go` owns the rename at the CR boundary:

| PipelineDoc (runtime tags) | CRD JSON ([pipeline_types.go](../../../pkg/k8s/api/v1/pipeline_types.go)) |
|---|---|
| `inputs.tables[].as` | `logicalName` |
| `outputs.tables[].partitionSpec[].source_column` | `partitionFields[].sourceColumn` |

Any future divergence is a bug; a unit test asserts doc→CR→doc round-trips
losslessly over every field.

---

## 4. Component schemas

### 4.1 Source of truth + sync

- `components/<name>/schema.json` — JSON Schema draft 2020-12, one per
  component, next to the code that parses the config. Authors update both in
  the same commit. A plain file is deliberately language-agnostic: it works
  identically for Go components and for `pandas-transform` (Python). The
  initial authoring pass ports today's chart-template blobs into these files
  and upgrades them to lint compliance (descriptions everywhere, defaults,
  annotations) — including properties the current blobs model thinly, e.g.
  data-generator's `random.userErrorMessage`.
- `make sync-component-schemas` copies each into
  `charts/datuplet-app/files/component-schemas/<name>.json`; the
  ComponentDefinition templates switch from inline blobs to
  `.Files.Get`. (Helm can only read files inside the chart directory — the
  synced copy exists for packaging; CI makes it mechanical.)
- CI job: run the sync target, `git diff --exit-code charts/` (same pattern as
  RFC 024's `verify-versions`).

### 4.2 The Form Subset (normative)

The UI renders these constructs natively, recursively, at any depth:

| Construct | Control |
|---|---|
| `object` + `properties`/`required` | field group (nested groups collapsible) |
| `array` of objects | repeater cards with add/remove |
| `array` of scalars | list editor |
| `additionalProperties: {type: string [enum: …]}` | key→value map rows (e.g. data-generator's `schema: column→type`) |
| scalar `string`/`integer`/`number` | input (numeric where typed) |
| `boolean` | checkbox; **tri-state select when `default` present** (unset / true / false) |
| `enum` | select |
| `x-datuplet-secret: true` | secret picker (`$[key]`, RFC 026 semantics unchanged) |
| `x-datuplet-multiline: "<lang>"` | code textarea (e.g. `sql`) |
| `x-datuplet-advanced: true` | property grouped into one collapsed **Advanced** section |
| `x-datuplet-produces` (schema root) | dynamic-output table-name resolution for the inputs picker (§6) |
| `title`, `description`, `default`, `examples` | display metadata (§4.4) |
| `minimum`/`maximum`/`minLength`/`minItems`/`pattern` | advisory client checks; server + component stay authoritative |

Out of subset: `oneOf`/`anyOf`/`allOf`/`not`, `$ref`/`$defs`,
`if`/`then`/`else`, `patternProperties`, `const`. An out-of-subset **node**
renders as a JSON sub-editor for that node only (never a whole-form
fallback). The fallback path exists for **third-party / operator-registered**
schemas only — built-ins must lint clean (below) and never exercise it.

**Schema lint** (a Go test walking `components/*/schema.json`):

1. Valid draft 2020-12 and entirely within the Form Subset — built-ins may
   never hit the JSON fallback. This is the guarantee that the **full**
   schema is form-editable, including rarely-used knobs like data-generator's
   `random.userErrorMessage` (failure injection is a first-class platform-test
   tool; the mockup trimmed it for demo purposes only).
2. Every property has a non-empty `description`.
3. `required` + `default` on the same property is a contradiction → error.
4. `x-datuplet-*` annotations are known and well-typed.

### 4.3 IO capability

New optional field on `ComponentDefinitionSpec`
([component_types.go:35](../../../pkg/k8s/api/v1/component_types.go)):

```go
// IO declares whether the component consumes and produces tables.
IO *ComponentIO `json:"io,omitempty"`

type ComponentIO struct {
    Inputs  string `json:"inputs,omitempty"`  // none | optional | required (default optional)
    Outputs string `json:"outputs,omitempty"` // none | optional | required (default optional)
}
```

- UI: `inputs: none` → no Inputs section at all (extractors, data-generator);
  `outputs: none` → no Outputs section (stdout-writer).
- Validation: declaring inputs on `inputs: none` → error; missing inputs on
  `inputs: required` → error; symmetric for outputs.
- Built-ins: data-generator / http-json-extractor / finnhub-extractor
  `{none, required}`; sql-transform / pandas-transform `{required, required}`;
  stdout-writer `{required, none}`.
- IO lives at the **definition** level, not per `VersionSpec`, deliberately:
  it declares what kind of component this is (extractor / transform / writer),
  which is identity, not behavior — a version that changed its IO class would
  be a different component. This also keeps the UI's catalog gating stable
  across version selection. (Per-version `configSchema`/`resources` are
  unaffected.)
- The existing "must have at least inputs or outputs" rule
  ([validate.go:236-244](../../../pkg/pipeline/validate/validate.go)) is
  subsumed by the capability checks and keeps working for components without
  an `io` declaration (both default `optional`).
- Requires the usual CRD triple: type + `DeepCopyInto` (pointer field) +
  manual CRD manifest in `charts/datuplet-app/crds/`.

### 4.4 Descriptions and defaults

- `description` — the always-visible one-line hint (lint-mandatory).
- `x-datuplet-doc` — optional long-form help, shown on hover (ⓘ). Both ship
  to agents via `datuplet components get --schema` since they live in the
  schema itself. Dynamic defaults (“all CPUs granted to the container”) belong
  here or in `description` — never in `default`.
- `default` — **display metadata only.** Rendered as placeholder text
  (`default: 1000`) or the empty option label (`— default (FULL_LOAD) —`).
  An untouched field stores nothing; the component applies its own default at
  runtime. Stored configs stay sparse; a version bump can change a default
  without touching saved pipelines.

---

## 5. Server (pipeline-api)

### 5.1 Storage

The `pipelines.yaml text` column
([002_pipelines_runs.sql](../../../pkg/pipelineapi/db/migrations/002_pipelines_runs.sql),
[store/pipeline.go](../../../pkg/pipelineapi/store/pipeline.go)) becomes
`doc jsonb NOT NULL`, plus a `description text NOT NULL DEFAULT ''` column
(written at PUT time from the doc) so list queries stay cheap. One
**destructive** migration: `TRUNCATE pipelines CASCADE` + column swap. No
conversion code anywhere. The upgrade-e2e CI stays green because the
migration exists; it just doesn't carry data. Two consequences stated
plainly:

- **Run history is wiped too.** `runs.pipeline_id` references `pipelines(id)`
  `ON DELETE CASCADE`
  ([002_pipelines_runs.sql](../../../pkg/pipelineapi/db/migrations/002_pipelines_runs.sql)),
  so truncating pipelines cascades into `runs`. POC-accepted; release notes
  say so explicitly.
- **Stale `Pipeline` CRs stay in the cluster.** Deleting a stored pipeline
  never deleted its CR
  ([store/pipeline.go:117-120](../../../pkg/pipelineapi/store/pipeline.go));
  the reset makes that pre-existing behavior more visible. Harmless — trigger
  re-applies the CR by name — and a cleanup job stays out of scope.

Interface fallout (named so the diff is no surprise): `PipelineStore` is
typed around `YAML string` / `GetYAMLByID` / `Put(..., yaml []byte)`
([http/stores.go:50-70](../../../pkg/pipelineapi/http/stores.go)) with the pgx
adapter reading/writing the string column
([http/stores_pgx.go:124-147](../../../pkg/pipelineapi/http/stores_pgx.go)) —
all of these retype to the doc. That includes the one non-CRUD consumer: the
run-timeline path reads the stored pipeline via `GetYAMLByID` and re-parses it
([run_handlers.go:229](../../../pkg/pipelineapi/http/run_handlers.go) →
[run_timeline.go:48](../../../pkg/pipelineapi/http/run_timeline.go)
`buildTimeline` → `config.Parse`); it moves to the doc-typed getter and takes
the parsed doc as a parameter.

### 5.2 API

| Endpoint | Change |
|---|---|
| `PUT /api/v1/projects/{pid}/pipelines/{name}` | Accepts `application/json` (doc) or YAML (any other/absent content type, parsed server-side). Same response contract as today: 204 clean / 200 + warning findings / 400 + error findings. 1 MiB cap unchanged. |
| `GET …/pipelines/{name}` | Returns `{id, name, doc, created_at, updated_at}` (doc as JSON object). `?format=yaml` returns `text/yaml` — deterministic rendering (struct field order; map keys sorted, which `gopkg.in/yaml.v3` does natively). |
| `GET …/pipelines` | List items gain `description`. |
| `POST …/pipelines/validate` | **New.** Body = doc (JSON or YAML), name optional. Runs the full PUT-time validation (structure, registry resolution, configSchema, secret refs, IO capability, resource gate) without persisting. Status contract: `200 {"findings": […]}` for every readable body — validation outcomes (errors *and* warnings) are findings, never HTTP errors; `400`/`413` only for an unreadable or oversized body; `5xx` only for infrastructure failures (e.g. the store read backing the resource gate). This keeps CLI exit semantics deterministic (§7: 0/1 from findings, ≥2 from transport). Same authz as PUT. |
| `DELETE`, admin endpoints | Unchanged. |
| Components catalog (`GET /api/v1/components(/{name})`) | **Additive:** list + detail responses gain the `io` capability object (§4.3) — the UI's section gating and CLI output need it ([component_handlers.go:30-40](../../../pkg/pipelineapi/http/component_handlers.go) response structs grow the field). Otherwise unchanged. |
| Trigger (`POST …/pipelines/{name}/runs`) | HTTP contract unchanged; the internal runbackend contract changes (§5.3). |

Contract details:

- `GET`'s JSON field rename (`yaml` → `doc`,
  [pipeline_handlers.go:90-95](../../../pkg/pipelineapi/http/pipeline_handlers.go))
  is a **breaking API change**; the CLI mirror
  ([cmd/datuplet/pipeline.go:28-34](../../../cmd/datuplet/pipeline.go)) updates
  in lockstep. POC: no API versioning.
- **Validate × resource diff-gate.** The non-superadmin resource gate compares
  old vs new state ([pipeline_handlers.go:183-218](../../../pkg/pipelineapi/http/pipeline_handlers.go)).
  `POST …/validate` mirrors PUT: when `name` is given it loads the stored doc
  and diffs against it; when absent (or nothing stored) it validates with
  create semantics (any `resources` from a non-superadmin → finding). A store
  read failure is a 5xx, never a finding.
- **Concurrency is unchanged**: PUT stays last-write-wins
  ([stores_pgx.go:146-169](../../../pkg/pipelineapi/http/stores_pgx.go),
  [pipeline_handlers.go:244-246](../../../pkg/pipelineapi/http/pipeline_handlers.go));
  optimistic locking is explicitly out of scope for this RFC.

The UI is served from the same origin; agents and the UI hit identical
endpoints — there is no separate "agent API".

### 5.3 Trigger path

The run flow (parse → apply `Pipeline` CR → create `PipelineRun`,
[runbackend/k8s.go:380](../../../pkg/pipelineapi/runbackend/k8s.go)) keeps its
shape, but the contract stops carrying raw CR YAML end-to-end:

- `TriggerRequest.PipelineYAML []byte`
  ([runbackend/backend.go:26-32](../../../pkg/pipelineapi/runbackend/backend.go))
  becomes the parsed `*config.Pipeline` doc.
- `ApplyPipelineCRD` ([k8s/pipeline_apply.go:21-31](../../../pkg/pipelineapi/k8s/pipeline_apply.go))
  stops unmarshaling YAML into `datupletv1.Pipeline` and instead renders the
  CR from the doc via `convert.go` (the single doc→CR conversion point).
- The trigger handler's double reparse
  ([run_handlers.go:83-100](../../../pkg/pipelineapi/http/run_handlers.go))
  collapses to one parse of the stored doc.

### 5.4 Validation re-pointing

Today the shared validator is CR-shaped: `config.Parse` delegates to
`validate.ValidatePipeline`, which strict-decodes the body into
`datupletv1.Pipeline` and requires `metadata.name` / `spec.stages`
([validate.go:69-103](../../../pkg/pipeline/validate/validate.go),
[parser.go:30-38](../../../pkg/pipeline/config/parser.go)). This RFC re-points
the validator at the PipelineDoc — strict-decode into the slimmed doc model,
`name`/`stages` checks on doc fields — and runs CR-shape-dependent checks
after the single `convert.go` conversion. The registry, config-schema,
`$[secret]` and resource checks are unaffected in substance.

Two contract details:

- **The operator keeps a CRD-shaped entry.** `PipelineReconciler` and run
  admission validate typed CRDs directly
  ([pipeline_controller.go:91](../../../pkg/k8s/controllers/pipeline_controller.go),
  [pipelinerun_controller.go:419](../../../pkg/k8s/controllers/pipelinerun_controller.go)
  — `validate.ValidateTyped`). `ValidateTyped` stays CRD-shaped for that path;
  the new doc-shaped entry is the API front door. Both are thin shells over
  one shared rule set, run on the shape produced by the single `convert.go`
  conversion — so the API and operator paths cannot drift.
- **Name context.** `name` is optional in the body (§3): the authoritative
  name comes from the route (PUT) or CLI argument, passed to validation as
  context. A doc-level `name`, when present, must equal the context name;
  `POST …/validate` without any name skips the equality check and validates
  format only (DNS-1123 when present).

One deliberate semantic change: the cross-stage input check
([validate.go:326-336](../../../pkg/pipeline/validate/validate.go)) currently
**errors** when a stage>0 input table isn't produced by an earlier stage. The
approved UX lets any stage read pre-existing storage tables (the picker's "in
storage" group), so this finding is demoted to a **warning** ("not produced
upstream — assumed to pre-exist in storage; the run fails at read time if it
doesn't"). Bucket-level acceptance via `registerOutputs`
([validate.go:194-207](../../../pkg/pipeline/validate/validate.go)) is
unchanged, which is also why `x-datuplet-produces` (§6) is **UI-only sugar**:
the server never resolves it; it only feeds the picker.

What validation still cannot catch: component-internal semantic invariants —
e.g. data-generator's "exactly one of `random`/`literal`, and `random.limit`
must be non-empty"
([config.go:92-125](../../../components/data-generator/config.go)) — because
`oneOf` is outside the Form Subset by design. These stay component-enforced:
the component fails fast at start with exit 1 (`FailedUser`) and a clear
`DUPLET_STATUS_MESSAGE`. Documented in §9; the agent-loop e2e exercises one
such case.

---

## 6. UI (`ui/product/`)

The pipeline page becomes a form-first builder (see the mockup for the
validated look & feel; Datuplet tokens, no new framework, no build step):

- **Layout:** left outline (stages → component cards; inline stage rename;
  stage delete with confirm when non-empty; add component / add stage), main
  editor panel for the selected component, topbar with name, mode toggle
  `[Form | YAML]`, Validate / Save / Trigger run.
- **Catalog modal:** "+ Add component" lists the registry (`GET
  /api/v1/components`) with descriptions; picking one appends a component with
  an empty config.
- **Config form:** `schema-form.js` is rewritten as a recursive Form Subset
  renderer (§4.2). Client-side `getErrors()` is advisory; server findings are
  authoritative and render in the same findings panel as today.
- **Inputs section** (only when `io.inputs ≠ none`): current inputs as chips;
  "+ Add input table…" opens a **modal picker** with two groups — *produced
  upstream* and *in storage* (`GET /api/v1/storage/projects/{pid}/tables`) —
  searchable, multi-select. Upstream candidates are the union of earlier
  stages' explicit `outputs.tables` entries, plus names resolved via
  `x-datuplet-produces` (below); a dynamic-bucket output with unresolvable
  names is shown as `<bucket> (dynamic)`, not selectable.
  - `x-datuplet-produces` (schema-root annotation, optional): a dot/wildcard
    path into the component's config that yields its runtime table names —
    data-generator sets `"tables[*].name"`. ~30 LOC generic resolver; this is
    what makes `raw.events` appear as a pickable upstream table in the mockup.
- **Outputs section** (only when `io.outputs ≠ none`): explicit two-mode UI
  with a segmented toggle:
  - **Dynamic — bucket only** → `defaultBucket` (+ `defaultWriteMode`); hint
    explains the component names tables at runtime.
  - **Explicit tables** → `tables[]` mapping rows (bucket + table name +
    per-table writeMode), "+ New table" and "Select existing…" (same picker
    modal — targeting an existing table, e.g. for APPEND).
  - Switching modes carries the bucket over. Mappings missing bucket or name
    are validation errors.
- **YAML mode:** serializes the in-memory doc via a vendored `js-yaml` ESM
  module (`ui/product/vendor/`, the no-build constraint). Switching back
  parses client-side; a parse error keeps you in YAML with the message shown —
  nothing is lost, nothing is sticky. Save sends JSON from form mode and the
  YAML text from YAML mode (server re-validates either way).
- **Gateway settings:** a collapsed "Pipeline settings" group (4 numeric
  fields, defaults as placeholders).
- Resources stay YAML-only + superadmin-gated (RFC 026 §4.4 unchanged). No
  walkthrough/tour ships in the product.
- **Hidden-subtree preservation:** the editor's in-memory state is the *full*
  stored doc. The form renders (and mutates) only the paths it has controls
  for; everything else — notably `resources`, and any config subtree shown in
  a JSON fallback node — rides along untouched and survives a form-mode JSON
  save byte-for-byte. Dropping an unrendered field is a bug, not a
  normalization.

---

## 7. CLI (`cmd/datuplet`) + agent loop

New/changed subcommands (existing flag conventions:
[pipeline.go:80-101](../../../cmd/datuplet/pipeline.go)):

```
datuplet components list [--json]
datuplet components get <name> [--version <v>] [--json | --schema]
    --schema prints the resolved version's configSchema JSON verbatim
datuplet pipeline get <name> [--json]        # YAML doc by default (?format=yaml)
datuplet pipeline put [<name>] -f <file|->   # JSON or YAML, sniffed by first byte
datuplet pipeline validate -f <file|-> [--name <n>] [--json]
    --name engages the update-mode resource-gate diff (§5.2)
    exit 0 = no error findings (warnings allowed), 1 = errors, ≥2 = transport
datuplet trigger <name> [--wait --json]      # unchanged
```

CLI contract changes called out: the response mirror drops `yaml` for `doc`
([pipeline.go:28-34](../../../cmd/datuplet/pipeline.go)); `get` prints the
server's `?format=yaml` rendering by default; `put` sets `Content-Type` per
sniffed body instead of always `application/yaml`
([pipeline.go:188-196](../../../cmd/datuplet/pipeline.go)); `put`'s name
fallback reads top-level `name` instead of `metadata.name`
([pipeline.go:331](../../../cmd/datuplet/pipeline.go)) and errors when neither
the positional nor the doc provides one (same message shape as today);
`components get --schema` resolves the version **client-side** with the
registry's documented rule — `--version` if given, else `defaultVersion`, else
the highest stable semver — over the detail response's `versions[]` (the API
stays a plain catalog; no server-side "resolved schema" endpoint).

Headless auth: two env vars close the loop — `DATUPLET_API_TOKEN` (honored
wherever the token file is read; precedence `--token-file` > env >
`~/.datuplet/api-token`) and `DATUPLET_REMOTE` (honored where
`~/.datuplet/cluster.json` supplies the pipeline-api URL today,
[remote.go:75](../../../cmd/datuplet/remote.go); precedence `--remote` > env >
cluster file). With both set, an agent needs **no** `~/.datuplet` state at
all. `datuplet login --password-stdin` remains for explicit non-interactive
login. A new docs page ("Agent quickstart") documents the loop:

```
datuplet components get data-generator --schema   # learn the config shape
datuplet pipeline validate -f events-etl.yaml     # findings as JSON, exit code
datuplet pipeline put -f events-etl.yaml
datuplet trigger events-etl --wait --json
datuplet storage sample curated.daily_summary     # verify output
```

Humans in the UI and agents on the CLI compose, validate, save, and trigger
the **same document through the same endpoints** — that is the whole point.

---

## 8. Testing & rollout

- **Unit:** doc parser (envelope rejection, unknown-field findings), YAML
  render determinism (golden files), doc→CR→doc lossless round-trip (§3),
  validate endpoint (incl. resource-gate create/update semantics),
  IO-capability rules, upstream-input warning demotion (§5.4), schema lint
  (walks every `components/*/schema.json`), JSONB store round-trip.
- **e2e (`make e2e-k8s`, OrbStack):** all scenario pipeline bodies rewritten
  to the doc format; one new agent-loop scenario driving
  `components get --schema → validate (bad config → exit 1) → fix → put →
  trigger --wait → storage sample` purely through the CLI.
- **Examples & docs (CI-guarded):** `examples/pipelines/*` rewritten;
  `docs/pipeline-api.md` (doc format + validate), `docs/components.md`
  (schema authoring guide: subset, annotations, lint), README/quickstarts,
  new agent quickstart.
- **Chart/CRD:** ComponentDefinition `io` field (CRD manifest + DeepCopy),
  schema files move, `fgaModel` untouched. `make tidy` (multi-module repo).
- Implementation lands via feature branch + draft PR per repo discipline;
  phases (server doc model → schemas/lint/io → UI builder → CLI → examples/
  docs/e2e) go to the implementation plan, not this RFC.

## 9. Risks & mitigations

- **Strict parsing bites loose YAML** → findings name the exact unknown key
  and path; `validate` lets anyone check before saving.
- **JSONB normalization reorders keys** → rendering always goes through the
  typed Go model (struct order) + yaml.v3 sorted maps, never raw JSONB text.
- **Vendored js-yaml (~40 KB)** → acceptable for the no-build UI; only loaded
  on the pipeline page.
- **Destructive migration drops stored pipelines *and cascades into run
  history*** (§5.1) → POC-accepted, release notes + `known-limitations.md`
  entry. Escape hatch: `pipeline get > backup.yaml` with the **old** CLI
  before upgrading, then a mechanical hand-conversion (delete
  `apiVersion`/`kind`, `metadata.name` → `name`, hoist the `spec:` contents to
  top level) — the release notes show the 5-line diff.
- **Schema/parser drift within a component** → not fully solvable by lint;
  the schema and parser live in the same directory and PR review + the
  agent-loop e2e (which validates against the schema, then runs the real
  component) keep them honest. Component-only semantic invariants remain a
  runtime failure by design (§5.4) — they fail fast as `FailedUser` with a
  clear status message, not as a late data error.
