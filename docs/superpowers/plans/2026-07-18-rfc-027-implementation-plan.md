# RFC 027 — Full Implementation Plan (all phases, single document)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. This document contains FIVE self-contained phase plans ("Parts"), each with its own task index, DAG, and phase gate, sharing one Global Constraints and one Harness section (below). Execute one Part at a time; every later Part opens with a **Preflight (anchor re-verification)** task — run it, never skip it.

**Spec:** `docs/superpowers/specs/2026-07-17-rfc-027-schema-first-pipeline-config-design.md` (Draft v4 — 3 Codex review rounds folded in).
**UX artifact (normative for Part 4):** `docs/superpowers/specs/2026-07-17-rfc-027-pipeline-builder-ux-mockup.html` — the approved interactive mockup. UI tasks port its behaviors; when a task references a mockup function by name, open the file and read that function.

## Parts

| Part | Phase | Tasks | Starts when |
|---|---|---|---|
| 1 | Server core: doc model, validator re-point, store, API, trigger | S0–S9 | now |
| 2 | Schemas + registry: `io` capability, schema.json files, sync, lint | R0–R7 | Part 1 gate passed |
| 3 | CLI: components, validate, doc CRUD, headless auth | C0–C5 | Part 1 gate passed (∥ Part 2 — file-disjoint except C2 needs R2's `io` in catalog JSON; C2 waits for R2) |
| 4 | UI: form-first builder | U0–U7 | Parts 1 + 2 gates passed |
| 5 | Examples, e2e, docs | E0–E5 | Parts 1–4 gates passed |

## Branching model

All implementation lands as **sequential commits on one feature branch** `feat/rfc-027-schema-first-config` off `main`. Never push `main`, never tag (repo rule). The Part 1 gate (S9) opens **the single draft PR** (`gh pr create --draft --base main --title "RFC 027: schema-first pipeline configuration"`); every later Part's gate pushes and **appends its phase summary to that PR body** (`gh pr edit <num> --body …`) — it does NOT open a new PR. Commit 0 (before S0): commit the spec, the mockup HTML, and this plan so subagents read them from the tree. At each Part start the orchestrator records `git rev-parse HEAD` as `<phase-start SHA>` — the base of that Part's cumulative Codex review. Task-level parallelism: tasks marked `Parallel: yes` are file-disjoint and may run in separate subagent worktrees off the branch, merged back in task-number order; serializing is always safe and is the default when in doubt.

**Deployment guard:** this branch contains the breaking migration (S5) long before the CLI/docs/examples catch up (Parts 3–5). The branch is **never deployed** to any long-lived environment — only to disposable e2e/OrbStack clusters. Real installs only ever upgrade via maintainer-tagged releases cut from `main` after the PR merges whole (existing repo release discipline), so a partially-compatible deployment cannot occur.

## Cross-part interface contract (fixed — Parts reference these names verbatim)

- `config.Pipeline{Name, Description string; Gateway GatewayConfig; Stages []Stage}` — envelope fields (`APIVersion`, `Kind`, `Metadata`, `Spec`) deleted; `Gateway`/`Stages` hoisted to top level (`pkg/pipeline/config/types.go`).
- `config.Parse(data []byte) (*Pipeline, error)` — keeps its current public shape (S0 verifies the exact signature; if today's returns findings too, keep that).
- `validate.ValidatePipelineDoc(raw []byte, contextName string, reg RegistryView, pol *Policy) (*datupletv1.Pipeline, []Finding)` — NEW front door: strict doc decode → name-context check → `DocToCR` → shared rules.
- `validate.ValidateTyped(p *datupletv1.Pipeline, reg RegistryView, pol *Policy) []Finding` — UNCHANGED signature (operator path: `pipeline_controller.go:91`, `pipelinerun_controller.go:419`).
- `config.DocToCR(p *Pipeline) *datupletv1.Pipeline` / `config.CRToDoc(cr *datupletv1.Pipeline) *Pipeline` — the ONLY doc⇄CR conversion (in `convert.go`); owns the field renames `as`↔`logicalName`, `partitionSpec[].source_column`↔`partitionFields[].sourceColumn`.
- `config.RenderYAML(p *Pipeline) ([]byte, error)` — deterministic YAML rendering (struct field order; `gopkg.in/yaml.v3` sorts map keys).
- DB: `pipelines.doc jsonb NOT NULL` + `pipelines.description text NOT NULL DEFAULT ''` (migration `012_pipelines_doc.sql`, destructive: `TRUNCATE pipelines CASCADE`).
- `http.PipelineStore`: `Put(ctx, projectID, name string, doc []byte, description string) error`; `Get(ctx, projectID, name string) (*PipelineDetail, error)` where `PipelineDetail{ID, Name string; Doc json.RawMessage; CreatedAt, UpdatedAt string}`; `GetDocByID(ctx, id string) ([]byte, error)`; `List(...)` items gain `Description string`.
- API JSON: detail `{id, name, doc, created_at, updated_at}` (`doc` = JSON object, not string); `GET ?format=yaml` → `text/yaml` body from `RenderYAML`; list item `{name, description, created_at, updated_at}`; `POST /api/v1/projects/{pid}/pipelines/validate` → `200 {"findings":[{path,message,severity}]}` for every readable body, `400`/`413` unreadable/oversized, `5xx` infra only.
- CRD: `ComponentDefinitionSpec.IO *ComponentIO` with `ComponentIO{Inputs, Outputs string}` (`none|optional|required`, empty = `optional`).
- Catalog JSON (list + detail): `io: {"inputs": "...", "outputs": "..."}`.
- Schema annotations (the complete known set): `x-datuplet-secret`, `x-datuplet-multiline`, `x-datuplet-advanced`, `x-datuplet-doc`, `x-datuplet-produces` (schema root only).
- CLI env vars: `DATUPLET_API_TOKEN` (precedence `--token-file` > env > `~/.datuplet/api-token`), `DATUPLET_REMOTE` (precedence `--remote` > env > `~/.datuplet/cluster.json`).
- UI: `ui/product/lib/schema-form.js` keeps its export `buildSchemaForm(container, schemaStr, initialValue, opts) → {getValue, getErrors, destroy}` (new recursive internals); vendored parser at `ui/product/vendor/js-yaml.mjs`.

## Global Constraints (every task implicitly includes these)

- **Never push `main`, never `git tag`.** All work on `feat/rfc-027-schema-first-config`; lands via the one draft PR.
- `go build ./... && go test ./...` green **before every commit**; `make tidy` (never bare `go mod tidy`) after any `go.mod` change — multi-module repo, drift fails CI.
- **CRD manifests are manually maintained in ONE place:** `charts/datuplet-app/crds/` (the old `utils/deploy/k8s/crds/` copy no longer exists — verified 2026-07-18). CRD type changes with pointer fields require `DeepCopyInto` updates.
- **Never use `filepath.Join`/`path.Join` for storage paths** (repo rule; not expected in this RFC, stated for completeness).
- POC greenfield (spec §2): no deprecation shims, no dual-read, no data conversion. Legacy envelope bodies are **rejected** with the pointed finding.
- macOS/BSD environment: use file-edit tooling, not `sed -i`/GNU flags.
- Conventional commits (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`), one logical commit per task.
- Chart/CRD/controller changes require `make e2e-k8s` against an OrbStack cluster before the PR is marked ready (Part 5 gate); unit-level gates per Part are listed in each gate task.
- The spec (Draft v4) is authoritative. If a task's code anchor (file:line) has drifted, follow the *named symbol*, not the stale line number, and note the drift in the task's commit message.

## Harness notes (orchestrator contract)

- **Subagent dispatch:** give each subagent its full task text verbatim, plus the Global Constraints and the Cross-part interface contract, the branch/worktree to use, and nothing else. Suggested Agent-tool model per task is in each Part's index.
- **Per-task Codex gate (after the subagent commits, before the next task):** run a Codex review of the task's commit via the codex plugin (companion `task` command with the commit SHA, e.g. `node ~/.claude/plugins/cache/openai-codex/codex/*/scripts/codex-companion.mjs task "Review commit <SHA> on branch feat/rfc-027-schema-first-config for correctness bugs and contract violations against docs/superpowers/specs/2026-07-17-rfc-027-schema-first-pipeline-config-design.md"`). Acceptance: **zero CRITICAL or MAJOR findings on the task's diff**. MINOR findings: fix if ≤5 min, otherwise record in the PR description. Findings to fix → dispatch a fixer subagent with the finding text verbatim; re-run the gate.
- **Phase gate (last task of each Part):** cumulative Codex review with base `<phase-start SHA>`, then the PR step per the Branching model.

---

<!-- ═══════════════ Part 1 — Server core ═══════════════ -->

# Part 1 — Server: PipelineDoc model, validator re-point, store, API, trigger

**Goal:** The server stores, validates, serves, and triggers envelope-free PipelineDocs (spec §3, §5). After S9: `PUT` accepts doc JSON/YAML and rejects envelopes, `GET` returns `doc` (+ `?format=yaml`), `POST …/validate` works, trigger runs end-to-end from a stored doc.

**Architecture:** One shape change flows outward from `pkg/pipeline/config` (types + parser) through `pkg/pipeline/validate` (doc front door; CRD entry retained for the operator), `convert.go` (single doc⇄CR point), the store (JSONB), the HTTP handlers, and the runbackend. TDD throughout; the doc⇄CR round-trip test is the drift tripwire.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| S0 | Preflight: verify anchors + commit spec/plan/mockup | sonnet | — | no |
| S1 | Slim `config.Pipeline` + strict doc decode + envelope rejection | opus | S0 | no |
| S2 | `DocToCR`/`CRToDoc` + lossless round-trip test | sonnet | S1 | no |
| S3 | `validate.ValidatePipelineDoc` front door; keep `ValidateTyped`; input-check demotion | opus | S2 | no |
| S4 | `config.RenderYAML` deterministic + golden test | sonnet | S1 | with S3 |
| S5 | Migration 012 + `store` retype + mechanical HTTP compile-fix | sonnet | S3 | no |
| S6 | API behavior: content negotiation, `?format=yaml`, list description | opus | S4, S5 | no |
| S7 | `POST …/pipelines/validate` endpoint | sonnet | S6 | with S8 |
| S8 | Trigger path: runbackend doc contract + timeline | opus | S3, S5 | with S7 |
| S9 | Part 1 gate: full test, cumulative Codex review, open THE draft PR | sonnet | S1–S8 | no |

### Task S0: Preflight — anchors + commit 0

**Files:** none created; commits `docs/superpowers/specs/2026-07-17-rfc-027-*` and `docs/superpowers/plans/2026-07-18-rfc-027-implementation-plan.md`.

- [ ] **Step 1:** `git checkout -b feat/rfc-027-schema-first-config main` (or continue if it exists).
- [ ] **Step 2:** Verify every Part-1 anchor still exists; record actual line numbers in a scratch note for the orchestrator:

```bash
grep -n "APIVersion string" pkg/pipeline/config/types.go        # expect ~line 6
grep -n "func Parse" pkg/pipeline/config/parser.go               # record exact signature
grep -n "func ValidateTyped\|func ValidatePipeline" pkg/pipeline/validate/validate.go
grep -n "ValidateTyped" pkg/k8s/controllers/pipeline_controller.go pkg/k8s/controllers/pipelinerun_controller.go
grep -n "PipelineYAML" pkg/pipelineapi/runbackend/backend.go
grep -n "GetYAMLByID" pkg/pipelineapi/http/stores.go pkg/pipelineapi/http/run_handlers.go
grep -n "buildTimeline" pkg/pipelineapi/http/run_timeline.go
ls pkg/pipelineapi/db/migrations/ | tail -3                      # expect 011_* as latest
```

- [ ] **Step 3:** If `config.Parse`'s signature differs from the contract's assumption, update the "Cross-part interface contract" section of the committed plan in the same commit (plan is code).
- [ ] **Step 4:** Commit: `git add docs/superpowers && git commit -m "docs: RFC 027 spec, UX mockup, implementation plan (commit 0)"`

### Task S1: Slim `config.Pipeline` + strict decode + envelope rejection

**Files:**
- Modify: `pkg/pipeline/config/types.go` (delete `APIVersion`, `Kind`, `Metadata`; hoist `Spec.Gateway`/`Spec.Stages`; add `Description`)
- Modify: `pkg/pipeline/config/parser.go`
- Test: `pkg/pipeline/config/parser_test.go`

**Interfaces:**
- Produces: `config.Pipeline{Name, Description string; Gateway GatewayConfig; Stages []Stage}`; parse errors for envelope keys and unknown fields carry the exact message strings below (S6/S7 surface them as findings).

- [ ] **Step 1: Write the failing tests** (add to `parser_test.go`):

```go
func TestParseRejectsLegacyEnvelope(t *testing.T) {
	body := []byte("apiVersion: datuplet.io/v1\nkind: Pipeline\nmetadata:\n  name: x\nspec:\n  stages: []\n")
	_, err := Parse(body)
	if err == nil || !strings.Contains(err.Error(), "legacy Kubernetes CR format") {
		t.Fatalf("want legacy-format error, got %v", err)
	}
}

func TestParseEnvelopeFreeDoc(t *testing.T) {
	body := []byte("name: events-etl\nstages:\n  - name: s1\n    components:\n      - name: c1\n        component: data-generator\n        config: {tables: [{name: events}]}\n        outputs: {defaultBucket: raw}\n")
	p, err := Parse(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if p.Name != "events-etl" || len(p.Stages) != 1 {
		t.Fatalf("bad doc: %+v", p)
	}
}

func TestParseRejectsUnknownTopLevelField(t *testing.T) {
	_, err := Parse([]byte("name: x\nbogus: 1\nstages: []\n"))
	if err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("want unknown-field error naming the key, got %v", err)
	}
}
```

- [ ] **Step 2:** Run: `go test ./pkg/pipeline/config/ -run TestParse -v` — expect FAIL (envelope fields still exist).
- [ ] **Step 3:** Implement. In `types.go`, the new top-level model:

```go
// Pipeline is the envelope-free PipelineDoc (RFC 027 §3). Canonical as JSON;
// YAML is a human rendering. Field names are normative — the CRD's diverging
// JSON names (logicalName, partitionFields/sourceColumn) are mapped in
// convert.go only.
type Pipeline struct {
	Name        string        `yaml:"name,omitempty" json:"name,omitempty"`
	Description string        `yaml:"description,omitempty" json:"description,omitempty"`
	Gateway     GatewayConfig `yaml:"gateway,omitempty" json:"gateway,omitempty"`
	Stages      []Stage       `yaml:"stages" json:"stages"`
}
```

Delete `Metadata`; keep every nested type (`Stage`, `Component`, `InputSpec`, `OutputSpec`, …) unchanged but add matching `json:` tags mirroring each `yaml:` tag (the doc is JSON-canonical). In `parser.go`, before strict decode, probe for the envelope:

```go
var probe map[string]any
if err := yaml.Unmarshal(data, &probe); err != nil { return nil, fmt.Errorf("parse pipeline doc: %w", err) }
for _, k := range []string{"apiVersion", "kind", "metadata"} {
	if _, ok := probe[k]; ok {
		return nil, errors.New("legacy Kubernetes CR format — Datuplet pipelines are envelope-free now; see docs/pipeline-api.md")
	}
}
```

then strict-decode with `yaml.v3` `KnownFields(true)` (or `sigs.k8s.io/yaml` `UnmarshalStrict`, matching whichever the file already imports) so unknown fields error naming the key. Update **every** compile break in the package (`convert.go` will break — stub minimally; S2 finishes it) and run `go build ./...` to enumerate external breaks; fix only those inside `pkg/pipeline/…` here, leave `pkg/pipelineapi`/controllers breaks for S3–S8 **unless the build must pass** — it must, so where an external caller reads `p.Spec.Stages` or `p.Metadata.Name`, mechanically rewrite to `p.Stages` / `p.Name` without behavior change.
- [ ] **Step 4:** Run: `go build ./... && go test ./pkg/pipeline/... -v` — expect PASS.
- [ ] **Step 5:** Commit: `feat: envelope-free PipelineDoc model + strict parser (RFC 027 S1)`

### Task S2: `DocToCR` / `CRToDoc` + lossless round-trip

**Files:**
- Modify: `pkg/pipeline/config/convert.go`
- Test: `pkg/pipeline/config/convert_test.go`

**Interfaces:**
- Produces: `func DocToCR(p *Pipeline) *datupletv1.Pipeline` (fills `TypeMeta{APIVersion: "datuplet.io/v1", Kind: "Pipeline"}`, `ObjectMeta{Name: p.Name}`) and `func CRToDoc(cr *datupletv1.Pipeline) *Pipeline`. These are the ONLY places that touch the renames `as`↔`logicalName` and `source_column`↔`sourceColumn` (spec §3 field-name authority; today's CR→runtime mapping at `convert.go:99` is the base).

- [ ] **Step 1: Write the failing round-trip test:**

```go
func TestDocCRDocRoundTripLossless(t *testing.T) {
	doc := &Pipeline{
		Name: "rt", Description: "d",
		Gateway: GatewayConfig{ChunkSize: 1024},
		Stages: []Stage{{Name: "s", Components: []Component{{
			Name: "c", Component: "sql-transform", Version: "1.2.3",
			Config: map[string]any{"sql": "SELECT 1"},
			Inputs: &InputSpec{Tables: []InputTableSpec{{Bucket: "raw", Table: "t", As: "alias", Since: "3d", TimestampColumn: "ts"}}},
			Outputs: &OutputSpec{Tables: []OutputTableSpec{{Name: "o", Bucket: "cur", WriteMode: "APPEND",
				PartitionSpec: []PartitionFieldSpec{{SourceColumn: "day", Transform: "day"}}}}},
			Resources: &ResourceSpec{Memory: "1Gi", CPU: "500m"},
		}}}},
	}
	got := CRToDoc(DocToCR(doc))
	if diff := cmp.Diff(doc, got); diff != "" {
		t.Fatalf("round trip lost data (-want +got):\n%s", diff)
	}
}
```

(Use `github.com/google/go-cmp/cmp` if already a dependency; otherwise `reflect.DeepEqual` with a `%#v` failure dump.)
- [ ] **Step 2:** Run: `go test ./pkg/pipeline/config/ -run RoundTrip -v` — expect FAIL (functions missing).
- [ ] **Step 3:** Implement both directions field-by-field over the existing conversion code, explicitly mapping `As ↔ LogicalName` and `PartitionSpec[].SourceColumn ↔ PartitionFields[].SourceColumn`. No reflection magic — plain struct copies; the test is the tripwire.
- [ ] **Step 4:** Run: `go build ./... && go test ./pkg/pipeline/config/ -v` — PASS.
- [ ] **Step 5:** Commit: `feat: DocToCR/CRToDoc single conversion point + lossless round-trip test (RFC 027 S2)`

### Task S3: Validator re-point — doc front door, CRD entry retained, input-check demotion

**Files:**
- Modify: `pkg/pipeline/validate/validate.go`
- Modify: `pkg/pipeline/config/parser.go` (delegate)
- Test: `pkg/pipeline/validate/validate_test.go`

**Interfaces:**
- Produces: `validate.ValidatePipelineDoc(raw []byte, contextName string, reg RegistryView, pol *Policy) (*datupletv1.Pipeline, []Finding)`. `ValidateTyped` keeps its exact current signature (operator callers: `pipeline_controller.go:91`, `pipelinerun_controller.go:419` — MUST NOT change).
- Consumes: S1 parser (strict doc decode + envelope error), S2 `DocToCR`.

- [ ] **Step 1: Write the failing tests:**

```go
func TestValidatePipelineDocEnvelopeRejected(t *testing.T) {
	_, fs := ValidatePipelineDoc([]byte("apiVersion: v1\nkind: Pipeline\n"), "x", nil, nil)
	requireFinding(t, fs, "error", "legacy Kubernetes CR format")
}

func TestValidatePipelineDocNameContext(t *testing.T) {
	body := []byte("name: other\nstages: []\n")
	_, fs := ValidatePipelineDoc(body, "route-name", nil, nil)
	requireFinding(t, fs, "error", `name "other" does not match`)
	// Empty context (POST /validate, no name): equality check skipped.
	_, fs = ValidatePipelineDoc(body, "", nil, nil)
	forbidFinding(t, fs, "does not match")
}

func TestUpstreamInputDemotedToWarning(t *testing.T) {
	body := []byte(`name: p
stages:
  - name: a
    components:
      - {name: c1, component: x, outputs: {defaultBucket: raw}}
  - name: b
    components:
      - name: c2
        component: y
        inputs: {tables: [{bucket: staging, table: preexisting}]}
        outputs: {defaultBucket: out}
`)
	_, fs := ValidatePipelineDoc(body, "p", nil, nil)
	f := findByPath(t, fs, "stages[1].components[0].inputs.tables[0]")
	if f.Severity != "warning" || !strings.Contains(f.Message, "assumed to pre-exist in storage") {
		t.Fatalf("want warning about pre-existing storage table, got %+v", f)
	}
}
```

(`requireFinding`/`forbidFinding`/`findByPath` — small local test helpers; write them in this file.)
- [ ] **Step 2:** Run: `go test ./pkg/pipeline/validate/ -run "Doc|Demoted" -v` — FAIL.
- [ ] **Step 3:** Implement `ValidatePipelineDoc`: (a) envelope probe + strict decode into `config.Pipeline` — reuse `config.Parse` internals or call it (avoid duplicating the strict decode; if `Parse` returns only `error`, wrap that error as one `Finding{Path:"", Severity:"error"}`); (b) name context: DNS-1123 check on the effective name (`contextName`, else doc `name`), equality finding when both present and different; (c) `cr := config.DocToCR(doc)`; (d) `findings = append(findings, ValidateTyped(cr, reg, pol)...)`. Inside the existing cross-stage input check (today's error at the `!availableTables[tableKey] && !availableBuckets[t.Bucket]` branch, `validate.go:326-336`), change `Severity` to `"warning"` and the message to `... not produced by an earlier stage — assumed to pre-exist in storage; the run fails at read time if it doesn't`. Re-point `config.Parse` to the doc path if it still validates through the old CR-decode entry (`parser.go:30-38`).
- [ ] **Step 4:** Run: `go build ./... && go test ./pkg/pipeline/... -v` — PASS. Also `go test ./pkg/k8s/... -count=1` (operator callers untouched, must stay green).
- [ ] **Step 5:** Commit: `feat: doc-shaped validator front door; upstream-input check demoted to warning (RFC 027 S3)`

### Task S4: Deterministic YAML rendering

**Files:**
- Create: `pkg/pipeline/config/render.go`
- Test: `pkg/pipeline/config/render_test.go` + golden file `pkg/pipeline/config/testdata/render_golden.yaml`

**Interfaces:**
- Produces: `config.RenderYAML(p *Pipeline) ([]byte, error)`.

- [ ] **Step 1: Failing test** — build the spec §3 example doc in Go, then:

```go
func TestRenderYAMLGolden(t *testing.T) {
	got, err := RenderYAML(exampleDoc())
	if err != nil { t.Fatal(err) }
	want, _ := os.ReadFile("testdata/render_golden.yaml")
	if !bytes.Equal(got, want) { t.Fatalf("golden mismatch:\n%s", got) }
}

func TestRenderYAMLDeterministic(t *testing.T) {
	a, _ := RenderYAML(exampleDoc()); b, _ := RenderYAML(exampleDoc())
	if !bytes.Equal(a, b) { t.Fatal("nondeterministic render") }
}

func TestRenderParseRoundTrip(t *testing.T) {
	y, _ := RenderYAML(exampleDoc())
	p, err := Parse(y)
	if err != nil { t.Fatalf("re-parse: %v", err) }
	if p.Name != exampleDoc().Name || len(p.Stages) != len(exampleDoc().Stages) { t.Fatal("lossy render") }
}
```

- [ ] **Step 2:** Run — FAIL. Generate the golden by running the implementation once and eyeballing it (config map keys sorted is EXPECTED — yaml.v3 behavior).
- [ ] **Step 3:** Implement: `RenderYAML` = `yaml.Marshal(p)` via `gopkg.in/yaml.v3` with a 2-space-indent `yaml.Encoder`. Struct field order comes from the S1 type; that is the whole determinism story.
- [ ] **Step 4:** `go test ./pkg/pipeline/config/ -v` — PASS.
- [ ] **Step 5:** Commit: `feat: deterministic PipelineDoc YAML rendering (RFC 027 S4)`

### Task S5: Migration 012 + store retype + mechanical HTTP compile-fix

**Files:**
- Create: `pkg/pipelineapi/db/migrations/012_pipelines_doc.sql`
- Modify: `pkg/pipelineapi/store/pipeline.go`, `pkg/pipelineapi/http/stores.go`, `pkg/pipelineapi/http/stores_pgx.go`, `pkg/pipelineapi/http/pipeline_handlers.go`, `pkg/pipelineapi/http/run_handlers.go` (mechanical only)
- Test: `pkg/pipelineapi/store/pipeline_test.go`

**Interfaces:**
- Produces: `store.Pipeline{…, Description string, Doc []byte}` (drop `YAML string`); `CreatePipeline(ctx, pool, projectID uuid.UUID, name, description string, doc []byte)`; `UpdatePipeline` symmetric; `GetDocByID(ctx, pool, id) ([]byte, error)`; list scan includes `description`. PLUS the **mechanical** retype of the HTTP layer so `go build ./... && go test ./...` stays green at this commit: `PipelineStore` interface + `stores_pgx.go` + every handler compile-fixed to the doc-typed store, with PUT validating via `ValidatePipelineDoc` (S3) and storing canonical JSON, GET returning `doc` as a raw JSON field. **No new behavior** — no content negotiation, no `?format=yaml`, no list `description` in the response yet (all S6). Split rationale: S5 = mechanical retype end-to-end (reviewable as "same behavior, new shape"), S6 = the new API contract.

- [ ] **Step 1:** Write `012_pipelines_doc.sql`:

```sql
-- RFC 027: envelope-free PipelineDoc, JSONB-canonical. DESTRUCTIVE (POC):
-- stored pipelines are discarded and, via runs.pipeline_id ON DELETE CASCADE,
-- run history goes with them. Release notes call this out.
TRUNCATE pipelines CASCADE;
ALTER TABLE pipelines DROP COLUMN yaml;
ALTER TABLE pipelines ADD COLUMN doc jsonb NOT NULL;
ALTER TABLE pipelines ADD COLUMN description text NOT NULL DEFAULT '';
```

- [ ] **Step 2:** Failing store test (follow the existing test harness in `pipeline_test.go` — pgx test pool): create with a doc `{"name":"x","stages":[]}` + description "d", read back `Doc` byte-equal (JSONB normalizes key order — compare via `json.Unmarshal` to `map[string]any` + `reflect.DeepEqual`, NOT `bytes.Equal`), list returns description.
- [ ] **Step 3:** Run: `go test ./pkg/pipelineapi/store/ -run Pipeline -v` — FAIL; retype `pipeline.go` (every `yaml` column/field → `doc`/`description`); run again — PASS.
- [ ] **Step 4:** Mechanically retype the HTTP layer per the Interfaces block (interface, pgx adapter, handlers, and the run-timeline read at `run_handlers.go:229` → `GetDocByID` + parse). Behavior-preserving only; existing handler tests updated in kind (bodies become docs), not in coverage.
- [ ] **Step 5:** `go build ./... && go test ./... ` — green (this is the whole point of the widened scope).
- [ ] **Step 6:** Commit: `feat: pipelines store doc jsonb + destructive migration 012; mechanical retype (RFC 027 S5)`

**Backout note (record in the commit message + PR body):** once migration 012 has run against a database, previous binaries (expecting `pipelines.yaml`) cannot serve it. Rollback = redeploy the previous chart **and** restore the DB from a pre-upgrade dump. POC posture: no automated backout; the release notes (E4) tell operators to take a `pg_dump` first if they care, and to cancel active runs before upgrading.

### Task S6: API behavior — content negotiation, `?format=yaml`, list description

**Files:**
- Modify: `pkg/pipelineapi/http/pipeline_handlers.go`
- Test: `pkg/pipelineapi/http/pipeline_handlers_test.go`

**Interfaces:**
- Consumes: S3 `ValidatePipelineDoc`, S4 `RenderYAML`, S5's already-retyped store/handler layer.
- Produces: the API contract of spec §5.2 — detail `{id,name,doc,created_at,updated_at}`, `?format=yaml`, list `{name,description,…}`, PUT content negotiation (`Content-Type: application/json` → body validated then stored as canonical JSON; anything else → YAML, validated then converted via `json.Marshal(parsedDoc)` before storing).

- [ ] **Step 1: Failing handler tests** (extend the existing test style in `pipeline_handlers_test.go`):

```go
// PUT JSON doc → 204; GET returns doc object; LIST includes description.
// PUT YAML doc → 204; GET ?format=yaml returns text/yaml, re-parseable via config.Parse.
// PUT legacy envelope → 400 with finding containing "legacy Kubernetes CR format".
// PUT doc whose name != URL name → 400 finding "does not match".
// Resource gate behavior preserved: non-superadmin adding resources → 403
// (StatusForbidden — the EXISTING contract at pipeline_handlers.go:207; keep it).
```

Write them as real tests against the existing test server harness — copy the arrange/act/assert shape of the current PUT tests, swapping bodies for docs.
- [ ] **Step 2:** Run: `go test ./pkg/pipelineapi/http/ -run Pipeline -v` — FAIL.
- [ ] **Step 3:** Implement in `handlePutPipeline` (1 MiB cap unchanged), `handleGetPipeline` (`?format=yaml` → `RenderYAML`, `Content-Type: text/yaml`), `handleListPipelines` (+`description`). The resource/gateway diff-gate logic (`pipeline_handlers.go:183-218`) keeps its shape AND its 403 status — it now diffs parsed docs, not YAML strings.
- [ ] **Step 4:** `go build ./... && go test ./pkg/pipelineapi/... -v` — PASS.
- [ ] **Step 5:** Commit: `feat: pipeline API speaks PipelineDoc json+yaml (RFC 027 S6)`

### Task S7: Validate endpoint

**Files:**
- Modify: `pkg/pipelineapi/http/pipeline_handlers.go`, `pkg/pipelineapi/http/server.go` (route)
- Test: `pkg/pipelineapi/http/pipeline_handlers_test.go`

**Interfaces:**
- Produces: `POST /api/v1/projects/{pid}/pipelines/validate` per spec §5.2 status contract; response body `{"findings":[{"path","message","severity"}]}` — the SAME findings JSON shape PUT uses.

- [ ] **Step 1: Failing tests:** valid doc → `200 {"findings":[]}` and nothing stored; doc with errors → `200` with findings (NOT 400); unreadable body → 400; `?name=x` with stored pipeline → resource-gate diff runs against stored doc (fixture: non-superadmin keeping identical resources → no finding; adding → finding); no name → create semantics (non-superadmin + resources → finding).
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Implement `handleValidatePipeline`: same body-read + `ValidatePipelineDoc` as PUT with `contextName` from optional `?name=`; skip the store write; store read failure → `http.Error(w, "…", 500)`. Route: `mux.Handle("POST /api/v1/projects/{pid}/pipelines/validate", auth.WithUser(...))` — register BEFORE the `{name}` PUT route cannot collide (`validate` is not a valid create target anyway; add a guard in PUT rejecting the reserved name `validate` with a finding).
- [ ] **Step 4:** `go test ./pkg/pipelineapi/http/ -v` — PASS.
- [ ] **Step 5:** Commit: `feat: POST pipelines/validate — findings without persisting (RFC 027 S7)`

### Task S8: Trigger + timeline on the doc

**Files:**
- Modify: `pkg/pipelineapi/runbackend/backend.go`, `pkg/pipelineapi/runbackend/k8s.go`, `pkg/pipelineapi/k8s/pipeline_apply.go`, `pkg/pipelineapi/http/run_handlers.go`, `pkg/pipelineapi/http/run_timeline.go`
- Test: extend `pkg/pipelineapi/runbackend/k8s_test.go` + `run_timeline` tests

**Interfaces:**
- Consumes: S2 `DocToCR`, S5 `GetDocByID`.
- Produces: `TriggerRequest.Doc *config.Pipeline` (replaces `PipelineYAML []byte`, `backend.go:26-32`); `ApplyPipelineCRD(ctx context.Context, c client.Client, namespace string, doc *config.Pipeline) error` — the **namespace parameter stays** (today's signature takes `namespace, pipelineYAML string` and forces `pl.Namespace = namespace` at `pipeline_apply.go:29`; the caller gets it from `ProjectNS.Ensure` at `runbackend/k8s.go:212-216` — that flow is unchanged, only the body param becomes the doc rendered via `DocToCR`); `buildTimeline(doc *config.Pipeline, …)` (replaces the YAML param, `run_timeline.go:48`); the trigger handler parses the stored doc exactly once (`run_handlers.go:83-100` double-parse collapses).

- [ ] **Step 1:** Failing tests: `k8s_test.go` — trigger with a doc fixture produces a `Pipeline` CR whose `ObjectMeta.Name`, stage/component fields match `DocToCR(doc)`; timeline test — `buildTimeline` over a doc fixture yields the same stage set as before (port one existing YAML-based case to a doc case).
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Mechanical re-plumb per the Interfaces block. `buildRunTuples`/`PipelineIntentFromPipeline` (`k8s.go:502-506`) already consume `*config.Pipeline` — they keep working with the slimmed type.
- [ ] **Step 4:** `go build ./... && go test ./pkg/pipelineapi/... ./pkg/k8s/... -v` — PASS.
- [ ] **Step 5:** Commit: `feat: trigger + timeline consume the parsed PipelineDoc (RFC 027 S8)`

### Task S9: Part 1 gate

- [ ] **Step 1:** `go build ./... && go test ./... && make tidy && git diff --exit-code go.*`
- [ ] **Step 2:** Cumulative Codex review, base = Part-1 `<phase-start SHA>`; fix CRITICAL/MAJOR.
- [ ] **Step 3:** Push branch; open THE draft PR (Branching model). Body: Part 1 summary + "destructive migration 012 wipes pipelines AND run history (POC)".
- [ ] **Step 4:** Live smoke on OrbStack (optional here, mandatory at Part 5): `helm upgrade` the app chart, PUT the spec §3 example via `curl`, trigger, watch it run.

<!-- ═══════════════ Part 2 — Schemas + registry ═══════════════ -->

# Part 2 — Component schemas as files, Form-Subset lint, IO capability

**Goal:** `components/<name>/schema.json` is the single schema source (synced into the chart, CI-guarded), every built-in schema is lint-clean (full Form-Subset coverage, descriptions everywhere), and `ComponentDefinition` carries `io` end-to-end (CRD → chart → catalog API → validation). Spec §4.

**Architecture:** Pure additive registry work; no pipeline-doc coupling beyond the S3 validator gaining io rules. Schema authoring is deliberately split into two tasks so the deep schemas (data-generator, sql-transform) get focused review.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| R0 | Preflight: rebase + re-verify Part-2 anchors | sonnet | Part 1 gate | no |
| R1 | `ComponentIO` CRD type + DeepCopy + CRD manifest | sonnet | R0 | with R3a/R3b |
| R2 | Catalog API exposes `io`; validation enforces io rules | sonnet | R1 | with R3a/R3b |
| R3a | Author `schema.json`: data-generator + sql-transform (full coverage) | opus | R0 | with R1, R3b |
| R3b | Author `schema.json`: http-json-extractor, finnhub-extractor, pandas-transform, stdout-writer | sonnet | R0 | with R1, R3a |
| R4 | `make sync-component-schemas` + chart `.Files.Get` + chart `io` fields | sonnet | R1, R3a, R3b | no |
| R5 | Form-Subset schema lint (Go test) | opus | R3a, R3b | with R4 |
| R6 | CI drift job | sonnet | R4 | no |
| R7 | Part 2 gate | sonnet | R1–R6 | no |

### Task R0: Preflight

- [ ] Rebase branch on latest `main`; `go build ./... && go test ./...` green.
- [ ] Verify anchors: `grep -n "VersionSpec\|DefaultVersion" pkg/k8s/api/v1/component_types.go`; `grep -rn "configSchema" charts/datuplet-app/templates/components/ | head`; `grep -n "type componentDetail\|type componentListItem\|io" pkg/pipelineapi/http/component_handlers.go` (record actual response-struct names); `ls charts/datuplet-app/crds/`.

### Task R1: `ComponentIO` CRD field

**Files:**
- Modify: `pkg/k8s/api/v1/component_types.go`, `pkg/k8s/api/v1/zz_generated.deepcopy.go` (or wherever `DeepCopyInto` for `ComponentDefinitionSpec` lives — R0 verified)
- Modify: `charts/datuplet-app/crds/datuplet.io_componentdefinitions.yaml`
- Test: `pkg/k8s/api/v1/component_types_test.go`

**Interfaces:**
- Produces: `ComponentDefinitionSpec.IO *ComponentIO`; `ComponentIO{Inputs string `json:"inputs,omitempty"`, Outputs string `json:"outputs,omitempty"`}`; helper `func (io *ComponentIO) InputsMode() string` returning `"optional"` for nil/empty (same for `OutputsMode`) — all consumers go through the helpers so the nil default lives once.

- [ ] **Step 1: Failing test:**

```go
func TestComponentIODefaults(t *testing.T) {
	var io *ComponentIO
	if io.InputsMode() != "optional" || io.OutputsMode() != "optional" {
		t.Fatal("nil ComponentIO must default to optional/optional")
	}
	io = &ComponentIO{Inputs: "none", Outputs: "required"}
	if io.InputsMode() != "none" || io.OutputsMode() != "required" { t.Fatal("explicit values must pass through") }
}

func TestComponentIODeepCopy(t *testing.T) {
	in := &ComponentDefinitionSpec{IO: &ComponentIO{Inputs: "none"}}
	out := in.DeepCopy()
	out.IO.Inputs = "required"
	if in.IO.Inputs != "none" { t.Fatal("DeepCopy aliased the IO pointer") }
}
```

- [ ] **Step 2:** Run: `go test ./pkg/k8s/api/v1/ -run ComponentIO -v` — FAIL.
- [ ] **Step 3:** Add the type + helpers + `DeepCopyInto` for the new pointer field (Global Constraints: manual DeepCopy). CRD manifest: add under `spec.versions[0].schema...properties.spec.properties`:

```yaml
io:
  type: object
  properties:
    inputs:  { type: string, enum: ["", "none", "optional", "required"] }
    outputs: { type: string, enum: ["", "none", "optional", "required"] }
```

- [ ] **Step 4:** `go build ./... && go test ./pkg/k8s/... -v` — PASS.
- [ ] **Step 5:** Commit: `feat: ComponentDefinition io capability field (RFC 027 R1)`

### Task R2: Catalog exposes `io`; validation enforces it

**Files:**
- Modify: `pkg/pipelineapi/http/component_handlers.go` (response structs, ~`:30-40`)
- Modify: `pkg/pipeline/validate/validate.go`
- Test: `pkg/pipelineapi/http/component_handlers_test.go`, `pkg/pipeline/validate/validate_test.go`

**Interfaces:**
- Consumes: R1 helpers.
- Produces: catalog list+detail JSON gain `"io": {"inputs": "...", "outputs": "..."}` (always present, defaults `"optional"`); validation rules — `io.inputs==none` + declared inputs → **error** `component <n> takes no inputs`; `io.inputs==required` + none declared → **error** `component <n> requires at least one input`; symmetric for outputs. **Plumbing prerequisite (do first):** extend `validate.ResolvedComponent` (`pkg/pipeline/validate/registry.go:25` — currently `Component/Version/Image/Prerelease/ConfigSchema/Resources`, no IO) with `IO *datupletv1.ComponentIO`, populate it in `StaticRegistry.Resolve` (`registry.go:74`), and update every `RegistryView` implementation + test fake (the pipelineapi `registry.View` delegates to `StaticRegistry` so it inherits the field). Only then add the rules where the resolved component is already in hand.

- [ ] **Step 1:** Failing tests: handler test asserts `io` in list+detail JSON for a fixture ComponentDefinition with `io: {inputs: none, outputs: required}` AND for one without `io` (expect `optional`/`optional`); validate tests cover the four rule outcomes above using a fake `RegistryView`.
- [ ] **Step 2:** Run — FAIL. **Step 3:** implement. **Step 4:** `go test ./pkg/pipelineapi/http/ ./pkg/pipeline/validate/ -v` — PASS.
- [ ] **Step 5:** Commit: `feat: io capability in catalog API + validation (RFC 027 R2)`

### Task R3a: Author schemas — data-generator + sql-transform

**Files:**
- Create: `components/data-generator/schema.json`, `components/sql-transform/schema.json`

**Interfaces:**
- Produces: lint-clean (R5 rules) schemas with **100% coverage of what the component parsers accept**. Sources of truth to read first: `components/data-generator/config.go` (Table/RandomSpec/LiteralSpec/Limit — including `rowInsertSpeed`, `random.userErrorMessage`, `literal.columns/rows`) and `components/sql-transform` config parsing; the current chart blobs (`charts/datuplet-app/templates/components/{data-generator,sql-transform}.yaml`) are the *starting* content, NOT the authority.

- [ ] **Step 1:** Read both components' config parsers; list every accepted key. Any key in the parser missing from the chart blob gets added (e.g. verify `userErrorMessage` placement); any blob key the parser ignores gets dropped (note it in the commit message).
- [ ] **Step 2:** Write the two schema files. Requirements (spec §4.2/§4.4): draft 2020-12; `additionalProperties: false` on every object; every property has a one-line `description`; long-form guidance in `x-datuplet-doc` (write real, operator-grade text — the spec's §4.4 examples for `threads`/`memory` are the bar); literal `default` ONLY where the component truly applies that literal (dynamic defaults go in `x-datuplet-doc` prose); `x-datuplet-multiline: "sql"` on `sql`; `x-datuplet-advanced: true` on `threads`, `memory`, `temp_directory`, `max_temp_size`; `x-datuplet-produces: "tables[*].name"` at the ROOT of data-generator's schema; data-generator's `random`/`literal` remain sibling optional objects (their exactly-one-of rule is component-enforced — spec §5.4 — note it in both `x-datuplet-doc`s).
- [ ] **Step 3:** Sanity: `python3 -m json.tool components/data-generator/schema.json > /dev/null && python3 -m json.tool components/sql-transform/schema.json > /dev/null`.
- [ ] **Step 4:** Commit: `feat: schema.json for data-generator + sql-transform, full parser coverage (RFC 027 R3a)`

### Task R3b: Author schemas — the remaining four built-ins

**Files:**
- Create: `components/http-json-extractor/schema.json`, `components/finnhub-extractor/schema.json`, `components/pandas-transform/schema.json`, `components/stdout-writer/schema.json`

Same rules and steps as R3a (read each component's config parser first; chart blobs are starting content only; descriptions mandatory; `x-datuplet-multiline: "python"` on pandas code; secrets keep `x-datuplet-secret` where the current blobs have it — e.g. finnhub's API key). Commit: `feat: schema.json for remaining built-ins (RFC 027 R3b)`

### Task R4: Sync target + chart consumes files + chart io

**Files:**
- Modify: `Makefile` (new target `sync-component-schemas`)
- Create: `charts/datuplet-app/files/component-schemas/*.json` (generated by the target, committed)
- Modify: `charts/datuplet-app/templates/components/*.yaml` (6 files)

**Interfaces:**
- Produces: `make sync-component-schemas` — for each `components/*/schema.json`, copy to `charts/datuplet-app/files/component-schemas/<name>.json`. Templates replace the inline `configSchema: |` blob with:

```yaml
      configSchema: |
{{ .Files.Get (printf "files/component-schemas/%s.json" "data-generator") | indent 8 }}
```

and each ComponentDefinition gains its spec-§4.3 `io` block (data-generator/http-json-extractor/finnhub `{inputs: none, outputs: required}`; sql-transform/pandas `{inputs: required, outputs: required}`; stdout-writer `{inputs: required, outputs: none}`).

- [ ] **Step 1:** Makefile target (BSD-safe):

```make
sync-component-schemas: ## Copy components/*/schema.json into the app chart
	@mkdir -p charts/datuplet-app/files/component-schemas
	@for f in components/*/schema.json; do \
		name=$$(basename $$(dirname $$f)); \
		cp "$$f" "charts/datuplet-app/files/component-schemas/$$name.json"; \
	done
```

- [ ] **Step 2:** Run it; edit the 6 templates; `helm template charts/datuplet-app | grep -A3 "configSchema"` renders valid JSON blobs for all six and `io:` blocks appear.
- [ ] **Step 3:** `helm lint charts/datuplet-app` — clean.
- [ ] **Step 4:** Commit: `feat: chart consumes synced component schemas + io declarations (RFC 027 R4)`

### Task R5: Form-Subset schema lint

**Files:**
- Create: `pkg/pipeline/schemalint/schemalint.go`, `pkg/pipeline/schemalint/schemalint_test.go`
- Create: `pkg/pipeline/schemalint/builtin_test.go` (walks `components/*/schema.json` via a relative path from the repo root — use `runtime.Caller` to locate it)

**Interfaces:**
- Produces: `schemalint.Lint(schema []byte) []Issue` with `Issue{Path, Rule, Message string}`. Rules (spec §4.2): (1) parses as JSON, `type` present on every schema node; (2) subset-only — allowed keys per node type; forbidden anywhere: `oneOf,anyOf,allOf,not,$ref,$defs,if,then,else,patternProperties,const`; (3) every property has non-empty `description`; (4) not(`required` contains K ∧ properties[K].default present); (5) unknown `x-datuplet-*` keys (outside the contract's set of five) are errors; `x-datuplet-produces` only at the root.
- `builtin_test.go`: `Lint` over every `components/*/schema.json` must return zero issues — THIS is the "100% form coverage for built-ins" guarantee.

- [ ] **Step 1:** Failing unit tests per rule — one minimal schema fixture per rule violation (write all five inline as Go strings), plus a passing fixture using every allowed construct (nested object, array-of-objects, scalar array, `additionalProperties` enum map, tri-state boolean w/ default, all five annotations).
- [ ] **Step 2:** Run: `go test ./pkg/pipeline/schemalint/ -v` — FAIL. **Step 3:** implement a recursive walker (plain `map[string]any` traversal; no JSON-Schema library needed for these five rules). **Step 4:** all lint tests + `builtin_test.go` PASS (fix R3a/R3b files if the lint catches them — that is the point).
- [ ] **Step 5:** Commit: `feat: Form-Subset schema lint; built-ins lint-clean (RFC 027 R5)`

### Task R6: CI drift job

**Files:**
- Modify: `.github/workflows/` — the workflow that runs repo checks (R0 records which file; follow the RFC 024 `verify-versions` job as the pattern)

- [ ] **Step 1:** Add a job step:

```yaml
- name: Component schema sync check
  run: |
    make sync-component-schemas
    git diff --exit-code charts/datuplet-app/files/component-schemas charts/datuplet-app/templates/components
```

- [ ] **Step 2:** Commit: `ci: fail on component schema drift (RFC 027 R6)`

### Task R7: Part 2 gate

- [ ] `go build ./... && go test ./... && make tidy && git diff --exit-code go.*`; `helm lint charts/datuplet-app`.
- [ ] Cumulative Codex review (base = Part-2 start SHA); fix CRITICAL/MAJOR.
- [ ] Push; append Part 2 summary to the draft PR body.

<!-- ═══════════════ Part 3 — CLI ═══════════════ -->

# Part 3 — CLI: components discovery, validate, doc CRUD, headless auth

**Goal:** `datuplet` speaks the doc natively and gives agents the full loop (spec §7). File-disjoint from Part 2 except C2 (needs R2's `io` in catalog JSON).

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| C0 | Preflight: verify remote.go/pipeline.go anchors | sonnet | Part 1 gate | no |
| C1 | Headless auth: `DATUPLET_API_TOKEN` + `DATUPLET_REMOTE` + `login --password-stdin` | sonnet | C0 | with C3 |
| C2 | `datuplet components list|get` (+ `--schema` client resolution) | sonnet | C0, R2 | with C3, C4 |
| C3 | `pipeline get/put` on the doc | sonnet | C0 | with C1 |
| C4 | `pipeline validate` + exit codes | sonnet | C3 | with C2 |
| C5 | Part 3 gate + agent quickstart doc | sonnet | C1–C4 | no |

### Task C0: Preflight

- [ ] Verify: `grep -n "api-token\|cluster.json" cmd/datuplet/remote.go` (headless seams, ~`:75`); `grep -n "pipelineDetailJSON\|extractMetadataName\|Content-Type" cmd/datuplet/pipeline.go`; `grep -n "case \"" cmd/datuplet/main.go` (dispatch table).

### Task C1: Headless auth

**Files:**
- Modify: `cmd/datuplet/remote.go`, `cmd/datuplet/login.go`, `cmd/datuplet/main.go` (usage text)
- Test: `cmd/datuplet/remote_test.go`, `cmd/datuplet/login_test.go`

**Interfaces:**
- Produces: token resolution precedence `--token-file` > `DATUPLET_API_TOKEN` > `~/.datuplet/api-token`; remote resolution `--remote` > `DATUPLET_REMOTE` > `~/.datuplet/cluster.json`. With both env vars set, NO `~/.datuplet` file is read (test asserts by pointing `$HOME` at an empty temp dir). `login --password-stdin` reads exactly one line from stdin, never prompts.

- [ ] **Step 1:** Failing tests: table-driven precedence tests for both resolutions (temp `$HOME`, env via `t.Setenv`); `--password-stdin` test drives login against a `httptest.Server` stubbing `POST /api/v1/auth/token`.
- [ ] **Step 2:** FAIL → implement in `loadRemoteArgs` + `runLogin` → PASS (`go test ./cmd/datuplet/ -v`).
- [ ] **Step 3:** Commit: `feat: headless CLI auth via env vars + --password-stdin (RFC 027 C1)`

### Task C2: components subcommand

**Files:**
- Create: `cmd/datuplet/components.go`, `cmd/datuplet/components_test.go`
- Modify: `cmd/datuplet/main.go` (dispatch + usage)

**Interfaces:**
- Consumes: `GET /api/v1/components` (list — NOTE: list version entries do **not** carry `configSchema`, per current `component_handlers.go:24`; only `io` is new) and `GET /api/v1/components/{name}` (detail — versions include `configSchema`, `component_handlers.go:42`). C0/R2 verified the exact field names; mirror them in local structs like `pipeline.go` does.
- Produces: `datuplet components list [--json]` (table: NAME, DISPLAY, DEFAULT, IO, DEPRECATED — no schema data needed); `datuplet components get <name> [--version v] [--json|--schema]` — `get` and `--schema` always call the **detail** endpoint. `--schema` version resolution, client-side, in a pure function the test can hit directly:

```go
// resolveVersion: --version if given; else spec.defaultVersion; else the
// highest stable semver among versions (prerelease entries excluded).
func resolveVersion(c componentDetailJSON, want string) (versionJSON, error)
```

- [ ] **Step 1:** Failing tests: `resolveVersion` table (explicit hit, explicit miss → error, default present, default absent → highest stable, all-prerelease → error); list/get against `httptest.Server` fixtures; `--schema` prints the schema bytes verbatim + trailing newline, nothing else.
- [ ] **Step 2:** FAIL → implement (flag parsing mirrors `parsePipelineFlags` — hand-rolled, same UX) → PASS.
- [ ] **Step 3:** Commit: `feat: datuplet components list/get with --schema (RFC 027 C2)`

### Task C3: pipeline get/put on the doc

**Files:**
- Modify: `cmd/datuplet/pipeline.go`
- Test: `cmd/datuplet/pipeline_test.go`

**Interfaces:**
- Produces: `pipelineDetailJSON{ID, Name string; Doc json.RawMessage; CreatedAt, UpdatedAt string}` (drop `YAML`); `get` default output = the server's `?format=yaml` body verbatim; `get --json` = the detail JSON; `put -f` sniffs the first non-whitespace byte (`{` → `application/json`, else `application/yaml`); name fallback reads top-level `name` (replaces `extractMetadataName` → rename to `extractDocName`); no positional AND no doc name → the existing error message shape, updated to say `set name in the doc`.

- [ ] **Step 1:** Failing tests against `httptest.Server`: get(yaml default) hits `?format=yaml`; put(json body) sends `Content-Type: application/json`; put(yaml body) sends yaml; put with mismatched positional vs doc name → local error before any HTTP call; list renders `description` column when present.
- [ ] **Step 2:** FAIL → implement → PASS.
- [ ] **Step 3:** Commit: `feat: pipeline get/put speak PipelineDoc (RFC 027 C3)`

### Task C4: pipeline validate

**Files:**
- Modify: `cmd/datuplet/pipeline.go` (subcommand `validate`), `cmd/datuplet/main.go` (usage)
- Test: `cmd/datuplet/pipeline_test.go`

**Interfaces:**
- Produces: `datuplet pipeline validate -f <file|-> [--name <n>] [--json]` → `POST …/pipelines/validate[?name=n]` (`--name` engages the update-mode resource-gate diff against the stored pipeline — spec §5.2/§7). Exit code **0** when no finding has `severity=="error"` (warnings print but pass); **1** when any error finding; **2+** transport/HTTP-5xx. Human output = the findings table (reuse the existing findings rendering if `trigger`/`put` has one; otherwise `SEVERITY  PATH  MESSAGE` columns); `--json` = the raw response body.

- [ ] **Step 1:** Failing tests: three fixtures (clean → exit 0; warnings-only → exit 0 + rendered warnings; errors → exit 1); server 500 → exit ≥2 with message on stderr.
- [ ] **Step 2:** FAIL → implement → PASS (`go test ./cmd/datuplet/ -v`).
- [ ] **Step 3:** Commit: `feat: datuplet pipeline validate with agent-grade exit codes (RFC 027 C4)`

### Task C5: Part 3 gate + agent quickstart

**Files:**
- Create: `docs/agent-quickstart.md`

- [ ] **Step 1:** Write the doc: prerequisites (`DATUPLET_REMOTE`, `DATUPLET_API_TOKEN` or `login --password-stdin`), then the full loop with real commands and expected outputs — `components list`, `components get data-generator --schema`, compose `events-etl.yaml` (embed the spec §3 example verbatim), `pipeline validate -f` (show a findings failure and the fix), `pipeline put -f`, `trigger events-etl --wait --json`, `storage sample curated.daily_summary`. Link from `README.md` and `docs/pipeline-api.md`.
- [ ] **Step 2:** `go build ./... && go test ./...`; cumulative Codex review (base = Part-3 start SHA); push; append Part 3 summary to the PR.
- [ ] **Step 3:** Commit: `docs: agent quickstart (RFC 027 C5)`

<!-- ═══════════════ Part 4 — UI ═══════════════ -->

# Part 4 — Form-first pipeline builder

**Goal:** `/ui/pipelines/:name` becomes the form-first builder of spec §6. The approved mockup (`docs/superpowers/specs/2026-07-17-rfc-027-pipeline-builder-ux-mockup.html`) is the normative UX: when a task says "port `<fn>` from the mockup", open the mockup and translate that function to the product module, replacing mock data with API calls. No build step; vanilla ES modules; every schema-derived string `esc()`d before `innerHTML` (existing rule, see current `schema-form.js` header).

**Architecture:** One in-memory doc object is the editor state; the form renderer and the YAML textarea are two views over it. `schema-form.js` keeps its public API so `components.js` (catalog page docs) keeps working. Since there is no JS test framework in the repo, each task's "test" step is a scripted browser check (serve the UI via a local pipeline-api or `python3 -m http.server` against `ui/product/`) with exact assertions listed; the definitive gate is Part 5's e2e + manual smoke.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| U0 | Preflight: anchors + vendored js-yaml | sonnet | Parts 1+2 gates | no |
| U1 | `schema-form.js` v2: recursive Form-Subset renderer | opus | U0 | no |
| U2 | Renderer decorations: defaults, ⓘ docs, Advanced, tri-state, fallback | sonnet | U1 | no |
| U3 | Builder page shell: outline, catalog modal, editor, save/validate | opus | U1 | no (touches same file as U4–U6 — serialize U3→U4→U5→U6) |
| U4 | Inputs modal picker + upstream/`x-datuplet-produces` resolution | sonnet | U3 | no |
| U5 | Outputs dual-mode editor | sonnet | U4 | no |
| U6 | YAML mode (two-way) + hidden-subtree preservation proof | opus | U5 | no |
| U7 | Part 4 gate | sonnet | U1–U6 | no |

### Task U0: Preflight + vendor js-yaml

**Files:**
- Create: `ui/product/vendor/js-yaml.mjs`
- Modify: `ui/product/index.html` (nothing yet — verify the module import map/paths convention only)

- [ ] **Step 1:** Rebase; verify anchors: `ls ui/product/lib ui/product/pages`; `grep -n "buildSchemaForm" ui/product/lib/schema-form.js ui/product/pages/pipeline-detail.js ui/product/pages/components.js`; confirm how pipeline-api serves `/ui/*` (`PIPELINE_API_UI_DIR`) so `vendor/` is picked up with no server change.
- [ ] **Step 2:** Vendor js-yaml as a single ESM file: download the dist ESM build of js-yaml 4.x (`https://esm.sh/js-yaml@4` bundled, or the package's `dist/js-yaml.mjs`) into `ui/product/vendor/js-yaml.mjs`. Record the exact version + source URL in a header comment. Verify in a browser console: `import('/ui/vendor/js-yaml.mjs').then(m => m.load('a: 1'))` → `{a: 1}`.
- [ ] **Step 3:** Commit: `feat: vendor js-yaml 4.x ESM for the pipeline builder (RFC 027 U0)`

### Task U1: `schema-form.js` v2 — recursive renderer

**Files:**
- Rewrite: `ui/product/lib/schema-form.js` (public API unchanged: `buildSchemaForm(container, schemaStr, initialValue, opts) → {getValue, getErrors, destroy}`)

**Interfaces:**
- Consumes: `opts.secretKeys` + `opts.listSecretsFn` (unchanged, used by the `x-datuplet-secret` picker — PRESERVE the existing secret-field code path and its null-prototype/`esc()` hardening; the current file's header comments document why).
- Produces: recursive rendering of the full Form Subset (spec §4.2 table): nested objects (collapsible groups), array-of-objects (repeater cards with add/remove, port the mockup's `renderRepeater`), scalar arrays (one-per-line textarea, as today), `additionalProperties` maps (key/value rows, port `renderMap` — value control from the map value schema: enum → select, else input), enum select, string/integer/number inputs, boolean checkbox. Display metadata on every field (spec §4.2/§4.4): the property key is the label (mono); `title`, when present, renders as the human label with the key as a mono suffix; `description` is the always-visible one-line hint after the label (lint guarantees it exists for built-ins). `getValue()` returns ONLY fields the user populated (empty string / unchecked-tri-state = absent). `getErrors()` covers: required-missing, integer parse, `minimum`/`minItems` violations — advisory only.

- [ ] **Step 1:** Port the mockup's `renderSchemaNode`/`renderRepeater`/`renderMap`/`renderScalar` into the module structure, keeping the existing file's conventions: null-prototype accumulators (`Object.create(null)` — the `__proto__` hardening comment explains why; keep it), `esc()` on every schema string, stable `data-` attributes for post-render wiring.
- [ ] **Step 2:** Browser check (serve UI; open `/ui/components`, pick data-generator): the FULL schema renders as controls — `tables` repeater, nested `random` group, `schema` map with type dropdowns, `limit` fields, `literal` group, `userErrorMessage` input. Zero JSON-fallback nodes for any built-in. `getValue()` after filling only `tables[0].name` returns exactly `{"tables":[{"name":"…"}]}`.
- [ ] **Step 3:** Commit: `feat: recursive Form-Subset schema renderer (RFC 027 U1)`

### Task U2: Renderer decorations

**Files:**
- Modify: `ui/product/lib/schema-form.js`, `ui/product/style.css`

**Interfaces (spec §4.4, §4.2):**
- `default` → input `placeholder` = `default: <v>`; enum empty option label `— default (<v>) —`; boolean **with** `default` → tri-state `<select>` (`— default (<v>) —` / `true` / `false`), boolean without → checkbox; `examples[0]` → input placeholder when `default` is absent (plain example, no `default:` prefix).
- `x-datuplet-doc` → `<span class="doci" title="…">ⓘ</span>` after the label (port the mockup's `.doci` styling into `style.css`).
- `x-datuplet-advanced` → all such properties of an object render inside ONE collapsed `<details>` labeled `Advanced` at the end of that object's group (port the mockup's partition in `renderSchemaNode`).
- `x-datuplet-multiline: "<lang>"` → `<textarea class="input--mono" rows="10" data-lang="<lang>">`.
- Out-of-subset node → JSON sub-editor `<textarea>` for that node only, with a warning callout naming the unsupported construct (`oneOf` etc.); `getValue()` parses it as JSON and reports a parse failure via `getErrors()`.

- [ ] **Step 1:** Implement each decoration; every title/placeholder string passes through `esc()`.
- [ ] **Step 2:** Browser check on sql-transform: `sql` renders as code area; `threads/memory/temp_directory/max_temp_size` sit under a single collapsed Advanced group, each with ⓘ hover text; leaving `threads` empty yields a `getValue()` WITHOUT `threads`.
- [ ] **Step 3:** Commit: `feat: schema-form decorations — defaults, docs, advanced, tri-state, fallback (RFC 027 U2)`

### Task U3: Builder page shell

**Files:**
- Rewrite: `ui/product/pages/pipeline-detail.js`
- Modify: `ui/product/style.css` (outline/editor/builder styles — port the mockup's `.stage/.ccard/.ed-head/.seg` class blocks, renamed to fit existing conventions)

**Interfaces:**
- Consumes: `api()`, `getComponents()`, `getComponent()`, `listSecrets()`, `getStorageCatalog()` from `/ui/api.js` (existing); U1 `buildSchemaForm`; new API shapes from Part 1 (`doc` field; PUT JSON; `POST …/validate`).
- Produces (state contract for U4–U6): module-level `doc` object (the FULL stored doc — spec §6 hidden-subtree preservation: only paths with controls are ever written; `resources` and unknown-to-the-form subtrees ride along), `sel = {s, c}`, functions `renderOutline()`, `renderEditor()`, `saveDoc()` (PUT JSON body `doc`, render findings), `validateDoc()` (POST validate, render findings), plus `getComponentMeta(name)` returning the catalog entry incl. `io`.

- [ ] **Step 1:** Port the mockup's outline (stage rename inline, stage delete with the components-count confirm, add stage, component cards select/remove), catalog modal (`openCatalog` — real catalog from `getComponents()`, deprecated tag kept), and editor shell (instance name field, version `<select>` over the component's `versions[]` defaulting per `pickDocVersion` — reuse/move that helper from the old file). Config form = `buildSchemaForm(schemaOf(component, version), comp.config, {secretKeys})`; on any change, write `getValue()` into `comp.config` **but** re-attach keys of `comp.config` that the form doesn't render (fallback-node keys are handled by the fallback itself; non-schema keys are preserved verbatim) — implement as `comp.config = {...unrenderedKeys(comp.config, schema), ...formHandle.getValue()}`.
- [ ] **Step 2:** Wire Save (PUT `application/json`, body = the full doc; 204 → "Saved.", 200 → warnings + findings, 400 → findings) and Validate (POST validate, always render findings; zero findings → success callout). Delete + recent-runs sections: keep from the old file unchanged.
- [ ] **Step 2b: Pipeline settings (gateway).** A collapsed "Pipeline settings" group above the outline's stages (spec §6): the four `GatewayConfig` numeric fields (`chunkSize`, `bufferSize`, `rowGroupSize`, `targetFileSize`) as number inputs with defaults as placeholders (`default: 33554432 (32 MiB)` etc. — values from `pkg/pipeline/config/types.go` constants). Empty inputs store nothing (`doc.gateway` absent when untouched — same defaults-are-metadata rule as config fields).
- [ ] **Step 3:** Browser check: create a pipeline with one data-generator via pure form interaction; Save; reload page; the form re-renders the saved config faithfully (two-way through the doc, no YAML involved). A stored doc with a `resources:` block (PUT one via curl as superadmin) survives a form-mode edit+save byte-for-byte on the `resources` subtree (`GET` and diff).
- [ ] **Step 4:** Commit: `feat: form-first pipeline builder shell (RFC 027 U3)`

### Task U4: Inputs picker

**Files:**
- Modify: `ui/product/pages/pipeline-detail.js`

**Interfaces:**
- Consumes: U3 state; `getStorageCatalog()`; `getComponentMeta(name).io`.
- Produces: Inputs section rendered only when `io.inputs !== "none"`; chips + `+ Add input table…` modal (port `openTablePicker(si, have, onAdd)` from the mockup) with groups **produced upstream** and **in storage** (storage catalog), search filter, multi-select, `Add N tables`. Upstream enumeration (port `upstreamTables`, generalized): explicit `outputs.tables` of earlier stages, PLUS names resolved via the component schema root's `x-datuplet-produces` path over that component's config (implement `resolveProduces(pathExpr, config)` supporting exactly `key` and `key[*]` segments joined by dots — e.g. `tables[*].name`), under its `outputs.defaultBucket`; dynamic buckets without a produces path list as `<bucket> (dynamic)`, disabled.

- [ ] **Step 1:** Implement; `resolveProduces` is a small pure function — put it in `ui/product/lib/produces.js` with a JSDoc contract so U7's review can eyeball it in isolation.
- [ ] **Step 2:** Browser check: with stage-1 data-generator config naming table `events` and defaultBucket `raw`, a stage-2 sql-transform's picker lists `raw.events — produced by stage <name>` plus real storage-catalog tables; picking two adds two chips; the generator itself shows NO Inputs section.
- [ ] **Step 3:** Commit: `feat: modal inputs picker with upstream awareness (RFC 027 U4)`

### Task U5: Outputs dual-mode editor

**Files:**
- Modify: `ui/product/pages/pipeline-detail.js`

**Interfaces:**
- Consumes: U4's picker (`Select existing…` reuses `openTablePicker`).
- Produces: port the mockup's `renderOutputs(box, comp)` verbatim in behavior: segmented `Dynamic — bucket only` / `Explicit tables`; dynamic → `{defaultBucket, defaultWriteMode?}`; explicit → `{tables: [{name, bucket, writeMode?}]}` rows + `+ New table` + `Select existing…`; mode switch carries the bucket; writeMode selects show `— default (FULL_LOAD) —`; section hidden when `io.outputs === "none"`. `outputs.buckets[]` in a stored doc → the outputs section shows a read-only note "multi-bucket outputs — edit in YAML mode" and preserves the subtree (hidden-subtree rule).

- [ ] **Step 1:** Implement. **Step 2:** Browser check: the four mockup-verified behaviors (dynamic default, bucket carry-over on switch, existing-table pick appends a mapping, empty mapping blocked by server findings on Save). **Step 3:** Commit: `feat: dual-mode outputs editor (RFC 027 U5)`

### Task U6: YAML mode, two-way

**Files:**
- Modify: `ui/product/pages/pipeline-detail.js`

**Interfaces:**
- Consumes: `ui/product/vendor/js-yaml.mjs` (U0); U3 `doc` state.
- Produces: `[Form | YAML]` toggle. →YAML: `jsyaml.dump(doc, {noRefs: true, sortKeys: false})` into a mono textarea. →Form: `jsyaml.load(text)`; on parse error stay in YAML with the message shown inline (nothing lost, nothing sticky); on success the parsed object REPLACES `doc` wholesale and the form re-renders. Save in YAML mode PUTs the textarea content with `Content-Type: application/yaml` (server is authoritative); Save in form mode PUTs JSON (U3). The legacy one-way builder, `componentBlockYAML`, `insertAtCursor`, `componentSnippet`, and the YAML-emitting serializer in the old file are DELETED (the doc is the only bridge now).

- [ ] **Step 1:** Implement; audit the final file for leftover dead exports (`grep -n "componentBlockYAML\|insertAtCursor" ui/product/`).
- [ ] **Step 2:** Browser check: build in form → toggle YAML (no `apiVersion` anywhere) → hand-edit a config value → toggle Form (value visible in the form) → toggle YAML → paste garbage (`{{`) → toggle Form → error shown, still in YAML, text intact → fix → Form works. Save from both modes; `GET ?format=yaml` matches expectations.
- [ ] **Step 3:** Commit: `feat: two-way YAML mode over the doc; legacy one-way builder removed (RFC 027 U6)`

### Task U7: Part 4 gate

- [ ] Full browser smoke: the two-component scenario from the mockup walkthrough (data-generator → sql-transform), built entirely by form, validated, saved, triggered against a live OrbStack install; run reaches `Succeeded`; `datuplet storage sample` shows `daily_summary` rows.
- [ ] `grep -rn "componentBlockYAML\|yamlIdentScalar\|yamlScalar\|yamlKeyValue\|insertAtCursor\|componentSnippet" ui/product/` — zero hits: the legacy hand-rolled YAML serializer is fully gone. (YAML-mode code importing `vendor/js-yaml.mjs` and the `putPipelineYAML` save path legitimately remain.)
- [ ] Cumulative Codex review (base = Part-4 start SHA); push; append Part 4 summary to the PR.

<!-- ═══════════════ Part 5 — Examples, e2e, docs ═══════════════ -->

# Part 5 — Examples, e2e, docs, release notes

**Goal:** Every shipped example, e2e scenario, and doc speaks PipelineDoc; a new e2e proves the agent loop; the PR flips to ready.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| E0 | Preflight: inventory every envelope-format body in the tree | sonnet | Parts 1–4 gates | no |
| E1 | Rewrite `examples/pipelines/*` | sonnet | E0 | with E2 |
| E2 | Rewrite e2e scenario pipeline bodies | sonnet | E0 | with E1 |
| E3 | Agent-loop e2e scenario | opus | E2, C4 | no |
| E4 | Docs sweep | sonnet | E0 | with E1–E3 |
| E5 | Final gate: `make e2e-k8s`, cumulative review, PR ready | sonnet | E1–E4 | no |

### Task E0: Preflight inventory

- [ ] `grep -rln "apiVersion: datuplet.io/v1" examples/ tests/ docs/ README.md ui/ charts/ | sort` — the checklist for E1/E2/E4. Anything in `charts/datuplet-app/crds/` or operator fixtures (`pkg/k8s/...` testdata) is EXEMPT (the CR remains the internal/K8s artifact). Save the two lists (rewrite vs exempt) into the PR description draft.

### Task E1: Examples

**Files:** every `examples/pipelines/*.yaml` (+ `examples/README.md`)

- [ ] **Step 1:** Rewrite each example to the doc format; drop the `PipelineRun` CR documents; replace each header's `kubectl apply` line with the CLI/REST/UI run instructions (`datuplet pipeline put -f … && datuplet trigger …`).
- [ ] **Step 2:** Guard: for each file, `datuplet pipeline validate -f <file>` against a live install exits 0 (or, offline, `go test` the parser over `examples/pipelines/*` — extend the existing examples CI-guard test from RFC 026 Phase 0 to call `config.Parse` on every example).
- [ ] **Step 3:** Commit: `docs: examples speak PipelineDoc (RFC 027 E1)`

### Task E2: e2e scenario bodies

**Files:** `tests/e2e/pipelines/k8s/*.yaml` (E0's list) + any Go string fixtures E0 found under `tests/e2e/`

- [ ] Rewrite to doc format; run the e2e suite's compile + any offline validation target; commit `test: e2e fixtures speak PipelineDoc (RFC 027 E2)`.

### Task E3: Agent-loop e2e

**Files:**
- Create: `tests/e2e/scenarios_agent_loop_test.go`

**Interfaces:** drives the REAL CLI binary (built by the e2e harness — follow the pattern of existing scenarios that shell out; E0 records which helper builds/locates binaries) with `DATUPLET_REMOTE`/`DATUPLET_API_TOKEN` set from the harness's install.

- [ ] **Step 1:** Test script: (1) `components get data-generator --schema` → stdout parses as JSON Schema, contains `"tables"`; (2) `pipeline validate -f bad.yaml` where `bad.yaml` omits required `tables` → exit 1, findings name `config.tables`; (3) fix → validate exit 0; (4) `pipeline put`; (5) `trigger --wait --json` → phase `Succeeded`; (6) `storage sample`/`query` asserts `daily_summary` row shape (reuse the suite's duckdb assertion helper in `tests/e2e/framework/assertions.go`).
- [ ] **Step 2:** Run against OrbStack: `make e2e-k8s E2E_RUN=AgentLoop` (or the suite's filter convention) — PASS.
- [ ] **Step 3:** Commit: `test: agent-loop e2e — schema→validate→put→trigger→verify (RFC 027 E3)`

### Task E4: Docs sweep

**Files:** `docs/pipeline-api.md`, `docs/components.md`, `docs/known-limitations.md`, `README.md`, `docs/quickstart-kind.md`, `docs/quickstart-gke.md`, `CLAUDE.md` (conventions: schema.json + sync target + lint + annotations list), release-notes snippet in the PR body

- [ ] **Step 1:** `pipeline-api.md`: PipelineDoc format section (spec §3 example verbatim), PUT/GET/validate contracts, `?format=yaml`, findings shape. `components.md`: schema authoring guide — Form Subset table, the five annotations, description/default rules, lint expectations, io semantics; per-component sections point at `components/<name>/schema.json` instead of embedding shapes. `known-limitations.md`: last-write-wins PUT; component semantic invariants fail at runtime; legacy format rejected. Quickstarts/README: examples + commands updated (E0 list).
- [ ] **Step 2:** Release-note text in the PR body: destructive migration (pipelines + run history; take a `pg_dump` first if you care; **cancel active runs before upgrading** — in-flight PipelineRun CRs/Jobs survive the DB reset as inert cluster objects and the reaper handles terminal cleanup), `yaml`→`doc` API rename, legacy-format rejection + 5-line manual conversion example, no automated backout (restore = previous chart + DB dump).
- [ ] **Step 3:** Commit: `docs: RFC 027 documentation sweep (RFC 027 E4)`

### Task E5: Final gate

- [ ] `go build ./... && go test ./... && make tidy && git diff --exit-code go.*`
- [ ] `make e2e-k8s` against OrbStack — full suite green (chart/CRD/controller/UI all changed in this RFC; this is mandatory, Global Constraints).
- [ ] Cumulative Codex review of the whole branch (base = `main` merge-base); fix CRITICAL/MAJOR.
- [ ] Push; finalize the PR body (all five Part summaries + release notes + E0 exempt-list); mark the PR **ready for review** for the maintainer. Never merge, never tag.

---

## Plan self-review record (author-run)

- **Spec coverage:** §3 → S1/S2/S4 (+E1/E2/E4); §4.1 → R3a/R3b/R4/R6; §4.2 → R5/U1/U2; §4.3 → R1/R2 (+chart in R4, UI gating U4/U5); §4.4 → R3a/R3b (authoring) + U2 (rendering); §5.1 → S5 (+S6 interfaces); §5.2 → S6/S7 (+R2 catalog row); §5.3 → S8; §5.4 → S3 (+R2 io rules); §6 → U0–U7; §7 → C1–C5; §8 → gates + E3; §9 → E4 release notes. Non-goals honored: no oneOf rendering (U2 fallback), `outputs.buckets[]` read-only note (U5), no walkthrough (U3 ports the mockup WITHOUT its tour code).
- **Type consistency:** `ValidatePipelineDoc`/`ValidateTyped`/`DocToCR`/`CRToDoc`/`RenderYAML`/`GetDocByID`/`TriggerRequest.Doc`/`ComponentIO.InputsMode` used identically across Parts (contract section is the single source).
- **Known open point (deliberate):** exact current signature of `config.Parse` and the response-struct names in `component_handlers.go` are verified by S0/R0/C0 preflights rather than asserted here — line-number anchors may drift, symbol names are the contract.
- **External plan review (Codex gpt-5.5, 2026-07-18): 13 findings, all folded in** — S5/S6 re-scoped (S5 = mechanical retype end-to-end keeping every commit green, S6 = API behavior only, serialized); S8 keeps `ApplyPipelineCRD`'s `namespace` param (`ProjectNS.Ensure` flow unchanged); R2 gained the `ResolvedComponent.IO` plumbing prerequisite (`validate/registry.go:25/:74`); S6's resource-gate status corrected to 403; C2 pinned to the detail endpoint (list omits `configSchema`); C4's `--name` flag added to spec §7; U3 gained the gateway "Pipeline settings" step; U1/U2 gained `title`/`description`/`examples` metadata rendering; U7's grep narrowed to the legacy serializer symbols; E3's assertions path fixed to `tests/e2e/framework/assertions.go`; Branching model gained the deployment guard; S5 + E4 gained backout/active-runs guidance.
