# RFC 026 — Full Implementation Plan (all phases, single document)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. This document contains FIVE self-contained phase plans ("Parts"), each with its own Global Constraints, Harness notes (orchestrator contract incl. the per-task Codex gate), task index, DAG, and phase gate. Execute one Part at a time; every later Part opens with a **Preflight (rebase checkpoint)** task that re-verifies its anchors against merged reality — run it, never skip it.

**Spec:** `docs/superpowers/specs/2026-07-04-rfc-026-pipeline-config-safety-design.md` (Draft v7 — all design questions resolved).

## Parts

| Part | Phase | Branch | Tasks | Starts when |
|---|---|---|---|---|
| 1 | 0 (examples) + 1 (config unification) | single branch (see Branching model) | 6 + 8 | now — two lanes (task-parallel, commits interleave) |
| 2 | 1.5 (secrets) | single branch | 10 | Phase 1 gate passed (∥ Part 3) |
| 3 | 2 (component registry + enforcement) | single branch | 13 | Phase 1 gate passed (∥ Part 2; see its Coordination note) |
| 4 | 3 (superadmin + resource policy) | single branch | 10 | Phase 2 gate passed |
| 5 | 4 (registry-driven UI) | single branch | 9 | Phases 2 + 1.5 + 3 gates passed |

**Cross-part interface contract (fixed — Parts reference these names verbatim):** `pkg/pipeline/validate` `Finding{path,message,severity}` / `ValidatePipeline` / `ValidateTyped` (Part 3 adds `reg RegistryView`; Part 4 adds `pol *Policy`); `ComponentSpec.ConfigMap()`; `RegistryView`/`ResolvedComponent` (whose `Resources` field is `datupletv1.ComponentResources` — single definition, no validate-package copy); `PipelineRun.status.{pipelineGeneration, resolvedSpec, components[]}` — from Part 4 on, `resolvedSpec` carries **effective** resources (defaults applied + clamped to Max at admission; nothing re-reads the registry after admission); catalog `GET /api/v1/components(/{name})`; secrets `GET|PUT|DELETE /api/v1/projects/{pid}/secrets(/{key})` (`[{key,updatedAt}]`); `is_superadmin` on `GET /api/v1/auth/me`.

## Branching model (single branch — decided by Tomas 2026-07-07; OVERRIDES per-Part branch wording)

All implementation lands as **sequential commits on `claude/affectionate-perlman-96129d`** (this session's worktree branch). The `feat/rfc-026-phase-*` branches named in this document are **not created**. Wherever a Part says "create branch X off `main`" read "continue on the single branch"; wherever it gates on "Phase N merged (to main)" read "Phase N's gate has passed on this branch". Mechanics:

- **Commit 0 (before Phase 0):** commit the spec + this plan (`docs/superpowers/specs/…`, `docs/superpowers/plans/…`) so subagents and preflights read them from the tree.
- **`<phase-start SHA>`:** at each phase start the orchestrator records `git rev-parse HEAD`; that SHA is the `base` of the phase gate's cumulative Codex review. For the very first phase gate it equals `main`.
- **One draft PR for the whole RFC:** the first phase gate (A6) runs `git push -u public claude/affectionate-perlman-96129d && gh pr create --draft --base main --title "RFC 026: component registry & safe, uniform pipeline configuration"` with the Phase 0 summary as the initial body (note: the GitHub remote is named `public` in this clone, not `origin`). Every later phase gate **pushes and appends its phase summary to that PR body** (`gh pr edit <num> --body …`) — it does NOT open a new PR. Never push `main`, never tag (unchanged repo rule).
- **Parallelism:** task-level parallelism (file-disjoint tasks in separate subagent worktrees) is unchanged; the orchestrator merges finished tasks onto the single branch one at a time, in task-number order. Lane/phase pairs that were "parallel PR trains" (Lane A ∥ Lane B; Phase 1.5 ∥ Phase 2) now interleave commits on the one branch under the same file-disjointness rules (the "R6 waits for S3+S4 landed" gate is unchanged) — or simply serialize; serializing is always safe and is the default when in doubt.
- **Rebase cadence:** rebase this branch onto latest `main` at every phase boundary (unrelated work may land on `main` independently); each later Part's Preflight then re-verifies its anchors against the rebased tree.

---

<!-- ═══════════════ Part 1 — Phases 0 + 1 ═══════════════ -->

# RFC 026 Phases 0+1 — Examples Consolidation & Config Unification — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task — one fresh subagent per task, orchestrator reviews + runs the Codex gate between tasks. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land RFC 026 Phase 0 (one trustworthy `examples/` tree, CI-guarded) and Phase 1 (one structured `config` field, one validation path, `configJSON` + `PipelineRun.parameters` deleted) as two parallel lanes on the single branch (one draft PR — Branching model).

**Architecture:** Phase 0 is docs/YAML plus one Go guard test (no production-code change) — Lane A. Phase 1 — Lane B — changes the CRD Go types + manifests, introduces `pkg/pipeline/validate` as the single validation source, re-implements `pkg/pipeline/config.Parse` as a thin delegate/converter over it (so its 8 consumers keep compiling), and reworks the controller's gateway-config generation. Both lanes commit to the single branch (see Branching model in the preamble). Spec: `docs/superpowers/specs/2026-07-04-rfc-026-pipeline-config-safety-design.md` (§4.2, §4.3, §4.8, Phase table §5).

**Tech Stack:** Go 1.x (root module), `sigs.k8s.io/yaml` (already direct), `k8s.io/apiextensions-apiserver` (already indirect v0.32.1 — promote to direct), manually-maintained CRD YAML (two copies), vanilla YAML examples, GNU-free BSD/macOS shell.

## Global Constraints

- **Never push `main`, never tag.** All work on the single branch `claude/affectionate-perlman-96129d`; lands via the one draft PR (Branching model). (Repo rule.)
- `go build ./... && go test ./...` green **before every commit**.
- `make tidy` (never bare `go mod tidy`) after any `go.mod` change or module deletion — the repo is multi-module and drift fails CI.
- CRD manifests are **manually maintained in two places**: `charts/datuplet-app/crds/` AND `utils/deploy/k8s/crds/`. Every CRD edit touches both.
- POC greenfield (RFC §2): no deprecation shims, no dual-read. `configJSON` and `spec.parameters` are deleted outright.
- macOS/BSD environment: use file-edit tooling, not `sed -i`/GNU flags.
- Do not "fix" pre-existing quirks outside scope (e.g. `k8s-retry-*` deleting in `-n datuplet-e2e` while manifests say `namespace: datuplet` — pre-existing, out of scope).
- Phase 1 requires `make e2e-k8s` against an OrbStack cluster before the PR is marked ready (CRD + controller changed). Phase 0 does not.
- Commit messages: conventional (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`), one logical commit per task.

## Harness notes (orchestrator contract)

- **Branch:** the single branch `claude/affectionate-perlman-96129d` (Branching model — no phase branches). Lanes A (Phase 0) and B (Phase 1) run **task-parallel** in subagent worktrees; their commits interleave on the branch (or serialize — orchestrator's choice). Exception: B7 requires all Lane A tasks landed (it edits files A creates).
- **Parallel tasks within a lane** are marked `Parallel: yes (disjoint files)`. Run each in its own git worktree off the branch; merge back in task-number order (files are disjoint by design → clean merges). Sequential tasks run directly on the branch.
- **Per-task Codex gate (after the subagent commits, before the next task):**
  1. Orchestrator runs MCP tool `mcp__codex-cli__review` with `{commit: "<task commit SHA>", model: "gpt-5.5", title: "<task id>", workingDirectory: "<lane worktree>"}`.
  2. Acceptance: **zero CRITICAL or MAJOR findings on the task's diff.** MINOR findings: fix if ≤5 min, otherwise record in the PR description.
  3. Findings to fix → dispatch a fixer subagent (same model as the task) with the finding text verbatim; re-run the gate.
- **Phase gate (last task of each lane):** cumulative `mcp__codex-cli__review` with `{base: "<phase-start SHA>", model: "gpt-5.5"}`, then the PR step per the Branching model (A6 opens the single draft PR; B8 appends).
- **Subagent dispatch:** give each subagent its full task text verbatim, plus the Global Constraints section, the branch/worktree to use, and nothing else. Suggested Agent-tool model per task is in the index below.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| A1 | Port 3 K8s examples to `examples/pipelines/` | sonnet | — | with B1 |
| A2 | Examples CI-guard test | sonnet | A4 | with A5 |
| A3 | Update all references (Makefile, docs, CLAUDE.md) | sonnet | A1 | with A5 |
| A4 | Delete dead example trees | sonnet | A3 | no |
| A5 | `examples/README.md` index | haiku | A1 | with A2, A3 |
| A6 | Phase 0 gate: full test, cumulative Codex review, opens THE single draft PR | sonnet | A1–A5 | no |
| B1 | CRD Go types: structured `Config`, delete `ConfigJSON` + `Parameters` | sonnet | — | with A1 |
| B2 | CRD YAML manifests (both copies) | sonnet | B1 | with B3 |
| B3 | `pkg/pipeline/validate` package | **opus** | B1 | with B2 |
| B4 | `config.Parse` delegates to validate + converter | **opus** | B3 | no |
| B5 | pipeline-api: findings 400 contract | sonnet | B3 | with B6 |
| B6 | Controller gateway-config + e2e fixture rewrite | sonnet | B1 | with B5, B6b |
| B6b | Controllers adopt `ValidateTyped` (kubectl-path checks) | sonnet | B3 | with B5, B6 |
| B7 | Examples/docs sweep to structured config (+3 new examples) | sonnet | B4, A6 gate passed | no |
| B8 | Phase 1 gate: `make e2e-k8s`, cumulative Codex review, PR-body update | sonnet | B1–B7 incl. B6b | no |

Task DAG (arrows = "blocks"):

```
Lane A: A1 → {A3, A5} → A4 → A2 → A6
Lane B: B1 → {B2, B3} ; B3 → {B4, B5, B6b} ; B1 → B6 ; {B4, A6-gate-passed} → B7 → B8
```

(A2 deliberately runs AFTER the A4 deletions: its glob must see only the
three ported files — the stale CLI-dialect YAMLs would fail strict decode.)

---

## Lane A — Phase 0 (examples consolidation)

### Task A1: Port 3 K8s examples to `examples/pipelines/`

**Files:**
- Create: `examples/pipelines/simple-http-extract.yaml`
- Create: `examples/pipelines/full-etl.yaml`
- Create: `examples/pipelines/etl-duckdb.yaml`

**Interfaces:**
- Produces: three CRD-valid multi-doc YAMLs (Pipeline + PipelineRun) whose `metadata.name`s are exactly `simple-pipeline`, `full-pipeline`, `duckdb-transform` — A3's Makefile edits and A2's guard test rely on these names and paths.

Note: RFC §4.8 lists six target files. `processors-drop.yaml`, `incremental-reads.yaml`, `secrets-http-auth.yaml` need nested/secret config that is only expressible cleanly after Phase 1 — they are **deliberately deferred to Task B7** ("no YAML authored in a shape that's about to be deleted", RFC §4.8).

- [ ] **Step 1: `simple-http-extract.yaml`** — copy `examples/k8s/simple-pipeline.yaml` verbatim, then: replace the whole leading comment block with the standard header below; delete the `parameters: {}` line from the PipelineRun doc (field is optional today, deleted in Phase 1). Keep `metadata.name: simple-pipeline` and both `namespace: datuplet` lines unchanged.

Standard header format (fill per file):

```yaml
---
# <Title> — <one-line what it demonstrates>
#
# Components: <list>
# Prerequisites: running Datuplet install (lakekeeper + pipeline-api), see docs/install.md
#
# Run it:
#   UI:      Pipelines → New → paste this YAML → Save → Trigger
#   REST:    see docs/pipeline-api.md "Upload a pipeline" + "Trigger a run"
#   kubectl: kubectl apply -f <this-file>   (applies Pipeline + PipelineRun)
```

- [ ] **Step 2: `full-etl.yaml`** — copy `examples/k8s/full-pipeline.yaml` verbatim; same header treatment; delete `parameters: {}`; keep `metadata.name: full-pipeline`, `namespace: datuplet`.

- [ ] **Step 3: `etl-duckdb.yaml`** — port `tests/e2e/pipelines/k8s/duckdb-etl.yaml` with these exact transformations (source is Go-templated; the example must be plain YAML):
  - `{{.RunPrefix}}-duckdb-etl` → `duckdb-transform` (Pipeline name — **must** match `Makefile` target `k8s-retry-duckdb`, which deletes `pipeline duckdb-transform`); run name → `duckdb-transform-run-1`.
  - `{{.RunPrefix}}-raw` → `raw`; `{{.RunPrefix}}-etl` → `etl`.
  - Images `datuplet/http-json-extractor:latest` → `ghcr.io/kacurez/http-json-extractor:latest`, `datuplet/sql-transform:latest` → `ghcr.io/kacurez/sql-transform:latest` (public examples use the public registry, like the other examples).
  - Delete `parameters: {}`. Add the standard header. Keep `namespace: datuplet`.

- [ ] **Step 4: sanity-check YAML parses**

Run: `python3 -c "import yaml,glob; [list(yaml.safe_load_all(open(f))) for f in glob.glob('examples/pipelines/*.yaml')]; print('ok')"`
Expected: `ok`

- [ ] **Step 5: Commit**

```bash
git add examples/pipelines/
git commit -m "docs(examples): port K8s examples to consolidated examples/pipelines/ tree (RFC 026 P0)"
```

### Task A2: Examples CI-guard test

**Files:**
- Create: `examples/examples_guard_test.go`
- Test: itself

**Interfaces:**
- Consumes: `examples/pipelines/*.yaml` from A1; `datupletv1` types (`pkg/k8s/api/v1`); `config.Parse(data []byte) (*config.Pipeline, error)` from `pkg/pipeline/config/parser.go:36`.
- Produces: a root-module test that fails CI whenever an example stops strict-decoding or stops passing semantic validation. B7 relies on this staying green after its sweep.

**Ordering:** this task runs AFTER A4 — the stale CLI-dialect YAMLs (nested
config) would fail strict decode; the glob must see only the three ported
files.

- [ ] **Step 1: Write the test (this is the complete file)**

```go
package examples_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	pipelineconfig "github.com/datuplet/datuplet/pkg/pipeline/config"
	"sigs.k8s.io/yaml"
)

// Guards RFC 026 §4.8: every example must strict-decode into the CRD
// types (typos fail) and pass the same semantic validation pipeline-api
// runs at save time. Examples can no longer rot silently.
func TestExamplesAreValid(t *testing.T) {
	files, err := filepath.Glob("pipelines/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no example YAMLs found under examples/pipelines/")
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			raw, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			for i, doc := range splitDocs(string(raw)) {
				var probe struct {
					Kind string `json:"kind"`
				}
				if err := yaml.Unmarshal([]byte(doc), &probe); err != nil {
					t.Fatalf("doc %d: %v", i, err)
				}
				switch probe.Kind {
				case "Pipeline":
					var p datupletv1.Pipeline
					if err := yaml.UnmarshalStrict([]byte(doc), &p); err != nil {
						t.Errorf("doc %d strict decode: %v", i, err)
					}
					if _, err := pipelineconfig.Parse([]byte(doc)); err != nil {
						t.Errorf("doc %d semantic validation: %v", i, err)
					}
				case "PipelineRun":
					var pr datupletv1.PipelineRun
					if err := yaml.UnmarshalStrict([]byte(doc), &pr); err != nil {
						t.Errorf("doc %d strict decode: %v", i, err)
					}
				default:
					t.Errorf("doc %d: unexpected kind %q", i, probe.Kind)
				}
			}
		})
	}
}

// splitDocs splits a multi-doc YAML stream on top-level "---" separators
// and drops empty/comment-only documents.
func splitDocs(s string) []string {
	var docs []string
	for _, d := range strings.Split(s, "\n---") {
		trimmed := strings.TrimSpace(strings.TrimPrefix(d, "---"))
		hasContent := false
		for _, line := range strings.Split(trimmed, "\n") {
			l := strings.TrimSpace(line)
			if l != "" && !strings.HasPrefix(l, "#") {
				hasContent = true
				break
			}
		}
		if hasContent {
			docs = append(docs, trimmed)
		}
	}
	return docs
}
```

- [ ] **Step 2: Run it — must pass against A1's files**

Run: `go test ./examples/ -run TestExamplesAreValid -v`
Expected: PASS, 3 sub-tests. (If `config.Parse`'s signature differs from `(*config.Pipeline, error)`, adapt the call — the signature is at `pkg/pipeline/config/parser.go:36`.)

- [ ] **Step 3: Negative check (temporarily)** — add `writeMod: FULL_LOAD` (typo) to a copy under `pipelines/`, re-run, expect FAIL on strict decode; delete the copy.

- [ ] **Step 4: Commit**

```bash
git add examples/examples_guard_test.go
git commit -m "test(examples): CI guard — strict-decode + semantic validation of every example (RFC 026 P0)"
```

### Task A3: Update all references

**Files:**
- Modify: `Makefile` (targets `k8s-retry-simple`, `k8s-retry-duckdb`, `k8s-retry-full` — around lines 261–277)
- Modify: `README.md:54`, `docs/quickstart-kind.md:153`, `docs/quickstart-gke.md:278`, `docs/pipeline-api.md:157`, `tests/e2e/scenarios/gcs-pipeline-k8s/README.md:29`, `tests/e2e/scenarios/gcs-pipeline-k8s/pipeline.yaml:7` (comment referencing `examples/k8s`), `CLAUDE.md` (key-directories table)

**Interfaces:**
- Consumes: A1's new paths. Produces: zero references to `examples/k8s/` or the old CLI-dialect files anywhere in the repo — A4's deletion depends on this.

- [ ] **Step 1: Makefile** — three path swaps, names unchanged:
  - `kubectl apply -f examples/k8s/simple-pipeline.yaml` → `kubectl apply -f examples/pipelines/simple-http-extract.yaml`
  - `kubectl apply -f examples/k8s/duckdb-pipeline.yaml` → `kubectl apply -f examples/pipelines/etl-duckdb.yaml` (this also **fixes the broken target** — the old path never existed)
  - `kubectl apply -f examples/k8s/full-pipeline.yaml` → `kubectl apply -f examples/pipelines/full-etl.yaml`

- [ ] **Step 2: Docs** — replace every occurrence of `examples/k8s/simple-pipeline.yaml` with `examples/pipelines/simple-http-extract.yaml` in `README.md`, `docs/quickstart-kind.md`, `docs/quickstart-gke.md`, `tests/e2e/scenarios/gcs-pipeline-k8s/README.md`; in `docs/pipeline-api.md` replace `examples/pipelines/simple-pipeline.yaml` with `examples/pipelines/simple-http-extract.yaml`.

- [ ] **Step 3: CLAUDE.md** — in the key-directories table, replace the `examples/k8s/, examples/pipelines/` row(s) with a single row: `| examples/pipelines/ | Example K8s pipeline manifests (CI-guarded). |` and remove any `examples/local-dev` mention.

- [ ] **Step 4: Verify no stragglers — ALL file types, not just md/Makefile**
(also update the `tests/e2e/scenarios/gcs-pipeline-k8s/pipeline.yaml:7`
comment that points at `examples/k8s`)

Run: `grep -rn "examples/k8s\|examples/local-dev\|simple-pipeline.yaml\|processor-pipeline.yaml\|incremental-test.yaml\|etl-pipeline.yaml" . | grep -v '.git/' | grep -v docs/superpowers | grep -v '^./examples/pipelines/'`
Expected: no output (spec/plan docs and the new examples themselves excluded).

- [ ] **Step 5: Commit**

```bash
git add Makefile README.md docs/ CLAUDE.md tests/e2e/scenarios/gcs-pipeline-k8s/README.md
git commit -m "docs: point all example references at consolidated examples/pipelines/ (RFC 026 P0)"
```

### Task A4: Delete dead example trees

**Files:**
- Delete: `examples/k8s/` (both files), `examples/pipelines/{simple-pipeline,etl-pipeline,processor-pipeline,incremental-test}.yaml` (old CLI-dialect files), `examples/local-dev/` (entire directory, including its `go.mod`/`go.sum`)

- [ ] **Step 1: Delete**

```bash
git rm -r examples/k8s examples/local-dev
git rm examples/pipelines/simple-pipeline.yaml examples/pipelines/etl-pipeline.yaml \
       examples/pipelines/processor-pipeline.yaml examples/pipelines/incremental-test.yaml
```

- [ ] **Step 2: Check the deleted Go module isn't referenced** — `examples/local-dev` was one of the repo's Go modules. Run: `grep -rn "local-dev" Makefile .github/ 2>/dev/null | grep -v '.git/'`. Expected: no output. If the `tidy` target enumerates modules via a loop/find, nothing to change; if it lists `examples/local-dev` explicitly, remove that entry.

- [ ] **Step 3: Full verify** (guard test doesn't exist yet — A2 runs after this task)

Run: `make tidy && go build ./... && go test ./... -count=1`
Expected: tidy clean, build green, full suite green.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore(examples): delete dead local-dev module and stale CLI-dialect examples (RFC 026 P0)"
```

### Task A5: `examples/README.md` index

**Files:**
- Create: `examples/README.md`

- [ ] **Step 1: Write the index** — table of the 3 current examples (file, what it demonstrates, components used), a note that 3 more (`processors-drop`, `incremental-reads`, `secrets-http-auth`) land with RFC 026 Phase 1, and a "How to run" section with the three methods (UI, REST per docs/pipeline-api.md §Upload/§Trigger, `kubectl apply -f`). Mention the CI guard: "every file here is validated by `examples/examples_guard_test.go`".

- [ ] **Step 2: Commit**

```bash
git add examples/README.md
git commit -m "docs(examples): add index README (RFC 026 P0)"
```

### Task A6: Phase 0 gate

- [ ] **Step 1:** `make tidy && go build ./... && go test ./... -count=1` — all green.
- [ ] **Step 2:** Orchestrator: cumulative Codex review `mcp__codex-cli__review {base: "<phase-start SHA>", model: "gpt-5.5", title: "RFC026 Phase 0"}`. Fix CRITICAL/MAJOR via fixer subagent; re-run until clean.
- [ ] **Step 3:** first phase gate → **open the single draft PR** (Branching model): `git push -u public claude/affectionate-perlman-96129d && gh pr create --draft --base main --title "RFC 026: component registry & safe, uniform pipeline configuration" --body "<Phase 0 summary + link to spec §4.8 + Codex-gate log>"`. Never push main.

---

## Lane B — Phase 1 (config unification)

### Task B1: CRD Go types — structured `Config`, delete `ConfigJSON` + `Parameters`

**Files:**
- Modify: `pkg/k8s/api/v1/pipeline_types.go:139-166` (ComponentSpec) and its DeepCopy at `:413-438`
- Modify: `pkg/k8s/api/v1/pipelinerun_types.go` (delete `Parameters` field ~line 48 + its DeepCopy lines)
- Modify: `go.mod` (promote `k8s.io/apiextensions-apiserver` v0.32.1 indirect → direct)
- Test: `pkg/k8s/api/v1/pipeline_types_test.go` (create)

**Interfaces:**
- Produces (all later tasks depend on these exact names):
  - `ComponentSpec.Config apiextensionsv1.JSON` (replaces `Config map[string]string`; `ConfigJSON` gone)
  - `func (c *ComponentSpec) ConfigMap() (map[string]any, error)` — nil-safe decode of `Config.Raw`; returns `(nil, nil)` when Config is empty
  - `PipelineRunSpec` without `Parameters`

- [ ] **Step 1: Failing test**

```go
package v1

import (
	"testing"

	"sigs.k8s.io/yaml"
)

func TestComponentSpecStructuredConfig(t *testing.T) {
	in := []byte(`
name: gen
image: x:latest
config:
  sql: |
    SELECT 1;
  threads: 4
  tables:
    - name: t1
      random: {schema: {id: int}}
`)
	var c ComponentSpec
	if err := yaml.UnmarshalStrict(in, &c); err != nil {
		t.Fatalf("strict decode: %v", err)
	}
	m, err := c.ConfigMap()
	if err != nil {
		t.Fatalf("ConfigMap: %v", err)
	}
	if m["threads"].(float64) != 4 {
		t.Errorf("threads = %v", m["threads"])
	}
	if _, ok := m["tables"].([]any); !ok {
		t.Errorf("tables not a list: %T", m["tables"])
	}
	cp := c.DeepCopy()
	if string(cp.Config.Raw) != string(c.Config.Raw) {
		t.Error("DeepCopy lost config")
	}
}

func TestComponentSpecEmptyConfig(t *testing.T) {
	var c ComponentSpec
	m, err := c.ConfigMap()
	if err != nil || m != nil {
		t.Fatalf("want (nil,nil), got (%v,%v)", m, err)
	}
}
```

- [ ] **Step 2: Run** `go test ./pkg/k8s/api/v1/ -run TestComponentSpec -v` — FAIL (Config is map[string]string; ConfigMap undefined).

- [ ] **Step 3: Implement.** In `pipeline_types.go`:

```go
import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	// ... existing imports
)

// in ComponentSpec — replaces both Config and ConfigJSON:

	// Config contains component-specific configuration as an arbitrary
	// structured object, validated against the component's registry
	// schema (RFC 026). Nested YAML is first-class.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Config apiextensionsv1.JSON `json:"config,omitempty"`

// new method:

// ConfigMap decodes Config into a generic map. Nil-safe: an unset
// config yields (nil, nil).
func (c *ComponentSpec) ConfigMap() (map[string]any, error) {
	if len(c.Config.Raw) == 0 {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal(c.Config.Raw, &m); err != nil {
		return nil, fmt.Errorf("component %q config: %w", c.Name, err)
	}
	return m, nil
}
```

  DeepCopyInto for ComponentSpec: replace the map-copy block with `in.Config.DeepCopyInto(&out.Config)`. In `pipelinerun_types.go`: delete the `Parameters map[string]string` field and its DeepCopy loop. Then: `go get k8s.io/apiextensions-apiserver@v0.32.1 && make tidy`.

- [ ] **Step 4: Run** the new tests → PASS; then `go build ./...` — **expected to FAIL** wherever code still reads `comp.Config[k]`/`comp.ConfigJSON`. **Scope rule (one sentence): a green `go build ./...` at the commit trumps package scoping — include the minimal MECHANICAL fix in whatever package breaks; the real rework with tests stays in B4/B5/B6/B6b.** Known breakage to fix mechanically here: `pkg/k8s/controllers/pipelinerun_jobs.go:446-458` (replace the map-copy + ConfigJSON merge with `config, err := comp.ConfigMap()`; propagate the error crudely — B6 does it properly with tests); check `pkg/k8s/controllers/pipeline_controller.go` for direct `comp.Config`/`ConfigJSON` reads in its validation block (~:85-204) and adapt the same way (B6b replaces that logic wholesale). `pkg/pipeline/config` decodes raw YAML into its own structs and does not touch the CRD types until B4 — it should not break; if it does, stop and re-read.
- [ ] **Step 5:** `go build ./... && go test ./pkg/k8s/... -count=1` → green. Commit:

```bash
git add pkg/k8s/api/v1/ pkg/k8s/controllers/ go.mod go.sum
git commit -m "feat(crd)!: structured component config (apiextensionsv1.JSON), delete configJSON + PipelineRun parameters (RFC 026 P1)"
```

### Task B2: CRD YAML manifests (both copies)

**Files:**
- Modify: `charts/datuplet-app/crds/datuplet.io_pipelines.yaml` (config block at ~103-112), `charts/datuplet-app/crds/datuplet.io_pipelineruns.yaml` (parameters block)
- Modify: `utils/deploy/k8s/crds/datuplet.io_pipelines.yaml`, `utils/deploy/k8s/crds/datuplet.io_pipelineruns.yaml` (same edits)

**Interfaces:** Produces the API-server-side schema B6's e2e fixtures and B8's `make e2e-k8s` depend on.

- [ ] **Step 1:** In both `datuplet.io_pipelines.yaml` copies, replace:

```yaml
                          config:
                            additionalProperties:
                              type: string
                            description: Config contains component-specific configuration
                              (simple key-value pairs)
                            type: object
                          configJSON:
                            description: ConfigJSON contains component-specific configuration
                              as JSON (for complex/nested values)
                            type: string
```

with:

```yaml
                          config:
                            description: Component-specific configuration as an
                              arbitrary structured object (validated against the
                              component's registry schema).
                            type: object
                            x-kubernetes-preserve-unknown-fields: true
```

(Indentation must match the surrounding file exactly.)

- [ ] **Step 2:** In both `datuplet.io_pipelineruns.yaml` copies, delete the `parameters:` property block (map[string]string, around lines 68-104 region) entirely.
- [ ] **Step 3:** Verify both copies stay in sync: `diff charts/datuplet-app/crds/datuplet.io_pipelines.yaml utils/deploy/k8s/crds/datuplet.io_pipelines.yaml` → empty (repeat for pipelineruns; if the two copies differed BEFORE this task, keep their pre-existing differences and only apply the edit — check with git diff instead).
- [ ] **Step 4:** `python3 -c "import yaml; yaml.safe_load(open('charts/datuplet-app/crds/datuplet.io_pipelines.yaml')); print('ok')"` → ok. Commit `feat(crd)!: manifests — structured config, drop configJSON + parameters (RFC 026 P1)`.

### Task B3: `pkg/pipeline/validate` package

**Files:**
- Create: `pkg/pipeline/validate/validate.go`, `pkg/pipeline/validate/walker.go`
- Test: `pkg/pipeline/validate/validate_test.go`

**Interfaces:**
- Consumes: B1's `datupletv1.Pipeline` types + `ConfigMap()`; `pkg/lib/secrets.Validate` (existing `$[...]` syntax checker).
- Produces (B4/B5/A2-successor rely on exact names):
  - The Finding type — lowercase JSON tags are load-bearing, B5's 400 body serializes these directly; only `"error"` is emitted in Phase 1 (`"warning"` is reserved for Phase 1.5's secrets ladder):

```go
type Finding struct {
	Path     string `json:"path"`
	Message  string `json:"message"`
	Severity string `json:"severity"` // "error" | "warning"
}
```
  - `func ValidatePipeline(data []byte) (*datupletv1.Pipeline, []Finding, error)` — `error` only for non-YAML/IO-level problems; strict-decode failures and semantic violations come back as Findings. Comment in code: Phase 2 extends the signature with `(reg RegistryView, pol Policy)`.
  - `func ValidateTyped(p *datupletv1.Pipeline) []Finding` — the semantic checks on an already-decoded object. `ValidatePipeline` = strict decode + `ValidateTyped`. B6b's controllers call `ValidateTyped` directly (they hold typed CRs, not YAML).

- [ ] **Step 1: Failing tests (table-driven).** Cover at minimum: (1) valid nested-config pipeline → 0 findings, parsed spec has the nested config; (2) unknown field `writeMod:` → finding whose **message** contains `writeMod` (strict-decode findings carry `Path: ""` — do not assert on path); (3) `configJSON:` present → finding (unknown field — proves deletion); (4) bad bucket name `RAW!` → finding whose message matches the current parser's message for that case; (5) output `defaultBucket` combined with `outputs.tables` → finding (exclusivity); (6) mid-string secret ref `url: "x-$[a]-y"` inside nested config → finding (whole-scalar rule); (7) valid `$[api_token]` whole-scalar deep inside nested config → 0 findings; (8) `metadata.name` empty → finding; (9) invalid `writeMode: UPSERT` → finding (enum); (10) processor `type: keep` → finding (only `drop` exists); (11) invalid partition `transform: week` → finding.
- [ ] **Step 2: Run** `go test ./pkg/pipeline/validate/ -v` → FAIL (package doesn't exist).
- [ ] **Step 3: Implement.**
  - `ValidatePipeline`: `yaml.UnmarshalStrict` into `datupletv1.Pipeline` (strict error → one Finding with the decode error text and Path `""`), then run semantic checks.
  - **Port ALL semantic checks from `pkg/pipeline/config/parser.go`** — the `Parse` body at :36-162 and **every** validate helper through end of file: identifier rules (:184-262), output validation (:276), writeMode enum (:334), processors (:369), partitionFields (:400). None are optional; read the whole parser first, move logic, don't reinvent it. Preserve the existing error message texts, but emit them as `Finding{Path: "stages[i].components[j].<field>", ...}` instead of a joined error.
  - `walker.go`: `func walkStrings(v any, path string, fn func(path, s string))` recursing through `map[string]any` / `[]any` / `string` — used to apply the secret-ref whole-scalar check to every string in `ConfigMap()` output.
- [ ] **Step 4: Run** → all table cases PASS. `go build ./...` green.
- [ ] **Step 5: Commit** `feat(validate): single ValidatePipeline path with structured findings (RFC 026 P1)`.

### Task B4: `config.Parse` delegates to validate + converter

**Files:**
- Modify: `pkg/pipeline/config/parser.go` (gut `Parse`/`ParseFile` decode path), `pkg/pipeline/config/types.go` (only if field mapping requires)
- Create: `pkg/pipeline/config/convert.go`
- Test: `pkg/pipeline/config/parser_test.go` (update existing tests: configJSON cases now fail, nested config cases now pass)

**Interfaces:**
- Consumes: B3's `validate.ValidatePipeline`.
- Produces: `config.Parse(data []byte) (*config.Pipeline, error)` — same signature, now strict + single-source. The 8 existing consumers (`pkg/pipeline/runner.go`, `pkg/pipelineapi/runbackend/{backend,k8s}.go`, `pkg/pipelineapi/tokens/capabilities.go`, `pkg/pipelineapi/store/pipeline.go`, `pkg/pipelineapi/http/{pipeline,run}_handlers.go`, `pkg/k8s/controllers/pipelinerun_jobs.go` [ParseSinceDuration only]) must compile and behave unchanged for valid input.

- [ ] **Step 1: Failing tests:** nested config YAML → `Parse` OK and `cfg.Stages[0].Components[0].Config["tables"]` is `[]any`; a `configJSON:` document → `Parse` returns error mentioning the unknown field; existing valid fixtures in the package tests still pass.
- [ ] **Step 2: Implement** `convert.go`:

```go
// FromCRD converts the typed CRD pipeline (the single validated
// representation) into the runtime view used by the runner, capability
// derivation, and gateway-config generation. It is the ONLY bridge
// between the two shapes (RFC 026 §4.3: no third dialect survives).
func FromCRD(p *datupletv1.Pipeline) (*Pipeline, error)
```

  mapping — **map only the fields `config.Pipeline` actually has; read
  `pkg/pipeline/config/types.go` in full before writing this**. Known
  mapping: metadata name/labels; gateway; stages/components: name, image,
  `ConfigMap()` → `Config map[string]any`, inputs, outputs. Two traps
  verified in advance: `config.Pipeline` has **no SecretsRef field** (the
  controller reads secretsRef from the typed CRD directly — do not invent
  one), and its `ResourceSpec` (types.go:138-142) is flat memory/cpu
  strings, not `corev1.ResourceRequirements` — populate it from the CRD
  limits via `.String()` only where set. Then `Parse` =
  `validate.ValidatePipeline` → findings→joined error (preserving today's
  "error contract": non-nil error on any finding) → `FromCRD`.
- [ ] **Step 3:** Delete the now-dead YAML-decode code in parser.go (the old unmarshal into config-local structs + duplicated checks). Keep `ParseSinceDuration` (used by runner + controller) and `ParseFile` (delegate to `Parse`).
- [ ] **Step 4:** `go build ./... && go test ./pkg/... -count=1` → green (runner/capabilities/runbackend tests prove behavior parity).
- [ ] **Step 5: Commit** `refactor(config): Parse delegates to validate + FromCRD converter — one validation source (RFC 026 P1)`.

### Task B5: pipeline-api findings contract

**Files:**
- Modify: `pkg/pipelineapi/http/pipeline_handlers.go` (`handlePutPipeline` at :125; the parse + name-check block at :153-166), `pkg/pipelineapi/http/run_handlers.go:85` (trigger re-parse error shape only if needed; anchor re-verified 2026-07-07 — the merged runs-UX PR #23 added list/timeline handlers to this file, function-disjoint from the trigger path)
- Test: package's existing handler tests + new cases

**Interfaces:**
- Consumes: B3 `validate.ValidatePipeline`.
- Produces: `PUT /api/v1/projects/{pid}/pipelines/{name}` returns, on validation failure, HTTP 400 with body `{"error":"validation failed","findings":[{"path":"...","message":"...","severity":"error"}]}`. The UI (Phase 4) and RFC §7 depend on this exact shape.

- [ ] **Step 1: Failing test:** PUT with `writeMod:` typo → 400, body decodes into the findings shape, `findings[0].path` non-empty. PUT with valid nested config → 204. PUT where YAML `metadata.name` ≠ URL name → 400 (existing behavior preserved).
- [ ] **Step 2: Implement** in `handlePutPipeline` — replace lines 153-157:

```go
	pl, findings, err := validate.ValidatePipeline(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid pipeline: "+err.Error())
		return
	}
	if len(findings) > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "validation failed", "findings": findings,
		})
		return
	}
```

  and the name check becomes `if pl.Name != "" && pl.Name != name { ... }`. (Use the package's existing JSON-response helper; if none exists, add `writeJSON` next to `writeError`.) `run_handlers.go` keeps `config.Parse` (now strict via B4) — only confirm its error path still returns 400.
- [ ] **Step 3:** Run handler tests → PASS. `go build ./...` green.
- [ ] **Step 4: Commit** `feat(api): structured validation findings on pipeline save (RFC 026 P1)`.

### Task B6: Controller gateway-config + e2e fixture rewrite

**Files:**
- Modify: `pkg/k8s/controllers/pipelinerun_jobs.go:445-458` (`generateGatewayConfig` — finish B1's mechanical patch properly: `(string, error)` signature, caller at :57 propagates the error)
- Modify: `tests/e2e/pipelines/k8s/error-bad-config.yaml`, `tests/e2e/pipelines/k8s/manual-large-input-join.yaml`
- Modify: **every** `tests/e2e/pipelines/k8s/*.yaml` carrying `parameters: {}` (the field is deleted from the CRD; modern kubectl's strict field validation rejects unknown fields, so leftover `parameters:` lines fail `kubectl apply` at B8's e2e run)
- Test: `pkg/k8s/controllers/` existing job-builder tests + one new nested-config case

**Interfaces:** Consumes B1's `ConfigMap()`. Produces gateway `gateway.yaml` ConfigMap content in which nested config appears as native YAML maps/lists (the gateway already consumes `map[string]any` — no gateway change).

- [ ] **Step 1: Failing test:** build a Job for a component whose `Config` is nested (tables list); assert the generated `gateway.yaml` string contains `tables:` with a nested `name: facts` (i.e., real YAML nesting, not an escaped JSON string).
- [ ] **Step 2: Implement:** `generateGatewayConfig` returns `(string, error)`; body starts:

```go
	config, err := comp.ConfigMap()
	if err != nil {
		return "", err
	}
	if config == nil {
		config = map[string]any{}
	}
```

  (delete the old map-copy + `ConfigJSON` json.Unmarshal merge block entirely). `buildComponentJob` propagates the error.
- [ ] **Step 3: Rewrite the two fixtures** — replace each `configJSON: |` block (and its apology comment) with native nested YAML under `config:`. For `error-bad-config.yaml`:

```yaml
          config:
            tables:
              - name: bad
                random:
                  schema: {id: int}
                  limit: {rowsCount: 10}
                  userErrorMessage: "simulated user error: bad config"
```

  For `manual-large-input-join.yaml`, transcribe its JSON into equivalent YAML the same way (two tables: `facts` random 5M rows with the 3-column schema; `dimensions` literal with the 10 rows — content identical to the JSON, just YAML syntax).
- [ ] **Step 4: Strip `parameters: {}` from every e2e fixture.** Run `grep -rln 'parameters' tests/e2e/pipelines/` and delete the `parameters: {}` line from each PipelineRun doc (also grep `tests/e2e/**/*.go` and any scenario templates for code that sets `spec.parameters` — none is expected; if found, remove it). Verify: `grep -rn 'parameters' tests/e2e/pipelines/` → empty.
- [ ] **Step 5:** `go test ./pkg/k8s/... -count=1` → PASS (the e2e tree is a separate Go module — its fixtures are YAML-only here; `make e2e-k8s` at B8 exercises them). Commit `feat(controller): structured config → gateway.yaml; e2e fixtures drop configJSON + parameters (RFC 026 P1)`.

### Task B6b: Controllers adopt `ValidateTyped` (kubectl-path checks)

**Files:**
- Modify: `pkg/k8s/controllers/pipeline_controller.go` (home-grown validation block ~:85-204)
- Modify: `pkg/k8s/controllers/pipelinerun_controller.go` (admission: validate the fetched Pipeline before building any Job)
- Test: controllers package tests (existing + two new cases)

**Interfaces:**
- Consumes: B3's `validate.ValidateTyped(p *datupletv1.Pipeline) []Finding`.
- Produces: RFC §4.3's Phase-1-scope guarantee — the kubectl path runs the SAME semantic checks as the API path, and no second validation implementation survives ("one validation path" becomes literally true in Phase 1, not Phase 2).

- [ ] **Step 1: Failing tests:** (a) Pipeline CR with a bad bucket name reconciled → `status.phase: Invalid`, message contains the finding text; (b) PipelineRun referencing that Pipeline → run fails `FailedUser` with the first finding in the status message, and **no Job objects are created** (assert via the fake client).
- [ ] **Step 2: Implement:** in `pipeline_controller.go`, replace the body of the existing validation block with `findings := validate.ValidateTyped(pipeline)` → phase `Invalid` + first finding message (keep the existing phase/condition plumbing; delete the superseded hand-rolled checks — they are the "second dialect" this phase removes). In `pipelinerun_controller.go`, at run admission (before the first `buildComponentJob`), run `ValidateTyped` on the fetched Pipeline; findings → set the run failed with `FailedUser` semantics per the repo's exit-code contract and emit the first finding in the status message. Do NOT gate on `pipeline.Status.Phase` — the run controller's own check is authoritative (spec §4.3).
- [ ] **Step 3:** `go build ./... && go test ./pkg/k8s/... -count=1` → PASS.
- [ ] **Step 4: Commit** `refactor(controllers): adopt validate.ValidateTyped — one validation path incl. kubectl (RFC 026 P1)`.

### Task B7: Examples/docs sweep (needs all Lane A tasks landed on the branch)

**Files:**
- Create: `examples/pipelines/processors-drop.yaml`, `examples/pipelines/incremental-reads.yaml`, `examples/pipelines/secrets-http-auth.yaml`
- Modify: `docs/components.md`, `examples/README.md`, `docs/secrets.md` (only if it shows configJSON — check)

- [ ] **Step 0:** verify all Lane A tasks (A1–A6) have landed on the branch — the consolidated `examples/pipelines/` tree + `examples/examples_guard_test.go` exist in the working tree (orchestrator ensures ordering; no merge needed on the single branch).
- [ ] **Step 1: `processors-drop.yaml`** — port old `examples/pipelines/processor-pipeline.yaml` (deleted in A4; recover the source with `git show $(git log --diff-filter=D --format=%h -1 -- examples/pipelines/processor-pipeline.yaml)^:examples/pipelines/processor-pipeline.yaml`): data-generator with the nested `tables:` config **as plain YAML under `config:`** (now legal), drop-processor outputs (`processors: [{type: drop, columns: [price, quantity]}]`), stdout-writer stage reading `buckets: [raw]` with `config: {format: json}`; standard A1 header; `namespace: datuplet`; metadata.name `processors-drop`; plus a PipelineRun doc (no `parameters`).
- [ ] **Step 2: `incremental-reads.yaml`** — two-doc file: Pipeline named `incremental-reads` with stdout-writer reading `{bucket: raw, table: products, since: 1m}` + header noting "run processors-drop first to create raw.products"; PipelineRun doc.
- [ ] **Step 3: `secrets-http-auth.yaml`** — http-json-extractor with:

```yaml
          config:
            url: "https://api.example.com/items"
            headers:
              Authorization: "Bearer $[api_token]"
```

  `spec.secretsRef: {name: http-auth-secrets}` (field still exists until Phase 1.5), header includes the `kubectl create secret generic http-auth-secrets --from-literal=api_token=... -n datuplet` prerequisite and a pointer to docs/secrets.md.
- [ ] **Step 4: `docs/components.md` sweep** — the **complete `kind: Pipeline` documents** in the file must strict-decode (verify by pasting through `validate.ValidatePipeline` in a scratch test, or careful manual check); **fragment blocks** (bare component-list or `config:` snippets — e.g. the per-component "Config schema" examples) can't be decode-checked, so verify them for *consistency* with the CRD shape by eye; remove any `configJSON` mention; **add the missing `pandas-transform` section** (read `components/pandas-transform/` source for its actual config keys and document them with a runnable example — same section format as the other five).
- [ ] **Step 5:** Update `examples/README.md` (3 new rows, drop the "coming with Phase 1" note). Run the guard: `go test ./examples/ -v` → 6 files PASS. Grep: `grep -rn configJSON docs/ examples/ tests/e2e/pipelines/ | grep -v superpowers` → empty.
- [ ] **Step 6: Commit** `docs(examples): structured-config sweep — 3 new examples, components.md fixed + pandas-transform (RFC 026 P1)`.

### Task B8: Phase 1 gate

- [ ] **Step 1:** `make tidy && go build ./... && go test ./... -count=1` green.
- [ ] **Step 2:** `make e2e-k8s` against an OrbStack cluster (CRD + controller changed — repo rule). Fix failures via subagents (model: opus for controller issues, sonnet otherwise) before proceeding.
- [ ] **Step 3:** Orchestrator: cumulative Codex review `{base: "<phase-start SHA>", model: "gpt-5.5", title: "RFC026 Phase 1"}` → zero CRITICAL/MAJOR.
- [ ] **Step 4:** Push + append the Phase 1 summary to the single draft PR body (`gh pr edit` — Branching model): structured-config contract, breaking CRD change note (POC), e2e-k8s log, Codex-gate log.

---

## Roadmap — Phases 1.5 / 2 / 3 / 4 (detailed plans EXIST)

Every phase has its own detailed plan as a later Part of this document, same
format and gates as this one. Later-phase plans reference earlier phases'
**interfaces** (fixed contracts) but never their line numbers; each opens
with a **Preflight (rebase checkpoint)** task that re-verifies every anchor
against merged reality before any code runs — run it, don't skip it:

- Phase 1.5: Part 2 (Phase 1.5) of this document (10 tasks)
- Phase 2:   Part 3 (Phase 2) of this document (13 tasks)
- Phase 3:   Part 4 (Phase 3) of this document (10 tasks)
- Phase 4:   Part 5 (Phase 4) of this document (9 tasks)

Quick overview (details in each plan):

| Phase | Branch | Key tasks (outline) | Models | Parallel with |
|---|---|---|---|---|
| 1.5 Secrets (§4.9) | single branch | managed Secret + lazy ensure; per-key merge-PATCH handlers; per-run snapshot in run controller + `SecretsResolved` re-key; delete `spec.secretsRef` (types + both CRD copies); per-namespace RBAC Roles; secrets UI page; e2e (snapshot immutability, missing-key FailedUser) | opus: snapshot controller task; sonnet: handlers/UI/RBAC; haiku: docs | Phase 2 (disjoint packages except `pipeline_types.go` — coordinate the secretsRef deletion task to not collide with Phase 2's component field task) |
| 2 Registry (§4.1–4.6) | single branch | ComponentDefinition types + CRD (both copies) + DeepCopy; definition reconciler (status Valid/Invalid); chart built-in CR templates + RBAC verbs; `validate` gains `reg RegistryView` (Phase 3 later adds `pol *Policy`) + JSON Schema validation (santhosh-tekuri/jsonschema); resolve-&-freeze at run admission (`status.components[]`, `pipelineGeneration`, stop re-fetching live Pipeline at stage boundaries — `pipelinerun_controller.go:183,337`); `component`/`version` fields replace `image` in ComponentSpec; error taxonomy fix (`FailedUser` vs transient); prerelease + `imagePullPolicy` rules; e2e: register-dev-versions bootstrap, fixtures flip to `component:`, new enforcement scenarios | opus: resolver/freeze + validate-registry tasks; sonnet: CRD/chart/reconciler; haiku: docs | Phase 1.5 (see left); UI catalog API stub can start once informer lands |
| 3 Superadmin + resources (§4.4–4.5) | single branch | `mustBeSuperadmin` (server-object discovery reuse from `admin_lakekeeper.go:526`, memoized; UUID-subject tuples); `admin grant --superadmin`; resources diff-gate on PUT; reject-then-clamp in validate+controller (full ResourceList, unlisted names rejected); registry default application; LimitRange in provisioning; gateway-knob bounds from `pipelinePolicy` values; e2e | opus: diff-gate + clamp semantics; sonnet: FGA wiring/CLI | Phase 4 UI catalog work |
| 4 UI (§4.7) | single branch | catalog page; builder v1 (picker + docs panel + findings rendering); builder v2 (schema-form renderer, `x-datuplet-secret` pickers, inputs/outputs pickers); one-way YAML toggle | sonnet throughout (vanilla JS); opus only for the schema→form renderer | last; needs 2 (and 3 for resource UI, 1.5 for pickers) |

## Self-review checklist (run before handing off)

- Spec coverage: §4.8 → A1-A6 + B7; §4.2/4.3 → B1-B8 incl. B6b (controllers adopt `ValidateTyped` — "one validation path" is literally true within Phase 1, kubectl path included); §5 Phase 0/1 rows → lanes A/B; e2e test-impact Phase 1 bullet → B6. Deferred-by-design: 3 example files moved from Phase 0 scope to B7 (lockstep rule, noted in A1).
- No placeholders: every created file has full code or an exact port recipe with acceptance tests; every step has a command + expected outcome.
- Type consistency: `ValidatePipeline(data []byte) (*datupletv1.Pipeline, []Finding, error)` used identically in B3/B4/B5; `ValidateTyped(*datupletv1.Pipeline) []Finding` in B3/B6b; `ConfigMap() (map[string]any, error)` in B1/B4/B6; example resource names `simple-pipeline`/`full-pipeline`/`duckdb-transform` consistent across A1/A3.
- Codex-review round (2026-07-04) folded in: A-lane reorder (guard after deletions), e2e `parameters:` strip, B6b added, B3 port range widened + path-vs-message assertion fixed, B4 mapping corrected against `types.go`, components.md fragment rule, B1 scope rule, straggler-grep widened.


---

<!-- ═══════════════ Part 2 — Phase 1.5 ═══════════════ -->

# RFC 026 Phase 1.5 — Secrets Management — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development — one fresh subagent per task, orchestrator reviews + runs the Codex gate between tasks. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Land RFC 026 §4.9 — project-scoped write-only secrets API + UI over one managed K8s Secret, per-run snapshot mounts, `Pipeline.spec.secretsRef` deleted, Secret RBAC narrowed to per-namespace Roles.

**Architecture:** pipeline-api owns `datuplet-project-secrets` per project namespace (ensured lazily via the same path as the namespace itself) and exposes per-key merge-PATCH endpoints. The run controller copies **referenced keys only** into a per-run snapshot Secret at admission (same pattern as the run-token Secret), mounts that, and fails `FailedUser` pre-pod on a missing key. Branch: the single branch (Branching model), starting **after Phase 1's gate has passed**. Spec: `docs/superpowers/specs/2026-07-04-rfc-026-pipeline-config-safety-design.md` §4.9, §7.

**Tech Stack:** Go, controller-runtime (uncached reads for Secrets), K8s merge-PATCH, vanilla-JS UI.

## Global Constraints

Same as the Phase 0/1 plan (Part 1 (Phases 0+1) of this document): never push `main`/tag; `go build ./... && go test ./...` before every commit; `make tidy` for any go.mod change; CRD manifests maintained in BOTH `charts/datuplet-app/crds/` and `utils/deploy/k8s/crds/`; POC greenfield (no deprecation shims); BSD/macOS tooling; `make e2e-k8s` before the PR is ready (controller + chart changes); conventional commits, one logical commit per task.

**Anchor policy (applies to every future-phase plan):** line numbers are given only for files Phase 1 does NOT rewrite; everything else is anchored by function name. Task S1 verifies all anchors against merged reality before anything else runs.

## Harness notes (orchestrator contract)

Identical to the Phase 0/1 plan: per-task Codex gate `mcp__codex-cli__review {commit: "<task sha>", model: "gpt-5.5", title: "<task id>"}` — zero CRITICAL/MAJOR to proceed, fixer subagent otherwise; phase gate reviews `{base: "<phase-start SHA>"}`. Parallel tasks are file-disjoint and run in separate worktrees off the branch, merged in task order.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| S1 | Preflight (rebase checkpoint) | sonnet | Phase 1 gate passed | — |
| S2 | `validate.ReferencedSecrets` helper | sonnet | S1 | with S4, S5 |
| S3 | Per-run snapshot in run controller + `SecretsResolved` re-key | **opus** | S2, S4 | no |
| S4 | Delete `spec.secretsRef`: types + both CRD manifests + example/fixture sweep | sonnet | S1 | with S2, S5 |
| S5 | Managed-secret ensure + write-only per-key API | sonnet | S1 | with S2, S4 |
| S6 | RBAC narrowing: per-namespace Roles, reaper rework | **opus** | S5 | with S7, S8 |
| S7 | Save-warn / trigger-400 ladder in handlers | sonnet | S2, S5 | with S6, S8 |
| S8 | Secrets UI page (real, write-only) | sonnet | S5 | with S6, S7 |
| S9 | e2e scenarios | sonnet | S3, S6, S7 | no |
| S10 | Phase gate: `make e2e-k8s`, cumulative Codex, PR-body update | sonnet | all | no |

```
S1 → {S2, S4, S5} ; {S2, S4} → S3 ; S5 → {S6, S7, S8} ; S2 → S7 ; {S3, S6, S7} → S9 → S10
```

---

### Task S1: Preflight (rebase checkpoint)

Verify the Phase 1 contract exists as planned; amend this plan's anchors inline if drift is found (record amendments in the task commit message; docs-only commit allowed).

- [ ] `grep -n "func ValidatePipeline\|func ValidateTyped\|type Finding" pkg/pipeline/validate/*.go` → all three symbols exist with the Phase 0/1 plan's signatures.
- [ ] `grep -n "Config apiextensionsv1.JSON\|func (c \*ComponentSpec) ConfigMap" pkg/k8s/api/v1/pipeline_types.go` → both present; `grep -n "SecretsRef" pkg/k8s/api/v1/pipeline_types.go` → still present (this phase deletes it).
- [ ] Anchor check (function names, not lines): `handlePending`, `startStage`, `applySecretsMount`, `updateSecretsResolvedCondition` in `pkg/k8s/controllers/`; `CreateRunOpts.SecretName`, `CreateRunResources` in `pkg/pipelineapi/k8s/run_create.go`; `storeProjectNS.Ensure` in `pkg/pipelineapi/runbackend/k8s.go`.
- [ ] **Reaper recon (feeds S6):** read `pkg/pipelineapi/k8s/reaper.go` — record HOW it finds run-token Secrets today (cluster-wide list vs per-namespace). Write findings into the S6 task notes.
- [ ] **Operator cache recon (feeds S3):** find the manager setup (`cmd/pipeline-operator/main.go`) — record whether Secrets are served from the manager cache; S3 must read the project Secret via an uncached reader (`mgr.GetAPIReader()`) either way, so per-namespace RBAC suffices.

### Task S2: `validate.ReferencedSecrets`

**Files:** Modify `pkg/pipeline/validate/` (new file `secrets.go` + tests).

**Interfaces:**
- Produces: `func ReferencedSecrets(p *datupletv1.Pipeline) []string` — sorted, deduplicated `$[name]` keys across every component's `ConfigMap()` string values (whole-scalar refs only, same walker as the syntax check). Consumed by S3 (snapshot), S7 (ladder).

- [ ] Failing tests: nested config with `$[a]` twice + `$[b]` → `["a","b"]`; no refs → empty; escaped `$$[x]` → not a ref.
- [ ] Implement by reusing the existing walker + the `$[...]` matcher from `pkg/lib/secrets`.
- [ ] `go test ./pkg/pipeline/validate/ -count=1` → PASS. Commit `feat(validate): ReferencedSecrets helper (RFC 026 P1.5)`.

### Task S3: Per-run snapshot + `SecretsResolved` re-key

**Files:** Modify `pkg/k8s/controllers/pipelinerun_controller.go` (`handlePending`), `pipelinerun_jobs.go` (`applySecretsMount`), `pipelinerun_status.go` (`updateSecretsResolvedCondition`), **`cmd/pipeline-operator/main.go`** (wire `mgr.GetAPIReader()` into the reconciler's new `APIReader client.Reader` field); tests in the controllers package.

**Interfaces:**
- Consumes: S2 `ReferencedSecrets`; S4 (SecretsRef gone from types).
- Produces: per-run Secret named `datuplet-runsecrets-<shortID(runID)>` (constant `runSecretsName(pr)` next to `componentJobName`), ownerRef'd to the PipelineRun, containing exactly the referenced keys; mounted at the existing `/var/run/secrets/datuplet` gateway-sidecar path. Gateway resolution is untouched.

- [ ] **Step 1: Failing tests:** (a) pipeline referencing `$[api_token]`, project Secret has it → snapshot Secret created with only that key, ownerRef set, Job mounts the snapshot name; (b) key missing → run `FailedUser`, message names the key, **zero Jobs and zero snapshot** created; (c) rotation isolation: after snapshot exists, mutate the project Secret in the fake client, reconcile the next stage → mounted Secret name and content unchanged; (d) no refs → no snapshot, no mount, condition absent.
- [ ] **Step 2: Implement in `handlePending`** (admission — before the first `startStage`): `refs := validate.ReferencedSecrets(pipeline)`; if empty → skip. Read `datuplet-project-secrets` in `pr.Namespace` via `r.APIReader` (add an `APIReader client.Reader` field to the reconciler, wired from `mgr.GetAPIReader()` in main — uncached, so per-namespace RBAC works and no cluster-wide Secret informer is needed). Missing Secret or missing keys → set run failed with `FailedUser` semantics + `DUPLET_STATUS_MESSAGE`-style reason listing missing keys; else create the snapshot Secret (subset copy, ownerRef via `controllerutil.SetControllerReference`), idempotent on re-reconcile (Get-before-Create).
- [ ] **Step 3:** `applySecretsMount` mounts `runSecretsName(pr)` whenever the run has refs (drop every `SecretsRef` read — S4 removed the field); `updateSecretsResolvedCondition` keys off "run has refs" instead of `SecretsRef != nil`, with the same True/False reasons (`SecretsRefMissing` reason renamed `SnapshotMissing`).
- [ ] **Step 4:** `go build ./... && go test ./pkg/k8s/... -count=1` → PASS. Commit `feat(controller): per-run secret snapshot at admission — race-free, rotation-exact (RFC 026 P1.5)`.

### Task S4: Delete `spec.secretsRef` (types, CRDs, YAML sweep)

**Files:** Modify `pkg/k8s/api/v1/pipeline_types.go` (SecretsRef field + type + DeepCopy), both `datuplet.io_pipelines.yaml` copies, `examples/pipelines/secrets-http-auth.yaml`, `tests/e2e/pipelines/k8s/secrets-happy.yaml`; grep-driven compile fixes.

- [ ] Delete `SecretsRef` from `PipelineSpec` + the `SecretsRef` struct + DeepCopy blocks; delete the `secretsRef` property from both CRD manifest copies.
- [ ] `go build ./...` — fix every compile error mechanically (known consumers from grep: controller mount/status code — coordinate with S3 if running concurrently is impossible; **S3 depends on this task, run S4 first**; `pkg/pipeline/config` converter maps no SecretsRef per the B4 contract, so it should not break).
- [ ] Sweep YAML: remove `secretsRef:` block from the example + fixture; update the example's header (secret now created via the S5 API or kubectl on `datuplet-project-secrets`).
- [ ] Examples guard green: `go test ./examples/ -count=1`. Commit `feat(crd)!: delete Pipeline.spec.secretsRef — managed project secrets (RFC 026 P1.5)`.

### Task S5: Managed-secret ensure + write-only per-key API

**Files:** Create `pkg/pipelineapi/http/secret_handlers.go` (+ tests); modify `pkg/pipelineapi/runbackend/k8s.go` (`storeProjectNS.Ensure` — **note: the type is unexported**, so export a helper for the handlers to call, e.g. `func EnsureProjectSecret(ctx, c client.Client, namespace string) error` in `runbackend` or `pkg/pipelineapi/k8s` — do NOT try to reach `storeProjectNS` directly); modify `pkg/pipelineapi/http/server.go` (route registration next to the existing pipeline routes + a secrets seam: `Server` gains the K8s client/ensure dependency via a `WithSecrets(...)` builder, mirroring the existing builder pattern).

**Interfaces:**
- Produces (consumed by S7, S8, Phase 4):
  - `GET    /api/v1/projects/{pid}/secrets` → `200 [{"key":"api_token","updatedAt":"<RFC3339>"}]` (names + annotation timestamps only — never values). Authz: `datuplet_member`.
  - `PUT    /api/v1/projects/{pid}/secrets/{key}` body `{"value":"..."}` → `204`. Authz: `data_admin`. Key must match `[A-Za-z0-9_-]+`.
  - `DELETE /api/v1/projects/{pid}/secrets/{key}` → `204` (404 if absent). Authz: `data_admin`.
  - Constant `ProjectSecretsName = "datuplet-project-secrets"` exported from `pkg/pipelineapi/k8s`.

- [ ] **Step 1: Failing handler tests** (fake K8s client): PUT creates the managed Secret if absent (lazy ensure) and sets `data[key]` + annotation `datuplet.io/updated-<key>`; second PUT for another key preserves the first (**merge-PATCH, not update**); GET lists names+timestamps, never values; DELETE removes one key only; GET as plain `viewer` works, PUT as `viewer` → 403.
- [ ] **Step 2: Implement.** Writes use `client.Patch(ctx, secret, client.RawPatch(types.MergePatchType, payload))` with payload marshaled from:

```go
map[string]any{
	"data":     map[string]any{key: base64.StdEncoding.EncodeToString([]byte(value))},
	"metadata": map[string]any{"annotations": map[string]any{"datuplet.io/updated-" + key: now}},
}
```

  DELETE: same shape with `"data": {key: nil}` and the annotation set to nil (merge-patch null deletes the entry). Lazy ensure: Get-before-Create of the empty managed Secret (and reuse/extend `storeProjectNS.Ensure` so first-run and first-secret share one path). **`now` comes from a clock passed in, not read in the patch helper**, so tests are deterministic.
- [ ] **Step 3:** wire routes; `go test ./pkg/pipelineapi/... -count=1` → PASS. Commit `feat(api): project secrets — write-only per-key merge-PATCH API (RFC 026 P1.5)`.

### Task S6: RBAC narrowing + reaper rework

**Files:** Modify `charts/datuplet-app/templates/pipeline-api/rbac.yaml` (drop the cluster-wide `secrets` rule at rules[3]), `charts/datuplet-app/templates/pipeline-operator/rbac.yaml` (drop cluster `secrets get/list/watch`), `pkg/pipelineapi/runbackend/k8s.go` (`Ensure` also creates Role+RoleBinding), `pkg/pipelineapi/k8s/reaper.go` (per S1 recon).

**Interfaces:** Produces per-project-namespace `Role datuplet-secrets` granting: pipeline-api SA → secrets `get,list,create,update,patch,delete` (managed + run-token + snapshot Secrets); operator SA → secrets `get,create` (project-secret read + snapshot create). Created idempotently by `Ensure` alongside the namespace.

- [ ] **Step 1:** chart edits — remove the cluster-wide secret verbs from both ClusterRoles; add `namespaces: list` to the pipeline-api ClusterRole (reaper iteration; today it has only `get,create`).
- [ ] **Step 2:** `Ensure` creates the Role + two RoleBindings (Get-before-Create) with the exact verb sets above. Failing test first with the fake client.
- [ ] **Step 3: Reaper** — per S1 recon: if it lists Secrets cluster-wide today, rework to: list project namespaces by the `datuplet.io/project-id` label, then list Secrets per namespace (now authorized via the Role). Preserve behavior tests.
- [ ] **Step 4:** unit green. **This task is the e2e-riskiest of the phase — flag for extra attention at S10's `make e2e-k8s`.** Commit `feat(rbac)!: secret verbs move to per-project-namespace Roles (RFC 026 P1.5)`.

### Task S7: Save-warn / trigger-400 ladder

**Files:** Modify `pkg/pipelineapi/http/pipeline_handlers.go` (PUT), `run_handlers.go` (trigger); tests.

**Interfaces:** Consumes S2 + S5. Produces the §7 contract: PUT with a `$[key]` absent from the project store → **200** with `{"findings":[{"path":...,"message":...,"severity":"warning"}]}` (clean save stays 204); trigger with missing keys → **400** hard. (Controller admission from S3 remains the authoritative check.)

- [ ] Failing tests: save-unknown-key → 200 + warning finding naming the key; save-known-key → 204; trigger-unknown-key → 400 + message; trigger-known → 202 as today.
- [ ] Implement: after validation passes, `refs := validate.ReferencedSecrets(pl)`; read the managed Secret's keys (Get via the same client S5 uses; absent Secret = all keys missing); PUT appends warning findings, trigger rejects.
- [ ] Green; commit `feat(api): secrets validation ladder — warn at save, reject at trigger (RFC 026 P1.5)`.

### Task S8: Secrets UI page

**Files:** Rewrite `ui/product/pages/settings-secrets.js`; extend `ui/product/api.js` (`listSecrets`, `putSecret`, `deleteSecret` following the existing fetch-wrapper conventions).

- [ ] Implement: table of key + updatedAt; "Add / update secret" form (key input validated `[A-Za-z0-9_-]+`, value input `type="password"`, note "values cannot be read back"); per-row delete with `confirm()`. Keep existing CSS variable conventions; no build step.
- [ ] `node --check ui/product/pages/settings-secrets.js && node --check ui/product/api.js` → OK.
- [ ] Manual verification checklist in the commit message: create key → appears in list; re-PUT → timestamp changes; delete → gone; value never rendered anywhere.
- [ ] Commit `feat(ui): real secrets page — write-only key management (RFC 026 P1.5)`.

### Task S9: e2e scenarios

**Files:** Modify `tests/e2e/pipelines/k8s/secrets-happy.yaml` + the Go scenario that drives it; add assertions per the spec's Phase 1.5 test-impact bullet.

- [ ] `secrets-happy` reworked: secret written via `PUT /secrets/{key}` (API path) instead of hand-created Secret; pipeline has no `secretsRef`.
- [ ] New assertions: save with missing key → response contains a warning finding; trigger with missing key → 400; kubectl-path run with missing key → `FailedUser`, **no pods**; after `PUT`, run succeeds; snapshot Secret exists during the run and is garbage-collected with the PipelineRun; updating the value after trigger does **not** change the in-flight run's resolved config.
- [ ] Commit `test(e2e): secrets ladder + snapshot immutability scenarios (RFC 026 P1.5)`.

### Task S10: Phase gate

- [ ] `make tidy && go build ./... && go test ./... -count=1` green.
- [ ] `make e2e-k8s` (OrbStack) — S6's RBAC narrowing is the likeliest breakage; fix via subagents before proceeding.
- [ ] Cumulative Codex review `{base: "<phase-start SHA>", model: "gpt-5.5"}` → zero CRITICAL/MAJOR.
- [ ] Push + append the Phase 1.5 summary to the single draft PR body (`gh pr edit` — Branching model): managed project secrets — write-only API, per-run snapshots.

## Self-review checklist

- Spec §4.9 bullets → tasks: managed Secret + lazy ensure (S5), secretsRef deletion + auto-mount→snapshot (S3/S4), write-only merge-PATCH API + annotations (S5), RBAC narrowing (S6), validation ladder (S7), `SecretsResolved` re-key (S3), UI page (S8), e2e bullet (S9). `x-datuplet-secret` is explicitly Phase 2 scope (schemas) — not here.
- Interfaces consumed exist per S1 preflight; anchors are function names only.
- Values never readable: GET returns names+timestamps; UI uses password inputs; findings/logs never carry values (asserted in S5/S7 tests).


---

<!-- ═══════════════ Part 3 — Phase 2 ═══════════════ -->

# RFC 026 Phase 2 — Component Registry & Enforcement — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development — one fresh subagent per task, orchestrator reviews + runs the Codex gate between tasks. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Land RFC 026 §4.1–§4.3 + §4.6: the `ComponentDefinition` cluster-scoped registry, `component:`/`version:` replacing `image:` in pipelines, JSON-Schema config validation, prerelease dev versions, and run-admission resolve-&-freeze with controller-side enforcement.

**Architecture:** New CRD + a definition reconciler in the operator (status `Valid|Invalid`); `pkg/pipeline/validate` gains a `RegistryView` and schema validation; the PipelineRun controller resolves ONCE at admission and snapshots the **resolved spec + component images** into `PipelineRun.status`, building every Job from the snapshot (never re-reading the live Pipeline). pipeline-api serves a read catalog + threads the registry into save/trigger validation; registration is CLI/kubectl-only in this phase (REST admin lands with the superadmin role, Phase 3). Branch: the single branch (Branching model), starting after Phase 1's gate has passed; may interleave with Phase 1.5 (see Coordination note).

**Tech Stack:** Go, controller-runtime, `github.com/santhosh-tekuri/jsonschema/v6` (JSON Schema 2020-12), CRD CEL validation, Helm templates.

## Global Constraints

Same as Part 1 (Phases 0+1) of this document: never push `main`/tag; `go build ./... && go test ./...` before every commit; `make tidy` after go.mod changes; CRD manifests maintained in BOTH `charts/datuplet-app/crds/` and `utils/deploy/k8s/crds/`; POC greenfield — `image:` is deleted outright, no compat shim; BSD/macOS tooling; `make e2e-k8s` before the PR is ready; conventional commits.

**Anchor policy:** function-name anchors only for files Phase 1/1.5 rewrite (`handlePending`, `startStage`, `checkStageComponents`, `buildComponentJob`, `runtimePullPolicy`, `validatePipeline` [pipeline_controller], `ValidateTyped`/`ValidatePipeline` [validate pkg]). R1 verifies all anchors.

**Coordination with Phase 1.5 (parallel lane) — corrected after review:** the overlap is bigger than the types file. Shared surfaces: `pkg/k8s/api/v1/pipeline_types.go` + the pipelines CRD manifests (1.5 deletes `secretsRef`; this phase swaps `image`→`component`/`version`), **and the controller admission region** — Phase 1.5's S3 rewrites `handlePending` (secret snapshot) and touches `pipelinerun_jobs.go`/`pipelinerun_status.go`, which R6/R7 also rework. Rule: **R1–R5 and R8–R10 may run in parallel with Phase 1.5; R6 (and therefore R7, R11–R13) must not start until Phase 1.5's S3+S4 have landed on the branch** — R6's admission text explicitly builds on the secret snapshot being in place. Whichever lane lands its shared-surface tasks second re-runs its preflight against the branch first.

**Spec refinement (documented deviation):** RFC §4.3 shows the snapshot as `pipelineGeneration` + `components[]`. To actually deliver "mid-run edits cannot change what an in-flight run executes", the snapshot here also stores the **validated `PipelineSpec` itself** (`status.resolvedSpec`) — generation alone can't reconstruct old spec content from K8s. Pipelines are already capped at 1 MiB by the API, so status size is safe. Update spec §4.3 wording when this phase's PR opens.

## Harness notes (orchestrator contract)

Identical to the Phase 0/1 plan: per-task Codex gate `mcp__codex-cli__review {commit: "<task sha>", model: "gpt-5.5", title: "<task id>"}` — zero CRITICAL/MAJOR to proceed; phase gate `{base: "<phase-start SHA>"}`. Parallel tasks are file-disjoint, separate worktrees, merged in task order.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| R1 | Preflight (rebase checkpoint) | sonnet | Phase 1 gate passed | — |
| R2 | `ComponentDefinition` types + `PipelineRunStatus` snapshot fields | sonnet | R1 | with R4 |
| R3 | CRD manifests: componentdefinitions (new) + pipelineruns status | sonnet | R2 | with R4, R5 |
| R4 | Schema-validation module (`validate/schema.go`) | **opus** | R1 | with R2, R3 |
| R5 | Registry-aware validate: `RegistryView` + resolution findings | **opus** | R2, R4 | with R3 |
| R6 | Resolve-&-freeze cutover: `component`/`version` replace `image`; jobs from snapshot | **opus** | R3, R5, **Phase 1.5 S3+S4 landed** | no |
| R7 | `imageID` capture + controller error taxonomy | sonnet | R6 | with R8, R9 |
| R8 | ComponentDefinition reconciler + operator RBAC | sonnet | R2, R3 | with R7, R9 |
| R9 | pipeline-api: registry view, catalog endpoints, `admin component` CLI, RBAC | sonnet | R5 | with R7, R8 |
| R10 | Chart: built-in ComponentDefinition templates (6) | sonnet | R3 | with R7–R9 |
| R11 | e2e: dev-registration bootstrap, fixture flip, new scenarios | sonnet | R6, R8, R9, R10 | no |
| R12 | Examples + docs flip to `component:` refs | sonnet | R6 | with R11 |
| R13 | Phase gate: `make e2e-k8s`, cumulative Codex, PR-body update | sonnet | all | no |

```
R1 → {R2, R4} ; R2 → R3 ; {R2, R4} → R5 ; {R3, R5} → R6 → {R7, R12}
R2/R3 → R8 ; R5 → R9 ; R3 → R10 ; {R6, R8, R9, R10} → R11 → R13
```

---

### Task R1: Preflight (rebase checkpoint)

- [ ] Phase 1 contract: `grep -n "func ValidatePipeline\|func ValidateTyped\|type Finding" pkg/pipeline/validate/*.go`; `grep -n "Config apiextensionsv1.JSON\|func (c \*ComponentSpec) ConfigMap" pkg/k8s/api/v1/pipeline_types.go` — all present.
- [ ] Phase 1.5 state: `grep -n "SecretsRef" pkg/k8s/api/v1/pipeline_types.go` — record whether 1.5's S3+S4 already landed on the branch (affects ordering only).
- [ ] Controller anchors exist: `grep -n "func.*handlePending\|func.*startStage\|func.*checkStageComponents\|func.*buildComponentJob\|func.*runtimePullPolicy" pkg/k8s/controllers/*.go` — five hits.
- [ ] `grep -n "santhosh-tekuri" go.mod` → absent (R4 adds it). `grep -rn "comp.Image\|\.Image\b" pkg/k8s/controllers/pipelinerun_jobs.go | head` → record every image-consumption site for R6.
- [ ] Amend this plan's anchors inline on drift; docs-only commit.

### Task R2: `ComponentDefinition` types + run-status snapshot fields

**Files:** Create `pkg/k8s/api/v1/component_types.go` (+ `component_types_test.go`); modify `pkg/k8s/api/v1/pipelinerun_types.go`, `groupversion_info.go` (SchemeBuilder registration).

**Interfaces (produced — R3/R5/R6/R8/R9/R10 and the Phase 3/4 plans reference these EXACTLY):**

```go
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:subresource:status
type ComponentDefinition struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec   ComponentDefinitionSpec   `json:"spec,omitempty"`
	Status ComponentDefinitionStatus `json:"status,omitempty"`
}

type ComponentDefinitionSpec struct {
	DisplayName    string        `json:"displayName,omitempty"`
	Description    string        `json:"description,omitempty"`
	Maintainer     string        `json:"maintainer,omitempty"`
	Deprecated     bool          `json:"deprecated,omitempty"`
	DefaultVersion string        `json:"defaultVersion,omitempty"` // "" → highest stable semver
	Versions       []VersionSpec `json:"versions"`
}

type VersionSpec struct {
	Version      string              `json:"version"`
	Image        string              `json:"image"`
	Prerelease   bool                `json:"prerelease,omitempty"`
	ConfigSchema string              `json:"configSchema,omitempty"` // JSON Schema 2020-12, string blob
	Resources    *ComponentResources `json:"resources,omitempty"`
}

type ComponentResources struct {
	Default corev1.ResourceRequirements `json:"default,omitempty"`
	Max     corev1.ResourceList         `json:"max,omitempty"`
}

type ComponentDefinitionStatus struct {
	Phase              string `json:"phase,omitempty"` // "Valid" | "Invalid"
	Message            string `json:"message,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
}

// pipelinerun_types.go — PipelineRunStatus gains:
	PipelineGeneration int64                     `json:"pipelineGeneration,omitempty"`
	ResolvedSpec       *PipelineSpec             `json:"resolvedSpec,omitempty"` // frozen at admission
	Components         []ResolvedComponentStatus `json:"components,omitempty"`

type ResolvedComponentStatus struct {
	Name      string `json:"name"`
	Component string `json:"component"`
	Version   string `json:"version"`
	Image     string `json:"image"`
	ImageID   string `json:"imageID,omitempty"` // observed after pull (R7)
}

// helpers (component_types.go):
func IsStableVersion(v string) bool              // ^v\d+\.\d+\.\d+$
func (s *ComponentDefinitionSpec) LatestStable() (VersionSpec, bool) // highest semver among !Prerelease
func (s *ComponentDefinitionSpec) FindVersion(v string) (VersionSpec, bool)
```

- [ ] Failing tests: strict-decode a full ComponentDefinition YAML (use the RFC §4.1 example); `LatestStable` picks `v0.2.0` over `v0.1.9` and skips `dev`; `FindVersion("dev")` works; DeepCopy round-trips (pointer fields: `Resources`, `ResolvedSpec` — remember the repo rule: CRD pointer fields need `DeepCopyInto` updates).
- [ ] Implement types + hand-written DeepCopy (follow `pipeline_types.go` style) + register both new kinds in `groupversion_info.go`'s SchemeBuilder.
- [ ] `go build ./... && go test ./pkg/k8s/... -count=1` → PASS. Commit `feat(crd): ComponentDefinition types + PipelineRun resolved-snapshot status (RFC 026 P2)`.

### Task R3: CRD manifests

**Files:** Create `charts/datuplet-app/crds/datuplet.io_componentdefinitions.yaml` + copy in `utils/deploy/k8s/crds/`; modify both `datuplet.io_pipelineruns.yaml` copies (status additions).

- [ ] componentdefinitions manifest: cluster-scoped, structural schema mirroring R2 (write by hand following the existing pipelines manifest style), `status` subresource, printcolumns Phase/Age, and this CEL rule on the versions items (the ONLY CEL — everything else is reconciler-validated, spec §4.1):

```yaml
                x-kubernetes-validations:
                  - rule: "self.prerelease == true || (self.image.contains(':') && !self.image.endsWith(':latest'))"
                    message: "stable versions must pin a non-latest image tag"
```

- [ ] pipelineruns manifests: add `pipelineGeneration` (integer), `components` (array of the R2 status object), `resolvedSpec` (`x-kubernetes-preserve-unknown-fields: true` object — it embeds the pipeline spec; do NOT duplicate the whole spec schema here).
- [ ] Verify YAML parses (python3 yaml.safe_load) + both dir copies identical. Commit `feat(crd): componentdefinitions manifest + run snapshot status (RFC 026 P2)`.

### Task R4: Schema-validation module

**Files:** Create `pkg/pipeline/validate/schema.go` + `schema_test.go`; `go get github.com/santhosh-tekuri/jsonschema/v6 && make tidy`.

**Interfaces (produced):**

```go
func CompileSchema(raw string) (*jsonschema.Schema, error)
// ValidateConfig validates cfg against schema, with the RFC 026 §4.9 secret-ref rules.
func ValidateConfig(schema *jsonschema.Schema, rawSchema string, cfg map[string]any, pathPrefix string) []Finding
```

**Algorithm (encode exactly — this is the subtle part):**
1. Collect `refPaths`: every instance path whose value is a whole-scalar `$[name]` string (reuse the walker).
2. For each ref path, introspect the RAW schema JSON by path segments (`properties` / `items` / `additionalProperties`): if the schema at that location is resolvable and its `type` does not admit `"string"` → finding "secret ref on non-string field". Unresolvable (free-form) → OK.
3. Collect `secretPaths`: schema locations annotated `"x-datuplet-secret": true` (walk the raw schema JSON — do NOT rely on the library exposing unknown keywords). For each, if the instance has a non-ref value → finding "field requires a $[secret] reference"; absent value → no finding (that's `required`'s job).
4. Deep-copy cfg replacing every ref value with the sentinel `"x"`; run library validation; convert errors to Findings with JSON-pointer paths prefixed by `pathPrefix`; **drop findings anchored exactly at a ref path** (those are content assertions — `format`/`pattern`/`enum`/`minLength` etc. must not evaluate placeholders, spec §4.1).

- [ ] Failing tests: required-missing → finding; wrong type int-for-string → finding; `enum` field with `$[ref]` → NO finding; `integer` field with `$[ref]` → "non-string" finding; `x-datuplet-secret` field with plaintext → finding, with `$[ref]` → none; unknown property vs `additionalProperties: false` → finding; invalid schema string → `CompileSchema` error.
- [ ] Implement; verify the library ignores the unknown `x-datuplet-secret` keyword (if v6 rejects unknown keywords in its default mode, strip them from the JSON before compiling — the raw-JSON walk in step 3 is the source of truth either way).
- [ ] `go test ./pkg/pipeline/validate/ -count=1` → PASS. Commit `feat(validate): JSON Schema config validation with secret-ref semantics (RFC 026 P2)`.

### Task R5: Registry-aware validate

**Files:** Modify `pkg/pipeline/validate/validate.go` (+ new `registry.go`, tests).

**Interfaces (produced — Phase 3 appends `pol *Policy` to these signatures):**

```go
type RegistryView interface {
	// version "" = unpinned → defaultVersion, else highest stable.
	Resolve(component, version string) (*ResolvedComponent, []Finding)
}

type ResolvedComponent struct {
	Component  string
	Version    string
	Image      string
	Prerelease bool
	ConfigSchema *jsonschema.Schema
	Resources    datupletv1.ComponentResources // reused directly — single definition (R2); no validate-package copy
}

func ValidateTyped(p *datupletv1.Pipeline, reg RegistryView) []Finding
func ValidatePipeline(data []byte, reg RegistryView) (*datupletv1.Pipeline, []Finding, error)

// test/fallback impl, also used by callers that have a definition list:
type StaticRegistry map[string]datupletv1.ComponentDefinitionSpec // keyed by name
```

Resolution findings (all with path `stages[i].components[j].component|version`): unknown component; unknown version; prerelease referenced without explicit pin (i.e. resolution landed on a prerelease via default — impossible by construction, but a pipeline pinning `version: dev` is FINE; the finding is only for a component with NO stable versions and no pin); definition `status.phase == Invalid` → finding; `deprecated: true` → **warning** finding. After resolution, run R4's `ValidateConfig` with the version's schema (no schema → skip config validation, config still syntax-checked for secrets).

- [ ] Failing tests using `StaticRegistry`: each finding case above + happy path + config-schema violation surfaces with full path `stages[0].components[0].config.url`.
- [ ] Implement; `nil` RegistryView = skip registry checks (keeps Phase-1 callers compiling during the cutover inside this lane only — by R6 every real caller passes a registry).
- [ ] Update existing callers mechanically (`config.Parse`'s delegate, handlers, controllers) to pass `nil` for now — R6/R9 wire real views. Build green.
- [ ] Commit `feat(validate): RegistryView resolution + schema-validated config (RFC 026 P2)`.

### Task R6: Resolve-&-freeze cutover (the big one)

**Files:** Modify `pkg/k8s/api/v1/pipeline_types.go` (ComponentSpec: + `Component`, + `Version`, **delete `Image`**; DeepCopy), both `datuplet.io_pipelines.yaml` manifests, `pkg/k8s/controllers/pipelinerun_controller.go` (`handlePending`, `startStage`, `checkStageComponents`), `pipelinerun_jobs.go` (`buildComponentJob`, image/pullPolicy consumption sites from R1 recon), `pkg/pipeline/config/convert.go` (converter drops Image, gains Component/Version), operator main (wire a registry-backed `RegistryView` from the manager cache).

**Interfaces:**
- Consumes: R5 `RegistryView`/`ValidateTyped`; R3 manifests; R2 status fields.
- Produces: runs execute exclusively from `status.resolvedSpec` + `status.components`; `ComponentSpec` = `name/component/version/config/inputs/outputs/resources`.

- [ ] **Step 1: Failing controller tests:** (0) **`status.stageStatuses` keeps being populated** — one entry per stage, initialized at admission from the (now frozen) spec's stages, updated through stage progression exactly as today; the runs-list feature (merged in PR #23, `feat/rfc-023-runs-ux`) mirrors this field into Postgres and MUST NOT regress: pipeline-observer marshals `pr.Status.StageStatuses` verbatim into the `runs.stage_statuses` jsonb column (`pkg/pipelineapi/k8s/observer.go`), and `pkg/pipelineapi/http/run_timeline.go` unmarshals it back as `[]datupletv1.StageStatus` — keep the field's Go type and JSON shape identical; only its initialization source changes (frozen spec instead of live spec); (a) admission of a valid run against a fake-client registry → `status.resolvedSpec` set, `status.components` carries resolved version+image, Jobs use that image; (b) omitted `version:` → latest stable resolved & recorded; (c) `version: dev` (prerelease) → image used with `imagePullPolicy: Always`; stable → `IfNotPresent` (reconcile with the existing `runtimePullPolicy` helper: that helper governs the GATEWAY sidecar and stays; the component container policy is registry-driven); (d) unknown component → run `FailedUser`, message names it, no Jobs, no snapshot; (e) **freeze:** mutate the Pipeline spec + registry in the fake client after admission, reconcile subsequent stages → Jobs still built from the snapshot (image + config unchanged), `pipelineGeneration` records the validated generation; (f) mid-run reconcile never Gets the live Pipeline for job-building (assert `startStage`/`checkStageComponents` signatures no longer take the live pipeline — they take the run and read `pr.Status.ResolvedSpec`).
- [ ] **Step 2: Types + manifests:** swap `Image` → `Component`/`Version` in `ComponentSpec` (+ validate: `component` required — add the check to `ValidateTyped`'s port), update both pipelines manifests (`component` required string, `version` optional string, `image` property deleted).
- [ ] **Step 3: Controller:** in `handlePending`, after the Phase-1.5 secret snapshot / before any stage: `findings := validate.ValidateTyped(pipeline, r.Registry)`; findings → `FailedUser` (first finding in the status message). Then resolve every component, write `resolvedSpec` (DeepCopy of the validated spec), `pipelineGeneration`, `components[]`, update status ONCE. Rework `startStage`/`checkStageComponents`/`buildComponentJob` to read from `pr.Status.ResolvedSpec` and `pr.Status.Components` (match by component instance name); delete the live-Pipeline re-fetch at stage boundaries. The existing `status.stageStatuses` initialization/updates in this region (`pr.Status.StageStatuses`, sized from the stages list) switch their source to `resolvedSpec.Stages` — behavior identical, source frozen (the runs-list feature mirrors this field into Postgres and must not regress). `r.Registry` is a small adapter over the manager client (`client.Get` on ComponentDefinition — cached, cluster-scoped) implementing `RegistryView`; refuse `status.phase == Invalid` definitions. **Forward note (Phase 3):** Part 4's T7 extends THIS admission step to write *effective* resources (registry defaults applied, clamped to Max) into `resolvedSpec` — the registry is never re-read after admission, for resources either.
- [ ] **Step 4:** converter + any remaining `Image` consumers fixed (R1 recon list). `go build ./... && go test ./pkg/... -count=1` green.
- [ ] **Step 5: Commit** `feat(controller)!: component/version replace image; resolve-&-freeze run snapshot (RFC 026 P2)`.

### Task R7: `imageID` capture + error taxonomy

**Files:** Modify `pkg/k8s/controllers/pipelinerun_controller.go` (`checkStageComponents`), `pipelinerun_status.go`; tests.

- [ ] Failing tests: (a) pod reports `containerStatuses[name=="component"].imageID` → copied into `status.components[i].imageID` once, never overwritten; (b) registry `Get` returning a transient API error at admission → requeue with backoff, run NOT failed; after the retry budget (5 consecutive) → `FailedApplication` (≥20 contract); validation findings → `FailedUser` (already from R6 — assert the classification boundary explicitly).
- [ ] Implement; taxonomy table lands as a code comment mirroring spec §7.
- [ ] Commit `feat(controller): imageID observation + explicit error taxonomy (RFC 026 P2)`.

### Task R8: ComponentDefinition reconciler + operator RBAC

**Files:** Create `pkg/k8s/controllers/componentdefinition_controller.go` (+ tests); modify `cmd/pipeline-operator/main.go` (register), `charts/datuplet-app/templates/pipeline-operator/rbac.yaml`.

- [ ] Failing tests: valid def → `status.phase: Valid`, `observedGeneration` set; each invalid case → `Invalid` + message: configSchema doesn't compile (use R4 `CompileSchema`); stable version not `vX.Y.Z`; duplicate versions; `defaultVersion` not in `versions`; non-prerelease image without a tag or with `:latest` (belt over the CEL); `resources.default` limits exceeding `max` for any resource name.
- [ ] Implement reconciler (status-only writes); RBAC: add to the operator ClusterRole `componentdefinitions` `get,list,watch` + `componentdefinitions/status` `get,update,patch`.
- [ ] Commit `feat(operator): ComponentDefinition reconciler — registration-time validation (RFC 026 P2)`.

### Task R9: pipeline-api registry view, catalog, CLI, RBAC

**Files:** Create `pkg/pipelineapi/registry/view.go` (TTL-cached, 10 s, `List` via the existing controller-runtime client → `RegistryView`), `pkg/pipelineapi/http/component_handlers.go` (+ tests); modify router, `cmd/pipeline-api/admin.go` (new `admin component register|list|deprecate` subcommands using the same K8s client setup as `create-project`), `charts/datuplet-app/templates/pipeline-api/rbac.yaml`.

**Interfaces (produced — Phase 4 consumes):**
- `GET /api/v1/components` → `200 [{name, displayName, description, deprecated, defaultVersion, versions:[{version, prerelease, image}]}]` — any authenticated user (WithUser only; catalog is the shared picker, spec §4.7).
- `GET /api/v1/components/{name}` → same + per-version `configSchema` string. 404 unknown.
- CLI: `admin component register --file def.yaml` (server-side apply of the CR), `admin component list`, `admin component deprecate NAME`. REST admin mutation endpoints are **Phase 3** (need `mustBeSuperadmin`).

- [ ] Failing tests: catalog list/detail shapes; unauthenticated → 401; PUT save with an unknown component now returns the R5 finding (handler threads the real registry view into `ValidatePipeline` — replace R5's temporary `nil`).
- [ ] RBAC: pipeline-api ClusterRole + `componentdefinitions` `get,list,watch`.
- [ ] Commit `feat(api): component catalog + registry-aware validation + admin CLI (RFC 026 P2)`.

### Task R10: Chart built-in ComponentDefinitions

**Files:** Create `charts/datuplet-app/templates/components/` — one template per built-in: data-generator, http-json-extractor, finnhub-extractor, sql-transform, pandas-transform, stdout-writer; modify `values.yaml` (`components.registry`, `components.tag` — image refs `{{ .Values.components.registry }}/<name>:{{ .Values.components.tag }}`, default tag = chart appVersion, never `latest`).

- [ ] Schemas: derive from `docs/components.md` + component sources — minimum required properties per component: data-generator `tables[]` (name + `random{schema,limit{rowsCount,sizeInBytes,timeoutInSeconds},userErrorMessage}` | `literal{columns,rows}`); http-json-extractor `url` (required) + `array_path,table_name,headers,pagination{...}`; finnhub-extractor `mode` (enum of the 7 documented modes), `symbols`, `apiKey` with **`x-datuplet-secret: true`**, `lookback_days`, `limit`; sql-transform `sql` (required), `threads` (integer), `temp_directory`; stdout-writer `format` (enum csv/json); pandas-transform — read `components/pandas-transform/` source for its real keys. All schemas `additionalProperties: false`.
- [ ] Resources per def: sensible defaults (requests 100m/128Mi, limits 500m/512Mi) with `max` {cpu: "2", memory: 2Gi, ephemeral-storage: 10Gi}; sql-transform gets bigger defaults (limits 1/1Gi).
- [ ] `helm template charts/datuplet-app | python3 -c "import sys,yaml; list(yaml.safe_load_all(sys.stdin)); print('ok')"` → ok.
- [ ] Commit `feat(chart): built-in ComponentDefinition catalog (RFC 026 P2)`.

### Task R11: e2e — dev registrations, fixture flip, new scenarios

**Files:** Modify e2e bootstrap (framework applies test ComponentDefinitions before scenarios), all `tests/e2e/pipelines/k8s/*.yaml` (image→component flip), Makefile e2e image-build target (add a `docker tag datuplet/data-generator:latest datuplet/data-generator:v0.0.1` style second tag so ONE stable version can exist for the resolution scenario); add scenarios.

- [ ] Bootstrap registers each `datuplet/<name>:latest` local image as `version: dev, prerelease: true` (schema `{"type":"object"}` permissive — dev DX path proven continuously) AND data-generator additionally as stable `v0.0.1` (second tag) for the resolution test.
- [ ] Flip all fixtures: `image: datuplet/<name>:latest` → `component: <name>` + `version: dev`.
- [ ] **Scenario notes (deviation from RFC test-impact wording, flag in the PR):** `error-bad-config` keeps its meaning — its `userErrorMessage` config is schema-VALID, so it remains the runtime user-error scenario. NEW `error-schema-invalid.yaml` (data-generator with an unknown config key vs `additionalProperties:false`... but dev registration uses a permissive schema — so this scenario pins data-generator to the STABLE `v0.0.1` whose registration carries the real schema) → kubectl path: Pipeline `Invalid`, run `FailedUser`, zero pods.
- [ ] New scenarios: unpinned data-generator resolves `v0.0.1` + recorded in `status.components`; unknown component → `FailedUser` no pods; **freeze**: long-running generator (`timeoutInSeconds: 60`), mid-run `kubectl patch` the pipeline config + registry image, assert run completes from the snapshot (job image unchanged, `status.resolvedSpec` untouched).
- [ ] Commit `test(e2e): registry enforcement + freeze scenarios; fixtures on component refs (RFC 026 P2)`.

### Task R12: Examples + docs flip

**Files:** Modify `examples/pipelines/*.yaml` (all 6), `docs/components.md`, `examples/README.md`, `docs/pipeline-api.md` snippets if they show `image:`.

- [ ] Examples: `image: ghcr.io/kacurez/<n>:latest` → `component: <n>` (version omitted — float on the registry default; one example pins a version with a comment explaining pinning). Guard test green (it now validates against a registry? NO — the guard runs `config.Parse` which passes `nil` registry... **check**: after R9 the handlers pass a real view but `config.Parse`'s internal call keeps `nil` → guard still passes without a cluster. Acceptable and intended: the guard checks shape + semantics, not registry membership; note this in the guard's comment).
- [ ] `docs/components.md`: each entry gains "Registry name: `<name>` · default version from the chart"; YAML examples use `component:`.
- [ ] Commit `docs(examples): component refs everywhere (RFC 026 P2)`.

### Task R13: Phase gate

- [ ] `make tidy && go build ./... && go test ./... -count=1` green.
- [ ] `make e2e-k8s` (OrbStack) — CRD + controller + chart changed; fix via subagents (opus for controller failures).
- [ ] Cumulative Codex review `{base: "<phase-start SHA>", model: "gpt-5.5"}` → zero CRITICAL/MAJOR.
- [ ] Push + append the Phase 2 summary to the single draft PR body (`gh pr edit` — Branching model) — include the two documented deviations (resolvedSpec snapshot refinement; error-bad-config scenario nuance) for spec back-porting.

## Self-review checklist

- Spec coverage: §4.1 registry/prerelease/CEL/layered validation → R2/R3/R8; §4.2 component/version/image-drop → R6; §4.3 single path + resolve-&-freeze + snapshot + taxonomy → R5/R6/R7; §4.6 always-on enforcement → R6 (no permissive mode anywhere); §4.7 catalog API → R9; chart packaging note → R10; e2e test-impact Phase 2 bullets → R11. REST admin endpoints deliberately Phase 3 (superadmin gate) — recorded in R9.
- Interface names match the Phase 3/4 plan briefs verbatim (`RegistryView`, `ResolvedComponent`, `ComponentResources`, catalog shapes, status fields).
- Documented deviations: `status.resolvedSpec` (freeze actually delivered); `error-bad-config` stays runtime-error + new `error-schema-invalid` scenario.


---

<!-- ═══════════════ Part 4 — Phase 3 ═══════════════ -->

# RFC 026 Phase 3 — Superadmin Role & Resource Policy — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development — one fresh subagent per task, orchestrator reviews + runs the Codex gate between tasks. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Land RFC 026 §4.4–§4.6 — a platform-superadmin tier (FGA `server.admin`, UUID subjects) and the reject-then-clamp resource + gateway-bounds policy. Non-superadmins may not modify any `resources` block or exceed chart-configured gateway bounds (pipeline-api **rejects**, 403); the controller **clamps** to the registry ceiling as defense-in-depth for the direct-kubectl path. Superadmin-gated REST registry endpoints (`PUT/DELETE /api/v1/admin/components/{name}`) land here too. Registry `resources.default` is applied when a component sets nothing; a per-namespace `LimitRange` is belt-and-braces.

**Architecture:** Superadmin authorization reuses the existing FGA `server.admin` relation — no model change (spec §4.5, decision 7). `mustBeSuperadmin` (next to `mustHaveRelation` in `pkg/pipelineapi/http`) resolves the session user's UUID subject and calls `authzr.Check(user:oidc~<uuid>, admin, server:<uuid>)`. The `server:<uuid>` object is discovered once via FGA `/changes` pagination (the routine already in `cmd/pipeline-api/admin_lakekeeper.go` — `discoverServerObject`) and memoized in the authz layer. `admin grant --superadmin` writes the UUID-subject tuple `(user:oidc~<uuid>, admin, server:<uuid>)`; `lakekeeper-bootstrap` stays untouched (Option A — it is DB-free and keeps writing only the legacy literal `user:oidc~admin` tuple; the seed admin's UUID tuple is granted post-install: e2e bootstrap runs the grant in T10, real installs run the documented one-time grant — T8 docs note). Resource enforcement is one contract, two layers: `validate.ValidateTyped(p, reg, pol)` (Phase 2 signature, this phase adds `pol *Policy`) produces reject findings at the API; the PipelineRunReconciler computes **effective resources once at run admission** — `ResolvedComponent.Resources.Default` applied when nil, clamped to `.Max`, unlisted names stripped — and writes them into the frozen `status.resolvedSpec`; Job build consumes the snapshot verbatim (the registry is never re-read after admission). Branch: the single branch (Branching model), starting **after Phase 2's gate has passed**. Spec: `docs/superpowers/specs/2026-07-04-rfc-026-pipeline-config-safety-design.md` §4.4, §4.5, §4.6, §7.

**Tech Stack:** Go, controller-runtime, OpenFGA Go SDK + raw FGA REST (`/changes`), `k8s.io/api/core/v1` `ResourceList`/`ResourceRequirements`/`resource.Quantity`, manually-maintained CRD YAML (two copies — only if CRD types are touched; this phase does NOT touch CRD types), Helm values + env plumbing, GNU-free BSD/macOS shell.

## Global Constraints

Same as the Phase 0/1 plan (Part 1 (Phases 0+1) of this document):

- **Never push `main`, never `git tag`.** All work on the single branch; lands via the one draft PR (Branching model). Applies to partial-progress checkpoints too. (Repo rule + CLAUDE.md.)
- `go build ./... && go test ./...` green **before every commit**.
- `make tidy` (never bare `go mod tidy`) after any `go.mod` change — the repo is multi-module and drift fails CI. (No `go.mod` change is expected this phase; run `make tidy` only if you add a dependency.)
- CRD manifests are **manually maintained in two places**: `charts/datuplet-app/crds/` AND `utils/deploy/k8s/crds/`. Every CRD edit touches both. **This phase does not change CRD types** (resources/config/component fields all landed in Phase 1/2) — if a task finds it needs a CRD field, STOP and flag it; do not silently edit CRDs.
- POC greenfield (RFC §2): no deprecation shims, no dual-read, enforcement always on. No permissive mode.
- macOS/BSD environment: use file-edit tooling, not `sed -i`/GNU flags; do not assume GNU `grep`/ANSI-strip flags.
- Chart/operator/controller changes gate on `make e2e-k8s` against an OrbStack cluster before the PR is marked ready (this phase changes the controller, the chart, and project provisioning — the gate is required).
- Do not "fix" pre-existing quirks outside scope.
- Conventional commits (`feat:`, `fix:`, `refactor:`, `test:`, `docs:`), one logical commit per task.

**Anchor policy (inherited from the Phase 1.5 plan):** line numbers are given ONLY for files that Phases 1/1.5/2 do NOT rewrite (e.g. `cmd/pipeline-api/admin_lakekeeper.go`, `cmd/pipeline-api/admin.go`, `pkg/pipelineapi/authz/*`, `pkg/pipelineapi/k8s/namespace.go`, chart files). Everything Phase 1/1.5/2 rewrote — `pkg/pipeline/validate/*`, `pkg/k8s/api/v1/pipeline_types.go`, `pkg/k8s/api/v1/pipelinerun_types.go`, `pkg/k8s/controllers/pipelinerun_jobs.go`, `pkg/k8s/controllers/pipelinerun_controller.go`, `pkg/pipelineapi/http/pipeline_handlers.go` — is anchored by **function name only**. Task T1 verifies every anchor against merged reality before anything else runs and amends this plan inline if drift is found.

## Harness notes (orchestrator contract)

- **Branch:** continue on the single branch **after Phase 2's gate has passed** (Phase 3 depends on Phase 2's `RegistryView`, `ResolvedComponent`, `ComponentResources`, `ValidateTyped(p, reg)` signature, `status.components[]` snapshot, and CLI registry writes).
- **Parallel tasks within the lane** are marked `Parallel: yes (disjoint files)`. Run each in its own git worktree off the branch; merge back in task-number order (files are disjoint by design → clean merges). Sequential tasks run directly on the branch.
- **Per-task Codex gate (after the subagent commits, before the next task):**
  1. Orchestrator runs MCP tool `mcp__codex-cli__review` with `{commit: "<task commit SHA>", model: "gpt-5.5", title: "<task id>", workingDirectory: "<lane worktree>"}`.
  2. Acceptance: **zero CRITICAL or MAJOR findings on the task's diff.** MINOR findings: fix if ≤5 min, otherwise record in the PR description.
  3. Findings to fix → dispatch a fixer subagent (same model as the task) with the finding text verbatim; re-run the gate.
- **Phase gate (last task, T10):** cumulative `mcp__codex-cli__review` with `{base: "<phase-start SHA>", model: "gpt-5.5", title: "RFC026 Phase 3"}`, then `make e2e-k8s`, then push + PR-body update (Branching model).
- **Subagent dispatch:** give each subagent its full task text verbatim, plus the Global Constraints section, the branch/worktree to use, and nothing else. Suggested Agent-tool model per task is in the index below.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| T1 | Preflight (rebase checkpoint) | sonnet | Phase 2 gate passed | — |
| T2 | authz: `ServerObject`/`TypeServer` + memoized `ServerObject(ctx)` + `IsServerAdmin` + shared `/changes` discovery | sonnet | T1 | with T4 |
| T3 | `mustBeSuperadmin` helper + REST admin registry endpoints (`PUT/DELETE /api/v1/admin/components/{name}`) | sonnet | T2 | no |
| T4 | CLI: `admin grant --superadmin` (UUID-subject tuple; `lakekeeper-bootstrap` untouched — Option A) | sonnet | T1 | with T2 |
| T5 | `validate.Policy` type + gateway-bounds findings + resource reject rules | **opus** | T1 | with T2, T4 |
| T6 | Handler resources/gateway diff-gate on PUT (non-superadmin → 403) | **opus** | T3, T5 | no |
| T7 | Controller: effective resources at run admission (`Resources.Default` when nil + clamp to `Max` + strip unlisted names) frozen into `resolvedSpec`; Jobs consume the snapshot; clamp noted in run status | **opus** | T5 | with T8 |
| T8 | Chart: `pipelinePolicy` values block + env plumbing to pipeline-api & operator | sonnet | T1 | with T7 |
| T9 | `LimitRange` in project-namespace provisioning | sonnet | T8 | no |
| T10 | Phase gate: `make e2e-k8s` + bootstrap superadmin grant + cumulative Codex + PR-body update | sonnet | all | no |

Task DAG (arrows = "blocks"):

```
T1 → {T2, T4, T5, T8}
T2 → T3 ; {T3, T5} → T6
T5 → T7 ; T8 → T7 (Policy value shape) ; T8 → T9
{T6, T7, T9} → T10
```

Parallel batches the orchestrator can run concurrently in separate worktrees:
- After T1: **{T2, T4, T5, T8}** are file-disjoint (authz pkg / cmd CLI / validate pkg / chart). Merge in ID order.
- T7 and T9 are disjoint from T6 but T7 consumes T5's `Policy` and the T8 value shape; run T7 ∥ T8-follow-ups only after T5 merged.

---

### Task T1: Preflight (rebase checkpoint)

Verify the Phase 1/1.5/2 contract exists as this plan assumes; amend anchors inline if drift is found (record amendments in the task commit message; docs-only commit allowed). **Do not proceed to any other task until every check below passes or its anchor is amended.**

- [ ] **Validate package (Phase 2 signature):**
  `grep -rn "func ValidateTyped\|type RegistryView\|type ResolvedComponent\|type Finding" pkg/pipeline/validate/*.go`
  Expected symbols and shapes (fail loudly + amend downstream tasks if any differ):
  - `func ValidateTyped(p *datupletv1.Pipeline, reg RegistryView) []Finding` — Phase 3 (Task T5) CHANGES this to `(p *datupletv1.Pipeline, reg RegistryView, pol *Policy)`.
  - `type RegistryView interface{ Resolve(component, version string) (*ResolvedComponent, []Finding) }`
  - `type ResolvedComponent struct{ Component, Version, Image string; Prerelease bool; ConfigSchema *jsonschema.Schema; Resources datupletv1.ComponentResources }` — **`ComponentResources` lives in `pkg/k8s/api/v1` (single definition, Phase 2 R2), NOT in the validate package**; also verify it: `grep -n "type ComponentResources" pkg/k8s/api/v1/component_types.go` → `{ Default corev1.ResourceRequirements; Max corev1.ResourceList }`.
  - `type Finding struct{ Path, Message, Severity string }` with lowercase json tags `path`/`message`/`severity`; severities `"error"` | `"warning"`.
- [ ] **Run-admission snapshot (Phase 2):**
  `grep -rn "Components\|PipelineGeneration\|ResolvedComponent\|Resolved" pkg/k8s/api/v1/pipelinerun_types.go`
  Expected: `PipelineRunStatus.Components []` with per-entry `{Name, Component, Version, Image, ImageID string}` and `PipelineGeneration int64`. Record the EXACT Go type name of a status-component entry (likely `ResolvedComponentStatus` or similar) — Task T7 iterates it. **This plan refers to it as `status.components[]`; substitute the real type name found here.**
- [ ] **ComponentDefinition CRD + registry informer (Phase 2):**
  `test -f pkg/k8s/api/v1/component_types.go && echo YES`; `grep -rln "ComponentDefinition" pkg/k8s/controllers/ pkg/pipelineapi/`.
  Record: (a) the Go type for a ComponentDefinition + its `Resources {Default, Max}` fields (Task T7's controller path may read `ResolvedComponent.Resources` off the run snapshot rather than re-reading the CRD — confirm which); (b) how pipeline-api reads definitions (informer/cache client) and the exact client + write path the Phase-2 CLI `admin component register|deprecate` uses — Task T3's REST endpoints are thin wrappers over that SAME write path (spec §4.1 lifecycle). Write the client type + method names into Task T3's notes.
- [ ] **Job-build resource anchor (function names, Phase 2 rewrote this file):**
  `grep -n "func (r \*PipelineRunReconciler) buildComponentJob\|func (r \*PipelineRunReconciler) generateGatewayConfig\|Resources" pkg/k8s/controllers/pipelinerun_jobs.go`.
  Pre-Phase-2 the resource application was `if comp.Resources != nil { container.Resources = *comp.Resources }` (nil = unlimited). Record where the resolved component + its `Resources.Default/.Max` are reachable inside `buildComponentJob` post-Phase-2 (the job is built from `pr.Status.Components[]`, not the live spec). Amend Task T7's anchor accordingly.
- [ ] **Handler + authz anchors:**
  `grep -n "func (s \*Server) mustHaveRelation\|func (s \*Server) handlePutPipeline\|ValidatePipeline\|validate\." pkg/pipelineapi/http/pipeline_handlers.go` → both funcs present; note whether `handlePutPipeline` calls `config.Parse` or `validate.ValidatePipeline` after Phase 1/2 (Task T6 replaces its parse+store body). `grep -n "func UserObject\|func ProjectObject\|type Object\|type ObjectType\|Check(" pkg/pipelineapi/authz/types.go pkg/pipelineapi/authz/authorizer.go` → confirm `Check(ctx, user string, relation string, obj Object)` and no `TypeServer`/`ServerObject` yet.
- [ ] **Bootstrap/CLI anchors (files NOT rewritten — line numbers OK but re-grep):**
  `grep -n "func writeServerAdminTuple\|func discoverServerObject\|func resolveStoreIDByName\|user:oidc~admin" cmd/pipeline-api/admin_lakekeeper.go` → confirm `writeServerAdminTuple` writes the literal `user:oidc~admin` subject and `discoverServerObject` exists (Task T2 ports it into `authz`; Task T4 adds the UUID-subject tuple alongside).
  `grep -n "func adminGrant\|GetUserByEmail\|UserObject\|dialProjectProvisioning" cmd/pipeline-api/admin.go` → confirm `adminGrant` resolves `--user` email → `store.GetUserByEmail` → `u.ID` and writes via `env.authorizer.WriteTuples`; UUID subject via `authz.UserObject(u.ID.String())`.
- [ ] **Chart env-plumbing recon (feeds T8):** read `charts/datuplet-app/templates/pipeline-api/deployment.yaml` (env block ~line 79+) and `charts/datuplet-app/templates/pipeline-operator/deployment.yaml` (env block ~line 52+). Record the env-var naming convention (`DATUPLET_*` / `PIPELINE_API_*`) and confirm the operator already reads env at `cmd/pipeline-operator/main.go` (`os.Getenv("GATEWAY_IMAGE")` ~:135, reconciler constructed ~:200). Write the exact insertion points into T8's notes.
- [ ] **Provisioning recon (feeds T9):** confirm `pkg/pipelineapi/k8s/namespace.go` `EnsureProjectNamespace(ctx, c, projectID)` still creates the Namespace get-before-create, and `pkg/pipelineapi/runbackend/k8s.go` `storeProjectNS.Ensure` calls it. Record whether Phase 1.5 already added a per-namespace `Role`/`RoleBinding` create inside `Ensure` (if so, T9's `LimitRange` create slots in next to it; if not, T9 adds the first extra object).
- [ ] Commit (docs-only, if any anchors amended): `docs(plan): RFC 026 Phase 3 preflight — anchors reconciled with merged Phase 1/1.5/2`.

---

### Task T2: authz — `ServerObject`, memoized discovery, `IsServerAdmin`

**Model:** sonnet — **Parallel: yes (disjoint files) with T4.**

**Files:**
- Modify: `pkg/pipelineapi/authz/types.go` (add `TypeServer` + `ServerObject`)
- Create: `pkg/pipelineapi/authz/server_admin.go` (memoized discovery + `IsServerAdmin`)
- Create: `pkg/pipelineapi/authz/changes.go` (shared `/changes` discovery, ported from `cmd/pipeline-api/admin_lakekeeper.go`)
- Modify: `cmd/pipeline-api/admin_lakekeeper.go` (`writeServerAdminTuple` + `discoverServerObject` delegate to the shared authz helper — remove the local copy)
- Modify: `pkg/pipelineapi/authz/openfga.go` (`OpenFGAAuthorizer` captures `apiURL`/`apiKey`/`storeID` so it can serve discovery)
- Test: `pkg/pipelineapi/authz/server_admin_test.go`

**Interfaces:**
- Consumes: nothing new (existing `Authorizer.Check`, `Object`).
- Produces (consumed by T3, T4, T10 bootstrap):
  - `TypeServer ObjectType = "server"` and `func ServerObject(uuid string) Object` (kind `server`, id `<uuid>` — NO `oidc~` prefix; the server object id is a raw UUID, same shape as `discoverServerObject`'s `server:<uuid>` regex).
  - `func DiscoverServerObject(ctx context.Context, fgaURL, apiKey, storeID string) (string, error)` — the ported `/changes` pagination routine (returns the full `server:<uuid>` wire string, matching `^server:[0-9a-f-]+$`).
  - A `ServerAdmin` seam that pipeline-api's http layer uses:
    ```go
    // ServerAdminChecker resolves the FGA server object once (memoized) and
    // answers "is this user a platform superadmin?". Backed by
    // OpenFGAAuthorizer in production; stubbed in tests. Discovery is lazy +
    // memoized: the first IsServerAdmin call runs DiscoverServerObject, caches
    // the server:<uuid> string, and every subsequent call is a plain Check.
    type ServerAdminChecker interface {
        // ServerObject returns the memoized server:<uuid> wire string,
        // discovering it on first call. Safe for concurrent use.
        ServerObject(ctx context.Context) (string, error)
        // IsServerAdmin returns true iff (user:oidc~<userUUID>, admin, server:<uuid>)
        // holds. userUUID is the DB user UUID (NOT pre-prefixed); the impl
        // applies UserObject normalization. ErrAuthzUnavailable propagates.
        IsServerAdmin(ctx context.Context, userUUID string) (bool, error)
    }
    ```

**Complete skeleton — `pkg/pipelineapi/authz/types.go` additions:**

```go
// (add to the ObjectType const block)
const TypeServer ObjectType = "server"

// ServerObject returns the FGA object for the lakekeeper server singleton.
// The uuid parameter is the server object UUID lakekeeper writes on first
// bootstrap (discovered via DiscoverServerObject / the /changes feed). No
// normalization is applied — unlike UserObject, the server id carries no
// "oidc~" prefix. Wire form: "server:<uuid>".
func ServerObject(uuid string) Object { return Object{kind: string(TypeServer), id: uuid} }
```

**Complete skeleton — `pkg/pipelineapi/authz/server_admin.go`:**

```go
package authz

import (
	"context"
	"fmt"
	"sync"
)

// serverAdmin is the production ServerAdminChecker. It memoizes the
// server:<uuid> object discovered from the FGA /changes feed and then
// answers IsServerAdmin with a plain Check. The lakekeeper server object
// is written exactly once (first bootstrap) and never changes, so a
// process-lifetime memo with no TTL is correct.
type serverAdmin struct {
	authzr  Authorizer
	fgaURL  string
	apiKey  string
	storeID string

	mu     sync.Mutex
	server string // memoized "server:<uuid>"; "" until first discovery
}

// NewServerAdmin builds a ServerAdminChecker. fgaURL/apiKey/storeID feed
// the one-time /changes discovery; authzr issues the Check. All are
// available at pipeline-api startup (main.go resolves storeID via
// ResolveStoreAndModel before constructing the OpenFGAAuthorizer).
func NewServerAdmin(authzr Authorizer, fgaURL, apiKey, storeID string) ServerAdminChecker {
	return &serverAdmin{authzr: authzr, fgaURL: fgaURL, apiKey: apiKey, storeID: storeID}
}

func (s *serverAdmin) ServerObject(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.server != "" {
		return s.server, nil
	}
	obj, err := DiscoverServerObject(ctx, s.fgaURL, s.apiKey, s.storeID)
	if err != nil {
		return "", fmt.Errorf("discover server object: %w", err)
	}
	s.server = obj
	return obj, nil
}

func (s *serverAdmin) IsServerAdmin(ctx context.Context, userUUID string) (bool, error) {
	serverWire, err := s.ServerObject(ctx)
	if err != nil {
		return false, err
	}
	obj, err := ParseObject(serverWire) // "server:<uuid>" → Object{server,<uuid>}
	if err != nil {
		return false, fmt.Errorf("parse server object %q: %w", serverWire, err)
	}
	return s.authzr.Check(ctx, UserObject(userUUID).String(), "admin", obj)
}
```

**Complete skeleton — `pkg/pipelineapi/authz/changes.go`** (verbatim port of `discoverServerObject` from `cmd/pipeline-api/admin_lakekeeper.go:680`, exported, self-contained HTTP client):

```go
package authz

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
)

var serverObjectPattern = regexp.MustCompile(`^server:[0-9a-f-]+$`)

// DiscoverServerObject scans the FGA /changes feed for the single
// server:<uuid> tuple lakekeeper writes on first bootstrap and returns its
// wire form. Ported from cmd/pipeline-api/admin_lakekeeper.go so both the
// bootstrap CLI and serve-time superadmin checks share one implementation.
func DiscoverServerObject(ctx context.Context, fgaURL, apiKey, storeID string) (string, error) {
	c := &http.Client{Timeout: 30 * time.Second}
	token := ""
	for i := 0; i < 100; i++ {
		u := fmt.Sprintf("%s/stores/%s/changes?page_size=100", strings.TrimRight(fgaURL, "/"), storeID)
		if token != "" {
			u += "&continuation_token=" + token
		}
		req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
		if apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}
		resp, err := c.Do(req)
		if err != nil {
			return "", err
		}
		var page struct {
			Changes []struct {
				TupleKey struct{ Object string } `json:"tuple_key"`
			} `json:"changes"`
			ContinuationToken string `json:"continuation_token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			resp.Body.Close()
			return "", err
		}
		resp.Body.Close()
		for _, ch := range page.Changes {
			if serverObjectPattern.MatchString(ch.TupleKey.Object) {
				return ch.TupleKey.Object, nil
			}
		}
		if page.ContinuationToken == "" {
			break
		}
		token = page.ContinuationToken
	}
	return "", fmt.Errorf("no server:<uuid> tuple found")
}
```

- [ ] **Step 1: `admin_lakekeeper.go` de-dup.** Replace the local `discoverServerObject` (line ~680) with a thin call to `authz.DiscoverServerObject`; keep `writeServerAdminTuple` (line 531) writing the literal `user:oidc~admin` subject but source `serverObj` from `authz.DiscoverServerObject`. Remove the now-unused `regexp` import from `admin_lakekeeper.go` if nothing else uses it. `go build ./cmd/pipeline-api/` must stay green.
- [ ] **Step 2: `OpenFGAAuthorizer` field capture.** In `NewOpenFGAAuthorizer` (openfga.go), store `apiURL`, `apiKey`, `storeID` on the struct (currently only `fga`, `modelID`, `deadline`). Add unexported getters or exported fields as the impl needs — main.go (T3's wiring) will call `authz.NewServerAdmin(a, apiURL, apiKey, storeID)` using the values it already has in scope at construction time, so the getters are optional; prefer passing the values main.go already holds. **Do NOT change the `Authorizer` interface** — `ServerAdminChecker` is a separate seam.
- [ ] **Step 3: Failing test** (`server_admin_test.go`): a fake `Authorizer` (reuse `authztest.Fake` — it has `.Allow(user, relation, obj)` + exact-match `Check`) plus a `serverAdmin` whose discovery is stubbed. Because `DiscoverServerObject` does live HTTP, the test injects the memo directly: seed `s.server = "server:11111111-1111-1111-1111-111111111111"` via a test-only constructor or by exporting a `newServerAdminWithObject(authzr, obj)` internal helper. Cases: (a) user with `.Allow(UserObject("u1").String(), "admin", ServerObject("1111..."))` → `IsServerAdmin(ctx,"u1")` true; (b) unseeded user → false; (c) `authzr.Check` returns `ErrAuthzUnavailable` → propagated. Assert `ServerObject(ctx)` is called once across two `IsServerAdmin` calls (memoization) — count via a fake that increments a discovery counter.
- [ ] **Step 4:** `go build ./... && go test ./pkg/pipelineapi/authz/... ./cmd/pipeline-api/... -count=1` → PASS.
- [ ] **Step 5: Commit** `feat(authz): server-object discovery + memoized IsServerAdmin superadmin check (RFC 026 P3)`.

---

### Task T3: `mustBeSuperadmin` + REST admin registry endpoints

**Model:** sonnet — **Parallel: no** (touches `server.go` route table + `pipeline_handlers.go` neighborhood + a new handler file).

**Files:**
- Create: `pkg/pipelineapi/http/superadmin.go` (`mustBeSuperadmin` helper)
- Create: `pkg/pipelineapi/http/component_admin_handlers.go` (`PUT`/`DELETE /api/v1/admin/components/{name}`)
- Modify: `pkg/pipelineapi/http/server.go` (`Server` gains a `serverAdmin authz.ServerAdminChecker` field + `WithServerAdmin` builder + route registration; add a registry-writer seam field)
- Modify: `cmd/pipeline-api/main.go` (construct `authz.NewServerAdmin(...)` right after the authorizer at ~line 169 and wire it via `WithServerAdmin`; wire the registry writer)
- Modify: the `GET /api/v1/auth/me` handler (locate via `grep -rn "auth/me" pkg/pipelineapi/http/`) — response gains `is_superadmin` (see Step 3b)
- Test: `pkg/pipelineapi/http/superadmin_test.go`, `pkg/pipelineapi/http/component_admin_handlers_test.go`

**Interfaces:**
- Consumes: T2's `authz.ServerAdminChecker`; Phase 2's ComponentDefinition write path (recorded in T1 — the same client/method the CLI `admin component register|deprecate` uses; **reference it exactly, do not invent a parallel writer**).
- Produces (consumed by T6):
  - `func (s *Server) mustBeSuperadmin(w http.ResponseWriter, r *http.Request) (*store.User, bool)` — next to `mustHaveRelation`. On success returns the resolved user + true; on failure writes the HTTP error and returns `ok=false`.
  - `PUT /api/v1/admin/components/{name}` (body: a ComponentDefinition spec JSON/YAML) → 204/200; `DELETE /api/v1/admin/components/{name}` → 204. Both superadmin-gated. Thin wrappers over the Phase-2 K8s write path.
  - **`GET /api/v1/auth/me` gains `"is_superadmin": <bool>`** — computed via the same `ServerAdminChecker`; `false` when the checker is unwired or errors (whoami must never fail over authz availability). This is a **consumed interface of the Phase 4 plan** (the builder hides the `resources` affordance unless true); the JSON key name `is_superadmin` is fixed — Phase 4's T1 preflight greps for it.

**Complete skeleton — `pkg/pipelineapi/http/superadmin.go`:**

```go
package http

import (
	"errors"
	"net/http"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// mustBeSuperadmin is the platform-admin guard, sibling to mustHaveRelation.
// It resolves the authenticated user and runs a single FGA Check of
// (user:oidc~<user-uuid>, admin, server:<uuid>) via the memoized
// ServerAdminChecker.
//
// Error mapping (mirrors mustHaveRelation):
//   - 401: no authenticated user.
//   - 503: superadmin checker not wired, or authz backend unavailable
//          (ErrAuthzUnavailable — includes server-object discovery failure).
//   - 500: unexpected authz error.
//   - 403: authenticated but not a superadmin.
//
// On success returns (user, true); callers must return immediately on false.
func (s *Server) mustBeSuperadmin(w http.ResponseWriter, r *http.Request) (*store.User, bool) {
	user, authed := auth.UserFromContext(r.Context())
	if !authed {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return nil, false
	}
	if s.serverAdmin == nil {
		writeError(w, http.StatusServiceUnavailable, "superadmin checks not configured")
		return nil, false
	}
	ok, err := s.serverAdmin.IsServerAdmin(r.Context(), user.ID.String())
	if errors.Is(err, authz.ErrAuthzUnavailable) {
		writeError(w, http.StatusServiceUnavailable, "authz backend unavailable")
		return nil, false
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "check superadmin")
		return nil, false
	}
	if !ok {
		writeError(w, http.StatusForbidden, "forbidden: superadmin required")
		return nil, false
	}
	return user, true
}
```

- [ ] **Step 1: `Server` wiring.** Add fields `serverAdmin authz.ServerAdminChecker` and a registry writer seam (e.g. `componentAdmin ComponentRegistryWriter` — a small interface `{ Put(ctx, name string, specYAML []byte) error; Delete(ctx, name string) error }` implemented in Phase-2 code / a thin adapter over the ComponentDefinition K8s client). Add `WithServerAdmin(c authz.ServerAdminChecker) *Server` and `WithComponentAdmin(w ComponentRegistryWriter) *Server`. Register routes only when both `s.resolver != nil && s.serverAdmin != nil && s.componentAdmin != nil`:
  ```go
  mux.Handle("PUT /api/v1/admin/components/{name}", auth.WithUser(s.resolver, http.HandlerFunc(s.handlePutComponentDefinition)))
  mux.Handle("DELETE /api/v1/admin/components/{name}", auth.WithUser(s.resolver, http.HandlerFunc(s.handleDeleteComponentDefinition)))
  ```
- [ ] **Step 2: Handlers** (`component_admin_handlers.go`): each begins `if _, ok := s.mustBeSuperadmin(w, r); !ok { return }`, validates `{name}` (DNS-1123 like the pipeline name check in `handlePutPipeline`), reads the body with a `MaxBytesReader` (1 MiB, same as pipeline PUT), and delegates to `s.componentAdmin.Put/Delete`. The write path is Phase 2's — a superadmin-gated thin wrapper, NOT a re-implementation of definition validation (Phase 2's reconciler still sets `status.phase Valid/Invalid`). Return 204 on success; map a Phase-2 "not found" on DELETE to 404.
- [ ] **Step 3: main.go wiring.** Right after the authorizer block (~line 169, where `storeID`, `openfgaURL`, `openfgaAPIKey` are in scope) add:
  ```go
  serverAdmin := authz.NewServerAdmin(authzr, openfgaURL, openfgaAPIKey, storeID)
  ```
  and pass it via `.WithServerAdmin(serverAdmin)` on the Server builder chain; wire the ComponentRegistryWriter over the Phase-2 ComponentDefinition client. (Guard: only construct when `authzr != nil` — the superadmin routes stay unregistered otherwise, matching the existing authz-gated route pattern.)
- [ ] **Step 3b: `is_superadmin` on `GET /api/v1/auth/me`.** In the me/whoami handler, add `is_superadmin` to the response JSON: `false` when `s.serverAdmin == nil`; otherwise `ok, _ := s.serverAdmin.IsServerAdmin(ctx, user.ID.String())` with any error degraded to `false` + a log line (the whoami call must never 5xx over authz availability). Failing test first: superadmin stub true → body contains `"is_superadmin":true`; stub error → 200 with `"is_superadmin":false`.
- [ ] **Step 4: Failing tests.** `superadmin_test.go`: a stub `ServerAdminChecker` (returns true/false/`ErrAuthzUnavailable`); assert 200-path returns the user, non-admin → 403, unavailable → 503, unwired → 503, unauthenticated → 401. `component_admin_handlers_test.go`: superadmin PUT → 204 and the stub writer received the name+body; non-superadmin PUT → 403 and writer NOT called; DELETE happy + 404.
- [ ] **Step 5:** `go build ./... && go test ./pkg/pipelineapi/http/... ./cmd/pipeline-api/... -count=1` → PASS.
- [ ] **Step 6: Commit** `feat(api): mustBeSuperadmin guard + superadmin-gated component registry REST (RFC 026 P3)`.

---

### Task T4: CLI — `admin grant --superadmin` (seed lands via grant — Option A)

**Model:** sonnet — **Parallel: yes (disjoint files) with T2** (T4 no longer touches `admin_lakekeeper.go` — that file is T2's, which ports the discovery routine out of it; keeping them disjoint is what makes the parallel claim true).

**Files:**
- Modify: `cmd/pipeline-api/admin.go` (`adminGrant` — add `--superadmin` flag; writes the UUID-subject server tuple)
- Test: `cmd/pipeline-api/admin_test.go` (or the existing admin test file — extend, don't create a parallel one if one exists)

**Interfaces:**
- Consumes: T2's `authz.ServerObject`, `authz.DiscoverServerObject`; existing `store.GetUserByEmail`, `env.authorizer.WriteTuples`, `authz.UserObject`.
- Produces: `pipeline-api admin grant --user EMAIL --superadmin` writes `(user:oidc~<user-uuid>, admin, server:<uuid>)`. `lakekeeper-bootstrap` is NOT modified (Option A — it keeps writing only the legacy literal `user:oidc~admin` tuple for lakekeeper's own console; the seed admin's UUID tuple is granted post-install via this command: e2e runs it in T10, real installs follow the T8 docs note — spec §4.5).

**Design decisions (resolve, do not invent):**
- **`--superadmin` is mutually independent of `--project`/`--role`.** When `--superadmin` is set, the grant targets the server object, NOT a project; `--project` becomes optional and is ignored (or error if both `--superadmin` and `--role` non-default are set — pick: error, clearer). The server UUID is discovered via `authz.DiscoverServerObject(ctx, openfgaURL, apiKey, storeID)`. `adminGrant` currently gets its authorizer + FGA connection via `dialProjectProvisioning` (which resolves `storeID`/`openfgaURL`/`apiKey` from flags/env at admin.go:137+). Reuse those resolved values — thread them out of `dialProjectProvisioning` (add `fgaURL`, `apiKey`, `storeID` to the returned `projectProvisioningEnv` struct) so the grant path can call `DiscoverServerObject`.
- **Seed-admin email for bootstrap:** the seed admin's UUID is not known at bootstrap time from FGA alone (bootstrap runs DB-free today — admin.go routes `lakekeeper-bootstrap` before the DB open). Two options; pick **Option A**:
  - **Option A (recommended):** bootstrap keeps writing only the literal `user:oidc~admin` tuple; the UUID-subject seed tuple is written by the **`initAdmin` provisioning path** (the chart's init flow that creates `admin@datuplet.local`). Add a new subcommand step or fold it into `admin grant --user <initAdminEmail> --superadmin` invoked by the same Job that seeds the admin user. Document this in the task. Rationale: bootstrap is DB-free by design; resolving email→UUID needs the DB.
  - **Option B (only if the seed flow already opens the DB):** if T1 recon shows the bootstrap Job has DB access, resolve the seed admin email→UUID there and write the tuple in `adminLakekeeperBootstrap` right after `writeServerAdminTuple`. Riskier — do not force a DB dependency into the DB-free path.

  **The plan's default is Option A**: the actual UUID-subject seed tuple lands via `admin grant --superadmin`, invoked for the init admin by the bootstrap superadmin-grant step added in Task T10 (e2e) and by a chart hook. Task T4 ships the `--superadmin` grant machinery; the wiring of *who* gets seeded is Task T8 (chart) / T10 (e2e).

- [ ] **Step 1: thread FGA coords out of `dialProjectProvisioning`.** Add `fgaURL string`, `apiKey string`, `storeID string` to `projectProvisioningEnv`; populate from the already-resolved `*openfgaURL`, `*apiKey`, `*storeID` inside `dialProjectProvisioning` (they're computed at admin.go:172-187). No behavior change for existing callers.
- [ ] **Step 2: `--superadmin` flag in `adminGrant`.** Add `superadmin := fs.Bool("superadmin", false, "Grant platform superadmin (FGA server.admin) to --user instead of a project role")`. Branch: when `*superadmin`:
  ```go
  serverWire, err := authz.DiscoverServerObject(ctx, env.fgaURL, env.apiKey, env.storeID)
  if err != nil { return fmt.Errorf("discover server object: %w", err) }
  serverObj, err := authz.ParseObject(serverWire)
  if err != nil { return err }
  tuple := authz.Tuple{
      User:     authz.UserObject(u.ID.String()).String(),
      Relation: "admin",
      Object:   serverObj,
  }
  if err := env.authorizer.WriteTuples(ctx, []authz.Tuple{tuple}); err != nil {
      return fmt.Errorf("write superadmin tuple: %w", err)
  }
  fmt.Printf("Granted superadmin (server.admin) to %s\n  FGA tuple: %s admin %s\n", u.Email, tuple.User, serverWire)
  return nil
  ```
  Guard: `--superadmin` skips the `--project` requirement (`adminGrant` currently errors if `--project` is empty — gate that behind `!*superadmin`). If both `--superadmin` and a non-default `--role` are passed → error `"--superadmin cannot be combined with --role/--project"`.
- [ ] **Step 3 (Option A wiring stub):** leave `adminLakekeeperBootstrap` writing only the `user:oidc~admin` tuple (unchanged). Add a short comment there pointing to `admin grant --superadmin` as the UUID-subject seed path. (No code beyond the comment — the actual seed invocation is Task T8/T10.)
- [ ] **Step 4: Failing test.** Table test for `adminGrant`'s role→tuple mapping is likely already present; extend with a `--superadmin` case using a fake authorizer + a stubbed `DiscoverServerObject`. Since `DiscoverServerObject` does live HTTP, make the superadmin branch call an injectable func var (package-level `var discoverServerObjectFn = authz.DiscoverServerObject`) so the test can override it — keep the seam minimal and documented. Assert the written tuple is `(user:oidc~<uuid>, admin, server:<uuid>)` and that `--superadmin` without `--project` does not error.
- [ ] **Step 5:** `go build ./... && go test ./cmd/pipeline-api/... -count=1` → PASS.
- [ ] **Step 6: Commit** `feat(cli): admin grant --superadmin writes UUID-subject server.admin tuple (RFC 026 P3)`.

---

### Task T5: `validate.Policy` + gateway-bounds findings + resource reject rules

**Model:** **opus** (clamp/reject semantics + full ResourceList arithmetic) — **Parallel: yes (disjoint files) with T2, T4.**

**Files:**
- Create: `pkg/pipeline/validate/policy.go` (`Policy`, `GatewayBounds`, resource-rule helpers)
- Modify: `pkg/pipeline/validate/validate.go` (change `ValidateTyped` signature to add `pol *Policy`; call the new checks)
- Modify: every in-repo caller of `ValidateTyped` / `ValidatePipeline` to pass a policy (nil where none) — grep-driven (`pipeline_handlers.go`, `pipelinerun_controller.go`, `pipeline_controller.go`, `config.Parse` if it calls through, examples guard test). **Mechanical: pass `nil` at every existing call site except where a task later threads a real policy (T6 handler, T7 controller).**
- Test: `pkg/pipeline/validate/policy_test.go`

**Interfaces:**
- Consumes: T1-confirmed `RegistryView`/`ResolvedComponent`/`ComponentResources`/`Finding`.
- Produces (consumed by T6 handler + T7 controller):
  ```go
  // Policy carries the chart-configured bounds a non-superadmin pipeline
  // must satisfy. A nil *Policy passed to ValidateTyped disables ALL bound
  // checks (used by callers that don't enforce policy, and by the kubectl
  // path before the controller clamps). (RFC 026 §4.4, §4.6)
  type Policy struct {
      Gateway GatewayBounds
  }

  // GatewayBounds are the per-pipeline gateway-knob ceilings. A zero value
  // for any field means "no bound on that knob".
  type GatewayBounds struct {
      MaxChunkSize      int64
      MaxBufferSize     int64
      MaxTargetFileSize int64
  }
  ```
  New signature: `func ValidateTyped(p *datupletv1.Pipeline, reg RegistryView, pol *Policy) []Finding`. `ValidatePipeline` (if it wraps `ValidateTyped`) grows the same trailing `pol *Policy` param and forwards it. **Comment in code:** this is the Phase-3 extension of the Phase-2 signature.

**Resource reject rules (spec §4.4 — implement exactly):**
For each component, after `reg.Resolve(...)` yields `ResolvedComponent.Resources` (`.Max corev1.ResourceList`):
1. If the component sets no `resources` → **no finding** (defaults are applied by the controller, not here).
2. If it sets `resources`, for EVERY resource name that appears in either `requests` or `limits`:
   - name absent from `.Max` → `Finding{Path: "stages[i].components[j].resources", Message: "resource \"<name>\" is not allowed for component <c> (not listed in registry max)", Severity: "error"}`.
   - `limits[name] > Max[name]` OR `requests[name] > Max[name]` → `Finding{... Message: "resources.<limits|requests>.<name>=<q> exceeds registry max <maxq>", Severity: "error"}`. Compare via `q.Cmp(maxq) > 0` on `resource.Quantity`.
   - Iterate the FULL `ResourceList` including `ephemeral-storage` and any extended resources — do NOT special-case cpu/memory.
3. **Who is allowed to set `resources` at all is NOT decided here** — this task only produces findings on *over-max / unlisted* resources. The superadmin diff-gate (any modification by a non-superadmin → 403) is Task T6, in the handler, because it needs the stored-pipeline comparison + the identity. `validate` is identity-agnostic.

**Gateway-bounds rules (spec §4.6):** when `pol != nil`, read the pipeline's gateway knobs (T1 recon: confirm the CRD field path — likely `p.Spec.Gateway.ChunkSize`/`BufferSize`/`TargetFileSize` as `int64`/`*int64`). For each knob that is set and whose corresponding bound in `pol.Gateway` is non-zero: `knob > bound` → `Finding{Path: "gateway.<knob>", Message: "gateway.<knob>=<n> exceeds policy max <bound>", Severity: "error"}`.

**Complete skeleton — `pkg/pipeline/validate/policy.go` (resource helper):**

```go
package validate

import (
	corev1 "k8s.io/api/core/v1"
)

// checkResourcesAgainstMax appends a Finding for every requests/limits entry
// whose resource name is absent from max, or whose quantity exceeds the max
// for that name. The full ResourceList is walked (cpu, memory,
// ephemeral-storage, extended resources). Returns findings; does not mutate.
// A nil-or-empty rr yields no findings (defaults handled by the controller).
func checkResourcesAgainstMax(rr *corev1.ResourceRequirements, max corev1.ResourceList, pathPrefix, component string) []Finding {
	if rr == nil {
		return nil
	}
	var out []Finding
	check := func(kind string, list corev1.ResourceList) {
		for name, q := range list {
			maxq, ok := max[name]
			if !ok {
				out = append(out, Finding{
					Path:     pathPrefix + ".resources",
					Message:  "resource \"" + string(name) + "\" is not allowed for component " + component + " (not listed in registry max)",
					Severity: "error",
				})
				continue
			}
			if q.Cmp(maxq) > 0 {
				out = append(out, Finding{
					Path:     pathPrefix + ".resources." + kind + "." + string(name),
					Message:  "resources." + kind + "." + string(name) + "=" + q.String() + " exceeds registry max " + maxq.String(),
					Severity: "error",
				})
			}
		}
	}
	check("limits", rr.Limits)
	check("requests", rr.Requests)
	return out
}
```

- [ ] **Step 1: Failing tests** (`policy_test.go`), table-driven, minimum cases:
  1. component with no `resources`, any Max → 0 findings.
  2. `limits.cpu: "4"` vs `Max.cpu: "2"` → 1 finding, message contains `exceeds registry max`.
  3. `requests.memory: 3Gi` vs `Max.memory: 2Gi` → finding.
  4. `limits.ephemeral-storage: 20Gi` vs `Max` with ephemeral-storage 10Gi → finding (proves full ResourceList).
  5. `limits."nvidia.com/gpu": 1` with a Max lacking that name → "not allowed / not listed in registry max" finding (proves unlisted-name rule for extended resources).
  6. `limits.cpu: "1"` vs `Max.cpu: "2"` → 0 findings (under max passes).
  7. `pol == nil` → gateway knobs ignored, no gateway findings even when huge.
  8. `pol.Gateway.MaxBufferSize = 512Mi`, pipeline bufferSize 1Gi → 1 gateway finding; bufferSize 256Mi → 0.
  9. `pol.Gateway.MaxChunkSize = 0` (unset bound) with any chunkSize → 0 findings (zero bound = no check).
- [ ] **Step 2: Run** `go test ./pkg/pipeline/validate/ -run Policy -v` → FAIL (Policy undefined).
- [ ] **Step 3: Implement.** Add `policy.go` with the types + `checkResourcesAgainstMax` + a `checkGatewayBounds(p, pol)` helper. In `validate.go`, change `ValidateTyped` to accept `pol *Policy`; after the Phase-2 registry resolution loop, for each component call `checkResourcesAgainstMax(comp.Resources, resolved.Resources.Max, "stages[i].components[j]", comp.Component)` and append; after the component loop, if `pol != nil` append `checkGatewayBounds(p, pol)`. **Preserve every existing Phase-2 finding** — only append.
- [ ] **Step 4: Mechanical caller update.** `grep -rn "ValidateTyped(\|ValidatePipeline(" --include=*.go . | grep -v _test.go` and add a trailing `nil` policy arg at each site (T6/T7 will replace their `nil` with a real policy in their own tasks). Update the examples guard test call if it calls `ValidatePipeline`.
- [ ] **Step 5:** `go build ./... && go test ./pkg/pipeline/... -count=1` → PASS.
- [ ] **Step 6: Commit** `feat(validate): Policy + gateway-bounds + reject-over-max/unlisted resource findings (RFC 026 P3)`.

---

### Task T6: Handler resources/gateway diff-gate on PUT

**Model:** **opus** (diff semantics — "unchanged resubmission passes; any modification by a non-superadmin → 403") — **Parallel: no** (edits `handlePutPipeline`).

**Files:**
- Modify: `pkg/pipelineapi/http/pipeline_handlers.go` (`handlePutPipeline`)
- Create: `pkg/pipelineapi/http/resource_diff.go` (the old-vs-new comparison logic)
- Test: `pkg/pipelineapi/http/pipeline_handlers_test.go` (extend) + `pkg/pipelineapi/http/resource_diff_test.go`

**Interfaces:**
- Consumes: T3's `mustBeSuperadmin` (indirect — via `s.serverAdmin.IsServerAdmin`), T5's `Policy`, the stored pipeline via `s.pipelines.GetByName` (Phase-0 `PipelineStore` — returns `PipelineDetail{YAML}`), `validate.ValidatePipeline`/`ValidateTyped`.
- Produces the §4.4 + §7 contract: a non-superadmin PUT that **modifies any `resources` block** OR **exceeds gateway bounds** → **403** with a clear message; unchanged resubmission of an admin-set value passes; superadmin PUT always passes the diff-gate (still subject to reject-over-max findings from T5 → 400). Clean save → 204.

**Diff-gate semantics (spec §4.4, implement exactly):**
1. Parse the incoming body into `*datupletv1.Pipeline` (strict decode — Phase 1 `validate.ValidatePipeline`).
2. Determine `isSuperadmin` once: `ok, err := s.serverAdmin.IsServerAdmin(ctx, userID)` (nil checker → treat as not-superadmin; `ErrAuthzUnavailable` → 503).
3. If **not** superadmin:
   - Load the **stored** pipeline (`s.pipelines.GetByName(ctx, projectID, name)`); if none (first create), the "old" spec is empty.
   - Parse the stored YAML into `*datupletv1.Pipeline` (same strict decode; a stored-but-unparseable pipeline should not hard-block — treat parse failure as "old resources = none" and rely on the reject findings, but log it).
   - **Resources diff:** for each component (matched by `name`), compare `new.resources` vs `old.resources` with `reflect.DeepEqual` (or `apiequality.Semantic.DeepEqual` from `k8s.io/apimachinery/pkg/api/equality`, which handles `resource.Quantity` canonical forms — **prefer `apiequality.Semantic.DeepEqual`** so `"1"` vs `"1000m"` counts as unchanged). Any component whose resources changed (added, removed, or altered), OR a new component that sets non-nil resources → **403**. A component that had resources and still has the identical block → OK.
   - **Gateway diff:** if the incoming gateway knobs exceed `pol.Gateway` bounds → 403 (this overlaps T5's finding, but the handler returns 403 for policy per §7, not 400). Simpler: run `validate.ValidateTyped(new, reg, pol)` and if any gateway-bound finding is present AND user is not superadmin → 403. Keep resource-over-max as 400 findings (T5) regardless of identity — over-max is a validation error, modifying-without-privilege is a 403. **Precedence:** check the diff-gate (403) BEFORE emitting reject findings (400), so a non-superadmin editing resources gets the clear "superadmin required" 403, not a confusing 400.
4. If superadmin: skip the diff-gate entirely AND validate with **`pol = nil`** — gateway bounds do not apply to superadmins (spec §4.6: "non-superadmins must stay within these"; a superadmin exceeding a bound is the intended use, review M7). Resource-over-max findings still apply to everyone — they come from the registry `Max` via `reg`, not from `pol`, so passing `pol=nil` keeps the max ceiling intact (spec §4.4: raising the ceiling means editing the registry, not the pipeline; the superadmin bypass covers the *modification* gate and the *gateway bounds*, never the registry *max*).

**Complete skeleton — `pkg/pipelineapi/http/resource_diff.go`:**

```go
package http

import (
	apiequality "k8s.io/apimachinery/pkg/api/equality"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
)

// resourcesModified reports whether any component's resources block differs
// between the old (stored) and new (incoming) pipeline specs. Components are
// matched by name. A component present in new with non-nil resources but
// absent from old counts as modified. Semantic quantity equality is used so
// "1" and "1000m" are treated as identical. (RFC 026 §4.4 diff-gate.)
func resourcesModified(oldP, newP *datupletv1.Pipeline) bool {
	oldRes := resourcesByComponent(oldP) // map[componentInstanceName]*corev1.ResourceRequirements
	for _, comp := range allComponents(newP) {
		newR := comp.Resources
		oldR := oldRes[comp.Name]
		if !apiequality.Semantic.DeepEqual(oldR, newR) {
			return true
		}
	}
	// Also catch a component removed from new that HAD resources in old:
	// removing an admin-set resources block is itself a modification.
	newRes := resourcesByComponent(newP)
	for name, oldR := range oldRes {
		if oldR == nil {
			continue
		}
		if _, ok := newRes[name]; !ok {
			return true
		}
	}
	return false
}
```

(`resourcesByComponent` and `allComponents` are small local helpers that flatten `p.Spec.Stages[].Components[]` — T1 recon confirms the exact spec path; write them against the real field names.)

- [ ] **Step 1: Failing tests.** Extend `pipeline_handlers_test.go` (it already builds a Server with `authztest.Fake` + `data_admin` seeded — reuse that harness; add a stub `ServerAdminChecker`). Cases:
  1. non-superadmin PUT adding a `resources` block where the stored pipeline had none → 403, body message mentions superadmin.
  2. non-superadmin PUT re-submitting the SAME resources block that's already stored → 204 (unchanged passes).
  3. non-superadmin PUT changing `limits.cpu` from stored `"1"` to `"2"` → 403.
  4. non-superadmin PUT with resources `"1000m"` where stored is `"1"` → 204 (semantic-equal, unchanged).
  5. superadmin PUT setting a fresh `resources` block within Max → 204.
  6. superadmin PUT setting resources OVER Max → 400 with a reject finding (max ceiling applies to superadmins too).
  7. non-superadmin PUT exceeding a gateway bound → 403.
  8. `ServerAdminChecker` unavailable → 503.
  `resource_diff_test.go`: unit-test `resourcesModified` for add/remove/alter/semantic-equal.
- [ ] **Step 2: Implement** in `handlePutPipeline`: after the existing name/DNS checks and body read, run the diff-gate logic above BEFORE `s.pipelines.Put`. Thread the real `*validate.Policy` (from a `Server` field wired in T8's main.go plumbing — add `s.policy *validate.Policy` + `WithPipelinePolicy`) and the registry view. Emit 403 for the modification gate + gateway-bound-for-non-superadmin; 400 for reject findings; 204 on clean save.
- [ ] **Step 3:** `go build ./... && go test ./pkg/pipelineapi/http/... -count=1` → PASS.
- [ ] **Step 4: Commit** `feat(api): superadmin-gated resources/gateway diff-gate on pipeline PUT (RFC 026 P3)`.

---

### Task T7: Controller — apply defaults + clamp to Max + strip unlisted names

**Model:** **opus** (clamp arithmetic + strip semantics + status message) — **Parallel: yes (disjoint files) with T8.**

**Freeze rule (review C2 — this is the load-bearing change vs the spec's literal wording):** effective resources are computed **once, at run admission**, inside the Phase-2 resolve-&-freeze step — NOT at Job build. Re-reading the registry at build time would let a mid-run registry edit change later stages' resources, violating the freeze (§4.3). So: the admission step applies `Default` when the spec's resources are nil, clamps to `Max`, strips unlisted names, and writes the **effective** values into `status.resolvedSpec`'s components; `buildComponentJob` consumes `resolvedSpec` verbatim with zero resource logic. Same enforcement (controller-side, kubectl path included), now actually frozen. Record this as a documented deviation in the phase PR body (spec §4.4 says "clamps at Job build" — admission IS where the build input is fixed).

**Files:**
- Modify: `pkg/k8s/controllers/pipelinerun_controller.go` (extend the Phase-2 admission resolve step: compute effective resources into `resolvedSpec` before persisting the snapshot; append the clamp note to the run status message)
- Modify: `pkg/k8s/controllers/pipelinerun_jobs.go` (`buildComponentJob` — SIMPLIFY: use the snapshot's resources verbatim; delete any resource fallback logic)
- Create: `pkg/k8s/controllers/resource_clamp.go` (clamp + strip + default-merge helpers, called from admission)
- Test: `pkg/k8s/controllers/resource_clamp_test.go` + admission cases + a `buildComponentJob` verbatim-consumption case

**Interfaces:**
- Consumes: the Phase-2 admission resolution (`ResolvedComponent.Resources` — `datupletv1.ComponentResources` — is in hand at the same moment the image is resolved; nothing is re-read later). T5's rules are the reject counterpart at the API; this task is the clamp counterpart (defense-in-depth for the kubectl path per §4.4 Layer 2).
- Produces: `status.resolvedSpec` components carry **effective** resources (`applyDefaultsThenClamp(specResources, resolved.Resources.Default, resolved.Resources.Max)`, unlisted names stripped) computed at admission; the built Job container's `Resources` equals the snapshot value byte-for-byte; a clamp/strip event is noted in the run status message (spec §4.4: "clamped run proceeds, with the clamp noted"). Clamping is **unconditional** — even superadmin-authored specs clamp (raising the ceiling is a registry edit). Mid-run registry edits cannot affect any stage of an in-flight run — add an explicit test for this.

**Clamp/strip/default semantics (spec §4.4, implement exactly):**
- **Default when nil:** if the component's spec `resources` is nil/empty → container gets `resolved.Resources.Default` verbatim (the pre-Phase-2 "nil = unlimited" behavior is gone). Record: default applied (not a clamp — no status note needed for pure default application, but harmless to note; keep the status message reserved for actual clamps).
- **Clamp when set:** for each resource name in the spec `requests`/`limits`:
  - name NOT in `Max` → **strip** it from the container resources (spec §4.4: "strips unlisted names"). Record a strip.
  - `limits[name] > Max[name]` → set `limits[name] = Max[name]`. Record a clamp.
  - `requests[name] > Max[name]` → set `requests[name] = Max[name]`. Also ensure `requests[name] <= limits[name]` after clamping (a request above the clamped limit is invalid to K8s) — clamp request down to the effective limit if needed.
- **Status note:** when any clamp OR strip happened for any component, append to the run status message a single line like `resources clamped for component <c>: cpu 4→2` (concise; exact format at implementer discretion but must name the component + at least one changed resource). Use the repo's status-message convention (`DUPLET_STATUS_MESSAGE:`-style is for stdout exit contract; the run STATUS message is the CRD `status.message` / condition — match whatever `pipelinerun_status.go` already writes).

**Complete skeleton — `pkg/k8s/controllers/resource_clamp.go`:**

```go
package controllers

import (
	corev1 "k8s.io/api/core/v1"
)

// clampResult carries the clamped requirements plus a human-readable note
// describing what changed (empty when nothing was clamped or stripped).
type clampResult struct {
	Resources corev1.ResourceRequirements
	Note      string // "" when unchanged; else e.g. "cpu 4→2, dropped nvidia.com/gpu"
}

// applyDefaultsThenClamp returns the resources to set on a component
// container. When spec is nil/empty it returns def (the registry default).
// Otherwise it clamps every requests/limits entry to max and strips names
// absent from max. Clamping is unconditional (RFC 026 §4.4 Layer 2 —
// defense-in-depth for the direct-kubectl path).
func applyDefaultsThenClamp(spec *corev1.ResourceRequirements, def corev1.ResourceRequirements, max corev1.ResourceList) clampResult {
	if spec == nil || (len(spec.Requests) == 0 && len(spec.Limits) == 0) {
		return clampResult{Resources: *def.DeepCopy()}
	}
	out := *spec.DeepCopy()
	var notes []string
	clampList := func(list corev1.ResourceList) corev1.ResourceList {
		if list == nil {
			return nil
		}
		res := corev1.ResourceList{}
		for name, q := range list {
			maxq, ok := max[name]
			if !ok {
				notes = append(notes, "dropped "+string(name))
				continue // strip unlisted
			}
			if q.Cmp(maxq) > 0 {
				notes = append(notes, string(name)+" "+q.String()+"→"+maxq.String())
				res[name] = maxq.DeepCopy()
			} else {
				res[name] = q
			}
		}
		return res
	}
	out.Limits = clampList(out.Limits)
	out.Requests = clampList(out.Requests)
	// A request above its (possibly clamped) limit is invalid — pull it down.
	for name, rq := range out.Requests {
		if lq, ok := out.Limits[name]; ok && rq.Cmp(lq) > 0 {
			out.Requests[name] = lq.DeepCopy()
		}
	}
	note := ""
	if len(notes) > 0 {
		note = joinNotes(notes) // small helper: strings.Join with ", "
	}
	return clampResult{Resources: out, Note: note}
}
```

- [ ] **Step 1: Failing tests** (`resource_clamp_test.go`), minimum:
  1. spec nil → returns Default verbatim, empty Note.
  2. `limits.cpu 4`, Max cpu 2 → limits.cpu becomes 2, Note mentions `cpu`.
  3. `limits."nvidia.com/gpu" 1`, Max lacks it → gpu stripped, Note mentions `dropped nvidia.com/gpu`.
  4. `requests.memory 3Gi` clamped to Max 2Gi, and limits.memory unset → requests clamped (and if limits.memory becomes the clamp bound, request ≤ limit invariant holds).
  5. spec within Max → unchanged, empty Note.
  6. ephemeral-storage over Max → clamped (full ResourceList).
  Plus a `buildComponentJob` test: a run snapshot component with over-max spec resources → the produced Job container's `Resources` are clamped, and the reconciler records the note (assert via the fake client's PipelineRun status or the returned Job).
- [ ] **Step 2: Implement.** Add `resource_clamp.go`. In `buildComponentJob`, replace the Phase-2 resource-application line with: locate the resolved component in `pr.Status.Components[]` matching `comp.Name`, obtain its `Default`/`Max` (per T1 recon), call `applyDefaultsThenClamp(comp.Resources, def, max)`, set `container.Resources = result.Resources`, and surface `result.Note` up to the caller so the reconciler can append it to the run status message (thread via the `(*batchv1.Job, *corev1.ConfigMap, error)` return — add a note out-param or collect notes on the reconciler during the stage build; keep it minimal).
- [ ] **Step 3:** `go build ./... && go test ./pkg/k8s/controllers/... -count=1` → PASS.
- [ ] **Step 4: Commit** `feat(controller): effective resources at admission — registry default + clamp/strip frozen into resolvedSpec (RFC 026 P3)`.

---

### Task T8: Chart — `pipelinePolicy` values + env plumbing

**Model:** sonnet — **Parallel: yes (disjoint files) with T7.**

**Files:**
- Modify: `charts/datuplet-app/values.yaml` (add `pipelinePolicy` block)
- Modify: `charts/datuplet-app/templates/pipeline-api/deployment.yaml` (env → pipeline-api reads the gateway bounds for the PUT diff-gate)
- Modify: `charts/datuplet-app/templates/pipeline-operator/deployment.yaml` (env → operator reads the same bounds if the controller needs them; controller clamp is registry-driven so bounds may not be needed operator-side — see decision below)
- Modify: `cmd/pipeline-api/main.go` (read `PIPELINE_POLICY_*` env → `validate.Policy` → `WithPipelinePolicy`)
- Modify: `cmd/pipeline-operator/main.go` (only if the controller enforces gateway bounds; see below)
- Test: `charts/datuplet-app` `helm template` smoke (manual in commit msg) + a small main.go env-parse unit test if practical
- Modify: `docs/install.md` (or the post-install section of the relevant quickstart) — **seed-superadmin step** (see the note below)

**Superadmin seed (closes the Option-A loop from T4):** no chart hook in the
POC. This task documents the one-time post-install step —
`pipeline-api admin grant --user <init-admin-email> --superadmin` — in the
install docs, right next to the existing `admin grant` examples; T10's e2e
bootstrap runs the same command. Record "no automated seed hook" as a
deliberate POC gap in the phase PR body.

**Values block (spec §4.6, verbatim defaults):**
```yaml
pipelinePolicy:
  gateway:                    # non-superadmins must stay within these
    maxChunkSize: 268435456       # 256Mi
    maxBufferSize: 536870912      # 512Mi
    maxTargetFileSize: 1073741824 # 1Gi
```

**Decision — where gateway bounds are enforced:** the spec's diff-gate (§4.4) is a **pipeline-api PUT** concern (403 for non-superadmins exceeding bounds). Resource **clamping** (§4.4 Layer 2) is registry-driven (Max lives in the ComponentDefinition, not chart values) and needs no chart env. Gateway bounds are NOT clamped by the controller in the spec text — only rejected at PUT. **Therefore: plumb `PIPELINE_POLICY_GATEWAY_*` env to pipeline-api ONLY.** Do not add gateway-bound env to the operator unless T7/T1 recon shows the controller must also reject out-of-bounds gateway knobs on the kubectl path. If it should (defense-in-depth parity), plumb the same three env vars to the operator and have the controller run `ValidateTyped(pipeline, reg, pol)` at admission with a policy built from operator env — **flag this as a scope decision for the maintainer in the PR description; default to pipeline-api-only to match the spec's literal §4.4 text.**

- [ ] **Step 1: values.yaml** — add the `pipelinePolicy` block (top-level, near `warehouse`/`pipelineOperator`).
- [ ] **Step 2: pipeline-api deployment env** — add three env vars in the pipeline-api container env block (pattern: the `mul`/`quote` helpers already used for query limits at deployment.yaml ~:164):
  ```yaml
  - name: PIPELINE_POLICY_GATEWAY_MAX_CHUNK_SIZE
    value: {{ .Values.pipelinePolicy.gateway.maxChunkSize | quote }}
  - name: PIPELINE_POLICY_GATEWAY_MAX_BUFFER_SIZE
    value: {{ .Values.pipelinePolicy.gateway.maxBufferSize | quote }}
  - name: PIPELINE_POLICY_GATEWAY_MAX_TARGET_FILE_SIZE
    value: {{ .Values.pipelinePolicy.gateway.maxTargetFileSize | quote }}
  ```
- [ ] **Step 3: main.go (pipeline-api)** — parse the three env vars (`strconv.ParseInt`, base 10; unset/empty/parse-error → 0 = no bound) into a `validate.Policy{Gateway: validate.GatewayBounds{...}}`; wire via a new `Server.WithPipelinePolicy(*validate.Policy)` builder consumed by T6. Log the effective bounds at startup (same style as the query-limit logging).
- [ ] **Step 4 (conditional per the decision above):** operator env + `cmd/pipeline-operator/main.go` parse + reconciler `Policy` field ONLY if the maintainer opts into controller-side gateway rejection. Default: skip.
- [ ] **Step 5:** `helm template charts/datuplet-app --set pipelinePolicy.gateway.maxBufferSize=123 | grep -A1 PIPELINE_POLICY_GATEWAY_MAX_BUFFER_SIZE` → shows `value: "123"`. `go build ./...` green. Record the helm-template output in the commit message.
- [ ] **Step 6: Commit** `feat(chart): pipelinePolicy gateway bounds values + env plumbing to pipeline-api (RFC 026 P3)`.

---

### Task T9: `LimitRange` in project-namespace provisioning

**Model:** sonnet — **Parallel: no** (depends on T8 for whether limits come from values; but the LimitRange defaults are provisioning-owned, not chart-owned — see below).

**Files:**
- Modify: `pkg/pipelineapi/k8s/namespace.go` (`EnsureProjectNamespace` also ensures a `LimitRange`) OR add `pkg/pipelineapi/k8s/limitrange.go` with `EnsureProjectLimitRange` called from `storeProjectNS.Ensure`
- Test: `pkg/pipelineapi/k8s/namespace_test.go` (extend) or `limitrange_test.go`

**Interfaces:**
- Consumes: existing `EnsureProjectNamespace` get-before-create pattern; `client.Client`.
- Produces: a namespace-scoped `LimitRange` named `datuplet-defaults` in every project namespace, created idempotently (get-before-create, AlreadyExists tolerated) alongside the namespace. Spec §4.4: "Namespace-level `LimitRange` in project namespaces is belt-and-braces, added by project provisioning." It is a SAFETY NET (a default container limit for pods created outside Datuplet's own path); it is NOT the primary ceiling (that's the registry Max + controller clamp).

**Design decision — LimitRange values:** the LimitRange is belt-and-braces, so keep its values conservative and provisioning-owned (a package constant), NOT wired from chart values — the per-component Max is the real ceiling and lives in the registry. Ship a `LimitRange` with a `default` + `defaultRequest` for cpu/memory only (a `type: Container` limit), e.g. `default: {cpu: "1", memory: 512Mi}`, `defaultRequest: {cpu: 100m, memory: 128Mi}`. Do NOT set a `max` on the LimitRange (that would fight the registry clamp and could reject legitimately-large registry defaults). **This is a documented deviation from spec §4.4's "belt-and-braces" wording** (which implies a full LimitRange): defaults-only, no `max`, provisioning-owned constants, no chart values wiring — record it in the phase PR body alongside the other deviations.

**Complete skeleton — `pkg/pipelineapi/k8s/limitrange.go`:**

```go
package k8s

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ProjectLimitRangeName is the LimitRange every project namespace carries.
const ProjectLimitRangeName = "datuplet-defaults"

// EnsureProjectLimitRange creates a belt-and-braces LimitRange in the
// project namespace if absent (idempotent, get-before-create). It sets only
// default container requests/limits — a safety net for any pod created
// outside Datuplet's own path. It deliberately sets NO max: the real ceiling
// is the ComponentDefinition resources.max enforced by the controller clamp
// (RFC 026 §4.4). Belt-and-braces, not the primary boundary.
func EnsureProjectLimitRange(ctx context.Context, c client.Client, projectID uuid.UUID) error {
	ns := NamespaceForProject(projectID)
	existing := &corev1.LimitRange{}
	err := c.Get(ctx, types.NamespacedName{Name: ProjectLimitRangeName, Namespace: ns}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get limitrange %s/%s: %w", ns, ProjectLimitRangeName, err)
	}
	lr := &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{Name: ProjectLimitRangeName, Namespace: ns},
		Spec: corev1.LimitRangeSpec{
			Limits: []corev1.LimitRangeItem{{
				Type: corev1.LimitTypeContainer,
				Default: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("512Mi"),
				},
				DefaultRequest: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("128Mi"),
				},
			}},
		},
	}
	if err := c.Create(ctx, lr); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create limitrange %s/%s: %w", ns, ProjectLimitRangeName, err)
	}
	return nil
}
```

- [ ] **Step 1: Failing test** (fake client): after `EnsureProjectLimitRange`, the LimitRange exists with the `Container` default; a second call is a no-op (no error); a pre-existing LimitRange is left untouched. If Phase 1.5 already added a Role create in `Ensure`, mirror its test style.
- [ ] **Step 2: Implement** `limitrange.go`; call `EnsureProjectLimitRange` from `storeProjectNS.Ensure` (`pkg/pipelineapi/runbackend/k8s.go`) right after `EnsureProjectNamespace` succeeds. Also call it from `EnsureProjectNamespace`'s callers in the admin `create-project --with-namespace` path if that path should get it too (T1 recon: confirm both call sites; add to both for parity). Ensure the RBAC allows `limitranges` create in the project namespace — if Phase 1.5 introduced per-namespace Roles, add `limitranges: [get, create]`; if pipeline-api still has a ClusterRole with namespace verbs, add there. **Flag any RBAC gap explicitly.**
- [ ] **Step 3:** `go build ./... && go test ./pkg/pipelineapi/... -count=1` → PASS.
- [ ] **Step 4: Commit** `feat(provisioning): belt-and-braces LimitRange in project namespaces (RFC 026 P3)`.

---

### Task T10: Phase gate

**Model:** sonnet — **Parallel: no.**

- [ ] **Step 1:** `make tidy && go build ./... && go test ./... -count=1` → all green.
- [ ] **Step 2: e2e fixtures + bootstrap superadmin grant** (spec §5 Phase-3 test-impact bullet):
  - Bootstrap: extend the e2e bootstrap (the harness that seeds the admin user + project — `tests/e2e/framework/` / the scenario bootstrap) to run `pipeline-api admin grant --user <e2e-admin-email> --superadmin` so the API-mode tests have a superadmin identity. (This is the Option-A UUID-subject seed from Task T4.)
  - New scenarios (add fixtures under `tests/e2e/pipelines/k8s/` + Go assertions in the scenario package):
    1. **Registry defaults applied:** a pipeline whose component sets no `resources` → the built Job's container carries the registry `resources.default` (assert via the component Job spec).
    2. **Over-max rejected at save (API path):** `PUT` a pipeline with `limits.cpu` above the component Max as a non-superadmin → 403 (diff-gate) OR 400 (over-max) — assert the correct code per T6 precedence (403 when the non-superadmin is *modifying* resources).
    3. **Clamped at admission (kubectl path):** `kubectl apply` a Pipeline+PipelineRun (hand-minted run token, per `tests/e2e/framework/k8s_token.go`) with over-max resources → the run PROCEEDS, the Job container carries the clamped-to-Max resources frozen in `resolvedSpec`, and the run status message notes the clamp.
    4. **Non-superadmin PUT modifying resources → 403; superadmin PUT succeeds.**
  - Reuse the Phase-2 e2e ComponentDefinition bootstrap (dev/prerelease versions registering local images) — the Max values for these scenarios live on those test ComponentDefinitions; set Max low enough to trigger clamping.
- [ ] **Step 3:** `make e2e-k8s` against an OrbStack cluster. The likeliest breakages: (a) RBAC for `limitranges` create (T9), (b) the superadmin bootstrap grant ordering (server object must exist — lakekeeper-bootstrap must have run first), (c) the diff-gate reading the stored pipeline. Fix via subagents (opus for controller/clamp issues, sonnet otherwise) before proceeding.
- [ ] **Step 4:** Orchestrator cumulative Codex review `mcp__codex-cli__review {base: "<phase-start SHA>", model: "gpt-5.5", title: "RFC026 Phase 3"}` → zero CRITICAL/MAJOR. Fix via fixer subagent; re-run until clean.
- [ ] **Step 5:** Push + append the Phase 3 summary to the single draft PR body (`gh pr edit` — Branching model): superadmin = FGA server.admin UUID-subject; reject-then-clamp resource contract; gateway bounds; LimitRange; e2e-k8s log; Codex-gate log; note the T8 pipeline-api-only-gateway-bounds scope decision. **Never push main.**

---

## Self-review checklist (run before handing off)

- **Spec coverage:** §4.4 (reject-then-clamp, full ResourceList, diff-gated resources) → T5 (reject) + T6 (diff-gate 403) + T7 (clamp/strip/default); §4.5 (superadmin = FGA server.admin, UUID subjects, memoized discovery) → T2 + T3 + T4; §4.6 (`pipelinePolicy.gateway` bounds) → T5 (findings) + T8 (values/env); §7 error contract (400 findings vs 403 policy) → T5/T6; §5 Phase-3 row + "Test impact (e2e) Phase 3" bullet → T10.
- **Fixed interface contract honored verbatim:** `Finding{path,message,severity}` lowercase tags; `ValidateTyped(p, reg, pol *Policy)`; `Policy{Gateway GatewayBounds}`, `GatewayBounds{MaxChunkSize,MaxBufferSize,MaxTargetFileSize int64}`; `Check(user:oidc~<uuid>, admin, server:<uuid>)`; new `authz.ServerObject(ctx)` (as `ServerAdminChecker.ServerObject`) + `IsServerAdmin`; `mustBeSuperadmin(w,r) (*store.User, bool)`; `PUT/DELETE /api/v1/admin/components/{name}` land here as thin wrappers over the Phase-2 write path; consumes Phase-2 `status.components[]` + `RegistryView`/`ResolvedComponent`/`ComponentResources`.
- **T1 is a Preflight (rebase checkpoint)** with exact grep commands + expected symbols + amend-on-drift, per the Phase 1.5 pattern.
- **No line numbers into Phase-1/1.5/2-rewritten files** (`validate/*`, `pipeline_types.go`, `pipelinerun_types.go`, `pipelinerun_jobs.go`, `pipelinerun_controller.go`, `pipeline_handlers.go`) — function-name anchors only. Line numbers used ONLY for un-rewritten files (`admin_lakekeeper.go`, `admin.go`, `authz/*`, `namespace.go`, chart/deployment env blocks, main.go env blocks).
- **Complete Go for new load-bearing skeletons:** `Policy`/`GatewayBounds` (T5), `ServerObject`/`ServerAdminChecker`/`serverAdmin`/`DiscoverServerObject` (T2), `mustBeSuperadmin` (T3), `resourcesModified` diff logic (T6), `applyDefaultsThenClamp` (T7), `EnsureProjectLimitRange` (T9). Modifications (handlers, CLI, chart, main.go wiring) are precise recipes.
- **Models:** opus on the diff-gate (T6) + clamp semantics (T7) + reject-rule arithmetic (T5); sonnet elsewhere; no haiku task (no docs-only task this phase — the e2e/PR doc work rides in T10). If a docs sweep is added later, use haiku.
- **Parallelism annotated:** {T2, T4, T5, T8} file-disjoint after T1; T7 ∥ T8; sequential where files collide (T3 route table, T6 handler, T9 provisioning, T10 gate).
- **Global Constraints** copied from the Phase 0/1 plan (never push main, `make tidy`, CRDs-in-both-dirs-if-touched [not touched this phase], e2e-k8s gate, BSD/macOS).


---

<!-- ═══════════════ Part 5 — Phase 4 ═══════════════ -->

# RFC 026 Phase 4 — Registry-driven UI — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development — one fresh subagent per task, orchestrator reviews + runs the Codex gate between tasks. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land RFC 026 §4.7 (+ §4.9 picker) — a registry-driven UI: a component catalog page, a pipeline builder v1 (catalog dropdown → YAML snippet + docs panel + inline validation findings), a reusable JSON-Schema→form renderer, and a builder v2 (per-component form, one-way "edit as YAML" toggle, storage-backed inputs/outputs pickers). All vanilla ES modules, **no build step**.

**Architecture:** Pure browser work under `ui/product/`. No Go, no chart, no CRD changes. The UI consumes three sets of endpoints that earlier phases already shipped: the component catalog (`GET /api/v1/components`, `GET /api/v1/components/{name}` — Phase 2), the write-only secrets list (`GET /api/v1/projects/{pid}/secrets` — Phase 1.5), and the existing storage browse endpoints (RFC 005 — already live). The findings-aware 400 contract on pipeline save (`PUT …/pipelines/{name}`) shipped in Phase 1. New files: `ui/product/pages/components.js` (catalog), `ui/product/lib/schema-form.js` (renderer) + `ui/product/lib/findings.js` (shared findings renderer); modified: `ui/product/api.js`, `ui/product/app.js` (route + nav), `ui/product/pages/pipeline-detail.js` (builder v1 → v2), `ui/product/style.css` (a small block of new classes). Branch: the single branch (Branching model), starting **after the Phase 2 and 1.5 gates have passed** (Phase 3's too, for the resource-hiding whoami flag — see Task 1 preflight). Spec: `docs/superpowers/specs/2026-07-04-rfc-026-pipeline-config-safety-design.md` §4.7, §4.9, §7, §5 (Phase 4 row).

**Tech Stack:** Vanilla ES modules (browser-native `import`, no bundler, no npm), the existing `api.js` fetch wrapper, native `<details>`/`<dialog>`/`confirm()`, the existing CSS-variable design system (`var(--s-*)`, `var(--fg-*)`, `var(--bg-*)`, `var(--text-*)`, `var(--status-*)`, `var(--accent)`, `var(--border)`, `var(--radius)`). `node --check` is the only available JS tooling (there is **no** JS test framework in the repo — verified: no `package.json`, no jest/vitest/mocha anywhere under `ui/`).

## Global Constraints

- **Never push `main`, never tag.** All work on the single branch; lands via the one draft PR (Branching model). (Repo rule.)
- **No build step, no external JS/CSS, no CDN, no npm.** Vanilla ES modules only, loaded by the browser directly (`ui/product/index.html` script tags + `import` graph rooted at `app.js`). Every new module is `import`ed via an absolute `/ui/...` path exactly like the existing pages. No transpilation, no JSX, no TypeScript.
- **Follow the existing CSS variable system.** Reuse the design tokens and existing classes (`.btn`, `.btn--primary/secondary/ghost`, `.table`, `.input`, `.textarea`, `.input--mono`, `label.field`, `.callout`, `.callout--warn`, `.code`, `code.inline`, `.mono`, `.badge`, `.empty-state`, `.spinner`). New classes are allowed **only** when no existing class fits, must be defined in `ui/product/style.css`, and must be built from `var(--*)` tokens — never hard-coded colors/sizes. (Verified token set: `--s-1..6`, `--text-xs..xxl`, `--fg-0..2`, `--bg-0..2`, `--border`, `--radius`, `--accent`, `--status-{ok,fail,running,pending}-{bg,fg}`, `--font-mono`.)
- **Escape everything.** Any value interpolated into `innerHTML` goes through `esc()` from `/ui/api.js`. Schema-derived strings (property names, descriptions, enum values, component names/descriptions, findings paths/messages) are all untrusted-ish and MUST be escaped — this is the one security-relevant invariant of the phase.
- **Abort-on-navigation pattern.** Every page `render()` that `await`s snapshots `const path = window.location.pathname; const aborted = () => window.location.pathname !== path;` and checks `aborted()` after each `await` before touching `#app`/`#page-head` (mirror `storage-catalog.js` / `query.js`). Pollers, if any, stash on `window.__datupletPoller` (none needed this phase).
- **401 handling is centralized.** Use the `api()` / `putYAML()` wrappers (they redirect to `/ui/login` on 401 and throw `'not authenticated'`); page code must swallow that specific message (`if (String(err.message) !== 'not authenticated') { … }`) exactly like existing pages, never render it.
- **`node --check <file>` must pass for every touched JS file before each commit** (the closest thing to a compile step available). It is a mandatory step in every UI task.
- POC greenfield (RFC §2): no back-compat shims. The old raw-textarea pipeline editor is *extended*, not preserved as a separate mode — the textarea becomes the "advanced / edit as YAML" surface (§4.7 item 2).
- macOS/BSD environment: use file-edit tooling, not `sed -i`/GNU flags.
- Conventional commits (`feat:`, `fix:`, `docs:`, `refactor:`), one logical commit per task.
- **No `make e2e-k8s` for this phase** — there is no controller/chart/CRD change. The phase gate substitutes a **scripted browser walkthrough** (Task 9) against a running stack, plus `node --check` across the whole `ui/product` tree.

## Harness notes (orchestrator contract)

- **Branch:** continue on the single branch (after the Phase 2 + 1.5 + 3 gates have passed — Task 1 verifies).
- **Parallel tasks** are marked `Parallel: yes (disjoint files)`. Run each in its own git worktree off the branch; merge back in task-number order (files disjoint by design → clean merges). Sequential tasks run directly on the branch.
- **Per-task Codex gate (after the subagent commits, before the next task):**
  1. Orchestrator runs MCP tool `mcp__codex-cli__review` with `{commit: "<task commit SHA>", model: "gpt-5.5", title: "<task id>", workingDirectory: "<branch worktree>"}`.
  2. Acceptance: **zero CRITICAL or MAJOR findings on the task's diff.** MINOR findings: fix if ≤5 min, otherwise record in the PR description.
  3. Findings to fix → dispatch a fixer subagent (same model as the task) with the finding text verbatim; re-run the gate.
- **Phase gate (Task 9):** cumulative `mcp__codex-cli__review` with `{base: "<phase-start SHA>", model: "gpt-5.5", title: "RFC026 Phase 4"}`, then the scripted browser walkthrough, then push + PR-body update (Branching model).
- **Subagent dispatch:** give each subagent its full task text verbatim, plus the Global Constraints section, the branch/worktree to use, and nothing else. Suggested Agent-tool model per task is in the index below.
- **Anchor policy (inherited from the Phase 1.5 plan):** line numbers appear **only** for files no earlier phase rewrites. Everything in `pipeline-detail.js`, `api.js`, `app.js` is anchored by **function/export name** (those files were touched by Phases 1/1.5 and may have shifted). Task 1 verifies every anchor against merged reality before any code runs and amends this plan inline on drift.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| T1 | Preflight (rebase checkpoint) | sonnet | Phases 2, 1.5, 3 gates passed | — |
| T2 | `api.js` additions + shared findings renderer (`lib/findings.js`) | sonnet | T1 | — |
| T3 | Catalog page `/ui/components` (list + detail) | sonnet | T2 | with T4 |
| T4 | Schema-form renderer module (`lib/schema-form.js`) | **opus** | T2 | with T3 |
| T5 | Builder v1: catalog dropdown → YAML snippet + docs panel + inline findings | sonnet | T2, T3 | — |
| T6 | Builder v2: per-component form panel + one-way "edit as YAML" toggle | sonnet | T4, T5 | — |
| T7 | Inputs/outputs pickers (storage-backed) wired into the form panel | sonnet | T6 | — |
| T8 | Docs + README: UI section, `x-datuplet-secret` note, screenshots-optional | haiku | T3, T6, T7 | — |
| T9 | Phase gate: full `node --check`, scripted browser walkthrough, cumulative Codex review, PR-body update | sonnet | T2–T8 | — |

Task DAG (arrows = "blocks"):

```
T1 → T2 → {T3, T4}
T3 → T5
{T4, T5} → T6 → T7
{T3, T6, T7} → T8
T2..T8 → T9
```

(T3 and T4 are file-disjoint — `pages/components.js` vs `lib/schema-form.js` — and both only depend on T2's api helpers, so they run in parallel worktrees. T5 needs the catalog helpers proven by T3's usage; T6 composes the renderer (T4) into the builder (T5).)

---

### Task T1: Preflight (rebase checkpoint)

**sonnet.** Verify the consumed contracts exist in the merged tree and amend this plan's anchors inline if drift is found (record amendments in the task commit message; a docs-only commit against this plan file is allowed, but the plan lives under `docs/superpowers/plans/` — commit there, not in the worktree's UI files).

Rationale: Phase 4 consumes endpoints produced by **three** earlier phases (2 = catalog, 1.5 = secrets list, 3 = the `whoami`/`auth/me` superadmin flag). None of them exist in the tree as of this plan's authoring (verified 2026-07-04: `grep -rln "v1/components" pkg/pipelineapi` → no hits; `grep -rln "projects/{pid}/secrets" pkg/pipelineapi` → no hits; `/api/v1/auth/me` returns `{id,email,mode}` with **no** `is_superadmin`). This task is the gate that confirms they landed as specified before any UI is written.

- [ ] **Catalog endpoints (Phase 2).** `grep -rn "v1/components" pkg/pipelineapi/http/*.go` → a `GET /api/v1/components` **and** `GET /api/v1/components/{name}` route exist. Read the handler(s) and record the **exact JSON field names** for the list item (`name, displayName, description, deprecated, defaultVersion, versions[]`) and the version item (`version, prerelease, image`) and the detail's per-version `configSchema` (a JSON-Schema string). If any field name differs from this plan's FIXED CONTRACT below, amend T2/T3/T4/T5 anchors and note it in the commit.
- [ ] **Secrets list (Phase 1.5).** `grep -rn "projects/{pid}/secrets" pkg/pipelineapi/http/*.go` → `GET` route exists; confirm the response is `[{key, updatedAt}]` (record the exact JSON tags — Phase 1.5 plan S5 says `key` + `updatedAt`). Confirm `settings-secrets.js` was already rewritten by Phase 1.5 (it should be the real page, not the kubectl cheat-sheet) and that `api.js` already has `listSecrets`/`putSecret`/`deleteSecret` (Phase 1.5 task S8). **If `listSecrets` already exists, T2 does NOT re-add it** — T2 only adds what's missing. Record which of the three helpers already exist.
- [ ] **whoami superadmin flag (Phase 3).** `grep -rn "is_superadmin\|IsSuperadmin\|superadmin" pkg/pipelineapi/http/auth_handlers.go` → the `handleMe` response (`GET /api/v1/auth/me`) now includes a boolean the UI can read (spec assumes `is_superadmin`; **the real endpoint is `/api/v1/auth/me`, not `/whoami`** — the prompt's "whoami" is `auth/me` in this codebase). Record the exact JSON key name. If Phase 3 named it differently (e.g. `superadmin`), amend T6's resource-hiding check. **If Phase 3 did NOT add any flag** (resource UI was descoped): record that, and T6 hides the `resources` field unconditionally (safe default — never render it), noting the follow-up.
- [ ] **Router mechanism (no drift expected — `app.js` is Phase-4-owned for the new route).** Confirm `ui/product/app.js` still uses the `routes` array of `{ pattern: /regex/, render: fn }` matched in `renderRoute()`, the `NAV_ITEMS` array for the sidebar, and that page modules export a single `render*(ctx)` taking `ctx.params` (regex capture groups). Confirm `renderNav` sets `window.__datupletActiveProjectID`. Record the exact export name convention (e.g. `renderStorageCatalog`) so T3's `renderComponents` matches.
- [ ] **api.js conventions.** Confirm `api(path, opts)`, `putYAML(path, text)`, and `esc(s)` are still exported from `/ui/api.js` with the signatures in this plan (401→`__datupletGoToLogin`, 204→null, JSON auto-parse). Confirm the storage helpers (`getStorageCatalog`, `getTableSchema`) still exist for T7.
- [ ] **CSS anchors.** Confirm `ui/product/style.css` still defines the token set and the reusable classes listed in Global Constraints. Record whether a `.banner` class exists — **verified 2026-07-04 it does NOT** (pipeline-detail.js references `.banner success`/`.banner error` but no such class is defined; the styled equivalent is `.callout` / `.callout--warn`). T2's findings renderer and T5's inline errors use `.callout`; **do not** introduce a dependency on the undefined `.banner`.
- [ ] **Pipeline-detail baseline (merged runs-UX, PR #23 — verified 2026-07-07).** `renderPipelineDetail` now ALSO renders a read-only **"Recent runs"** section: a `#pipeline-runs` container filled by a module-level `loadRecentRuns(pid, pipelineID)` helper (`GET …/runs?pipeline_id=<id>&limit=10`), gated on `!isNew && pipelineID`, plus imports of `phaseToPillClass`/`formatDuration`/`durationFrom` from `/ui/format.js`. **T5/T6/T7 rewrite this page's template — they MUST preserve this section, its imports, and the `loadRecentRuns` call, keeping it below the builder/editor.** Confirm the section is still shaped this way in the merged tree and record any further drift.
- [ ] **No commit needed unless anchors drift.** If everything matches, record "no drift" in the orchestrator log and proceed to T2. If anything drifted, commit the amended plan: `docs(plan): amend RFC 026 Phase 4 anchors after preflight`.

---

### Task T2: `api.js` additions + shared findings renderer

**sonnet.** **Files:**
- Modify: `ui/product/api.js` (add `getComponents`, `getComponent`, and — only if T1 found it missing — `listSecrets`; add a `findingsError` helper on the shared 400-findings-aware path).
- Create: `ui/product/lib/findings.js` (the shared findings-list renderer, reused by T5's inline errors and any future findings surface).
- Verify: `node --check` on both.

**Interfaces:**
- Consumes: T1-verified endpoints. `esc` from `/ui/api.js`.
- Produces (T3/T5/T6 rely on exact names):
  - `getComponents()` → `Promise<Array>` of catalog list items (`{name, displayName, description, deprecated, defaultVersion, versions:[{version, prerelease, image}]}`).
  - `getComponent(name)` → `Promise<Object>` — same shape **plus** per-version `configSchema` (JSON Schema draft 2020-12, as a **string**).
  - `listSecrets(pid)` → `Promise<Array<{key, updatedAt}>>` (add only if T1 says Phase 1.5 didn't already ship it).
  - `putPipelineYAML(pid, name, yamlText)` → resolves `{ok:true}` on 204, resolves `{ok:false, findings:[…]}` on 400-with-findings, resolves `{ok:true, findings:[…]}` on 200-with-warnings, and **throws** on any other non-2xx (or auth). This wraps the raw `PUT` so builder code gets a uniform findings-aware result instead of a thrown opaque error. Findings shape: `{path, message, severity}` (`"error"|"warning"`).
  - `renderFindings(findings)` (from `lib/findings.js`) → an HTML **string**: a list grouping each finding as `path` (mono, or `(root)` when empty) + `message`, colored by severity (`error` → `--status-fail-fg`, `warning` → `--status-running-fg`). Empty/null input → `''`.

- [ ] **Step 1: `getComponents` / `getComponent` in `api.js`.** Add next to the storage helpers (after `getTableSnapshots`), following the existing JSDoc + `api()` style verbatim:

```js
// ----- Component registry (RFC 026 §4.1, §4.7) -----
//
// Read-only catalog of ComponentDefinitions. Readable by any authenticated
// project member (the catalog is the shared component picker). Both return
// the parsed body or throw via api() on non-2xx / 401.

/**
 * List registered components.
 * Returns [{ name, displayName, description, deprecated, defaultVersion,
 *            versions: [{ version, prerelease, image }] }, ...].
 */
export async function getComponents() {
  return api('/api/v1/components');
}

/**
 * One component with per-version configSchema (JSON Schema draft 2020-12,
 * as a string). Same top-level shape as a list item plus
 * versions[].configSchema.
 */
export async function getComponent(name) {
  return api(`/api/v1/components/${encodeURIComponent(name)}`);
}
```

- [ ] **Step 2: `listSecrets` (conditional).** If T1 recorded that Phase 1.5 did **not** add it, add:

```js
/**
 * List project secret KEY NAMES + timestamps (never values — write-only API).
 * Returns [{ key, updatedAt }, ...]. Used by the schema-form secret picker.
 */
export async function listSecrets(projectId) {
  return api(`/api/v1/projects/${encodeURIComponent(projectId)}/secrets`);
}
```

  Otherwise skip — reuse the existing export. Record which path was taken in the commit body.

- [ ] **Step 3: `putPipelineYAML` findings-aware wrapper in `api.js`.** The existing `putYAML` throws on any non-2xx (flattening the 400 findings body into an opaque `Error`). Builder v1/v2 need the **structured** findings. Add a dedicated wrapper (do not change `putYAML` — other callers depend on its throw semantics):

```js
// putPipelineYAML wraps the pipeline-save PUT so callers get the RFC 026 §7
// findings contract as data, not a thrown string. Resolves:
//   { ok:true }                          — 204 (clean save)
//   { ok:true,  findings:[...] }         — 200 (saved with warnings)
//   { ok:false, findings:[...] }         — 400 { error, findings } (rejected)
// Throws (like api()) on 401 → login redirect, and on any other non-2xx
// with no findings body (so genuine server/transport errors still surface).
export async function putPipelineYAML(projectId, name, yamlText) {
  const r = await fetch(
    `/api/v1/projects/${encodeURIComponent(projectId)}/pipelines/${encodeURIComponent(name)}`,
    {
      method: 'PUT',
      credentials: 'include',
      headers: { 'Content-Type': 'application/yaml' },
      body: yamlText,
    },
  );
  if (r.status === 401) {
    if (typeof window.__datupletGoToLogin === 'function') window.__datupletGoToLogin();
    throw new Error('not authenticated');
  }
  if (r.status === 204) return { ok: true };
  const ct = r.headers.get('content-type') || '';
  const body = ct.includes('application/json') ? await r.json() : null;
  const findings = body && Array.isArray(body.findings) ? body.findings : null;
  if (r.status === 200) return { ok: true, findings: findings || [] };
  if (r.status === 400 && findings) return { ok: false, findings };
  // Non-findings error (413, 500, name mismatch returned as plain text, …).
  throw new Error(`${r.status}: ${(body && body.error) || (await r.text()) || r.statusText}`);
}
```

  Note: a 400 name-mismatch may arrive as `{error: "..."}` **without** `findings` — that path throws, and the builder renders it as a top-level banner (T5). Only findings-bearing 400s become inline field errors.

- [ ] **Step 4: `lib/findings.js` (complete file).**

```js
// findings.js — renders the RFC 026 §7 validation findings array as an HTML
// string. Shared by the pipeline builder's inline error panel (and any other
// findings surface). No state, no DOM — returns a string; callers assign it
// to innerHTML. Every interpolated value is esc()'d (findings come from the
// server but paths/messages are still untrusted for HTML purposes).

import { esc } from '/ui/api.js';

const SEV_FG = {
  error: 'var(--status-fail-fg)',
  warning: 'var(--status-running-fg)',
};

/**
 * @param {Array<{path?:string, message:string, severity?:string}>} findings
 * @returns {string} HTML — '' when there are no findings.
 */
export function renderFindings(findings) {
  if (!Array.isArray(findings) || findings.length === 0) return '';
  const items = findings.map((f) => {
    const sev = f.severity === 'warning' ? 'warning' : 'error';
    const fg = SEV_FG[sev];
    const where = f.path ? esc(f.path) : '(root)';
    return `
      <li class="finding finding--${sev}">
        <code class="mono finding-path" style="color:${fg};">${where}</code>
        <span class="finding-msg">${esc(f.message)}</span>
      </li>`;
  }).join('');
  const errCount = findings.filter((f) => f.severity !== 'warning').length;
  const cls = errCount > 0 ? 'callout callout--warn' : 'callout';
  const heading = errCount > 0
    ? `${errCount} validation error${errCount !== 1 ? 's' : ''}`
    : 'Saved with warnings';
  return `
    <div class="${cls} findings-block">
      <strong>${esc(heading)}</strong>
      <ul class="findings-list">${items}</ul>
    </div>`;
}
```

- [ ] **Step 5: CSS for findings** — append to `ui/product/style.css` (tokens only, no hard-coded values):

```css
/* ---------- Validation findings (RFC 026 Phase 4) ---------- */
.findings-block { margin-top: var(--s-3); }
.findings-list { list-style: none; margin: var(--s-2) 0 0; padding: 0; display: flex; flex-direction: column; gap: var(--s-2); }
.finding { display: flex; flex-direction: column; gap: var(--s-1); font-size: var(--text-sm); }
.finding-path { font-size: var(--text-xs); }
.finding-msg { color: var(--fg-0); }
```

- [ ] **Step 6: Syntax check.** `node --check ui/product/api.js && node --check ui/product/lib/findings.js` → OK.
- [ ] **Step 7: Manual verification checklist (record in commit body; run in Task 9's live walkthrough since these are pure helpers):**
  - `import { getComponents } from '/ui/api.js'` resolves in the browser console with no error (module graph intact).
  - `renderFindings([{path:'stages[0].components[0].config.url', message:'required', severity:'error'}])` returns a string containing `stages[0]` and `required`; `renderFindings([])` returns `''`; `renderFindings(null)` returns `''`.
- [ ] **Step 8: Commit** `feat(ui): api catalog helpers + findings-aware pipeline save + shared findings renderer (RFC 026 P4)`.

---

### Task T3: Catalog page `/ui/components`

**sonnet.** **Parallel: yes (disjoint files)** — with T4. **Files:**
- Create: `ui/product/pages/components.js`.
- Modify: `ui/product/app.js` (register the route + add a nav item).
- Modify: `ui/product/style.css` (small block: versions table already covered by `.table`; add only a deprecation badge + schema definition-list classes if `.badge` doesn't fit).
- Verify: `node --check` on `components.js` and `app.js`.

**Interfaces:**
- Consumes: T2's `getComponents`, `getComponent`; `esc`, `emptyState`, `skeletonRows`, `spinner` from existing modules; `icons` from `/ui/icons.js`.
- Produces: `export async function renderComponents(ctx)` — one page module serving **two views** off a query/hash: the **list** (all components) and the **detail** (`?name=<component>` or a sub-path). Also a self-contained **JSON-Schema → definition-list** renderer (`schemaDocs(schemaStr)`) that turns a component version's `configSchema` string into readable docs (property name, type, required marker, description, enum values, `x-datuplet-secret` badge). No external libs.

Decision — **routing shape for the two views:** use a query param on one route, not two regex routes, to keep the router diff minimal and avoid a second capture-group pattern. Route: `{ pattern: /^\/ui\/components\/?$/, render: renderComponents }`. Detail is `/ui/components?name=<name>` (read via `new URLSearchParams(window.location.search)`). **Verified nuance (app.js line ~194):** the global `/ui/*` click interceptor guards `pushState` with `if (window.location.pathname !== href)` — it compares the bare `pathname` against the full `href` (which includes `?name=…`), so a query-bearing link is always `!==` and always pushes; and it calls `renderRoute()` unconditionally afterward. So both list→detail and detail→detail (`?name=a`→`?name=b`) navigations push + re-render correctly. Because `renderRoute`'s own abort guard snapshots `pathname` only (not `search`), `renderComponents` MUST use its own search-aware `aborted()` (shown in the skeleton) so a detail→detail switch mid-fetch aborts the stale render. The subagent must confirm this interceptor behavior is unchanged from what T1 recorded before relying on it.

- [ ] **Step 1: Route + nav registration in `app.js`.**
  - Import: add `import { renderComponents } from '/ui/pages/components.js';` next to the other page imports.
  - Route: add `{ pattern: /^\/ui\/components\/?$/, render: renderComponents },` to the `routes` array (place it just before the `/ui/settings/secrets` entry so nav order and route order agree).
  - Nav: add `{ href: '/ui/components', label: 'Components', icon: 'box' }` to `NAV_ITEMS` (place between `query` and `settings/secrets`). **Verify the icon name exists** in `/ui/icons.js` during T1/T3 — if `box` is absent, pick an existing one (candidates to grep: `box`, `package`, `grid`, `layers`, `database`); record the chosen icon. The prefix-match in `navItemsHTML` already handles the query-param detail view staying "active" (pathname is still `/ui/components`).

- [ ] **Step 2: `components.js` — page skeleton (this is the complete core; fill the two render bodies per the recipe).**

```js
// Components catalog (RFC 026 §4.7 item 1). Two views off one route:
//   /ui/components            → list of registered ComponentDefinitions
//   /ui/components?name=<n>   → detail: versions table + rendered config docs
//
// The catalog is readable by any authenticated project member — it IS the
// shared component picker. Deprecated components get a badge but stay listed.

import { esc, getComponents, getComponent } from '/ui/api.js';
import { emptyState, skeletonRows, spinner } from '/ui/components.js';
import * as icons from '/ui/icons.js';

export async function renderComponents(ctx) {
  const head = document.getElementById('page-head');
  const app = document.getElementById('app');
  const path = window.location.pathname;
  const search = window.location.search;
  const aborted = () => window.location.pathname !== path || window.location.search !== search;

  const selected = new URLSearchParams(search).get('name');
  if (selected) {
    await renderDetail(head, app, selected, aborted);
  } else {
    await renderList(head, app, aborted);
  }
}

async function renderList(head, app, aborted) {
  head.innerHTML = `
    <h1>Components</h1>
    <div class="actions">
      <input class="input" data-role="page-search" type="search" placeholder="Filter components…" />
    </div>`;
  app.innerHTML = `<table class="table"><thead><tr>
      <th>Component</th><th>Description</th><th>Default version</th><th></th>
    </tr></thead><tbody>${skeletonRows(4, 4)}</tbody></table>`;

  let comps;
  try {
    comps = await getComponents();
    if (aborted()) return;
  } catch (e) {
    if (aborted()) return;
    if (String(e.message) === 'not authenticated') return;
    app.innerHTML = `<p style="color:var(--status-fail-fg);">Failed to load components: ${esc(e.message)}</p>`;
    return;
  }
  if (!Array.isArray(comps) || comps.length === 0) {
    app.innerHTML = emptyState({ icon: icons.box, text: 'No components registered.' });
    return;
  }

  const rows = (list) => list.map(rowHTML).join('')
    || `<tr><td colspan="4" style="color:var(--fg-2);text-align:center;padding:var(--s-5);">No components match.</td></tr>`;
  app.innerHTML = `<table class="table"><thead><tr>
      <th>Component</th><th>Description</th><th>Default version</th><th></th>
    </tr></thead><tbody id="comp-body">${rows(comps)}</tbody></table>`;

  const searchInput = head.querySelector('[data-role="page-search"]');
  searchInput?.addEventListener('input', () => {
    const q = searchInput.value.trim().toLowerCase();
    const filtered = q ? comps.filter((c) =>
      `${c.name} ${c.displayName || ''} ${c.description || ''}`.toLowerCase().includes(q)) : comps;
    document.getElementById('comp-body').innerHTML = rows(filtered);
  });
}

function rowHTML(c) {
  const dep = c.deprecated ? ` <span class="badge badge--deprecated">deprecated</span>` : '';
  const href = `/ui/components?name=${encodeURIComponent(c.name)}`;
  const def = c.defaultVersion || (c.versions && c.versions.length ? '(latest)' : '—');
  return `<tr>
    <td><a href="${href}"><code class="inline">${esc(c.name)}</code></a>${dep}</td>
    <td>${esc(c.description || c.displayName || '')}</td>
    <td><code class="mono">${esc(def)}</code></td>
    <td><a class="btn btn--ghost" href="${href}">Details</a></td>
  </tr>`;
}

async function renderDetail(head, app, name, aborted) {
  head.innerHTML = `
    <h1><code class="inline">${esc(name)}</code></h1>
    <div class="actions"><a class="btn btn--secondary" href="/ui/components">Back to catalog</a></div>`;
  app.innerHTML = `<p aria-busy="true">${spinner()} Loading…</p>`;

  let c;
  try {
    c = await getComponent(name);
    if (aborted()) return;
  } catch (e) {
    if (aborted()) return;
    if (String(e.message) === 'not authenticated') return;
    app.innerHTML = `<p style="color:var(--status-fail-fg);">Failed to load ${esc(name)}: ${esc(e.message)}</p>`;
    return;
  }

  const dep = c.deprecated
    ? `<div class="callout callout--warn">${icons.alertTriangle || ''} This component is deprecated.</div>` : '';
  const versions = Array.isArray(c.versions) ? c.versions : [];
  const versionRows = versions.map((v) => `
    <tr>
      <td><code class="mono">${esc(v.version)}</code>${v.version === c.defaultVersion ? ' <span class="badge">default</span>' : ''}</td>
      <td>${v.prerelease ? '<span class="badge badge--pre">prerelease</span>' : 'stable'}</td>
      <td><code class="mono">${esc(v.image)}</code></td>
    </tr>`).join('') || `<tr><td colspan="3" style="color:var(--fg-2);">No versions.</td></tr>`;

  // Config docs come from the default (or first) version's schema.
  const docVersion = versions.find((v) => v.version === c.defaultVersion)
    || versions.find((v) => !v.prerelease) || versions[0];
  const docsHTML = docVersion && docVersion.configSchema
    ? schemaDocs(docVersion.configSchema)
    : `<p style="color:var(--fg-2);">No config schema.</p>`;

  app.innerHTML = `
    ${dep}
    <p>${esc(c.description || '')}</p>
    <h3 class="section-h">Versions</h3>
    <table class="table"><thead><tr><th>Version</th><th>Channel</th><th>Image</th></tr></thead>
      <tbody>${versionRows}</tbody></table>
    <h3 class="section-h">Config schema${docVersion ? ` <code class="inline">${esc(docVersion.version)}</code>` : ''}</h3>
    ${docsHTML}`;
}
```

- [ ] **Step 3: `schemaDocs(schemaStr)` — the JSON-Schema → definition-list renderer (complete, append to `components.js`).** No external validator; a defensive walk of the parsed schema's top-level `properties`. Handles `type`, `enum`, `description`, `required[]`, `x-datuplet-secret`, and nested `object`/`array` via a short type-summary string (not a full recursive form — that's T4's job; here it's read-only docs).

```js
// schemaDocs renders a JSON Schema (draft 2020-12) string as a read-only
// definition list of its top-level properties. Best-effort: unparseable or
// non-object schemas degrade to a note. Never throws.
function schemaDocs(schemaStr) {
  let schema;
  try {
    schema = JSON.parse(schemaStr);
  } catch {
    return `<p style="color:var(--status-fail-fg);">Config schema is not valid JSON.</p>`;
  }
  if (!schema || typeof schema !== 'object') {
    return `<p style="color:var(--fg-2);">Config schema is empty.</p>`;
  }
  const props = schema.properties && typeof schema.properties === 'object' ? schema.properties : {};
  const required = Array.isArray(schema.required) ? schema.required : [];
  const keys = Object.keys(props);
  if (keys.length === 0) {
    return `<p style="color:var(--fg-2);">This component accepts arbitrary config (no declared properties).</p>`;
  }
  const rows = keys.map((k) => {
    const p = props[k] || {};
    const isReq = required.includes(k);
    const secret = p['x-datuplet-secret'] === true;
    const enumVals = Array.isArray(p.enum)
      ? ` <span class="schema-enum">one of: ${p.enum.map((e) => esc(String(e))).join(', ')}</span>` : '';
    return `
      <div class="schema-prop">
        <div class="schema-prop-head">
          <code class="mono">${esc(k)}</code>
          <span class="schema-type">${esc(typeSummary(p))}</span>
          ${isReq ? '<span class="badge badge--req">required</span>' : ''}
          ${secret ? '<span class="badge badge--secret">secret</span>' : ''}
        </div>
        ${p.description ? `<div class="schema-desc">${esc(p.description)}</div>` : ''}
        ${enumVals}
      </div>`;
  }).join('');
  const extra = schema.additionalProperties === false
    ? '' : `<p class="schema-note">Additional properties are allowed.</p>`;
  return `<div class="schema-docs">${rows}${extra}</div>`;
}

// typeSummary → a short human string for a property schema node.
function typeSummary(p) {
  if (p.enum) return 'enum';
  const t = p.type;
  if (t === 'array') {
    const items = p.items && p.items.type ? p.items.type : 'any';
    return `array<${items}>`;
  }
  if (Array.isArray(t)) return t.join(' | ');
  return t || 'any';
}
```

- [ ] **Step 4: CSS** — append to `ui/product/style.css`. Reuse `.badge`; add only variant colors + the schema-doc layout:

```css
/* ---------- Components catalog (RFC 026 Phase 4) ---------- */
.section-h { margin: var(--s-5) 0 var(--s-2); font-size: var(--text-lg); }
.badge--deprecated { color: var(--status-fail-fg); }
.badge--pre { color: var(--status-running-fg); }
.badge--req { color: var(--status-running-fg); }
.badge--secret { color: var(--accent); }
.schema-docs { display: flex; flex-direction: column; gap: var(--s-3); }
.schema-prop { border: 1px solid var(--border); border-radius: var(--radius); padding: var(--s-3); background: var(--bg-1); }
.schema-prop-head { display: flex; align-items: center; gap: var(--s-2); flex-wrap: wrap; }
.schema-type { color: var(--fg-2); font-size: var(--text-xs); font-family: var(--font-mono); }
.schema-desc { color: var(--fg-1); font-size: var(--text-sm); margin-top: var(--s-1); }
.schema-enum { color: var(--fg-2); font-size: var(--text-xs); display: block; margin-top: var(--s-1); }
.schema-note { color: var(--fg-2); font-size: var(--text-xs); }
```

  (If `.badge` base styles don't already zero out inheritance, verify these variant selectors win by specificity; the base `.badge` is defined ~line 747.)

- [ ] **Step 5: Syntax check.** `node --check ui/product/pages/components.js && node --check ui/product/app.js`.
- [ ] **Step 6: Manual verification checklist (record in commit; executed live in Task 9):**
  1. Nav shows a **Components** item; clicking it lands on `/ui/components`. Expected DOM: `#page-head h1` text `Components`, `#app table.table` with a row per component.
  2. Type a component substring in the filter → tbody rows narrow; clearing restores all. Expected: `#comp-body tr` count changes.
  3. Click a component name or its "Details" button → URL becomes `/ui/components?name=<n>`; `#page-head h1` shows the component name; `#app` shows a **Versions** `table.table` (one row per version, `default`/`prerelease` badges correct) and a **Config schema** section with one `.schema-prop` per top-level property, `required`/`secret` badges where the schema declares them.
  4. A deprecated component shows a `.badge--deprecated` in the list and a `.callout--warn` on detail.
  5. "Back to catalog" returns to the list.
- [ ] **Step 7: Commit** `feat(ui): component catalog page — list, versions table, schema docs (RFC 026 P4)`.

---

### Task T4: Schema-form renderer module

**opus.** **Parallel: yes (disjoint files)** — with T3 (touches only `lib/schema-form.js`; **no style.css edit here** — T3 also appends to style.css, so the form CSS lands in T6 to keep this parallel pair genuinely file-disjoint). **Files:**
- Create: `ui/product/lib/schema-form.js`.
- Verify: `node --check`.

This is the one genuinely non-trivial module of the phase: it turns a JSON-Schema `properties` object into DOM form controls and reads them back out into a plain JS object. It is a **pure module** — it builds into a container element you pass, wires its own input listeners, and exposes a `getValue()` accessor. No network, no routing.

**Interfaces (T6 depends on exact names):**
- Consumes: `esc` from `/ui/api.js`; a `listSecretsFn` **injected** (so the module never imports project-scoped API directly — T6 passes `() => listSecrets(pid)`), keeping this module dependency-light and testable in isolation.
- Produces:
  - `export function buildSchemaForm(container, schemaStr, initialValue, opts)` — parses the schema string, renders fields into `container` (an element), returns a **handle** `{ getValue, getErrors, destroy }`.
    - `getValue()` → a plain JS object (only fields with a non-empty value are included; unset optional fields are omitted, not `null`).
    - `getErrors()` → `[{path, message}]` for client-side-detectable problems (required field empty, object subeditor JSON unparseable, secret field not a `$[...]` ref). This is a **convenience pre-check**, not authoritative — the server validator is the source of truth (spec §4.3). T6 may show these before saving.
    - `destroy()` → removes listeners (no-op if none; present for symmetry with future needs).
  - `opts`: `{ listSecretsFn?: () => Promise<Array<{key}>>, secretKeys?: string[] }` — either an async fetch of secret keys or a pre-fetched list (T6 fetches once and passes `secretKeys`).

**Type handling matrix (spec §4.7 item 3 — implement exactly these):**

| Schema node | Control |
|---|---|
| `type: string` (no enum, no `x-datuplet-secret`) | `<input type="text">` (or `<textarea>` when `format` suggests multiline, e.g. the property key is `sql` or schema has `"x-datuplet-multiline": true` — optional nicety; default text input) |
| `type: number` / `type: integer` | `<input type="number">` (integer adds `step="1"`); read back with `Number()`, integers via `parseInt` — omit when blank |
| `type: boolean` | `<input type="checkbox">`; read back as `true`/`false` (always included when the property is present in the form, since a checkbox has a definite state) |
| any node with `enum: [...]` | `<select class="input">` with an empty "— choose —" first option (unless required) + one option per enum value |
| `type: array` with `items.type` scalar (string/number/integer) | array-of-scalars editor: a `<textarea>` one-value-per-line **OR** a repeatable input list. **Use one-value-per-line textarea** (simplest, no add/remove button state) with a hint "one per line"; read back by splitting on `\n`, trimming, dropping blanks, coercing per `items.type` |
| `type: object` (or no type but has nested shape, or `additionalProperties`) | **collapsible JSON subeditor**: a native `<details>` containing a `<textarea class="input--mono">` pre-filled with pretty-printed JSON of the initial value; read back via `JSON.parse` (parse error → a `getErrors()` entry, and `getValue` omits the key). This is the explicit fallback in the spec — do not attempt to recurse into nested object forms. |
| property with `x-datuplet-secret: true` | **secret picker**: a `<select>` populated from `secretKeys` (each option value `$[<key>]`) + a first option "— none —" + a final option "+ manage secrets…" that links to `/ui/settings/secrets` (rendered as a note/link beside the select, not a real option to avoid selecting it). Read back the `$[key]` string. `getErrors()` flags a required secret field left unset, and any secret value that is not a `$[...]` ref. (§4.9: `x-datuplet-secret` REQUIRES a `$[...]` ref; plaintext is rejected server-side, and the picker structurally prevents it.) |

**Required-field marking:** properties in the schema's top-level `required[]` get a visible `*` marker and (for text/number/select) the `required` attribute; empties surface in `getErrors()`.

- [ ] **Step 1: Module skeleton + parse guard (complete core; the per-type field builders are the substance — write them per the matrix).**

```js
// schema-form.js — JSON Schema (draft 2020-12) → HTML form → plain JS object.
//
// Pure module: build into a container, read back with getValue(). No network
// (secret keys are injected via opts), no routing. Only top-level properties
// get real controls; nested objects fall back to a JSON subeditor textarea
// (RFC 026 §4.7 item 3 — two-way object editing is explicitly out of scope).
//
// Every schema-derived string is esc()'d before it reaches innerHTML.

import { esc } from '/ui/api.js';

/**
 * @param {HTMLElement} container
 * @param {string} schemaStr   JSON Schema as a string (component configSchema)
 * @param {object} initialValue  existing config object (may be {})
 * @param {{secretKeys?: string[], listSecretsFn?: () => Promise<Array<{key:string}>>}} [opts]
 * @returns {{getValue: () => object, getErrors: () => Array<{path:string,message:string}>, destroy: () => void}}
 */
export function buildSchemaForm(container, schemaStr, initialValue = {}, opts = {}) {
  let schema;
  try {
    schema = JSON.parse(schemaStr || '{}');
  } catch {
    container.innerHTML = `<div class="callout callout--warn">Component config schema is not valid JSON — use the “Edit as YAML” view.</div>`;
    return degenerate(initialValue);
  }
  const props = schema && typeof schema.properties === 'object' ? schema.properties : null;
  if (!props || Object.keys(props).length === 0) {
    // No declared properties → nothing to render as a form. Signal caller to
    // fall back to raw editing.
    container.innerHTML = `<div class="callout">This component has no structured schema; edit its config as YAML.</div>`;
    return degenerate(initialValue);
  }
  const required = new Set(Array.isArray(schema.required) ? schema.required : []);
  const secretKeys = Array.isArray(opts.secretKeys) ? opts.secretKeys : [];

  // fieldReaders: key → () => { present:boolean, value:any, error?:string }
  const fieldReaders = {};
  const parts = Object.keys(props).map((key) => {
    const node = props[key] || {};
    const isReq = required.has(key);
    const init = initialValue == null ? undefined : initialValue[key];
    const built = buildField(key, node, isReq, init, secretKeys);
    fieldReaders[key] = built.read;
    return built.html;
  });

  container.innerHTML = `<div class="sform">${parts.join('')}</div>`;
  // Post-render wiring (e.g. array textarea autosize) can attach here via
  // container.querySelector using stable data-attributes set in buildField.

  function collect() {
    const out = {};
    const errors = [];
    for (const key of Object.keys(fieldReaders)) {
      const r = fieldReaders[key](container);
      if (r.error) errors.push({ path: key, message: r.error });
      if (r.present) out[key] = r.value;
    }
    return { out, errors };
  }
  return {
    getValue: () => collect().out,
    getErrors: () => collect().errors,
    destroy: () => { container.innerHTML = ''; },
  };
}

function degenerate(initialValue) {
  // A handle that just echoes the initial value (used when there's no usable
  // schema). getValue returns a shallow copy so callers can't mutate state.
  return {
    getValue: () => (initialValue && typeof initialValue === 'object' ? { ...initialValue } : {}),
    getErrors: () => [],
    destroy: () => {},
  };
}
```

- [ ] **Step 2: `buildField(key, node, isReq, init, secretKeys)`** — returns `{ html, read }` where `read(container)` locates the control by a stable `data-sf-key="<key>"` attribute and returns `{present, value, error?}`. Implement one branch per the type matrix. Skeleton for the branching + two representative branches (string and object-subeditor) shown; the agent writes the rest following the same shape:

```js
function buildField(key, node, isReq, init, secretKeys) {
  const label = fieldLabel(key, node, isReq);
  const id = `sf-${key}`;
  const dk = esc(key);

  // --- secret picker (highest precedence: x-datuplet-secret) ---
  if (node['x-datuplet-secret'] === true) {
    return secretField(key, isReq, init, secretKeys, label, id, dk);
  }
  // --- enum → select ---
  if (Array.isArray(node.enum)) {
    return enumField(key, node, isReq, init, label, id, dk);
  }
  // --- boolean → checkbox ---
  if (node.type === 'boolean') {
    return booleanField(key, isReq, init, label, id, dk);
  }
  // --- number / integer ---
  if (node.type === 'number' || node.type === 'integer') {
    return numberField(key, node, isReq, init, label, id, dk);
  }
  // --- array of scalars ---
  if (node.type === 'array' && node.items && isScalarType(node.items.type)) {
    return scalarArrayField(key, node, isReq, init, label, id, dk);
  }
  // --- object / unknown-nested → JSON subeditor (explicit fallback) ---
  if (node.type === 'object' || node.type === undefined) {
    return objectSubeditor(key, isReq, init, label, id, dk);
  }
  // --- default: string text input ---
  return stringField(key, node, isReq, init, label, id, dk);
}

function isScalarType(t) { return t === 'string' || t === 'number' || t === 'integer'; }

function fieldLabel(key, node, isReq) {
  const req = isReq ? ' <span class="sform-req" title="required">*</span>' : '';
  const desc = node.description ? `<span class="sform-desc">${esc(node.description)}</span>` : '';
  return `<span class="sform-label"><code class="mono">${esc(key)}</code>${req}</span>${desc}`;
}

// Representative branch: plain string.
function stringField(key, node, isReq, init, label, id, dk) {
  const val = init == null ? '' : String(init);
  const multiline = key === 'sql' || node['x-datuplet-multiline'] === true;
  const control = multiline
    ? `<textarea class="input textarea input--mono" id="${id}" data-sf-key="${dk}" ${isReq ? 'required' : ''}>${esc(val)}</textarea>`
    : `<input class="input" type="text" id="${id}" data-sf-key="${dk}" value="${esc(val)}" ${isReq ? 'required' : ''}>`;
  return {
    html: `<label class="field sform-field">${label}${control}</label>`,
    read: (c) => {
      const el = c.querySelector(`[data-sf-key="${cssEsc(key)}"]`);
      const v = el ? el.value.trim() : '';
      if (v === '') return { present: false, value: undefined, error: isReq ? 'required' : undefined };
      return { present: true, value: v };
    },
  };
}

// Representative branch: object → collapsible JSON subeditor.
function objectSubeditor(key, isReq, init, label, id, dk) {
  const pretty = init == null ? '' : safePretty(init);
  return {
    html: `
      <div class="field sform-field">
        ${label}
        <details class="sform-json" ${pretty ? 'open' : ''}>
          <summary>edit as JSON</summary>
          <textarea class="input textarea input--mono" id="${id}" data-sf-key="${dk}" spellcheck="false" placeholder="{ }">${esc(pretty)}</textarea>
        </details>
      </div>`,
    read: (c) => {
      const el = c.querySelector(`[data-sf-key="${cssEsc(key)}"]`);
      const raw = el ? el.value.trim() : '';
      if (raw === '') return { present: false, value: undefined, error: isReq ? 'required' : undefined };
      try {
        return { present: true, value: JSON.parse(raw) };
      } catch (e) {
        return { present: false, value: undefined, error: `not valid JSON (${e.message})` };
      }
    },
  };
}

function safePretty(v) { try { return JSON.stringify(v, null, 2); } catch { return ''; } }

// cssEsc: escape a property key for use inside a [data-sf-key="..."] selector.
// Property names are schema-controlled but may contain characters that break
// an attribute selector; prefer CSS.escape when available.
function cssEsc(s) {
  if (window.CSS && typeof window.CSS.escape === 'function') return window.CSS.escape(s);
  return String(s).replace(/["\\\]]/g, '\\$&');
}
```

  **Remaining branches to implement (same `{html, read}` contract):**
  - `enumField`: `<select class="input" data-sf-key>`; first option `<option value="">— choose —</option>` (omit when required and there's a natural default); one `<option>` per enum value (value = `String(e)`, escaped). Read: empty → `{present:false, error: isReq?'required':undefined}`; else coerce back to the enum entry's original type by matching against `node.enum` (string compare on `String(e)` then return the matched original) so an integer enum round-trips as a number.
  - `booleanField`: `<input type="checkbox" data-sf-key>` (checked from `init === true`). Read: **always present**, `value: el.checked`. (A boolean has a definite state; including it is correct.)
  - `numberField`: `<input type="number" data-sf-key>` (add `step="1"` + `inputmode="numeric"` for integer). Read: blank → `{present:false, error: isReq?'required':undefined}`; else `Number(v)` (integer via `parseInt(v,10)`); `NaN` → error `must be a number`.
  - `scalarArrayField`: `<textarea data-sf-key>` with hint "one per line"; init pre-filled by joining an array with `\n` (coerce each to string). Read: split on `\n`, `map(trim)`, drop blanks; empty list → `{present:false}` (unless required → error); else coerce each element per `node.items.type` (number/integer via Number/parseInt; a non-numeric element → error `line N is not a number`).
  - `secretField`: `<select class="input" data-sf-key>` — first `<option value="">— none —</option>`, then one `<option value="$[<key>]">$[<key>]</option>` per secret key; if `init` is a `$[...]` ref not in the list, add it as a selected option so an existing value survives a stale list. Beside the select, a small link: `<a href="/ui/settings/secrets" class="sform-manage">manage secrets…</a>`. Read: empty → `{present:false, error: isReq?'a secret reference is required':undefined}`; a value must match `^\$\[[^\]]+\]$` else error `must be a $[secret] reference` (defensive — the select only offers valid refs, but guard the stale-init path).

- [ ] **Step 3: CSS — deliberately NOT in this task.** The `.sform*` classes referenced above are defined in **T6's CSS step** (T3 ∥ T4 must stay file-disjoint and T3 already appends to `style.css`). Until T6 lands the form renders unstyled — acceptable, nothing consumes it before T6.

- [ ] **Step 4: Syntax check.** `node --check ui/product/lib/schema-form.js`.
- [ ] **Step 5: Isolation verification (no framework exists — do it in the browser console; record the exact snippet + expected output in the commit body).** In Task 9's live stack, from the console:

```js
const m = await import('/ui/lib/schema-form.js');
const box = document.createElement('div'); document.body.appendChild(box);
const schema = JSON.stringify({
  type:'object', required:['url'],
  properties:{
    url:{type:'string', format:'uri', description:'endpoint'},
    threads:{type:'integer'},
    active:{type:'boolean'},
    mode:{enum:['append','overwrite']},
    tags:{type:'array', items:{type:'string'}},
    headers:{type:'object'},
    token:{type:'string','x-datuplet-secret':true}
  }
});
const h = m.buildSchemaForm(box, schema, { threads: 4, active: true, mode:'append', tags:['a','b'] }, { secretKeys:['api_token'] });
// expected DOM: url text input (required *), threads number input =4,
//   active checkbox checked, mode select with append selected,
//   tags textarea "a\nb", headers <details><textarea>, token <select> with $[api_token].
console.log(h.getValue()); // { threads:4, active:true, mode:'append', tags:['a','b'] } (url empty → omitted)
console.log(h.getErrors()); // [{path:'url', message:'required'}]
box.remove();
```

  Expected: `getValue()` matches the comment; `getErrors()` reports the missing required `url`. Fill `url`, set `token` to `$[api_token]`, re-read → `url` present, no errors. Put invalid JSON in the `headers` subeditor → `getErrors()` includes a `headers` "not valid JSON" entry and `getValue()` omits `headers`.
- [ ] **Step 6: Commit** `feat(ui): JSON-Schema → form renderer (string/number/bool/enum/array/object/secret) (RFC 026 P4)`.

---

### Task T5: Builder v1 — catalog dropdown + docs panel + inline findings

**sonnet.** **Files:**
- Modify: `ui/product/pages/pipeline-detail.js` (extend, do not replace, the existing editor — **and keep the merged "Recent runs" section intact**: `#pipeline-runs` + `loadRecentRuns` + the `/ui/format.js` imports, below the builder/editor; see T1's pipeline-detail baseline).
- Modify: `ui/product/pages/components.js` (export `schemaDocs` — Step 2).
- Modify: `ui/product/style.css` (the `.builder-*` layout classes — Step 3).
- Verify: `node --check` on all three.

**Interfaces:**
- Consumes: T2 `getComponents`, `getComponent`, `putPipelineYAML`; T2 `renderFindings` from `/ui/lib/findings.js`; existing `api`, `esc` from `/ui/api.js`.
- Produces: builder v1 UX layered onto the existing textarea page — the textarea remains as the "advanced" surface (spec §4.7 item 2). Adds: (a) an **"Add component"** control above the editor: a `<select>` populated from the catalog; picking one inserts a correctly-shaped YAML component snippet at the textarea cursor; (b) a **docs side panel** that shows the selected component's rendered config schema (reuse the catalog's `schemaDocs` — **export it from `components.js`** so it's shared, or duplicate the tiny renderer; prefer export); (c) on save, **inline findings** rendered under the editor via `renderFindings`, replacing the current opaque `err.message` banner.

**Anchor note (Task 1 verified):** `renderPipelineDetail(ctx)`, the module-level `STARTER_YAML` const, the `#pipeline-form` / `#pipeline-msg` / `textarea[name=yaml]` element IDs/names, the submit handler, **and the "Recent runs" section (`#pipeline-runs` + `loadRecentRuns` — merged PR #23, must survive every T5/T6/T7 template rewrite)** are the stable anchors. `STARTER_YAML` currently uses `image:` — **update it to the registry shape** (`component:` + structured `config:`) as part of this task (the raw `image:` field was dropped from `ComponentSpec` in Phase 2 — §4.2 decision 10; a starter template teaching `image:` would now be invalid).

- [ ] **Step 1: Update `STARTER_YAML`** to the Phase-2 registry shape:

```js
const STARTER_YAML = `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: c1
          component: http-json-extractor
          config:
            url: "https://api.example.com/items"
          outputs:
            defaultBucket: raw
            defaultWriteMode: FULL_LOAD
`;
```

- [ ] **Step 2: Export `schemaDocs` from `components.js`** (change `function schemaDocs` → `export function schemaDocs`, likewise `typeSummary` if referenced — keep `typeSummary` module-private by inlining or export both). In `pipeline-detail.js` import it: `import { schemaDocs } from '/ui/pages/components.js';`. (A page module importing another page module's helper is fine — they're plain ES modules; verify no circular import: `components.js` imports only api/components/icons, not pipeline-detail, so the graph stays acyclic.)

- [ ] **Step 3: Two-column layout + add-component control.** Wrap the existing form and a new docs aside in a two-column container. The existing `<form id="pipeline-form">` stays intact; add above the `<textarea>` (still inside the form or just above it):

```html
<div class="builder-toolbar">
  <label class="field builder-add">Add component
    <select class="input" id="add-component"><option value="">Loading…</option></select>
  </label>
  <button type="button" class="btn btn--secondary" id="insert-component" disabled>Insert snippet</button>
</div>
```

  and a docs aside as a sibling column:

```html
<aside class="builder-docs" id="builder-docs">
  <p style="color:var(--fg-2);">Pick a component to see its config schema.</p>
</aside>
```

  Layout via a new `.builder-layout` grid (editor column + docs aside). CSS (append to style.css):

```css
/* ---------- Pipeline builder (RFC 026 Phase 4) ---------- */
.builder-layout { display: grid; grid-template-columns: minmax(0,1fr) 320px; gap: var(--s-4); align-items: start; }
.builder-toolbar { display: flex; align-items: flex-end; gap: var(--s-2); margin-bottom: var(--s-3); }
.builder-add { margin-bottom: 0; flex: 1; }
.builder-docs { border: 1px solid var(--border); border-radius: var(--radius); padding: var(--s-3); background: var(--bg-1); position: sticky; top: var(--s-4); max-height: 80vh; overflow-y: auto; }
@media (max-width: 860px) { .builder-layout { grid-template-columns: 1fr; } .builder-docs { position: static; } }
```

- [ ] **Step 4: Populate the dropdown + wire insert + docs.** After the page HTML is set, fetch the catalog once and wire behavior. Recipe (agent writes against the existing structure; abort-guard the fetch):

```js
// after app.innerHTML = ...; inside renderPipelineDetail, guarded by aborted()
const sel = document.getElementById('add-component');
const insertBtn = document.getElementById('insert-component');
const docs = document.getElementById('builder-docs');
let comps = [];
try {
  comps = await getComponents();
  if (aborted()) return;
} catch (e) { /* swallow 'not authenticated'; otherwise leave dropdown disabled */ }
sel.innerHTML = `<option value="">— choose a component —</option>` +
  comps.map((c) => `<option value="${esc(c.name)}">${esc(c.displayName || c.name)}${c.deprecated ? ' (deprecated)' : ''}</option>`).join('');

sel.addEventListener('change', async () => {
  const name = sel.value;
  insertBtn.disabled = !name;
  if (!name) { docs.innerHTML = `<p style="color:var(--fg-2);">Pick a component to see its config schema.</p>`; return; }
  docs.innerHTML = `<p aria-busy="true">Loading…</p>`;
  try {
    const c = await getComponent(name);
    if (aborted()) return;
    const v = (c.versions || []).find((x) => x.version === c.defaultVersion)
      || (c.versions || []).find((x) => !x.prerelease) || (c.versions || [])[0];
    docs.innerHTML = `<h3 class="section-h" style="margin-top:0;"><code class="inline">${esc(name)}</code></h3>` +
      (v && v.configSchema ? schemaDocs(v.configSchema) : `<p style="color:var(--fg-2);">No schema.</p>`);
    // stash the resolved default version for the snippet
    sel.dataset.version = v ? v.version : '';
  } catch (e) { if (!aborted()) docs.innerHTML = `<p style="color:var(--status-fail-fg);">${esc(e.message)}</p>`; }
});

insertBtn.addEventListener('click', () => {
  const name = sel.value; if (!name) return;
  const snippet = componentSnippet(name);       // helper below
  insertAtCursor(document.querySelector('textarea[name=yaml]'), snippet);
});
```

  **`componentSnippet(name)`** — produce a correctly-indented component list entry. Because YAML indentation depends on insertion context, insert a **stand-alone `components:`-level entry** with a leading comment telling the user where to paste it if the cursor isn't already under a `components:` block. Keep it minimal and valid:

```js
function componentSnippet(name) {
  const ver = document.getElementById('add-component').dataset.version || '';
  return `        - name: ${name.replace(/[^a-z0-9-]/g, '-')}
          component: ${name}${ver ? `\n          version: ${ver}` : ''}
          config: {}
`;
}
```

  **`insertAtCursor(textarea, text)`** — reuse the exact cursor-splice pattern from `query.js` (`selectionStart`/`selectionEnd` slice + reset caret + `focus()`). Do not re-derive; copy that proven idiom.

- [ ] **Step 5: Inline findings on save.** Replace the submit handler's save + error rendering. Instead of `putYAML` + `catch → banner error`, call `putPipelineYAML` and render findings:

```js
// inside submit handler, replacing the putYAML call + its catch banner:
const msg = document.getElementById('pipeline-msg');
msg.innerHTML = '';
try {
  const res = await putPipelineYAML(pid, targetName, yamlText);
  if (res.ok && (!res.findings || res.findings.length === 0)) {
    msg.innerHTML = `<div class="callout">Saved.</div>`;
  } else if (res.ok) {
    // 200 saved-with-warnings
    msg.innerHTML = `<div class="callout">Saved with warnings.</div>` + renderFindings(res.findings);
  } else {
    // 400 rejected — inline findings, not saved
    msg.innerHTML = renderFindings(res.findings);
  }
  if (res.ok && isNew) { /* existing replaceState → renderRoute jump */ }
} catch (err) {
  if (String(err.message) !== 'not authenticated') {
    msg.innerHTML = `<div class="callout callout--warn">${esc(err.message)}</div>`;
  }
}
```

  (Keep the `btn.disabled` toggling and the `isNew` navigation exactly as they are. Note the switch from the undefined `.banner success`/`.banner error` classes to the styled `.callout` / `.callout--warn` — do this consistently, including the delete-error path.)

- [ ] **Step 6: Syntax check.** `node --check ui/product/pages/pipeline-detail.js && node --check ui/product/pages/components.js`.
- [ ] **Step 7: Manual verification checklist (record in commit; run live in Task 9):**
  1. Open `/ui/pipelines/_new`. Expected: two-column layout, editor left with the new registry-shape starter YAML, docs aside right, an "Add component" select above the editor populated with catalog names.
  2. Pick a component → docs aside shows its schema (`.schema-prop` rows); "Insert snippet" enables.
  3. Click "Insert snippet" → a `- name: … / component: … / config: {}` block appears in the textarea at the cursor; caret lands after it; textarea keeps focus.
  4. Save a deliberately invalid pipeline (e.g. remove a required field or add `writeMod:` typo) → under the editor, `renderFindings` shows a `.callout--warn` with `N validation errors` and a mono path per finding; the pipeline is **not** saved (stays on the page).
  5. Fix it and save → `.callout` "Saved." (204) and, for `_new`, navigation to the named detail view.
- [ ] **Step 8: Commit** `feat(ui): pipeline builder v1 — catalog picker, docs panel, inline findings (RFC 026 P4)`.

---

### Task T6: Builder v2 — per-component form panel + one-way "edit as YAML" toggle

**sonnet.** **Files:**
- Modify: `ui/product/pages/pipeline-detail.js`.
- Modify: `ui/product/style.css` (the `.sform*` classes relocated here from T4 — see its Step 3 note).
- Verify: `node --check`.

**Interfaces:**
- Consumes: T4 `buildSchemaForm`; T2 `getComponent`, `listSecrets`; T1's whoami superadmin key. Builds on T5's two-column layout and catalog dropdown.
- Produces: builder v2 (spec §4.7 item 3). A **form mode** for authoring a single component's config: pick a component (reusing T5's dropdown), get a schema-generated form (T4) in the docs column position, fill it, and **"Add to YAML"** serializes the form's `getValue()` into a component block appended/inserted into the textarea. Plus the **one-way "Edit as YAML" toggle**: the textarea is always the source of truth; the form is a *builder* that emits into it. There is **no reverse sync** (form does not parse the textarea back) — this is the explicit non-goal (§4.7: "Two-way form↔YAML sync deliberately out of scope; one-way toggle with a confirm").

**Design — how "form mode" and "YAML mode" coexist without two-way sync:**
- The **textarea is canonical and always present** (builder v1 already made it the advanced view).
- Builder v2 adds a **"Build component" sub-panel** (in/replacing the docs aside): when a component is picked, render *both* the read-only `schemaDocs` **and** a live `buildSchemaForm` beneath a toggle. Two buttons: **"Insert as YAML"** (serializes `getValue()` → YAML component block → `insertAtCursor`, same splice as v1) and, when the user wants to hand-edit, an **"Edit as YAML" confirm**.
- The **"Edit as YAML" toggle with confirm** governs switching a partially-filled form into the raw textarea: clicking it pops a `confirm()` ("Switching to YAML editing. Your form entries for this component will be inserted into the YAML and the form cleared — you can't switch a hand-edited block back to the form. Continue?"). On OK: serialize the current form value into the textarea (so nothing is lost) and collapse the form. This satisfies "one-way toggle with a confirm" precisely — the confirm guards the irreversible direction.
- **Resource field hiding (spec §4.2 / §4.4 / prompt):** the builder must **HIDE** the `resources` field unless the session user is superadmin. Fetch `GET /api/v1/auth/me` once at page load; read the T1-recorded superadmin key (`is_superadmin` per spec assumption). The schema form only renders `config` properties (resources are a sibling of `config` in the component spec, not inside the JSON Schema), so in practice: **do not add any `resources` control to the form**, and if a "resources" affordance is later desired, gate it behind `me.is_superadmin === true`. For this phase, document in a code comment that resources remain YAML-only and are superadmin-gated server-side (§4.4 diff-gate); the form never emits a `resources` key. If T1 found no whoami flag, treat as non-superadmin (hide unconditionally).

- [ ] **Step 1: Fetch whoami + secret keys once at page load** (guarded by `aborted()`), store on locals:

```js
let me = null, secretKeys = [];
try { me = await api('/api/v1/auth/me'); if (aborted()) return; } catch { /* swallow */ }
try { const s = await listSecrets(pid); if (aborted()) return; secretKeys = (s || []).map((x) => x.key); } catch { /* secrets optional */ }
const isSuperadmin = !!(me && me.is_superadmin); // T1-verified key name
```

- [ ] **Step 2: Extend the aside into a build panel** with a mode toggle. When a component is picked (T5's `sel` change handler), in addition to `schemaDocs`, render a form container and controls:

```html
<div class="builder-form-wrap">
  <div class="builder-mode">
    <button type="button" class="btn btn--ghost" id="toggle-docs" aria-pressed="true">Form</button>
    <button type="button" class="btn btn--ghost" id="edit-as-yaml">Edit as YAML…</button>
  </div>
  <div id="builder-form"></div>
  <button type="button" class="btn btn--primary" id="insert-form" disabled>Insert as YAML</button>
</div>
```

  Then instantiate the renderer into `#builder-form`:

```js
const c = await getComponent(name);            // already fetched in v1 handler — reuse
const v = pickDocVersion(c);                   // default→stable→first (helper shared with v1)
const formHandle = buildSchemaForm(
  document.getElementById('builder-form'),
  (v && v.configSchema) || '{}',
  {},                                          // fresh component → empty initial config
  { secretKeys },
);
document.getElementById('insert-form').disabled = false;
```

  (`pickDocVersion` — extract the "default → first stable → first" version-selection logic from T5 into a shared helper so v1 and v2 agree.)

- [ ] **Step 3: "Insert as YAML" — serialize form → YAML component block.** No YAML library exists in the browser; write a **tiny, sufficient serializer** for the constrained shape the form produces (scalars, arrays of scalars, and JSON-object subvalues). Recipe:

```js
document.getElementById('insert-form').addEventListener('click', () => {
  const errs = formHandle.getErrors();
  if (errs.length) {
    document.getElementById('pipeline-msg').innerHTML = renderFindings(
      errs.map((e) => ({ path: `config.${e.path}`, message: e.message, severity: 'error' })));
    return;
  }
  const cfg = formHandle.getValue();
  const block = componentBlockYAML(sel.value, sel.dataset.version, cfg); // helper below
  insertAtCursor(document.querySelector('textarea[name=yaml]'), block);
});
```

  **`componentBlockYAML(name, version, cfg)`** — emit a `components:`-level entry with a nested `config:`. For serializing `cfg` values use a minimal converter: strings → quoted if they contain YAML-special chars or a newline (multi-line strings use block scalar `|`); numbers/booleans → literal; arrays of scalars → `- item` lines; objects → `JSON.stringify` inline (valid YAML flow syntax) as the pragmatic fallback that matches the form's own object-subeditor semantics. Keep indentation at the `components:` list level (8 spaces for `- name:`, matching v1's snippet). Include a short unit-style self-check in the console verification (Step 5). This serializer is intentionally small — the server re-parses and validates authoritatively (§4.3), so it only needs to emit *valid, correctly-shaped* YAML, not canonical YAML.

- [ ] **Step 4: "Edit as YAML" confirm (the one-way toggle).**

```js
document.getElementById('edit-as-yaml').addEventListener('click', () => {
  if (!confirm('Switch to YAML editing? Your current form entries for this component will be inserted into the YAML and the form cleared. You can’t convert a hand-edited block back into the form.')) return;
  // Preserve work: insert whatever the form currently holds, then collapse it.
  const cfg = formHandle.getValue();
  insertAtCursor(document.querySelector('textarea[name=yaml]'), componentBlockYAML(sel.value, sel.dataset.version, cfg));
  document.querySelector('.builder-form-wrap')?.remove();     // form gone; textarea is now the surface
  document.querySelector('textarea[name=yaml]')?.focus();
});
```

- [ ] **Step 4b: `.sform*` CSS** — append to `ui/product/style.css` (tokens only; these are the classes T4's renderer emits):

```css
/* ---------- Schema form (RFC 026 Phase 4) ---------- */
.sform { display: flex; flex-direction: column; gap: var(--s-2); }
.sform-field { gap: var(--s-1); }
.sform-label { display: flex; align-items: center; gap: var(--s-1); color: var(--fg-0); }
.sform-req { color: var(--status-fail-fg); }
.sform-desc { display: block; color: var(--fg-2); font-size: var(--text-xs); font-weight: 400; margin-top: var(--s-1); }
.sform-json summary { cursor: pointer; color: var(--fg-1); font-size: var(--text-sm); }
.sform-manage { font-size: var(--text-xs); color: var(--accent); }
```

- [ ] **Step 5: Syntax check + console verification of the serializer.** `node --check ui/product/pages/pipeline-detail.js`. In Task 9's live stack, verify `componentBlockYAML('sql-transform','v0.1.0',{sql:'SELECT 1;\nSELECT 2;', threads:4, tags:['a','b'], headers:{A:'b'}})` produces a block that, pasted under a `components:` list, round-trips through the server save without a YAML/structure finding (multi-line `sql` as block scalar, `threads: 4` unquoted int, `tags:` as a list, `headers:` as `{"A":"b"}` flow map). Record the emitted string in the commit body.
- [ ] **Step 6: Manual verification checklist (record in commit; run live in Task 9):**
  1. New pipeline → pick `http-json-extractor` → a **form** appears (url text input marked required, headers as a JSON subeditor `<details>`, any secret field as a `$[...]` select populated from `listSecrets`).
  2. Fill `url`, leave a required field blank → "Insert as YAML" surfaces a findings block naming the missing field (client pre-check); does not insert.
  3. Fill all required → "Insert as YAML" splices a valid `- name/component/version/config:` block into the textarea; saving it yields "Saved."
  4. Click "Edit as YAML…" → `confirm()` fires; on OK the current form values are inserted into the textarea and the form panel disappears; on Cancel nothing changes.
  5. As a **non-superadmin** session, no `resources` control is ever rendered by the form (and the emitted YAML never contains `resources`). (If a superadmin affordance was built, verify it appears only when `is_superadmin` is true.)
- [ ] **Step 7: Commit** `feat(ui): pipeline builder v2 — schema form panel + one-way edit-as-YAML toggle (RFC 026 P4)`.

---

### Task T7: Inputs/outputs pickers (storage-backed)

**sonnet.** **Files:**
- Modify: `ui/product/pages/pipeline-detail.js`.
- Verify: `node --check`.

**Interfaces:**
- Consumes: existing `getStorageCatalog(pid)` from `/ui/api.js` (RFC 005 — returns `{tables:[{namespace, name, current_snapshot_id}]}`, verified in docs/pipeline-api.md §Storage endpoints). Builds on T6's build panel.
- Produces: **inputs/outputs pickers** (spec §4.7 item 3, last clause: "plus inputs/outputs pickers backed by the existing storage endpoints"). In the build panel, below the config form, add two small pickers that help the user assemble the component's `inputs.tables[]` (existing tables to read) and confirm output bucket/table names. On "Insert as YAML" (T6), fold the chosen inputs/outputs into the emitted component block.

**Scope discipline:** inputs reference **existing** tables (a real picker from the catalog); outputs are **new** tables the run will create (so they can't be picked from an existing list — offer bucket + table name inputs + writeMode select instead, matching the CRD `outputs.tables[]` / `outputs.defaultBucket` shape). This mirrors the `full-pipeline` example's input/output vocabulary. Keep it minimal: one repeatable input-table picker (namespace/table dropdowns fed by the catalog) and a simple outputs block (bucket text + optional table name + writeMode enum `APPEND|FULL_LOAD` — the CRD enum from `OutputTableSpec` in `pkg/k8s/api/v1/pipeline_types.go`). For bucket/table labels rendered in these pickers, reuse the existing `.chip` / `.chip--bucket` / `.chip--table` classes (added by the merged runs-UX PR #23) rather than introducing new ones.

- [ ] **Step 1: Fetch the storage catalog once** at page load (guarded), for the inputs picker:

```js
let storageTables = [];
try { const sc = await getStorageCatalog(pid); if (aborted()) return; storageTables = (sc && sc.tables) || []; } catch { /* optional */ }
```

- [ ] **Step 2: Render inputs/outputs sub-panels** in the build panel (below `#builder-form`):

```html
<details class="builder-io"><summary>Inputs (read existing tables)</summary>
  <div id="io-inputs"></div>
  <button type="button" class="btn btn--ghost" id="add-input">+ add input table</button>
</details>
<details class="builder-io"><summary>Outputs (tables this component writes)</summary>
  <label class="field">Default bucket <input class="input" id="out-bucket" type="text" placeholder="raw"></label>
  <label class="field">Table name (optional) <input class="input" id="out-table" type="text"></label>
  <label class="field">Write mode
    <select class="input" id="out-writemode"><option value="">— default —</option><option>APPEND</option><option>FULL_LOAD</option></select>
  </label>
</details>
```

  Each "add input table" row: two selects (namespace → table) driven by `storageTables` (group by namespace like `query.js`'s `buildSchemaPane`), plus a remove button. Keep the rows in a small in-closure array; read them at insert time.

- [ ] **Step 3: Fold inputs/outputs into the emitted block.** Extend `componentBlockYAML` (T6) to accept optional `inputs`/`outputs` objects and emit `inputs:`/`outputs:` sub-keys when non-empty:
  - `inputs.tables: [{bucket, table}]` — the CRD field names are `bucket` + `table` (`InputTableSpec` in `pkg/k8s/api/v1/pipeline_types.go`); the storage catalog's `namespace` maps to `bucket`.
  - `outputs`: when a table name is given → `tables:[{name, bucket, writeMode}]` (**`bucket` is REQUIRED on `OutputTableSpec`** — `pkg/k8s/api/v1/pipeline_types.go`; take it from the `#out-bucket` field, which doubles as the table's bucket in that case); bucket-only → `{ defaultBucket, defaultWriteMode }`. Never emit a `tables` entry without `bucket`. Only emit what the user filled.

- [ ] **Step 4: Syntax check.** `node --check ui/product/pages/pipeline-detail.js`.
- [ ] **Step 5: Manual verification checklist (record in commit; run live in Task 9):**
  1. Build panel shows collapsible **Inputs** and **Outputs** sections.
  2. "+ add input table" adds a row with a namespace select (from the storage catalog) and a table select that narrows to that namespace; remove works.
  3. Fill an input table + an output bucket, "Insert as YAML" → the emitted block contains an `inputs: { tables: [...] }` and an `outputs:` block with the chosen bucket/writeMode; saving validates clean.
  4. With no storage tables in the project, the inputs picker shows an empty-but-usable state (no crash).
- [ ] **Step 6: Commit** `feat(ui): inputs/outputs pickers backed by storage catalog (RFC 026 P4)`.

---

### Task T8: Docs + README

**haiku.** **Files:**
- Modify: `docs/pipeline-api.md` (a short "Components" endpoint stub reference if Phase 2 didn't fully document it — check first), and add a UI-usage note.
- Modify: `README.md` and/or `docs/` UI section (wherever the UI is described) — document the catalog page, the builder, and the `x-datuplet-secret` picker behavior.
- Modify: `CLAUDE.md` "Browser UI" bullet if the new page/route warrants a mention (the bullet currently describes `ui/product/` — add components page + builder to the inventory succinctly).

- [ ] **Step 1:** Add a short "Registry-driven UI" subsection wherever the UI is user-documented: catalog at `/ui/components`, pipeline builder (pick component → form or YAML), that secret fields require a `$[key]` ref and are populated from the project secrets list, and that two-way form↔YAML sync is intentionally not provided ("edit as YAML" is one-way). Note the resources field is superadmin-only and YAML-only.
- [ ] **Step 2:** Ensure `docs/pipeline-api.md` documents `GET /api/v1/components` + `GET /api/v1/components/{name}` (if Phase 2 already did, skip; else add a stub matching the observed response shape from T1).
- [ ] **Step 3:** No code — verify docs render (markdown lint-free; internal links valid). Commit `docs(ui): document registry-driven catalog + builder + secret pickers (RFC 026 P4)`.

---

### Task T9: Phase gate

**sonnet.**

- [ ] **Step 1: Whole-tree syntax check.** `for f in $(git ls-files 'ui/product/**/*.js'); do node --check "$f" || echo "FAIL $f"; done` → no FAIL lines. (BSD/zsh-safe; `git ls-files` avoids `find` portability issues.)
- [ ] **Step 2: Scripted browser walkthrough (substitutes for e2e — there is no JS test framework and no controller/chart change).** Against a running stack (the repo's standard OrbStack dev deploy, or `PIPELINE_API_UI_DIR` pointing at `ui/product` per CLAUDE.md), drive the following and record pass/fail with the observed DOM in the PR body. Use the `575462609294:webapp-testing` Playwright skill if available, else a manual click-through with screenshots:
  1. **Catalog:** `/ui/components` lists components; filter narrows; a detail view shows the versions table + schema docs; deprecated badge present where applicable. (T3 checklist.)
  2. **Builder v1:** `/ui/pipelines/_new` shows the two-column builder; add-component dropdown populated; insert-snippet splices valid YAML; an invalid save renders inline findings with paths and does not persist; a valid save shows "Saved." (T5 checklist.)
  3. **Schema form (v2):** picking a component renders a form with correct control per type (text/number/checkbox/select/array-textarea/object-`<details>`/secret-select); required markers shown; "Insert as YAML" emits a block that saves clean; missing-required pre-check blocks insert with a findings message. (T4 + T6 checklists.)
  4. **Edit-as-YAML toggle:** confirm dialog appears; OK inserts current form values and removes the form; Cancel is a no-op. (T6.)
  5. **Secret picker:** a `x-datuplet-secret` field renders a `$[...]` select fed by `listSecrets`; a plaintext value is structurally impossible; "manage secrets…" links to `/ui/settings/secrets`. (T4/T6.)
  6. **Inputs/outputs pickers:** input-table rows populate from the storage catalog and fold into the emitted YAML; outputs bucket/writeMode fold in. (T7.)
  7. **Resource hiding:** as a non-superadmin, no `resources` control renders and no `resources` key is emitted. (T6.)
  8. **Regression:** existing pages (pipelines list, runs, storage, query, secrets) still load; nav shows the new Components item and highlights correctly.
- [ ] **Step 3: Cumulative Codex review.** `mcp__codex-cli__review {base: "<phase-start SHA>", model: "gpt-5.5", title: "RFC026 Phase 4"}` → zero CRITICAL/MAJOR. Fix via fixer subagents (sonnet, or opus for schema-form.js findings) and re-run until clean.
- [ ] **Step 4: Final PR update.** Push + append the Phase 4 summary to the single draft PR body (`gh pr edit` — Branching model): consumes Phase 2 catalog + Phase 1.5 secrets list + Phase 3 whoami flag + RFC 026 §7 findings contract; scripted-walkthrough log; Codex-gate log; note: no e2e-k8s (UI-only, no controller/chart/CRD change). With all phases landed, **mark the PR ready-for-review is Tomas's call — leave it draft.** Never push main.

---

## Self-review checklist (run before handing off)

- **Spec coverage:** §4.7 item 1 (catalog) → T3; item 2 (builder v1: picker + docs + inline findings) → T5; item 3 (schema form + inputs/outputs pickers + one-way YAML toggle) → T4 (renderer) + T6 (form panel + toggle) + T7 (pickers). §4.9 secret picker (Phase-4 half) → T4 secret field + T6 wiring. §7 findings shape → T2 `putPipelineYAML` + `lib/findings.js`, surfaced by T5/T6. §5 Phase 4 row (depends on 2, 1.5, 3-for-resources) → T1 preflight verifies all three. Non-goals honored: **no two-way form↔YAML sync** (T6 is emit-only; "edit as YAML" is one-way with confirm) — stated explicitly in T6.
- **No build step:** every task is vanilla ES modules + `node --check`; no npm/bundler/transpile anywhere. New modules imported via absolute `/ui/...` paths.
- **Interface consistency:** `getComponents()/getComponent(name)` (T2) used identically in T3/T5/T6; `buildSchemaForm(container, schemaStr, initialValue, opts) → {getValue,getErrors,destroy}` (T4) consumed by T6; `renderFindings(findings)` (T2) consumed by T5/T6; `putPipelineYAML(pid,name,yaml) → {ok,findings?}` (T2) consumed by T5; `schemaDocs(schemaStr)` exported from `components.js` (T3) reused by T5; `insertAtCursor`/`pickDocVersion`/`componentBlockYAML` shared helpers introduced once (T5/T6) and reused (T6/T7).
- **Model assignment:** sonnet for vanilla-JS pages/wiring (T1,T2,T3,T5,T6,T7,T9); **opus only** for the schema-form renderer (T4); haiku for docs (T8). Matches the roadmap-table guidance.
- **Parallelism:** only T3 ∥ T4 (disjoint: `pages/components.js` vs `lib/schema-form.js`, both dependent only on T2). Everything else is sequential due to shared edits to `pipeline-detail.js` (T5→T6→T7) or the composition dependency.
- **Anchors:** no line numbers into `pipeline-detail.js`/`api.js`/`app.js` (Phase-1/1.5-touched) — all by export/function name; T1 re-verifies. Line refs given only conceptually (e.g. `.badge` ~747 in style.css — a file no earlier phase rewrites, and even that is advisory).
- **Recon-verified facts baked in:** router = `routes` regex array + `NAV_ITEMS` in `app.js`; `render*(ctx)` export convention; `esc`/`api`/`putYAML` in `api.js`; storage catalog shape `{tables:[{namespace,name,current_snapshot_id}]}`; `.callout`/`.callout--warn` are the styled banners (`.banner` is **undefined** — do not use); `auth/me` returns `{id,email,mode}` today (Phase 3 must add the superadmin flag — a consumed interface T1 gates on); no `ui/product/lib/` dir yet (T2/T4 create it); `<details>` not yet used anywhere (introduced by T4/T7); no JS test framework (verification is `node --check` + scripted walkthrough).
