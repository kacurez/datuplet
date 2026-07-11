# RFC 024 — Deployment & Release Simplification — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task — one fresh subagent per task, orchestrator reviews + runs the Codex gate between tasks. Steps use checkbox (`- [ ]`) syntax for tracking.

> **✅ UNBLOCKED (2026-07-11).** RFC 025 (PR #25), RFC 026 (PR #24), and the
> TableCommit/local-exec deletion (PR #26) are all merged; the worktree is rebased
> onto public main @ `156e9a1`. This plan and its spec were **reconciled against
> that merged reality on 2026-07-11** — the former "Pre-execution revisit checklist"
> is now the [Actioned reconciliation log](#actioned-reconciliation-log) at the
> bottom (what changed and where). Both pre-dispatch chores are **done (2026-07-11)**:
> the `file:line` anchors were re-grepped against `156e9a1` (minor drift fixed; tasks
> also say "re-grep first" since future PRs drift them), and a Codex review of the
> reconciled plan ran — its **6 confirmed findings are folded in** (F1 install.sh CRD
> apply; F2 local `components.registry`; F3 `pandas-transform` ships image-less +
> tag mismatch → T6.3 broadened; F4 upgrade-e2e needs a post-T6.3 release; F5 T6.3
> DAG dependency; F6 hermetic fixture `/users`). Remaining: **maintainer green-light**.
>
> **⚠ One item is urgent and independent of executing this plan**: `main` cannot cut
> a release right now — RFC 026's `components.tag: v0.1.0` trips release.yml's
> pin-step guard (spec §2.2). T2.4 fixes it structurally; a one-line stopgap is in
> T2.4's note if a release must ship before Phase 2.

**Goal:** Land RFC 024's seven workstreams: one tested install/upgrade entrypoint, a release that publishes charts exactly as committed, CI that installs every release from the published artifacts, an upgrade-path e2e lane, version-sync checks, Renovate-driven dependency bumps, and a fail-closed e2e suite (W7 — the CI e2e gate is currently green-but-vacuous).

**Architecture:** All phases are implemented and committed on **one branch in this worktree** (`claude/modest-easley-f5173d`), landing as a **single final draft PR** — no per-phase branches or PRs (maintainer decision 2026-07-11). Phase 0 deletes the legacy `utils/deploy/k8s/` raw-manifest tree (now 14 files incl. a third CRD copy) and pins floating versions; Phase 1 introduces `scripts/install.sh`/`upgrade.sh` and converges Makefile/e2e/docs onto them; Phase 2 inverts image pinning (ghcr defaults + `tag → .Chart.AppVersion`, four service images — iceberg-job is gone) and replaces release-time sed with bump-before-tag + a tag-match guard; Phases 3–5 add the `release-verify` and `upgrade-e2e` workflows plus the `verify-versions` CI check; Phase 6 adds Renovate + the dependency-upgrade runbook **and T6.3 closes RFC 026's component-image publish gap**; Phase 7 (independent, run early) makes the e2e gate fail-closed. Spec: `docs/superpowers/specs/2026-07-04-rfc-024-deployment-simplification-design.md` (§6 workstreams, §8 rollout).

**Tech Stack:** bash (BSD/macOS-compatible, bash 3.2 — no associative arrays), helm 3.14, kubectl, GitHub Actions (kind via `helm/kind-action`), `yq` (mikefarah, preinstalled on ubuntu runners), chart-releaser (unchanged), Renovate.

## Global Constraints

- **Single branch, single PR (maintainer decision 2026-07-11).** ALL phases are implemented and committed on **this worktree's branch `claude/modest-easley-f5173d`** (worktree `modest-easley-f5173d`, where this plan lives). There are **no per-phase branches and no per-phase PRs**; one final **draft** PR to `main` is opened after every phase is complete (see "Final integration"). Never push `main`, never tag. (Repo rule: land via a feature-branch PR — this branch is that feature branch.)
- One logical commit per task still holds — they just all accumulate on the one branch, in phase/task order.
- `go build ./... && go test ./...` green **before every commit** (most tasks here are non-Go; still run it — cheap insurance).
- Chart/operator/controller changes gate on `make e2e-k8s` against an OrbStack cluster before the PR is marked ready (repo rule). Phases 0, 1, 2 touch charts/deploy path → gate applies.
- POC greenfield (RFC 024 §4): no transition shims, no dual paths — each phase replaces the old mechanism in the same PR (docs + Makefile + CI switch together).
- macOS/BSD shell: no GNU-only `sed -i`/grep flags in anything that runs locally (Makefile, scripts/). `perl -pi -e` is the portable in-place editor. CI-only workflow steps may use GNU tools.
- All bash scripts: `set -euo pipefail`, `bash -n` clean, bash-3.2-compatible (empty arrays expanded as `${arr[@]+"${arr[@]}"}`).
- Conventional commits (`feat:`, `fix:`, `docs:`, `test:`, `chore:`, `ci:`), one logical commit per task.
- Helm release names are fixed: `datuplet-operators`, `datuplet-infra`, `datuplet-app`, `datuplet-lakekeeper`. Published helm repo: `https://kacurez.github.io/datuplet` (chart refs `datuplet/<chart-name>`). Chart versions are lockstep — all four share the release version.
- Do not fix pre-existing quirks outside scope (e.g. `k8s-retry-*` deleting in `-n datuplet-e2e` while example manifests say `namespace: datuplet` — out of scope here and for RFC 026 alike).

## Harness notes (orchestrator contract)

- **Branch:** everything commits to **`claude/modest-easley-f5173d`** in this worktree. Phases are commit *groups* on that one branch, not separate branches. Recommended commit order: **Phase 7 first** (independent; touches only `tests/e2e/`, `scripts/e2e-port-forward.sh`, `e2e.yml`; makes the `make e2e-k8s` gate every later phase relies on actually meaningful — it's green-but-vacuous today, RFC §2.5/P8), then **0 → 6 sequential** (each builds on the previous; T6.3 lands right after Phase 2, before Phase 3). Tasks marked `Parallel: yes` are disjoint-file — a subagent may use an ephemeral sub-worktree for isolation, but its commits **merge back onto this branch** in task order (no long-lived side branches).
- **Per-task Codex gate** (after the subagent commits, before the next task): orchestrator runs `mcp__codex__codex` (read-only sandbox, `config: {model_reasoning_effort: "high"}`, cwd = this worktree) with a prompt to review the task's commit diff. Note: do **not** pass a `model` override — `gpt-5.2-codex` is rejected on a ChatGPT-account Codex; use the server default. (The `mcp__codex-cli__review` tool referenced in earlier drafts is gone — that server disconnected.) Acceptance: zero CRITICAL/MAJOR on the task diff. MINOR: fix if ≤5 min, else record in the final PR description. Findings → fixer subagent with the finding text verbatim; re-run the gate.
- **Phase gate** (last task of each phase): run the phase's tests (`make e2e-k8s` etc.) + a cumulative `mcp__codex__codex` review of the accumulated diff (read-only, high reasoning), fix CRITICAL/MAJOR, then **commit — do NOT open a PR**. The single PR is opened once, at the end (see "Final integration"). Because the diff accumulates on one branch, each gate's cumulative review naturally re-covers prior phases too.
- **Subagent dispatch:** give each subagent its full task text verbatim + the Global Constraints section; all work is on this one branch/worktree (say so), nothing else.

## Task index

| ID | Task | Model | Depends on | Parallel |
|----|------|-------|-----------|----------|
| T0.1 | Delete `utils/deploy/` tree; relocate sample secret; repoint comments | sonnet | — | with T0.2, T0.3 |
| T0.2 | Rewrite fossil Makefile `k8s-*` targets | sonnet | — | with T0.1, T0.3 |
| T0.3 | Docs truth sweep (install.md, pipeline-api.md) | sonnet | — | with T0.1, T0.2 |
| T0.4 | Pin kubectl + MinIO; commit Chart.lock; `dependency update`→`build` | sonnet | T0.2 (Makefile) | no |
| T0.5 | Workflow hygiene: release.yml concurrency + fga-version-check hardening | sonnet | — | with T0.1–T0.3 |
| T0.6 | Phase 0 gate: e2e-k8s, cumulative review, commit (no PR) | sonnet | T0.1–T0.5 | no |
| T1.1 | `scripts/install.sh` | **opus** | — | with T1.2 |
| T1.2 | `scripts/upgrade.sh` | **opus** | — | with T1.1 |
| T1.3 | Local values + Makefile wrappers converge on scripts | sonnet | T1.1, T1.2 | no |
| T1.4 | Docs converge on install.sh/upgrade.sh | sonnet | T1.3 | no |
| T1.5 | Phase 1 gate: deploy-local + e2e-k8s, review, commit (no PR) | sonnet | T1.1–T1.4 | no |
| T2.1 | Image helper + ghcr/appVersion defaults (app + infra charts) | sonnet | — | with T2.4 |
| T2.2 | pullPolicy flip Always→IfNotPresent + doc-note removal | sonnet | T2.1 | no |
| T2.3 | Complete image overrides in e2e + local values files | sonnet | T2.2 | no |
| T2.4 | release.yml de-sed: verify-tag guard + `make bump-version` | **opus** | — | with T2.1 |
| T2.5 | Phase 2 gate: template assertions, e2e-k8s, review, commit (no PR) | sonnet | T2.1–T2.4 | no |
| T3.1 | Parameterize `k8s-smoke` (SMOKE_URL) | haiku | — | with T3.2 |
| T3.2 | Verify pipeline template + `run-pipeline.sh` REST driver | sonnet | — | with T3.1 |
| T3.3 | `.github/workflows/release-verify.yml` | **opus** | T3.1, T3.2 | no |
| T3.4 | Phase 3 gate: dispatch-run against latest release, docs, commit (no PR) | sonnet | T3.3 | no |
| T4.1 | `scripts/verify-versions.sh` + pr.yml wiring | sonnet | — | no |
| T4.2 | Phase 4 gate: negative tests, review, commit (no PR) | sonnet | T4.1 | no |
| T5.1 | `.github/workflows/upgrade-e2e.yml` | **opus** | — | no |
| T5.2 | known-limitations: N-1→N policy + snapshot rec; gate; commit (no PR) | sonnet | T5.1 | no |
| T6.1 | `renovate.json` | sonnet | — | with T6.2 |
| T6.2 | `docs/dependency-upgrades.md` runbook; gate; commit (no PR) | sonnet | — | with T6.1 |
| T6.3 | Close component-image publish gap (`components.tag` → published tag + pandas-transform) | sonnet | T2.1, T2.4 | land after Phase 2, before Phase 3 (blocks W3/W5) |
| T7.1 | Generalize `PreCheck` (drop the OrbStack context sniff) | sonnet | — | no |
| T7.2 | Fail-closed CI mode: `E2E_REQUIRE=1` + `SkipOrFail` sweep | **opus** | T7.1 | no |
| T7.3 | Supervised port-forwards + `go test -json` summary in CI | sonnet | — | with T7.4 |
| T7.4 | Hermetic HTTP fixture (in-cluster, replaces jsonplaceholder) | sonnet | — | with T7.3 |
| T7.5 | Revive FGA-matrix tests + enable remote-CLI in CI | **opus** | T7.2 | no |
| T7.6 | Phase 7 gate: CI run w/ zero unexpected skips, review, commit (no PR) | sonnet | T7.1–T7.5 | no |

Task DAG (arrows = "blocks"):

```
P0: {T0.1, T0.2, T0.3, T0.5} ; T0.2 → T0.4 ; {T0.1–T0.5} → T0.6
P1: {T1.1, T1.2} → T1.3 → T1.4 → T1.5
P2: {T2.1, T2.4} ; T2.1 → T2.2 → T2.3 ; {T2.3, T2.4} → T2.5
P3: {T3.1, T3.2} → T3.3 → T3.4
P4: T4.1 → T4.2
P5: T5.1 → T5.2
P6: {T6.1, T6.2} → phase gate (inside T6.2) ; T6.3 depends on T2.1+T2.4 (reuses the image helper / bump-version) and BLOCKS P3 (W3) + P5 (W5) going green — land it right after Phase 2, before Phase 3
P7: T7.1 → T7.2 → T7.5 ; {T7.3, T7.4} parallel ; {T7.1–T7.5} → T7.6
    (P7 is independent of P0–P6 and recommended to run FIRST — see Harness notes)
```

---

## Phase 0 — Deploy-code hygiene

### Task T0.1: Delete `utils/deploy/` tree; repoint comments

> **Reconciled 2026-07-11 against `156e9a1`.** RFC 026 already deleted
> `utils/deploy/k8s/rbac/sample-pipeline-secret.yaml` (secretsRef removed) — the
> earlier "relocate the keeper" step is gone. The tree is now **14 files including
> a third CRD copy** (`datuplet.io_componentdefinitions.yaml`). `docs/secrets.md`
> no longer references the tree (RFC 026 rewrote it). Reference line numbers below
> re-verified against merged main.

**Files:**
- Delete: `utils/deploy/` (entire tree — 14 files, incl. all three CRD copies)
- Modify (all are stale comments/usage-text, verified present): `cmd/pipeline-api/main.go:539`, `tests/e2e/framework/scenario.go:45`, `tests/e2e/framework/pipeline_api_client.go:28`, `charts/datuplet-app/templates/pipeline-operator/rbac.yaml:7`, `pkg/k8s/controllers/pipelinerun_controller.go:289`, `docs/pipeline-api.md:536`

**Interfaces:**
- Produces: zero `utils/deploy` references anywhere outside `docs/superpowers/` — T4.1's `verify-versions.sh` check and T0.2's Makefile rewrite assume the tree is gone.

- [ ] **Step 1: Delete the fossil.** `git rm -r utils/deploy`

- [ ] **Step 2: Repoint the references** (re-grep first — `grep -rn 'utils/deploy' --include='*.go' --include='*.md' --include='*.yaml' . | grep -v superpowers | grep -v '^Makefile'` — line numbers drift as upstream moves):
  - `cmd/pipeline-api/main.go` usage text: `(used by the CronJob — see utils/deploy/k8s/pipeline-api-reaper.yaml)` → `(used by the CronJob — see charts/datuplet-app/templates/reaper/cronjob.yaml)`.
  - `docs/pipeline-api.md` reaper section: `(utils/deploy/k8s/pipeline-api-reaper.yaml)` → `(charts/datuplet-app/templates/reaper/cronjob.yaml)`.
  - `tests/e2e/framework/scenario.go:45`: `(matches utils/deploy/k8s/minio.yaml)` → `(matches the datuplet-infra chart's MinIO values + tests/e2e/values-infra.yaml)`.
  - `tests/e2e/framework/pipeline_api_client.go:28`: `// Matches utils/deploy/k8s/pipeline-api.yaml.` → `// Matches the datuplet-app chart's pipeline-api NodePort (pipelineApi.service.nodePort).`
  - `charts/datuplet-app/templates/pipeline-operator/rbac.yaml:7`: drop the `(mirrors utils/deploy/k8s/rbac/pipeline-operator-rbac.yaml)` parenthetical; keep the rest.
  - `pkg/k8s/controllers/pipelinerun_controller.go:289`: the comment `utils/deploy/k8s/rbac/); there is no controller-gen …` → reword to reference `charts/datuplet-app/crds/` as the manually-maintained CRD home (keep the "no controller-gen" point).

- [ ] **Step 3: Verify no stragglers**

Run: `grep -rn "utils/deploy" --exclude-dir=.git --exclude-dir=superpowers . ; echo "exit=$?"`
Expected: no matches, `exit=1`. (Makefile hits remain until T0.2 if running out of order — the phase gate re-checks.)

- [ ] **Step 4: Build + commit**

```bash
go build ./... && go test ./tests/e2e/framework/ ./cmd/... -count=1
git add -A
git commit -m "chore(deploy): delete legacy utils/deploy raw-manifest tree (RFC 024 P0)"
```

### Task T0.2: Rewrite fossil Makefile `k8s-*` targets

**Files:**
- Modify: `Makefile` — targets `k8s-reload-crds` (~line 281), `k8s-rebuild-operators` (~289), `k8s-smoke` "k8s-up" hint (~124) — verified against `156e9a1`; find by target name

**Interfaces:**
- Produces: `k8s-reload-crds` applies `charts/datuplet-app/crds/`; `k8s-rebuild-operators` no longer applies raw manifests; a `K8S_NS ?= datuplet` variable used by both. `k8s-retry-*` targets keep depending on `k8s-reload-crds` unchanged.

- [ ] **Step 1: Add the namespace variable** near the other variable definitions at the top of the Makefile:

```makefile
# Namespace for the k8s-* developer-loop targets (deploy-local installs
# into `datuplet`; override for e2e clusters: make k8s-reload-crds K8S_NS=datuplet-e2e).
K8S_NS ?= datuplet
```

- [ ] **Step 2: Replace `k8s-reload-crds`** (the chart's `crds/` is the single canonical CRD source — CLAUDE.md). Applying the **directory** (not per-file) matters more now: `charts/datuplet-app/crds/` holds **three** CRDs (`pipelines`, `pipelineruns`, `componentdefinitions`) — the fossil target this replaces enumerated only the first two by path, so it silently never reloaded RFC 026's registry CRD:

```makefile
k8s-reload-crds: ## Apply the chart's CRD manifests to the cluster
	@echo "Reloading CRDs from charts/datuplet-app/crds/ ..."
	kubectl apply --server-side --force-conflicts \
	  --field-manager=datuplet-dev -f charts/datuplet-app/crds/
	@echo "CRDs reloaded successfully"
```

- [ ] **Step 3: Replace `k8s-rebuild-operators`** — build + restart only; helm owns manifests. The deployment name is **not** release-prefixed — verified against `156e9a1`: `charts/datuplet-app/templates/pipeline-operator/deployment.yaml:23` renders a bare `name: pipeline-operator`:

```makefile
k8s-rebuild-operators: docker-build-operators k8s-reload-crds ## Rebuild operator image + reload CRDs + rollout restart
	kubectl rollout restart deployment/pipeline-operator -n $(K8S_NS)
	kubectl rollout status deployment/pipeline-operator -n $(K8S_NS) --timeout=60s
	@echo "Operators rebuilt and ready!"
```

- [ ] **Step 4: Fix the dead hint** in `k8s-smoke` (~line 106): `Run 'make k8s-up' first.` → `Run 'make deploy-local' first.`

- [ ] **Step 5: Verify + commit**

Run: `make -n k8s-reload-crds k8s-rebuild-operators | head -20` — expected: commands reference `charts/datuplet-app/crds/`, no `utils/deploy`.

```bash
git add Makefile
git commit -m "chore(make): k8s dev targets use chart CRDs + rollout restart, drop raw-manifest applies (RFC 024 P0)"
```

### Task T0.3: Docs truth sweep

**Files:**
- Modify: `docs/install.md` (lines ~20-27, ~60-62, ~130-138), `docs/pipeline-api.md` (lines ~276, ~362)

- [ ] **Step 1: install.md helm-repo paragraph** (lines 20–27). The repo has released tags through `v0.7.1`; replace the "Once 0.1.0 is released… Until then" wording with:

```markdown
Datuplet charts are published to the public helm repo on every release tag:

​```bash
helm repo add datuplet https://kacurez.github.io/datuplet
helm repo update
​```

The commands below install from a local clone (development default). To
install a published version instead, replace `charts/<name>` with
`datuplet/<name> --version <X.Y.Z>` in each command.
```

- [ ] **Step 2: install.md prerequisites** (line ~62): replace `https://charts.bitnami.com/bitnami (MinIO subchart)` with `https://charts.min.io/ (MinIO subchart)`.

- [ ] **Step 3: install.md local-development section** (~line 158): delete the claim that `deploy-local-helm` applies "values overrides from `tests/local/values-*.yaml`" (the directory does not exist; Phase 1 introduces real override files — the doc gets rewritten again then). Interim wording: "It runs the four helm upgrades in phase order followed by `scripts/register.sh`."

- [ ] **Step 4: pipeline-api.md**: line ~276 `./undeploy-local.sh --delete-namespace` → `make undeploy-local` (same sentence structure); line ~362 `(utils/deploy/k8s/pipeline-api-reaper.yaml)` → `(charts/datuplet-app/templates/reaper/cronjob.yaml)`.

- [ ] **Step 5: Verify + commit**

Run: `grep -rn "0.1.0 is released\|tests/local\|charts.bitnami.com\|undeploy-local.sh\|pipeline-api-reaper.yaml" docs/*.md` — expected: no output.

```bash
git add docs/
git commit -m "docs: fix stale install/pipeline-api claims (helm repo live, MinIO repo URL, dead paths) (RFC 024 P0)"
```

### Task T0.4: Pin kubectl + MinIO; commit Chart.lock; `dependency update` → `build`

**Files:**
- Modify (line numbers verified against `156e9a1`; re-grep before editing): `charts/datuplet-app/values.yaml:276-278` (kubectl), `charts/datuplet-infra/values.yaml:22-25`, `charts/datuplet-lakekeeper/values.yaml:101,162`, `charts/datuplet-infra/Chart.yaml` (minio pin, `~line 21`), `.gitignore` (the four `charts/*/Chart.lock` ignore lines — re-grep), `Makefile` (dep lines in `deploy-local-helm` + `e2e-k8s-deploy`), `.github/workflows/pr.yml` (helm `dependency update` step), `docs/install.md` + `docs/quickstart-kind.md` (`helm dependency` blocks)
- Create: `charts/datuplet-operators/Chart.lock`, `charts/datuplet-infra/Chart.lock`, `charts/datuplet-lakekeeper/Chart.lock` (generated)

**Interfaces:**
- Produces: committed `Chart.lock` for the three dependency-bearing charts (datuplet-app has no `dependencies:` block — no lock), `helm dependency build` as the documented/scripted command everywhere. T4.1's lock-consistency check depends on this.

- [ ] **Step 1: Pin the kubectl helper image.** Bitnami retired versioned `bitnami/kubectl` tags (only `:latest` + `bitnamilegacy/kubectl:X.Y` remain — see the comment at `charts/datuplet-infra/values.yaml:22`). Verify the legacy pin is multi-arch before adopting:

Run: `docker manifest inspect docker.io/bitnamilegacy/kubectl:1.31 | grep -c 'arm64\|amd64'`
Expected: ≥2. If arm64 is missing, use `alpine/k8s:1.31.12` instead (multi-arch kubectl+helm toolbox image) — apply whichever passes to **all four sites**: `charts/datuplet-app/values.yaml:267`, `charts/datuplet-infra/values.yaml:25` (update the stale explanatory comments at both), `charts/datuplet-lakekeeper/values.yaml:101` and `:162`.

- [ ] **Step 2: Pin MinIO exactly.** In `charts/datuplet-infra/Chart.yaml` change `version: "~5.4.0"` to the exact currently-resolved version: run `helm dependency update charts/datuplet-infra` once, read the resolved version from the generated `Chart.lock` (`grep -A1 'name: minio' charts/datuplet-infra/Chart.lock`), and write that exact string (e.g. `version: "5.4.10"`).

- [ ] **Step 3: Commit Chart.lock files.** Remove `.gitignore` lines 113–118 (the four `charts/*/Chart.lock` entries and their comment — keep line 111 `charts/*/charts/`, tarballs stay ignored). Then:

```bash
for c in datuplet-operators datuplet-infra datuplet-lakekeeper; do
  helm dependency update "charts/$c"
done
git add charts/*/Chart.lock .gitignore
```

Note: `charts/datuplet-app` has no dependencies — `helm dependency update` there is a no-op and must NOT be given a lock file.

- [ ] **Step 4: Switch `update` → `build`.** Replace `helm dependency update charts/…` with `helm dependency build charts/…` in: `Makefile` `deploy-local-helm` (4 lines) and `e2e-k8s-deploy` (4 lines), `.github/workflows/pr.yml:72-73` (step name too), `docs/install.md:67-71`, `docs/quickstart-kind.md:62-65`. (`build` installs the exact locked versions and fails on lock/manifest drift; `update` re-resolves ranges.)

- [ ] **Step 5: Verify + commit**

Run: `for c in datuplet-operators datuplet-infra datuplet-lakekeeper; do helm dependency build charts/$c || echo "BUILD FAILED $c"; done` — expected: all succeed. `helm lint charts/datuplet-infra` — expected: no errors.

```bash
git add -A
git commit -m "chore(charts): pin kubectl helper + MinIO exactly, commit Chart.lock, switch to dependency build (RFC 024 P0)"
```

### Task T0.5: Workflow hygiene — release concurrency + fga-version-check hardening

**Files:**
- Modify: `.github/workflows/release.yml` (top level, after the `on:` block), `.github/workflows/fga-version-check.yml`

**Interfaces:**
- Produces: serialized release runs (T2.4's verify-tag guard and T3.3's `workflow_run` trigger assume one release run at a time per tag); a strict must-increase FGA version check (complements T4.1's cross-chart equality check — this one guards the *bump*, that one guards the *sync*).

- [ ] **Step 1: Serialize releases.** In `.github/workflows/release.yml`, insert between the `on:` block and `permissions:`:

```yaml
# Serialize release runs: two tags pushed close together would otherwise run
# chart-releaser concurrently against gh-pages and one git push loses the
# race. No cancel-in-progress — a started release must run to completion.
concurrency:
  group: release
  cancel-in-progress: false
```

- [ ] **Step 2: fga-version-check checkout bump.** In `.github/workflows/fga-version-check.yml` line 13: `actions/checkout@v4` → `actions/checkout@v6` (keep `fetch-depth: 0`).

- [ ] **Step 3: must-increase comparison.** Replace the final `if`/`else` block (lines ~33-43) with:

```bash
          if [ -z "$old_version" ]; then
            echo "✓ DSL changed; no prior fgaModel.version on $BASE — accepting $new_version"
          elif [ "$old_version" = "$new_version" ]; then
            cat <<EOF >&2
          ✗ DSL files changed but fgaModel.version did not bump.

          You modified one or more files in $FGA_DIR. Bump $VALUES :: fgaModel.version
          to indicate the model is being updated. See docs/fga-model-upgrades.md.
          EOF
            exit 1
          elif [ "$(printf '%s\n%s\n' "$old_version" "$new_version" | sort -V | tail -1)" != "$new_version" ]; then
            echo "✗ fgaModel.version moved backwards or reuses an older number: $old_version → $new_version" >&2
            exit 1
          else
            echo "✓ DSL changed AND fgaModel.version bumped: $old_version → $new_version"
          fi
```

- [ ] **Step 4: Verify + commit**

Run: `python3 -c "import yaml; [yaml.safe_load(open(f)) for f in ['.github/workflows/release.yml','.github/workflows/fga-version-check.yml']]; print('ok')"` — expected: `ok`.

```bash
git add .github/workflows/release.yml .github/workflows/fga-version-check.yml
git commit -m "ci: serialize release runs + harden fga-version-check (RFC 024 P0)"
```

### Task T0.6: Phase 0 gate

- [ ] **Step 1:** `make tidy && go build ./... && go test ./... -count=1` — green.
- [ ] **Step 2:** `make e2e-k8s` against an OrbStack cluster (chart values changed — repo rule).
- [ ] **Step 3:** Re-run T0.1 Step 4's grep repo-wide — no `utils/deploy` references.
- [ ] **Step 4:** Orchestrator: cumulative Codex review `{base: "main", model: "gpt-5.5", title: "RFC024 Phase 0"}` → zero CRITICAL/MAJOR.
- [ ] **Step 5:** Ensure all Phase 0 commits are on `claude/modest-easley-f5173d`. **No PR** — the single draft PR is opened at Final integration after every phase completes.

---

## Phase 1 — install.sh / upgrade.sh

### Task T1.1: `scripts/install.sh`

**Files:**
- Create: `scripts/install.sh` (mode 0755)

**Interfaces:**
- Produces (T1.3 wrappers, T3.3 release-verify, T5.1 upgrade-e2e call these exact flags): `--namespace NS`, `--from-source` (default) / `--from-repo`, `--version vX.Y.Z|X.Y.Z` (leading `v` stripped), `-f FILE` (all charts, repeatable), `-f-operators|-f-infra|-f-app|-f-lakekeeper FILE` (per-chart, repeatable), `--register-mode exec|job`, `--skip-register`, `--preflight-only`, `--dry-run`, trailing `-- ARGS…` passed to `register.sh` verbatim. Env override: `DATUPLET_HELM_REPO` (default `https://kacurez.github.io/datuplet`).

- [ ] **Step 1: Write the script** (complete file):

```bash
#!/usr/bin/env bash
# scripts/install.sh — RFC 024 W1: the single tested install entrypoint.
#
# 5-phase install: datuplet-operators → datuplet-infra → datuplet-app →
# datuplet-lakekeeper → register.sh. Idempotent: helm upgrade --install
# throughout; register.sh subcommands are idempotent by design.
#
# From a repo checkout (default):   scripts/install.sh --namespace datuplet
# From the published helm repo:     scripts/install.sh --from-repo --version v0.8.0
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
HELM_REPO_URL="${DATUPLET_HELM_REPO:-https://kacurez.github.io/datuplet}"
CHARTS="datuplet-operators datuplet-infra datuplet-app datuplet-lakekeeper"

NAMESPACE=datuplet
MODE=from-source
VERSION=""
REGISTER_MODE=exec
SKIP_REGISTER=false
PREFLIGHT_ONLY=false
DRY_RUN=false
COMMON_VALUES=() OPERATORS_VALUES=() INFRA_VALUES=() APP_VALUES=() LAKEKEEPER_VALUES=()
REGISTER_ARGS=()

usage() { sed -n '3,10p' "$0"; cat <<'EOF'

Options:
  --namespace NS          Target namespace (default: datuplet)
  --from-source           Install ./charts from this checkout (default)
  --from-repo             Install published charts from the helm repo
  --version X.Y.Z|vX.Y.Z  Chart version (required with --from-repo)
  -f FILE                 Values file applied to every chart (repeatable)
  -f-operators FILE       Values for datuplet-operators only (repeatable)
  -f-infra FILE           Values for datuplet-infra only (repeatable)
  -f-app FILE             Values for datuplet-app only (repeatable)
  -f-lakekeeper FILE      Values for datuplet-lakekeeper only (repeatable)
  --register-mode M       exec|job — forwarded to register.sh --mode (default: exec)
  --skip-register         Stop after the four helm installs
  --preflight-only        Run preflight checks, then exit 0
  --dry-run               Print every mutating command instead of executing
  -- ARGS...              Everything after -- is forwarded to register.sh
EOF
}

die() { echo "ERROR: $*" >&2; exit 1; }
run() { if $DRY_RUN; then echo "+ $*"; else "$@"; fi; }

while [ $# -gt 0 ]; do
  case "$1" in
    --namespace)     NAMESPACE=$2; shift 2 ;;
    --from-source)   MODE=from-source; shift ;;
    --from-repo)     MODE=from-repo; shift ;;
    --version)       VERSION=${2#v}; shift 2 ;;
    -f)              COMMON_VALUES+=("$2"); shift 2 ;;
    -f-operators)    OPERATORS_VALUES+=("$2"); shift 2 ;;
    -f-infra)        INFRA_VALUES+=("$2"); shift 2 ;;
    -f-app)          APP_VALUES+=("$2"); shift 2 ;;
    -f-lakekeeper)   LAKEKEEPER_VALUES+=("$2"); shift 2 ;;
    --register-mode) REGISTER_MODE=$2; shift 2 ;;
    --skip-register) SKIP_REGISTER=true; shift ;;
    --preflight-only) PREFLIGHT_ONLY=true; shift ;;
    --dry-run)       DRY_RUN=true; shift ;;
    -h|--help)       usage; exit 0 ;;
    --)              shift; REGISTER_ARGS=("$@"); break ;;
    *)               usage >&2; die "unknown flag: $1" ;;
  esac
done

preflight() {
  command -v kubectl >/dev/null 2>&1 || die "kubectl not found on PATH"
  command -v helm    >/dev/null 2>&1 || die "helm not found on PATH"
  local hv; hv=$(helm version --template '{{.Version}}')
  case "$hv" in
    v3.1[4-9].*|v3.[2-9][0-9].*|v[4-9].*) : ;;
    *) die "helm >= 3.14 required (found $hv)" ;;
  esac
  kubectl version >/dev/null 2>&1 || die "Kubernetes cluster unreachable (kubectl version failed)"
  local minor
  minor=$(kubectl version -o json 2>/dev/null \
    | sed -n 's/.*"minor": *"\([0-9]*\)[^"]*".*/\1/p' | tail -1)
  [ "${minor:-0}" -ge 28 ] || die "Kubernetes >= 1.28 required (server minor: ${minor:-unknown})"
  kubectl get storageclass -o name 2>/dev/null | grep -q . \
    || die "no StorageClass in the cluster (CNPG Postgres + MinIO need PVCs)"
  if [ "$MODE" = from-repo ]; then
    [ -n "$VERSION" ] || die "--from-repo requires --version"
    curl -fsS "$HELM_REPO_URL/index.yaml" >/dev/null \
      || die "helm repo unreachable: $HELM_REPO_URL"
  else
    for c in $CHARTS; do
      run helm dependency build "$REPO_ROOT/charts/$c" \
        || die "helm dependency build failed for $c (Chart.lock drift?)"
    done
  fi
  local st
  for c in $CHARTS; do
    st=$(helm status -n "$NAMESPACE" "$c" -o json 2>/dev/null \
      | sed -n 's/.*"status":"\([^"]*\)".*/\1/p' | head -1) || true
    case "${st:-}" in
      pending-*|failed)
        die "release $c is '$st' — resolve first (helm rollback/uninstall $c -n $NAMESPACE)" ;;
    esac
  done
  echo "preflight OK (namespace=$NAMESPACE mode=$MODE${VERSION:+ version=$VERSION})"
}

chart_values() {  # $1 = chart name → echoes -f args (common first, then per-chart)
  local f
  for f in ${COMMON_VALUES[@]+"${COMMON_VALUES[@]}"}; do printf -- '-f\n%s\n' "$f"; done
  case "$1" in
    datuplet-operators)  for f in ${OPERATORS_VALUES[@]+"${OPERATORS_VALUES[@]}"};  do printf -- '-f\n%s\n' "$f"; done ;;
    datuplet-infra)      for f in ${INFRA_VALUES[@]+"${INFRA_VALUES[@]}"};          do printf -- '-f\n%s\n' "$f"; done ;;
    datuplet-app)        for f in ${APP_VALUES[@]+"${APP_VALUES[@]}"};              do printf -- '-f\n%s\n' "$f"; done ;;
    datuplet-lakekeeper) for f in ${LAKEKEEPER_VALUES[@]+"${LAKEKEEPER_VALUES[@]}"};do printf -- '-f\n%s\n' "$f"; done ;;
  esac
}

install_chart() {  # $1 = chart name; $2.. = extra helm flags
  local chart=$1; shift
  local src="$REPO_ROOT/charts/$chart" vflags=()
  if [ "$MODE" = from-repo ]; then src="datuplet/$chart"; vflags=(--version "$VERSION"); fi
  local vals=()
  while IFS= read -r line; do [ -n "$line" ] && vals+=("$line"); done < <(chart_values "$chart")
  run helm upgrade --install "$chart" "$src" -n "$NAMESPACE" \
    ${vflags[@]+"${vflags[@]}"} ${vals[@]+"${vals[@]}"} "$@"
}

preflight
$PREFLIGHT_ONLY && exit 0

if [ "$MODE" = from-repo ]; then
  run helm repo add datuplet "$HELM_REPO_URL" --force-update
  run helm repo update datuplet
fi

# apply_crds <chart> — helm applies crds/ on FIRST install but skips them on
# every subsequent upgrade; install.sh uses `helm upgrade --install`, so on a
# reused cluster (local re-deploy, or a CRD schema bump) the CRDs would go
# stale. Apply them explicitly first — mirrors the current Makefile's
# `kubectl apply -f charts/datuplet-app/crds/` step (which the Makefile
# wrappers dropped when they moved to install.sh — F1 of the 2026-07-11 codex
# review). Same source logic as install_chart.
apply_crds() {
  local chart=$1
  if [ "$MODE" = from-source ]; then
    run kubectl apply --server-side --force-conflicts \
      --field-manager=datuplet-install -f "$REPO_ROOT/charts/$chart/crds/"
  elif $DRY_RUN; then
    echo "+ helm show crds datuplet/$chart --version $VERSION | kubectl apply --server-side -f -"
  else
    helm show crds "datuplet/$chart" --version "$VERSION" \
      | kubectl apply --server-side --force-conflicts --field-manager=datuplet-install -f -
  fi
}

# RFC 015 5-phase install; strict order (see docs/install.md phase table).
# CRD-bearing charts (operators = CNPG CRDs, app = Pipeline/PipelineRun/
# ComponentDefinition) get an explicit CRD apply before their helm step.
apply_crds datuplet-operators
install_chart datuplet-operators  --create-namespace --wait --timeout 5m
install_chart datuplet-infra      --wait --wait-for-jobs --timeout 10m
apply_crds datuplet-app
install_chart datuplet-app        --wait --wait-for-jobs --timeout 10m
install_chart datuplet-lakekeeper --wait --wait-for-jobs --timeout 10m

if ! $SKIP_REGISTER; then
  run "$SCRIPT_DIR/register.sh" --namespace "$NAMESPACE" --mode "$REGISTER_MODE" \
    ${REGISTER_ARGS[@]+"${REGISTER_ARGS[@]}"}
fi
echo "OK: datuplet installed in namespace '$NAMESPACE'"
```

Note: `apply_crds` is duplicated in `upgrade.sh` (T1.2) — extract it into a
tiny sourced `scripts/lib/crds.sh` if you prefer DRY, but two ~10-line copies
is acceptable for POC. The `--from-repo` path needs `helm repo add` to have run
first (it has, above).

- [ ] **Step 2: Syntax + dry-run checks**

Run: `bash -n scripts/install.sh && chmod +x scripts/install.sh` — expected: silent.
Run: `scripts/install.sh --dry-run --skip-register 2>&1 | grep -c '+ helm upgrade --install'` — expected: `4` (needs a reachable cluster for preflight; on a dev machine with OrbStack this passes).
Run: `scripts/install.sh --from-repo --dry-run 2>&1; echo $?` — expected: `ERROR: --from-repo requires --version`, exit 1.

- [ ] **Step 3: Commit**

```bash
git add scripts/install.sh
git commit -m "feat(deploy): install.sh — single tested 5-phase install entrypoint (RFC 024 W1)"
```

### Task T1.2: `scripts/upgrade.sh`

**Files:**
- Create: `scripts/upgrade.sh` (mode 0755)

**Interfaces:**
- Consumes: same helm/values conventions as T1.1.
- Produces (T3.3 + T5.1 call these): `--namespace NS`, `--phase operators|infra|app|lakekeeper|all` (default `app`), `--from-source|--from-repo --version V`, same `-f`/`-f-<chart>` flags as install.sh, `--dry-run`. CRD apply is phase-aware: `operators` → `charts/datuplet-operators/crds/`, `app` → `charts/datuplet-app/crds/`, `all` → both, `--from-repo` → `helm show crds datuplet/<chart> --version V`. Failure policy: forward-only, **no `--atomic`**, recovery = re-run (RFC 024 §6.1).

- [ ] **Step 1: Write the script** (complete file):

```bash
#!/usr/bin/env bash
# scripts/upgrade.sh — RFC 024 W1: phase-aware upgrade with explicit CRD apply.
#
# helm NEVER upgrades CRDs shipped in a chart's crds/ directory; this script
# applies them (server-side) before upgrading the corresponding release.
# Forward-only by design: no --atomic, no helm rollback (hook Jobs, CRD
# applies and DB migrations sit outside helm's rollback scope). Recovery
# from a mid-flight failure: fix the cause, re-run the same command.
#
#   scripts/upgrade.sh                             # common case: app phase, from source
#   scripts/upgrade.sh --phase all --from-repo --version v0.9.0
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
HELM_REPO_URL="${DATUPLET_HELM_REPO:-https://kacurez.github.io/datuplet}"
FIELD_MANAGER=datuplet-upgrade

NAMESPACE=datuplet
PHASE=app
MODE=from-source
VERSION=""
DRY_RUN=false
COMMON_VALUES=() OPERATORS_VALUES=() INFRA_VALUES=() APP_VALUES=() LAKEKEEPER_VALUES=()

usage() { sed -n '3,13p' "$0"; cat <<'EOF'

Options:
  --namespace NS       Target namespace (default: datuplet)
  --phase P            operators|infra|app|lakekeeper|all (default: app)
  --from-source        Upgrade from ./charts in this checkout (default)
  --from-repo          Upgrade from the published helm repo
  --version X.Y.Z      Chart version (required with --from-repo; leading v stripped)
  -f FILE              Values file applied to every upgraded chart (repeatable)
  -f-operators FILE    Values for datuplet-operators only (repeatable)
  -f-infra FILE        Values for datuplet-infra only (repeatable)
  -f-app FILE          Values for datuplet-app only (repeatable)
  -f-lakekeeper FILE   Values for datuplet-lakekeeper only (repeatable)
  --dry-run            Print every mutating command instead of executing
EOF
}

die() { echo "ERROR: $*" >&2; exit 1; }
run() { if $DRY_RUN; then echo "+ $*"; else "$@"; fi; }

while [ $# -gt 0 ]; do
  case "$1" in
    --namespace)   NAMESPACE=$2; shift 2 ;;
    --phase)       PHASE=$2; shift 2 ;;
    --from-source) MODE=from-source; shift ;;
    --from-repo)   MODE=from-repo; shift ;;
    --version)     VERSION=${2#v}; shift 2 ;;
    -f)            COMMON_VALUES+=("$2"); shift 2 ;;
    -f-operators)  OPERATORS_VALUES+=("$2"); shift 2 ;;
    -f-infra)      INFRA_VALUES+=("$2"); shift 2 ;;
    -f-app)        APP_VALUES+=("$2"); shift 2 ;;
    -f-lakekeeper) LAKEKEEPER_VALUES+=("$2"); shift 2 ;;
    --dry-run)     DRY_RUN=true; shift ;;
    -h|--help)     usage; exit 0 ;;
    *)             usage >&2; die "unknown flag: $1" ;;
  esac
done

case "$PHASE" in operators|infra|app|lakekeeper|all) : ;; *) die "invalid --phase: $PHASE" ;; esac
if [ "$MODE" = from-repo ]; then
  [ -n "$VERSION" ] || die "--from-repo requires --version"
  run helm repo add datuplet "$HELM_REPO_URL" --force-update
  run helm repo update datuplet
fi

apply_crds() {  # $1 = chart that ships a crds/ dir
  local chart=$1
  echo "--- applying CRDs for $chart (helm skips crds/ on upgrade)"
  if [ "$MODE" = from-source ]; then
    run kubectl apply --server-side --force-conflicts \
      --field-manager="$FIELD_MANAGER" -f "$REPO_ROOT/charts/$chart/crds/"
  elif $DRY_RUN; then
    echo "+ helm show crds datuplet/$chart --version $VERSION | kubectl apply --server-side --force-conflicts --field-manager=$FIELD_MANAGER -f -"
  else
    helm show crds "datuplet/$chart" --version "$VERSION" \
      | kubectl apply --server-side --force-conflicts \
          --field-manager="$FIELD_MANAGER" -f -
  fi
}

chart_values() {  # $1 = chart name → echoes -f args, one per line
  local f
  for f in ${COMMON_VALUES[@]+"${COMMON_VALUES[@]}"}; do printf -- '-f\n%s\n' "$f"; done
  case "$1" in
    datuplet-operators)  for f in ${OPERATORS_VALUES[@]+"${OPERATORS_VALUES[@]}"};  do printf -- '-f\n%s\n' "$f"; done ;;
    datuplet-infra)      for f in ${INFRA_VALUES[@]+"${INFRA_VALUES[@]}"};          do printf -- '-f\n%s\n' "$f"; done ;;
    datuplet-app)        for f in ${APP_VALUES[@]+"${APP_VALUES[@]}"};              do printf -- '-f\n%s\n' "$f"; done ;;
    datuplet-lakekeeper) for f in ${LAKEKEEPER_VALUES[@]+"${LAKEKEEPER_VALUES[@]}"};do printf -- '-f\n%s\n' "$f"; done ;;
  esac
}

upgrade_release() {  # $1 = chart name; $2.. = extra helm flags
  local chart=$1; shift
  local src="$REPO_ROOT/charts/$chart" vflags=()
  if [ "$MODE" = from-repo ]; then
    src="datuplet/$chart"; vflags=(--version "$VERSION")
  else
    run helm dependency build "$REPO_ROOT/charts/$chart"
  fi
  local vals=()
  while IFS= read -r line; do [ -n "$line" ] && vals+=("$line"); done < <(chart_values "$chart")
  run helm upgrade "$chart" "$src" -n "$NAMESPACE" \
    ${vflags[@]+"${vflags[@]}"} ${vals[@]+"${vals[@]}"} "$@"
}

fga_sync_warn() {
  # Belt (the CI check in verify-versions.sh is the suspenders): after an
  # app-only upgrade, warn when the deployed lakekeeper FGA pin differs.
  command -v jq >/dev/null 2>&1 || { echo "note: jq not found — skipping FGA cross-chart check"; return 0; }
  local a b
  a=$(helm get values -n "$NAMESPACE" datuplet-app -a -o json 2>/dev/null | jq -r '.fgaModel.version // empty')
  b=$(helm get values -n "$NAMESPACE" datuplet-lakekeeper -a -o json 2>/dev/null | jq -r '.platform.fgaModelVersion // empty')
  if [ -n "$a" ] && [ -n "$b" ] && [ "$a" != "$b" ]; then
    echo "WARNING: fgaModel.version=$a (datuplet-app) != platform.fgaModelVersion=$b (datuplet-lakekeeper)." >&2
    echo "         Upgrade datuplet-lakekeeper too — see docs/fga-model-upgrades.md." >&2
  fi
}

do_phase() {
  case "$1" in
    operators)  apply_crds datuplet-operators
                upgrade_release datuplet-operators --wait --timeout 5m ;;
    infra)      upgrade_release datuplet-infra --wait --wait-for-jobs --timeout 10m ;;
    app)        apply_crds datuplet-app
                upgrade_release datuplet-app --wait --wait-for-jobs --timeout 10m
                fga_sync_warn ;;
    lakekeeper) upgrade_release datuplet-lakekeeper --wait --wait-for-jobs --timeout 10m ;;
  esac
}

if [ "$PHASE" = all ]; then
  for p in operators infra app lakekeeper; do do_phase "$p"; done
else
  do_phase "$PHASE"
fi
echo "OK: upgrade complete (phase=$PHASE namespace=$NAMESPACE)"
```

- [ ] **Step 2: Syntax + dry-run checks**

Run: `bash -n scripts/upgrade.sh && chmod +x scripts/upgrade.sh` — silent.
Run: `scripts/upgrade.sh --dry-run 2>&1 | grep -c 'kubectl apply --server-side'` — expected: `1` (app phase applies app CRDs).
Run: `scripts/upgrade.sh --phase all --dry-run 2>&1 | grep -c 'kubectl apply --server-side'` — expected: `2` (operators + app CRDs).
Run: `scripts/upgrade.sh --phase bogus --dry-run 2>&1; echo $?` — expected: `ERROR: invalid --phase: bogus`, exit 1.

- [ ] **Step 3: Commit**

```bash
git add scripts/upgrade.sh
git commit -m "feat(deploy): upgrade.sh — phase-aware upgrades with explicit CRD apply (RFC 024 W1)"
```

### Task T1.3: Local values + Makefile wrappers converge on scripts

**Files:**
- Create: `tests/local/values-local-app.yaml`
- Modify: `Makefile` — `deploy-local-helm` (~lines 203-217), `e2e-k8s-deploy` (~lines 148-176; only the dep-update + helm + register block)

**Interfaces:**
- Consumes: T1.1/T1.2 flag surface.
- Produces: `make deploy-local-helm` and `make e2e-k8s-deploy` drive `scripts/install.sh` — there is exactly one tested install path (RFC 024 P6 fix). `tests/local/values-local-app.yaml` is the blessed local override (extended in T2.3).

- [ ] **Step 1: Create `tests/local/values-local-app.yaml`**:

```yaml
# Local-development overrides for charts/datuplet-app (OrbStack / kind).
# Images are built locally (make docker-build-k8s) and already present on
# the node; Always would pull from a registry and fail/timeout.
# Passed automatically by `make deploy-local` / `deploy-local-helm`.
image:
  pullPolicy: IfNotPresent
# Point the built-in ComponentDefinitions at the locally-built component
# images. Preserves what `deploy-local-helm` does today via
# `--set components.registry=datuplet` (Makefile) — WITHOUT this the built-ins
# resolve to ghcr.io/kacurez/<comp>:<components.tag>, which isn't on the node,
# so every built-in pipeline ImagePullBackOffs (F2 of the 2026-07-11 codex
# review). `docker-build-k8s` must produce images at <registry>/<comp>:<tag>
# for whatever tag components.tag carries (verify against the Makefile build
# tags — today the built components are tagged `datuplet/<comp>:latest`; if
# components.tag is a stable vX.Y.Z the reconciler's :latest-ban does NOT
# apply to it, but the TAG must still match a locally-present image — align
# the two, or tag the local builds to match components.tag).
components:
  registry: datuplet
```

- [ ] **Step 2: Rewrite `deploy-local-helm`** — replace the 4× `helm dependency update` + 4× `helm upgrade --install` + `register.sh` recipe body with:

```makefile
deploy-local-helm: ## Install/upgrade all 4 charts + register.sh via scripts/install.sh (no docker build)
	./scripts/install.sh --namespace datuplet --from-source \
	  -f-app tests/local/values-local-app.yaml
```

(Keep the target comment block above it; update its text to mention install.sh.)

- [ ] **Step 3: Rewrite the install section of `e2e-k8s-deploy`** — replace the 4× `helm dependency build` + 4× `helm upgrade --install … -f tests/e2e/values-*.yaml` + `./scripts/register.sh --namespace datuplet-e2e` block with:

```makefile
	./scripts/install.sh --namespace datuplet-e2e --from-source \
	  -f-operators tests/e2e/values-operators.yaml \
	  -f-infra tests/e2e/values-infra.yaml \
	  -f-app tests/e2e/values-app.yaml \
	  -f-lakekeeper tests/e2e/values-lakekeeper.yaml
```

Keep everything after it (port-forward, `go test`, teardown) byte-identical. Keep the phase-ordering comment, updating it to point at `scripts/install.sh`.

- [ ] **Step 4: Verify**

Run: `make -n deploy-local-helm` — expected: exactly one `./scripts/install.sh` invocation, no bare `helm upgrade`.
Run: `make -n e2e-k8s-deploy | grep -c 'install.sh'` — expected: `1`.

- [ ] **Step 5: Commit**

```bash
git add Makefile tests/local/
git commit -m "chore(make): deploy-local + e2e converge on scripts/install.sh (RFC 024 W1)"
```

### Task T1.4: Docs converge on install.sh/upgrade.sh

**Files:**
- Modify: `docs/install.md` (§Install lines ~64-100, §Upgrade ~102-128, §Local development ~130-138), `docs/quickstart-kind.md` (§3 install lines ~56-101), `docs/quickstart-gke.md` (its 4× `helm upgrade --install` block — locate with `grep -n 'helm upgrade --install' docs/quickstart-gke.md`)

- [ ] **Step 1: install.md §Install** — replace the numbered 0–5 command block with:

```markdown
​```bash
# From a local clone (development):
./scripts/install.sh --namespace datuplet

# From the published helm repo (no clone of the charts needed):
./scripts/install.sh --namespace datuplet --from-repo --version v0.8.0
​```

`install.sh` runs preflight checks (kubectl/helm versions, cluster
reachability, K8s ≥ 1.28, StorageClass present, chart availability, no
half-installed releases), then the four helm phases in order — each with
`--wait`/`--wait-for-jobs` — and finally `scripts/register.sh`. Pass
per-chart values with `-f-app my-app-values.yaml` (also `-f-infra`,
`-f-operators`, `-f-lakekeeper`); flags after `--` go to register.sh.
Re-running is safe (idempotent). `--preflight-only` and `--dry-run` are
available for checking a cluster before touching it.
```

Keep the phase table at the top of the doc unchanged (it explains *why* the phases exist).

- [ ] **Step 2: install.md §Upgrade** — replace the four `helm upgrade` blocks with:

```markdown
​```bash
# The common case — a Datuplet release (Phase 3 only). Applies the chart's
# CRDs first (helm never upgrades crds/), then upgrades datuplet-app:
./scripts/upgrade.sh --namespace datuplet --phase app

# Everything (dependency bumps, FGA model changes):
./scripts/upgrade.sh --namespace datuplet --phase all
​```

Upgrades are **forward-only**: no `--atomic`, no `helm rollback` (hook
Jobs, CRD applies, and DB migrations sit outside helm's rollback scope).
Recovery from a failed upgrade is: fix the cause, re-run the same command.
```

Keep the FGA cross-chart note + `register.sh is NOT re-run` line.

- [ ] **Step 3: install.md §Local development** — state that `make deploy-local` = image build + `install.sh` with `tests/local/values-local-app.yaml`.

- [ ] **Step 4: quickstart-kind.md §3** — replace the `helm dependency` + 4× `helm upgrade --install` block with the single `./scripts/install.sh --namespace datuplet -f-app tests/local/values-local-app.yaml` invocation (kind loads local images — the pullPolicy override is required; say so). **Keep §4 (register/bootstrap) if it documents flags** — fold its flag examples into the install.sh `-- …` passthrough form.

- [ ] **Step 5: quickstart-gke.md** — replace its helm-install sequence with `install.sh` (`--register-mode job` if the doc's flow is CI-shaped; keep its GKE-specific values-file guidance, passing those files via `-f-app`/`-f-infra`).

- [ ] **Step 6: Verify + commit**

Run: `grep -rn 'helm upgrade --install datuplet' docs/*.md | grep -v superpowers` — expected: no output (the manual sequence lives only in `install.sh` now).

```bash
git add docs/
git commit -m "docs: install/upgrade/quickstarts converge on install.sh + upgrade.sh (RFC 024 W1)"
```

### Task T1.5: Phase 1 gate

- [ ] **Step 1:** `make tidy && go build ./... && go test ./... -count=1` — green.
- [ ] **Step 2:** On OrbStack: `make undeploy-local && make deploy-local` — full install through install.sh succeeds; `make k8s-smoke` passes.
- [ ] **Step 3:** `scripts/upgrade.sh --phase app` against that install — no-op upgrade succeeds (idempotency).
- [ ] **Step 4:** `make e2e-k8s` — green (e2e now exercises install.sh).
- [ ] **Step 5:** Cumulative `mcp__codex__codex` review of the diff so far → zero CRITICAL/MAJOR. Commit on the shared branch. **No PR** (single PR at Final integration).

---

## Phase 2 — De-sed the release

### Task T2.1: Image helper + ghcr/appVersion defaults

> **Reconciled 2026-07-11.** PR #26 deleted the `iceberg-job` image, its
> `image.icebergJob` values block, and the operator's `ICEBERG_JOB_IMAGE` env —
> so this task now covers **four** service images (`pipelineApi pipelineOperator
> pipelineObserver gateway`) + `queryWorker.image` + infra's pipelineApi, not six.
> Line numbers below drifted (RFC 025/026 edits) — **re-grep, don't trust them**.
> Critical: do **not** touch the `components.{registry,tag}` block (RFC 026's
> ComponentDefinition image pins — a platform-independent version axis, RFC §2.7/§6.2).

**Files:**
- Modify: `charts/datuplet-app/templates/_helpers.tpl` (add helper), `charts/datuplet-app/values.yaml` (image block + `queryWorker.image`), `charts/datuplet-infra/values.yaml` (pipelineApi image)
- Modify (re-grep for exact sites — `grep -rn 'repository }}:{{\|queryWorker).image' charts/datuplet-app/templates/`): pipeline-api deployment, pipeline-observer deployment, pipeline-operator deployment (`image:` + the `GATEWAY_IMAGE` env value — **only** these two now; `ICEBERG_JOB_IMAGE` is gone), pipeline-api-migrate Job, reaper CronJob, query-worker deployment, and infra keygen Job

**Interfaces:**
- Produces: every datuplet image renders as `<repository>:<tag | default .Chart.AppVersion>`; committed defaults are `ghcr.io/kacurez/<img>` + `tag: ""`. T2.3 depends on the exact values keys staying unchanged (only their default *values* change). `components.{registry,tag}` are untouched.

- [ ] **Step 1: Add the helper** to `charts/datuplet-app/templates/_helpers.tpl`:

```yaml
{{/*
datuplet-app.image — render "<repository>:<tag>", defaulting the tag to the
chart appVersion (RFC 024 W2: charts are released as committed; the release
pipeline no longer rewrites values). Usage:
  {{ include "datuplet-app.image" (dict "img" .Values.image.pipelineApi "root" $) }}
*/}}
{{- define "datuplet-app.image" -}}
{{- printf "%s:%s" .img.repository (.img.tag | default .root.Chart.AppVersion) -}}
{{- end -}}
```

- [ ] **Step 2: Convert the 7 datuplet-app call sites.** Enumerate first (must find exactly these):

Run: `grep -rn 'repository }}:{{\|((.Values.queryWorker).image)' charts/datuplet-app/templates/`

Replace each `image:`/`value:` composition, e.g. `pipeline-api/deployment.yaml:69` becomes:

```yaml
          image: "{{ include "datuplet-app.image" (dict "img" .Values.image.pipelineApi "root" $) }}"
```

Same pattern for `pipelineObserver`, `pipelineOperator` (`image:`), the operator deployment's `GATEWAY_IMAGE` env value (→ `.Values.image.gateway`), and the migrate Job + reaper CronJob (`.Values.image.pipelineApi`). (There is no `ICEBERG_JOB_IMAGE` anymore — PR #26 removed it.) For the query-worker deployment, replace the inline-defaults form with:

```yaml
          image: "{{ include "datuplet-app.image" (dict "img" .Values.queryWorker.image "root" $) }}"
```

- [ ] **Step 3: datuplet-infra** — `keygen-job.yaml:49` gets the inline equivalent (one site; no helper needed):

```yaml
          image: "{{ .Values.image.pipelineApi.repository }}:{{ .Values.image.pipelineApi.tag | default .Chart.AppVersion }}"
```

- [ ] **Step 4: Flip the values defaults.** In `charts/datuplet-app/values.yaml`: every `repository: datuplet/<img>` → `repository: ghcr.io/kacurez/<img>` and every `tag: latest` → `tag: ""` — **five images**: pipelineApi, pipelineOperator, pipelineObserver, gateway (under `image:`), plus `queryWorker.image` (update its "Defaults to :latest" comment to "Defaults to the chart appVersion"). Same for `charts/datuplet-infra/values.yaml` (pipelineApi). Add above each image block: `# tag "" resolves to the chart appVersion; local dev overrides via tests/local/values-local-app.yaml.` **Do not touch `components.registry`/`components.tag`** (they stay `ghcr.io/kacurez` + `v0.1.0` — RFC 026's independent component-version pin).

- [ ] **Step 5: Template assertions**

Run: `helm template datuplet-app charts/datuplet-app --namespace datuplet | grep -E 'image:|_IMAGE' | sort -u`
Expected: every datuplet **service** image renders `ghcr.io/kacurez/<img>:1.0` (appVersion is `"1.0"` in git today — the value is irrelevant, the *mechanism* is under test); the six built-in ComponentDefinition images still render `ghcr.io/kacurez/<comp>:v0.1.0` (untouched — do not "fix" these); zero `datuplet/` refs, zero `:latest` on any datuplet image.
Run: `helm template datuplet-app charts/datuplet-app --set image.pipelineApi.tag=override | grep 'pipeline-api:override'` — expected: 1+ match (explicit tag wins).

- [ ] **Step 6: Commit**

```bash
git add charts/
git commit -m "feat(charts)!: ghcr defaults + tag defaulting to appVersion — charts released as committed (RFC 024 W2)"
```

### Task T2.2: pullPolicy flip + doc-note removal

**Files:**
- Modify: `charts/datuplet-app/values.yaml:22` (+ comment), `docs/install.md` (pullPolicy caveat lines ~84-86), `docs/quickstart-kind.md` (kind pullPolicy override note ~76-80)

- [ ] **Step 1:** `pullPolicy: Always` → `pullPolicy: IfNotPresent` in `charts/datuplet-app/values.yaml:22`; rewrite the adjacent comment: `# IfNotPresent is safe: image tags are pinned (default = chart appVersion). Local dev with :latest overrides keeps working because the local image is already present.` Check infra: `grep -n pullPolicy charts/datuplet-infra/values.yaml` — if a chart-global pullPolicy exists there, flip it identically.
- [ ] **Step 2:** Remove the now-obsolete caveats: install.md's "chart default is image.pullPolicy=Always … kind clusters must add --set image.pullPolicy=IfNotPresent" note and quickstart-kind's equivalent paragraph (the local values file from T1.3 still sets it explicitly — harmless belt).
- [ ] **Step 3:** `helm template datuplet-app charts/datuplet-app | grep -c 'imagePullPolicy: Always'` — expected: `0`. Commit `feat(charts): default pullPolicy IfNotPresent — tags are pinned now (RFC 024 W2, §9 Q2)`.

### Task T2.3: Complete image overrides in e2e + local values

**Files:**
- Modify: `tests/e2e/values-app.yaml:7-18`, `tests/local/values-local-app.yaml`
- Create: `tests/e2e/values-infra-images.yaml`? **No** — modify `tests/e2e/values-infra.yaml` (add image block)

**Interfaces:**
- Produces: e2e + local installs run **only** locally built `datuplet/*:latest` images. Without this, post-T2.1 chart defaults would pull `ghcr.io/kacurez/*:1.0` (nonexistent) and every e2e run fails — this task is what keeps the flip honest.

- [ ] **Step 1: `tests/e2e/values-app.yaml`** — extend the `image:` block (re-check what it currently overrides post-merge) to **all four** service images + keep queryWorker's existing block (icebergJob no longer exists — do not add it):

```yaml
image:
  pullPolicy: IfNotPresent
  pipelineApi:      {repository: datuplet/pipeline-api,      tag: latest}
  pipelineOperator: {repository: datuplet/pipeline-operator, tag: latest}
  pipelineObserver: {repository: datuplet/pipeline-observer, tag: latest}
  gateway:          {repository: datuplet/gateway,           tag: latest}
```

(Preserve the existing explanatory comment about kind + pullPolicy. Note: e2e sets `components.builtins: false` and registers local component images as prerelease `dev` ComponentDefinitions via the harness — so no `components.*` override belongs here.)

- [ ] **Step 2: `tests/e2e/values-infra.yaml`** — append:

```yaml
# Post-RFC-024-W2 the chart default is ghcr.io/kacurez/pipeline-api:<appVersion>;
# e2e runs the locally built image (kind-loaded).
image:
  pipelineApi: {repository: datuplet/pipeline-api, tag: latest}
```

- [ ] **Step 3: `tests/local/values-local-app.yaml`** — extend with the same four-image block as Step 1 (identical content, OrbStack shares the docker image cache).

- [ ] **Step 4: Verify**: `helm template datuplet-app charts/datuplet-app -f tests/e2e/values-app.yaml | grep -cE 'image:.*datuplet/'` — expected ≥4 (the four service images; the built-in ComponentDefinition images stay on `ghcr.io/kacurez/*:v0.1.0`). Commit `test(e2e): complete local-image overrides for the ghcr-default charts (RFC 024 W2)`.

### Task T2.4: release.yml de-sed: verify-tag guard + `make bump-version`

> **Reconciled 2026-07-11.** This task now also **fixes an already-broken release**:
> RFC 026's `components.tag: v0.1.0` in `datuplet-app/values.yaml` trips the current
> pin-step guard (`grep -qP "^\s*tag:\s+(?!latest$)"` matches it → the step aborts on
> the next `v*` tag). Deleting the whole sed+guard removes the failure. **If a release
> must be cut before this task lands**, apply the stopgap first: change the guard's
> grep to exclude the components block, e.g. `grep -vE '^\s+' | ...` won't work —
> instead anchor the guard to the known service-image tag lines only, or (simplest)
> `make bump-version` + hand-pin the five service `tag:` lines and skip the sed. Note
> this in the PR so the maintainer knows the guard was load-bearing-broken.

**Files:**
- Modify: `.github/workflows/release.yml` (delete the "Pin chart image tags + versions" step entirely — re-grep its line range, it shifted; add `verify-tag` job; wire `needs`), `Makefile` (new `bump-version` target), `CLAUDE.md` (release-discipline bullet)

**Interfaces:**
- Produces: releases publish charts byte-identical to the tagged commit, and the release stops aborting on `components.tag`. Release flow becomes: `make bump-version VERSION=X.Y.Z` → commit → tag `vX.Y.Z` → push tag. T3.3's release-verify assumes published chart version == tag. `bump-version` touches only `Chart.yaml` `version`/`appVersion` — **not** `components.tag` (independent axis).

- [ ] **Step 1: Makefile `bump-version`** (portable — perl, not sed -i):

```makefile
.PHONY: bump-version
bump-version: ## Set version+appVersion in all four charts (make bump-version VERSION=0.8.0)
ifndef VERSION
	$(error VERSION is required, e.g. make bump-version VERSION=0.8.0)
endif
	@for c in datuplet-operators datuplet-infra datuplet-app datuplet-lakekeeper; do \
	  perl -pi -e 's/^version: .*/version: $(VERSION)/; s/^appVersion: .*/appVersion: "$(VERSION)"/' charts/$$c/Chart.yaml; \
	done
	@grep -H -E '^(version|appVersion):' charts/*/Chart.yaml
```

- [ ] **Step 2: Add the `verify-tag` job** as the first job in `release.yml`:

```yaml
  verify-tag:
    name: tag matches committed chart versions
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - uses: actions/checkout@v6
      - name: Verify
        run: |
          set -eu
          VERSION="${GITHUB_REF#refs/tags/v}"
          fail=0
          for c in datuplet-operators datuplet-infra datuplet-app datuplet-lakekeeper; do
            v=$(sed -n 's/^version: *//p' "charts/$c/Chart.yaml")
            av=$(sed -n 's/^appVersion: *"\{0,1\}\([^"]*\)"\{0,1\} *$/\1/p' "charts/$c/Chart.yaml")
            [ "$v" = "$VERSION" ]  || { echo "::error::charts/$c version=$v != tag $VERSION"; fail=1; }
            [ "$av" = "$VERSION" ] || { echo "::error::charts/$c appVersion=$av != tag $VERSION"; fail=1; }
          done
          [ $fail -eq 0 ] || { echo "::error::Run 'make bump-version VERSION=$VERSION', commit, then re-tag."; exit 1; }
```

Add `needs: verify-tag` to the `services` and `components` jobs; `charts` already needs those two (transitively gated).

- [ ] **Step 3: Delete the sed step.** Remove the entire "Pin chart image tags + versions to release tag" step (the `for img in …` seds, the PCRE guard loop, and the `Chart.yaml` bump loop — lines ~61-109). Keep "Resolve version tag", gh-pages bootstrap, and chart-releaser steps unchanged.

- [ ] **Step 4: CLAUDE.md** — in "Branch + release discipline", extend the maintainer-cuts-releases bullet with: `Release prep: 'make bump-version VERSION=X.Y.Z' → commit → tag vX.Y.Z (the release workflow fails on tag/chart-version mismatch, and charts publish exactly as committed).`

- [ ] **Step 5: Verify + commit**

Run: `grep -n 'sed -i\|tag: latest' .github/workflows/release.yml` — expected: no output. `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/release.yml')); print('ok')"` — `ok`.

```bash
git add .github/workflows/release.yml Makefile CLAUDE.md
git commit -m "ci(release)!: bump-before-tag + verify-tag guard; charts publish as committed (RFC 024 W2)"
```

### Task T2.5: Phase 2 gate

- [ ] **Step 1:** `make tidy && go build ./... && go test ./... -count=1` green; `helm lint` all four charts.
- [ ] **Step 2:** `make e2e-k8s` on OrbStack — proves the e2e values overrides (T2.3) keep local images working end-to-end.
- [ ] **Step 3:** Template spot-checks from T2.1 Step 5 + T2.2 Step 3 all pass.
- [ ] **Step 4:** Cumulative `mcp__codex__codex` review → clean. Commit on the shared branch. **No PR** (single PR at Final integration). **Carry forward to the final PR body**: first release after this merges requires `make bump-version` before tagging — the old tag flow now fails loudly at verify-tag.

---

## Phase 3 — release-verify

### Task T3.1: Parameterize `k8s-smoke`

**Files:**
- Modify: `Makefile` `k8s-smoke` (~lines 97-120)

- [ ] **Step 1:** Add `SMOKE_URL ?= http://localhost:30081` next to `K8S_NS`; replace every literal `http://localhost:30081` inside the target (4 occurrences: healthz curl + error text, openid-configuration, jwks) with `$(SMOKE_URL)`.
- [ ] **Step 2:** `make -n k8s-smoke SMOKE_URL=http://example:1234 | grep -c example:1234` — expected ≥3. Commit `chore(make): k8s-smoke accepts SMOKE_URL override (RFC 024 W3)`.

### Task T3.2: Verify pipeline template + REST driver

**Files:**
- Create: `tests/release-verify/pipeline.yaml` (envsubst template), `tests/release-verify/run-pipeline.sh` (0755)

> **Reconciled 2026-07-11.** RFC 026 removed `image:` from ComponentSpec — pipelines
> now reference registry entries via `component:` (+ optional `version:`). The verify
> pipeline uses the chart's **built-in ComponentDefinitions** (installed by
> `helm install datuplet-app`), so there is **no image templating** — no
> `IMAGE_PREFIX`/`IMAGE_TAG`. This is deliberate: exercising `component:
> http-json-extractor` is exactly what surfaces the §6.6 component-image gap
> (built-ins pinned to `:v0.1.0` → `ImagePullBackOff` if that tag was never
> published). release-verify going red on that is the intended signal, not a flake.

**Interfaces:**
- Produces (T3.3 + T5.1 consume): `run-pipeline.sh <pipeline-yaml>` — env: `BASE_URL` (default `http://localhost:30081`), `DATUPLET_ADMIN_EMAIL`, `DATUPLET_ADMIN_PASSWORD` (required); uploads via `PUT /api/v1/projects/{pid}/pipelines/{name}`, triggers via `POST …/pipelines/{name}/runs`, polls `GET …/runs/{id}` until `Succeeded` (10 min timeout); exit 0 on success. Template var in pipeline.yaml: `PIPELINE_NAME` only (no image vars — component refs resolve against the registry).

- [ ] **Step 1: `tests/release-verify/pipeline.yaml`** — read-back shape (write a table via the gateway + inline Iceberg commit, then read it back) using **built-in component refs**, envsubst-templated on the name only. Confirm the current config schema of each component with `kubectl get componentdefinition http-json-extractor -o yaml` (or `charts/datuplet-app/templates/components/*.yaml`) — RFC 026's schemas are authoritative and reject unknown keys:

```yaml
# Release-verification pipeline (RFC 024 W3). Rendered with:
#   PIPELINE_NAME=release-verify envsubst '$PIPELINE_NAME' < pipeline.yaml
# Uses the chart's built-in ComponentDefinitions (version omitted → defaultVersion).
apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: ${PIPELINE_NAME}
  namespace: datuplet
spec:
  stages:
    - name: extract
      components:
        - name: json-extractor
          component: http-json-extractor
          config:
            url: "https://jsonplaceholder.typicode.com/posts"
          outputs:
            defaultBucket: verify
            defaultWriteMode: FULL_LOAD
    - name: read
      components:
        - name: stdout-writer
          component: stdout-writer
          inputs:
            tables:
              - bucket: verify
                table: data
          config:
            format: json
```

  Note on the external URL: release-verify installs from published charts via
  `install.sh` (helm only) — the W7 in-cluster HTTP fixture is applied by
  `e2e-k8s-deploy`, not here — so this pipeline reaches `jsonplaceholder` over the
  internet. Acceptable for a retriable release gate; if flakiness bites, add a
  `kubectl apply -f tests/e2e/manifests/http-fixture.yaml` pre-step in T3.3 and swap
  the URL to the in-cluster Service.

- [ ] **Step 2: `tests/release-verify/run-pipeline.sh`** (complete file). ⚠ The jq field paths below encode the expected response shapes — before first use, confirm them against `docs/pipeline-api.md` lines 152-193 (§Upload a pipeline / §Trigger a run / §Runs) and the response structs in `pkg/pipelineapi/http/pipeline_handlers.go` + `run_handlers.go`; adjust the three `jq -r` expressions if the field names differ:

```bash
#!/usr/bin/env bash
# Upload + trigger + await one pipeline through the public REST API — the
# same path a user takes. Used by release-verify and upgrade-e2e workflows.
set -euo pipefail
BASE_URL=${BASE_URL:-http://localhost:30081}
EMAIL=${DATUPLET_ADMIN_EMAIL:?set DATUPLET_ADMIN_EMAIL}
PASSWORD=${DATUPLET_ADMIN_PASSWORD:?set DATUPLET_ADMIN_PASSWORD}
PIPELINE_FILE=${1:?usage: run-pipeline.sh <rendered-pipeline.yaml>}
TIMEOUT_S=${TIMEOUT_S:-600}

NAME=$(sed -n 's/^  name: //p' "$PIPELINE_FILE" | head -1)
[ -n "$NAME" ] || { echo "FATAL: no metadata.name in $PIPELINE_FILE" >&2; exit 2; }
JAR=$(mktemp); trap 'rm -f "$JAR"' EXIT

echo "--- login $BASE_URL as $EMAIL"
curl -fsS -c "$JAR" -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" \
  "$BASE_URL/api/v1/auth/login" >/dev/null

PID=$(curl -fsS -b "$JAR" "$BASE_URL/api/v1/projects" | jq -r '.[0].id // .projects[0].id')
[ -n "$PID" ] && [ "$PID" != null ] || { echo "FATAL: no project found" >&2; exit 2; }

echo "--- upload pipeline '$NAME' to project $PID"
curl -fsS -b "$JAR" -X PUT -H 'Content-Type: application/yaml' \
  --data-binary @"$PIPELINE_FILE" \
  "$BASE_URL/api/v1/projects/$PID/pipelines/$NAME" >/dev/null

echo "--- trigger run"
RUN_ID=$(curl -fsS -b "$JAR" -X POST \
  "$BASE_URL/api/v1/projects/$PID/pipelines/$NAME/runs" | jq -r '.id // .runId')
[ -n "$RUN_ID" ] && [ "$RUN_ID" != null ] || { echo "FATAL: trigger returned no run id" >&2; exit 2; }
echo "run id: $RUN_ID"
echo "$RUN_ID" > /tmp/datuplet-last-run-id   # consumed by upgrade-e2e's post-upgrade assert

deadline=$(( $(date +%s) + TIMEOUT_S ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  STATUS=$(curl -fsS -b "$JAR" "$BASE_URL/api/v1/projects/$PID/runs/$RUN_ID" \
    | jq -r '.status // .phase')
  echo "  status: $STATUS"
  case "$STATUS" in
    Succeeded) echo "OK: run $RUN_ID Succeeded"; exit 0 ;;
    Failed*|Cancelled) echo "FAIL: run $RUN_ID ended $STATUS" >&2; exit 1 ;;
  esac
  sleep 10
done
echo "FAIL: run $RUN_ID did not finish within ${TIMEOUT_S}s" >&2; exit 1
```

- [ ] **Step 3: Local verification** (needs a deployed OrbStack stack from `make deploy-local`):

```bash
bash -n tests/release-verify/run-pipeline.sh && chmod +x tests/release-verify/run-pipeline.sh
PIPELINE_NAME=rv-local envsubst '$PIPELINE_NAME' \
  < tests/release-verify/pipeline.yaml > /tmp/rv.yaml
DATUPLET_ADMIN_EMAIL=admin@datuplet.local DATUPLET_ADMIN_PASSWORD=changeme \
  tests/release-verify/run-pipeline.sh /tmp/rv.yaml
```

Expected: `OK: run <id> Succeeded`. Fix the jq paths now if the API shapes differ.

- [ ] **Step 4: Commit** `test(release-verify): REST-driven verification pipeline + driver (RFC 024 W3)`.

### Task T3.3: `.github/workflows/release-verify.yml`

**Files:**
- Create: `.github/workflows/release-verify.yml`

**Interfaces:**
- Consumes: T1.1 install.sh, T1.2 upgrade.sh, T3.1 SMOKE_URL, T3.2 driver. A **separate workflow** from release.yml — its publish jobs derive tags from `GITHUB_REF` and must never run on a schedule (RFC 024 §6.3).

- [ ] **Step 1: Write the workflow** (complete file):

```yaml
name: release-verify

# Installs the PUBLISHED artifacts (gh-pages charts + ghcr images) on a
# fresh kind cluster and runs a real pipeline. A release is announceable
# when this is green (RFC 024 W3). Deliberately a separate workflow from
# release.yml, whose publish jobs must only ever run on tag pushes.
on:
  workflow_run:
    workflows: [release]
    types: [completed]
  schedule:
    - cron: '17 5 * * 1'   # weekly bit-rot canary (gh-pages/ghcr/upstream repos)
  workflow_dispatch:
    inputs:
      version:
        description: Version to verify (vX.Y.Z or X.Y.Z); empty = latest published
        required: false

permissions:
  contents: read

jobs:
  verify:
    # For workflow_run: only verify successful releases.
    if: github.event_name != 'workflow_run' || github.event.workflow_run.conclusion == 'success'
    runs-on: ubuntu-latest
    timeout-minutes: 40
    env:
      HELM_REPO_URL: https://kacurez.github.io/datuplet
    steps:
      - name: Resolve version to verify
        id: ver
        run: |
          set -eu
          case "${{ github.event_name }}" in
            workflow_dispatch) V="${{ inputs.version }}" ;;
            workflow_run)      V="${{ github.event.workflow_run.head_branch }}" ;;  # the tag name
            *)                 V="" ;;
          esac
          if [ -z "$V" ]; then
            V=$(curl -fsSL "$HELM_REPO_URL/index.yaml" \
              | yq '.entries.datuplet-app[].version' | sort -V | tail -1)
          fi
          echo "version=${V#v}" >> "$GITHUB_OUTPUT"
          echo "verifying version ${V#v}"

      - uses: actions/checkout@v6
        with:
          # Verify with the scripts as released for tag-triggered runs;
          # scheduled runs use main's scripts against the latest release.
          ref: ${{ github.event_name == 'workflow_run' && github.event.workflow_run.head_branch || '' }}

      - name: Install Helm
        uses: azure/setup-helm@v5
        with:
          version: 'v3.14.4'

      - name: Write kind config (NodePort 30081 → host)
        run: |
          cat > /tmp/kind-config.yaml <<'EOF'
          kind: Cluster
          apiVersion: kind.x-k8s.io/v1alpha4
          nodes:
            - role: control-plane
              extraPortMappings:
                - containerPort: 30081
                  hostPort: 30081
          EOF

      - name: Create kind cluster
        uses: helm/kind-action@v1.14.0
        with:
          version: v0.23.0
          cluster_name: release-verify
          node_image: kindest/node:v1.30.0
          config: /tmp/kind-config.yaml

      - name: Install from published artifacts
        run: |
          ./scripts/install.sh --namespace datuplet \
            --from-repo --version "${{ steps.ver.outputs.version }}" \
            --register-mode job \
            -- --admin-email verify@datuplet.local --admin-password 'rv-ci-password-1'

      - name: Smoke
        run: make k8s-smoke SMOKE_URL=http://localhost:30081

      - name: Run verification pipeline (built-in component refs)
        run: |
          # Pipeline references built-in ComponentDefinitions (component:/version:
          # omitted → defaultVersion). This exercises the chart's registry CRs and
          # will surface the §6.6 component-image gap (built-ins pinned to :v0.1.0)
          # as an ImagePullBackOff → run failure if those tags were never published.
          PIPELINE_NAME=release-verify envsubst '$PIPELINE_NAME' \
            < tests/release-verify/pipeline.yaml > /tmp/rv.yaml
          DATUPLET_ADMIN_EMAIL=verify@datuplet.local \
          DATUPLET_ADMIN_PASSWORD='rv-ci-password-1' \
            tests/release-verify/run-pipeline.sh /tmp/rv.yaml

      - name: Idempotent re-upgrade (no-op)
        run: |
          ./scripts/upgrade.sh --namespace datuplet --phase app \
            --from-repo --version "${{ steps.ver.outputs.version }}"

      - name: Dump state on failure
        if: failure()
        run: |
          kubectl get pods -n datuplet -o wide || true
          for pod in $(kubectl -n datuplet get pods -o name 2>/dev/null); do
            echo "=== $pod ==="; kubectl -n datuplet describe "$pod" | tail -30 || true
            kubectl -n datuplet logs "$pod" --all-containers --tail=100 || true
          done
```

- [ ] **Step 2:** `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/release-verify.yml')); print('ok')"` → ok. Commit `ci(release-verify): install every release from published artifacts on kind (RFC 024 W3)`.

### Task T3.4: Phase 3 gate

- [ ] **Step 1:** Push the shared branch; run the workflow manually against it: `gh workflow run release-verify.yml --ref claude/modest-easley-f5173d -f version=<latest released tag>` and watch: `gh run watch`. (This is the task's real test — it exercises the full published-artifact path.) Debug failures via the failure-dump step output; jq-path mismatches from T3.2 surface here if the local check was skipped. **⚠ Bootstrap caveat (F7):** if the latest published tag pre-dates T6.3, the built-in-component pipeline step **will** fail with `ImagePullBackOff` (`:v0.1.0` images were never published) — that's expected, not a plan defect. Until a post-T6.3 release exists, either run against such a release, or accept this as a known bootstrap failure (log it loudly, don't gate the branch on it). Green is required only once a post-T6.3 release is published.
- [ ] **Step 2:** Add the release-discipline note to `docs/install.md` (§Helm repo): "A release is announceable when its `release-verify` workflow run is green."
- [ ] **Step 3:** Cumulative `mcp__codex__codex` review → clean. Commit on the shared branch. **No PR** (single PR at Final integration).

---

## Phase 4 — verify-versions CI check

### Task T4.1: `scripts/verify-versions.sh` + pr.yml wiring

**Files:**
- Create: `scripts/verify-versions.sh` (0755)
- Modify: `.github/workflows/pr.yml` (new job `verify-versions`)

**Interfaces:**
- Produces: a repo-local check runnable as `scripts/verify-versions.sh` (needs `helm` + `yq`); CI-fatal on: FGA cross-chart drift, any `utils/deploy` reference, `:latest`/`datuplet/` in rendered chart output, missing/inconsistent Chart.lock. Env `VERIFY_IMAGE_ALLOWLIST` = extended-regex of exempt image lines (default: match nothing).

- [ ] **Step 1: Write the script** (complete file):

```bash
#!/usr/bin/env bash
# scripts/verify-versions.sh — RFC 024 W4: cross-file version-sync checks.
# Requires: helm, yq (mikefarah). Run from the repo root.
set -euo pipefail
cd "$(dirname "$0")/.."
command -v yq >/dev/null || { echo "FATAL: yq required"; exit 2; }
command -v helm >/dev/null || { echo "FATAL: helm required"; exit 2; }

fail=0
err() { echo "FAIL: $*" >&2; fail=1; }
ALLOW="${VERIFY_IMAGE_ALLOWLIST:-^$}"
CHARTS="datuplet-operators datuplet-infra datuplet-app datuplet-lakekeeper"

# 1. FGA model version must match across app + lakekeeper charts
a=$(yq '.fgaModel.version' charts/datuplet-app/values.yaml)
b=$(yq '.platform.fgaModelVersion' charts/datuplet-lakekeeper/values.yaml)
[ "$a" = "$b" ] || err "FGA model drift: datuplet-app fgaModel.version=$a != datuplet-lakekeeper platform.fgaModelVersion=$b (docs/fga-model-upgrades.md)"

# 2. The legacy raw-manifest tree stays dead
if grep -rn 'utils/deploy' --exclude-dir=.git --exclude-dir=superpowers . >/dev/null 2>&1; then
  grep -rn 'utils/deploy' --exclude-dir=.git --exclude-dir=superpowers . >&2
  err "reference to deleted utils/deploy/ tree (single deploy source = charts/)"
fi

# 3. Rendered charts (default values) must not contain floating or dev image refs
for c in $CHARTS; do
  helm dependency build "charts/$c" >/dev/null
  rendered=$(helm template "$c" "charts/$c" --namespace datuplet 2>/dev/null)
  bad=$(printf '%s\n' "$rendered" | grep -nE '(:latest|[^A-Za-z0-9_.-]datuplet/)' | grep -Ev "$ALLOW" || true)
  [ -z "$bad" ] || { printf '%s\n' "$bad" >&2; err "$c renders ':latest' or 'datuplet/' dev image refs with default values"; }
done

# 4. Chart.lock present + consistent for every dependency-bearing chart
for c in $CHARTS; do
  if grep -q '^dependencies:' "charts/$c/Chart.yaml"; then
    [ -f "charts/$c/Chart.lock" ] || err "charts/$c has dependencies but no committed Chart.lock"
  fi
done

if [ "$fail" -ne 0 ]; then echo "verify-versions: FAILED"; exit 1; fi
echo "verify-versions: OK"
```

- [ ] **Step 2: Wire into pr.yml** — append a job:

```yaml
  verify-versions:
    name: version-sync checks
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v6
      - name: Install Helm
        uses: azure/setup-helm@v5
        with:
          version: 'v3.14.4'
      - name: verify-versions
        run: scripts/verify-versions.sh
```

- [ ] **Step 3: Negative tests** (run each, revert after):
  - Temporarily set lakekeeper `platform.fgaModelVersion: "9.9"` → script exits 1 with `FGA model drift`.
  - Add `# see utils/deploy/foo` to README.md → exits 1 with `reference to deleted utils/deploy`.
  - Set `charts/datuplet-app/values.yaml` pipelineApi `tag: latest` → exits 1 with `renders ':latest'`.
  - Clean tree → `verify-versions: OK`, exit 0.

- [ ] **Step 4: Commit** `ci: verify-versions — FGA sync, no legacy tree, no floating rendered images, Chart.lock (RFC 024 W4)`.

### Task T4.2: Phase 4 gate

- [ ] **Step 1:** `bash -n scripts/verify-versions.sh`; full local run passes on clean tree; `go build ./... && go test ./... -count=1` green (untouched, but repo rule).
- [ ] **Step 2:** Cumulative `mcp__codex__codex` review → clean. Commit on the shared branch. **No PR** (single PR at Final integration).

---

## Phase 5 — upgrade e2e lane

### Task T5.1: `.github/workflows/upgrade-e2e.yml`

> **Reconciled 2026-07-11.** Post-RFC-026 pipelines use `component:` refs, so the
> upgrade lane must keep the built-in ComponentDefinitions registered *and* pointed
> at pullable images on both sides of the upgrade. This requires a dedicated values
> overlay (below) — `tests/e2e/values-app.yaml` sets `components.builtins: false`
> (for the Go harness path) which would leave the REST-driven pipeline with no
> registry entries.
>
> **⚠ Bootstrapping constraint (F4 of the 2026-07-11 codex review).** The N-1 install
> pulls the *already-published* release's chart, which pins its own `components.tag`
> to unpublished images — and T6.3 (a HEAD change) **cannot retro-fix an
> already-published release**. So the first upgrade-e2e run can only pass once the
> latest published release is one cut **after T6.3 shipped** (i.e. its built-in
> component images exist). Options: (a) gate this workflow's first green on
> `latest-published ≥ first-post-T6.3 release`; (b) until then, install N-1 with a
> `-f-app` overlay that sets `components.registry/tag` to a known-good published
> combination, or `components.builtins:false` + register dev defs for the seed. Do
> not treat a red first run as a plan defect — it's this ordering. Encode the gate so
> it's obvious (skip-with-loud-log until the prerequisite release exists).

**Files:**
- Create: `.github/workflows/upgrade-e2e.yml`, `tests/upgrade-e2e/values-app.yaml` (e2e app overlay with `components: {builtins: true, registry: datuplet, tag: latest}` and the four service-image overrides — a copy of `tests/e2e/values-app.yaml` minus its `components.builtins: false` line)

**Interfaces:**
- Consumes: install.sh/upgrade.sh (T1.x), run-pipeline.sh + pipeline.yaml (T3.2, component refs), e2e values files (T2.3) + the new upgrade overlay, the e2e workflow's build + kind-load recipe (re-copy from the current `.github/workflows/e2e.yml` build+load steps — line range drifted after PR #26 removed iceberg-job).
- Produces: the N-1→N upgrade proof — published release installed, state seeded, upgraded in place to HEAD, seeded state + fresh pipeline asserted (RFC 024 W5).

- [ ] **Step 1: Write the workflow** (complete file):

```yaml
name: upgrade-e2e

# Installs the latest PUBLISHED release, seeds real state, upgrades
# in place to HEAD (charts + images from this checkout), and proves the
# upgrade: hooks + migrations succeed, seeded history survives, a fresh
# pipeline runs. This is the tested "N-1 → N, one hop" support statement
# (RFC 024 W5, known-limitations.md).
on:
  push:
    branches: [main]
  pull_request:
    types: [labeled, synchronize]
  workflow_dispatch:

concurrency:
  group: upgrade-e2e-${{ github.ref }}
  cancel-in-progress: true

permissions:
  contents: read

jobs:
  upgrade:
    # PRs run only when labeled 'upgrade-e2e' (keeps PR cost flat).
    if: >
      github.event_name != 'pull_request' ||
      contains(github.event.pull_request.labels.*.name, 'upgrade-e2e')
    runs-on: ubuntu-latest
    timeout-minutes: 50
    env:
      NS: datuplet-e2e
      HELM_REPO_URL: https://kacurez.github.io/datuplet
      ADMIN_EMAIL: upgrade@datuplet.local
      ADMIN_PASSWORD: ue-ci-password-1
    steps:
      - uses: actions/checkout@v6

      - name: Free disk space
        run: |
          sudo rm -rf /usr/share/dotnet /opt/ghc /usr/local/.ghcup \
            /usr/local/lib/android /opt/hostedtoolcache/CodeQL || true

      - name: Set up Go
        uses: actions/setup-go@v6
        with:
          go-version-file: go.mod
          cache: true

      - name: Install Helm
        uses: azure/setup-helm@v5
        with:
          version: 'v3.14.4'

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v4

      - name: Write kind config (NodePort 30081 → host)
        run: |
          cat > /tmp/kind-config.yaml <<'EOF'
          kind: Cluster
          apiVersion: kind.x-k8s.io/v1alpha4
          nodes:
            - role: control-plane
              extraPortMappings:
                - containerPort: 30081
                  hostPort: 30081
          EOF

      - name: Create kind cluster
        uses: helm/kind-action@v1.14.0
        with:
          version: v0.23.0
          cluster_name: datuplet-e2e
          node_image: kindest/node:v1.30.0
          config: /tmp/kind-config.yaml

      - name: Resolve latest published release
        id: prev
        run: |
          V=$(curl -fsSL "$HELM_REPO_URL/index.yaml" \
            | yq '.entries.datuplet-app[].version' | sort -V | tail -1)
          echo "version=$V" >> "$GITHUB_OUTPUT"
          echo "upgrading FROM $V"

      - name: Install published release N-1
        run: |
          ./scripts/install.sh --namespace "$NS" \
            --from-repo --version "${{ steps.prev.outputs.version }}" \
            --register-mode job \
            -- --admin-email "$ADMIN_EMAIL" --admin-password "$ADMIN_PASSWORD"

      - name: Seed state (pipeline run + second project)
        run: |
          # Component refs (no image templating). This N-1 seed run depends on
          # the built-in ComponentDefinition images being pullable at the N-1
          # release's components.tag — i.e. on the §6.6 component-image gap being
          # closed (T6.3). If T6.3 hasn't landed, this step is the failure signal.
          PIPELINE_NAME=seeded-pipeline envsubst '$PIPELINE_NAME' \
            < tests/release-verify/pipeline.yaml > /tmp/seed.yaml
          DATUPLET_ADMIN_EMAIL="$ADMIN_EMAIL" DATUPLET_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
            tests/release-verify/run-pipeline.sh /tmp/seed.yaml
          cp /tmp/datuplet-last-run-id /tmp/seeded-run-id
          # Second project + grants → real FGA tuples + DB rows to migrate.
          ./scripts/register.sh --namespace "$NS" --mode job \
            --project-name seeded-project \
            --admin-email "$ADMIN_EMAIL" --admin-password "$ADMIN_PASSWORD"

      - name: Build + load HEAD images
        run: |
          make docker-build-k8s build-components-e2e
          set -eu
          # No iceberg-job (deleted by PR #26). Component images are loaded so the
          # post-upgrade run can resolve built-ins against locally-built images.
          for img in \
            datuplet/pipeline-api:latest datuplet/pipeline-observer:latest \
            datuplet/pipeline-operator:latest datuplet/gateway:latest \
            datuplet/data-generator:latest datuplet/sql-transform:latest \
            datuplet/stdout-writer:latest datuplet/http-json-extractor:latest \
            datuplet/query-worker:latest \
          ; do kind load docker-image --name datuplet-e2e "$img"; done
          docker image prune --all --force || true

      - name: Upgrade in place to HEAD
        run: |
          # ⚠ Component resolution: do NOT reuse tests/e2e/values-app.yaml's
          # components.builtins:false here — that path expects the Go e2e harness
          # to register dev ComponentDefinitions, which this REST-driven lane never
          # runs. Keep builtins:true and point the built-ins at the HEAD-built local
          # images so `component:` refs resolve. Use a dedicated overlay:
          #   tests/upgrade-e2e/values-app.yaml = tests/e2e/values-app.yaml MINUS the
          #   components.builtins:false line, PLUS:
          #     components: {builtins: true, registry: datuplet, tag: latest}
          #     (chart pullPolicy IfNotPresent keeps the kind-loaded images in use)
          ./scripts/upgrade.sh --namespace "$NS" --phase all --from-source \
            -f-operators tests/e2e/values-operators.yaml \
            -f-infra tests/e2e/values-infra.yaml \
            -f-app tests/upgrade-e2e/values-app.yaml \
            -f-lakekeeper tests/e2e/values-lakekeeper.yaml

      - name: Assert seeded run survived + fresh pipeline works
        run: |
          set -eu
          make k8s-smoke SMOKE_URL=http://localhost:30081
          # Fresh pipeline on the upgraded stack — built-in component refs now
          # resolve against the HEAD-built local images (registry=datuplet tag=latest).
          PIPELINE_NAME=post-upgrade envsubst '$PIPELINE_NAME' \
            < tests/release-verify/pipeline.yaml > /tmp/post.yaml
          DATUPLET_ADMIN_EMAIL="$ADMIN_EMAIL" DATUPLET_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
            tests/release-verify/run-pipeline.sh /tmp/post.yaml
          # Seeded run id still listable through the API after migrations:
          SEEDED=$(cat /tmp/seeded-run-id)
          JAR=$(mktemp)
          curl -fsS -c "$JAR" -H 'Content-Type: application/json' \
            -d "{\"email\":\"$ADMIN_EMAIL\",\"password\":\"$ADMIN_PASSWORD\"}" \
            http://localhost:30081/api/v1/auth/login >/dev/null
          PID=$(curl -fsS -b "$JAR" http://localhost:30081/api/v1/projects | jq -r '.[0].id // .projects[0].id')
          curl -fsS -b "$JAR" "http://localhost:30081/api/v1/projects/$PID/runs" \
            | grep -q "$SEEDED" || { echo "FAIL: seeded run $SEEDED missing after upgrade"; exit 1; }
          echo "OK: seeded run survived, fresh pipeline succeeded"

      - name: Dump state on failure
        if: failure()
        run: |
          kubectl get pods -n "$NS" -o wide || true
          kubectl get jobs -n "$NS" || true
          for pod in $(kubectl -n "$NS" get pods -o name 2>/dev/null); do
            echo "=== $pod ==="; kubectl -n "$NS" logs "$pod" --all-containers --tail=100 || true
          done
```

- [ ] **Step 2:** YAML-parse check (python3 one-liner as in T3.3). Note in the workflow-adjacent commit message: the seeded pipeline runs the **N-1 dialect** and post-upgrade assertions are deliberately infra-scoped (history listable + new-shape pipeline works) — old pipelines are NOT re-triggered (POC greenfield; RFC 024 §6.5).

- [ ] **Step 3: Commit** `ci(upgrade-e2e): published N-1 → HEAD in-place upgrade lane (RFC 024 W5)`.

### Task T5.2: known-limitations policy + phase gate

**Files:**
- Modify: `docs/known-limitations.md` (§Upgrade, ~line 79)

- [ ] **Step 1:** Add to §Upgrade:

```markdown
**Upgrade support statement (0.x).** Upgrades are tested for exactly one
hop — latest published release → next (the `upgrade-e2e` workflow).
Skipping releases is best-effort. Upgrades are forward-only: `upgrade.sh`
uses no `--atomic` and there is no rollback path for hook Jobs, CRD
applies, or DB migrations — recovery is fix-and-re-run.

**Snapshot before infra upgrades.** Take a CNPG backup/snapshot before
Phase 2 (`datuplet-infra`) or Phase 4 (`datuplet-lakekeeper`) upgrades —
CNPG backups are not configured by the charts (see above), so this is a
manual step.
```

- [ ] **Step 2:** Trigger the workflow on the shared branch (`gh workflow run upgrade-e2e.yml --ref claude/modest-easley-f5173d`). jq-path or seed issues surface here. **⚠ Same bootstrap caveat as release-verify (F7):** the N-1 install pulls the latest published chart, whose built-in component images don't exist until a post-T6.3 release — so the seed step fails on `ImagePullBackOff` until then. Gate green only once a post-T6.3 release is the N-1; before that, run with a `-f-app` overlay pointing components at pullable images, or treat as a known, logged bootstrap failure.
- [ ] **Step 3:** Cumulative `mcp__codex__codex` review → clean. Commit on the shared branch. **No PR** (single PR at Final integration).

---

## Phase 6 — Renovate + dependency runbook

### Task T6.1: `renovate.json`

**Files:**
- Create: `renovate.json`

- [ ] **Step 1: Write the config**:

```json
{
  "$schema": "https://docs.renovatebot.com/renovate-schema.json",
  "extends": ["config:recommended", ":semanticCommits"],
  "schedule": ["before 6am on monday"],
  "labels": ["dependencies"],
  "prConcurrentLimit": 3,
  "packageRules": [
    { "matchManagers": ["helmv3"], "groupName": "helm chart dependencies",
      "postUpdateOptions": ["helmUpdateSubChartArchives"] },
    { "matchManagers": ["gomod"], "groupName": "go modules",
      "matchUpdateTypes": ["minor", "patch"],
      "postUpdateOptions": ["gomodTidy"] },
    { "matchManagers": ["dockerfile"], "groupName": "docker base images" },
    { "matchManagers": ["github-actions"], "groupName": "github actions" }
  ],
  "customManagers": [
    {
      "customType": "regex",
      "description": "Helper images pinned in chart values (kubectl, openfga CLI)",
      "managerFilePatterns": ["/^charts/.+/values\\.yaml$/"],
      "matchStrings": [
        "(?:kubectl|openfgaCli):\\s*\"(?<depName>[A-Za-z0-9./-]+):(?<currentValue>[A-Za-z0-9._-]+)\"",
        "image:\\s*(?<depName>[A-Za-z0-9./-]+/kubectl):(?<currentValue>[A-Za-z0-9._-]+)"
      ],
      "datasourceTemplate": "docker"
    }
  ]
}
```

- [ ] **Step 2: Validate**: `npx --yes --package renovate -- renovate-config-validator renovate.json` — expected: `Config validated successfully`. (Requires node on the dev machine; if unavailable, the orchestrator runs it.)
- [ ] **Step 3:** Note for the PR body (manual maintainer step, not automatable from the repo): install/enable the Renovate GitHub App for the repository. Every Renovate PR runs the full PR + e2e suite — that is the validation gate (RFC 024 W6).
- [ ] **Step 4: Commit** `chore(deps): Renovate config — grouped weekly helm/docker/go/actions bumps (RFC 024 W6, §9 Q4)`.

### Task T6.2: `docs/dependency-upgrades.md` + phase gate

**Files:**
- Create: `docs/dependency-upgrades.md`
- Modify: `CLAUDE.md` (Documentation pointers list), `docs/known-limitations.md` (link)

- [ ] **Step 1: Write the runbook.** Structure (fill each section with the checklist content below — this is the whole document, ~90 lines):

```markdown
# Dependency Upgrades

How to take a dependency bump (Renovate PR or manual). Mechanical
validation = CI (unit + helm lint + e2e). This page is the judgment
checklist per dependency — what CI cannot see.

## Lakekeeper (charts/datuplet-lakekeeper, dep `lakekeeper`)
1. Read the upstream release notes for: Iceberg REST API changes, authz
   (OpenFGA) model/config changes, STS/vended-credentials behavior, chart
   value renames.
2. Re-check the three datuplet touchpoints compile-and-pass against it:
   - `pkg/datagateway/lakekeeper/` (resolver: table create/load + vended creds)
   - `pkg/catalogwriter/` (REST client + VendedCreds)
   - `pkg/pipelineapi/storage/catalog_proxy.go` (catalog proxy)
3. `platform.fgaModelVersion` does NOT move with lakekeeper versions — it
   tracks Datuplet's FGA DSL only (docs/fga-model-upgrades.md). Bump it
   only if datuplet's model changed.
4. Gate: `make e2e-k8s` (full suite exercises catalog + STS paths).
5. Upgrade order on clusters: `upgrade.sh --phase lakekeeper` alone is
   fine; app does not need a simultaneous bump unless the REST API broke.

## CloudNativePG (charts/datuplet-operators, dep `cloudnative-pg`)
1. Read CNPG upgrade notes for operator→cluster compatibility (the
   operator must be upgraded before Cluster CRs that use new fields).
2. CNPG CRDs live in `charts/datuplet-operators/crds/` — refresh them from
   the upstream release when bumping minor versions, and remember helm
   never upgrades CRDs: clusters take them via `upgrade.sh --phase operators`.
3. Gate: e2e + on a scratch cluster verify `kubectl describe cluster pg`
   reconciles clean after `upgrade.sh --phase operators` then `--phase infra`.

## OpenFGA (charts/datuplet-infra, dep `openfga`)
1. Diff the chart's values surface (datastore config keys have moved
   between chart minors before).
2. FGA model semantics are pinned by datuplet's DSL + version tuple —
   an engine bump does not change them; authz-bootstrap's hash pin
   guards against silent drift.
3. Gate: e2e (authz-bootstrap + storage-browse scenarios cover FGA).

## MinIO (charts/datuplet-infra, dep `minio`)
1. Dev/e2e-only backend (production = external S3/GCS). Diff root-cred
   and persistence value names; `tests/e2e/values-infra.yaml` pins
   rootUser/rootPassword for the framework.
2. Gate: e2e.

## Helper images (chart values: kubectl, openfga/cli)
1. Bumps arrive via the Renovate regex manager. kubectl minor should stay
   within one minor of the cluster versions we target (K8s 1.28+).
2. Gate: e2e (every hook Job uses these images).

## Go modules / Dockerfile bases / GitHub Actions
Grouped weekly Renovate PRs; `make tidy` discipline applies (multi-module
repo). Gate: PR suite + e2e.
```

- [ ] **Step 2:** Link it: CLAUDE.md "Documentation pointers" gets `- Dependency bumps: [docs/dependency-upgrades.md](docs/dependency-upgrades.md).`; known-limitations.md's upgrade section links it.
- [ ] **Step 3: Phase gate:** `go build ./... && go test ./... -count=1` green; cumulative `mcp__codex__codex` review; commit on the shared branch. **No PR** (single PR at Final integration).

### Task T6.3: Close the component-image publishing gap (RFC 026 follow-through)

> **Added 2026-07-11 (answers "add ComponentDefinition registration to deploy scripts?").**
> Registration itself needs **no** script change: RFC 026 ships the six built-ins as
> ComponentDefinition CR chart templates (`charts/datuplet-app/templates/components/*.yaml`,
> gated on `components.builtins`, default true), so `helm install datuplet-app` —
> hence `install.sh` — already registers them. The real gap is **publishing**: those
> CRs pin `ghcr.io/kacurez/<comp>:v0.1.0` (`components.tag`), but `_release-image.yml`
> tags component images with the *platform* version, so `:v0.1.0` is never produced.
> A stock `--from-repo` install `ImagePullBackOff`s the built-ins (W3 release-verify
> catches this). Close it here. **Depends on T2.1 (image helper) + T2.4 (bump-version); blocks W3 and W5 going green — land right after Phase 2, before Phase 3.**
>
> **Code-graph note (2026-07-11):** the ComponentDefinition reconciler
> (`pkg/k8s/controllers/componentdefinition_controller.go`) validates spec *format*
> only — it marks the `:v0.1.0` built-ins `Valid` despite the missing image, so
> **neither `helm install` nor the reconciler flags this gap; only a real pipeline
> run does.** That is why T6.3 must land before W3/W5 can go green, and why there is
> no "just check status.phase" shortcut. Registration itself needs no script change
> (helm templates register the built-ins; a `pipeline-api admin component register
> --file` CLI exists for off-chart cases — `cmd/pipeline-api/admin_component.go`).

> **Two distinct sub-gaps (F3 of the 2026-07-11 codex review — verified):**
> **(a) tag mismatch** — the 5 built-ins that DO get built (`data-generator`,
> `sql-transform`, `stdout-writer`, `http-json-extractor`, `finnhub-extractor`) are
> pinned to `components.tag: v0.1.0` while `_release-image.yml` publishes them at the
> *platform* version; **(b) `pandas-transform` has no image at all** — it ships as a
> built-in ComponentDefinition template (`charts/datuplet-app/templates/components/pandas-transform.yaml`)
> but is absent from `_release-components.yml` (5 jobs, no pandas) *and* from the
> local `docker-build-k8s` set (`Makefile` comments it out: "no build wired anywhere
> yet"). So even option A leaves `pandas-transform` permanently unpullable. Both must
> be closed. **Depends on T2.1/T2.4; blocks P3 (W3) and P5 (W5) — land right after Phase 2.**

**Files:**
- Modify: `charts/datuplet-app/values.yaml` (`components.tag`), `Makefile` (`bump-version` from T2.4), `.github/workflows/_release-components.yml` (+`_release-image.yml` reuse) and `docker-build-k8s`/`build-components-e2e` if wiring pandas, or `charts/datuplet-app/templates/components/pandas-transform.yaml` (+ `values.yaml`) if disabling it; `docs/components.md`

- [ ] **Step 1: Decide the axis (maintainer).** Option A: track the platform version
  — set `components.tag` to the release version like the service images. Option B: a
  `components-vX.Y.Z` release train that publishes `:vX.Y.Z` component images +
  a `componentdefinitions.yaml` artifact (RFC §6.6). Recommend **A now, B when a
  component needs to ship off-cadence**.
- [ ] **Step 2 (option A) — tag mismatch (sub-gap a):** keep `components.tag` an
  explicit value the release bumps alongside `Chart.yaml` in `make bump-version`
  (extend T2.4's target to also set it). The ComponentDefinition CEL rule requires a
  `vX.Y.Z`-shaped stable version, so `components.tag` must be `v<release>` (e.g.
  `v0.8.0`), not `0.8.0`, and appVersion (`"1.0"` today) is NOT `vX.Y.Z`-shaped — so
  `components.tag` cannot simply default to appVersion; the bump-version approach is
  required. (Do NOT route it through T2.1's service-image helper for the same reason.)
- [ ] **Step 3 — `pandas-transform` (sub-gap b), pick one:**
  - **wire it**: add a `pandas-transform` job to `_release-components.yml`, and add its
    `docker build` to `docker-build-k8s` + `build-components-e2e` (+ the e2e/upgrade-e2e
    kind-load lists); OR
  - **disable the built-in**: gate the `pandas-transform.yaml` template behind a
    separate value (default off) or delete it until an image exists — so a stock
    `builtins:true` install ships only built-ins whose images are published.
  Recommend disable-until-published (smaller, honest) unless pandas-transform is
  actually needed at launch.
- [ ] **Step 4: Local/e2e tag consistency (closes F2's tail).** With built-ins now
  pinned to `components.tag: v<release>`, the **local** path must resolve too: today
  `docker-build-k8s` tags components `datuplet/<comp>:latest`, but the T1.3 local
  overlay sets `components.registry: datuplet` with the chart's `v<release>` tag →
  mismatch, and `:latest` can't be used for a *stable* built-in (reconciler ban). Pick
  one and record it in T1.3's overlay + the Makefile: (i) local dev uses
  `components.builtins: false` + dev-version registration (the e2e pattern — cleanest,
  mutable `:latest` allowed on prerelease), or (ii) `docker-build-k8s` tags local
  component images at the same `v<release>` the overlay pins. Do the same for the
  upgrade-e2e overlay (T5.1). Verify a built-in pipeline actually runs on `make deploy-local`.
- [ ] **Step 5: Verify** no built-in resolves to an unpublished image:
  `helm template datuplet-app charts/datuplet-app | grep -oE 'ghcr.io/kacurez/[a-z-]+:[^"]+' | sort -u`
  — every tag equals the (v-prefixed) release version, and every component appears in
  the release job list. Commit `fix(charts): close RFC 026 component-image publish gap — tag sync + pandas-transform (RFC 024 T6.3)`.
- [ ] **Step 6:** If option B is chosen instead, this becomes its own phase — see RFC §6.6; do not inline the train here.

---

## Phase 7 — e2e effectiveness

> **Sequencing:** this phase is independent of Phases 0–6 and is
> **recommended to land first**. RFC 024's own gates (W3/W5/W7) and future work all
> rely on `make e2e-k8s`, which is currently green-but-vacuous (CI run 28856550215 =
> 3 passed / ~24 skipped, every pipeline scenario included). Fixing that first makes
> every subsequent gate trustworthy. Evidence + design: RFC §2.5 correction, P8, §6.7.

### Task T7.1: Generalize `PreCheck` (drop the OrbStack context sniff)

**Files:**
- Modify: `tests/e2e/framework/setup.go` (whole `PreCheck` body)

**Interfaces:**
- Produces: `PreCheck()` that works on ANY kubectl context (kind, OrbStack, minikube); optional `DATUPLET_E2E_CONTEXT` pin that **fails** on mismatch (never switches — the current code silently runs `kubectl config use-context`, mutating developer kubeconfig). T7.2 converts its callers.

- [ ] **Step 1: Replace the `PreCheck` body** (complete function):

```go
// PreCheck validates that the K8s infrastructure is available on the
// CURRENT kubectl context. It never switches contexts. Set
// DATUPLET_E2E_CONTEXT to guard against running the suite against an
// unintended cluster (mismatch = error, not a silent switch).
func PreCheck() error {
	out, err := exec.Command("kubectl", "config", "current-context").Output()
	if err != nil {
		return fmt.Errorf("kubectl context not available: %w", err)
	}
	current := strings.TrimSpace(string(out))
	if want := os.Getenv("DATUPLET_E2E_CONTEXT"); want != "" && current != want {
		return fmt.Errorf("kubectl context is %q, DATUPLET_E2E_CONTEXT wants %q — switch manually", current, want)
	}

	// The real availability check: the operator Deployment exists in the
	// e2e namespace (name is not release-prefixed — the chart renders a
	// bare `pipeline-operator`).
	ns := os.Getenv("DATUPLET_E2E_NAMESPACE")
	if ns == "" {
		ns = "datuplet-e2e"
	}
	if err := exec.Command("kubectl", "get", "deploy", "pipeline-operator", "-n", ns).Run(); err != nil {
		return fmt.Errorf("pipeline-operator not deployed in %s namespace (context %s): %w", ns, current, err)
	}
	return nil
}
```

  Update the package doc comment (lines 1-4) to drop the OrbStack wording.

- [ ] **Step 2:** `cd tests/e2e && go vet ./... && go build ./...` — green. On OrbStack: `make e2e-k8s` still green (context no longer sniffed, deploy check unchanged).
- [ ] **Step 3: Commit** `fix(e2e): PreCheck works on any kubectl context — kills the CI silent-skip root cause (RFC 024 W7)`.

### Task T7.2: Fail-closed CI mode — `E2E_REQUIRE=1` + `SkipOrFail` sweep

**Files:**
- Create: `tests/e2e/framework/require.go`
- Modify: `tests/e2e/main_test.go`, `tests/e2e/scenarios_test.go`, `scenarios_query_test.go`, `scenarios_audit_test.go`, `scenarios_local_query_test.go`, `scenarios_remote_cli_test.go`, `.github/workflows/e2e.yml` (env)

**Interfaces:**
- Produces: `framework.SkipOrFail(t, format, args...)` — skips locally, `t.Fatalf`s when `E2E_REQUIRE=1`. CI (e2e.yml) sets `E2E_REQUIRE: "1"` at the job level. T7.5/T7.6 rely on the classification below.

- [ ] **Step 1: `require.go`** (complete file):

```go
package framework

import (
	"os"
	"testing"
)

// SkipOrFail marks an infrastructure-availability gap. Locally it skips
// (fast iteration against a partial stack); under E2E_REQUIRE=1 (set by
// CI) it FAILS — a green CI run must mean the suite actually ran.
// Deliberate gates (opt-in proofs, environment-capability detection,
// the E2E_K8S mode switch) stay plain t.Skip.
func SkipOrFail(t *testing.T, format string, args ...any) {
	t.Helper()
	if os.Getenv("E2E_REQUIRE") == "1" {
		t.Fatalf("E2E_REQUIRE=1, refusing to skip: "+format, args...)
	}
	t.Skipf(format, args...)
}
```

- [ ] **Step 2: `main_test.go`** — under require mode, a bootstrap failure aborts the run instead of arming universal skips. After the `SetupFGABootstrap` error branch's `fmt.Fprintf`, add:

```go
			if os.Getenv("E2E_REQUIRE") == "1" {
				fmt.Fprintln(os.Stderr, "E2E: E2E_REQUIRE=1 — treating bootstrap failure as fatal")
				os.Exit(1)
			}
```

- [ ] **Step 3: Classification sweep.** Convert every *infrastructure-availability* skip to `framework.SkipOrFail`; leave *deliberate* gates as `t.Skip`. The authoritative inventory (from `grep -rn 't.Skip' tests/e2e`):
  - **Convert** (availability): "SharedHarness nil" (all sites), "precheck failed" (all sites — incl. the shared gate in the `TestScenarios` runner, find it via `grep -n 'PreCheck\|precheck' tests/e2e/scenarios_test.go tests/e2e/framework/scenario.go` and convert THERE so every subtest inherits it), "pipeline-api not reachable" (all sites), "admin session login failed", "query-worker Deployment not found" / "0 ready replicas", "docker not available", "admin-creds Secret not found", "./bin/datuplet not found".
  - **Keep `t.Skip`** (deliberate): `E2E_K8S=1 required` (mode gate — the suite doubles as unit-runnable), `RFC011_BIG_DATA_PROOF=1 to run` (opt-in), the NetworkPolicy kindnet-capability skip (`scenarios_query_test.go:~1156`), and the two FGA-matrix permanent skips (revived in T7.5 — until then they stay documented skips).
- [ ] **Step 4: `e2e.yml`** — add to the `e2e` job:

```yaml
    env:
      E2E_REQUIRE: "1"
```

- [ ] **Step 5:** `cd tests/e2e && go vet ./... && go build ./...`; on OrbStack `make e2e-k8s` — behaviour unchanged locally (no env set). Commit `feat(e2e): fail-closed CI mode — infra-availability skips become failures under E2E_REQUIRE=1 (RFC 024 W7)`.

### Task T7.3: Supervised port-forwards + `go test -json` summary in CI

**Files:**
- Modify: `scripts/e2e-port-forward.sh` (supervision + JSON output), `.github/workflows/e2e.yml` (summary step)

**Interfaces:**
- Produces: port-forwards that survive pod rollouts (the query suite rolls pipeline-api mid-run via `kubectl set env`); a `$GITHUB_STEP_SUMMARY` table of pass/fail/skip with reasons. Consumes T7.2's `E2E_REQUIRE`.

- [ ] **Step 1: Supervision.** In `e2e-port-forward.sh`, replace `maybe_pf`'s single background `kubectl port-forward` with a respawn loop (the PID tracked is the supervisor's; `cleanup` unchanged):

```bash
# supervise_pf <local_port> <svc> <target_port> <name> — respawn the forward
# whenever it dies (kubectl port-forward exits when its target pod restarts;
# the query e2e rolls pipeline-api mid-suite).
supervise_pf() {
	while true; do
		kubectl port-forward -n "$NS" --address 127.0.0.1 "svc/$2" "$1:$3" >/dev/null 2>&1 || true
		echo "e2e: port-forward $4 (localhost:$1) exited; respawning in 1s" >&2
		sleep 1
	done
}

maybe_pf() {
	if port_open "$1"; then
		echo "e2e: $4 already reachable on localhost:$1 (skipping port-forward)"
		return 0
	fi
	echo "e2e: supervised port-forward svc/$2 $1:$3 (ns $NS)"
	supervise_pf "$1" "$2" "$3" "$4" &
	PF_PIDS+=("$!")
	wait_port "$1" "$4"
}
```

  `cleanup()` must also kill the supervisors' children: change the `kill "$pid"` line to `kill "$pid" 2>/dev/null; pkill -P "$pid" 2>/dev/null || true`.

- [ ] **Step 2: JSON stream.** Change the final `go test` invocation to emit both human and machine output when `E2E_JSON` is set:

```bash
if [ -n "${E2E_JSON:-}" ]; then
	E2E_K8S=1 \
		DATUPLET_OPENFGA_URL="http://localhost:8180" \
		DATUPLET_LAKEKEEPER_URL="http://localhost:8181" \
		DATUPLET_PIPELINE_API_URL="http://localhost:30081" \
		go test -v -count=1 -timeout 30m -json ./... | tee "$E2E_JSON" | \
		grep -E '"Action":"(pass|fail|skip)"' --line-buffered | \
		jq -r 'select(.Test != null) | "\(.Action)\t\(.Test)"' || exit 1
else
	E2E_K8S=1 \
		DATUPLET_OPENFGA_URL="http://localhost:8180" \
		DATUPLET_LAKEKEEPER_URL="http://localhost:8181" \
		DATUPLET_PIPELINE_API_URL="http://localhost:30081" \
		go test -v -count=1 -timeout 30m ./...
fi
```

  ⚠ pipefail note: with `set -euo pipefail`, `go test`'s non-zero exit propagates through the pipe — verify with a deliberately failing test locally.

- [ ] **Step 3: e2e.yml summary step** (after the `make e2e-k8s-deploy` step; the make step gains `E2E_JSON=/tmp/e2e-report.json` in its env):

```yaml
      - name: Test summary
        if: always()
        run: |
          set -eu
          [ -f /tmp/e2e-report.json ] || { echo "no e2e report produced" >> "$GITHUB_STEP_SUMMARY"; exit 0; }
          {
            echo "## e2e results"
            echo '| outcome | count |'; echo '|---|---|'
            for a in pass fail skip; do
              n=$(jq -r --arg a "$a" 'select(.Action==$a and .Test!=null) | .Test' /tmp/e2e-report.json | wc -l)
              echo "| $a | $n |"
            done
            echo; echo "### skipped"
            jq -r 'select(.Action=="skip" and .Test!=null) | "- \(.Test)"' /tmp/e2e-report.json
          } >> "$GITHUB_STEP_SUMMARY"
```

- [ ] **Step 4:** `bash -n scripts/e2e-port-forward.sh`; YAML-parse e2e.yml; commit `ci(e2e): supervised port-forwards + test-outcome summary (RFC 024 W7)`.

### Task T7.4: Hermetic HTTP fixture — replace jsonplaceholder

**Files:**
- Create: `tests/e2e/manifests/http-fixture.yaml`, `tests/e2e/manifests/posts.json`, `tests/e2e/manifests/users.json`
- Modify: **every** `tests/e2e` reference to `jsonplaceholder` — enumerate with `grep -rln jsonplaceholder tests/e2e/` (F6 of the 2026-07-11 codex review: it's not just the 10 pipeline YAMLs under `tests/e2e/pipelines/k8s/` — also `tests/e2e/scenarios/gcs-pipeline-k8s/pipeline.yaml`, and any inline refs like `tests/e2e/scenarios_secrets_test.go`; and both `/posts` **and** `/users` paths are used — `multi-table-join.yaml`, `multi-component-stage.yaml` hit `/users`). Plus `Makefile` `e2e-k8s-deploy` (apply step) and `.github/workflows/e2e.yml` (preload the nginx image).

**Interfaces:**
- Produces: in-cluster `http://e2e-http-fixture.datuplet-e2e.svc.cluster.local/{posts,users}` serving committed snapshots of the jsonplaceholder payloads — zero external network in the e2e data path.

- [ ] **Step 1: Snapshot both payloads** (one-time, committed): `curl -fsS https://jsonplaceholder.typicode.com/posts > tests/e2e/manifests/posts.json` and `.../users > tests/e2e/manifests/users.json`. Verify posts is a 100-object array (`userId,id,title,body`) and users a 10-object array (`jq 'length'`). Scenario assertions must keep passing: `grep -rn '100\|rowCount\|RowCount' tests/e2e/framework/verifier.go tests/e2e/scenarios_test.go | head` — confirm expected counts match the snapshots (they were written against the same payloads).

- [ ] **Step 2: `http-fixture.yaml`** (complete manifest; ConfigMap for nginx config only — the JSON files are applied from file to keep the YAML small):

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: e2e-http-fixture-nginx
data:
  default.conf: |
    server {
      listen 80;
      location /posts {
        default_type application/json;
        alias /data/posts.json;
      }
      location /users {
        default_type application/json;
        alias /data/users.json;
      }
    }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: e2e-http-fixture
spec:
  replicas: 1
  selector: {matchLabels: {app: e2e-http-fixture}}
  template:
    metadata: {labels: {app: e2e-http-fixture}}
    spec:
      containers:
        - name: nginx
          # ECR Public mirror — no Docker Hub anonymous-pull rate limits.
          image: public.ecr.aws/nginx/nginx:1.27-alpine
          ports: [{containerPort: 80}]
          volumeMounts:
            - {name: conf, mountPath: /etc/nginx/conf.d}
            - {name: data, mountPath: /data}
      volumes:
        - {name: conf, configMap: {name: e2e-http-fixture-nginx}}
        - {name: data, configMap: {name: e2e-http-fixture-data}}
---
apiVersion: v1
kind: Service
metadata:
  name: e2e-http-fixture
spec:
  selector: {app: e2e-http-fixture}
  ports: [{port: 80, targetPort: 80}]
```

- [ ] **Step 3: Deploy wiring.** In `Makefile` `e2e-k8s-deploy`, after the helm installs / before the test run:

```makefile
	kubectl -n datuplet-e2e create configmap e2e-http-fixture-data \
	  --from-file=posts.json=tests/e2e/manifests/posts.json \
	  --from-file=users.json=tests/e2e/manifests/users.json \
	  --dry-run=client -o yaml | kubectl apply -n datuplet-e2e -f -
	kubectl apply -n datuplet-e2e -f tests/e2e/manifests/http-fixture.yaml
	kubectl -n datuplet-e2e rollout status deploy/e2e-http-fixture --timeout=60s
```

  In `e2e.yml`, add `public.ecr.aws/nginx/nginx:1.27-alpine` to the image preload: `docker pull` it and append it to the `kind load docker-image` loop.

- [ ] **Step 4: URL swap + prove it's total.** Replace the host `https://jsonplaceholder.typicode.com` → `http://e2e-http-fixture.datuplet-e2e.svc.cluster.local` everywhere it appears under `tests/e2e/` (preserving the `/posts` and `/users` paths). Cross-namespace Service DNS FQDN is used because component pods run in per-project namespaces. **Verification is zero-remaining-matches**, not a fixed file count: `grep -rl jsonplaceholder tests/e2e/ | grep -v manifests/` must return nothing (the committed snapshots under `manifests/` may keep the name in a provenance comment; exclude them).

- [ ] **Step 5:** On OrbStack: `make e2e-k8s` — scenarios pass against the fixture. Commit `test(e2e): hermetic in-cluster HTTP fixture replaces jsonplaceholder (RFC 024 W7)`.

### Task T7.5: Revive the FGA-matrix tests + enable remote-CLI in CI

**Files:**
- Modify: `tests/e2e/framework/bootstrap.go` (project-row seeding), `tests/e2e/scenarios_test.go:353,373` (un-skip), `tests/e2e/scenarios_remote_cli_test.go` (namespace/release parameterization), `.github/workflows/e2e.yml` (build the CLI)

- [ ] **Step 1: FGA-matrix revival.** The skip texts document the exact gap: the harness writes FGA tuples but never seeds a Datuplet **project DB row** matching `h.LakekeeperProjectID`, so `mustHaveRelation` 404s before authz fires. Read the two test docstrings (`scenarios_test.go:340-380`) for the written revival plan; implement in `SetupFGABootstrap`: create the project through pipeline-api's admin path (`kubectl exec … pipeline-api admin create-project` — the bootstrap already execs admin commands, see the run-log evidence) so the DB row + FGA tuple share the ID, then delete both `t.Skip` calls. If the revival exceeds ~a day of effort, DELETE both tests instead and note it in the PR — a permanently-skipped test is dead weight (maintainer picks in PR review).
- [ ] **Step 2: Remote-CLI in CI.** `.github/workflows/e2e.yml` gains a `make build` step (produces `./bin/datuplet`; cheap, cached Go). In `scenarios_remote_cli_test.go`, replace the hardcoded helm namespace/release constants (~line 100 region — the skip message "requires a Helm install (run via make e2e-remote-cli, not make e2e-k8s)" is stale: `make e2e-k8s-deploy` IS a helm install, in `datuplet-e2e`) with `DATUPLET_E2E_NAMESPACE` (existing env) + `DATUPLET_E2E_RELEASE` (new, default `datuplet-app`). The docker-availability and binary checks became `SkipOrFail` in T7.2 — CI now satisfies both, so the test runs.
- [ ] **Step 3:** On OrbStack: `make e2e-k8s` green incl. the revived tests. Commit `test(e2e): revive FGA-matrix scenarios + run remote-CLI in CI (RFC 024 W7)`.

### Task T7.6: Phase 7 gate

- [ ] **Step 1:** `make tidy && go build ./... && go test ./... -count=1` green; `cd tests/e2e && go vet ./...`.
- [ ] **Step 2:** Verify locally on OrbStack first: `E2E_REQUIRE=1 make e2e-k8s` (or via the port-forward wrapper) — job green **and** every remaining skip is on the deliberate-gate allowlist (`BigDataJoinProof`, NetworkPolicy-capability). The *CI* proof (e2e.yml running with `E2E_REQUIRE=1`, ~3-executed → full-set) lands when the single final PR opens (Final integration) — capture that run's step-summary table in the PR body. **No per-phase PR.**
- [ ] **Step 3:** On OrbStack: `make e2e-k8s` — local dev flow unchanged (skips still allowed without `E2E_REQUIRE`).
- [ ] **Step 4:** Cumulative `mcp__codex__codex` review → zero CRITICAL/MAJOR. Commit on the shared branch. **No PR** (single PR at Final integration).

---

## Final integration — the one PR

Run **once, after every phase is committed** on `claude/modest-easley-f5173d`
(recommended order: 7 → 0 → 1 → 2 → T6.3 → 3 → 4 → 5 → 6).

- [ ] **Step 1: Whole-branch verification.** `make tidy && go build ./... && go test ./... -count=1` green; `make e2e-k8s` green on OrbStack; `scripts/verify-versions.sh` clean. Confirm the RFC docs (`docs/superpowers/specs/2026-07-04-rfc-024-*.md` + this plan) are committed too.
- [ ] **Step 2: Final cumulative Codex review** over the entire `main..HEAD` diff via `mcp__codex__codex` (read-only, high reasoning) → zero CRITICAL/MAJOR.
- [ ] **Step 3: Push + open ONE draft PR** to `main`:
  `git push -u origin claude/modest-easley-f5173d` then
  `gh pr create --draft --title "RFC 024: deployment & release simplification" --body "<body>"`.
  The body must include: the phase list (0–7 + T6.3) with what each delivered; the
  **carried-forward release note from T2.4** (post-merge, releases require
  `make bump-version` before tagging — the sed guard is gone); the **Phase 7 CI proof**
  (e2e.yml ran with `E2E_REQUIRE=1`, ~3-executed → full set, skip-summary table); and a
  note that this PR also closes the urgent `components.tag` release-guard breakage (§2.2).
- [ ] **Step 4: The PR's own CI is the acceptance gate** — e2e (now fail-closed), pr.yml
  (incl. verify-versions), and — on the first tag cut after merge — release-verify. Do
  **not** merge on red. Maintainer merges (repo rule; agents never push `main`/tag).

Rationale for one PR (maintainer decision 2026-07-11): the phases are tightly
coupled (W1 scripts underpin W3/W5; W2 defaults underpin release-verify; T6.3
unblocks W3/W5), so a single reviewable unit avoids a broken intermediate `main`
and the eight-PR merge dance. The per-phase Codex gates keep each step honest
*within* the branch; the final review + PR CI is the integration gate.

---

## Actioned reconciliation log

The pre-execution checklist below was **actioned against merged main @ `156e9a1`
on 2026-07-11** (PRs #24/#25/#26 all merged). Each item records its outcome. Both
pre-dispatch chores are now **done**: anchors re-grepped (item 11a), and a Codex
review ran with its 6 confirmed findings folded in (item 12). Only maintainer
green-light remains.

1. **T0.1 — ✅ resolved.** RFC 026 already deleted `examples/k8s/` **and**
   `sample-pipeline-secret.yaml` (secretsRef removed). T0.1 rewritten: no relocation,
   just delete `utils/deploy` (now 14 files) + repoint code comments. `docs/secrets.md`
   no longer references the tree.
2. **T0.1/T0.2 — ⚠ confirmed + widened.** RFC 026 kept **both** CRD copies and added
   a **third** CRD (`componentdefinitions`) to each. `utils/deploy` deletion now covers
   it; T0.2's `k8s-reload-crds` fix now applies the whole `charts/datuplet-app/crds/`
   dir (the fossil target only listed 2 of 3 CRDs by path). Diff the copies before
   deleting (chart copy canonical).
3. **T3.2/T5.1 — ✅ actioned.** `image:` is gone from ComponentSpec; the verify
   pipeline now uses `component:` refs with **no** image templating (T3.2 rewritten).
   upgrade-e2e's post-upgrade run resolves built-ins against HEAD-built local images
   via a new `tests/upgrade-e2e/values-app.yaml` overlay (`components.builtins:true`,
   `registry:datuplet tag:latest`) — see T5.1's reconciliation note.
4. **T2.1 — ✅ actioned + narrowed.** The service-image sweep is now **four** images
   (iceberg-job deleted by PR #26). The built-in ComponentDefinition CRs are
   deliberately **excluded** from the appVersion sweep (their `components.tag` is an
   independent axis); W4's `:latest` render-check passes over them (they're
   `:v0.1.0`, neither `:latest` nor `datuplet/`).
5. **T0.3/T1.4 — partially resolved.** RFC 026 repointed example references, but the
   three stale install.md claims (0.1.0 wording, `tests/local`, MinIO repo URL) are
   **all still present** (verified) — T0.3 stands unchanged.
6. **T5.1 — ✅ actioned.** Seed + post-upgrade pipelines use `component:` refs (T5.1
   updated). The N-1 seed depends on T6.3 (component-image publish gap) being closed —
   noted inline as the failure signal.
7. **Component release train — ⚠ still open, now concrete (T6.3 added).** RFC 026 did
   **not** ship a component train; `_release-components.yml` still builds all five
   component images at the **platform** tag, while ComponentDefinitions pin `:v0.1.0`
   → unpullable built-ins. New **T6.3** closes it (option A: track platform version;
   option B: the `components-v*` train). This is the one piece of deployment work the
   merged RFC 026 left on the table.
8. **RFC 025 query surface — ✅ verified minimal.** `DATUPLET_QUERY_WAREHOUSE` and the
   post-install warehouse step are gone (025 Phase 0). Re-anchor install.md/ad-hoc-query
   docs when T0.3/T1.4 run (re-grep). `tests/e2e/values-app.yaml`'s query block shrank —
   T2.3 says re-check current content rather than assuming.
9. **T2.1 — query-worker template — ✅ noted.** 025 edited
   `query-worker/deployment.yaml`; T2.1 now says re-grep the image line rather than
   trusting `:69`.
10. **query-worker default-on — ✅ confirmed** (`queryWorker.enabled: true`, "RFC 025
    Q1"). Stock installs + release-verify + upgrade-e2e now exercise the worker for
    free; reflected in the release-verify/upgrade-e2e tasks (no restructuring).
11a. **✅ Anchors re-grepped (2026-07-11)** against `156e9a1`: T0.1's six
    utils/deploy refs exact; T2.1's image call sites exact (4 svc + queryWorker +
    infra, no icebergJob); release.yml pin-step `:61`; `components.tag` `:272`. Fixed
    cosmetic drift: operator deployment name `:22`→`:23`, Makefile target line-nums
    (~281/~289/~124), app kubectl `:276-278`, install.md `tests/local` `~158`.
12. **✅ Codex review done (2026-07-11)** via `mcp__codex__codex` (read-only, high
    reasoning) — 6 findings, all verified against the repo and **folded in**:
    F1 (install.sh must apply CRDs — current e2e-k8s-deploy does, Makefile:178) → T1.1
    `apply_crds`; F2 (local overlay lost `--set components.registry=datuplet`,
    Makefile:252) → T1.3 overlay; F3 (`pandas-transform` shipped as built-in but built
    /released nowhere — 5 release jobs, Makefile skips it; plus the v0.1.0 tag
    mismatch) → T6.3 broadened + CI drift guard; F4 (upgrade-e2e's N-1 needs a
    post-T6.3 published release — T6.3 can't retro-fix published charts) → T5.1 gate;
    F5 (T6.3 depends on T2.1/T2.4, not independent) → DAG fixed; F6 (fixture missing
    `/users`) → T7.4 covers both paths, verify = zero remaining matches.
13. **✅ Second Codex review (2026-07-11, after the single-PR model change + spec/plan
    commit `602de9e`)**: F1/F3/F4/F6 confirmed resolved; 8 mostly spec↔plan drift issues
    folded in — spec §8 rewritten to the one-branch/one-PR model (was "each phase
    independently landable / same PR"); §6 "six"→"seven workstreams"; §6.7 + Phase-7
    row de-stale'd ("first within RFC 024", not "before RFC 025/026"); §8 Phase-0 drops
    the moot sample-secret relocation; §5.2/§6.6/§8 now distinguish the deferred
    component *train* from T6.3's immediate publish-gap fix; plan T6.3 index-row + intro
    corrected to depend on T2.1/T2.4 (F5 tail); plan T6.3 Step 4 added for local/e2e tag
    consistency (F2 tail); interim release-verify/upgrade-e2e gates flagged to expect a
    loud skip until the first post-T6.3 release (F7).

## Self-review checklist (run before handing off)

- Spec coverage: §6.1→T1.1/T1.2 (+recovery policy text in upgrade.sh header + T5.2), §6.2→T2.1–T2.4, §6.3→T3.1–T3.4 (+separate-workflow rationale in the YAML comment), §6.4→T4.1, §6.5→T5.1/T5.2, §6.6→T6.1/T6.2 **+ T6.3** (RFC 026 shipped the registry but not component-image publishing; T6.3 closes that gap — the merged-reality delta), §8 Phase 0→T0.1–T0.5. pullPolicy flip (§9 Q2)→T2.2. Bump-before-tag (§9/external review F1)→T2.4. Phase-aware CRDs (F4)→T1.2. Values surface on upgrade.sh (F6)→T1.2/T5.1. kind port mapping (F5)→T3.3/T5.1. v-prefix normalization (F3)→T1.1/T1.2 (`${2#v}`). Workflow-review fixes (2026-07-07: release concurrency, fga-version-check hardening)→T0.5; the non-root-module CI test gap is deliberately out of scope (spun off separately). P8/§6.7 (e2e effectiveness)→T7.1–T7.6: precheck generalization→T7.1, fail-closed+SkipOrFail→T7.2, supervised forwards+summary→T7.3, hermetic fixture→T7.4, dead-test revival+remote-CLI→T7.5; kind retained per §6.7 (minikube rejected).
- No placeholders: every created file is complete; the one deliberate lookup (T3.2 jq field paths) cites the exact doc lines + handler files to confirm against and has a local verification step that catches mismatches before CI.
- Type consistency: install.sh/upgrade.sh flag names identical everywhere they're called (T1.3 wrappers, T3.3, T5.1); `run-pipeline.sh` env contract (`BASE_URL`, `DATUPLET_ADMIN_EMAIL/PASSWORD`, `/tmp/datuplet-last-run-id`) consistent between T3.2, T3.3, T5.1; `SMOKE_URL` consistent between T3.1 and both workflows; helm release/chart names consistent throughout.
