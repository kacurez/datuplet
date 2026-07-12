# RFC 024 — Deployment & Release Simplification

- **Status**: **Unblocked — ready to execute** (2026-07-11). RFC 025 (PR #25, merge `9636585`) and RFC 026 (PR #24, merge `968ffbd`) are merged; PR #26 (`156e9a1`) deleted TableCommit + local execution. Design approved as v3 (§9 resolved; codex gpt-5.5 review incorporated). §2 facts + the plan's checklist **re-verified against merged main @ `156e9a1` on 2026-07-11** — deltas folded in below and into the plan (`docs/superpowers/plans/2026-07-04-rfc-024-deployment-implementation.md`). The pre-025/026 "Pre-execution revisit checklist" is now an *actioned reconciliation log* (plan bottom).
- **⚠ Urgent, independent of this RFC**: `main` **cannot cut a release right now**. RFC 026 added `components.tag: v0.1.0` to `charts/datuplet-app/values.yaml`; release.yml's pin-step guard aborts on any non-`latest` `tag:` line, so the next `v*` tag fails at "Pin chart image tags". No release has been cut since RFC 026 merged, so it is latent. W2 (§6.2) removes the whole sed mechanism and fixes it structurally; until W2 lands, a one-line guard hotfix is needed before any release (see §2.2).
- **Renumbering note**: the component-registry/config-safety RFC formerly numbered 023 in early drafts is **RFC 026** (merged); RFC 023 is the runs-UX work (PR #23), no deployment surface.
- **Date**: 2026-07-04 (reconciled 2026-07-11)
- **Related**: RFC 026 (ComponentDefinition registry — **merged**; ships built-in ComponentDefinition CRs as chart templates, §2.7); RFC 025 (query service — **merged**; query-worker now default-on, `DATUPLET_QUERY_WAREHOUSE` removed)
- **Related docs**: `docs/install.md`, `docs/fga-model-upgrades.md`, `docs/known-limitations.md`

## 1. Summary

Datuplet deploys as four helm charts plus `register.sh` (the 5-phase install), and a
tag push releases ten multi-arch images to ghcr.io plus four charts to gh-pages.
The **phase model itself is sound** — each phase maps to a distinct upgrade cadence,
which is exactly what "mostly we only update Datuplet, deps rarely change" needs.
This RFC keeps it.

What is missing is the reliability engineering *around* the phase model:

1. **Nothing ever installs the published artifacts.** e2e builds images from source
   and installs charts from the working tree. The helm repo + ghcr images that a
   first-time user consumes are exercised by no CI job.
2. **Nothing ever tests an upgrade.** Every CI install is from-scratch. Postgres
   migrations, FGA pin/hash flow, hook ordering, and CRD schema changes are
   upgrade-path code with zero upgrade-path coverage — and helm silently
   **does not upgrade CRDs** shipped in `crds/`, so today a `helm upgrade
   datuplet-app` can deploy controllers whose CRD schema was never applied.
3. **Charts in git are not the charts users get.** The release workflow
   sed-rewrites `values.yaml` (image repo + tag) and all four `Chart.yaml`s at
   release time. Local dev depends on the unpinned git state; production depends
   on the sed output; a grep guard protects the sed from itself.
4. **Version knowledge is scattered and partly unenforced**: chart dependency pins
   in four `Chart.yaml`s, `fgaModel.version` duplicated across two charts with a
   documented CI gap, `bitnami/kubectl:latest` floating, `minio: ~5.4.0` floating
   with no committed `Chart.lock` — and an entire **pre-helm raw-manifest deploy
   tree** (`utils/deploy/k8s/`, 14 files, including a second already-diverged copy
   of the CRDs) still wired into Makefile dev targets and docs.
5. **The install procedure exists in ~5 hand-rolled copies** and they have already
   drifted (details in §3, P6).

The proposal (§6) is seven workstreams on top of the existing structure — an
`install.sh`/`upgrade.sh` single entrypoint, release-time pinning moved into the
charts themselves, a release-verify job, an upgrade e2e lane, CI version-sync
checks, Renovate-driven dependency bumps, and an e2e-effectiveness fix (the CI
e2e gate is currently green-but-vacuous — §2.5, P8, §6.7) — plus an explicit
answer to "should components move to their own repo?" (**not now; RFC 026 makes
it nearly free later** — §5.2, §6.6).

## 2. Current state (verified against merged main @ `156e9a1`, 2026-07-11)

### 2.1 Release artifacts

Tag push `v*.*.*` (latest observed: `v0.7.1`) triggers `.github/workflows/release.yml`:

| Bucket | Artifacts | CI jobs |
|---|---|---|
| services (`_release-services.yml`) | pipeline-api, pipeline-observer, pipeline-operator, gateway, query-worker | 5 × (amd64 + arm64 + manifest) = 15 |
| components (`_release-components.yml`) | data-generator, sql-transform, stdout-writer, http-json-extractor, finnhub-extractor | 5 × 3 = 15 |
| charts | datuplet-operators, -infra, -app, -lakekeeper → chart-releaser → gh-pages (`packages_with_index=true`, no GitHub Releases entries) | 1 |

(PR #26 deleted the `iceberg-job` image + its release job — services dropped 6→5. Component images are still tagged with the **platform** release version via `_release-image.yml`; see §6.6 for why RFC 026's `components.tag: v0.1.0` pin makes that a real gap.)

Every image is rebuilt and every chart republished on every tag, regardless of what
changed. Images get `:X.Y.Z`, `:X.Y`, `:latest` tags.

### 2.2 Release-time chart mutation

`release.yml` "Pin chart image tags" step:

- sed-rewrites `repository: datuplet/<img>` → `ghcr.io/<owner>/<img>` for 5 images in
  `datuplet-app/values.yaml` (`pipeline-api pipeline-operator pipeline-observer gateway
  query-worker`) + 1 in `datuplet-infra/values.yaml` (the keygen Job runs the
  pipeline-api image);
- sed-rewrites every `tag: latest` → `tag: X.Y.Z`, protected by a PCRE grep guard that
  aborts if any non-`latest` tag line exists (this guard has already caused one release
  fix: commit `0ffac06` "stop chart-pin guard tripping on comments");
- sed-bumps `version:` + `appVersion:` in all four `Chart.yaml`s.

Consequence: the chart a user installs is not the chart in git at the tag; the chart
in git is only installable against locally built `datuplet/*:latest` images.

**⚠ This guard is now broken (2026-07-11).** RFC 026 added `components.tag: v0.1.0`
to `datuplet-app/values.yaml` (line ~272) — an intentionally non-`latest`,
platform-version-independent component pin (§2.7). The guard's
`grep -qP "^[[:space:]]*tag:[[:space:]]+(?!latest$)"` matches it, so the pin step
aborts with "has a non-'latest' tag line; refusing blanket replace" on the next
`v*` tag. Verified by running the exact guard against current `main`. It is latent
only because no release has been cut since RFC 026 merged. Two fixes: **structural**
— W2 (§6.2) deletes the sed+guard entirely; **stopgap** — exclude the `components.`
block from the guard (or bump-before-tag manually) before any release lands ahead of
W2.

### 2.3 Install and upgrade

5-phase sequential install (`docs/install.md`, mirrored in `Makefile`
`deploy-local-helm` / `e2e-k8s-deploy`, `docs/quickstart-kind.md`,
`docs/quickstart-gke.md`):

1. `datuplet-operators` — CNPG operator (`cloudnative-pg 0.23.0` subchart)
2. `datuplet-infra` — CNPG Cluster, OpenFGA (`0.3.3`), MinIO (`~5.4.0`), keygen Job
3. `datuplet-app` — control plane, CRDs, FGA DSL, migrate + authz-bootstrap hook Jobs
4. `datuplet-lakekeeper` — lakekeeper subchart (`0.10.0`), `wait-for-fga-pin` hook
5. `scripts/register.sh` — 499-line idempotent bash; `--mode=exec|job`; lakekeeper
   bootstrap + admin user + project + warehouse attach + grant

Ordering is enforced at runtime by pre-install poll Jobs (`wait-for-platform`,
`wait-for-fga-pin`) that fail loudly when a phase is missing. Upgrades are documented
as independent per-phase `helm upgrade`s; `register.sh` is not re-run.

### 2.4 Version pins and where they live

| Thing | Pin | Location | Enforcement |
|---|---|---|---|
| CNPG operator chart | `0.23.0` | operators `Chart.yaml` | none (manual bump) |
| OpenFGA chart | `0.3.3` | infra `Chart.yaml` | none |
| MinIO chart | `~5.4.0` (floating) | infra `Chart.yaml` | none; **no `Chart.lock` committed** — `helm dependency update` per clone may resolve different versions |
| Lakekeeper chart | `0.10.0` | lakekeeper `Chart.yaml` | none |
| FGA model | `"4.4"` twice | app `fgaModel.version` + lakekeeper `platform.fgaModelVersion` | CI enforces DSL→version inside app (`fga-version-check.yml`); **cross-chart sync unenforced** (documented gap) |
| kubectl helper image | `bitnami/kubectl:latest` | app + infra + lakekeeper values (lakekeeper's inline in value strings, outside any `externalImages` block) | floating (documented limitation) |
| openfga CLI image | `openfga/cli:v0.6.2` | app values | manual |
| built-in component images | `components.tag: v0.1.0` (hardcoded, platform-version-independent) | `charts/datuplet-app/values.yaml` `components.{registry,tag}` (RFC 026) | none — and `_release-image.yml` tags the images with the *platform* version, so `ghcr.io/kacurez/<comp>:v0.1.0` may not exist (§6.6) |
| CRD manifests | **three CRDs × two copies** (`pipelines`, `pipelineruns`, `componentdefinitions`) | `charts/datuplet-app/crds/` + `utils/deploy/k8s/crds/` | **none — RFC 026 dutifully edited both copies (its plan mandated it); nothing prevents schema drift.** W1/Phase 0 deletes the `utils/deploy` copy; W4 bans re-adding refs |

### 2.5 CI coverage today

- `pr.yml`: go vet/test (+GCS integration), helm lint + template.
- `e2e.yml` (PRs + main): kind, **source-built** images `kind load`ed, working-tree
  charts with `tests/e2e/values-*.yaml` overrides, register.sh, pipeline suite.
- `fga-version-check.yml`: DSL→version coupling inside datuplet-app only.

**Correction (2026-07-07): the e2e suite is green-but-vacuous in CI.** Audit of
run 28856550215 (a merged-to-main run, conclusion *success*): **3 tests passed,
~24 skipped — including every pipeline scenario** (`TestScenarios` "passes" as
an empty parent; all query tests except LocalMint skipped; remote-CLI, audit,
big-data skipped). Root cause: `tests/e2e/framework/setup.go` `PreCheck()`
**requires a kubectl context containing the string "orbstack"** — CI's
`kind-datuplet-e2e` context fails it, and every scenario turns that failure
into `t.Skip`, so the job stays green. The deploy/helm/register part of the job
is real coverage; the test suite on top of it is not. See P8 / W7 (§6.7).

Not covered anywhere: install from published charts/images, any upgrade path, CRD
schema application on upgrade — and, per the correction above, in-CI pipeline
execution itself.

### 2.6 Observed drift (evidence that duplication is already biting)

Re-verified against `156e9a1` (2026-07-11): RFC 026 fixed some of this incidentally,
but the core fossil survives.

- `docs/install.md` still says "Once 0.1.0 is released, install from the public repo …
  Until then, install from a local clone" — tags are at `v0.7.1`. **Still present.**
- `docs/install.md` still claims `make deploy-local` applies overrides from
  `tests/local/values-*.yaml` — that directory still does not exist. **Still present.**
- `docs/install.md` still lists the MinIO subchart repo as `charts.bitnami.com/bitnami`;
  the actual dependency is `https://charts.min.io/`. **Still present.**
- `utils/deploy/k8s/` is the **pre-helm deployment path**, still present — now **14
  files including a third CRD copy** (`datuplet.io_componentdefinitions.yaml`, added
  by RFC 026 alongside the chart copy). `minio.yaml` still references a deleted
  `deploy-local.sh`. RFC 026 **did** delete `utils/deploy/k8s/rbac/sample-pipeline-secret.yaml`
  (secretsRef removed from the CRD), so the earlier plan to *relocate* that file is moot
  — it is simply gone.
- Live tooling still consumes the fossil: `make k8s-reload-crds` applies **only the two
  old CRD copies by explicit path** (`pipelines`, `pipelineruns` — it never learned
  about `componentdefinitions`, so operators using it get a stale registry CRD), also
  as a dependency of every `k8s-retry-*` target; `make k8s-rebuild-operators` applies
  raw CRDs/RBAC/`operators.yaml` **over helm-managed resources** (hardcoded namespace
  `datuplet-e2e`); `docs/pipeline-api.md` points the reaper CronJob at the fossil path
  and mentions a nonexistent `./undeploy-local.sh`; `make k8s-smoke` hints a nonexistent
  `make k8s-up`.
- RFC 026 **did** consolidate `examples/` (deleted `examples/k8s/` + `examples/local-dev/`,
  moved everything to `examples/pipelines/`, added a CI guard) — so the earlier plan's
  examples-relocation work is already done upstream; W7/Phase 0 no longer touch it.

### 2.7 ComponentDefinition registration (RFC 026, merged — informs the "add registration to deploy scripts?" question)

RFC 026 ships the six built-in components as **ComponentDefinition CR chart templates**
(`charts/datuplet-app/templates/components/*.yaml`), gated on `components.builtins`
(default `true`) and pinned to `components.registry` (`ghcr.io/kacurez`) +
`components.tag` (`v0.1.0`). The CRD itself is in `charts/datuplet-app/crds/`
(helm applies `crds/` before templates). So a stock `helm upgrade --install
datuplet-app` **already registers the built-ins** — no separate registration step is
needed in `install.sh`/`register.sh` for the normal path. Two consequences that *do*
land on this RFC:

1. **The pinned images must exist.** `components.tag: v0.1.0` is deliberately
   independent of the platform version, but `_release-image.yml` tags component images
   with the *platform* release version — so `ghcr.io/kacurez/data-generator:v0.1.0`
   etc. are only present if a `v0.1.0`-era release published them. On the current
   pipeline a fresh `--from-repo` install would `ImagePullBackOff` the built-in
   components. This is precisely what W3 release-verify catches, and it is the concrete
   form of the "component release train" gap (§6.6).
2. **e2e opts out**: the harness sets `components.builtins: false` and registers
   locally-built `:latest` images as prerelease `dev` ComponentDefinitions via
   `tests/e2e/framework/components_bootstrap.go` (avoids two owners of the same
   cluster-scoped CR). W7/W5's fixtures inherit this; nothing new required there.

**Code-graph validation (2026-07-11, via codebase-memory index of `156e9a1`).** Two
facts confirmed against the Go source, both sharpening the above:

- **The reconciler validates spec *format* only, not image pullability.**
  `ComponentDefinitionReconciler.Reconcile` (`pkg/k8s/controllers/componentdefinition_controller.go:39`)
  calls `validateComponentDefinitionSpec` and sets `status.phase = Valid|Invalid`
  purely on shape (image-tag syntax, semver, `:latest` ban, `default ∈ versions`,
  `default ≤ max` — per its unit tests). A built-in pinned to `:v0.1.0` therefore
  reconciles to **Valid** even when that image was never published — so **nothing at
  install/reconcile time catches the §6.6 publish gap; only an actual pipeline run
  (W3 release-verify) does.** This is the strongest single argument for W3.
- **An imperative registration CLI already exists**: `pipeline-api admin component
  register --file <ComponentDefinition.yaml>` (`cmd/pipeline-api/admin_component.go`,
  → `applyComponentDefinition` → k8s apply), alongside `admin component list`.
  pipeline-api reads the registry through a **TTL-cached K8s list View**
  (`pkg/pipelineapi/registry/view.go`), not a live informer. So the answer to "add
  ComponentDefinition registration to the deploy scripts?" is confirmed **no for the
  built-ins** (helm templates register them; `install.sh` needs no step), and the
  CLI is the ready-made path if a future install ever needs to register off-chart
  components imperatively.

## 3. Problems

- **P1 — Published artifacts unverified.** The exact thing a new user runs is the one
  thing CI never runs. "Reliable first deploy" is currently hope-based.
- **P2 — Upgrades untested + CRD upgrade gap.** No N→N+1 coverage for migrations,
  FGA pin flow, or hooks; `helm upgrade` never applies `crds/` changes, and no doc or
  script compensates.
- **P3 — Release mutates charts.** git ≠ published; sed + grep-guard fragility
  (already caused a release fix); local-dev and production run structurally different
  values.
- **P4 — Version truth scattered/floating + a parallel deploy path.** Cross-chart
  FGA drift unenforced; MinIO range floating without lockfile; `kubectl:latest`;
  and the legacy `utils/deploy/k8s/` tree duplicates chart-managed resources
  (CRDs, RBAC, Deployments) while Makefile dev targets still apply it — a second
  source of deploy truth that is already wrong.
- **P5 — One release train for everything.** A one-line fix in finnhub-extractor
  costs a full platform release (33 image jobs) and a platform version bump; user-space
  components are version-coupled to the control plane.
- **P6 — Install procedure ×5.** install.md, two quickstarts, `deploy-local-helm`,
  `e2e-k8s-deploy` each hand-roll the sequence; three drift instances already (§2.6).
- **P7 — No dependency-update lane.** "New lakekeeper version" has no trigger
  (nothing watches upstream), no checklist (which code must be re-checked), and no
  compatibility record (which datuplet version was tested with which lakekeeper).
- **P8 — The e2e gate is green-but-vacuous in CI.** The framework's `PreCheck`
  hard-requires an OrbStack kubectl context, so on CI kind nearly every test
  skips and the job passes anyway (§2.5 evidence: 3 passed / ~24 skipped on a
  successful main run). Compounding: skips are silent (no summary, no budget),
  `kubectl port-forward` endpoints die when pods roll mid-suite (turning later
  tests into more skips), 10 fixtures depend on `jsonplaceholder.typicode.com`
  (external flake once the suite *does* run), two FGA-matrix tests are
  permanently skipped, and the remote-CLI test never runs in CI. Every "gate on
  `make e2e-k8s`" claim — including RFC 025's and RFC 026's phase gates — is
  weaker than it looks until this is fixed.

## 4. Goals / non-goals

**Goals**

- G1: One tested command for first install; the same code path CI verifies.
- G2: Boring, tested upgrade for the common case (Datuplet-only release), including CRDs.
- G3: A defined, mostly automated lane for dependency bumps (lakekeeper, CNPG, OpenFGA, MinIO, helper images).
- G4: Single source of truth per version fact, CI-enforced where two places must agree.
- G5: Preserve the per-phase upgrade-cadence separation (it is the feature, not the bug).
- G6: Explicit decision on splitting components (and anything else) out of the repo.

**POC stance (maintainer, 2026-07-04).** Datuplet is pre-1.0 and this work is
greenfield: no transition shims, no dual paths, no deprecation windows. Each
workstream replaces the old mechanism outright — docs, Makefile, CI, and workflows
switch in the same PR that introduces the replacement.

**Non-goals**

- GitOps packaging (ArgoCD app-of-apps, sync-waves). The 4-chart split is already
  GitOps-shaped; a docs section can come later. Nothing here blocks it.
- Replacing helm, supporting non-K8s targets, HA/multi-cluster.
- Changing `register.sh` semantics or the credential model.
- RFC 026 scope (registry design is settled there; §6.6 only consumes it).

## 5. Options considered

### 5.1 Shape of the install

| | A — Umbrella chart (one `helm install`) | B — Installer/operator (Go CLI or meta-operator) | C — Keep 4 charts + thin tested script (recommended) |
|---|---|---|---|
| First-install UX | best (one command) | good | good (one command via script) |
| Ordering | helm has no inter-subchart ordering; CNPG webhook + FGA-pin sequencing would need fragile hook gymnastics | full control | already solved (poll Jobs + sequential `--wait`) |
| Cadence separation | lost — one release couples infra+app+catalog again | kept | kept |
| Cost | high (re-architect hooks; CRD timing) | high (new binary, new failure modes, still wraps helm) | low (productize what `deploy-local-helm` already does) |

A was already deliberately rejected for 0.1 (`docs/known-limitations.md` "No umbrella
chart"). B is worth revisiting only if the script's flag surface sprawls. **C.**

### 5.2 Where components live

| | Split repo now | Monorepo, separate component release train | Monorepo, single train (status quo, hardened) — recommended now |
|---|---|---|---|
| Component fix cost | own release, no platform bump | own tag, no platform bump | full platform release |
| Version coherence | matrix (platform × component set) must be tracked + tested | matrix, but same repo/e2e | trivial — one version = one tested set |
| SDK handling | must publish SDK modules cross-repo | in-repo (multi-module already: `make tidy` covers `components/*`) | in-repo |
| e2e | cross-repo wiring | same repo | same repo |
| Prereq | RFC 026 registry (else pipeline YAML pins raw images) | RFC 026 registry | none |

Key insight: **RFC 026 changes the economics.** Once components are addressed via
ComponentDefinition registry entries (additive version entries, `:latest` banned,
prerelease channel for dev), "releasing a component" collapses to *push image +
publish a ComponentDefinition manifest* — no helm release, no platform version bump,
and it works identically from this repo or another one. Splitting **before** RFC 026
lands buys nothing and costs SDK/e2e plumbing while the interfaces are still moving.

**Recommendation (accepted)**: stay single-train. RFC 026 (merged) built the registry
consumption side; the long-term component *release train* (independent cadence) stays
deferred (§6.6 option B) — split into a separate repo only when a concrete forcing
function appears (external contributors, private components, genuinely divergent
cadence). What is **not** deferred is the immediate publish gap RFC 026 left — the
built-in component images must exist for a from-repo install to work — which RFC 024
closes now as plan task T6.3 (§6.6). Same in-repo logic applies to charts.

### 5.3 Version source of truth

Considered a root `versions.yaml` BOM that generates/validates everything. Rejected
(for now): helm needs pins in `Chart.yaml` regardless, so a BOM file is a *third*
copy unless it becomes a generator — more machinery than the problem needs. Instead:
**keep pins where the tooling reads them, add one CI check that asserts every
must-agree pair actually agrees** (§6.4). Revisit a BOM only if we later want
generated compatibility docs.

## 6. Design

Seven workstreams (W1–W7); see §8 for the single-branch rollout.

### 6.1 W1 — `scripts/install.sh`: the one entrypoint

A thin bash sibling of `register.sh` (same conventions: idempotent, `--dry-run`,
kubectl/helm only):

```
scripts/install.sh [--namespace datuplet] [-f values-common.yaml]
                   [-f-app values-app.yaml ...]   # per-chart overrides
                   [--from-repo|--from-source]    # published charts vs ./charts
                   [--version vX.Y.Z|X.Y.Z]       # with --from-repo; leading "v"
                                                  # stripped — helm wants X.Y.Z
                   [--register-mode exec|job]     # passed through to register.sh
                   [--skip-register] [--preflight-only] [--dry-run]
```

- **Preflight** (fail fast, before any mutation): kubectl + helm versions, cluster
  reachability + K8s ≥1.28, default StorageClass exists, chart-repo reachability
  (`--from-repo`) or `helm dependency build` success (`--from-source`), no leftover
  half-installed release from a previous attempt.
- Then exactly what `deploy-local-helm` does today: 4 × `helm upgrade --install
  --wait [--wait-for-jobs]` in phase order + `register.sh` (flags passed through;
  `--mode=job` for CI).
- **Single tested path**: `Makefile deploy-local-helm` and `e2e-k8s-deploy` become
  wrappers around it (with their values files), the release-verify job (§6.3) runs it
  with `--from-repo`, and all four docs reduce their install section to one
  invocation. That kills P6 structurally — the procedure can no longer drift from
  the tested one because there is only one.
- Fixes fold in: docs stop referencing phantom `tests/local/values-*.yaml`; a
  committed `tests/local/values-local-*.yaml` set (pullPolicy `IfNotPresent`,
  `datuplet/*:latest` repos) becomes the blessed local override, passed by the
  Makefile wrapper.

And the upgrade twin:

```
scripts/upgrade.sh [--namespace datuplet] [--phase app|infra|operators|lakekeeper|all]
                   [--from-repo --version vX.Y.Z | --from-source]
                   [-f values-common.yaml] [-f-app values-app.yaml ...] [--dry-run]
```

- Same values-override surface as install.sh — required so the upgrade e2e lane
  (§6.5) can inject locally built HEAD images once W2 makes chart defaults point at
  ghcr.
- **Applies CRDs first, phase-aware** — closing the helm-ignores-`crds/` gap (P2)
  for **both** CRD-bearing charts: the `operators` phase applies
  `charts/datuplet-operators/crds/` (CNPG CRDs), the `app` phase applies
  `charts/datuplet-app/crds/` (Pipeline, PipelineRun **and ComponentDefinition** —
  RFC 026's registry CRD, whose schema evolves with the registry and is exactly the
  kind of CRD helm-upgrade would otherwise silently skip), `all` applies both.
  Source is the working tree (`--from-source`) or `helm show crds datuplet/<chart>
  --version X.Y.Z` (`--from-repo`); apply via `kubectl apply --server-side`. Then
  `helm upgrade` of the requested phase(s). Applying the whole `crds/` dir picks up
  all three app CRDs automatically — no per-CRD enumeration.
- Default `--phase app` — the common "Datuplet release" case is the shortest command.
- Prints the FGA cross-chart reminder when it detects `fgaModel.version` changed
  (belt; the CI check in §6.4 is the suspenders).

**Failure & recovery policy (POC).** Upgrades are forward-only: no `--atomic`, no
`helm rollback` — hook Jobs, CRD applies, and DB migrations sit outside helm's
rollback scope, so `--atomic` would advertise an undo it cannot deliver. Every step
of install.sh/upgrade.sh is idempotent; recovery from a mid-flight failure is "fix
the cause, re-run the same command". Postgres migrations follow the existing
forward-only discipline (`docs/postgres-migrations.md`). A "snapshot the CNPG
cluster before Phase 2/4 upgrades" recommendation goes to `known-limitations.md`
(CNPG backups are already a documented gap there — this RFC does not design backup
machinery).

### 6.2 W2 — Charts are released as committed (kill the sed)

Invert the pinning so the release workflow stops rewriting chart content:

- `values.yaml` defaults become the **published** refs: `repository:
  ghcr.io/kacurez/<img>` and `tag: ""`.
- Templates resolve the tag as `{{ .Values.image.X.tag | default .Chart.AppVersion }}`
  (one `_helpers.tpl` helper covering both value shapes: the **four** `image.*`
  service entries — `pipelineApi pipelineOperator pipelineObserver gateway`;
  `icebergJob` was removed by PR #26 — **and** `queryWorker.image`, which lives
  outside the `image` block — plus infra's pipeline-api ref).
- **Leave `components.tag` alone.** RFC 026's `components.{registry,tag}` (the
  built-in ComponentDefinition image pins) are a *separate, platform-independent*
  version axis (§2.7) — the helper and the appVersion default do **not** touch them.
  This is the deployment-side embodiment of §6.6's "component release train is
  independent of the platform version". It also means W2's tag-match guard and the
  W4 `:latest`-render check must both whitelist the `components.` block (it is
  intentionally a non-appVersion, non-`latest`, `vX.Y.Z` pin).
- Local dev / e2e pass the already-existing values overrides (`datuplet/*` repos +
  `tag: latest`) — which `tests/e2e/values-*.yaml` effectively does today, now made
  symmetric for local via W1.
- `release.yml` stops mutating charts **entirely** — the 30-line sed block, its PCRE
  guard, *and* the `Chart.yaml` stamping are all deleted. Chart `version`/`appVersion`
  are **committed before tagging** (release prep = one bump commit, then tag that
  commit), and release.yml instead gains a cheap guard job: fail unless the tag
  matches `version`/`appVersion` in all four `Chart.yaml`s. Without bump-before-tag,
  `git checkout vX.Y.Z` would render `tag: ""` against a stale committed
  `appVersion` and from-source installs would resolve wrong images.
- Commit `Chart.lock` for the three dependency-bearing charts (operators, infra,
  lakekeeper — datuplet-app has no chart dependencies); docs/Make switch
  `helm dependency update` → `helm dependency build`; pin MinIO exactly (`5.4.x`,
  not `~5.4.0`). Reproducible clones (P4).
- Pin `bitnami/kubectl` to a version in all **three** charts that reference it
  (app, infra, lakekeeper — the latter inline in values strings, outside any
  `externalImages` block).
- With tags pinned by default, flip chart-global `image.pullPolicy` default
  `Always` → `IfNotPresent` (Always was a `:latest`-era safety; pinned tags make it
  registry-hammering with no benefit). Local-dev overrides no longer need to care.

Net effect: `git checkout v0.9.0 && ./scripts/install.sh --from-source` and
`install.sh --from-repo --version v0.9.0` install the same thing (P3 gone) — the
bump-before-tag discipline plus the tag-match guard are what make this identity
actually hold — and the release workflow performs no chart mutation at all:
verify, build, package, publish.

### 6.3 W3 — `release-verify`: prove the published artifacts install

A dedicated `release-verify.yml` workflow — deliberately **not** a job inside
`release.yml`, whose publish jobs derive image tags from `GITHUB_REF` and must only
ever run on tag pushes (a `schedule:` trigger on that file would fire them against
a branch ref). Triggers: `workflow_run` on release completion; weekly `schedule`
against the latest published version (resolved from the helm repo `index.yaml`) to
catch bit-rot in gh-pages/ghcr/upstream chart repos; `workflow_dispatch` for ad-hoc
re-runs. Steps:

1. kind cluster with a host-port mapping for NodePort 30081 (the e2e kind config
   exposes no host ports, and `k8s-smoke` probes `http://localhost:30081` —
   alternatively reuse `scripts/e2e-port-forward.sh` and give `k8s-smoke` a URL
   override);
2. `helm repo add datuplet https://kacurez.github.io/datuplet` — **no repo checkout
   for the charts**; images pull from ghcr.io (multi-arch manifests get their first
   automated consumer);
3. `install.sh --from-repo --version <tag> --register-mode job` (W1 lands first, so
   release-verify runs the same entrypoint users run — never a parallel variant);
4. `k8s-smoke` probes + one real pipeline (the `read-back` e2e scenario) + a no-op
   `upgrade.sh --phase app --from-repo --version <tag>` re-run (idempotency check).

Release discipline addition to `CLAUDE.md`/release docs: a release is announceable
when `release-verify` is green, not when the tag is pushed. This is the direct fix
for "reliable way to deploy for the first time" — every release has been installed,
from the public artifacts, before any user tries.

### 6.4 W4 — CI version-sync checks

One `verify-versions` script (repo-local, run in `pr.yml`) asserting:

- `datuplet-app fgaModel.version == datuplet-lakekeeper platform.fgaModelVersion`
  (closes the documented cross-chart gap);
- no reference to `utils/deploy/` anywhere in the repo (the legacy tree is deleted
  in Phase 0; this check keeps it — and any second deploy path — from coming back);
- `helm template` output of all four charts (default values) contains no `:latest`
  image reference and no `datuplet/*` dev ref — catches helper images wherever they
  live (`bootstrap.externalImages.*`, lakekeeper's inline `bitnami/kubectl` value
  strings, future additions), not just known value keys;
- `Chart.lock` present and consistent with `Chart.yaml` for every chart that has a
  `dependencies:` block.

### 6.5 W5 — Upgrade e2e lane

New workflow (main pushes; PR-triggerable via label to keep PR cost flat):

1. kind; `install.sh --from-repo --version <latest published release>`;
2. seed state: run one pipeline, create a second user/project (so migrations and FGA
   tuples have real rows to migrate);
3. `upgrade.sh --phase all --from-source` with the e2e image-override values files
   (`-f tests/e2e/values-*.yaml`) so the freshly built/`kind load`ed HEAD images are
   used — post-W2 chart defaults point at ghcr `:appVersion` — CRD apply included;
4. assert: migrate + authz-bootstrap Jobs succeed, seeded runs still listable, new
   pipeline runs end-to-end.

This is the direct fix for "reliable way to update": Postgres migrations
(`docs/postgres-migrations.md` discipline), FGA pin/hash behaviour, hook ordering,
and CRD evolution get exercised on every merge instead of on the first user's
cluster. It also (deliberately) makes "upgrade from N-1" the *defined* support
statement for 0.x: we test exactly one hop, and skipping releases is best-effort —
codified in `known-limitations.md` as part of this workstream (§9 Q7).

### 6.6 W6 — Cadence: components and dependencies

**Components (answers the repo-split question).** Per §5.2: stay in the monorepo.
RFC 026 (merged) built the **consumption** side of the registry — the
ComponentDefinition CRD + built-in CR templates pinned to `components.tag: v0.1.0`,
a version axis independent of the platform chart version (§2.7). But it did **not**
build the **production** side: `_release-image.yml` still tags component images with
the *platform* release version, so nothing publishes `ghcr.io/kacurez/<comp>:v0.1.0`.
Net: a stock `--from-repo` install today registers built-ins whose images can't be
pulled. **This is now a concrete gap, not a hypothetical** — and it is the one place
the merged RFC 026 left deployment work on the table. The plan-review pass (§9,
2026-07-11) found it has **two parts**: (a) the five built-ins that *are* built are
tag-mismatched (`:v0.1.0` vs the platform-versioned published images); and (b)
**`pandas-transform` ships as a built-in ComponentDefinition but is built and
released nowhere** — absent from `_release-components.yml` (5 jobs) and from the
local `docker-build-k8s` set. So it is permanently unpullable regardless of the tag
axis; T6.3 must either wire its build+release or disable that built-in, and a CI
guard should assert every shipped built-in has a matching build/release job.

Options for closing it (maintainer choice; the cheapest is a Phase-6 addition here
rather than reopening RFC 026):
- **(a)** point `components.tag` at the platform version too (give up the
  independent axis for 0.x) — one values edit, `_release-image.yml` already produces
  those tags; simplest, and the independent axis wasn't being used yet;
- **(b)** a `components-vX.Y.Z` tag train reusing `_release-components.yml` that
  publishes `:vX.Y.Z` component images + a rendered `componentdefinitions.yaml`
  artifact (the design RFC 024 always sketched for this) — platform tags then drop
  the components bucket (30 → 15 image jobs) and a component fix stops implying a
  platform version. More work; the right long-term shape.
W3 release-verify is what makes this gap *visible* either way (built-ins
`ImagePullBackOff` if unpublished). A repo split stays a later organizational choice
at near-zero cost (deliverable = *image + manifest*).

**Dependencies (answers "when there is a new lakekeeper").**

- **Renovate** (over Dependabot: native `helm-values`/`helmv3` datasources +
  grouping) watching: the four `Chart.yaml` dependency pins, `bootstrap.
  externalImages.*`, Dockerfile base images, Go modules (grouped), GitHub Actions.
  Every bump PR runs the full e2e — a lakekeeper `0.10 → 0.11` PR is *born* with the
  evidence of whether datuplet still works against it.
- `docs/dependency-upgrades.md` — the judgment checklist automation can't do, per
  dependency. Lakekeeper (the richest case): read upstream changelog for REST/authz
  changes; re-check `pkg/datagateway/lakekeeper/` resolver + `pkg/catalogwriter/`
  client + `pkg/pipelineapi/storage/catalog_proxy.go`; confirm FGA model
  compatibility (lakekeeper ships its own FGA expectations); bump is **independent**
  of `fgaModel.version` unless datuplet's DSL changed. CNPG: operator-then-cluster
  order, read CNPG upgrade notes. OpenFGA/MinIO: chart-value surface diffs.
- Compatibility record: the released chart already *is* the record (its `Chart.yaml`
  pins name the tested subchart versions) — W2's "charts released as committed"
  makes `git log charts/datuplet-lakekeeper/Chart.yaml` the audit trail. No extra
  matrix doc to maintain.

### 6.7 W7 — e2e effectiveness: a green run must mean the suite ran

Fixes P8. Five moves, all in `tests/e2e/` + `scripts/e2e-port-forward.sh` +
`e2e.yml` — no chart or product-code changes:

1. **Generalize `PreCheck`** (`framework/setup.go`): use the *current* kubectl
   context — the real precheck is "is `deploy/pipeline-operator` present in
   `$DATUPLET_E2E_NAMESPACE`", which already exists. The "orbstack"
   string-sniff and the framework's silent `kubectl config use-context`
   switching (a test framework mutating developer kubeconfig) both go. An
   optional `DATUPLET_E2E_CONTEXT` pin *fails* on mismatch instead of
   switching.
2. **Fail-closed CI mode**: `E2E_REQUIRE=1` (set by `e2e.yml`, not locally). A
   `framework.SkipOrFail` helper replaces `t.Skip` at every
   *infrastructure-availability* gate (harness nil, precheck failed, endpoint
   unreachable, worker not ready, docker/binary missing): local runs keep
   skipping for fast iteration; CI **fails**. Deliberate gates stay skips:
   `RFC011_BIG_DATA_PROOF` opt-in, NetworkPolicy-on-kindnet capability
   detection, `E2E_K8S` mode gate. `TestMain` hard-exits non-zero on bootstrap
   failure under require mode.
3. **Supervised port-forwards + visible outcomes**: `kubectl port-forward`
   dies when a pod rolls (the query suite rolls pipeline-api mid-run) — the
   script gains a respawn loop per endpoint. The suite runs as
   `go test -json`, and a summarizer writes pass/fail/**skip-with-reason**
   counts to `$GITHUB_STEP_SUMMARY`; under require mode a skip outside the
   deliberate-gate allowlist fails the job (belt to SkipOrFail's suspenders).
4. **Hermetic HTTP fixture**: 10 pipeline fixtures fetch
   `jsonplaceholder.typicode.com` — an internet dependency in the middle of
   the flagship reliability gate. An in-cluster static server (nginx +
   ConfigMap serving the same JSON shape) replaces it; fixture URLs swap to
   the in-cluster Service. No external network in the e2e data path.
5. **Dead coverage revived or removed**: the two permanently-skipped
   FGA-matrix tests get the seeding their own docstrings prescribe (Datuplet
   project DB row matching the lakekeeper project) or are deleted; the
   remote-CLI test runs in CI (build `bin/datuplet` in the job, parameterize
   its namespace/release detection).

**kind vs minikube (considered, resolved: keep kind).** The vacuous runs were
never kind's fault — the context sniff was. On GitHub runners minikube uses
the docker driver (same containerized-node model as kind, similar startup);
its one real advantage (`minikube docker-env` in-daemon builds, saving the
image-copy step) doesn't outweigh migrating every workflow, quickstart, and
the Phase 3/5 kind configs — and the disk pressure the copy causes is already
mitigated (prune steps). Revisit only if image-copy time/disk becomes the
bottleneck again; the next step then is a kind-local-registry pattern, not a
cluster-tool swap.

**Sequencing (recommendation).** W7 should be implemented **first within RFC 024**
(RFC 025/026 are already merged). Every other phase's gate leans on `make e2e-k8s`,
and P8 means that gate currently proves deploy-ability but almost no behaviour — so
making it fail-closed first is what makes all the *other* RFC 024 gates trustworthy.
W7 touches only the e2e harness/fixtures/workflow. The plan commits phases in the
order 7 → 0 → 1 → 2 → T6.3 → 3 → 4 → 5 → 6 on a single branch (§8).

## 7. Deliberately unchanged

Four charts, five phases, sequential `--wait` install, poll-Job ordering guards,
lockstep chart versions (one datuplet version = one tested set — simplest possible
compatibility statement), `register.sh` as imperative idempotent bootstrap, gh-pages
helm repo, multi-arch image pipeline, K8s-only. Complexity that earns its keep.

## 8. Rollout

**Execution model (maintainer decision 2026-07-11): one branch, one PR.** All phases
are implemented and committed on a single branch (`claude/modest-easley-f5173d`) and
land as **one final draft PR** — not per-phase PRs. Phases below are commit *groups*
on that branch, gated individually (tests + a cumulative Codex review, then commit),
with a single integration PR at the end (see the plan's "Final integration"). The
phases are still designed so each is a self-contained, greenfield replacement (no dual
paths); they just accumulate on one branch. Recommended commit order: 7 → 0 → 1 → 2 →
T6.3 → 3 → 4 → 5 → 6.

| Phase | Contents | Depends on |
|---|---|---|
| 0 | Deploy-code hygiene: remove the legacy `utils/deploy/k8s/` tree + repoint its consumers (detail below), fix stale install.md claims (0.1.0 wording, phantom `tests/local`, MinIO repo URL), pin kubectl (three charts) + MinIO, commit `Chart.lock` (dependency-bearing charts), workflow hygiene (release.yml `concurrency` group; fga-version-check checkout@v6 + must-increase) | — |
| 1 | W1 install.sh/upgrade.sh; Makefile/e2e/docs switch to it | — |
| 2 | W2 de-sed release (values defaults → ghcr + appVersion, pullPolicy → IfNotPresent; bump-before-tag + tag-match guard; release.yml stops mutating charts) | 1 |
| 3 | W3 release-verify workflow (runs on the next tag) | 1 |
| 4 | W4 verify-versions CI check | 0 |
| 5 | W5 upgrade e2e lane + N-1→N policy in known-limitations.md | 1, a published release to upgrade from |
| 6 | W6 Renovate + dependency-upgrades doc; **plan task T6.3** closes RFC 026's component-image publish gap (tag sync + `pandas-transform`) — lands after Phase 2, blocks 3/5 | 2 (for T6.3) |
| 7 | W7 e2e effectiveness (fail-closed CI, precheck generalization, supervised forwards + summary, hermetic fixtures, dead-test revival) | — ; **recommended to implement FIRST within RFC 024** (§6.7) |

**Component train vs T6.3 (distinct things).** The long-term *component release
train* (independent `components-vX.Y.Z` cadence, off-platform-version) stays deferred
— out of RFC 024's committed scope, revisited if a component needs to ship
off-cadence (§5.2, §6.6 option B). What RFC 024 **does** own now is the *immediate
publish gap* (plan task T6.3): the built-in component images the merged chart already
references must actually exist (tag sync + wiring/disabling `pandas-transform`). T6.3
is the minimum to make a from-repo install pull its built-ins; the train is the
larger future refactor.

**Phase 0 detail — legacy deploy-tree removal.** `utils/deploy/k8s/` (14 raw
manifests; §2.6) is deleted outright — greenfield, no deprecation:

- No keepers to relocate: RFC 026 already deleted `rbac/sample-pipeline-secret.yaml`
  (secretsRef was removed from the CRD) and rewrote `docs/secrets.md`, so the whole
  tree — all 14 files, including the third CRD copy (`componentdefinitions`) — is
  deleted outright.
- `k8s-reload-crds` → `kubectl apply -f charts/datuplet-app/crds/` — one canonical
  CRD source (the chart, as CLAUDE.md already states). The `k8s-retry-*` targets
  inherit the fix via their dependency.
- `k8s-rebuild-operators` → image build + `kubectl rollout restart` only. Helm owns
  manifests; no more raw CRD/RBAC/Deployment applies over helm-managed resources.
  Fix its hardcoded `datuplet-e2e` namespace while touching it.
- Doc/target repoints: `docs/pipeline-api.md` reaper section → chart's
  `templates/reaper/cronjob.yaml`; its `./undeploy-local.sh` mention →
  `make undeploy-local`; `k8s-smoke`'s "run `make k8s-up`" hint →
  `make deploy-local`; the four code comments citing `utils/deploy/k8s/` paths
  (`cmd/pipeline-api/main.go`, two in `tests/e2e/framework/`, one in
  `charts/datuplet-app/templates/pipeline-operator/rbac.yaml`) point at the chart
  equivalents.
- Recurrence guard: W4's "no `utils/deploy/` references" check (§6.4).

Boundary with RFC 026: `examples/local-dev/` (dead since local mode was removed)
and the `examples/k8s/*` pipeline-example layout — including the missing
`duckdb-pipeline.yaml` that `k8s-retry-duckdb` applies — belong to RFC 026
Phase 0 (examples consolidation). RFC 024 deliberately does not touch example
*pipelines*; it only removes the parallel *deploy* path.

**Phase 0 also carries two workflow-hygiene fixes** (2026-07-07 workflow review):
`release.yml` gains a `concurrency` group (no cancel) — today two tags pushed close
together run chart-releaser concurrently against gh-pages and one push loses the
race; and `fga-version-check.yml` moves `checkout@v4` → `v6` (every other workflow
is on v6) and checks that `fgaModel.version` *increased* rather than merely
changed (reusing an old version number currently passes).

## 9. Design questions — resolved by maintainer 2026-07-04

- **Q1 — Entrypoint form**: bash `install.sh`/`upgrade.sh` (matches `register.sh`
  precedent, zero new build artifacts). Revisit a Go CLI only if flags sprawl.
- **Q2 — pullPolicy**: flip chart-global default `Always` → `IfNotPresent` in W2.
- **Q3 — Upgrade e2e cadence**: every main push (PR-triggerable via label).
- **Q4 — Dependency automation**: Renovate.
- **Q5 — OCI chart publishing to ghcr**: deferred; gh-pages stays the only chart
  publish surface.
- **Q6 — Component train**: deferred out of this RFC — owned and solved under
  RFC 026 (§6.6).
- **Q7 — Support statement**: adopted — "upgrades tested N-1→N, one hop; skipping
  releases is best-effort" lands in `known-limitations.md` as part of W5.
- **POC stance**: greenfield build — no migration paths, compat shims, or
  deprecation windows anywhere in this RFC's scope (§4).
- **External review (codex gpt-5.5, high reasoning, 2026-07-04)**: 11 findings, all
  incorporated in draft v3 — chart-version **bump-before-tag + tag-match guard**
  (replaces release-time `Chart.yaml` stamping), release-verify as a **separate
  workflow** (a `schedule:` on `release.yml` would fire publish jobs off a branch
  ref), **phase-aware CRD apply** (CNPG CRDs in `datuplet-operators/crds/` were
  missed), upgrade.sh values-override surface, `v`-prefix normalization for helm
  `--version`, release-verify port-mapping, render-based `:latest` check (kubectl
  is in three charts, lakekeeper's inline), `Chart.lock` scoped to dependency-bearing
  charts, query-worker's off-pattern image key, MinIO repo-URL doc drift, and the
  explicit forward-only failure/recovery policy (§6.1).
- **Cross-RFC sequencing (2026-07-07, post-rebase @ `f162ebc`)**: execution
  order is **RFC 025** (storage UI via query service) → **RFC 026** (component
  registry + config unification) → this RFC. RFC 023 (runs UX) merged with
  zero deployment-surface impact. RFC 025 is a net helper here: its Phase 0
  deletes the last post-install helm step (`queryWorker.query.warehouse` +
  `DATUPLET_QUERY_WAREHOUSE`) — directly serving G1 — and its worker hardening
  lands before W3's release-verify would exercise the worker; remaining
  touchpoints are adjacency-only (query-worker deployment template, install /
  ad-hoc-query docs, e2e values anchors). These, plus the known RFC 026
  collisions (dual CRD copies vs the `utils/deploy` deletion, `examples/k8s/`
  removal vs the sample-secret relocation, component-train ownership), are
  tracked in the implementation plan's **Pre-execution revisit checklist**.
- **Workflow review (2026-07-07)**: two hygiene fixes folded into Phase 0 —
  release.yml concurrency serialization (gh-pages push race) and
  fga-version-check hardening (checkout@v6, must-increase comparison). The
  third finding (non-root Go module tests never run in PR CI) is out of this
  RFC's scope and spun off as its own task.
- **e2e review (2026-07-07, maintainer requested)**: audit of CI run
  28856550215 proved the e2e gate green-but-vacuous (3 passed / ~24 skipped;
  root cause: `PreCheck` requires an "orbstack" kubectl context — CI kind
  fails it, every scenario skips, job passes). Folded in as **P8 + W7 (§6.7) +
  rollout Phase 7**. kind is retained (minikube considered and rejected —
  §6.7); jsonplaceholder dependency replaced by an in-cluster fixture.
- **Merged-reality reconciliation (2026-07-11, after PRs #24/#25/#26)**: RFC 025
  + 026 merged and PR #26 deleted TableCommit/local-exec, so this RFC moved from
  *deferred* to *unblocked* and every §2 fact + plan anchor was re-verified against
  `156e9a1`. Deltas folded in: iceberg-job image gone (services 6→5, §2.1/§2.2/W2
  image list); RFC 026 ships built-in ComponentDefinition CR templates so no new
  registration step is needed (§2.7) **but** its `components.tag: v0.1.0` pin
  (a) is unsatisfiable by the current release pipeline (§6.6 production-side gap,
  release-verify catches it) and (b) **breaks the release guard today** (§2.2 urgent
  note — the sole immediately-actionable item); `utils/deploy` survives with a *third*
  CRD copy (Phase 0 deletion now covers componentdefinitions too); RFC 026 already did
  the examples consolidation + secretsRef removal, so those Phase-0/W7 items are
  dropped; Pipeline spec is now `component:`/`version:` (verify pipelines use registry
  refs, no image templating — plan T3.2/T5.1); PreCheck's orbstack sniff and the
  jsonplaceholder dependency (still 13 files) are unchanged, so W7 stands as written.
  Full item-by-item log: plan's "Pre-execution revisit checklist" (now an actioned
  reconciliation log).
- **Code-graph re-analysis (2026-07-11, codebase-memory MCP index of `156e9a1`,
  8026 nodes / 36806 edges)**: re-ran the deployment-relevant analysis against the
  structural graph rather than grep. Confirmed: no iceberg-job binary/entry-point
  (only the proto `TableCommitResult` type survives for DG's inline commit — correct
  per RFC 021); the four service entry points (`cmd/{datuplet,pipeline-api,
  pipeline-observer,pipeline-operator}`); `register.sh`'s admin-command contract is
  real (`adminLakekeeperBootstrap`, `adminCreateProject`, `adminCreateUser`,
  `adminAttachWarehouse`, `adminGrant` all present in `cmd/pipeline-api/`). New
  material findings folded into §2.7: the ComponentDefinition reconciler checks
  format-not-pullability (so only W3 catches the §6.6 gap), and an `admin component
  register` CLI + TTL-cached registry `View` exist. No contradictions with the
  reconciled spec surfaced — the graph pass strengthened §2.7/§6.6/W3, changed no
  conclusions.
- **Plan Codex review (2026-07-11, `mcp__codex__codex`, read-only, high reasoning)**:
  reviewed the implementation plan against `156e9a1`. 6 findings, all verified against
  the repo and folded into the plan (details in the plan's reconciliation-log item 12).
  The one with spec impact: the component-image gap (§6.6) has a second part —
  `pandas-transform` is a shipped built-in with **no** image build/release anywhere,
  not merely a tag mismatch (§6.6 updated). The other five were plan-level
  (install.sh CRD apply, local `components.registry` overlay, upgrade-e2e bootstrap
  ordering, T6.3 DAG dependency, hermetic-fixture `/users`). No spec conclusion changed;
  the review confirmed the repo-reality claims (iceberg-job gone, release guard broken
  on `components.tag`, built-ins as templates, `admin component register` CLI, bare
  operator name).
- **Second Codex review (2026-07-11, after the single-branch/single-PR model change +
  spec/plan commit `602de9e`)**: confirmed the first-round fixes landed; its remaining
  findings were spec↔plan drift, now reconciled in this doc — §8 rewritten to the
  one-branch/one-PR model, §6 "six"→"seven workstreams", §6.7/§8 Phase-7 sequencing
  de-stale'd ("first within RFC 024"), §8 Phase-0 relocation removed (RFC 026 already
  deleted the file), and §5.2/§6.6/§8 now separate the deferred component *train* from
  T6.3's immediate publish-gap fix. Plan-side: T6.3 dependency corrected, a local/e2e
  tag-consistency step added, and the interim release-verify/upgrade-e2e gates marked
  as expected-to-fail until the first post-T6.3 release.

## 10. Risks

- **release-verify flakiness blocks releases** (upstream chart repos, ghcr, DockerHub
  for MinIO/kubectl images): job is re-runnable in isolation; the weekly cron
  distinguishes "release broken" from "world broken".
- **W2 changes local-dev muscle memory** (`make deploy-local` must inject local
  values): mitigated — the Makefile wrapper does it; direct `helm upgrade` against
  git charts now pulls ghcr `:appVersion` images instead of local `:latest`, which is
  arguably the safer surprise of the two.
- **Server-side CRD apply conflicts** if anything else field-manages the CRDs:
  upgrade.sh uses `--server-side --force-conflicts` with the same field manager
  consistently.
- **Renovate PR volume**: grouped presets + weekly schedule; e2e cost per PR is the
  real budget item and is bounded by grouping.
- **Two entry scripts to keep honest** (`install.sh`, `upgrade.sh`): both are
  exercised by CI (release-verify runs install; upgrade e2e runs upgrade), so drift
  fails loudly — same trick as everything else in this RFC.
