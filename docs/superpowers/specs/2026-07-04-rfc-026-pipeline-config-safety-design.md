# RFC 026 — Component Registry & Safe, Uniform Pipeline Configuration

**Status:** Draft v7 — two maintainer-review rounds + two external design
reviews (Codex gpt-5.5, high reasoning; round 1: 14 findings folded in —
resolve-&-freeze run snapshot, reject-then-clamp resource contract, full
ResourceList ceilings, `PipelineRun.parameters` deleted, kubectl-pruning
caveat, layered definition validation, UUID-subject superadmin grants,
explicit error taxonomy; round 2 on the new §4.9 secrets section: 11
findings folded in — per-run secret snapshot at admission, merge-PATCH
per-key writes, annotation timestamps, Secret RBAC narrowed to
per-namespace Roles, `SecretsResolved` re-keyed, complete
componentdefinitions RBAC, lazy Secret ensure, all content assertions
skipped for `$[...]` refs). **POC greenfield posture — no
migration/back-compat obligations.** All §9 items resolved — raw `image:`
dropped entirely. No code written yet.
**Scope:** Pipeline configuration UX + safety: uniform component config shape,
a registry of allowed components, superadmin-gated resource limits, a
registry-driven UI, and one consolidated, trustworthy `examples/` tree.

---

## 1. Problems (verified against code)

### P1 — Config is bifurcated and inconsistent across surfaces

`ComponentSpec` has two config fields
([pipeline_types.go:147-153](../../../pkg/k8s/api/v1/pipeline_types.go)):
`config: map[string]string` (flat, string values only per the CRD schema —
`additionalProperties: type: string` in
[datuplet.io_pipelines.yaml:103-108](../../../charts/datuplet-app/crds/datuplet.io_pipelines.yaml))
and `configJSON: string` for anything nested. The e2e fixture
[manual-large-input-join.yaml:22-24](../../../tests/e2e/pipelines/k8s/manual-large-input-join.yaml)
carries a comment acknowledging the wart.

Worse, two different parsers disagree:

- `PUT /pipelines/{name}` validates with `pkg/pipeline/config.Parse`, whose
  `Config` is `map[string]any` ([types.go:57](../../../pkg/pipeline/config/types.go))
  — **nested config passes** — and stores the raw YAML in Postgres.
- Trigger-time `ApplyPipelineCRD` unmarshals the same YAML into the typed CRD
  struct where `Config` is `map[string]string`
  ([pipeline_apply.go:22-25](../../../pkg/pipelineapi/k8s/pipeline_apply.go)).
  **Nested config fails here.**

Net effect: a pipeline with nested config **saves fine and fails at trigger**.
`docs/components.md` teaches exactly this broken shape (nested `config:` for
data-generator in a `kind: Pipeline` manifest), and no component config is
schema-validated anywhere — typos surface as runtime component failures.

Precisely what forces `configJSON` today: any **non-string** config value.
`sql: |` itself is fine (one string), but sql-transform's `threads: 4` (int),
finnhub's `symbols: [...]` (array), http-json-extractor's `headers:` (map) and
data-generator's `tables:` (nested list) all violate
`additionalProperties: type: string` and must be hand-escaped into JSON.

### P2 — Any image runs

`image` is a free-form string. No allowlist, no registry, no validation
anywhere (`pkg/pipeline/config/parser.go` accepts it as-is; the controller
passes it straight to the Job container at
[pipelinerun_jobs.go:197](../../../pkg/k8s/controllers/pipelinerun_jobs.go)).
Anyone with `data_admin` on a project — and anyone with K8s RBAC to create
`datuplet.io` CRs directly, bypassing pipeline-api — can run an arbitrary
container in the cluster.

Existing pod hardening limits the blast radius (caps dropped, seccomp
`RuntimeDefault`, `automountServiceAccountToken: false`, secrets + run token
mounted on the gateway sidecar only), but an arbitrary image still means:
compute abuse, network egress, and read/write of whatever data the run's
grants allow.

### P3 — Resources are uncapped and user-settable

`resources` is a full `corev1.ResourceRequirements`, passed through verbatim
([pipelinerun_jobs.go:234-236](../../../pkg/k8s/controllers/pipelinerun_jobs.go)).
When nil, the container runs **with no limits at all**. Gateway knobs
(`chunkSize`, `bufferSize`, …) are similarly unbounded. Any project user can
starve the cluster.

### P4 — No platform-admin tier

Authz has per-project relations only: `data_admin` (write + trigger) and
`describe` (read), checked via OpenFGA
([pipeline_handlers.go:40-82](../../../pkg/pipelineapi/http/pipeline_handlers.go)).
The FGA model already defines a server-level `admin` relation
([server.fga:13](../../../charts/datuplet-app/files/fga/components/server.fga))
but pipeline-api never uses it for pipeline-related privileges. There is no
role that can do "infra-affecting" things (raise limits, allow an image)
without also being just another project admin.

### P5 — UI is a raw YAML textarea

`ui/product/pages/pipeline-detail.js` renders one `<textarea>` and a Save
button as the pipeline *config editing* surface. (The runs-UX work merged
in PR #23 added a read-only "Recent runs" table to this page plus a real
runs list and run-detail timeline elsewhere — run *observability* is fine;
the editing surface is unchanged, and §4.7's builder must preserve that
section.) No component catalog, no form, no client-side validation, no
docs. Server errors come back as opaque 400s.

### P6 — Examples are fragmented and partly dead

Three folders with different dialects and staleness (full audit in Appendix A):

- `examples/k8s/` — current, referenced by README, quickstarts, Makefile. But
  `Makefile:270` references `examples/k8s/duckdb-pipeline.yaml` **which does
  not exist**.
- `examples/pipelines/` — CLI-dialect YAMLs with stale headers ("Run with:
  `./bin/datuplet run <file>`" — the CLI now requires `--remote`);
  `etl-pipeline.yaml` additionally uses nested `config:` that the CRD rejects.
- `examples/local-dev/` — dead. Local-file mode no longer exists
  (CLAUDE.md: "K8s is the only supported deployment surface").

### P7 — Secrets require kubectl

The runtime model is sound (`$[name]` whole-scalar refs, resolved at gateway
boot from a Secret mounted on the sidecar only, `SecretsResolved` condition
— docs/secrets.md), but the *management* surface doesn't exist: the UI
"Secrets" page is an admitted placeholder that renders `kubectl create
secret` copy-paste instructions
([settings-secrets.js:1-9](../../../ui/product/pages/settings-secrets.js) —
"pipeline-api has no secrets endpoints yet"). Every secret therefore
requires giving a user kubectl + core-API RBAC in the project namespace —
contradicting the posture that pipeline-api/UI is the only user surface.
Each pipeline also carries `spec.secretsRef.name` boilerplate naming a
hand-created Secret, and a wrong name surfaces only at run time as a
`FailedMount`.

---

## 2. Goals / Non-goals

**Goals**

1. One config dialect: a single structured `config` object, schema-validated
   per component, identical in docs, examples, UI, API, and CRD.
2. A registry of allowed components; pipelines reference registry entries, not
   raw image URIs. Nothing unregistered ever gets scheduled.
3. Resource + gateway limits are policy, not user input: sane defaults from
   the registry, hard ceilings enforced by the controller, overridable only by
   a platform superadmin.
4. Registry-driven UI: pick a component, fill a schema-generated form, see
   docs inline.
5. One `examples/` tree, every file CRD-valid and CI-guarded.
6. Secrets managed through the same surface as everything else: a
   project-scoped, write-only secrets API + UI backed by one managed K8s
   Secret — no kubectl, no per-pipeline `secretsRef` boilerplate; schema-aware
   secret fields in the builder.

**Non-goals (this RFC)**

- Admission webhooks (see §8 Alternatives — deliberately deferred).
- Per-project component visibility / private registries (future; registry is
  cluster-global for now).
- Image signature / provenance verification (digest pinning is the 0.x
  answer; sigstore is future work).
- NetworkPolicy egress restrictions for component pods (real concern, separate
  RFC — noted in §9).

**Non-constraint:** backward compatibility. Datuplet is a POC — everything
below is designed greenfield. Breaking CRD/API changes land without
deprecation windows, migration shims, or dual-read paths; the handful of
existing dev pipelines are rewritten by hand (maintainer decision,
2026-07-04).

---

## 3. Design overview

```
┌────────────────────────────┐
│ ComponentDefinition (CRD,  │  cluster-scoped catalog, superadmin-managed,
│ cluster-scoped)            │  chart ships built-ins
│  name, versions[]:         │
│   image (digest-pinnable)  │
│   configSchema (JSON Schema)│
│   resources {default,max}  │
│   docs, deprecated         │
└─────────┬──────────────────┘
          │ resolved + validated against
┌─────────▼──────────────────┐
│ Pipeline CR                │  component: <registry-name>  (replaces image:)
│  stages[].components[]:    │  version: v0.1.0             (optional)
│   name / component /       │  config: {…}                 (structured, nested OK)
│   version / config /       │  inputs / outputs            (unchanged)
│   inputs / outputs         │  resources                   (superadmin-only)
└─────────┬──────────────────┘
          │
   enforcement, twice:
   1) pipeline-api PUT + trigger  → early, friendly errors (400 with details;
      over-max resources rejected here)
   2) PipelineRun controller      → hard guarantee at run admission:
      validates + resolves ONCE, snapshots the result into the run (§4.3);
      unresolvable component / schema violation → run fails FailedUser,
      nothing is scheduled; over-max resources → clamped (defense-in-depth).
      Direct-kubectl CRs hit the same wall.
```

The controller is the only thing that creates pods, so controller-side
enforcement is the actual security boundary; pipeline-api validation is UX.
This closes the "pure CRD, anyone can create something bad" hole without
webhook/cert infrastructure.

---

## 4. Detailed design

### 4.1 `ComponentDefinition` CRD (the registry)

Cluster-scoped, manually maintained manifest in `charts/datuplet-app/crds/`
like the existing CRDs; Go types in `pkg/k8s/api/v1/component_types.go`.

```yaml
apiVersion: datuplet.io/v1
kind: ComponentDefinition
metadata:
  name: http-json-extractor          # the key pipelines reference
spec:
  displayName: HTTP JSON Extractor
  description: Fetches JSON from an HTTP endpoint into an Iceberg table.
  maintainer: datuplet
  deprecated: false
  defaultVersion: v0.1.0             # optional; omitted → highest semver in versions[]
  versions:
    - version: v0.1.0
      image: ghcr.io/kacurez/http-json-extractor:v0.1.0   # ":latest" rejected; digest pin recommended
      configSchema: |                 # JSON Schema (draft 2020-12), as string
        {
          "type": "object",
          "required": ["url"],
          "properties": {
            "url":        {"type": "string", "format": "uri"},
            "array_path": {"type": "string"},
            "table_name": {"type": "string"},
            "headers":    {"type": "object", "additionalProperties": {"type": "string"}},
            "pagination": {"type": "object"}
          },
          "additionalProperties": false
        }
      resources:
        default:                      # applied when the pipeline sets nothing
          requests: {cpu: 100m, memory: 128Mi}
          limits:   {cpu: "1",  memory: 512Mi, ephemeral-storage: 1Gi}
        max:                          # full ResourceList ceiling (§4.4):
          cpu: "2"                    # resource names absent here are
          memory: 2Gi                 # not allowed in pipeline resources
          ephemeral-storage: 10Gi
    - version: dev                    # prerelease: mutable tag allowed, never
      prerelease: true                # auto-selected, imagePullPolicy Always
      image: localhost:5000/http-json-extractor:dev
      configSchema: '{"type": "object"}'   # permissive while iterating
```

Notes:

- **Version resolution (decided 2026-07-04):** a pipeline that omits
  `version:` resolves to the registry's `defaultVersion`; when that field is
  unset too, the **latest registered stable version** wins (highest semver
  among non-prerelease entries — stable versions are validated as
  `vMAJOR.MINOR.PATCH`). "Latest" always means latest *in the registry*, never
  a floating image tag: stable versions with a `:latest` (or missing) image
  tag are rejected at registration (a CRD CEL rule covers this one check;
  see the layered-validation note below for the rest). Resolution happens
  **once per run** and is snapshotted into `PipelineRun.status.components[]`
  (§4.3). Traceability caveat, stated honestly: with digests warn-only, a
  *stable* tag (`:v0.1.0`) can still be mutated upstream — the recorded image
  string is what was *requested*; the controller additionally records the
  pulled `imageID` digest from pod `containerStatuses` for what actually
  *ran*. Digest pinning stays recommended (warn-only — decided 2026-07-04).
- **Prerelease versions (component-dev DX):** `prerelease: true` entries may
  use mutable tags (`:dev`), run with `imagePullPolicy: Always` (stable
  versions get `IfNotPresent`), are excluded from default-version resolution,
  and must be pinned explicitly (`version: dev`) in a pipeline. Developer
  loop: register the dev version **once**, then `docker push` → re-run with
  no further registry interaction. A component with only prerelease versions
  errors on an unpinned reference.
- `configSchema` is a string blob (same pattern as the FGA DSL files) — keeps
  the CRD schema simple and feeds straight into a Go JSON Schema validator
  (candidate: `santhosh-tekuri/jsonschema`; final pick at plan time).
- **Definition validation is layered — CRD CEL is deliberately minimal.** The
  CRD schema + one CEL rule cover only field shapes and the
  `:latest`/missing-tag ban. Everything needing real logic — `configSchema`
  compiles as JSON Schema, image reference syntax, `defaultVersion` ∈
  `versions[]`, semver validity/ordering, `resources.default` ≤
  `resources.max` — is validated (a) at registration time by the admin
  API/CLI and (b) continuously by a small ComponentDefinition reconciler in
  the operator that sets `status.phase: Valid|Invalid` + message, so
  kubectl-applied definitions get the same scrutiny. Pipeline validation
  refuses to resolve against an `Invalid` definition.
- **Chart packaging:** the ComponentDefinition **CRD** lives in
  `charts/datuplet-app/crds/` (Helm installs `crds/` before templates and
  never templates it — matches the repo's manually-maintained-CRD
  convention). The built-in **CR instances** (data-generator,
  http-json-extractor, finnhub-extractor, sql-transform, pandas-transform,
  stdout-writer) are Helm **templates**, so image tags follow the chart's
  pinned version. Cluster-scoped CR instances owned by a release imply
  single-install-per-cluster — acceptable, datuplet-app is a cluster
  singleton — and they're removed on uninstall. `queryengine` is not a
  pipeline component (RFC 022 ad-hoc query worker) and stays out of the
  registry.
- `$[secret]` references appear in config *values* and interact with schema
  validation by rule (§4.9): a `$[...]` ref is legal only where the schema
  type is `string`; for ref values the validator checks the `$[...]` syntax
  (existing `pkg/lib/secrets.Validate`) and skips **all content assertions**
  (`format`, `pattern`, `minLength`/`maxLength`, `enum`, `const`) — the ref
  is opaque until the gateway resolves it at boot. A property annotated
  `x-datuplet-secret: true` **requires** a `$[...]` ref — plaintext
  credentials can't be saved into pipeline YAML for fields the component
  declares sensitive, and the UI renders a secret picker there.

CRD lifecycle: `kubectl apply` by cluster operators (GitOps-friendly), plus
`pipeline-api admin component register|list|deprecate` subcommands and
superadmin-gated REST (`/api/v1/admin/components`) so the UI catalog and
future self-service registration have an API. pipeline-api reads definitions
via an informer (component list is small; cache + watch).

### 4.2 Pipeline spec changes

`ComponentSpec` gains/changes ([pipeline_types.go:139-166](../../../pkg/k8s/api/v1/pipeline_types.go)):

```yaml
- name: extract-posts            # instance name (unchanged, unique per pipeline)
  component: http-json-extractor # NEW: registry reference
  version: v0.1.0                # NEW, optional: omitted → latest registered (§4.1)
  config:                        # CHANGED: arbitrary structured object
    url: "https://api.example.com/items"
    headers:
      Authorization: "Bearer $[api_token]"
  inputs:  {…}                   # unchanged
  outputs: {…}                   # unchanged
  # resources:  superadmin-only (see §4.4)
```

SQL transforms read naturally — SQL as a YAML block scalar, typed values
alongside, no `configJSON` and no string-escaping:

```yaml
- name: transform
  component: sql-transform
  config:
    sql: |
      CREATE TABLE order_summary AS
      SELECT country, SUM(total) AS revenue
      FROM orders GROUP BY country;
    threads: 4                   # int — today this alone forces configJSON
  inputs:
    tables: [{bucket: raw, table: orders}]
  outputs:
    tables: [{name: order_summary, bucket: curated, writeMode: FULL_LOAD}]
```

- `config` becomes `apiextensionsv1.JSON` in Go (CRD:
  `x-kubernetes-preserve-unknown-fields: true`). Nested YAML is now
  first-class; **`configJSON` is removed outright** — POC greenfield, no
  deprecation window, no dual-read path. This kills P1 and makes every example
  in `docs/components.md` actually valid.
- **`image` is dropped from `ComponentSpec` entirely** (decided 2026-07-04).
  Its only real use case was dev iteration, which
  prerelease registry versions (§4.1) cover with a one-time registration —
  keeping every image reference inside the registry: uniform validation,
  schema checks even for dev components, and full run-status traceability.
  Adding an escape hatch back later is trivial; removing one that pipelines
  depend on is not.
- Uniform ordering (`name, component, version, config, inputs, outputs`) is
  enforced socially via docs/examples/UI ordering — YAML key order isn't
  enforceable, but the generated UI and all shipped examples always present
  the same shape, which is what "uniform" means in practice.

### 4.3 One validation path

Today's asymmetry (permissive parse at save, strict unmarshal at trigger) is
replaced by a single function used by all three consumers:

```
pkg/pipeline/validate.ValidatePipeline(yaml []byte, reg RegistryView, pol Policy)
    → (*datupletv1.Pipeline, []Finding, error)
```

- strict-decodes into the typed CRD struct (unknown fields = error, so typos
  like `writeMod:` fail at save, not silently no-op). Honesty note on the
  kubectl path: the K8s API server **prunes** unknown fields per
  structural-schema rules instead of erroring, so typo ergonomics there stay
  worse — accepted; enforcement parity concerns what remains after pruning,
  which the controller fully validates. Inside `config`,
  `x-kubernetes-preserve-unknown-fields: true` disables pruning by design —
  the component's JSON Schema is the validator there on both paths,
- runs the existing semantic checks from `pkg/pipeline/config` (bucket/table
  regexes, input/output exclusivity, secret-ref syntax),
- resolves `component`/`version` against the registry and validates `config`
  against that version's JSON Schema (structured findings with JSON paths →
  friendly 400s and UI inline errors),
- checks policy (resources ≤ max, gateway knobs within bounds, prerelease
  versions explicitly pinned).

Callers: pipeline-api `PUT` (full validation, friendly errors), pipeline-api
trigger (revalidation just before the PipelineRun CR is created), and the
PipelineRun controller (same function minus the HTTP dressing — the hard
guarantee; violations → run `FailedUser` with a
`DUPLET_STATUS_MESSAGE`-style reason, no Jobs built; the Pipeline controller
independently sets `status.phase: Invalid` for UI/kubectl feedback).

**Resolve & freeze at run admission (TOCTOU guard).** The PipelineRun
controller validates and resolves **once**, when it admits the run — against
the Pipeline spec and registry state it reads at that moment — and snapshots
the outcome into the run status before building any Job:

```yaml
# PipelineRun.status — resolved fields written at admission and frozen;
# observed runtime fields (imageID) are appended later as pods report.
pipelineGeneration: 7          # generation of the Pipeline that was validated
components:
  - name: extract-posts
    component: http-json-extractor
    version: v0.1.0            # resolved (spec may have omitted it) — frozen
    image: ghcr.io/kacurez/http-json-extractor:v0.1.0   # frozen
    imageID: ""                # observed: filled from pod containerStatuses
```

Every stage's Job is built from this snapshot — never from a re-read of the
live Pipeline or registry (today the controller re-fetches the live Pipeline
at stage boundaries, `pipelinerun_controller.go:183,337`; that goes away) —
so editing a pipeline or registering a new version mid-run cannot change
what an in-flight run executes. Enforcement verdicts are the run
controller's own: `Pipeline.status.phase` is **informational only** and never
trusted as an enforcement input, which also closes the stale-status race
where a just-edited (now invalid) Pipeline still reads `Ready` because
`status.observedGeneration` lags `metadata.generation`.

**`PipelineRun.spec.parameters` is deleted.** It is dead code today — a flat
`map[string]string` no controller consumes (`pipelinerun_types.go:48`) — and
per-run overrides of *structured* config need real merge semantics (target
component, JSON path, type coercion, precedence) that nothing currently
needs. Greenfield: drop the field in Phase 1; re-propose with an actual
design if the need appears.

`pkg/pipeline/config` shrinks to an internal detail of `validate` or is
absorbed by it — plan-time decision; no third dialect survives.

### 4.4 Resource + limits policy (superadmin-gated)

Per the requirement: limits are adjustable, but at superadmin level, not user
level.

- **Defaults:** when a component sets no `resources`, the controller applies
  the registry version's `resources.default`. The "nil = unlimited" behavior
  at [pipelinerun_jobs.go:234](../../../pkg/k8s/controllers/pipelinerun_jobs.go)
  goes away.
- **Ceilings — one contract, two layers (reject, then clamp):**
  `resources.max` is a **full `corev1.ResourceList`**, not just cpu/memory —
  ephemeral-storage and extended resources count (an e2e fixture already
  requests ephemeral-storage). A resource *name* not listed in `max` is not
  allowed in pipeline `resources` at all. Layer 1: pipeline-api **rejects**
  over-max or unlisted-name resources at save/trigger (structured 400
  finding — the user-facing contract). Layer 2: the controller **clamps** to
  `max` (and strips unlisted names) at Job build — defense-in-depth for the
  direct-kubectl path; a clamped run proceeds, with the clamp noted in the
  run status message. Clamping applies unconditionally, even for superadmins
  (raising the ceiling means editing the registry entry, which is
  superadmin-gated). Namespace-level `LimitRange` in project namespaces is
  belt-and-braces, added by project provisioning.
- **Who may set `resources` on a pipeline:** only superadmins may *change* it.
  pipeline-api compares old vs new spec on PUT: a non-superadmin save that
  modifies any `resources` block (or `gateway` knobs beyond chart-configured
  bounds) → 403 with a clear message. Unchanged resubmission of an
  admin-set value is fine, so a superadmin tweak survives later edits by
  regular users.
- Gateway knobs get chart-level bounds (`pipelinePolicy.gateway.maxBufferSize`
  etc.) with generous defaults; only superadmins may exceed them.

### 4.5 Superadmin role

Reuse the FGA model's existing server-level admin rather than inventing a
parallel role system:

- Check: `user X` has `admin` on `server:<lakekeeper-server-object>` — the
  relation already exists in
  [server.fga:13](../../../charts/datuplet-app/files/fga/components/server.fga).
  pipeline-api gains a `mustBeSuperadmin` helper next to `mustHaveRelation`.
- Granting — **subject mapping matters:** the existing bootstrap tuple's
  subject is the literal `user:oidc~admin`, while authenticated pipeline-api
  users are checked as `user:oidc~<user-uuid>` (DB-UUID subjects, same
  convention as `admin grant`, `cmd/pipeline-api/admin.go:584`) — the two do
  not match. So `admin grant --user EMAIL --superadmin` resolves the email to
  the user's UUID and writes `(user:oidc~<uuid>, admin, server:<uuid>)`, and
  lakekeeper-bootstrap additionally writes that UUID-subject tuple for the
  seed admin (the legacy `user:oidc~admin` tuple stays for lakekeeper's own
  console).
- **No FGA model changes, and no second authorization store (decided
  2026-07-04).** The model is copied from lakekeeper; extending it carries
  ongoing merge/maintenance cost (docs/fga-model-upgrades.md, cross-chart
  version-sync hazard) — the `datuplet_super_admin` extension idea is
  dropped. A Postgres `is_superadmin` flag was briefly considered as a
  fallback and **rejected: it would split authorization across two brains**
  (FGA tuples + a DB column — two audit surfaces, two grant paths, divergent
  revocation semantics). It is also unnecessary: pipeline-api *already*
  writes `(user:oidc~admin, admin, server:<uuid>)` at lakekeeper-bootstrap
  and already contains the server-object discovery routine
  (`cmd/pipeline-api/admin_lakekeeper.go:526` — `writeServerAdminTuple`,
  discovery via FGA `/changes` pagination). `mustBeSuperadmin` reuses that:
  discover the `server:<uuid>` once at serve time (memoized), then a plain
  FGA `Check(user, "admin", server)`. Authorization stays 100% OpenFGA.

Superadmin-gated operations: registry CRUD via API/CLI (including prerelease
dev versions), changing `resources`/out-of-bounds gateway knobs, and future
policy edits.

### 4.6 Policy

Greenfield POC: **enforcement is always on**. No permissive mode, no compat
shim, no migration path — the few existing dev pipelines are rewritten by
hand when each phase lands. Chart value block (datuplet-app):

```yaml
pipelinePolicy:
  gateway:                    # non-superadmins must stay within these
    maxChunkSize: 268435456       # 256Mi
    maxBufferSize: 536870912      # 512Mi
    maxTargetFileSize: 1073741824 # 1Gi
```

### 4.7 Registry-driven UI

Phase-gated (last), all vanilla ES modules, no build step:

1. **Catalog page** (`/ui/components`): list ComponentDefinitions with
   description, versions, deprecation — rendered from
   `GET /api/v1/components` (readable by any authenticated project member —
   deliberate for the POC: the catalog *is* the shared component picker;
   per-project visibility is already a stated non-goal, §2).
2. **Pipeline builder v1**: "add component" → dropdown from catalog →
   YAML snippet inserted with the right shape + a docs panel showing the
   config schema. Save-time errors render inline with JSON paths (the API
   now returns structured findings). The textarea stays — it becomes the
   "advanced" view rather than the only view.
3. **Pipeline builder v2**: schema-generated form for config (hand-rolled
   renderer for string/number/bool/enum/array + object fallback to a JSON
   subeditor), plus inputs/outputs pickers backed by the existing storage
   endpoints. Two-way form↔YAML sync deliberately out of scope; "edit as
   YAML" is a one-way toggle with a confirm.

### 4.8 Examples consolidation (independent of everything above)

Target layout — **one** folder, K8s dialect only:

```
examples/
  README.md                     # index; how to run each: UI, REST, kubectl
  pipelines/
    simple-http-extract.yaml    # from k8s/simple-pipeline.yaml
    full-etl.yaml               # from k8s/full-pipeline.yaml
    etl-duckdb.yaml             # NEW — fills the missing Makefile reference
    incremental-reads.yaml      # port of pipelines/incremental-test.yaml
    processors-drop.yaml        # port of pipelines/processor-pipeline.yaml
    secrets-http-auth.yaml      # $[token] + managed project-secrets showcase
```

- Delete `examples/local-dev/` (dead: local mode no longer exists) and the
  CLI-mode `examples/pipelines/*` after porting. SDK component-dev samples
  live with the SDKs (`sdk/go`, `sdk/python`), not in examples.
- Dead files and broken references are fixed immediately (Phase 0); the
  example YAMLs then evolve in lockstep with the spec — structured `config`
  in Phase 1, `component:` refs in Phase 2 — as part of each phase's
  definition of done, enforced by the CI guard below. No YAML is authored in
  a shape that's about to be deleted. Each example carries a header comment
  stating what it demonstrates + exact run instructions for UI, REST and
  kubectl.
- **CI guard:** a Go test walks `examples/pipelines/*.yaml`, strict-decodes
  into `datupletv1.Pipeline`, and runs the shared validator. Examples can no
  longer rot silently. Update all references: `README.md:54`, `Makefile:265+`
  (incl. the broken duckdb target), `docs/quickstart-kind.md:153`,
  `docs/quickstart-gke.md:278`, `docs/pipeline-api.md:157`,
  CLAUDE.md key-directories table.
- `docs/components.md`: fix the five component entries so every YAML block is
  CRD-valid; add the missing `pandas-transform` entry; after Phase 2, add
  "registry name + version" to each entry.

### 4.9 Secrets management

**What stays (the runtime is sound):** `$[name]` whole-scalar refs in
`config`, resolution at gateway boot from `/var/run/secrets/datuplet/`,
Secret volume on the gateway sidecar only (0440, `fsGroup` 65532), the
`SecretsResolved` run condition, and the fail-fast gateway crash on a
missing key. Nothing about resolution changes.

**What is redesigned (the management surface):**

- **One managed Secret per project.** pipeline-api owns a well-known K8s
  Secret in each project namespace (`datuplet-project-secrets`). Its keys
  are the `$[name]` names. It is **ensured lazily** — on the first
  secrets-API write or first run — via the same ensure path as the project
  namespace itself (namespaces are provisioned lazily today,
  `pkg/pipelineapi/runbackend/k8s.go`), not assumed to exist.
- **`Pipeline.spec.secretsRef` is deleted** (greenfield, like `configJSON`),
  and runs mount a **per-run snapshot, not the shared store**: at run
  admission — the same moment as the §4.3 resolve-&-freeze — the controller
  copies **only the keys the pipeline references** from
  `datuplet-project-secrets` into a per-run Secret (ownerRef'd to the
  PipelineRun, deleted with it; same pattern as the existing per-run token
  Secret, `pkg/pipelineapi/k8s/run_create.go`) and mounts *that* on the
  gateway sidecar. Consequences, all load-bearing: a missing key fails the
  run `FailedUser` at admission **before any pod exists** (race-free — the
  check and the copy are one read, and it guards the kubectl path too);
  rotation semantics are exact (a value update affects the next run,
  **never** an in-flight one — a shared-Secret mount could leak a write
  landing between trigger and gateway boot); and the gateway only ever sees
  the keys its pipeline references, not the whole project store.
  Per-pipeline secret *namespacing* stays future work; within a project
  every pipeline author is `data_admin` anyway.
- **Write-only secrets API** (values can be written, never read back):
  - `GET    /api/v1/projects/{pid}/secrets` → key names + timestamps only
  - `PUT    /api/v1/projects/{pid}/secrets/{key}` → create/update one key
  - `DELETE /api/v1/projects/{pid}/secrets/{key}`
  Writes are **server-side merge-PATCHes touching only that key** — atomic
  on the API server, so concurrent kubectl/API writers can't lose each
  other's keys (no read-modify-write of the whole Secret). Per-key
  timestamps live as annotations on the same Secret object
  (`datuplet.io/updated-<key>: <RFC3339>`) — K8s has no per-key metadata,
  and this keeps it one object, one store. Authz: `data_admin` to
  write/delete, `datuplet_member` to list names. kubectl on the managed
  Secret keeps working (GitOps escape hatch) — the API is a convenience
  layer over the same object, no second store, no split brain.
- **RBAC narrowing (required, not optional):** pipeline-api's ClusterRole
  already carries broad Secret verbs
  (`charts/datuplet-app/templates/pipeline-api/rbac.yaml:47`); adding `get`
  there would let the SA read *any* Secret cluster-wide. Phase 1.5 moves
  all Secret verbs off the ClusterRoles: project provisioning creates a
  namespace-scoped `Role` + `RoleBinding` in each project namespace for
  pipeline-api (manage `datuplet-project-secrets` + run-token Secrets) and
  for the operator (read the project store + create per-run snapshot
  Secrets).
- **Validation ladder** (replaces "find out at FailedMount"): at **save**,
  a `$[name]` referencing a key absent from the project store yields a
  *warning* finding (create-pipeline-first flows stay legal); at
  **trigger**, missing keys are a hard 400 (UX pre-check); the
  **admission-time snapshot copy is the authoritative, race-free check**
  on both API and kubectl paths; the gateway boot check remains as
  defense-in-depth.
- **`SecretsResolved` condition survives the `secretsRef` deletion:** the
  status writer keys off `SecretsRef != nil` today
  (`pkg/k8s/controllers/pipelinerun_status.go:20`) and is re-keyed to
  "per-run snapshot Secret present".
- **Schema-aware secret fields:** component `configSchema` marks sensitive
  properties with `x-datuplet-secret: true` (custom JSON Schema annotation,
  ignored by standard validators). Effect: the value MUST be a `$[...]` ref
  (plaintext rejected at save), and the UI builder renders a secret picker
  fed by the list endpoint instead of a text input. The general rule for
  all fields: `$[...]` is allowed wherever the schema type is `string`; for
  ref values **all content assertions are skipped** — `format`, `pattern`,
  `minLength`/`maxLength`, `enum`, `const` — because they would evaluate
  the placeholder, not the resolved secret. Only the string type gate
  applies. **Sequencing:** this sub-feature needs schemas to exist, so
  `x-datuplet-secret` *enforcement* rides with Phase 2 and the *picker*
  with Phase 4; Phase 1.5 proper is only the store/API/snapshot/UI-page.
- **UI:** `settings-secrets.js` stops being a kubectl cheat-sheet and
  becomes the real page: list key names + created/updated, add/update a key
  (value input is masked, write-only), delete with a confirm. Lands with
  Phase 1.5.

---

## 5. Phasing

Each phase lands via its own PR train, independently shippable, e2e-gated
(`make e2e-k8s` for controller/chart phases).

| Phase | Content | Size | Depends on |
|---|---|---|---|
| 0 | Examples: delete dead folders, consolidate to one tree, fix Makefile/docs refs, CI guard (§4.8) | S | — |
| 1 | Config unification: structured `config` **replaces** `config`+`configJSON` (breaking — POC), single `ValidatePipeline` path, strict decode at PUT (fixes save-OK/trigger-fail), delete dead `PipelineRun.spec.parameters`; examples/docs swept to the new shape | M | — |
| 1.5 | Secrets management (§4.9): managed project Secret, delete `spec.secretsRef`, per-run snapshot mount, write-only merge-PATCH API, RBAC narrowing, validation ladder, real secrets UI page (`x-datuplet-secret` enforcement rides with 2, picker with 4) | M | 1 (validator); parallel to 2 |
| 2 | `ComponentDefinition` CRD + definition reconciler (status Valid/Invalid) + chart-shipped built-ins + RBAC + resolution/enforcement with run-admission resolve-&-freeze snapshot + prerelease dev versions + config schema validation; examples flip to `component:` refs | L | 1 |
| 3 | Superadmin role + resource policy: registry default/max, controller clamps, superadmin-gated `resources`/gateway bounds, LimitRange in project provisioning | M | 2 |
| 4 | UI: catalog page, builder v1 (picker + docs + inline errors), builder v2 (schema forms incl. `x-datuplet-secret` pickers) | M–L | 2, 1.5 (3 for resource UI) |

Phase 0 can start immediately and is pure cleanup value even if the rest is
re-scoped.

### Touched-file map (from code recon)

- CRD types: `pkg/k8s/api/v1/pipeline_types.go`,
  `pipelinerun_types.go` (drop `spec.parameters`, add
  `status.pipelineGeneration` + `status.components[]`), new
  `component_types.go` (DeepCopy for pointer fields), manifests in
  `charts/datuplet-app/crds/` (manually maintained — both copies: also
  `utils/deploy/k8s/crds/`).
- Validation: new `pkg/pipeline/validate/`; `pkg/pipeline/config/` absorbed.
- API: `pkg/pipelineapi/http/pipeline_handlers.go` (PUT),
  `run_handlers.go` (trigger), new `component_handlers.go` + admin routes,
  `pkg/pipelineapi/k8s/pipeline_apply.go` (drop duplicate unmarshal),
  authz superadmin helper in `pkg/pipelineapi/http` + `authz`.
- Controller: `pkg/k8s/controllers/pipelinerun_jobs.go` (build from the
  status snapshot, apply default/clamped resources),
  `pipelinerun_controller.go` (run-admission validate + resolve-&-freeze,
  fail-fast, error taxonomy), pipeline controller (phase Invalid), new
  ComponentDefinition reconciler (status Valid/Invalid).
- Charts: `datuplet-app` — ComponentDefinition CRD in `crds/`, built-in CR
  templates, `pipelinePolicy` values, and **RBAC**
  (`templates/pipeline-operator/rbac.yaml`, `templates/pipeline-api/rbac.yaml`):
  operator gets `componentdefinitions` get/list/watch **+
  `componentdefinitions/status` update/patch** (the reconciler writes
  Valid/Invalid); pipeline-api gets get/list/watch **+
  create/update/patch/delete** (admin registry CRUD). Secret verbs move off
  the ClusterRoles into per-project-namespace Roles (§4.9). No FGA file
  changes.
- UI: `ui/product/pages/` (components.js, pipeline-detail.js,
  settings-secrets.js rewrite), `api.js`.
- CLI: `pipeline-api admin component …`, `admin grant --superadmin`.
- Secrets (Phase 1.5): new `pkg/pipelineapi/http/secret_handlers.go`
  (per-key merge-PATCH writes, annotation timestamps), lazy ensure of the
  managed Secret alongside the namespace ensure
  (`pkg/pipelineapi/runbackend/k8s.go`), controller snapshot logic in
  `pipelinerun_controller.go`/`pipelinerun_jobs.go` (copy referenced keys →
  per-run Secret at admission, mount that; `applySecretsMount` keys off the
  snapshot), `pipelinerun_status.go` (`SecretsResolved` re-keyed to
  snapshot presence), `spec.secretsRef` removed from `pipeline_types.go` +
  CRD manifests, per-namespace secret Roles in project provisioning,
  docs/secrets.md rewrite.

---

### Test impact (e2e)

The K8s e2e scenarios apply Pipeline/PipelineRun CRs **directly via kubectl**
(hand-minting the run token — `tests/e2e/framework/k8s_token.go`), i.e. they
exercise precisely the direct-CRD path that controller-side enforcement
guards. That's a feature: after Phase 2 the suite proves the security
boundary for free. Required work per phase:

- **Phase 1:** rewrite the two `configJSON` fixtures
  (`manual-large-input-join.yaml`, `error-bad-config.yaml`) to structured
  `config`. No harness-code impact — e2e is a separate Go module that submits
  raw YAML and does not import `pkg/k8s/api`.
- **Phase 2:**
  - Fixtures use locally built `datuplet/<name>:latest` images (all 14
    files). e2e bootstrap applies test `ComponentDefinition`s registering
    those local images as **prerelease `dev` versions** (mutable tags are
    legal there; `imagePullPolicy: Always` keeps every run on the freshest
    local build), and fixtures flip `image:` → `component:` + `version: dev`.
    This doubles as continuous coverage of the component-dev DX path.
  - `error-bad-config` changes meaning: schema-invalid config now fails
    fast (save 400 / controller `FailedUser` before scheduling) instead of at
    component runtime — assertions updated; `error-crash.yaml` keeps covering
    genuine runtime failure.
  - New scenarios: reference to an unregistered component name/version →
    Pipeline `Invalid` + run `FailedUser`, nothing scheduled (kubectl path);
    omitted `version:` → latest registered stable resolved + recorded in
    `PipelineRun.status.components[]`; mid-run Pipeline edit does not affect
    the in-flight run (freeze snapshot).
- **Phase 1.5:** `secrets-happy.yaml` switches from a hand-created Secret +
  `spec.secretsRef` to the managed project Secret written via the API; new
  assertions: save with a missing key → warning finding, trigger with a
  missing key → 400, kubectl-path run with a missing key → `FailedUser` at
  admission with no pods created, run resolves after `PUT /secrets/{key}`,
  per-run snapshot Secret is deleted with the run, and a value update after
  trigger does **not** change the in-flight run's resolved config.
- **Phase 3:** new scenarios: registry defaults applied to the Job spec;
  over-max resources rejected at save (API path) **and** clamped at Job
  build (kubectl path — run proceeds, clamp noted in status);
  non-superadmin PUT modifying `resources` → 403; superadmin path succeeds.
  Bootstrap gains a superadmin grant for the API-mode tests.

Per repo discipline, every chart/operator/controller phase gates on
`make e2e-k8s` against an OrbStack cluster; the Phase 0 examples CI guard is
unit-level and separate.

## 6. Security considerations

- **Boundary:** the PipelineRun controller is the sole pod creator; its
  enforcement holds even for CRs created with kubectl. A raw Pipeline CR that
  fails validation sits at `phase: Invalid`, inert.
- **RBAC posture (documented, Phase 2):** end users get no K8s RBAC on
  `datuplet.io` resources; pipeline-api's ServiceAccount is the only writer.
  The registry protects against *casual* misuse; K8s RBAC remains the wall
  against hostile cluster users.
- **What the registry does NOT protect against:** a malicious *registered*
  image. Digest pinning (recommended, warn-only) prevents tag-mutation
  attacks when used; the recorded pod `imageID` (§4.3) at least makes
  mutation detectable after the fact. Provenance/signing is future work.
- Blast radius of a rogue component container today, unchanged and worth
  restating: no K8s SA token, no secret mounts (gateway-only), all caps
  dropped, seccomp RuntimeDefault; it can use compute, egress the network,
  and touch data its run grants allow. Egress NetworkPolicy is the missing
  piece — deliberately deferred (§9, resolved decision 4).

## 7. Error handling contract

- pipeline-api PUT/trigger: 400 with `{findings: [{path, message}]}` for
  validation; 403 for policy (non-superadmin touching `resources`,
  out-of-bounds gateway knobs, registry CRUD).
- Controller error taxonomy — explicit, because today job-build errors
  blanket-classify as `FailedApplication`
  (`pipelinerun_controller.go:359`); Phase 2 fixes the classification:
  - spec/schema/policy validation failure, unresolvable component/version,
    reference to an `Invalid` definition → run `FailedUser` (exit-code
    contract 1) with the first finding in the status message;
  - registry informer not synced, K8s API errors, image-pull infrastructure
    failures → transient requeue (escalating to `FailedApplication`, ≥20,
    after the retry budget) — never `FailedUser`.
- Unknown YAML fields fail at save with the field path (strict decode; the
  kubectl path prunes instead — see §4.3).
- Secrets ladder (§4.9): missing key at save → warning finding; at trigger →
  400 (UX pre-check); at run admission the snapshot copy is the
  authoritative, race-free check → `FailedUser` before any pod; gateway
  boot crash + `SecretsResolved: False` stays as defense-in-depth. Secret
  *values* never appear in findings, logs, or GET responses.

## 8. Alternatives considered

1. **Validating admission webhook** instead of controller-side enforcement:
   stronger (CRs can't even be created invalid) but brings cert management +
   availability coupling into a 0.x product; controller enforcement gives the
   same "nothing bad runs" guarantee. Revisit when there's a GA-hardening RFC.
2. **Registry in Postgres via pipeline-api** instead of a CRD: easier CRUD UI,
   but the controller would depend on pipeline-api availability for
   enforcement, direct-CRD bypass would re-open, and GitOps flows lose. CRD +
   informer keeps a single source of truth all three consumers can watch.
3. **Helm-values/ConfigMap allowlist** (image prefix list): trivially simple,
   but no per-component schemas, versions, resource policy, or UI catalog —
   fails most of the goals. Rejected; prerelease dev versions (§4.1) cover
   the "just let me hack" need.
4. **Keep `configJSON`, add schema validation only:** keeps the wart the whole
   RFC exists to remove; docs and UI would still teach two dialects.

## 9. Decisions & open questions

### Resolved (maintainer review, 2026-07-04)

1. **CRD kind name:** `ComponentDefinition`, registry as CRD — approved.
2. **Default version:** omitted `version:` → latest *registered* version
   (registry-controlled), never a floating image tag; `:latest` images
   rejected at registration. See §4.1.
3. **Structured config incl. SQL:** everything under `config:` as plain YAML
   (SQL block scalars, ints, arrays, maps); `configJSON` removed outright.
   See §4.2.
4. **Component egress NetworkPolicy:** deferred, out of scope.
5. **examples/ layout:** flat `examples/pipelines/` as proposed.
6. **POC greenfield posture:** no migration paths, no deprecation windows —
   `configJSON` removed outright, no compat shim, no permissive mode,
   enforcement always on.
7. **Superadmin = FGA `server.admin`, single authorization brain:** no FGA
   model changes AND no Postgres role flag (rejected as authz split-brain).
   Addressability is already proven in code — bootstrap writes the
   `server.admin` tuple today (`admin_lakekeeper.go:526`); serve-mode
   discovers + memoizes the `server:<uuid>` object the same way. Caveat
   folded in from external review: grants must use **UUID subjects**
   (`user:oidc~<user-uuid>`) because the bootstrap tuple's literal
   `user:oidc~admin` subject doesn't match session users — see §4.5.
   `datuplet_super_admin` model extension dropped.
8. **Digest pinning:** warn-only in 0.x (the `:latest` ban on stable versions
   already prevents the default path from floating on a mutable tag).
9. **`resources` UX:** stays in pipeline YAML, diff-gated to superadmins
   (§4.4).

10. **Raw `image:` dropped from `ComponentSpec` entirely** — prerelease
    registry versions (§4.1) are the only path for unreleased images; no
    superadmin escape hatch. Confirmed after the prerelease design landed.
11. **Secrets management joins the redesign (§4.9):** runtime resolution
    model unchanged; management moves from kubectl to a write-only
    project-scoped API + UI over one managed K8s Secret;
    `Pipeline.spec.secretsRef` deleted — runs mount a **per-run snapshot of
    referenced keys** taken at admission (race-free validation, exact
    rotation semantics, least-privilege exposure); per-key merge-PATCH
    writes; Secret RBAC narrowed to per-namespace Roles;
    `x-datuplet-secret` schema annotation drives pickers + forbids plaintext
    in sensitive fields. Still a single store (the K8s Secret) and a single
    authz brain (FGA `data_admin`).

### Still open

None — all design questions resolved as of 2026-07-04.

---

## Appendix A — Examples audit (2026-07-04)

| File | Verdict | Evidence |
|---|---|---|
| `examples/k8s/simple-pipeline.yaml` | current | referenced README.md:54, quickstarts, Makefile:265 |
| `examples/k8s/full-pipeline.yaml` | current | Makefile:276 |
| `examples/k8s/duckdb-pipeline.yaml` | **missing** | referenced by Makefile:270 (`k8s-retry-duckdb` fails) |
| `examples/pipelines/simple-pipeline.yaml` | stale header | "Run with ./bin/datuplet run <file>" — CLI now requires `--remote`; CLI dialect |
| `examples/pipelines/etl-pipeline.yaml` | stale header + nested config (not CRD-valid) | same |
| `examples/pipelines/processor-pipeline.yaml` | works but confusing | bucket-input without table on stdout-writer |
| `examples/pipelines/incremental-test.yaml` | current (CLI dialect) | `since:` matches CRD |
| `examples/local-dev/*` | **dead** | local mode removed; no refs from tests/docs/Makefile |

e2e fixtures (`tests/e2e/pipelines/k8s/`) are separate, templated, and stay
that way.

## Appendix B — Current authz relations in use

- `data_admin` — PUT/DELETE pipeline, trigger run
  ([pipeline_handlers.go:126](../../../pkg/pipelineapi/http/pipeline_handlers.go))
- `describe` — read pipelines/runs
- FGA `server.admin` — exists in model, unused by pipeline-api today
