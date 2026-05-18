# Contributing to Datuplet

Contributor's guide. Users deploying Datuplet should start at
[README.md](README.md) and [docs/install.md](docs/install.md).

Datuplet is **experimental**. APIs, CRD shapes, and chart values may change
between 0.x releases.

## Project overview

Datuplet is a streaming ETL platform for pipelines with limited local disk.
Components process data in chunks via Apache Iceberg on object storage
(MinIO / S3 / GCS). A **Data Gateway sidecar** handles all storage I/O on
behalf of each component, so component SDKs stay ~300 LOC per language.

**Key architectural principle:** storage paths flow through the system as
**opaque strings**. Lakekeeper allocates the per-table data prefix; the
Data Gateway passes whatever URL it gets back verbatim; only the backend
layer interprets URLs.

Primary languages: Go (backend, operators, CLI), Python (SDK + a few
scripts), YAML (K8s manifests), Shell (automation). TypeScript only in
the browser UI.

## Working style

- Sketch a 2-3 bullet plan before non-trivial changes; explore with
  `grep` / `find`, don't guess at paths.
- One logical commit per task. `go build ./... && go test ./...` (or at
  least the touched package) before every commit.
- Keep diffs minimal — no scope creep, no premature abstraction, no
  comments that restate the code.
- Chart/operator/controller changes need `make e2e-k8s` against an
  OrbStack cluster too — unit tests don't catch deployment-behaviour bugs.

## Branch + release discipline

- All work lands via PR off a feature branch. Never push directly to `main`.
- **Agents must never `git push origin main` or `git tag`.** Push the
  feature branch, open a draft PR with `gh pr create --draft`, mention
  the PR number in the response. The maintainer reviews, merges, and cuts
  release tags (`v0.x.y`) — the tag triggers the release workflow.
- Applies to partial-progress checkpoints too: land via PR, not direct push.

## Documentation pointers

- User-facing entries: [README.md](README.md), [docs/install.md](docs/install.md),
  [docs/quickstart-kind.md](docs/quickstart-kind.md),
  [docs/quickstart-gke.md](docs/quickstart-gke.md).
- Reference: [docs/architecture.md](docs/architecture.md),
  [docs/pipeline-api.md](docs/pipeline-api.md),
  [docs/auth-flow.md](docs/auth-flow.md),
  [docs/warehouse-setup.md](docs/warehouse-setup.md).
- Upgrade discipline: [docs/postgres-migrations.md](docs/postgres-migrations.md),
  [docs/fga-model-upgrades.md](docs/fga-model-upgrades.md).
- Honesty list: [docs/known-limitations.md](docs/known-limitations.md).
- Proto definitions: `api/proto/`; main service is
  `api/proto/gateway/v2/gateway.proto`. Generated Go is checked in alongside.

## Non-obvious conventions

These are conventions a new contributor wouldn't infer from the code:

- **Never use `filepath.Join` or `path.Join` for storage paths** — they
  corrupt URLs (e.g., `s3://bucket` becomes `s3:/bucket`). Use the
  package-local `joinStoragePath()` helper (defined in
  `pkg/datagateway/server_v2.go`, `pkg/datagateway/partition/router.go`,
  `pkg/datagateway/buffer/manager.go`).
- **`pkg/lib/datalake/` is metadata-only fallback I/O** (Read / Write / List),
  used by TableCommit (manifest read) and the pipeline-api storage walker
  fallback. **`pkg/datagateway/backend/`** is the full data-plane
  abstraction (buffering, format conversion, backends). Don't confuse the two.
- **Buffer package (`pkg/datagateway/buffer/`) outputs Parquet only.** For
  other formats on read, use `pkg/datagateway/format/` adapters to convert
  on the fly.
- **Exit code contract**: `0=Succeeded`, `1=FailedUser`, `>=20=FailedApplication`.
  Status messages use the `DUPLET_STATUS_MESSAGE:` prefix in stdout.
- **CRD manifests are manually maintained** in `charts/datuplet-app/crds/`
  — not auto-generated. CRD type changes require updating `DeepCopyInto`
  for pointer fields.
- **`UseSSL` and `UsePathStyle`** are independent storage-credential
  concerns — never infer one from the other.
- **Secret references use `$[name]`** (whole-scalar only) inside
  `component.config`. Resolved by the Data Gateway sidecar at boot from
  files mounted under `/var/run/secrets/datuplet/`. The K8s
  `Pipeline.spec.secretsRef.name` names a Secret in the same namespace.
- **Run-token references** use `PipelineRun.spec.runTokenRef.name`. The
  named Secret carries a `token` key projected at
  `/var/run/secrets/datuplet-runtoken/token` **on the gateway sidecar
  only**, never on the component container. Pod hardening is unconditional:
  `automountServiceAccountToken: false` on every Pod; the component
  container drops all Linux capabilities and runs with
  `seccompProfile: RuntimeDefault`.
- **K8s is the only supported deployment surface.** No local-file mode,
  no docker-compose. A new feature is complete when it works on K8s:
  update CRD types in `pkg/k8s/api/v1/`, update controllers in
  `pkg/k8s/controllers/`, update CRD manifests in
  `charts/datuplet-app/crds/`, and exercise via the pipeline-api REST
  handlers.
- **Lakekeeper is the catalog of record.** Data Gateway calls lakekeeper
  for table create/load + STS-vended credentials
  (`pkg/datagateway/lakekeeper/`); TableCommit posts metadata commits to
  lakekeeper REST (`pkg/icebergjob/`); pipeline-api's `/api/v1/storage`
  handlers proxy lakekeeper via a service-account JWT
  (`pkg/pipelineapi/storage/catalog_proxy.go`) when
  `DATUPLET_LAKEKEEPER_URL` is set, falling back to a directory walker
  for tests + local-mode-without-lakekeeper.
- **Per-table files.json manifests**: Data Gateway writes one `files.json`
  per `(namespace, table)` at `<table-base>/.run-state/<run-id>/files.json`
  (inside the table's lakekeeper-managed prefix). Wire shape in
  `pkg/datagateway/files_manifest.go`:
  `{"run_id":"...", "namespace":"...", "table":"...", "paths":[...]}`.
  TableCommit loads the table, derives the per-table manifest path, reads
  through the iceberg-go FS (vended creds, not the long-lived MinIO mount),
  then runs `txn.AddFiles` + `Commit`. Missing manifest on a known table
  = success-zero.
- **Per-run JWT**: pipeline-api mints **one RS256 JWT per run** via
  `tokens.MintRunToken`. Audience is `datuplet-catalog` (lakekeeper). The
  K8s run-token Secret carries a single `token` key.
- **Cancellation is OpenFGA tuple deletion** (not a denylist). Reaper
  sweeps stragglers. Cancel deletes FGA tuples first so the blast radius
  is ≤15 s (the STS-credential renewal cadence). The pipeline-api cancel
  path also patches `datuplet.io/cancel=true` on every component pod;
  the Data Gateway watches the downward-API annotation file and exits
  cleanly.
- **Browser UI** lives at `ui/product/` (vanilla ES modules, no build step).
  Pipeline-api serves it at `/ui/*` when `PIPELINE_API_UI_DIR` is set; the
  K8s Deployment sets this to `/app/ui/product`. On 401 the fetch wrapper
  redirects to `/ui/login`.
- **`pipeline-observer` runs in its own Deployment** (single replica,
  single-writer to the `runs` table). Pipeline-api defaults to 2 replicas
  (HTTP-only). The 24h reaper lives in a separate CronJob with a narrower
  ServiceAccount.
- **Storage UI security**: strict identifier validation,
  canonical-containment check, symlink rejection, no SQL surface, ≤100
  rows / 1 MiB preview cap. Exact regex in `pkg/pipelineapi/storage/`.
- **Fine-grained authz via OpenFGA**: synthetic run identities are
  `user:oidc~<run-uuid>`; trigger writes
  `(user:oidc~<run-uuid>, editor, project:<lakekeeper-project-id>)`. Each
  run gets one RS256 JWT (`aud=datuplet-catalog`, 24h). Interactive
  storage browse uses 5-minute impersonation JWTs
  (`tokens.MintImpersonation`).
- **JWT-driven warehouse routing**: Data Gateway sidecars and TableCommit
  Jobs validate the mounted run-token JWT against pipeline-api's JWKS at
  boot. The validator checks signature (RS256 only), `iss=datuplet-api`,
  `aud=datuplet-catalog`, exp/nbf with ±60 s skew, `token_kind=run`,
  required non-empty `project_id`/`warehouse`/`run_id`/`sub`,
  `sub == run_id`, and `run_id == $RUN_ID` (Secret-swap defence). On any
  failure, the binary fails fast at boot.
- **Zero long-lived storage credentials at runtime — for S3 and GCS.**
  Lakekeeper holds warehouse credentials; all runtime access uses
  lakekeeper-vended STS (S3) / OAuth-bearer (GCS) credentials. The
  `pipeline-api admin lakekeeper-bootstrap` subcommand registers
  credentials once per warehouse, then never reads them again. For GCS,
  both `--gcs-credential-type=system-identity` (Workload Identity
  Federation; default) and `--gcs-credential-type=service-account-key`
  are first-class.
- **`pkg/datupleticeio/`** centralizes Datuplet's iceberg-go IO scheme
  registrations. It overrides iceberg-go's default `gs://` factory with
  one that consumes `gcs.oauth2.token` from props and wraps it in a
  refreshing `oauth2.TokenSource`. Every binary that calls into
  iceberg-go must blank-import this package — registration is
  load-bearing at process init time. See RFC 019 §4.5.
- **Bearer-credential redaction (RFC 019 §4.10)** is enforced via
  `String()` Stringer methods on `pkg/catalogwriter.{S3Creds, GCSCreds}`
  and the `*vendedTokenSource` / `*refreshingTokenSource` types. They
  redact secret fields under `%v` / `%+v` / `%s`. `%#v` deliberately
  bypasses Stringer — reviewers catch that one.
- **`sql-transform` component**: runs user SQL inside an embedded DuckDB
  engine and is **credentials-clean** — it never holds S3, GCS, or
  lakekeeper credentials. Inputs stream from Data Gateway via Arrow IPC
  and are materialized into a DuckDB table before user SQL runs (workaround
  for duckdb-go's GROUP BY / JOIN / UNION ALL bug against registered Arrow
  streams). Outputs run `COPY <name> TO '<staged>.parquet'` and stream
  the file back via the SDK.
- **FGA model versioning**: `fgaModel.version` in
  `datuplet-app/values.yaml` and `platform.fgaModelVersion` in
  `datuplet-lakekeeper/values.yaml` must be kept in sync on FGA model
  upgrades. CI enforces the DSL→version coupling in `datuplet-app` but
  does NOT enforce cross-chart drift — reviewers must catch that manually.
- **`pg-lakekeeper` + `pg-lakekeeper-pw` Secrets** are created in the
  `infra` chart's keygen Job and consumed cross-phase by the `lakekeeper`
  chart's lakekeeper Deployment.
- **CNPG CRDs** live in `charts/datuplet-operators/crds/` (sidesteps
  helm's pre-flight REST-mapper validation issue for
  `Cluster.postgresql.cnpg.io`).
- **Monorepo `go mod tidy`**: the repo has multiple Go modules
  (`./`, `tests/e2e/`, `components/*/`, `sdk/go/`, `sdk/go/arrow/`,
  `examples/local-dev/`). Tidying root in isolation drifts the others —
  their Docker builds + e2e enforce parity and fail CI. Always run
  `make tidy` (covers every module).

## Key directories

| Directory | Purpose |
|-----------|---------|
| `pkg/pipeline/` | Pipeline execution engine. |
| `pkg/datagateway/` | Data Gateway service (format conversion, processors, buffering). |
| `pkg/datagateway/backend/` | Storage backends (MinIO/S3, GCS, local FS). |
| `pkg/datagateway/lakekeeper/` | Data Gateway's lakekeeper resolver. |
| `pkg/datupleticeio/` | iceberg-go `gs://` IO factory override (vended OAuth tokens). |
| `pkg/icebergjob/` | TableCommit Job binary (per-table iceberg-go transactions). |
| `pkg/catalogwriter/` | Shared lakekeeper REST client + VendedCreds. |
| `pkg/k8s/api/v1/` | CRD types (Pipeline, PipelineRun). |
| `pkg/k8s/controllers/` | PipelineRun controller; schedules per-table commit Jobs. |
| `cmd/pipeline-operator/` | Operator binary. |
| `cmd/pipeline-observer/` | Standalone informer; mirrors PipelineRun status to DB. |
| `cmd/pipeline-api/` | HTTP server + admin subcommands. |
| `pkg/pipelineapi/` | HTTP handlers, auth, authz, runbackend, store, storage UI. |
| `pkg/lib/datalake/` | Metadata-only fallback I/O. NOT the data plane. |
| `sdk/go/`, `sdk/python/` | Thin SDKs (~200 LOC each). |
| `sdk/python-sandbox/` | Sandbox library (DuckDB + pre-signed URLs). |
| `components/` | Built-in extractors, transforms, writers. |
| `ui/product/` | Browser SPA (vanilla ES modules). |
| `charts/` | Four Helm charts: `datuplet-operators`, `-infra`, `-app`, `-lakekeeper`. |
| `utils/docker/` | Dockerfiles for all services. |
| `examples/k8s/`, `examples/pipelines/` | Example manifests. |
