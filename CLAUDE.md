# Contributing to Datuplet

This file is the contributor's guide for the Datuplet codebase. If you're a
**user** trying to deploy Datuplet, start at [README.md](README.md) and
[docs/install.md](docs/install.md) instead.

Datuplet is **experimental** software. APIs, CRD shapes, and chart values may
change between 0.x releases. PRs are welcome but expect rebase churn.

## Project overview

Datuplet is a streaming ETL platform for pipelines with limited local disk.
Components process data in chunks via Apache Iceberg on object storage
(MinIO / S3 / GCS), and a **Data Gateway sidecar** handles all storage I/O on
behalf of each component, so component SDKs stay ~300 LOC per language.

**Key architectural principle**: storage paths flow through the system as
**opaque strings**. Lakekeeper allocates the per-table data prefix; the
Data Gateway passes whatever URL it gets back verbatim; only the backend
layer interprets URLs.

## Languages

Primary: Go (backend, operators), Python (Python SDK + a few scripts), YAML
(K8s manifests), Shell (automation). Markdown for docs. TypeScript is
secondary (browser UI only).

## Working style

For non-trivial changes:

1. **Explore first.** Read README, this file, the relevant doc under
   `docs/`, and the code you're touching. Don't guess at file locations —
   use `grep` and `find`.
2. **Sketch before coding.** A 2-3 bullet plan ("I'll modify these files,
   use this approach, won't touch X") catches misalignments cheaply.
3. **Implement task-by-task.** One logical commit per task. Run
   `go build ./... && go test ./...` (or at least the touched package)
   before every commit.
4. **Keep diffs minimal.** Don't refactor surrounding code, don't add
   abstractions for hypothetical future requirements, don't write
   comments that just restate the code.

## Quick reference

```bash
# Build
make build                    # CLI binary
make build-components         # all component Docker images

# Lint / static analysis
make lint                     # go vet + deadcode
go vet ./...                  # fast compilation check

# Tests
go test -v ./...                              # all tests
go test -v ./pkg/datagateway/format/          # single package
go test -v -run TestParquetRoundTrip ./pkg/datagateway/buffer/

# E2E tests (K8s only — requires OrbStack with Kubernetes enabled)
make e2e-k8s

# Sample mode (data discovery — random rows from a component config)
./bin/datuplet sample --image datuplet/data-generator:latest \
  --config '{"tables":[{"name":"t","random":{"schema":{"id":"int"},"limit":{"rowsCount":5}}}]}'

# Browse Iceberg data via DuckDB (after a pipeline run)
./utils/iceberg-cli.sh -l                              # list tables
./utils/iceberg-cli.sh -la test_simple/products        # table details
./utils/iceberg-cli.sh "SELECT *" -f test_simple/products
```

## Kubernetes deployment (OrbStack — inner-loop developer flow)

```bash
make deploy-local             # build images + install all 4 charts + register

# Trigger a pipeline:
open http://localhost:30081/ui/
# or
kubectl apply -f examples/k8s/simple-pipeline.yaml
kubectl get pipelineruns -n datuplet -w
```

See [docs/install.md](docs/install.md) for the supported install path
(four-chart Helm install) and [docs/quickstart-kind.md](docs/quickstart-kind.md)
for `kind`-based evaluation.

## Helm chart layout

Datuplet ships four charts installed in sequence (five phases counting
`scripts/register.sh`):

```bash
helm dependency update charts/datuplet-operators
helm dependency update charts/datuplet-infra
helm dependency update charts/datuplet-app
helm dependency update charts/datuplet-lakekeeper

helm upgrade --install datuplet-operators  charts/datuplet-operators  -n datuplet --create-namespace --wait --timeout 5m
helm upgrade --install datuplet-infra      charts/datuplet-infra      -n datuplet --wait --wait-for-jobs --timeout 10m
helm upgrade --install datuplet-app        charts/datuplet-app        -n datuplet --wait --wait-for-jobs --timeout 10m
helm upgrade --install datuplet-lakekeeper charts/datuplet-lakekeeper -n datuplet --wait --wait-for-jobs --timeout 10m
./scripts/register.sh --namespace datuplet
```

See [docs/install.md](docs/install.md), [docs/postgres-migrations.md](docs/postgres-migrations.md),
and [docs/fga-model-upgrades.md](docs/fga-model-upgrades.md) for upgrade
discipline.

## Testing

- For changes that touch deployment behaviour (chart templates, controllers,
  operator), run `make e2e-k8s` against an OrbStack cluster. Type-checks and
  unit tests verify code correctness, not feature correctness.
- For backend Go changes, `go test ./...` is sufficient.

## Code reviews

Before opening a PR:

- Build + tests green on touched packages.
- For chart changes: `helm template charts/<chart>` renders cleanly; `make e2e-k8s`
  passes locally.
- Diffs minimal and focused on the stated task.

## Proto / gRPC

Proto definitions live in `api/proto/`. Generated Go code is checked in
alongside the protos.
- `api/proto/gateway/v2/gateway.proto` — DataGateway gRPC service.

## Documentation

User-facing:

- **[README.md](README.md)** — elevator pitch + quickstart pointers.
- **[docs/install.md](docs/install.md)** — supported install workflow.
- **[docs/quickstart-kind.md](docs/quickstart-kind.md)** — 10-minute local
  evaluation on kind.
- **[docs/quickstart-gke.md](docs/quickstart-gke.md)** — GKE + GCS deployment.
- **[docs/architecture.md](docs/architecture.md)** — system overview + data
  flow diagram.
- **[docs/components.md](docs/components.md)** — built-in components catalog.
- **[docs/warehouse-setup.md](docs/warehouse-setup.md)** — S3 / GCS / MinIO
  configuration.
- **[docs/troubleshooting.md](docs/troubleshooting.md)** — common failure
  modes.
- **[docs/known-limitations.md](docs/known-limitations.md)** — honest list
  of what doesn't work yet.

System reference (for contributors and advanced operators):

- **[docs/pipeline-api.md](docs/pipeline-api.md)** — REST API reference.
- **[docs/auth-flow.md](docs/auth-flow.md)** — end-to-end token lifecycle
  (session cookie → run JWT → impersonation JWT, OpenFGA legs).
- **[docs/secrets.md](docs/secrets.md)** — `$[name]` secret references in
  pipelines.
- **[docs/postgres-migrations.md](docs/postgres-migrations.md)** — migration
  discipline.
- **[docs/fga-model-upgrades.md](docs/fga-model-upgrades.md)** — FGA model
  upgrade procedure.

## Non-obvious conventions

These are conventions a new contributor wouldn't infer from the code:

- **Never use `filepath.Join` or `path.Join` for storage paths** — they
  corrupt URLs (e.g., `s3://bucket` becomes `s3:/bucket`). Use the
  `joinStoragePath()` helpers in `pkg/lib/storagepath`.
- **`pkg/lib/datalake/`** is for simple metadata I/O (Read / Write / List),
  used by TableCommit (manifest read) and the pipeline-api storage walker
  fallback (`pkg/pipelineapi/storage/`). **`pkg/datagateway/backend/`** is
  the full data-plane abstraction (buffering, format conversion, backends).
  Don't confuse the two.
- **Buffer package (`pkg/datagateway/buffer/`) outputs Parquet only.** For
  other formats on read, use `pkg/datagateway/format/` adapters to convert
  on the fly.
- **Exit code contract**: `0=Succeeded`, `1=FailedUser`, `>=20=FailedApplication`.
  Status messages use the `DUPLET_STATUS_MESSAGE:` prefix in stdout.
- **CRD manifests are manually maintained** in
  `charts/datuplet-app/crds/` — not auto-generated. CRD type changes
  require updating `DeepCopyInto` for pointer fields.
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
- **K8s is the only supported deployment surface.** No local-file mode, no
  docker-compose mode. Component developers iterate via
  `datuplet run --remote <pipeline-api-url>` against a remote K8s cluster.
  A new feature is complete when it works on K8s: update CRD types in
  `pkg/k8s/api/v1/`, update controllers in `pkg/k8s/controllers/`, update
  CRD manifests in `charts/datuplet-app/crds/`, and exercise via the
  pipeline-api REST handlers.
- **Lakekeeper is the catalog of record.** Data Gateway calls lakekeeper
  for table create/load + STS-vended credentials
  (`pkg/datagateway/lakekeeper/`); TableCommit posts metadata commits to
  lakekeeper REST (`pkg/tablecommit/`); pipeline-api's `/api/v1/storage`
  handlers proxy lakekeeper via a service-account JWT
  (`pkg/pipelineapi/storage/catalog_proxy.go`) when
  `DATUPLET_LAKEKEEPER_URL` is set, falling back to a directory walker
  for tests + local-mode-without-lakekeeper.
- **Per-table files.json manifests**: Data Gateway writes one `files.json`
  per `(namespace, table)` at `<table-base>/.run-state/<run-id>/files.json`
  (inside the table's lakekeeper-managed prefix). The wire shape (in
  `pkg/datagateway/files_manifest.go`) is
  `{"run_id":"...", "namespace":"...", "table":"...", "paths":[...]}`.
  TableCommit loads the table, derives the per-table manifest path, reads
  through the iceberg-go FS (vended creds, not the long-lived MinIO mount),
  then runs `txn.AddFiles` + `Commit`. Missing manifest on a known table =
  success-zero (the run produced no parquet for this target).
- **Per-run JWT**: pipeline-api mints **one RS256 JWT per run** via
  `tokens.MintRunToken`. Audience is `datuplet-catalog` (lakekeeper). The
  K8s run-token Secret carries a single `token` key.
- **Cancellation** is OpenFGA tuple deletion (not a denylist). Reaper
  sweeps stragglers. Cancel deletes FGA tuples first so the blast radius
  is ≤15 s (the STS-credential renewal cadence). The pipeline-api cancel
  path also patches `datuplet.io/cancel=true` on every component pod; the
  Data Gateway watches the downward-API annotation file and exits
  cleanly.
- **Browser UI** lives at `ui/product/` (vanilla ES modules, no build step).
  Pipeline-api serves it at `/ui/*` when `PIPELINE_API_UI_DIR` is set; the
  K8s Deployment sets this to `/app/ui/product`. On 401 the fetch wrapper
  redirects to `/ui/login`.
- **`pipeline-observer` runs in its own Deployment** (single replica,
  single-writer to the `runs` table). Pipeline-api defaults to 2 replicas
  (HTTP-only). The 24h reaper lives in a separate CronJob with a narrower
  ServiceAccount.
- **Storage UI security**: identifier regex
  `^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`, canonical-containment check,
  symlink rejection, no SQL surface, ≤100 rows / 1 MiB preview cap.
- **Fine-grained authz via OpenFGA**: synthetic run identities are
  `user:oidc~<run-uuid>`; trigger writes
  `(user:oidc~<run-uuid>, editor, project:<lakekeeper-project-id>)`. Each
  run gets one RS256 JWT (`aud=datuplet-catalog`, 24h). Interactive
  storage browse uses 5-minute impersonation JWTs
  (`tokens.MintImpersonation`).
- **JWT-driven warehouse routing**: Data Gateway sidecars and TableCommit
  Jobs validate the mounted run-token JWT against pipeline-api's JWKS at
  boot. The validator checks signature (RS256 only), `iss=datuplet-api`,
  `aud=datuplet-catalog`, exp/nbf with ±60s skew, `token_kind=run`,
  required non-empty `project_id`/`warehouse`/`run_id`/`sub`,
  `sub == run_id`, and `run_id == $RUN_ID` (Secret-swap defence). On any
  failure, the binary fails fast at boot (`log.Fatalf` for Data Gateway,
  non-zero return for TableCommit).
- **Zero long-lived S3 credentials** on any Datuplet Deployment at runtime.
  Lakekeeper holds warehouse credentials; all runtime S3 access uses
  lakekeeper-vended STS credentials. The
  `pipeline-api admin lakekeeper-bootstrap --s3-access-key ...` subcommand
  registers credentials once per warehouse, then never reads them again.
- **`sql-transform` component**: runs user SQL inside an embedded DuckDB
  engine and is **credentials-clean** — it never holds S3 or lakekeeper
  credentials. Inputs stream from Data Gateway via Arrow IPC and are
  materialized into a DuckDB table before user SQL runs (workaround for
  duckdb-go's GROUP BY / JOIN / UNION ALL bug against registered Arrow
  streams). Outputs run `COPY <name> TO '<staged>.parquet'` and stream the
  file back via the SDK. Logical names work end-to-end on both inputs and
  outputs.
- **FGA model versioning**: `fgaModel.version` in `datuplet-app/values.yaml`
  and `platform.fgaModelVersion` in `datuplet-lakekeeper/values.yaml` must
  be kept in sync on FGA model upgrades. CI enforces the DSL→version
  coupling in `datuplet-app` but does NOT enforce cross-chart drift —
  reviewers must catch that manually.
- **`pg-lakekeeper` + `pg-lakekeeper-pw` Secrets** are created in the
  `infra` chart's keygen Job and consumed cross-phase by the `lakekeeper`
  chart's lakekeeper Deployment.
- **CNPG CRDs** live in `charts/datuplet-operators/crds/` (this sidesteps
  helm's pre-flight REST-mapper validation issue for `Cluster.postgresql.cnpg.io`).

## Key directories

| Directory | Purpose |
|-----------|---------|
| **Services** | |
| `pkg/pipeline/` | Pipeline execution engine (orchestrates stages, commits). |
| `pkg/datagateway/` | Data Gateway service (format conversion, processors, buffering, partition routing). |
| `pkg/tablecommit/` | TableCommit Job binary (per-table iceberg-go transactions via lakekeeper). |
| `pkg/datagateway/lakekeeper/` | Data Gateway's lakekeeper resolver (LoadOrCreate + vended-creds backend). |
| `pkg/catalogwriter/` | Shared lakekeeper REST client + VendedCreds + RetryOnConflict helper used by Data Gateway, TableCommit, and pipeline-api storage. |
| **Kubernetes operators** | |
| `pkg/k8s/api/v1/` | CRD types (Pipeline, PipelineRun). |
| `pkg/k8s/controllers/` | Single PipelineRun controller; schedules per-table commit Jobs directly. |
| `cmd/pipeline-operator/` | The pipeline-operator binary. |
| `cmd/pipeline-observer/` | Standalone informer binary; mirrors PipelineRun status to DB. |
| **pipeline-api** | |
| `cmd/pipeline-api/` | HTTP server + admin subcommands. |
| `pkg/pipelineapi/http/` | Handlers + `Server` + narrow store interfaces. |
| `pkg/pipelineapi/auth/` | Session-cookie verification. |
| `pkg/pipelineapi/authz/` | OpenFGA-backed authz. |
| `pkg/pipelineapi/runbackend/` | `Backend` interface + `K8sBackend`. |
| `pkg/pipelineapi/k8s/` | Informer/observer + coalesce + reaper. |
| `pkg/pipelineapi/store/` | pgx-backed stores. |
| **Shared libraries** | |
| `pkg/lib/datalake/` | Storage abstraction (MinIO / S3 / filesystem). |
| `pkg/lib/orchestrator/` | `Orchestrator` interface + Docker implementation (used by `datuplet run --remote`). |
| `pkg/pipeline/config/` | Pipeline YAML parsing and validation. |
| **SDKs & components** | |
| `sdk/go/`, `sdk/python/` | Thin SDKs (~200 LOC each). |
| `sdk/python-sandbox/` | Sandbox library (DuckDB + pre-signed URLs). |
| `components/` | Built-in extractors, transforms, writers. |
| **Browser UI** | |
| `ui/product/` | SPA served by pipeline-api at `/ui/*` (vanilla ES modules, no build step). |
| `pkg/pipelineapi/storage/` | Pure-Go Iceberg catalog/table service backing `/ui/storage`. |
| **Utilities** | |
| `charts/` | Four Helm charts (`datuplet-operators`, `datuplet-infra`, `datuplet-app`, `datuplet-lakekeeper`). |
| `utils/docker/` | Dockerfiles for all services. |
| `examples/k8s/` | Example Pipeline / PipelineRun YAMLs. |
| `examples/pipelines/` | Example pipeline YAMLs (used by `datuplet run --remote`). |
