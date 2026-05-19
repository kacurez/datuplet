# Changelog

All notable changes to Datuplet are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] — 2026-05-19

### Fixed

- **`pipeline-api admin lakekeeper-bootstrap` 403 on non-default projects.**
  Bootstrap now grants the service identity (`oidc~pipeline-api-bootstrap`)
  `project_admin` on the target project via a check-then-write FGA tuple
  before issuing the warehouse-create POST. Without this, Lakekeeper rejected
  the create as forbidden because the bootstrap JWT had no project-scoped
  tuple. Idempotent: re-runs skip the write when the tuple already exists.
  Block is skipped entirely for the default project (`00000000-...`) and
  when `OPENFGA_URL` is empty (local-mode-without-FGA).
- **`lakekeeperWarehouseExists` probe now scopes by `--lakekeeper-project-id`.**
  Adds the `x-project-id` HTTP header to the existence probe so bootstrap
  no longer false-positives "warehouse already exists" when the warehouse
  name happens to exist in a different project. Error messages on probe
  failure now include the project ID for diagnostics.
- **`gcsSpec.Validate` rejects `--gcs-credential-type=system-identity` +
  `--sts-enabled=false` up front.** Previously surfaced ~15 min later as
  a confusing runtime "lakekeeper response missing gcs.oauth2.token" at
  the first TableCommit. Service-account-key mode + `--sts-enabled=false`
  remains valid (Lakekeeper returns the static key fields without STS
  downscoping).
- **`pipeline-api admin attach-warehouse --type=gcs` now works.** The
  v0.2.0 stub returned `"not yet supported"`; the canonical 5-step
  bootstrap (`lakekeeper-bootstrap` → `create-user` → `create-project` →
  `attach-warehouse` → `grant`) is now usable for GCS warehouses.
  Implementation: `pkg/pipelineapi/lakekeeper/manager.go` gains
  `GCSWarehouseProfile` + `EnsureGCSWarehouseInProject` siblings to the
  existing `S3WarehouseProfile` + `EnsureS3WarehouseInProject` (renamed
  from `EnsureWarehouseInProject`).
- **TableCommit panic on first GCS write.** `*datupleticeio.gcsIO` now
  implements `iceio.WriteFileIO` (Create + WriteFile + ReadFrom on the
  returned writer), unblocking iceberg-go's snapshot-manifest +
  metadata.json write path. Latent v0.2.0 bug — no GCS `PipelineRun`
  could ever reach a successful TableCommit before this. Surfaced by
  live deploy on 2026-05-19; root-cause confirmed via deepwiki against
  iceberg-go upstream.
- **`scripts/register.sh` handles `--warehouse-type=gcs`.** New
  `--gcs-bucket`, `--gcs-credential-type`, `--gcs-key-prefix`, and
  `--gcs-sa-key-file` flags; fail-fast validation rejects missing
  bucket, unknown credential types, `service-account-key` without a key
  file, and `system-identity` + `--no-sts` (mirrors the in-binary
  validator).

### Changed

- `pkg/pipelineapi/lakekeeper.EnsureWarehouseInProject` renamed to
  `EnsureS3WarehouseInProject`. New `EnsureGCSWarehouseInProject`
  sibling. No compat alias kept — callers in `cmd/pipeline-api/` and
  `tests/e2e/framework/` were updated in the same change.
- `cmd/pipeline-api admin attach-warehouse` accepts a new
  `--gcs-credential-type` flag (default: `system-identity`); existing
  S3 invocations are unaffected.

## [0.2.0] — 2026-05-18

### Added

- **Native GCS warehouse support.** DataGateway writes Parquet directly
  to `gs://` warehouses; TableCommit performs iceberg-go transactions
  against `gs://` table locations; storage browser renders `gs://`.
- **GCS Workload Identity mode.** Bootstrap with
  `--gcs-credential-type=system-identity` to register a Lakekeeper
  warehouse without static service-account keys (chart values
  `workloadIdentity.enabled=true` + `gcpServiceAccount` required).
  Both system-identity and the existing service-account-key modes are
  fully supported; system-identity is the default.
- **Helm-driven per-run tolerations.** New chart value
  `runtimeDefaults.tolerations` on `charts/datuplet-app` flows into
  operator-spawned PipelineRun + TableCommit Pods. Empty default is a no-op.

### Changed

- `pkg/catalogwriter.Creds` is now a sealed interface (`S3Creds`,
  `GCSCreds`) instead of a flat struct. Downstream consumers type-switch
  at one boundary.
- `pkg/catalogwriter.VendedCreds.Get(ctx)` returns the `Creds` interface;
  callers must set `ExpectedCredsType` for scheme-aware fail-closed
  parsing.

### Internal

- New `pkg/datupleticeio/` package centralizes iceberg-go IO scheme
  registration; overrides the upstream `gs://` factory with one that
  consumes `gcs.oauth2.token` from props + a refreshing TokenSource.
- New `tools/lint/notokenlog/` CI analyzer rejects fmt-verb formatting
  of bearer-credential types.
- See `docs/tmp/rfc/019-gcs-backend.md` for the full design.

## [0.1.0] — TBD

Initial public release. Datuplet is experimental — APIs, CRD shapes, and chart
values may change between 0.x releases.

### Added

- Four-chart Helm install for Kubernetes deployment:
  `datuplet-operators`, `datuplet-infra`, `datuplet-app`, `datuplet-lakekeeper`.
- Streaming ETL via a Data Gateway sidecar that handles all storage I/O for
  thin component containers (~300 LOC per SDK).
- Apache Iceberg as the table format on top of S3 / GCS / bundled MinIO.
- `pipeline-api` HTTP control plane with a browser UI at `/ui/*` for
  pipelines, runs, secrets, and storage browse.
- `pipeline-observer` (single-replica) that mirrors `PipelineRun` status into
  Postgres via a K8s informer.
- `pipeline-operator` that reconciles `PipelineRun` CRDs into per-stage
  component Pods and per-table iceberg commit Jobs.
- Lakekeeper as the Iceberg REST catalog of record, with OIDC validation
  against pipeline-api's JWKS and STS-vended credentials for all runtime
  S3 access. No long-lived warehouse credentials on any Datuplet Deployment.
- OpenFGA-backed fine-grained authorization on projects, runs, and tables.
- Built-in component images: `data-generator`, `http-json-extractor`,
  `finnhub-extractor`, `sql-transform` (embedded DuckDB), and
  `stdout-writer`.

### Known limitations

See [docs/known-limitations.md](docs/known-limitations.md) for the full list.
Headline items for 0.1:

- GKE is the only validated cloud target. EKS / AKS quickstarts come in a
  later release.
- Single-replica defaults for everything except `pipeline-api` (2 replicas).
- No CNPG backup configuration shipped with the chart — operators should
  configure WAL archival themselves before any production use.
- Base image is `alpine:3.19`. Distroless / Wolfi variants are future work.
