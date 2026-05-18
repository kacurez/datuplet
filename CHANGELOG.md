# Changelog

All notable changes to Datuplet are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
