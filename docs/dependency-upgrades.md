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
