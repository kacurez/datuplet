# Install Guide

Datuplet ships as four helm charts plus a one-time registration script — a **5-phase install**
designed so each phase maps to its own upgrade cadence:

| Phase | Chart | Cadence | Contents |
|---|---|---|---|
| 1 | `datuplet-operators` | rare (K8s admin) | CNPG operator + CRDs |
| 2 | `datuplet-infra` | rare (ops) | CNPG Postgres Cluster, OpenFGA, MinIO, keygen Job |
| 3 | `datuplet-app` | every Datuplet release | pipeline-api, pipeline-observer, pipeline-operator, CRDs, reaper, authz-bootstrap + FGA DSL |
| 4 | `datuplet-lakekeeper` | rare (catalog admin) | Lakekeeper subchart + `lakekeeper-config` Secret |
| 5 | `scripts/register.sh` | once per environment | Warehouse, admin user, project, grants |

This split means Datuplet releases (Phase 3) never touch stateful infra (Phase 2) or the
Iceberg catalog (Phase 4). The install order is fixed and sequential; each phase depends on
the previous.

## Helm repo

Datuplet charts are published to the public helm repo on every release tag:

```bash
helm repo add datuplet https://kacurez.github.io/datuplet
helm repo update
```

`scripts/install.sh` (below) handles both sources itself: it installs from a
local clone by default, or from this published repo when called with
`--from-repo --version vX.Y.Z`.

## Credential model

Datuplet Deployments carry **zero long-lived S3 credentials** at runtime:

- Warehouse credentials (S3 access key + secret) flow as CLI arguments to
  `pipeline-api admin lakekeeper-bootstrap` during Phase 5 (`register.sh`). Pipeline-api
  forwards them to lakekeeper and keeps no record.
- Lakekeeper holds the warehouse credentials and vends short-lived STS credentials per
  request (scoped to the requesting run's token).
- Pipeline-api, pipeline-observer, pipeline-operator, and iceberg-job commit Pods all
  use lakekeeper-vended STS creds — never long-lived S3 keys from env or Secrets.

## Components overview

**pipeline-api** — stateless HTTP API. Defaults to 2 replicas. Handles login, pipeline
trigger, cancel, and browser UI.

**pipeline-observer** — single-replica K8s informer that mirrors PipelineRun status to the
Postgres `runs` table. Intentionally not scaled; 1 replica avoids concurrent-writer races
on the DB mirror. If the observer Pod is down, run status DB rows drift until it recovers —
pipeline runs still execute, only status reporting lags. See `docs/known-limitations.md` for the
recommended alert.

**pipeline-operator** — reconciles PipelineRun CRDs into component Pods + iceberg-job commit
Jobs.

## Prerequisites

- Kubernetes 1.28+ (OrbStack for local development; any conformant cluster for production).
- `helm` 3.14+.
- `kubectl` configured to the target cluster.
- Network access to: `https://cloudnative-pg.github.io/charts`,
  `https://lakekeeper.github.io/lakekeeper-charts`, `https://openfga.github.io/helm-charts`,
  `https://charts.min.io/` (MinIO subchart).

## Install

```bash
# From a local clone (development):
./scripts/install.sh --namespace datuplet

# From the published helm repo (no clone of the charts needed):
./scripts/install.sh --namespace datuplet --from-repo --version v0.8.0
```

`install.sh` runs preflight checks (kubectl/helm versions, cluster
reachability, K8s ≥ 1.28, StorageClass present, chart availability, no
half-installed releases), then the four helm phases in order — each with
`--wait`/`--wait-for-jobs` — and finally `scripts/register.sh`. Pass
per-chart values with `-f-app my-app-values.yaml` (also `-f-infra`,
`-f-operators`, `-f-lakekeeper`); flags after `--` go to register.sh.
Re-running is safe (idempotent). `--preflight-only` and `--dry-run` are
available for checking a cluster before touching it.

After `register.sh` completes, open `http://localhost:30081/ui/` (OrbStack NodePort) and log
in with the admin credentials printed by the script (or stored in the `datuplet-app-admin-creds`
Secret).

## Grant platform superadmin

Platform superadmins are the only users who may register/modify component
definitions and set pipeline `resources`/gateway knobs beyond the chart's
`pipelinePolicy` bounds. There is **no automated seed hook** in the chart (a
deliberate POC gap) — grant it once as a post-install step by `kubectl exec`ing
the same `admin grant` subcommand `register.sh` uses, this time with
`--superadmin`:

```bash
POD=$(kubectl get pod -n datuplet -l app.kubernetes.io/name=pipeline-api \
  -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n datuplet "$POD" -- \
  pipeline-api admin grant --user admin@datuplet.local --superadmin
```

Use the email of your initial admin user (`pipelineApi.initAdmin.email`,
default `admin@datuplet.local`). The grant writes an FGA `server.admin` tuple;
it is idempotent and safe to re-run.

## Upgrade

Upgrade phases independently based on what changed:

```bash
# The common case — a Datuplet release (Phase 3 only). Applies the chart's
# CRDs first (helm never upgrades crds/), then upgrades datuplet-app:
./scripts/upgrade.sh --namespace datuplet --phase app

# Everything (dependency bumps, FGA model changes):
./scripts/upgrade.sh --namespace datuplet --phase all
```

Upgrades are **forward-only**: no `--atomic`, no `helm rollback` (hook
Jobs, CRD applies, and DB migrations sit outside helm's rollback scope).
Recovery from a failed upgrade is: fix the cause, re-run the same command.

`--phase` also accepts `operators`, `infra`, or `lakekeeper` individually
(rare; cluster-admin/catalog-admin territory) — see `./scripts/upgrade.sh --help`.

`register.sh` is NOT re-run on upgrade unless adding new warehouses or projects.

For FGA model changes (DSL or version bump), see `docs/fga-model-upgrades.md` for the
version-bump procedure and the cross-chart `platform.fgaModelVersion` coupling requirement.

## Local development (OrbStack)

```bash
make deploy-local
```

`make deploy-local` is image build + `install.sh`: it builds the five service
images and the five built-in component images
(`docker-build-k8s build-components-local`), then runs
`./scripts/install.sh --namespace datuplet -f-app tests/local/values-local-app.yaml`
(`deploy-local-helm`), which sets `image.pullPolicy=IfNotPresent` and
`components.registry=datuplet` so the freshly built local images are used
instead of pulling from a registry. OrbStack shares its image cache with the
cluster, so the build step only needs to run once — after that,
`make deploy-local-helm` alone re-applies chart changes without rebuilding.

## Service URLs (OrbStack)

| Service | URL |
|---|---|
| Pipeline-api UI | `http://localhost:30081/ui/` |
| Pipeline-api REST | `http://localhost:30081/api/v1/` |

## Troubleshooting

**`helm install` hangs past timeout.**
Check pending Pods: `kubectl get pods -n datuplet`. Common causes:

- Image pull failures: `kubectl describe pod <pod> -n datuplet` → Events section.
- CNPG cluster not provisioned yet: `kubectl describe cluster -n datuplet pg`.
  CNPG takes 30–60 s to provision the Postgres cluster on first install.

**authz-bootstrap Job fails.**
Check logs: `kubectl logs job/datuplet-app-authz-bootstrap-1 -n datuplet`.
Common causes: OpenFGA not yet Ready, or DSL hash mismatch (see `docs/fga-model-upgrades.md`).
Authz-bootstrap is a pre-install hook in Phase 3; pipeline-api Pods start only after it
succeeds.

**Phase 4 `wait-for-fga-pin` Job fails.**
Phase 3's authz-bootstrap has not completed successfully. Ensure Phase 3 is fully installed
before installing Phase 4. Re-run `./scripts/upgrade.sh --namespace datuplet --phase app` if in doubt.

**CNPG cluster not healthy.**
`kubectl describe cluster -n datuplet pg`. Look for `managed.roles[]`
reconciliation errors. The `create-app-roles-1` Job (Phase 2) runs automatically to create
the per-app DB users; wait for it to complete before investigating further.

**register.sh fails.**
Re-run — all subcommands are idempotent.

**`wait-for-platform` pre-install hook fails.**
Phase 2 is not fully installed. Run `kubectl get pods -n datuplet` and ensure all Phase 2
Pods are Ready before installing Phase 3.

## Ad-hoc SQL query service

The ad-hoc SQL query service (browser console + `POST /api/v1/projects/{pid}/query`)
is **experimental but on by default** — the query-worker Pod is part of the
stock install's footprint, and `make deploy-local` / `docker-build-k8s` build
its image automatically. No post-install step is needed. To disable it,
helm-upgrade with `queryWorker.enabled=false`. The laptop `datuplet-query` CLI
remains a separate opt-in (`allowClientSideQuery=true`). See
[docs/ad-hoc-query.md](ad-hoc-query.md) for details.

## Further reading

- `docs/ad-hoc-query.md` — ad-hoc SQL query service (console, REST, CLI) + how to enable it.
- `docs/postgres-migrations.md` — DB migration discipline.
- `docs/fga-model-upgrades.md` — FGA model upgrade procedure.
- `docs/known-limitations.md` — known limitations for 0.1.
