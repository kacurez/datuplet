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

The commands below install from a local clone (development default). To
install a published version instead, replace `charts/<name>` with
`datuplet/<name> --version <X.Y.Z>` in each command.

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
# 0a. Register each dependency chart's repo — `helm dependency build` (unlike
# `update`) requires these to be locally registered; it won't auto-fetch
# "unmanaged" repos.
helm repo add cloudnative-pg https://cloudnative-pg.github.io/charts
helm repo add openfga https://openfga.github.io/helm-charts
helm repo add minio https://charts.min.io/
helm repo add lakekeeper https://lakekeeper.github.io/lakekeeper-charts

# 0b. Fetch subchart tarballs (gitignored; required once per clone or version bump)
helm dependency build charts/datuplet-operators
helm dependency build charts/datuplet-infra
helm dependency build charts/datuplet-app
helm dependency build charts/datuplet-lakekeeper

# 1. Phase 1 — CNPG operator + CRDs
helm upgrade --install datuplet-operators charts/datuplet-operators \
  -n datuplet --create-namespace --wait --timeout 5m

# 2. Phase 2 — stateful infrastructure (Postgres, OpenFGA, MinIO, keygen)
helm upgrade --install datuplet-infra charts/datuplet-infra \
  -n datuplet --wait --wait-for-jobs --timeout 10m

# 3. Phase 3 — Datuplet control plane (pipeline-api, observer, operator, CRDs, authz-bootstrap)
#    --wait-for-jobs is required: the pre-install migrate + authz-bootstrap Jobs must complete
#    before Deployments become Ready.
#    Note: chart default is image.pullPolicy=Always (safe with pinned registry tags).
#    Local/kind clusters that pre-load images via `kind load docker-image` must
#    add `--set image.pullPolicy=IfNotPresent` here — see docs/quickstart-kind.md.
helm upgrade --install datuplet-app charts/datuplet-app \
  -n datuplet --wait --wait-for-jobs --timeout 10m

# 4. Phase 4 — Lakekeeper (requires Phase 3 FGA pin to exist; will poll + fail if missing)
helm upgrade --install datuplet-lakekeeper charts/datuplet-lakekeeper \
  -n datuplet --wait --wait-for-jobs --timeout 10m

# 5. Phase 5 — one-time business-state registration (idempotent; safe to re-run)
./scripts/register.sh --namespace datuplet
```

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
# Phase 1 — CNPG operator upgrade (cluster-admin; rarely needed)
helm upgrade datuplet-operators charts/datuplet-operators \
  -n datuplet --wait --timeout 5m

# Phase 2 — infrastructure upgrade (rare; ops review required)
helm upgrade datuplet-infra charts/datuplet-infra \
  -n datuplet --wait --wait-for-jobs --timeout 10m

# Phase 3 — Datuplet release (most common; every Datuplet version bump)
helm upgrade datuplet-app charts/datuplet-app \
  -n datuplet --wait --wait-for-jobs --timeout 10m

# Phase 4 — Lakekeeper upgrade (rare; catalog admin; update platform.fgaModelVersion
#            to match datuplet-app's fgaModel.version before upgrading)
helm upgrade datuplet-lakekeeper charts/datuplet-lakekeeper \
  -n datuplet --wait --wait-for-jobs --timeout 10m
```

`register.sh` is NOT re-run on upgrade unless adding new warehouses or projects.

For FGA model changes (DSL or version bump), see `docs/fga-model-upgrades.md` for the
version-bump procedure and the cross-chart `platform.fgaModelVersion` coupling requirement.

## Local development (OrbStack)

```bash
make deploy-local    # runs all 4 helm upgrade --installs + register.sh (namespace: datuplet)
```

The `make deploy-local` target is the canonical local workflow. It runs
the four helm upgrades in phase order followed by `scripts/register.sh`.

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
before installing Phase 4. Re-run `helm upgrade datuplet-app` if in doubt.

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
