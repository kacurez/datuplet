# FGA Model Upgrades

The OpenFGA authorization model is stored as DSL files in
`charts/datuplet-app/files/fga/` and managed via the `fgaModel.version` chart value in
`charts/datuplet-app/values.yaml`. **The FGA model lives in Phase 3 (`datuplet-app`)** because
it is Datuplet's authorization schema — it changes when Datuplet changes, not when
infrastructure changes.

The authz-bootstrap Job (a `pre-install,pre-upgrade` hook in `datuplet-app`) uploads the model
on first install and skips the upload on subsequent installs when the version pin already exists
with a matching DSL hash.

## `fgaModel.version` is the source of truth

Two chart values must match at all times:

| Chart | Value key | Role |
|---|---|---|
| `charts/datuplet-app/values.yaml` | `fgaModel.version` | Phase 3 origin — used by authz-bootstrap to write the pin tuple and by pipeline-api's `OPENFGA_MODEL_VERSION` env |
| `charts/datuplet-lakekeeper/values.yaml` | `platform.fgaModelVersion` | Phase 4 mirror — used by the `lakekeeper-config` Secret's `LAKEKEEPER__OPENFGA__CONFIGURED_MODEL_VERSION` env |

Both must be bumped together when the model changes. If they diverge, pipeline-api and
lakekeeper will resolve different model IDs and authorization checks will behave inconsistently.

**Known gap:** CI enforces DSL changes → version bump in `datuplet-app` but does NOT enforce
cross-chart value sync between `fgaModel.version` (app) and `platform.fgaModelVersion`
(lakekeeper). Operators must bump both manually. See [docs/known-limitations.md](known-limitations.md).

## Additive vs breaking changes

**Additive** (new type, new relation, new tuple condition — the common case):
1. Edit DSL files in `charts/datuplet-app/files/fga/components/*.fga`.
2. Bump `fgaModel.version` in `charts/datuplet-app/values.yaml`.
3. Bump `platform.fgaModelVersion` in `charts/datuplet-lakekeeper/values.yaml` to match.
4. `helm upgrade datuplet-app charts/datuplet-app -n datuplet --wait --wait-for-jobs`
5. `helm upgrade datuplet-lakekeeper charts/datuplet-lakekeeper -n datuplet --wait --wait-for-jobs`

Authz-bootstrap finds no pin tuple for the new version, uploads the model, writes the pin,
and exits 0. Pipeline-api rolls out with the new `OPENFGA_MODEL_VERSION` env; lakekeeper
picks up the matching `LAKEKEEPER__OPENFGA__CONFIGURED_MODEL_VERSION` from the re-rendered
`lakekeeper-config` Secret.

**Breaking** (rename a type, remove a relation, change semantics) — **not supported by helm
upgrade in v0.1**. Process:

1. Tear down the CNPG cluster to wipe the openfga DB (and lakekeeper DB):
   ```bash
   kubectl delete cluster.postgresql.cnpg.io pg -n datuplet
   ```
2. `helm upgrade datuplet-app charts/datuplet-app -n datuplet \`
   `  --set fgaModel.version=<new-version> --wait --wait-for-jobs`
3. `helm upgrade datuplet-lakekeeper charts/datuplet-lakekeeper -n datuplet \`
   `  --set platform.fgaModelVersion=<new-version> --wait --wait-for-jobs`
4. `./scripts/register.sh --namespace datuplet`  (re-bootstrap warehouse + admin user)

Existing durable FGA tuples (project_admin, server-admin) need manual rewriting when relations
are renamed or removed. No automated tooling today — use `curl` against the OpenFGA API or
`pipeline-api admin grant`. See [docs/known-limitations.md](known-limitations.md).

## Re-forking from upstream Lakekeeper

Our model is Lakekeeper's model plus four Datuplet extensions. The current fork is
Lakekeeper **4.13** (shipped with Lakekeeper v0.14.2). The version must be **exactly** the
version Lakekeeper hard-codes as active (`AuthorizationModelVersion` in its OpenFGA migration
module); Lakekeeper looks up that version's pin and, finding ours, does not upload its own
model. Do not set `LAKEKEEPER__OPENFGA__AUTHORIZATION_MODEL_VERSION`.

Recipe for the next Lakekeeper bump (`<tag>` = Lakekeeper release, `<v>` = its active model
version):

1. Fetch the upstream components:
   ```bash
   gh api "repos/lakekeeper/lakekeeper/contents/authz/openfga/v<v>?ref=<tag>" --jq '.[].path'
   gh api "repos/lakekeeper/lakekeeper/contents/authz/openfga/v<v>/components/<file>?ref=<tag>" \
     --jq .content | base64 -d > charts/datuplet-app/files/fga/components/<file>
   ```
   Copy `fga.mod` the same way. Replace every component (`git rm` the ones upstream dropped).
2. Re-apply the extensions, each marked `DATUPLET EXTENSION` in the DSL. On 4.13:
   - `project.fga`: `editor`, `viewer` and
     `datuplet_member: editor or viewer or project_admin or security_admin`;
     `data_admin: … or editor` (reaches table `can_write_data` through warehouse
     `modify_effective … or data_admin from project`);
     `select_effective: … or viewer` (reaches table `can_read_data`).
   - `lakekeeper_table.fga`: `explicit_reader: [user, role#assignee]`, added to
     `select_effective` **and** `visible_below`. Without `visible_below`, a single-table share
     loses namespace `can_get_metadata` and warehouse `can_use` / `can_get_config`.

   Upstream's granted vs `_effective` split decides where an extension lands: actions read
   the `_effective` twin, so extensions feed the twin, not the bare (granted-only) relation.
3. Update `datuplet-app.fgaModelComponents` in `charts/datuplet-app/templates/_helpers.tpl` to
   list `fga.mod`'s components in order. It feeds both the bootstrap DSL hash and the
   ConfigMap volume items.
4. Set `fgaModel.version` and `platform.fgaModelVersion` to `<v>`.
5. Check pipeline-api's relation names against the new model (since 4.13 project reads check
   `describe_effective`; `describe` matches direct grants only).
6. Verify: `go test ./pkg/pipelineapi/authz/ -run TestFGAModelCompiles` (compiles the model,
   checks component lists and extension chains) and `make test-authz-integration` (runs the
   access matrix on a real OpenFGA server), then `make e2e-k8s`.

## Hash drift detection

Authz-bootstrap computes a SHA-256 hash of all DSL files at chart-render time and stores it
in a sentinel ConfigMap `<release>-fga-version-sentinel` alongside the version pin.

On each re-run (helm upgrade), the bootstrap logic applies one of three outcomes:

| State | Outcome |
|---|---|
| Pin doesn't exist for this version | Upload model, write pin + hash to sentinel |
| Pin exists, hash matches | Skip — idempotent no-op |
| Pin exists, hash **differs** | **FAIL LOUDLY** — DSL changed without bumping `fgaModel.version` |

The fail-loud case catches the operator error of editing DSL files and running `helm upgrade`
without bumping the version. The old model remains pinned; the DSL change never takes effect
until the version is bumped.

## CI policy

The workflow `.github/workflows/fga-version-check.yml` fails any PR that modifies
`charts/datuplet-app/files/fga/**` without also bumping `fgaModel.version` in
`charts/datuplet-app/values.yaml`. This is belt-and-suspenders with the runtime
drift detection above.

**CI gap:** the workflow enforces DSL → version coupling within `datuplet-app` but does NOT
detect drift between `fgaModel.version` (app) and `platform.fgaModelVersion` (lakekeeper).
Cross-chart sync is the operator's responsibility on upgrade.

## Further reading

- `charts/datuplet-app/files/fga/` — DSL source files.
- `charts/datuplet-app/values.yaml` — `fgaModel.version` and the authz-bootstrap Job config.

