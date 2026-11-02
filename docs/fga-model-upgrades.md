# FGA Model Changes

The OpenFGA authorization model is stored as DSL files in
`charts/datuplet-app/files/fga/` and versioned by the `fgaModel.version` chart value in
`charts/datuplet-app/values.yaml`. **The FGA model lives in Phase 3 (`datuplet-app`).**

The authz-bootstrap Job (a `pre-install,pre-upgrade` hook in `datuplet-app`) uploads the model
into a new store on install and writes the version pin tuple.

## The version is Lakekeeper's active model version

Two chart values must match at all times:

| Chart | Value key | Role |
|---|---|---|
| `charts/datuplet-app/values.yaml` | `fgaModel.version` | Phase 3 origin — used by authz-bootstrap to write the pin tuple and by pipeline-api's `OPENFGA_MODEL_VERSION` env |
| `charts/datuplet-lakekeeper/values.yaml` | `platform.fgaModelVersion` | Phase 4 mirror — used by the `wait-for-fga-pin` hook and the `wait-for-fga-store` initContainer (via the `lakekeeper-config` Secret's `DATUPLET_FGA_MODEL_VERSION` key) to wait for the pin |

Both always equal the active model version of the pinned Lakekeeper image (4.13 for v0.14.2).
Lakekeeper has no setting that selects a model version: it looks up the pin of its hard-coded
active version (see "Re-forking from upstream Lakekeeper"). Any other version would leave
pipeline-api and Lakekeeper on different models. `scripts/verify-versions.sh` (a PR CI check)
fails if the two values drift.

## Changing the model: fresh install only

Datuplet is always installed fresh. There is no upgrade, migration or tuple-rewrite path
for the FGA model.

Any DSL change, additive or breaking, is made the same way:

1. Edit the fork in `charts/datuplet-app/files/fga/components/*.fga` **at the same version**.
   Do not bump `fgaModel.version` / `platform.fgaModelVersion`; they move only with a
   Lakekeeper re-fork (below).
2. Verify (step 6 of the re-fork recipe).
3. Install fresh with `scripts/install.sh` into a new cluster or namespace, then
   `scripts/register.sh`. Never `helm upgrade` an existing install onto a changed model.

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

   Intended, accepted behaviour change in 4.13: project `can_grant_data_admin` is
   `security_admin or admin from server`, so editors (`data_admin`) can no longer grant
   `data_admin` through Lakekeeper's permissions API. Datuplet itself never calls that API.
   Both authz tests pin this.
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

Authz-bootstrap computes a SHA-256 hash of all DSL files at chart-render time and stores it,
keyed by version, in a sentinel ConfigMap `<release>-fga-version-sentinel` next to the pin.

| State | Outcome |
|---|---|
| No pin for this version (fresh install, new store) | Upload model, write pin + hash to sentinel |
| Pin exists, hash matches | Skip — idempotent no-op |
| Pin exists, hash **differs** | **Fail** — the job refuses to upload |

Because model changes keep the same version, re-running the bootstrap against an
**existing** store after editing the DSL hits the last row and the job refuses. That is
intended: model changes take effect only through a fresh install, whose new store has no pin.

## CI policy

Cross-chart drift between `fgaModel.version` (app) and `platform.fgaModelVersion`
(lakekeeper) is caught by `scripts/verify-versions.sh` in PR CI.

The workflow `.github/workflows/fga-version-check.yml` still fails any PR that modifies
`charts/datuplet-app/files/fga/**` without bumping `fgaModel.version`. That predates the
fresh-install rule and conflicts with step 1 above; a DSL-only change needs the workflow
relaxed first.

## Further reading

- `charts/datuplet-app/files/fga/` — DSL source files.
- `charts/datuplet-app/values.yaml` — `fgaModel.version` and the authz-bootstrap Job config.

