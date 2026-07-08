# Secrets

Pipeline configs can reference secret values without embedding them in YAML.
Secrets live in one **managed, write-only** Kubernetes Secret per project —
you never create or edit that Secret directly. You set values through the
pipeline-api or the UI, and the Data Gateway sidecar resolves every
reference at boot into a plain, resolved config for the component. The
component never sees the `$[name]` marker or the raw Secret.

## Syntax

Use `$[name]` as a **whole scalar** inside `spec.stages[*].components[*].config`:

```yaml
config:
  password: $[db_password]
  api_key:  $[api_token]
```

Rules:

- Whole-value only. `url: "postgres://user:$[pw]@host"` is rejected at parse
  time — put the full value (e.g. `postgres://user:hunter2@host`) in the
  secret instead.
- Names match `[A-Za-z0-9_-]+`.
- To write a literal `$[x]`, escape it as `$$[x]`.
- Multiple refs in one scalar (`"$[a] $[b]"`) are rejected.
- Only `component.config` is scanned; other fields (`image`, `inputs`, …) never carry secrets.

Syntax errors are caught at pipeline parse / CRD admission time with a
path-aware message, e.g. `stages[0].components[0].config.password`.

## Setting secret values

Each project has exactly one managed Secret, `datuplet-project-secrets`, in
the project's Kubernetes namespace. Pipeline-api creates and owns it — you
never `kubectl apply` a Secret yourself. Two ways to set values:

### UI

Settings → Secrets lists key names and their last-updated time (values are
never shown or returned) and lets you set or delete a key.

### API

Requires the `data_admin` role on the project.

```bash
# List keys (names + updatedAt only — values are never returned)
curl -s -H "Authorization: Bearer $TOKEN" \
  https://<host>/api/v1/projects/$PROJECT_ID/secrets

# Set a key (creates the managed Secret on first write)
curl -s -X PUT -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"value":"hunter2"}' \
  https://<host>/api/v1/projects/$PROJECT_ID/secrets/db_password

# Delete a key
curl -s -X DELETE -H "Authorization: Bearer $TOKEN" \
  https://<host>/api/v1/projects/$PROJECT_ID/secrets/api_token
```

Keys must match `^[A-Za-z0-9_-]+$`.

## Validation ladder

- **Save** (`PUT /api/v1/projects/{pid}/pipelines/{name}`): the pipeline is
  saved regardless of whether its `$[name]` refs resolve. A reference to a
  key that isn't set yet comes back as a `warning` finding, not a save
  failure — useful when you're setting up a pipeline before its secrets.
- **Trigger** (`POST /api/v1/projects/{pid}/pipelines/{name}/runs`): a
  missing key hard-rejects with `400` before any run row is created.

## Per-run snapshots

At run admission the PipelineRun controller reads the *exact* set of
`$[name]` keys the pipeline references, copies only those keys from the
managed project Secret into a per-run Secret (`datuplet-runsecrets-<id>`),
and mounts that snapshot read-only at `/var/run/secrets/datuplet` on the
**gateway sidecar only** — never on the component container. If a
referenced key is missing from the project Secret at admission time (e.g.
it was deleted after trigger but before admission), the run fails
`FailedUser` and no snapshot or component Job is created.

Because the snapshot is copied once at admission and is immutable for the
run's lifetime, **rotating or deleting a project secret never affects an
in-flight run** — only runs admitted afterward see the new value. The
snapshot Secret is owned by the `PipelineRun` and garbage-collected with it.

### Observing resolution status

The PipelineRun's `status.conditions` reports a `SecretsResolved` condition
(absent when the pipeline references no secrets):

| Status | Reason            | Meaning                                                        |
|--------|-------------------|-----------------------------------------------------------------|
| True   | `Resolved`        | The per-run snapshot was created; every referenced key was found. |
| False  | `SnapshotMissing` | One or more referenced keys were absent from the project Secret at admission — the run is `FailedUser`. |

```bash
kubectl get pipelinerun <name> -n <project-namespace> \
  -o jsonpath='{.status.conditions}' | jq
```

## Limits (v1)

No mid-string substitution, no external providers (Vault, SOPS, cloud
secret managers), no structured/JSON values, no per-pipeline secret scoping
(all keys live in one project-wide Secret; a pipeline only pulls in the
keys it actually references).
