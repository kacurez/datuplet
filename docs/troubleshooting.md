# Troubleshooting

Common failure modes and how to fix them.

---

## 1. `ImagePullBackOff` on component pods

**Symptom.** `kubectl get pods -n datuplet` shows a component pod with
`ImagePullBackOff` or `ErrImagePull`.

**Diagnosis.**

```bash
kubectl describe pod <pod-name> -n datuplet
# Look at the Events section at the bottom.
```

**Causes and fixes.**

- Wrong image tag. The pipeline YAML references an image tag that does not exist
  in the registry. Update `spec.stages[*].components[*].image` to a valid tag.
- Private registry not authenticated. If using `ghcr.io/kacurez/*` images with
  a private fork, add an `imagePullSecret` to the Pipeline spec or to the
  `datuplet` ServiceAccount.
- Network policy blocking egress to the registry. Check cluster egress rules.

---

## 2. `authz-bootstrap` Job fails

**Symptom.** `helm upgrade --install datuplet-app` hangs or returns a hook
timeout. `kubectl get pods -n datuplet` shows `authz-bootstrap-*` pod in
`Error` or `CrashLoopBackOff`.

**Diagnosis.**

```bash
kubectl logs -n datuplet job/datuplet-app-authz-bootstrap-1
```

**Causes and fixes.**

- OpenFGA Pod is not yet Ready. Wait for it:
  ```bash
  kubectl rollout status deploy/openfga -n datuplet
  ```
  Then re-run `helm upgrade datuplet-app charts/datuplet-app -n datuplet --wait --wait-for-jobs`.
- Phase 2 (`datuplet-infra`) is not fully installed. Run
  `kubectl get pods -n datuplet` and ensure all Phase 2 pods are Running/Completed.
- DSL hash mismatch: the DSL changed while the sentinel ConfigMap holds the old hash for
  this version. Model changes need a fresh install; see
  [docs/fga-model-upgrades.md](fga-model-upgrades.md).

---

## 3. Pipeline stuck in `Pending`

**Symptom.** A PipelineRun stays in `Pending` or the component pods never
appear.

**Diagnosis.**

```bash
kubectl logs -n datuplet deploy/pipeline-operator
kubectl describe pipelinerun <name> -n datuplet
```

**Causes and fixes.**

- RBAC missing on the project namespace. The operator creates a namespace per
  project; the operator's ServiceAccount must have `create`, `list`, `watch` on
  pods + jobs in that namespace. Check the ClusterRole bound to the operator SA:
  ```bash
  kubectl get clusterrolebinding -l app.kubernetes.io/name=pipeline-operator
  ```
- Operator Pod crashed. Restart it:
  ```bash
  kubectl rollout restart deploy/pipeline-operator -n datuplet
  ```
- Lakekeeper is not Ready. The operator resolves the warehouse from the run-token
  JWT; if lakekeeper is down, pods won't boot cleanly.

---

## 4. Component fails with "Missing Authorization Header"

**Symptom.** Component pod exits with error mentioning missing authorization or
JWT, or the Data Gateway sidecar logs `failed to validate run token`.

**Diagnosis.**

```bash
kubectl logs -n datuplet <pod-name> -c datagateway
```

**Causes and fixes.**

- `spec.runTokenRef` not set on the PipelineRun. The run-token Secret must be
  referenced. If you created the PipelineRun manually (not through the API), add:
  ```yaml
  spec:
    runTokenRef:
      name: <run-token-secret-name>
  ```
- The run-token Secret was deleted before the pod read it. Secrets are
  auto-created by pipeline-api at trigger time; deleting them manually breaks
  in-flight runs.
- pipeline-api rolled out mid-run, rotating the signing key. The new JWKS key
  ID (`kid`) won't match the run token's `kid`. This is a known POC limitation;
  avoid rolling pipeline-api during active runs.

---

## 5. `scripts/register.sh` fails on `lakekeeper-bootstrap`

**Symptom.** `register.sh` exits with an error on Step 1
(`lakekeeper-bootstrap`).

**Diagnosis.**

```bash
# Check lakekeeper pod health
kubectl get pods -n datuplet -l app.kubernetes.io/name=lakekeeper

# Check pipeline-api OIDC discovery (lakekeeper polls this at startup)
kubectl exec -n datuplet deploy/pipeline-api -- \
  wget -qO- http://localhost:8081/.well-known/openid-configuration
```

**Causes and fixes.**

- Lakekeeper pod is not Ready. Wait for it to finish its database migration init
  container:
  ```bash
  kubectl describe pod -n datuplet -l app.kubernetes.io/name=lakekeeper
  ```
- Pipeline-api OIDC discovery not reachable. Lakekeeper's init container polls
  this before starting; if pipeline-api is unhealthy, lakekeeper won't start.
  Check pipeline-api logs: `kubectl logs -n datuplet deploy/pipeline-api`.
- `bootstrap: already done`. Lakekeeper returns this after the first bootstrap.
  All `register.sh` steps are idempotent — this is not an error. The script will
  continue.

---

## 6. CNPG cluster never goes `Ready`

**Symptom.** `kubectl get cluster -n datuplet pg` shows the cluster in a
non-Ready state after several minutes.

**Diagnosis.**

```bash
kubectl describe cluster -n datuplet pg
kubectl get events -n datuplet --sort-by='.lastTimestamp' | tail -20
```

**Causes and fixes.**

- Insufficient cluster resources. CNPG provisions a PVC (default 1 Gi) and
  starts a Postgres pod. Ensure the node has at least 512 Mi of allocatable
  memory and a StorageClass that provisions PVCs.
  ```bash
  kubectl get storageclass
  kubectl get pvc -n datuplet
  ```
- Stuck PVC (PVC in `Pending`). The StorageClass may not have a provisioner
  running, or the node has no attached storage. On kind, use
  `standard` StorageClass; on GKE, `standard-rwo` works.
- `create-app-roles` Job still running. Wait for it:
  ```bash
  kubectl get jobs -n datuplet
  ```

---

## 7. `helm install` hangs at `wait-for-platform` init container

**Symptom.** During `helm upgrade --install datuplet-app`, pods enter `Init`
state and stay there. Logs show "Waiting for Phase 2 platform components...".

**Diagnosis.**

```bash
kubectl logs -n datuplet -l app.kubernetes.io/name=pipeline-api -c wait-for-platform
```

**Causes and fixes.**

- Phase 2 (`datuplet-infra`) is not fully installed. The `wait-for-platform`
  init container polls for OpenFGA, MinIO, and CNPG readiness. Run Phase 2 first:
  ```bash
  helm upgrade --install datuplet-infra charts/datuplet-infra \
    -n datuplet --wait --wait-for-jobs --timeout 10m
  ```
- `minio.enabled: false` on Phase 2, but Phase 3 still expects MinIO. Pass
  `--set minio.enabled=false` on Phase 3 install as well so the wait script
  skips the MinIO check.

---

## 8. Browser UI shows "401 Unauthorized"

**Symptom.** Navigating to `/ui/` redirects to `/ui/login`, or the API returns
401 on every request.

**Cause.** Session cookie expired (24 h sliding) or the cookie was cleared.

**Fix.** Log in again at `/ui/login`. Sessions slide on every request; you'll
only be logged out if no request is made for 24 hours. If the issue persists
after login, check browser cookie settings (the cookie is HttpOnly + SameSite=Lax
— it requires the same origin, so port-forwarding with a different hostname can
cause issues).

---

## 9. Table commit fails / no data committed

**Symptom.** A PipelineRun completes with `FailedApplication`; the component
Pod's gateway sidecar log contains a commit error (e.g. "commit failed" or a
lakekeeper 409 after retries exhausted).

**Diagnosis.** Since RFC 021 the Data Gateway sidecar commits Iceberg tables
inline via its commit pool — there is no separate TableCommit Job/Pod to
inspect. Check the gateway sidecar container logs of the component Pod:

```bash
kubectl logs -n datuplet <component-pod-name> -c gateway
```

**Causes and fixes.**

- No rows were produced for a table. This is expected when an upstream stage
  produced zero rows; the commit pool treats zero parquet paths for a table as
  success-zero and skips the commit. If you see this as a failure, check that
  the component actually produced output.
- Lakekeeper returned a 409 conflict and the retry budget
  (`catalogwriter.RetryOnConflict`) was exhausted. This happens when multiple
  runs write to the same table concurrently; retries are automatic, but a
  persistent conflict surfaces as a failed commit in the gateway log.
- There is no `files.json` (or `.run-state/`) breadcrumb any more; don't look
  for one. The commit-pool error in the gateway log is the place to start.
- A read is slow on a table with merge-on-read deletes or an evolved schema.
  Each read logs one summary line in the gateway container when it ends:
  `read summary: table=<ns>.<table> format=<fmt> raw_files=N decoded_files=N
  rows=N bytes=N chunks=N outcome=<ok|error|cancelled> duration=<d>`.
  A high `decoded_files` means the slower decoded path ran. A file is served raw
  only when it has no deletes and matches the table's current schema (see
  [architecture.md](architecture.md)), so a table with delete files,
  renamed/added/dropped columns or non-canonical types decodes. `rows` and
  `bytes` are what the gateway produced, not what the client confirmed.
  `decoded_files` counts decoded scan tasks that were actually started, so it
  can exceed the number of distinct files when a large file is split into
  several tasks that are not coalesced. `outcome=error` includes a failed write
  or send to the client; `outcome=cancelled` means the reader was closed (or
  the run cancelled) before the last chunk was delivered. Expected, not a failure — see
  [known-limitations.md](known-limitations.md) for the delete-metrics
  workaround. Non-Parquet request formats always decode.
- The commit fails with `partition spec changed during the run (spec id X → Y)`.
  The table's default partition spec changed between the write and the commit
  (another client evolved it). The files were written for spec X and are not
  committed; re-run the pipeline. This error is not retried.
- `OpenWriter` or the first chunk fails with InvalidArgument (HTTP 400) and an
  `invalid partitioning` message. An output's `partition_fields` names a column
  the table does not have, uses an unknown transform (`days`, `bucket[-1]`), a
  transform the column type does not allow, two time transforms on one column,
  or a partition name that collides with another column; or the existing table
  is a partitioned format-version 3 table, or its spec partitions by a nested
  (struct) field. Fix `partition_fields` (see
  [known-limitations.md](known-limitations.md)).
- The first chunk fails with InvalidArgument (HTTP 400): `column "<c>" has Arrow
  type <X>, which cannot be converted to the table type <Y>`. The existing
  table partitions by `<c>`, and the component writes that column with a type
  the partition value cannot be computed from (e.g. a string into a `long`
  column). Write the column with the table's type, or drop and recreate the
  table.
- The gateway log shows `goplane: warning: table <ns>.<table>: 16 partition
  files open (DATUPLET_GATEWAY_MAX_OPEN_PARTITIONS); finishing the least
  recently written one after N distinct partition tuples`, and at close
  `wrote F files for N distinct partition tuples`. The batches spread over more
  distinct partition tuples than the open-file cap, so the writer writes many
  small files and commits get slower (measured: `bucket[200]` wrote 48 324
  files of ~41 rows, 532 s, 43 s of it commit). Cluster (sort) the input by the
  partition column(s), or choose a coarser transform (e.g. `month` instead of
  `day`, fewer buckets). Raise `DATUPLET_GATEWAY_MAX_OPEN_PARTITIONS` (up to
  1024) only if the pod has memory for it: each open file keeps upload buffers
  (at least 5 MiB per S3 multipart part).
- A partitioned write uses more gateway memory than expected. The open files of
  one writer share `DATUPLET_GATEWAY_PARTITION_BUFFER_BYTES` (default 256 MiB,
  16 MiB-8 GiB) of buffered row groups; lower it, or lower
  `DATUPLET_GATEWAY_MAX_OPEN_PARTITIONS`, to shrink row groups and the peak.
  The budget is a flush trigger on encoded pages, not a hard bound (without it
  each open file held ~82 MiB: `bucket[8]` peaked at 1 174 MiB against 517 MiB
  unpartitioned).
- The gateway log shows `partition_fields [...] differ from the existing
  table's partition spec [...]`. The table already existed with another spec,
  which is used; drop the table to repartition it.

---

## 10. Run completes but no data appears in storage

**Symptom.** PipelineRun phase is `Succeeded` but the Storage browse tab shows
no new data (or the table doesn't exist).

**Diagnosis.**

```bash
# Check the gateway sidecar logs for each component Pod in the run
kubectl get pods -n datuplet -l datuplet.io/run-id=<run-id>
kubectl logs -n datuplet <component-pod-name> -c gateway
```

**Causes and fixes.**

- The commit failed silently in the gateway sidecar's commit pool. Check the
  gateway logs for `FailedUser` or `FailedApplication` exit codes on the
  component Pod.
- Lakekeeper returned a 409 conflict and the retry budget was exhausted. This
  happens when multiple runs write to the same table concurrently. The next run
  will succeed; the failed run's data is orphaned in S3 but not committed.
- The data files were written but never committed. List the table's data prefix
  directly in S3 and compare with the table's snapshot (files from a failed
  writer stay in storage uncommitted):
  ```bash
  # For MinIO:
  kubectl exec -n datuplet deploy/minio -- \
    mc ls local/datuplet/<table-uuid>/data/
  ```
- The Pipeline's output bucket does not match a registered Lakekeeper namespace.
  Check that the namespace exists in Lakekeeper: open the Storage tab in the UI
  and verify the namespace is listed.
