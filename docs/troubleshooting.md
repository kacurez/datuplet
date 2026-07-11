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
- DSL hash mismatch on upgrade. See [docs/fga-model-upgrades.md](fga-model-upgrades.md).

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
- `files.json` at `<table-base>/.run-state/<run-id>/files.json` is written as
  an audit breadcrumb only — its absence or a write failure is a logged
  warning, not the cause of a commit failure. Don't treat a missing
  `files.json` as the root cause; look at the commit-pool error in the gateway
  log instead.

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
- The `files.json` path is wrong. Confirm by listing the prefix directly in S3:
  ```bash
  # For MinIO:
  kubectl exec -n datuplet deploy/minio -- \
    mc ls local/datuplet/<table-uuid>/.run-state/<run-id>/
  ```
- The Pipeline's output bucket does not match a registered Lakekeeper namespace.
  Check that the namespace exists in Lakekeeper: open the Storage tab in the UI
  and verify the namespace is listed.
