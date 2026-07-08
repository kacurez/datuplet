# Quickstart — GKE + GCS

Deploy Datuplet on a real GKE cluster with a GCS bucket as the Iceberg warehouse.
Plan for 30–45 minutes. This is the smoke-test path for v0.2; production use
requires additional hardening (network policies, IAM tightening, CNPG backups).

---

## Prerequisites

- GCP project with billing enabled.
- [`gcloud` CLI](https://cloud.google.com/sdk/docs/install) authenticated
  (`gcloud auth login && gcloud auth application-default login`).
- [`kubectl`](https://kubernetes.io/docs/tasks/tools/)
- [`helm`](https://helm.sh/) 3.14+
- The Datuplet repo cloned locally (`git clone https://github.com/kacurez/datuplet`).

Set a project variable to avoid repeating it:

```bash
export GCP_PROJECT=<your-gcp-project-id>
export GCP_REGION=us-central1
export GCS_BUCKET=<your-bucket-name>    # must be globally unique
```

---

## 1. Create the GKE cluster

```bash
gcloud container clusters create datuplet \
  --project="${GCP_PROJECT}" \
  --region="${GCP_REGION}" \
  --release-channel=regular \
  --machine-type=e2-standard-2 \
  --num-nodes=2 \
  --workload-pool="${GCP_PROJECT}.svc.id.goog"

gcloud container clusters get-credentials datuplet \
  --region="${GCP_REGION}" --project="${GCP_PROJECT}"
```

Two `e2-standard-2` nodes are enough for a single-pipeline smoke test. For
anything heavier, add nodes or switch to `e2-standard-4`.

---

## 2. Create a GCS bucket

```bash
gcloud storage buckets create "gs://${GCS_BUCKET}" \
  --project="${GCP_PROJECT}" \
  --location="${GCP_REGION}" \
  --uniform-bucket-level-access
```

Uniform bucket-level access is required; ACL-based access is not supported by
Datuplet's Lakekeeper integration.

---

## 3. Set up GCS access — choose your mode

Pick one. Both are fully supported in v0.2.

### Mode A — Workload Identity Federation (recommended on GKE)

Lakekeeper runs as a Kubernetes ServiceAccount bound to a GCP service
account via `iam.workloadIdentityUser`. No static key file is generated.

The GKE cluster created in step 1 already has `--workload-pool` enabled.

```bash
GSA="datuplet-lakekeeper-warehouse@${GCP_PROJECT}.iam.gserviceaccount.com"

gcloud iam service-accounts create datuplet-lakekeeper-warehouse \
  --project="${GCP_PROJECT}" \
  --display-name="Datuplet Lakekeeper Warehouse Access"

gcloud storage buckets add-iam-policy-binding "gs://${GCS_BUCKET}" \
  --member="serviceAccount:${GSA}" \
  --role=roles/storage.objectAdmin

gcloud iam service-accounts add-iam-policy-binding "${GSA}" \
  --role=roles/iam.workloadIdentityUser \
  --member="serviceAccount:${GCP_PROJECT}.svc.id.goog[datuplet/lakekeeper]"
```

### Mode B — Static service-account key (works anywhere)

Use this when you can't set up Workload Identity (non-GKE clusters,
restricted-IAM environments, or you just want to deploy in one fewer step).

```bash
gcloud iam service-accounts create datuplet-lakekeeper-warehouse \
  --project="${GCP_PROJECT}"

GSA="datuplet-lakekeeper-warehouse@${GCP_PROJECT}.iam.gserviceaccount.com"

gcloud storage buckets add-iam-policy-binding "gs://${GCS_BUCKET}" \
  --member="serviceAccount:${GSA}" \
  --role=roles/storage.objectAdmin

gcloud iam service-accounts keys create datuplet-sa.json \
  --iam-account="${GSA}" \
  --project="${GCP_PROJECT}"
```

Keep `datuplet-sa.json` safe — the next step hands it to Lakekeeper once at
bootstrap, after which the file can be deleted.

---

## 4. Install the four Helm charts

```bash
cd datuplet  # repo root

helm dependency update charts/datuplet-operators
helm dependency update charts/datuplet-infra
helm dependency update charts/datuplet-app
helm dependency update charts/datuplet-lakekeeper

# Phase 1 — CNPG operator + CRDs
helm upgrade --install datuplet-operators charts/datuplet-operators \
  -n datuplet --create-namespace --wait --timeout 5m

# Phase 2 — stateful infra (Postgres, OpenFGA).
# Disable bundled MinIO — GCS is the warehouse.
helm upgrade --install datuplet-infra charts/datuplet-infra \
  -n datuplet --wait --wait-for-jobs --timeout 10m \
  --set minio.enabled=false

# Phase 3 — Datuplet control plane
helm upgrade --install datuplet-app charts/datuplet-app \
  -n datuplet --wait --wait-for-jobs --timeout 10m \
  --set warehouse.type=gcs \
  --set warehouse.gcs.bucket="${GCS_BUCKET}"

# Phase 4 — Lakekeeper (Mode A — Workload Identity)
helm upgrade --install datuplet-lakekeeper charts/datuplet-lakekeeper \
  -n datuplet --wait --wait-for-jobs --timeout 10m \
  --set workloadIdentity.enabled=true \
  --set workloadIdentity.gcpServiceAccount="${GSA}" \
  --set platform.enableGcpSystemCredentials=true

# Phase 4 — Lakekeeper (Mode B — static key; no extra flags needed)
# helm upgrade --install datuplet-lakekeeper charts/datuplet-lakekeeper \
#   -n datuplet --wait --wait-for-jobs --timeout 10m
```

Phase 2 provisions a CNPG Postgres cluster (30–60 s on first install). If it
times out, check: `kubectl get pods -n datuplet`.

---

## 5. Bootstrap the GCS warehouse

The order matters: **create the Datuplet project first**, capture its
`lakekeeper_project_id`, **then** run `lakekeeper-bootstrap` targeting that
project. Otherwise the warehouse lands on lakekeeper's default project and
the Datuplet project sees no warehouses at run time.

```bash
POD=$(kubectl get pods -n datuplet -l app.kubernetes.io/name=pipeline-api \
  --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')

LK_URL=http://lakekeeper.datuplet.svc.cluster.local:8181
SIGNING_KEY=/var/run/secrets/datuplet-signing-key/signing-key.pem
OFGA_URL=http://openfga.datuplet.svc.cluster.local:8080

# 1. Admin user
kubectl exec -n datuplet "${POD}" -- \
  /usr/local/bin/pipeline-api admin create-user \
    --email=admin@example.com --password=<strong-password>

# 2. Project (allocates a fresh lakekeeper project ID)
kubectl exec -n datuplet "${POD}" -- \
  /usr/local/bin/pipeline-api admin create-project \
    --name=default \
    --creator-email=admin@example.com \
    --lakekeeper-url=$LK_URL \
    --signing-key-file=$SIGNING_KEY \
    --openfga-url=$OFGA_URL

# 3. Capture the project's lakekeeper_project_id from Postgres
PG_PW=$(kubectl -n datuplet get secret pg-pipeline-api-pw -o jsonpath='{.data.password}' | base64 -d)
LK_PROJECT_ID=$(kubectl -n datuplet exec pg-1 -c postgres -- \
  env PGPASSWORD="$PG_PW" psql -h 127.0.0.1 -U pipeline_api_user -d pipeline_api -t -A \
    -c "SELECT lakekeeper_project_id FROM projects WHERE name = 'default';")
echo "Datuplet project lakekeeper_project_id: $LK_PROJECT_ID"
```

### Step 4 onwards — Mode A (Workload Identity)

No key file to copy. Run bootstrap with `--gcs-credential-type=system-identity`:

```bash
# 4. Bootstrap — Mode A (system-identity, recommended)
kubectl exec -n datuplet "${POD}" -- \
  /usr/local/bin/pipeline-api admin lakekeeper-bootstrap \
    --type=gcs \
    --gcs-bucket="${GCS_BUCKET}" \
    --gcs-credential-type=system-identity \
    --warehouse-name=datuplet \
    --lakekeeper-project-id="${LK_PROJECT_ID}" \
    --lakekeeper-url="${LK_URL}" \
    --signing-key-file="${SIGNING_KEY}"
```

### Step 4 onwards — Mode B (static service-account key)

```bash
# 4. Copy the GCS SA key into the pod
kubectl cp datuplet-sa.json datuplet/"${POD}":/tmp/datuplet-sa.json

# 5. Bootstrap — Mode B (service-account-key)
kubectl exec -n datuplet "${POD}" -- \
  /usr/local/bin/pipeline-api admin lakekeeper-bootstrap \
    --type=gcs \
    --gcs-bucket="${GCS_BUCKET}" \
    --gcs-credential-type=service-account-key \
    --gcs-sa-key-file=/tmp/datuplet-sa.json \
    --warehouse-name=datuplet \
    --lakekeeper-project-id="${LK_PROJECT_ID}" \
    --lakekeeper-url="${LK_URL}" \
    --signing-key-file="${SIGNING_KEY}"

# Cleanup: remove the SA key from the pod and local disk
kubectl exec -n datuplet "${POD}" -- rm /tmp/datuplet-sa.json
rm datuplet-sa.json
```

### Final step (both modes)

```bash
# Grant the admin role on the project
kubectl exec -n datuplet "${POD}" -- \
  /usr/local/bin/pipeline-api admin grant \
    --user=admin@example.com \
    --project=default \
    --role=admin \
    --lakekeeper-url="${LK_URL}" \
    --signing-key-file="${SIGNING_KEY}" \
    --openfga-url="${OFGA_URL}"
```

Notes:

- `register.sh` automates the S3 / MinIO flow but does not yet drive
  `lakekeeper-bootstrap --type=gcs`. The manual `kubectl exec` steps above
  reproduce what the script does for S3.
- The `--lakekeeper-project-id` flag tells bootstrap which lakekeeper project
  to create the warehouse in. Without it, the warehouse defaults to
  lakekeeper's `00000000-...0` project and pipeline-api can't find it.

---

## 6. Reach the UI

For v0.1, use `kubectl port-forward` to access the UI. LoadBalancer / Ingress
configuration is left to the operator.

```bash
kubectl port-forward -n datuplet svc/pipeline-api 8080:8081
```

Open `http://localhost:8080/ui/` and log in with the admin credentials from step 5.

---

## 7. Trigger a pipeline

Apply the example pipeline (update the image tag to `ghcr.io/kacurez/*:v0.1.0`
once 0.1.0 is released):

```bash
kubectl apply -f examples/pipelines/simple-http-extract.yaml
kubectl get pipelineruns -n datuplet -w
```

Watch the run reach `Succeeded`. Then browse results in the UI under "Storage".

---

## 8. Inspect output in GCS

After a successful run, Iceberg data files land under the Lakekeeper-assigned
UUID prefix:

```bash
gcloud storage ls "gs://${GCS_BUCKET}/" --recursive | head -30
```

You'll see `.parquet` files and an `iceberg/` metadata tree. The exact paths are
UUID-keyed by Lakekeeper and are considered opaque — use the Datuplet UI or an
Iceberg client to browse, not raw GCS paths.

---

## 9. Tear down

```bash
gcloud container clusters delete datuplet \
  --region="${GCP_REGION}" --project="${GCP_PROJECT}" --quiet

gcloud storage rm --recursive "gs://${GCS_BUCKET}/"
gcloud iam service-accounts delete "datuplet-lakekeeper-warehouse@${GCP_PROJECT}.iam.gserviceaccount.com" \
  --project="${GCP_PROJECT}" --quiet
```

---

## Known limitations on GKE

- CNPG has no backup configuration. Enable WAL archival before any production use.
- No NetworkPolicy restricting DG sidecar egress. Add policies for production.
- EKS and AKS quickstarts are not yet validated.
- No LoadBalancer / Ingress shipped; use port-forward for access.

See [known-limitations.md](known-limitations.md) for the full list.
