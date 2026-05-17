# Quickstart — GKE + GCS

Deploy Datuplet on a real GKE cluster with a GCS bucket as the Iceberg warehouse.
Plan for 30–45 minutes. This is the smoke-test path for v0.1; production use
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

## 3. Create a GCP service account and key

This service account gives Lakekeeper access to the bucket. Lakekeeper stores the
key and exchanges it for short-lived OAuth tokens on behalf of each pipeline run —
no running Pod holds a long-lived credential at runtime.

```bash
# Create the service account
gcloud iam service-accounts create datuplet-warehouse \
  --project="${GCP_PROJECT}" \
  --description="Datuplet Iceberg warehouse access" \
  --display-name="Datuplet Warehouse"

SA_EMAIL="datuplet-warehouse@${GCP_PROJECT}.iam.gserviceaccount.com"

# Grant bucket access
gcloud storage buckets add-iam-policy-binding "gs://${GCS_BUCKET}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role=roles/storage.objectAdmin

# Allow the SA to create tokens for itself (needed for STS-vended creds)
gcloud iam service-accounts add-iam-policy-binding "${SA_EMAIL}" \
  --project="${GCP_PROJECT}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role=roles/iam.serviceAccountTokenCreator

# Generate a key file
gcloud iam service-accounts keys create datuplet-sa.json \
  --iam-account="${SA_EMAIL}" \
  --project="${GCP_PROJECT}"
```

Keep `datuplet-sa.json` safe. It is passed once to Lakekeeper at bootstrap time,
then deleted. Do not commit it to source control.

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

# Phase 4 — Lakekeeper
helm upgrade --install datuplet-lakekeeper charts/datuplet-lakekeeper \
  -n datuplet --wait --wait-for-jobs --timeout 10m
```

Phase 2 provisions a CNPG Postgres cluster (30–60 s on first install). If it
times out, check: `kubectl get pods -n datuplet`.

---

## 5. Bootstrap the GCS warehouse

GCS bootstrap requires the service-account key file inside the pipeline-api pod.
Copy it in and run `lakekeeper-bootstrap` directly:

```bash
# Copy the key into the pod
POD=$(kubectl get pods -n datuplet -l app.kubernetes.io/name=pipeline-api \
  --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')

kubectl cp datuplet-sa.json datuplet/"${POD}":/tmp/datuplet-sa.json

# Run bootstrap inside the pod
kubectl exec -n datuplet "${POD}" -- \
  /usr/local/bin/pipeline-api admin lakekeeper-bootstrap \
    --type=gcs \
    --gcs-bucket="${GCS_BUCKET}" \
    --gcs-sa-key-file=/tmp/datuplet-sa.json \
    --warehouse-name=datuplet

# Create the admin user + project
kubectl exec -n datuplet "${POD}" -- \
  /usr/local/bin/pipeline-api admin create-user \
    --email=admin@example.com --password=<strong-password>

kubectl exec -n datuplet "${POD}" -- \
  /usr/local/bin/pipeline-api admin create-project \
    --name=default \
    --creator-email=admin@example.com \
    --lakekeeper-url=http://lakekeeper.datuplet.svc.cluster.local:8181 \
    --signing-key-file=/var/run/secrets/datuplet-signing-key/signing-key.pem \
    --openfga-url=http://openfga.datuplet.svc.cluster.local:8080

kubectl exec -n datuplet "${POD}" -- \
  /usr/local/bin/pipeline-api admin grant \
    --user=admin@example.com \
    --project=default \
    --role=admin \
    --lakekeeper-url=http://lakekeeper.datuplet.svc.cluster.local:8181 \
    --signing-key-file=/var/run/secrets/datuplet-signing-key/signing-key.pem \
    --openfga-url=http://openfga.datuplet.svc.cluster.local:8080

# Remove the key from the pod
kubectl exec -n datuplet "${POD}" -- rm /tmp/datuplet-sa.json
rm datuplet-sa.json
```

Notes for the GCS path:

- `register.sh` automates the S3 / MinIO flow but does not yet drive
  `lakekeeper-bootstrap --type=gcs`. The manual `kubectl exec` steps above
  reproduce what the script does for S3.
- For v0.1, **`pipeline-api admin attach-warehouse` is S3-only**. Multi-warehouse
  GCS attachment is deferred. Single-warehouse setups (the v0.1 quickstart) work
  because `lakekeeper-bootstrap --type=gcs` creates the warehouse on the
  default lakekeeper project, and `create-project` maps the Datuplet project
  to that same lakekeeper project — no separate attach step needed.

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
kubectl apply -f examples/k8s/simple-pipeline.yaml
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
gcloud iam service-accounts delete "datuplet-warehouse@${GCP_PROJECT}.iam.gserviceaccount.com" \
  --project="${GCP_PROJECT}" --quiet
```

---

## Known limitations on GKE for v0.1

- CNPG has no backup configuration. Enable WAL archival before any production use.
- No NetworkPolicy restricting DG sidecar egress. Add policies for production.
- EKS and AKS quickstarts are not yet validated.
- No LoadBalancer / Ingress shipped; use port-forward for access.

See [known-limitations.md](known-limitations.md) for the full list.
