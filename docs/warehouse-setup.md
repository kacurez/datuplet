# Warehouse Setup

This guide covers how to prepare an S3 or GCS bucket for Datuplet warehouse use, then wire the prepared infrastructure into Helm values.

For chart install steps, see [docs/install.md](install.md). For an end-to-end
GKE + GCS deployment walkthrough, see [docs/quickstart-gke.md](quickstart-gke.md).

---

## Part 1 — S3 bucket preparation

Works for AWS S3, MinIO, and any S3-compatible store.

### 1. Create the bucket

**AWS:**

```bash
aws s3api create-bucket \
  --bucket my-org-datuplet \
  --region us-east-1 \
  --create-bucket-configuration LocationConstraint=us-east-1
```

> Note: `--create-bucket-configuration` is required for every region except `us-east-1`.
> For `us-east-1` omit the flag entirely.

**MinIO (already provisioned by chart when `minio.enabled: true`)** — no action needed; skip to [Part 1 step 5](#5-wire-the-values).

### 2. IAM policy (AWS only)

Lakekeeper needs object-level access plus the ability to call `sts:AssumeRole` when
STS-vended credentials are enabled (the default — `warehouse.s3.stsEnabled: true`).

Create a policy document and attach it to an IAM user or role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::my-org-datuplet/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::my-org-datuplet"
    },
    {
      "Effect": "Allow",
      "Action": ["sts:AssumeRole"],
      "Resource": "*"
    }
  ]
}
```

Save it as `datuplet-warehouse-policy.json`, then:

```bash
aws iam create-policy \
  --policy-name datuplet-warehouse \
  --policy-document file://datuplet-warehouse-policy.json

aws iam attach-user-policy \
  --user-name datuplet-warehouse \
  --policy-arn arn:aws:iam::<account-id>:policy/datuplet-warehouse
```

### 3. Encryption and versioning (recommended)

```bash
# Server-side encryption (SSE-S3)
aws s3api put-bucket-encryption \
  --bucket my-org-datuplet \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}
    }]
  }'

# Versioning — helps recover from partial Iceberg commits during failure
aws s3api put-bucket-versioning \
  --bucket my-org-datuplet \
  --versioning-configuration Status=Enabled
```

### 4. Create the K8s Secret

**Skip if `minio.enabled: true`** — the chart auto-wires MinIO credentials.

For AWS, generate an access key for the IAM user:

```bash
aws iam create-access-key --user-name datuplet-warehouse
```

Then create the K8s Secret in the chart's target namespace:

```bash
kubectl -n datuplet create secret generic my-s3-creds \
  --from-literal=accessKey=<access-key-id> \
  --from-literal=secretKey=<secret-access-key>
```

### 5. Wire the values

```yaml
# values.prod.yaml
minio:
  enabled: false

warehouse:
  type: s3
  name: datuplet
  s3:
    bucket: my-org-datuplet
    region: us-east-1
    endpoint: https://s3.us-east-1.amazonaws.com
    pathStyleAccess: false      # false for AWS native; true for MinIO / most S3-compat stores
    stsEnabled: true
    existingSecret: my-s3-creds
```

`helm install` registers the warehouse with Lakekeeper using these credentials.  The
`warehouse.name` value must be stable after first install — Lakekeeper persists warehouse
state in Postgres tied to this name.

---

## Part 2 — GCS

Lakekeeper supports two credential modes for GCS warehouses, both fully
supported and equally tested in v0.2.

### 1. Create the bucket (both modes)

```bash
gcloud storage buckets create gs://my-org-datuplet \
  --location=us \
  --uniform-bucket-level-access
```

Uniform bucket-level access is required — ACL-based access is not supported by Datuplet's
Lakekeeper integration.

---

### Mode A — Workload Identity Federation (recommended on GKE)

**Prerequisite:** GKE cluster created with Workload Identity enabled
(`--workload-pool=<project>.svc.id.goog` at cluster create time). Existing
clusters can be retrofitted via the GKE console or
`gcloud container clusters update --workload-pool=...`.

**GSA + WIF binding (3 gcloud commands):**

```bash
GSA="datuplet-lakekeeper-warehouse@${GCP_PROJECT}.iam.gserviceaccount.com"

# 1. Create the GSA (skip if exists).
gcloud iam service-accounts create datuplet-lakekeeper-warehouse \
  --project="${GCP_PROJECT}"

# 2. Grant the GSA write access to the bucket.
gcloud storage buckets add-iam-policy-binding "gs://${GCS_BUCKET}" \
  --member="serviceAccount:${GSA}" \
  --role=roles/storage.objectAdmin

# 3. Bind the K8s ServiceAccount (datuplet/lakekeeper) as workloadIdentityUser
#    on the GSA.
gcloud iam service-accounts add-iam-policy-binding "${GSA}" \
  --role=roles/iam.workloadIdentityUser \
  --member="serviceAccount:${GCP_PROJECT}.svc.id.goog[datuplet/lakekeeper]"
```

**Chart values** (`charts/datuplet-lakekeeper` install):

```bash
helm upgrade --install datuplet-lakekeeper charts/datuplet-lakekeeper \
  -n datuplet --wait --wait-for-jobs --timeout 10m \
  --set workloadIdentity.enabled=true \
  --set workloadIdentity.gcpServiceAccount="${GSA}" \
  --set platform.enableGcpSystemCredentials=true
```

**Bootstrap invocation:**

```bash
pipeline-api admin lakekeeper-bootstrap \
  --type=gcs --gcs-bucket="${GCS_BUCKET}" \
  --gcs-credential-type=system-identity \
  --lakekeeper-project-id="${LK_PROJECT_ID}" \
  --lakekeeper-url="${LK_URL}" \
  --signing-key-file="${SIGNING_KEY}"
```

**Troubleshooting:**

- Pod fails to authenticate to GCS → check the KSA annotation:
  `kubectl get sa lakekeeper -n datuplet -o jsonpath='{.metadata.annotations}'`
  should contain `iam.gke.io/gcp-service-account: <GSA email>`.
- Vended-token errors → check Lakekeeper logs for "GCS access denied" with
  the GSA. Most common cause: the WIF binding was set up against a different
  KSA namespace/name than what the chart actually creates (`datuplet/lakekeeper`).

---

### Mode B — Static service-account key (works anywhere)

**Create the SA + key:**

```bash
gcloud iam service-accounts create datuplet-lakekeeper-warehouse \
  --project="${GCP_PROJECT}"

GSA="datuplet-lakekeeper-warehouse@${GCP_PROJECT}.iam.gserviceaccount.com"

gcloud storage buckets add-iam-policy-binding "gs://${GCS_BUCKET}" \
  --member="serviceAccount:${GSA}" \
  --role=roles/storage.objectAdmin

gcloud iam service-accounts keys create /tmp/datuplet-sa.json \
  --iam-account="${GSA}" \
  --project="${GCP_PROJECT}"
```

**Chart values** — no WIF flags needed:

```bash
helm upgrade --install datuplet-lakekeeper charts/datuplet-lakekeeper \
  -n datuplet --wait --wait-for-jobs --timeout 10m
```

**Bootstrap:**

```bash
pipeline-api admin lakekeeper-bootstrap \
  --type=gcs --gcs-bucket="${GCS_BUCKET}" \
  --gcs-credential-type=service-account-key \
  --gcs-sa-key-file=/tmp/datuplet-sa.json \
  --lakekeeper-project-id="${LK_PROJECT_ID}" \
  --lakekeeper-url="${LK_URL}" \
  --signing-key-file="${SIGNING_KEY}"
```

**Cleanup:** delete `/tmp/datuplet-sa.json` after bootstrap — Lakekeeper has
the key persisted internally.

**Rotation:** to rotate the key, generate a new one + re-bootstrap with the
same `--gcs-credential-type=service-account-key`. Lakekeeper replaces the
stored credential.

---

### Manual smoke test (optional, both modes)

```bash
GCS_BUCKET=my-org-datuplet \
GCS_SA_KEY_FILE=path/to/sa-key.json \
./tests/helm/gcs-smoke.sh
```

See `tests/helm/gcs-smoke.sh` for details.

---

## Multi-tenant note

The chart provisions one warehouse via `warehouse.name` (default: `datuplet`).
Multi-warehouse installs are out of scope for v0.1.  To run a second independent warehouse,
install the chart in a second namespace with a distinct `warehouse.name`:

```bash
helm install datuplet-staging ./charts/datuplet-app \
  --namespace datuplet-staging --create-namespace \
  --set warehouse.name=staging \
  -f values.staging.yaml
```

## Switching between S3 and GCS

The chart's `values.schema.json` enforces:

- `warehouse.type: gcs` requires `minio.enabled: false`
- `warehouse.type: s3` with `minio.enabled: false` requires `warehouse.s3.endpoint` to be set

Switching storage backends on an existing install is **not supported** — Lakekeeper persists
the warehouse storage profile in Postgres and cannot migrate data between backends.

To switch:

1. `helm uninstall datuplet -n datuplet` (preserves PVCs and chart-managed Secrets)
2. Delete the `lakekeeper-pgkey` Secret if a fresh Lakekeeper state is needed
3. Reinstall with the new warehouse values
