# Quickstart — kind (local laptop)

Install Datuplet on a local kind cluster in about 10 minutes. The bundled MinIO
instance is the warehouse; no cloud account is required.

---

## Prerequisites

- [`kind`](https://kind.sigs.k8s.io/) v0.20+
- [`helm`](https://helm.sh/) 3.14+
- [`kubectl`](https://kubernetes.io/docs/tasks/tools/) configured (will point at
  the kind cluster after creation)
- Docker running locally

---

## 1. Create a kind cluster

The node-port mapping exposes the pipeline-api UI on `http://localhost:8080/ui/`.

```bash
kind create cluster --name datuplet --config - <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30081
        hostPort: 8080
        protocol: TCP
EOF
```

Verify the cluster is up:

```bash
kubectl cluster-info --context kind-datuplet
```

---

## 2. Clone the repo

```bash
git clone https://github.com/kacurez/datuplet
cd datuplet
```

Once 0.1.0 is released you can also install from the Helm repo
(`helm repo add datuplet https://kacurez.github.io/datuplet`). Until then, use
the path-based commands below.

---

## 3. Install the four charts

Each `helm dependency update` fetches subchart tarballs (gitignored). Run once
per clone or after a version bump.

```bash
helm dependency update charts/datuplet-operators
helm dependency update charts/datuplet-infra
helm dependency update charts/datuplet-app
helm dependency update charts/datuplet-lakekeeper

# Phase 1 — CNPG operator + CRDs
helm upgrade --install datuplet-operators charts/datuplet-operators \
  -n datuplet --create-namespace --wait --timeout 5m

# Phase 2 — stateful infra (Postgres, OpenFGA, MinIO)
helm upgrade --install datuplet-infra charts/datuplet-infra \
  -n datuplet --wait --wait-for-jobs --timeout 10m

# Phase 3 — Datuplet control plane (pipeline-api, observer, operator)
helm upgrade --install datuplet-app charts/datuplet-app \
  -n datuplet --wait --wait-for-jobs --timeout 10m

# Phase 4 — Lakekeeper Iceberg catalog
helm upgrade --install datuplet-lakekeeper charts/datuplet-lakekeeper \
  -n datuplet --wait --wait-for-jobs --timeout 10m
```

Phase 2 provisions CNPG Postgres (30–60 s on first install), so the timeout is
intentionally generous. If it times out, check:

```bash
kubectl get pods -n datuplet
kubectl describe cluster -n datuplet pg
```

The bundled MinIO is enabled by default (`minio.enabled: true` in the infra chart).
No warehouse credentials are needed for the quickstart path.

---

## 4. Bootstrap the warehouse + admin user

`register.sh` runs five idempotent steps: create the Lakekeeper warehouse, create
an admin user, create a project, attach the warehouse, and grant the admin role.

```bash
./scripts/register.sh --namespace datuplet
```

The script reads MinIO credentials from the `minio` Secret in the namespace
automatically. At the end it prints the admin email and password.

Default credentials (POC — change before any production use):

- Email: `admin@datuplet.local`
- Password: `changeme`

To use custom credentials:

```bash
./scripts/register.sh --namespace datuplet \
  --admin-email you@example.com --admin-password <strong-password>
```

---

## 5. Open the UI

```bash
open http://localhost:8080/ui/
```

Log in with the admin credentials printed by `register.sh`. You'll land on the
Runs dashboard.

If the port isn't reachable, verify the kind node-port mapping:

```bash
kubectl get svc -n datuplet pipeline-api
# EXTERNAL-IP will be blank for kind; the NodePort is 30081
```

---

## 6. Trigger a pipeline

The repo ships a simple example pipeline that fetches JSON from a public API and
writes it as an Iceberg table.

```bash
kubectl apply -f examples/k8s/simple-pipeline.yaml
kubectl get pipelineruns -n datuplet -w
```

The run moves through `Pending → Running → Succeeded` in about 30–60 seconds.
In the UI, go to the "Runs" page to see live status.

---

## 7. Browse the result

In the UI, click "Storage" in the left nav. You'll see the `raw` namespace with
the `data` table populated by the extractor.

Alternatively, inspect via kubectl:

```bash
# List pods for the last run
kubectl get pods -n datuplet -l datuplet.io/pipeline=simple-pipeline

# View component logs
kubectl logs -n datuplet <pod-name> -c component
```

---

## 8. Tear down

```bash
kind delete cluster --name datuplet
```

This removes the cluster and all in-cluster state (Postgres, MinIO data).

---

## Next steps

- Add secrets to a pipeline: [docs/secrets.md](secrets.md)
- Switch to S3 or GCS: [docs/warehouse-setup.md](warehouse-setup.md)
- Deploy on GKE: [docs/quickstart-gke.md](quickstart-gke.md)
- Full install reference: [docs/install.md](install.md)
