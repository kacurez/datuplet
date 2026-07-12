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

You can install from the Helm repo using:

```bash
helm repo add datuplet https://kacurez.github.io/datuplet
```

Or use the path-based commands below for development:

---

## 3. Build images, load them into kind, and install

kind does **not** share the host Docker daemon (unlike OrbStack) — images built
locally have to be explicitly loaded onto the kind node before Kubernetes can
use them. Build the same images `make deploy-local` builds on OrbStack, then
`kind load` each one:

```bash
# Build the five service images (pipeline-api, pipeline-observer,
# pipeline-operator, gateway, query-worker) + the five built-in component
# images. `build-components-local` additionally tags the built-ins
# `:v0.1.0` (COMPONENT_TAG) to match the chart's `components.tag` default.
make docker-build-k8s build-components-local

for img in \
  datuplet/pipeline-api:latest \
  datuplet/pipeline-observer:latest \
  datuplet/pipeline-operator:latest \
  datuplet/gateway:latest \
  datuplet/query-worker:latest \
  datuplet/data-generator:v0.1.0 \
  datuplet/sql-transform:v0.1.0 \
  datuplet/stdout-writer:v0.1.0 \
  datuplet/http-json-extractor:v0.1.0 \
  datuplet/finnhub-extractor:v0.1.0 \
; do
  kind load docker-image --name datuplet "$img"
done

./scripts/install.sh --namespace datuplet \
  -f-infra tests/local/values-local-infra.yaml \
  -f-app tests/local/values-local-app.yaml
```

Note: the chart ships a sixth built-in, `pandas-transform`, with no local build
target yet — don't use it on a local/kind cluster (it will ImagePullBackOff); the
five built-ins above, including `http-json-extractor` used by the example, work fine.

`tests/local/values-local-app.yaml` sets `image.pullPolicy=IfNotPresent` (so
kubelet uses the kind-loaded image instead of trying to pull a non-existent
`datuplet/*:latest` from Docker Hub) and `components.registry=datuplet` (so the
built-in ComponentDefinitions point at the images just loaded, not `ghcr.io`).
`install.sh` runs preflight checks, the four helm phases in order, and finally
`scripts/register.sh` — see [docs/install.md](install.md) for the full command
reference.

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

`register.sh` already ran as the last step of `install.sh` above — it runs five
idempotent steps: create the Lakekeeper warehouse, create an admin user, create
a project, attach the warehouse, and grant the admin role. At the end it prints
the admin email and password.

Default credentials (POC — change before any production use):

- Email: `admin@datuplet.local`
- Password: `changeme`

To use custom credentials instead, pass them through `install.sh`'s `--`
passthrough (everything after `--` goes to `register.sh`):

```bash
./scripts/install.sh --namespace datuplet \
  -f-infra tests/local/values-local-infra.yaml \
  -f-app tests/local/values-local-app.yaml \
  -- --admin-email you@example.com --admin-password 'replace-with-a-strong-password'
```

Or re-run `register.sh` directly after the fact — it's idempotent and safe to
re-run with different flags:

```bash
./scripts/register.sh --namespace datuplet \
  --admin-email you@example.com --admin-password 'replace-with-a-strong-password'
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
kubectl apply -f examples/pipelines/simple-http-extract.yaml
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
