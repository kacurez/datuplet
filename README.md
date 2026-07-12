# Datuplet

![Status: Experimental](https://img.shields.io/badge/status-experimental-orange)
![CI: TBD](https://img.shields.io/badge/CI-TBD-lightgrey)
![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)

Datuplet is an experimental streaming ETL platform for Kubernetes. It orchestrates
multi-stage pipelines where thin component containers (~300 LOC per SDK) write to
Apache Iceberg tables on S3, GCS, or bundled MinIO — with no long-lived cloud
credentials on any running Deployment. A Data Gateway sidecar handles all storage
I/O for each component via gRPC (control) and HTTP (data); Lakekeeper, an Iceberg
REST catalog, holds warehouse credentials and vends short-lived STS credentials per
run. Fine-grained per-project authorization is enforced by OpenFGA.

This is a 0.x release. APIs, CRD shapes, and chart values may change between minor
versions. GKE is the only validated cloud target for 0.1.

---

## 10-minute quickstart (kind)

```bash
# 1. Create a kind cluster
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

# 2. Clone the repo
git clone https://github.com/kacurez/datuplet && cd datuplet

# 3. Build images and load them into kind (kind doesn't share the host
#    Docker daemon, unlike OrbStack, so images must be loaded explicitly)
make docker-build-k8s build-components-local
for img in \
  datuplet/pipeline-api:latest datuplet/pipeline-observer:latest \
  datuplet/pipeline-operator:latest datuplet/gateway:latest datuplet/query-worker:latest \
  datuplet/data-generator:v0.1.0 datuplet/sql-transform:v0.1.0 \
  datuplet/stdout-writer:v0.1.0 datuplet/http-json-extractor:v0.1.0 \
  datuplet/finnhub-extractor:v0.1.0 \
; do kind load docker-image --name datuplet "$img"; done

# 4. Install the four charts + bootstrap warehouse/admin user (register.sh runs last)
./scripts/install.sh --namespace datuplet -f-app tests/local/values-local-app.yaml

# 5. Open the UI
open http://localhost:8080/ui/
# Login: admin@datuplet.local / changeme  (change these in production)

# 6. Trigger a pipeline
kubectl apply -f examples/pipelines/simple-http-extract.yaml
kubectl get pipelineruns -n datuplet -w
```

Full step-by-step version: [docs/quickstart-kind.md](docs/quickstart-kind.md)

---

## Try on your own cluster (GKE + GCS)

See [docs/quickstart-gke.md](docs/quickstart-gke.md) for a 30-minute walkthrough
using a real GKE cluster and a GCS bucket as the Iceberg warehouse.

---

## What's in the box

- Four-chart Helm install: `datuplet-operators`, `datuplet-infra`, `datuplet-app`,
  `datuplet-lakekeeper` — each with its own upgrade cadence.
- Streaming ETL via a Data Gateway sidecar (gRPC control + HTTP data) with
  thin Go and Python SDKs.
- Apache Iceberg table format on top of S3 / GCS / bundled MinIO.
- `pipeline-api` HTTP control plane and browser UI for pipelines, runs, secrets,
  storage browse, and the registry-driven component catalog.
- `pipeline-observer` K8s informer that mirrors PipelineRun status into Postgres.
- `pipeline-operator` controller that reconciles PipelineRun CRDs into component
  Pods, each with a Data Gateway sidecar.
- Lakekeeper Iceberg REST catalog with OIDC validation and STS-vended credentials.
  No long-lived warehouse credentials on any Datuplet Deployment.
- OpenFGA fine-grained authorization per project, run, and table.
- Built-in component images: `data-generator`, `http-json-extractor`,
  `finnhub-extractor`, `sql-transform` (embedded DuckDB), `stdout-writer`.

---

## Architecture at a glance

A user triggers a pipeline via the UI or REST API. `pipeline-api` mints a
per-run RS256 JWT and creates a PipelineRun CRD. `pipeline-operator` schedules
component Pods, each with a Data Gateway sidecar. The sidecar fetches
STS credentials from Lakekeeper, writes parquet to S3/GCS, and commits the
files to the Iceberg table inline via its commit pool (iceberg-go against
Lakekeeper), leaving a per-table `files.json` audit breadcrumb. OpenFGA
enforces authorization at every step.

Full diagram and component descriptions: [docs/architecture.md](docs/architecture.md)

---

## Documentation

| Document | Description |
|---|---|
| [docs/quickstart-kind.md](docs/quickstart-kind.md) | 10-minute install on a local kind cluster |
| [docs/quickstart-gke.md](docs/quickstart-gke.md) | GKE + GCS deployment |
| [docs/install.md](docs/install.md) | Full install guide (all clusters) |
| [docs/architecture.md](docs/architecture.md) | System overview, data flow, auth |
| [docs/components.md](docs/components.md) | Built-in component catalog |
| [docs/warehouse-setup.md](docs/warehouse-setup.md) | S3 / GCS / MinIO warehouse prep |
| [docs/pipeline-api.md](docs/pipeline-api.md) | pipeline-api REST reference |
| [docs/ad-hoc-query.md](docs/ad-hoc-query.md) | Ad-hoc SQL query (browser console, REST, CLI) |
| [docs/auth-flow.md](docs/auth-flow.md) | Token lifecycle (session → run JWT → FGA) |
| [docs/secrets.md](docs/secrets.md) | Secret references in pipeline YAML |
| [docs/troubleshooting.md](docs/troubleshooting.md) | Common failures and fixes |
| [docs/known-limitations.md](docs/known-limitations.md) | Known gaps for 0.1 |
| [docs/postgres-migrations.md](docs/postgres-migrations.md) | DB migration discipline |
| [docs/fga-model-upgrades.md](docs/fga-model-upgrades.md) | FGA model upgrade procedure |
| [CHANGELOG.md](CHANGELOG.md) | Release history |

---

## Contributing

PRs welcome. This is an early-stage project; opening an issue to discuss the
change first is encouraged for anything beyond small bug fixes. Coding conventions,
agent workflow, and architectural context are in [CLAUDE.md](CLAUDE.md).

---

## License

Apache-2.0. See [LICENSE](LICENSE).
