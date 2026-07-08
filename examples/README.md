# Datuplet Pipeline Examples

This directory contains runnable pipeline examples demonstrating Datuplet's core capabilities.

## Available Examples

| File | What it Demonstrates | Components |
|------|----------------------|-----------|
| `pipelines/simple-http-extract.yaml` | Basic extraction: fetch JSON from public API and write to table | `http-json-extractor` |
| `pipelines/full-etl.yaml` | Full ETL: extract JSON, then SQL-transform into curated table | `http-json-extractor`, `sql-transform` |
| `pipelines/etl-duckdb.yaml` | Extract JSON, then aggregate with DuckDB SQL transform | `http-json-extractor`, `sql-transform` |
| `pipelines/processors-drop.yaml` | Generate data, then drop columns with a gateway processor before commit | `data-generator`, `stdout-writer` |
| `pipelines/incremental-reads.yaml` | Incremental read of a table using the `since` duration filter | `stdout-writer` |
| `pipelines/secrets-http-auth.yaml` | HTTP extraction authenticated with a secret-backed header | `http-json-extractor` |

## How to Run

Every file in this directory is validated by the CI guard (`examples/examples_guard_test.go`), ensuring all example pipelines are syntactically correct.

You can run a pipeline using any of these methods:

### 1. UI (Pipeline Management Portal)
1. Navigate to **Pipelines** → **New**
2. Paste the entire YAML content from an example file
3. Click **Save**
4. Click **Trigger** to start a run

### 2. REST API
See [`docs/pipeline-api.md`](../docs/pipeline-api.md) for:
- **Upload a pipeline** — `PUT /api/v1/projects/{pid}/pipelines/{name}` with the YAML
- **Trigger a run** — `POST /api/v1/projects/{pid}/pipelines/{name}/runs`

### 3. kubectl (Direct Kubernetes Deployment)
```bash
kubectl apply -f examples/pipelines/<example-file>
```

This applies both the `Pipeline` resource and triggers a `PipelineRun` in the cluster.

## Prerequisites

All examples require a running Datuplet installation. See [`docs/install.md`](../docs/install.md) for setup instructions.
