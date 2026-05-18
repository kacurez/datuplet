# gcs-pipeline-k8s e2e scenario

End-to-end scenario writing to a `gs://` warehouse against an in-cluster
fake-gcs-server. Exercises:

- Lakekeeper warehouse registration with `--gcs-credential-type=service-account-key`
  (Workload Identity requires real GCP; SA-key is the in-cluster fake).
- DataGateway parquet writes via the GCS backend (Slice B).
- TableCommit transactions via iceberg-go's `gs://` factory (Slice D).
- Storage browser `LoadFS` dispatch via `GCSProps` (Slice H).
- Run-token contract verification (no change from S3 path).

## Run

```bash
./run.sh
```

The script `kubectl get nodes`-skips when no cluster is reachable, so it's safe
to invoke from the parent e2e Make target on a developer machine without
a configured cluster.

## Manual run prerequisites

1. `kubectl config current-context` points at a cluster with at least one
   Ready node.
2. The datuplet stack is already deployed: `make deploy-local` (OrbStack)
   or the equivalent on a GKE cluster.
3. `examples/k8s/simple-pipeline.yaml` runs successfully on the same
   cluster (proves the stack is healthy before adding GCS into the mix).

## How fake-gcs-server is configured

- Image: `fsouza/fake-gcs-server:1.49` (verified compatible in Slice A0 probe 3).
- Scheme: `http`; bearer-token validation is disabled (per the spike memo).
- Storage: in-memory only — every restart loses state. Acceptable for an
  e2e harness; production uses a real `gs://` bucket.

## Known gap: bootstrap wiring

The `run.sh` script deploys fake-gcs-server and applies the pipeline, but
does not yet run `pipeline-api admin lakekeeper-bootstrap` against the
fake-gcs-server to register a GCS warehouse. Until that step is wired, the
scenario will fail at "waiting for Succeeded" because lakekeeper has no GCS
warehouse registered. Bootstrap-against-fake-gcs wiring is a follow-on task.

## Slice I cleanup

When this scenario is retired (or replaced by a real-GCP e2e), delete
the directory entirely — including this README.
