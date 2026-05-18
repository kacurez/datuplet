#!/usr/bin/env bash
set -euo pipefail

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NS="${NAMESPACE:-datuplet}"

if ! kubectl get nodes 2>/dev/null | grep -qE '^[a-z0-9-]+ +Ready'; then
    echo "SKIP: no Ready nodes in current cluster; scenario requires a working k8s cluster"
    exit 0
fi

if ! kubectl get namespace "${NS}" 2>/dev/null | grep -q "${NS}"; then
    echo "SKIP: namespace '${NS}' does not exist; run 'make deploy-local' (OrbStack) or equivalent first"
    exit 0
fi

echo "==> Deploying fake-gcs-server"
kubectl apply -f "${SCENARIO_DIR}/fake-gcs-server.yaml" -n "${NS}"
kubectl rollout status deploy/fake-gcs-server -n "${NS}" --timeout=2m

echo "==> Bootstrap-against-fake-gcs check"
# This scenario needs three things wired before it can pass CI:
#
#   1. STORAGE_EMULATOR_HOST=http://fake-gcs-server.${NS}:4443 on the
#      Lakekeeper container so the Rust GCS crate routes API calls to the
#      in-cluster fake-gcs instead of storage.googleapis.com. Add via
#      charts/datuplet-lakekeeper/values.yaml + extraEnvFrom or a new
#      `platform.storageEmulatorHost` value.
#
#   2. scripts/register.sh extended with a --warehouse-type=gcs branch
#      that calls pipeline-api admin lakekeeper-bootstrap with
#      --gcs-credential-type=service-account-key and a fake SA JSON.
#
#   3. A separate "gcs-smoke" project + attach-warehouse so this pipeline
#      doesn't silently write to the S3 warehouse the default project
#      already has attached. (Otherwise the scenario false-positive
#      passes by writing to MinIO.)
#
# Until those three land, the scenario fails CI deliberately with this
# clear message rather than a confusing pipeline-run timeout. Reviewers
# fixing forward should land them in that order.
echo "FAIL: GCS-via-Lakekeeper bootstrap is not yet wired for the in-cluster"
echo "      fake-gcs-server harness. See the comment block above for the"
echo "      three TODOs needed to make this scenario actually exercise the"
echo "      gs:// path end-to-end."
exit 1
