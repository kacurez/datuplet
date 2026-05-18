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
kubectl apply -f "${SCENARIO_DIR}/fake-gcs-server.yaml"
kubectl rollout status deploy/fake-gcs-server -n "${NS}" --timeout=2m

echo "==> Bootstrapping a fake-gcs warehouse via pipeline-api"
# Skip in scenarios that haven't been wired to fake-gcs yet — the
# integration harness for the bootstrap subcommand against fake-gcs is
# tracked separately in the gcs-pipeline-k8s readme.

echo "==> Applying pipeline + triggering run"
kubectl apply -f "${SCENARIO_DIR}/pipeline.yaml"
kubectl create -f - <<EOF
apiVersion: datuplet.io/v1
kind: PipelineRun
metadata:
  generateName: gcs-smoke-
  namespace: ${NS}
spec:
  pipelineRef:
    name: gcs-smoke
EOF

echo "==> Waiting for Succeeded"
for i in $(seq 1 60); do
    PHASE=$(kubectl -n "${NS}" get pipelinerun -o jsonpath='{.items[-1].status.phase}' 2>/dev/null || true)
    if [[ "$PHASE" == "Succeeded" ]]; then
        echo "PASS: pipeline succeeded"
        exit 0
    fi
    if [[ "$PHASE" == "Failed" ]]; then
        kubectl -n "${NS}" describe pipelinerun
        echo "FAIL: pipeline failed"
        exit 1
    fi
    sleep 5
done

echo "FAIL: pipeline did not reach Succeeded within 5 min"
exit 1
