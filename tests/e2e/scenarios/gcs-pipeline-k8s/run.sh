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
# This scenario can't actually exercise the gs:// path end-to-end via
# Lakekeeper today. The blocker is upstream: Lakekeeper hardcodes
# storage.googleapis.com — no GCS endpoint override exists in its
# Rust source (verified at crates/lakekeeper/src/service/storage/gcs/
# mod.rs). The in-cluster fake-gcs-server can't be reached without
# either:
#
#   (a) real GCP credentials wired via GitHub Actions secrets, OR
#   (b) an upstream Lakekeeper PR adding LAKEKEEPER__GCS_ENDPOINT (or
#       equivalent) so the Rust GCS crate can be pointed at fake-gcs.
#
# Until one of those lands, this script is a skeleton — kept in tree
# so the harness is ready when the upstream/credentials story settles.
# Not run in CI (see .github/workflows/e2e.yml).
echo "SKIP: GCS-via-Lakekeeper testing requires either real GCP creds"
echo "      or an upstream endpoint-override PR. Skipping until wired."
exit 0
