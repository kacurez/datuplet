#!/usr/bin/env bash
# Run the K8s e2e suite with host-side port-forwards for the endpoints the
# test framework (tests/e2e/framework) reaches on localhost.
#
# Why this exists:
#   The e2e framework was written for OrbStack, which surfaces K8s
#   NodePorts / services on localhost automatically. CI uses a plain kind
#   cluster (helm/kind-action, no extraPortMappings), which exposes NOTHING
#   to the host — so SetupFGABootstrap fails to reach OpenFGA at
#   localhost:8180 and every K8s scenario SKIPs (a green-but-vacuous run).
#   This wrapper forwards the four endpoints the framework expects, then
#   runs the suite.
#
# Endpoints (framework defaults — see framework/bootstrap.go,
# framework/pipeline_api_client.go, framework/scenario.go):
#   OpenFGA      localhost:8180  -> svc/openfga      :8080  (HTTP API)
#   Lakekeeper   localhost:8181  -> svc/lakekeeper   :8181
#   pipeline-api localhost:30081 -> svc/pipeline-api :8081  (NodePort default)
#   MinIO        localhost:30900 -> svc/minio        :9000  (NodePort default)
#
# Each forward is only started if the local port is NOT already reachable,
# so this is safe on OrbStack (where the NodePorts are already bound — an
# unconditional forward would fail with "address already in use") as well
# as on CI kind (where nothing is bound and all four get forwarded).
set -euo pipefail

NS="${1:-datuplet-e2e}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

declare -a PF_PIDS=()
cleanup() {
	for pid in "${PF_PIDS[@]:-}"; do
		[ -n "${pid:-}" ] && pkill -P "$pid" 2>/dev/null; kill "$pid" 2>/dev/null || true
	done
}
trap cleanup EXIT INT TERM

# port_open <port> — true if something is already listening on 127.0.0.1:<port>.
port_open() {
	(exec 3<>"/dev/tcp/127.0.0.1/$1") >/dev/null 2>&1 && { exec 3>&- 3<&-; return 0; }
	return 1
}

# wait_port <port> <name> — block until the local port accepts a connection.
wait_port() {
	for _ in $(seq 1 60); do
		port_open "$1" && return 0
		sleep 1
	done
	echo "ERROR: $2 not reachable on localhost:$1 after 60s" >&2
	return 1
}

# supervise_pf <local_port> <svc> <target_port> <name> — respawn the forward
# whenever it dies (kubectl port-forward exits when its target pod restarts;
# the query e2e rolls pipeline-api mid-suite).
supervise_pf() {
	while true; do
		kubectl port-forward -n "$NS" --address 127.0.0.1 "svc/$2" "$1:$3" >/dev/null 2>&1 || true
		echo "e2e: port-forward $4 (localhost:$1) exited; respawning in 1s" >&2
		sleep 1
	done
}

maybe_pf() {
	if port_open "$1"; then
		echo "e2e: $4 already reachable on localhost:$1 (skipping port-forward)"
		return 0
	fi
	echo "e2e: supervised port-forward svc/$2 $1:$3 (ns $NS)"
	supervise_pf "$1" "$2" "$3" "$4" &
	PF_PIDS+=("$!")
	wait_port "$1" "$4"
}

maybe_pf 8180  openfga      8080 OpenFGA
maybe_pf 8181  lakekeeper   8181 Lakekeeper
maybe_pf 30081 pipeline-api 8081 pipeline-api
maybe_pf 30900 minio        9000 MinIO

# OpenFGA runs with authn.method=preshared (chart default), so the test
# framework must send a bearer token to OpenFGA's HTTP API. The keygen Job
# stores it in the <release>-openfga-api-key Secret under key "keys". Source
# it into OPENFGA_API_KEY (which framework/bootstrap.go reads) unless the
# caller already exported one. kubectl does the base64 decode (base64decode)
# so this stays portable across GNU/BSD base64.
if [ -z "${OPENFGA_API_KEY:-}" ]; then
	secret="$(kubectl get secret -n "$NS" -o name 2>/dev/null | grep openfga-api-key | head -1 || true)"
	if [ -n "$secret" ]; then
		OPENFGA_API_KEY="$(kubectl get "$secret" -n "$NS" -o go-template='{{index .data "keys" | base64decode}}')"
		export OPENFGA_API_KEY
		echo "e2e: sourced OPENFGA_API_KEY from $secret"
	else
		echo "e2e: WARNING — no *openfga-api-key secret found in $NS; FGA bootstrap may 401" >&2
	fi
fi

echo "e2e: endpoints ready; running suite"
cd "$REPO_ROOT/tests/e2e"
if [ -n "${E2E_JSON:-}" ]; then
	E2E_K8S=1 \
		DATUPLET_OPENFGA_URL="http://localhost:8180" \
		DATUPLET_LAKEKEEPER_URL="http://localhost:8181" \
		DATUPLET_PIPELINE_API_URL="http://localhost:30081" \
		go test -v -count=1 -timeout 30m -json ./... | tee "$E2E_JSON" | \
		grep -E '"Action":"(pass|fail|skip)"' --line-buffered | \
		jq -r 'select(.Test != null) | "\(.Action)\t\(.Test)"' || exit 1
else
	E2E_K8S=1 \
		DATUPLET_OPENFGA_URL="http://localhost:8180" \
		DATUPLET_LAKEKEEPER_URL="http://localhost:8181" \
		DATUPLET_PIPELINE_API_URL="http://localhost:30081" \
		go test -v -count=1 -timeout 30m ./...
fi
