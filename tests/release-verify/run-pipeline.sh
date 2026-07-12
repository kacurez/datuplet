#!/usr/bin/env bash
# Upload + trigger + await one pipeline through the public REST API — the
# same path a user takes. Used by release-verify and upgrade-e2e workflows.
set -euo pipefail
BASE_URL=${BASE_URL:-http://localhost:30081}
EMAIL=${DATUPLET_ADMIN_EMAIL:?set DATUPLET_ADMIN_EMAIL}
PASSWORD=${DATUPLET_ADMIN_PASSWORD:?set DATUPLET_ADMIN_PASSWORD}
PIPELINE_FILE=${1:?usage: run-pipeline.sh <rendered-pipeline.yaml>}
TIMEOUT_S=${TIMEOUT_S:-600}

NAME=$(sed -n 's/^  name: //p' "$PIPELINE_FILE" | head -1)
[ -n "$NAME" ] || { echo "FATAL: no metadata.name in $PIPELINE_FILE" >&2; exit 2; }
JAR=$(mktemp); trap 'rm -f "$JAR"' EXIT

echo "--- login $BASE_URL as $EMAIL"
curl -fsS -c "$JAR" -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" \
  "$BASE_URL/api/v1/auth/login" >/dev/null

# GET /api/v1/projects returns a bare JSON array of project objects
# (pkg/pipelineapi/http/project_handlers.go handleListProjects: writeJSON
# with a []projectJSON slice) — no wrapping envelope.
PID=$(curl -fsS -b "$JAR" "$BASE_URL/api/v1/projects" | jq -r '.[0].id')
[ -n "$PID" ] && [ "$PID" != null ] || { echo "FATAL: no project found" >&2; exit 2; }

echo "--- upload pipeline '$NAME' to project $PID"
curl -fsS -b "$JAR" -X PUT -H 'Content-Type: application/yaml' \
  --data-binary @"$PIPELINE_FILE" \
  "$BASE_URL/api/v1/projects/$PID/pipelines/$NAME" >/dev/null

echo "--- trigger run"
# POST .../pipelines/{name}/runs returns {"id":..., "status":"Pending", "k8s_ns":...}
# (pkg/pipelineapi/http/run_handlers.go handleTriggerRun) — the run id key is "id".
RUN_ID=$(curl -fsS -b "$JAR" -X POST \
  "$BASE_URL/api/v1/projects/$PID/pipelines/$NAME/runs" | jq -r '.id')
[ -n "$RUN_ID" ] && [ "$RUN_ID" != null ] || { echo "FATAL: trigger returned no run id" >&2; exit 2; }
echo "run id: $RUN_ID"
echo "$RUN_ID" > /tmp/datuplet-last-run-id   # consumed by upgrade-e2e's post-upgrade assert

deadline=$(( $(date +%s) + TIMEOUT_S ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  # GET .../runs/{id} embeds runJSON, whose run state field is "phase"
  # (json:"phase" in pkg/pipelineapi/http/run_handlers.go) — there is no
  # top-level "status" key on this response.
  STATUS=$(curl -fsS -b "$JAR" "$BASE_URL/api/v1/projects/$PID/runs/$RUN_ID" \
    | jq -r '.phase')
  echo "  status: $STATUS"
  case "$STATUS" in
    Succeeded) echo "OK: run $RUN_ID Succeeded"; exit 0 ;;
    Failed*|Cancelled|Expired) echo "FAIL: run $RUN_ID ended $STATUS" >&2; exit 1 ;;
  esac
  sleep 10
done
echo "FAIL: run $RUN_ID did not finish within ${TIMEOUT_S}s" >&2; exit 1
