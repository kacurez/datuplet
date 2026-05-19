#!/usr/bin/env bash
# scripts/register.sh — RFC 014.9.3 post-install business-state bootstrap.
#
# Runs AFTER `helm install datuplet-infra` (Layer 1) and
# `helm install datuplet-app` (Layer 2) are complete.
#
# Performs five idempotent steps in sequence:
#   1. pipeline-api admin lakekeeper-bootstrap  (warehouse + server-admin FGA tuple)
#   2. pipeline-api admin create-user            (admin user account)
#   3. pipeline-api admin create-project         (Datuplet project + FGA project_admin)
#   4. pipeline-api admin attach-warehouse       (associate warehouse with project)
#   5. pipeline-api admin grant                  (grant role to admin user on project)
#
# Execution modes:
#   --mode=exec (default): kubectl exec into the pipeline-api Pod.
#   --mode=job:            spawn ephemeral one-shot Jobs. Use for CI/CD
#                          environments without exec privileges.
#
# Usage:
#   scripts/register.sh [OPTIONS]
#
# Required (or defaulted for POC):
#   --namespace        NS        Kubernetes namespace        (default: datuplet)
#
# Admin credentials (both must be provided together, or POC defaults are used):
#   --admin-email    EMAIL     Admin user email    (default: admin@datuplet.local — INSECURE POC)
#   --admin-password PASSWORD  Admin user password (default: changeme — INSECURE POC)
#
# Project / warehouse options:
#   --project-name   NAME      Datuplet project name        (default: default)
#   --warehouse-name NAME      Lakekeeper warehouse name    (default: datuplet)
#   --warehouse-type s3|gcs    Warehouse storage type       (default: s3)
#
# S3 / MinIO options (for --warehouse-type=s3):
#   --s3-endpoint    URL       S3/MinIO endpoint URL
#                              (default: read from minio Secret in the namespace,
#                               falls back to http://minio.<namespace>.svc.cluster.local:9000)
#   --s3-bucket      BUCKET    S3 bucket name               (default: datuplet)
#   --s3-access-key  KEY       S3 access key
#                              (default: read from minio Secret key 'rootUser')
#   --s3-secret-key  SECRET    S3 secret key
#                              (default: read from minio Secret key 'rootPassword')
#   --s3-region      REGION    S3 region                    (default: local-01)
#   --path-style               Enable S3 path-style         (default: true)
#   --no-sts                   Disable STS-vended creds     (default: sts enabled,
#                              REQUIRED for --warehouse-type=gcs + --gcs-credential-type=system-identity)
#
# GCS options (for --warehouse-type=gcs):
#   --gcs-bucket            BUCKET   GCS bucket name (required)
#   --gcs-key-prefix        PREFIX   Optional key prefix under the bucket
#   --gcs-credential-type   TYPE     system-identity (default; WIF) | service-account-key
#   --gcs-sa-key-file       PATH     Path to a Google SA JSON key file
#                                    (required iff --gcs-credential-type=service-account-key)
#
# Misc:
#   --mode exec|job            Execution mode               (default: exec)
#   --context KUBECONTEXT      kubectl context to use
#   --dry-run                  Print commands without executing
#   --help                     Show this help
#
# MinIO credentials note:
#   The minio subchart (fullnameOverride=minio) creates a Secret named "minio"
#   with keys "rootUser" and "rootPassword". This script reads those by default.
#   If you use --s3-access-key / --s3-secret-key, the Secret is not consulted.

set -euo pipefail

# ─── Defaults ─────────────────────────────────────────────────────────────────
NAMESPACE="datuplet"
ADMIN_EMAIL="admin@datuplet.local"
ADMIN_PASSWORD="changeme"
PROJECT_NAME="default"
WAREHOUSE_NAME="datuplet"
WAREHOUSE_TYPE="s3"
S3_ENDPOINT=""
S3_BUCKET="datuplet"
S3_ACCESS_KEY=""
S3_SECRET_KEY=""
S3_REGION="local-01"
PATH_STYLE="true"
STS_ENABLED="true"
GCS_BUCKET=""
GCS_KEY_PREFIX=""
GCS_CREDENTIAL_TYPE="system-identity"
GCS_SA_KEY_FILE=""
MODE="exec"
KUBECTL_CONTEXT=""
DRY_RUN="false"

# ─── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace)        NAMESPACE="$2";        shift 2 ;;
    --admin-email)      ADMIN_EMAIL="$2";      shift 2 ;;
    --admin-password)   ADMIN_PASSWORD="$2";   shift 2 ;;
    --project-name)     PROJECT_NAME="$2";     shift 2 ;;
    --warehouse-name)   WAREHOUSE_NAME="$2";   shift 2 ;;
    --warehouse-type)   WAREHOUSE_TYPE="$2";   shift 2 ;;
    --s3-endpoint)      S3_ENDPOINT="$2";      shift 2 ;;
    --s3-bucket)        S3_BUCKET="$2";        shift 2 ;;
    --s3-access-key)    S3_ACCESS_KEY="$2";    shift 2 ;;
    --s3-secret-key)    S3_SECRET_KEY="$2";    shift 2 ;;
    --s3-region)        S3_REGION="$2";        shift 2 ;;
    --path-style)       PATH_STYLE="true";     shift 1 ;;
    --no-path-style)    PATH_STYLE="false";    shift 1 ;;
    --no-sts)           STS_ENABLED="false";   shift 1 ;;
    --gcs-bucket)         GCS_BUCKET="$2";          shift 2 ;;
    --gcs-key-prefix)     GCS_KEY_PREFIX="$2";      shift 2 ;;
    --gcs-credential-type) GCS_CREDENTIAL_TYPE="$2"; shift 2 ;;
    --gcs-sa-key-file)    GCS_SA_KEY_FILE="$2";     shift 2 ;;
    --mode)             MODE="$2";             shift 2 ;;
    --context)          KUBECTL_CONTEXT="$2";  shift 2 ;;
    --dry-run)          DRY_RUN="true";        shift 1 ;;
    --help|-h)
      grep '^#' "$0" | grep -v '^#!/' | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ─── Validate mode ────────────────────────────────────────────────────────────
if [[ "$MODE" != "exec" && "$MODE" != "job" ]]; then
  echo "ERROR: --mode must be 'exec' or 'job'" >&2
  exit 1
fi

# ─── Validate warehouse-type ─────────────────────────────────────────────────
if [[ "$WAREHOUSE_TYPE" != "s3" && "$WAREHOUSE_TYPE" != "gcs" ]]; then
  echo "ERROR: --warehouse-type must be 's3' or 'gcs' (got '${WAREHOUSE_TYPE}')" >&2
  exit 1
fi

# ─── Validate GCS args (fail fast before kubectl exec) ───────────────────────
if [[ "$WAREHOUSE_TYPE" == "gcs" ]]; then
  if [[ -z "$GCS_BUCKET" ]]; then
    echo "ERROR: --gcs-bucket is required with --warehouse-type=gcs" >&2
    exit 1
  fi
  case "$GCS_CREDENTIAL_TYPE" in
    system-identity|service-account-key) ;;
    *) echo "ERROR: --gcs-credential-type must be 'system-identity' or 'service-account-key' (got '${GCS_CREDENTIAL_TYPE}')" >&2; exit 1 ;;
  esac
  if [[ "$GCS_CREDENTIAL_TYPE" == "service-account-key" && -z "$GCS_SA_KEY_FILE" ]]; then
    echo "ERROR: --gcs-credential-type=service-account-key requires --gcs-sa-key-file" >&2
    exit 1
  fi
  if [[ "$GCS_CREDENTIAL_TYPE" == "system-identity" && "$STS_ENABLED" != "true" ]]; then
    # Mirrors gcsSpec.Validate (Slice 3 of v0.2.1): WIF requires STS downscoping.
    echo "ERROR: --gcs-credential-type=system-identity requires --sts-enabled (do not pass --no-sts)" >&2
    exit 1
  fi
fi

# ─── kubectl wrapper ──────────────────────────────────────────────────────────
KUBECTL="kubectl"
if [[ -n "$KUBECTL_CONTEXT" ]]; then
  KUBECTL="kubectl --context=${KUBECTL_CONTEXT}"
fi

kube() {
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[dry-run] kubectl $*"
  else
    $KUBECTL "$@"
  fi
}

# ─── Resolve minio credentials from the cluster ───────────────────────────────
# The minio subchart (fullnameOverride=minio) creates Secret "minio" with
# keys "rootUser" and "rootPassword". We read those unless overridden by flags.
resolve_minio_creds() {
  if [[ -z "$S3_ACCESS_KEY" ]] || [[ -z "$S3_SECRET_KEY" ]]; then
    echo "  Resolving MinIO credentials from Secret 'minio' in namespace ${NAMESPACE}..."
    if S3_ACCESS_KEY_RAW=$($KUBECTL get secret -n "$NAMESPACE" minio \
        -o jsonpath='{.data.rootUser}' 2>/dev/null); then
      S3_ACCESS_KEY=$(printf '%s' "$S3_ACCESS_KEY_RAW" | base64 -d)
      S3_SECRET_KEY=$($KUBECTL get secret -n "$NAMESPACE" minio \
        -o jsonpath='{.data.rootPassword}' | base64 -d)
      echo "  MinIO credentials resolved from Secret."
    else
      echo "  WARNING: Secret 'minio' not found in namespace ${NAMESPACE}." >&2
      echo "  Pass --s3-access-key and --s3-secret-key explicitly." >&2
      if [[ "$DRY_RUN" != "true" ]]; then
        exit 1
      fi
      S3_ACCESS_KEY="${S3_ACCESS_KEY:-datuplet}"
      S3_SECRET_KEY="${S3_SECRET_KEY:-changeme}"
    fi
  fi
  if [[ -z "$S3_ENDPOINT" ]]; then
    S3_ENDPOINT="http://minio.${NAMESPACE}.svc.cluster.local:9000"
    echo "  S3 endpoint defaulting to ${S3_ENDPOINT}"
  fi
}

# ─── Resolve signing-key path for exec mode ───────────────────────────────────
# The signing-key Secret (signing-key) has key:
#   signing-key.pem — RS256 private key (projected into pipeline-api at
#                     /var/run/secrets/datuplet-signing-key/signing-key.pem
#                     per the Deployment's volumeMount).
# In exec mode we exec into the pipeline-api Pod where the signing key is
# already projected at SIGNING_KEY_FILE (env in the Deployment, Slice 6.4).
# In job mode we mount the Secret at the same path.
SIGNING_KEY_FILE="/var/run/secrets/datuplet-signing-key/signing-key.pem"
SIGNING_KEY_SECRET="signing-key"

# ─── Resolve OpenFGA info for lakekeeper-bootstrap ───────────────────────────
# lakekeeper-bootstrap needs OPENFGA_URL + OPENFGA_API_KEY to write the
# server-admin FGA tuple (Slice 8). These are environment variables on the
# pipeline-api Pod (Deployment sets them from the openfga-api-key Secret).
# In exec mode, they're already in env; in job mode we read the Secret.
OPENFGA_API_KEY_SECRET="openfga-api-key"
LAKEKEEPER_URL="http://lakekeeper.${NAMESPACE}.svc.cluster.local:8181"
OPENFGA_URL="http://openfga.${NAMESPACE}.svc.cluster.local:8080"

# ─── Resolve pipeline-api Pod name ───────────────────────────────────────────
resolve_pipeline_api_pod() {
  local pod
  # Filter by phase=Running — both the main Deployment and the
  # pipeline-api-migrate Job carry app.kubernetes.io/name=pipeline-api
  # (intentional for NetworkPolicy selectors). The migrate Pod ends up
  # in Succeeded phase post-install, so without the filter we sometimes
  # pick it up and `kubectl exec` fails with "cannot exec into a
  # completed pod; current phase is Succeeded".
  pod=$($KUBECTL get pods -n "$NAMESPACE" \
    -l "app.kubernetes.io/name=pipeline-api" \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -z "$pod" ]]; then
    echo "ERROR: No pipeline-api Pod found in namespace ${NAMESPACE}" >&2
    echo "  Check: kubectl get pods -n ${NAMESPACE} -l app.kubernetes.io/name=pipeline-api" >&2
    exit 1
  fi
  echo "$pod"
}

# ─── exec-mode runner ─────────────────────────────────────────────────────────
# Runs a pipeline-api admin subcommand by kubectl exec into the pipeline-api Pod.
# The Pod already has SIGNING_KEY_FILE, DATABASE_URL, OPENFGA_URL, OPENFGA_API_KEY,
# OPENFGA_STORE_NAME, OPENFGA_MODEL_VERSION set via the Deployment. pipeline-api
# self-discovers store_id + model_id from these at startup (Slice 7a).
exec_admin() {
  local pod="$1"; shift
  local subcommand="$1"; shift
  echo "  kubectl exec ${pod}: pipeline-api admin ${subcommand} $*"
  if [[ "$DRY_RUN" != "true" ]]; then
    $KUBECTL exec -n "$NAMESPACE" "$pod" -- \
      /usr/local/bin/pipeline-api admin "$subcommand" "$@"
  fi
}

# ─── job-mode runner ─────────────────────────────────────────────────────────
# Spawns an ephemeral Job using the pipeline-api image from the Deployment,
# runs the admin subcommand inside, then waits for completion.
job_admin() {
  local subcommand="$1"; shift
  local job_name
  job_name="register-$(echo "${subcommand}" | tr '_' '-')-$(date +%s)"

  # Resolve the pipeline-api image from the Deployment.
  local image
  image=$($KUBECTL get deployment -n "$NAMESPACE" "pipeline-api" \
    -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true)
  if [[ -z "$image" ]]; then
    echo "ERROR: Cannot resolve pipeline-api image from Deployment pipeline-api" >&2
    exit 1
  fi

  # Resolve DATABASE_URL from the Deployment env (pipeline-api reads DATABASE_URL).
  local db_url
  db_url=$($KUBECTL get deployment -n "$NAMESPACE" "pipeline-api" \
    -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="DATABASE_URL")].value}' 2>/dev/null || true)
  if [[ -z "$db_url" ]]; then
    # Fallback: construct from the platform postgres connection Secret.
    local pg_secret="pg-pipeline-api"
    local pg_host pg_port pg_db pg_user pg_pass
    pg_host=$($KUBECTL get secret -n "$NAMESPACE" "$pg_secret" -o jsonpath='{.data.POSTGRES_HOST}' | base64 -d)
    pg_port=$($KUBECTL get secret -n "$NAMESPACE" "$pg_secret" -o jsonpath='{.data.POSTGRES_PORT}' | base64 -d)
    pg_db=$($KUBECTL get secret -n "$NAMESPACE" "$pg_secret"   -o jsonpath='{.data.POSTGRES_DB}'   | base64 -d)
    pg_user=$($KUBECTL get secret -n "$NAMESPACE" "$pg_secret" -o jsonpath='{.data.POSTGRES_USER}'  | base64 -d)
    local pw_secret="pg-pipeline-api-pw"
    pg_pass=$($KUBECTL get secret -n "$NAMESPACE" "$pw_secret" -o jsonpath='{.data.password}' | base64 -d)
    db_url="postgres://${pg_user}:${pg_pass}@${pg_host}:${pg_port}/${pg_db}?sslmode=disable"
  fi

  # Resolve OpenFGA store-name + model-version from the Deployment env. Slice
  # 7a replaced OPENFGA_STORE_ID / OPENFGA_MODEL_ID with name+version env vars
  # — pipeline-api self-discovers the IDs at startup via
  # authz.ResolveStoreAndModel. Pass the name+version through so spawned Jobs
  # do the same resolution at startup.
  local store_name model_version openfga_api_key
  store_name=$($KUBECTL get deployment -n "$NAMESPACE" "pipeline-api" \
    -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="OPENFGA_STORE_NAME")].value}' 2>/dev/null || true)
  if [[ -z "$store_name" ]]; then store_name="datuplet"; fi
  model_version=$($KUBECTL get deployment -n "$NAMESPACE" "pipeline-api" \
    -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="OPENFGA_MODEL_VERSION")].value}' 2>/dev/null || true)
  openfga_api_key=$($KUBECTL get secret -n "$NAMESPACE" "$OPENFGA_API_KEY_SECRET" \
    -o jsonpath='{.data.keys}' | base64 -d)

  echo "  Spawning Job ${job_name}: pipeline-api admin ${subcommand} $*"
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[dry-run] would create Job ${job_name}"
    return 0
  fi

  $KUBECTL create job -n "$NAMESPACE" "$job_name" \
    --image="$image" \
    -- /usr/local/bin/pipeline-api admin "$subcommand" "$@"

  # Patch the Job with required env vars via patch (kubectl create job doesn't accept --env).
  $KUBECTL patch job -n "$NAMESPACE" "$job_name" --type=json -p="[
    {\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/env\", \"value\": [
      {\"name\": \"DATABASE_URL\",    \"value\": \"${db_url}\"},
      {\"name\": \"SIGNING_KEY_FILE\",\"value\": \"${SIGNING_KEY_FILE}\"},
      {\"name\": \"OPENFGA_URL\",     \"value\": \"${OPENFGA_URL}\"},
      {\"name\": \"OPENFGA_API_KEY\", \"value\": \"${openfga_api_key}\"},
      {\"name\": \"OPENFGA_STORE_NAME\",   \"value\": \"${store_name}\"},
      {\"name\": \"OPENFGA_MODEL_VERSION\",\"value\": \"${model_version}\"}
    ]},
    {\"op\": \"add\", \"path\": \"/spec/template/spec/volumes\", \"value\": [
      {\"name\": \"signing-key\", \"secret\": {\"secretName\": \"${SIGNING_KEY_SECRET}\"}}
    ]},
    {\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/volumeMounts\", \"value\": [
      {\"name\": \"signing-key\", \"mountPath\": \"/var/run/secrets/datuplet-signing-key\", \"readOnly\": true}
    ]}
  ]"

  echo "  Waiting for Job ${job_name} to complete..."
  $KUBECTL wait job -n "$NAMESPACE" "$job_name" \
    --for=condition=complete --timeout=120s
  echo "  Job ${job_name} succeeded."
}

# ─── Dispatch an admin subcommand via the configured mode ─────────────────────
run_admin() {
  local subcommand="$1"; shift
  if [[ "$MODE" == "exec" ]]; then
    exec_admin "$PIPELINE_API_POD" "$subcommand" "$@"
  else
    job_admin "$subcommand" "$@"
  fi
}

# ─── Warn about insecure POC defaults ─────────────────────────────────────────
if [[ "$ADMIN_EMAIL" == "admin@datuplet.local" || "$ADMIN_PASSWORD" == "changeme" ]]; then
  echo "WARNING: Using insecure POC default admin credentials." >&2
  echo "         Pass --admin-email and --admin-password for production installs." >&2
fi

echo "=== Datuplet register.sh ==="
echo "  mode:             ${MODE}"
echo "  namespace:        ${NAMESPACE}"
echo "  project-name:     ${PROJECT_NAME}"
echo "  warehouse-name:   ${WAREHOUSE_NAME}"
echo "  warehouse-type:   ${WAREHOUSE_TYPE}"
echo ""

# ─── Resolve pipeline-api Pod (exec mode only) ───────────────────────────────
PIPELINE_API_POD=""
if [[ "$MODE" == "exec" ]]; then
  echo ">> Resolving pipeline-api Pod..."
  PIPELINE_API_POD=$(resolve_pipeline_api_pod)
  echo "   Pod: ${PIPELINE_API_POD}"
fi

# ─── Resolve MinIO credentials ───────────────────────────────────────────────
if [[ "$WAREHOUSE_TYPE" == "s3" ]] && [[ "$DRY_RUN" != "true" ]]; then
  resolve_minio_creds
elif [[ "$WAREHOUSE_TYPE" == "s3" ]] && [[ "$DRY_RUN" == "true" ]]; then
  S3_ACCESS_KEY="${S3_ACCESS_KEY:-datuplet}"
  S3_SECRET_KEY="${S3_SECRET_KEY:-changeme}"
  S3_ENDPOINT="${S3_ENDPOINT:-http://minio.${NAMESPACE}.svc.cluster.local:9000}"
fi

# ─── Shared flags for lakekeeper+openfga connectivity ────────────────────────
# These are injected via env vars in exec mode (the Pod env already has them).
# In job mode they're passed explicitly; exec mode also accepts them as flags
# for clarity (pipeline-api reads them from env when flags are absent).
LK_FLAGS=(
  "--lakekeeper-url=${LAKEKEEPER_URL}"
  "--signing-key-file=${SIGNING_KEY_FILE}"
)
PROVISIONING_FLAGS=(
  "--signing-key-file=${SIGNING_KEY_FILE}"
  "--lakekeeper-url=${LAKEKEEPER_URL}"
  "--openfga-url=${OPENFGA_URL}"
)

# ─── Warehouse-type-specific flag arrays ─────────────────────────────────────
# Exactly one of these is populated based on $WAREHOUSE_TYPE; Steps 1 and 4
# splice in $WAREHOUSE_FLAGS which is just a pointer to whichever is set.
S3_FLAGS=()
GCS_FLAGS=()
if [[ "$WAREHOUSE_TYPE" == "s3" ]]; then
  S3_FLAGS=(
    "--type=s3"
    "--bucket=${S3_BUCKET}"
    "--s3-region=${S3_REGION}"
    "--s3-endpoint=${S3_ENDPOINT}"
    "--s3-access-key=${S3_ACCESS_KEY}"
    "--s3-secret-key=${S3_SECRET_KEY}"
  )
  if [[ "$PATH_STYLE" == "true" ]]; then
    S3_FLAGS+=("--path-style")
  fi
  if [[ "$STS_ENABLED" == "true" ]]; then
    S3_FLAGS+=("--sts-enabled")
  fi
elif [[ "$WAREHOUSE_TYPE" == "gcs" ]]; then
  GCS_FLAGS=(
    "--type=gcs"
    "--gcs-bucket=${GCS_BUCKET}"
    "--gcs-credential-type=${GCS_CREDENTIAL_TYPE}"
  )
  if [[ -n "$GCS_KEY_PREFIX" ]]; then
    GCS_FLAGS+=("--gcs-key-prefix=${GCS_KEY_PREFIX}")
  fi
  if [[ "$GCS_CREDENTIAL_TYPE" == "service-account-key" ]]; then
    GCS_FLAGS+=("--gcs-sa-key-file=${GCS_SA_KEY_FILE}")
  fi
  if [[ "$STS_ENABLED" == "true" ]]; then
    GCS_FLAGS+=("--sts-enabled")
  fi
fi

# ═════════════════════════════════════════════════════════════════════════════
# Step 1: lakekeeper-bootstrap
# Creates the lakekeeper warehouse + writes the server-admin FGA tuple.
# Flags (actual flag names from adminLakekeeperBootstrap):
#   --lakekeeper-url, --warehouse-name, --type, --bucket, --s3-region,
#   --s3-endpoint, --path-style, --sts-enabled, --s3-access-key,
#   --s3-secret-key, --signing-key-file, --key-id, --audience
# OPENFGA_URL + OPENFGA_API_KEY + OPENFGA_STORE_NAME are consumed from env
# by adminLakekeeperBootstrap (reads os.Getenv — not flags).
# In exec mode, these are already in the Pod's environment.
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo ">> Step 1: lakekeeper-bootstrap"
run_admin lakekeeper-bootstrap \
  "${LK_FLAGS[@]}" \
  "--warehouse-name=${WAREHOUSE_NAME}" \
  "${S3_FLAGS[@]}" "${GCS_FLAGS[@]}"

# ═════════════════════════════════════════════════════════════════════════════
# Step 2: create-user
# Flags: --email, --password
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo ">> Step 2: create-user (${ADMIN_EMAIL})"
run_admin create-user \
  "--email=${ADMIN_EMAIL}" \
  "--password=${ADMIN_PASSWORD}"

# ═════════════════════════════════════════════════════════════════════════════
# Step 3: create-project
# Flags: --name, --creator-email (+ provisioning flags for lakekeeper/openfga)
# Resolve creator-email from the admin email (the user we just created).
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo ">> Step 3: create-project (${PROJECT_NAME})"
run_admin create-project \
  "${PROVISIONING_FLAGS[@]}" \
  "--name=${PROJECT_NAME}" \
  "--creator-email=${ADMIN_EMAIL}"

# ═════════════════════════════════════════════════════════════════════════════
# Step 4: attach-warehouse
# Associates the project with the warehouse created in Step 1.
# Flags: --project, --warehouse, --type, S3 flags (+ provisioning flags)
# Note: --warehouse in attach-warehouse maps to the warehouse name in
#       lakekeeper (adminAttachWarehouse flag: --warehouse).
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo ">> Step 4: attach-warehouse (${WAREHOUSE_NAME} → ${PROJECT_NAME})"
run_admin attach-warehouse \
  "${PROVISIONING_FLAGS[@]}" \
  "--project=${PROJECT_NAME}" \
  "--warehouse=${WAREHOUSE_NAME}" \
  "${S3_FLAGS[@]}" "${GCS_FLAGS[@]}"

# ═════════════════════════════════════════════════════════════════════════════
# Step 5: grant
# Grants admin role to the admin user on the project.
# Flags: --user (email), --project, --role (+ provisioning flags)
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo ">> Step 5: grant admin on ${PROJECT_NAME} to ${ADMIN_EMAIL}"
run_admin grant \
  "${PROVISIONING_FLAGS[@]}" \
  "--user=${ADMIN_EMAIL}" \
  "--project=${PROJECT_NAME}" \
  "--role=admin"

echo ""
echo "=== register.sh complete ==="
echo "  Login at: http://localhost:30081/ui/login"
echo "  Email:    ${ADMIN_EMAIL}"
echo "  Password: ${ADMIN_PASSWORD}"
