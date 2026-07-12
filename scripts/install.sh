#!/usr/bin/env bash
# scripts/install.sh — RFC 024 W1: the single tested install entrypoint.
#
# 5-phase install: datuplet-operators → datuplet-infra → datuplet-app →
# datuplet-lakekeeper → register.sh. Idempotent: helm upgrade --install
# throughout; register.sh subcommands are idempotent by design.
#
# From a repo checkout (default):   scripts/install.sh --namespace datuplet
# From the published helm repo:     scripts/install.sh --from-repo --version v0.8.0
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
HELM_REPO_URL="${DATUPLET_HELM_REPO:-https://kacurez.github.io/datuplet}"
CHARTS="datuplet-operators datuplet-infra datuplet-app datuplet-lakekeeper"

NAMESPACE=datuplet
MODE=from-source
VERSION=""
REGISTER_MODE=exec
SKIP_REGISTER=false
PREFLIGHT_ONLY=false
DRY_RUN=false
COMMON_VALUES=() OPERATORS_VALUES=() INFRA_VALUES=() APP_VALUES=() LAKEKEEPER_VALUES=()
REGISTER_ARGS=()

usage() { sed -n '3,9p' "$0"; cat <<'EOF'

Options:
  --namespace NS          Target namespace (default: datuplet)
  --from-source           Install ./charts from this checkout (default)
  --from-repo             Install published charts from the helm repo
  --version X.Y.Z|vX.Y.Z  Chart version (required with --from-repo)
  -f FILE                 Values file applied to every chart (repeatable)
  -f-operators FILE       Values for datuplet-operators only (repeatable)
  -f-infra FILE           Values for datuplet-infra only (repeatable)
  -f-app FILE             Values for datuplet-app only (repeatable)
  -f-lakekeeper FILE      Values for datuplet-lakekeeper only (repeatable)
  --register-mode M       exec|job — forwarded to register.sh --mode (default: exec)
  --skip-register         Stop after the four helm installs
  --preflight-only        Run preflight checks, then exit 0
  --dry-run               Print every mutating command instead of executing
  -- ARGS...              Everything after -- is forwarded to register.sh
EOF
}

die() { echo "ERROR: $*" >&2; exit 1; }
run() { if $DRY_RUN; then echo "+ $*"; else "$@"; fi; }

while [ $# -gt 0 ]; do
  case "$1" in
    --namespace)     NAMESPACE=$2; shift 2 ;;
    --from-source)   MODE=from-source; shift ;;
    --from-repo)     MODE=from-repo; shift ;;
    --version)       VERSION=${2#v}; shift 2 ;;
    -f)              COMMON_VALUES+=("$2"); shift 2 ;;
    -f-operators)    OPERATORS_VALUES+=("$2"); shift 2 ;;
    -f-infra)        INFRA_VALUES+=("$2"); shift 2 ;;
    -f-app)          APP_VALUES+=("$2"); shift 2 ;;
    -f-lakekeeper)   LAKEKEEPER_VALUES+=("$2"); shift 2 ;;
    --register-mode) REGISTER_MODE=$2; shift 2 ;;
    --skip-register) SKIP_REGISTER=true; shift ;;
    --preflight-only) PREFLIGHT_ONLY=true; shift ;;
    --dry-run)       DRY_RUN=true; shift ;;
    -h|--help)       usage; exit 0 ;;
    --)              shift; REGISTER_ARGS=("$@"); break ;;
    *)               usage >&2; die "unknown flag: $1" ;;
  esac
done

preflight() {
  command -v kubectl >/dev/null 2>&1 || die "kubectl not found on PATH"
  command -v helm    >/dev/null 2>&1 || die "helm not found on PATH"
  local hv; hv=$(helm version --template '{{.Version}}')
  case "$hv" in
    v3.1[4-9].*|v3.[2-9][0-9].*|v[4-9].*) : ;;
    *) die "helm >= 3.14 required (found $hv)" ;;
  esac
  kubectl version >/dev/null 2>&1 || die "Kubernetes cluster unreachable (kubectl version failed)"
  local minor
  minor=$(kubectl version -o json 2>/dev/null \
    | sed -n 's/.*"minor": *"\([0-9]*\)[^"]*".*/\1/p' | tail -1)
  [ "${minor:-0}" -ge 28 ] || die "Kubernetes >= 1.28 required (server minor: ${minor:-unknown})"
  kubectl get storageclass -o name 2>/dev/null | grep -q . \
    || die "no StorageClass in the cluster (CNPG Postgres + MinIO need PVCs)"
  kubectl get storageclass -o jsonpath='{range .items[*]}{.metadata.annotations.storageclass\.kubernetes\.io/is-default-class}{"\n"}{end}' 2>/dev/null | grep -qx true \
    || echo "WARNING: no default StorageClass; CNPG Postgres + MinIO PVCs will stay Pending unless you set storageClass via -f values" >&2
  if [ "$MODE" = from-repo ]; then
    [ -n "$VERSION" ] || die "--from-repo requires --version"
    curl -fsS "$HELM_REPO_URL/index.yaml" >/dev/null \
      || die "helm repo unreachable: $HELM_REPO_URL"
  else
    run helm repo add cloudnative-pg https://cloudnative-pg.github.io/charts
    run helm repo add openfga https://openfga.github.io/helm-charts
    run helm repo add minio https://charts.min.io/
    run helm repo add lakekeeper https://lakekeeper.github.io/lakekeeper-charts
    for c in $CHARTS; do
      run helm dependency build "$REPO_ROOT/charts/$c" \
        || die "helm dependency build failed for $c (Chart.lock drift?)"
    done
  fi
  local st
  for c in $CHARTS; do
    st=$(helm status -n "$NAMESPACE" "$c" -o json 2>/dev/null \
      | sed -n 's/.*"status":"\([^"]*\)".*/\1/p' | head -1) || true
    case "${st:-}" in
      pending-*|failed|uninstalling)
        die "release $c is '$st' — resolve first (helm rollback/uninstall $c -n $NAMESPACE)" ;;
    esac
  done
  echo "preflight OK (namespace=$NAMESPACE mode=$MODE${VERSION:+ version=$VERSION})"
}

chart_values() {  # $1 = chart name → echoes -f args (common first, then per-chart)
  local f
  for f in ${COMMON_VALUES[@]+"${COMMON_VALUES[@]}"}; do printf -- '-f\n%s\n' "$f"; done
  case "$1" in
    datuplet-operators)  for f in ${OPERATORS_VALUES[@]+"${OPERATORS_VALUES[@]}"};  do printf -- '-f\n%s\n' "$f"; done ;;
    datuplet-infra)      for f in ${INFRA_VALUES[@]+"${INFRA_VALUES[@]}"};          do printf -- '-f\n%s\n' "$f"; done ;;
    datuplet-app)        for f in ${APP_VALUES[@]+"${APP_VALUES[@]}"};              do printf -- '-f\n%s\n' "$f"; done ;;
    datuplet-lakekeeper) for f in ${LAKEKEEPER_VALUES[@]+"${LAKEKEEPER_VALUES[@]}"};do printf -- '-f\n%s\n' "$f"; done ;;
  esac
}

install_chart() {  # $1 = chart name; $2.. = extra helm flags
  local chart=$1; shift
  local src="$REPO_ROOT/charts/$chart" vflags=()
  if [ "$MODE" = from-repo ]; then src="datuplet/$chart"; vflags=(--version "$VERSION"); fi
  local vals=()
  while IFS= read -r line; do [ -n "$line" ] && vals+=("$line"); done < <(chart_values "$chart")
  run helm upgrade --install "$chart" "$src" -n "$NAMESPACE" \
    ${vflags[@]+"${vflags[@]}"} ${vals[@]+"${vals[@]}"} "$@"
}

preflight
$PREFLIGHT_ONLY && exit 0

if [ "$MODE" = from-repo ]; then
  run helm repo add datuplet "$HELM_REPO_URL" --force-update
  run helm repo update datuplet
fi

# apply_crds <chart> — helm applies crds/ on FIRST install but skips them on
# every subsequent upgrade; install.sh uses `helm upgrade --install`, so on a
# reused cluster (local re-deploy, or a CRD schema bump) the CRDs would go
# stale. Apply them explicitly first — mirrors the current Makefile's
# `kubectl apply -f charts/datuplet-app/crds/` step (which the Makefile
# wrappers dropped when they moved to install.sh — F1 of the 2026-07-11 codex
# review). Same source logic as install_chart.
apply_crds() {
  local chart=$1
  if [ "$MODE" = from-source ]; then
    run kubectl apply --server-side --force-conflicts \
      --field-manager=datuplet-install -f "$REPO_ROOT/charts/$chart/crds/"
  elif $DRY_RUN; then
    echo "+ helm show crds datuplet/$chart --version $VERSION | kubectl apply --server-side --force-conflicts --field-manager=datuplet-install -f -"
  else
    helm show crds "datuplet/$chart" --version "$VERSION" \
      | kubectl apply --server-side --force-conflicts --field-manager=datuplet-install -f -
  fi
}

# RFC 015 5-phase install; strict order (see docs/install.md phase table).
# CRD-bearing charts (operators = CNPG CRDs, app = Pipeline/PipelineRun/
# ComponentDefinition) get an explicit CRD apply before their helm step.
apply_crds datuplet-operators
install_chart datuplet-operators  --create-namespace --wait --timeout 5m
install_chart datuplet-infra      --wait --wait-for-jobs --timeout 10m
apply_crds datuplet-app
install_chart datuplet-app        --wait --wait-for-jobs --timeout 10m
install_chart datuplet-lakekeeper --wait --wait-for-jobs --timeout 10m

if ! $SKIP_REGISTER; then
  run "$SCRIPT_DIR/register.sh" --namespace "$NAMESPACE" --mode "$REGISTER_MODE" \
    ${REGISTER_ARGS[@]+"${REGISTER_ARGS[@]}"}
fi
echo "OK: datuplet installed in namespace '$NAMESPACE'"
