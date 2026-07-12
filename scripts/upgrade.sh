#!/usr/bin/env bash
# scripts/upgrade.sh — RFC 024 W1: phase-aware upgrade with explicit CRD apply.
#
# helm NEVER upgrades CRDs shipped in a chart's crds/ directory; this script
# applies them (server-side) before upgrading the corresponding release.
# Forward-only by design: no --atomic, no helm rollback (hook Jobs, CRD
# applies and DB migrations sit outside helm's rollback scope). Recovery
# from a mid-flight failure: fix the cause, re-run the same command.
#
#   scripts/upgrade.sh                             # common case: app phase, from source
#   scripts/upgrade.sh --phase all --from-repo --version v0.9.0
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
HELM_REPO_URL="${DATUPLET_HELM_REPO:-https://kacurez.github.io/datuplet}"
FIELD_MANAGER=datuplet-upgrade

NAMESPACE=datuplet
PHASE=app
MODE=from-source
VERSION=""
DRY_RUN=false
COMMON_VALUES=() OPERATORS_VALUES=() INFRA_VALUES=() APP_VALUES=() LAKEKEEPER_VALUES=()

usage() { sed -n '3,11p' "$0"; cat <<'EOF'

Options:
  --namespace NS       Target namespace (default: datuplet)
  --phase P            operators|infra|app|lakekeeper|all (default: app)
  --from-source        Upgrade from ./charts in this checkout (default)
  --from-repo          Upgrade from the published helm repo
  --version X.Y.Z      Chart version (required with --from-repo; leading v stripped)
  -f FILE              Values file applied to every upgraded chart (repeatable)
  -f-operators FILE    Values for datuplet-operators only (repeatable)
  -f-infra FILE        Values for datuplet-infra only (repeatable)
  -f-app FILE          Values for datuplet-app only (repeatable)
  -f-lakekeeper FILE   Values for datuplet-lakekeeper only (repeatable)
  --dry-run            Print every mutating command instead of executing
EOF
}

die() { echo "ERROR: $*" >&2; exit 1; }
run() { if $DRY_RUN; then echo "+ $*"; else "$@"; fi; }

while [ $# -gt 0 ]; do
  case "$1" in
    --namespace)   NAMESPACE=$2; shift 2 ;;
    --phase)       PHASE=$2; shift 2 ;;
    --from-source) MODE=from-source; shift ;;
    --from-repo)   MODE=from-repo; shift ;;
    --version)     VERSION=${2#v}; shift 2 ;;
    -f)            COMMON_VALUES+=("$2"); shift 2 ;;
    -f-operators)  OPERATORS_VALUES+=("$2"); shift 2 ;;
    -f-infra)      INFRA_VALUES+=("$2"); shift 2 ;;
    -f-app)        APP_VALUES+=("$2"); shift 2 ;;
    -f-lakekeeper) LAKEKEEPER_VALUES+=("$2"); shift 2 ;;
    --dry-run)     DRY_RUN=true; shift ;;
    -h|--help)     usage; exit 0 ;;
    *)             usage >&2; die "unknown flag: $1" ;;
  esac
done

case "$PHASE" in operators|infra|app|lakekeeper|all) : ;; *) die "invalid --phase: $PHASE" ;; esac
if [ "$MODE" = from-repo ]; then
  [ -n "$VERSION" ] || die "--from-repo requires --version"
  run helm repo add datuplet "$HELM_REPO_URL" --force-update
  run helm repo update datuplet
fi

apply_crds() {  # $1 = chart that ships a crds/ dir
  local chart=$1
  echo "--- applying CRDs for $chart (helm skips crds/ on upgrade)"
  if [ "$MODE" = from-source ]; then
    run kubectl apply --server-side --force-conflicts \
      --field-manager="$FIELD_MANAGER" -f "$REPO_ROOT/charts/$chart/crds/"
  elif $DRY_RUN; then
    echo "+ helm show crds datuplet/$chart --version $VERSION | kubectl apply --server-side --force-conflicts --field-manager=$FIELD_MANAGER -f -"
  else
    helm show crds "datuplet/$chart" --version "$VERSION" \
      | kubectl apply --server-side --force-conflicts \
          --field-manager="$FIELD_MANAGER" -f -
  fi
}

chart_values() {  # $1 = chart name → echoes -f args, one per line
  local f
  for f in ${COMMON_VALUES[@]+"${COMMON_VALUES[@]}"}; do printf -- '-f\n%s\n' "$f"; done
  case "$1" in
    datuplet-operators)  for f in ${OPERATORS_VALUES[@]+"${OPERATORS_VALUES[@]}"};  do printf -- '-f\n%s\n' "$f"; done ;;
    datuplet-infra)      for f in ${INFRA_VALUES[@]+"${INFRA_VALUES[@]}"};          do printf -- '-f\n%s\n' "$f"; done ;;
    datuplet-app)        for f in ${APP_VALUES[@]+"${APP_VALUES[@]}"};              do printf -- '-f\n%s\n' "$f"; done ;;
    datuplet-lakekeeper) for f in ${LAKEKEEPER_VALUES[@]+"${LAKEKEEPER_VALUES[@]}"};do printf -- '-f\n%s\n' "$f"; done ;;
  esac
}

upgrade_release() {  # $1 = chart name; $2.. = extra helm flags
  local chart=$1; shift
  local src="$REPO_ROOT/charts/$chart" vflags=()
  if [ "$MODE" = from-repo ]; then
    src="datuplet/$chart"; vflags=(--version "$VERSION")
  else
    run helm dependency build "$REPO_ROOT/charts/$chart"
  fi
  local vals=()
  while IFS= read -r line; do [ -n "$line" ] && vals+=("$line"); done < <(chart_values "$chart")
  run helm upgrade "$chart" "$src" -n "$NAMESPACE" \
    ${vflags[@]+"${vflags[@]}"} ${vals[@]+"${vals[@]}"} "$@"
}

fga_sync_warn() {
  # Belt (the CI check in verify-versions.sh is the suspenders): after an
  # app-only upgrade, warn when the deployed lakekeeper FGA pin differs.
  command -v jq >/dev/null 2>&1 || { echo "note: jq not found — skipping FGA cross-chart check"; return 0; }
  local a b
  a=$(helm get values -n "$NAMESPACE" datuplet-app -a -o json 2>/dev/null | jq -r '.fgaModel.version // empty' || true)
  b=$(helm get values -n "$NAMESPACE" datuplet-lakekeeper -a -o json 2>/dev/null | jq -r '.platform.fgaModelVersion // empty' || true)
  if [ -n "$a" ] && [ -n "$b" ] && [ "$a" != "$b" ]; then
    echo "WARNING: fgaModel.version=$a (datuplet-app) != platform.fgaModelVersion=$b (datuplet-lakekeeper)." >&2
    echo "         Upgrade datuplet-lakekeeper too — see docs/fga-model-upgrades.md." >&2
  fi
}

do_phase() {
  case "$1" in
    operators)  apply_crds datuplet-operators
                upgrade_release datuplet-operators --wait --timeout 5m ;;
    infra)      upgrade_release datuplet-infra --wait --wait-for-jobs --timeout 10m ;;
    app)        apply_crds datuplet-app
                upgrade_release datuplet-app --wait --wait-for-jobs --timeout 10m
                fga_sync_warn ;;
    lakekeeper) upgrade_release datuplet-lakekeeper --wait --wait-for-jobs --timeout 10m ;;
  esac
}

if [ "$PHASE" = all ]; then
  for p in operators infra app lakekeeper; do do_phase "$p"; done
else
  do_phase "$PHASE"
fi
echo "OK: upgrade complete (phase=$PHASE namespace=$NAMESPACE)"
