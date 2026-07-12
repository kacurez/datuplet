#!/usr/bin/env bash
# scripts/verify-versions.sh — RFC 024 W4: cross-file version-sync checks.
# Requires: helm, yq (mikefarah). Run from the repo root.
set -euo pipefail
cd "$(dirname "$0")/.."
command -v yq >/dev/null || { echo "FATAL: yq required"; exit 2; }
command -v helm >/dev/null || { echo "FATAL: helm required"; exit 2; }

fail=0
err() { echo "FAIL: $*" >&2; fail=1; }
ALLOW="${VERIFY_IMAGE_ALLOWLIST:-^$}"
CHARTS="datuplet-operators datuplet-infra datuplet-app datuplet-lakekeeper"

# helm dependency build (used below) needs each dependency repo locally
# registered — unlike `update` it won't auto-fetch "unmanaged" repos. Register
# them up front so this check is self-contained (CI + local). Idempotent.
helm repo add cloudnative-pg https://cloudnative-pg.github.io/charts >/dev/null 2>&1 || true
helm repo add openfga https://openfga.github.io/helm-charts >/dev/null 2>&1 || true
helm repo add minio https://charts.min.io/ >/dev/null 2>&1 || true
helm repo add lakekeeper https://lakekeeper.github.io/lakekeeper-charts >/dev/null 2>&1 || true

# 1. FGA model version must match across app + lakekeeper charts. Guard against
# a missing key: yq prints "null" for an absent path, and null==null would
# falsely pass — so require both non-empty and non-"null" first.
a=$(yq '.fgaModel.version' charts/datuplet-app/values.yaml)
b=$(yq '.platform.fgaModelVersion' charts/datuplet-lakekeeper/values.yaml)
{ [ -n "$a" ] && [ "$a" != "null" ]; } || err "datuplet-app values.yaml: fgaModel.version missing/null"
{ [ -n "$b" ] && [ "$b" != "null" ]; } || err "datuplet-lakekeeper values.yaml: platform.fgaModelVersion missing/null"
[ "$a" = "$b" ] || err "FGA model drift: datuplet-app fgaModel.version=$a != datuplet-lakekeeper platform.fgaModelVersion=$b (docs/fga-model-upgrades.md)"

# 2. The legacy raw-manifest tree stays dead. git grep = tracked source only
# (so gitignored SDD scratch + untracked local settings are excluded for free);
# the RFC design/plan docs under docs/superpowers and this script's own grep
# pattern are not references TO the tree, so exclude them.
if git grep -In 'utils/deploy' -- ':!docs/superpowers' ':!scripts/verify-versions.sh' >/dev/null 2>&1; then
  git grep -In 'utils/deploy' -- ':!docs/superpowers' ':!scripts/verify-versions.sh' >&2
  err "reference to deleted utils/deploy/ tree (single deploy source = charts/)"
fi

# 3. Rendered charts (default values) must not contain floating or dev refs for
# OUR images: a ghcr.io/kacurez/* image on :latest (should be the pinned
# appVersion/components.tag), or any datuplet/<name>:<tag> dev-repo ref (local
# override leaking into defaults). Scoped to our images on purpose — third-party
# helper images (curlimages/curl, bitnami/kubectl, alpine/k8s, …) manage their
# own tags and are out of RFC 024's control. Comment lines skipped; the
# datuplet/ arm requires a `<name>:` tag so paths like /etc/datuplet/fga or
# /var/run/secrets/datuplet/ don't false-match. VERIFY_IMAGE_ALLOWLIST exempts
# specific lines if ever needed.
for c in $CHARTS; do
  helm dependency build "charts/$c" >/dev/null
  rendered=$(helm template "$c" "charts/$c" --namespace datuplet 2>/dev/null)
  bad=$(printf '%s\n' "$rendered" \
    | grep -vE '^[[:space:]]*#' \
    | grep -nE '(ghcr\.io/kacurez/[a-z0-9-]+:latest([^A-Za-z0-9]|$)|[^A-Za-z0-9_.-]datuplet/[a-z0-9-]+:)' \
    | grep -Ev "$ALLOW" || true)
  [ -z "$bad" ] || { printf '%s\n' "$bad" >&2; err "$c renders a ghcr.io/kacurez/*:latest or datuplet/ dev image ref with default values"; }
done

# 4. Chart.lock COMMITTED for every dependency-bearing chart. Use git (not
# `-f`): check 3's `helm dependency build` above regenerates a missing lock on
# disk, so an on-disk presence check would pass even for a PR that deleted the
# committed lock. `git ls-files --error-unmatch` asserts it is tracked.
for c in $CHARTS; do
  if grep -q '^dependencies:' "charts/$c/Chart.yaml"; then
    git ls-files --error-unmatch "charts/$c/Chart.lock" >/dev/null 2>&1 \
      || err "charts/$c has dependencies but no committed Chart.lock (helm dependency build regenerates it locally — it must be committed)"
  fi
done

if [ "$fail" -ne 0 ]; then echo "verify-versions: FAILED"; exit 1; fi
echo "verify-versions: OK"
