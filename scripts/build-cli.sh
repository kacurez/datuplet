#!/bin/sh
# Usage: build-cli.sh <version-without-v> <outdir>
set -eu

[ $# -eq 2 ] || { echo "usage: $0 <version-without-v> <outdir>" >&2; exit 2; }
version=$1
outdir=$2

root=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$outdir"
outdir=$(cd "$outdir" && pwd)

pkg=github.com/datuplet/datuplet/pkg/buildinfo
commit=$(git -C "$root" rev-parse --short HEAD 2>/dev/null || echo none)
date=$(date -u +%Y-%m-%dT%H:%M:%SZ)
ldflags="-s -w -X $pkg.Version=$version -X $pkg.Commit=$commit -X $pkg.Date=$date"

stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT

for os in darwin linux; do
  for arch in amd64 arm64; do
    rm -f "$stage/datuplet"
    (cd "$root" && CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath -ldflags "$ldflags" -o "$stage/datuplet" ./cmd/datuplet)
    chmod 0755 "$stage/datuplet"
    tar czf "$outdir/datuplet_${version}_${os}_${arch}.tar.gz" -C "$stage" datuplet
  done
done

cd "$outdir"
: > checksums.txt
for f in datuplet_"${version}"_*.tar.gz; do
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$f" >> checksums.txt
  else
    shasum -a 256 "$f" >> checksums.txt
  fi
done
