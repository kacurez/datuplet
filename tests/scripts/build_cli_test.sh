#!/bin/sh
set -eu

root=$(cd "$(dirname "$0")/../.." && pwd)
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

fail() { echo "FAIL: $1" >&2; exit 1; }

version=0.0.0-test
sh "$root/scripts/build-cli.sh" "$version" "$out"

for platform in darwin_amd64 darwin_arm64 linux_amd64 linux_arm64; do
  f="$out/datuplet_${version}_${platform}.tar.gz"
  [ -f "$f" ] || fail "missing $f"
  [ "$(tar tzf "$f")" = "datuplet" ] || fail "$platform tarball must contain exactly datuplet"
  echo "PASS: $platform tarball contains exactly datuplet"
done

[ -f "$out/checksums.txt" ] || fail "missing checksums.txt"
[ "$(wc -l < "$out/checksums.txt" | tr -d ' ')" = 4 ] || fail "checksums.txt must list 4 files"
if command -v sha256sum >/dev/null 2>&1; then
  (cd "$out" && sha256sum -c checksums.txt >/dev/null) || fail "sha256sum -c failed"
else
  (cd "$out" && shasum -a 256 -c checksums.txt >/dev/null) || fail "shasum -c failed"
fi
echo "PASS: checksums.txt verifies"

mkdir "$out/x"
tar xzf "$out/datuplet_${version}_linux_amd64.tar.gz" -C "$out/x"
file "$out/x/datuplet" | grep -q 'statically linked' || fail "linux/amd64 binary is not statically linked: $(file "$out/x/datuplet")"
echo "PASS: linux/amd64 binary is statically linked"
