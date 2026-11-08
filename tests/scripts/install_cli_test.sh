#!/bin/sh
set -eu

root=$(cd "$(dirname "$0")/../.." && pwd)
work=$(mktemp -d)
srv_pid=""
cleanup() {
  if [ -n "$srv_pid" ]; then
    kill "$srv_pid" 2>/dev/null || true
    wait "$srv_pid" 2>/dev/null || true
  fi
  chmod -R u+w "$work" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

fail() { echo "FAIL: $1" >&2; exit 1; }

version=0.0.0-test
tag=v$version
assets="$work/good/$tag"
sh "$root/scripts/build-cli.sh" "$version" "$assets" >/dev/null

port=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])')
(cd "$work" && exec python3 -m http.server "$port" --bind 127.0.0.1 >/dev/null 2>&1) &
srv_pid=$!
i=0
until curl -fsS "http://127.0.0.1:$port/good/$tag/checksums.txt" >/dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -lt 50 ] || fail "http server did not start"
  sleep 0.2
done
base="http://127.0.0.1:$port/good"

# Case A: explicit install dir
dir_a="$work/a/bin"
out=$(DATUPLET_RELEASE_BASE=$base DATUPLET_VERSION=$tag DATUPLET_INSTALL_DIR=$dir_a sh "$root/scripts/install-cli.sh") \
  || fail "case A: installer failed"
[ -x "$dir_a/datuplet" ] || fail "case A: binary not installed"
"$dir_a/datuplet" version | grep -q "client: $version " || fail "case A: unexpected version output: $("$dir_a/datuplet" version)"
echo "$out" | grep -q "installed datuplet $version to $dir_a/datuplet" || fail "case A: missing install message: $out"
[ "$(ls "$dir_a")" = "datuplet" ] || fail "case A: stray files in install dir"
echo "PASS: A explicit install dir"

# Case B: unwritable system dir falls back to $HOME/.local/bin
ro="$work/ro"
mkdir "$ro"
chmod 0555 "$ro"
home_b="$work/home"
mkdir "$home_b"
out=$(HOME=$home_b PATH=/usr/bin:/bin:/usr/sbin:/sbin DATUPLET_SYSTEM_BIN_DIR=$ro \
  DATUPLET_RELEASE_BASE=$base DATUPLET_VERSION=$tag sh "$root/scripts/install-cli.sh") \
  || fail "case B: installer failed"
[ -x "$home_b/.local/bin/datuplet" ] || fail "case B: binary not in \$HOME/.local/bin"
echo "$out" | grep -q 'export PATH' || fail "case B: missing PATH hint: $out"
[ -z "$(ls "$ro")" ] || fail "case B: wrote into read-only dir"
echo "PASS: B fallback to \$HOME/.local/bin with PATH hint"

# Case C: checksum mismatch
bad="$work/bad/$tag"
mkdir -p "$bad"
cp "$assets"/* "$bad/"
sed 's/^[0-9a-f]\{8\}/00000000/' "$assets/checksums.txt" > "$bad/checksums.txt"
dir_c="$work/c/bin"
if DATUPLET_RELEASE_BASE="http://127.0.0.1:$port/bad" DATUPLET_VERSION=$tag DATUPLET_INSTALL_DIR=$dir_c sh "$root/scripts/install-cli.sh" >/dev/null 2>&1; then
  fail "case C: installer must fail on checksum mismatch"
fi
[ ! -e "$dir_c/datuplet" ] || fail "case C: binary installed despite mismatch"
echo "PASS: C checksum mismatch fails and installs nothing"
