#!/bin/sh
# Install the datuplet CLI from a GitHub Release.
# Env: DATUPLET_VERSION (tag, e.g. v0.14.0; default latest), DATUPLET_INSTALL_DIR.
set -eu

repo=kacurez/datuplet
base=${DATUPLET_RELEASE_BASE:-https://github.com/$repo/releases/download}

die() { echo "install-cli: $*" >&2; exit 1; }

case $(uname -s) in
  Darwin) os=darwin ;;
  Linux) os=linux ;;
  *) die "unsupported OS: $(uname -s) (supported: darwin, linux)" ;;
esac
case $(uname -m) in
  x86_64|amd64) arch=amd64 ;;
  arm64|aarch64) arch=arm64 ;;
  *) die "unsupported architecture: $(uname -m) (supported: amd64, arm64)" ;;
esac

tag=${DATUPLET_VERSION:-}
if [ -z "$tag" ]; then
  tag=$(curl -fsSL "https://api.github.com/repos/$repo/releases/latest" \
    | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1) \
    || die "could not query the latest release"
  [ -n "$tag" ] || die "could not determine the latest release tag; set DATUPLET_VERSION"
fi
version=${tag#v}

if [ -n "${DATUPLET_INSTALL_DIR:-}" ]; then
  dir=$DATUPLET_INSTALL_DIR
  mkdir -p "$dir" 2>/dev/null || die "cannot create $dir"
  [ -w "$dir" ] || die "$dir is not writable"
else
  sysdir=${DATUPLET_SYSTEM_BIN_DIR:-/usr/local/bin}
  if [ -d "$sysdir" ] && [ -w "$sysdir" ]; then
    dir=$sysdir
  else
    dir=$HOME/.local/bin
    mkdir -p "$dir" || die "cannot create $dir"
  fi
fi

tmp=$(mktemp -d)
target_tmp=""
trap 'rm -rf "$tmp"; [ -z "$target_tmp" ] || rm -f "$target_tmp"' EXIT

file=datuplet_${version}_${os}_${arch}.tar.gz
curl -fsSL -o "$tmp/$file" "$base/$tag/$file" || die "download failed: $base/$tag/$file"
curl -fsSL -o "$tmp/checksums.txt" "$base/$tag/checksums.txt" || die "download failed: $base/$tag/checksums.txt"

grep -F "  $file" "$tmp/checksums.txt" > "$tmp/check.txt" || true
[ "$(grep -c . "$tmp/check.txt")" = 1 ] || die "no checksum entry for $file in checksums.txt"
if command -v sha256sum >/dev/null 2>&1; then
  (cd "$tmp" && sha256sum -c check.txt >/dev/null 2>&1) || die "checksum mismatch for $file"
elif command -v shasum >/dev/null 2>&1; then
  (cd "$tmp" && shasum -a 256 -c check.txt >/dev/null 2>&1) || die "checksum mismatch for $file"
else
  die "neither sha256sum nor shasum found"
fi

mkdir "$tmp/x"
tar xzf "$tmp/$file" -C "$tmp/x" datuplet || die "could not extract $file"

target_tmp=$(mktemp "$dir/.datuplet.XXXXXX") || die "cannot write to $dir"
install -m 0755 "$tmp/x/datuplet" "$target_tmp" || die "install failed"
mv -f "$target_tmp" "$dir/datuplet" || die "install failed"
target_tmp=""

echo "installed datuplet $version to $dir/datuplet"
case ":$PATH:" in
  *":$dir:"*) ;;
  *) echo "add it to your PATH:  export PATH=\"$dir:\$PATH\"" ;;
esac
