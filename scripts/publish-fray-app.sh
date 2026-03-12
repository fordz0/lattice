#!/usr/bin/env bash
set -euo pipefail

VERSION=${1:?usage: publish-fray-app.sh <version>}
BASE_URL="https://github.com/fordz0/lattice/releases/download/fray-v${VERSION}"
RPC_PORT=7780

if ! command -v lattice >/dev/null 2>&1; then
  echo "lattice binary not found on PATH"
  exit 1
fi

if ! lattice --rpc-port "$RPC_PORT" status >/dev/null 2>&1; then
  echo "lattice daemon is not reachable on port ${RPC_PORT}"
  exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
  sha256_file() {
    sha256sum "$1" | awk '{print $1}'
  }
else
  sha256_file() {
    shasum -a 256 "$1" | awk '{print $1}'
  }
fi

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

download_and_hash() {
  local filename=$1
  local path="$tmpdir/$filename"
  curl -fsSL "$BASE_URL/$filename" -o "$path"
  sha256_file "$path"
}

linux_x86_64_hash=$(download_and_hash "fray-linux-x86_64")
linux_aarch64_hash=$(download_and_hash "fray-linux-aarch64")
macos_x86_64_hash=$(download_and_hash "fray-macos-x86_64")
macos_aarch64_hash=$(download_and_hash "fray-macos-aarch64")

lattice --rpc-port "$RPC_PORT" publish-app fray \
  --version "$VERSION" \
  --description "Distributed threads for Lattice" \
  --linux-x86-64 "$BASE_URL/fray-linux-x86_64" \
  --linux-x86-64-sha256 "$linux_x86_64_hash" \
  --linux-aarch64 "$BASE_URL/fray-linux-aarch64" \
  --linux-aarch64-sha256 "$linux_aarch64_hash" \
  --macos-x86-64 "$BASE_URL/fray-macos-x86_64" \
  --macos-x86-64-sha256 "$macos_x86_64_hash" \
  --macos-aarch64 "$BASE_URL/fray-macos-aarch64" \
  --macos-aarch64-sha256 "$macos_aarch64_hash"

echo "published fray v${VERSION} to Lattice app registry"
