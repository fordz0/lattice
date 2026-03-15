#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ] || [ "$#" -gt 4 ]; then
  echo "usage: $0 <chrome-zip> <version> [output-dir] [key-path]" >&2
  exit 1
fi

ZIP_PATH="$1"
VERSION="$2"
OUT_DIR="${3:-release-assets}"
KEY_PATH="${4:-}"

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

EXT_DIR="$TMP_DIR/ext"
mkdir -p "$EXT_DIR" "$OUT_DIR"
unzip -q "$ZIP_PATH" -d "$EXT_DIR"

if [ -z "$KEY_PATH" ]; then
  KEY_PATH="$TMP_DIR/lattice-extension-chrome.pem"
fi

npx --yes crx3 \
  -p "$KEY_PATH" \
  -o "$OUT_DIR/lattice-extension-chrome-${VERSION}.crx" \
  -- "$EXT_DIR"

echo "$OUT_DIR/lattice-extension-chrome-${VERSION}.crx"
