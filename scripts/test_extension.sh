#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

cd "$REPO_ROOT"

node --check lattice-ext/background.js
node --check lattice-ext/content.js
node --check lattice-ext/setup.js
node --test lattice-ext/tests/*.mjs
