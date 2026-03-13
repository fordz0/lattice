#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 /path/to/lattice-packages" >&2
  exit 1
fi

target_dir="$1"
mkdir -p "$target_dir"

cat >"$target_dir/README.md" <<'EOF'
# lattice-packages

Static package hosting repo for Lattice.

The intended layout is:

- default branch: this README and repo-level docs
- `gh-pages` branch: generated APT repository contents under `apt/`

The main Lattice release workflow can publish the generated APT snapshot here
automatically when these are configured in the main repo:

- `APT_REPO_TARGET`
- optional `APT_REPO_BRANCH`
- optional `APT_REPO_SUBDIR`
- `PACKAGING_PUSH_TOKEN`

Once published, the repo can be served via GitHub Pages or another static host.
EOF

cat >"$target_dir/.gitignore" <<'EOF'
.DS_Store
Thumbs.db
EOF

echo "Bootstrapped APT repo skeleton at $target_dir"
