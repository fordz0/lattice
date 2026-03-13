#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 /path/to/homebrew-lattice" >&2
  exit 1
fi

target_dir="$1"
mkdir -p "$target_dir/Formula"

cat >"$target_dir/README.md" <<'EOF'
# homebrew-lattice

Homebrew tap for Lattice.

This repo is intended to be updated by the main Lattice release workflow.
Each `lattice-v*` release can publish an updated `Formula/lattice-net.rb`
here when the following are configured in the main repo:

- `HOMEBREW_TAP_REPO`
- `PACKAGING_PUSH_TOKEN`

You can also update the formula manually by copying the generated
`lattice-net.rb` asset from a GitHub release.

Install with:

```sh
brew tap fordz0/lattice
brew install lattice-net
```
EOF

touch "$target_dir/Formula/.gitkeep"

echo "Bootstrapped Homebrew tap skeleton at $target_dir"
