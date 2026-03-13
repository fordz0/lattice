# lattice-net Homebrew staging

This directory is the upstream staging area for a personal Homebrew tap for
`lattice-net`.

It is not the tap repo itself. The release workflow now generates a ready-to-use
`lattice-net.rb` formula asset from the macOS release tarballs and checksums.

The intended flow is:

1. Push a `lattice-v*` tag.
2. Let the GitHub release workflow publish macOS tarballs, checksum files, and
   a rendered `lattice-net.rb`.
3. Copy that generated formula into your tap repo.

If you want the release workflow to update the tap repo automatically, configure:

- repository variable `HOMEBREW_TAP_REPO`
  Example: `fordz0/homebrew-lattice`
- repository secret `PACKAGING_PUSH_TOKEN`
  A GitHub token with contents write access to the tap repo

Suggested tap layout:

```text
homebrew-lattice/
  Formula/
    lattice-net.rb
```

Suggested tap flow:

```sh
git clone git@github.com:fordz0/homebrew-lattice.git
cd homebrew-lattice
mkdir -p Formula
curl -LO https://github.com/fordz0/lattice/releases/download/lattice-vX.Y.Z/lattice-net.rb
mv lattice-net.rb Formula/lattice-net.rb
```

If you need to render the formula locally instead of using the release asset:

```sh
python3 packaging/homebrew/lattice-net/render_formula.py \
  --version X.Y.Z \
  --macos-x86-sha256 lattice-macos-x86_64.tar.gz.sha256 \
  --macos-arm64-sha256 lattice-macos-aarch64.tar.gz.sha256 \
  --output Formula/lattice-net.rb
```

Later improvements:

1. Publish a dedicated `homebrew-lattice` tap repo.
2. Add a `-head` formula only if you actually need a bleeding-edge tap.
