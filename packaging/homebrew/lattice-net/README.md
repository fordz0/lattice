# lattice-net Homebrew staging

This directory is a staging copy of the files you would publish in a personal
Homebrew tap for `lattice-net`.

It is not the tap repo itself. The intended flow is:

1. Push a `lattice-v*` tag.
2. Let the GitHub release workflow publish macOS release tarballs and `.sha256`
   files.
3. Copy the formula template into your tap repo and fill in the version and
   checksums from that release.

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
cp /path/to/lattice/packaging/homebrew/lattice-net/Formula/lattice-net.rb.template \
  Formula/lattice-net.rb
```

Then replace the placeholder version and sha256 values with the actual release
values from the `lattice-v*` GitHub release.

Later improvements:

1. Add `launchd` integration for `lattice up/down` on macOS.
2. Publish a stable `lattice-net` formula from release tarballs.
3. Add a `-head` formula only if you actually need a bleeding-edge tap.
