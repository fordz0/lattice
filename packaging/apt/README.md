# Debian and Ubuntu packaging

This directory is the upstream staging area for Debian-style binary packaging and
APT repository metadata.

It does two practical things:

1. Builds release `.deb` packages for `amd64` and `arm64`
2. Builds an unsigned APT repository snapshot from those packages

The release workflow uses these scripts to attach:

- `lattice_<version>-1_amd64.deb`
- `lattice_<version>-1_arm64.deb`
- `lattice-apt-repo.tar.gz`

to every `lattice-v*` GitHub release.

## What this is not

This repo does not publish a live signed APT repository by itself.

For a real public APT repo, you still want a dedicated host or repo, plus GPG
signing for `Release` metadata. The generated `lattice-apt-repo.tar.gz` is the
snapshot you would publish there.

If you want the release workflow to push that snapshot automatically, configure:

- repository variable `APT_REPO_TARGET`
  Example: `fordz0/lattice-packages`
- optional repository variable `APT_REPO_BRANCH`
  Default: `gh-pages`
- optional repository variable `APT_REPO_SUBDIR`
  Default: `apt`
- repository secret `PACKAGING_PUSH_TOKEN`
  A GitHub token with contents write access to the target repo

## Release assets generated

- `.deb` packages install:
  - `/usr/bin/lattice`
  - `/usr/bin/lattice-daemon`
  - `/usr/lib/systemd/user/lattice-daemon.service`
  - docs under `/usr/share/doc/lattice/`
- the repo snapshot contains:
  - `pool/`
  - `dists/stable/main/binary-amd64/Packages`
  - `dists/stable/main/binary-arm64/Packages`
  - `dists/stable/Release`

## Manual usage

From the repo root, after building release tarballs:

```sh
python3 packaging/apt/build_repo.py \
  --version 0.1.4 \
  --amd64-tarball lattice-linux-x86_64.tar.gz \
  --arm64-tarball lattice-linux-aarch64.tar.gz \
  --output-dir dist/apt-release
```

That writes:

- `dist/apt-release/lattice_0.1.4-1_amd64.deb`
- `dist/apt-release/lattice_0.1.4-1_arm64.deb`
- `dist/apt-release/lattice-apt-repo.tar.gz`

## Suggested public repo layout later

```text
apt/
  dists/
  pool/
```

Then sign `dists/stable/Release` and publish:

- `InRelease`
- `Release`
- `Release.gpg`

in the usual Debian way.

There is also an example source-list file at:

- [lattice.sources.example](./lattice.sources.example)

Replace the placeholder URI with the host that serves your extracted
`lattice-apt-repo/` contents.
