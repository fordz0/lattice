# lattice-net-bin AUR skeleton

This directory is a staging copy of the files you would publish to the AUR for
`lattice-net-bin`.

It is not the AUR repo itself. When you're ready, clone the AUR package repo and
copy these files across:

```sh
git clone ssh://aur@aur.archlinux.org/lattice-net-bin.git
cd lattice-net-bin
cp /path/to/lattice/packaging/aur/lattice-net-bin/PKGBUILD .
cp /path/to/lattice/packaging/aur/lattice-net-bin/.SRCINFO .
cp /path/to/lattice/packaging/aur/lattice-net-bin/lattice-daemon.service .
cp /path/to/lattice/packaging/aur/lattice-net-bin/lattice-net-bin.install .
```

Then test locally:

```sh
makepkg -si
systemctl --user daemon-reload
systemctl --user enable --now lattice-daemon
lattice up
lattice status
```

Suggested release flow:

1. Keep `lattice-net-git` for bleeding-edge users.
2. Use `lattice-net-bin` for fast installs from GitHub Releases.
3. Let the OS package manager update `lattice` and `lattice-daemon`.
4. Let `lattice update` handle Lattice apps installed from the registry.

Like the `-git` package, the install hook reloads unit definitions and restarts
`lattice-daemon` automatically if it was already active, covering both the
system-service path and active user services.
