# lattice-net-git AUR skeleton

This directory is a staging copy of the files you would publish to the AUR for
`lattice-net-git`.

It is not the AUR repo itself. When you're ready, clone the AUR package repo and
copy these files across:

```sh
git clone ssh://aur@aur.archlinux.org/lattice-net-git.git
cd lattice-net-git
cp /path/to/lattice/packaging/aur/lattice-net-git/PKGBUILD .
cp /path/to/lattice/packaging/aur/lattice-net-git/.SRCINFO .
cp /path/to/lattice/packaging/aur/lattice-net-git/lattice-daemon.service .
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

1. Keep `lattice-net-git` as the first package.
2. Add stable `lattice-net` later once release tarballs are published.
3. Let the OS package manager update `lattice` and `lattice-daemon`.
4. Let `lattice update` handle Lattice apps installed from the registry.
