# Windows Installer Plan

Current state:

- GitHub Releases publish `lattice.exe` and `lattice-daemon.exe` in a zip archive
- `lattice up` can manage the daemon directly after extraction
- This is enough for early Windows users, but it is not a polished installer experience

Planned MSI work:

1. Install `lattice.exe` and `lattice-daemon.exe` into `Program Files`
2. Add an Add/Remove Programs entry
3. Register an optional per-user startup or service path for `lattice-daemon`
4. Preserve user data in `%LOCALAPPDATA%\Lattice`
5. Optionally add an uninstall action that keeps user data unless explicitly removed

Suggested implementation path:

- Start with WiX Toolset because it produces a conventional MSI
- Reuse the GitHub release zip artifacts as installer inputs
- Keep `lattice up/down` working even after MSI install so CLI behavior stays consistent across platforms

Open design questions:

- Per-user background process vs Windows Service for `lattice-daemon`
- Whether to install a Start Menu shortcut for setup/docs
- Whether the MSI should install the Firefox extension helper assets or only the core binaries
