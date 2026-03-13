# Windows Installer

This directory contains the first real MSI implementation for Lattice.

Current behavior:

- uses a standard Windows installer UI with an install-directory picker
- installs `lattice.exe` and `lattice-daemon.exe` into `Program Files\Lattice`
- adds the install directory to the system `PATH`
- adds Start Menu entries for:
  - `Lattice`
  - `Lattice README`
  - `Lattice Getting Started`
  - `Lattice Firefox Extension`
  - `Remove Lattice Data`
- includes `LICENSE` and `README.md`
- installs and starts the `lattice-daemon` Windows service automatically
- configures the service to run with:
  - `--service`
  - `--data-dir "%ProgramData%\Lattice"`
- configures the daemon service to start automatically on boot
- keeps runtime daemon management available in the CLI too:
  - `lattice up`
  - `lattice down`
  - `lattice service ...`
- supports `lattice service uninstall --purge-data` if you want the CLI to remove the daemon data directory as part of service removal
- stops and removes the Windows service on uninstall
- preserves `%ProgramData%\Lattice` on uninstall for now
- includes a `Remove Lattice Data` maintenance shortcut for wiping daemon data explicitly
- sets Windows Add/Remove Programs metadata and support links

What it does not do yet:

- bundle the Firefox extension
- offer a “remove user data” checkbox inside the MSI UI

Build locally with WiX v4:

```powershell
dotnet tool install --global wix --version 4.*
pwsh ./packaging/windows/msi/build-msi.ps1 `
  -SourceDir ./dist/lattice-windows-x86_64 `
  -Version 0.1.1 `
  -OutputPath ./dist/lattice-windows-x86_64.msi
```

The GitHub release workflow uses the same script on `windows-latest`.

Next likely improvements:

1. make the `Remove Lattice Data` flow smarter about prompting if the service is still in use
2. offer an opt-in purge path for `%ProgramData%\Lattice` directly inside the MSI UI
3. add richer post-install guidance from the installer itself
