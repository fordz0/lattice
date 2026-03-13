# Windows Installer

This directory contains the first real MSI implementation for Lattice.

Current behavior:

- installs `lattice.exe` and `lattice-daemon.exe` into `Program Files\Lattice`
- adds the install directory to the system `PATH`
- includes `LICENSE` and `README.md`
- installs and starts the `lattice-daemon` Windows service automatically
- configures the service to run with:
  - `--service`
  - `--data-dir "%ProgramData%\Lattice"`
- keeps runtime daemon management available in the CLI too:
  - `lattice up`
  - `lattice down`
  - `lattice service ...`

What it does not do yet:

- install Start Menu shortcuts
- bundle the Firefox extension
- offer a “remove user data” checkbox

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

1. make daemon service installation optional in the MSI UI
2. add Start Menu shortcuts for CLI/setup docs
3. keep `%LOCALAPPDATA%\Lattice` on uninstall by default, with an opt-in purge path
