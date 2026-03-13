$ErrorActionPreference = "Stop"

$serviceName = "lattice-daemon"
$dataDir = Join-Path $env:ProgramData "Lattice"

Write-Host ""
Write-Host "This will remove the local Lattice daemon data in:" -ForegroundColor Yellow
Write-Host "  $dataDir" -ForegroundColor Yellow
Write-Host ""
Write-Host "The lattice-daemon service will be stopped first if it is running." -ForegroundColor Yellow
Write-Host ""

$confirm = Read-Host "Continue? [y/N]"
if ($confirm -notmatch '^(y|yes)$') {
  Write-Host "Cancelled."
  exit 0
}

try {
  & sc.exe stop $serviceName | Out-Null
  Start-Sleep -Seconds 2
} catch {
}

if (Test-Path $dataDir) {
  Remove-Item -Path $dataDir -Recurse -Force
  Write-Host "Removed $dataDir" -ForegroundColor Green
} else {
  Write-Host "No data directory found at $dataDir" -ForegroundColor Yellow
}
