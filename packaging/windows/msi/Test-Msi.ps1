param(
    [Parameter(Mandatory = $true)]
    [string]$Path,

    [string]$ExpectedVersion
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $Path)) {
    throw "MSI does not exist: $Path"
}

$scriptPath = Join-Path $PSScriptRoot "Test-Msi.vbs"
if (-not (Test-Path -LiteralPath $scriptPath)) {
    throw "MSI validator script is missing: $scriptPath"
}

$arguments = @('//NoLogo', $scriptPath, (Resolve-Path -LiteralPath $Path).Path)
if ($ExpectedVersion) {
    $arguments += $ExpectedVersion
}

& cscript.exe @arguments
if ($LASTEXITCODE -ne 0) {
    throw "MSI validation failed"
}
