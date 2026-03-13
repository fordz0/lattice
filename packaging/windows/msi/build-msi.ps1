param(
    [Parameter(Mandatory = $true)]
    [string]$SourceDir,

    [Parameter(Mandatory = $true)]
    [string]$Version,

    [Parameter(Mandatory = $true)]
    [string]$OutputPath
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $SourceDir)) {
    throw "SourceDir does not exist: $SourceDir"
}

$wxsPath = Join-Path $PSScriptRoot "Lattice.wxs"
if (-not (Test-Path $wxsPath)) {
    throw "Missing WiX source file: $wxsPath"
}

$resolvedSource = (Resolve-Path $SourceDir).Path
$outputDir = Split-Path -Parent $OutputPath
if ($outputDir) {
    New-Item -ItemType Directory -Force -Path $outputDir | Out-Null
}

wix build `
    -arch x64 `
    -d SourceDir="$resolvedSource" `
    -d Version="$Version" `
    $wxsPath `
    -o $OutputPath
