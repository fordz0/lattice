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

$wixVersion = (& wix --version).Trim()
if (-not $wixVersion) {
    throw "Failed to determine WiX tool version"
}

$uiExtensionList = & wix extension list --global
if ($uiExtensionList -notmatch 'WixToolset\.UI\.wixext') {
    & wix extension add --global "WixToolset.UI.wixext/$wixVersion"
}

& wix build `
    -arch x64 `
    -ext WixToolset.UI.wixext `
    -d SourceDir="$resolvedSource" `
    -d Version="$Version" `
    $wxsPath `
    -o $OutputPath
