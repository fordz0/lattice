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

function Invoke-Wix {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    $output = & wix @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw ("wix {0} failed:`n{1}" -f ($Arguments -join ' '), ($output -join [Environment]::NewLine))
    }
    return $output
}

New-Item -ItemType Directory -Force -Path (Join-Path $env:USERPROFILE ".wix\extensions") | Out-Null
$wixVersion = (& wix --version).Trim()
if (-not $wixVersion) {
    throw "Failed to determine WiX tool version"
}

$uiExtensionList = ""
try {
    $uiExtensionList = Invoke-Wix -Arguments @('extension', 'list', '-g')
} catch {
    $uiExtensionList = ""
}

if ($uiExtensionList -notmatch 'WixToolset\.UI\.wixext') {
    Invoke-Wix -Arguments @('extension', 'add', '-g', "WixToolset.UI.wixext/$wixVersion") | Out-Null
}

$uiExtensionDll = Join-Path $env:USERPROFILE ".wix\extensions\WixToolset.UI.wixext\$wixVersion\wixext4\WixToolset.UI.wixext.dll"
if (-not (Test-Path $uiExtensionDll)) {
    throw "WiX UI extension DLL not found at expected path: $uiExtensionDll"
}

Invoke-Wix -Arguments @(
    'build',
    '-arch', 'x64',
    '-ext', $uiExtensionDll,
    '-d', "SourceDir=$resolvedSource",
    '-d', "Version=$Version",
    $wxsPath,
    '-o', $OutputPath
)
