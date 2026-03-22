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
$wixToolVersion = (& wix --version).Trim()
if (-not $wixToolVersion) {
    throw "Failed to determine WiX tool version"
}
$wixPackageVersion = ($wixToolVersion -split '\+')[0]

$localExtensionRoot = Join-Path (Get-Location) ".wix\extensions\WixToolset.UI.wixext"
if (Test-Path $localExtensionRoot) {
    Remove-Item -Recurse -Force $localExtensionRoot
}

$requiredExtensions = @("WixToolset.UI.wixext", "WixToolset.Firewall.wixext")
$extensionList = ""
try {
    $extensionList = Invoke-Wix -Arguments @('extension', 'list', '-g')
} catch {
    $extensionList = ""
}

foreach ($ext in $requiredExtensions) {
    if ($extensionList -notmatch "$([regex]::Escape($ext))/$([regex]::Escape($wixPackageVersion))") {
        try {
            Invoke-Wix -Arguments @('extension', 'remove', '-g', $ext) | Out-Null
        } catch {
        }
        Invoke-Wix -Arguments @('extension', 'add', '-g', "$ext/$wixPackageVersion") | Out-Null
    }
}

Invoke-Wix -Arguments @(
    'build',
    '-arch', 'x64',
    '-ext', "WixToolset.UI.wixext/$wixPackageVersion",
    '-ext', "WixToolset.Firewall.wixext/$wixPackageVersion",
    '-d', "SourceDir=$resolvedSource",
    '-d', "Version=$Version",
    $wxsPath,
    '-o', $OutputPath
)
