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

$uiExtensionList = ""
try {
    $uiExtensionList = Invoke-Wix -Arguments @('extension', 'list', '-g')
} catch {
    $uiExtensionList = ""
}

if ($uiExtensionList -notmatch 'WixToolset\.UI\.wixext') {
    Invoke-Wix -Arguments @('extension', 'add', '-g', 'WixToolset.UI.wixext') | Out-Null
}

$uiExtensionRoot = Join-Path $env:USERPROFILE ".wix\extensions\WixToolset.UI.wixext"
$uiExtensionDll = Get-ChildItem -Path $uiExtensionRoot -Filter "WixToolset.UI.wixext.dll" -Recurse -File -ErrorAction SilentlyContinue |
    Sort-Object FullName -Descending |
    Select-Object -First 1
if ($null -eq $uiExtensionDll) {
    throw "WiX UI extension DLL not found under $uiExtensionRoot"
}

Invoke-Wix -Arguments @(
    'build',
    '-arch', 'x64',
    '-ext', $uiExtensionDll.FullName,
    '-d', "SourceDir=$resolvedSource",
    '-d', "Version=$Version",
    $wxsPath,
    '-o', $OutputPath
)
