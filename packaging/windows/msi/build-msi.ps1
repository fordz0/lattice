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
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Arguments
    )

    $output = & wix @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw ("wix {0} failed:`n{1}" -f ($Arguments -join ' '), ($output -join [Environment]::NewLine))
    }
    return $output
}

$uiExtensionList = Invoke-Wix extension list --global
if ($uiExtensionList -notmatch 'WixToolset\.UI\.wixext') {
    Invoke-Wix extension add --global WixToolset.UI.wixext | Out-Null
    $uiExtensionList = Invoke-Wix extension list --global
}
if ($uiExtensionList -notmatch 'WixToolset\.UI\.wixext') {
    throw "WiX UI extension is still unavailable after installation attempt"
}

Invoke-Wix build `
    -arch x64 `
    -ext WixToolset.UI.wixext `
    -d SourceDir="$resolvedSource" `
    -d Version="$Version" `
    $wxsPath `
    -o $OutputPath
