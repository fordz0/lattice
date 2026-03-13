param(
    [Parameter(Mandatory = $true)]
    [string]$Path,

    [string]$ExpectedVersion
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $Path)) {
    throw "MSI does not exist: $Path"
}

function Get-MsiPropertyValue {
    param(
        [Parameter(Mandatory = $true)]$Database,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $view = $Database.OpenView("SELECT `Value` FROM `Property` WHERE `Property`='$Name'")
    $view.Execute()
    $record = $view.Fetch()
    if ($null -eq $record) {
        return $null
    }
    return $record.StringData(1)
}

function Test-MsiTableHasRows {
    param(
        [Parameter(Mandatory = $true)]$Database,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $tableView = $Database.OpenView("SELECT `Name` FROM `_Tables` WHERE `Name`='$Name'")
    $tableView.Execute()
    $tableRecord = $tableView.Fetch()
    if ($null -eq $tableRecord) {
        return $false
    }

    $rowView = $Database.OpenView(("SELECT * FROM `{0}`" -f $Name))
    $rowView.Execute()
    $row = $rowView.Fetch()
    return $null -ne $row
}

$installer = New-Object -ComObject WindowsInstaller.Installer
$resolvedPath = (Resolve-Path -LiteralPath $Path).Path
$database = $installer.GetType().InvokeMember('OpenDatabase', [System.Reflection.BindingFlags]::InvokeMethod, $null, $installer, @($resolvedPath, 0))

$productName = Get-MsiPropertyValue -Database $database -Name "ProductName"
if ($productName -ne "Lattice") {
    throw "Unexpected ProductName: $productName"
}

$manufacturer = Get-MsiPropertyValue -Database $database -Name "Manufacturer"
if ($manufacturer -ne "benjf") {
    throw "Unexpected Manufacturer: $manufacturer"
}

$installDir = Get-MsiPropertyValue -Database $database -Name "ARPINSTALLLOCATION"
if ($installDir -ne "[INSTALLFOLDER]") {
    throw "Unexpected ARPINSTALLLOCATION: $installDir"
}

$helpLink = Get-MsiPropertyValue -Database $database -Name "ARPHELPLINK"
if ($helpLink -ne "https://lattice.benjf.dev/getting-started") {
    throw "Unexpected ARPHELPLINK: $helpLink"
}

if ($ExpectedVersion) {
    $productVersion = Get-MsiPropertyValue -Database $database -Name "ProductVersion"
    if ($productVersion -ne $ExpectedVersion) {
        throw "Unexpected ProductVersion: $productVersion"
    }
}

if (-not (Test-MsiTableHasRows -Database $database -Name "ServiceInstall")) {
    throw "MSI is missing ServiceInstall rows"
}

if (-not (Test-MsiTableHasRows -Database $database -Name "Shortcut")) {
    throw "MSI is missing Shortcut rows"
}

Write-Output "MSI validation passed"
