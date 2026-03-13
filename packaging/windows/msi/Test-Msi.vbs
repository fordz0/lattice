Option Explicit

Dim msiPath, expectedVersion
If WScript.Arguments.Count < 1 Then
  WScript.Echo "Missing MSI path"
  WScript.Quit 1
End If

msiPath = WScript.Arguments.Item(0)
If WScript.Arguments.Count >= 2 Then
  expectedVersion = WScript.Arguments.Item(1)
Else
  expectedVersion = ""
End If

Dim installer, database
Set installer = CreateObject("WindowsInstaller.Installer")
Set database = installer.OpenDatabase(msiPath, 0)

Function GetPropertyValue(name)
  Dim view, record
  Set view = database.OpenView("SELECT `Value` FROM `Property` WHERE `Property`='" & name & "'")
  view.Execute
  Set record = view.Fetch
  If record Is Nothing Then
    GetPropertyValue = ""
  Else
    GetPropertyValue = record.StringData(1)
  End If
End Function

Function TableHasRows(name)
  Dim tableView, tableRecord, rowView, row
  Set tableView = database.OpenView("SELECT `Name` FROM `_Tables` WHERE `Name`='" & name & "'")
  tableView.Execute
  Set tableRecord = tableView.Fetch
  If tableRecord Is Nothing Then
    TableHasRows = False
    Exit Function
  End If

  Set rowView = database.OpenView("SELECT * FROM `" & name & "`")
  rowView.Execute
  Set row = rowView.Fetch
  TableHasRows = Not (row Is Nothing)
End Function

Sub Fail(message)
  WScript.Echo message
  WScript.Quit 1
End Sub

If GetPropertyValue("ProductName") <> "Lattice" Then
  Fail "Unexpected ProductName: " & GetPropertyValue("ProductName")
End If

If GetPropertyValue("Manufacturer") <> "benjf" Then
  Fail "Unexpected Manufacturer: " & GetPropertyValue("Manufacturer")
End If

If GetPropertyValue("ARPHELPLINK") <> "https://lattice.benjf.dev/getting-started" Then
  Fail "Unexpected ARPHELPLINK: " & GetPropertyValue("ARPHELPLINK")
End If

If expectedVersion <> "" And GetPropertyValue("ProductVersion") <> expectedVersion Then
  Fail "Unexpected ProductVersion: " & GetPropertyValue("ProductVersion")
End If

If Not TableHasRows("ServiceInstall") Then
  Fail "MSI is missing ServiceInstall rows"
End If

If Not TableHasRows("Shortcut") Then
  Fail "MSI is missing Shortcut rows"
End If

WScript.Echo "MSI validation passed"
