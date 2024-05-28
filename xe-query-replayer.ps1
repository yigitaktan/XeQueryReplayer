<#
╔═════════════════════════════════════════════════════════════════════════════════╗
║ THE DEVELOPER MAKES NO GUARANTEE THAT THE POWERSHELL SCRIPT WILL SATISFY YOUR   ║
║ SPECIFIC REQUIREMENTS, OPERATE ERROR-FREE, OR FUNCTION WITHOUT INTERRUPTION.    ║
║ WHILE EVERY EFFORT HAS BEEN MADE TO ENSURE THE STABILITY AND EFFICACY OF THE    ║
║ SOFTWARE, IT IS INHERENT IN THE NATURE OF SOFTWARE DEVELOPMENT THAT UNEXPECTED  ║
║ ISSUES MAY OCCUR. YOUR PATIENCE AND UNDERSTANDING ARE APPRECIATED AS I          ║
║ CONTINUALLY STRIVE TO IMPROVE AND ENHANCE MY SOFTWARE SOLUTIONS.                ║
╚═════════════════════════════════════════════════════════════════════════════════╝
┌───────────┬─────────────────────────────────────────────────────────────────────┐
│ Usage     │ 1) Run CMD or PowerShell                                            │
│           │ 2) PowerShell -ExecutionPolicy Bypass -File .\xe-query-replayer.ps1 │
├───────────┼─────────────────────────────────────────────────────────────────────┤
│ Developer │ Yigit Aktan - yigita@microsoft.com                                  │
└───────────┴─────────────────────────────────────────────────────────────────────┘
#>

<#
    This section changes the window title of the current PowerShell session.
    This can be useful for distinguishing between multiple sessions or for
    clearly identifying the purpose of a particular script or session.
#>
$Host.UI.RawUI.WindowTitle = "XEvent Query Replayer"

$Global:AppVer = "05.2024.3.003"

<# 
    Verifying the encoding of the script and its associated function file.
    Ensuring they are encoded in UTF-16LE or UTF-16BE, which is necessary for correct execution.
#>
$EncodingErrorForMainScript = $false
$EncodingErrorForFunctionFile = $false

$Byte = Get-Content -Encoding Byte -ReadCount 2 -TotalCount 2 -Path $MyInvocation.MyCommand.Path
if (-not (($Byte[0] -eq 0xff -and $Byte[1] -eq 0xfe) -or ($Byte[0] -eq 0xfe -and $Byte[1] -eq 0xff))) {
  $EncodingErrorForMainScript = $true
}

$FunctionFile = Join-Path $PSScriptRoot "functions.psm1"
$Byte = Get-Content -Encoding Byte -ReadCount 2 -TotalCount 2 -Path $FunctionFile
if (-not (($Byte[0] -eq 0xff -and $Byte[1] -eq 0xfe) -or ($Byte[0] -eq 0xfe -and $Byte[1] -eq 0xff))) {
  $EncodingErrorForFunctionFile = $true
}

if ($EncodingErrorForMainScript -or $EncodingErrorForFunctionFile) {
  Clear-Host
  Write-Host " XEvent Query Replayer" -ForegroundColor Yellow
  Write-Host

  if ($EncodingErrorForMainScript -and $EncodingErrorForFunctionFile) {
    $BothFilesEncodingErrorText = @(" [x] ","'xe-query-replayer.ps1' and 'functions.psm1' files must be in either UTF-16LE or UTF-16BE encoding. `n     Please open these files in a text editor like Notepad++ and save them as specified.")
    $BothFilesEncodingErrorColour = @("Gray","Red")

    for ([int]$i = 0; $i -lt $BothFilesEncodingErrorText.Length; $i++) {
      Write-Host $BothFilesEncodingErrorText[$i] -Foreground $BothFilesEncodingErrorColour[$i] -NoNewline
    }
    Write-Host
  } elseif ($EncodingErrorForMainScript) {
    $MainFileEncodingErrorText = @(" [x] ","The 'xe-query-replayer.ps1' file must be in either UTF-16LE or UTF-16BE encoding. `n     Please open these files in a text editor like Notepad++ and save them as specified.")
    $MainFileEncodingErrorColour = @("Gray","Red")

    for ([int]$i = 0; $i -lt $MainFileEncodingErrorText.Length; $i++) {
      Write-Host $MainFileEncodingErrorText[$i] -Foreground $MainFileEncodingErrorColour[$i] -NoNewline
    }
    Write-Host
  } else {
    $FunctionFileEncodingErrorText = @(" [x] ","The 'function.psm1' file must be in either UTF-16LE or UTF-16BE encoding. `n     Please open these files in a text editor like Notepad++ and save them as specified.")
    $FunctionFileEncodingErrorColour = @("Gray","Red")

    for ([int]$i = 0; $i -lt $FunctionFileEncodingErrorText.Length; $i++) {
      Write-Host $FunctionFileEncodingErrorText[$i] -Foreground $FunctionFileEncodingErrorColour[$i] -NoNewline
    }
    Write-Host
  }
  exit
}

<# 
    Importing custom functions from the 'functions.psm1' module.
    This module contains various utility functions used throughout the script.
#>
Import-Module -DisableNameChecking .\functions.psm1

<# 
    Hiding the console cursor for cleaner visual output during script execution.
#>
[Console]::CursorVisible = $false

<# 
    Clearing the console to provide a clean slate for our script's output.
#>
Clear-Host

<#
    Initializes a global variable to set the log file path, combining the script's root directory with the specified file name "log.txt".
#>
$Global:LogFile = $PSScriptRoot + "\log.txt"

<#
    The script validates the presence of 'config.txt', creating a sample with default settings if it's missing, and then terminates.
#>
$ConfigFile = $PSScriptRoot + "\config.txt"

if (-not (Test-Path $ConfigFile -PathType Leaf)) {
  try {
    "[AuthenticationType]=SQL" | Set-Content -Path $ConfigFile -Force
    "[ServerName]=SQLINSTANCE" | Add-Content -Path $ConfigFile
    "[DatabaseName]=DemoDB" | Add-Content -Path $ConfigFile
    "[UserName]=MyDemoUser" | Add-Content -Path $ConfigFile
    "[Password]=Password.1" | Add-Content -Path $ConfigFile
    "[XelPath]=C:\xel_files\DemoDB_capture" | Add-Content -Path $ConfigFile
    "[ReplayType]=3" | Add-Content -Path $ConfigFile
    "[ErrorLogType]=1" | Add-Content -Path $ConfigFile
    "[AutoStart]=0" | Add-Content -Path $ConfigFile

    Show_Title_Table
    Write_Error_Text -Text "config.txt file not found. However, an example config.txt has been created in the current directory. You should modify its contents according to your needs." -Prefix " [!]" -Color "Gray","Yellow"
    Write_Log_To_File -FilePath $Global:LogFile -LogMsg "[Type]     : Error`n[Message]  : config.txt file not found. However, an example config.txt has been created in the current directory. You should modify its contents according to your needs." | Out-Null
    exit
  }
  catch {
    Show_Title_Table
    Write_Error_Text -Text "config.txt file not found. I attempted to create a new config.txt in the same directory as the script but was unsuccessful." -Prefix " [x]" -Color "Gray","Red"
    Write_Log_To_File -FilePath $Global:LogFile -LogMsg "[Type]     : Error`n[Message]  : config.txt file not found. I attempted to create a new config.txt in the same directory as the script but was unsuccessful." | Out-Null
    exit
  }
}

<#
    Initialize global variables to keep track of various metrics during the process.
#>
$Global:SuccessfulSPQueryCount = 0 # Count of successful stored procedure queries executed.
$Global:FailedSPQueryCount = 0 # Count of failed stored procedure queries.
$Global:SuccessfulAdhocQueryCount = 0 # Count of successful ad-hoc (non-stored procedure) queries executed.
$Global:FailedAdhocQueryCount = 0 # Count of failed ad-hoc queries.
$Global:ExecCount = 0 # Total count of queries executed, both successful and failed.
$Global:RpcCompletedCount = 0 # Count of Remote Procedure Call queries that have completed successfully.
$Global:SqlBatchCompletedCount = 0 # Count of SQL batch queries that have completed successfully.
$Global:TotalQueryCount = 0 # Aggregate count of all queries, from all XEL files.
$Global:TotalQueryCountPerFile = 0 # Count of all queries processed within the current file.
$Global:SuccessfulQueryCount = 0 # Total count of successful queries, both stored procedure and ad-hoc.
$Global:FailedQueryCount = 0 # Total count of failed queries, both stored procedure and ad-hoc.
$Global:SuccessfulSPQueryCountPerFile = 0 # Count of successful stored procedure queries executed for the current file.
$Global:SuccessfulAdhocQueryCountPerFile = 0 # Count of successful ad-hoc queries executed for the current file.

<# 
    This function displays a title table with information about the application, the developer, and the version.
    The table visually segregates the title, developer's name, company, and the application version for a cleaner display.
#>
Show_Title_Table

<# 
    Extracting configuration parameters from the configuration file.
    These parameters will be used to control how the script interacts with SQL Server.
#>
$AuthenticationType = Get-Content -Path $ConfigFile | ForEach-Object { if ($_ -match "^\[AuthenticationType\]=(.*)$") { $Matches[1].Trim() } }
$ServerName = Get-Content -Path $ConfigFile | ForEach-Object { if ($_ -match "^\[ServerName\]=(.*)$") { $Matches[1].Trim() } }
$DatabaseName = Get-Content -Path $ConfigFile | ForEach-Object { if ($_ -match "^\[DatabaseName\]=(.*)$") { $Matches[1].Trim() } }
$UserName = Get-Content -Path $ConfigFile | ForEach-Object { if ($_ -match "^\[UserName\]=(.*)$") { $Matches[1].Trim() } }
$Password = Get-Content -Path $ConfigFile | ForEach-Object { if ($_ -match "^\[Password\]=(.*)$") { $Matches[1].Trim() } }
$XelPath = Get-Content -Path $ConfigFile | ForEach-Object { if ($_ -match "^\[XelPath\]=(.*)$") { $Matches[1].Trim() } }
$ReplayType = Get-Content -Path $ConfigFile | ForEach-Object { if ($_ -match "^\[ReplayType\]=(.*)$") { $Matches[1].Trim() } }
$Global:LogType = Get-Content -Path $ConfigFile | ForEach-Object { if ($_ -match "^\[LogType\]=(.*)$") { $Matches[1].Trim() } }
$AutoStart = Get-Content -Path $ConfigFile | ForEach-Object { if ($_ -match "^\[AutoStart\]=(.*)$") { $Matches[1].Trim() } }

<# 
    Validating the extracted configuration parameters to ensure they are present and correctly formatted.
#>
$ConfigParams = @{
  AuthenticationType = $AuthenticationType
  ServerName = $ServerName
  ParallelConnections = $ParallelConnections
  DatabaseName = $DatabaseName
  UserName = $UserName
  Password = $Password
  XelPath = $XelPath
  ReplayType = $ReplayType
  LogType = $Global:LogType
  AutoStart = $AutoStart
}

Validate_Config_File -ConfigParams $ConfigParams

<# 
    Load XEvent assembly.
#>
Add-Type -Path ".\Microsoft.SqlServer.XEvent.XELite.dll"

<# 
    Constructing the connection string based on the extracted and validated configuration parameters.
#>
if ($AuthenticationType.ToUpper() -eq "WIN") {
  $Global:ConnectionString = "Server=$ServerName;Database=$DatabaseName;Trusted_Connection=True;Application Name=XeQueryReplayer;"
}
elseif ($AuthenticationType.ToUpper() -eq "SQL") {
  $Global:ConnectionString = "Server=$ServerName;Database=$DatabaseName;User Id=$UserName;Password=$Password;Application Name=XeQueryReplayer;"
}

<# 
    Testing the SQL connection to ensure the script can communicate with SQL Server.
#>
$ConnectionSuccessful = Connection_Spinner -Function {
  param($ConnString)
  try {
    $SqlConnection = New-Object System.Data.SqlClient.SqlConnection
    $SqlConnection.ConnectionString = $ConnString
    $SqlConnection.Open()
    $SqlConnection.Close()
    return $true
  }
  catch {
    $SqlConnection.Close()
    return $false
  }
} -ConnStr $Global:ConnectionString -Label "Establishing connection to the server..."

if (-not $ConnectionSuccessful) {
  Move_The_Cursor 0 6
  Write_Error_Text -Text "Failed to establish SQL connection. Please check the credentials in the config.txt file." -Prefix " [x]" -Color "Gray","Red"
  Write_Log_To_File -FilePath $Global:LogFile -LogMsg "[Type]     : Error`n[Message]  : Failed to establish SQL connection. Please check the credentials in the config.txt file." | Out-Null
  Write-Host
  exit
}

<# 
    If AutoStart is set to 0, this block displays a menu with options to start the replay process or exit.
    The menu allows the user to select an option, and based on the selection, either the replay process is 
    started or the script exits.
#>
if ($AutoStart -eq 0) {
  Move_The_Cursor 1 6
  $MenuOptionArray = @("Start Replay","Exit")
  $MenuResult = Create_Menu -MenuOptions $MenuOptionArray -MenuRowColor Gray

  Switch($MenuResult){
      0 {

      }
      1 {
          Exit
      }
  }
}

<# 
    Display a countdown timer from 5 seconds, indicating the start of a process.
#>
if ($AutoStart -eq 1) {
  for ($i = 5; $i -ge 1; $i --) {
    Write_Color_Text -Text "`r  Starting in ",$i," seconds..." -Colour DarkGray,Gray,DarkGray -NoNewline
    Start-Sleep -Seconds 1
  }
}

<# 
    Display the header of a table indicating the process of analyzing files and retrieving queries from them.
#>
Clear-Host
Show_Analyzing_Files

<# 
    Retrieving all '.xel' files from the specified XelPath directory. The files are then sorted based on
    their creation time in ascending order. The sorted list of files is stored in the $XelFiles variable.
#>
$XelFiles = Get-ChildItem -Path $XelPath -Filter *.xel | Sort-Object CreationTime

<# 
    Initializing a global ArrayList named RpcSqlEventsNotFoundArrayList. This list will be used to
    store file names that don't contain the 'rpc_completed' or 'sql_batch_completed' events.
#>
$Global:RpcSqlEventsNotFoundArrayList = New-Object System.Collections.ArrayList

<# 
    Initialize the counters for the total number of XEL files and the number being processed.
#>
$script:TotalXelFileCount = 0
$script:TotalXelFileCountForProcess = 0

foreach ($FileForProcess in $XelFiles) {
  $script:TotalXelFileCountForProcess++
  $script:FileProgressForProcess = "$($script:TotalXelFileCountForProcess)/$($XelFiles.Count)"
  Process_XEvents -FilePath $FileForProcess.FullName -FileProgress $script:FileProgressForProcess
  $Global:TotalQueryCountPerFile = 0
}

<# 
    If the global counter CheckRpcSqlEvents is greater than or equal to 1, it indicates
    that the current file ($FileItem) does not contain the required events. In such a case,
    an error log is written to the specified global log file mentioning the missing events.
#>
foreach ($FileItem in $Global:RpcSqlEventsNotFoundArrayList) {
  if ($Global:CheckRpcSqlEvents -ge 1) {
    Write_Log_To_File -FilePath $Global:LogFile -LogMsg "[Type]     : Error`n[Message]  : The file '$FileItem' does not contain 'rpc_completed' or 'sql_batch_completed' events!" | Out-Null
  }
}

<# 
    Checking if the global counter CheckRpcSqlEvents is less than 1. If it is, this indicates that none of the
    scanned files contain the required 'rpc_completed' or 'sql_batch_completed' events. The screen is cleared,
    a title table is shown, an error message is displayed on the console, and the same error message is also
    written to the specified global log file. After the error display, the script terminates its execution.
#>
if ($Global:CheckRpcSqlEvents -lt 1) {
  Clear-Host
  Show_Title_Table
  Write_Error_Text -Text "No rpc_completed or sql_batch_completed events were found in any of the scanned files." -Prefix " [x]" -Color "Gray","Red"
  Write_Log_To_File -FilePath $Global:LogFile -LogMsg "[Type]     : Error`n[Message]  : No rpc_completed or sql_batch_completed events were found in any of the scanned files." | Out-Null
  Write-Host
  exit
}

<# 
    Initialize the counters for the total number of XEL files and the number being processed.
#>
Start-Sleep -Seconds 3
Clear-Host
Show_Progress_Table

<# 
    This command writes an informational log message to a specified global log file.
    The message indicates the beginning of a replay operation.
#>
switch ($ReplayType) {
  "1" {
    Write_Log_To_File -FilePath $Global:LogFile -LogMsg "[Type]     : Information`n[Message]  : Replay operation started for the 'rpc_completed' event. | config.txt : [ReplayType]=1" | Out-Null
  }
  "2" {
    Write_Log_To_File -FilePath $Global:LogFile -LogMsg "[Type]     : Information`n[Message]  : Replay operation started for the 'sql_batch_completed' event. | config.txt : [ReplayType]=2" | Out-Null
  }
  "3" {
    Write_Log_To_File -FilePath $Global:LogFile -LogMsg "[Type]     : Information`n[Message]  : Replay operation started for both 'rpc_completed' and 'sql_batch_completed' events. | config.txt : [ReplayType]=3" | Out-Null
  }
}

<# 
    Iterate over each XEL file to read and process the events within.
#>
foreach ($FileForRead in $XelFiles) {
  $script:TotalXelFileCount++
  $script:FileProgress = "$($script:TotalXelFileCount)/$($XelFiles.Count)"
  Read_XEvents -FilePath $FileForRead.FullName -FileProgress $script:FileProgress -ReplayType $ReplayType
}

<# 
    Display the result table headers with designated areas for successful and 
    failed query counts for both Stored Procedures (SP) and Ad-Hoc queries.
#>
Clear-Host
Show_Result_Table
Move_The_Cursor 13 3
Write-Host $Global:SuccessfulSPQueryCount -ForegroundColor Yellow
Move_The_Cursor 29 3
Write-Host $Global:FailedSPQueryCount -ForegroundColor Yellow
Move_The_Cursor 13 5
Write-Host $Global:SuccessfulAdhocQueryCount -ForegroundColor Yellow
Move_The_Cursor 29 5
Write-Host $Global:FailedAdhocQueryCount -ForegroundColor Yellow
Write-Host
Write-Host

<# 
    This block parses a "HH:MM:SS" time format, and converts it into a human-readable 
    duration, omitting zero values. Result is stored in $FormattedDuration.
#>
$Hours,$Minutes,$Seconds = $Global:formattedTime -split ":"

$FilteredParts = @()
if ($Hours -ne "00") { $FilteredParts += ($Hours -replace "^0+") + " hour" + ("s" * [int]($Hours -ne "01")) }
if ($Minutes -ne "00") { $FilteredParts += ($Minutes -replace "^0+") + " minute" + ("s" * [int]($Minutes -ne "01")) }
if ($Seconds -ne "00") { $FilteredParts += ($Seconds -replace "^0+") + " second" + ("s" * [int]($Seconds -ne "01")) }

$FormattedDuration = $FilteredParts -join ", "

<# 
    Displaying the success message
#>
Write-Host " The replay operation has been successfully" -ForegroundColor Yellow
Write-Host " completed in $FormattedDuration." -ForegroundColor Yellow
Write_Log_To_File -FilePath $Global:LogFile -LogMsg "[Type]     : Information`n[Message]  : The replay operation has been successfully completed in $FormattedDuration." | Out-Null
Write-Host

<#
    Makes the console cursor visible.
#>
[Console]::CursorVisible = $true
