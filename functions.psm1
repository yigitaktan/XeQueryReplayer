<#
╔════════════════════════════════════════════════════════════════════════════════╗
║ THE DEVELOPER MAKES NO GUARANTEE THAT THE POWERSHELL SCRIPT WILL SATISFY YOUR  ║
║ SPECIFIC REQUIREMENTS, OPERATE ERROR-FREE, OR FUNCTION WITHOUT INTERRUPTION.   ║
║ WHILE EVERY EFFORT HAS BEEN MADE TO ENSURE THE STABILITY AND EFFICACY OF THE   ║
║ SOFTWARE, IT IS INHERENT IN THE NATURE OF SOFTWARE DEVELOPMENT THAT UNEXPECTED ║
║ ISSUES MAY OCCUR. YOUR PATIENCE AND UNDERSTANDING ARE APPRECIATED AS I         ║
║ CONTINUALLY STRIVE TO IMPROVE AND ENHANCE MY SOFTWARE SOLUTIONS.               ║
╚════════════════════════════════════════════════════════════════════════════════╝
┌───────────┬────────────────────────────────────────────────────────────────────┐
│ Usage     │ 1) Run CMD or PowerShell                                           │
│           │ 2) powershell.exe -File .\xe-query-replayer.ps1                    │
├───────────┼────────────────────────────────────────────────────────────────────┤
│ Developer │ Yigit Aktan - yigita@microsoft.com                                 │
└───────────┴────────────────────────────────────────────────────────────────────┘
#>

<# 
    This function displays a spinning animation in the console while running a specified function.
    It provides a visual indicator that something is happening in the background. 
#>
function Connection_Spinner {
  param([scriptblock]$function,[string]$Label,[string]$ConnStr)

  $JobArguments = @($ConnStr)
  $Job = Start-Job -ScriptBlock $function -ArgumentList $JobArguments

  #$Symbols = @("⣾⣿", "⣽⣿", "⣻⣿", "⢿⣿", "⡿⣿", "⣟⣿", "⣯⣿", "⣷⣿", "⣿⣾", "⣿⣽", "⣿⣻", "⣿⢿", "⣿⡿", "⣿⣟", "⣿⣯", "⣿⣷")
  $Symbols = @("|","/","-","\")
  $i = 0;

  while ($Job.State -eq "Running") {
    $Symbol = $Symbols[$i]

    Move_The_Cursor 0 6
    Write-Host -NoNewline " [" -ForegroundColor Gray
    Write-Host -NoNewline "$symbol" -ForegroundColor DarkGray
    Write-Host -NoNewline "]" -ForegroundColor Gray
    Write-Host " $Label" -ForegroundColor Yellow -NoNewline

    Start-Sleep -Milliseconds 100
    Move_The_Cursor 0 6
    Write-Host "                                             " -NoNewline
    $i++
    if ($i -eq $Symbols.Count) {
      $i = 0;
    }
  }
  $Result = Receive-Job -Job $Job
  Remove-Job -Job $Job
  return $Result
}

<#
    Rendering the title table in the console to showcase the application name, developer's details, and version.
#>
function Show_Title_Table {
  #$AppVer = "05.2024.3.001"
  Write-Host " ┌─────────────────────────────────────────┐" -ForegroundColor DarkGray
  Write_Color_Text -Text ' │          ','XEvent Query Replayer','          │' -Colour DarkGray,White,DarkGray
  Write-Host " ├─────────────┬───────────┬───────────────┤" -ForegroundColor DarkGray
  Write_Color_Text -Text ' │ ','Yigit Aktan',' │ ','Microsoft',' │ ',$Global:AppVer,' │' -Colour DarkGray,Gray,DarkGray,Gray,DarkGray,Gray,DarkGray,Gray,DarkGray
  Write-Host " └─────────────┴───────────┴───────────────┘" -ForegroundColor DarkGray
  Write-Host ""
}

<# 
    This function creates a console menu with options and highlights the selected option.
    It uses specified colors for the menu rows and allows users to navigate through 
    the options using the arrow keys. The menu title and options are displayed with 
    formatted text, and the selected option is returned when the Enter key is pressed.
#>
function Create_Menu (){  
  param(
      [Parameter(Mandatory=$True)][array]$MenuOptions,
      [Parameter(Mandatory=$True)][array]$MenuRowColor
  )

  $MaxValue = $MenuOptions.count-1
  $Selection = 0
  $EnterPressed = $False
  
  Clear-Host

  while($EnterPressed -eq $False){
      Write-Host " ┌─────────────────────────────────────────┐" -ForegroundColor DarkGray
      Write_Color_Text -Text ' │          ','XEvent Query Replayer','          │' -Colour DarkGray,White,DarkGray
      Write-Host " ├─────────────┬───────────┬───────────────┤" -ForegroundColor DarkGray
      Write_Color_Text -Text ' │ ','Yigit Aktan',' │ ','Microsoft',' │ ',$Global:AppVer,' │' -Colour DarkGray,Gray,DarkGray,Gray,DarkGray,Gray,DarkGray,Gray,DarkGray
      Write-Host " └─────────────┴───────────┴───────────────┘" -ForegroundColor DarkGray
      Write-Host ""

      for ($i=0; $i -le $MaxValue; $i++){
          
          if ($i -eq $Selection){
            Write-Host " " -NoNewline
              Write-Host -BackgroundColor $MenuRowColor -ForegroundColor Black " $($MenuOptions[$i]) "         
          } else {
            Write-Host " " -NoNewline
              Write-Host " $($MenuOptions[$i]) "        
          }
      }
 
      $KeyInput = $host.ui.rawui.readkey("NoEcho,IncludeKeyDown").virtualkeycode

      switch($KeyInput){        
          13{
              $EnterPressed = $True
              return $Selection
              Clear-Host
              break
          }
          38{
              if ($Selection -eq 0){
                  $Selection = $MaxValue
              } else {
                  $Selection -= 1
              }              
              Clear-Host
              break           
          }
          40{
              if ($Selection -eq $MaxValue){                
                  $Selection = 0
              } else {
                  $Selection +=1
              }
              Clear-Host
              break             
          }
          default{
              Clear-Host
          }
      }
  }
}

<#
    Visual representation of the analyzing files process in the console.
#>
function Show_Analyzing_Files {
  Write-Host " ┌────────────────────┬────────────────────┐" -ForegroundColor DarkGray
  Write_Color_Text -Text ' │ ','Analyzing Files','    │                    │' -Colour DarkGray,Gray,DarkGray
  Write-Host " ├────────────────────┼────────────────────┤" -ForegroundColor DarkGray
  Write_Color_Text -Text ' │ ','Retrieving Queries',' │                    │' -Colour DarkGray,Gray,DarkGray
  Write-Host " └────────────────────┴────────────────────┘" -ForegroundColor DarkGray
}

<#
    Displays a progress table in the console, presenting metrics such as duration, progress, percentage, process, and files.
#>
function Show_Progress_Table {
  Clear-Host
  Write-Host " ┌─────────────┬───────────────────────────┐" -ForegroundColor DarkGray
  Write_Color_Text -Text " │ ","Duration","    │                           │" -Colour DarkGray,Gray,DarkGray
  Write-Host " ├─────────────┼───────────────────────────┤" -ForegroundColor DarkGray
  Write_Color_Text -Text " │ ","Progress","    │                           │" -Colour DarkGray,Gray,DarkGray
  Write-Host " ├─────────────┼───────────────────────────┤" -ForegroundColor DarkGray
  Write_Color_Text -Text " │ ","Percentage","  │                           │" -Colour DarkGray,Gray,DarkGray
  Write-Host " ├─────────────┼───────────────────────────┤" -ForegroundColor DarkGray
  Write_Color_Text -Text " │ ","Process","     │                           │" -Colour DarkGray,Gray,DarkGray
  Write-Host " ├─────────────┼───────────────────────────┤" -ForegroundColor DarkGray
  Write_Color_Text -Text " │ ","Files","       │                           │" -Colour DarkGray,Gray,DarkGray
  Write-Host " └─────────────┴───────────────────────────┘" -ForegroundColor DarkGray
  Write-Host
  Write_Color_Text -Text " Press ","ESC"," key to terminate the replay operation and exit." -Colour DarkGray,Gray,DarkGray
}

<# 
    This function writes text to the console in specified colors.
    It allows for colorful console output to make messages stand out. 
#>
function Write_Color_Text {
  param([String[]]$Text,[ConsoleColor[]]$Colour,[switch]$NoNewline = $false)
  for ([int]$i = 0; $i -lt $Text.Length; $i++) { Write-Host $Text[$i] -Foreground $Colour[$i] -NoNewline }
  if ($NoNewline -eq $false) { Write-Host '' }
}

<# 
    This function moves the cursor to a specified position in the console window.
    It is useful for controlling where text is output in the console. 
#>
function Move_The_Cursor ([int]$x,[int]$y) {
  $Host.UI.RawUI.CursorPosition = New-Object System.Management.Automation.Host.Coordinates $x,$y
}

<#
    Renders a result table in the console, showing statistics for successful and failed stored procedures (SP) and ad-hoc queries.
#>
function Show_Result_Table {
  Write_Color_Text -Text "           ┌───────────────┬───────────────┐" -Colour DarkGray
  Write_Color_Text -Text "           │ ","Successful","    │ ","Failed","        │" -Colour DarkGray,Gray,DarkGray,Gray,DarkGray
  Write_Color_Text -Text " ┌─────────┼───────────────┼───────────────┤" -Colour DarkGray
  Write_Color_Text -Text " │ ","SP","      │               │               │" -Colour DarkGray,Gray,DarkGray
  Write_Color_Text -Text " ├─────────┼───────────────┼───────────────┤" -Colour DarkGray
  Write_Color_Text -Text " │ ","Ad-Hoc","  │               │               │" -Colour DarkGray,Gray,DarkGray
  Write_Color_Text -Text " └─────────┴───────────────┴───────────────┘" -Colour DarkGray
}

<# 
    This function validates the parameters read from a configuration file.
    It checks that mandatory parameters are present and correctly formatted, and writes error messages if not. 
#>
function Validate_Config_File {
  param(
    [hashtable]$ConfigParams
  )

  $ErrorMessages = @()

  if ($null -eq $ConfigParams['AuthenticationType'])
  { $ErrorMessages += "[AuthenticationType] parameter was not found in the config.txt file." }
  else {
    if ([string]::IsNullOrEmpty($ConfigParams['AuthenticationType']) -or (($ConfigParams['AuthenticationType'].ToUpper() -ne "SQL") -and ($ConfigParams['AuthenticationType'].ToUpper() -ne "WIN")))
    { $ErrorMessages += "You must write 'WIN' for Windows authentication or 'SQL' for SQL Server Authentication as the value for [AuthenticationType] parameter. The parameter cannot be left empty" }
    else {
      if ($ConfigParams['AuthenticationType'].ToUpper() -eq "SQL") {
        if ($null -eq $ConfigParams['UserName'])
        { $ErrorMessages += "[Username] parameter was not found in the config.txt file. Since you selected [AuthenticationType] parameter as 'SQL' [Username] parameter is mandatory." }
        else {
          if ([string]::IsNullOrEmpty($ConfigParams['UserName']))
          { $ErrorMessages += "If [AuthenticationType] parameter is set to 'SQL', you cannot leave the [Username] parameter empty." }
        }
        if ($null -eq $ConfigParams['Password'])
        { $ErrorMessages += "[Password] parameter was not found in the config.txt file. Since you selected [AuthenticationType] parameter as 'SQL' [Password] parameter is mandatory." }
        else {
          if ([string]::IsNullOrEmpty($ConfigParams['Password']))
          { $ErrorMessages += "If [AuthenticationType] parameter is set to 'SQL', you cannot leave the [Password] parameter empty." }
        }
      }
    }
  }

  if ($null -eq $ConfigParams['ServerName']) {
    $ErrorMessages += "[ServerName] parameter was not found in the config.txt file."
  }
  else {
    if ([string]::IsNullOrEmpty($ConfigParams['ServerName'])) {
      $ErrorMessages += "You cannot enter an empty value for [ServerName] parameter."
    }
  }

  if ($null -eq $ConfigParams['DatabaseName']) {
    $ErrorMessages += "[DatabaseName] parameter was not found in the config.txt file."
  }
  else {
    if ([string]::IsNullOrEmpty($ConfigParams['DatabaseName'])) {
      $ErrorMessages += "You cannot enter an empty value for [DatabaseName] parameter."
    }
  }

  if ($null -eq $ConfigParams['ReplayType']) {
    $ErrorMessages += "[ReplayType] parameter was not found in the config.txt file."
  }
  else {
    if ([string]::IsNullOrEmpty($ConfigParams['ReplayType'])) {
      $ErrorMessages += "You cannot enter an empty value for [ReplayType] parameter."
    }
    else {
      if ($ConfigParams['ReplayType'] -notmatch '^[123]$') {
        $ErrorMessages += "Only 1, 2 and 3 can be entered for [ReplayType] parameter.     1 = (rpc_completed) 2 = (sql_batch_completed) 3 = (Both)"
      }
    }
  }

  if ($null -eq $ConfigParams['LogType']) {
    $ErrorMessages += "[LogType] parameter was not found in the config.txt file."
  }
  else {
    if ([string]::IsNullOrEmpty($ConfigParams['LogType'])) {
      $ErrorMessages += "You cannot enter an empty value for [LogType] parameter."
    }
    else {
      if ($ConfigParams['LogType'] -notmatch '^[12]$') {
        $ErrorMessages += "Only 1 and 2 can be entered for [LogType] parameter.<NEWLINE>1 = (Basic logging)<NEWLINE>2 = (Detailed logging. If queries result in errors during execution, they will also be logged, potentially causing a substantial increase in the log size)"
      }
    }
  }

  if ($null -eq $ConfigParams['XelPath']) {
    $ErrorMessages += "[XelPath] parameter was not found in the config.txt file."
  }
  else {
    if ([string]::IsNullOrEmpty($ConfigParams['XelPath'])) {
      $ErrorMessages += "You cannot enter an empty value for [XelPath] parameter."
    }
    else {
      if (-not (Test-Path $ConfigParams['XelPath'] -PathType Container)) {
        $ErrorMessages += "The directory defined in the [XelPath] parameter within config.txt could not be found."
      }
      else {
        $xelFiles = Get-ChildItem -Path $ConfigParams['XelPath'] -Filter *.xel
        if ($xelFiles.Count -eq 0) {
          $ErrorMessages += "No *.xel files found in the directory defined in [XelPath] parameter."
        }
      }
    }
  }

  if ($null -eq $ConfigParams['AutoStart']) {
    $ErrorMessages += "[AutoStart] parameter was not found in the config.txt file."
  }
  else {
    if ([string]::IsNullOrEmpty($ConfigParams['AutoStart'])) {
      $ErrorMessages += "You cannot enter an empty value for [AutoStart] parameter."
    }
    else {
      if ($ConfigParams['AutoStart'] -notmatch '^[01]$') {
        $ErrorMessages += "Only 0 and 1 are valid values for [AutoStart] parameter.<NEWLINE>0 = (Manual start with Enter)<NEWLINE>1 = (Automatically run)"
      }
    }
  }


  $scriptPath = $PSScriptRoot
  $file1 = "Microsoft.Data.SqlClient.dll"
  $file2 = "Microsoft.SqlServer.XEvent.XELite.dll"

  $file1Path = Join-Path -Path $scriptPath -ChildPath $file1
  $file2Path = Join-Path -Path $scriptPath -ChildPath $file2

  $file1Exists = Test-Path -Path $file1Path
  $file2Exists = Test-Path -Path $file2Path

  if (-not $file1Exists -and -not $file2Exists) {
    $ErrorMessages += "$file1 and $file2 not found. Make sure they are in the same path as the script."
  } elseif (-not $file1Exists) {
    $ErrorMessages += "$file1 not found. Make sure it is in the same path as the script."
  } elseif (-not $file2Exists) {
    $ErrorMessages += "$file2 not found. Make sure it is in the same path as the script."
  }

  
  if ($ErrorMessages.Count -gt 0) {
    $ErrorMessages | ForEach-Object {
      Write_Error_Text -Text $_ -Prefix " [x]" -Color "Gray","Red"
      Write-Host
    }
    exit
  }

}

<# 
    The function waits for the user to press either the Enter or Escape key based on the specified DesiredKey parameter.
    If Enter is pressed, it returns "START". If Escape is pressed, the script exits.
    The function ignores CTRL+C inputs, ensuring the script doesn't exit unintentionally.
#>
function Read_Key_Until_Pressed {
  param(
    [ValidateSet('Enter','Escape')]
    [string]$DesiredKey
  )

  [System.Console]::TreatControlCAsInput = $true

  $KeyInfo = $null
  while ($true) {
    $KeyInfo = [Console]::ReadKey($true)
    if ($KeyInfo.Key -eq 'Enter') {
      [System.Console]::TreatControlCAsInput = $false
      return "START"
    } elseif ($KeyInfo.Key -eq 'Escape') {
      Write-Host
      exit
    }
  }

  [System.Console]::TreatControlCAsInput = $false
  return $false
}

<# 
    This function writes an error message to a specified file.
    It is useful for logging errors for later review. 
#>
function Write_Error_Text {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Text,

    [Parameter(Mandatory = $true)]
    [string]$Prefix,

    [Parameter(Mandatory = $true)]
    [string[]]$Color
  )

  $PrefixColor = $Color[0]
  $TextColor = $Color[1]

  function GetNextLine ([string]$RemainingText,[int]$MaxLength) {
    if ($RemainingText.Length -le $MaxLength) { return $RemainingText }

    $BreakPoint = $MaxLength
    while ($BreakPoint -gt 0 -and ($RemainingText[$BreakPoint] -ne ' ')) {
      $BreakPoint --
    }

    if ($BreakPoint -eq 0) {
      $BreakPoint = $MaxLength
    }

    return $RemainingText.Substring(0,$BreakPoint)
  }

  $Text = $Text -replace '<NEWLINE>',"`n"

  $FirstLineMaxLen = 68 - $Prefix.Length - 1
  $FirstLine = GetNextLine ($Text -split "`n")[0] $FirstLineMaxLen
  $Text = $Text.Substring($FirstLine.Length).Trim()

  Write-Host $Prefix -NoNewline -ForegroundColor $PrefixColor
  Write-Host " $FirstLine" -ForegroundColor $TextColor

  $Padding = " " * ($Prefix.Length + 1)

  foreach ($Line in ($Text -split "`n")) {
    while ($Line.Length -gt 0) {
      $LinePart = GetNextLine $Line 64
      $Line = $Line.Substring($LinePart.Length).Trim()
      Write-Host "$Padding$LinePart" -ForegroundColor $TextColor
    }
  }
}

<#
    Executes SQL queries, either stored procedures (SP) or ad-hoc, against a given database connection.
    Keeps track of successful and failed query counts, adjusting global counters accordingly.
#>
function Execute_SqlQuery {
  param(
      [ValidateSet("SP", "ADHOC")]
      [string]$Type,
      [string]$Statement,
      [string]$ConnectionString
  )

  try {
      $SqlConnection = New-Object System.Data.SqlClient.SqlConnection($ConnectionString)
      $SqlCommand = $SqlConnection.CreateCommand()
      $SqlConnection.Open()

      if ($Type -eq "SP") {
          $SqlCommand.CommandType = [System.Data.CommandType]::Text

          $SpName = $Statement.Split(' ')[1].TrimEnd(';')
          
          if ($SpName -ieq "sp_reset_connection") {
              return
          }

          $ParamString = $Statement.Substring($Statement.IndexOf($SpName) + $SpName.Length).Trim()

          $ParamPairs = $ParamString -split ","
          $ParamList = @()
          foreach ($ParamPair in $ParamPairs) {
              $PairParts = $ParamPair -split "="
              if ($PairParts.Count -eq 2) {
                  $ParamName = $PairParts[0].Trim()
                  $ParamValue = $PairParts[1].Trim()

                  if ($ParamValue -match "^N'|^'") {
                      $ParamList += "$ParamName=$ParamValue"
                  } elseif ($ParamValue -match "^\d+$") {
                      $ParamList += "$ParamName=$ParamValue"
                  } else {
                      $ParamList += "$ParamName=N'$ParamValue'"
                  }
              }
          }

          $SqlCommand.CommandText = "exec $SpName " + ($ParamList -join ", ")
      } elseif ($Type -eq "ADHOC") {
          $SqlCommand.CommandType = [System.Data.CommandType]::Text
          $SqlCommand.CommandText = $Statement.TrimEnd(";")
      }

      $SqlCommand.ExecuteNonQuery() | Out-Null
      $Global:SuccessfulQueryCount++
      $Global:ExecCount++
      if ($Type -eq "SP") {
          $Global:SuccessfulSPQueryCount++
          $Global:SuccessfulSPQueryCountPerFile++
      } else {
          $Global:SuccessfulAdhocQueryCount++
          $Global:SuccessfulAdhocQueryCountPerFile++
      }
  } catch {
      if ($Global:LogType -eq 2) {
          if ($Type -eq "SP") {
              Write_Log_To_File -FilePath $Global:LogFile -LogMsg "[Type]     : Error`n[SP Name]  : $SpName`n[Message]  : $_" | Out-Null
          } else {
              $TrimmedAdHocText = $Statement.TrimStart()
              Write_Log_To_File -FilePath $Global:LogFile -LogMsg "[Type]     : Error`n[Ad-Hoc]   : $TrimmedAdHocText`n[Message]  : $_" | Out-Null
          }
      }

      $Global:FailedQueryCount++
      $Global:ExecCount++
      if ($Type -eq "SP") {
          $Global:FailedSPQueryCount++
      } else {
          $Global:FailedAdhocQueryCount++
      }
  } finally {
      $SqlConnection.Close()
  }
}

<#
    Processes Extended Events (XEvents) from an input file to track specific SQL Server events,
    such as rpc_completed and sql_batch_completed. Skips certain predefined queries and
    updates the global counters for processed events. Additionally, updates the console with
    the progress for better user feedback.
#>
function Process_XEvents {
  param(
    [string]$FilePath,
    [string]$FileProgress
  )

  $script:StartTime = Get-Date

  $XEvents = New-Object -TypeName Microsoft.SqlServer.XEvent.XELite.XEFileEventStreamer -ArgumentList $FilePath

  $FoundRpc = $false
  $FoundSqlBatch = $false

  $XEvents.ReadEventStream(
    {
      return [System.Threading.Tasks.Task]::CompletedTask
    },
    {
      param($XEvent)

      if ($XEvent.Name -eq "rpc_completed") {
        $FoundRpc = $true
        if ($XEvent.Fields["statement"] -ne "exec sp_reset_connection") {
          $Global:RpcCompletedCount++
        }
      }
      if ($XEvent.Name -eq "sql_batch_completed") {
        $FoundSqlBatch = $true
        if ($XEvent.Fields["batch_text"] -ne "exec sp_reset_connection") {
          $Global:SqlBatchCompletedCount++
        }
      }

      if ($FoundRpc -or $FoundSqlBatch) {
        $Global:CheckRpcSqlEvents++
      }
      else {
        $Global:RpcSqlEventsNotFoundArrayList.Add($FilePath) | Out-Null
      }

      return [System.Threading.Tasks.Task]::CompletedTask
    },[System.Threading.CancellationToken]::None
  ).Wait()

  $Global:TotalQueryCountPerFile = $Global:RpcCompletedCount + $Global:SqlBatchCompletedCount
  $Global:TotalQueryCount = $Global:TotalQueryCountPerFile

  Move_The_Cursor 24 1
  Write-Host ($FileProgress) -ForegroundColor Yellow
  Move_The_Cursor 24 3
  Write-Host ($Global:TotalQueryCount) -ForegroundColor Yellow
}

<#
    Reads and processes Extended Events (XEvents) from an input file to identify specific
    SQL Server events: "rpc_completed" and "sql_batch_completed". Excludes certain predefined
    events for accuracy. Executes identified SQL queries while updating the console progress bar
    and related metrics to provide real-time feedback on query execution progress.
#>
function Read_XEvents {
  param(
    [string]$FilePath,
    [string]$FileProgress,
    [string]$ReplayType
  )

  $originalMode = [Console]::TreatControlCAsInput
  [Console]::TreatControlCAsInput = $true

  $ExtractFileName = (Get-Item $FilePath).Name
  Write_Log_To_File -FilePath $Global:LogFile -LogMsg "[Type]     : Information`n[Message]  : Starting replay of '$ExtractFileName'." | Out-Null

  $Global:SuccessfulSPQueryCountPerFile = 0
  $Global:SuccessfulAdhocQueryCountPerFile = 0

  $XEvents = New-Object -TypeName Microsoft.SqlServer.XEvent.XELite.XEFileEventStreamer -ArgumentList $FilePath

  try {

    $XEvents.ReadEventStream(
      {
        return [System.Threading.Tasks.Task]::CompletedTask
      },
      {
        param($XEvent)

        $ElapsedTime = [datetime]::Now - $script:StartTime
        $Global:FormattedTime = "{0:D2}:{1:D2}:{2:D2}" -f $ElapsedTime.Hours,$ElapsedTime.Minutes,$ElapsedTime.Seconds

        if (($ReplayType -eq "1" -or $ReplayType -eq "3") -and $XEvent.Name -eq "rpc_completed") {

          if ([Console]::KeyAvailable) {
            $key = [System.Console]::ReadKey($true)
            if ($key.Key -eq 'Escape') {
              Move_The_Cursor 1 12
              Write-Host "ESC key was just pressed, so the replay process stopped and exited.    " -NoNewline -ForegroundColor Gray
              Write-Host
              [Console]::TreatControlCAsInput = $originalMode
              exit
            }
          }

          if ($XEvent.Fields["statement"] -ne "exec sp_reset_connection") {
          
            if ($XEvent.Fields["object_name"] -eq "sp_executesql") {
              Execute_SqlQuery -Type "ADHOC" -Statement $XEvent.Fields["statement"] -ConnectionString $Global:ConnectionString
            }
            if ($XEvent.Fields["object_name"] -ne "sp_executesql") {
              if ($XEvent.Fields["statement"] -match "\S+\s+exec") {
                Execute_SqlQuery -Type "ADHOC" -Statement $XEvent.Fields["statement"] -ConnectionString $Global:ConnectionString
              }
              else {
                Execute_SqlQuery -Type "SP" -Statement $XEvent.Fields["statement"] -ConnectionString $Global:ConnectionString
              }
            }

            $completedBlocks = [math]::Truncate(($Global:ExecCount / $Global:TotalQueryCount) * 25)
            $PercentageCalculation = [math]::Truncate(($Global:ExecCount / $Global:TotalQueryCount) * 100)
            $remainingBlocks = 25 - $completedBlocks

            $completedBar = '█' * $completedBlocks
            $remainingBar = '░' * $remainingBlocks

            Move_The_Cursor 17 1
            Write-Host ($Global:FormattedTime) -ForegroundColor Yellow
            Move_The_Cursor 17 3
            Write_Color_Text -Text $completedBar,$remainingBar -Colour Yellow,DarkGray
            Move_The_Cursor 17 5
            Write-Host ("%" + $PercentageCalculation) -ForegroundColor Yellow
            Move_The_Cursor 17 7
            Write-Host ("$Global:ExecCount/$Global:TotalQueryCount") -ForegroundColor Yellow
            Move_The_Cursor 17 9
            Write-Host ($FileProgress) -ForegroundColor Yellow
          } }

        if (($ReplayType -eq "2" -or $ReplayType -eq "3") -and $XEvent.Name -eq "sql_batch_completed") {

          if ([Console]::KeyAvailable) {
            $key = [System.Console]::ReadKey($true)
            if ($key.Key -eq 'Escape') {
              Move_The_Cursor 1 12
              Write-Host "ESC key was just pressed, so the replay process stopped and exited.    " -NoNewline -ForegroundColor Gray
              Write-Host
              [Console]::TreatControlCAsInput = $originalMode
              exit
            }
          }

          if ($XEvent.Fields["batch_text"] -ne "exec sp_reset_connection") {
            Execute_SqlQuery -Type "ADHOC" -Statement $XEvent.Fields["batch_text"] -ConnectionString $Global:ConnectionString

            $completedBlocks = [math]::Truncate(($Global:ExecCount / $Global:TotalQueryCount) * 25)
            $PercentageCalculation = [math]::Truncate(($Global:ExecCount / $Global:TotalQueryCount) * 100)
            $remainingBlocks = 25 - $completedBlocks

            $completedBar = '█' * $completedBlocks
            $remainingBar = '░' * $remainingBlocks
            Move_The_Cursor 17 1
            Write-Host ($Global:FormattedTime) -ForegroundColor Yellow
            Move_The_Cursor 17 3
            Write_Color_Text -Text $completedBar,$remainingBar -Colour Yellow,DarkGray
            Move_The_Cursor 17 5
            Write-Host ("%" + $PercentageCalculation) -ForegroundColor Yellow
            Move_The_Cursor 17 7
            Write-Host ("$Global:ExecCount/$Global:TotalQueryCount") -ForegroundColor Yellow
            Move_The_Cursor 17 9
            Write-Host ($FileProgress) -ForegroundColor Yellow
          } }

        return [System.Threading.Tasks.Task]::CompletedTask

      },[System.Threading.CancellationToken]::None
    ).Wait()

  }
  catch {
    exit
  }

  Write_Log_To_File -FilePath $Global:LogFile -LogMsg "[Type]     : Information`n[Message]  : '$ExtractFileName' file was replayed with $Global:SuccessfulSPQueryCountPerFile rpc_completed and $Global:SuccessfulAdhocQueryCountPerFile sql_batch_completed events." | Out-Null

  [Console]::TreatControlCAsInput = $originalMode

}

<# 
    This function writes log messages to a specified file.
    It is useful for logging errors for later review. 
#>
function Write_Log_To_File {
  param(
    [string]$FilePath,
    [string]$LogMsg
  )

  if (-not (Test-Path $FilePath)) {
    New-Item -Path $FilePath -ItemType File -Force
  }

  $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

  $RetryCount = 0
  $MaxRetries = 5
  $WaitSeconds = 2

  do {
    try {
      $FileStream = [System.IO.File]::Open($FilePath,[System.IO.FileMode]::Append,[System.IO.FileAccess]::Write,[System.IO.FileShare]::Read)
      $StreamWriter = New-Object System.IO.StreamWriter ($FileStream)
      $StreamWriter.WriteLine("[Timestamp]: $Timestamp")
      $StreamWriter.WriteLine($LogMsg)
      $StreamWriter.WriteLine("------------------------------------------------------------")
      $StreamWriter.Close()
      $FileStream.Close()
      $RetryCount = $MaxRetries
    }
    catch {
      if ($RetryCount -ge $MaxRetries) {

      }
      else {
        Start-Sleep -Seconds $WaitSeconds
        $RetryCount++
      }
    }
  } while ($RetryCount -lt $MaxRetries)
}
