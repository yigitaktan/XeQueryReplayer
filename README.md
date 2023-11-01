# XEvent Query Replayer

* **[Getting started with the script](#getting-started-with-the-script)**
* **[Script components](#script-components)**
* **[Prerequisites](#prerequisites)**
* **[Preparing the config.txt file](#preparing-the-configtxt-file)**
* **[Running the script](#running-the-script)**


## Getting started with the script
XEvent Query Replayer is a PowerShell script that reads the RPC:Completed and SQL:BatchCompleted events from XEL files captured with Extended Events and replays the Stored Procedures and Ad-hoc queries to a specified SQL instance's database.

## Script components
XEvent Query Replayer consists of a total of 5 files. The codebase is built within the `xe-query-replayer.ps1` and `functions.psm1` files.

* **[xe-query-replayer.ps1](https://xe-query-replayer.ps1):** This is the main script. The script is executed by running this file.
* **[functions.psm1](https://xe-query-replayer.ps1):** All functions used in the `xe-query-replayer.ps1` file are stored in this file, and the script cannot run without it.
* **[config.txt](https://config.txt):** This file contains various parameters related to how the script operates and the details of the instance to be replayed. You can see these parameters in more detail in the "Preparing the config.txt file" section.
* **[Microsoft.SqlServer.XEvent.XELite.dll](https://Microsoft.SqlServer.XEvent.XELite.dll):** XELite is a cross-platform library developed by Microsoft to read XEvents from XEL files or live SQL streams. Script reads and processes the XEvent files using the classes within this file.
* **[Microsoft.Data.SqlClient.dll](https://Microsoft.Data.SqlClient.dll):** `Microsoft.Data.SqlClient` is a data provider for Microsoft SQL Server and Azure SQL Database. This namespace has a dependency on the XELite DLL, and XELite cannot be used unless this DLL is in the same directory.

## Prerequisites
This script requires that **.NET Framework 4.6.2** or a later version be installed on the machine where it's deployed. As highlighted in the [Script components](#script-components) section, both the `Microsoft.SqlServer.XEvent.XELite.dll` and `Microsoft.Data.SqlClient.dll` files are compatible with this framework version or newer. If you do not have **.NET Framework 4.6.2** or a higher version installed, you can download and install it from this URL: http://go.microsoft.com/fwlink/?linkid=780600

If you want to determine which version of the .NET Framework is installed on the machine where the script will be run, you can execute the following PowerShell script to see the highest installed version.

<pre>$release = Get-ItemPropertyValue -LiteralPath 'HKLM:SOFTWARE\Microsoft\NET Framework Setup\NDP\v4\Full' -Name Release
switch ($release) {
    { $_ -ge 533320 } { $version = '4.8.1 or later'; break }
    { $_ -ge 528040 } { $version = '4.8'; break }
    { $_ -ge 461808 } { $version = '4.7.2'; break }
    { $_ -ge 461308 } { $version = '4.7.1'; break }
    { $_ -ge 460798 } { $version = '4.7'; break }
    { $_ -ge 394802 } { $version = '4.6.2'; break }
    { $_ -ge 394254 } { $version = '4.6.1'; break }
    { $_ -ge 393295 } { $version = '4.6'; break }
    { $_ -ge 379893 } { $version = '4.5.2'; break }
    { $_ -ge 378675 } { $version = '4.5.1'; break }
    { $_ -ge 378389 } { $version = '4.5'; break }
    default { $version = $null; break }
}

if ($version) {
    Write-Host -Object ".NET Framework Version: $version"
} else {
    Write-Host -Object '.NET Framework Version 4.5 or later is not detected.'
}</pre>

## Preparing the config.txt file
The **config.txt** file consists of 9 parameters: `AuthenticationType`, `ServerName`, `DatabaseName`, `UserName`, `Password`, `XelPath`, `ReplayType`, `LogType`, and `AutoStart`.
  
* **[ServerName]**: Represents the SQL instance you wish to connect to. If you are connecting to the default instance, you should enter the instance name in this parameter as **SERVERNAME**. If it's a named instance, you should enter it as **SERVERNAME\INSTANCENAME**.
  
* **[DatabaseName]**: This is the database where you want to replay the XEL files.
  
* **[UserName]**: If "**SQL**" is entered for the `AuthenticationType` parameter, the `UserName` parameter must definitely be filled out. The user with which the connection will be established is determined in this parameter.
  
* **[Password]**: Similar to the `UserName` parameter, if your `AuthenticationType` is "**SQL**", define the password for the entered username in this parameter.

* **[XelPath]**: You should write the directory where your XEL files are located in this parameter. Please enter only the folder path, do not write the file name.

* **[ReplayType]**: In this parameter, you determine which statements within the captured XEL files will be replayed. You can only enter the values **1**, **2**, or **3**. A value of 1 will execute only the **rpc_completed** events, meaning it will execute Stored Procedures (SPs). A value of 2 will execute only the **sql_batch_completed** events, meaning it will execute ad-hoc queries. A value of **3** will execute both **rpc_completed** and **sql_batch_completed** events.

* **[LogType]**: This parameter specifies how the script should perform logging. Upon its first execution, the script creates a file named **log.txt** in the directory it's located in. The LogType parameter can only accept the values **1** or **2**. A value of "**1**" represents basic logging, capturing only informational messages and general errors. A value of "**2**" provides detailed logging; if there are errors in the executed statements, it will document which statements had issues, their parameters, and the specific errors encountered. This can result in the log file becoming excessively large and potentially difficult to open and read.

* **[AutoStart]**: This parameter indicates whether the script should immediately begin the replay process when the main file is executed. This parameter can take two values: "**0**" and "**1**". If you input "**0**", the script won't start the replay automatically. Instead, it will prompt you with the message, "_Please press Enter to start replay or ESC to exit._" If you input "**1**", the script will quickly begin the replay process based on the values entered in `config.txt` without displaying any prompt.


If `AuthenticationType` is specified as "**SQL**", all the parameters mentioned above must be written in the `config.txt` file. If it is specified as "**WIN**", the `UserName` and `Password` parameters are not required. If `AuthenticationType` is specified as "**WIN**" and the `UserName` and `Password` parameters are still set to specific values in the file, these two parameters will be skipped, and whether or not they have any values will not affect the operation of the script.


The name of the configuration file must be **`config.txt`**. The previously mentioned 9 parameters should be written inside square brackets and then assigned their respective values. Below is an example of how a config.txt file should be written.

<pre>
[AuthenticationType]=SQL
[ServerName]=DBPROD01\SQL2019
[DatabaseName]=DemoDB
[UserName]=MyDemoUser
[Password]=Password.1
[XelPath]=C:\xel_files\DemoDB_capture
[ReplayType]=3
[LogType]=1
[AutoStart]=0
</pre>

The `config.txt` file should be located in the same directory as the script. When the script is run, if the `config.txt` file cannot be found, a sample file will be created.

## Running the script
The script can be easily run by opening a command prompt. There is no need to open the console with a user that has administrator privileges. All you need to do is place the necessary files mentioned above into a single folder and run the following command.

<pre>powershell.exe -File .\xe-query-replayer.ps1</pre>
![xe-run](https://github.com/yigitaktan/XeQueryReplayer/assets/51110247/e39ce69a-508b-4cde-8906-02e938e9a64a)
