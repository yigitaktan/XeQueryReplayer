# XEvent Query Replayer

* **[Getting started with the script](#getting-started-with-the-script)**
* **[Script components](#script-components)**
* **[Preparing the config.txt file](#preparing-the-configtxt-file)**

## Getting started with the script
XEvent Query Replayer is a PowerShell script that reads the RPC:Completed and SQL:BatchCompleted events from XEL files captured with Extended Events and replays the Stored Procedures and Ad-hoc queries to a specified SQL instance's database.

## Script components
XEvent Query Replayer consists of a total of 5 files. The codebase is built within the `xe-query-replayer.ps1` and `functions.psm1` files.

* **[xe-query-replayer.ps1](https://xe-query-replayer.ps1):** The primary script.
* **[functions.psm1](https://xe-query-replayer.ps1):** All functions used in the `xe-query-replayer.ps1` file are stored in this file, and the script cannot run without it.
* **[config.txt](https://config.txt):** This file contains various parameters related to how the script operates and the details of the instance to be replayed. You can see these parameters in more detail in the "Preparing the config.txt file" section.
* **[Microsoft.SqlServer.XEvent.XELite.dll](https://Microsoft.SqlServer.XEvent.XELite.dll):** XELite is a cross-platform library developed by Microsoft to read XEvents from XEL files or live SQL streams. Script reads and processes the XEvent files using the classes within this file.
* **[Microsoft.Data.SqlClient.dll](https://Microsoft.Data.SqlClient.dll):** `Microsoft.Data.SqlClient` is a data provider for Microsoft SQL Server and Azure SQL Database. This namespace has a dependency on the XELite DLL, and XELite cannot be used unless this DLL is in the same directory.

## Preparing the config.txt file
