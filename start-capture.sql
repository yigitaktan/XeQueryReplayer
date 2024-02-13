/*
╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║ THE DEVELOPER MAKES NO GUARANTEE THAT THE T-SQL SCRIPT WILL SATISFY YOUR SPECIFIC REQUIREMENTS, OPERATE ERROR-FREE, OR FUNCTION WITHOUT      ║
║ INTERRUPTION. WHILE EVERY EFFORT HAS BEEN MADE TO ENSURE THE STABILITY AND EFFICACY OF THE SOFTWARE, IT IS INHERENT IN THE NATURE OF         ║
║ SOFTWARE DEVELOPMENT THAT UNEXPECTED ISSUES MAY OCCUR. YOUR PATIENCE AND UNDERSTANDING ARE APPRECIATED AS I CONTINUALLY STRIVE TO IMPROVE    ║
║ AND ENHANCE MY SOFTWARE SOLUTIONS.                                                                                                           ║
╚══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝
┌────────────┬─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Info       │ This script initiates an Extended Event session for the specified duration, capturing both SP and/or ad-hoc queries.            │
├────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Developer  │ Yigit Aktan - yigita@microsoft.com                                                                                              │
├────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Parameters │ @DatabaseName       : Name of the database to monitor.                                                                          │
│            │ @xEventSessionName  : Name for the Extended Events session.                                                                     │
│            │ @CapturePath        : Path where the session's output files should be saved.                                                    │         
│            │ @Duration           : Duration to capture events. Format: "HH:MM:SS" (hours:minutes:seconds)                                    │
│            │ @MaxFileSize        : Maximum file size for each event capture file (in MB).                                                    │
│            │ @MaxRolloverFiles   : Maximum number of rollover files.                                                                         │
│            │ @MaxMemory          : Maximum memory to allocate for the session (in MB).                                                       │
│            │ @CollectType        : Type of SQL statements to capture. This parameter can accept the values "sp", "adhoc", or "sp-and-adhoc". │
└────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
*/

DECLARE 
    @DatabaseName        SYSNAME         = 'DemoDB',
    @CapturePath         NVARCHAR(500)   = 'C:\xel_files\DemoDB_capture',
    @Duration            VARCHAR(10)     = '00:30:00',
    @MaxFileSize         INT             = 500,
    @MaxRolloverFiles    INT             = 10000,
    @MaxMemory           INT             = 100,
    @CollectType         VARCHAR(50)     = 'sp-and-adhoc';

DECLARE
    @CreateXeSession     NVARCHAR(MAX),
    @StartXeSession      NVARCHAR(MAX),
    @DropXeSession       NVARCHAR(MAX),
    @xEventSessionName   VARCHAR(25),
    @RandomID            VARCHAR(4)
	
SET @RandomID = CHAR(CAST((RAND() * 26) + 65 AS INT)) + CHAR(CAST((RAND() * 26) + 65 AS INT)) + CHAR(CAST((RAND() * 10) + 48 AS INT)) + CHAR(CAST((RAND() * 10) + 48 AS INT));

SET @xEventSessionName = @DatabaseName + '_' + REPLACE(@CollectType, '-', '_') + '_' + @RandomID;

-- Ensure the capture path ends with a backslash
SET @CapturePath = @CapturePath + IIF(RIGHT(@CapturePath, 1) <> '\', '\', '');

-- Adjusting the create session based on the value of @CollectType
SET @CreateXeSession = 'CREATE EVENT SESSION [' + @xEventSessionName + '] ON SERVER ';

IF @CollectType IN ('sp', 'sp-and-adhoc')
BEGIN
    SET @CreateXeSession += ' ADD EVENT sqlserver.rpc_completed(
        SET collect_data_stream=(1),
            collect_output_parameters=(1)
        ACTION(
            sqlserver.client_app_name,
            sqlserver.client_hostname,
            sqlserver.client_pid,
            sqlserver.database_id,
            sqlserver.database_name,
            sqlserver.server_instance_name,
            sqlserver.session_id,
            sqlserver.username
        )
        WHERE ([sqlserver].[database_id]=(' + CONVERT(VARCHAR, DB_ID(@DatabaseName)) + '))
    )';
END

IF @CollectType IN ('adhoc', 'sp-and-adhoc')
BEGIN
    IF CHARINDEX('ADD EVENT', @CreateXeSession) > 0
    BEGIN
        SET @CreateXeSession += ',';
    END

    SET @CreateXeSession += ' ADD EVENT sqlserver.sql_batch_completed(
        ACTION(
            sqlserver.client_app_name,
            sqlserver.client_hostname,
            sqlserver.client_pid,
            sqlserver.database_id,
            sqlserver.database_name,
            sqlserver.server_instance_name,
            sqlserver.session_id,
            sqlserver.username
        )
        WHERE ([sqlserver].[database_id]=(' + CONVERT(VARCHAR, DB_ID(@DatabaseName)) + '))
    )';
END

SET @CreateXeSession += ' ADD TARGET package0.event_file(
        SET filename=N''' + @CapturePath + @xEventSessionName + '.xel'',
            max_file_size=(' + CONVERT(VARCHAR, @MaxFileSize) + '),
            max_rollover_files=(' + CONVERT(VARCHAR, @MaxRolloverFiles) + ')
    )
    WITH (
        MAX_MEMORY=' + CONVERT(VARCHAR, @MaxMemory) + ' MB,
        EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,
        MAX_DISPATCH_LATENCY=60 SECONDS,
        MAX_EVENT_SIZE=0 KB,
        MEMORY_PARTITION_MODE=PER_CPU,
        TRACK_CAUSALITY=ON,
        STARTUP_STATE=OFF
    );';

-- Drop the session if exists
IF EXISTS (SELECT * FROM sys.server_event_sessions WHERE [name] = @xEventSessionName)
BEGIN    
    SET @DropXeSession = 'DROP EVENT SESSION [' + @xEventSessionName + '] ON SERVER';
    EXECUTE sp_executesql @DropXeSession;
END

-- Execute the create session SQL
EXECUTE sp_executesql @CreateXeSession;

-- Start the session
SET @StartXeSession = 'ALTER EVENT SESSION [' + @xEventSessionName + '] ON SERVER STATE = START';
EXECUTE sp_executesql @StartXeSession;

-- Wait for delay
DECLARE @WaitForDelay NVARCHAR(MAX) = 'WAITFOR DELAY ''' + @Duration + '''';
EXECUTE sp_executesql @WaitForDelay;

-- Stop the session
DECLARE @StopXeSession NVARCHAR(MAX) = 'ALTER EVENT SESSION [' + @xEventSessionName + '] ON SERVER STATE = STOP';
EXECUTE sp_executesql @StopXeSession;

-- Drop the session if exists
IF EXISTS (SELECT * FROM sys.server_event_sessions WHERE [name] = @xEventSessionName)
BEGIN    
    SET @DropXeSession = 'DROP EVENT SESSION [' + @xEventSessionName + '] ON SERVER';
    EXECUTE sp_executesql @DropXeSession;
END
