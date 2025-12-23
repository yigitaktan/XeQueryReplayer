--------------------------------------
-- Step 12 - Query Store, Round Two --
--------------------------------------

USE [master]
GO

DECLARE @DbName                   sysname      = N'DemoDB';
DECLARE @OperationMode            nvarchar(20) = N'READ_WRITE';
DECLARE @DataFlushIntervalSeconds int          = 60;
DECLARE @IntervalLengthMinutes    int          = 1;
DECLARE @MaxStorageSizeMB         int          = 10000;
DECLARE @MaxPlansPerQuery         int          = 1000;
DECLARE @QueryCaptureMode         nvarchar(20) = N'ALL';

BEGIN TRY
    IF NULLIF(LTRIM(RTRIM(@DbName)), N'') IS NULL
        THROW 50000, 'Database name cannot be empty.', 1;

    IF DB_ID(@DbName) IS NULL
        THROW 50000, 'Database not found.', 1;

    IF UPPER(@OperationMode) NOT IN (N'READ_WRITE', N'READ_ONLY')
        THROW 50000, 'Invalid OPERATION_MODE. Use READ_WRITE or READ_ONLY.', 1;

    IF UPPER(@QueryCaptureMode) NOT IN (N'ALL', N'AUTO', N'NONE', N'CUSTOM')
        THROW 50000, 'Invalid QUERY_CAPTURE_MODE. Use ALL, AUTO, NONE, or CUSTOM.', 1;


    DECLARE @sql nvarchar(max) =
        N'ALTER DATABASE ' + QUOTENAME(@DbName) + N' SET QUERY_STORE = ON;' + CHAR(13) + CHAR(10) +
        N'ALTER DATABASE ' + QUOTENAME(@DbName) + N' SET QUERY_STORE (' + CHAR(13) + CHAR(10) +
        N'      OPERATION_MODE = ' + UPPER(@OperationMode) + N',' + CHAR(13) + CHAR(10) +
        N'      DATA_FLUSH_INTERVAL_SECONDS = ' + CAST(@DataFlushIntervalSeconds AS nvarchar(10)) + N',' + CHAR(13) + CHAR(10) +
        N'      INTERVAL_LENGTH_MINUTES = ' + CAST(@IntervalLengthMinutes AS nvarchar(10)) + N',' + CHAR(13) + CHAR(10) +
        N'      MAX_STORAGE_SIZE_MB = ' + CAST(@MaxStorageSizeMB AS nvarchar(10)) + N',' + CHAR(13) + CHAR(10) +
        N'      MAX_PLANS_PER_QUERY = ' + CAST(@MaxPlansPerQuery AS nvarchar(10)) + N',' + CHAR(13) + CHAR(10) +
        N'      QUERY_CAPTURE_MODE = ' + UPPER(@QueryCaptureMode) + CHAR(13) + CHAR(10) +
        N');';

    EXEC sys.sp_executesql @sql;


    DECLARE @verifySql nvarchar(max) =
        N'USE ' + QUOTENAME(@DbName) + N';
          SELECT
              actual_state_desc,
              desired_state_desc,
              current_storage_size_mb,
              max_storage_size_mb,
              interval_length_minutes,
              stale_query_threshold_days,
              query_capture_mode_desc
          FROM sys.database_query_store_options;';

    EXEC sys.sp_executesql @verifySql;
END TRY
BEGIN CATCH
    DECLARE @ErrMsg   nvarchar(4000) = ERROR_MESSAGE();
    DECLARE @ErrNum   int            = ERROR_NUMBER();
    DECLARE @ErrSev   int            = ERROR_SEVERITY();
    DECLARE @ErrState int            = ERROR_STATE();

    PRINT N'Error: ' + ISNULL(@ErrMsg, N'');
    PRINT N'(No=' + CAST(@ErrNum AS nvarchar(10)) +
          N', Severity=' + CAST(@ErrSev AS nvarchar(10)) +
          N', State=' + CAST(@ErrState AS nvarchar(10)) + N')';

    THROW;
END CATCH;
