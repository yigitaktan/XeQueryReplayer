USE [master]
GO

DECLARE @DbName sysname  = N'DemoDB';
DECLARE @TargetLevel int = 120;

BEGIN TRY
    IF NULLIF(LTRIM(RTRIM(@DbName)), N'') IS NULL
        THROW 50000, 'Database name cannot be empty.', 1;

    IF @TargetLevel NOT IN (100,110,120,130,140,150,160,170)
        THROW 50000, 'Invalid compatibility level.', 1;

    IF DB_ID(@DbName) IS NULL
        THROW 50000, 'Database not found.', 1;

    DECLARE @CurrentLevel int = (SELECT compatibility_level FROM sys.databases WHERE name = @DbName);

    PRINT 'Current level: ' + CAST(@CurrentLevel AS nvarchar(10));
    PRINT 'Target level: ' + CAST(@TargetLevel AS nvarchar(10));

    IF @CurrentLevel <> @TargetLevel
    BEGIN
        DECLARE @sql nvarchar(max) = N'ALTER DATABASE ' + QUOTENAME(@DbName) + N' SET COMPATIBILITY_LEVEL = ' + CAST(@TargetLevel AS nvarchar(10));

        EXEC sys.sp_executesql @sql;
        PRINT 'Compatibility level updated.';
    END
    ELSE
        PRINT 'No change needed. Already at target level.';

    SELECT name, compatibility_level FROM sys.databases WHERE name = @DbName;
END TRY
BEGIN CATCH
    PRINT 'Error: ' + ERROR_MESSAGE();
    THROW;
END CATCH;
