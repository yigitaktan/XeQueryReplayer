-------------------------------------------------------
-- Step 9 / Round 1 - Removing the Restored Database --
-------------------------------------------------------

USE [master]
GO

IF DATABASEPROPERTYEX (N'DemoDB', N'Version') > 0
BEGIN
    ALTER DATABASE [DemoDB] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [DemoDB];
END
GO
