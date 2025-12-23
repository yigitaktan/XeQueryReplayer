----------------------------------------------------
-- Step 7 / Round 1 - Extracting Query Store Data --
----------------------------------------------------

USE [master]
GO

DBCC CLONEDATABASE (DemoDB, DemoDB_cL120);
