-----------------------------------------------------
-- Step 15 / Round 2 - Extracting Query Store Data --
-----------------------------------------------------

USE [master]
GO

DBCC CLONEDATABASE (DemoDB, DemoDB_cL170);
