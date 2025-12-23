-----------------------------------------
-- Step 8 / Round 1 – Verify the Clone --
-----------------------------------------

USE [DemoDB_cL120]
GO

;WITH stmt_level AS
(
    SELECT q.object_id,
           q.query_id,
           stmt_executions = SUM(CONVERT(bigint, rs.count_executions))
    FROM sys.query_store_runtime_stats rs
    JOIN sys.query_store_plan  p ON p.plan_id  = rs.plan_id
    JOIN sys.query_store_query q ON q.query_id = p.query_id
    WHERE rs.replica_group_id = 1
      AND q.object_id > 0
      AND ISNULL(q.is_internal_query, 0) = 0
    GROUP BY q.object_id,
             q.query_id
),
sp_level AS
(
    SELECT object_id,
           sp_call_count = MAX(stmt_executions)
    FROM stmt_level
    GROUP BY object_id
),
adhoc_level AS
(
    SELECT q.query_id,
           adhoc_executions = SUM(CONVERT(bigint, rs.count_executions))
    FROM sys.query_store_runtime_stats rs
    JOIN sys.query_store_plan  p ON p.plan_id  = rs.plan_id
    JOIN sys.query_store_query q ON q.query_id = p.query_id
    WHERE rs.replica_group_id = 1
      AND (q.object_id IS NULL OR q.object_id = 0)
      AND ISNULL(q.is_internal_query, 0) = 0
    GROUP BY q.query_id
)
SELECT qs_total_sp_execution_count    = (SELECT SUM(sp_call_count) FROM sp_level),
       qs_total_adhoc_execution_count = (SELECT SUM(adhoc_executions) FROM adhoc_level),
       qs_total_sp_count              = (SELECT COUNT(*) FROM sp_level),
       qs_total_adhoc_query_count     = (SELECT COUNT(*) FROM adhoc_level);
GO

DECLARE @TableRowCounts TABLE ([TableName] VARCHAR(128), [RowCount] INT);
INSERT INTO @TableRowCounts ([TableName], [RowCount])
EXEC sp_MSforeachtable 'SELECT ''?'' [TableName], COUNT(*) [RowCount] FROM ?';
SELECT [TableName], [RowCount]
FROM @TableRowCounts
ORDER BY [TableName]
GO
