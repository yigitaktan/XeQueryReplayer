USE DemoDB_cL120;
GO

DECLARE @TableRowCounts TABLE ([TableName] VARCHAR(128), [RowCount] INT);
INSERT INTO @TableRowCounts ([TableName], [RowCount])
EXEC sp_MSforeachtable 'SELECT ''?'' [TableName], COUNT(*) [RowCount] FROM ?';
SELECT [TableName], [RowCount]
FROM @TableRowCounts
ORDER BY [TableName]
GO

SELECT TOP(100) q.query_id, qt.query_sql_text
FROM sys.query_store_query q
INNER JOIN sys.query_store_query_text qt 
ON qt.query_text_id = q.query_text_id
GO
