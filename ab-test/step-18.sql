/*======================================================================================
Query Store CL Regression Comparator (LowerCL vs HigherCL)
======================================================================================*/
SET NOCOUNT ON;

----------------------------------------------------------------------------------------
-- Parameters
----------------------------------------------------------------------------------------
DECLARE
      @DbA sysname = N'DemoDB_cL120'           -- input A
    , @DbB sysname = N'DemoDB_cL170'           -- input B
    , @MinExecCount bigint = NULL              -- e.g. 50
    , @MinRegressionRatio decimal(9,4) = NULL  -- e.g. 1.25
    , @TopN int = NULL                         -- e.g. 100
    , @StartTime datetime2(0) = NULL           -- e.g. '2025-12-24T00:00:00'
    , @EndTime   datetime2(0) = NULL           -- e.g. '2025-12-24T23:59:59'
    , @Metric sysname = N'LogicalReads'        -- LogicalReads | CPU | Duration
    , @IncludeAdhoc bit = 1                    -- 1 | 0
    , @IncludeSP bit = 1                       -- 1 | 0
    , @GroupBy sysname = N'QueryHash'          -- QueryHash | QueryText | NormalizedText
    , @PersistResults bit = 0                  -- 1 | 0
    , @ResultsTable sysname = N'dbo.QueryStoreCLRegressionResults';

----------------------------------------------------------------------------------------
-- Validation
----------------------------------------------------------------------------------------
IF @Metric NOT IN (N'LogicalReads', N'CPU', N'Duration')
    THROW 50001, 'Invalid @Metric. Use LogicalReads, CPU, or Duration.', 1;

IF @GroupBy NOT IN (N'QueryHash', N'QueryText', N'NormalizedText')
    THROW 50002, 'Invalid @GroupBy. Use QueryHash, QueryText, or NormalizedText.', 1;

IF @StartTime IS NOT NULL AND @EndTime IS NOT NULL AND @EndTime <= @StartTime
    THROW 50003, '@EndTime must be greater than @StartTime.', 1;

IF PARSENAME(@ResultsTable, 2) IS NULL OR PARSENAME(@ResultsTable, 1) IS NULL
    THROW 50004, '@ResultsTable must be two-part name like dbo.TableName.', 1;

----------------------------------------------------------------------------------------
-- Resolve compatibility levels for A/B, then map to Lower/Higher  (context-independent)
----------------------------------------------------------------------------------------
DECLARE
      @CL_A smallint = NULL
    , @CL_B smallint = NULL
    , @LowerCL smallint = NULL
    , @HigherCL smallint = NULL
    , @DbLower sysname = NULL
    , @DbHigher sysname = NULL
    , @LabelLower sysname = NULL
    , @LabelHigher sysname = NULL;

SELECT @CL_A = CAST(d.compatibility_level AS smallint)
FROM sys.databases AS d
WHERE d.name = @DbA;

SELECT @CL_B = CAST(d.compatibility_level AS smallint)
FROM sys.databases AS d
WHERE d.name = @DbB;

IF @CL_A IS NULL OR @CL_B IS NULL
    THROW 50010, 'Could not resolve compatibility_level for one or both DBs. Check @DbA/@DbB names.', 1;

IF @CL_A <= @CL_B
BEGIN
    SET @DbLower   = @DbA;
    SET @DbHigher  = @DbB;
    SET @LowerCL   = @CL_A;
    SET @HigherCL  = @CL_B;
END
ELSE
BEGIN
    SET @DbLower   = @DbB;
    SET @DbHigher  = @DbA;
    SET @LowerCL   = @CL_B;
    SET @HigherCL  = @CL_A;
END;

SET @LabelLower  = CONCAT(N'CL', @LowerCL);
SET @LabelHigher = CONCAT(N'CL', @HigherCL);

----------------------------------------------------------------------------------------
-- Temp tables
----------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#QS_Agg') IS NOT NULL DROP TABLE #QS_Agg;
CREATE TABLE #QS_Agg
(
    SourceDb              sysname        NOT NULL,   -- will store @DbLower or @DbHigher
    QueryType             varchar(10)    NOT NULL,   -- SP | Adhoc
    ObjName               sysname        NULL,       -- only for SP/object queries

    GroupKeyHash          varbinary(32)  NOT NULL,   -- join key
    QueryHash             binary(8)      NULL,
    QueryTextSample       nvarchar(max)  NULL,
    NormalizedTextSample  nvarchar(max)  NULL,

    QueryIdMin            bigint         NULL,
    QueryIdMax            bigint         NULL,

    PlanCount             int            NOT NULL,
    ExecCount             bigint         NOT NULL,

    TotalMetric           decimal(38,6)  NOT NULL,
    AvgMetric             decimal(38,6)  NOT NULL,

    TotalDuration         decimal(38,6)  NOT NULL,
    AvgDuration           decimal(38,6)  NOT NULL,

    IntervalStartMin      datetime2(0)   NULL,
    IntervalEndMax        datetime2(0)   NULL,

    ConfidenceNote        varchar(50)    NOT NULL
);

----------------------------------------------------------------------------------------
-- Detect whether runtime_stats_interval has end_time (per Lower/Higher DB)
----------------------------------------------------------------------------------------
DECLARE @HasEndTime_L bit = 0, @HasEndTime_H bit = 0;
DECLARE @chk nvarchar(max);

SET @chk = N'
SELECT @HasEndTimeOut =
    CASE WHEN EXISTS (
        SELECT 1
        FROM ' + QUOTENAME(@DbLower) + N'.sys.columns
        WHERE object_id = OBJECT_ID(''' + @DbLower + N'.sys.query_store_runtime_stats_interval'')
          AND name = ''end_time''
    ) THEN 1 ELSE 0 END;';
EXEC sp_executesql @chk, N'@HasEndTimeOut bit OUTPUT', @HasEndTimeOut=@HasEndTime_L OUTPUT;

SET @chk = N'
SELECT @HasEndTimeOut =
    CASE WHEN EXISTS (
        SELECT 1
        FROM ' + QUOTENAME(@DbHigher) + N'.sys.columns
        WHERE object_id = OBJECT_ID(''' + @DbHigher + N'.sys.query_store_runtime_stats_interval'')
          AND name = ''end_time''
    ) THEN 1 ELSE 0 END;';
EXEC sp_executesql @chk, N'@HasEndTimeOut bit OUTPUT', @HasEndTimeOut=@HasEndTime_H OUTPUT;

----------------------------------------------------------------------------------------
-- Dynamic SQL template: aggregate one DB into #QS_Agg
----------------------------------------------------------------------------------------
DECLARE @tmpl nvarchar(max) = N'
INSERT INTO #QS_Agg
(
    SourceDb, QueryType, ObjName,
    GroupKeyHash, QueryHash, QueryTextSample, NormalizedTextSample,
    QueryIdMin, QueryIdMax,
    PlanCount, ExecCount,
    TotalMetric, AvgMetric,
    TotalDuration, AvgDuration,
    IntervalStartMin, IntervalEndMax,
    ConfidenceNote
)
SELECT
      @DbName AS SourceDb
    , CASE WHEN q.object_id > 0 THEN ''SP'' ELSE ''Adhoc'' END AS QueryType
    , CASE WHEN q.object_id > 0 THEN sch.name + ''.'' + obj.name ELSE NULL END AS ObjName

    , HASHBYTES(''SHA2_256'', CONVERT(varbinary(max),
          CASE
              WHEN @GroupBy = ''QueryHash'' THEN CONVERT(nvarchar(100), q.query_hash, 1)
              WHEN @GroupBy = ''QueryText'' THEN qt.query_sql_text
              ELSE LOWER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(qt.query_sql_text, CHAR(13), '' ''), CHAR(10), '' ''), CHAR(9), '' ''))))
          END
      )) AS GroupKeyHash

    , q.query_hash
    , qt.query_sql_text AS QueryTextSample
    , LOWER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(qt.query_sql_text, CHAR(13), '' ''), CHAR(10), '' ''), CHAR(9), '' '')))) AS NormalizedTextSample

    , MIN(q.query_id) AS QueryIdMin
    , MAX(q.query_id) AS QueryIdMax

    , COUNT(DISTINCT p.plan_id) AS PlanCount
    , SUM(rs.count_executions) AS ExecCount

    , SUM(
        CASE
            WHEN @Metric = ''LogicalReads'' THEN CONVERT(decimal(38,12), rs.avg_logical_io_reads) * CONVERT(decimal(38,12), rs.count_executions)
            WHEN @Metric = ''CPU''         THEN CONVERT(decimal(38,12), rs.avg_cpu_time)        * CONVERT(decimal(38,12), rs.count_executions)
            ELSE                                 CONVERT(decimal(38,12), rs.avg_duration)        * CONVERT(decimal(38,12), rs.count_executions)
        END
      ) AS TotalMetric

    , CASE WHEN SUM(rs.count_executions) = 0 THEN 0
           ELSE
             SUM(
               CASE
                   WHEN @Metric = ''LogicalReads'' THEN CONVERT(decimal(38,12), rs.avg_logical_io_reads) * CONVERT(decimal(38,12), rs.count_executions)
                   WHEN @Metric = ''CPU''         THEN CONVERT(decimal(38,12), rs.avg_cpu_time)        * CONVERT(decimal(38,12), rs.count_executions)
                   ELSE                                 CONVERT(decimal(38,12), rs.avg_duration)        * CONVERT(decimal(38,12), rs.count_executions)
               END
             ) / SUM(rs.count_executions)
      END AS AvgMetric

    , SUM(CONVERT(decimal(38,12), rs.avg_duration) * CONVERT(decimal(38,12), rs.count_executions)) AS TotalDuration
    , CASE WHEN SUM(rs.count_executions)=0 THEN 0
           ELSE SUM(CONVERT(decimal(38,12), rs.avg_duration) * CONVERT(decimal(38,12), rs.count_executions)) / SUM(rs.count_executions)
      END AS AvgDuration

    , MIN(rsi.start_time) AS IntervalStartMin
    , {{INTERVAL_END_EXPR}} AS IntervalEndMax

    , CASE WHEN {{HAS_END_TIME}} = 1 THEN ''WEIGHTED_TOTAL''
           ELSE ''WEIGHTED_TOTAL;INTERVAL_END_FALLBACK''
      END AS ConfidenceNote
FROM {{DB}}.sys.query_store_query q
JOIN {{DB}}.sys.query_store_plan p
  ON p.query_id = q.query_id
JOIN {{DB}}.sys.query_store_runtime_stats rs
  ON rs.plan_id = p.plan_id
JOIN {{DB}}.sys.query_store_runtime_stats_interval rsi
  ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
JOIN {{DB}}.sys.query_store_query_text qt
  ON qt.query_text_id = q.query_text_id
LEFT JOIN {{DB}}.sys.objects obj
  ON obj.object_id = q.object_id
LEFT JOIN {{DB}}.sys.schemas sch
  ON sch.schema_id = obj.schema_id
WHERE 1=1
  AND (
        (@IncludeSP = 1 AND q.object_id > 0)
     OR (@IncludeAdhoc = 1 AND (q.object_id = 0 OR q.object_id IS NULL))
  )
  AND (@StartTime IS NULL OR rsi.start_time >= @StartTime)
  AND (@EndTime   IS NULL OR rsi.start_time <  @EndTime)
GROUP BY
      q.object_id, sch.name, obj.name
    , q.query_hash
    , qt.query_sql_text
    , CASE
          WHEN @GroupBy = ''QueryHash'' THEN CONVERT(nvarchar(100), q.query_hash, 1)
          WHEN @GroupBy = ''QueryText'' THEN qt.query_sql_text
          ELSE LOWER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(qt.query_sql_text, CHAR(13), '' ''), CHAR(10), '' ''), CHAR(9), '' ''))))
      END;
';

----------------------------------------------------------------------------------------
-- Build and execute per Lower/Higher DB
----------------------------------------------------------------------------------------
DECLARE @sqlL nvarchar(max) =
    REPLACE(
      REPLACE(
        REPLACE(@tmpl, N'{{DB}}', QUOTENAME(@DbLower)),
        N'{{HAS_END_TIME}}', CAST(@HasEndTime_L AS nvarchar(1))
      ),
      N'{{INTERVAL_END_EXPR}}',
      CASE WHEN @HasEndTime_L = 1 THEN N'MAX(rsi.end_time)' ELSE N'MAX(rsi.start_time)' END
    );

DECLARE @sqlH nvarchar(max) =
    REPLACE(
      REPLACE(
        REPLACE(@tmpl, N'{{DB}}', QUOTENAME(@DbHigher)),
        N'{{HAS_END_TIME}}', CAST(@HasEndTime_H AS nvarchar(1))
      ),
      N'{{INTERVAL_END_EXPR}}',
      CASE WHEN @HasEndTime_H = 1 THEN N'MAX(rsi.end_time)' ELSE N'MAX(rsi.start_time)' END
    );

EXEC sp_executesql
    @sqlL,
    N'@DbName sysname, @Metric sysname, @GroupBy sysname, @IncludeAdhoc bit, @IncludeSP bit, @StartTime datetime2(0), @EndTime datetime2(0)',
    @DbName=@DbLower, @Metric=@Metric, @GroupBy=@GroupBy, @IncludeAdhoc=@IncludeAdhoc, @IncludeSP=@IncludeSP, @StartTime=@StartTime, @EndTime=@EndTime;

EXEC sp_executesql
    @sqlH,
    N'@DbName sysname, @Metric sysname, @GroupBy sysname, @IncludeAdhoc bit, @IncludeSP bit, @StartTime datetime2(0), @EndTime datetime2(0)',
    @DbName=@DbHigher, @Metric=@Metric, @GroupBy=@GroupBy, @IncludeAdhoc=@IncludeAdhoc, @IncludeSP=@IncludeSP, @StartTime=@StartTime, @EndTime=@EndTime;

----------------------------------------------------------------------------------------
-- Compare (internal columns use _L/_H)
----------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#Compare') IS NOT NULL DROP TABLE #Compare;
CREATE TABLE #Compare
(
    QueryType            varchar(10)   NOT NULL,
    ObjName              sysname       NULL,
    GroupKeyHash         varbinary(32) NOT NULL,

    QueryHash_L          binary(8) NULL,
    QueryHash_H          binary(8) NULL,

    QueryIdRange_L       varchar(60) NULL,
    QueryIdRange_H       varchar(60) NULL,

    PlanCount_L          int NOT NULL,
    PlanCount_H          int NOT NULL,

    ExecCount_L          bigint NOT NULL,
    ExecCount_H          bigint NOT NULL,

    TotalMetric_L        decimal(38,6) NOT NULL,
    TotalMetric_H        decimal(38,6) NOT NULL,

    AvgMetric_L          decimal(38,6) NOT NULL,
    AvgMetric_H          decimal(38,6) NOT NULL,

    TotalDuration_L      decimal(38,6) NOT NULL,
    TotalDuration_H      decimal(38,6) NOT NULL,

    AvgDuration_L        decimal(38,6) NOT NULL,
    AvgDuration_H        decimal(38,6) NOT NULL,

    RegressionRatio      decimal(19,6) NULL,    -- H / L
    DeltaAvgMetric       decimal(38,6) NULL,    -- H - L
    ImpactScore          decimal(38,6) NULL,    -- (H-L)*Exec_H

    ConfidenceFlags      varchar(500) NULL,
    QueryTextSample      nvarchar(max) NULL
);

;WITH a AS (SELECT * FROM #QS_Agg WHERE SourceDb = @DbLower),
      b AS (SELECT * FROM #QS_Agg WHERE SourceDb = @DbHigher)
INSERT #Compare
SELECT
      COALESCE(b.QueryType, a.QueryType) AS QueryType
    , COALESCE(b.ObjName, a.ObjName)     AS ObjName
    , COALESCE(b.GroupKeyHash, a.GroupKeyHash) AS GroupKeyHash

    , a.QueryHash AS QueryHash_L
    , b.QueryHash AS QueryHash_H

    , CASE WHEN a.QueryIdMin IS NULL THEN NULL
           ELSE CAST(a.QueryIdMin AS varchar(30)) + '-' + CAST(a.QueryIdMax AS varchar(30))
      END AS QueryIdRange_L
    , CASE WHEN b.QueryIdMin IS NULL THEN NULL
           ELSE CAST(b.QueryIdMin AS varchar(30)) + '-' + CAST(b.QueryIdMax AS varchar(30))
      END AS QueryIdRange_H

    , ISNULL(a.PlanCount,0) AS PlanCount_L
    , ISNULL(b.PlanCount,0) AS PlanCount_H

    , ISNULL(a.ExecCount,0) AS ExecCount_L
    , ISNULL(b.ExecCount,0) AS ExecCount_H

    , ISNULL(a.TotalMetric,0) AS TotalMetric_L
    , ISNULL(b.TotalMetric,0) AS TotalMetric_H

    , ISNULL(a.AvgMetric,0) AS AvgMetric_L
    , ISNULL(b.AvgMetric,0) AS AvgMetric_H

    , ISNULL(a.TotalDuration,0) AS TotalDuration_L
    , ISNULL(b.TotalDuration,0) AS TotalDuration_H

    , ISNULL(a.AvgDuration,0) AS AvgDuration_L
    , ISNULL(b.AvgDuration,0) AS AvgDuration_H

    , CASE WHEN NULLIF(a.AvgMetric,0) IS NULL THEN NULL ELSE b.AvgMetric / NULLIF(a.AvgMetric,0) END AS RegressionRatio
    , (ISNULL(b.AvgMetric,0) - ISNULL(a.AvgMetric,0)) AS DeltaAvgMetric
    , CASE
          WHEN NULLIF(a.AvgMetric,0) IS NULL THEN NULL
          ELSE (ISNULL(b.AvgMetric,0) - ISNULL(a.AvgMetric,0)) * CONVERT(decimal(38,6), ISNULL(b.ExecCount,0))
      END AS ImpactScore

    , CAST(
          COALESCE(
              CASE WHEN ISNULL(a.ExecCount,0)=0 OR ISNULL(b.ExecCount,0)=0
                   THEN 'MISSING_ONE_SIDE;' ELSE '' END
          , '') +
          COALESCE(
              CASE WHEN @MinExecCount IS NOT NULL
                     AND (ISNULL(a.ExecCount,0)<@MinExecCount OR ISNULL(b.ExecCount,0)<@MinExecCount)
                   THEN 'LOW_EXEC;' ELSE '' END
          , '') +
          COALESCE(
              CASE WHEN ISNULL(a.PlanCount,0)>1 OR ISNULL(b.PlanCount,0)>1
                   THEN 'MULTI_PLAN;' ELSE '' END
          , '') +
          COALESCE(a.ConfidenceNote + ';', '') +
          COALESCE(b.ConfidenceNote + ';', '')
      AS varchar(500)) AS ConfidenceFlags

    , COALESCE(b.QueryTextSample, a.QueryTextSample) AS QueryTextSample
FROM a
FULL OUTER JOIN b
  ON a.GroupKeyHash = b.GroupKeyHash
 AND ISNULL(a.QueryType,'') = ISNULL(b.QueryType,'')
 AND ISNULL(a.ObjName,'')   = ISNULL(b.ObjName,'');

----------------------------------------------------------------------------------------
-- Resultset #1: Simplified output (ONLY new pair columns) + 2 decimals + ' - ' separator
----------------------------------------------------------------------------------------
DECLARE @out1 nvarchar(max) = N'
;WITH filtered AS
(
    SELECT *
    FROM #Compare
    WHERE RegressionRatio IS NOT NULL
      AND AvgMetric_H > AvgMetric_L
      AND (@MinExecCount IS NULL OR (ExecCount_L >= @MinExecCount AND ExecCount_H >= @MinExecCount))
      AND (@MinRegressionRatio IS NULL OR RegressionRatio >= @MinRegressionRatio)
)
SELECT TOP (CASE WHEN @TopN IS NULL THEN 2147483647 ELSE @TopN END)
      QueryType
    , ObjName
    , CONVERT(varchar(66), GroupKeyHash, 1) AS GroupKeyHashHex

    , CONCAT(COALESCE(QueryIdRange_L,''?''), '' - '', COALESCE(QueryIdRange_H,''?'')) AS [QueryIdRange_L-H]
    , CONCAT(COALESCE(CONVERT(varchar(50), QueryHash_L, 1), ''?''), '' - '', COALESCE(CONVERT(varchar(50), QueryHash_H, 1), ''?'')) AS [QueryHashHex_L-H]

    , CONCAT(CONVERT(varchar(30), PlanCount_L), '' - '', CONVERT(varchar(30), PlanCount_H)) AS [PlanCount_L-H]
    , CONCAT(CONVERT(varchar(30), ExecCount_L), '' - '', CONVERT(varchar(30), ExecCount_H)) AS [ExecCount_L-H]

    , CONCAT(
          CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(TotalMetric_L, 2))),
          '' - '',
          CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(TotalMetric_H, 2)))
      ) AS ' + QUOTENAME('Total' + @Metric + '_L-H') + N'

    , CONCAT(
          CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(AvgMetric_L, 2))),
          '' - '',
          CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(AvgMetric_H, 2)))
      ) AS ' + QUOTENAME('Avg' + @Metric + '_L-H') + N'

    , CONCAT(
          CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(TotalDuration_L, 2))),
          '' - '',
          CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(TotalDuration_H, 2)))
      ) AS [TotalDuration_L-H]

    , CONCAT(
          CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(AvgDuration_L, 2))),
          '' - '',
          CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(AvgDuration_H, 2)))
      ) AS [AvgDuration_L-H]

    , CONVERT(decimal(19,2), ROUND(RegressionRatio, 2)) AS RegressionRatio
    , CONVERT(decimal(38,2), ROUND(DeltaAvgMetric, 2))  AS DeltaAvgMetric
    , CONVERT(decimal(38,2), ROUND(ImpactScore, 2))     AS ImpactScore
    , ConfidenceFlags
    , QueryTextSample
FROM filtered
ORDER BY ImpactScore DESC;
';

EXEC sp_executesql
    @out1,
    N'@TopN int, @MinExecCount bigint, @MinRegressionRatio decimal(9,4)',
    @TopN=@TopN, @MinExecCount=@MinExecCount, @MinRegressionRatio=@MinRegressionRatio;

----------------------------------------------------------------------------------------
-- Resultset #2: Summary (2 decimals on decimal aggregates)
----------------------------------------------------------------------------------------
;WITH filtered AS
(
    SELECT *
    FROM #Compare
    WHERE RegressionRatio IS NOT NULL
      AND AvgMetric_H > AvgMetric_L
      AND (@MinExecCount IS NULL OR (ExecCount_L >= @MinExecCount AND ExecCount_H >= @MinExecCount))
      AND (@MinRegressionRatio IS NULL OR RegressionRatio >= @MinRegressionRatio)
)
SELECT
      @DbLower   AS LowerDb
    , @DbHigher  AS HigherDb
    , @LabelLower  AS LowerCL
    , @LabelHigher AS HigherCL
    , @Metric  AS Metric
    , @GroupBy AS GroupBy
    , COUNT(*) AS RegressionCount
    , SUM(CASE WHEN ConfidenceFlags LIKE '%MULTI_PLAN%' THEN 1 ELSE 0 END) AS MultiPlanCount
    , CONVERT(decimal(38,2), ROUND(SUM(ImpactScore), 2)) AS SumImpactScore
    , CONVERT(decimal(38,2), ROUND(MAX(ImpactScore), 2)) AS MaxImpactScore
    , CONVERT(decimal(19,2), ROUND(AVG(RegressionRatio), 2)) AS AvgRegressionRatio
FROM filtered;

----------------------------------------------------------------------------------------
-- Resultset #3 + #4: MULTI-PLAN only
----------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#MultiGroups') IS NOT NULL DROP TABLE #MultiGroups;
CREATE TABLE #MultiGroups
(
    GroupKeyHash varbinary(32) NOT NULL,
    QueryType    varchar(10)   NOT NULL,
    ObjName      sysname       NULL
);

;WITH filtered AS
(
    SELECT *
    FROM #Compare
    WHERE RegressionRatio IS NOT NULL
      AND AvgMetric_H > AvgMetric_L
      AND (@MinExecCount IS NULL OR (ExecCount_L >= @MinExecCount AND ExecCount_H >= @MinExecCount))
      AND (@MinRegressionRatio IS NULL OR RegressionRatio >= @MinRegressionRatio)
)
INSERT #MultiGroups (GroupKeyHash, QueryType, ObjName)
SELECT DISTINCT
      f.GroupKeyHash
    , f.QueryType
    , f.ObjName
FROM filtered f
WHERE f.ConfidenceFlags LIKE '%MULTI_PLAN%';

IF EXISTS (SELECT 1 FROM #MultiGroups)
BEGIN
    ----------------------------------------------------------------------------------------
    -- Plan-level aggregation for multi-plan groups
    ----------------------------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#PlanAgg') IS NOT NULL DROP TABLE #PlanAgg;
    CREATE TABLE #PlanAgg
    (
        SourceDb         sysname        NOT NULL,
        QueryType        varchar(10)    NOT NULL,
        ObjName          sysname        NULL,
        GroupKeyHash     varbinary(32)  NOT NULL,

        PlanId           bigint         NOT NULL,
        QueryId          bigint         NULL,

        ExecCount        bigint         NOT NULL,
        TotalMetric      decimal(38,6)  NOT NULL,
        AvgMetric        decimal(38,6)  NOT NULL,

        TotalDuration    decimal(38,6)  NOT NULL,
        AvgDuration      decimal(38,6)  NOT NULL,

        IntervalStartMin datetime2(0)   NULL,
        IntervalEndMax   datetime2(0)   NULL,

        PlanXmlHash      varbinary(32)  NULL
    );

    DECLARE @planTmpl nvarchar(max) = N'
    INSERT INTO #PlanAgg
    (
        SourceDb, QueryType, ObjName, GroupKeyHash,
        PlanId, QueryId,
        ExecCount, TotalMetric, AvgMetric,
        TotalDuration, AvgDuration,
        IntervalStartMin, IntervalEndMax,
        PlanXmlHash
    )
    SELECT
          @DbName AS SourceDb
        , CASE WHEN q.object_id > 0 THEN ''SP'' ELSE ''Adhoc'' END AS QueryType
        , CASE WHEN q.object_id > 0 THEN sch.name + ''.'' + obj.name ELSE NULL END AS ObjName
        , HASHBYTES(''SHA2_256'', CONVERT(varbinary(max),
              CASE
                  WHEN @GroupBy = ''QueryHash'' THEN CONVERT(nvarchar(100), q.query_hash, 1)
                  WHEN @GroupBy = ''QueryText'' THEN qt.query_sql_text
                  ELSE LOWER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(qt.query_sql_text, CHAR(13), '' ''), CHAR(10), '' ''), CHAR(9), '' ''))))
              END
          )) AS GroupKeyHash

        , p.plan_id AS PlanId
        , q.query_id AS QueryId

        , SUM(rs.count_executions) AS ExecCount

        , SUM(
            CASE
                WHEN @Metric = ''LogicalReads'' THEN CONVERT(decimal(38,12), rs.avg_logical_io_reads) * CONVERT(decimal(38,12), rs.count_executions)
                WHEN @Metric = ''CPU''         THEN CONVERT(decimal(38,12), rs.avg_cpu_time)        * CONVERT(decimal(38,12), rs.count_executions)
                ELSE                                 CONVERT(decimal(38,12), rs.avg_duration)        * CONVERT(decimal(38,12), rs.count_executions)
            END
          ) AS TotalMetric

        , CASE WHEN SUM(rs.count_executions)=0 THEN 0
               ELSE
                 SUM(
                   CASE
                       WHEN @Metric = ''LogicalReads'' THEN CONVERT(decimal(38,12), rs.avg_logical_io_reads) * CONVERT(decimal(38,12), rs.count_executions)
                       WHEN @Metric = ''CPU''         THEN CONVERT(decimal(38,12), rs.avg_cpu_time)        * CONVERT(decimal(38,12), rs.count_executions)
                       ELSE                                 CONVERT(decimal(38,12), rs.avg_duration)        * CONVERT(decimal(38,12), rs.count_executions)
                   END
                 ) / SUM(rs.count_executions)
          END AS AvgMetric

        , SUM(CONVERT(decimal(38,12), rs.avg_duration) * CONVERT(decimal(38,12), rs.count_executions)) AS TotalDuration
        , CASE WHEN SUM(rs.count_executions)=0 THEN 0
               ELSE SUM(CONVERT(decimal(38,12), rs.avg_duration) * CONVERT(decimal(38,12), rs.count_executions)) / SUM(rs.count_executions)
          END AS AvgDuration

        , MIN(rsi.start_time) AS IntervalStartMin
        , {{INTERVAL_END_EXPR}} AS IntervalEndMax

        , HASHBYTES(''SHA2_256'', CONVERT(varbinary(max), CONVERT(nvarchar(max), p.query_plan))) AS PlanXmlHash
    FROM {{DB}}.sys.query_store_query q
    JOIN {{DB}}.sys.query_store_plan p
      ON p.query_id = q.query_id
    JOIN {{DB}}.sys.query_store_runtime_stats rs
      ON rs.plan_id = p.plan_id
    JOIN {{DB}}.sys.query_store_runtime_stats_interval rsi
      ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
    JOIN {{DB}}.sys.query_store_query_text qt
      ON qt.query_text_id = q.query_text_id
    LEFT JOIN {{DB}}.sys.objects obj
      ON obj.object_id = q.object_id
    LEFT JOIN {{DB}}.sys.schemas sch
      ON sch.schema_id = obj.schema_id
    WHERE 1=1
      AND (
            (@IncludeSP = 1 AND q.object_id > 0)
         OR (@IncludeAdhoc = 1 AND (q.object_id = 0 OR q.object_id IS NULL))
      )
      AND (@StartTime IS NULL OR rsi.start_time >= @StartTime)
      AND (@EndTime   IS NULL OR rsi.start_time <  @EndTime)
      AND EXISTS
      (
          SELECT 1
          FROM #MultiGroups mg
          WHERE mg.GroupKeyHash = HASHBYTES(''SHA2_256'', CONVERT(varbinary(max),
                CASE
                    WHEN @GroupBy = ''QueryHash'' THEN CONVERT(nvarchar(100), q.query_hash, 1)
                    WHEN @GroupBy = ''QueryText'' THEN qt.query_sql_text
                    ELSE LOWER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(qt.query_sql_text, CHAR(13), '' ''), CHAR(10), '' ''), CHAR(9), '' ''))))
                END
          ))
            AND mg.QueryType = CASE WHEN q.object_id > 0 THEN ''SP'' ELSE ''Adhoc'' END
            AND ISNULL(mg.ObjName, '''') = ISNULL(CASE WHEN q.object_id > 0 THEN sch.name + ''.'' + obj.name ELSE NULL END, '''')
      )
    GROUP BY
          q.object_id, sch.name, obj.name
        , q.query_hash
        , qt.query_sql_text
        , CASE
              WHEN @GroupBy = ''QueryHash'' THEN CONVERT(nvarchar(100), q.query_hash, 1)
              WHEN @GroupBy = ''QueryText'' THEN qt.query_sql_text
              ELSE LOWER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(qt.query_sql_text, CHAR(13), '' ''), CHAR(10), '' ''), CHAR(9), '' ''))))
          END
        , p.plan_id
        , q.query_id
        , p.query_plan;
    ';

    DECLARE @planSqlL nvarchar(max) =
        REPLACE(
          REPLACE(@planTmpl, N'{{DB}}', QUOTENAME(@DbLower)),
          N'{{INTERVAL_END_EXPR}}',
          CASE WHEN @HasEndTime_L = 1 THEN N'MAX(rsi.end_time)' ELSE N'MAX(rsi.start_time)' END
        );

    DECLARE @planSqlH nvarchar(max) =
        REPLACE(
          REPLACE(@planTmpl, N'{{DB}}', QUOTENAME(@DbHigher)),
          N'{{INTERVAL_END_EXPR}}',
          CASE WHEN @HasEndTime_H = 1 THEN N'MAX(rsi.end_time)' ELSE N'MAX(rsi.start_time)' END
        );

    EXEC sp_executesql
        @planSqlL,
        N'@DbName sysname, @Metric sysname, @GroupBy sysname, @IncludeAdhoc bit, @IncludeSP bit, @StartTime datetime2(0), @EndTime datetime2(0)',
        @DbName=@DbLower, @Metric=@Metric, @GroupBy=@GroupBy, @IncludeAdhoc=@IncludeAdhoc, @IncludeSP=@IncludeSP, @StartTime=@StartTime, @EndTime=@EndTime;

    EXEC sp_executesql
        @planSqlH,
        N'@DbName sysname, @Metric sysname, @GroupBy sysname, @IncludeAdhoc bit, @IncludeSP bit, @StartTime datetime2(0), @EndTime datetime2(0)',
        @DbName=@DbHigher, @Metric=@Metric, @GroupBy=@GroupBy, @IncludeAdhoc=@IncludeAdhoc, @IncludeSP=@IncludeSP, @StartTime=@StartTime, @EndTime=@EndTime;

    ----------------------------------------------------------------------------------------
    -- Resultset #3 output (plan drilldown) - 2 decimals for decimal columns
    ----------------------------------------------------------------------------------------
    ;WITH ranked AS
    (
        SELECT
              pa.SourceDb
            , CASE WHEN pa.SourceDb = @DbLower THEN @LabelLower ELSE @LabelHigher END AS SourceCL
            , CASE WHEN pa.SourceDb = @DbLower THEN 'L' ELSE 'H' END AS Side
            , pa.QueryType
            , pa.ObjName
            , CONVERT(varchar(66), pa.GroupKeyHash, 1) AS GroupKeyHashHex
            , pa.PlanId
            , pa.QueryId
            , pa.ExecCount
            , CONVERT(decimal(38,2), ROUND(pa.TotalMetric, 2))   AS TotalMetric
            , CONVERT(decimal(38,2), ROUND(pa.AvgMetric, 2))     AS AvgMetric
            , CONVERT(decimal(38,2), ROUND(pa.TotalDuration, 2)) AS TotalDuration
            , CONVERT(decimal(38,2), ROUND(pa.AvgDuration, 2))   AS AvgDuration
            , pa.IntervalStartMin
            , pa.IntervalEndMax
            , CONVERT(varchar(66), pa.PlanXmlHash, 1) AS PlanXmlHashHex
            , DENSE_RANK() OVER (PARTITION BY pa.SourceDb, pa.GroupKeyHash ORDER BY pa.AvgMetric DESC) AS RankByAvgMetric
            , DENSE_RANK() OVER (PARTITION BY pa.SourceDb, pa.GroupKeyHash ORDER BY pa.ExecCount DESC) AS RankByExecCount
        FROM #PlanAgg pa
    )
    SELECT
          SourceDb
        , SourceCL
        , Side
        , QueryType
        , ObjName
        , GroupKeyHashHex
        , PlanId
        , QueryId
        , ExecCount
        , TotalMetric
        , AvgMetric
        , TotalDuration
        , AvgDuration
        , IntervalStartMin
        , IntervalEndMax
        , PlanXmlHashHex
        , RankByAvgMetric
        , RankByExecCount
    FROM ranked
    ORDER BY
          GroupKeyHashHex
        , Side
        , RankByAvgMetric
        , RankByExecCount;

    ----------------------------------------------------------------------------------------
    -- Resultset #4: Dominant plan XML diff (SIMPLIFIED) + 2 decimals + ' - ' separator
    ----------------------------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#DominantPlans') IS NOT NULL DROP TABLE #DominantPlans;
    CREATE TABLE #DominantPlans
    (
        SourceDb     sysname       NOT NULL,
        GroupKeyHash varbinary(32) NOT NULL,
        QueryType    varchar(10)   NOT NULL,
        ObjName      sysname       NULL,
        PlanId       bigint        NOT NULL,
        QueryId      bigint        NULL,
        ExecCount    bigint        NOT NULL,
        AvgMetric    decimal(38,6) NOT NULL,
        TotalMetric  decimal(38,6) NOT NULL
    );

    ;WITH r AS
    (
        SELECT
              pa.SourceDb
            , pa.GroupKeyHash
            , pa.QueryType
            , pa.ObjName
            , pa.PlanId
            , pa.QueryId
            , pa.ExecCount
            , pa.AvgMetric
            , pa.TotalMetric
            , ROW_NUMBER() OVER
              (
                PARTITION BY pa.SourceDb, pa.GroupKeyHash
                ORDER BY pa.ExecCount DESC, pa.AvgMetric DESC, pa.PlanId DESC
              ) AS rn
        FROM #PlanAgg pa
    )
    INSERT #DominantPlans
    SELECT
          SourceDb, GroupKeyHash, QueryType, ObjName
        , PlanId, QueryId, ExecCount, AvgMetric, TotalMetric
    FROM r
    WHERE rn = 1;

    IF OBJECT_ID('tempdb..#DomPairs') IS NOT NULL DROP TABLE #DomPairs;
    CREATE TABLE #DomPairs
    (
        GroupKeyHash  varbinary(32) NOT NULL,
        QueryType     varchar(10)   NOT NULL,
        ObjName       sysname       NULL,

        PlanId_L      bigint        NULL,
        QueryId_L     bigint        NULL,
        Exec_L        bigint        NULL,
        AvgM_L        decimal(38,6) NULL,

        PlanId_H      bigint        NULL,
        QueryId_H     bigint        NULL,
        Exec_H        bigint        NULL,
        AvgM_H        decimal(38,6) NULL
    );

    INSERT #DomPairs
    SELECT
          COALESCE(b.GroupKeyHash, a.GroupKeyHash) AS GroupKeyHash
        , COALESCE(b.QueryType, a.QueryType)       AS QueryType
        , COALESCE(b.ObjName, a.ObjName)           AS ObjName

        , a.PlanId    AS PlanId_L
        , a.QueryId   AS QueryId_L
        , a.ExecCount AS Exec_L
        , a.AvgMetric AS AvgM_L

        , b.PlanId    AS PlanId_H
        , b.QueryId   AS QueryId_H
        , b.ExecCount AS Exec_H
        , b.AvgMetric AS AvgM_H
    FROM (SELECT * FROM #DominantPlans WHERE SourceDb = @DbLower) a
    FULL OUTER JOIN (SELECT * FROM #DominantPlans WHERE SourceDb = @DbHigher) b
      ON a.GroupKeyHash = b.GroupKeyHash
     AND ISNULL(a.QueryType,'') = ISNULL(b.QueryType,'')
     AND ISNULL(a.ObjName,'')   = ISNULL(b.ObjName,'');

    IF OBJECT_ID('tempdb..#PlanXml') IS NOT NULL DROP TABLE #PlanXml;
    CREATE TABLE #PlanXml
    (
        SourceDb      sysname       NOT NULL,
        GroupKeyHash  varbinary(32) NOT NULL,
        QueryType     varchar(10)   NOT NULL,
        ObjName       sysname       NULL,
        PlanId        bigint        NOT NULL,
        QueryId       bigint        NULL,
        QueryPlanXml  xml           NULL,
        QueryPlanText nvarchar(max) NULL
    );

    DECLARE @px nvarchar(max);

    SET @px = N'
    INSERT #PlanXml (SourceDb, GroupKeyHash, QueryType, ObjName, PlanId, QueryId, QueryPlanXml, QueryPlanText)
    SELECT
          @DbName
        , dp.GroupKeyHash
        , dp.QueryType
        , dp.ObjName
        , dp.PlanId_L
        , dp.QueryId_L
        , p.query_plan
        , CONVERT(nvarchar(max), p.query_plan)
    FROM ' + QUOTENAME(@DbLower) + N'.sys.query_store_plan p
    JOIN #DomPairs dp
      ON dp.PlanId_L = p.plan_id
     AND (dp.QueryId_L IS NULL OR dp.QueryId_L = p.query_id)
    WHERE dp.PlanId_L IS NOT NULL;';
    EXEC sp_executesql @px, N'@DbName sysname', @DbName=@DbLower;

    SET @px = N'
    INSERT #PlanXml (SourceDb, GroupKeyHash, QueryType, ObjName, PlanId, QueryId, QueryPlanXml, QueryPlanText)
    SELECT
          @DbName
        , dp.GroupKeyHash
        , dp.QueryType
        , dp.ObjName
        , dp.PlanId_H
        , dp.QueryId_H
        , p.query_plan
        , CONVERT(nvarchar(max), p.query_plan)
    FROM ' + QUOTENAME(@DbHigher) + N'.sys.query_store_plan p
    JOIN #DomPairs dp
      ON dp.PlanId_H = p.plan_id
     AND (dp.QueryId_H IS NULL OR dp.QueryId_H = p.query_id)
    WHERE dp.PlanId_H IS NOT NULL;';
    EXEC sp_executesql @px, N'@DbName sysname', @DbName=@DbHigher;

    IF OBJECT_ID('tempdb..#PlanInd') IS NOT NULL DROP TABLE #PlanInd;
    CREATE TABLE #PlanInd
    (
        SourceDb          sysname       NOT NULL,
        GroupKeyHash      varbinary(32) NOT NULL,
        QueryType         varchar(10)   NOT NULL,
        ObjName           sysname       NULL,
        PlanId            bigint        NOT NULL,
        QueryId           bigint        NULL,

        QueryPlanXml      xml           NULL,
        PlanXmlHash       varbinary(32) NULL,

        IndexSeekCount    int NULL,
        IndexScanCount    int NULL,
        TableScanCount    int NULL,

        HasHashJoin       bit NULL,
        HasMergeJoin      bit NULL,
        HasNestedLoops    bit NULL,
        HasParallelism    bit NULL,

        GrantedMemoryKB   bigint NULL,
        HasSpillToTempDb  bit NULL,
        HasMissingIndex   bit NULL
    );

    ;WITH x AS
    (
        SELECT
              px.SourceDb
            , px.GroupKeyHash
            , px.QueryType
            , px.ObjName
            , px.PlanId
            , px.QueryId
            , px.QueryPlanXml AS PlanXml
            , px.QueryPlanText
        FROM #PlanXml px
    )
    INSERT #PlanInd
    SELECT
          x.SourceDb
        , x.GroupKeyHash
        , x.QueryType
        , x.ObjName
        , x.PlanId
        , x.QueryId
        , x.PlanXml AS QueryPlanXml
        , CASE WHEN x.QueryPlanText IS NULL THEN NULL
               ELSE HASHBYTES('SHA2_256', CONVERT(varbinary(max), x.QueryPlanText))
          END AS PlanXmlHash

        , v.IndexSeekCount
        , v.IndexScanCount
        , v.TableScanCount

        , v.HasHashJoin
        , v.HasMergeJoin
        , v.HasNestedLoops
        , v.HasParallelism

        , v.GrantedMemoryKB
        , v.HasSpillToTempDb
        , v.HasMissingIndex
    FROM x
    OUTER APPLY
    (
        SELECT
              IndexSeekCount =
                  CASE WHEN x.PlanXml IS NULL THEN NULL
                       ELSE (SELECT COUNT(*) FROM x.PlanXml.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Index Seek"]') AS t(n))
                  END
            , IndexScanCount =
                  CASE WHEN x.PlanXml IS NULL THEN NULL
                       ELSE (SELECT COUNT(*) FROM x.PlanXml.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Index Scan"]') AS t(n))
                  END
            , TableScanCount =
                  CASE WHEN x.PlanXml IS NULL THEN NULL
                       ELSE (SELECT COUNT(*) FROM x.PlanXml.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Table Scan"]') AS t(n))
                  END
            , HasHashJoin =
                  CASE WHEN x.PlanXml IS NULL THEN NULL
                       WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Hash Match"]') = 1 THEN 1 ELSE 0 END
            , HasMergeJoin =
                  CASE WHEN x.PlanXml IS NULL THEN NULL
                       WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Merge Join"]') = 1 THEN 1 ELSE 0 END
            , HasNestedLoops =
                  CASE WHEN x.PlanXml IS NULL THEN NULL
                       WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Nested Loops"]') = 1 THEN 1 ELSE 0 END
            , HasParallelism =
                  CASE WHEN x.PlanXml IS NULL THEN NULL
                       WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Parallelism"]') = 1 THEN 1 ELSE 0 END
            , GrantedMemoryKB =
                  CASE WHEN x.PlanXml IS NULL THEN NULL
                       ELSE TRY_CONVERT(bigint,
                           x.PlanXml.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
                                            (//MemoryGrantInfo/@GrantedMemory)[1]', 'nvarchar(50)')
                         )
                  END
            , HasSpillToTempDb =
                  CASE WHEN x.PlanXml IS NULL THEN NULL
                       WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //Warnings/SpillToTempDb') = 1 THEN 1 ELSE 0 END
            , HasMissingIndex =
                  CASE WHEN x.PlanXml IS NULL THEN NULL
                       WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //MissingIndexes') = 1 THEN 1 ELSE 0 END
    ) v;

    DECLARE @out4 nvarchar(max) = N'
    ;WITH c AS
    (
        SELECT *
        FROM #Compare
        WHERE RegressionRatio IS NOT NULL
          AND AvgMetric_H > AvgMetric_L
          AND (@MinExecCount IS NULL OR (ExecCount_L >= @MinExecCount AND ExecCount_H >= @MinExecCount))
          AND (@MinRegressionRatio IS NULL OR RegressionRatio >= @MinRegressionRatio)
          AND ConfidenceFlags LIKE ''%MULTI_PLAN%''
    ),
    pL AS (SELECT * FROM #PlanInd WHERE SourceDb = @DbLower),
    pH AS (SELECT * FROM #PlanInd WHERE SourceDb = @DbHigher)
    SELECT
          c.QueryType
        , c.ObjName
        , CONVERT(varchar(66), c.GroupKeyHash, 1) AS GroupKeyHashHex

        , CONVERT(decimal(38,2), ROUND(c.ImpactScore, 2))     AS ImpactScore
        , CONVERT(decimal(19,2), ROUND(c.RegressionRatio, 2)) AS RegressionRatio

        , CONCAT(COALESCE(CONVERT(varchar(30), c.ExecCount_L), ''?''), '' - '', COALESCE(CONVERT(varchar(30), c.ExecCount_H), ''?'')) AS [ExecCount_L-H]
        , CONCAT(
              COALESCE(CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(c.AvgMetric_L, 2))), ''?''), '' - '',
              COALESCE(CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(c.AvgMetric_H, 2))), ''?'')
          ) AS [AvgMetric_L-H]

        , CONCAT(COALESCE(CONVERT(varchar(30), dp.PlanId_L), ''?''), '' - '', COALESCE(CONVERT(varchar(30), dp.PlanId_H), ''?'')) AS [DominantPlanId_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(30), dp.QueryId_L), ''?''), '' - '', COALESCE(CONVERT(varchar(30), dp.QueryId_H), ''?'')) AS [DominantQueryId_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(30), dp.Exec_L), ''?''), '' - '', COALESCE(CONVERT(varchar(30), dp.Exec_H), ''?'')) AS [DominantPlanExec_L-H]
        , CONCAT(
              COALESCE(CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(dp.AvgM_L, 2))), ''?''), '' - '',
              COALESCE(CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(dp.AvgM_H, 2))), ''?'')
          ) AS [DominantPlanAvgMetric_L-H]

        , CONCAT(
            COALESCE(CONVERT(varchar(66), pL.PlanXmlHash, 1), ''?''),
            '' - '',
            COALESCE(CONVERT(varchar(66), pH.PlanXmlHash, 1), ''?'')
          ) AS [PlanXmlHashHex_L-H]

        , CONCAT(COALESCE(CONVERT(varchar(20), pL.IndexSeekCount), ''?''), '' - '', COALESCE(CONVERT(varchar(20), pH.IndexSeekCount), ''?'')) AS [IndexSeekCount_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(20), pL.IndexScanCount), ''?''), '' - '', COALESCE(CONVERT(varchar(20), pH.IndexScanCount), ''?'')) AS [IndexScanCount_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(20), pL.TableScanCount), ''?''), '' - '', COALESCE(CONVERT(varchar(20), pH.TableScanCount), ''?'')) AS [TableScanCount_L-H]

        , CONCAT(COALESCE(CONVERT(varchar(1), pL.HasHashJoin),    ''?''), '' - '', COALESCE(CONVERT(varchar(1), pH.HasHashJoin),    ''?'')) AS [HasHashJoin_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(1), pL.HasMergeJoin),   ''?''), '' - '', COALESCE(CONVERT(varchar(1), pH.HasMergeJoin),   ''?'')) AS [HasMergeJoin_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(1), pL.HasNestedLoops), ''?''), '' - '', COALESCE(CONVERT(varchar(1), pH.HasNestedLoops), ''?'')) AS [HasNestedLoops_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(1), pL.HasParallelism), ''?''), '' - '', COALESCE(CONVERT(varchar(1), pH.HasParallelism), ''?'')) AS [HasParallelism_L-H]

        , CONCAT(COALESCE(CONVERT(varchar(30), pL.GrantedMemoryKB), ''?''), '' - '', COALESCE(CONVERT(varchar(30), pH.GrantedMemoryKB), ''?'')) AS [GrantedMemoryKB_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(1), pL.HasSpillToTempDb), ''?''), '' - '', COALESCE(CONVERT(varchar(1), pH.HasSpillToTempDb), ''?'')) AS [SpillToTempDb_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(1), pL.HasMissingIndex),  ''?''), '' - '', COALESCE(CONVERT(varchar(1), pH.HasMissingIndex),  ''?'')) AS [MissingIndex_L-H]

        , CONCAT(
              CASE WHEN pL.PlanXmlHash IS NOT NULL AND pH.PlanXmlHash IS NOT NULL AND pL.PlanXmlHash <> pH.PlanXmlHash THEN ''PLAN_SHAPE_CHANGED;'' ELSE '''' END,
              CASE WHEN ISNULL(pL.HasHashJoin,0)    <> ISNULL(pH.HasHashJoin,0)    THEN ''JOIN_HASH_CHANGED;'' ELSE '''' END,
              CASE WHEN ISNULL(pL.HasMergeJoin,0)   <> ISNULL(pH.HasMergeJoin,0)   THEN ''JOIN_MERGE_CHANGED;'' ELSE '''' END,
              CASE WHEN ISNULL(pL.HasNestedLoops,0) <> ISNULL(pH.HasNestedLoops,0) THEN ''JOIN_NL_CHANGED;'' ELSE '''' END,
              CASE WHEN ISNULL(pL.IndexSeekCount,0) <> ISNULL(pH.IndexSeekCount,0) THEN ''INDEX_SEEK_COUNT_CHANGED;'' ELSE '''' END,
              CASE WHEN ISNULL(pL.IndexScanCount,0) <> ISNULL(pH.IndexScanCount,0) THEN ''INDEX_SCAN_COUNT_CHANGED;'' ELSE '''' END,
              CASE WHEN ISNULL(pL.TableScanCount,0) <> ISNULL(pH.TableScanCount,0) THEN ''TABLE_SCAN_COUNT_CHANGED;'' ELSE '''' END,
              CASE WHEN ISNULL(pL.HasParallelism,0) <> ISNULL(pH.HasParallelism,0) THEN ''PARALLELISM_CHANGED;'' ELSE '''' END,
              CASE WHEN ISNULL(pL.HasSpillToTempDb,0) <> ISNULL(pH.HasSpillToTempDb,0) THEN ''SPILL_CHANGED;'' ELSE '''' END,
              CASE WHEN ISNULL(pL.GrantedMemoryKB,-1) <> ISNULL(pH.GrantedMemoryKB,-1) THEN ''GRANT_CHANGED;'' ELSE '''' END
          ) AS DiffFlags

        , c.QueryTextSample
        , pL.QueryPlanXml AS ' + QUOTENAME('PlanXml_' + @LabelLower) + N'
        , pH.QueryPlanXml AS ' + QUOTENAME('PlanXml_' + @LabelHigher) + N'
    FROM c
    JOIN #DomPairs dp
      ON dp.GroupKeyHash = c.GroupKeyHash
     AND ISNULL(dp.QueryType,'''') = ISNULL(c.QueryType,'''')
     AND ISNULL(dp.ObjName,'''')   = ISNULL(c.ObjName,'''')
    LEFT JOIN pL
      ON pL.GroupKeyHash = c.GroupKeyHash
     AND ISNULL(pL.QueryType,'''') = ISNULL(c.QueryType,'''')
     AND ISNULL(pL.ObjName,'''')   = ISNULL(c.ObjName,'''')
     AND pL.PlanId = dp.PlanId_L
    LEFT JOIN pH
      ON pH.GroupKeyHash = c.GroupKeyHash
     AND ISNULL(pH.QueryType,'''') = ISNULL(c.QueryType,'''')
     AND ISNULL(pH.ObjName,'''')   = ISNULL(c.ObjName,'''')
     AND pH.PlanId = dp.PlanId_H
    ORDER BY c.ImpactScore DESC;
    ';

    EXEC sp_executesql
        @out4,
        N'@DbLower sysname, @DbHigher sysname, @MinExecCount bigint, @MinRegressionRatio decimal(9,4)',
        @DbLower=@DbLower, @DbHigher=@DbHigher, @MinExecCount=@MinExecCount, @MinRegressionRatio=@MinRegressionRatio;
END;

----------------------------------------------------------------------------------------
-- Persist results (DROP + CREATE each run)
-- NOTE: Persist still stores numeric columns from #Compare (unchanged).
----------------------------------------------------------------------------------------
IF @PersistResults = 1
BEGIN
    DECLARE
          @rtSchema sysname = PARSENAME(@ResultsTable, 2)
        , @rtName   sysname = PARSENAME(@ResultsTable, 1);

    IF @rtSchema IS NULL OR @rtName IS NULL
        THROW 50030, 'Invalid @ResultsTable. Must be two-part name.', 1;

    DECLARE @psql nvarchar(max) = N'
    IF OBJECT_ID(N''' + REPLACE(@rtSchema,'''','''''') + N'.' + REPLACE(@rtName,'''','''''') + N''',''U'') IS NOT NULL
        DROP TABLE ' + QUOTENAME(@rtSchema) + N'.' + QUOTENAME(@rtName) + N';

    SELECT TOP (0)
          CAST(SYSUTCDATETIME() AS datetime2(0)) AS CollectedAt
        , CAST(@DbLower     AS sysname)          AS LowerDb
        , CAST(@DbHigher    AS sysname)          AS HigherDb
        , CAST(@LabelLower  AS sysname)          AS LowerCL
        , CAST(@LabelHigher AS sysname)          AS HigherCL
        , CAST(@Metric      AS sysname)          AS Metric
        , CAST(@GroupBy     AS sysname)          AS GroupBy
        , CAST(c.QueryType  AS varchar(10))      AS QueryType
        , CAST(c.ObjName    AS sysname)          AS ObjName
        , CAST(c.GroupKeyHash AS varbinary(32))  AS GroupKeyHash
        , CAST(c.PlanCount_L AS int)             AS PlanCount_L
        , CAST(c.PlanCount_H AS int)             AS PlanCount_H
        , CAST(c.ExecCount_L AS bigint)          AS ExecCount_L
        , CAST(c.ExecCount_H AS bigint)          AS ExecCount_H
        , CAST(c.TotalMetric_L AS decimal(38,6)) AS TotalMetric_L
        , CAST(c.TotalMetric_H AS decimal(38,6)) AS TotalMetric_H
        , CAST(c.AvgMetric_L   AS decimal(38,6)) AS AvgMetric_L
        , CAST(c.AvgMetric_H   AS decimal(38,6)) AS AvgMetric_H
        , CAST(c.RegressionRatio AS decimal(19,6)) AS RegressionRatio
        , CAST(c.ImpactScore     AS decimal(38,6)) AS ImpactScore
        , CAST(c.ConfidenceFlags AS varchar(500))  AS ConfidenceFlags
        , CAST(c.QueryTextSample AS nvarchar(max)) AS QueryTextSample
    INTO ' + QUOTENAME(@rtSchema) + N'.' + QUOTENAME(@rtName) + N'
    FROM #Compare AS c;

    INSERT INTO ' + QUOTENAME(@rtSchema) + N'.' + QUOTENAME(@rtName) + N'
    (
        CollectedAt,
        LowerDb, HigherDb, LowerCL, HigherCL,
        Metric, GroupBy,
        QueryType, ObjName, GroupKeyHash,
        PlanCount_L, PlanCount_H,
        ExecCount_L, ExecCount_H,
        TotalMetric_L, TotalMetric_H,
        AvgMetric_L, AvgMetric_H,
        RegressionRatio, ImpactScore,
        ConfidenceFlags, QueryTextSample
    )
    SELECT
          SYSUTCDATETIME() AS CollectedAt
        , @DbLower, @DbHigher, @LabelLower, @LabelHigher
        , @Metric, @GroupBy
        , c.QueryType, c.ObjName, c.GroupKeyHash
        , c.PlanCount_L, c.PlanCount_H
        , c.ExecCount_L, c.ExecCount_H
        , c.TotalMetric_L, c.TotalMetric_H
        , c.AvgMetric_L, c.AvgMetric_H
        , c.RegressionRatio, c.ImpactScore
        , c.ConfidenceFlags, c.QueryTextSample
    FROM #Compare AS c
    WHERE c.RegressionRatio IS NOT NULL
      AND c.AvgMetric_H > c.AvgMetric_L
      AND (@MinExecCount IS NULL OR (c.ExecCount_L >= @MinExecCount AND c.ExecCount_H >= @MinExecCount))
      AND (@MinRegressionRatio IS NULL OR c.RegressionRatio >= @MinRegressionRatio);
    ';

    EXEC sp_executesql
        @psql,
        N'@DbLower sysname, @DbHigher sysname, @LabelLower sysname, @LabelHigher sysname, @Metric sysname, @GroupBy sysname, @MinExecCount bigint, @MinRegressionRatio decimal(9,4)',
        @DbLower=@DbLower, @DbHigher=@DbHigher,
        @LabelLower=@LabelLower, @LabelHigher=@LabelHigher,
        @Metric=@Metric, @GroupBy=@GroupBy,
        @MinExecCount=@MinExecCount, @MinRegressionRatio=@MinRegressionRatio;
END;

/*======================================================================================
OVERVIEW
----------------------------------------------------------------------------------------
This script compares Query Store performance metrics between two databases running at
different compatibility levels (LowerCL vs HigherCL).

Its primary goal is to identify:
- Queries whose performance regressed after a compatibility level change
- The magnitude and business impact of those regressions
- Whether regressions are driven by plan changes, multi-plan behavior, or plan shape differences

The script is designed to be executed from ANY database context.
Actual compatibility levels are resolved dynamically from @DbA and @DbB.
======================================================================================*/

/*======================================================================================
METRICS & TERMINOLOGY
----------------------------------------------------------------------------------------
Metric (@Metric):
- LogicalReads : Logical I/O (buffer reads)
- CPU          : CPU time
- Duration     : Query duration

LowerCL / HigherCL:
- LowerCL  : Database with the LOWER compatibility level
- HigherCL : Database with the HIGHER compatibility level
(The mapping is automatic; @DbA and @DbB order does not matter.)

Regression definition:
- AvgMetric_H > AvgMetric_L
- Optional filters:
    * @MinExecCount
    * @MinRegressionRatio

All numeric values in result sets are DISPLAYED with 2 decimal places
for readability. Internally, full precision is preserved.
======================================================================================*/

/*======================================================================================
RESULT SET #1 — REGRESSION OVERVIEW (PRIMARY RESULT SET)
----------------------------------------------------------------------------------------
Purpose:
- High-level list of regressed queries
- Ordered by ImpactScore (highest business impact first)

How to read:
- Each row represents ONE logical query group (based on @GroupBy)
- Values are shown as "LowerCL - HigherCL" pairs

Key columns:
- QueryType:
    SP     = Stored Procedure
    Adhoc  = Ad-hoc query

- ObjName:
    Schema.ObjectName for SPs, NULL for Adhoc queries

- GroupKeyHashHex:
    Stable hash representing the grouping key
    (used to correlate rows across result sets)

- QueryIdRange_L-H:
    Min-Max QueryId range in LowerCL and HigherCL
    Useful to detect Query Store fragmentation or re-compilation behavior

- PlanCount_L-H:
    Number of distinct execution plans observed per side
    >1 indicates MULTI_PLAN behavior

- ExecCount_L-H:
    Total execution count per side

- Total<Metric>_L-H:
    Total aggregated metric over the analysis window

- Avg<Metric>_L-H:
    Weighted average metric per execution

- RegressionRatio:
    AvgMetric_H / AvgMetric_L
    Example:
        1.25 = 25% regression
        2.00 = 100% regression

- DeltaAvgMetric:
    AvgMetric_H - AvgMetric_L

- ImpactScore:
    (AvgMetric_H - AvgMetric_L) * ExecCount_H
    This represents TOTAL additional cost introduced by the regression.
    This is the PRIMARY ranking signal.

- ConfidenceFlags:
    Heuristics describing data quality or risk:
      * MISSING_ONE_SIDE
      * LOW_EXEC
      * MULTI_PLAN
      * INTERVAL_END_FALLBACK
======================================================================================*/

/*======================================================================================
RESULT SET #2 — SUMMARY
----------------------------------------------------------------------------------------
Purpose:
- Executive-level summary of detected regressions

Key columns:
- RegressionCount:
    Total number of regressed query groups

- MultiPlanCount:
    Number of regressed queries exhibiting MULTI_PLAN behavior

- SumImpactScore:
    Total cumulative regression impact across all queries

- MaxImpactScore:
    Single most expensive regression

- AvgRegressionRatio:
    Average regression ratio across all regressed queries

Use case:
- Quick health check after compatibility level changes
- Baseline comparison across test runs
======================================================================================*/

/*======================================================================================
RESULT SET #3 — MULTI-PLAN DRILLDOWN (PLAN-LEVEL)
----------------------------------------------------------------------------------------
Returned ONLY if multi-plan regressions exist.

Purpose:
- Inspect plan-level behavior for queries with multiple plans
- Understand which plans dominate execution and cost

How to read:
- Each row represents ONE PLAN for ONE query group and ONE side (L or H)

Key columns:
- SourceDb / SourceCL / Side:
    Identify which database and compatibility level the plan belongs to

- PlanId / QueryId:
    Native Query Store identifiers

- ExecCount:
    Execution count for this specific plan

- AvgMetric / TotalMetric:
    Cost contribution of this plan

- RankByAvgMetric:
    Rank within the group based on AvgMetric (descending)

- RankByExecCount:
    Rank within the group based on execution frequency

Use case:
- Identify plan skew
- Detect plan regression masked by averaging
======================================================================================*/

/*======================================================================================
RESULT SET #4 — DOMINANT PLAN COMPARISON + PLAN SHAPE DIFF
----------------------------------------------------------------------------------------
Returned ONLY for MULTI_PLAN regressions.

Purpose:
- Compare the dominant execution plan in LowerCL vs HigherCL
- Highlight structural plan changes

Dominant plan definition:
- Highest ExecCount
- Tie-breaker: higher AvgMetric, then higher PlanId

Key columns:
- ExecCount_L-H / AvgMetric_L-H:
    Dominant plan cost comparison

- DominantPlanId_L-H / DominantQueryId_L-H:
    Identifiers of dominant plans per side

- PlanXmlHashHex_L-H:
    Hash of plan XML (fast equality check)

- Operator counts (IndexSeek, Scan, TableScan):
    Structural differences in access paths

- Join indicators:
    HasHashJoin / HasMergeJoin / HasNestedLoops

- Memory & spill indicators:
    GrantedMemoryKB
    SpillToTempDb
    MissingIndex

- DiffFlags:
    Human-readable summary of detected plan shape differences

- PlanXml_<CL>:
    Actual execution plan XML
    Clickable in SSMS for visual inspection

Use case:
- Root cause analysis
- Confirm CE changes, join strategy changes, or memory grant regressions
======================================================================================*/

/*======================================================================================
PERSISTENCE (@PersistResults)
----------------------------------------------------------------------------------------
If @PersistResults = 1:
- Results are written into @ResultsTable
- Table is DROPPED and RE-CREATED on each execution
- Stored values keep full numeric precision (no rounding)

If @PersistResults = 0:
- No permanent objects are created
======================================================================================*/
