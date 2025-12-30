/*
+----------------------------------------------------------------------------------------------------------+------+
|  QUERY STORE CL REGRESSION COMPARATOR                                                                    | v1.5 |
+----------------------------------------------------------------------------------------------------------+------+
|  PURPOSE                                                                                                        |
|  -------                                                                                                        |
|  Compares Query Store data between two databases running at different compatibility levels.                     |
|  Designed for A/B testing during CL upgrades to identify, quantify, and analyze query performance regressions.  |
+-----------------------------------------------------------------------------------------------------------------+
|  DOCUMENTATION                                                                                                  |
|  -------------                                                                                                  |
|  https://github.com/yigitaktan/XeQueryReplayer/blob/main/ab-test/regression-comparator.md                       |
+-----------------------------------------------------------------------------------------------------------------+
|  DISCLAIMER                                                                                                     |
|  ----------                                                                                                     |
|  Provided as-is, without warranty. Results depend on workload replay quality and Query Store accuracy.          |
|  The author assumes no responsibility for unintended consequences resulting from its use.                       |
+-----------------------------------------------------------------------------------------------------------------+
|  AUTHOR                                                                                                         |
|  ------                                                                                                         |
|  Yigit Aktan                                                                                                    |
|  Microsoft | Global Delivery                                                                                    |
+-----------------------------------------------------------------------------------------------------------------+
*/

SET NOCOUNT ON;

-------------------------------------------------------------------------------------------------------------------
-- Parameters
-------------------------------------------------------------------------------------------------------------------
DECLARE
      @DbA sysname                     = N'DemoDB_CL120'           -- Baseline database (LowerCL)
    , @DbB sysname                     = N'DemoDB_CL170'           -- Candidate database (HigherCL)
    , @MinExecCount bigint             = NULL                      -- e.g. 50
    , @MinRegressionRatio decimal(9,4) = NULL                      -- e.g. 1.25
    , @TopN int                        = NULL                      -- e.g. 100
    , @StartTime datetime2(0)          = NULL                      -- e.g. '2025-12-25 21:48:56'
    , @EndTime   datetime2(0)          = NULL                      -- e.g. '2025-12-25 22:00:00'
    , @Metric sysname                  = N'LogicalReads'           -- LogicalReads | CPU | Duration
    , @GroupBy sysname                 = N'QueryHash'              -- QueryHash | QueryText | NormalizedText
    , @StatementType varchar(10)       = N'ALL'                    -- ALL | SELECT | INSERT | UPDATE | DELETE
    , @IncludeAdhoc bit                = 1                         -- 1 | 0
    , @IncludeSP bit                   = 1                         -- 1 | 0
    , @OnlyMultiPlan bit               = 0                         -- 1 | 0
    , @PersistResults bit              = 0                         -- 1 | 0
    , @ResultsTable sysname            = N'dbo.RegressionResults';

-------------------------------------------------------------------------------------------------------------------
-- Validation
-------------------------------------------------------------------------------------------------------------------
IF @Metric NOT IN (N'LogicalReads', N'CPU', N'Duration')
    THROW 50001, 'Invalid @Metric. Use LogicalReads, CPU, or Duration.', 1;

IF @GroupBy NOT IN (N'QueryHash', N'QueryText', N'NormalizedText')
    THROW 50002, 'Invalid @GroupBy. Use QueryHash, QueryText, or NormalizedText.', 1;

IF @StartTime IS NOT NULL AND @EndTime IS NOT NULL AND @EndTime <= @StartTime
    THROW 50003, '@EndTime must be greater than @StartTime.', 1;

IF PARSENAME(@ResultsTable, 2) IS NULL OR PARSENAME(@ResultsTable, 1) IS NULL
    THROW 50004, '@ResultsTable must be two-part name like dbo.TableName.', 1;

IF @StatementType IS NULL
    THROW 50005, 'Invalid @StatementType. Use ALL, SELECT, INSERT, UPDATE, or DELETE. (NULL is not allowed)', 1;

SET @StatementType = UPPER(LTRIM(RTRIM(@StatementType)));

IF @StatementType NOT IN ('ALL','SELECT','INSERT','UPDATE','DELETE')
    THROW 50006, 'Invalid @StatementType. Use ALL, SELECT, INSERT, UPDATE, or DELETE.', 1;

IF @OnlyMultiPlan NOT IN (0,1)
    THROW 50007, 'Invalid @OnlyMultiPlan. Use 0 (everything) or 1 (only MULTI_PLAN).', 1;

-------------------------------------------------------------------------------------------------------------------
-- Resolve compatibility levels for A/B, then map to Lower/Higher
-------------------------------------------------------------------------------------------------------------------
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

-------------------------------------------------------------------------------------------------------------------
-- Temp table
-------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#QS_Agg') IS NOT NULL DROP TABLE #QS_Agg;
CREATE TABLE #QS_Agg
(
    SourceDb              sysname        NOT NULL,
    QueryType             varchar(10)    NOT NULL,
    ObjName               sysname        NULL,
    GroupKeyHash          varbinary(32)  NOT NULL,
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

-------------------------------------------------------------------------------------------------------------------
-- Detect whether runtime_stats_interval has end_time (per Lower/Higher DB)
-------------------------------------------------------------------------------------------------------------------
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

-------------------------------------------------------------------------------------------------------------------
-- Dynamic SQL: aggregate one DB into #QS_Agg
-------------------------------------------------------------------------------------------------------------------
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
OUTER APPLY
(
    SELECT
        DetectedType =
            CASE
                WHEN @StatementType = ''ALL'' THEN NULL
                ELSE
                (
                    SELECT TOP (1) v.Typ
                    FROM
                    (
                        SELECT
                              Typ = ''SELECT''
                            , Pos =
                                  CASE
                                      WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''SELECT%'' THEN 1
                                      WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''WITH%''   THEN NULLIF(PATINDEX(''%SELECT%'', UPPER(qt.query_sql_text)),0)
                                      ELSE NULL
                                  END
                        UNION ALL
                        SELECT
                              Typ = ''INSERT''
                            , Pos =
                                  CASE
                                      WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''INSERT%'' THEN 1
                                      WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''WITH%''   THEN NULLIF(PATINDEX(''%INSERT%'', UPPER(qt.query_sql_text)),0)
                                      ELSE NULL
                                  END
                        UNION ALL
                        SELECT
                              Typ = ''UPDATE''
                            , Pos =
                                  CASE
                                      WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''UPDATE%'' THEN 1
                                      WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''WITH%''   THEN NULLIF(PATINDEX(''%UPDATE%'', UPPER(qt.query_sql_text)),0)
                                      ELSE NULL
                                  END
                        UNION ALL
                        SELECT
                              Typ = ''DELETE''
                            , Pos =
                                  CASE
                                      WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''DELETE%'' THEN 1
                                      WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''WITH%''   THEN NULLIF(PATINDEX(''%DELETE%'', UPPER(qt.query_sql_text)),0)
                                      ELSE NULL
                                  END
                    ) v
                    WHERE v.Pos IS NOT NULL
                    ORDER BY v.Pos ASC
                )
            END
) st
WHERE 1=1
  AND (
        (@IncludeSP = 1 AND q.object_id > 0)
     OR (@IncludeAdhoc = 1 AND (q.object_id = 0 OR q.object_id IS NULL))
  )
  AND (@StartTime IS NULL OR rsi.start_time >= @StartTime)
  AND (@EndTime   IS NULL OR rsi.start_time <  @EndTime)
  AND (
        @StatementType = ''ALL''
        OR st.DetectedType = @StatementType
      )
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

-------------------------------------------------------------------------------------------------------------------
-- Build and execute per Lower/Higher DB
-------------------------------------------------------------------------------------------------------------------
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
    N'@DbName sysname, @Metric sysname, @GroupBy sysname, @IncludeAdhoc bit, @IncludeSP bit, @StartTime datetime2(0), @EndTime datetime2(0), @StatementType varchar(10)',
    @DbName=@DbLower, @Metric=@Metric, @GroupBy=@GroupBy, @IncludeAdhoc=@IncludeAdhoc, @IncludeSP=@IncludeSP, @StartTime=@StartTime, @EndTime=@EndTime, @StatementType=@StatementType;

EXEC sp_executesql
    @sqlH,
    N'@DbName sysname, @Metric sysname, @GroupBy sysname, @IncludeAdhoc bit, @IncludeSP bit, @StartTime datetime2(0), @EndTime datetime2(0), @StatementType varchar(10)',
    @DbName=@DbHigher, @Metric=@Metric, @GroupBy=@GroupBy, @IncludeAdhoc=@IncludeAdhoc, @IncludeSP=@IncludeSP, @StartTime=@StartTime, @EndTime=@EndTime, @StatementType=@StatementType;

-------------------------------------------------------------------------------------------------------------------
-- Compare (internal columns use _L/_H)
-------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#Compare') IS NOT NULL DROP TABLE #Compare;
CREATE TABLE #Compare
(
    QueryType            varchar(10) NOT NULL,
    ObjName              sysname NULL,
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
    RegressionRatio      decimal(19,6) NULL,
    DeltaAvgMetric       decimal(38,6) NULL,
    ImpactScore          decimal(38,6) NULL,
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

-------------------------------------------------------------------------------------------------------------------
-- Build filtered group set and compute DominantPlanId/QueryId for Resultset#1 + Persist
-------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#FilteredGroups') IS NOT NULL DROP TABLE #FilteredGroups;
CREATE TABLE #FilteredGroups
(
    GroupKeyHash varbinary(32) NOT NULL,
    QueryType    varchar(10)   NOT NULL,
    ObjNameNorm  sysname       NOT NULL,
    CONSTRAINT PK_FilteredGroups PRIMARY KEY (GroupKeyHash, QueryType, ObjNameNorm)
);

INSERT #FilteredGroups (GroupKeyHash, QueryType, ObjNameNorm)
SELECT DISTINCT
      c.GroupKeyHash
    , c.QueryType
    , ISNULL(c.ObjName,'') AS ObjNameNorm
FROM #Compare c
WHERE c.RegressionRatio IS NOT NULL
  AND c.AvgMetric_H > c.AvgMetric_L
  AND (@MinExecCount IS NULL OR (c.ExecCount_L >= @MinExecCount AND c.ExecCount_H >= @MinExecCount))
  AND (@MinRegressionRatio IS NULL OR c.RegressionRatio >= @MinRegressionRatio)
  AND (@OnlyMultiPlan = 0 OR c.ConfidenceFlags LIKE '%MULTI_PLAN%');

IF OBJECT_ID('tempdb..#DominantPlans_All') IS NOT NULL DROP TABLE #DominantPlans_All;
CREATE TABLE #DominantPlans_All
(
    SourceDb     sysname       NOT NULL,
    GroupKeyHash varbinary(32) NOT NULL,
    QueryType    varchar(10)   NOT NULL,
    ObjName      sysname       NULL,
    PlanId       bigint        NOT NULL,
    QueryId      bigint        NULL,
    ExecCount    bigint        NOT NULL,
    AvgMetric    decimal(38,6) NOT NULL
);

DECLARE @domTmpl nvarchar(max) = N'
;WITH planAgg AS
(
    SELECT
          @DbName AS SourceDb
        , HASHBYTES(''SHA2_256'', CONVERT(varbinary(max),
              CASE
                  WHEN @GroupBy = ''QueryHash'' THEN CONVERT(nvarchar(100), q.query_hash, 1)
                  WHEN @GroupBy = ''QueryText'' THEN qt.query_sql_text
                  ELSE LOWER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(qt.query_sql_text, CHAR(13), '' ''), CHAR(10), '' ''), CHAR(9), '' ''))))
              END
          )) AS GroupKeyHash
        , CASE WHEN q.object_id > 0 THEN ''SP'' ELSE ''Adhoc'' END AS QueryType
        , CASE WHEN q.object_id > 0 THEN sch.name + ''.'' + obj.name ELSE NULL END AS ObjName
        , p.plan_id AS PlanId
        , q.query_id AS QueryId
        , SUM(rs.count_executions) AS ExecCount
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
    OUTER APPLY
    (
        SELECT
            DetectedType =
                CASE
                    WHEN @StatementType = ''ALL'' THEN NULL
                    ELSE
                    (
                        SELECT TOP (1) v.Typ
                        FROM
                        (
                            SELECT Typ=''SELECT'',
                                   Pos=CASE
                                            WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''SELECT%'' THEN 1
                                            WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''WITH%''   THEN NULLIF(PATINDEX(''%SELECT%'', UPPER(qt.query_sql_text)),0)
                                            ELSE NULL
                                       END
                            UNION ALL
                            SELECT Typ=''INSERT'',
                                   Pos=CASE
                                            WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''INSERT%'' THEN 1
                                            WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''WITH%''   THEN NULLIF(PATINDEX(''%INSERT%'', UPPER(qt.query_sql_text)),0)
                                            ELSE NULL
                                       END
                            UNION ALL
                            SELECT Typ=''UPDATE'',
                                   Pos=CASE
                                            WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''UPDATE%'' THEN 1
                                            WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''WITH%''   THEN NULLIF(PATINDEX(''%UPDATE%'', UPPER(qt.query_sql_text)),0)
                                            ELSE NULL
                                       END
                            UNION ALL
                            SELECT Typ=''DELETE'',
                                   Pos=CASE
                                            WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''DELETE%'' THEN 1
                                            WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''WITH%''   THEN NULLIF(PATINDEX(''%DELETE%'', UPPER(qt.query_sql_text)),0)
                                            ELSE NULL
                                       END
                        ) v
                        WHERE v.Pos IS NOT NULL
                        ORDER BY v.Pos ASC
                    )
                END
    ) st
    WHERE 1=1
      AND (
            (@IncludeSP = 1 AND q.object_id > 0)
         OR (@IncludeAdhoc = 1 AND (q.object_id = 0 OR q.object_id IS NULL))
      )
      AND (@StartTime IS NULL OR rsi.start_time >= @StartTime)
      AND (@EndTime   IS NULL OR rsi.start_time <  @EndTime)
      AND (
            @StatementType = ''ALL''
            OR st.DetectedType = @StatementType
          )
      AND EXISTS
      (
          SELECT 1
          FROM #FilteredGroups fg
          WHERE fg.GroupKeyHash = HASHBYTES(''SHA2_256'', CONVERT(varbinary(max),
                CASE
                    WHEN @GroupBy = ''QueryHash'' THEN CONVERT(nvarchar(100), q.query_hash, 1)
                    WHEN @GroupBy = ''QueryText'' THEN qt.query_sql_text
                    ELSE LOWER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(qt.query_sql_text, CHAR(13), '' ''), CHAR(10), '' ''), CHAR(9), '' ''))))
                END
          ))
            AND fg.QueryType = CASE WHEN q.object_id > 0 THEN ''SP'' ELSE ''Adhoc'' END
            AND fg.ObjNameNorm = ISNULL(CASE WHEN q.object_id > 0 THEN sch.name + ''.'' + obj.name ELSE NULL END, '''')
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
),
r AS
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
        , ROW_NUMBER() OVER
          (
            PARTITION BY pa.SourceDb, pa.GroupKeyHash, pa.QueryType, ISNULL(pa.ObjName,'''')
            ORDER BY pa.ExecCount DESC, pa.AvgMetric DESC, pa.PlanId DESC
          ) AS rn
    FROM planAgg pa
)
INSERT INTO #DominantPlans_All (SourceDb, GroupKeyHash, QueryType, ObjName, PlanId, QueryId, ExecCount, AvgMetric)
SELECT
      SourceDb, GroupKeyHash, QueryType, ObjName, PlanId, QueryId, ExecCount, AvgMetric
FROM r
WHERE rn = 1;
';

DECLARE @domSqlL nvarchar(max) = REPLACE(@domTmpl, N'{{DB}}', QUOTENAME(@DbLower));
DECLARE @domSqlH nvarchar(max) = REPLACE(@domTmpl, N'{{DB}}', QUOTENAME(@DbHigher));

EXEC sp_executesql
    @domSqlL,
    N'@DbName sysname, @Metric sysname, @GroupBy sysname, @IncludeAdhoc bit, @IncludeSP bit, @StartTime datetime2(0), @EndTime datetime2(0), @StatementType varchar(10)',
    @DbName=@DbLower, @Metric=@Metric, @GroupBy=@GroupBy, @IncludeAdhoc=@IncludeAdhoc, @IncludeSP=@IncludeSP, @StartTime=@StartTime, @EndTime=@EndTime, @StatementType=@StatementType;

EXEC sp_executesql
    @domSqlH,
    N'@DbName sysname, @Metric sysname, @GroupBy sysname, @IncludeAdhoc bit, @IncludeSP bit, @StartTime datetime2(0), @EndTime datetime2(0), @StatementType varchar(10)',
    @DbName=@DbHigher, @Metric=@Metric, @GroupBy=@GroupBy, @IncludeAdhoc=@IncludeAdhoc, @IncludeSP=@IncludeSP, @StartTime=@StartTime, @EndTime=@EndTime, @StatementType=@StatementType;

IF OBJECT_ID('tempdb..#DomPairs_All') IS NOT NULL DROP TABLE #DomPairs_All;
CREATE TABLE #DomPairs_All
(
    GroupKeyHash  varbinary(32) NOT NULL,
    QueryType     varchar(10) NOT NULL,
    ObjName       sysname NULL,
    DominantPlanId_L  bigint NULL,
    DominantPlanId_H  bigint NULL,
    DominantQueryId_L bigint NULL,
    DominantQueryId_H bigint NULL
);

INSERT #DomPairs_All
SELECT
      COALESCE(b.GroupKeyHash, a.GroupKeyHash) AS GroupKeyHash
    , COALESCE(b.QueryType, a.QueryType)       AS QueryType
    , COALESCE(b.ObjName, a.ObjName)           AS ObjName
    , a.PlanId  AS DominantPlanId_L
    , b.PlanId  AS DominantPlanId_H
    , a.QueryId AS DominantQueryId_L
    , b.QueryId AS DominantQueryId_H
FROM (SELECT * FROM #DominantPlans_All WHERE SourceDb = @DbLower) a
FULL OUTER JOIN (SELECT * FROM #DominantPlans_All WHERE SourceDb = @DbHigher) b
  ON a.GroupKeyHash = b.GroupKeyHash
 AND ISNULL(a.QueryType,'') = ISNULL(b.QueryType,'')
 AND ISNULL(a.ObjName,'')   = ISNULL(b.ObjName,'');

-------------------------------------------------------------------------------------------------------------------
-- Resultset #1: Primary analysis view
-------------------------------------------------------------------------------------------------------------------
DECLARE @out1 nvarchar(max) = N'
;WITH filtered AS
(
    SELECT *
    FROM #Compare
    WHERE RegressionRatio IS NOT NULL
      AND AvgMetric_H > AvgMetric_L
      AND (@MinExecCount IS NULL OR (ExecCount_L >= @MinExecCount AND ExecCount_H >= @MinExecCount))
      AND (@MinRegressionRatio IS NULL OR RegressionRatio >= @MinRegressionRatio)
      AND (@OnlyMultiPlan = 0 OR ConfidenceFlags LIKE ''%MULTI_PLAN%'')
)
SELECT TOP (CASE WHEN @TopN IS NULL THEN 2147483647 ELSE @TopN END)
      f.QueryType
    , f.ObjName
    , CONVERT(varchar(66), f.GroupKeyHash, 1) AS GroupKeyHashHex
    , CONCAT(COALESCE(f.QueryIdRange_L,''?''), '' - '', COALESCE(f.QueryIdRange_H,''?'')) AS [QueryIdRange_L-H]
    , CONCAT(COALESCE(CONVERT(varchar(50), f.QueryHash_L, 1), ''?''), '' - '', COALESCE(CONVERT(varchar(50), f.QueryHash_H, 1), ''?'')) AS [QueryHashHex_L-H]
    , CONCAT(COALESCE(CONVERT(varchar(30), dp.DominantPlanId_L), ''?''), '' - '', COALESCE(CONVERT(varchar(30), dp.DominantPlanId_H), ''?''))  AS [DominantPlanId_L-H]
    , CONCAT(COALESCE(CONVERT(varchar(30), dp.DominantQueryId_L), ''?''), '' - '', COALESCE(CONVERT(varchar(30), dp.DominantQueryId_H), ''?'')) AS [DominantQueryId_L-H]
    , CONCAT(CONVERT(varchar(30), f.PlanCount_L), '' - '', CONVERT(varchar(30), f.PlanCount_H)) AS [PlanCount_L-H]
    , CONCAT(CONVERT(varchar(30), f.ExecCount_L), '' - '', CONVERT(varchar(30), f.ExecCount_H)) AS [ExecCount_L-H]
    , CONCAT(
          CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(f.TotalMetric_L, 2))),
          '' - '',
          CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(f.TotalMetric_H, 2)))
      ) AS ' + QUOTENAME('Total' + @Metric + '_L-H') + N'
    , CONCAT(
          CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(f.AvgMetric_L, 2))),
          '' - '',
          CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(f.AvgMetric_H, 2)))
      ) AS ' + QUOTENAME('Avg' + @Metric + '_L-H') + N'
    , CONCAT(
          CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(f.TotalDuration_L, 2))),
          '' - '',
          CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(f.TotalDuration_H, 2)))
      ) AS [TotalDuration_L-H]
    , CONCAT(
          CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(f.AvgDuration_L, 2))),
          '' - '',
          CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(f.AvgDuration_H, 2)))
      ) AS [AvgDuration_L-H]
    , CONVERT(decimal(19,2), ROUND(f.RegressionRatio, 2)) AS RegressionRatio
    , CONVERT(decimal(38,2), ROUND(f.DeltaAvgMetric, 2))  AS DeltaAvgMetric
    , CONVERT(decimal(38,2), ROUND(f.ImpactScore, 2))     AS ImpactScore
    , f.ConfidenceFlags
    , f.QueryTextSample
FROM filtered f
LEFT JOIN #DomPairs_All dp
  ON dp.GroupKeyHash = f.GroupKeyHash
 AND ISNULL(dp.QueryType,'''') = ISNULL(f.QueryType,'''')
 AND ISNULL(dp.ObjName,'''')   = ISNULL(f.ObjName,'''')
ORDER BY f.ImpactScore DESC;
';

EXEC sp_executesql
    @out1,
    N'@TopN int, @MinExecCount bigint, @MinRegressionRatio decimal(9,4), @OnlyMultiPlan bit',
    @TopN=@TopN, @MinExecCount=@MinExecCount, @MinRegressionRatio=@MinRegressionRatio, @OnlyMultiPlan=@OnlyMultiPlan;

-------------------------------------------------------------------------------------------------------------------
-- Resultset #2: Summary
-------------------------------------------------------------------------------------------------------------------
;WITH filtered AS
(
    SELECT *
    FROM #Compare
    WHERE RegressionRatio IS NOT NULL
      AND AvgMetric_H > AvgMetric_L
      AND (@MinExecCount IS NULL OR (ExecCount_L >= @MinExecCount AND ExecCount_H >= @MinExecCount))
      AND (@MinRegressionRatio IS NULL OR RegressionRatio >= @MinRegressionRatio)
      AND (@OnlyMultiPlan = 0 OR ConfidenceFlags LIKE '%MULTI_PLAN%')
)
SELECT
      @DbLower   AS LowerDb
    , @DbHigher  AS HigherDb
    , CONVERT(varchar(10), @LowerCL)  AS LowerCL
    , CONVERT(varchar(10), @HigherCL) AS HigherCL
    , @Metric  AS Metric
    , @GroupBy AS GroupBy
    , COUNT(*) AS RegressionCount
    , SUM(CASE WHEN ConfidenceFlags LIKE '%MULTI_PLAN%' THEN 1 ELSE 0 END) AS MultiPlanCount
    , CONVERT(decimal(38,2), ROUND(SUM(ImpactScore), 2)) AS SumImpactScore
    , CONVERT(decimal(38,2), ROUND(MAX(ImpactScore), 2)) AS MaxImpactScore
    , CONVERT(decimal(19,2), ROUND(AVG(RegressionRatio), 2)) AS AvgRegressionRatio
FROM filtered;

-------------------------------------------------------------------------------------------------------------------
-- Resultset #3 + #4: MULTI-PLAN only
-------------------------------------------------------------------------------------------------------------------
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
      AND (@OnlyMultiPlan = 0 OR ConfidenceFlags LIKE '%MULTI_PLAN%')
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
-------------------------------------------------------------------------------------------------------------------
-- Plan-level aggregation for multi-plan groups
-------------------------------------------------------------------------------------------------------------------
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
    OUTER APPLY
    (
        SELECT
            DetectedType =
                CASE
                    WHEN @StatementType = ''ALL'' THEN NULL
                    ELSE
                    (
                        SELECT TOP (1) v.Typ
                        FROM
                        (
                            SELECT Typ=''SELECT'',
                                   Pos=CASE
                                            WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''SELECT%'' THEN 1
                                            WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''WITH%''   THEN NULLIF(PATINDEX(''%SELECT%'', UPPER(qt.query_sql_text)),0)
                                            ELSE NULL
                                       END
                            UNION ALL
                            SELECT Typ=''INSERT'',
                                   Pos=CASE
                                            WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''INSERT%'' THEN 1
                                            WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''WITH%''   THEN NULLIF(PATINDEX(''%INSERT%'', UPPER(qt.query_sql_text)),0)
                                            ELSE NULL
                                       END
                            UNION ALL
                            SELECT Typ=''UPDATE'',
                                   Pos=CASE
                                            WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''UPDATE%'' THEN 1
                                            WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''WITH%''   THEN NULLIF(PATINDEX(''%UPDATE%'', UPPER(qt.query_sql_text)),0)
                                            ELSE NULL
                                       END
                            UNION ALL
                            SELECT Typ=''DELETE'',
                                   Pos=CASE
                                            WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''DELETE%'' THEN 1
                                            WHEN UPPER(LTRIM(qt.query_sql_text)) LIKE ''WITH%''   THEN NULLIF(PATINDEX(''%DELETE%'', UPPER(qt.query_sql_text)),0)
                                            ELSE NULL
                                       END
                        ) v
                        WHERE v.Pos IS NOT NULL
                        ORDER BY v.Pos ASC
                    )
                END
    ) st
    WHERE 1=1
      AND (
            (@IncludeSP = 1 AND q.object_id > 0)
         OR (@IncludeAdhoc = 1 AND (q.object_id = 0 OR q.object_id IS NULL))
      )
      AND (@StartTime IS NULL OR rsi.start_time >= @StartTime)
      AND (@EndTime   IS NULL OR rsi.start_time <  @EndTime)
      AND (
            @StatementType = ''ALL''
            OR st.DetectedType = @StatementType
          )
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
        N'@DbName sysname, @Metric sysname, @GroupBy sysname, @IncludeAdhoc bit, @IncludeSP bit, @StartTime datetime2(0), @EndTime datetime2(0), @StatementType varchar(10)',
        @DbName=@DbLower, @Metric=@Metric, @GroupBy=@GroupBy, @IncludeAdhoc=@IncludeAdhoc, @IncludeSP=@IncludeSP, @StartTime=@StartTime, @EndTime=@EndTime, @StatementType=@StatementType;

    EXEC sp_executesql
        @planSqlH,
        N'@DbName sysname, @Metric sysname, @GroupBy sysname, @IncludeAdhoc bit, @IncludeSP bit, @StartTime datetime2(0), @EndTime datetime2(0), @StatementType varchar(10)',
        @DbName=@DbHigher, @Metric=@Metric, @GroupBy=@GroupBy, @IncludeAdhoc=@IncludeAdhoc, @IncludeSP=@IncludeSP, @StartTime=@StartTime, @EndTime=@EndTime, @StatementType=@StatementType;

-------------------------------------------------------------------------------------------------------------------
-- Resultset #3: Plan drilldown
-------------------------------------------------------------------------------------------------------------------
    ;WITH ranked AS
    (
        SELECT
              pa.SourceDb
            , CASE WHEN pa.SourceDb = @DbLower
                   THEN CONVERT(varchar(10), @LowerCL)
                   ELSE CONVERT(varchar(10), @HigherCL)
              END AS SourceCL
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

-------------------------------------------------------------------------------------------------------------------
-- Resultset #4: Dominant plan XML
-------------------------------------------------------------------------------------------------------------------
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
          AND (@OnlyMultiPlan = 0 OR ConfidenceFlags LIKE ''%MULTI_PLAN%'')
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
              CASE WHEN ISNULL(pL.HasMissingIndex,0) <> ISNULL(pH.HasMissingIndex,0) THEN ''MISSING_INDEX_CHANGED;'' ELSE '''' END,
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
        N'@DbLower sysname, @DbHigher sysname, @MinExecCount bigint, @MinRegressionRatio decimal(9,4), @OnlyMultiPlan bit',
        @DbLower=@DbLower, @DbHigher=@DbHigher, @MinExecCount=@MinExecCount, @MinRegressionRatio=@MinRegressionRatio, @OnlyMultiPlan=@OnlyMultiPlan;
END;

-------------------------------------------------------------------------------------------------------------------
-- Persist results (DROP + CREATE each run)
-------------------------------------------------------------------------------------------------------------------
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
        , CONVERT(sysname, @LowerCL)             AS LowerCL
        , CONVERT(sysname, @HigherCL)            AS HigherCL
        , CAST(@Metric      AS sysname)          AS Metric
        , CAST(@GroupBy     AS sysname)          AS GroupBy
        , CAST(c.QueryType  AS varchar(10))      AS QueryType
        , CAST(c.ObjName    AS sysname)          AS ObjName
        , CAST(c.GroupKeyHash AS varbinary(32))  AS GroupKeyHash
        , CAST(dp.DominantPlanId_L  AS bigint)   AS DominantPlanId_L
        , CAST(dp.DominantPlanId_H  AS bigint)   AS DominantPlanId_H
        , CAST(dp.DominantQueryId_L AS bigint)   AS DominantQueryId_L
        , CAST(dp.DominantQueryId_H AS bigint)   AS DominantQueryId_H
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
    FROM #Compare AS c
    LEFT JOIN #DomPairs_All dp
      ON dp.GroupKeyHash = c.GroupKeyHash
     AND ISNULL(dp.QueryType,'''') = ISNULL(c.QueryType,'''')
     AND ISNULL(dp.ObjName,'''')   = ISNULL(c.ObjName,'''')
    ;

    INSERT INTO ' + QUOTENAME(@rtSchema) + N'.' + QUOTENAME(@rtName) + N'
    (
        CollectedAt,
        LowerDb, HigherDb, LowerCL, HigherCL,
        Metric, GroupBy,
        QueryType, ObjName, GroupKeyHash,
        DominantPlanId_L, DominantPlanId_H,
        DominantQueryId_L, DominantQueryId_H,
        PlanCount_L, PlanCount_H,
        ExecCount_L, ExecCount_H,
        TotalMetric_L, TotalMetric_H,
        AvgMetric_L, AvgMetric_H,
        RegressionRatio, ImpactScore,
        ConfidenceFlags, QueryTextSample
    )
    SELECT
          SYSUTCDATETIME() AS CollectedAt
        , @DbLower, @DbHigher, CONVERT(sysname, @LowerCL), CONVERT(sysname, @HigherCL)
        , @Metric, @GroupBy
        , c.QueryType, c.ObjName, c.GroupKeyHash
        , dp.DominantPlanId_L, dp.DominantPlanId_H
        , dp.DominantQueryId_L, dp.DominantQueryId_H
        , c.PlanCount_L, c.PlanCount_H
        , c.ExecCount_L, c.ExecCount_H
        , c.TotalMetric_L, c.TotalMetric_H
        , c.AvgMetric_L, c.AvgMetric_H
        , c.RegressionRatio, c.ImpactScore
        , c.ConfidenceFlags, c.QueryTextSample
    FROM #Compare AS c
    LEFT JOIN #DomPairs_All dp
      ON dp.GroupKeyHash = c.GroupKeyHash
     AND ISNULL(dp.QueryType,'''') = ISNULL(c.QueryType,'''')
     AND ISNULL(dp.ObjName,'''')   = ISNULL(c.ObjName,'''')
    WHERE c.RegressionRatio IS NOT NULL
      AND c.AvgMetric_H > c.AvgMetric_L
      AND (@MinExecCount IS NULL OR (c.ExecCount_L >= @MinExecCount AND c.ExecCount_H >= @MinExecCount))
      AND (@MinRegressionRatio IS NULL OR c.RegressionRatio >= @MinRegressionRatio)
      AND (@OnlyMultiPlan = 0 OR c.ConfidenceFlags LIKE ''%MULTI_PLAN%'');
    ';

    EXEC sp_executesql
        @psql,
        N'@DbLower sysname, @DbHigher sysname, @LowerCL smallint, @HigherCL smallint, @Metric sysname, @GroupBy sysname, @MinExecCount bigint, @MinRegressionRatio decimal(9,4), @OnlyMultiPlan bit',
        @DbLower=@DbLower, @DbHigher=@DbHigher,
        @LowerCL=@LowerCL, @HigherCL=@HigherCL,
        @Metric=@Metric, @GroupBy=@GroupBy,
        @MinExecCount=@MinExecCount, @MinRegressionRatio=@MinRegressionRatio,
        @OnlyMultiPlan=@OnlyMultiPlan;
END;
