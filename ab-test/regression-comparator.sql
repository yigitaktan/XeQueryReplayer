/*
+--------------------------------------------------------------------------------------------------------+--------+
|  QUERY STORE CL REGRESSION COMPARATOR                                                                  | v1.6.7 |
+--------------------------------------------------------------------------------------------------------+--------+
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
    , @MinExecCount bigint             = 100                       -- e.g. 50, 100, NULL
    , @MinRegressionRatio decimal(9,4) = 1.25                      -- e.g. 1.25, 1.10, NULL
    , @TopN int                        = 100                       -- e.g. 100
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
-- Perf table
-------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#Perf') IS NOT NULL DROP TABLE #Perf;
CREATE TABLE #Perf
(
    StepNo       int IDENTITY(1,1) NOT NULL,
    StepName     nvarchar(200) NOT NULL,
    Ms           int NULL,
    RowsAffected bigint NULL,
    StartAt      datetime2(3) NOT NULL,
    EndAt        datetime2(3) NOT NULL
);

DECLARE @t0 datetime2(3), @t1 datetime2(3), @rc bigint;

-------------------------------------------------------------------------------------------------------------------
-- Collation handling
-------------------------------------------------------------------------------------------------------------------
DECLARE
      @TempdbCollation sysname = CAST(DATABASEPROPERTYEX('tempdb', 'Collation') AS sysname)
    , @CollateClause  nvarchar(200) = N' COLLATE ' + CAST(DATABASEPROPERTYEX('tempdb', 'Collation') AS sysname);

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
    THROW 50007, 'Invalid @OnlyMultiPlan. Use 0 or 1.', 1;

-------------------------------------------------------------------------------------------------------------------
-- Resolve compatibility levels for A/B, then map to Lower/Higher
-------------------------------------------------------------------------------------------------------------------
SET @t0 = SYSDATETIME();

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

SET @t1 = SYSDATETIME();
INSERT #Perf (StepName, Ms, RowsAffected, StartAt, EndAt)
VALUES (N'Resolve CL & Lower/Higher mapping', DATEDIFF(MILLISECOND,@t0,@t1), NULL, @t0, @t1);

-------------------------------------------------------------------------------------------------------------------
-- Detect whether runtime_stats_interval has end_time
-------------------------------------------------------------------------------------------------------------------
SET @t0 = SYSDATETIME();

DECLARE @HasEndTime_L bit = 0, @HasEndTime_H bit = 0;

DECLARE @chk nvarchar(max);

SET @chk = N'
SELECT @HasEndTimeOut =
    CASE WHEN EXISTS (
        SELECT 1
        FROM ' + QUOTENAME(@DbLower) + N'.sys.columns
        WHERE object_id = OBJECT_ID(''' + REPLACE(@DbLower,'''','''''') + N'.sys.query_store_runtime_stats_interval'')
          AND name = ''end_time''
    ) THEN 1 ELSE 0 END;';
EXEC sp_executesql @chk, N'@HasEndTimeOut bit OUTPUT', @HasEndTimeOut=@HasEndTime_L OUTPUT;

SET @chk = N'
SELECT @HasEndTimeOut =
    CASE WHEN EXISTS (
        SELECT 1
        FROM ' + QUOTENAME(@DbHigher) + N'.sys.columns
        WHERE object_id = OBJECT_ID(''' + REPLACE(@DbHigher,'''','''''') + N'.sys.query_store_runtime_stats_interval'')
          AND name = ''end_time''
    ) THEN 1 ELSE 0 END;';
EXEC sp_executesql @chk, N'@HasEndTimeOut bit OUTPUT', @HasEndTimeOut=@HasEndTime_H OUTPUT;

SET @t1 = SYSDATETIME();
INSERT #Perf (StepName, Ms, RowsAffected, StartAt, EndAt)
VALUES (N'Detect rsi.end_time existence', DATEDIFF(MILLISECOND,@t0,@t1), NULL, @t0, @t1);

-------------------------------------------------------------------------------------------------------------------
-- Step 1: Pre-aggregate runtime stats into #RS_PlanAgg_*
-------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#RS_PlanAgg') IS NOT NULL DROP TABLE #RS_PlanAgg;
CREATE TABLE #RS_PlanAgg
(
    SourceDb         sysname       NOT NULL,
    plan_id          bigint        NOT NULL,
    query_id         bigint        NOT NULL,
    query_text_id    bigint        NOT NULL,
    object_id        int           NULL,
    sum_exec         bigint        NOT NULL,
    total_reads      decimal(38,6) NOT NULL,
    total_cpu        decimal(38,6) NOT NULL,
    total_dur        decimal(38,6) NOT NULL,
    avg_reads        decimal(38,6) NOT NULL,
    avg_cpu          decimal(38,6) NOT NULL,
    avg_dur          decimal(38,6) NOT NULL,
    IntervalStartMin datetime2(0)  NULL,
    IntervalEndMax   datetime2(0)  NULL,
    PRIMARY KEY (SourceDb, plan_id)
);

CREATE INDEX IX_RS_PlanAgg_Query ON #RS_PlanAgg (SourceDb, query_id) INCLUDE (sum_exec, avg_reads, avg_cpu, avg_dur, total_reads, total_cpu, total_dur, object_id, query_text_id);

DECLARE @rsSql nvarchar(max);

SET @t0 = SYSDATETIME();

SET @rsSql = N'
;WITH rsf AS
(
    SELECT
          p.plan_id
        , p.query_id
        , q.query_text_id
        , q.object_id
        , rs.count_executions
        , rs.avg_logical_io_reads
        , rs.avg_cpu_time
        , rs.avg_duration
        , rsi.start_time
        , ' + CASE WHEN @HasEndTime_L = 1 THEN N'rsi.end_time' ELSE N'rsi.start_time' END + N' AS end_time_eff
    FROM ' + QUOTENAME(@DbLower) + N'.sys.query_store_runtime_stats rs
    JOIN ' + QUOTENAME(@DbLower) + N'.sys.query_store_runtime_stats_interval rsi
      ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
    JOIN ' + QUOTENAME(@DbLower) + N'.sys.query_store_plan p
      ON p.plan_id = rs.plan_id
    JOIN ' + QUOTENAME(@DbLower) + N'.sys.query_store_query q
      ON q.query_id = p.query_id
    WHERE 1=1
      AND (
            (@IncludeSP = 1 AND q.object_id > 0)
         OR (@IncludeAdhoc = 1 AND (q.object_id = 0 OR q.object_id IS NULL))
      )
      AND (@StartTime IS NULL OR rsi.start_time >= @StartTime)
      AND (@EndTime   IS NULL OR rsi.start_time <  @EndTime)
),
agg AS
(
    SELECT
          plan_id
        , query_id
        , query_text_id
        , object_id
        , SUM(CONVERT(bigint, count_executions)) AS sum_exec
        , SUM(CONVERT(decimal(38,12), avg_logical_io_reads) * CONVERT(decimal(38,12), count_executions)) AS total_reads
        , SUM(CONVERT(decimal(38,12), avg_cpu_time) * CONVERT(decimal(38,12), count_executions)) AS total_cpu
        , SUM(CONVERT(decimal(38,12), avg_duration) * CONVERT(decimal(38,12), count_executions)) AS total_dur
        , MIN(start_time) AS IntervalStartMin
        , MAX(end_time_eff) AS IntervalEndMax
    FROM rsf
    GROUP BY plan_id, query_id, query_text_id, object_id
)
INSERT #RS_PlanAgg
(
    SourceDb, plan_id, query_id, query_text_id, object_id,
    sum_exec, total_reads, total_cpu, total_dur,
    avg_reads, avg_cpu, avg_dur,
    IntervalStartMin, IntervalEndMax
)
SELECT
      @DbName
    , a.plan_id
    , a.query_id
    , a.query_text_id
    , a.object_id
    , a.sum_exec
    , CONVERT(decimal(38,6), a.total_reads)
    , CONVERT(decimal(38,6), a.total_cpu)
    , CONVERT(decimal(38,6), a.total_dur)
    , CONVERT(decimal(38,6), CASE WHEN a.sum_exec=0 THEN 0 ELSE a.total_reads / a.sum_exec END)
    , CONVERT(decimal(38,6), CASE WHEN a.sum_exec=0 THEN 0 ELSE a.total_cpu   / a.sum_exec END)
    , CONVERT(decimal(38,6), CASE WHEN a.sum_exec=0 THEN 0 ELSE a.total_dur   / a.sum_exec END)
    , a.IntervalStartMin
    , a.IntervalEndMax
FROM agg a;
';

EXEC sp_executesql
    @rsSql,
    N'@DbName sysname, @IncludeAdhoc bit, @IncludeSP bit, @StartTime datetime2(0), @EndTime datetime2(0)',
    @DbName=@DbLower, @IncludeAdhoc=@IncludeAdhoc, @IncludeSP=@IncludeSP, @StartTime=@StartTime, @EndTime=@EndTime;

SET @rsSql = REPLACE(@rsSql, QUOTENAME(@DbLower) + N'.sys.', QUOTENAME(@DbHigher) + N'.sys.');
SET @rsSql = REPLACE(@rsSql, CASE WHEN @HasEndTime_L = 1 THEN N'rsi.end_time' ELSE N'rsi.start_time' END, CASE WHEN @HasEndTime_H = 1 THEN N'rsi.end_time' ELSE N'rsi.start_time' END);

EXEC sp_executesql
    @rsSql,
    N'@DbName sysname, @IncludeAdhoc bit, @IncludeSP bit, @StartTime datetime2(0), @EndTime datetime2(0)',
    @DbName=@DbHigher, @IncludeAdhoc=@IncludeAdhoc, @IncludeSP=@IncludeSP, @StartTime=@StartTime, @EndTime=@EndTime;

SET @t1 = SYSDATETIME();
SELECT @rc = COUNT_BIG(*) FROM #RS_PlanAgg;
INSERT #Perf (StepName, Ms, RowsAffected, StartAt, EndAt)
VALUES (N'Runtime stats into #RS_PlanAgg', DATEDIFF(MILLISECOND,@t0,@t1), @rc, @t0, @t1);

-------------------------------------------------------------------------------------------------------------------
-- Step 2: Build #QS_Text based on query_text_id list
-------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#QS_Text') IS NOT NULL DROP TABLE #QS_Text;
CREATE TABLE #QS_Text
(
    SourceDb         sysname        NOT NULL,
    query_text_id    bigint         NOT NULL,
    QueryTextSample  nvarchar(4000) NULL,
    NormalizedSample nvarchar(4000) NULL,
    QueryTextHash    varbinary(32)  NULL,
    NormalizedHash   varbinary(32)  NULL,
    DetectedType     varchar(10)    NULL,
    PRIMARY KEY (SourceDb, query_text_id)
);

SET @t0 = SYSDATETIME();

DECLARE @textSql nvarchar(max);

SET @textSql = N'
;WITH ids AS
(
    SELECT DISTINCT query_text_id
    FROM #RS_PlanAgg
    WHERE SourceDb = @DbName
)
INSERT #QS_Text (SourceDb, query_text_id, QueryTextSample, NormalizedSample, QueryTextHash, NormalizedHash, DetectedType)
SELECT
      @DbName
    , qt.query_text_id
    , LEFT(qt.query_sql_text, 4000) AS QueryTextSample
    , LEFT(LOWER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(qt.query_sql_text, CHAR(13), '' ''), CHAR(10), '' ''), CHAR(9), '' '')))), 4000) AS NormalizedSample
    , HASHBYTES(''SHA2_256'', CONVERT(varbinary(max), LEFT(qt.query_sql_text, 4000)))
    , HASHBYTES(''SHA2_256'', CONVERT(varbinary(max), LEFT(LOWER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(qt.query_sql_text, CHAR(13), '' ''), CHAR(10), '' ''), CHAR(9), '' '')))), 4000)))
    , CASE
          WHEN @StatementType = ''ALL'' THEN NULL
          ELSE
          (
              SELECT TOP (1) v.Typ
              FROM
              (
                  SELECT Typ=''SELECT'',
                         Pos=CASE
                                 WHEN UPPER(LTRIM(x.CleanNoParamBlock)) LIKE ''SELECT%'' THEN 1
                                 ELSE NULLIF(PATINDEX(''%SELECT%'', UPPER(x.CleanNoParamBlock)),0)
                             END
                  UNION ALL
                  SELECT Typ=''INSERT'',
                         Pos=CASE
                                 WHEN UPPER(LTRIM(x.CleanNoParamBlock)) LIKE ''INSERT%'' THEN 1
                                 ELSE NULLIF(PATINDEX(''%INSERT%'', UPPER(x.CleanNoParamBlock)),0)
                             END
                  UNION ALL
                  SELECT Typ=''UPDATE'',
                         Pos=CASE
                                 WHEN UPPER(LTRIM(x.CleanNoParamBlock)) LIKE ''UPDATE%'' THEN 1
                                 ELSE NULLIF(PATINDEX(''%UPDATE%'', UPPER(x.CleanNoParamBlock)),0)
                             END
                  UNION ALL
                  SELECT Typ=''DELETE'',
                         Pos=CASE
                                 WHEN UPPER(LTRIM(x.CleanNoParamBlock)) LIKE ''DELETE%'' THEN 1
                                 ELSE NULLIF(PATINDEX(''%DELETE%'', UPPER(x.CleanNoParamBlock)),0)
                             END
              ) v
              WHERE v.Pos IS NOT NULL
              ORDER BY v.Pos ASC
          )
      END
FROM ids
JOIN ' + QUOTENAME(@DbLower) + N'.sys.query_store_query_text qt
  ON qt.query_text_id = ids.query_text_id
OUTER APPLY
(
    SELECT RawClean =
        LTRIM(REPLACE(REPLACE(REPLACE(LEFT(qt.query_sql_text, 4000), CHAR(13), '' ''), CHAR(10), '' ''), CHAR(9), '' ''))
) c0
OUTER APPLY
(
    SELECT CleanNoParamBlock =
        CASE
            WHEN c0.RawClean LIKE ''(%'' AND CHARINDEX('')'', c0.RawClean) > 0
                THEN LTRIM(SUBSTRING(c0.RawClean, CHARINDEX('')'', c0.RawClean) + 1, 4000))
            ELSE c0.RawClean
        END
) x;
';

EXEC sp_executesql
    @textSql,
    N'@DbName sysname, @StatementType varchar(10)',
    @DbName=@DbLower, @StatementType=@StatementType;

SET @textSql = REPLACE(@textSql, QUOTENAME(@DbLower) + N'.sys.', QUOTENAME(@DbHigher) + N'.sys.');

EXEC sp_executesql
    @textSql,
    N'@DbName sysname, @StatementType varchar(10)',
    @DbName=@DbHigher, @StatementType=@StatementType;

SET @t1 = SYSDATETIME();
SELECT @rc = COUNT_BIG(*) FROM #QS_Text;
INSERT #Perf (StepName, Ms, RowsAffected, StartAt, EndAt)
VALUES (N'Build #QS_Text', DATEDIFF(MILLISECOND,@t0,@t1), @rc, @t0, @t1);

-------------------------------------------------------------------------------------------------------------------
-- Step 3: Build #QS_Agg using #RS_PlanAgg + metadata + precomputed hashes
-------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#QS_Agg') IS NOT NULL DROP TABLE #QS_Agg;
CREATE TABLE #QS_Agg
(
    SourceDb              sysname        NOT NULL,
    QueryType             varchar(10)    NOT NULL,
    ObjName               sysname        NULL,
    GroupKeyHash          varbinary(32)  NOT NULL,
    QueryHash             binary(8)      NULL,
    QueryTextSample       nvarchar(4000) NULL,
    NormalizedTextSample  nvarchar(4000) NULL,
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
    ConfidenceNote        varchar(80)    NOT NULL
);

CREATE INDEX IX_QS_Agg_Key
ON #QS_Agg (SourceDb, GroupKeyHash, QueryType)
INCLUDE (ObjName, ExecCount, AvgMetric, TotalMetric, PlanCount, QueryHash);

SET @t0 = SYSDATETIME();

DECLARE @aggSql nvarchar(max);

SET @aggSql = N'
INSERT #QS_Agg
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
      r.SourceDb
    , CASE WHEN q.object_id > 0 THEN ''SP'' ELSE ''Adhoc'' END AS QueryType
    , CASE WHEN q.object_id > 0 THEN (sch.name + ''.'' + obj.name)' + @CollateClause + N' ELSE NULL END AS ObjName
    , CASE
          WHEN @GroupBy = ''QueryHash'' THEN HASHBYTES(''SHA2_256'', CONVERT(varbinary(8), q.query_hash))
          WHEN @GroupBy = ''QueryText'' THEN t.QueryTextHash
          ELSE t.NormalizedHash
      END AS GroupKeyHash
    , q.query_hash
    , MIN(t.QueryTextSample)      AS QueryTextSample
    , MIN(t.NormalizedSample)     AS NormalizedTextSample
    , MIN(q.query_id)             AS QueryIdMin
    , MAX(q.query_id)             AS QueryIdMax
    , COUNT(DISTINCT r.plan_id)   AS PlanCount
    , SUM(r.sum_exec)             AS ExecCount
    , SUM(
        CASE
            WHEN @Metric = ''LogicalReads'' THEN r.total_reads
            WHEN @Metric = ''CPU''         THEN r.total_cpu
            ELSE                                 r.total_dur
        END
      ) AS TotalMetric
    , CASE WHEN SUM(r.sum_exec)=0 THEN 0
           ELSE
             SUM(
                CASE
                    WHEN @Metric = ''LogicalReads'' THEN r.total_reads
                    WHEN @Metric = ''CPU''         THEN r.total_cpu
                    ELSE                                 r.total_dur
                END
             ) / SUM(r.sum_exec)
      END AS AvgMetric
    , SUM(r.total_dur) AS TotalDuration
    , CASE WHEN SUM(r.sum_exec)=0 THEN 0 ELSE SUM(r.total_dur) / SUM(r.sum_exec) END AS AvgDuration
    , MIN(r.IntervalStartMin) AS IntervalStartMin
    , MAX(r.IntervalEndMax)   AS IntervalEndMax
    , ''WEIGHTED_TOTAL;PLAN_FIRST_PREAGG'' AS ConfidenceNote
FROM #RS_PlanAgg r
JOIN ' + QUOTENAME(@DbLower) + N'.sys.query_store_query q
  ON q.query_id = r.query_id
JOIN #QS_Text t
  ON t.SourceDb' + @CollateClause + N' = r.SourceDb' + @CollateClause + N'
 AND t.query_text_id = r.query_text_id
LEFT JOIN ' + QUOTENAME(@DbLower) + N'.sys.objects obj
  ON obj.object_id = q.object_id
LEFT JOIN ' + QUOTENAME(@DbLower) + N'.sys.schemas sch
  ON sch.schema_id = obj.schema_id
WHERE r.SourceDb = @DbName
  AND (
        @StatementType = ''ALL''
        OR t.DetectedType = @StatementType
      )
GROUP BY
      r.SourceDb
    , q.object_id, sch.name, obj.name
    , q.query_hash
    , CASE
          WHEN @GroupBy = ''QueryHash'' THEN HASHBYTES(''SHA2_256'', CONVERT(varbinary(8), q.query_hash))
          WHEN @GroupBy = ''QueryText'' THEN t.QueryTextHash
          ELSE t.NormalizedHash
      END;
';

EXEC sp_executesql
    @aggSql,
    N'@DbName sysname, @Metric sysname, @GroupBy sysname, @StatementType varchar(10)',
    @DbName=@DbLower, @Metric=@Metric, @GroupBy=@GroupBy, @StatementType=@StatementType;

SET @aggSql = REPLACE(@aggSql, QUOTENAME(@DbLower) + N'.sys.', QUOTENAME(@DbHigher) + N'.sys.');

EXEC sp_executesql
    @aggSql,
    N'@DbName sysname, @Metric sysname, @GroupBy sysname, @StatementType varchar(10)',
    @DbName=@DbHigher, @Metric=@Metric, @GroupBy=@GroupBy, @StatementType=@StatementType;

SET @t1 = SYSDATETIME();
SELECT @rc = COUNT_BIG(*) FROM #QS_Agg;
INSERT #Perf (StepName, Ms, RowsAffected, StartAt, EndAt)
VALUES (N'Build #QS_Agg', DATEDIFF(MILLISECOND,@t0,@t1), @rc, @t0, @t1);


-------------------------------------------------------------------------------------------------------------------
-- Compare
-------------------------------------------------------------------------------------------------------------------
SET @t0 = SYSDATETIME();

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
    QueryTextSample      nvarchar(4000) NULL
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
          COALESCE(CASE WHEN ISNULL(a.ExecCount,0)=0 OR ISNULL(b.ExecCount,0)=0 THEN 'MISSING_ONE_SIDE;' ELSE '' END,'')
        + COALESCE(CASE WHEN @MinExecCount IS NOT NULL AND (ISNULL(a.ExecCount,0)<@MinExecCount OR ISNULL(b.ExecCount,0)<@MinExecCount) THEN 'LOW_EXEC;' ELSE '' END,'')
        + COALESCE(CASE WHEN ISNULL(a.PlanCount,0)>1 OR ISNULL(b.PlanCount,0)>1 THEN 'MULTI_PLAN;' ELSE '' END,'')
        + COALESCE(a.ConfidenceNote + ';','')
        + COALESCE(b.ConfidenceNote + ';','')
      AS varchar(500)) AS ConfidenceFlags
    , COALESCE(b.QueryTextSample, a.QueryTextSample) AS QueryTextSample
FROM a
FULL OUTER JOIN b
  ON a.GroupKeyHash = b.GroupKeyHash
 AND ISNULL(a.QueryType,'') = ISNULL(b.QueryType,'')
 AND ISNULL(a.ObjName,'')   = ISNULL(b.ObjName,'');

SET @t1 = SYSDATETIME();
SELECT @rc = COUNT_BIG(*) FROM #Compare;
INSERT #Perf (StepName, Ms, RowsAffected, StartAt, EndAt)
VALUES (N'Build #Compare', DATEDIFF(MILLISECOND,@t0,@t1), @rc, @t0, @t1);

-------------------------------------------------------------------------------------------------------------------
-- Temp Table: #FilteredGroups
-------------------------------------------------------------------------------------------------------------------
SET @t0 = SYSDATETIME();

IF OBJECT_ID('tempdb..#FilteredGroups') IS NOT NULL DROP TABLE #FilteredGroups;
CREATE TABLE #FilteredGroups
(
    GroupKeyHash varbinary(32) NOT NULL,
    QueryType    varchar(10)   NOT NULL,
    ObjNameNorm  sysname       NOT NULL,
    PRIMARY KEY (GroupKeyHash, QueryType, ObjNameNorm)
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

SET @t1 = SYSDATETIME();
SELECT @rc = COUNT_BIG(*) FROM #FilteredGroups;
INSERT #Perf (StepName, Ms, RowsAffected, StartAt, EndAt)
VALUES (N'Build #FilteredGroups', DATEDIFF(MILLISECOND,@t0,@t1), @rc, @t0, @t1);

-------------------------------------------------------------------------------------------------------------------
-- Temp Table: #DominantPlans_All
-------------------------------------------------------------------------------------------------------------------
SET @t0 = SYSDATETIME();

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

DECLARE @domSql nvarchar(max);

SET @domSql = N'
;WITH base AS
(
    SELECT
          r.SourceDb
        , r.plan_id
        , r.query_id
        , r.sum_exec
        , CASE WHEN @Metric = ''LogicalReads'' THEN r.avg_reads
               WHEN @Metric = ''CPU''         THEN r.avg_cpu
               ELSE                                 r.avg_dur
          END AS AvgMetricSel
        , q.object_id
        , q.query_hash
        , CASE WHEN q.object_id > 0 THEN ''SP'' ELSE ''Adhoc'' END AS QueryType
        , CASE WHEN q.object_id > 0 THEN (sch.name + ''.'' + obj.name)' + @CollateClause + N' ELSE NULL END AS ObjName
        , CASE
              WHEN @GroupBy = ''QueryHash'' THEN HASHBYTES(''SHA2_256'', CONVERT(varbinary(8), q.query_hash))
              WHEN @GroupBy = ''QueryText'' THEN t.QueryTextHash
              ELSE t.NormalizedHash
          END AS GroupKeyHash
    FROM #RS_PlanAgg r
    JOIN ' + QUOTENAME(@DbLower) + N'.sys.query_store_query q
      ON q.query_id = r.query_id
    JOIN #QS_Text t
      ON t.SourceDb' + @CollateClause + N' = r.SourceDb' + @CollateClause + N'
     AND t.query_text_id = r.query_text_id
    LEFT JOIN ' + QUOTENAME(@DbLower) + N'.sys.objects obj
      ON obj.object_id = q.object_id
    LEFT JOIN ' + QUOTENAME(@DbLower) + N'.sys.schemas sch
      ON sch.schema_id = obj.schema_id
    WHERE r.SourceDb = @DbName
      AND (
            @StatementType = ''ALL''
            OR t.DetectedType = @StatementType
          )
      AND EXISTS
      (
          SELECT 1
          FROM #FilteredGroups fg
          WHERE fg.GroupKeyHash = CASE
                                     WHEN @GroupBy = ''QueryHash'' THEN HASHBYTES(''SHA2_256'', CONVERT(varbinary(8), q.query_hash))
                                     WHEN @GroupBy = ''QueryText'' THEN t.QueryTextHash
                                     ELSE t.NormalizedHash
                                  END
            AND fg.QueryType = CASE WHEN q.object_id > 0 THEN ''SP'' ELSE ''Adhoc'' END
            AND fg.ObjNameNorm' + @CollateClause + N' = ISNULL(CASE WHEN q.object_id > 0 THEN (sch.name + ''.'' + obj.name)' + @CollateClause + N' ELSE NULL END, '''')' + @CollateClause + N'
      )
),
r AS
(
    SELECT
          b.SourceDb, b.GroupKeyHash, b.QueryType, b.ObjName
        , b.plan_id AS PlanId
        , b.query_id AS QueryId
        , b.sum_exec AS ExecCount
        , CONVERT(decimal(38,6), b.AvgMetricSel) AS AvgMetric
        , ROW_NUMBER() OVER
          (
            PARTITION BY b.SourceDb, b.GroupKeyHash, b.QueryType, ISNULL(b.ObjName,'''')
            ORDER BY b.sum_exec DESC, b.AvgMetricSel DESC, b.plan_id DESC
          ) AS rn
    FROM base b
)
INSERT #DominantPlans_All (SourceDb, GroupKeyHash, QueryType, ObjName, PlanId, QueryId, ExecCount, AvgMetric)
SELECT SourceDb, GroupKeyHash, QueryType, ObjName, PlanId, QueryId, ExecCount, AvgMetric
FROM r
WHERE rn = 1;
';

EXEC sp_executesql
    @domSql,
    N'@DbName sysname, @Metric sysname, @GroupBy sysname, @StatementType varchar(10)',
    @DbName=@DbLower, @Metric=@Metric, @GroupBy=@GroupBy, @StatementType=@StatementType;

SET @domSql = REPLACE(@domSql, QUOTENAME(@DbLower) + N'.sys.', QUOTENAME(@DbHigher) + N'.sys.');

EXEC sp_executesql
    @domSql,
    N'@DbName sysname, @Metric sysname, @GroupBy sysname, @StatementType varchar(10)',
    @DbName=@DbHigher, @Metric=@Metric, @GroupBy=@GroupBy, @StatementType=@StatementType;

SET @t1 = SYSDATETIME();
SELECT @rc = COUNT_BIG(*) FROM #DominantPlans_All;
INSERT #Perf (StepName, Ms, RowsAffected, StartAt, EndAt)
VALUES (N'Build #DominantPlans_All', DATEDIFF(MILLISECOND,@t0,@t1), @rc, @t0, @t1);

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
    , CONCAT(CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(f.TotalMetric_L, 2))), '' - '', CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(f.TotalMetric_H, 2)))) AS [TotalMetric_L-H]
    , CONCAT(CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(f.AvgMetric_L, 2))),   '' - '', CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(f.AvgMetric_H, 2)))) AS [AvgMetric_L-H]
    , CONCAT(CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(f.TotalDuration_L, 2))), '' - '', CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(f.TotalDuration_H, 2)))) AS [TotalDuration_L-H]
    , CONCAT(CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(f.AvgDuration_L, 2))),   '' - '', CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(f.AvgDuration_H, 2)))) AS [AvgDuration_L-H]
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
    , CONCAT(
          CONVERT(varchar(30), SUM(CASE WHEN QueryType='SP'    THEN ExecCount_L ELSE 0 END)),
          ' - ',
          CONVERT(varchar(30), SUM(CASE WHEN QueryType='SP'    THEN ExecCount_H ELSE 0 END))
      ) AS [TotalExecCount_L-H_SP]
    , CONCAT(
          CONVERT(varchar(30), SUM(CASE WHEN QueryType='Adhoc' THEN ExecCount_L ELSE 0 END)),
          ' - ',
          CONVERT(varchar(30), SUM(CASE WHEN QueryType='Adhoc' THEN ExecCount_H ELSE 0 END))
      ) AS [TotalExecCount_L-H_Adhoc]
    , CONCAT(
          CONVERT(varchar(30), SUM(ExecCount_L)),
          ' - ',
          CONVERT(varchar(30), SUM(ExecCount_H))
      ) AS [TotalExecCount_L-H_All]
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

IF EXISTS (SELECT 1 FROM #MultiGroups)
BEGIN
    SET @t0 = SYSDATETIME();

    DECLARE @planSql nvarchar(max);

    SET @planSql = N'
    INSERT #PlanAgg
    (
        SourceDb, QueryType, ObjName, GroupKeyHash,
        PlanId, QueryId,
        ExecCount, TotalMetric, AvgMetric,
        TotalDuration, AvgDuration,
        IntervalStartMin, IntervalEndMax,
        PlanXmlHash
    )
    SELECT
          r.SourceDb
        , CASE WHEN q.object_id > 0 THEN ''SP'' ELSE ''Adhoc'' END AS QueryType
        , CASE WHEN q.object_id > 0 THEN (sch.name + ''.'' + obj.name)' + @CollateClause + N' ELSE NULL END AS ObjName
        , CASE
              WHEN @GroupBy = ''QueryHash'' THEN HASHBYTES(''SHA2_256'', CONVERT(varbinary(8), q.query_hash))
              WHEN @GroupBy = ''QueryText'' THEN t.QueryTextHash
              ELSE t.NormalizedHash
          END AS GroupKeyHash
        , r.plan_id AS PlanId
        , r.query_id AS QueryId
        , r.sum_exec AS ExecCount
        , CONVERT(decimal(38,6),
              CASE WHEN @Metric = ''LogicalReads'' THEN r.total_reads
                   WHEN @Metric = ''CPU''         THEN r.total_cpu
                   ELSE                                 r.total_dur
              END
          ) AS TotalMetric
        , CONVERT(decimal(38,6),
              CASE WHEN @Metric = ''LogicalReads'' THEN r.avg_reads
                   WHEN @Metric = ''CPU''         THEN r.avg_cpu
                   ELSE                                 r.avg_dur
              END
          ) AS AvgMetric
        , r.total_dur AS TotalDuration
        , r.avg_dur   AS AvgDuration
        , r.IntervalStartMin
        , r.IntervalEndMax
        , HASHBYTES(''SHA2_256'', CONVERT(varbinary(max), CONVERT(nvarchar(max), p.query_plan))) AS PlanXmlHash
    FROM #RS_PlanAgg r
    JOIN ' + QUOTENAME(@DbLower) + N'.sys.query_store_query q
      ON q.query_id = r.query_id
    JOIN ' + QUOTENAME(@DbLower) + N'.sys.query_store_plan p
      ON p.plan_id = r.plan_id
    JOIN #QS_Text t
      ON t.SourceDb' + @CollateClause + N' = r.SourceDb' + @CollateClause + N'
     AND t.query_text_id = r.query_text_id
    LEFT JOIN ' + QUOTENAME(@DbLower) + N'.sys.objects obj
      ON obj.object_id = q.object_id
    LEFT JOIN ' + QUOTENAME(@DbLower) + N'.sys.schemas sch
      ON sch.schema_id = obj.schema_id
    WHERE r.SourceDb = @DbName
      AND (
            @StatementType = ''ALL''
            OR t.DetectedType = @StatementType
          )
      AND EXISTS
      (
          SELECT 1
          FROM #MultiGroups mg
          WHERE mg.GroupKeyHash = CASE
                                     WHEN @GroupBy = ''QueryHash'' THEN HASHBYTES(''SHA2_256'', CONVERT(varbinary(8), q.query_hash))
                                     WHEN @GroupBy = ''QueryText'' THEN t.QueryTextHash
                                     ELSE t.NormalizedHash
                                  END
            AND mg.QueryType = CASE WHEN q.object_id > 0 THEN ''SP'' ELSE ''Adhoc'' END
            AND ISNULL(mg.ObjName, '''')' + @CollateClause + N' = ISNULL(CASE WHEN q.object_id > 0 THEN (sch.name + ''.'' + obj.name)' + @CollateClause + N' ELSE NULL END, '''')' + @CollateClause + N'
      );
    ';

    EXEC sp_executesql
        @planSql,
        N'@DbName sysname, @Metric sysname, @GroupBy sysname, @StatementType varchar(10)',
        @DbName=@DbLower, @Metric=@Metric, @GroupBy=@GroupBy, @StatementType=@StatementType;

    SET @planSql = REPLACE(@planSql, QUOTENAME(@DbLower) + N'.sys.', QUOTENAME(@DbHigher) + N'.sys.');

    EXEC sp_executesql
        @planSql,
        N'@DbName sysname, @Metric sysname, @GroupBy sysname, @StatementType varchar(10)',
        @DbName=@DbHigher, @Metric=@Metric, @GroupBy=@GroupBy, @StatementType=@StatementType;

    SET @t1 = SYSDATETIME();
    SELECT @rc = COUNT_BIG(*) FROM #PlanAgg;
    INSERT #Perf (StepName, Ms, RowsAffected, StartAt, EndAt)
    VALUES (N'Build MultiPlan drilldown', DATEDIFF(MILLISECOND,@t0,@t1), @rc, @t0, @t1);

    ;WITH ranked AS
    (
        SELECT
              pa.SourceDb
            , CASE WHEN pa.SourceDb = @DbLower THEN CONVERT(varchar(10), @LowerCL) ELSE CONVERT(varchar(10), @HigherCL) END AS SourceCL
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
          SourceDb, SourceCL, Side, QueryType, ObjName, GroupKeyHashHex,
          PlanId, QueryId, ExecCount, TotalMetric, AvgMetric, TotalDuration, AvgDuration,
          IntervalStartMin, IntervalEndMax, PlanXmlHashHex, RankByAvgMetric, RankByExecCount
    FROM ranked
    ORDER BY GroupKeyHashHex, Side, RankByAvgMetric, RankByExecCount;

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
              pa.SourceDb, pa.GroupKeyHash, pa.QueryType, pa.ObjName,
              pa.PlanId, pa.QueryId, pa.ExecCount, pa.AvgMetric, pa.TotalMetric,
              ROW_NUMBER() OVER (PARTITION BY pa.SourceDb, pa.GroupKeyHash ORDER BY pa.ExecCount DESC, pa.AvgMetric DESC, pa.PlanId DESC) AS rn
        FROM #PlanAgg pa
    )
    INSERT #DominantPlans
    SELECT SourceDb, GroupKeyHash, QueryType, ObjName, PlanId, QueryId, ExecCount, AvgMetric, TotalMetric
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
    WHERE dp.PlanId_H IS NOT NULL;';
    EXEC sp_executesql @px, N'@DbName sysname', @DbName=@DbHigher;

    IF OBJECT_ID('tempdb..#PlanInd') IS NOT NULL DROP TABLE #PlanInd;
    CREATE TABLE #PlanInd
    (
        SourceDb                 sysname       NOT NULL,
        GroupKeyHash             varbinary(32) NOT NULL,
        QueryType                varchar(10)   NOT NULL,
        ObjName                  sysname       NULL,
        PlanId                   bigint        NOT NULL,
        QueryId                  bigint        NULL,
        QueryPlanXml             xml           NULL,
        PlanXmlHash              varbinary(32) NULL,
        IndexSeekCount           int NULL,
        IndexScanCount           int NULL,
        TableScanCount           int NULL,
        KeyLookupCount           int NULL,
        RIDLookupCount           int NULL,
        HasHashJoin              bit NULL,
        HasMergeJoin             bit NULL,
        HasNestedLoops           bit NULL,
        SortCount                int NULL,
        ComputeScalarCount       int NULL,
        FilterCount              int NULL,
        SpoolCount               int NULL,
        HashAggCount             int NULL,
        StreamAggCount           int NULL,
        HasParallelism           bit NULL,
        HasAdaptiveJoin          bit NULL,
        HasBatchMode             bit NULL,
        HasColumnstore           bit NULL,
        RequestedMemoryKB        bigint NULL,
        GrantedMemoryKB          bigint NULL,
        UsedMemoryKB             bigint NULL,
        HasMemoryGrantWarning    bit NULL,
        HasSpillToTempDb         bit NULL,
        HasMissingIndex          bit NULL,
        HasImplicitConversion    bit NULL,
        HasNoJoinPredicateWarn   bit NULL,
        HasMissingStatsWarn      bit NULL
    );

    ;WITH x AS
    (
        SELECT
              px.SourceDb, px.GroupKeyHash, px.QueryType, px.ObjName, px.PlanId, px.QueryId
            , px.QueryPlanXml AS PlanXml
            , px.QueryPlanText
        FROM #PlanXml px
    )
    INSERT #PlanInd
    (
        SourceDb, GroupKeyHash, QueryType, ObjName, PlanId, QueryId,
        QueryPlanXml, PlanXmlHash,
        IndexSeekCount, IndexScanCount, TableScanCount, KeyLookupCount, RIDLookupCount,
        HasHashJoin, HasMergeJoin, HasNestedLoops,
        SortCount, ComputeScalarCount, FilterCount, SpoolCount,
        HashAggCount, StreamAggCount,
        HasParallelism, HasAdaptiveJoin, HasBatchMode, HasColumnstore,
        RequestedMemoryKB, GrantedMemoryKB, UsedMemoryKB, HasMemoryGrantWarning,
        HasSpillToTempDb, HasMissingIndex, HasImplicitConversion, HasNoJoinPredicateWarn, HasMissingStatsWarn
    )
    SELECT
          x.SourceDb, x.GroupKeyHash, x.QueryType, x.ObjName, x.PlanId, x.QueryId
        , x.PlanXml
        , CASE WHEN x.QueryPlanText IS NULL THEN NULL ELSE HASHBYTES('SHA2_256', CONVERT(varbinary(max), x.QueryPlanText)) END AS PlanXmlHash
        , CASE WHEN x.PlanXml IS NULL THEN NULL ELSE (SELECT COUNT(*) FROM x.PlanXml.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Index Seek"]')  AS t(n)) END
        , CASE WHEN x.PlanXml IS NULL THEN NULL ELSE (SELECT COUNT(*) FROM x.PlanXml.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Index Scan"]')  AS t(n)) END
        , CASE WHEN x.PlanXml IS NULL THEN NULL ELSE (SELECT COUNT(*) FROM x.PlanXml.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Table Scan"]')  AS t(n)) END
        , CASE WHEN x.PlanXml IS NULL THEN NULL ELSE (SELECT COUNT(*) FROM x.PlanXml.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Key Lookup"]') AS t(n)) END
        , CASE WHEN x.PlanXml IS NULL THEN NULL ELSE (SELECT COUNT(*) FROM x.PlanXml.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="RID Lookup"]') AS t(n)) END
        , CASE WHEN x.PlanXml IS NULL THEN NULL WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Hash Match"]')    = 1 THEN 1 ELSE 0 END
        , CASE WHEN x.PlanXml IS NULL THEN NULL WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Merge Join"]')    = 1 THEN 1 ELSE 0 END
        , CASE WHEN x.PlanXml IS NULL THEN NULL WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Nested Loops"]')  = 1 THEN 1 ELSE 0 END
        , CASE WHEN x.PlanXml IS NULL THEN NULL ELSE (SELECT COUNT(*) FROM x.PlanXml.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Sort"]')           AS t(n)) END
        , CASE WHEN x.PlanXml IS NULL THEN NULL ELSE (SELECT COUNT(*) FROM x.PlanXml.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Compute Scalar"]') AS t(n)) END
        , CASE WHEN x.PlanXml IS NULL THEN NULL ELSE (SELECT COUNT(*) FROM x.PlanXml.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Filter"]')         AS t(n)) END
        , CASE WHEN x.PlanXml IS NULL THEN NULL ELSE (SELECT COUNT(*) FROM x.PlanXml.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[contains(@PhysicalOp,"Spool")]') AS t(n)) END
        , CASE WHEN x.PlanXml IS NULL THEN NULL ELSE (SELECT COUNT(*) FROM x.PlanXml.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@LogicalOp="Aggregate" and @PhysicalOp="Hash Match"]') AS t(n)) END
        , CASE WHEN x.PlanXml IS NULL THEN NULL ELSE (SELECT COUNT(*) FROM x.PlanXml.nodes('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Stream Aggregate"]')                 AS t(n)) END
        , CASE WHEN x.PlanXml IS NULL THEN NULL WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Parallelism"]') = 1 THEN 1 ELSE 0 END
        , CASE WHEN x.PlanXml IS NULL THEN NULL WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@PhysicalOp="Adaptive Join"]') = 1 THEN 1 ELSE 0 END
        , CASE WHEN x.PlanXml IS NULL THEN NULL WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //RelOp[@EstimatedExecutionMode="Batch"]') = 1 THEN 1 ELSE 0 END
        , CASE 
              WHEN x.PlanXml IS NULL THEN NULL
              WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //IndexScan[@Storage="ColumnStore"]') = 1
                OR x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //IndexSeek[@Storage="ColumnStore"]') = 1
              THEN 1 ELSE 0 END
        , CASE WHEN x.PlanXml IS NULL THEN NULL
               ELSE TRY_CONVERT(bigint, x.PlanXml.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; (//MemoryGrantInfo/@RequestedMemory)[1]', 'nvarchar(50)'))
          END
        , CASE WHEN x.PlanXml IS NULL THEN NULL
               ELSE TRY_CONVERT(bigint, x.PlanXml.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; (//MemoryGrantInfo/@GrantedMemory)[1]', 'nvarchar(50)'))
          END
        , CASE WHEN x.PlanXml IS NULL THEN NULL
               ELSE TRY_CONVERT(bigint, x.PlanXml.value('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; (//MemoryGrantInfo/@UsedMemory)[1]', 'nvarchar(50)'))
          END
        , CASE WHEN x.PlanXml IS NULL THEN NULL
               WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //Warnings/MemoryGrantWarning') = 1 THEN 1 ELSE 0 END
        , CASE WHEN x.PlanXml IS NULL THEN NULL WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //Warnings/SpillToTempDb') = 1 THEN 1 ELSE 0 END
        , CASE WHEN x.PlanXml IS NULL THEN NULL WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //MissingIndexes')          = 1 THEN 1 ELSE 0 END
        , CASE WHEN x.PlanXml IS NULL THEN NULL WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //Warnings/PlanAffectingConvert') = 1 THEN 1 ELSE 0 END
        , CASE WHEN x.PlanXml IS NULL THEN NULL WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //Warnings/NoJoinPredicate')     = 1 THEN 1 ELSE 0 END
        , CASE WHEN x.PlanXml IS NULL THEN NULL WHEN x.PlanXml.exist('declare default element namespace "http://schemas.microsoft.com/sqlserver/2004/07/showplan"; //Warnings/MissingStatistics')    = 1 THEN 1 ELSE 0 END
    FROM x;

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
    )
    , pL AS (SELECT * FROM #PlanInd WHERE SourceDb = @DbLower)
    , pH AS (SELECT * FROM #PlanInd WHERE SourceDb = @DbHigher)
    SELECT
          c.QueryType
        , c.ObjName
        , CONVERT(varchar(66), c.GroupKeyHash, 1) AS GroupKeyHashHex
        , CONVERT(decimal(38,2), ROUND(c.ImpactScore, 2))     AS ImpactScore
        , CONVERT(decimal(19,2), ROUND(c.RegressionRatio, 2)) AS RegressionRatio
        , CONCAT(COALESCE(CONVERT(varchar(30), c.ExecCount_L), ''?''), '' - '', COALESCE(CONVERT(varchar(30), c.ExecCount_H), ''?'')) AS [ExecCount_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(c.AvgMetric_L, 2))), ''?''), '' - '', COALESCE(CONVERT(varchar(30), CONVERT(decimal(38,2), ROUND(c.AvgMetric_H, 2))), ''?'')) AS [AvgMetric_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(30), dp.PlanId_L), ''?''), '' - '', COALESCE(CONVERT(varchar(30), dp.PlanId_H), ''?'')) AS [DominantPlanId_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(66), pL.PlanXmlHash, 1), ''?''), '' - '', COALESCE(CONVERT(varchar(66), pH.PlanXmlHash, 1), ''?'')) AS [PlanXmlHashHex_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(30), pL.IndexSeekCount),  ''?''), '' - '', COALESCE(CONVERT(varchar(30), pH.IndexSeekCount),  ''?'')) AS [IndexSeekCount_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(30), pL.IndexScanCount),  ''?''), '' - '', COALESCE(CONVERT(varchar(30), pH.IndexScanCount),  ''?'')) AS [IndexScanCount_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(30), pL.TableScanCount),  ''?''), '' - '', COALESCE(CONVERT(varchar(30), pH.TableScanCount),  ''?'')) AS [TableScanCount_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(10), pL.HasHashJoin),    ''?''), '' - '', COALESCE(CONVERT(varchar(10), pH.HasHashJoin),    ''?'')) AS [HasHashJoin_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(10), pL.HasMergeJoin),   ''?''), '' - '', COALESCE(CONVERT(varchar(10), pH.HasMergeJoin),   ''?'')) AS [HasMergeJoin_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(10), pL.HasNestedLoops), ''?''), '' - '', COALESCE(CONVERT(varchar(10), pH.HasNestedLoops), ''?'')) AS [HasNestedLoops_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(10), pL.HasParallelism), ''?''), '' - '', COALESCE(CONVERT(varchar(10), pH.HasParallelism), ''?'')) AS [HasParallelism_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(30), pL.GrantedMemoryKB), ''?''), '' - '', COALESCE(CONVERT(varchar(30), pH.GrantedMemoryKB), ''?'')) AS [GrantedMemoryKB_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(10), pL.HasSpillToTempDb), ''?''), '' - '', COALESCE(CONVERT(varchar(10), pH.HasSpillToTempDb), ''?'')) AS [SpillToTempDb_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(10), pL.HasMissingIndex), ''?''), '' - '', COALESCE(CONVERT(varchar(10), pH.HasMissingIndex), ''?'')) AS [MissingIndex_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(30), pL.KeyLookupCount), ''?''), '' - '', COALESCE(CONVERT(varchar(30), pH.KeyLookupCount), ''?'')) AS [KeyLookupCount_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(30), pL.RIDLookupCount), ''?''), '' - '', COALESCE(CONVERT(varchar(30), pH.RIDLookupCount), ''?'')) AS [RIDLookupCount_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(30), pL.SortCount), ''?''), '' - '', COALESCE(CONVERT(varchar(30), pH.SortCount), ''?'')) AS [SortCount_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(30), pL.HashAggCount), ''?''), '' - '', COALESCE(CONVERT(varchar(30), pH.HashAggCount), ''?'')) AS [HashAggCount_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(30), pL.StreamAggCount), ''?''), '' - '', COALESCE(CONVERT(varchar(30), pH.StreamAggCount), ''?'')) AS [StreamAggCount_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(30), pL.SpoolCount), ''?''), '' - '', COALESCE(CONVERT(varchar(30), pH.SpoolCount), ''?'')) AS [SpoolCount_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(30), pL.ComputeScalarCount), ''?''), '' - '', COALESCE(CONVERT(varchar(30), pH.ComputeScalarCount), ''?'')) AS [ComputeScalarCount_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(30), pL.FilterCount), ''?''), '' - '', COALESCE(CONVERT(varchar(30), pH.FilterCount), ''?'')) AS [FilterCount_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(10), pL.HasAdaptiveJoin), ''?''), '' - '', COALESCE(CONVERT(varchar(10), pH.HasAdaptiveJoin), ''?'')) AS [HasAdaptiveJoin_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(10), pL.HasBatchMode), ''?''), '' - '', COALESCE(CONVERT(varchar(10), pH.HasBatchMode), ''?'')) AS [HasBatchMode_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(10), pL.HasColumnstore), ''?''), '' - '', COALESCE(CONVERT(varchar(10), pH.HasColumnstore), ''?'')) AS [HasColumnstore_L-H]
      --, CONCAT(COALESCE(CONVERT(varchar(30), pL.RequestedMemoryKB), ''?''), '' - '', COALESCE(CONVERT(varchar(30), pH.RequestedMemoryKB), ''?'')) AS [RequestedMemoryKB_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(30), pL.GrantedMemoryKB),   ''?''), '' - '', COALESCE(CONVERT(varchar(30), pH.GrantedMemoryKB),   ''?'')) AS [GrantedMemoryKB_L-H]
      --, CONCAT(COALESCE(CONVERT(varchar(30), pL.UsedMemoryKB),      ''?''), '' - '', COALESCE(CONVERT(varchar(30), pH.UsedMemoryKB),      ''?'')) AS [UsedMemoryKB_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(10), pL.HasMemoryGrantWarning), ''?''), '' - '', COALESCE(CONVERT(varchar(10), pH.HasMemoryGrantWarning), ''?'')) AS [MemoryGrantWarning_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(10), pL.HasImplicitConversion), ''?''), '' - '', COALESCE(CONVERT(varchar(10), pH.HasImplicitConversion), ''?'')) AS [ImplicitConversion_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(10), pL.HasNoJoinPredicateWarn), ''?''), '' - '', COALESCE(CONVERT(varchar(10), pH.HasNoJoinPredicateWarn), ''?'')) AS [NoJoinPredicateWarn_L-H]
        , CONCAT(COALESCE(CONVERT(varchar(10), pL.HasMissingStatsWarn), ''?''), '' - '', COALESCE(CONVERT(varchar(10), pH.HasMissingStatsWarn), ''?'')) AS [MissingStatsWarn_L-H]
        , (
            ISNULL(CASE WHEN (pL.PlanXmlHash IS NULL AND pH.PlanXmlHash IS NOT NULL)
                           OR (pL.PlanXmlHash IS NOT NULL AND pH.PlanXmlHash IS NULL)
                           OR (pL.PlanXmlHash IS NOT NULL AND pH.PlanXmlHash IS NOT NULL AND pL.PlanXmlHash <> pH.PlanXmlHash)
                          THEN ''PLAN_SHAPE_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.IndexSeekCount,-1) <> ISNULL(pH.IndexSeekCount,-1) THEN ''INDEX_SEEK_COUNT_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.IndexScanCount,-1) <> ISNULL(pH.IndexScanCount,-1) THEN ''INDEX_SCAN_COUNT_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.TableScanCount,-1) <> ISNULL(pH.TableScanCount,-1) THEN ''TABLE_SCAN_COUNT_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.HasHashJoin,0)    <> ISNULL(pH.HasHashJoin,0)    THEN ''JOIN_HASH_CHANGED;''  ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.HasMergeJoin,0)   <> ISNULL(pH.HasMergeJoin,0)   THEN ''JOIN_MERGE_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.HasNestedLoops,0) <> ISNULL(pH.HasNestedLoops,0) THEN ''JOIN_NL_CHANGED;''    ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.HasParallelism,0) <> ISNULL(pH.HasParallelism,0) THEN ''PARALLELISM_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.HasSpillToTempDb,0) <> ISNULL(pH.HasSpillToTempDb,0) THEN ''SPILL_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.HasMissingIndex,0)  <> ISNULL(pH.HasMissingIndex,0)  THEN ''MISSING_INDEX_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.KeyLookupCount,-1) <> ISNULL(pH.KeyLookupCount,-1) THEN ''KEY_LOOKUP_COUNT_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.RIDLookupCount,-1) <> ISNULL(pH.RIDLookupCount,-1) THEN ''RID_LOOKUP_COUNT_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.SortCount,-1) <> ISNULL(pH.SortCount,-1) THEN ''SORT_COUNT_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.HashAggCount,-1) <> ISNULL(pH.HashAggCount,-1) THEN ''HASH_AGG_COUNT_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.StreamAggCount,-1) <> ISNULL(pH.StreamAggCount,-1) THEN ''STREAM_AGG_COUNT_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.SpoolCount,-1) <> ISNULL(pH.SpoolCount,-1) THEN ''SPOOL_COUNT_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.FilterCount,-1) <> ISNULL(pH.FilterCount,-1) THEN ''FILTER_COUNT_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.ComputeScalarCount,-1) <> ISNULL(pH.ComputeScalarCount,-1) THEN ''COMPUTE_SCALAR_COUNT_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.HasAdaptiveJoin,0) <> ISNULL(pH.HasAdaptiveJoin,0) THEN ''ADAPTIVE_JOIN_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.HasBatchMode,0) <> ISNULL(pH.HasBatchMode,0) THEN ''BATCH_MODE_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.HasColumnstore,0) <> ISNULL(pH.HasColumnstore,0) THEN ''COLUMNSTORE_USAGE_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.RequestedMemoryKB,-1) <> ISNULL(pH.RequestedMemoryKB,-1) THEN ''REQUESTED_MEMORY_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.GrantedMemoryKB,-1)   <> ISNULL(pH.GrantedMemoryKB,-1)   THEN ''GRANTED_MEMORY_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.UsedMemoryKB,-1)      <> ISNULL(pH.UsedMemoryKB,-1)      THEN ''USED_MEMORY_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.HasMemoryGrantWarning,0) <> ISNULL(pH.HasMemoryGrantWarning,0) THEN ''MEMORY_GRANT_WARNING_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.HasImplicitConversion,0) <> ISNULL(pH.HasImplicitConversion,0) THEN ''IMPLICIT_CONVERSION_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.HasNoJoinPredicateWarn,0) <> ISNULL(pH.HasNoJoinPredicateWarn,0) THEN ''NO_JOIN_PREDICATE_WARN_CHANGED;'' ELSE '''' END, '''')
            + ISNULL(CASE WHEN ISNULL(pL.HasMissingStatsWarn,0) <> ISNULL(pH.HasMissingStatsWarn,0) THEN ''MISSING_STATS_WARN_CHANGED;'' ELSE '''' END, '''')
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
END
ELSE
BEGIN
    PRINT 'Resultset #3/#4 skipped: No MULTI_PLAN groups found for the current filters.';
END;

-------------------------------------------------------------------------------------------------------------------
-- Persist results
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
        , CAST(c.QueryTextSample AS nvarchar(4000)) AS QueryTextSample
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
          SYSUTCDATETIME()
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
    FROM #Compare c
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

-------------------------------------------------------------------------------------------------------------------
-- Perf output to Messages (PRINT)
-------------------------------------------------------------------------------------------------------------------
PRINT '';
PRINT '+-----------------------------------------------------------------------------------------------------------------------------+';
PRINT '|                                                     PERF (step timings)                                                     |';
PRINT '+----+------------------------------------------+----------+--------------+-------------------------+-------------------------+';
PRINT '|No  |StepName                                  |Ms        |RowsAffected  |StartAt                  |EndAt                    |';
PRINT '+----+------------------------------------------+----------+--------------+-------------------------+-------------------------+';

DECLARE
      @pStepNo int
    , @pStepName nvarchar(200)
    , @pMs int
    , @pRows bigint
    , @pStart datetime2(3)
    , @pEnd datetime2(3)
    , @line nvarchar(4000);

DECLARE perf_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT StepNo, StepName, Ms, RowsAffected, StartAt, EndAt
    FROM #Perf
    ORDER BY StepNo;

OPEN perf_cursor;
FETCH NEXT FROM perf_cursor INTO @pStepNo, @pStepName, @pMs, @pRows, @pStart, @pEnd;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @line =
          '|'
        + RIGHT(REPLICATE(' ',4) + CONVERT(varchar(4), @pStepNo), 4) + '|'
        + LEFT(CONVERT(varchar(42), ISNULL(@pStepName,N'')) + REPLICATE(' ',42), 42) + '|'
        + RIGHT(REPLICATE(' ',10) + CONVERT(varchar(10), ISNULL(@pMs,0)), 10) + '|'
        + RIGHT(REPLICATE(' ',14) + COALESCE(CONVERT(varchar(14), @pRows), 'NULL'), 14) + '|'
        + LEFT(CONVERT(varchar(23), @pStart, 121) + REPLICATE(' ',25), 25) + '|'
        + LEFT(CONVERT(varchar(23), @pEnd,   121) + REPLICATE(' ',25), 25) + '|';

    PRINT @line;

    FETCH NEXT FROM perf_cursor INTO @pStepNo, @pStepName, @pMs, @pRows, @pStart, @pEnd;
END;

CLOSE perf_cursor;
DEALLOCATE perf_cursor;

PRINT '+----+------------------------------------------+----------+--------------+-------------------------+-------------------------+';
