# Query Store CL Regression Comparator

* **[Quick Start](#quick-start)**
* **[Script Parameters and Execution Model](#script-parameters-and-execution-model)**
* **[How Queries Are Grouped](#how-queries-are-grouped)**
* **[Metric Selection](#metric-selection)**
* **[Metric Aggregation and Weighting Model](#metric-aggregation-and-weighting-model)**
* **[Regression Detection Methodology](#regression-detection-methodology)**
* **[Result Sets Overview](#result-sets-overview)**
* **[Result Set #1 - Regression Overview (Primary Analysis View)](#result-set-1--regression-overview-primary-analysis-view)**
* **[Result Set #2 - Summary Statistics](#result-set-2---summary-statistics)**
* **[Result Set #3 - Multi-Plan Drill-Down (Plan-Level Metrics)](#result-set-3--multi-plan-drill-down-plan-level-metrics)**
* **[Result Set #4 - Dominant Plan Shape Comparison](#result-set-4---dominant-plan-shape-comparison)**
* **[Understanding ConfidenceFlags](#understanding-confidenceflags)**
* **[Recommended Analysis Workflow](#recommended-analysis-workflow)**
* **[Typical Troubleshooting Questions](#typical-troubleshooting-questions)**
* **[Analytical Deliverables and Decision Readiness](#analytical-deliverables-and-decision-readiness)**

## Quick Start

> [!NOTE]
> You can access and download the full script from here: [Query Store CL Regression Comparator](./regression-comparator.sql)

This quick start is designed to get you from "two Query Store datasets" to a ranked regression list in a few minutes.

> Throughout this document:
> - **LowerCL** refers to the database running at the lower SQL Server compatibility level (baseline).
> - **HigherCL** refers to the database running at the higher SQL Server compatibility level (candidate).

### What you need

Before running the script, ensure you have:

- Two databases containing Query Store data for the same captured workload, replayed under:
  - **LowerCL** baseline (e.g., CL 120)
  - **HigherCL** candidate (e.g., CL 170)
- Query Store is enabled and has collected runtime stats on both sides
- (Optional but recommended) A replay-aligned time window you can filter to (`@StartTime`, `@EndTime`)

### Run it (minimal parameter block)

Start with the baseline triage setup below. It keeps noise under control while surfacing real risk.

```sql
DECLARE
      @DbA sysname                     = N'DemoDB_CL120'           -- LowerCL (baseline database)
    , @DbB sysname                     = N'DemoDB_CL170'           -- HigherCL (candidate database)
    , @MinExecCount bigint             = 50                        -- Minimum execution count to reduce low-signal noise
    , @MinRegressionRatio decimal(9,4) = 1.25                      -- Only include regressions >= 25% (AvgMetric_H / AvgMetric_L)
    , @TopN int                        = 100                       -- Limit output to top N regressions by impact
    , @StartTime datetime2(0)          = NULL                      -- Optional start of Query Store analysis window
    , @EndTime   datetime2(0)          = NULL                      -- Optional end of Query Store analysis window
    , @Metric sysname                  = N'LogicalReads'           -- Comparison metric: LogicalReads | CPU | Duration
    , @GroupBy sysname                 = N'QueryHash'              -- Query grouping strategy: QueryHash | QueryText | NormalizedText
    , @StatementType varchar(10)       = N'ALL'                    -- Statement filter: ALL | SELECT | INSERT | UPDATE | DELETE
    , @IncludeAdhoc bit                = 1                         -- Include ad-hoc queries (1 = include, 0 = exclude)
    , @IncludeSP bit                   = 1                         -- Include stored procedure statements (1 = include, 0 = exclude)
    , @OnlyMultiPlan bit               = 0                         -- 1 = return only query groups with multiple plans
    , @PersistResults bit              = 1                         -- Persist comparison results (1 = enable, 0 = disable)
    , @ResultsTable sysname            = N'dbo.RegressionResults'; -- Target table used when @PersistResults = 1
```
Execute the full script after setting parameters. The script reads these parameters and produces the result sets described below.


## Script Parameters and Execution Model

The script is designed to be fully deterministic and operator-driven. In other words, the output is entirely shaped by the parameter block at the top of the script. Those parameters control which two Query Store snapshots are compared, how queries are correlated, what metric is evaluated, what qualifies as a regression, and whether results are persisted for historical tracking.

At a high level, the script runs in four phases:

1. **Extract** Query Store runtime stats independently from each database (`@DbA` and `@DbB`)
2. **Normalize / Group** queries into logical “query groups” using `@GroupBy`
3. **Compute** weighted totals, averages, ratios, and impact using the selected `@Metric`
4. **Filter + Rank + Flag** the output using thresholds and inclusion rules (`@MinExecCount`, `@MinRegressionRatio`, `@OnlyMultiPlan`, `@IncludeAdhoc`, `@IncludeSP`, and optional time filtering)

The same Query Store data combined with the same parameter values will always produce the same result sets, which makes the script suitable for repeatable A/B testing runs and regression tracking.

### Database Inputs

- `@DbA` (Baseline database)  
  The database containing Query Store data from the **LowerCL** replay.  
  Example: `DemoDB_CL120`

- `@DbB` (Candidate database)  
  The database containing Query Store data from the **HigherCL** replay.  
  Example: `DemoDB_CL170`

These two databases are treated as independent evidence sources. The script never assumes that `query_id` or `plan_id` values match between them. Correlation happens through the grouping strategy (`@GroupBy`).

### Scope and Noise Control

- `@MinExecCount`  
  Filters out low-signal query groups where execution volume is too small to be trustworthy.  
  This is one of the most important parameters for preventing false positives.

  **Behavior**:
  - If `NULL`, no minimum is enforced (more complete, but noisier).
  - If set (e.g., `50`), query groups where execution count is below the threshold on one or both sides are flagged (and depending on script logic may be excluded from the primary regression list).

> [!TIP]
> - Use a higher value when your capture window is short and workload is spiky.
> - Use a lower value when your capture window is long and you want more coverage.

- `@MinRegressionRatio`  
  Controls the minimum regression ratio that qualifies as a “regression candidate.”  
  The script computes:

  `RegressionRatio = AvgMetric_H / NULLIF(AvgMetric_L, 0)`

  **Behavior**:
  - If `NULL`, the script does not enforce a minimum ratio and will output everything that worsened (useful for broad discovery).
  - If set (e.g., `1.25`), only groups that are at least **25% worse** in higher compatibility level will be included in the primary regression result set.

> [!TIP]
> - Start with something like `1.25` for initial triage.
> - Move to `1.10` if you need to catch smaller regressions with high impact.
> - Move to `1.50+` when you only care about "obviously bad" regressions.

- `@TopN`  
  Limits the output to the top N rows after sorting by impact or ranking logic.  
  This is purely a usability control for large workloads.

  **Behavior**:
  - If `NULL`, no limit is applied.
  - If set (e.g., `100`), returns the top 100 regressions based on the script's ranking (typically ImpactScore descending).

> [!TIP]
> Use `@TopN` for iterative tuning cycles where you only want to focus on the worst offenders first.

### Time Window Filtering

- `@StartTime` and `@EndTime`  
  Restrict analysis to a specific Query Store runtime stats interval window. This is useful when:
  - You replayed multiple workloads into the same Query Store
  - You want to compare only a specific replay attempt
  - You need to exclude warm-up or ramp-down phases

  **Behavior**:
  - If both are `NULL`, the script analyzes the full available Query Store history in each database.
  - If provided, the script filters Query Store runtime stats intervals to those that overlap the specified window.
> [!IMPORTANT]
> Some SQL Server versions expose limited interval end-time metadata in Query Store. In those cases, the script may fall back to using the maximum observed `start_time` for interval bounding, and it will flag this via `INTERVAL_END_FALLBACK`. This does not invalidate results, but it can slightly reduce temporal precision when slicing narrow windows.

> [!TIP]
> - Use the widest reasonable window that cleanly maps to your replay period.
> - Avoid overly tight windows unless you are certain about Query Store interval boundaries.

### Metric Configuration

- `@Metric`  
  Chooses the single metric used for all comparisons, ratios, and impact calculations.

  Supported values:
  - `LogicalReads` (recommended default)  
    Best for diagnosing plan efficiency and I/O regressions (index access changes, scan vs seek shifts, CE changes impacting join order, etc.).
  - `CPU`  
    Best for compute-bound workloads where logical reads are stable but CPU time changes due to different plan shapes or operators.
  - `Duration`  
    Best for latency / end-to-end response time comparisons. Most sensitive to concurrency and blocking effects, so interpret carefully.

  **Design principle**:
  The script derives all totals and averages from the selected metric using execution-weighted aggregation (not simple averages), ensuring that high-frequency executions dominate the math appropriately.

### Query Grouping Strategy

- `@GroupBy`  
  Defines how the script correlates "the same logical query" across LowerCL and HigherCL.

  Supported values:
  - `QueryHash`  
    Default and recommended. Most stable across environments. Groups by compiled query shape rather than literal text.
  - `QueryText`  
    Exact text match. Useful when you want strict identity matching (usually for controlled workloads).
  - `NormalizedText`  
    Groups by whitespace-normalized text. Useful for ad-hoc heavy systems where the same query appears with minor formatting differences.

  **Trade-off**:
  - Too strict (`QueryText`) can fragment results (same logical query becomes multiple groups).
  - Too loose can mix unrelated queries if normalization collapses differences.

### Statement-Type Filtering

- `@StatementType`  
  Filters results by the statement class:

  Supported values:
  - `ALL` (default)
  - `SELECT`
  - `INSERT`
  - `UPDATE`
  - `DELETE`

  **When it matters**:
  - Use `SELECT` when you only care about read-path regressions and want to reduce noise from write-heavy replay behavior.
  - Use `INSERT/UPDATE/DELETE` to isolate write regressions or investigate plan regressions driven by DML patterns.

> [!NOTE]
> Classification is typically derived from Query Store query text inspection logic. For mixed batches or complex statements, categorization may not be perfect; treat this as a pragmatic filter, not a formal parser.

### Workload-Type Inclusion (Ad-hoc vs Stored Procedures)

- `@IncludeAdhoc`  
  Controls whether ad-hoc queries are included.

  Values:
  - `1` include ad-hoc queries
  - `0` exclude ad-hoc queries

- `@IncludeSP`  
  Controls whether stored procedure queries are included.

  Values:
  - `1` include stored procedure queries
  - `0` exclude stored procedure queries

  **How to use these together**:
  - Both `1`: full workload comparison (most common)
  - Only ad-hoc: `@IncludeAdhoc = 1`, `@IncludeSP = 0`
  - Only SP: `@IncludeAdhoc = 0`, `@IncludeSP = 1`

> [!TIP]
> If your tuning process is split (e.g., first stabilize SPs, then review ad-hoc), these toggles let you isolate each category without changing the rest of the script.

### Multi-Plan Focus Mode

- `@OnlyMultiPlan`  
  Restricts output to query groups that have multiple plans on either side.

  Values:
  - `1` only show multi-plan groups
  - `0` show all qualifying groups

  **When to use**:
  - When you specifically want to investigate plan instability, parameter sensitivity, or plan-shape divergence across CLs.
  - When the regression list is huge and you want to first isolate regressions likely caused by plan behavior changes.

### Persistence and Historical Tracking

- `@PersistResults`  
  Controls whether the script persists its raw and/or summarized results into a permanent table.

  Values:
  - `1` persist results
  - `0` do not persist (ad-hoc analysis only)

  **Why persist**:
  Persisting allows you to:
  - Compare multiple replay runs over time
  - Track whether regressions improved after tuning
  - Build a history of "known offenders" per database / CL pair
  - Integrate results into a broader pipeline (reporting, dashboards, automation)

- `@ResultsTable`  
  The fully qualified target table name used when `@PersistResults = 1`.  
  Example: `dbo.QueryStoreCLRegressionResults`

> [!TIP]
> - Keep this table in a dedicated utility database if multiple teams use it.
> - Consider adding a run identifier (timestamp, run label, capture window) if the script doesn’t already store one, so multiple runs can coexist cleanly.


## How Queries Are Grouped

One of the core challenges in compatibility level A/B testing is reliably correlating the same logical query across two different environments. Query IDs and Plan IDs are not stable across restores, replays, or compatibility levels, so direct ID comparison is not sufficient.

To solve this, the script uses a configurable **logical grouping strategy**, controlled by the `@GroupBy` parameter.

| GroupBy option     | Description                          | When to use                               |
|--------------------|--------------------------------------|-------------------------------------------|
| `QueryHash`        | Groups by compiled query shape       | Default and recommended for most analyses |
| `QueryText`        | Groups by exact query text           | Useful for static, well-controlled code   |
| `NormalizedText`   | Groups by whitespace-normalized text | Useful for ad-hoc–heavy systems           |

Each logical group is internally represented by a **GroupKeyHash**, which acts as a stable identifier for the query across both databases.

This approach allows the script to:

- Correlate queries even when `query_id` values differ
- Tolerate minor plan or compilation differences
- Focus analysis on logical behavior, not physical identifiers

Choosing the correct grouping strategy is critical. Overly strict grouping can fragment results, while overly loose grouping can mix unrelated queries. For most workloads, `QueryHash` provides the best balance.



## Metric Selection
The comparison metric is controlled by the @Metric parameter:

- **LogicalReads**: Best for plan efficiency and I/O analysis
- **CPU**: Best for CPU-bound workloads
- **Duration**: Best for end-to-end latency analysis

All comparisons, ratios, and impact calculations are derived consistently from this single metric, ensuring analytical integrity.


## Metric Aggregation and Weighting Model

Query Store exposes runtime statistics as averages per plan and interval. To produce accurate, workload-representative comparisons, the script does not rely on simple averages.

Instead, all metrics are aggregated using **execution-count–weighted math**:

- **TotalMetric**  
  `SUM(avg_metric × execution_count)`

- **AvgMetric**  
  `TotalMetric / TotalExecutionCount`

This ensures that:

- Frequently executed plans contribute proportionally more to results
- Rare executions do not skew averages
- Comparisons reflect real system impact, not statistical artifacts

This weighting model is applied consistently across:

- LowerCL and HigherCL
- Group-level aggregation
- Plan-level drill-down
- ImpactScore calculation

Without weighted aggregation, A/B analysis can easily misrepresent reality, especially in workloads with uneven execution distributions.

## Regression Detection Methodology

The script intentionally does not treat every metric increase as a problem. Instead, it is designed to answer a more important question:

**Does this change materially hurt the system?**

For that reason:

- **RegressionRatio** indicates direction and magnitude
- **ImpactScore** indicates real-world cost

A query with a high ratio but low execution count is often irrelevant. Conversely, a moderate ratio applied to a high-frequency query can represent a severe regression.

This is why the default analysis workflow prioritizes ImpactScore over ratios, and why filtering thresholds are designed to reduce noise rather than hide risk.


## Result Sets Overview

The script produces multiple result sets, each designed to answer a different analytical question during a compatibility level A/B comparison.  
Rather than forcing all information into a single, overloaded output, the results are intentionally separated to support a structured analysis workflow.

Each result set builds on the previous one and is intended to be consumed in sequence:

- **Result Set #1 – Regression Overview**  
  Identifies where regressions exist by comparing LowerCL and HigherCL at the query-group level.  
  This result set quantifies regression severity using metrics such as AvgMetric delta, RegressionRatio, and ImpactScore, and serves as the primary entry point for analysis.

- **Result Set #2 – Summary Statistics**  
  Provides an aggregated, high-level view of the overall regression landscape, including counts, total impact, and distribution characteristics.  
  This result set is useful for understanding systemic risk and supporting go/no-go decisions.

- **Result Set #3 – Multi-Plan Drill-Down**  
  Explains why a regression may have occurred by exposing plan-level behavior for query groups with multiple execution plans.  
  It helps identify plan dominance shifts, parameter sensitivity, and plan instability across compatibility levels.

- **Result Set #4 – Dominant Plan Shape Comparison**  
  Provides low-level diagnostic detail by comparing the dominant execution plan from LowerCL and HigherCL.  
  This result set highlights operator-level differences, join strategy changes, memory grant behavior, and other plan-shape variations that often explain observed regressions.


This layered output model allows engineers to move from high-level risk identification to low-level plan analysis without losing context, while keeping each result set focused, readable, and purpose-driven.


## Result Set #1 – Regression Overview (Primary Analysis View)
This is the main entry point for analysis.

It lists only queries where:

- The metric is worse in HigherCL than LowerCL
- The regression passes optional execution count and ratio thresholds

Key Columns and How to Read Them:

| Column                   | Meaning                                                                                                                                   |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `GroupKeyHashHex`        | Unique identifier for the logical query group                                                                                             |
| `DominantQueryId_L-H`    | LowerCL-HigherCL dominant `query_id` range for the group (the `query_id` that contributed the most to executions/impact on each side) |
| `QueryIdRange_L-H`       | `query_id` ranges observed in LowerCL-HigherCL for the group                                                                          |
| `DominantPlanId_L-H`     | LowerCL-HigherCL dominant `plan_id` range for the group (the `plan_id` that was most prevalent / most executed on each side)          |
| `PlanCount_L-H`          | Number of distinct cached/executed plans per side (LowerCL-HigherCL)                                                              |
| `ExecCount_L-H`          | Total executions per side (LowerCL-HigherCL)                                                                                          |
| `Avg<Metric>_L-H`        | Average metric value per execution (LowerCL-HigherCL)                                                                                 |
| `DeltaAvgMetric`         | `Avg<Metric>_H − Avg<Metric>_L` (absolute change in average metric from LowerCL to HigherCL)                                              |
| `Total<Metric>_L-H`      | Total metric consumption (LowerCL-HigherCL)                                                                                           |
| `RegressionRatio`        | `Avg<Metric>_H / NULLIF(Avg<Metric>_L, 0)` (how much worse/better HigherCL is vs LowerCL)                                                 |
| `ImpactScore`            | `(Avg<Metric>_H − Avg<Metric>_L) × ExecCount_H` (estimated total delta impact in HigherCL execution volume)                               |
| `ConfidenceFlags`        | Signals that affect trustworthiness (e.g., low executions, high plan count instability, missing side, text/hash mismatch, etc.)           |
| `QueryTextSample`        | Representative query text for the group (sample to quickly recognize the workload)                                                        |

> [!TIP]
> - Sort by ImpactScore descending - this surfaces what actually hurts the system
> - A high RegressionRatio with a low ImpactScore is often negligible
> - A moderate ratio with a high ImpactScore deserves immediate attention


## Result Set #2 - Summary Statistics
This result set provides a high-level assessment of the comparison:

- Number of regressed query groups
- How many involve multiple plans
- Aggregate and peak impact
- Average regression ratio

This view is useful for:

- Go / No-Go decisions
- Management summaries
- Comparing multiple replay iterations


## Result Set #3 – Multi-Plan Drill-Down (Plan-Level Metrics)
This result set appears only if multi-plan scenarios exist.

It breaks down each plan within a query group, per database, including:

- Execution counts
- Average and total metric usage
- Ranking by impact and frequency

This view answers:

- Which plan is dominant?
- Are plans evenly used or skewed?
- Is a bad plan dominating in HigherCL?


## Result Set #4 - Dominant Plan Shape Comparison
This result set compares the dominant execution plan from each side.

It includes:

- Plan XML hashes
- Operator counts (Index Seek, Scan, Table Scan)
- Join type usage (Hash, Merge, Nested Loops)
- Parallelism indicators
- Memory grants and spill detection
- Missing index presence
- Explicit plan shape difference flags

This view is used to determine:

- Whether a regression is plan-shape driven
- Whether CE changes altered join strategy
- Whether memory or spill behavior changed


## Understanding ConfidenceFlags
The ConfidenceFlags column helps interpret reliability:

| Flag                  | Meaning |
|-----------------------|---------|
| MISSING_ONE_SIDE      | The query (or grouped query signature) was observed only on one side (LowerCL or HigherCL). This usually indicates workload drift, filtering effects, or capture window mismatch rather than a true regression. |
| LOW_EXEC              | The execution count on one or both sides is below the configured @MinExecCount threshold. Results with this flag should be treated with lower confidence due to insufficient sample size. |
| MULTI_PLAN            | Multiple execution plans were observed for the same query/group. This may indicate parameter sensitivity, plan instability, or optimizer behavior changes across compatibility levels. |
| INTERVAL_END_FALLBACK | The Query Store runtime statistics interval does not expose a reliable end_time column (engine-version dependent). The script fell back to using the maximum observed start_time, which may slightly reduce temporal precision. |
| WEIGHTED_TOTAL        | Metrics were calculated using execution-count-weighted aggregation (SUM(avg_metric × execution_count)), ensuring that frequently executed plans contribute proportionally more to totals and averages. This improves accuracy compared to simple averages, especially for uneven execution distributions. |

> [!NOTE]
> - WEIGHTED_TOTAL is not a warning. It is an informational confidence indicator stating that the metric math is based on statistically correct, weighted aggregation.
> - When WEIGHTED_TOTAL appears without INTERVAL_END_FALLBACK, the time window and aggregation are both reliable.
> - When combined with LOW_EXEC or MULTI_PLAN, interpretation should consider plan variability or sample size.

Flags do not automatically invalidate results, but they require engineering judgment.


## Recommended Analysis Workflow

This section describes a practical, step-by-step workflow for analyzing Query Store results produced by the Query Store CL Regression Comparator script. The goal is to move from raw regression signals to actionable engineering decisions in a controlled and repeatable way.

The workflow is intentionally ordered. Skipping steps often leads to false positives, misinterpretation, or unnecessary mitigation work.

---

### Step 1 - Start with Result Set #1 (Regression Overview)

Always begin with **Result Set #1**, which provides the high-level regression overview.

Actions:
- Sort the result set by **ImpactScore** in descending order
- Focus on the **top-impact queries first**, not on the highest regression ratios

Why:
- ImpactScore reflects real workload impact, not just relative degradation
- A query that regresses slightly but runs thousands of times is usually more important than a query that regresses heavily but runs rarely

> [!NOTE]
> A high `RegressionRatio` without a correspondingly high `ImpactScore` is often operationally insignificant.

---

### Step 2 - Validate Execution Volume and Confidence Signals

Before deep analysis, validate that the regression signal is trustworthy.

Actions:
- Review `ExecCount_L-H`
- Inspect the `ConfidenceFlags` column carefully

Key checks:
- Queries flagged with **LOW_EXEC** may be statistical noise
- Queries flagged with **MISSING_ONE_SIDE** often indicate workload drift or replay mismatch
- Queries flagged with **INTERVAL_END_FALLBACK** require careful time-window interpretation

Why:
- Query Store aggregates are only meaningful when sample size is sufficient
- Confidence flags exist to prevent premature conclusions

> RULE OF THUMB:  
> Do not invest deep analysis effort until execution volume and confidence flags look reasonable.

---

### Step 3 - Identify Plan Instability (MULTI_PLAN Detection)

Next, determine whether the regression is associated with plan instability.

Actions:
- Check the `PlanCount_L-H` column
- Look for the **MULTI_PLAN** confidence flag

Interpretation:
- `PlanCount_L > 1` or `PlanCount_H > 1` indicates plan churn
- Plan churn across compatibility levels often signals:
  - Parameter sensitivity
  - Cardinality Estimator behavior changes
  - Different join ordering or join algorithm selection

Why:
- Plan instability is one of the most common causes of CL-related regressions
- Treating a plan instability problem as a pure performance problem often leads to incorrect fixes

---

### Step 4 - Drill Down Using Result Set #3 (Multi-Plan Details)

If multi-plan behavior exists, move to **Result Set #3**.

Actions:
- Identify the **dominant plan** on each side
- Compare execution distribution across plans
- Determine whether:
  - A new, more expensive plan became dominant in HigherCL
  - The same plan exists but is used less frequently

Key questions:
- Is one plan responsible for most of the executions?
- Did plan dominance change between LowerCL and HigherCL?
- Is the regression caused by plan selection rather than plan cost?

Why:
- Query Store often contains multiple plans, but only one usually matters
- Dominant plan analysis prevents chasing irrelevant plans

---

### Step 5 - Compare Dominant Plan Shapes (Result Set #4)

Once the dominant plan is identified, analyze **Result Set #4** to understand why the regression occurred.

Actions:
- Compare operator usage (Index Seek vs Scan, Join types)
- Review parallelism indicators
- Inspect memory grant differences and spill indicators
- Check for missing index signals

Interpretation:
- Join type changes often point to CE behavior differences
- Increased memory grants or spills suggest cardinality misestimation
- Loss of index seeks often explains LogicalReads regressions

Why:
- Most CL regressions are rooted in plan shape changes, not engine bugs
- Understanding the plan shape prevents blind mitigation

---

### Step 6 - Correlate with Metric Type

Always interpret findings in the context of the selected metric.

Guidelines:
- **LogicalReads** regressions usually indicate plan efficiency issues
- **CPU** regressions often correlate with operator changes or parallelism
- **Duration** regressions may involve blocking, waits, or concurrency effects

Why:
- The same plan change can have different implications depending on the metric
- Misaligned metric interpretation leads to incorrect conclusions

---

### Step 7 - Eliminate Common False Positives

Before taking action, explicitly rule out common false positives.

Checklist:
- [ ] Execution count sufficient on both sides
- [ ] Same statement type (SELECT vs DML)
- [ ] No significant workload drift
- [ ] No known stats updates between replays
- [ ] Replay environment comparable across rounds

Why:
- Query Store captures **what happened**, not **why it happened**
- Context matters as much as metrics

---

### Step 8 - Decide on Mitigation Strategy

Only after completing the previous steps should mitigation be considered.

Possible actions:
- Plan forcing (temporary or scoped)
- Query-level hints (e.g., `OPTIMIZE FOR`, `USE HINT`)
- Index or statistics adjustments
- Query rewrite
- Compatibility-level scoped optimizer hints

Decision factors:
- Regression severity
- Query criticality
- Plan stability
- Long-term maintainability

> [!IMPORTANT]  
> Plan forcing should be a last resort, not the default response.

---

### Step 9 - Re-Validate After Mitigation

After applying mitigation:
- Re-run the replay
- Re-execute the comparator script
- Confirm that:
  - Regression is resolved
  - No new regressions were introduced

Why:
- Every fix has a blast radius
- Re-validation closes the A/B testing loop

---

### Step 10 - Document and Persist Results

Finally, persist findings for traceability.

Actions:
- Store results using `@PersistResults`
- Capture:
  - Original regression
  - Root cause
  - Mitigation applied
  - Post-fix validation outcome

Why:
- CL upgrades are rarely one-off events
- Historical regression knowledge compounds in value over time

---

Following this workflow ensures that compatibility level upgrades are evaluated **systematically**, **defensively**, and **with engineering discipline**, minimizing both performance risk and unnecessary remediation work.



## Typical Troubleshooting Questions
#### RegressionRatio is high but ImpactScore is low. Should I worry?
No. This usually indicates low execution frequency. ImpactScore should drive prioritization.

#### Why do I see MULTI_PLAN but no real regression?
Multiple plans alone are not a problem. Focus on whether the dominant plan changed or became more expensive.

#### Which metric should I trust most?
- LogicalReads for plan efficiency
- CPU for compute-bound systems
- Duration for user-facing latency

Always align the metric with the workload’s bottleneck.

#### Why does a query appear only on one side?
This can happen due to plan elimination, parameter sensitivity, or replay timing. Such cases require manual validation.

#### Does a plan shape change always mean regression?
No. Some plan changes are improvements. The metric and ImpactScore determine whether the change is harmful.


## Analytical Deliverables and Decision Readiness

At the completion of this step, the A/B testing process transitions from data collection and comparison into engineering-grade decision readiness. The outputs generated by the Query Store CL Regression Comparator are no longer raw observations, but validated analytical artifacts that can be used to make informed, defensible decisions.

By the end of this phase, you should have achieved the following:

### 1. A Prioritized, Impact-Weighted Regression Inventory
- A ranked list of query-level regressions, ordered by real workload impact rather than raw ratios
- Clear separation between:
  - High-impact, high-confidence regressions
  - Low-impact or low-confidence signals
- Quantitative evidence of how each regression contributes to overall resource consumption in HigherCL

This inventory serves as the authoritative reference for all subsequent tuning and mitigation work.

---

### 2. Root Cause Classification for Each Meaningful Regression
For every regression that survives confidence validation, you should be able to clearly attribute it to one or more of the following categories:

- Execution plan shape changes (join strategy, access path, operator selection)
- Plan instability or parameter sensitivity (multi-plan behavior)
- Cardinality estimation differences introduced by the new compatibility level
- Memory grant behavior changes (over-allocation, under-allocation, spills)
- Parallelism behavior shifts
- Environmental or replay-related artifacts (validated and ruled out)

This classification transforms regressions from "symptoms" into understood behaviors.

---

### 3. Explicit Confidence Assessment of the Compatibility Level Change
Based on the aggregated results:

- You should be able to state, with evidence:
  - Whether the compatibility level change is safe as-is
  - Whether it is conditionally safe with targeted mitigations
  - Or whether it introduces unacceptable risk without broader remediation

This assessment is grounded in:
- Execution-weighted metrics
- Confidence flags
- Plan dominance analysis
- Replay consistency validation

It replaces intuition or anecdotal testing with measurable proof.

---

### 4. Actionable, Scoped Mitigation Candidates
For regressions that require intervention, you should now have:

- A short, prioritized list of queries eligible for:
  - Plan forcing (temporary or scoped)
  - Query-level hints
  - Index or statistics adjustments
  - Query rewrites
  - Compatibility-level–scoped optimizer hints
- Clear understanding of why each mitigation is expected to work
- Awareness of potential side effects and blast radius

This ensures that mitigations are surgical, not reactive.

---

### 5. A Closed Analytical Loop Ready for Re-Validation
This step produces outputs that are explicitly designed to be re-tested.

You should be prepared to:
- Re-run replay after mitigation
- Re-execute the comparator script
- Verify that:
  - Target regressions are resolved
  - No new regressions are introduced
  - Overall workload behavior remains stable

This closes the A/B testing feedback loop and prevents regression drift.

---

### 6. Persistable Knowledge for Future CL Upgrades
When results are persisted:

- The organization gains a historical record of:
  - Known regression patterns
  - Effective mitigations
  - Queries sensitive to optimizer changes
- Future compatibility level upgrades become faster, safer, and more predictable

Each completed analysis compounds the value of the next one.

---

At this point, the A/B testing effort has moved beyond experimentation.  
The system is now in a decision-ready state, where compatibility level changes can be approved, mitigated, or deferred based on evidence, not assumptions.

