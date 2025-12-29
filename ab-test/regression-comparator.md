## Query Store CL Regression Comparator script
The Query Store CL Regression Comparator script performs the following actions:

- Aggregates Query Store runtime statistics separately for each database
- Normalizes queries using a configurable grouping strategy
- Compares execution metrics between lower CL and higher CL
- Calculates regression indicators such as ratios and impact scores
- Flags confidence and risk conditions
- Provides plan-level drill-down for multi-plan scenarios
- Optionally persists raw comparison results for historical tracking

The script is intentionally designed to be:

- Compatibility-level aware
- Replay-friendly
- Deterministic and repeatable
- Focused on impact, not just ratios


## Script Parameters and Execution Model

The script is designed to be fully deterministic and operator-driven. In other words, the output is entirely shaped by the parameter block at the top of the script. Those parameters control **which two Query Store snapshots are compared**, **how queries are correlated**, **what metric is evaluated**, **what qualifies as a regression**, and **whether results are persisted for historical tracking**.

At a high level, the script runs in four phases:

1. **Extract** Query Store runtime stats independently from each database (`@DbA` and `@DbB`)
2. **Normalize / Group** queries into logical “query groups” using `@GroupBy`
3. **Compute** weighted totals, averages, ratios, and impact using the selected `@Metric`
4. **Filter + Rank + Flag** the output using thresholds and inclusion rules (`@MinExecCount`, `@MinRegressionRatio`, `@OnlyMultiPlan`, `@IncludeAdhoc`, `@IncludeSP`, and optional time filtering)

The same Query Store data combined with the same parameter values will always produce the same result sets, which makes the script suitable for repeatable A/B testing runs and regression tracking.

### Database Inputs

- `@DbA` (LowerCL / Baseline database)  
  The database containing Query Store data from the **lower compatibility level** replay.  
  Example: `DemoDB_CL120`

- `@DbB` (HigherCL / Candidate database)  
  The database containing Query Store data from the **higher compatibility level** replay.  
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
  - If set (e.g., `1.25`), only groups that are at least **25% worse** in HigherCL will be included in the primary regression result set.

> [!TIP]
> - Start with something like `1.25` for initial triage.
> - Move to `1.10` if you need to catch smaller regressions with high impact.
> - Move to `1.50+` when you only care about “obviously bad” regressions.

- `@TopN`  
  Limits the output to the top N rows after sorting by impact or ranking logic.  
  This is purely a usability control for large workloads.

  **Behavior**:
  - If `NULL`, no limit is applied.
  - If set (e.g., `100`), returns the top 100 regressions based on the script’s ranking (typically ImpactScore descending).

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

### Metric Selection

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
  Defines how the script correlates “the same logical query” across LowerCL and HigherCL.

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
  - Build a history of “known offenders” per database / CL pair
  - Integrate results into a broader pipeline (reporting, dashboards, automation)

- `@ResultsTable`  
  The fully qualified target table name used when `@PersistResults = 1`.  
  Example: `dbo.QueryStoreCLRegressionResults`

> [!TIP]
> - Keep this table in a dedicated utility database if multiple teams use it.
> - Consider adding a run identifier (timestamp, run label, capture window) if the script doesn’t already store one, so multiple runs can coexist cleanly.


## How Queries Are Grouped

One of the core challenges in compatibility level A/B testing is reliably correlating the *same logical query* across two different environments. Query IDs and Plan IDs are not stable across restores, replays, or compatibility levels, so direct ID comparison is not sufficient.

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
- Focus analysis on *logical behavior*, not physical identifiers

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


## Result Sets Overview
The script produces multiple result sets, each serving a specific analytical purpose.


## Result Set #1 – Regression Overview (Primary Analysis View)
This is the main entry point for analysis.

It lists only queries where:

- The metric is worse in higher CL than lower CL
- The regression passes optional execution count and ratio thresholds

Key Columns and How to Read Them:

| Column                   | Meaning                                                                                                                                   |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `GroupKeyHashHex`        | Unique identifier for the logical query group                                                                                             |
| `DominantQueryId_L-H`    | **LowerCL–HigherCL** dominant `query_id` range for the group (the `query_id` that contributed the most to executions/impact on each side) |
| `QueryIdRange_L-H`       | `query_id` ranges observed in **LowerCL–HigherCL** for the group                                                                          |
| `DominantPlanId_L-H`     | **LowerCL–HigherCL** dominant `plan_id` range for the group (the `plan_id` that was most prevalent / most executed on each side)          |
| `PlanCount_L-H`          | Number of **distinct** cached/executed plans per side (**LowerCL–HigherCL**)                                                              |
| `ExecCount_L-H`          | Total executions per side (**LowerCL–HigherCL**)                                                                                          |
| `Avg<Metric>_L-H`        | Average metric value per execution (**LowerCL–HigherCL**)                                                                                 |
| `DeltaAvgMetric`         | `Avg<Metric>_H − Avg<Metric>_L` (absolute change in average metric from LowerCL to HigherCL)                                              |
| `Total<Metric>_L-H`      | Total metric consumption (**LowerCL–HigherCL**)                                                                                           |
| `RegressionRatio`        | `Avg<Metric>_H / NULLIF(Avg<Metric>_L, 0)` (how much worse/better HigherCL is vs LowerCL)                                                 |
| `ImpactScore`            | `(Avg<Metric>_H − Avg<Metric>_L) × ExecCount_H` (estimated total delta impact in HigherCL execution volume)                               |
| `ConfidenceFlags`        | Signals that affect trustworthiness (e.g., low executions, high plan count instability, missing side, text/hash mismatch, etc.)           |
| `QueryTextSample`        | Representative query text for the group (sample to quickly recognize the workload)                                                        |

> [!TIP]
> - Sort by ImpactScore descending - this surfaces what actually hurts the system
> - A high RegressionRatio with a low ImpactScore is often negligible
> - A moderate ratio with a high ImpactScore deserves immediate attention


## Regression Detection Philosophy

The script intentionally does not treat every metric increase as a problem. Instead, it is designed to answer a more important question:

**“Does this change materially hurt the system?”**

For that reason:

- **RegressionRatio** indicates *direction and magnitude*
- **ImpactScore** indicates *real-world cost*

A query with a high ratio but low execution count is often irrelevant. Conversely, a moderate ratio applied to a high-frequency query can represent a severe regression.

This is why the default analysis workflow prioritizes **ImpactScore over ratios**, and why filtering thresholds are designed to reduce noise rather than hide risk.


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
- Is a bad plan dominating in higher CL?


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


## ConfidenceFlags Explained
The ConfidenceFlags column helps interpret reliability:

| Flag                  | Meaning |
|-----------------------|---------|
| MISSING_ONE_SIDE      | The query (or grouped query signature) was observed only on one side (LowerCL or HigherCL). This usually indicates workload drift, filtering effects, or capture window mismatch rather than a true regression. |
| LOW_EXEC              | The execution count on one or both sides is below the configured @MinExecCount threshold. Results with this flag should be treated with lower confidence due to insufficient sample size. |
| MULTI_PLAN            | Multiple execution plans were observed for the same query/group. This may indicate parameter sensitivity, plan instability, or optimizer behavior changes across compatibility levels. |
| INTERVAL_END_FALLBACK | The Query Store runtime statistics interval does not expose a reliable end_time column (engine-version dependent). The script fell back to using the maximum observed start_time, which may slightly reduce temporal precision. |
| WEIGHTED_TOTAL        | Metrics were calculated using execution-count–weighted aggregation (SUM(avg_metric × execution_count)), ensuring that frequently executed plans contribute proportionally more to totals and averages. This improves accuracy compared to simple averages, especially for uneven execution distributions. |

> [!NOTE]
> - WEIGHTED_TOTAL is not a warning. It is an informational confidence indicator stating that the metric math is based on statistically correct, weighted aggregation.
> - When WEIGHTED_TOTAL appears without INTERVAL_END_FALLBACK, the time window and aggregation are both reliable.
> - When combined with LOW_EXEC or MULTI_PLAN, interpretation should consider plan variability or sample size.

Flags do not automatically invalidate results, but they require engineering judgment.


## Recommended Analysis Workflow
1. Start with Result Set #1
2. Sort by ImpactScore
3.	Identify top-impact regressions
4.	Check ConfidenceFlags
5.	If MULTI_PLAN exists:
    - Drill into Result Set #3
    - Identify dominant plans
6.	Use Result Set #4 to understand why the regression happened
7.	Decide on mitigation:
    - Query rewrite
    - Index changes
    - Plan forcing
    - Compatibility-level scoped fixes


## Typical Troubleshooting Questions
#### Q: RegressionRatio is high but ImpactScore is low. Should I worry?
No. This usually indicates low execution frequency. ImpactScore should drive prioritization.

#### Q: Why do I see MULTI_PLAN but no real regression?
Multiple plans alone are not a problem. Focus on whether the dominant plan changed or became more expensive.

#### Q: Which metric should I trust most?
- LogicalReads for plan efficiency
- CPU for compute-bound systems
- Duration for user-facing latency

Always align the metric with the workload’s bottleneck.

#### Q: Why does a query appear only on one side?
This can happen due to plan elimination, parameter sensitivity, or replay timing. Such cases require manual validation.

#### Q: Does a plan shape change always mean regression?
No. Some plan changes are improvements. The metric and ImpactScore determine whether the change is harmful.


## Outcome of This Step

At the end of this step, you should have:

- A ranked list of real, measurable regressions
- Clear understanding of root causes
- Confidence in whether the compatibility level change is safe
- Actionable inputs for tuning or mitigation

This concludes the analytical phase of the A/B testing process.
