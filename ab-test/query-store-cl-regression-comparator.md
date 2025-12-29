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

## How Queries Are Grouped
Queries can be grouped using the @GroupBy parameter:

| GroupBy option    | Description                          | When to use                               |
| ---------------   | ------------------------------------ | ----------------------------------------- |
| `QueryHash`       | Groups by compiled query shape       | Default and recommended for most analyses |
| `QueryText`       | Groups by exact query text           | Useful for static workloads               |
| `NormalizedText`  | Groups by whitespace-normalized text | Useful for ad-hoc heavy systems           |

Each group is internally represented by a GroupKeyHash, which uniquely identifies the logical query across both environments.


## Metric Selection
The comparison metric is controlled by the @Metric parameter:

- **LogicalReads**: Best for plan efficiency and I/O analysis
- **CPU**: Best for CPU-bound workloads
- **Duration**: Best for end-to-end latency analysis

All comparisons, ratios, and impact calculations are derived consistently from this single metric, ensuring analytical integrity.

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
