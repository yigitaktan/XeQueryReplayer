# A/B Tests
<br/>

> [!IMPORTANT]
> This document may have newer versions over time. To always read the most up-to-date and accurate version, make sure to refer to the following link:<br/><br/>
> https://github.com/yigitaktan/XeQueryReplayer/blob/main/ab-test/README.md<br/><br/>
> That repository is the authoritative source and will always reflect the latest updates and improvements.

<br/>

* **[Upgrading Compatibility Level](#upgrading-compatibility-level)**
* **[Getting Started](#getting-started)**
* **[Step 1 / Capturing Data](#step-1--capturing-data)**
* **[Step 2 / Round 1 - Syncing Up](#step-2--round-1---syncing-up)**
* **[Step 3 / Round 1 - Check/Configure Compatibility Level](#step-3--round-1---checkconfigure-compatibility-level)**
* **[Step 4 / Round 1 - Configuring the Query Store](#step-4--round-1---configuring-the-query-store)**
* **[Step 5 / Round 1 - Replaying Data](#step-5--round-1---replaying-data)**
* **[Step 6 / Round 1 - Verify Collected Data](#step-6--round-1---verify-collected-data)**
* **[Step 7 / Round 1 - Extracting Query Store Data](#step-7--round-1---extracting-query-store-data)**
* **[Step 8 / Round 1 - Verify the Clone](#step-8--round-1---verify-the-clone)**
* **[Step 9 / Round 1 - Removing the Restored Database](#step-9--round-1---removing-the-restored-database)**
* **[Step 10 / Round 2 - Data Recreation](#step-10--round-2---data-recreation)**
* **[Step 11 / Round 2 - Check/Configure Compatibility Level](#step-11--round-2---checkconfigure-compatibility-level)**
* **[Step 12 / Round 2 - Configuring the Query Store](#step-12--round-2---configuring-the-query-store)**
* **[Step 13 / Round 2 - Replaying Data](#step-13--round-2---replaying-data)**
* **[Step 14 / Round 2 - Verify Collected Data](#step-14--round-2---verify-collected-data)**
* **[Step 15 / Round 2 - Extracting Query Store Data](#step-15--round-2---extracting-query-store-data)**
* **[Step 16 / Round 2 - Verify the Clone](#step-16--round-2---verify-the-clone)**
* **[Step 17 / Round 2 - Removing the Restored Database](#step-17--round-2---removing-the-restored-database)**
* **[Step 18 / Analysis Time](#step-18--analysis-time)**
* **[Query Store CL Regression Comparator script](#query-store-cl-regression-comparator-script)**
  * **[How Queries Are Grouped](#how-queries-are-grouped)**
  * **[Metric Selection](#metric-selection)**
  * **[Result Sets Overview](#result-sets-overview)**
  * **[Result Set #1 – Regression Overview (Primary Analysis View)](#result-set-1--regression-overview-primary-analysis-view)**
  * **[Result Set #2 - Summary Statistics](#result-set-2---summary-statistics)**
  * **[Result Set #3 – Multi-Plan Drill-Down (Plan-Level Metrics)](#result-set-3--multi-plan-drill-down-plan-level-metrics)**
  * **[Result Set #4 - Dominant Plan Shape Comparison](#result-set-4---dominant-plan-shape-comparison)**
  * **[ConfidenceFlags Explained](#confidenceflags-explained)**
  * **[Recommended Analysis Workflow](#recommended-analysis-workflow)**
  * **[Typical Troubleshooting Questions](#typical-troubleshooting-questions)**
  * **[Outcome of This Step](#outcome-of-this-step)**
<br/>

## Upgrading Compatibility Level

When you change the compatibility level in SQL Server. In other words, when you move a database to a higher compatibility level, you naturally expect SQL Server to maintain the same I/O behavior for your queries, or ideally produce better and more efficient I/O.

This document focuses on the most critical part of that process: A/B testing. Specifically, it explains how to use [XEvent Query Replayer](https://github.com/yigitaktan/XeQueryReplayer/blob/main/README.md) to validate query behavior and performance during compatibility level changes, and how to confidently assess the impact of those changes before going live.

The diagram below illustrates the entire end-to-end workflow, starting from capturing workload data on the production server and continuing through all subsequent stages of the A/B testing and analysis process.


<img width="1196" height="588" alt="2025-12-29_08-43-46" src="https://github.com/user-attachments/assets/fc2d3d88-1cdb-43d8-b1b8-1a2ccb7d6c37" />

The scenario described below uses a database named DemoDB as an example, demonstrating a compatibility level upgrade from SQL Server 2014 (120) to SQL Server 2025 (170).

In this setup, our production database, DemoDB, is currently running on SQL Server 2019 with a 120 compatibility level. As part of the migration scenario, this database will be moved to a newly deployed SQL Server 2025 instance, where its compatibility level will then be raised to 170.



## Getting Started
First, we need a test server with SQL Server 2025 installed. This will be our replay server. If you don't already have one, make sure you set up a dedicated test environment running SQL Server 2025.
Since you will be restoring the DemoDB backup into this test environment, don't forget to allocate enough disk space to accommodate the full database restore. Having sufficient storage upfront is important to avoid interruptions later in the replay and testing process.
Next, you need to install and properly configure XEvent Query Replayer for the replay server.
It’s important to note that XEvent Query Replayer does not have to be physically installed on the replay server itself. As long as the machine where the tool runs can access the XEL files and connect to the SQL Server 2025 replay instance, you can execute the replay from another client or server.

That said, if your goal is to complete the replay as quickly as possible, the best approach is to place both XEvent Query Replayer and the XEL files directly on the replay server. This avoids unnecessary network traffic and eliminates latency caused by reading XEL files over the network. My strong recommendation is to keep everything local to ensure the replay finishes in the shortest possible time.

All required details for installing and configuring XEvent Query Replayer are covered in the documentation linked below:

- [Getting started with the script](https://github.com/yigitaktan/XeQueryReplayer/blob/main/README.md#getting-started-with-the-script)
- [Script components](https://github.com/yigitaktan/XeQueryReplayer/blob/main/README.md#script-components)
- [Prerequisites](https://github.com/yigitaktan/XeQueryReplayer/blob/main/README.md#prerequisites)
- [Preparing the config.txt file](https://github.com/yigitaktan/XeQueryReplayer/blob/main/README.md#preparing-the-configtxt-file)
- [Running the script](https://github.com/yigitaktan/XeQueryReplayer/blob/main/README.md#running-the-script)
- [Encoding requirement](https://github.com/yigitaktan/XeQueryReplayer/blob/main/README.md#encoding-requirement)
- [Creating the test environment](https://github.com/yigitaktan/XeQueryReplayer/blob/main/README.md#creating-the-test-environment)
- [PowerShell execution policy settings](https://github.com/yigitaktan/XeQueryReplayer/blob/main/README.md#powershell-execution-policy-settings)
- [Logging](https://github.com/yigitaktan/XeQueryReplayer/blob/main/README.md#logging)
- [Errors](https://github.com/yigitaktan/XeQueryReplayer/blob/main/README.md#errors)


Once XEvent Query Replayer is fully set up and ready, we can move on and start with the first step of the replay process.



## Step 1 / Capturing Data
As the first step, we capture the production workload using the [start-capture.sql](https://github.com/yigitaktan/XeQueryReplayer/blob/main/start-capture.sql) script. The key decision here is how long you want to capture. If your workload is very busy and the server is handling a lot of concurrent activity, a 5-minute capture is often enough to get a representative sample.

One important thing to keep in mind: if you capture 5 minutes of workload, you should not expect to replay it in exactly 5 minutes with XEvent Query Replayer. The more queries you capture (especially under high concurrency), the longer the replay will take.

The start-capture.sql script supports several parameters, but the two most important ones are:

- **@Duration**: how long the capture should run, in "HH:MM:SS" format. For example, for a 5-minute capture you would use: 00:05:00
- **@CollectType**: what type of queries you want to capture:
  - "sp" (stored procedures)
  - "adhoc" (ad-hoc queries)
  - "sp-and-adhoc" (both)

All parameters are documented in detail in the script itself via comments.

When you run the script, it creates an Extended Events session, captures the workload using the selected options, and once the capture is completed, it drops the session automatically. Because of that, the user running the script must have permission to create and drop Extended Events sessions.



## Step 2 / Round 1 - Syncing Up
The timestamp of the data on the replay server must match the timestamp of the workload you captured.

In other words, the database state on the replay server must reflect the same point in time as the moment when the capture started.

For example, let’s say you started capturing workload on the production system at 10:10 and stopped it at 10:20, resulting in a 10-minute capture. To replay this workload correctly, you must restore a backup taken at 10:10 (or as close as possible) onto the replay server. The replay database must represent the 10:10 state of the data.

The reason for this is straightforward: if a query executed at 10:09 performed a DROP, INSERT, or UPDATE, and a query captured at 10:10 depending on that change, the replayed query will fail unless the replay database already contains that exact data state. Aligning the database to the same point in time ensures that all replayed queries can run successfully without schema or data-related errors ([Restore a SQL Server Database to a Point in Time](https://learn.microsoft.com/en-us/sql/relational-databases/backup-restore/restore-a-sql-server-database-to-a-point-in-time-full-recovery-model?view=sql-server-ver17)).



## Step 3 / Round 1 - Check/Configure Compatibility Level
Before replaying any data, the first thing we need to do is check the database compatibility level.

As shown in the replay workflow diagram at the beginning of the document, I split the process into two separate rounds:

- Round 1 runs on the lower compatibility level
- Round 2 runs on the upgraded compatibility level

At this point, we are working on Round 1, where the database must run on the original compatibility level.

In this scenario, we are using an upgrade path from 120 to 170, so the database (DemoDB) restored on the replay server must be running at compatibility level 120. To verify this, and to set it explicitly if needed, we should run the [step3.sql](https://github.com/yigitaktan/XeQueryReplayer/blob/main/ab-test/step-3.sql) script to ensure the compatibility level is correctly configured before starting the first replay.



## Step 4 / Round 1 - Configuring the Query Store
In this step, we run [step-4.sql](https://github.com/yigitaktan/XeQueryReplayer/blob/main/ab-test/step-4.sql) on the replay server to configure Query Store for DemoDB. The goal is simple: when we replay the captured Extended Events workload, we want Query Store to capture and retain all the metadata generated by those queries.

That means we will use Query Store to collect the things we care about most during A/B testing, especially I/O metrics and execution plan information. Later, during analysis, we will query this data using the Query Store DMVs.

For this setup, I typically use the following Query Store configuration:

- Operation mode: **READ_WRITE**
- Data Flush Interval: **1 minute**
- Statistics Collection Interval: **1 minute**
- Query Capture Mode: **ALL**

I use ALL because I don’t want Query Store to make decisions for me. I want to capture every query that shows up during replay.

Query Store capture mode supports four values, with the following meanings:

- **ALL**: Captures all queries
- **AUTO**: Captures queries based on resource consumption
- **NONE**: Stops capturing new queries
- **CUSTOM (SQL Server 2019+)**: Captures queries based on custom capture policy options

For Max Storage Size, I usually set 10 GB for this type of testing, and it’s often sufficient. However, the right value depends heavily on your workload, especially how many concurrent transactions per second you’re generating during replay. So, in real projects, you should size this per database based on your expected capture volume.



## Step 5 / Round 1 - Replaying Data
If you have completed the first four steps on the replay server, meaning the database state has been aligned to the capture start timestamp, the compatibility level is set correctly, and Query Store is configured, you are now ready to replay the captured XEL files using XEvent Query Replayer.

At the beginning of the document, I shared the documentation links that walk through how to set up and run XEvent Query Replayer. To avoid issues during replay, make sure you follow those steps carefully and verify that the tool is fully operational before starting the replay process.

<img width="581" height="363" alt="step5" src="https://github.com/user-attachments/assets/f14f66b0-fccf-4b31-90fa-909f541456df" />



## Step 6 / Round 1 - Verify Collected Data
After the replay finishes, you should validate that the replayed queries made it into Query Store. The easiest way to do that is to run [step-6.sql](https://github.com/yigitaktan/XeQueryReplayer/blob/main/ab-test/step-6.sql), which checks whether Query Store has stored the expected query metadata. If Query Store is empty, missing data, or looks incomplete, don’t move on to the next steps yet, stop and troubleshoot first. At this stage, you need to understand why the data isn’t there.

In practice, the most common cause is that the replay workload includes INSERT / UPDATE / DELETE activity, which changes data over time. If your replay database state doesn’t truly match the capture start point, queries can fail or behave differently, and Query Store won’t reflect the workload correctly. In those cases, repeating the point-in-time restore and replaying again is usually the cleanest and healthiest way to get reliable results.

<img width="716" height="49" alt="step6" src="https://github.com/user-attachments/assets/91b0658f-77f4-4cd3-b8bc-92f4af203480" />



## Step 7 / Round 1 - Extracting Query Store Data
In the previous step, we confirmed that the replayed workload was successfully captured in Query Store. From this point on, all of our analysis will be done exclusively using Query Store data, we no longer need the actual user table data in the database.

Keeping the full database around at this stage only wastes disk space on the replay server, and more importantly, it can prevent us from having enough space for the next restore in the second round. For that reason, we want to retain only the Query Store metadata and get rid of the user data, effectively shrinking the database footprint.

To achieve this, we use the [DBCC CLONEDATABASE](https://learn.microsoft.com/en-us/sql/t-sql/database-console-commands/dbcc-clonedatabase-transact-sql?view=sql-server-ver17) command. When you run CLONEDATABASE with its default options, it does exactly what we need: it creates a clone of the database without user table data, while preserving Query Store metadata.

The result is a much smaller database, containing only the information required for analysis, and freeing up disk space on the replay server for the next round of testing.

To create the database clone as described, use [step-7.sql](https://github.com/yigitaktan/XeQueryReplayer/blob/main/ab-test/step-7.sql) script. In the script, you’ll see that DemoDB is cloned as DemoDB_cl120.
The reason I add the _cl120 suffix is to clearly indicate which compatibility level the Query Store data belongs to. This makes things much easier during analysis, especially when you’re comparing multiple rounds side by side. Since in the first round we ran the replay with compatibility level 120, the clone is named DemoDB_cl120 accordingly.


## Step 8 / Round 1 - Verify the Clone
At this point, we need to verify that the database clone was created exactly as intended. That means there should be no data in the user tables, while all the replayed workload must still be present in Query Store.

To validate this, we run the [step-8.sql](https://github.com/yigitaktan/XeQueryReplayer/blob/main/ab-test/step-8.sql) script. This step confirms that the clone contains only Query Store metadata and no user data, ensuring the clone is ready and safe to use for analysis.

<img width="626" height="255" alt="step8" src="https://github.com/user-attachments/assets/44a0061d-107b-42e3-b572-510da1dadc1e" />


## Step 9 / Round 1 - Removing the Restored Database
Now that DemoDB_cl120 contains all the Query Store metadata we need, we can safely remove the original DemoDB from the replay server. Keeping it around only wastes disk space.

Using the [step-9.sql](https://github.com/yigitaktan/XeQueryReplayer/blob/main/ab-test/step-9.sql) script, we drop DemoDB and free up space on the server for the next restore and replay round.


## Step 10 / Round 2 - Data Recreation
To start Round 2, we restore the DemoDB backup again, exactly the same way we did in Round 1.

The key requirement is the same: the restored backup must represent the correct point in time, meaning the same timestamp / LSN that matches the start of the workload capture. This ensures the replay environment is consistent and ready for the second replay run under the upgraded compatibility level.


## Step 11 / Round 2 - Check/Configure Compatibility Level
Before replaying the data in Round 2, we must make sure the compatibility level has been upgraded to 170. To do that, we run the [step-11.sql](https://github.com/yigitaktan/XeQueryReplayer/blob/main/ab-test/step-11.sql) script.

This is the same script we used in the first round to set the compatibility level to 120. The only difference now is that we set it to 170 so the replay runs under the upgraded compatibility level.


## Step 12 / Round 2 - Configuring the Query Store
Next, we enable and configure Query Store again using the exact same settings as in Round 1. This is important to ensure the comparison between the two rounds is fair and consistent.

To do this, simply run the [step-12.sql](https://github.com/yigitaktan/XeQueryReplayer/blob/main/ab-test/step-12.sql) script.


## Step 13 / Round 2 - Replaying Data
Now it's time to replay the same captured workload again, but this time under compatibility level 170.

Before you start the replay, do one last sanity check:

- The database was restored from the same backup / point in time used in Round 1
- The compatibility level is set to 170
- Query Store is enabled and configured with the same settings as before

Once you have confirmed all of that, you can run XEvent Query Replayer and replay the XEL workload against the database in CL 170.

<img width="580" height="360" alt="step13" src="https://github.com/user-attachments/assets/f90fe9f5-68b6-4e50-8b3d-edf0547c509e" />


## Step 14 / Round 2 - Verify Collected Data
After the replay operation completes, this step is used to verify that the replayed workload has been successfully captured by Query Store.

At this stage, you check whether the replayed queries are properly logged in Query Store, confirming that the metadata required for analysis is available before moving forward. To do this, run the [step-14.sql](https://github.com/yigitaktan/XeQueryReplayer/blob/main/ab-test/step-14.sql) script.


## Step 15 / Round 2 - Extracting Query Store Data
Just like in Round 1, once you have confirmed that the replayed workload is present in Query Store, you can get rid of the bulky user data to avoid wasting disk space on the replay server.

At this point, we only need the Query Store metadata for analysis. So we use [DBCC CLONEDATABASE](https://learn.microsoft.com/en-us/sql/t-sql/database-console-commands/dbcc-clonedatabase-transact-sql?view=sql-server-ver17) to create a clone of the database that does not include user table data, but does retain the Query Store metadata from the replay. This gives us a much smaller database footprint while preserving everything we need for the A/B comparison. To create the database clone at this stage, simply run the [step-15.sql](https://github.com/yigitaktan/XeQueryReplayer/blob/main/ab-test/step-15.sql) script.


## Step 16 / Round 2 - Verify the Clone
Just like we did in Step 8 during Round 1, at this stage we need to verify that the newly created clone contains Query Store data only and does not include any user table data.

To validate this, we run the [step-16.sql](https://github.com/yigitaktan/XeQueryReplayer/blob/main/ab-test/step-16.sql) script. This check confirms that the clone was created correctly and is ready to be used for analysis.

<img width="625" height="275" alt="step16" src="https://github.com/user-attachments/assets/a3e99911-c9a2-4889-a972-4ea66c9f1f4f" />


## Step 17 / Round 2 - Removing the Restored Database
Once you have confirmed that the Query Store data is present in the clone, you can safely drop the original DemoDB that still contains user data.

To do this, simply run the [step-17.sql](https://github.com/yigitaktan/XeQueryReplayer/blob/main/ab-test/step-17.sql) script.


## Step 18 / Analysis Time
The purpose of this step is to identify, quantify, and explain query performance regressions that occur after a compatibility level change, using Query Store data collected during replay.

At this point in the A/B testing workflow:

- Production workload has already been replayed on both environments
- Query Store has captured execution statistics for both sides
- The goal is no longer data collection, but evidence-based analysis

This step provides a deterministic, query-level comparison between two databases running at different compatibility levels (LowerCL vs HigherCL), allowing engineers to:

- Detect regressions that matter
- Rank them by real impact
- Understand whether regressions are caused by plan instability, plan shape changes, or execution behavior differences

For a detailed breakdown of how this comparison is implemented, including query grouping strategy, metric normalization, execution-weighted aggregation, regression scoring, and result interpretation, click through to the dedicated [Query Store CL Regression Comparator](https://github.com/yigitaktan/XeQueryReplayer/blob/main/ab-test/regression-comparator.md) documentation, where the script logic and analysis methodology are explained in depth.
