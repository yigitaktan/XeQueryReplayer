# A/B Tests
<br/>

> [!IMPORTANT]
> This document may have newer versions over time. To always read the most up-to-date and accurate version, make sure to refer to the following link:<br/><br/>
> https://github.com/yigitaktan/XeQueryReplayer/blob/main/ab-test/README.md<br/><br/>
> That repository is the authoritative source and will always reflect the latest updates and improvements.

<br/>

## Upgrading Compatibility Level

When you change the compatibility level in SQL Server. In other words, when you move a database to a higher compatibility level, you naturally expect SQL Server to maintain the same I/O behavior for your queries, or ideally produce better and more efficient I/O.

This document focuses on the most critical part of that process: A/B testing. Specifically, it explains how to use [XEvent Query Replayer](https://github.com/yigitaktan/XeQueryReplayer/blob/main/README.md) to validate query behavior and performance during compatibility level changes, and how to confidently assess the impact of those changes before going live.

The diagram below illustrates the entire end-to-end workflow, starting from capturing workload data on the production server and continuing through all subsequent stages of the A/B testing and analysis process.


<img width="1196" height="588" alt="2025-12-29_08-43-46" src="https://github.com/user-attachments/assets/fc2d3d88-1cdb-43d8-b1b8-1a2ccb7d6c37" />

The scenario described below uses a database named DemoDB as an example, demonstrating a compatibility level upgrade from SQL Server 2014 (120) to SQL Server 2025 (170).

In this setup, our production database, DemoDB, is currently running on SQL Server 2019 with a 120 compatibility level. As part of the migration scenario, this database will be moved to a newly deployed SQL Server 2025 instance, where its compatibility level will then be raised to 170.

<br/>

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

<br/>

## Step 1 / Capturing Data
As the first step, we capture the production workload using the [start-capture.sql](https://github.com/yigitaktan/XeQueryReplayer/blob/main/start-capture.sql) script. The key decision here is how long you want to capture. If your workload is very busy and the server is handling a lot of concurrent activity, a 5-minute capture is often enough to get a representative sample.

One important thing to keep in mind: if you capture 5 minutes of workload, you should not expect to replay it in exactly 5 minutes with XEvent Query Replayer. The more queries you capture (especially under high concurrency), the longer the replay will take.

The start-capture.sql script supports several parameters, but the two most important ones are:

- `@Duration`: how long the capture should run, in "HH:MM:SS" format. For example, for a 5-minute capture you would use: 00:05:00
- `@CollectType`: what type of queries you want to capture:
  - "sp" (stored procedures)
  - "adhoc" (ad-hoc queries)
  - "sp-and-adhoc" (both)

All parameters are documented in detail in the script itself via comments.

When you run the script, it creates an Extended Events session, captures the workload using the selected options, and once the capture is completed, it drops the session automatically. Because of that, the user running the script must have permission to create and drop Extended Events sessions.
