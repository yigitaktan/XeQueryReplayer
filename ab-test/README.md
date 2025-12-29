# A/B Tests
<br/>

> [!IMPORTANT]
> This document may have newer versions over time. To always read the most up-to-date and accurate version, make sure to refer to the following link:<br/><br/>
> https://github.com/yigitaktan/XeQueryReplayer/blob/main/ab-test/README.md<br/><br/>
> That repository is the authoritative source and will always reflect the latest updates and improvements.

<br/>

## Getting Started

When you change the compatibility level in SQL Server. In other words, when you move a database to a higher compatibility level, you naturally expect SQL Server to maintain the same I/O behavior for your queries, or ideally produce better and more efficient I/O.

This document focuses on the most critical part of that process: A/B testing. Specifically, it explains how to use [XEvent Query Replayer](https://github.com/yigitaktan/XeQueryReplayer/blob/main/README.md) to validate query behavior and performance during compatibility level changes, and how to confidently assess the impact of those changes before going live.

The diagram below illustrates the entire end-to-end workflow, starting from capturing workload data on the production server and continuing through all subsequent stages of the A/B testing and analysis process.


<img width="1196" height="588" alt="2025-12-29_08-43-46" src="https://github.com/user-attachments/assets/fc2d3d88-1cdb-43d8-b1b8-1a2ccb7d6c37" />

