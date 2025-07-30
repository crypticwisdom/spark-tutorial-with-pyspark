# Apache Spark Deep-Dive Notes (NOTES FROM TUTORIAL - EMIL KAMINSKI) : [text](https://www.youtube.com/watch?v=iXVIPQEGZ9Y&list=PL19TxVqoJEnSWDIkyI8va3njcLrVKDfc9&index=3)

- Main components like driver and worker
- Spark UI
- Deployment modes
- RDDs, Dataframes and Datasets
- Transformations and actions
- Job, Stages and Tasks
- Data Shuffling
- Introduction to Optimization


**Spark Flavours on Cloud:** -
- databricks
- google cloud dataproc
- ibm watson studio
- amazon emr
- Microsoft Azure Databricks

---

## Spark Architecture

---

Say we have a data we want to analyze and have written the application in (Python, Java, SQL, Scala, R), The application is submitted and it communicates with the Driver through Spark Session which is the only entry point for Spark, the driver parses the application and analyzes what we want to do with the data and also it will figure out the most efficient way of doing it (catalyst optimizer -> creates optimized logical plan), it will then split (DAGs into Stages then stages in tasks) it into tasks (task, data partition info, conf, distributed variables), which will be distributed to the executors (each exec. process these in parallel, multiple tasks as the number of slots: numbers of slots = total number of cpu cores).

Say we have 2 executors with 4 cores each, each executor is able to process 4 task in parallel, the driver sends tasks to the executor and it also sends information a bout which executor would process/analyze which part of the data. Then executor grab the data and exchange the information between each other (if needed) and then send the results to the driver or writes to somewhere else.

**Example: Counting a Dataset**
Our application communicates with the Driver through spark session, Driver takes that application and analyze what’s the optimized way of executing it and also analyze the data, how much of data is it, how much executor we have and most efficient way not only in executing the application but also in distributing that datasets to the executors.
- It creates tasks which carries instruction of how to process a partition.
- The driver looks at the data and decides how its going to be splited in partitions, it send task to executors, to grab different partitons of data, and assign task to each and every slot (cpu in the executor), now these slots can have equal number of tasks or not (slot-1 can have 4 task to execute, slot-2 can have 4, slot-3 can have 4, slot-4 can have or 3, just so the tasks are splitted across the executors, so in summary some slot can have more or less tasks in slot).
- Now the executor starts executing task, in theis case, maybe slot-4, finishes first because of less tasks, the slot1, 2, and 3, in this case all task were success, it can also happend that some task will fail and driver needs to reassign a task to another slot; so we almost done (local count of each task on partition are done, but we don't have a final count of entire data), the next task, now the driver will assign a single executor to get all local counts from slots and executors in the cluster, and the next task is to perform final count. Now after the last count, depending on the application the instruction maybe to send or save the data to file, then there is no need to communicate back to the driver, it just writes it to the file; also the program could say "display the number to user", in such case the final count is sent back to the driver in other for it display the final result to user.



### Example Workflow:

1. You write code using transformations (e.g. `.filter()`, `.groupBy()`).
2. Spark parses it into a **Logical Plan**.
3. When you call an **action** (e.g., `.collect()`), Spark optimizes the plan, converts it to a **Physical Plan**, splits it into a **DAG** of **Stages**, then breaks stages into **Tasks**.
4. Tasks are sent to Executors for parallel execution.
5. Results are returned to the Driver or written to storage.

---

## Spark Deployment Modes - 

* **Local**: Driver and Executors run on the same machine. Ideal for testing.
* **Client**: Driver runs on your local machine, Executors run on the cluster.
* **Cluster**: Driver and Executors both run in the cluster. Suitable for production.

---

## RDDs, DataFrames, and Datasets

* **RDD (Resilient Distributed Dataset)**:

  * Low-level, immutable distributed collection of objects.
  * Provides fine-grained control, fault-tolerance, and lineage.
  * Created from `SparkContext`. (Spark Core Engine)
  * You can find details of any application using RDD in Job and Stages section in the UI.

* **DataFrame**:

  * High-level abstraction similar to a SQL table.
  * Built on top of RDDs and optimized using the Catalyst optimizer.
  * Created from `SparkSession.read()`.
  * You can find details of any application using DF/DS in Job, Stages, Spark SQL/Dataframe section in the UI.

* **Dataset**: Combines the benefits of RDD and Dataframe, offering a strongly-typed object oriented API with optimization capabilities of dataframes.  Created from SparkSession (Spark SQL Module)

Dataframe and Dataset (Scala) are Optimized by the Catalyst Optmizer in Spark and these Abstraction are created from Spark SQL also Structured API.

---

## Transformations and Actions

### What are Transformations?

* Operations that define a new dataset from an existing one. (Business logic you want to apply to your data)
* **Lazy**: They don’t execute until an action is called.
* **Immutable**: Each transformation creates a new dataset.

#### Narrow vs Wide Transformations:

* **Narrow**: Data is not shuffled. Each input partition maps to one output partition.

  * Examples: `map()`, `filter()`, `flatMap()`, `union()`, `coalesce()`, cast, creat new column, 

* **Wide**: Data is shuffled between partitions. Means exchange of information between executors. Triggers new stage.

  * Examples: `groupByKey()`, `reduceByKey()`, `join()`, `distinct()`, `repartition()`

### What are Actions?

* Operations that trigger the execution of transformations.
* Examples: `collect()`, `count()`, `first()`, `take()`, `foreach()`, `saveAsTextFile()`

> **Tip**: Actions **materialize** the computation. Without them, Spark will not compute anything.

---

## Jobs, Stages, and Tasks
Based on general rule: When we submit application to the driver, it splits the application into 1 or more Jobs, usually these Jobs runs 1 after the other, inside each job we have 1 or more stages, and stage 2 can start only after stage 1 is done, and each stage hold multiple tasks, and these tasks are sent to each CPU core of executors to run in parallel. And the reason for multiple stages is because APache Spark tries to compact all the transformation which does not require data shuffling into 1 stage (narrow transformation), and then only when the shuffling is requred then the new stage will be created.
The above is an explanation based on the DAG flow, stages shown in DAG UI holds narrow transformations and only when there is a SHuffle you will then find a line which moves fromthe first stage to another new stage.

* **Job**: Triggered by an action. Represents the full execution of a DAG.
* **Stage**: A set of tasks that can be executed together. Created when a wide transformation (like shuffle) is encountered.
* **Task**: Smallest unit of work. One task per data partition.

### Execution Flow:

1. User calls an **action** -> **Job** created.
2. Spark divides Job into **Stages** based on wide transformations.
3. Each Stage is divided into **Tasks**.
4. Tasks are scheduled to **Executors** based on available CPU cores.

> Example: With 2 executors having 4 cores each, 8 tasks can run in parallel.

---

## Understanding DAG

### DAG in Spark:

- Directed -> The edges (arrows) have a direction (A -> B -> C).
- Acyclic -> There are no loops; You can't go bak to an ealier point (no A -> B -> A; only A -> B -> C -> D -> ...)
- Graph -> A structure made of nodes (points) and edges (arrows).
In simple term: A DAG is like a flowchart wwhere each step leads forward never looping back.
- Represents the lineage and dependencies between operations.
- **Optimized DAG** is created after the Catalyst optimizer finishes planning.

> DAG = Blueprint for execution. Spark uses this to break computation into stages and tasks.

### Spark UI DAG:

* Shows the **Execution DAG** (not the raw logical plan).
* Optimized and used to plan stages and tasks.
* Visualizes transformations and stage boundaries (shuffles).

**Analogy:**
  - DAG = entire recipe
  - Stage = Parts of the recipe you must complete before moving to the next
  - Task = Individual step like "cut one onion" (done in parallel)

### Why does Spark use DAG?
- Optimizations:
  - Spark can see the entire chain of transformations before running them.
  - This allows it to optimize, execution (e.g, wide transformation into 1 stage)
- Fault Tolerance:
  - If a worker node fails, spark can re-compute list data by following the DAG lineage (because the DAG tells Spark how the data was created)
- Better Scheduling:
  - Spark can break the DAG into stages and schedule task efficiently across the cluster.


### Visualization DAG in Spark:
The Spark UI (4040) shows a DAG Visualization of how spark broke your code into:
    - Transformation (Arrows)
    - Stages (Blocks)
    - Shuffles (Boundaries vbetween stages)

---

## Spark Planning:
- Start with: Logical Planning:
  - when:
    - As soon as the driver reads your spark code transformation on RDD/DF/DS.
  - What it does:
    - Spark at this stage builds a logical DAG of all transformations
    - This is a high-level recipe of "what needs to happen", but not yet, how.
    **E.g.:** .read(), .filter(), groupBy(), .count() ...
    Logical plan will look like: `Read CSV -> Filter(Condition) -> GroupBy -> count()`
    This is not represented as a DAG yet;
    But no execution is carried out because it not the end of spark processing, NEXT (Optimization)

- Then: Optimization (Catalyst Optimizer):
  - WHEN: Still before any execution; Spark applies its Catalyst Optimizer.
  - What it does:
    - Looks at the Logical plan, re-writes it for Efficiency:
      - Combines multiple filters (.filter().filter(), into 1 filter())
      - Push down filter to the source (e.g., only load rows with column used in filter condition)
      - Choose the best join strategies (Broadcast joins VS shuffle joins)
    After this Spark has an Optimized Logical Plan

- Then: Physical Planning
  - What it is: Spark figure and hold:
    - What algorithm to use (HASH join, sort, merge, e.t.c)
    - How to split work across executors
  - Result: A Physical plan.

- DAG Scheduler: BUilding the execution DAG
  - when: after an action is called
  - what it does: 
    - Takes Physical plan and turns it into a DAGof stages
    - Stages are separated by WIDE transformations (groupBy, Join, sort, ...) because those require shuffling.

- Task Scheduling:
  - What happens:
    - The DAG Scheduler hands each stage to the Task Scheduler 
    - The task scheduler created tasks (One per partition) and ships them to executors
  - Execution starts after the executor collects the task, processes it and write to storage or return to Driver.
  
Important Notes:
- Logical, Optimized and Physical Planning happen when you define transformations but they stay as plans only.
- Execution (Jobs/Stage/Tasks) only starts when an action is called.
- `Code -> Logical Plan -> Optimizer (Optimized Logical Plan) -> Physical Plan -> DAG Scheduler -> Task Scheduler -> Executors`

- DAG visualization shows, optimized logical plan turned in execution DAG (What Spark will actually run)
- logical plans are not shown in UI
- what are the 3 plans represented as?
  - Logical Plan -> Only exists in driver (not shown in UI)
  - Optimized Logical Plan -> Basis for the execution DAG
  - Physical Plan -> Also not directly
Summary:
  - UI shows 1 high-level DAG per Job (shows the whole job flow).
  - Stage DAGs under each job (shows how the job is split into stages).



### When Is Execution Triggered?

* Only when an **Action** is called.
* Multiple actions on the same un-cached dataset = multiple jobs.

> Analogy: Writing code builds a plan. Calling an action is like hitting "Run" on a machine.

---

## Final Notes:

* `.read()` is a **data source definition**, not a transformation.
* The DAG shown in Spark UI is the **optimized execution DAG**, not the original logical plan.
* Planning happens on the **Driver**, execution happens on **Executors**.

```python
  df = spark.read.json("data.json")  # Defines source, no job yet
  df_filtered = df.filter("age > 30")  # Transformation, lazy
  df_filtered.show()  # Action -> triggers job, stages, tasks
```

---

Proper Flow:

```
Your Code
   ↓
Logical Plan (Unresolved) — created by the Catalyst Analyzer
   ↓
Optimized Logical Plan — via Catalyst Optimizer
   ↓
Physical Plan — multiple options generated
   ↓
Selected Physical Plan — Spark picks the best one
   ↓
DAG Scheduler builds an Execution DAG (from physical plan)
   ↓
Job(s) — 1 per action
   ↓
Stage(s) — separated by shuffle boundaries (wide transformations)
   ↓
Tasks — 1 per data partition
   ↓
Executors — tasks are executed here

```

---

## Spark UI:
- Resource 1: https://www.youtube.com/watch?v=rNpzrkB5KQQ

* Default port: `localhost:4040`

### **Sections in the UI:**

**Jobs:** Gives you a list of completed, failed, active jobs, timeline ...; And you can click on a Job to get a proper detail of it with other infos. The job detailk pages for a particular job shows you a DAG made up of 1 or more stages.
**Stages:** Its a child object of a Job, you can see detail for a stage of a particular Job (created per action on un-cached DF) and tasks in each stage and how these tasks are spread across executors; Breakdown of each job into stages.
**Storage:** Shows cached data (.cache()), you come here to see what's already cache; It doesn't hold your processed data on disk it holds only caches; stores query plan; Once the cluster restarts it loss cache data in memory.
**Environments:** Shows environemt configs and variables.
**Executors:** Shows you number of executors in the cluster and how busy they are.
**SQL/DF:** Shows you DAG of Optimized Query Plan, how spark has interpreted the query and the query plan it is using. 

> Use the DAG tab to understand how your transformations are executed under the hood.
> **DAG Visualization**: Optimized DAG per job, showing Stages, transformations and shuffles.

---

The type of transformation we are doing will denote whether we need a shuffle.
**Wide transformation:** This type of transformation can't be done on a single executor, it has to move data from executors, shuffle it around, change how that data is chunked up, put it back on those executors.

If I have written a bunch of transformation on a DF and then I specify an action '.show()' on that DF. It will traverse upward to the list of used transformation that has to be done on the DF (Narrow and Wide trans), and then, it make a query plan, and in that plan any time it is having to shuffle data, a new stage is created (which is handling a number of tasks). Each time there is a shuffle, there will be a new stage with a new number of tasks to be executed by executors.

As the processes moves to the next stage, other tasks from the previous stages are taken out so that new tasks from the new stage can run (task can be more than 1, and 1 task is ran on 1 slot (CPU core)).



### DAG in UI:
- **Execution DAG (Job DAG):** Full DAG for a job; includes all stages. Found in Jobs tab; This shows the entire execution plan of a Job, the diagram shows 1 or more block (stages and its tasks and operations (nodes) that spark will perform within that specific stage). If a Job has more than 1 stages in it, the diagram show a linked line between each stages which represents a shuffle.
Stages are caused by shuffles, and in each stage, spark tries to combine all narrow transformation into 1 stage. And Jobs are caused by action call.
- **Stage DAG:** Breakdown of internal RDD operations in a single stage. shows transformation chain of RDDs; This is the internal plan for a specific stage, showing how RDDs and transformations (like mapPartitions) are connected.
Each box is an RDD (Resilient Distributed Dataset) and each arrow is a dependency — this forms a DAG (since there are no cycles).

---
...



## **Laziness of Spark:**
The DAG structure you see in the Spark SQL /Dataframe section of the UI, is not a DAG it is a Plan.
- Here he talked about the .explain() used to view Spark Plan, Physical, Optimized .. Plans and also showed some examples in databrick UI. I will re-visit this and also watch What a support video for this.
- ...


## **Data Shuffling:**
- This is the process of redistributing data across partitions, and typically involves data exchange between executor nodes.
- Wide transformations, require shuffling
- It requires saving data to disk, sending it over network and reading data from the disk
- Data Shuffling can be very expensive
- Sometimes it can be mitigated or avoided by even code changes, like for instance avoiding sorting the data
- Nevertheless, it's often a "necessary evil".
...


## **Optimization:**
What user can do to Optimize Spark:
- Use optimized format for storaging data, like parquet, delta, instead of CSV.
- Avoid expensize operations like sort
- Minimize volume of data
- Cache/Persist dataframes
- Repartition/Coalsce
- Avoid UDFs
- Partition and/or index data
- Bucketing
- Optimize Cluster
- ...


### **Spark is Already doing alot for us:**
- Catalyst Optimizer: handles logical and physical query plan optimizations.
  - 
- Tungsten Engine: provides low-level optimizations and code generation techniques to improve overall performance.
- AQE: dynamically adjust query execution based on runtime conditions.

...