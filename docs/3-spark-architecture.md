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

Say we have a data we want to analyze and have written the application in (Python, Java, SQL, Scala, R), The application is submitted and it communicates with the Driver through Spark Session which is the only entry point for Spark, the driver parses the application and analyzes what we want to do with the data and also it will figure out the most efficient way of doing it (catalyst optimizer -> creates optimized logical plan), it will then split (DAGs into Stages then stages into tasks) it into tasks (task, data partition info, conf, distributed variables), which will be distributed to the executors (each exec. process these in parallel, multiple tasks as the number of slots: numbers of slots = total number of cpu cores).

Say we have 2 executors with 4 cores each, each executor is able to process 4 task in parallel, the driver sends tasks to the executor and it also sends information a bout which executor would process/analyze which part of the data. Then executor grab the data and exchange the information between each other (if needed) and then send the results to the driver or writes to somewhere else.

**Example: Counting a Dataset**
Our application communicates with the Driver through spark session, Driver takes that application and analyze what’s the optimized way of executing it and also analyze the data, how much of data is it, how much executor we have and most efficient way not only in executing the application but also in distributing that datasets to the executors.
- It creates tasks which carries instruction of how to process a partition.
- The driver looks at the data and decides how its going to be splited in partitions, it send task to executors, to grab different partitons of data, and assign task to each and every slot (cpu in the executor), now these slots can have equal number of tasks or not (slot-1 can have 4 task to execute, slot-2 can have 4, slot-3 can have 4, slot-4 can have or 3, just so the tasks are splitted across the executors, so in summary some slot can have more or less tasks in slot).
- Now the executor starts executing task, in theis case, maybe slot-4, finishes first because of less tasks, the slot1, 2, and 3, in this case all task were success, it can also happend that some task will fail and driver needs to reassign a task to another slot; so we almost done (local count of each task on partition are done, but we don't have a final count of entire data), the next task, now the driver will assign a single executor to get all local counts from slots and executors in the cluster, and the next task is to perform final count. Now after the last count, depending on the application the instruction maybe to send or save the data to file, then there is no need to communicate back to the driver, it just writes it to the file; also the program could say "display the number to user", in such case the final count is sent back to the driver in other for it display the final result to user.




---




## Spark Deployment Modes

* **Local**: Driver and Executors run on the same machine. Ideal for testing.
* **Client**: Driver runs on your local machine, Executors run on the cluster.
* **Cluster**: Driver and Executors both run in the cluster. Suitable for production.


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
The above is an explanation based on the DAG flow, stages shown in DAG UI holds narrow transformations and only when there is a Shuffle you will then find a line which moves fromthe first stage to another new stage.

* **Job**: Triggered by an action operation in code, unless cache. Represents the full execution of a DAG.
* **Stage**: A set of tasks that can be executed together. Created when a wide transformation (like shuffle) is encountered.
* **Task**: Smallest unit of work. One task per data partition.

> Example: With 2 executors having 4 cores each, 8 tasks can run in parallel.






---









## Understanding DAG (Directed Acyclic Graph)

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

Summary:
  - UI shows 1 high-level DAG per Job (shows the whole job flow).
  - Stage DAGs under each job (shows how the job is split into stages).



### When Is Execution Triggered?

* Only when an **Action** is called.
* Multiple actions on the same un-cached dataset = multiple jobs.

> Analogy: Writing code builds a plan. Calling an action is like hitting "Run" on a machine.

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



### DAGs in UI:
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







## ✅ Apache Spark Execution Flow

Let’s use this example as a reference:

```python
  df = spark.read.parquet("s3://data").filter("age > 25").select("name")
  df.show()
```

---

## 🔷 1. **User Code Definition (in Driver Program)**

* You write **transformations** (`read`, `filter`, `select`) and **actions** (`show()`).
* These operations are **lazy** — they’re not executed until an **action** is called.

✅ At this point, a **DataFrame** object is created, which contains an **Unresolved Logical Plan**.

---

## 🔷 2. **Catalyst Analyzer (Analysis Phase)**

* Spark uses the **Catalyst Analyzer** (part of the Catalyst optimizer) to:

  * Validate and resolve column names, data sources, aliases, and types.

🧠 Converts:

> **Unresolved Logical Plan** → **Resolved Logical Plan**

---

## 🔷 3. **Catalyst Optimizer (Optimization Phase)**

* Spark applies **rule-based optimizations** to the resolved plan:

  * Filter pushdown
  * Constant folding
  * Column pruning
  * Join reordering

🧠 Converts:

> **Resolved Logical Plan** → **Optimized Logical Plan**

---

## 🔷 4. **Physical Planning**

* Spark generates multiple **Physical Plans** (possible execution strategies).
* A **Cost-Based Optimizer (CBO)** selects the best plan based on metrics like:

  * Shuffle size
  * Join strategy
  * Memory footprint

🧠 Converts:

> **Optimized Logical Plan** → **Best Physical Plan**

---

## 🔷 5. **DAG Generation (Execution Plan)**

* The chosen physical plan is transformed into a **DAG (Directed Acyclic Graph)** of stages and tasks.
* Each node in the DAG is a computation step.
* Spark identifies **stage boundaries** based on **wide transformations** (e.g., `join`, `groupBy`), which involve **shuffles**.

---

## 🔷 6. **DAG Scheduler (Job & Stage Creation)**

* On triggering the **action** (like `.show()`):

  * Spark submits a **job** to the DAG Scheduler.
  * The job is split into **stages**, depending on shuffle boundaries.
  * Each stage is composed of **tasks**, one for each partition of the input data.

💡

| Term      | Trigger/Event                        |
| --------- | ------------------------------------ |
| **Job**   | Triggered by an **action**           |
| **Stage** | Created at each **shuffle boundary** |
| **Task**  | One task per partition per stage     |

---

## 🔷 7. **Task Scheduler & Cluster Manager**

* The **Task Scheduler** sends the tasks to the **Cluster Manager** (YARN, Kubernetes, Mesos, or Standalone).
* The **Cluster Manager** allocates resources (executors on worker nodes).

---

## 🔷 8. **Execution on Executors**

* Each **executor**:

  * Pulls the required **partition** of the data.
  * Runs the task logic (e.g., filter, project, shuffle, write).
  * Sends results back to the driver (if needed) or writes to external storage.

Executors also:

* Cache/persist data (if requested)
* Spill to disk if memory is exceeded
* Write shuffle files to disk during wide transformations

---

## ✅ End-to-End Diagram

```
User Code
   ↓
Unresolved Logical Plan (DataFrame holds this)
   ↓
Catalyst Analyzer
   ↓
Resolved Logical Plan
   ↓
Catalyst Optimizer
   ↓
Optimized Logical Plan
   ↓
Physical Plan Generation
   ↓
DAG of Execution Plan
   ↓
DAG Scheduler → Jobs → Stages (split on shuffle)
   ↓
Task Scheduler → Tasks per partition
   ↓
Cluster Manager allocates resources
   ↓
Executors execute tasks (pull partition → compute → write)
```

---

## ✅ Summary Table

| Stage              | Output                  | Notes                                          |
| ------------------ | ----------------------- | ---------------------------------------------- |
| Code Definition    | Unresolved Logical Plan | DataFrame created, no data yet                 |
| Catalyst Analyzer  | Resolved Logical Plan   | Validates and resolves names and schemas       |
| Catalyst Optimizer | Optimized Logical Plan  | Applies optimization rules                     |
| Physical Planning  | Best Physical Plan      | Chooses execution strategy                     |
| DAG Generation     | DAG of Stages/Tasks     | Defines execution graph                        |
| DAG Scheduler      | Job → Stages → Tasks    | Based on actions and shuffle boundaries        |
| Task Scheduler     | Task Submission         | Sends tasks to available executors             |
| Executors          | Task Execution          | Reads data, processes, writes/shuffles results |

---

Let me know if you want a **graphical diagram**, **timeline animation**, or even **visual comparison with SQL execution**.
