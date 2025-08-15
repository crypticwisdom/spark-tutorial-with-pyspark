# Apache Spark Notes: Spark Execution and DAGs

**Resources used:**
- (NOTES FROM TUTORIAL - EMIL KAMINSKI) : [Youtube link](https://www.youtube.com/watch?v=iXVIPQEGZ9Y&list=PL19TxVqoJEnSWDIkyI8va3njcLrVKDfc9&index=3)
- (Reading and Understanding DAG: https://youtu.be/O_45zAz1OGk?si=ZpqtZzTHGiBM5fxv)


**Spark Flavours on Cloud:**
- databricks
- google cloud dataproc
- ibm watson studio
- Amazon EMR
- Microsoft Azure Databricks

---

## Spark Execution

---

Say we have a data we want to analyze and have written the application in (Python, Java, Spark SQL, Scala, R). The application is submitted and it communicates with the Driver through Spark Session which is the only entry point for Spark, the driver parses the application and analyzes what we want to do with the data and also it will figure out the most efficient way of doing it (catalyst optimizer -> creates optimized logical plan), it will then split (DAGs into Stages then stages into tasks) it into tasks (task, data partition info, conf, distributed variables), which will be distributed to the executors (each exec. process these in parallel, multiple tasks as the number of slots: numbers of slots = total number of cpu cores).

Say we have 2 executors with 4 cores each, each executor is able to process 4 task in parallel, the driver sends tasks to the executor and it also sends information about which executor would process/analyze which part of the data. Then executor grab the data and exchange the information between each other (shuffle, if needed) and then send the results to the driver or writes to somewhere else.

**Example: Counting a Dataset**
Our application communicates with the Driver through spark session, Driver takes that application and analyze what’s the optimized way of executing it and also analyze the data, how much of data is it, how much executor we have and most efficient way not only in executing the application but also in distributing that datasets to the executors.
- It creates tasks which carries instruction of how to process a partition.
- The driver looks at the data and decides how its going to be splited in partitions, it send task to executors, to grab different partitons of data, and assign task to each and every slot (cpu in the executor), now these slots can have equal number of tasks or not (slot-1 can have are splitted acros4 task to execute, slot-2 can have 4, slot-3 can have 4, slot-4 can have or 3, just so the tasks s the executors, so in summary some slot can have more or less tasks in slot).
- Now the executor starts executing task, in this case, maybe slot-4, finishes first because of less tasks, the slot1, 2, and 3, in this case all task were success, it can also happend that some task will fail and driver needs to reassign a task to another slot; so we are almost done (with local count of each task on partition are done, but we don't have a final count of entire data), the next task, now the driver will assign a single executor to get all local counts from slots and executors in the cluster, and the next task is to perform final count. Now after the last count, depending on the application the instruction maybe to send or save the data to file, then there is no need to communicate back to the driver, it just writes it to the file; also the program could say "display the number to user", in such case the final count is sent back to the driver in other for it display the final result to user.



---


## Transformations and Actions:

### What are Transformations?

* Operations that define a new dataframe/dataset from an existing one. (Business logic you want to apply to your data)
* **Lazy**: They don’t execute until an action is called.
* **Immutable**: Each transformation creates a new dataset.

#### Narrow vs Wide Transformations:
A wide dependency (or wide transformation) style transformation will have input partitions
contributing to many output partitions. You will often hear this referred to as a shuffle whereby
Spark will exchange partitions across the cluster. 

With narrow transformations, Spark will automatically perform an operation called pipelining, meaning that if we specify multiple filters on DataFrames, they’ll all be performed in-memory.

* **Narrow**: Data is not shuffled. Each input partition maps to one output partition.
  * Examples: `map()`, `filter()`, `flatMap()`, `union()`, `coalesce()`, cast, create new column, 

* **Wide**: Data is shuffled between partitions. Means exchange of information between executors. Triggers new stage.
  * Examples: `groupByKey()`, `reduceByKey()`, `join()`, `distinct()`, `repartition()`

### What are Actions?

* Operations that trigger the execution of transformations.
* Examples: `collect()`, `count()`, `first()`, `take()`, `foreach()`, `saveAsTextFile()`, `write()`, `...`.

> **Tip**: Actions **materialize** the computation. Without them, Spark will not compute anything.
There are three kinds of actions:
- Actions to view data in the console
- Actions to collect data to native objects in the respective language
- Actions to write to output data sources.

Transformations allow us to build up our logical transformation plan. To trigger the computation, we run an action. An action instructs Spark to compute a result from a series of transformations.

> At this point, all you need to understand is that a Spark job represents a set of transformations triggered by an individual action, and you can monitor that job from the Spark UI.

---


## Jobs, Stages, and Tasks:
When we submit application to the driver, it splits the application into 1 or more Jobs, usually these Jobs runs 1 after the other, inside each job we have 1 or more stages, and stage 2 can start only after stage 1 is done, and each stage hold multiple tasks, and these tasks are sent to each CPU core of executors to run in parallel. And the reason for multiple stages is because Apache Spark tries to compact all the transformation which does not require data shuffling into 1 stage (narrow transformation), and then only when the shuffling is requred then the new stage will be created.

The above is an explanation based on the DAG flow, stages shown in DAG UI holds narrow transformations and only when there is a Shuffle you will then find a line which moves from the first stage to another new stage.

* **Job**: Triggered by an action operation in code, unless cache. Represents the full execution of a DAG.
* **Stage**: A set of tasks that can be executed together. Created when a wide transformation (like shuffle) is encountered.
* **Task**: Smallest unit of work. One task per data partition.


### When Is Execution Triggered?

* Only when an **Action** is called.
* Multiple actions on the same un-cached dataset = multiple jobs.

> Analogy: Writing code builds a plan. Calling an action is like hitting "Run" on a machine.


---

## ✅ Apache Spark Execution Flow

Let’s use this example as a reference:

```python
df = spark.read.parquet("s3://data").filter("age > 25").select("name")
df.show()
```

---

## 🔷 1. **User Code Definition (in Driver Program)**

* You write **transformations** (`read`, `filter`, `select`) and **actions** (`show()`).
* Transformations are **lazy** — they are only *planned*, not executed, until an **action** is triggered.
* ✅ At this point, a **DataFrame** object is created, holding an **Unresolved Logical Plan**.

---

## 🔷 2. **Catalyst Analyzer (Analysis Phase)**

* Spark’s **Catalyst Analyzer**:

  * Validates and resolves column names, table names, aliases, and data types.
* 🧠 Converts:

  > **Unresolved Logical Plan** → **Resolved Logical Plan**

---

## 🔷 3. **Catalyst Optimizer (Optimization Phase)**

* Applies **rule-based optimizations** such as:

  * Filter pushdown
  * Constant folding
  * Column pruning
  * Predicate simplification
  * Join reordering (rule-based unless CBO kicks in)
* 🧠 Converts:

  > **Resolved Logical Plan** → **Optimized Logical Plan**

---

## 🔷 4. **Physical Planning**

* Spark’s **Physical Planner** takes the Optimized Logical Plan and generates one or more **candidate Physical Plans**.
* If **CBO** is enabled (`spark.sql.cbo.enabled=true` + table stats collected):

  * Spark evaluates each plan’s cost and chooses the cheapest.
* If **CBO** is off:

  * Spark uses fixed heuristics to pick a plan.
* 🧠 Converts:

  > **Optimized Logical Plan** → **Chosen Physical Plan** (a single tree of physical operators)

---

## 🔷 5. **Preparation for Execution**

* This step isn’t explicitly named in Spark’s docs but happens before DAG generation:

  * The chosen Physical Plan is wrapped into an **RDD-based Execution Plan** (every physical operator maps to one or more RDD operations).
  * Wide dependencies (shuffles) and narrow dependencies are identified here.
* Then it’s transformed into a **Stage DAG**.

---

## 🔷 6. **DAG Generation (Execution Plan)**

* From the Physical Plan’s RDD lineage, Spark builds a **DAG of stages**.
* Stage boundaries are set at **wide transformations** (e.g., `groupBy`, `reduceByKey`, joins that require shuffles).
* Each stage will later be broken into **tasks** for execution.

---

## 🔷 7. **DAG Scheduler (Job & Stage Creation)**

* When the action (`.show()`) is triggered:

  * A **job** is submitted to the **DAGScheduler**.
  * The job is split into **stages** based on shuffle boundaries.
  * Narrow transformations are fused into the same stage.
  * Each stage contains **tasks**, one per partition of its input RDD.

---

## 🔷 8. **Task Scheduler**

* Assigns **tasks** from each stage to available executor slots.
* Responsible for local/remote task scheduling, retries, and speculative execution.

---

## 🔷 9. **Execution on Executors**

* Each executor:

  * Pulls its input **partition** (from a data source, cache, or shuffle output).
  * Runs the task code (physical operators for that partition).
  * Writes results to:

    * The driver (if final action collects)
    * External storage
    * Local disk (shuffle output for downstream stages)
* Executors also:

  * **Cache** results if `.cache()` or `.persist()` is used
  * **Spill to disk** when memory is exceeded (local worker node disk, configured via `spark.local.dir`)
  * Store **shuffle files** on local disk during wide transformations

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
Physical Plan Generation (CBO chooses best)
   ↓
RDD Execution Plan (operators mapped to RDDs)
   ↓
DAG of Stages (split at shuffles)
   ↓
DAG Scheduler → Jobs → Stages
   ↓
Task Scheduler → Tasks per partition
   ↓
Executors run tasks (read partition → compute → write/send results)
```

---


## Spark UI:
- Resource 1: https://www.youtube.com/watch?v=rNpzrkB5KQQ
* Default port: `localhost:4040`

### **Sections in the UI:**

**Jobs:** Gives you a list of completed, failed, active jobs, timeline ...; And you can click on a Job to get a proper detail of it with other infos. The job detail pages for a particular job shows you a DAG made up of 1 or more stages.
**Stages:** Its a child object of a Job, you can see detail for a stage of a particular Job (created per action on un-cached DF) and tasks in each stage and how these tasks are spread across executors; Breakdown of each job into stages.
**Storage:** Shows cached data (.cache()), you come here to see what's already cache; It doesn't hold your processed data on disk it holds only caches; stores query plan; Once the cluster restarts it loss cache data in memory.
**Environments:** Shows environemt configs and variables.
**Executors:** Shows you number of executors in the cluster and how busy they are.
**SQL/DF:** Shows you DAG of Optimized Query Plan, how spark has interpreted the query and the query plan it is using to process data.

> Use the DAG tab to understand how your transformations are executed under the hood.
> **DAG Visualization**: Optimized DAG per job, showing Stages, transformations and shuffles.


The type of transformation we are doing will denote whether we need a shuffle.
**Wide transformation:** This type of transformation can't be done on a single executor, it has to move data from executors, shuffle it around, change how that data is chunked up, put it back on those executors.

If I have written a bunch of transformation on a DF and then I specify an action `.show()` on that DF. It will traverse upward to the list of used transformation that has to be done on the DF (Narrow and Wide trans), and then, it make a query plan, and in that plan any time it is having to shuffle data, a new stage is created (which is handling a number of tasks). Each time there is a shuffle, there will be a new stage with a new number of tasks to be executed by executors.

As the processes moves to the next stage, other tasks from the previous stages are taken out so that new tasks from the new stage can run (task can be more than 1, and 1 task is ran on 1 slot (CPU core)).



---



### Understanding DAG (Directed Acyclic Graph)

- Directed -> The edges (arrows) have a direction (A -> B -> C).
- Acyclic -> There are no loops; You can't go back to an ealier point (no A -> B -> A; only A -> B -> C -> D -> ...)
- Graph -> A structure made of nodes (points) and edges (arrows).

**In simple term:** A DAG is like a flowchart where each step leads forward never looping back.
- Represents the lineage and dependencies between operations.
- **Optimized DAG** is created after the Catalyst optimizer finishes planning.

> DAG = Blueprint for execution. Spark uses this to break computation into stages and tasks.


**Analogy:**
  - DAG = entire recipe
  - Stage = Parts of the recipe you must complete before moving to the next
  - Task = Individual step like "cut one onion" (done in parallel)

#### Why does Spark use DAG?
- Optimizations:
  - Spark can see the entire chain of transformations before running them.
  - This allows it to optimize, execution (e.g, wide transformation into 1 stage)
- Fault Tolerance:
  - If a worker node fails, spark can re-compute list data by following the DAG lineage (because the DAG tells Spark how the data was created)
- Better Scheduling:
  - Spark can break the DAG into stages and schedule task efficiently across the cluster.


---


## DAG in Apache Spark & Its UI Visualizations

Apache Spark builds a **Directed Acyclic Graph (DAG)** to represent the logical flow of computations. A DAG is a **graph without cycles**, where each node represents a computation (e.g., transformation), and edges represent data dependencies.

---

## 🧠 What is a DAG in Spark?

* A **DAG** is Spark’s way of tracking **lineage**, **fault tolerance**, and **execution plan**.
* Spark does **not** execute instructions line-by-line. Instead:

  1. It builds a **logical plan** from your code.
  2. Optimizes it into a **physical plan**.
  3. Breaks it into **stages** based on **shuffle boundaries**.
  4. Finally, schedules **tasks** for each partition of data in a stage.

---

## 📊 DAG-Related Diagrams in Spark UI

Spark’s Web UI (`localhost:4040`) offers multiple **DAG visualizations** based on the execution level:

---

### 1️⃣ **Job-Level DAG (Execution DAG)**

Where?: Click on a Job → DAG Visualization

#### 📌 What It Shows:

* A high-level diagram of how the Job is split into **Stages**.
* Each **block** = one **Stage**
* Arrows between blocks = **Shuffle dependencies**
* Nodes inside = operations/tasks within the stage

#### 🧠 Key Concepts:

* A **Job** is triggered by an **Action** (e.g., `count()`, `show()`)
* Spark **combines narrow transformations** into a single stage
* **Shuffle** triggers stage boundaries → wide transformations

---

### 2️⃣ **Stage-Level DAG (RDD Lineage / Stage DAG)**

🔍 Where?:  **Stages Tab** → Click on a Stage → RDD Graph

#### 📌 What It Shows:

* Internal **RDD dependencies** used in that stage
* Boxes = RDDs
* Arrows = transformation dependencies (e.g., `map`, `flatMap`)

#### 🧠 Key Concepts:

* Helps visualize the **lineage graph** of data
* Useful for debugging or understanding transformation chains

---

### 3️⃣ **SQL/DataFrame Plan DAG (Query Plan Tree)**
Where?: **SQL/DataFrame Tab** → Click on a query → View Physical/Logical Plans

#### 📌 What It Shows:

* A **tree-based DAG** of query processing stages:

  * **Parsed Logical Plan**
  * **Analyzed Logical Plan**
  * **Optimized Logical Plan**
  * **Physical Plan**

#### 🧠 Key Concepts:

* Built by the **Catalyst Optimizer**
* Reflects rule-based optimizations (e.g., predicate pushdown)
* Not visible for **RDD-only** applications

---

## ✅ Summary Table

| UI Section        | Visualization Type       | Focus                                 | Triggered By             |
| ----------------- | ------------------------ | ------------------------------------- | ------------------------ |
| **Jobs**          | Job DAG / Execution Plan | Stages & shuffles                     | Spark Actions            |
| **Stages**        | RDD Lineage DAG          | Transformation chain within a stage   | Each stage in a job      |
| **SQL/DataFrame** | Query Plan Tree          | Logical & physical query optimization | DataFrame or SQL Queries |

---




What I am lacking in Spsark:
- The proper Flow
- Dags diagrame
- Physical Plans and their graphical reprentations.